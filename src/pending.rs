//! Speculative oracle updates observed before canonical inclusion.
//!
//! Pending values are intentionally isolated from [`crate::OracleTracker`].

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

use alloy_primitives::{Address, B256, Bytes, I256};
use tokio::sync::broadcast;

use crate::{FeedId, FeedRegistration, OracleAdapterId};

mod adapter;
mod chainlink;
#[cfg(feature = "pending-oracle-ethereum")]
mod ethereum;
#[cfg(feature = "pending-oracle-mev-share")]
mod mev_share;
mod source;

use adapter::adapter_owns_update;
pub use adapter::{
    ChainlinkPendingAdapter, PendingOracleAdapter, PendingOracleAdapterError,
    PendingOracleAdapterFailure, PendingOracleAdapterObservation, PendingOracleCandidateReport,
    PendingOracleInterest,
};
pub use chainlink::{
    CHAINLINK_FORWARD_SELECTOR, CHAINLINK_SVR_FORWARD_SELECTOR,
    CHAINLINK_TRANSMIT_SECONDARY_SELECTOR, CHAINLINK_TRANSMIT_SELECTOR, ChainlinkPendingReport,
    DecodedChainlinkOcr2, PendingOracleDecodeError, decode_chainlink_ocr2_calldata,
};
#[cfg(feature = "pending-oracle-ethereum")]
pub use ethereum::AlchemyPendingTransactionSource;
#[cfg(feature = "pending-oracle-mev-share")]
pub use mev_share::MevSharePendingTransactionSource;
pub use source::{
    PendingOracleCandidateSource, PendingOracleCoverageGap, PendingOracleSourceDescriptor,
    PendingOracleSourceError, PendingOracleSourceFuture, PendingOracleSourceHealth,
    PendingOracleSourceSession, PendingOracleSourceSink, PendingOracleSourceState,
};

/// Ethereum mainnet chain id.
pub const ETHEREUM_MAINNET_CHAIN_ID: u64 = 1;

/// Opt-in configuration for the pending-oracle channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOracleConfig {
    chain_id: u64,
    feed_filter: PendingFeedFilter,
    sources: BTreeSet<PendingOracleSource>,
    channel_capacity: usize,
}

impl PendingOracleConfig {
    /// Observe pending-capable feeds registered on an explicit chain.
    pub const fn for_chain(chain_id: u64) -> Self {
        Self {
            chain_id,
            feed_filter: PendingFeedFilter::AllRegistered,
            sources: BTreeSet::new(),
            channel_capacity: 1_024,
        }
    }

    /// Observe pending-capable feeds registered on Ethereum mainnet.
    pub const fn ethereum_mainnet() -> Self {
        Self::for_chain(ETHEREUM_MAINNET_CHAIN_ID)
    }

    /// Return the configured chain id.
    pub const fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Restrict pending observation to ids already present in the oracle registry.
    pub fn registered_feeds(mut self, filter: PendingFeedFilter) -> Self {
        self.feed_filter = filter;
        self
    }

    /// Return the filter applied to registered feeds.
    pub const fn feed_filter(&self) -> &PendingFeedFilter {
        &self.feed_filter
    }

    /// Enable candidates received from an Ethereum public pending-transaction stream.
    pub fn public_mempool(mut self) -> Self {
        self.sources.insert(PendingOracleSource::PublicMempool);
        self
    }

    /// Enable candidates received from the Flashbots MEV-Share event stream.
    pub fn mev_share(mut self) -> Self {
        self.sources.insert(PendingOracleSource::MevShare);
        self
    }

    /// Enable candidates received from one built-in or caller-defined transport family.
    pub fn source(mut self, source: PendingOracleSource) -> Self {
        self.sources.insert(source);
        self
    }

    /// Return whether a pending transport is enabled.
    pub fn source_enabled(&self, source: &PendingOracleSource) -> bool {
        self.sources.contains(source)
    }

    /// Iterate over enabled pending transports.
    pub fn sources(&self) -> impl Iterator<Item = PendingOracleSource> + '_ {
        self.sources.iter().cloned()
    }

    /// Set the bounded pending-event capacity. Zero is clamped to one.
    pub fn channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = channel_capacity.max(1);
        self
    }

    /// Return the bounded pending-event capacity.
    pub const fn channel_capacity_value(&self) -> usize {
        self.channel_capacity
    }
}

/// Built-in and caller-defined pre-inclusion transport families.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PendingOracleSource {
    /// Ethereum node pending-transaction propagation.
    PublicMempool,
    /// Flashbots MEV-Share transaction and bundle hints.
    MevShare,
    /// Caller-defined transport family with adapter-owned semantics.
    Custom(String),
}

impl PendingOracleSource {
    /// Construct a caller-defined transport family.
    pub fn custom(id: impl Into<String>) -> Self {
        Self::Custom(id.into())
    }

    /// Return the stable transport-family identifier.
    pub fn as_str(&self) -> &str {
        match self {
            Self::PublicMempool => "ethereum-public-mempool",
            Self::MevShare => "flashbots-mev-share",
            Self::Custom(id) => id,
        }
    }
}

/// Selection over feeds already registered with [`crate::OracleRuntime`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum PendingFeedFilter {
    /// Observe every registered feed supported by an installed pending adapter.
    #[default]
    AllRegistered,
    /// Observe only these stable registered feed ids.
    Only(BTreeSet<FeedId>),
}

impl PendingFeedFilter {
    /// Select a subset by stable feed id without repeating registration metadata.
    pub fn only<I, S>(ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Only(ids.into_iter().map(|id| FeedId::new(id.into())).collect())
    }

    fn includes(&self, id: &FeedId) -> bool {
        match self {
            Self::AllRegistered => true,
            Self::Only(ids) => ids.contains(id),
        }
    }
}

/// Runtime view of registered feeds eligible for pending observation.
#[derive(Clone)]
pub struct PendingOracleRuntime {
    config: PendingOracleConfig,
    feed_ids: Arc<Mutex<Vec<FeedId>>>,
    registrations: Arc<Mutex<Vec<FeedRegistration>>>,
    adapters: Arc<Mutex<BTreeMap<OracleAdapterId, Arc<dyn PendingOracleAdapter>>>>,
    sender: broadcast::Sender<PendingOracleStreamEvent>,
    sequence: Arc<AtomicU64>,
    updates: Arc<Mutex<BTreeMap<PendingOracleUpdateId, PendingOracleUpdate>>>,
    source_health: Arc<Mutex<BTreeMap<PendingOracleSourceId, PendingOracleSourceHealth>>>,
}

impl std::fmt::Debug for PendingOracleRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingOracleRuntime")
            .field("config", &self.config)
            .field(
                "feed_ids",
                &self
                    .feed_ids
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()),
            )
            .field(
                "adapter_ids",
                &self
                    .adapters
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .keys()
                    .collect::<Vec<_>>(),
            )
            .field("update_count", &self.update_count())
            .finish_non_exhaustive()
    }
}

impl PendingOracleRuntime {
    pub(crate) fn from_registrations<'a>(
        config: PendingOracleConfig,
        registrations: impl IntoIterator<Item = &'a FeedRegistration>,
        adapters: impl IntoIterator<Item = Arc<dyn PendingOracleAdapter>>,
    ) -> Self {
        let feed_filter = config.feed_filter().clone();
        let (sender, _) = broadcast::channel(config.channel_capacity_value());
        let registrations = registrations
            .into_iter()
            .filter(|registration| feed_filter.includes(&registration.id))
            .cloned()
            .collect::<Vec<_>>();
        let mut installed = BTreeMap::<OracleAdapterId, Arc<dyn PendingOracleAdapter>>::new();
        let chainlink: Arc<dyn PendingOracleAdapter> = Arc::new(ChainlinkPendingAdapter);
        installed.insert(chainlink.adapter_id(), chainlink);
        for adapter in adapters {
            installed.entry(adapter.adapter_id()).or_insert(adapter);
        }
        Self {
            config,
            feed_ids: Arc::new(Mutex::new(
                registrations
                    .iter()
                    .map(|registration| registration.id.clone())
                    .collect(),
            )),
            registrations: Arc::new(Mutex::new(registrations)),
            adapters: Arc::new(Mutex::new(installed)),
            sender,
            sequence: Arc::new(AtomicU64::new(0)),
            updates: Arc::new(Mutex::new(BTreeMap::new())),
            source_health: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Return adapter-owned transport filters for the current inherited feed scope.
    pub fn interests(&self) -> Vec<PendingOracleInterest> {
        let registrations = self
            .registrations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        self.adapters
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .flat_map(|adapter| adapter.interests(&registrations))
            .collect()
    }

    /// Register an additional oracle-family pending adapter.
    ///
    /// Returns `false` when an adapter with the same stable id is already installed.
    pub fn register_adapter<A>(&self, adapter: A) -> bool
    where
        A: PendingOracleAdapter,
    {
        let id = adapter.adapter_id();
        let mut adapters = self
            .adapters
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if adapters.contains_key(&id) {
            return false;
        }
        adapters.insert(id, Arc::new(adapter));
        true
    }

    /// Return the pending-channel configuration.
    pub const fn config(&self) -> &PendingOracleConfig {
        &self.config
    }

    /// Return registered feed ids inherited by the pending channel.
    pub fn feed_ids(&self) -> Vec<FeedId> {
        self.feed_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Subscribe to the speculative pending-event channel.
    pub fn subscribe(&self) -> PendingOracleSubscription {
        PendingOracleSubscription {
            receiver: self.sender.subscribe(),
        }
    }

    /// Create a publisher for an installed pending source or adapter.
    pub fn publisher(&self) -> PendingOraclePublisher {
        PendingOraclePublisher {
            sender: self.sender.clone(),
            sequence: Arc::clone(&self.sequence),
        }
    }

    /// Start a pluggable pending source and return its owned task session.
    pub fn start_source<S>(
        &self,
        source: S,
    ) -> Result<PendingOracleSourceSession, PendingOracleSourceError>
    where
        S: PendingOracleCandidateSource,
    {
        let descriptor = source.descriptor();
        if !self.config.source_enabled(&descriptor.source) {
            return Err(PendingOracleSourceError::SourceDisabled {
                transport: descriptor.source.clone(),
            });
        }
        if self
            .source_health
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&descriptor.id)
            .is_some_and(|health| health.state != PendingOracleSourceState::Stopped)
        {
            return Err(PendingOracleSourceError::DuplicateSource {
                id: descriptor.id.clone(),
            });
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| PendingOracleSourceError::RuntimeUnavailable)?;
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let sink = PendingOracleSourceSink::new(self.clone(), descriptor.clone());
        sink.connecting();
        let task_sink = sink.clone();
        let task = handle.spawn(async move {
            let result = Box::new(source).run(task_sink.clone(), receiver).await;
            match &result {
                Ok(()) => task_sink.stopped(),
                Err(error) => task_sink.failed(error),
            }
            result
        });
        Ok(PendingOracleSourceSession::new(descriptor, shutdown, task))
    }

    /// Return the latest health snapshot for one concrete source.
    pub fn source_health(
        &self,
        source_id: &PendingOracleSourceId,
    ) -> Option<PendingOracleSourceHealth> {
        self.source_health
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(source_id)
            .cloned()
    }

    pub(crate) fn insert_source_health(&self, health: PendingOracleSourceHealth) {
        self.source_health
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(health.descriptor.id.clone(), health.clone());
        self.publisher()
            .publish(PendingOracleEvent::SourceHealthChanged(health));
    }

    pub(crate) fn mutate_source_health(
        &self,
        descriptor: &PendingOracleSourceDescriptor,
        mutate: impl FnOnce(&mut PendingOracleSourceHealth),
    ) {
        let mut health = self
            .source_health
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let health =
            health
                .entry(descriptor.id.clone())
                .or_insert_with(|| PendingOracleSourceHealth {
                    descriptor: descriptor.clone(),
                    state: PendingOracleSourceState::Connecting,
                    last_transport_message_at: None,
                    last_candidate_at: None,
                    coverage_gap_count: 0,
                    last_error: None,
                });
        mutate(health);
    }

    pub(crate) fn update_source_health(
        &self,
        descriptor: &PendingOracleSourceDescriptor,
        mutate: impl FnOnce(&mut PendingOracleSourceHealth),
    ) {
        let health = {
            let mut sources = self
                .source_health
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let health =
                sources
                    .entry(descriptor.id.clone())
                    .or_insert_with(|| PendingOracleSourceHealth {
                        descriptor: descriptor.clone(),
                        state: PendingOracleSourceState::Connecting,
                        last_transport_message_at: None,
                        last_candidate_at: None,
                        coverage_gap_count: 0,
                        last_error: None,
                    });
            mutate(health);
            health.clone()
        };
        self.publisher()
            .publish(PendingOracleEvent::SourceHealthChanged(health));
    }

    /// Record and stream one proposed update, collapsing exact repeats by update id.
    pub fn observe(&self, update: PendingOracleUpdate) -> PendingOracleObserveOutcome {
        if update.status != PendingOracleStatus::Pending
            || update
                .transmissions
                .iter()
                .any(|transmission| transmission.status != PendingOracleTransmissionStatus::Pending)
        {
            return PendingOracleObserveOutcome::Conflict;
        }
        let feed_ids = self
            .feed_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if update.feed_ids.iter().any(|id| !feed_ids.contains(id)) {
            return PendingOracleObserveOutcome::OutsideScope;
        }
        drop(feed_ids);
        let mut updates = self
            .updates
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(existing) = updates.get_mut(&update.id) {
            if existing.status != PendingOracleStatus::Pending {
                return PendingOracleObserveOutcome::Conflict;
            }
            if existing.feed_ids != update.feed_ids
                || existing.evidence != update.evidence
                || existing.proposed_value != update.proposed_value
            {
                return PendingOracleObserveOutcome::Conflict;
            }

            if update.transmissions.iter().any(|candidate| {
                existing
                    .transmissions
                    .iter()
                    .find(|tracked| tracked.id == candidate.id)
                    .is_some_and(|tracked| !tracked.same_variant_content(candidate))
            }) {
                return PendingOracleObserveOutcome::Conflict;
            }

            let added = update
                .transmissions
                .into_iter()
                .filter(|candidate| {
                    !existing
                        .transmissions
                        .iter()
                        .any(|tracked| tracked.id == candidate.id)
                })
                .collect::<Vec<_>>();
            existing.transmissions.extend(added.iter().cloned());
            drop(updates);

            for transmission in &added {
                self.publisher()
                    .publish(PendingOracleEvent::TransmissionAdded {
                        update_id: update.id,
                        transmission: transmission.clone(),
                    });
            }
            return if added.is_empty() {
                PendingOracleObserveOutcome::Duplicate
            } else {
                PendingOracleObserveOutcome::TransmissionAdded { count: added.len() }
            };
        }
        updates.insert(update.id, update.clone());
        drop(updates);
        self.publisher()
            .publish(PendingOracleEvent::Observed(update));
        PendingOracleObserveOutcome::Observed
    }

    /// Route one transport candidate through every interested pending adapter.
    pub fn observe_candidate(
        &self,
        candidate: PendingTransportCandidate,
    ) -> Result<PendingOracleCandidateReport, PendingOracleAdapterError> {
        if candidate.chain_id != self.config.chain_id() {
            return Err(PendingOracleAdapterError::WrongChain {
                expected: self.config.chain_id(),
                observed: candidate.chain_id,
            });
        }
        if !self.config.source_enabled(&candidate.source) {
            return Err(PendingOracleAdapterError::SourceDisabled(
                candidate.source.clone(),
            ));
        }
        let registrations = self
            .registrations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let adapters = self
            .adapters
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut report = PendingOracleCandidateReport::default();
        for adapter in adapters {
            if !adapter
                .interests(&registrations)
                .iter()
                .any(|interest| interest.matches(&candidate))
            {
                continue;
            }
            let adapter_id = adapter.adapter_id();
            let updates = match adapter.decode(&candidate, &registrations) {
                Ok(updates) => updates,
                Err(error) => {
                    report
                        .failures
                        .push(PendingOracleAdapterFailure { adapter_id, error });
                    continue;
                }
            };
            for update in updates {
                let update_id = update.id;
                report.observations.push(PendingOracleAdapterObservation {
                    adapter_id: adapter_id.clone(),
                    update_id,
                    outcome: self.observe(update),
                });
            }
        }
        Ok(report)
    }

    /// Return one tracked pending update by content identity.
    pub fn update(&self, id: &PendingOracleUpdateId) -> Option<PendingOracleUpdate> {
        self.updates
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(id)
            .cloned()
    }

    /// Return the number of distinct tracked pending updates.
    pub fn update_count(&self) -> usize {
        self.updates
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len()
    }

    /// Resolve a transmission after an adapter matched its committed oracle signal.
    ///
    /// A successful receipt alone is not sufficient evidence. Callers should
    /// invoke this only after matching the oracle-family confirmation emitted by
    /// the same committed transaction.
    pub fn resolve_confirmed_transmission(
        &self,
        resolution: PendingOracleResolution,
    ) -> PendingOracleResolveOutcome {
        let mut updates = self
            .updates
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(update) = updates.get_mut(&resolution.update_id) else {
            return PendingOracleResolveOutcome::UnknownUpdate;
        };
        let Some(transmission) = update
            .transmissions
            .iter_mut()
            .find(|transmission| transmission.id == resolution.transmission_id)
        else {
            return PendingOracleResolveOutcome::UnknownTransmission;
        };
        if transmission
            .ordering
            .transaction_hash()
            .is_some_and(|hash| hash != resolution.transaction_hash)
        {
            return PendingOracleResolveOutcome::Conflict;
        }
        let target = match resolution.kind {
            PendingOracleResolutionKind::Landed => PendingOracleTransmissionStatus::Landed,
            PendingOracleResolutionKind::Reverted => PendingOracleTransmissionStatus::Reverted,
        };
        if transmission.status == target {
            return PendingOracleResolveOutcome::Duplicate;
        }
        if transmission.status != PendingOracleTransmissionStatus::Pending {
            return PendingOracleResolveOutcome::Conflict;
        }
        transmission.status = target;
        update.status =
            if resolution.kind == PendingOracleResolutionKind::Landed {
                PendingOracleStatus::Landed
            } else if update.transmissions.iter().all(|transmission| {
                transmission.status == PendingOracleTransmissionStatus::Reverted
            }) {
                PendingOracleStatus::Reverted
            } else {
                PendingOracleStatus::Pending
            };
        drop(updates);
        self.publisher()
            .publish(PendingOracleEvent::Resolved(resolution));
        PendingOracleResolveOutcome::Resolved
    }

    /// Correlate one committed oracle log with pending transaction variants.
    ///
    /// Only an adapter-specific confirmation emitted by the same transaction
    /// hash resolves a candidate as landed. Unrelated successful transactions
    /// and removed logs are ignored.
    pub fn observe_confirmed_log(
        &self,
        log: &crate::alloy_rpc_types_eth::Log,
    ) -> Vec<PendingOracleResolution> {
        if log.removed {
            return Vec::new();
        }
        let (Some(transaction_hash), Some(block_number)) = (log.transaction_hash, log.block_number)
        else {
            return Vec::new();
        };
        let updates = self
            .updates
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|update| update.status == PendingOracleStatus::Pending)
            .cloned()
            .collect::<Vec<_>>();
        let adapters = self
            .adapters
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut resolutions = Vec::new();
        for update in updates {
            if !adapters.iter().any(|adapter| {
                adapter_owns_update(&adapter.adapter_id(), &update)
                    && adapter.confirms(&update, log)
            }) {
                continue;
            }
            for transmission in update.transmissions.iter().filter(|transmission| {
                transmission.status == PendingOracleTransmissionStatus::Pending
                    && transmission.ordering.transaction_hash() == Some(transaction_hash)
            }) {
                let resolution = PendingOracleResolution::new(
                    update.id,
                    transmission.id,
                    transaction_hash,
                    block_number,
                    PendingOracleResolutionKind::Landed,
                );
                if self.resolve_confirmed_transmission(resolution.clone())
                    == PendingOracleResolveOutcome::Resolved
                {
                    resolutions.push(resolution);
                }
            }
        }
        resolutions
    }

    /// Decode, route, and record one possible Chainlink OCR2 transmission.
    pub fn observe_chainlink_candidate(
        &self,
        candidate: PendingTransportCandidate,
    ) -> Result<PendingOracleCandidateOutcome, PendingOracleAdapterError> {
        match self.observe_candidate(candidate) {
            Ok(report) => {
                if let Some(failure) = report
                    .failures
                    .into_iter()
                    .find(|failure| failure.adapter_id.as_str() == ChainlinkPendingAdapter::ID)
                {
                    return Err(failure.error);
                }
                Ok(report
                    .observations
                    .into_iter()
                    .find(|observation| {
                        observation.adapter_id.as_str() == ChainlinkPendingAdapter::ID
                    })
                    .map_or(PendingOracleCandidateOutcome::Ignored, |observation| {
                        PendingOracleCandidateOutcome::Tracked(observation.outcome)
                    }))
            }
            Err(PendingOracleAdapterError::WrongChain { expected, observed }) => {
                Ok(PendingOracleCandidateOutcome::WrongChain { expected, observed })
            }
            Err(PendingOracleAdapterError::SourceDisabled(source)) => {
                Ok(PendingOracleCandidateOutcome::SourceDisabled(source))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn refresh<'a>(
        &mut self,
        registrations: impl IntoIterator<Item = &'a FeedRegistration>,
    ) {
        let registrations = registrations
            .into_iter()
            .filter(|registration| self.config.feed_filter().includes(&registration.id))
            .cloned()
            .collect::<Vec<_>>();
        *self
            .feed_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = registrations
            .iter()
            .map(|registration| registration.id.clone())
            .collect();
        *self
            .registrations
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = registrations.clone();

        let in_scope = self
            .feed_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut expired = Vec::new();
        let mut updates = self
            .updates
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        updates.retain(|id, update| {
            update.feed_ids.retain(|feed_id| {
                if !in_scope.contains(feed_id) {
                    return false;
                }
                match &update.evidence {
                    PendingOracleEvidence::ChainlinkReport(report) => {
                        registrations.iter().any(|registration| {
                            &registration.id == feed_id
                                && registration.current_aggregator == Some(report.aggregator)
                        })
                    }
                    PendingOracleEvidence::Adapter { .. } => true,
                }
            });
            if update.feed_ids.is_empty() {
                expired.push(*id);
                false
            } else {
                true
            }
        });
        drop(updates);
        for id in expired {
            self.publisher().publish(PendingOracleEvent::Expired(id));
        }
    }
}

/// Outcome of submitting one observation to the pending tracker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingOracleObserveOutcome {
    /// A new update identity was recorded and streamed.
    Observed,
    /// The report was known but one or more new transaction variants were added.
    TransmissionAdded {
        /// Number of newly recorded variants.
        count: usize,
    },
    /// The update identity was already present.
    Duplicate,
    /// The same update or transmission identity carried different immutable content.
    Conflict,
    /// At least one affected feed is not in this runtime's inherited pending scope.
    OutsideScope,
}

/// Outcome of routing one generic transport candidate through a pending adapter.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingOracleCandidateOutcome {
    /// The candidate did not match a supported pending oracle call.
    Ignored,
    /// Its transport was not enabled by [`PendingOracleConfig`].
    SourceDisabled(PendingOracleSource),
    /// The observation came from a chain other than the explicitly configured chain.
    WrongChain {
        /// Configured chain id.
        expected: u64,
        /// Candidate chain id.
        observed: u64,
    },
    /// The report was decoded but its aggregator is not registered in pending scope.
    AggregatorOutsideScope(Address),
    /// The candidate produced or updated a tracked pending report.
    Tracked(PendingOracleObserveOutcome),
}

/// Report-level pending lifecycle state.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PendingOracleStatus {
    /// At least one transport variant may still land.
    #[default]
    Pending,
    /// A variant landed and emitted its matching committed oracle signal.
    Landed,
    /// Every known variant resolved without the matching oracle update.
    Reverted,
}

/// Oracle-confirmed outcome of one committed transmission.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingOracleResolutionKind {
    /// The transaction landed and emitted the expected oracle signal.
    Landed,
    /// The transaction reverted or landed without the expected oracle signal.
    Reverted,
}

/// Correlation between one pending transport variant and its committed outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOracleResolution {
    /// Stable report identity.
    pub update_id: PendingOracleUpdateId,
    /// Stable transport variant identity.
    pub transmission_id: PendingOracleTransmissionId,
    /// Hash of the committed transaction.
    pub transaction_hash: B256,
    /// Block containing the resolved attempt.
    pub block_number: u64,
    /// Oracle-confirmed outcome.
    pub kind: PendingOracleResolutionKind,
}

impl PendingOracleResolution {
    /// Construct a committed resolution after matching its oracle signal.
    pub const fn new(
        update_id: PendingOracleUpdateId,
        transmission_id: PendingOracleTransmissionId,
        transaction_hash: B256,
        block_number: u64,
        kind: PendingOracleResolutionKind,
    ) -> Self {
        Self {
            update_id,
            transmission_id,
            transaction_hash,
            block_number,
            kind,
        }
    }
}

/// Outcome of correlating a committed transmission with pending state.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingOracleResolveOutcome {
    /// Pending state and stream were updated.
    Resolved,
    /// This exact outcome was already recorded.
    Duplicate,
    /// No pending report has this update identity.
    UnknownUpdate,
    /// The report exists but not this transport variant.
    UnknownTransmission,
    /// Immutable transaction identity or a previous resolution disagreed.
    Conflict,
}

/// Transport observation supplied to an installed pending oracle adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingTransportCandidate {
    /// Chain where the transaction was observed.
    pub chain_id: u64,
    /// Stable identity of this transport variant.
    pub id: PendingOracleTransmissionId,
    /// Built-in or caller-defined transport family.
    pub source: PendingOracleSource,
    /// Concrete node, relay, or consumer-defined source identifier.
    pub source_id: PendingOracleSourceId,
    /// Outer transaction destination.
    pub to: Address,
    /// Outer transaction calldata.
    pub calldata: Bytes,
    /// Ordering reference disclosed by the source.
    pub ordering: PendingOracleOrderingHandle,
    /// Chain head at which the candidate was observed.
    pub observed_at_head: Option<u64>,
}

impl PendingTransportCandidate {
    /// Construct a transport candidate for pending adapter routing.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: u64,
        id: PendingOracleTransmissionId,
        source: PendingOracleSource,
        source_id: PendingOracleSourceId,
        to: Address,
        calldata: Bytes,
        ordering: PendingOracleOrderingHandle,
        observed_at_head: Option<u64>,
    ) -> Self {
        Self {
            chain_id,
            id,
            source,
            source_id,
            to,
            calldata,
            ordering,
            observed_at_head,
        }
    }
}

/// Stable identity for one proposed oracle update across transport variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PendingOracleUpdateId(B256);

impl PendingOracleUpdateId {
    /// Construct an update id from an adapter-defined content hash.
    pub const fn from_hash(hash: B256) -> Self {
        Self(hash)
    }

    /// Return the underlying content hash.
    pub const fn as_hash(&self) -> B256 {
        self.0
    }
}

/// Stable identity for one transaction variant carrying a proposed update.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PendingOracleTransmissionId(B256);

impl PendingOracleTransmissionId {
    /// Construct a transmission id from a transaction or transport content hash.
    pub const fn from_hash(hash: B256) -> Self {
        Self(hash)
    }

    /// Return the underlying content hash.
    pub const fn as_hash(&self) -> B256 {
        self.0
    }
}

/// Stable identifier for a pending transport such as a node or relay stream.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PendingOracleSourceId(String);

impl PendingOracleSourceId {
    /// Construct a source identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the source identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PendingOracleSourceId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Oracle-specific call path carrying a proposed update.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingOracleRoute {
    /// A Chainlink OCR transmission sent directly to an aggregator.
    ChainlinkDirect {
        /// Destination aggregator.
        aggregator: Address,
        /// Whether the secondary transmit entry point was used.
        secondary: bool,
    },
    /// A Chainlink OCR transmission wrapped by a forwarder.
    ChainlinkForwarded {
        /// Outer call destination.
        forwarder: Address,
        /// Inner aggregator destination.
        aggregator: Address,
        /// Whether the secondary transmit entry point was used.
        secondary: bool,
    },
    /// Adapter-owned route not represented by a built-in variant.
    Adapter {
        /// Adapter owning the route semantics.
        adapter_id: OracleAdapterId,
        /// Adapter-local route kind.
        kind: String,
    },
}

impl PendingOracleRoute {
    /// Construct an adapter-owned route.
    pub fn adapter(adapter_id: OracleAdapterId, kind: impl Into<String>) -> Self {
        Self::Adapter {
            adapter_id,
            kind: kind.into(),
        }
    }
}

/// Concrete handle a downstream searcher may use to order after a transmission.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum PendingOracleOrderingHandle {
    /// A hash exposed by MEV-Share for hash-bound backrunning.
    MevShare {
        /// Relay-provided transaction hash.
        hash: B256,
    },
    /// Complete signed Ethereum transaction suitable for exact simulation.
    RawEthereumTransaction {
        /// Hash of the signed transaction.
        tx_hash: B256,
        /// Encoded signed transaction envelope.
        signed_envelope: Bytes,
    },
    /// A transaction hash with no claim that a specific relay accepts it as a constraint.
    TransactionHashOnly {
        /// Observed transaction hash.
        tx_hash: B256,
    },
    /// Chain-specific sequencer preconfirmation metadata.
    SequencerPreconfirmation {
        /// Preconfirmed transaction hash.
        tx_hash: B256,
        /// Proposed block number.
        block_number: u64,
        /// Sequencer payload identity.
        payload_id: B256,
        /// Transaction position within the preconfirmed payload.
        sequence_index: u64,
    },
    /// The source disclosed no actionable ordering reference.
    #[default]
    None,
}

impl PendingOracleOrderingHandle {
    /// Return whether this handle contains a concrete transaction reference.
    pub const fn is_referenceable(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Return the transaction hash when this handle exposes one.
    pub const fn transaction_hash(&self) -> Option<B256> {
        match self {
            Self::MevShare { hash } => Some(*hash),
            Self::RawEthereumTransaction { tx_hash, .. }
            | Self::TransactionHashOnly { tx_hash }
            | Self::SequencerPreconfirmation { tx_hash, .. } => Some(*tx_hash),
            Self::None => None,
        }
    }
}

/// Fidelity available for an optimistic simulation.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PendingOracleSimulationFidelity {
    /// No sufficient transaction or state projection material was disclosed.
    InsufficientMaterial,
    /// Only the decoded proposed answer can be projected.
    AnswerOnlyProjection,
    /// Adapter-specific state effects were independently verified.
    VerifiedStateProjection,
    /// The complete signed transaction can execute against a disposable fork.
    ExactSignedTransaction,
}

/// Simulation material disclosed for one transmission.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingOracleSimulationMaterial {
    /// Execute the signed envelope carried by the ordering handle.
    ExactSignedTransaction,
    /// Apply independently verified adapter-specific state effects.
    VerifiedStateProjection,
    /// Project only the decoded answer.
    AnswerOnlyProjection,
    /// Do not use this candidate for an optimistic state simulation.
    InsufficientMaterial,
}

impl PendingOracleSimulationMaterial {
    /// Return the advertised simulation fidelity.
    pub const fn fidelity(self) -> PendingOracleSimulationFidelity {
        match self {
            Self::ExactSignedTransaction => PendingOracleSimulationFidelity::ExactSignedTransaction,
            Self::VerifiedStateProjection => {
                PendingOracleSimulationFidelity::VerifiedStateProjection
            }
            Self::AnswerOnlyProjection => PendingOracleSimulationFidelity::AnswerOnlyProjection,
            Self::InsufficientMaterial => PendingOracleSimulationFidelity::InsufficientMaterial,
        }
    }
}

/// Lifecycle state of one transaction variant.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PendingOracleTransmissionStatus {
    /// The variant has been observed but not canonically resolved.
    #[default]
    Pending,
    /// The transaction landed and emitted the matching oracle confirmation signal.
    Landed,
    /// The transaction landed but reverted or did not emit the expected signal.
    Reverted,
    /// The source no longer exposes this variant and it was not observed landing.
    Dropped,
    /// A newer variant replaced this transport attempt.
    Superseded,
}

/// One transaction or relay variant carrying a proposed oracle update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOracleTransmission {
    /// Stable transport identity.
    pub id: PendingOracleTransmissionId,
    /// Source that disclosed this variant.
    pub source: PendingOracleSourceId,
    /// Oracle-specific outer and inner call path.
    pub route: PendingOracleRoute,
    /// Concrete downstream ordering reference, if one was disclosed.
    pub ordering: PendingOracleOrderingHandle,
    /// Available optimistic simulation material.
    pub simulation: PendingOracleSimulationMaterial,
    /// Hash of the complete outer calldata.
    pub calldata_hash: B256,
    /// Chain head at which this source observation was made.
    pub observed_at_head: Option<u64>,
    /// Local first-observation timestamp.
    pub first_seen_at: SystemTime,
    /// Current transport lifecycle state.
    pub status: PendingOracleTransmissionStatus,
}

impl PendingOracleTransmission {
    /// Construct an unresolved transmission observed at the current local time.
    pub fn new(
        id: PendingOracleTransmissionId,
        source: PendingOracleSourceId,
        route: PendingOracleRoute,
        ordering: PendingOracleOrderingHandle,
        simulation: PendingOracleSimulationMaterial,
        calldata_hash: B256,
        observed_at_head: Option<u64>,
    ) -> Self {
        Self {
            id,
            source,
            route,
            ordering,
            simulation,
            calldata_hash,
            observed_at_head,
            first_seen_at: SystemTime::now(),
            status: PendingOracleTransmissionStatus::Pending,
        }
    }

    /// Return whether the transmission has a usable transaction reference.
    pub const fn is_referenceable(&self) -> bool {
        self.ordering.is_referenceable()
    }

    /// Return a coarse ranking suitable for selecting among otherwise valid variants.
    pub const fn actionability_rank(&self) -> u8 {
        match (&self.ordering, self.simulation.fidelity()) {
            (
                PendingOracleOrderingHandle::RawEthereumTransaction { .. },
                PendingOracleSimulationFidelity::ExactSignedTransaction,
            ) => 4,
            (PendingOracleOrderingHandle::MevShare { .. }, _) => 3,
            (PendingOracleOrderingHandle::RawEthereumTransaction { .. }, _) => 2,
            (PendingOracleOrderingHandle::TransactionHashOnly { .. }, _) => 1,
            (PendingOracleOrderingHandle::SequencerPreconfirmation { .. }, _) => 1,
            (PendingOracleOrderingHandle::None, _) => 0,
        }
    }

    fn same_variant_content(&self, other: &Self) -> bool {
        self.id == other.id
            && self.source == other.source
            && self.route == other.route
            && self.ordering == other.ordering
            && self.simulation == other.simulation
            && self.calldata_hash == other.calldata_hash
    }
}

/// Adapter-specific evidence supporting a pending oracle observation.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingOracleEvidence {
    /// A decoded Chainlink OCR2 report.
    ChainlinkReport(ChainlinkPendingReport),
    /// Evidence decoded by an oracle adapter not otherwise represented here.
    Adapter {
        /// Adapter that owns the evidence semantics.
        adapter_id: OracleAdapterId,
        /// Adapter-local evidence kind.
        kind: String,
        /// Content digest used for correlation and deduplication.
        digest: B256,
    },
}

impl PendingOracleEvidence {
    /// Construct generic adapter-owned evidence.
    pub fn adapter(adapter_id: OracleAdapterId, kind: impl Into<String>, digest: B256) -> Self {
        Self::Adapter {
            adapter_id,
            kind: kind.into(),
            digest,
        }
    }
}

/// Proposed update for one or more feeds already registered with the runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOracleUpdate {
    /// Stable content identity.
    pub id: PendingOracleUpdateId,
    /// Registered feeds affected by this update.
    pub feed_ids: Vec<FeedId>,
    /// Adapter evidence used to recognize the update.
    pub evidence: PendingOracleEvidence,
    /// Decoded proposed raw value when the adapter can supply one.
    pub proposed_value: Option<PendingOracleValue>,
    /// Distinct transaction or relay variants carrying the same update content.
    pub transmissions: Vec<PendingOracleTransmission>,
    /// Current report-level speculative lifecycle.
    pub status: PendingOracleStatus,
}

impl PendingOracleUpdate {
    /// Construct a pending update from its identity, affected feeds, and evidence.
    pub fn new(
        id: PendingOracleUpdateId,
        feed_ids: impl IntoIterator<Item = FeedId>,
        evidence: PendingOracleEvidence,
    ) -> Self {
        Self {
            id,
            feed_ids: feed_ids.into_iter().collect(),
            evidence,
            proposed_value: None,
            transmissions: Vec::new(),
            status: PendingOracleStatus::Pending,
        }
    }

    /// Attach the decoded proposed raw oracle value.
    pub fn with_proposed_value(mut self, proposed_value: PendingOracleValue) -> Self {
        self.proposed_value = Some(proposed_value);
        self
    }

    /// Attach one observed transaction or relay variant.
    pub fn with_transmission(mut self, transmission: PendingOracleTransmission) -> Self {
        self.transmissions.push(transmission);
        self
    }
}

/// Oracle-agnostic proposed raw answer decoded before canonical inclusion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOracleValue {
    /// Signed unscaled answer in the registered feed's native decimals.
    pub raw_answer: I256,
    /// Oracle-family timestamp associated with this proposed answer.
    pub observed_at: u64,
}

/// Typed event emitted by the speculative pending pipeline.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingOracleEvent {
    /// A newly observed proposed oracle update.
    Observed(PendingOracleUpdate),
    /// Another transaction or relay variant was observed for a known update.
    TransmissionAdded {
        /// Known update identity.
        update_id: PendingOracleUpdateId,
        /// Newly observed transport variant.
        transmission: PendingOracleTransmission,
    },
    /// A candidate no longer maps to an eligible feed or outlived its observation window.
    Expired(PendingOracleUpdateId),
    /// A committed attempt was correlated with its oracle-family confirmation.
    Resolved(PendingOracleResolution),
    /// A concrete source changed connection or coverage state.
    SourceHealthChanged(PendingOracleSourceHealth),
    /// A source reported an interval that cannot be backfilled reliably.
    CoverageGap(PendingOracleCoverageGap),
}

/// Sequence-numbered event delivered to pending subscribers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOracleStreamEvent {
    /// Monotonic sequence within one pending runtime.
    pub sequence: u64,
    /// Typed pending event.
    pub event: PendingOracleEvent,
}

/// Cloneable publishing handle used by pending sources and adapters.
#[derive(Clone, Debug)]
pub struct PendingOraclePublisher {
    sender: broadcast::Sender<PendingOracleStreamEvent>,
    sequence: Arc<AtomicU64>,
}

impl PendingOraclePublisher {
    /// Publish an event and return the number of active receivers that accepted it.
    pub fn publish(&self, event: PendingOracleEvent) -> usize {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        self.sender
            .send(PendingOracleStreamEvent { sequence, event })
            .unwrap_or(0)
    }
}

/// Receiving side of a bounded pending-oracle channel.
#[derive(Debug)]
pub struct PendingOracleSubscription {
    receiver: broadcast::Receiver<PendingOracleStreamEvent>,
}

impl PendingOracleSubscription {
    /// Wait for the next pending event.
    pub async fn recv(&mut self) -> Result<PendingOracleStreamEvent, PendingOracleRecvError> {
        self.receiver.recv().await.map_err(Into::into)
    }
}

/// Pending receiver failure with explicit lag reporting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PendingOracleRecvError {
    /// The publisher closed because the owning runtime was dropped.
    #[error("pending oracle channel closed")]
    Closed,
    /// This receiver fell behind the bounded channel.
    #[error("pending oracle receiver lagged by {missed} events")]
    Lagged {
        /// Number of events skipped by this receiver.
        missed: u64,
    },
}

impl From<broadcast::error::RecvError> for PendingOracleRecvError {
    fn from(error: broadcast::error::RecvError) -> Self {
        match error {
            broadcast::error::RecvError::Closed => Self::Closed,
            broadcast::error::RecvError::Lagged(missed) => Self::Lagged { missed },
        }
    }
}
