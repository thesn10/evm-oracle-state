use std::{
    collections::BTreeSet,
    fmt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::alloy_network::{Ethereum, Network};
use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::SolCall;
use evm_fork_cache::{
    ColdStartCall, ColdStartConfig, ColdStartPlan, ColdStartPlanner, ColdStartResults,
    ColdStartStep, StateView,
    cache::EvmCache,
    reactive::{
        EventSubscriber, HandlerId, InterestOwnerSubscriber, ReactiveBatchReport, ReactiveConfig,
        ReactiveEngine, ReactiveHandler, ReactiveInterest, ReactiveRuntime, SubscriberBackfill,
    },
};

use crate::{
    ChainlinkFeedProvider, FeedConfig, FeedId, FeedRegistration, OracleAdapterFeedSkip,
    OracleAdapterPlugin, OracleCodeRegistry, OracleCodeWarmupPolicy, OracleCodeWarmupReport,
    OracleDiscoveryContext, OracleError, OracleFeedReadinessReport, OracleFeedStatus,
    OracleHookEvent, OraclePrice, OraclePriceCorrected, OraclePriceUpdate, OracleReactiveHandler,
    OracleReadOverlay, OracleReconciler, OracleRegistry, OracleSignal, OracleStorageAdapter,
    OracleStorageSync, OracleTracker, RoundData, StalenessPolicy,
};

#[cfg(feature = "pending-oracle-updates")]
use evm_fork_cache::reactive::ReactiveInput;

#[cfg(feature = "pending-oracle-updates")]
use crate::{
    PendingOracleAdapter, PendingOracleCandidateSource, PendingOracleConfig, PendingOracleRuntime,
    PendingOracleSourceError, PendingOracleSourceSession,
};

type OracleEventCallback = Arc<dyn Fn(&OracleHookEvent) + Send + Sync>;

alloy_sol_types::sol! {
    function latestRoundData() external view returns (
        uint80 roundId,
        int256 answer,
        uint256 startedAt,
        uint256 updatedAt,
        uint80 answeredInRound
    );
}

/// Builder-friendly Chainlink feed registration.
#[derive(Clone, Debug)]
pub struct ChainlinkFeed {
    config: FeedConfig,
}

impl ChainlinkFeed {
    /// Create a feed registration for a Chainlink-compatible proxy.
    pub fn new(proxy: Address) -> Self {
        Self {
            config: FeedConfig {
                proxy,
                ..Default::default()
            },
        }
    }

    /// Set a stable feed id.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.config.id = Some(FeedId::new(id));
        self
    }

    /// Set a stable feed id.
    pub fn feed_id(mut self, id: FeedId) -> Self {
        self.config.id = Some(id);
        self
    }

    /// Set a human-readable label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.config.label = Some(label.into());
        self
    }

    /// Set the base symbol.
    pub fn base(mut self, base: impl Into<String>) -> Self {
        self.config.base = Some(base.into());
        self
    }

    /// Set the quote symbol.
    pub fn quote(mut self, quote: impl Into<String>) -> Self {
        self.config.quote = Some(quote.into());
        self
    }

    /// Set a max-age staleness policy.
    pub fn max_age_secs(mut self, max_age_secs: u64) -> Self {
        self.config.staleness = StalenessPolicy::max_age(max_age_secs);
        self
    }

    /// Set the full staleness policy.
    pub fn staleness(mut self, staleness: StalenessPolicy) -> Self {
        self.config.staleness = staleness;
        self
    }

    /// Allow zero or negative answers for signed generic feeds.
    pub fn allow_zero_or_negative_answer(mut self, allow: bool) -> Self {
        self.config.staleness = self.config.staleness.allow_zero_or_negative_answer(allow);
        self
    }

    /// Convert into the lower-level registration config.
    pub fn into_config(self) -> FeedConfig {
        self.config
    }
}

impl From<ChainlinkFeed> for FeedConfig {
    fn from(feed: ChainlinkFeed) -> Self {
        feed.into_config()
    }
}

/// Builder for a high-level oracle runtime facade.
pub struct OracleRuntimeBuilder<P> {
    provider: P,
    feeds: Vec<ChainlinkFeed>,
    now_timestamp: Option<u64>,
    storage_sync: OracleStorageSync,
    callbacks: Vec<OracleEventCallback>,
    #[cfg(feature = "pending-oracle-updates")]
    pending_config: Option<PendingOracleConfig>,
    #[cfg(feature = "pending-oracle-updates")]
    pending_adapters: Vec<Arc<dyn PendingOracleAdapter>>,
}

/// Cache-native runtime builder with first-class oracle adapter plugins.
pub struct OracleCacheRuntimeBuilder {
    adapters: Vec<Arc<dyn OracleAdapterPlugin>>,
    now_timestamp: Option<u64>,
    storage_sync: OracleStorageSync,
    storage_warmup_enabled: bool,
    storage_warmup_mode: OracleStorageWarmupMode,
    code_registry: OracleCodeRegistry,
    code_warmup_policy: OracleCodeWarmupPolicy,
    callbacks: Vec<OracleEventCallback>,
    #[cfg(feature = "pending-oracle-updates")]
    pending_config: Option<PendingOracleConfig>,
    #[cfg(feature = "pending-oracle-updates")]
    pending_adapters: Vec<Arc<dyn PendingOracleAdapter>>,
}

/// Best-effort cache-native oracle runtime build report.
#[non_exhaustive]
#[derive(Debug)]
pub struct OracleCacheRuntimeBuildReport {
    /// Runtime seeded with every successfully discovered feed.
    pub runtime: OracleRuntime<()>,
    /// Feeds that were skipped because an adapter could not register them.
    pub skipped: Vec<OracleAdapterFeedSkip>,
    /// Feed/runtime readiness after discovery, warmup, and skipped-source classification.
    pub feed_statuses: Vec<OracleFeedReadinessReport>,
    /// Storage slots warmed for direct oracle event writes.
    pub storage_warmup: OracleStorageWarmupReport,
    /// Bytecode seed/verify/etch results.
    pub code_warmup: OracleCodeWarmupReport,
}

/// Result of prewarming oracle storage slots through `evm-fork-cache`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OracleStorageWarmupReport {
    /// Warmup mode used for this run.
    pub mode: OracleStorageWarmupMode,
    /// Per-feed readiness after storage warmup.
    pub feed_statuses: Vec<OracleFeedReadinessReport>,
    /// Slots requested for warmup.
    pub requested_slots: usize,
    /// Slots successfully loaded into the cache.
    pub loaded_slots: usize,
    /// Slots that failed to load.
    pub failed_slots: Vec<OracleStorageWarmupFailure>,
    /// Read calls submitted to cold-start discovery.
    pub discovery_calls: usize,
    /// Cold-start summary when the warmup used `EvmCache::run_cold_start`.
    pub cold_start: Option<OracleColdStartWarmupReport>,
}

impl OracleStorageWarmupReport {
    /// Return true when no storage warmup work was requested or performed.
    pub fn is_empty(&self) -> bool {
        self.requested_slots == 0
            && self.loaded_slots == 0
            && self.failed_slots.is_empty()
            && self.discovery_calls == 0
            && self.cold_start.is_none()
    }
}

/// Storage warmup implementation to use for oracle direct-write slots.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OracleStorageWarmupMode {
    /// Bulk-fetch declared hot slots through `EvmCache::prewarm_slots`.
    #[default]
    PrewarmSlots,
    /// Verify declared hot slots through `EvmCache::run_cold_start`.
    ColdStart,
}

/// Summary of an oracle cold-start storage warmup run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OracleColdStartWarmupReport {
    /// Cold-start rounds executed.
    pub rounds: usize,
    /// Total slots requested in verify phases.
    pub verified_slots: usize,
    /// Slots whose fetched value changed and was injected.
    pub changed_slots: usize,
    /// Total fetch failures across verify/probe phases.
    pub failed_slots: usize,
    /// Total storage slots discovered by optional discovery calls.
    pub discovered_slots: usize,
    /// Total accounts discovered by optional discovery calls.
    pub discovered_accounts: usize,
    /// Total discovery calls executed.
    pub discover_calls: usize,
}

/// One failed oracle storage warmup slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleStorageWarmupFailure {
    /// Contract address.
    pub address: Address,
    /// Storage slot.
    pub slot: U256,
    /// Fetch error message.
    pub reason: String,
}

/// Result of registering oracle handlers with a reactive engine.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OracleReactiveInstallReport {
    /// Handler ids submitted to the engine.
    pub handler_ids: Vec<HandlerId>,
}

impl OracleReactiveInstallReport {
    /// Number of handlers submitted.
    pub fn len(&self) -> usize {
        self.handler_ids.len()
    }

    /// Return true when no handlers were submitted.
    pub fn is_empty(&self) -> bool {
        self.handler_ids.is_empty()
    }
}

/// Result of unregistering oracle handlers from a reactive engine.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OracleReactiveUninstallReport {
    /// Handler ids requested for removal.
    pub handler_ids: Vec<HandlerId>,
    /// Handler ids that were present and removed.
    pub removed_handler_ids: Vec<HandlerId>,
}

/// Result of refreshing oracle handlers after runtime mutation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OracleReactiveRefreshReport {
    /// Handler ids requested for removal from the previous install handle.
    pub previous_handler_ids: Vec<HandlerId>,
    /// Previous handler ids that were present and removed.
    pub removed_handler_ids: Vec<HandlerId>,
    /// Current handler ids installed after the refresh.
    pub installed_handler_ids: Vec<HandlerId>,
}

/// Result of mutating a runtime's oracle registration set.
#[derive(Clone, Debug, Default)]
pub struct OracleRuntimeMutationReport {
    /// Feed ids registered by this mutation.
    pub registered_feed_ids: Vec<FeedId>,
    /// Feed ids removed by this mutation.
    pub removed_feed_ids: Vec<FeedId>,
    /// Feeds skipped by adapter discovery, when applicable.
    pub skipped: Vec<OracleAdapterFeedSkip>,
    /// Feed/runtime readiness after the mutation.
    pub feed_statuses: Vec<OracleFeedReadinessReport>,
    /// Current handler ids after this mutation.
    pub current_handler_ids: Vec<HandlerId>,
}

#[derive(Clone)]
struct OracleAdapterRuntimeState {
    adapter: Arc<dyn OracleAdapterPlugin>,
    registrations: Vec<FeedRegistration>,
}

impl fmt::Debug for OracleAdapterRuntimeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OracleAdapterRuntimeState")
            .field("adapter_id", &self.adapter.adapter_id())
            .field("registrations_len", &self.registrations.len())
            .finish()
    }
}

impl<P> OracleRuntimeBuilder<P> {
    /// Create a runtime builder around a provider.
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            feeds: Vec::new(),
            now_timestamp: None,
            storage_sync: OracleStorageSync::chainlink_defaults(),
            callbacks: Vec::new(),
            #[cfg(feature = "pending-oracle-updates")]
            pending_config: None,
            #[cfg(feature = "pending-oracle-updates")]
            pending_adapters: Vec::new(),
        }
    }

    /// Add one feed registration.
    pub fn feed(mut self, feed: ChainlinkFeed) -> Self {
        self.feeds.push(feed);
        self
    }

    /// Set a fixed timestamp for deterministic registration status classification.
    pub fn now_timestamp(mut self, now_timestamp: u64) -> Self {
        self.now_timestamp = Some(now_timestamp);
        self
    }

    /// Add one storage adapter.
    pub fn storage_adapter<A>(mut self, adapter: A) -> Self
    where
        A: OracleStorageAdapter + 'static,
    {
        self.storage_sync.push_adapter(Arc::new(adapter));
        self
    }

    /// Replace the storage-sync registry.
    pub fn storage_sync(mut self, storage_sync: OracleStorageSync) -> Self {
        self.storage_sync = storage_sync;
        self
    }

    /// Subscribe to every typed oracle event emitted by this runtime.
    pub fn on_event<F>(mut self, callback: F) -> Self
    where
        F: Fn(&OracleHookEvent) + Send + Sync + 'static,
    {
        self.callbacks.push(Arc::new(callback));
        self
    }

    /// Subscribe to immediate event-derived price updates.
    pub fn on_price_update<F>(self, callback: F) -> Self
    where
        F: Fn(&OraclePriceUpdate) + Send + Sync + 'static,
    {
        self.on_event(move |event| {
            if let OracleHookEvent::PriceUpdate(update) = event {
                callback(update);
            }
        })
    }

    /// Subscribe to proxy corrections.
    pub fn on_price_corrected<F>(self, callback: F) -> Self
    where
        F: Fn(&OraclePriceCorrected) + Send + Sync + 'static,
    {
        self.on_event(move |event| {
            if let OracleHookEvent::PriceCorrected(corrected) = event {
                callback(corrected);
            }
        })
    }

    /// Enable speculative pending updates for the feeds registered by this builder.
    #[cfg(feature = "pending-oracle-updates")]
    pub fn pending_updates(mut self, config: PendingOracleConfig) -> Self {
        self.pending_config = Some(config);
        self
    }

    /// Install an additional oracle-family decoder for the pending channel.
    #[cfg(feature = "pending-oracle-updates")]
    pub fn pending_adapter<A>(mut self, adapter: A) -> Self
    where
        A: PendingOracleAdapter,
    {
        self.pending_adapters.push(Arc::new(adapter));
        self
    }
}

impl<P: ChainlinkFeedProvider> OracleRuntimeBuilder<P> {
    /// Register feeds and construct the runtime.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::Provider`] when a proxy read fails,
    /// [`OracleError::DuplicateFeedId`]/[`OracleError::DuplicateProxy`] when
    /// two feeds collide, and [`OracleError::Config`] when the system clock is
    /// before the UNIX epoch and no `now_timestamp` was set.
    pub async fn build(self) -> Result<OracleRuntime<P>, OracleError> {
        let now_timestamp = match self.now_timestamp {
            Some(now_timestamp) => now_timestamp,
            None => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(crate::error::clock_error)?
                .as_secs(),
        };
        let mut registry = OracleRegistry::new_at_timestamp(now_timestamp);
        for feed in self.feeds {
            registry
                .register_chainlink_feed(&self.provider, feed.into_config())
                .await?;
        }

        let tracker = OracleTracker::new(registry);
        #[cfg(feature = "pending-oracle-updates")]
        let pending = self.pending_config.map(|config| {
            PendingOracleRuntime::from_registrations(
                config,
                tracker.registrations_iter(),
                self.pending_adapters,
            )
        });

        Ok(OracleRuntime {
            provider: self.provider,
            tracker,
            storage_sync: self.storage_sync,
            callbacks: self.callbacks,
            adapter_states: Vec::new(),
            #[cfg(feature = "pending-oracle-updates")]
            pending,
            #[cfg(feature = "pending-oracle-updates")]
            pending_source_sessions: Vec::new(),
        })
    }
}

impl Default for OracleCacheRuntimeBuilder {
    fn default() -> Self {
        Self {
            adapters: Vec::new(),
            now_timestamp: None,
            storage_sync: OracleStorageSync::chainlink_defaults(),
            storage_warmup_enabled: true,
            storage_warmup_mode: OracleStorageWarmupMode::default(),
            code_registry: OracleCodeRegistry::default(),
            code_warmup_policy: OracleCodeWarmupPolicy::default(),
            callbacks: Vec::new(),
            #[cfg(feature = "pending-oracle-updates")]
            pending_config: None,
            #[cfg(feature = "pending-oracle-updates")]
            pending_adapters: Vec::new(),
        }
    }
}

impl OracleCacheRuntimeBuilder {
    /// Install one oracle adapter plugin.
    pub fn install_adapter<A>(mut self, adapter: A) -> Self
    where
        A: OracleAdapterPlugin + 'static,
    {
        self.adapters.push(Arc::new(adapter));
        self
    }

    /// Set a fixed timestamp for deterministic registration status classification.
    pub fn now_timestamp(mut self, now_timestamp: u64) -> Self {
        self.now_timestamp = Some(now_timestamp);
        self
    }

    /// Add one storage adapter.
    pub fn storage_adapter<A>(mut self, adapter: A) -> Self
    where
        A: OracleStorageAdapter + 'static,
    {
        self.storage_sync.push_adapter(Arc::new(adapter));
        self
    }

    /// Replace the storage-sync registry.
    pub fn storage_sync(mut self, storage_sync: OracleStorageSync) -> Self {
        self.storage_sync = storage_sync;
        self
    }

    /// Enable or disable oracle storage warmup during cache-native builds.
    ///
    /// Enabled by default. When enabled, the builder asks installed storage
    /// adapters which slots must be hot for direct event writes and bulk-loads
    /// them through the cache before returning the runtime.
    pub fn storage_warmup(mut self, enabled: bool) -> Self {
        self.storage_warmup_enabled = enabled;
        self
    }

    /// Set the oracle storage warmup mode.
    pub fn storage_warmup_mode(mut self, mode: OracleStorageWarmupMode) -> Self {
        self.storage_warmup_mode = mode;
        self
    }

    /// Use `EvmCache::run_cold_start` for declared oracle storage slots.
    pub fn storage_cold_start(self) -> Self {
        self.storage_warmup_mode(OracleStorageWarmupMode::ColdStart)
    }

    /// Disable oracle storage warmup.
    pub fn disable_storage_warmup(self) -> Self {
        self.storage_warmup(false)
    }

    /// Replace the bytecode seed/etch registry.
    pub fn code_registry(mut self, registry: OracleCodeRegistry) -> Self {
        self.code_registry = registry;
        self
    }

    /// Set the bytecode warmup failure policy.
    pub fn code_warmup_policy(mut self, policy: OracleCodeWarmupPolicy) -> Self {
        self.code_warmup_policy = policy;
        self
    }

    /// Add a canonical bytecode seed that must verify against on-chain code hash.
    pub fn code_seed(mut self, address: Address, code: Bytes) -> Self {
        self.code_registry = self.code_registry.seed(address, code);
        self
    }

    /// Add multiple canonical bytecode seeds that must verify against chain code hash.
    pub fn code_seeds<I>(mut self, seeds: I) -> Self
    where
        I: IntoIterator<Item = (Address, Bytes)>,
    {
        self.code_registry = self.code_registry.seed_many(seeds);
        self
    }

    /// Add explicit simulation-only bytecode at `address`.
    pub fn code_etch(mut self, address: Address, code: Bytes) -> Self {
        self.code_registry = self.code_registry.etch(address, code);
        self
    }

    /// Add multiple explicit simulation-only bytecode etches.
    pub fn code_etches<I>(mut self, etches: I) -> Self
    where
        I: IntoIterator<Item = (Address, Bytes)>,
    {
        self.code_registry = self.code_registry.etch_many(etches);
        self
    }

    /// Subscribe to every typed oracle event emitted by this runtime.
    pub fn on_event<F>(mut self, callback: F) -> Self
    where
        F: Fn(&OracleHookEvent) + Send + Sync + 'static,
    {
        self.callbacks.push(Arc::new(callback));
        self
    }

    /// Subscribe to immediate event-derived price updates.
    pub fn on_price_update<F>(self, callback: F) -> Self
    where
        F: Fn(&OraclePriceUpdate) + Send + Sync + 'static,
    {
        self.on_event(move |event| {
            if let OracleHookEvent::PriceUpdate(update) = event {
                callback(update);
            }
        })
    }

    /// Enable speculative pending updates for adapter-discovered registrations.
    #[cfg(feature = "pending-oracle-updates")]
    pub fn pending_updates(mut self, config: PendingOracleConfig) -> Self {
        self.pending_config = Some(config);
        self
    }

    /// Install an additional oracle-family decoder for the pending channel.
    #[cfg(feature = "pending-oracle-updates")]
    pub fn pending_adapter<A>(mut self, adapter: A) -> Self
    where
        A: PendingOracleAdapter,
    {
        self.pending_adapters.push(Arc::new(adapter));
        self
    }

    /// Register adapter-discovered feeds and construct the runtime.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedSkipped`] when any adapter reported a
    /// skipped feed (use [`Self::build_report`] to receive skips as data),
    /// [`OracleError::Policy`] when code warmup fails the configured
    /// [`OracleCodeWarmupPolicy`], [`OracleError::Provider`] when adapter
    /// discovery reads fail, [`OracleError::DuplicateFeedId`]/
    /// [`OracleError::DuplicateProxy`] when discovered feeds collide, and
    /// [`OracleError::Config`] when the system clock is before the UNIX epoch
    /// and no `now_timestamp` was set.
    pub async fn build(self, cache: &mut EvmCache) -> Result<OracleRuntime<()>, OracleError> {
        let report = self.build_report(cache).await?;
        if let Some(skipped) = report.skipped.first() {
            return Err(cache_runtime_skip_error(skipped));
        }
        Ok(report.runtime)
    }

    /// Register every compatible adapter feed and return skipped feeds instead of failing fast.
    pub async fn build_report(
        self,
        cache: &mut EvmCache,
    ) -> Result<OracleCacheRuntimeBuildReport, OracleError> {
        let now_timestamp = match self.now_timestamp {
            Some(now_timestamp) => now_timestamp,
            None => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(crate::error::clock_error)?
                .as_secs(),
        };

        let code_warmup = self
            .code_registry
            .apply_to_cache_with_policy(cache, self.code_warmup_policy)?;
        let mut discovered_by_adapter = Vec::new();
        let mut seeded = Vec::new();
        let mut warmup_registrations = Vec::new();
        let mut skipped = Vec::new();
        for adapter in &self.adapters {
            let report = adapter
                .discover(OracleDiscoveryContext {
                    cache,
                    now_timestamp,
                })
                .await?;
            skipped.extend(report.skipped);
            let registrations = report
                .feeds
                .iter()
                .map(|feed| feed.registration.clone())
                .collect::<Vec<_>>();
            seeded.extend(report.feeds.into_iter().map(|feed| {
                warmup_registrations.push(feed.registration.clone());
                (feed.registration, feed.round)
            }));
            discovered_by_adapter.push(OracleAdapterRuntimeState {
                adapter: Arc::clone(adapter),
                registrations,
            });
        }

        let storage_warmup = if self.storage_warmup_enabled {
            prewarm_oracle_storage(
                cache,
                &self.storage_sync,
                &warmup_registrations.iter().collect::<Vec<_>>(),
                self.storage_warmup_mode,
            )
        } else {
            OracleStorageWarmupReport {
                feed_statuses: feed_readiness_from_registrations(&warmup_registrations),
                ..OracleStorageWarmupReport::default()
            }
        };
        let mut feed_statuses = storage_warmup.feed_statuses.clone();
        feed_statuses.extend(skipped.iter().map(skipped_feed_readiness));
        let tracker = OracleTracker::from_registrations_at_timestamp(seeded, now_timestamp)?;
        #[cfg(feature = "pending-oracle-updates")]
        let pending = self.pending_config.map(|config| {
            PendingOracleRuntime::from_registrations(
                config,
                tracker.registrations_iter(),
                self.pending_adapters,
            )
        });
        Ok(OracleCacheRuntimeBuildReport {
            runtime: OracleRuntime {
                provider: (),
                tracker,
                storage_sync: self.storage_sync,
                callbacks: self.callbacks,
                adapter_states: discovered_by_adapter,
                #[cfg(feature = "pending-oracle-updates")]
                pending,
                #[cfg(feature = "pending-oracle-updates")]
                pending_source_sessions: Vec::new(),
            },
            skipped,
            feed_statuses,
            storage_warmup,
            code_warmup,
        })
    }
}

/// High-level facade for registration, typed events, reads, and reconciliation.
pub struct OracleRuntime<P> {
    provider: P,
    tracker: OracleTracker,
    storage_sync: OracleStorageSync,
    callbacks: Vec<OracleEventCallback>,
    adapter_states: Vec<OracleAdapterRuntimeState>,
    #[cfg(feature = "pending-oracle-updates")]
    pending: Option<PendingOracleRuntime>,
    #[cfg(feature = "pending-oracle-updates")]
    pending_source_sessions: Vec<PendingOracleSourceSession>,
}

impl<P: fmt::Debug> fmt::Debug for OracleRuntime<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("OracleRuntime");
        debug
            .field("provider", &self.provider)
            .field("tracker", &self.tracker)
            .field("storage_sync", &self.storage_sync)
            .field("callbacks_len", &self.callbacks.len())
            .field("adapter_states", &self.adapter_states);
        #[cfg(feature = "pending-oracle-updates")]
        debug.field("pending", &self.pending);
        #[cfg(feature = "pending-oracle-updates")]
        debug.field(
            "pending_source_sessions_len",
            &self.pending_source_sessions.len(),
        );
        debug.finish()
    }
}

impl<P> OracleRuntime<P> {
    /// Start building a runtime around a provider.
    pub fn builder(provider: P) -> OracleRuntimeBuilder<P> {
        OracleRuntimeBuilder::new(provider)
    }

    /// Borrow the provider used for authoritative proxy reads and reconciliation.
    pub fn provider(&self) -> &P {
        &self.provider
    }

    /// Borrow the typed state store for snapshot, price, and registration reads.
    pub fn tracker(&self) -> &OracleTracker {
        &self.tracker
    }

    /// Mutably borrow the tracker.
    ///
    /// **Warning:** mutating the tracker directly (registering or removing
    /// feeds through [`OracleTracker`] methods) desynchronizes this runtime's
    /// adapter bookkeeping: `adapter_states` still references removed feeds
    /// (or misses added ones), so [`Self::reactive_handlers`] and the
    /// `refresh_handlers*` methods rebuild handler routing from a stale feed
    /// set. Use [`Self::register_seeded_feed`], [`Self::register_adapter`],
    /// [`Self::unregister_feed_by_id`], or [`Self::unregister_feed_by_proxy`]
    /// instead — they keep typed state and handler routing in sync. Reserve
    /// this accessor for operations that do not change the registration set
    /// (for example [`OracleTracker::reconcile`] or
    /// [`OracleTracker::apply_batch_report`]).
    pub fn tracker_mut(&mut self) -> &mut OracleTracker {
        &mut self.tracker
    }

    /// Return feed/runtime readiness for all registered feeds.
    pub fn feed_readiness(&self) -> Vec<OracleFeedReadinessReport> {
        self.tracker.feed_readiness()
    }

    /// Borrow the opt-in speculative pending-update runtime, when enabled.
    #[cfg(feature = "pending-oracle-updates")]
    pub fn pending_updates(&self) -> Option<&PendingOracleRuntime> {
        self.pending.as_ref()
    }

    /// Start and retain a pending candidate source for the lifetime of this runtime.
    ///
    /// The corresponding source family must be enabled in the runtime's
    /// [`PendingOracleConfig`]. Dropping the runtime requests source shutdown.
    #[cfg(feature = "pending-oracle-updates")]
    pub fn start_pending_source<S>(&mut self, source: S) -> Result<(), PendingOracleSourceError>
    where
        S: PendingOracleCandidateSource,
    {
        let pending = self
            .pending
            .as_ref()
            .ok_or(PendingOracleSourceError::PendingUpdatesDisabled)?;
        let session = pending.start_source(source)?;
        self.pending_source_sessions.push(session);
        Ok(())
    }

    pub(crate) fn storage_sync(&self) -> &OracleStorageSync {
        &self.storage_sync
    }

    /// Replace the direct-storage registry used by subsequently constructed
    /// reactive handlers and storage warmup calls.
    ///
    /// This is primarily useful after cache-native discovery returns an
    /// [`OracleTracker`] that needs a deployment-specific storage adapter:
    /// construct the facade with [`OracleRuntime::from_tracker`], then retain
    /// the custom registry here before calling [`Self::reactive_runtime`],
    /// [`Self::reactive_handler`], or [`Self::prewarm_storage`].
    ///
    /// If this runtime's handlers are already installed in a live reactive
    /// engine, replace the registry only as part of a handler refresh so the
    /// installed handlers cannot retain the old storage policy.
    pub fn with_storage_sync(mut self, storage_sync: OracleStorageSync) -> Self {
        self.storage_sync = storage_sync;
        self
    }

    /// Build a fresh built-in Chainlink-compatible reactive handler from current registrations.
    ///
    /// Adapter plugin handlers are not included. Use [`Self::reactive_handlers`],
    /// [`Self::reactive_runtime`], or [`Self::register_subscriber`] when the runtime
    /// was built with installed adapter plugins.
    pub fn reactive_handler(&self) -> OracleReactiveHandler {
        OracleReactiveHandler::with_storage_sync(
            self.tracker.registrations().collect(),
            self.storage_sync.clone(),
        )
    }

    /// Build fresh reactive handlers for every source installed in this runtime.
    ///
    /// The first handler is the built-in Chainlink-compatible handler. Any adapter
    /// plugin handlers discovered during cache-native startup follow it.
    ///
    /// Handler ids must be unique across installed adapter plugins: when two
    /// adapters return handlers with the same [`HandlerId`], only the first
    /// handler is kept and the second adapter's event routing is silently
    /// dropped (a `tracing` warning is emitted). An adapter handler that
    /// reuses the built-in Chainlink handler id is fine — the built-in
    /// handler is rebuilt from *all* current registrations, so it already
    /// routes that adapter's feeds.
    pub fn reactive_handlers(&self) -> Vec<Arc<dyn ReactiveHandler<Ethereum>>> {
        let built_in: Arc<dyn ReactiveHandler<Ethereum>> = Arc::new(self.reactive_handler());
        let built_in_id = built_in.id();
        let mut adapter_ids = BTreeSet::new();
        let mut handlers = vec![built_in];
        for state in self
            .adapter_states
            .iter()
            .filter(|state| !state.registrations.is_empty())
        {
            let handler = state
                .adapter
                .reactive_handler(state.registrations.clone(), self.storage_sync.clone());
            let id = handler.id();
            if id == built_in_id {
                // By design: the adapter delegates to the built-in Chainlink
                // handler, which is already built from all registrations.
                continue;
            }
            if !adapter_ids.insert(id.clone()) {
                tracing::warn!(
                    handler_id = %id,
                    adapter_id = %state.adapter.adapter_id(),
                    "duplicate reactive HandlerId from adapter plugins; \
                     dropping this adapter's handler routing"
                );
                continue;
            }
            handlers.push(handler);
        }
        handlers
    }

    /// Return current reactive handler ids for this runtime.
    pub fn reactive_handler_ids(&self) -> Vec<HandlerId> {
        self.reactive_handlers()
            .into_iter()
            .map(|handler| handler.id())
            .collect()
    }

    /// Build a reactive runtime with this oracle runtime's handlers already installed.
    pub fn reactive_runtime(&self) -> Result<ReactiveRuntime<Ethereum>, OracleError> {
        let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
        for handler in self.reactive_handlers() {
            runtime.register_handler(handler).map_err(runtime_error)?;
        }
        Ok(runtime)
    }

    /// Register this runtime's handlers with an engine, using continuity-safe backfill.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::Reactive`] when the engine rejects a handler
    /// registration (for example a handler id that is already installed).
    pub async fn install_handlers<S>(
        &self,
        engine: &mut ReactiveEngine<S, Ethereum>,
    ) -> Result<OracleReactiveInstallReport, OracleError>
    where
        S: InterestOwnerSubscriber<Ethereum>,
    {
        let mut report = OracleReactiveInstallReport::default();
        for handler in self.reactive_handlers() {
            let id = handler.id();
            engine
                .register_handler(handler)
                .await
                .map_err(runtime_error)?;
            report.handler_ids.push(id);
        }
        Ok(report)
    }

    /// Refresh an existing engine install after registering or unregistering feeds.
    ///
    /// `installed` must be the report returned by a previous install/refresh.
    /// The old handler ids in that report are removed first, then the runtime's
    /// current handlers are installed with continuity-safe backfill.
    pub async fn refresh_handlers<S>(
        &self,
        engine: &mut ReactiveEngine<S, Ethereum>,
        installed: &mut OracleReactiveInstallReport,
    ) -> Result<OracleReactiveRefreshReport, OracleError>
    where
        S: InterestOwnerSubscriber<Ethereum>,
    {
        self.refresh_handlers_inner(engine, installed, OracleHandlerRefreshBackfill::Continuity)
            .await
    }

    /// Refresh an existing engine install after mutation using explicit backfill.
    pub async fn refresh_handlers_with_backfill<S>(
        &self,
        engine: &mut ReactiveEngine<S, Ethereum>,
        installed: &mut OracleReactiveInstallReport,
        backfill: SubscriberBackfill,
    ) -> Result<OracleReactiveRefreshReport, OracleError>
    where
        S: InterestOwnerSubscriber<Ethereum>,
    {
        self.refresh_handlers_inner(
            engine,
            installed,
            OracleHandlerRefreshBackfill::Explicit(backfill),
        )
        .await
    }

    /// Refresh an existing engine install after mutation without backfill.
    pub async fn refresh_handlers_live_only<S>(
        &self,
        engine: &mut ReactiveEngine<S, Ethereum>,
        installed: &mut OracleReactiveInstallReport,
    ) -> Result<OracleReactiveRefreshReport, OracleError>
    where
        S: InterestOwnerSubscriber<Ethereum>,
    {
        self.refresh_handlers_inner(engine, installed, OracleHandlerRefreshBackfill::LiveOnly)
            .await
    }

    /// Unregister exactly the handlers recorded by a previous install/refresh report.
    ///
    /// Await each subscriber removal before changing the report. On failure or
    /// cancellation, `installed` retains the outstanding ids for retry.
    pub async fn uninstall_installed_handlers<S>(
        &self,
        engine: &mut ReactiveEngine<S, Ethereum>,
        installed: &mut OracleReactiveInstallReport,
    ) -> Result<OracleReactiveUninstallReport, OracleError>
    where
        S: InterestOwnerSubscriber<Ethereum>,
    {
        let handler_ids = installed.handler_ids.clone();
        let removed_handler_ids =
            unregister_handler_ids(engine, &mut installed.handler_ids).await?;
        Ok(OracleReactiveUninstallReport {
            handler_ids,
            removed_handler_ids,
        })
    }

    /// Register this runtime's handlers with explicit owner-scoped backfill.
    ///
    /// The backfill must identify exactly one hash-certified block retained in
    /// the engine's rollback journal. Use global canonical ingestion for deeper
    /// history, or [`Self::install_handlers`] for coordinated catch-up.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::Reactive`] when the engine rejects a handler
    /// registration (for example a handler id that is already installed).
    pub async fn install_handlers_with_backfill<S>(
        &self,
        engine: &mut ReactiveEngine<S, Ethereum>,
        backfill: SubscriberBackfill,
    ) -> Result<OracleReactiveInstallReport, OracleError>
    where
        S: InterestOwnerSubscriber<Ethereum>,
    {
        let mut report = OracleReactiveInstallReport::default();
        for handler in self.reactive_handlers() {
            let id = handler.id();
            engine
                .register_handler_with_backfill(handler, backfill)
                .await
                .map_err(runtime_error)?;
            report.handler_ids.push(id);
        }
        Ok(report)
    }

    /// Register this runtime's handlers without any backfill.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::Reactive`] when the engine rejects a handler
    /// registration (for example a handler id that is already installed).
    pub async fn install_handlers_live_only<S>(
        &self,
        engine: &mut ReactiveEngine<S, Ethereum>,
    ) -> Result<OracleReactiveInstallReport, OracleError>
    where
        S: InterestOwnerSubscriber<Ethereum>,
    {
        let mut report = OracleReactiveInstallReport::default();
        for handler in self.reactive_handlers() {
            let id = handler.id();
            engine
                .register_handler_live_only(handler)
                .await
                .map_err(runtime_error)?;
            report.handler_ids.push(id);
        }
        Ok(report)
    }

    /// Unregister this runtime's handlers from an engine.
    ///
    /// **Warning:** this recomputes handler ids from the *current*
    /// registration set. If the runtime's registrations changed since the
    /// handlers were installed (feeds registered or unregistered, adapters
    /// added), the recomputed set can miss handlers that are still installed
    /// in the engine, leaving them registered. Prefer
    /// [`Self::uninstall_installed_handlers`] with the report returned by the
    /// original install/refresh call — it removes exactly the handler ids
    /// that were installed.
    pub async fn uninstall_handlers<S>(
        &self,
        engine: &mut ReactiveEngine<S, Ethereum>,
    ) -> Result<OracleReactiveUninstallReport, OracleError>
    where
        S: InterestOwnerSubscriber<Ethereum>,
    {
        let mut report = OracleReactiveUninstallReport::default();
        for handler in self.reactive_handlers() {
            let id = handler.id();
            report.handler_ids.push(id.clone());
            if engine
                .unregister_handler(&id)
                .await
                .map_err(runtime_error)?
                .is_some()
            {
                report.removed_handler_ids.push(id);
            }
        }
        Ok(report)
    }

    /// Register this runtime's oracle interests with an event subscriber.
    pub async fn register_subscriber<S>(&self, subscriber: &mut S) -> Result<(), OracleError>
    where
        S: EventSubscriber<Ethereum>,
    {
        subscriber
            .register_interests(&self.reactive_interests())
            .await
            .map_err(runtime_error)
    }

    /// Read one subscriber batch, apply it to the cache, and return the typed
    /// batch digest, or `None` when the subscriber has no batch ready.
    pub async fn next_subscriber_events<S>(
        &mut self,
        cache: &mut EvmCache,
        runtime: &mut ReactiveRuntime<Ethereum>,
        subscriber: &mut S,
    ) -> Result<Option<crate::OracleBatchReport>, OracleError>
    where
        S: EventSubscriber<Ethereum>,
    {
        let Some(batch) = subscriber.next_batch().await.map_err(runtime_error)? else {
            return Ok(None);
        };
        #[cfg(feature = "pending-oracle-updates")]
        let committed_logs = batch
            .records()
            .iter()
            .filter_map(|record| match &record.input {
                ReactiveInput::Log(log) => Some(log.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let report = runtime.ingest_batch(cache, batch).map_err(runtime_error)?;
        let typed = self.apply_batch_report(&report)?;
        #[cfg(feature = "pending-oracle-updates")]
        if let Some(pending) = &self.pending {
            for log in &committed_logs {
                pending.observe_confirmed_log(log);
            }
        }
        Ok(Some(typed))
    }

    /// Build a read overlay over the current tracker state.
    pub fn read_overlay(&self) -> OracleReadOverlay<'_> {
        OracleReadOverlay::new(&self.tracker)
    }

    /// Register an already-discovered feed and seed its latest round.
    ///
    /// This updates typed state immediately. If a reactive engine has already
    /// installed this runtime's handlers, call one of the `refresh_handlers*`
    /// methods with the previous install report so the subscriber receives the
    /// new event interests.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::DuplicateFeedId`] or
    /// [`OracleError::DuplicateProxy`] when the registration collides with an
    /// existing feed; typed state is unchanged in that case.
    pub fn register_seeded_feed(
        &mut self,
        registration: FeedRegistration,
        round: RoundData,
    ) -> Result<OracleRuntimeMutationReport, OracleError> {
        let id = registration.id.clone();
        self.tracker
            .insert_seeded_registration(registration, round)?;
        #[cfg(feature = "pending-oracle-updates")]
        self.refresh_pending_scope();
        Ok(self.mutation_report([id], []))
    }

    /// Register an already-discovered adapter-owned feed and seed its latest round.
    ///
    /// The adapter must already be installed in this runtime. This is the
    /// low-level path for adapter code that discovers one additional feed and
    /// wants that feed included in the adapter handler rebuilt on refresh.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::Config`] when `adapter_id` is not installed in
    /// this runtime, and [`OracleError::DuplicateFeedId`]/
    /// [`OracleError::DuplicateProxy`] when the registration collides with an
    /// existing feed.
    pub fn register_adapter_seeded_feed(
        &mut self,
        adapter_id: impl AsRef<str>,
        registration: FeedRegistration,
        round: RoundData,
    ) -> Result<OracleRuntimeMutationReport, OracleError> {
        let id = registration.id.clone();
        let state_index = self
            .adapter_state_index(adapter_id.as_ref())
            .ok_or_else(|| adapter_not_installed(adapter_id.as_ref()))?;
        self.tracker
            .insert_seeded_registration(registration.clone(), round)?;
        self.adapter_states[state_index]
            .registrations
            .push(registration);
        #[cfg(feature = "pending-oracle-updates")]
        self.refresh_pending_scope();
        Ok(self.mutation_report([id], []))
    }

    /// Unregister a feed by id.
    ///
    /// This removes typed state, pending reconciliation, and any adapter routing
    /// references for the feed. Refresh installed handlers afterward to apply
    /// the routing change to a live engine.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedNotFound`] when no feed is registered under
    /// `id`.
    pub fn unregister_feed_by_id(
        &mut self,
        id: FeedId,
    ) -> Result<OracleRuntimeMutationReport, OracleError> {
        let removed = self
            .tracker
            .remove_by_id(id)
            .ok_or(OracleError::FeedNotFound)?;
        let id = removed.id.clone();
        self.remove_adapter_registration(id.clone());
        #[cfg(feature = "pending-oracle-updates")]
        self.refresh_pending_scope();
        Ok(self.mutation_report([], [id]))
    }

    /// Unregister a feed by proxy.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedNotFound`] when no feed is registered under
    /// `proxy`.
    pub fn unregister_feed_by_proxy(
        &mut self,
        proxy: Address,
    ) -> Result<OracleRuntimeMutationReport, OracleError> {
        let removed = self
            .tracker
            .remove_by_proxy(proxy)
            .ok_or(OracleError::FeedNotFound)?;
        let id = removed.id.clone();
        self.remove_adapter_registration(id.clone());
        #[cfg(feature = "pending-oracle-updates")]
        self.refresh_pending_scope();
        Ok(self.mutation_report([], [id]))
    }

    /// Return the latest typed price by feed id string.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedNotFound`] when no feed is registered under
    /// `id` or the feed has no current snapshot.
    pub fn price(&self, id: impl AsRef<str>) -> Result<OraclePrice, OracleError> {
        self.tracker.price(id)
    }

    /// Return the latest typed price by proxy address.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedNotFound`] when no feed is registered under
    /// `proxy` or the feed has no current snapshot.
    pub fn price_by_proxy(&self, proxy: Address) -> Result<OraclePrice, OracleError> {
        self.tracker.price_by_proxy(proxy)
    }

    /// Return the latest round by feed id string.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedNotFound`] when no feed is registered under
    /// `id` or the feed has no current snapshot.
    pub fn latest_round(&self, id: impl AsRef<str>) -> Result<RoundData, OracleError> {
        self.tracker.latest_round(id)
    }

    /// Apply a committed reactive batch and return the typed batch digest.
    ///
    /// The returned [`crate::OracleBatchReport`] carries the raw per-event
    /// hooks (`events`, in emission order) plus the digested per-feed changes,
    /// continuity incidents, and the `requires_full_refresh` routing flag —
    /// the same consumer shape as `evm-amm-state`'s `AmmSyncBatchReport`.
    /// Registered callbacks fire for every event before this returns.
    ///
    /// Legacy `oracle.answer_updated` signals (emitted by custom handlers that
    /// predate the rich [`OraclePriceUpdate`] payload) update typed tracker
    /// state but are **not** decoded into [`OracleHookEvent`]s, so `on_event`/
    /// `on_price_update` callbacks do not fire for them. Emit the rich
    /// `oracle.price_update` signal from custom handlers to get typed
    /// callbacks.
    pub fn apply_batch_report<N: Network>(
        &mut self,
        report: &ReactiveBatchReport<N>,
    ) -> Result<crate::OracleBatchReport, OracleError> {
        let mut events = Vec::new();
        for signal in report
            .applied
            .iter()
            .flat_map(|applied| applied.hook_signals.iter())
        {
            if let Some(signal) = OracleSignal::from_hook(signal)? {
                events.push(signal.to_event());
            }
        }

        self.tracker.apply_batch_report(report)?;
        self.emit(&events);
        Ok(crate::OracleBatchReport::from_events(events))
    }

    /// Reconcile all currently pending proxy-kind event updates.
    ///
    /// Derived-source requests
    /// ([`crate::OracleReconciliationKind::DerivedProtocolRead`]) are left
    /// queued; satisfy those with [`OracleRuntime::reconcile_derived`].
    pub async fn reconcile_pending(&mut self) -> Result<Vec<OracleHookEvent>, OracleError>
    where
        P: ChainlinkFeedProvider,
    {
        let mut reconciler = OracleReconciler::default();
        for request in self
            .tracker
            .pending_reconciliations()
            .iter()
            .filter(|request| request.kind == crate::OracleReconciliationKind::Proxy)
            .cloned()
        {
            reconciler.enqueue(request);
        }

        let mut events = Vec::new();
        while let Some(result) = reconciler
            .reconcile_next(&mut self.tracker, &self.provider)
            .await?
        {
            events.extend(result.hooks);
        }
        #[cfg(feature = "pending-oracle-updates")]
        self.refresh_pending_scope();
        self.emit(&events);
        Ok(events)
    }

    /// Reconcile all currently pending derived-source updates through their
    /// protocols' own view calls (Morpho `price()`, Euler `getQuote`), read
    /// through `cache`.
    ///
    /// Successful reads promote `EventPending` snapshots to `Confirmed` or
    /// `Corrected` and fire the registered hook callbacks; failed reads leave
    /// their requests queued and are surfaced in
    /// [`crate::DerivedReconcileReport::failed`].
    pub fn reconcile_derived(
        &mut self,
        cache: &mut evm_fork_cache::cache::EvmCache,
    ) -> crate::DerivedReconcileReport {
        let report = self.tracker.reconcile_derived_pending_with(cache);
        let events: Vec<OracleHookEvent> = report
            .reconciled
            .iter()
            .flat_map(|result| result.hooks.iter().cloned())
            .collect();
        self.emit(&events);
        report
    }

    fn emit(&self, events: &[OracleHookEvent]) {
        for event in events {
            for callback in &self.callbacks {
                callback(event);
            }
        }
    }

    /// Return the complete log/event interest set for built-in and adapter handlers.
    pub fn reactive_interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        let mut interests = Vec::new();
        for handler in self.reactive_handlers() {
            interests.extend(handler.interests());
        }
        interests
    }

    /// Return the oracle storage slots this runtime can prewarm for direct event writes.
    pub fn storage_warmup_slots(&self) -> Vec<(Address, U256)> {
        self.storage_sync
            .warm_slots_for_registrations(self.tracker.registrations_iter())
    }

    /// Bulk-load direct-write oracle storage slots into the supplied cache.
    pub fn prewarm_storage(&self, cache: &mut EvmCache) -> OracleStorageWarmupReport {
        let registrations = self.tracker.registrations_iter().collect::<Vec<_>>();
        prewarm_oracle_storage(
            cache,
            &self.storage_sync,
            &registrations,
            OracleStorageWarmupMode::default(),
        )
    }

    /// Verify direct-write oracle storage slots through `EvmCache::run_cold_start`.
    pub fn cold_start_storage(&self, cache: &mut EvmCache) -> OracleStorageWarmupReport {
        let registrations = self.tracker.registrations_iter().collect::<Vec<_>>();
        prewarm_oracle_storage(
            cache,
            &self.storage_sync,
            &registrations,
            OracleStorageWarmupMode::ColdStart,
        )
    }

    async fn refresh_handlers_inner<S>(
        &self,
        engine: &mut ReactiveEngine<S, Ethereum>,
        installed: &mut OracleReactiveInstallReport,
        backfill: OracleHandlerRefreshBackfill,
    ) -> Result<OracleReactiveRefreshReport, OracleError>
    where
        S: InterestOwnerSubscriber<Ethereum>,
    {
        let previous_handler_ids = installed.handler_ids.clone();
        let removed_handler_ids =
            unregister_handler_ids(engine, &mut installed.handler_ids).await?;
        for handler in self.reactive_handlers() {
            let id = handler.id();
            match backfill {
                OracleHandlerRefreshBackfill::Continuity => engine.register_handler(handler).await,
                OracleHandlerRefreshBackfill::Explicit(backfill) => {
                    engine
                        .register_handler_with_backfill(handler, backfill)
                        .await
                }
                OracleHandlerRefreshBackfill::LiveOnly => {
                    engine.register_handler_live_only(handler).await
                }
            }
            .map_err(runtime_error)?;
            installed.handler_ids.push(id);
        }
        Ok(OracleReactiveRefreshReport {
            previous_handler_ids,
            removed_handler_ids,
            installed_handler_ids: installed.handler_ids.clone(),
        })
    }

    fn mutation_report(
        &self,
        registered_feed_ids: impl IntoIterator<Item = FeedId>,
        removed_feed_ids: impl IntoIterator<Item = FeedId>,
    ) -> OracleRuntimeMutationReport {
        OracleRuntimeMutationReport {
            registered_feed_ids: registered_feed_ids.into_iter().collect(),
            removed_feed_ids: removed_feed_ids.into_iter().collect(),
            skipped: Vec::new(),
            feed_statuses: feed_readiness_from_registrations(self.tracker.registrations_iter()),
            current_handler_ids: self.reactive_handler_ids(),
        }
    }

    fn adapter_state_index(&self, adapter_id: &str) -> Option<usize> {
        self.adapter_states
            .iter()
            .position(|state| state.adapter.adapter_id().as_str() == adapter_id)
    }

    fn push_adapter_state(
        &mut self,
        adapter: Arc<dyn OracleAdapterPlugin>,
        registrations: Vec<FeedRegistration>,
    ) {
        if registrations.is_empty() {
            return;
        }

        let adapter_id = adapter.adapter_id();
        if let Some(existing) = self
            .adapter_states
            .iter_mut()
            .find(|state| state.adapter.adapter_id().as_str() == adapter_id.as_str())
        {
            existing.registrations.extend(registrations);
            return;
        }

        self.adapter_states.push(OracleAdapterRuntimeState {
            adapter,
            registrations,
        });
    }

    fn remove_adapter_registration(&mut self, id: FeedId) {
        for state in &mut self.adapter_states {
            state
                .registrations
                .retain(|registration| registration.id != id);
        }
    }

    #[cfg(feature = "pending-oracle-updates")]
    fn refresh_pending_scope(&mut self) {
        if let Some(pending) = &mut self.pending {
            pending.refresh(self.tracker.registrations_iter());
        }
    }
}

impl OracleRuntime<()> {
    /// Start building a cache-native runtime from oracle adapter plugins.
    pub fn cache_builder() -> OracleCacheRuntimeBuilder {
        OracleCacheRuntimeBuilder::default()
    }

    /// Build a runtime facade from a cache-native tracker.
    ///
    /// The returned runtime starts with **Chainlink-default storage sync and
    /// no adapter-plugin routing**, regardless of how the tracker's feeds
    /// were discovered: any custom [`OracleStorageSync`] configuration is
    /// reset to [`OracleStorageSync::chainlink_defaults`] unless replaced with
    /// [`OracleRuntime::with_storage_sync`], and non-Chainlink
    /// feeds already in the tracker (Pyth, RedStone, custom adapter families)
    /// remain visible in typed state but receive **no reactive events** —
    /// their adapter handlers are gone until the adapters are re-installed
    /// via [`Self::register_adapter`]. Prefer keeping the runtime returned by
    /// [`OracleCacheRuntimeBuilder::build`] when adapter plugins are in play.
    pub fn from_tracker(tracker: OracleTracker) -> Self {
        Self {
            provider: (),
            tracker,
            storage_sync: OracleStorageSync::chainlink_defaults(),
            callbacks: Vec::new(),
            adapter_states: Vec::new(),
            #[cfg(feature = "pending-oracle-updates")]
            pending: None,
            #[cfg(feature = "pending-oracle-updates")]
            pending_source_sessions: Vec::new(),
        }
    }

    /// Discover and register feeds from an adapter on an existing cache-native runtime.
    ///
    /// This mutates typed state and retains the adapter for future handler
    /// rebuilds. If handlers are already installed in an engine, call one of the
    /// `refresh_handlers*` methods with the previous install report afterward.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::Provider`] when adapter discovery reads fail,
    /// and [`OracleError::DuplicateFeedId`]/[`OracleError::DuplicateProxy`]
    /// when a discovered feed collides with an existing registration. On
    /// error the runtime is unchanged: feeds inserted earlier in the same
    /// call are rolled back and the adapter is not retained.
    pub async fn register_adapter<A>(
        &mut self,
        adapter: A,
        cache: &mut EvmCache,
    ) -> Result<OracleRuntimeMutationReport, OracleError>
    where
        A: OracleAdapterPlugin + 'static,
    {
        let adapter = Arc::new(adapter);
        let discovery = adapter
            .discover(OracleDiscoveryContext {
                cache,
                now_timestamp: self.tracker.now_timestamp(),
            })
            .await?;

        // Insert discovered feeds live and roll back on failure instead of
        // cloning the whole tracker for every call. This is an exact inverse:
        // discovery ran before any mutation, `insert_seeded_registration`
        // touches only the registration map, the proxy-id index, and the
        // snapshot map, and `remove_by_id` removes exactly those entries plus
        // pending reconciliations — which a just-inserted proxy cannot have,
        // because nothing runs between the inserts and the rollback.
        let mut registered_feed_ids: Vec<FeedId> = Vec::new();
        let mut registrations = Vec::new();
        for feed in &discovery.feeds {
            if let Err(error) = self
                .tracker
                .insert_seeded_registration(feed.registration.clone(), feed.round.clone())
            {
                for id in registered_feed_ids {
                    self.tracker.remove_by_id(id);
                }
                return Err(error);
            }
            registered_feed_ids.push(feed.registration.id.clone());
            registrations.push(feed.registration.clone());
        }

        self.push_adapter_state(adapter, registrations);
        #[cfg(feature = "pending-oracle-updates")]
        self.refresh_pending_scope();
        let mut feed_statuses =
            feed_readiness_from_registrations(self.tracker.registrations_iter());
        feed_statuses.extend(discovery.skipped.iter().map(skipped_feed_readiness));
        Ok(OracleRuntimeMutationReport {
            registered_feed_ids,
            removed_feed_ids: Vec::new(),
            skipped: discovery.skipped,
            feed_statuses,
            current_handler_ids: self.reactive_handler_ids(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum OracleHandlerRefreshBackfill {
    Continuity,
    Explicit(SubscriberBackfill),
    LiveOnly,
}

fn runtime_error(error: impl ToString) -> OracleError {
    OracleError::Reactive(error.to_string())
}

fn adapter_not_installed(adapter_id: &str) -> OracleError {
    OracleError::Config(crate::error::OracleConfigError::AdapterNotInstalled {
        adapter: crate::OracleAdapterId::new(adapter_id.to_string()),
    })
}

fn feed_readiness_from_registrations<'a>(
    registrations: impl IntoIterator<Item = &'a FeedRegistration>,
) -> Vec<OracleFeedReadinessReport> {
    registrations
        .into_iter()
        .map(|registration| OracleFeedReadinessReport {
            id: Some(registration.id.clone()),
            proxy: registration.proxy,
            status: registration.status,
            reason: None,
        })
        .collect()
}

fn skipped_feed_readiness(skipped: &OracleAdapterFeedSkip) -> OracleFeedReadinessReport {
    OracleFeedReadinessReport {
        id: skipped.feed.id(),
        proxy: skipped.proxy,
        status: OracleFeedStatus::Unsupported,
        reason: Some(skipped.reason.to_string()),
    }
}

fn storage_warmup_feed_readiness(
    registrations: &[&FeedRegistration],
    failed_slots: &[OracleStorageWarmupFailure],
) -> Vec<OracleFeedReadinessReport> {
    if failed_slots.is_empty() {
        return feed_readiness_from_registrations(registrations.iter().copied());
    }

    let failed_addresses = failed_slots
        .iter()
        .map(|failure| failure.address)
        .collect::<BTreeSet<_>>();
    registrations
        .iter()
        .map(|registration| {
            let failed = feed_storage_addresses(registration)
                .into_iter()
                .any(|address| failed_addresses.contains(&address));
            OracleFeedReadinessReport {
                id: Some(registration.id.clone()),
                proxy: registration.proxy,
                status: if failed {
                    OracleFeedStatus::Degraded
                } else {
                    registration.status
                },
                reason: failed
                    .then(|| "oracle storage warmup failed for one or more feed slots".to_string()),
            }
        })
        .collect()
}

fn feed_storage_addresses(registration: &FeedRegistration) -> Vec<Address> {
    let mut addresses = vec![registration.proxy];
    addresses.extend(registration.current_aggregator);
    addresses.extend(
        registration
            .source
            .event_aggregators(registration.current_aggregator),
    );
    addresses.sort_unstable();
    addresses.dedup();
    addresses
}

async fn unregister_handler_ids<S>(
    engine: &mut ReactiveEngine<S, Ethereum>,
    handler_ids: &mut Vec<HandlerId>,
) -> Result<Vec<HandlerId>, OracleError>
where
    S: InterestOwnerSubscriber<Ethereum>,
{
    let mut removed = Vec::new();
    while let Some(id) = handler_ids.first().cloned() {
        if engine
            .unregister_handler(&id)
            .await
            .map_err(runtime_error)?
            .is_some()
        {
            removed.push(id);
        }
        // Retain failed and not-yet-attempted owners for an explicit retry.
        handler_ids.remove(0);
    }
    Ok(removed)
}

fn cache_runtime_skip_error(skipped: &OracleAdapterFeedSkip) -> OracleError {
    OracleError::FeedSkipped(Box::new(crate::OracleFeedSkip::from_adapter_skip(skipped)))
}

fn prewarm_oracle_storage(
    cache: &mut EvmCache,
    storage_sync: &OracleStorageSync,
    registrations: &[&crate::FeedRegistration],
    mode: OracleStorageWarmupMode,
) -> OracleStorageWarmupReport {
    let slots = storage_sync.warm_slots_for_registrations(registrations.iter().copied());
    let discovery_calls = oracle_cold_start_discovery_calls(registrations);
    let requested_slots = slots.len();
    if slots.is_empty() && discovery_calls.is_empty() {
        return OracleStorageWarmupReport {
            mode,
            feed_statuses: feed_readiness_from_registrations(registrations.iter().copied()),
            ..OracleStorageWarmupReport::default()
        };
    }

    match mode {
        OracleStorageWarmupMode::PrewarmSlots => {
            let report = cache.prewarm_slots(&slots);
            let failed_slots = report
                .failed
                .into_iter()
                .map(|(address, slot, error)| OracleStorageWarmupFailure {
                    address,
                    slot,
                    reason: error.to_string(),
                })
                .collect::<Vec<_>>();
            let feed_statuses = storage_warmup_feed_readiness(registrations, &failed_slots);
            OracleStorageWarmupReport {
                mode,
                feed_statuses,
                requested_slots,
                loaded_slots: report.loaded,
                failed_slots,
                discovery_calls: 0,
                cold_start: None,
            }
        }
        OracleStorageWarmupMode::ColdStart => {
            let mut planner =
                OracleSlotColdStartPlanner::new(slots.clone(), discovery_calls.clone());
            match cache.run_cold_start(&mut planner, ColdStartConfig::default()) {
                Ok(report) => {
                    let failed = report.failed_slots;
                    let cold_start = OracleColdStartWarmupReport::from(report);
                    OracleStorageWarmupReport {
                        mode,
                        feed_statuses: feed_readiness_from_registrations(
                            registrations.iter().copied(),
                        ),
                        requested_slots,
                        loaded_slots: requested_slots.saturating_sub(failed),
                        failed_slots: Vec::new(),
                        discovery_calls: cold_start.discover_calls,
                        cold_start: Some(cold_start),
                    }
                }
                Err(error) => {
                    let failed_slots = slots
                        .into_iter()
                        .map(|(address, slot)| OracleStorageWarmupFailure {
                            address,
                            slot,
                            reason: error.to_string(),
                        })
                        .collect::<Vec<_>>();
                    let feed_statuses = storage_warmup_feed_readiness(registrations, &failed_slots);
                    OracleStorageWarmupReport {
                        mode,
                        feed_statuses,
                        requested_slots,
                        loaded_slots: 0,
                        failed_slots,
                        discovery_calls: discovery_calls.len(),
                        cold_start: None,
                    }
                }
            }
        }
    }
}

struct OracleSlotColdStartPlanner {
    slots: Vec<(Address, U256)>,
    slot_set: BTreeSet<(Address, U256)>,
    discover: Vec<ColdStartCall>,
    verified_discovered_slots: bool,
}

impl OracleSlotColdStartPlanner {
    fn new(slots: Vec<(Address, U256)>, discover: Vec<ColdStartCall>) -> Self {
        let slot_set = slots.iter().copied().collect();
        Self {
            slots,
            slot_set,
            discover,
            verified_discovered_slots: false,
        }
    }
}

impl ColdStartPlanner for OracleSlotColdStartPlanner {
    fn initial_plan(&mut self, _state: &dyn StateView) -> ColdStartPlan {
        ColdStartPlan {
            verify: self.slots.clone(),
            discover: self.discover.clone(),
            ..Default::default()
        }
    }

    fn on_results(&mut self, results: &ColdStartResults, _state: &dyn StateView) -> ColdStartStep {
        if self.verified_discovered_slots {
            return ColdStartStep::Done;
        }
        self.verified_discovered_slots = true;

        let mut slots = results
            .discovered
            .iter()
            .flat_map(|discovered| discovered.access.slots.iter().copied())
            .filter(|slot| !self.slot_set.contains(slot))
            .collect::<Vec<_>>();
        slots.sort_unstable();
        slots.dedup();
        if slots.is_empty() {
            return ColdStartStep::Done;
        }

        ColdStartStep::Continue(ColdStartPlan {
            verify: slots,
            ..Default::default()
        })
    }
}

impl From<evm_fork_cache::ColdStartRunReport> for OracleColdStartWarmupReport {
    fn from(report: evm_fork_cache::ColdStartRunReport) -> Self {
        Self {
            rounds: report.rounds,
            verified_slots: report.verified_slots,
            changed_slots: report.changed_slots,
            failed_slots: report.failed_slots,
            discovered_slots: report.discovered_slots,
            discovered_accounts: report.discovered_accounts,
            discover_calls: report
                .per_round
                .iter()
                .map(|round| round.discover_calls)
                .sum(),
        }
    }
}

fn oracle_cold_start_discovery_calls(
    registrations: &[&crate::FeedRegistration],
) -> Vec<ColdStartCall> {
    registrations
        .iter()
        .map(|registration| {
            let read_proxy = registration.source.read_proxy(registration.proxy);
            ColdStartCall {
                from: Address::ZERO,
                to: read_proxy,
                calldata: Bytes::from(latestRoundDataCall {}.abi_encode()),
                restrict_to: Some(vec![read_proxy]),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use evm_fork_cache::{ColdStartCallResult, StorageAccessList};
    use crate::revm::context::result::{ExecutionResult, Output, SuccessReason};

    #[derive(Default)]
    struct EmptyStateView;

    impl StateView for EmptyStateView {
        fn storage(&self, _address: Address, _slot: U256) -> Option<U256> {
            None
        }
    }

    #[test]
    fn cold_start_planner_verifies_discovered_slots_in_second_round() {
        let declared = (Address::repeat_byte(0x11), U256::from(1_u64));
        let discovered = (Address::repeat_byte(0x22), U256::from(2_u64));
        let mut planner = OracleSlotColdStartPlanner::new(vec![declared], Vec::new());
        let initial = planner.initial_plan(&EmptyStateView);
        assert_eq!(initial.verify, vec![declared]);

        let mut access = StorageAccessList::default();
        access.slots.insert(discovered);
        access.slots.insert(declared);
        let results = ColdStartResults {
            discovered: vec![ColdStartCallResult {
                result: ExecutionResult::Success {
                    reason: SuccessReason::Return,
                    #[cfg(feature = "alloy1")]
                    gas_used: 0,
                    #[cfg(feature = "alloy1")]
                    gas_refunded: 0,
                    #[cfg(feature = "alloy2")]
                    gas: Default::default(),
                    logs: Vec::new(),
                    output: Output::Call(Bytes::new()),
                },
                access,
            }],
            ..Default::default()
        };

        let ColdStartStep::Continue(next) = planner.on_results(&results, &EmptyStateView) else {
            panic!("discovered slots should schedule a follow-up verify round");
        };
        assert_eq!(next.verify, vec![discovered]);
        assert!(next.discover.is_empty());

        assert!(matches!(
            planner.on_results(&ColdStartResults::default(), &EmptyStateView),
            ColdStartStep::Done
        ));
    }
}
