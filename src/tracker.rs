use crate::alloy_network::{Ethereum, Network};
use alloy_primitives::{Address, B256, I256, U256};
use evm_fork_cache::reactive::{ReactiveBatchReport, ReactiveReport, ReportTag};

use crate::{
    AggregatorChange, AggregatorLayoutEvidence, ChainlinkFeedProvider, FeedId, FeedRegistration,
    FeedSource, ORACLE_LEGACY_ANSWER_UPDATED_KIND, ORACLE_SIGNAL_NAMESPACE, OracleBlockRef,
    OracleError, OracleFeedReadinessReport, OraclePrice, OracleRegistry, OracleRoundStatus,
    OracleSignalKind, OracleSnapshot, OracleValueSource, OracleValueStatus, RoundData,
    registry::snapshot_from_proxy_read, state::EventSnapshotInput,
};

/// Event-derived oracle update carried in reactive hook payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleUpdate {
    /// Feed id.
    pub id: FeedId,
    /// Proxy address.
    pub proxy: Address,
    /// Emitting aggregator.
    pub aggregator: Address,
    /// Event-derived round.
    pub round: RoundData,
    /// Event block number, when known.
    pub block_number: Option<u64>,
    /// Event log index, when known.
    pub log_index: Option<u64>,
    /// Event-derived value lifecycle.
    pub value_status: OracleValueStatus,
}

/// Rich event-derived oracle price update emitted by the reactive handler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OraclePriceUpdate {
    /// Feed id.
    pub id: FeedId,
    /// Proxy address.
    pub proxy: Address,
    /// Emitting aggregator.
    pub aggregator: Address,
    /// Optional human-readable feed label.
    pub label: Option<String>,
    /// Optional base symbol.
    pub base: Option<String>,
    /// Optional quote symbol.
    pub quote: Option<String>,
    /// Raw signed event answer.
    pub raw_answer: I256,
    /// Feed decimals.
    pub decimals: u8,
    /// Event round id.
    pub event_round_id: U256,
    /// Event-derived `startedAt` timestamp.
    pub started_at: u64,
    /// Event `updatedAt` timestamp.
    pub updated_at: u64,
    /// Event block number, when known.
    pub block_number: Option<u64>,
    /// Event block hash, when known.
    pub block_hash: Option<B256>,
    /// Event log index, when known.
    pub log_index: Option<u64>,
    /// Round validity classification.
    pub round_status: OracleRoundStatus,
    /// Value reconciliation lifecycle.
    pub value_status: OracleValueStatus,
    /// Source that produced this value.
    pub source: OracleValueSource,
}

impl OraclePriceUpdate {
    fn round(&self) -> RoundData {
        RoundData {
            round_id: self.event_round_id,
            answer: self.raw_answer,
            started_at: self.started_at,
            updated_at: self.updated_at,
            answered_in_round: self.event_round_id,
        }
    }
}

/// How a queued [`OracleReconciliationRequest`] is satisfied.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum OracleReconciliationKind {
    /// Authoritative Chainlink-shaped proxy read (`latestRoundData()`),
    /// served by the provider-backed [`OracleTracker::reconcile`] /
    /// [`OracleReconciler`] paths.
    #[default]
    Proxy,
    /// Authoritative derived-source protocol read (Morpho `price()`, Euler
    /// `getQuote`), served by
    /// [`OracleTracker::reconcile_derived_pending_with`] through a
    /// [`crate::OracleDerivedReader`].
    DerivedProtocolRead,
}

/// Event-specific reconciliation request queued after an event update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleReconciliationRequest {
    /// Feed id.
    pub id: FeedId,
    /// Proxy address to read authoritatively.
    pub proxy: Address,
    /// Aggregator that emitted the event.
    pub aggregator: Option<Address>,
    /// Event-derived round that needs confirmation.
    pub event_round: RoundData,
    /// Event block number, when known.
    pub block_number: Option<u64>,
    /// Event block hash, when known.
    pub block_hash: Option<B256>,
    /// Event log index, when known.
    pub log_index: Option<u64>,
    /// Read path that satisfies this request.
    pub kind: OracleReconciliationKind,
}

impl OracleReconciliationRequest {
    /// Block identity for providers that support hash-pinned reads.
    pub fn block_ref(&self) -> Option<OracleBlockRef> {
        Some(OracleBlockRef {
            number: self.block_number?,
            hash: self.block_hash?,
        })
    }
}

/// Hook emitted when an event price is confirmed by the proxy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OraclePriceConfirmed {
    /// Feed id.
    pub id: FeedId,
    /// Proxy address.
    pub proxy: Address,
    /// Best-known current aggregator.
    pub aggregator: Option<Address>,
    /// Optional human-readable feed label.
    pub label: Option<String>,
    /// Optional base symbol.
    pub base: Option<String>,
    /// Optional quote symbol.
    pub quote: Option<String>,
    /// Confirmed raw answer.
    pub raw_answer: I256,
    /// Feed decimals.
    pub decimals: u8,
    /// Event round id.
    pub event_round_id: U256,
    /// Event `updatedAt` timestamp.
    pub updated_at: u64,
    /// Event block number, when known.
    pub block_number: Option<u64>,
    /// Event block hash, when known.
    pub block_hash: Option<B256>,
    /// Event log index, when known.
    pub log_index: Option<u64>,
    /// Round validity classification.
    pub round_status: OracleRoundStatus,
    /// Resulting value lifecycle.
    pub value_status: OracleValueStatus,
    /// Source that produced this value.
    pub source: OracleValueSource,
    /// Authoritative proxy round.
    pub proxy_round: RoundData,
}

/// Hook emitted when a proxy read corrects an event price.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OraclePriceCorrected {
    /// Feed id.
    pub id: FeedId,
    /// Proxy address.
    pub proxy: Address,
    /// Best-known current aggregator.
    pub aggregator: Option<Address>,
    /// Optional human-readable feed label.
    pub label: Option<String>,
    /// Optional base symbol.
    pub base: Option<String>,
    /// Optional quote symbol.
    pub quote: Option<String>,
    /// Raw event answer that was corrected.
    pub event_answer: I256,
    /// Corrected raw answer from the proxy.
    pub raw_answer: I256,
    /// Feed decimals.
    pub decimals: u8,
    /// Event round id.
    pub event_round_id: U256,
    /// Event `updatedAt` timestamp.
    pub updated_at: u64,
    /// Event block number, when known.
    pub block_number: Option<u64>,
    /// Event block hash, when known.
    pub block_hash: Option<B256>,
    /// Event log index, when known.
    pub log_index: Option<u64>,
    /// Round validity classification.
    pub round_status: OracleRoundStatus,
    /// Resulting value lifecycle.
    pub value_status: OracleValueStatus,
    /// Source that produced this value.
    pub source: OracleValueSource,
    /// Corrected authoritative proxy round.
    pub corrected_round: RoundData,
}

/// Hook emitted when the proxy read is stale under policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OraclePriceStale {
    /// Feed id.
    pub id: FeedId,
    /// Proxy address.
    pub proxy: Address,
    /// Best-known current aggregator.
    pub aggregator: Option<Address>,
    /// Optional human-readable feed label.
    pub label: Option<String>,
    /// Optional base symbol.
    pub base: Option<String>,
    /// Optional quote symbol.
    pub quote: Option<String>,
    /// Raw stale answer.
    pub raw_answer: I256,
    /// Feed decimals.
    pub decimals: u8,
    /// Event round id.
    pub event_round_id: U256,
    /// Event `updatedAt` timestamp.
    pub updated_at: u64,
    /// Event block number, when known.
    pub block_number: Option<u64>,
    /// Event block hash, when known.
    pub block_hash: Option<B256>,
    /// Event log index, when known.
    pub log_index: Option<u64>,
    /// Round validity classification.
    pub round_status: OracleRoundStatus,
    /// Resulting value lifecycle.
    pub value_status: OracleValueStatus,
    /// Source that produced this value.
    pub source: OracleValueSource,
    /// Authoritative proxy round.
    pub proxy_round: RoundData,
}

/// Rich oracle hook events emitted by reconciliation.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OracleHookEvent {
    /// Immediate event-derived price update.
    PriceUpdate(OraclePriceUpdate),
    /// Event price matched the authoritative proxy read.
    PriceConfirmed(OraclePriceConfirmed),
    /// Event price was corrected by the authoritative proxy read.
    PriceCorrected(OraclePriceCorrected),
    /// Authoritative proxy read was stale under policy.
    PriceStale(OraclePriceStale),
    /// Proxy now points at a different aggregator.
    AggregatorChanged(AggregatorChange),
}

/// Hook emitted when proxy reconciliation detects an aggregator change.
pub type OracleAggregatorChanged = AggregatorChange;

/// One completed event reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleReconciliationResult {
    /// Request that was reconciled.
    pub request: OracleReconciliationRequest,
    /// Lifecycle hooks emitted by this reconciliation.
    pub hooks: Vec<OracleHookEvent>,
}

/// Reconciliation summary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Number of feeds checked.
    pub checked_feeds: usize,
    /// Feed/runtime readiness after reconciliation.
    pub feed_statuses: Vec<OracleFeedReadinessReport>,
    /// Feeds whose snapshot changed.
    pub changed_feeds: Vec<FeedId>,
    /// Aggregator changes detected while reading proxies.
    pub aggregator_changes: Vec<AggregatorChange>,
}

/// One derived-source protocol read that failed during a
/// [`OracleTracker::reconcile_derived_pending_with`] pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedReconcileFailure {
    /// Request whose protocol read failed; it remains queued.
    pub request: OracleReconciliationRequest,
    /// Read error.
    pub error: OracleError,
}

/// Summary of one [`OracleTracker::reconcile_derived_pending_with`] pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DerivedReconcileReport {
    /// Requests whose protocol read completed, with the hooks each produced.
    pub reconciled: Vec<OracleReconciliationResult>,
    /// Requests whose protocol read failed; each remains queued.
    pub failed: Vec<DerivedReconcileFailure>,
}

/// Mutable typed oracle state.
#[derive(Clone, Debug)]
pub struct OracleTracker {
    registry: OracleRegistry,
    pending_reconciliations: Vec<OracleReconciliationRequest>,
}

impl OracleTracker {
    /// Create a tracker from an existing registry.
    pub fn new(registry: OracleRegistry) -> Self {
        Self {
            registry,
            pending_reconciliations: Vec::new(),
        }
    }

    /// Seed a tracker from registrations and round data.
    pub fn from_registrations_at_timestamp(
        feeds: Vec<(FeedRegistration, RoundData)>,
        now_timestamp: u64,
    ) -> Result<Self, OracleError> {
        let mut registry = OracleRegistry::new_at_timestamp(now_timestamp);
        for (registration, round) in feeds {
            let id = registration.id.clone();
            if registry.registrations_map().contains_key(&id) {
                return Err(OracleError::DuplicateFeedId(id.to_string()));
            }
            if registry.id_by_proxy(registration.proxy).is_some() {
                return Err(OracleError::DuplicateProxy(registration.proxy));
            }

            let snapshot = OracleSnapshot::proxy_read(
                id.clone(),
                registration.proxy,
                registration.current_aggregator,
                registration.metadata.clone(),
                round,
                now_timestamp,
                &registration.staleness,
            );
            registry
                .registrations_map_mut()
                .insert(id.clone(), registration.clone());
            registry
                .snapshots_by_proxy_mut()
                .insert(registration.proxy, snapshot);
            registry.record_proxy_id(registration.proxy, id);
        }
        Ok(Self {
            registry,
            pending_reconciliations: Vec::new(),
        })
    }

    /// Return cloned registrations for rebuilding reactive routing.
    ///
    /// Prefer [`Self::registrations_iter`] when read-only access is enough —
    /// this iterator clones every registration it yields.
    pub fn registrations(&self) -> impl Iterator<Item = FeedRegistration> + '_ {
        self.registry.registrations()
    }

    /// Iterate over registrations without cloning, in feed-id order.
    pub fn registrations_iter(&self) -> impl Iterator<Item = &FeedRegistration> {
        self.registry.registrations_iter()
    }

    /// Return feed/runtime readiness for all registered feeds.
    pub fn feed_readiness(&self) -> Vec<OracleFeedReadinessReport> {
        self.registry.feed_readiness()
    }

    /// Insert a pre-discovered feed and seed its latest proxy-read snapshot.
    pub fn insert_seeded_registration(
        &mut self,
        registration: FeedRegistration,
        round: RoundData,
    ) -> Result<(), OracleError> {
        self.registry
            .insert_seeded_registration(registration, round)
    }

    /// Remove a feed by id, including its latest snapshot and pending reconciliation.
    pub fn remove_by_id(&mut self, id: FeedId) -> Option<FeedRegistration> {
        let registration = self.registry.remove_registration_by_id(id)?;
        self.clear_pending_for_proxy(registration.proxy);
        Some(registration)
    }

    /// Remove a feed by proxy, including its latest snapshot and pending reconciliation.
    pub fn remove_by_proxy(&mut self, proxy: Address) -> Option<FeedRegistration> {
        let registration = self.registry.remove_registration_by_proxy(proxy)?;
        self.clear_pending_for_proxy(registration.proxy);
        Some(registration)
    }

    /// Return the timestamp used for snapshot freshness classification.
    pub fn now_timestamp(&self) -> u64 {
        self.registry.now_timestamp()
    }

    /// Return the latest snapshot by proxy.
    pub fn latest(&self, proxy: Address) -> Option<&OracleSnapshot> {
        self.registry.latest(proxy)
    }

    /// Return the feed registration for a proxy.
    pub fn registration_for_proxy(&self, proxy: Address) -> Option<&FeedRegistration> {
        let id = self.registry.id_by_proxy(proxy)?;
        self.registry.registrations_map().get(id)
    }

    /// Return the latest actionable-facing price by feed id string.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedNotFound`] when no feed is registered under
    /// `id` or the feed has no current snapshot.
    pub fn price(&self, id: impl AsRef<str>) -> Result<OraclePrice, OracleError> {
        let registration = self
            .registration_by_id_str(id.as_ref())
            .ok_or(OracleError::FeedNotFound)?;
        self.price_for_registration(registration)
    }

    /// Return the latest actionable-facing price by feed id.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedNotFound`] when no feed is registered under
    /// `id` or the feed has no current snapshot.
    pub fn price_by_id(&self, id: FeedId) -> Result<OraclePrice, OracleError> {
        let registration = self
            .registry
            .registrations_map()
            .get(&id)
            .ok_or(OracleError::FeedNotFound)?;
        self.price_for_registration(registration)
    }

    /// Return the latest actionable-facing price by proxy address.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedNotFound`] when no feed is registered under
    /// `proxy` or the feed has no current snapshot.
    pub fn price_by_proxy(&self, proxy: Address) -> Result<OraclePrice, OracleError> {
        let id = self
            .registry
            .id_by_proxy(proxy)
            .cloned()
            .ok_or(OracleError::FeedNotFound)?;
        self.price_by_id(id)
    }

    /// Return the latest round by feed id string.
    ///
    /// # Errors
    ///
    /// Returns [`OracleError::FeedNotFound`] when no feed is registered under
    /// `id` or the feed has no current snapshot.
    pub fn latest_round(&self, id: impl AsRef<str>) -> Result<RoundData, OracleError> {
        Ok(self.price(id)?.round_data())
    }

    /// Return the latest round by proxy address.
    pub fn latest_round_by_proxy(&self, proxy: Address) -> Result<RoundData, OracleError> {
        Ok(self.price_by_proxy(proxy)?.round_data())
    }

    /// Return event-specific proxy reconciliations waiting to be processed.
    pub fn pending_reconciliations(&self) -> &[OracleReconciliationRequest] {
        &self.pending_reconciliations
    }

    /// Apply committed reactive reports to typed oracle state.
    ///
    /// Built-in handlers emit each update twice for compatibility: as a rich
    /// `oracle.price_update` signal and as a legacy `oracle.answer_updated`
    /// signal. A legacy signal is skipped only when the same batch carries a
    /// rich update for the same proxy and event round — legacy-only signals
    /// from custom handlers still apply even when unrelated rich updates
    /// share the batch. Legacy signals update typed state but are not decoded
    /// into [`OracleHookEvent`]s, so runtime `on_event`/`on_price_update`
    /// callbacks do not fire for them.
    pub fn apply_batch_report<N: Network>(
        &mut self,
        report: &ReactiveBatchReport<N>,
    ) -> Result<(), OracleError> {
        // Keys (proxy, event round id) of rich updates in this batch. Legacy
        // `oracle.answer_updated` duplicates of these are skipped so one event
        // is not applied twice; legacy signals for other state keys or rounds
        // are still applied.
        let rich_price_update_keys = report
            .applied
            .iter()
            .flat_map(|applied| applied.hook_signals.iter())
            .filter(|signal| {
                signal.namespace.as_ref() == ORACLE_SIGNAL_NAMESPACE
                    && signal.kind.as_ref() == OracleSignalKind::PriceUpdate.as_str()
            })
            .filter_map(|signal| signal.payload.as_ref()?.downcast_ref::<OraclePriceUpdate>())
            .map(|update| (update.proxy, update.event_round_id))
            .collect::<std::collections::BTreeSet<_>>();

        for applied in &report.applied {
            for signal in &applied.hook_signals {
                if signal.namespace.as_ref() != ORACLE_SIGNAL_NAMESPACE {
                    continue;
                }

                let Some(payload) = signal.payload.as_ref() else {
                    continue;
                };

                match signal.kind.as_ref() {
                    kind if kind == OracleSignalKind::PriceUpdate.as_str() => {
                        if let Some(update) = payload.downcast_ref::<OraclePriceUpdate>() {
                            self.apply_price_update_with_tags(update.clone(), &signal.labels)?;
                        }
                    }
                    kind if kind == ORACLE_LEGACY_ANSWER_UPDATED_KIND => {
                        if let Some(update) = payload.downcast_ref::<OracleUpdate>()
                            && !rich_price_update_keys
                                .contains(&(update.proxy, update.round.round_id))
                        {
                            self.apply_update(update.clone())?;
                        }
                    }
                    _ => {}
                }
            }
        }

        if report.reports.iter().any(|report| {
            matches!(
                report.as_ref(),
                // A removed block can be outside the cache's retained journal.
                // The typed tracker may still hold its event-derived value, so
                // the reported dropped anchor is enough to revoke that value.
                ReactiveReport::Reorg(reorg)
                    if reorg.dropped.is_some() || !reorg.dropped_blocks.is_empty()
            )
        }) {
            self.mark_event_snapshots_unknown();
        }

        Ok(())
    }

    /// Reconcile all registered feeds with authoritative proxy reads.
    pub async fn reconcile<P: ChainlinkFeedProvider>(
        &mut self,
        provider: &P,
    ) -> Result<ReconcileReport, OracleError> {
        let registrations: Vec<_> = self.registry.registrations().collect();
        let mut report = ReconcileReport::default();

        for mut registration in registrations {
            report.checked_feeds += 1;
            if !registration.source.supports_proxy_reconciliation() {
                continue;
            }
            let mut new_source = None;
            let (round, new_aggregator, new_layout) = if matches!(
                registration.source,
                FeedSource::AaveSynchronicityPegToBase { .. }
            ) {
                let reconciled =
                    reconcile_current_aave_synchronicity_peg_to_base(self, provider, &registration)
                        .await?;
                new_source = reconciled.new_source;
                (
                    reconciled.proxy_round,
                    reconciled.new_aggregator,
                    reconciled.new_layout,
                )
            } else {
                let read_proxy = registration.source.read_proxy(registration.proxy);
                let round = registration
                    .source
                    .normalize_round(provider.latest_round_data(read_proxy).await?);
                let new_aggregator = provider
                    .aggregator(read_proxy)
                    .await
                    .unwrap_or(registration.current_aggregator);
                let new_layout = self
                    .registry
                    .detect_aggregator_layout(provider, new_aggregator, None)
                    .await;
                (round, new_aggregator, new_layout)
            };
            let old_aggregator = registration.current_aggregator;
            let snapshot = snapshot_from_proxy_read(
                &registration,
                round,
                new_aggregator,
                self.registry.now_timestamp(),
            );

            let changed = self
                .registry
                .latest(registration.proxy)
                .is_none_or(|old| old != &snapshot);
            if changed {
                report.changed_feeds.push(registration.id.clone());
            }

            if old_aggregator != new_aggregator {
                report.aggregator_changes.push(AggregatorChange {
                    id: registration.id.clone(),
                    proxy: registration.proxy,
                    old: old_aggregator,
                    new: new_aggregator,
                });
            }

            registration.current_aggregator = new_aggregator;
            registration.aggregator_layout = new_layout.clone();
            if let Some(stored) = self
                .registry
                .registrations_map_mut()
                .get_mut(&registration.id)
            {
                stored.current_aggregator = new_aggregator;
                stored.aggregator_layout = new_layout;
                if let Some(new_source) = new_source {
                    stored.source = new_source;
                }
            }
            self.registry.replace_snapshot(snapshot);
            self.clear_pending_for_proxy(registration.proxy);
        }

        report.feed_statuses = self
            .registry
            .registrations_iter()
            .map(|registration| OracleFeedReadinessReport {
                id: Some(registration.id.clone()),
                proxy: registration.proxy,
                status: registration.status,
                reason: None,
            })
            .collect();

        Ok(report)
    }

    // `FeedId: Borrow<str>` makes this an O(log N) map lookup by string id.
    pub(crate) fn registration_by_id_str(&self, id: &str) -> Option<&FeedRegistration> {
        self.registry.registrations_map().get(id)
    }

    fn price_for_registration(
        &self,
        registration: &FeedRegistration,
    ) -> Result<OraclePrice, OracleError> {
        let snapshot = self
            .registry
            .latest(registration.proxy)
            .ok_or(OracleError::FeedNotFound)?;
        Ok(OraclePrice::from_snapshot(snapshot, registration))
    }

    fn apply_update(&mut self, update: OracleUpdate) -> Result<(), OracleError> {
        let Some(registration) = self.registry.registrations_map().get(&update.id).cloned() else {
            return Ok(());
        };

        if !registration
            .source
            .accepts_event_from(registration.current_aggregator, update.aggregator)
        {
            return Ok(());
        }

        let snapshot = OracleSnapshot::event(EventSnapshotInput {
            id: update.id.clone(),
            proxy: update.proxy,
            aggregator: Some(update.aggregator),
            metadata: registration.metadata,
            round: update.round,
            now_timestamp: self.registry.now_timestamp(),
            staleness: &registration.staleness,
            block_number: update.block_number,
            block_hash: None,
            value_status: update.value_status,
            source: registration.source.event_value_source(),
        });
        if let Some(kind) = Self::reconciliation_kind(&registration.source) {
            self.queue_reconciliation(OracleReconciliationRequest {
                id: update.id.clone(),
                proxy: update.proxy,
                aggregator: Some(update.aggregator),
                event_round: snapshot.round.clone(),
                block_number: update.block_number,
                block_hash: None,
                log_index: update.log_index,
                kind,
            });
        }
        self.registry.replace_snapshot(snapshot);
        Ok(())
    }

    fn apply_price_update_with_tags(
        &mut self,
        update: OraclePriceUpdate,
        labels: &[ReportTag],
    ) -> Result<(), OracleError> {
        let Some(mut registration) = self.registry.registrations_map().get(&update.id).cloned()
        else {
            return Ok(());
        };

        if let Some(new_source) = pyth_source_from_event_tags(&registration.source, labels) {
            if let Some(stored) = self.registry.registrations_map_mut().get_mut(&update.id) {
                stored.source = new_source.clone();
            }
            registration.source = new_source;
        }

        if !registration
            .source
            .accepts_event_from(registration.current_aggregator, update.aggregator)
        {
            return Ok(());
        }

        let event_round = update.round();
        if update.value_status == OracleValueStatus::Unknown {
            self.mark_matching_event_snapshot_unknown(
                update.proxy,
                &event_round,
                update.block_number,
                update.block_hash,
            );
            return Ok(());
        }

        let snapshot = OracleSnapshot::event(EventSnapshotInput {
            id: update.id.clone(),
            proxy: update.proxy,
            aggregator: Some(update.aggregator),
            metadata: registration.metadata,
            round: event_round.clone(),
            now_timestamp: self.registry.now_timestamp(),
            staleness: &registration.staleness,
            block_number: update.block_number,
            block_hash: update.block_hash,
            value_status: update.value_status,
            source: update.source,
        });
        if let Some(kind) = Self::reconciliation_kind(&registration.source) {
            self.queue_reconciliation(OracleReconciliationRequest {
                id: update.id.clone(),
                proxy: update.proxy,
                aggregator: Some(update.aggregator),
                event_round,
                block_number: update.block_number,
                block_hash: update.block_hash,
                log_index: update.log_index,
                kind,
            });
        }
        self.registry.replace_snapshot(snapshot);
        Ok(())
    }

    fn mark_matching_event_snapshot_unknown(
        &mut self,
        proxy: Address,
        event_round: &RoundData,
        block_number: Option<u64>,
        block_hash: Option<B256>,
    ) {
        let Some(snapshot) = self.registry.snapshots_by_proxy_mut().get_mut(&proxy) else {
            return;
        };
        if !is_event_originated_source(snapshot.source) {
            return;
        }
        if !proxy_round_matches_event(&snapshot.round, event_round) {
            return;
        }
        if block_number.is_some() && snapshot.block_number != block_number {
            return;
        }
        if block_hash.is_some() && snapshot.block_hash != block_hash {
            return;
        }

        snapshot.mark_unknown();
        self.clear_pending_for_proxy(proxy);
    }

    fn mark_event_snapshots_unknown(&mut self) {
        let mut unknown_proxies = Vec::new();
        for snapshot in self.registry.snapshots_by_proxy_mut().values_mut() {
            if is_event_originated_source(snapshot.source) {
                unknown_proxies.push(snapshot.proxy);
                snapshot.mark_unknown();
            }
        }
        for proxy in unknown_proxies {
            self.clear_pending_for_proxy(proxy);
        }
    }

    /// Reconcile every pending derived-source request through `reader`.
    ///
    /// Derived sources (Morpho, Euler) are confirmed by their protocol's own
    /// view call rather than a Chainlink proxy read: a read equal to the
    /// event-recomputed value promotes the snapshot to
    /// [`OracleValueStatus::Confirmed`]; a differing read installs the
    /// authoritative value as [`OracleValueStatus::Corrected`] with
    /// read-time timestamps. Either way the resulting snapshot carries
    /// [`OracleValueSource::Proxy`], because the value now comes from the
    /// feed's authoritative source path. Failed reads leave their request
    /// queued and are reported in
    /// [`DerivedReconcileReport::failed`] instead of failing the pass.
    ///
    /// Proxy-kind requests are untouched; drive those through the
    /// provider-backed [`OracleTracker::reconcile`] / [`OracleReconciler`]
    /// paths.
    ///
    /// Note: correction does not refresh the dependency baselines stored in
    /// the feed's [`FeedSource`] legs; subsequent dependency events recompute
    /// from the original discovery baselines until the feed is re-discovered.
    pub fn reconcile_derived_pending_with<R: crate::OracleDerivedReader>(
        &mut self,
        reader: &mut R,
    ) -> DerivedReconcileReport {
        let requests: Vec<OracleReconciliationRequest> = self
            .pending_reconciliations
            .iter()
            .filter(|request| request.kind == OracleReconciliationKind::DerivedProtocolRead)
            .cloned()
            .collect();
        let mut report = DerivedReconcileReport::default();
        for request in requests {
            let Some(registration) = self.registry.registrations_map().get(&request.id).cloned()
            else {
                report.failed.push(DerivedReconcileFailure {
                    request,
                    error: OracleError::FeedNotFound,
                });
                continue;
            };
            let answer = match reader.read_derived_value(&registration) {
                Ok(answer) => answer,
                Err(error) => {
                    report
                        .failed
                        .push(DerivedReconcileFailure { request, error });
                    continue;
                }
            };
            // A matching read confirms the event round verbatim; a differing
            // read installs the authoritative answer with read-time
            // timestamps (the classification helper treats any timestamp
            // difference as a correction, which is exactly right here).
            let proxy_round = if answer == request.event_round.answer {
                request.event_round.clone()
            } else {
                let now_timestamp = self.registry.now_timestamp();
                RoundData {
                    round_id: request.event_round.round_id,
                    answer,
                    started_at: now_timestamp,
                    updated_at: now_timestamp,
                    answered_in_round: request.event_round.answered_in_round,
                }
            };
            // Preserve dependency routing: derived feeds keep their
            // dependency aggregator and (absent) layout across protocol-read
            // reconciliation.
            let apply = self.apply_reconciled_request(
                &request,
                proxy_round,
                registration.current_aggregator,
                registration.aggregator_layout.clone(),
                None,
                true,
            );
            match apply {
                Ok(hooks) => report
                    .reconciled
                    .push(OracleReconciliationResult { request, hooks }),
                Err(error) => report
                    .failed
                    .push(DerivedReconcileFailure { request, error }),
            }
        }
        report
    }

    fn queue_reconciliation(&mut self, request: OracleReconciliationRequest) {
        self.clear_pending_for_proxy(request.proxy);
        self.pending_reconciliations.push(request);
    }

    /// Read path that can authoritatively confirm an event value from this
    /// source, or `None` when no reconciliation path exists (for example
    /// fixed-price sources).
    fn reconciliation_kind(source: &FeedSource) -> Option<OracleReconciliationKind> {
        if source.supports_proxy_reconciliation() {
            Some(OracleReconciliationKind::Proxy)
        } else if source.supports_derived_reconciliation() {
            Some(OracleReconciliationKind::DerivedProtocolRead)
        } else {
            None
        }
    }

    fn clear_pending_for_proxy(&mut self, proxy: Address) {
        self.pending_reconciliations
            .retain(|request| request.proxy != proxy);
    }

    fn complete_pending_reconciliation(&mut self, request: &OracleReconciliationRequest) {
        self.pending_reconciliations
            .retain(|pending| pending != request);
    }

    fn apply_reconciled_request(
        &mut self,
        request: &OracleReconciliationRequest,
        proxy_round: RoundData,
        new_aggregator: Option<Address>,
        new_layout: Option<AggregatorLayoutEvidence>,
        new_source: Option<FeedSource>,
        round_is_normalized: bool,
    ) -> Result<Vec<OracleHookEvent>, OracleError> {
        let Some(registration) = self.registry.registrations_map().get(&request.id).cloned() else {
            return Err(OracleError::FeedNotFound);
        };

        let proxy_round = if round_is_normalized {
            proxy_round
        } else {
            registration.source.normalize_round(proxy_round)
        };
        let old_aggregator = registration.current_aggregator;
        let mut snapshot = snapshot_from_proxy_read(
            &registration,
            proxy_round.clone(),
            new_aggregator,
            self.registry.now_timestamp(),
        );
        let value_status =
            reconciliation_value_status(&snapshot.round_status, request, &proxy_round);
        snapshot.set_value_status(value_status);

        let mut hooks = Vec::new();
        if old_aggregator != new_aggregator {
            hooks.push(OracleHookEvent::AggregatorChanged(AggregatorChange {
                id: registration.id.clone(),
                proxy: registration.proxy,
                old: old_aggregator,
                new: new_aggregator,
            }));
        }

        match value_status {
            OracleValueStatus::Confirmed => {
                hooks.push(OracleHookEvent::PriceConfirmed(OraclePriceConfirmed {
                    id: registration.id.clone(),
                    proxy: registration.proxy,
                    aggregator: new_aggregator,
                    label: registration.label.clone(),
                    base: registration.base.clone(),
                    quote: registration.quote.clone(),
                    raw_answer: proxy_round.answer,
                    decimals: registration.metadata.decimals,
                    event_round_id: request.event_round.round_id,
                    updated_at: request.event_round.updated_at,
                    block_number: request.block_number,
                    block_hash: request.block_hash,
                    log_index: request.log_index,
                    round_status: snapshot.round_status.clone(),
                    value_status,
                    source: OracleValueSource::Proxy,
                    proxy_round: proxy_round.clone(),
                }));
            }
            OracleValueStatus::Corrected => {
                hooks.push(OracleHookEvent::PriceCorrected(OraclePriceCorrected {
                    id: registration.id.clone(),
                    proxy: registration.proxy,
                    aggregator: new_aggregator,
                    label: registration.label.clone(),
                    base: registration.base.clone(),
                    quote: registration.quote.clone(),
                    event_answer: request.event_round.answer,
                    raw_answer: proxy_round.answer,
                    decimals: registration.metadata.decimals,
                    event_round_id: request.event_round.round_id,
                    updated_at: request.event_round.updated_at,
                    block_number: request.block_number,
                    block_hash: request.block_hash,
                    log_index: request.log_index,
                    round_status: snapshot.round_status.clone(),
                    value_status,
                    source: OracleValueSource::Proxy,
                    corrected_round: proxy_round.clone(),
                }));
            }
            OracleValueStatus::EventPending
            | OracleValueStatus::RequiresRepair
            | OracleValueStatus::Unknown => {}
        }

        if matches!(snapshot.round_status, OracleRoundStatus::Stale { .. }) {
            hooks.push(OracleHookEvent::PriceStale(OraclePriceStale {
                id: registration.id.clone(),
                proxy: registration.proxy,
                aggregator: new_aggregator,
                label: registration.label.clone(),
                base: registration.base.clone(),
                quote: registration.quote.clone(),
                raw_answer: proxy_round.answer,
                decimals: registration.metadata.decimals,
                event_round_id: request.event_round.round_id,
                updated_at: request.event_round.updated_at,
                block_number: request.block_number,
                block_hash: request.block_hash,
                log_index: request.log_index,
                round_status: snapshot.round_status.clone(),
                value_status,
                source: OracleValueSource::Proxy,
                proxy_round: proxy_round.clone(),
            }));
        }

        if let Some(stored) = self
            .registry
            .registrations_map_mut()
            .get_mut(&registration.id)
        {
            stored.current_aggregator = new_aggregator;
            stored.aggregator_layout = new_layout;
            if let Some(new_source) = new_source {
                stored.source = new_source;
            }
        }
        self.registry.replace_snapshot(snapshot);
        self.complete_pending_reconciliation(request);
        Ok(hooks)
    }
}

/// Reconciles event-derived oracle state against authoritative proxy reads.
#[derive(Clone, Debug, Default)]
pub struct OracleReconciler {
    queue: std::collections::VecDeque<OracleReconciliationRequest>,
}

impl OracleReconciler {
    /// Enqueue an event reconciliation request.
    pub fn enqueue(&mut self, request: OracleReconciliationRequest) {
        self.queue.push_back(request);
    }

    /// Reconcile the next queued event against authoritative proxy reads.
    pub async fn reconcile_next<P: ChainlinkFeedProvider>(
        &mut self,
        tracker: &mut OracleTracker,
        provider: &P,
    ) -> Result<Option<OracleReconciliationResult>, OracleError> {
        let request = loop {
            let Some(front) = self.queue.front().cloned() else {
                return Ok(None);
            };
            if front.kind == OracleReconciliationKind::DerivedProtocolRead {
                // Derived requests are satisfied by protocol reads, not proxy
                // reads: drop this reconciler's copy and leave the tracker's
                // pending entry for `reconcile_derived_pending_with`.
                self.queue.pop_front();
                continue;
            }
            break front;
        };

        let Some(registration) = tracker
            .registry
            .registrations_map()
            .get(&request.id)
            .cloned()
        else {
            return Err(OracleError::FeedNotFound);
        };
        if !registration.source.supports_proxy_reconciliation() {
            self.queue.pop_front();
            return Ok(Some(OracleReconciliationResult {
                request,
                hooks: Vec::new(),
            }));
        }
        let block_ref = request.block_ref();
        let reconciled = if matches!(
            registration.source,
            FeedSource::AaveSynchronicityPegToBase { .. }
        ) {
            reconcile_aave_synchronicity_peg_to_base(
                tracker,
                provider,
                &registration,
                &request,
                block_ref,
            )
            .await?
        } else {
            reconcile_single_proxy_source(tracker, provider, &registration, &request, block_ref)
                .await?
        };
        let hooks = tracker.apply_reconciled_request(
            &request,
            reconciled.proxy_round,
            reconciled.new_aggregator,
            reconciled.new_layout,
            reconciled.new_source,
            reconciled.round_is_normalized,
        )?;
        self.queue.pop_front();

        Ok(Some(OracleReconciliationResult { request, hooks }))
    }
}

struct ReconciledSourceRead {
    proxy_round: RoundData,
    new_aggregator: Option<Address>,
    new_layout: Option<AggregatorLayoutEvidence>,
    new_source: Option<FeedSource>,
    round_is_normalized: bool,
}

async fn reconcile_single_proxy_source<P: ChainlinkFeedProvider>(
    tracker: &mut OracleTracker,
    provider: &P,
    registration: &FeedRegistration,
    request: &OracleReconciliationRequest,
    block_ref: Option<OracleBlockRef>,
) -> Result<ReconciledSourceRead, OracleError> {
    let read_proxy = registration.source.read_proxy(registration.proxy);
    let proxy_round = provider.latest_round_data_at(read_proxy, block_ref).await?;
    let new_aggregator = provider
        .aggregator_at(read_proxy, block_ref)
        .await
        .unwrap_or_else(|_| {
            tracker
                .current_aggregator(&request.id)
                .or(request.aggregator)
        });
    let new_layout = tracker
        .detect_aggregator_layout(provider, new_aggregator, block_ref)
        .await;
    Ok(ReconciledSourceRead {
        proxy_round,
        new_aggregator,
        new_layout,
        new_source: None,
        round_is_normalized: false,
    })
}

async fn reconcile_aave_synchronicity_peg_to_base<P: ChainlinkFeedProvider>(
    tracker: &mut OracleTracker,
    provider: &P,
    registration: &FeedRegistration,
    request: &OracleReconciliationRequest,
    block_ref: Option<OracleBlockRef>,
) -> Result<ReconciledSourceRead, OracleError> {
    let FeedSource::AaveSynchronicityPegToBase {
        asset_to_peg_proxy,
        asset_to_peg_aggregator,
        peg_to_base_proxy,
        peg_to_base_aggregator,
        ..
    } = registration.source.clone()
    else {
        unreachable!("caller checked source family")
    };

    let asset_round = provider
        .latest_round_data_at(asset_to_peg_proxy, block_ref)
        .await?;
    let peg_round = provider
        .latest_round_data_at(peg_to_base_proxy, block_ref)
        .await?;
    let new_asset_aggregator = provider
        .aggregator_at(asset_to_peg_proxy, block_ref)
        .await
        .unwrap_or(Some(asset_to_peg_aggregator));
    let new_peg_aggregator = provider
        .aggregator_at(peg_to_base_proxy, block_ref)
        .await
        .unwrap_or(Some(peg_to_base_aggregator));
    let changed_dependency_round = if new_peg_aggregator == request.aggregator {
        &peg_round
    } else {
        &asset_round
    };
    let derived_answer = registration
        .source
        .normalize_dependency_answers(asset_round.answer, peg_round.answer)
        .ok_or_else(|| {
            OracleError::Config(crate::error::OracleConfigError::Other(
                "source is not an Aave peg-to-base synchronicity source".to_string(),
            ))
        })?;
    let proxy_round = RoundData {
        round_id: changed_dependency_round.round_id,
        answer: derived_answer,
        started_at: changed_dependency_round.started_at,
        updated_at: changed_dependency_round.updated_at,
        answered_in_round: changed_dependency_round.answered_in_round,
    };
    let new_source = match (new_asset_aggregator, new_peg_aggregator) {
        (Some(asset_aggregator), Some(peg_aggregator)) => {
            registration.source.with_synchronicity_dependency_state(
                asset_aggregator,
                asset_round.answer,
                peg_aggregator,
                peg_round.answer,
            )
        }
        _ => None,
    };
    let new_layout = tracker
        .detect_aggregator_layout(provider, new_asset_aggregator, block_ref)
        .await;

    Ok(ReconciledSourceRead {
        proxy_round,
        new_aggregator: new_asset_aggregator,
        new_layout,
        new_source,
        round_is_normalized: true,
    })
}

async fn reconcile_current_aave_synchronicity_peg_to_base<P: ChainlinkFeedProvider>(
    tracker: &mut OracleTracker,
    provider: &P,
    registration: &FeedRegistration,
) -> Result<ReconciledSourceRead, OracleError> {
    let FeedSource::AaveSynchronicityPegToBase {
        asset_to_peg_proxy,
        asset_to_peg_aggregator,
        peg_to_base_proxy,
        peg_to_base_aggregator,
        ..
    } = registration.source.clone()
    else {
        unreachable!("caller checked source family")
    };

    let asset_round = provider.latest_round_data(asset_to_peg_proxy).await?;
    let peg_round = provider.latest_round_data(peg_to_base_proxy).await?;
    let new_asset_aggregator = provider
        .aggregator(asset_to_peg_proxy)
        .await
        .unwrap_or(Some(asset_to_peg_aggregator));
    let new_peg_aggregator = provider
        .aggregator(peg_to_base_proxy)
        .await
        .unwrap_or(Some(peg_to_base_aggregator));
    let representative_round = if peg_round.updated_at >= asset_round.updated_at {
        &peg_round
    } else {
        &asset_round
    };
    let derived_answer = registration
        .source
        .normalize_dependency_answers(asset_round.answer, peg_round.answer)
        .ok_or_else(|| {
            OracleError::Config(crate::error::OracleConfigError::Other(
                "source is not an Aave peg-to-base synchronicity source".to_string(),
            ))
        })?;
    let proxy_round = RoundData {
        round_id: representative_round.round_id,
        answer: derived_answer,
        started_at: representative_round.started_at,
        updated_at: representative_round.updated_at,
        answered_in_round: representative_round.answered_in_round,
    };
    let new_source = match (new_asset_aggregator, new_peg_aggregator) {
        (Some(asset_aggregator), Some(peg_aggregator)) => {
            registration.source.with_synchronicity_dependency_state(
                asset_aggregator,
                asset_round.answer,
                peg_aggregator,
                peg_round.answer,
            )
        }
        _ => None,
    };
    let new_layout = tracker
        .detect_aggregator_layout(provider, new_asset_aggregator, None)
        .await;

    Ok(ReconciledSourceRead {
        proxy_round,
        new_aggregator: new_asset_aggregator,
        new_layout,
        new_source,
        round_is_normalized: true,
    })
}

impl OracleTracker {
    async fn detect_aggregator_layout<P: ChainlinkFeedProvider>(
        &mut self,
        provider: &P,
        aggregator: Option<Address>,
        block: Option<OracleBlockRef>,
    ) -> Option<AggregatorLayoutEvidence> {
        self.registry
            .detect_aggregator_layout(provider, aggregator, block)
            .await
    }

    fn current_aggregator(&self, id: &FeedId) -> Option<Address> {
        self.registry
            .registrations_map()
            .get(id)
            .and_then(|registration| registration.current_aggregator)
    }
}

fn reconciliation_value_status(
    round_status: &OracleRoundStatus,
    request: &OracleReconciliationRequest,
    proxy_round: &RoundData,
) -> OracleValueStatus {
    match round_status {
        OracleRoundStatus::Unknown => OracleValueStatus::RequiresRepair,
        OracleRoundStatus::Fresh
        | OracleRoundStatus::Stale { .. }
        | OracleRoundStatus::IncompleteRound
        | OracleRoundStatus::InvalidAnswer
            if proxy_round_matches_event(proxy_round, &request.event_round) =>
        {
            OracleValueStatus::Confirmed
        }
        OracleRoundStatus::Fresh
        | OracleRoundStatus::Stale { .. }
        | OracleRoundStatus::IncompleteRound
        | OracleRoundStatus::InvalidAnswer => OracleValueStatus::Corrected,
    }
}

fn is_event_originated_source(source: OracleValueSource) -> bool {
    matches!(
        source,
        OracleValueSource::Event | OracleValueSource::Derived
    )
}

fn proxy_round_matches_event(proxy_round: &RoundData, event_round: &RoundData) -> bool {
    proxy_round.round_id == event_round.round_id
        && proxy_round.answer == event_round.answer
        && proxy_round.updated_at == event_round.updated_at
}

fn pyth_source_from_event_tags(source: &FeedSource, labels: &[ReportTag]) -> Option<FeedSource> {
    let (_pyth, price_id, current_expo, current_conf) = source.pyth_source()?;
    let mut expo = current_expo;
    let mut conf = current_conf;
    let mut saw_pyth_label = false;

    for label in labels {
        match label.key.as_str() {
            "pyth_expo" => {
                if let Ok(value) = label.value.parse::<i32>() {
                    expo = value;
                    saw_pyth_label = true;
                }
            }
            "pyth_conf" => {
                if let Ok(value) = label.value.parse::<u64>() {
                    conf = value;
                    saw_pyth_label = true;
                }
            }
            _ => {}
        }
    }

    saw_pyth_label.then(|| {
        source
            .with_pyth_event_metadata(price_id, expo, conf)
            .expect("pyth_source returned Some for Pyth source")
    })
}

#[allow(dead_code)]
fn _ethereum_batch_report_type_is_supported(_: &ReactiveBatchReport<Ethereum>) {}
