use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, I256, U256, keccak256};
use crate::alloy_rpc_types_eth::Filter;
use alloy_sol_types::sol;
use evm_fork_cache::{
    StateUpdate, StateView,
    cache::EvmCache,
    reactive::{
        ChainStatus, HandlerError, HandlerId, HandlerOutcome, HookSignal, InvalidationReason,
        InvalidationRequest, LogInterest, ReactiveContext, ReactiveEffect, ReactiveHandler,
        ReactiveInput, ReactiveInterest, ReportTag, RouteKeySpec, StateEffectQuality,
    },
    state_update::PurgeScope,
};

use crate::{
    ANSWER_UPDATED_TOPIC, AdapterFuture, AssetId, Denomination, EvmCacheChainlinkReader, Feed,
    FeedConfig, FeedId, FeedMetadata, FeedRegistration, FeedSource, ORACLE_SIGNAL_NAMESPACE,
    OracleAdapterFeedSkip, OracleAdapterId, OracleAdapterPlugin, OracleAdapterSkipReason,
    OracleDiscoveredFeed, OracleDiscoveryContext, OracleDiscoveryReport, OracleError,
    OracleFeedStatus, OraclePriceUpdate, OracleSignalKind, OracleStorageSync, OracleValueSource,
    OracleValueStatus, REDSTONE_VALUE_UPDATE_TOPIC, RedstoneValueUpdate, RoundData,
    StalenessPolicy, decode_answer_updated, decode_redstone_value_update, state::classify_round,
};

sol! {
    interface RedstonePriceFeedInterface {
        function getDataFeedId() external view returns (bytes32);
        function getPriceFeedAdapter() external view returns (address);
    }
}

use RedstonePriceFeedInterface::{getDataFeedIdCall, getPriceFeedAdapterCall};

const ADAPTER_ID: &str = "evm-oracle-state.redstone";
const HANDLER_ID: &str = "evm-oracle-state.redstone";
const REDSTONE_NO_ROUNDS_ROUND_ID: u64 = 1;
const REDSTONE_MULTI_FEED_DATA_FEEDS_STORAGE_LOCATION: B256 =
    alloy_primitives::b256!("5e9fb4cb0eb3c2583734d3394f30bb14b241acb9b3a034f7e7ba1a62db4370f1");
const REDSTONE_PRICE_FEEDS_VALUES_MAPPING_STORAGE_LOCATION: B256 =
    alloy_primitives::b256!("4dd0c77efa6f6d590c97573d8c70b714546e7311202ff7c11c484cc841d91bfc");
const REDSTONE_PRICE_FEEDS_LATEST_UPDATE_TIMESTAMPS_STORAGE_LOCATION: B256 =
    alloy_primitives::b256!("3d01e4d77237ea0f771f1786da4d4ff757fcba6a92933aa53b1dcef2d6bd6fe2");
const REDSTONE_PRICE_FEEDS_WITH_ROUNDS_ROUND_TIMESTAMPS_MAPPING_STORAGE_LOCATION: B256 =
    alloy_primitives::b256!("207e00944d909d1224f0c253d58489121d736649f8393199f55eecf4f0cf3eb0");
const REDSTONE_PRICE_FEEDS_WITH_ROUNDS_LATEST_ROUND_ID_STORAGE_LOCATION: B256 =
    alloy_primitives::b256!("c68d7f1ee07d8668991a8951e720010c9d44c2f11c06b5cac61fbc4083263938");
const REDSTONE_DATA_TIMESTAMP_BITS: usize = 48;
const REDSTONE_BLOCK_TIMESTAMP_BITS: usize = 48;
const REDSTONE_MULTI_FEED_VALUE_BITS: usize = 152;
const REDSTONE_MULTI_FEED_BLOCK_TIMESTAMP_OFFSET_BITS: usize = REDSTONE_DATA_TIMESTAMP_BITS;
const REDSTONE_MULTI_FEED_VALUE_OFFSET_BITS: usize =
    REDSTONE_DATA_TIMESTAMP_BITS + REDSTONE_BLOCK_TIMESTAMP_BITS;
const REDSTONE_MULTI_FEED_IS_VALUE_BIGGER_OFFSET_BITS: usize =
    REDSTONE_MULTI_FEED_VALUE_OFFSET_BITS + REDSTONE_MULTI_FEED_VALUE_BITS;
const REDSTONE_PRICE_FEEDS_BLOCK_TIMESTAMP_OFFSET_BITS: usize = 128;

/// Declarative RedStone push-feed registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedstoneFeed {
    price_feed: Address,
    adapter: Option<Address>,
    data_feed_id: Option<B256>,
    feed_id: Option<FeedId>,
    label: Option<String>,
    base: Option<AssetId>,
    quote: Option<Denomination>,
    staleness: StalenessPolicy,
}

impl RedstoneFeed {
    /// Register a RedStone price-feed wrapper and discover adapter/feed id from it.
    pub fn new(price_feed: Address) -> Self {
        Self {
            price_feed,
            adapter: None,
            data_feed_id: None,
            feed_id: None,
            label: None,
            base: None,
            quote: None,
            staleness: StalenessPolicy::default(),
        }
    }

    /// Register a RedStone price-feed wrapper and discover adapter/feed id from it.
    pub fn price_feed(price_feed: Address) -> Self {
        Self::new(price_feed)
    }

    /// Register a RedStone price-feed wrapper with an explicit adapter and data feed id.
    pub fn push(price_feed: Address, adapter: Address, data_feed_id: B256) -> Self {
        Self::new(price_feed)
            .adapter(adapter)
            .data_feed_id(data_feed_id)
    }

    /// Return the user-facing price-feed wrapper.
    pub fn price_feed_address(&self) -> Address {
        self.price_feed
    }

    /// Set the RedStone adapter/event-source address.
    pub fn adapter(mut self, adapter: Address) -> Self {
        self.adapter = Some(adapter);
        self
    }

    /// Set the RedStone data feed id.
    pub fn data_feed_id(mut self, data_feed_id: B256) -> Self {
        self.data_feed_id = Some(data_feed_id);
        self
    }

    /// Set a stable feed id.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.feed_id = Some(FeedId::new(id));
        self
    }

    /// Set a stable feed id.
    pub fn feed_id(mut self, id: FeedId) -> Self {
        self.feed_id = Some(id);
        self
    }

    /// Set a human-readable label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the base asset label.
    pub fn base(mut self, base: AssetId) -> Self {
        self.base = Some(base);
        self
    }

    /// Set the quote denomination label.
    pub fn quote(mut self, quote: Denomination) -> Self {
        self.quote = Some(quote);
        self
    }

    /// Set a max-age staleness policy.
    pub fn max_age_secs(mut self, max_age_secs: u64) -> Self {
        self.staleness = StalenessPolicy::max_age(max_age_secs);
        self
    }

    /// Set the full staleness policy.
    pub fn staleness(mut self, staleness: StalenessPolicy) -> Self {
        self.staleness = staleness;
        self
    }

    fn feed_for_skip(&self) -> Feed {
        let mut feed = Feed::proxy(self.price_feed);
        if let Some(id) = self.feed_id.clone() {
            feed = feed.feed_id(id);
        }
        if let Some(label) = &self.label {
            feed = feed.label(label.clone());
        }
        if let Some(base) = &self.base {
            feed = feed.base(base.clone());
        }
        if let Some(quote) = &self.quote {
            feed = feed.quote(quote.clone());
        }
        feed.staleness(self.staleness)
    }

    fn config(&self) -> FeedConfig {
        FeedConfig {
            proxy: self.price_feed,
            id: self.feed_id.clone(),
            label: self.label.clone(),
            base: self.base.clone().map(String::from),
            quote: self.quote.clone().map(String::from),
            staleness: self.staleness,
        }
    }
}

/// Cache-backed RedStone push oracle discovery adapter.
#[derive(Clone, Debug, Default)]
pub struct RedstoneOracleAdapter {
    feeds: Vec<RedstoneFeed>,
    now_timestamp: Option<u64>,
}

impl RedstoneOracleAdapter {
    /// Create an empty RedStone adapter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an adapter for one RedStone price-feed wrapper.
    pub fn price_feed(price_feed: Address) -> Self {
        Self::new().feed(RedstoneFeed::new(price_feed))
    }

    /// Add one RedStone feed.
    pub fn feed(mut self, feed: RedstoneFeed) -> Self {
        self.feeds.push(feed);
        self
    }

    /// Add multiple RedStone feeds.
    pub fn feeds(mut self, feeds: impl IntoIterator<Item = RedstoneFeed>) -> Self {
        self.feeds.extend(feeds);
        self
    }

    /// Set a fixed timestamp for deterministic registration status classification.
    pub fn now_timestamp(mut self, now_timestamp: u64) -> Self {
        self.now_timestamp = Some(now_timestamp);
        self
    }

    fn timestamp(&self, fallback: Option<u64>) -> Result<u64, OracleError> {
        if let Some(now_timestamp) = self.now_timestamp.or(fallback) {
            return Ok(now_timestamp);
        }
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(crate::error::clock_error)?
            .as_secs())
    }

    fn discover_feeds(
        &self,
        cache: &mut EvmCache,
        _now_timestamp: u64,
    ) -> Result<OracleDiscoveryReport, OracleError> {
        let mut report = OracleDiscoveryReport::new();
        for feed in &self.feeds {
            match self.discover_feed(cache, feed) {
                Ok(discovered) => report = report.with_feed(discovered),
                Err(error) => {
                    report = report.with_skip(OracleAdapterFeedSkip {
                        feed: feed.feed_for_skip(),
                        proxy: feed.price_feed,
                        reason: OracleAdapterSkipReason::UnsupportedRedstoneSource {
                            error: error.to_string(),
                        },
                    });
                }
            }
        }
        Ok(report)
    }

    fn discover_feed(
        &self,
        cache: &mut EvmCache,
        feed: &RedstoneFeed,
    ) -> Result<OracleDiscoveredFeed, OracleError> {
        let data_feed_id = match feed.data_feed_id {
            Some(data_feed_id) => data_feed_id,
            None => cache
                .call_sol(feed.price_feed, getDataFeedIdCall {})
                .map_err(provider_error)?,
        };
        let adapter = match feed.adapter {
            Some(adapter) => adapter,
            None => cache
                .call_sol(feed.price_feed, getPriceFeedAdapterCall {})
                .map_err(provider_error)?,
        };

        let reader = EvmCacheChainlinkReader::new(cache);
        let metadata = FeedMetadata {
            decimals: reader.read_decimals(feed.price_feed)?,
            description: reader.read_description(feed.price_feed)?,
            version: reader.read_version(feed.price_feed)?,
        };
        let round = reader.read_latest_round_data(feed.price_feed)?;
        let config = feed.config();
        let id = config
            .id
            .unwrap_or_else(|| derive_redstone_feed_id(config.label.as_deref(), feed.price_feed));
        let registration = FeedRegistration {
            id,
            proxy: config.proxy,
            label: config.label,
            base: config.base,
            quote: config.quote,
            staleness: config.staleness,
            current_aggregator: Some(adapter),
            aggregator_layout: None,
            metadata,
            source: FeedSource::redstone_push(feed.price_feed, adapter, data_feed_id),
            status: OracleFeedStatus::Ready,
        };

        Ok(OracleDiscoveredFeed::new(registration, round))
    }
}

impl OracleAdapterPlugin for RedstoneOracleAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        OracleAdapterId::new(ADAPTER_ID)
    }

    fn discover<'a>(
        &'a self,
        ctx: OracleDiscoveryContext<'a>,
    ) -> AdapterFuture<'a, OracleDiscoveryReport> {
        Box::pin(async move {
            let now_timestamp = self.timestamp(Some(ctx.now_timestamp))?;
            self.discover_feeds(ctx.cache, now_timestamp)
        })
    }

    fn reactive_handler(
        &self,
        registrations: Vec<FeedRegistration>,
        _storage_sync: OracleStorageSync,
    ) -> Arc<dyn ReactiveHandler<Ethereum>> {
        Arc::new(RedstoneReactiveHandler::new(registrations))
    }
}

/// Storage helpers for RedStone `MultiFeedAdapterWithoutRounds` push feeds.
///
/// The live RedStone multi-feed adapter stores each feed in a packed
/// `DataFeedDetails` word keyed by `dataFeedId`. A direct event write is only
/// emitted when that packed slot is already hot in the cache. If the value does
/// not fit the inline `uint152` field, the overflow slot must also be hot.
#[derive(Clone, Debug, Default)]
pub struct RedstoneMultiFeedStorageAdapter;

impl RedstoneMultiFeedStorageAdapter {
    /// Return the storage slot for `dataFeeds[dataFeedId]`.
    pub fn data_feed_details_slot(data_feed_id: B256) -> U256 {
        keyed_slot(
            data_feed_id,
            REDSTONE_MULTI_FEED_DATA_FEEDS_STORAGE_LOCATION,
        )
    }

    /// Return the overflow slot for `dataFeeds[dataFeedId].biggerValue`.
    pub fn bigger_value_slot(data_feed_id: B256) -> U256 {
        Self::data_feed_details_slot(data_feed_id) + U256::from(1_u8)
    }

    /// Pack RedStone `DataFeedDetails`.
    ///
    /// RedStone's `ValueUpdate` event exposes one timestamp. The storage layout
    /// keeps both data-package and block timestamps, so the event timestamp is
    /// used for the block timestamp and `timestamp * 1000` is used for the
    /// millisecond data timestamp.
    pub fn pack_data_feed_details_from_event(value: U256, updated_at: u64) -> Option<U256> {
        let data_timestamp_ms = updated_at.checked_mul(1_000)?;
        Self::pack_data_feed_details(value, data_timestamp_ms, updated_at)
    }

    /// Pack RedStone `DataFeedDetails` with explicit timestamps.
    pub fn pack_data_feed_details(
        value: U256,
        data_timestamp_ms: u64,
        block_timestamp: u64,
    ) -> Option<U256> {
        if data_timestamp_ms > uint_mask_u64(REDSTONE_DATA_TIMESTAMP_BITS)
            || block_timestamp > uint_mask_u64(REDSTONE_BLOCK_TIMESTAMP_BITS)
        {
            return None;
        }

        let inline_value = value & uint_mask(REDSTONE_MULTI_FEED_VALUE_BITS);
        let is_value_bigger = if value > uint_mask(REDSTONE_MULTI_FEED_VALUE_BITS) {
            U256::from(1_u8)
        } else {
            U256::ZERO
        };

        Some(
            U256::from(data_timestamp_ms)
                | (U256::from(block_timestamp) << REDSTONE_MULTI_FEED_BLOCK_TIMESTAMP_OFFSET_BITS)
                | (inline_value << REDSTONE_MULTI_FEED_VALUE_OFFSET_BITS)
                | (is_value_bigger << REDSTONE_MULTI_FEED_IS_VALUE_BIGGER_OFFSET_BITS),
        )
    }

    fn state_updates_for_value_update(
        adapter: Address,
        event: &RedstoneValueUpdate,
        state: &dyn StateView,
    ) -> Option<Vec<StateUpdate>> {
        let details_slot = Self::data_feed_details_slot(event.data_feed_id);
        state.storage(adapter, details_slot)?;
        let details = Self::pack_data_feed_details_from_event(event.value, event.updated_at)?;
        let mut updates = vec![StateUpdate::slot(adapter, details_slot, details)];

        if event.value > uint_mask(REDSTONE_MULTI_FEED_VALUE_BITS) {
            let bigger_value_slot = Self::bigger_value_slot(event.data_feed_id);
            state.storage(adapter, bigger_value_slot)?;
            updates.push(StateUpdate::slot(adapter, bigger_value_slot, event.value));
        }

        Some(updates)
    }

    fn state_updates_for_answer(
        adapter: Address,
        data_feed_id: B256,
        value: U256,
        updated_at: u64,
        state: &dyn StateView,
    ) -> Option<Vec<StateUpdate>> {
        let event = RedstoneValueUpdate {
            adapter,
            data_feed_id,
            value,
            updated_at,
            block_number: None,
            log_index: None,
            removed: false,
        };
        Self::state_updates_for_value_update(adapter, &event, state)
    }
}

/// Storage helpers for RedStone `PriceFeedsAdapter*` push-feed layouts.
///
/// These layouts are less common for the current multi-feed deployments but are
/// still useful for older RedStone adapters and merged price-feed wrappers.
#[derive(Clone, Debug, Default)]
pub struct RedstonePriceFeedsStorageAdapter;

impl RedstonePriceFeedsStorageAdapter {
    /// Return the storage slot for the no-rounds `values[dataFeedId]` mapping.
    pub fn value_slot(data_feed_id: B256) -> U256 {
        keyed_slot(
            data_feed_id,
            REDSTONE_PRICE_FEEDS_VALUES_MAPPING_STORAGE_LOCATION,
        )
    }

    /// Return the storage slot for a with-rounds `values[dataFeedId][roundId]` value.
    pub fn round_value_slot(data_feed_id: B256, round_id: U256) -> U256 {
        keyed_slot2(
            data_feed_id,
            round_id,
            REDSTONE_PRICE_FEEDS_VALUES_MAPPING_STORAGE_LOCATION,
        )
    }

    /// Return the packed latest-update timestamps slot for no-rounds adapters.
    pub fn latest_update_timestamps_slot() -> U256 {
        U256::from_be_slice(
            REDSTONE_PRICE_FEEDS_LATEST_UPDATE_TIMESTAMPS_STORAGE_LOCATION.as_slice(),
        )
    }

    /// Return the slot storing the latest round id for with-rounds adapters.
    pub fn latest_round_id_slot() -> U256 {
        U256::from_be_slice(
            REDSTONE_PRICE_FEEDS_WITH_ROUNDS_LATEST_ROUND_ID_STORAGE_LOCATION.as_slice(),
        )
    }

    /// Return the with-rounds timestamp slot for `roundId`.
    pub fn round_timestamp_slot(round_id: U256) -> U256 {
        mapping_slot(
            round_id,
            U256::from_be_slice(
                REDSTONE_PRICE_FEEDS_WITH_ROUNDS_ROUND_TIMESTAMPS_MAPPING_STORAGE_LOCATION
                    .as_slice(),
            ),
        )
    }

    /// Pack no-rounds latest update timestamps.
    pub fn pack_latest_update_timestamps_from_event(updated_at: u64) -> Option<U256> {
        let data_timestamp_ms = updated_at.checked_mul(1_000)?;
        Self::pack_latest_update_timestamps(data_timestamp_ms, updated_at)
    }

    /// Pack no-rounds latest update timestamps with explicit timestamps.
    ///
    /// Always returns `Some`: both packed fields are 128 bits wide, so every
    /// `u64` input fits. The `Option` return is kept for signature stability
    /// with the other packing helpers.
    pub fn pack_latest_update_timestamps(
        data_timestamp_ms: u64,
        block_timestamp: u64,
    ) -> Option<U256> {
        Some(
            (U256::from(data_timestamp_ms) << REDSTONE_PRICE_FEEDS_BLOCK_TIMESTAMP_OFFSET_BITS)
                | U256::from(block_timestamp),
        )
    }

    fn state_updates_for_value_update(
        adapter: Address,
        event: &RedstoneValueUpdate,
        state: &dyn StateView,
    ) -> Option<Vec<StateUpdate>> {
        let value_slot = Self::value_slot(event.data_feed_id);
        let timestamp_slot = Self::latest_update_timestamps_slot();
        state.storage(adapter, value_slot)?;
        state.storage(adapter, timestamp_slot)?;
        let timestamps = Self::pack_latest_update_timestamps_from_event(event.updated_at)?;

        Some(vec![
            StateUpdate::slot(adapter, value_slot, event.value),
            StateUpdate::slot(adapter, timestamp_slot, timestamps),
        ])
    }

    fn state_updates_for_no_rounds_answer(
        adapter: Address,
        data_feed_id: B256,
        value: U256,
        updated_at: u64,
        state: &dyn StateView,
    ) -> Option<Vec<StateUpdate>> {
        let event = RedstoneValueUpdate {
            adapter,
            data_feed_id,
            value,
            updated_at,
            block_number: None,
            log_index: None,
            removed: false,
        };
        Self::state_updates_for_value_update(adapter, &event, state)
    }

    fn state_updates_for_round_answer(
        adapter: Address,
        data_feed_id: B256,
        round_id: U256,
        value: U256,
        updated_at: u64,
        state: &dyn StateView,
    ) -> Option<Vec<StateUpdate>> {
        let value_slot = Self::round_value_slot(data_feed_id, round_id);
        let round_timestamp_slot = Self::round_timestamp_slot(round_id);
        let latest_round_id_slot = Self::latest_round_id_slot();
        state.storage(adapter, value_slot)?;
        state.storage(adapter, round_timestamp_slot)?;
        state.storage(adapter, latest_round_id_slot)?;

        Some(vec![
            StateUpdate::slot(adapter, value_slot, value),
            StateUpdate::slot(adapter, round_timestamp_slot, U256::from(updated_at)),
            StateUpdate::slot(adapter, latest_round_id_slot, round_id),
        ])
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RedstoneValueUpdateKey {
    adapter: Address,
    data_feed_id: B256,
}

/// Reactive-runtime bridge for RedStone push oracle feeds.
#[derive(Clone, Debug)]
pub struct RedstoneReactiveHandler {
    registrations_by_value_update: BTreeMap<RedstoneValueUpdateKey, Vec<FeedRegistration>>,
    registrations_by_answer_updated: BTreeMap<Address, Vec<FeedRegistration>>,
}

impl RedstoneReactiveHandler {
    /// Create a handler from current RedStone feed registrations.
    pub fn new(registrations: Vec<FeedRegistration>) -> Self {
        let mut registrations_by_value_update: BTreeMap<
            RedstoneValueUpdateKey,
            Vec<FeedRegistration>,
        > = BTreeMap::new();
        let mut registrations_by_answer_updated: BTreeMap<Address, Vec<FeedRegistration>> =
            BTreeMap::new();
        let mut seen_value_updates = BTreeSet::new();
        let mut seen_answer_updates = BTreeSet::new();

        for registration in registrations {
            let FeedSource::RedstonePush {
                price_feed,
                adapter,
                data_feed_id,
            } = registration.source
            else {
                continue;
            };

            let value_key = RedstoneValueUpdateKey {
                adapter,
                data_feed_id,
            };
            if seen_value_updates.insert((registration.id.clone(), value_key)) {
                registrations_by_value_update
                    .entry(value_key)
                    .or_default()
                    .push(registration.clone());
            }

            if seen_answer_updates.insert((registration.id.clone(), price_feed)) {
                registrations_by_answer_updated
                    .entry(price_feed)
                    .or_default()
                    .push(registration);
            }
        }

        Self {
            registrations_by_value_update,
            registrations_by_answer_updated,
        }
    }

    /// Stable handler id.
    pub fn id(&self) -> HandlerId {
        HandlerId::new(HANDLER_ID)
    }

    /// Interests for RedStone adapter and merged-feed events.
    pub fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        let mut interests = Vec::new();
        let mut value_adapters = BTreeSet::new();
        for key in self.registrations_by_value_update.keys() {
            if value_adapters.insert(key.adapter) {
                interests.push(log_interest(key.adapter, REDSTONE_VALUE_UPDATE_TOPIC));
            }
        }
        interests.extend(
            self.registrations_by_answer_updated
                .keys()
                .copied()
                .map(|price_feed| log_interest(price_feed, ANSWER_UPDATED_TOPIC)),
        );
        interests
    }

    fn handle_value_update(
        &self,
        ctx: &ReactiveContext,
        log: &crate::alloy_rpc_types_eth::Log,
        state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let event = decode_redstone_value_update(log).map_err(|error| {
            HandlerError::new(format!("decode RedStone ValueUpdate failed: {error}"))
        })?;
        let key = RedstoneValueUpdateKey {
            adapter: event.adapter,
            data_feed_id: event.data_feed_id,
        };
        let Some(registrations) = self.registrations_by_value_update.get(&key) else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };

        let answer = redstone_value_to_i256(event.value)?;
        Ok(self.outcome_for_registrations(
            ctx,
            log,
            registrations,
            event.adapter,
            answer,
            U256::from(REDSTONE_NO_ROUNDS_ROUND_ID),
            event.updated_at,
            event.block_number,
            event.log_index,
            event.removed,
            Some(event),
            state,
        ))
    }

    fn handle_answer_updated(
        &self,
        ctx: &ReactiveContext,
        log: &crate::alloy_rpc_types_eth::Log,
        state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let price_feed = log.address();
        let Some(registrations) = self.registrations_by_answer_updated.get(&price_feed) else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        let event = decode_answer_updated(log).map_err(|error| {
            HandlerError::new(format!("decode RedStone AnswerUpdated failed: {error}"))
        })?;

        Ok(self.outcome_for_registrations(
            ctx,
            log,
            registrations,
            price_feed,
            event.current,
            event.round_id,
            event.updated_at,
            event.block_number,
            event.log_index,
            event.removed,
            None,
            state,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn outcome_for_registrations(
        &self,
        ctx: &ReactiveContext,
        log: &crate::alloy_rpc_types_eth::Log,
        registrations: &[FeedRegistration],
        event_source: Address,
        raw_answer: I256,
        event_round_id: U256,
        updated_at: u64,
        block_number: Option<u64>,
        log_index: Option<u64>,
        removed: bool,
        value_update: Option<RedstoneValueUpdate>,
        state: &dyn StateView,
    ) -> HandlerOutcome {
        let block_number = block_number.or_else(|| ctx.block.as_ref().map(|block| block.number));
        let block_hash = log
            .block_hash
            .or_else(|| ctx.block.as_ref().map(|block| block.hash));
        let log_index = log_index.or(ctx.log_index);
        let value_status = if removed {
            OracleValueStatus::RequiresRepair
        } else {
            OracleValueStatus::EventPending
        };
        let mut effects = Vec::new();
        let mut tags = Vec::new();
        let direct_updates = if removed {
            None
        } else {
            registrations.first().and_then(|registration| {
                redstone_state_updates(
                    registration,
                    event_source,
                    raw_answer,
                    event_round_id,
                    updated_at,
                    value_update.as_ref(),
                    state,
                )
            })
        };
        let has_direct_updates = direct_updates.is_some();
        if let Some(updates) = direct_updates {
            effects.extend(updates.into_iter().map(ReactiveEffect::StateUpdate));
        }

        for registration in registrations {
            if !has_direct_updates {
                append_redstone_invalidations(&mut effects, registration, event_source);
            }
            let hook_tags = redstone_hook_tags(registration, event_source, value_update.as_ref());
            tags.extend(hook_tags.clone());
            let normalized_answer = registration
                .source
                .normalize_answer_from_event(Some(event_source), raw_answer);
            let round = RoundData {
                round_id: event_round_id,
                answer: normalized_answer,
                started_at: updated_at,
                updated_at,
                answered_in_round: event_round_id,
            };
            let round_status = classify_round(&round, updated_at, &registration.staleness);
            effects.push(ReactiveEffect::Hook(HookSignal {
                namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
                kind: Cow::Borrowed(OracleSignalKind::PriceUpdate.as_str()),
                labels: hook_tags,
                payload: Some(Arc::new(OraclePriceUpdate {
                    id: registration.id.clone(),
                    proxy: registration.proxy,
                    aggregator: event_source,
                    label: registration.label.clone(),
                    base: registration.base.clone(),
                    quote: registration.quote.clone(),
                    raw_answer: normalized_answer,
                    decimals: registration.metadata.decimals,
                    event_round_id,
                    started_at: updated_at,
                    updated_at,
                    block_number,
                    block_hash,
                    log_index,
                    round_status,
                    value_status,
                    source: OracleValueSource::Event,
                })),
            }));
        }

        HandlerOutcome {
            effects,
            quality: if has_direct_updates {
                StateEffectQuality::ExactFromInput
            } else {
                StateEffectQuality::RequiresRepair
            },
            tags,
        }
    }
}

impl ReactiveHandler<Ethereum> for RedstoneReactiveHandler {
    fn id(&self) -> HandlerId {
        self.id()
    }

    fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        self.interests()
    }

    fn handle(
        &self,
        ctx: &ReactiveContext,
        input: &ReactiveInput<Ethereum>,
        state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        if log.removed && !matches!(ctx.chain_status, ChainStatus::Reorged { .. }) {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        }

        match log.topics().first().copied() {
            Some(REDSTONE_VALUE_UPDATE_TOPIC) => self.handle_value_update(ctx, log, state),
            Some(ANSWER_UPDATED_TOPIC) => self.handle_answer_updated(ctx, log, state),
            _ => Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect)),
        }
    }
}

fn log_interest(address: Address, topic: B256) -> ReactiveInterest<Ethereum> {
    ReactiveInterest::Logs(LogInterest {
        provider_filter: Filter::new().address(address).event_signature(topic),
        local_matcher: None,
        route_key: Some(RouteKeySpec::EmitterAddress),
    })
}

fn append_redstone_invalidations(
    effects: &mut Vec<ReactiveEffect>,
    registration: &FeedRegistration,
    event_source: Address,
) {
    let mut addresses = BTreeSet::from([registration.proxy, event_source]);
    if let FeedSource::RedstonePush { adapter, .. } = registration.source {
        addresses.insert(adapter);
    }
    effects.extend(addresses.into_iter().map(|address| {
        ReactiveEffect::Invalidate(InvalidationRequest {
            scope: PurgeScope::AllStorage,
            address,
            reason: InvalidationReason::HandlerRequested,
        })
    }));
}

fn redstone_hook_tags(
    registration: &FeedRegistration,
    event_source: Address,
    value_update: Option<&RedstoneValueUpdate>,
) -> Vec<ReportTag> {
    let mut tags = vec![
        ReportTag::new("feed_id", registration.id.to_string()),
        ReportTag::new("proxy", format!("{:?}", registration.proxy)),
        ReportTag::new("aggregator", format!("{event_source:?}")),
    ];
    if let Some(value_update) = value_update {
        tags.push(ReportTag::new(
            "data_feed_id",
            format!("{:?}", value_update.data_feed_id),
        ));
    }
    tags
}

fn redstone_value_to_i256(value: U256) -> Result<I256, HandlerError> {
    I256::try_from(value)
        .map_err(|_| HandlerError::new(format!("RedStone value {value} does not fit int256")))
}

#[allow(clippy::too_many_arguments)]
fn redstone_state_updates(
    registration: &FeedRegistration,
    event_source: Address,
    raw_answer: I256,
    event_round_id: U256,
    updated_at: u64,
    value_update: Option<&RedstoneValueUpdate>,
    state: &dyn StateView,
) -> Option<Vec<StateUpdate>> {
    match value_update {
        Some(event) => redstone_state_updates_for_value_update(registration, event, state),
        None => redstone_state_updates_for_answer_updated(
            registration,
            event_source,
            raw_answer,
            event_round_id,
            updated_at,
            state,
        ),
    }
}

fn redstone_state_updates_for_value_update(
    registration: &FeedRegistration,
    event: &RedstoneValueUpdate,
    state: &dyn StateView,
) -> Option<Vec<StateUpdate>> {
    let FeedSource::RedstonePush {
        adapter,
        data_feed_id,
        ..
    } = registration.source
    else {
        return None;
    };
    if event.adapter != adapter || event.data_feed_id != data_feed_id {
        return None;
    }

    RedstoneMultiFeedStorageAdapter::state_updates_for_value_update(adapter, event, state).or_else(
        || RedstonePriceFeedsStorageAdapter::state_updates_for_value_update(adapter, event, state),
    )
}

fn redstone_state_updates_for_answer_updated(
    registration: &FeedRegistration,
    event_source: Address,
    raw_answer: I256,
    event_round_id: U256,
    updated_at: u64,
    state: &dyn StateView,
) -> Option<Vec<StateUpdate>> {
    let FeedSource::RedstonePush {
        price_feed,
        adapter,
        data_feed_id,
    } = registration.source
    else {
        return None;
    };
    if event_source != price_feed || adapter != price_feed {
        return None;
    }
    let value = redstone_i256_to_u256(raw_answer)?;

    if event_round_id == U256::from(REDSTONE_NO_ROUNDS_ROUND_ID) {
        RedstoneMultiFeedStorageAdapter::state_updates_for_answer(
            adapter,
            data_feed_id,
            value,
            updated_at,
            state,
        )
        .or_else(|| {
            RedstonePriceFeedsStorageAdapter::state_updates_for_no_rounds_answer(
                adapter,
                data_feed_id,
                value,
                updated_at,
                state,
            )
        })
    } else {
        RedstonePriceFeedsStorageAdapter::state_updates_for_round_answer(
            adapter,
            data_feed_id,
            event_round_id,
            value,
            updated_at,
            state,
        )
    }
}

fn redstone_i256_to_u256(value: I256) -> Option<U256> {
    let raw = value.into_raw();
    if raw >> 255 == U256::ZERO {
        Some(raw)
    } else {
        None
    }
}

fn keyed_slot(key: B256, base_slot: B256) -> U256 {
    let mut preimage = [0_u8; 64];
    preimage[..32].copy_from_slice(key.as_slice());
    preimage[32..].copy_from_slice(base_slot.as_slice());
    U256::from_be_slice(keccak256(preimage).as_slice())
}

fn keyed_slot2(key0: B256, key1: U256, base_slot: B256) -> U256 {
    let mut preimage = [0_u8; 96];
    preimage[..32].copy_from_slice(key0.as_slice());
    preimage[32..64].copy_from_slice(&key1.to_be_bytes::<32>());
    preimage[64..].copy_from_slice(base_slot.as_slice());
    U256::from_be_slice(keccak256(preimage).as_slice())
}

fn mapping_slot(key: U256, base_slot: U256) -> U256 {
    let mut preimage = [0_u8; 64];
    preimage[..32].copy_from_slice(&key.to_be_bytes::<32>());
    preimage[32..].copy_from_slice(&base_slot.to_be_bytes::<32>());
    U256::from_be_slice(keccak256(preimage).as_slice())
}

fn uint_mask(bits: usize) -> U256 {
    debug_assert!(bits <= 256);
    match bits {
        0 => U256::ZERO,
        256 => U256::MAX,
        bits => (U256::from(1_u8) << bits) - U256::from(1_u8),
    }
}

fn uint_mask_u64(bits: usize) -> u64 {
    // Callers mask sub-word fields only; a full-width (or wider) mask would
    // make range guards like `x > uint_mask_u64(bits)` silently inoperative.
    debug_assert!(bits < 64);
    (1_u64 << bits) - 1
}

fn derive_redstone_feed_id(label: Option<&str>, price_feed: Address) -> FeedId {
    if let Some(label) = label {
        let normalized = label
            .chars()
            .filter_map(|ch| {
                if ch.is_ascii_alphanumeric() {
                    Some(ch.to_ascii_lowercase())
                } else if ch.is_ascii_whitespace() || matches!(ch, '/' | '_' | '-') {
                    Some('-')
                } else {
                    None
                }
            })
            .collect::<String>()
            .trim_matches('-')
            .to_string();
        if !normalized.is_empty() {
            return FeedId::new(normalized);
        }
    }
    FeedId::new(format!("redstone-{price_feed:?}"))
}

fn provider_error(error: impl std::fmt::Debug) -> OracleError {
    OracleError::Provider(format!("{error:?}"))
}
