use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_provider::{RootProvider, network::AnyNetwork};
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_rpc_types_eth::{Filter, Log as RpcLog};
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::{
    StateView,
    cache::EvmCache,
    reactive::{
        BlockRef, ChainStatus, EventSubscriber, HandlerError, HandlerId, HandlerOutcome,
        HookSignal, InputSource, InterestOwnerSubscriber, LogInterest, ReactiveConfig,
        ReactiveEffect, ReactiveEngine, ReactiveHandler, ReactiveInput, ReactiveInputBatch,
        ReactiveInputRecord, ReactiveInterest, ReactiveRuntime, ReportTag, RouteKeySpec,
        StateEffectQuality, SubscriberBackfill, SubscriberError, SubscriberNextBatch,
    },
};
use evm_oracle_state::{
    AdapterFuture, Feed, FeedId, FeedMetadata, FeedRegistration, FeedSource,
    ORACLE_SIGNAL_NAMESPACE, OracleAdapterFeedSkip, OracleAdapterId, OracleAdapterPlugin,
    OracleAdapterSkipReason, OracleDiscoveredFeed, OracleDiscoveryContext, OracleDiscoveryReport,
    OracleFeedStatus, OraclePriceUpdate, OracleRegistry, OracleRoundStatus, OracleRuntime,
    OracleSignalKind, OracleSourceDescriptor, OracleStorageSync, OracleTracker,
    OracleTransformDescriptor, OracleValueSource, OracleValueStatus, RoundData, StalenessPolicy,
};

const CUSTOM_TOPIC: B256 = B256::with_last_byte(0x91);

#[derive(Clone, Debug)]
struct CustomOracleAdapter {
    id: OracleAdapterId,
    feed_id: FeedId,
    proxy: Address,
    event_source: Address,
    initial_answer: I256,
}

impl CustomOracleAdapter {
    fn new(proxy: Address, event_source: Address, initial_answer: I256) -> Self {
        Self {
            id: OracleAdapterId::new("test.custom-oracle"),
            feed_id: FeedId::new("custom-usd"),
            proxy,
            event_source,
            initial_answer,
        }
    }

    fn registration(&self) -> FeedRegistration {
        let source = OracleSourceDescriptor::new(
            self.id.clone(),
            Cow::Borrowed("test-source"),
            self.proxy,
            self.proxy,
            self.event_source,
        )
        .transform(OracleTransformDescriptor::Identity);

        FeedRegistration {
            id: self.feed_id.clone(),
            proxy: self.proxy,
            label: Some("Custom/USD".to_string()),
            base: Some("CUSTOM".to_string()),
            quote: Some("USD".to_string()),
            staleness: StalenessPolicy::max_age(3_600),
            current_aggregator: Some(self.event_source),
            aggregator_layout: None,
            metadata: metadata(),
            source: FeedSource::custom(source),
            status: OracleFeedStatus::Ready,
        }
    }
}

#[test]
fn runtime_registers_and_unregisters_seeded_feed() {
    let proxy = Address::repeat_byte(0x13);
    let aggregator = Address::repeat_byte(0x23);
    let mut oracle = OracleRuntime::from_tracker(OracleTracker::new(
        OracleRegistry::new_at_timestamp(1_700_000_030),
    ));
    let registration = FeedRegistration {
        id: FeedId::new("dynamic-chainlink-usd"),
        proxy,
        label: Some("Dynamic/USD".to_string()),
        base: Some("DYN".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(aggregator),
        aggregator_layout: None,
        metadata: metadata(),
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
    };

    let registered = oracle
        .register_seeded_feed(
            registration,
            round(1, I256::unchecked_from(321_i64), 1_700_000_000),
        )
        .expect("seeded feed should register");
    assert_eq!(
        registered.registered_feed_ids,
        vec![FeedId::new("dynamic-chainlink-usd")]
    );
    assert!(
        registered
            .current_handler_ids
            .contains(&HandlerId::new("evm-oracle-state.chainlink"))
    );

    let price = oracle
        .price("dynamic-chainlink-usd")
        .expect("dynamic feed should be readable");
    assert_eq!(price.raw_answer, I256::unchecked_from(321_i64));
    assert!(
        oracle
            .reactive_interests()
            .iter()
            .any(|interest| match interest {
                ReactiveInterest::Logs(interest) => interest.matches(&rpc_log(
                    aggregator,
                    vec![evm_oracle_state::ANSWER_UPDATED_TOPIC],
                    Vec::new(),
                    101,
                    0,
                )),
                _ => false,
            })
    );

    let removed = oracle
        .unregister_feed_by_proxy(proxy)
        .expect("seeded feed should unregister by proxy");
    assert_eq!(
        removed.removed_feed_ids,
        vec![FeedId::new("dynamic-chainlink-usd")]
    );
    assert!(oracle.price("dynamic-chainlink-usd").is_err());
}

impl OracleAdapterPlugin for CustomOracleAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        self.id.clone()
    }

    fn discover<'a>(
        &'a self,
        _ctx: OracleDiscoveryContext<'a>,
    ) -> AdapterFuture<'a, OracleDiscoveryReport> {
        Box::pin(async move {
            Ok(
                OracleDiscoveryReport::new().with_feed(OracleDiscoveredFeed::new(
                    self.registration(),
                    round(1, self.initial_answer, 1_700_000_000),
                )),
            )
        })
    }

    fn reactive_handler(
        &self,
        registrations: Vec<FeedRegistration>,
        _storage_sync: OracleStorageSync,
    ) -> Arc<dyn ReactiveHandler<Ethereum>> {
        Arc::new(CustomOracleReactiveHandler {
            event_source: self.event_source,
            registrations,
        })
    }
}

#[derive(Clone, Debug)]
struct SkippingOracleAdapter {
    id: OracleAdapterId,
    proxy: Address,
}

impl SkippingOracleAdapter {
    fn new(proxy: Address) -> Self {
        Self {
            id: OracleAdapterId::new("test.skipping-oracle"),
            proxy,
        }
    }
}

impl OracleAdapterPlugin for SkippingOracleAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        self.id.clone()
    }

    fn discover<'a>(
        &'a self,
        _ctx: OracleDiscoveryContext<'a>,
    ) -> AdapterFuture<'a, OracleDiscoveryReport> {
        Box::pin(async move {
            Ok(
                OracleDiscoveryReport::new().with_skip(OracleAdapterFeedSkip {
                    feed: Feed::proxy(self.proxy).id("skipped-custom").build(),
                    proxy: self.proxy,
                    reason: OracleAdapterSkipReason::NotChainlinkCompatible {
                        error: "synthetic unsupported oracle".to_string(),
                    },
                }),
            )
        })
    }

    fn reactive_handler(
        &self,
        registrations: Vec<FeedRegistration>,
        _storage_sync: OracleStorageSync,
    ) -> Arc<dyn ReactiveHandler<Ethereum>> {
        Arc::new(CustomOracleReactiveHandler {
            event_source: self.proxy,
            registrations,
        })
    }
}

#[derive(Clone, Debug)]
struct CustomOracleReactiveHandler {
    event_source: Address,
    registrations: Vec<FeedRegistration>,
}

impl ReactiveHandler<Ethereum> for CustomOracleReactiveHandler {
    fn id(&self) -> HandlerId {
        HandlerId::new("test.custom-oracle")
    }

    fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        vec![ReactiveInterest::Logs(LogInterest {
            provider_filter: Filter::new()
                .address(self.event_source)
                .event_signature(CUSTOM_TOPIC),
            local_matcher: None,
            route_key: Some(RouteKeySpec::EmitterAddress),
        })]
    }

    fn handle(
        &self,
        ctx: &evm_fork_cache::reactive::ReactiveContext,
        input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        if log.address() != self.event_source || log.topics().first().copied() != Some(CUSTOM_TOPIC)
        {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        }

        let answer = I256::unchecked_from(
            U256::try_from_be_slice(log.data().data.as_ref())
                .unwrap_or_default()
                .to::<u64>() as i128,
        );
        let block_number = log
            .block_number
            .or_else(|| ctx.block.as_ref().map(|b| b.number));
        let block_hash = log
            .block_hash
            .or_else(|| ctx.block.as_ref().map(|b| b.hash));
        let log_index = log.log_index.or(ctx.log_index);

        let effects = self
            .registrations
            .iter()
            .map(|registration| {
                ReactiveEffect::Hook(HookSignal {
                    namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
                    kind: Cow::Borrowed(OracleSignalKind::PriceUpdate.as_str()),
                    labels: vec![ReportTag::new("feed_id", registration.id.to_string())],
                    payload: Some(Arc::new(OraclePriceUpdate {
                        id: registration.id.clone(),
                        proxy: registration.proxy,
                        aggregator: self.event_source,
                        label: registration.label.clone(),
                        base: registration.base.clone(),
                        quote: registration.quote.clone(),
                        raw_answer: answer,
                        decimals: registration.metadata.decimals,
                        event_round_id: U256::from(2_u64),
                        started_at: 1_700_000_120,
                        updated_at: 1_700_000_120,
                        block_number,
                        block_hash,
                        log_index,
                        round_status: OracleRoundStatus::Fresh,
                        value_status: OracleValueStatus::EventPending,
                        source: OracleValueSource::Event,
                    })),
                })
            })
            .collect();

        Ok(HandlerOutcome {
            effects,
            quality: StateEffectQuality::RequiresRepair,
            tags: Vec::new(),
        })
    }
}

#[tokio::test]
async fn external_adapter_discovers_feed_and_installs_custom_reactive_handler() {
    let proxy = Address::repeat_byte(0x10);
    let event_source = Address::repeat_byte(0x20);
    let adapter = CustomOracleAdapter::new(proxy, event_source, I256::unchecked_from(123_i64));
    let mut cache = setup_cache().await;

    let runtime = OracleRuntime::cache_builder()
        .install_adapter(adapter)
        .now_timestamp(1_700_000_030)
        .build(&mut cache)
        .await
        .expect("external adapter should discover and register its feed");

    let price = runtime.price("custom-usd").expect("registered price");
    assert_eq!(price.raw_answer, I256::unchecked_from(123_i64));
    assert_eq!(price.base.as_deref(), Some("CUSTOM"));
    assert_eq!(price.quote.as_deref(), Some("USD"));

    let reactive = runtime
        .reactive_runtime()
        .expect("external adapter handler should install");
    assert_eq!(
        runtime.reactive_handlers().len(),
        2,
        "runtime should expose built-in plus adapter handlers"
    );
    let matching_log = rpc_log(event_source, vec![CUSTOM_TOPIC], Vec::new(), 101, 0);
    assert!(reactive.interests().iter().any(|interest| match interest {
        ReactiveInterest::Logs(interest) => interest.matches(&matching_log),
        _ => false,
    }));
}

#[tokio::test]
async fn runtime_installs_and_uninstalls_handlers_with_engine_owner_lifecycle() {
    let proxy = Address::repeat_byte(0x12);
    let event_source = Address::repeat_byte(0x22);
    let adapter = CustomOracleAdapter::new(proxy, event_source, I256::unchecked_from(123_i64));
    let mut cache = setup_cache().await;
    let runtime = OracleRuntime::cache_builder()
        .install_adapter(adapter)
        .now_timestamp(1_700_000_030)
        .build(&mut cache)
        .await
        .expect("external adapter should build runtime");
    let mut engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );

    let installed = runtime
        .install_handlers_live_only(&mut engine)
        .await
        .expect("oracle handlers should install");
    assert_eq!(installed.len(), 2);
    assert!(
        engine
            .runtime()
            .contains_handler(&HandlerId::new("evm-oracle-state.chainlink"))
    );
    assert!(
        engine
            .runtime()
            .contains_handler(&HandlerId::new("test.custom-oracle"))
    );
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("test.custom-oracle"))
            .is_some()
    );

    let removed = runtime
        .uninstall_handlers(&mut engine)
        .await
        .expect("uninstall handlers");
    assert_eq!(removed.removed_handler_ids.len(), 2);
    assert!(
        !engine
            .runtime()
            .contains_handler(&HandlerId::new("test.custom-oracle"))
    );
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("test.custom-oracle"))
            .is_none()
    );

    assert!(
        runtime
            .install_handlers_with_backfill(&mut engine, SubscriberBackfill::range(90, 100))
            .await
            .is_err()
    );
    assert!(engine.subscriber().owners.is_empty());
    engine
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(event_source, vec![CUSTOM_TOPIC], vec![], 100, 0)),
                context(100, 0),
            ),
        )
        .expect("retain canonical backfill anchor");
    let retained = context(100, 0).block.expect("block context");
    let backfill = SubscriberBackfill::from_canonical_block_through(retained, retained.number)
        .expect("exact retained backfill");
    runtime
        .install_handlers_with_backfill(&mut engine, backfill)
        .await
        .expect("oracle handlers should install with explicit backfill");
    assert_eq!(
        engine
            .subscriber()
            .backfills
            .get(&HandlerId::new("test.custom-oracle")),
        Some(&backfill)
    );
}

#[tokio::test]
async fn failed_uninstall_retains_outstanding_handler_ids_for_retry() {
    let adapter = CustomOracleAdapter::new(
        Address::repeat_byte(0x12),
        Address::repeat_byte(0x22),
        I256::unchecked_from(123_i64),
    );
    let mut cache = setup_cache().await;
    let runtime = OracleRuntime::cache_builder()
        .install_adapter(adapter)
        .now_timestamp(1_700_000_030)
        .build(&mut cache)
        .await
        .expect("runtime");
    let mut engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );
    let mut installed = runtime
        .install_handlers_live_only(&mut engine)
        .await
        .expect("install");
    assert_eq!(installed.len(), 2);
    let first = installed.handler_ids[0].clone();
    let rejected = installed.handler_ids[1].clone();
    engine.subscriber_mut().fail_removal = Some(rejected.clone());

    assert!(
        runtime
            .uninstall_installed_handlers(&mut engine, &mut installed)
            .await
            .is_err()
    );
    assert_eq!(installed.handler_ids, vec![rejected.clone()]);
    assert!(!engine.runtime().contains_handler(&first));
    assert!(engine.runtime().contains_handler(&rejected));
    assert!(engine.subscriber().owner_interests(&rejected).is_some());

    engine.subscriber_mut().fail_removal = None;
    let report = runtime
        .uninstall_installed_handlers(&mut engine, &mut installed)
        .await
        .expect("retry");
    assert_eq!(report.removed_handler_ids, vec![rejected.clone()]);
    assert!(installed.is_empty());
    assert!(!engine.runtime().contains_handler(&rejected));
    assert!(engine.subscriber().owner_interests(&rejected).is_none());
}

#[tokio::test]
async fn runtime_registers_adapter_on_demand_and_refreshes_live_engine() {
    let proxy = Address::repeat_byte(0x14);
    let event_source = Address::repeat_byte(0x24);
    let adapter = CustomOracleAdapter::new(proxy, event_source, I256::unchecked_from(100_i64));
    let mut cache = setup_cache().await;
    let mut oracle = OracleRuntime::cache_builder()
        .now_timestamp(1_700_000_030)
        .build(&mut cache)
        .await
        .expect("empty cache-native runtime should build");
    let mut engine = ReactiveEngine::new(
        ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default()),
        RecordingSubscriber::default(),
    );

    let mut installed = oracle
        .install_handlers_live_only(&mut engine)
        .await
        .expect("empty runtime should install built-in handler");
    assert_eq!(installed.len(), 1);
    assert!(
        !engine
            .runtime()
            .contains_handler(&HandlerId::new("test.custom-oracle"))
    );

    let mutation = oracle
        .register_adapter(adapter, &mut cache)
        .await
        .expect("adapter should register dynamically");
    assert_eq!(
        mutation.registered_feed_ids,
        vec![FeedId::new("custom-usd")]
    );
    assert!(
        mutation
            .current_handler_ids
            .contains(&HandlerId::new("test.custom-oracle"))
    );

    let refreshed = oracle
        .refresh_handlers_live_only(&mut engine, &mut installed)
        .await
        .expect("refresh should replace installed handlers");
    assert_eq!(
        refreshed.previous_handler_ids,
        vec![HandlerId::new("evm-oracle-state.chainlink")]
    );
    assert_eq!(installed.len(), 2);
    assert!(
        engine
            .runtime()
            .contains_handler(&HandlerId::new("test.custom-oracle"))
    );
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("test.custom-oracle"))
            .is_some()
    );

    let report = engine
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    event_source,
                    vec![CUSTOM_TOPIC],
                    U256::from(456_u64).to_be_bytes::<32>().to_vec(),
                    105,
                    7,
                )),
                context(105, 7),
            ),
        )
        .expect("dynamically registered adapter should receive events");
    let events = oracle
        .apply_batch_report(&report)
        .expect("dynamic adapter hook should update state");
    assert_eq!(events.events.len(), 1);
    assert_eq!(
        oracle
            .price("custom-usd")
            .expect("dynamic price")
            .raw_answer,
        I256::unchecked_from(456_i64)
    );

    let removed = oracle
        .unregister_feed_by_id(FeedId::new("custom-usd"))
        .expect("dynamic adapter feed should unregister");
    assert_eq!(removed.removed_feed_ids, vec![FeedId::new("custom-usd")]);
    let refreshed = oracle
        .refresh_handlers_live_only(&mut engine, &mut installed)
        .await
        .expect("refresh should remove empty adapter handler");
    assert!(
        refreshed
            .removed_handler_ids
            .contains(&HandlerId::new("test.custom-oracle"))
    );
    assert_eq!(installed.len(), 1);
    assert!(
        !engine
            .runtime()
            .contains_handler(&HandlerId::new("test.custom-oracle"))
    );
    assert!(
        engine
            .subscriber()
            .owner_interests(&HandlerId::new("test.custom-oracle"))
            .is_none()
    );

    let report = engine
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    event_source,
                    vec![CUSTOM_TOPIC],
                    U256::from(999_u64).to_be_bytes::<32>().to_vec(),
                    106,
                    8,
                )),
                context(106, 8),
            ),
        )
        .expect("removed adapter event should not be routed");
    let events = oracle
        .apply_batch_report(&report)
        .expect("removed adapter event should produce no oracle events");
    assert!(events.is_empty());
    assert!(oracle.price("custom-usd").is_err());
}

#[tokio::test]
async fn external_adapter_event_updates_tracker_state_through_standard_oracle_hook() {
    let proxy = Address::repeat_byte(0x11);
    let event_source = Address::repeat_byte(0x21);
    let adapter = CustomOracleAdapter::new(proxy, event_source, I256::unchecked_from(100_i64));
    let mut cache = setup_cache().await;
    let mut oracle = OracleRuntime::cache_builder()
        .install_adapter(adapter)
        .now_timestamp(1_700_000_030)
        .build(&mut cache)
        .await
        .expect("external adapter should build runtime");
    let mut reactive = oracle
        .reactive_runtime()
        .expect("external adapter handler should install");

    let report = reactive
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(rpc_log(
                    event_source,
                    vec![CUSTOM_TOPIC],
                    U256::from(456_u64).to_be_bytes::<32>().to_vec(),
                    105,
                    7,
                )),
                context(105, 7),
            ),
        )
        .expect("custom oracle event should commit");

    let events = oracle
        .apply_batch_report(&report)
        .expect("standard oracle hook should update tracker state");
    assert_eq!(events.events.len(), 1);

    let price = oracle.price("custom-usd").expect("updated price");
    assert_eq!(price.raw_answer, I256::unchecked_from(456_i64));
    assert!(price.is_event_pending());
    assert_eq!(price.updated_at, 1_700_000_120);
}

#[tokio::test]
async fn cache_runtime_builder_rejects_duplicate_feed_ids_across_external_adapters() {
    let mut cache = setup_cache().await;
    let first = CustomOracleAdapter::new(
        Address::repeat_byte(0x31),
        Address::repeat_byte(0x41),
        I256::unchecked_from(1_i64),
    );
    let second = CustomOracleAdapter::new(
        Address::repeat_byte(0x32),
        Address::repeat_byte(0x42),
        I256::unchecked_from(2_i64),
    );

    let error = OracleRuntime::cache_builder()
        .install_adapter(first)
        .install_adapter(second)
        .now_timestamp(1_700_000_030)
        .build(&mut cache)
        .await
        .expect_err("duplicate feed ids from external adapters should be rejected");

    assert!(
        error.to_string().contains("custom-usd"),
        "duplicate-feed error should identify the colliding feed id: {error}"
    );
}

#[tokio::test]
async fn cache_runtime_builder_exposes_and_fails_fast_on_adapter_skips() {
    let proxy = Address::repeat_byte(0x51);
    let mut cache = setup_cache().await;
    let report = OracleRuntime::cache_builder()
        .install_adapter(SkippingOracleAdapter::new(proxy))
        .now_timestamp(1_700_000_030)
        .build_report(&mut cache)
        .await
        .expect("build_report should surface adapter skips");

    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].proxy, proxy);
    assert!(
        report.runtime.price("skipped-custom").is_err(),
        "skipped feeds should not seed typed state"
    );

    let error = OracleRuntime::cache_builder()
        .install_adapter(SkippingOracleAdapter::new(proxy))
        .now_timestamp(1_700_000_030)
        .build(&mut cache)
        .await
        .expect_err("build should fail fast when adapters report skipped feeds");
    assert!(
        error.to_string().contains("skipped-custom")
            && error.to_string().contains("synthetic unsupported oracle"),
        "skip error should identify the skipped feed and reason: {error}"
    );
}

async fn setup_cache() -> EvmCache {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    EvmCache::new(Arc::new(provider)).await
}

fn metadata() -> FeedMetadata {
    FeedMetadata {
        decimals: 8,
        description: "CUSTOM / USD".to_string(),
        version: U256::ZERO,
    }
}

fn round(id: u64, answer: I256, updated_at: u64) -> RoundData {
    RoundData {
        round_id: U256::from(id),
        answer,
        started_at: updated_at,
        updated_at,
        answered_in_round: U256::from(id),
    }
}

fn rpc_log(
    address: Address,
    topics: Vec<B256>,
    data: Vec<u8>,
    block_number: u64,
    log_index: u64,
) -> RpcLog {
    RpcLog {
        inner: PrimitiveLog::new_unchecked(address, topics, Bytes::from(data)),
        block_hash: Some(block_hash(block_number)),
        block_number: Some(block_number),
        block_timestamp: Some(1_700_000_000 + block_number),
        transaction_hash: Some(B256::repeat_byte(0x77)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}

fn context(block_number: u64, log_index: u64) -> evm_fork_cache::reactive::ReactiveContext {
    let block = BlockRef {
        number: block_number,
        hash: block_hash(block_number),
        parent_hash: Some(block_hash(block_number.saturating_sub(1))),
        timestamp: Some(1_700_000_000 + block_number),
    };
    evm_fork_cache::reactive::ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Synthetic,
        chain_status: ChainStatus::Included {
            block,
            confirmations: 0,
        },
        block: Some(block),
        transaction_index: Some(0),
        log_index: Some(log_index),
    }
}

fn batch(
    input: ReactiveInput<Ethereum>,
    context: evm_fork_cache::reactive::ReactiveContext,
) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, context)])
}

#[derive(Default)]
struct RecordingSubscriber {
    base_interests: Vec<ReactiveInterest<Ethereum>>,
    owners: BTreeMap<HandlerId, Vec<ReactiveInterest<Ethereum>>>,
    backfills: BTreeMap<HandlerId, SubscriberBackfill>,
    fail_removal: Option<HandlerId>,
}

impl EventSubscriber<Ethereum> for RecordingSubscriber {
    fn register_interests(
        &mut self,
        interests: &[ReactiveInterest<Ethereum>],
    ) -> evm_fork_cache::reactive::SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.base_interests = interests;
            self.owners.clear();
            self.backfills.clear();
            Ok(())
        })
    }

    fn next_batch(&mut self) -> SubscriberNextBatch<'_, Ethereum> {
        Box::pin(async { Ok(None) })
    }
}

impl InterestOwnerSubscriber<Ethereum> for RecordingSubscriber {
    fn add_interest_owner(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
    ) -> evm_fork_cache::reactive::SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.owners.insert(owner, interests);
            Ok(())
        })
    }

    fn add_interest_owner_with_backfill(
        &mut self,
        owner: HandlerId,
        interests: &[ReactiveInterest<Ethereum>],
        backfill: SubscriberBackfill,
    ) -> evm_fork_cache::reactive::SubscriberOperation<'_, ()> {
        let interests = interests.to_vec();
        Box::pin(async move {
            self.backfills.insert(owner.clone(), backfill);
            self.owners.insert(owner, interests);
            Ok(())
        })
    }

    fn remove_interest_owner(
        &mut self,
        owner: &HandlerId,
    ) -> evm_fork_cache::reactive::SubscriberOperation<'_, Option<Vec<ReactiveInterest<Ethereum>>>>
    {
        let owner = owner.clone();
        Box::pin(async move {
            if self.fail_removal.as_ref() == Some(&owner) {
                return Err(SubscriberError::Unsupported("test removal rejected"));
            }
            self.backfills.remove(&owner);
            Ok(self.owners.remove(&owner))
        })
    }

    fn owner_interests(&self, owner: &HandlerId) -> Option<&[ReactiveInterest<Ethereum>]> {
        self.owners.get(owner).map(Vec::as_slice)
    }
}
