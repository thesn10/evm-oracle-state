//! Core-internals hardening coverage: the error taxonomy (non-provider
//! failures no longer masquerade as `OracleError::Provider`), string-id
//! lookup consistency across register/unregister, the per-key legacy-signal
//! dedup in `apply_batch_report`, and `register_adapter` rollback.

use std::{borrow::Cow, marker::PhantomData, sync::Arc};

use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, I256, U256};
use evm_oracle_state::alloy_provider::{RootProvider, network::AnyNetwork};
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::{
    cache::EvmCache,
    reactive::{
        AppliedReport, HandlerId, HookSignal, InputRef, ReactiveBatchReport, ReactiveHandler,
        StateEffectQuality,
    },
    state_update::StateDiff,
};
use evm_oracle_state::{
    AdapterFuture, Denomination, Feed, FeedId, FeedMetadata, FeedRegistration, FeedSource,
    ORACLE_LEGACY_ANSWER_UPDATED_KIND, ORACLE_SIGNAL_NAMESPACE, OracleAdapter, OracleAdapterId,
    OracleAdapterPlugin, OracleConfigError, OracleDiscoveredFeed, OracleDiscoveryContext,
    OracleDiscoveryReport, OracleError, OracleFeedStatus, OraclePriceUpdate, OracleReactiveHandler,
    OracleRoundStatus, OracleRuntime, OracleSignalKind, OracleStorageSync, OracleTracker,
    OracleUpdate, OracleValueSource, OracleValueStatus, PricePolicy, PricePolicyViolation,
    RoundData, StalenessPolicy, ValuedAmount,
};

fn chainlink_registration(id: &str, proxy: Address, aggregator: Address) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new(id),
        proxy,
        label: Some(format!("{id} label")),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(aggregator),
        aggregator_layout: None,
        metadata: FeedMetadata {
            decimals: 8,
            description: id.to_string(),
            version: U256::from(1_u8),
        },
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
    }
}

fn round(id: u64, answer: i64, updated_at: u64) -> RoundData {
    RoundData {
        round_id: U256::from(id),
        answer: I256::unchecked_from(answer),
        started_at: updated_at,
        updated_at,
        answered_in_round: U256::from(id),
    }
}

fn tracker_with(feeds: Vec<(FeedRegistration, RoundData)>, now_timestamp: u64) -> OracleTracker {
    OracleTracker::from_registrations_at_timestamp(feeds, now_timestamp).expect("seed tracker")
}

async fn mocked_cache() -> EvmCache {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    EvmCache::new(Arc::new(provider)).await
}

// --- Task 1: error taxonomy ------------------------------------------------

#[test]
fn policy_rejection_is_policy_not_provider() {
    let proxy = Address::repeat_byte(0x11);
    let aggregator = Address::repeat_byte(0x21);
    // Round is one day older than the tracker clock with a 1h max age.
    let tracker = tracker_with(
        vec![(
            chainlink_registration("eth-usd", proxy, aggregator),
            round(1, 200_000_000_000, 1_700_000_000),
        )],
        1_700_086_400,
    );

    let price = tracker.price("eth-usd").expect("registered price");
    assert_eq!(
        price.round_status,
        OracleRoundStatus::Stale {
            age_secs: 86_400,
            max_age_secs: 3_600
        }
    );
    let error = price
        .require(PricePolicy::liquidation().quote(Denomination::Usd))
        .expect_err("stale price must fail liquidation policy");
    assert!(
        matches!(error, OracleError::Policy(_)),
        "expected Policy, got {error:?}"
    );
}

#[test]
fn negative_valuation_is_policy_not_provider() {
    let error = ValuedAmount::checked_new(Denomination::Usd, I256::unchecked_from(-1_i64), 8)
        .expect_err("negative valued amount must be rejected");
    assert!(
        matches!(error, OracleError::Policy(_)),
        "expected Policy, got {error:?}"
    );
}

#[test]
fn missing_adapter_is_config_not_provider() {
    let mut runtime = OracleRuntime::from_tracker(tracker_with(Vec::new(), 1_700_000_000));
    let registration = chainlink_registration(
        "late-feed",
        Address::repeat_byte(0x31),
        Address::repeat_byte(0x41),
    );
    let error = runtime
        .register_adapter_seeded_feed("never-installed", registration, round(1, 1, 1_700_000_000))
        .expect_err("uninstalled adapter must be rejected");
    assert!(
        matches!(error, OracleError::Config(_)),
        "expected Config, got {error:?}"
    );
}

#[test]
fn duplicate_seeded_feed_is_duplicate_error_not_provider() {
    let proxy = Address::repeat_byte(0x12);
    let aggregator = Address::repeat_byte(0x22);
    let mut runtime = OracleRuntime::from_tracker(tracker_with(
        vec![(
            chainlink_registration("eth-usd", proxy, aggregator),
            round(1, 1, 1_700_000_000),
        )],
        1_700_000_000,
    ));

    let error = runtime
        .register_seeded_feed(
            chainlink_registration("eth-usd", Address::repeat_byte(0x13), aggregator),
            round(1, 1, 1_700_000_000),
        )
        .expect_err("duplicate feed id must be rejected");
    assert!(
        matches!(error, OracleError::DuplicateFeedId(_)),
        "expected DuplicateFeedId, got {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn adapter_builder_fail_fast_error_is_feed_skipped() {
    let mut cache = mocked_cache().await;
    let error = OracleAdapter::builder()
        .feed(
            Feed::proxy(Address::repeat_byte(0x77))
                .label("Broken/USD")
                .max_age_secs(3_600),
        )
        .build(&mut cache)
        .await
        .expect_err("unreadable proxy must fail fast");
    assert!(
        matches!(error, OracleError::FeedSkipped(_)),
        "expected FeedSkipped, got {error:?}"
    );
    let message = error.to_string();
    assert!(
        message.contains("was skipped:"),
        "skip message should describe the skip, got: {message}"
    );
    assert!(
        !message.contains("provider read failed"),
        "skip message must not claim a provider read failure, got: {message}"
    );
}

#[test]
fn oracle_error_is_cloneable_and_comparable() {
    let error = OracleError::Policy(PricePolicyViolation::Stale {
        age_secs: 100,
        max_age_secs: 60,
    });
    assert_eq!(error.clone(), error);
    assert_ne!(
        error,
        OracleError::Config(OracleConfigError::Other("price is stale".to_string()))
    );
    assert_eq!(OracleError::FeedNotFound, OracleError::FeedNotFound);
}

// --- Task 2: string-id lookups across register/unregister -------------------

#[test]
fn string_id_lookups_stay_correct_across_register_and_unregister() {
    let aggregator = Address::repeat_byte(0x20);
    let first_proxy = Address::repeat_byte(0x51);
    let second_proxy = Address::repeat_byte(0x52);
    let mut tracker = tracker_with(
        vec![(
            chainlink_registration("btc-usd", first_proxy, aggregator),
            round(7, 42, 1_700_000_000),
        )],
        1_700_000_100,
    );

    assert_eq!(
        tracker.price("btc-usd").expect("registered id").proxy,
        first_proxy
    );

    let removed = tracker
        .remove_by_id(FeedId::new("btc-usd"))
        .expect("feed removes by id");
    assert_eq!(removed.proxy, first_proxy);
    assert!(
        matches!(tracker.price("btc-usd"), Err(OracleError::FeedNotFound)),
        "removed id must not resolve"
    );
    assert!(matches!(
        tracker.price_by_proxy(first_proxy),
        Err(OracleError::FeedNotFound)
    ));

    tracker
        .insert_seeded_registration(
            chainlink_registration("btc-usd", second_proxy, aggregator),
            round(8, 43, 1_700_000_050),
        )
        .expect("same id re-registers under a new proxy");
    let price = tracker.price("btc-usd").expect("re-registered id resolves");
    assert_eq!(price.proxy, second_proxy);
    assert_eq!(price.raw_answer, I256::unchecked_from(43_i64));
    assert!(
        matches!(
            tracker.price_by_proxy(first_proxy),
            Err(OracleError::FeedNotFound)
        ),
        "old proxy mapping must not linger"
    );
    assert_eq!(
        tracker
            .price_by_proxy(second_proxy)
            .expect("new proxy resolves")
            .id,
        FeedId::new("btc-usd")
    );
}

// --- Task 4.2: per-key legacy-signal dedup ----------------------------------

fn synthetic_batch(signals: Vec<HookSignal>) -> ReactiveBatchReport<Ethereum> {
    ReactiveBatchReport {
        applied: vec![AppliedReport {
            input_ref: InputRef::Block {
                chain_id: Some(1),
                hash: B256::repeat_byte(0xbb),
                number: 1,
            },
            handler_id: HandlerId::new("test.synthetic"),
            quality: StateEffectQuality::NoStateEffect,
            tags: Vec::new(),
            diff: StateDiff::default(),
            state_updates: Vec::new(),
            invalidations: Vec::new(),
            resyncs: Vec::new(),
            speculative: Vec::new(),
            hook_signals: signals,
            _network: PhantomData,
        }],
        resyncs: Vec::new(),
        speculative: Vec::new(),
        reports: Vec::new(),
    }
}

fn legacy_signal(registration: &FeedRegistration, round: RoundData) -> HookSignal {
    HookSignal {
        namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
        kind: Cow::Borrowed(ORACLE_LEGACY_ANSWER_UPDATED_KIND),
        labels: Vec::new(),
        payload: Some(Arc::new(OracleUpdate {
            id: registration.id.clone(),
            proxy: registration.proxy,
            aggregator: registration.current_aggregator.expect("test aggregator"),
            round,
            block_number: Some(1),
            log_index: Some(0),
            value_status: OracleValueStatus::EventPending,
        })),
    }
}

fn rich_signal(
    registration: &FeedRegistration,
    round: RoundData,
    block_hash: Option<B256>,
) -> HookSignal {
    HookSignal {
        namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
        kind: Cow::Borrowed(OracleSignalKind::PriceUpdate.as_str()),
        labels: Vec::new(),
        payload: Some(Arc::new(OraclePriceUpdate {
            id: registration.id.clone(),
            proxy: registration.proxy,
            aggregator: registration.current_aggregator.expect("test aggregator"),
            label: registration.label.clone(),
            base: registration.base.clone(),
            quote: registration.quote.clone(),
            raw_answer: round.answer,
            decimals: registration.metadata.decimals,
            event_round_id: round.round_id,
            started_at: round.started_at,
            updated_at: round.updated_at,
            block_number: Some(1),
            block_hash,
            log_index: Some(1),
            round_status: OracleRoundStatus::Fresh,
            value_status: OracleValueStatus::EventPending,
            source: OracleValueSource::Event,
        })),
    }
}

#[test]
fn legacy_only_signal_applies_alongside_unrelated_rich_update() {
    let aggregator_a = Address::repeat_byte(0xa1);
    let aggregator_b = Address::repeat_byte(0xb1);
    let registration_a = chainlink_registration("feed-a", Address::repeat_byte(0xa0), aggregator_a);
    let registration_b = chainlink_registration("feed-b", Address::repeat_byte(0xb0), aggregator_b);
    let mut tracker = tracker_with(
        vec![
            (registration_a.clone(), round(1, 100, 1_700_000_000)),
            (registration_b.clone(), round(1, 200, 1_700_000_000)),
        ],
        1_700_000_100,
    );

    // A custom handler emitted a legacy-only signal for feed A while the
    // built-in handler emitted a rich update for the unrelated feed B in the
    // same batch. The legacy update must still apply.
    let report = synthetic_batch(vec![
        legacy_signal(&registration_a, round(2, 111, 1_700_000_050)),
        rich_signal(&registration_b, round(2, 222, 1_700_000_060), None),
    ]);
    tracker
        .apply_batch_report(&report)
        .expect("batch should apply");

    let price_a = tracker.price("feed-a").expect("feed A price");
    assert_eq!(
        price_a.raw_answer,
        I256::unchecked_from(111_i64),
        "legacy-only update for feed A must not be dropped by feed B's rich update"
    );
    assert_eq!(price_a.source, OracleValueSource::Event);
    let price_b = tracker.price("feed-b").expect("feed B price");
    assert_eq!(price_b.raw_answer, I256::unchecked_from(222_i64));
}

#[test]
fn legacy_duplicate_of_same_key_rich_update_is_skipped() {
    let aggregator = Address::repeat_byte(0xa2);
    let registration = chainlink_registration("feed-c", Address::repeat_byte(0xc0), aggregator);
    let mut tracker = tracker_with(
        vec![(registration.clone(), round(1, 100, 1_700_000_000))],
        1_700_000_100,
    );

    // Built-in handlers emit the same event as a rich update plus a legacy
    // compatibility signal. The legacy duplicate for the same proxy and round
    // must be skipped so the rich snapshot (which carries the block hash) is
    // not overwritten by the hash-less legacy variant.
    let block_hash = B256::repeat_byte(0xcc);
    let event_round = round(2, 123, 1_700_000_050);
    let report = synthetic_batch(vec![
        rich_signal(&registration, event_round.clone(), Some(block_hash)),
        legacy_signal(&registration, event_round),
    ]);
    tracker
        .apply_batch_report(&report)
        .expect("batch should apply");

    assert_eq!(
        tracker.price("feed-c").expect("feed C price").raw_answer,
        I256::unchecked_from(123_i64)
    );
    let pending = tracker.pending_reconciliations();
    assert_eq!(pending.len(), 1, "one reconciliation per proxy");
    assert_eq!(
        pending[0].block_hash,
        Some(block_hash),
        "rich update's block hash must survive; the legacy duplicate would have erased it"
    );
}

// --- Task 2.3: register_adapter rollback ------------------------------------

#[derive(Debug)]
struct CollidingAdapter {
    fresh: FeedRegistration,
    colliding: FeedRegistration,
}

impl OracleAdapterPlugin for CollidingAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        OracleAdapterId::new("test.colliding-adapter")
    }

    fn discover<'a>(
        &'a self,
        _ctx: OracleDiscoveryContext<'a>,
    ) -> AdapterFuture<'a, OracleDiscoveryReport> {
        Box::pin(async move {
            Ok(OracleDiscoveryReport::new()
                .with_feed(OracleDiscoveredFeed::new(
                    self.fresh.clone(),
                    round(1, 10, 1_700_000_000),
                ))
                .with_feed(OracleDiscoveredFeed::new(
                    self.colliding.clone(),
                    round(1, 20, 1_700_000_000),
                )))
        })
    }

    fn reactive_handler(
        &self,
        registrations: Vec<FeedRegistration>,
        storage_sync: OracleStorageSync,
    ) -> Arc<dyn ReactiveHandler<Ethereum>> {
        Arc::new(OracleReactiveHandler::with_storage_sync(
            registrations,
            storage_sync,
        ))
    }
}

#[tokio::test]
async fn register_adapter_rolls_back_partial_insert_on_error() {
    let aggregator = Address::repeat_byte(0x61);
    let existing_proxy = Address::repeat_byte(0x62);
    let existing = chainlink_registration("existing-feed", existing_proxy, aggregator);
    let mut runtime = OracleRuntime::from_tracker(tracker_with(
        vec![(existing.clone(), round(1, 1, 1_700_000_000))],
        1_700_000_000,
    ));
    let mut cache = mocked_cache().await;

    // The adapter discovers one fresh feed and then one that collides with an
    // already-registered proxy: the whole call must fail and undo the fresh
    // feed's insertion.
    let adapter = CollidingAdapter {
        fresh: chainlink_registration("fresh-feed", Address::repeat_byte(0x63), aggregator),
        colliding: chainlink_registration("colliding-feed", existing_proxy, aggregator),
    };
    let error = runtime
        .register_adapter(adapter, &mut cache)
        .await
        .expect_err("colliding discovery must fail");
    assert!(
        matches!(error, OracleError::DuplicateProxy(proxy) if proxy == existing_proxy),
        "expected DuplicateProxy"
    );

    assert!(
        matches!(runtime.price("fresh-feed"), Err(OracleError::FeedNotFound)),
        "partially inserted feed must be rolled back"
    );
    assert!(
        runtime.price("existing-feed").is_ok(),
        "pre-existing feed must survive the rollback"
    );
    assert_eq!(
        runtime.tracker().registrations_iter().count(),
        1,
        "tracker must be exactly as before the failed call"
    );
}
