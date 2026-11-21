use std::{borrow::Cow, future::Future, pin::Pin, sync::Arc};

use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use evm_fork_cache::{
    StateUpdate, StateView,
    reactive::{
        BlockRef, ChainStatus, HookSignal, InputSource, ReactiveContext, ReactiveEffect,
        ReactiveInput, ReportTag, StateEffectQuality,
    },
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, ChainlinkFeed, ChainlinkFeedProvider, FeedId, FeedMetadata,
    FeedRegistration, FeedSource, ORACLE_SIGNAL_NAMESPACE, OracleCodeMismatch,
    OracleCodeUnverifiable, OracleCodeWarmupPolicy, OracleCodeWarmupReport, OracleError,
    OracleFeedStatus, OracleHookEvent, OraclePriceUpdate, OracleReactiveHandler, OracleRoundStatus,
    OracleRuntime, OracleSignal, OracleSignalKind, OracleStorageAdapter, OracleStorageError,
    OracleStorageSync, OracleTracker, OracleValueSource, OracleValueStatus, RoundData,
    StalenessPolicy,
};

type ProviderFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, OracleError>> + Send + 'a>>;

#[derive(Clone, Debug)]
struct MockProvider {
    proxy: Address,
    aggregator: Address,
    metadata: FeedMetadata,
    round: RoundData,
}

impl ChainlinkFeedProvider for MockProvider {
    fn decimals(&self, proxy: Address) -> ProviderFuture<'_, u8> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(self.metadata.decimals) })
    }

    fn description(&self, proxy: Address) -> ProviderFuture<'_, String> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(self.metadata.description.clone()) })
    }

    fn version(&self, proxy: Address) -> ProviderFuture<'_, U256> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(self.metadata.version) })
    }

    fn latest_round_data(&self, proxy: Address) -> ProviderFuture<'_, RoundData> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(self.round.clone()) })
    }

    fn aggregator(&self, proxy: Address) -> ProviderFuture<'_, Option<Address>> {
        assert_eq!(proxy, self.proxy);
        Box::pin(async move { Ok(Some(self.aggregator)) })
    }
}

#[derive(Debug)]
struct FixedStorageAdapter;

impl OracleStorageAdapter for FixedStorageAdapter {
    fn name(&self) -> &'static str {
        "fixed-test"
    }

    fn warm_slots_for_registration(&self, registration: &FeedRegistration) -> Vec<(Address, U256)> {
        vec![(registration.proxy, U256::from(7_u64))]
    }

    fn state_updates_for_answer(
        &self,
        registration: &FeedRegistration,
        _update: &OraclePriceUpdate,
        _state: &dyn StateView,
    ) -> Result<Option<Vec<StateUpdate>>, OracleStorageError> {
        Ok(Some(vec![StateUpdate::slot(
            registration.proxy,
            U256::from(7_u64),
            U256::from(42_u64),
        )]))
    }
}

#[test]
fn tracker_runtime_retains_custom_storage_sync() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration(proxy, aggregator),
            round(100, 2_000_000_000, 1_700_000_000),
        )],
        1_700_000_100,
    )
    .expect("seed tracker");
    let mut storage_sync = OracleStorageSync::chainlink_defaults();
    storage_sync.push_adapter(Arc::new(FixedStorageAdapter));

    let runtime = OracleRuntime::from_tracker(tracker).with_storage_sync(storage_sync);

    assert!(
        runtime
            .storage_warmup_slots()
            .contains(&(proxy, U256::from(7_u64)))
    );
}

fn metadata() -> FeedMetadata {
    FeedMetadata {
        decimals: 8,
        description: "ETH / USD".to_string(),
        version: U256::from(4_u64),
    }
}

fn round(id: u64, answer: i64, updated_at: u64) -> RoundData {
    RoundData {
        round_id: U256::from(id),
        answer: I256::unchecked_from(answer),
        started_at: updated_at.saturating_sub(60),
        updated_at,
        answered_in_round: U256::from(id),
    }
}

fn registration(proxy: Address, aggregator: Address) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("eth-usd"),
        proxy,
        label: Some("ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(aggregator),
        aggregator_layout: None,
        metadata: metadata(),
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
    }
}

fn uint_topic(value: U256) -> B256 {
    B256::from(value.to_be_bytes::<32>())
}

fn int_topic(value: I256) -> B256 {
    B256::from(value.into_raw().to_be_bytes::<32>())
}

fn word(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}

fn context(block_number: u64, log_index: u64) -> ReactiveContext {
    let block = BlockRef {
        number: block_number,
        hash: block_hash(block_number),
        parent_hash: None,
        timestamp: Some(1_700_000_000 + block_number),
    };
    ReactiveContext {
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

#[test]
fn typed_signal_decodes_hook_without_string_matching() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let update = OraclePriceUpdate {
        id: FeedId::new("eth-usd"),
        proxy,
        aggregator,
        label: Some("ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        raw_answer: I256::unchecked_from(2_000_000_000_i64),
        decimals: 8,
        event_round_id: U256::from(101_u64),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        block_number: Some(100),
        block_hash: Some(block_hash(100)),
        log_index: Some(3),
        round_status: OracleRoundStatus::Fresh,
        value_status: OracleValueStatus::EventPending,
        source: OracleValueSource::Event,
    };
    let hook = HookSignal {
        namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
        kind: Cow::Borrowed(OracleSignalKind::PriceUpdate.as_str()),
        labels: vec![ReportTag::new("feed_id", "eth-usd")],
        payload: Some(Arc::new(update.clone())),
    };

    let signal = OracleSignal::from_hook(&hook)
        .expect("typed decode should not fail")
        .expect("oracle hook should decode");
    assert!(matches!(signal, OracleSignal::PriceUpdate(decoded) if decoded == &update));
    assert_eq!(
        signal.to_event(),
        OracleHookEvent::PriceUpdate(update.clone())
    );
    assert_eq!(
        OracleHookEvent::PriceUpdate(update).kind_str(),
        OracleSignalKind::PriceUpdate.as_str()
    );
}

#[test]
fn tracker_price_facade_returns_actionable_scaled_prices() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration(proxy, aggregator),
            round(100, 2_000_000_000, 1_700_000_000),
        )],
        1_700_000_100,
    )
    .expect("seed tracker");

    let price = tracker.price("eth-usd").expect("price by id");
    assert_eq!(price.raw_answer, I256::unchecked_from(2_000_000_000_i64));
    assert_eq!(price.decimals, 8);
    assert!(price.is_actionable());
    assert!(price.is_confirmed());
    assert_eq!(price.round_status, OracleRoundStatus::Fresh);
    assert_eq!(price.value_status, OracleValueStatus::Confirmed);
    assert_eq!(price.source, OracleValueSource::Proxy);
    assert_eq!(
        price.scaled_to(18).expect("scale to wad"),
        I256::unchecked_from(20_000_000_000_000_000_000_i128)
    );
    assert_eq!(
        tracker.price_by_proxy(proxy).expect("price by proxy"),
        price
    );
}

#[tokio::test]
async fn runtime_builder_registers_feeds_and_builds_reactive_handler() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let provider = MockProvider {
        proxy,
        aggregator,
        metadata: metadata(),
        round: round(100, 2_000_000_000, 1_700_000_000),
    };

    let runtime = OracleRuntime::builder(provider)
        .now_timestamp(1_700_000_100)
        .feed(
            ChainlinkFeed::new(proxy)
                .id("eth-usd")
                .label("ETH/USD")
                .base("ETH")
                .quote("USD")
                .max_age_secs(3_600),
        )
        .build()
        .await
        .expect("runtime build");

    assert_eq!(
        runtime.price("eth-usd").unwrap().proxy,
        Address::repeat_byte(0x10)
    );
    assert_eq!(runtime.reactive_handler().interests().len(), 1);
}

#[test]
fn storage_adapter_path_emits_state_updates_without_purge_fallback() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::with_storage_sync(
        vec![registration(proxy, aggregator)],
        OracleStorageSync::new().with_adapter(Arc::new(FixedStorageAdapter)),
    );
    let log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(2_100_000_000_i64)),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_100_u64)),
        120,
        0,
    );

    let outcome = handler
        .handle_for_test(&context(120, 0), &ReactiveInput::<Ethereum>::Log(log))
        .expect("handler outcome");

    assert!(outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::StateUpdate(StateUpdate::Slot {
                address,
                slot,
                value
            }) if *address == proxy && *slot == U256::from(7_u64) && *value == U256::from(42_u64)
        )
    }));
    assert!(
        !outcome
            .effects
            .iter()
            .any(|effect| matches!(effect, ReactiveEffect::Invalidate(_))),
        "supported storage adapters should bypass purge fallback"
    );
    assert_eq!(outcome.quality, StateEffectQuality::ExactFromInput);
    let update = outcome
        .effects
        .iter()
        .find_map(|effect| match effect {
            ReactiveEffect::Hook(signal)
                if signal.kind.as_ref() == OracleSignalKind::PriceUpdate.as_str() =>
            {
                signal.payload.as_ref()?.downcast_ref::<OraclePriceUpdate>()
            }
            _ => None,
        })
        .expect("price update hook");
    assert_eq!(update.round_status, OracleRoundStatus::Fresh);
    assert_eq!(update.value_status, OracleValueStatus::EventPending);
    assert_eq!(update.source, OracleValueSource::Event);
}

#[test]
fn code_warmup_policy_distinguishes_contradictions_from_unverifiable_claims() {
    let report = OracleCodeWarmupReport {
        mismatched: vec![OracleCodeMismatch {
            address: Address::repeat_byte(0x41),
            expected: B256::repeat_byte(0x01),
            actual: B256::repeat_byte(0x02),
        }],
        unverifiable: vec![OracleCodeUnverifiable {
            address: Address::repeat_byte(0x42),
            reason: "transient provider failure".to_string(),
        }],
        ..Default::default()
    };

    assert!(
        report
            .enforce_policy(OracleCodeWarmupPolicy::best_effort())
            .is_ok(),
        "best effort should only record failures"
    );
    assert!(
        report
            .enforce_policy(OracleCodeWarmupPolicy::allow_unverifiable())
            .is_err(),
        "allow_unverifiable should still fail on code hash contradictions"
    );

    let transient_only = OracleCodeWarmupReport {
        unverifiable: report.unverifiable,
        ..Default::default()
    };
    assert!(
        transient_only
            .enforce_policy(OracleCodeWarmupPolicy::allow_unverifiable())
            .is_ok(),
        "default policy should allow unsettled verification without contradictions"
    );
    assert!(
        transient_only
            .enforce_policy(OracleCodeWarmupPolicy::strict())
            .is_err(),
        "strict policy should fail on unsettled verification"
    );
}
