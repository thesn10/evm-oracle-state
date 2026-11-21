use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_provider::{RootProvider, network::AnyNetwork};
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use alloy_sol_types::{SolCall, sol};
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::reactive::{
    BlockRef, ChainStatus, InputSource, ReactiveConfig, ReactiveInput, ReactiveInputBatch,
    ReactiveInputRecord, ReactiveRuntime,
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, ChainlinkFeedProvider, FeedId, FeedMetadata, FeedRegistration,
    FeedSource, OracleFeedStatus, OracleHookEvent, OraclePriceUpdate, OracleReactiveHandler,
    OracleReadOverlay, OracleReconciler, OracleRoundStatus, OracleTracker, OracleValueSource,
    OracleValueStatus, RoundData, StalenessPolicy,
};

type ProviderFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, evm_oracle_state::OracleError>> + Send + 'a>>;

sol! {
    interface AggregatorV3Interface {
        function latestRoundData() external view returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        );
        function decimals() external view returns (uint8);
        function description() external view returns (string);
        function version() external view returns (uint256);
    }
}

use AggregatorV3Interface::{decimalsCall, descriptionCall, latestRoundDataCall, versionCall};

#[derive(Clone, Debug)]
struct MutableProvider {
    proxy: Address,
    aggregator: Address,
    metadata: FeedMetadata,
    round: RoundData,
}

impl ChainlinkFeedProvider for MutableProvider {
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

fn metadata() -> FeedMetadata {
    FeedMetadata {
        decimals: 8,
        description: "ETH / USD".to_string(),
        version: U256::from(4_u64),
    }
}

fn registration(proxy: Address, aggregator: Address) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("eth-usd"),
        proxy,
        label: Some("ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(60 * 60),
        current_aggregator: Some(aggregator),
        aggregator_layout: None,
        metadata: metadata(),
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
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

fn word(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn uint_topic(value: U256) -> B256 {
    B256::from(value.to_be_bytes::<32>())
}

fn int_topic(value: I256) -> B256 {
    B256::from(value.into_raw().to_be_bytes::<32>())
}

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}

fn block_ref(block_number: u64) -> BlockRef {
    BlockRef {
        number: block_number,
        hash: block_hash(block_number),
        parent_hash: Some(block_hash(block_number.saturating_sub(1))),
        timestamp: Some(1_700_000_000 + block_number),
    }
}

fn context(block_number: u64, log_index: u64) -> evm_fork_cache::reactive::ReactiveContext {
    let block = block_ref(block_number);
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

fn reorged_context(block_number: u64, log_index: u64) -> evm_fork_cache::reactive::ReactiveContext {
    let block = block_ref(block_number);
    evm_fork_cache::reactive::ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Synthetic,
        chain_status: ChainStatus::Reorged {
            dropped_from: block,
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
        transaction_hash: Some(B256::repeat_byte(0x66)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn batch(
    input: ReactiveInput<Ethereum>,
    context: evm_fork_cache::reactive::ReactiveContext,
) -> ReactiveInputBatch<Ethereum> {
    ReactiveInputBatch::new(vec![ReactiveInputRecord::new(input, context)])
}

async fn setup_cache() -> EvmCache {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    EvmCache::new(Arc::new(provider)).await
}

struct AnswerUpdateInput {
    proxy: Address,
    aggregator: Address,
    answer: i64,
    round_id: u64,
    updated_at: u64,
    block_number: u64,
    log_index: u64,
}

async fn apply_answer_update(
    tracker: &mut OracleTracker,
    event: AnswerUpdateInput,
) -> evm_fork_cache::reactive::ReactiveBatchReport<Ethereum> {
    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;
    let log = rpc_log(
        event.aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(event.answer)),
            uint_topic(U256::from(event.round_id)),
        ],
        word(U256::from(event.updated_at)),
        event.block_number,
        event.log_index,
    );

    let report = runtime
        .ingest_batch(
            &mut cache,
            batch(
                ReactiveInput::Log(log),
                context(event.block_number, event.log_index),
            ),
        )
        .expect("runtime should commit oracle event");
    tracker
        .apply_batch_report(&report)
        .expect("committed report should update typed oracle state");

    assert!(
        tracker.latest(event.proxy).is_some(),
        "test helper must leave the tracked proxy present"
    );
    report
}

fn seeded_tracker(proxy: Address, aggregator: Address, now_timestamp: u64) -> OracleTracker {
    OracleTracker::from_registrations_at_timestamp(
        vec![(
            registration(proxy, aggregator),
            round(100, 2_000_000_000, 1_700_000_000),
        )],
        now_timestamp,
    )
    .expect("seed tracker")
}

fn find_price_update(
    report: &evm_fork_cache::reactive::ReactiveBatchReport<Ethereum>,
) -> &OraclePriceUpdate {
    report
        .applied
        .iter()
        .flat_map(|applied| applied.hook_signals.iter())
        .find_map(|signal| {
            if signal.namespace.as_ref() == "evm-oracle-state"
                && signal.kind.as_ref() == "oracle.price_update"
            {
                signal.payload.as_ref()?.downcast_ref::<OraclePriceUpdate>()
            } else {
                None
            }
        })
        .expect("committed event should expose a typed OraclePriceUpdate hook payload")
}

#[tokio::test]
async fn price_update_hook_is_immediate_metadata_rich_and_queues_reconciliation() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let mut tracker = seeded_tracker(proxy, aggregator, 1_700_000_500);

    let report = apply_answer_update(
        &mut tracker,
        AnswerUpdateInput {
            proxy,
            aggregator,
            answer: 2_150_000_000,
            round_id: 101,
            updated_at: 1_700_000_430,
            block_number: 140,
            log_index: 7,
        },
    )
    .await;

    let update = find_price_update(&report);
    assert_eq!(update.id.as_str(), "eth-usd");
    assert_eq!(update.proxy, proxy);
    assert_eq!(update.aggregator, aggregator);
    assert_eq!(update.label.as_deref(), Some("ETH/USD"));
    assert_eq!(update.base.as_deref(), Some("ETH"));
    assert_eq!(update.quote.as_deref(), Some("USD"));
    assert_eq!(update.raw_answer, I256::unchecked_from(2_150_000_000_i64));
    assert_eq!(update.decimals, 8);
    assert_eq!(update.event_round_id, U256::from(101_u64));
    assert_eq!(update.updated_at, 1_700_000_430);
    assert_eq!(update.block_number, Some(140));
    assert_eq!(update.block_hash, Some(block_hash(140)));
    assert_eq!(update.log_index, Some(7));
    assert_eq!(update.round_status, OracleRoundStatus::Fresh);
    assert_eq!(update.value_status, OracleValueStatus::EventPending);
    assert_eq!(update.source, OracleValueSource::Event);

    let snapshot = tracker.latest(proxy).expect("event snapshot");
    assert_eq!(
        snapshot.round.answer,
        I256::unchecked_from(2_150_000_000_i64)
    );
    assert_eq!(snapshot.source, OracleValueSource::Event);
    assert_eq!(snapshot.value_status, OracleValueStatus::EventPending);
    assert!(snapshot.requires_reconciliation());

    let pending = tracker.pending_reconciliations();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id.as_str(), "eth-usd");
    assert_eq!(pending[0].proxy, proxy);
    assert_eq!(pending[0].aggregator, Some(aggregator));
    assert_eq!(pending[0].block_number, Some(140));
    assert_eq!(pending[0].block_hash, Some(block_hash(140)));
}

#[tokio::test]
async fn oracle_read_overlay_serves_event_price_before_reconciliation_and_refuses_reorged_event() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let mut tracker = seeded_tracker(proxy, aggregator, 1_700_000_500);

    let _ = apply_answer_update(
        &mut tracker,
        AnswerUpdateInput {
            proxy,
            aggregator,
            answer: 2_175_000_000,
            round_id: 101,
            updated_at: 1_700_000_440,
            block_number: 141,
            log_index: 0,
        },
    )
    .await;

    let overlay = OracleReadOverlay::new(&tracker);
    let latest = overlay
        .try_call(proxy, &latestRoundDataCall {}.abi_encode())
        .expect("overlay call should encode latestRoundData")
        .expect("registered proxy should be served from event-pending state");
    let decoded = latestRoundDataCall::abi_decode_returns(&latest).expect("latestRoundData ABI");
    assert_eq!(U256::from(decoded.roundId), U256::from(101_u64));
    assert_eq!(decoded.answer, I256::unchecked_from(2_175_000_000_i64));
    assert_eq!(decoded.updatedAt, U256::from(1_700_000_440_u64));
    assert_eq!(U256::from(decoded.answeredInRound), U256::from(101_u64));

    let decimals = overlay
        .try_call(proxy, &decimalsCall {}.abi_encode())
        .expect("overlay call should encode decimals")
        .expect("registered proxy decimals should be served");
    assert_eq!(decimalsCall::abi_decode_returns(&decimals).unwrap(), 8);

    let description = overlay
        .try_call(proxy, &descriptionCall {}.abi_encode())
        .expect("overlay call should encode description")
        .expect("registered proxy description should be served");
    assert_eq!(
        descriptionCall::abi_decode_returns(&description).unwrap(),
        "ETH / USD"
    );

    let version = overlay
        .try_call(proxy, &versionCall {}.abi_encode())
        .expect("overlay call should encode version")
        .expect("registered proxy version should be served");
    assert_eq!(
        versionCall::abi_decode_returns(&version).unwrap(),
        U256::from(4_u64)
    );

    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;
    let mut removed_log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(2_175_000_000_i64)),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_440_u64)),
        141,
        0,
    );
    removed_log.removed = true;
    let removed = runtime
        .ingest_batch(
            &mut cache,
            batch(ReactiveInput::Log(removed_log), reorged_context(141, 0)),
        )
        .expect("removed log should produce a reorg report");
    tracker
        .apply_batch_report(&removed)
        .expect("reorg report should de-authorize event state");

    let overlay = OracleReadOverlay::new(&tracker);
    assert!(
        overlay
            .try_call(proxy, &latestRoundDataCall {}.abi_encode())
            .expect("overlay call should be decodable")
            .is_none(),
        "reorged event prices must not remain actionable through the simulation overlay"
    );
}

#[tokio::test]
async fn reconciler_confirms_matching_events_and_corrects_mismatched_proxy_reads() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let mut tracker = seeded_tracker(proxy, aggregator, 1_700_000_500);
    let _ = apply_answer_update(
        &mut tracker,
        AnswerUpdateInput {
            proxy,
            aggregator,
            answer: 2_200_000_000,
            round_id: 101,
            updated_at: 1_700_000_450,
            block_number: 142,
            log_index: 0,
        },
    )
    .await;

    let mut reconciler = OracleReconciler::default();
    reconciler.enqueue(tracker.pending_reconciliations()[0].clone());
    let matching_provider = MutableProvider {
        proxy,
        aggregator,
        metadata: metadata(),
        round: round(101, 2_200_000_000, 1_700_000_450),
    };
    let result = reconciler
        .reconcile_next(&mut tracker, &matching_provider)
        .await
        .expect("matching proxy read should reconcile")
        .expect("one reconciliation result");
    let confirmed = result
        .hooks
        .iter()
        .find_map(|hook| match hook {
            OracleHookEvent::PriceConfirmed(confirmed) => Some(confirmed),
            _ => None,
        })
        .expect("matching proxy read should emit a confirmation hook");
    assert_eq!(confirmed.proxy, proxy);
    assert_eq!(
        confirmed.raw_answer,
        I256::unchecked_from(2_200_000_000_i64)
    );
    assert_eq!(confirmed.proxy_round.round_id, U256::from(101_u64));
    assert_eq!(
        tracker.latest(proxy).unwrap().value_status,
        OracleValueStatus::Confirmed
    );
    assert_eq!(
        tracker.latest(proxy).unwrap().round_status,
        OracleRoundStatus::Fresh
    );
    assert!(tracker.pending_reconciliations().is_empty());

    let _ = apply_answer_update(
        &mut tracker,
        AnswerUpdateInput {
            proxy,
            aggregator,
            answer: 2_250_000_000,
            round_id: 102,
            updated_at: 1_700_000_470,
            block_number: 143,
            log_index: 0,
        },
    )
    .await;
    let mut reconciler = OracleReconciler::default();
    reconciler.enqueue(tracker.pending_reconciliations()[0].clone());
    let correcting_provider = MutableProvider {
        proxy,
        aggregator,
        metadata: metadata(),
        round: round(102, 2_249_500_000, 1_700_000_470),
    };
    let result = reconciler
        .reconcile_next(&mut tracker, &correcting_provider)
        .await
        .expect("mismatched proxy read should reconcile")
        .expect("one reconciliation result");
    let corrected = result
        .hooks
        .iter()
        .find_map(|hook| match hook {
            OracleHookEvent::PriceCorrected(corrected) => Some(corrected),
            _ => None,
        })
        .expect("mismatched proxy read should emit a correction hook");
    assert_eq!(
        corrected.event_answer,
        I256::unchecked_from(2_250_000_000_i64)
    );
    assert_eq!(
        corrected.corrected_round.answer,
        I256::unchecked_from(2_249_500_000_i64)
    );
    assert_eq!(
        tracker.latest(proxy).unwrap().value_status,
        OracleValueStatus::Corrected
    );
    assert_eq!(
        tracker.latest(proxy).unwrap().round.answer,
        I256::unchecked_from(2_249_500_000_i64)
    );
}

#[tokio::test]
async fn reconciler_reports_aggregator_changes_and_stale_proxy_reads() {
    let proxy = Address::repeat_byte(0x10);
    let old_aggregator = Address::repeat_byte(0x20);
    let new_aggregator = Address::repeat_byte(0x21);
    let mut tracker = seeded_tracker(proxy, old_aggregator, 1_700_008_000);
    let _ = apply_answer_update(
        &mut tracker,
        AnswerUpdateInput {
            proxy,
            aggregator: old_aggregator,
            answer: 2_300_000_000,
            round_id: 101,
            updated_at: 1_700_000_500,
            block_number: 144,
            log_index: 0,
        },
    )
    .await;

    let mut reconciler = OracleReconciler::default();
    reconciler.enqueue(tracker.pending_reconciliations()[0].clone());
    let provider = MutableProvider {
        proxy,
        aggregator: new_aggregator,
        metadata: metadata(),
        round: round(101, 2_300_000_000, 1_700_000_500),
    };
    let result = reconciler
        .reconcile_next(&mut tracker, &provider)
        .await
        .expect("stale proxy read should reconcile into stale state")
        .expect("one reconciliation result");

    let changed = result
        .hooks
        .iter()
        .find_map(|hook| match hook {
            OracleHookEvent::AggregatorChanged(changed) => Some(changed),
            _ => None,
        })
        .expect("aggregator change hook");
    assert_eq!(changed.proxy, proxy);
    assert_eq!(changed.old, Some(old_aggregator));
    assert_eq!(changed.new, Some(new_aggregator));

    let stale = result
        .hooks
        .iter()
        .find_map(|hook| match hook {
            OracleHookEvent::PriceStale(stale) => Some(stale),
            _ => None,
        })
        .expect("stale proxy read hook");
    assert_eq!(stale.proxy, proxy);
    assert_eq!(stale.raw_answer, I256::unchecked_from(2_300_000_000_i64));
    assert_eq!(
        tracker.latest(proxy).unwrap().aggregator,
        Some(new_aggregator)
    );
    assert_eq!(
        tracker.latest(proxy).unwrap().round_status,
        OracleRoundStatus::Stale {
            age_secs: 7_500,
            max_age_secs: 3_600
        }
    );
    assert_eq!(
        tracker.latest(proxy).unwrap().value_status,
        OracleValueStatus::Confirmed
    );
    assert_eq!(
        tracker.latest(proxy).unwrap().source,
        OracleValueSource::Proxy
    );
}
