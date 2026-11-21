use std::sync::Arc;

use evm_oracle_state::alloy_eips::BlockId;
use evm_oracle_state::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_provider::{RootProvider, network::AnyNetwork};
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::cache::EvmCache;
use evm_fork_cache::reactive::{
    BlockRef, ChainStatus, HandlerId, InputSource, ReactiveConfig, ReactiveEffect, ReactiveInput,
    ReactiveInputBatch, ReactiveInputRecord, ReactiveInterest, ReactiveRuntime, RouteKey,
    RouteKeySpec, StateEffectQuality,
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, FeedId, FeedMetadata, FeedRegistration, FeedSource, OracleFeedStatus,
    OracleReactiveHandler, OracleRoundStatus, OracleTracker, OracleValueSource, RoundData,
    StalenessPolicy,
};

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
        transaction_hash: Some(B256::repeat_byte(0x44)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
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

fn word(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn uint_topic(value: U256) -> B256 {
    B256::from(value.to_be_bytes::<32>())
}

fn int_topic(value: I256) -> B256 {
    B256::from(value.into_raw().to_be_bytes::<32>())
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
        metadata: FeedMetadata {
            decimals: 8,
            description: "ETH / USD".to_string(),
            version: U256::from(4_u64),
        },
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
    }
}

fn initial_round() -> RoundData {
    RoundData {
        round_id: U256::from(100_u64),
        answer: I256::unchecked_from(2_000_000_000_i64),
        started_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        answered_in_round: U256::from(100_u64),
    }
}

#[test]
fn reactive_handler_builds_interests_for_current_aggregators() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::new(vec![registration(proxy, aggregator)]);
    let log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(1)),
            uint_topic(U256::from(1_u64)),
        ],
        word(U256::from(1_700_000_000_u64)),
        101,
        0,
    );

    let matching_interest = handler
        .interests()
        .into_iter()
        .find_map(|interest| match interest {
            ReactiveInterest::Logs(interest) if interest.matches(&log) => Some(interest),
            _ => None,
        })
        .expect("AnswerUpdated log from current aggregator should match");

    assert!(matches!(
        matching_interest.route_key,
        Some(RouteKeySpec::EmitterAddress)
    ));
    assert_eq!(
        matching_interest.route_key(&log),
        Some(RouteKey::Address(aggregator))
    );
}

#[tokio::test]
async fn answer_updated_emits_invalidation_and_committed_oracle_update() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let registration = registration(proxy, aggregator);
    let mut tracker = OracleTracker::from_registrations_at_timestamp(
        vec![(registration.clone(), initial_round())],
        1_700_000_200,
    )
    .expect("seed tracker");
    let handler = OracleReactiveHandler::new(tracker.registrations().collect::<Vec<_>>());
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;

    let event_answer = I256::unchecked_from(2_100_000_000_i64);
    let log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(event_answer),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_180_u64)),
        102,
        0,
    );

    let batch_report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(102, 0)))
        .expect("runtime should commit handler effects");

    assert_eq!(batch_report.applied.len(), 1);
    let applied = &batch_report.applied[0];
    assert_eq!(
        applied.handler_id,
        HandlerId::new("evm-oracle-state.chainlink")
    );
    assert_eq!(applied.quality, StateEffectQuality::RequiresRepair);
    assert_eq!(applied.invalidations.len(), 2);
    assert!(applied.invalidations.iter().any(|request| {
        request.address == proxy
            && matches!(
                request.scope,
                evm_fork_cache::state_update::PurgeScope::AllStorage
            )
    }));
    assert!(applied.invalidations.iter().any(|request| {
        request.address == aggregator
            && matches!(
                request.scope,
                evm_fork_cache::state_update::PurgeScope::AllStorage
            )
    }));
    assert!(applied.hook_signals.iter().any(|signal| {
        signal.namespace.as_ref() == "evm-oracle-state"
            && signal.kind.as_ref() == "oracle.answer_updated"
            && signal.payload.is_some()
    }));

    tracker
        .apply_batch_report(&batch_report)
        .expect("committed report should update typed state");
    let snapshot = tracker.latest(proxy).expect("updated snapshot");
    assert_eq!(snapshot.round.answer, event_answer);
    assert_eq!(snapshot.round.updated_at, 1_700_000_180);
    assert_eq!(snapshot.round_status, OracleRoundStatus::Fresh);
    assert_eq!(snapshot.source, OracleValueSource::Event);
    assert!(
        snapshot.requires_reconciliation(),
        "event-derived Chainlink state should be flagged for proxy reconciliation"
    );
}

#[tokio::test]
async fn unknown_aggregator_log_is_ignored() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let unknown = Address::repeat_byte(0x99);
    let handler = OracleReactiveHandler::new(vec![registration(proxy, aggregator)]);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime
        .register_handler(Arc::new(handler))
        .expect("register handler");
    let mut cache = setup_cache().await;
    let log = rpc_log(
        unknown,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(1)),
            uint_topic(U256::from(1_u64)),
        ],
        word(U256::from(1_700_000_000_u64)),
        103,
        0,
    );

    let batch_report = runtime
        .ingest_batch(&mut cache, batch(ReactiveInput::Log(log), context(103, 0)))
        .expect("unknown logs should not fail the runtime");

    assert!(batch_report.applied.is_empty());
    assert!(batch_report.resyncs.is_empty());
    assert!(batch_report.speculative.is_empty());
}

#[test]
fn handler_outcome_for_removed_logs_is_empty() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::new(vec![registration(proxy, aggregator)]);
    let mut log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(1)),
            uint_topic(U256::from(1_u64)),
        ],
        word(U256::from(1_700_000_000_u64)),
        104,
        0,
    );
    log.removed = true;

    let outcome = handler
        .handle_for_test(&context(104, 0), &ReactiveInput::Log(log))
        .expect("removed log should be a no-op");

    assert!(outcome.effects.is_empty());
    assert_eq!(outcome.quality, StateEffectQuality::NoStateEffect);
    assert!(!outcome.effects.iter().any(|effect| {
        matches!(
            effect,
            ReactiveEffect::Invalidate(_) | ReactiveEffect::Hook(_)
        )
    }));
}

/// A removed log delivered with `ChainStatus::Reorged` describes a value the
/// chain rolled back: the handler must purge (proxy + aggregator) and emit
/// `RequiresRepair` hooks, and must never re-apply the reverted value as an
/// exact `StateUpdate` write.
#[test]
fn removed_reorged_log_purges_instead_of_writing() {
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let handler = OracleReactiveHandler::new(vec![registration(proxy, aggregator)]);
    let mut log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(I256::unchecked_from(2_100_000_000_i64)),
            uint_topic(U256::from(101_u64)),
        ],
        word(U256::from(1_700_000_105_u64)),
        105,
        0,
    );
    log.removed = true;
    let block = block_ref(105);
    let reorged_context = evm_fork_cache::reactive::ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Synthetic,
        chain_status: ChainStatus::Reorged {
            dropped_from: block,
        },
        block: Some(block),
        transaction_index: Some(0),
        log_index: Some(0),
    };

    let outcome = handler
        .handle_for_test(&reorged_context, &ReactiveInput::Log(log))
        .expect("removed reorged log is handled");

    assert!(
        !outcome
            .effects
            .iter()
            .any(|effect| matches!(effect, ReactiveEffect::StateUpdate(_))),
        "reverted event values must never be re-applied as exact writes"
    );
    let purged: Vec<Address> = outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ReactiveEffect::Invalidate(request) => Some(request.address),
            _ => None,
        })
        .collect();
    assert!(purged.contains(&aggregator), "aggregator must be purged");
    assert!(purged.contains(&proxy), "feed proxy must be purged");
    assert_eq!(outcome.quality, StateEffectQuality::RequiresRepair);
    assert!(
        outcome
            .effects
            .iter()
            .any(|effect| matches!(effect, ReactiveEffect::Hook(_))),
        "RequiresRepair hooks still fire so trackers queue reconciliation"
    );
}

#[allow(dead_code)]
fn _block_id_is_available_for_reconcile_signatures(block: BlockId) -> BlockId {
    block
}
