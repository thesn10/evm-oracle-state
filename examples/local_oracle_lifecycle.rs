use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use evm_oracle_state::alloy_network::{AnyNetwork, Ethereum};
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256};
use evm_oracle_state::alloy_provider::RootProvider;
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use alloy_sol_types::SolCall;
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::{
    cache::EvmCache,
    reactive::{
        BlockRef, ChainStatus, InputSource, ReactiveConfig, ReactiveInput, ReactiveInputBatch,
        ReactiveInputRecord, ReactiveRuntime,
    },
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, ChainlinkFeed, ChainlinkFeedProvider, FeedMetadata, OracleHookEvent,
    OracleRuntime, OracleSignalKind, ProviderFuture, RoundData,
};

alloy_sol_types::sol! {
    function latestRoundData() external view returns (
        uint80 roundId,
        int256 answer,
        uint256 startedAt,
        uint256 updatedAt,
        uint80 answeredInRound
    );
}

const PROXY: Address = Address::repeat_byte(0x10);
const AGGREGATOR: Address = Address::repeat_byte(0x20);

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let initial_round = round_data(100, 2_000_000_000, 1_700_000_000);
    let event_round = round_data(101, 2_150_000_000, 1_700_000_180);
    let provider = DemoChainlinkProvider::new(initial_round.clone());
    let provider_controls = provider.clone();
    let callback_events = Arc::new(Mutex::new(Vec::<String>::new()));
    let callback_events_for_builder = Arc::clone(&callback_events);

    let mut oracle = OracleRuntime::builder(provider)
        .feed(
            ChainlinkFeed::new(PROXY)
                .id("eth-usd")
                .label("ETH/USD")
                .base("ETH")
                .quote("USD")
                .max_age_secs(3_600),
        )
        .now_timestamp(1_700_000_200)
        .on_event(move |event| {
            callback_events_for_builder
                .lock()
                .expect("callback log lock")
                .push(format!("callback kind={}", event.kind_str()));
        })
        .build()
        .await?;

    println!("1. cold start");
    print_price("   proxy read", &oracle.price("eth-usd")?);

    let mut reactive = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    reactive
        .register_handler(Arc::new(oracle.reactive_handler()))
        .map_err(to_boxed_error)?;
    let mut cache = mocked_cache().await;

    let log = answer_updated_log(
        AGGREGATOR,
        event_round.answer,
        event_round.round_id,
        event_round.updated_at,
        120,
        3,
    );
    let report = reactive
        .ingest_batch(
            &mut cache,
            ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
                ReactiveInput::Log(log),
                context(120, 3),
            )]),
        )
        .map_err(to_boxed_error)?;
    let hook_events = oracle.apply_batch_report(&report)?;

    println!("\n2. event fast path");
    println!(
        "   reactive effects applied={} invalidations={} hooks={}",
        report.applied.len(),
        report
            .applied
            .iter()
            .map(|applied| applied.invalidations.len())
            .sum::<usize>(),
        hook_events.events.len()
    );
    for event in &hook_events.events {
        print_hook("   hook", event);
    }
    print_price("   typed state", &oracle.price("eth-usd")?);

    println!("\n3. simulation overlay before reconciliation");
    let calldata = latestRoundDataCall {}.abi_encode();
    let bytes = oracle
        .read_overlay()
        .try_call(PROXY, &calldata)?
        .expect("registered feed should answer latestRoundData()");
    let overlay_round = latestRoundDataCall::abi_decode_returns(&bytes)?;
    println!(
        "   latestRoundData() answer={} round={} updated_at={}",
        overlay_round.answer, overlay_round.roundId, overlay_round.updatedAt
    );

    println!("\n4. proxy reconciliation");
    provider_controls.set_round(event_round);
    let reconciliation_events = oracle.reconcile_pending().await?;
    for event in &reconciliation_events {
        print_hook("   hook", event);
    }
    print_price("   reconciled state", &oracle.price("eth-usd")?);

    println!("\n5. user callbacks");
    for line in callback_events.lock().expect("callback log lock").iter() {
        println!("   {line}");
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct DemoChainlinkProvider {
    metadata: FeedMetadata,
    round: Arc<Mutex<RoundData>>,
}

impl DemoChainlinkProvider {
    fn new(round: RoundData) -> Self {
        Self {
            metadata: FeedMetadata {
                decimals: 8,
                description: "ETH / USD".to_string(),
                version: U256::from(4_u64),
            },
            round: Arc::new(Mutex::new(round)),
        }
    }

    fn set_round(&self, round: RoundData) {
        *self.round.lock().expect("round lock") = round;
    }
}

impl ChainlinkFeedProvider for DemoChainlinkProvider {
    fn decimals(&self, proxy: Address) -> ProviderFuture<'_, u8> {
        assert_eq!(proxy, PROXY);
        let decimals = self.metadata.decimals;
        Box::pin(async move { Ok(decimals) })
    }

    fn description(&self, proxy: Address) -> ProviderFuture<'_, String> {
        assert_eq!(proxy, PROXY);
        let description = self.metadata.description.clone();
        Box::pin(async move { Ok(description) })
    }

    fn version(&self, proxy: Address) -> ProviderFuture<'_, U256> {
        assert_eq!(proxy, PROXY);
        let version = self.metadata.version;
        Box::pin(async move { Ok(version) })
    }

    fn latest_round_data(&self, proxy: Address) -> ProviderFuture<'_, RoundData> {
        assert_eq!(proxy, PROXY);
        let round = self.round.lock().expect("round lock").clone();
        Box::pin(async move { Ok(round) })
    }

    fn aggregator(&self, proxy: Address) -> ProviderFuture<'_, Option<Address>> {
        assert_eq!(proxy, PROXY);
        Box::pin(async move { Ok(Some(AGGREGATOR)) })
    }
}

fn round_data(round_id: u64, answer: i64, updated_at: u64) -> RoundData {
    RoundData {
        round_id: U256::from(round_id),
        answer: I256::unchecked_from(answer),
        started_at: updated_at.saturating_sub(60),
        updated_at,
        answered_in_round: U256::from(round_id),
    }
}

fn answer_updated_log(
    aggregator: Address,
    answer: I256,
    round_id: U256,
    updated_at: u64,
    block_number: u64,
    log_index: u64,
) -> RpcLog {
    RpcLog {
        inner: PrimitiveLog::new_unchecked(
            aggregator,
            vec![
                ANSWER_UPDATED_TOPIC,
                B256::from(answer.into_raw().to_be_bytes::<32>()),
                B256::from(round_id.to_be_bytes::<32>()),
            ],
            Bytes::from(U256::from(updated_at).to_be_bytes::<32>().to_vec()),
        ),
        block_hash: Some(block_hash(block_number)),
        block_number: Some(block_number),
        block_timestamp: Some(1_700_000_000 + block_number),
        transaction_hash: Some(B256::repeat_byte(0x77)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
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

async fn mocked_cache() -> EvmCache {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    EvmCache::new(Arc::new(provider)).await
}

fn block_hash(block_number: u64) -> B256 {
    B256::repeat_byte(block_number as u8)
}

fn print_price(label: &str, price: &evm_oracle_state::OraclePrice) {
    println!(
        "{label}: answer={} decimals={} round_status={:?} value_status={:?} source={:?} actionable={}",
        price.raw_answer,
        price.decimals,
        price.round_status,
        price.value_status,
        price.source,
        price.is_actionable()
    );
}

fn print_hook(label: &str, event: &OracleHookEvent) {
    match event {
        OracleHookEvent::PriceUpdate(update) => println!(
            "{label}: kind={} feed={} answer={} round={} block={:?}",
            OracleSignalKind::PriceUpdate.as_str(),
            update.id,
            update.raw_answer,
            update.event_round_id,
            update.block_number
        ),
        OracleHookEvent::PriceConfirmed(confirmed) => println!(
            "{label}: kind={} feed={} answer={} round={}",
            OracleSignalKind::PriceConfirmed.as_str(),
            confirmed.id,
            confirmed.raw_answer,
            confirmed.proxy_round.round_id
        ),
        OracleHookEvent::PriceCorrected(corrected) => println!(
            "{label}: kind={} feed={} event_answer={} corrected_answer={}",
            OracleSignalKind::PriceCorrected.as_str(),
            corrected.id,
            corrected.event_answer,
            corrected.raw_answer
        ),
        OracleHookEvent::PriceStale(stale) => println!(
            "{label}: kind={} feed={} answer={}",
            OracleSignalKind::PriceStale.as_str(),
            stale.id,
            stale.raw_answer
        ),
        OracleHookEvent::AggregatorChanged(change) => println!(
            "{label}: kind={} feed={} old={:?} new={:?}",
            OracleSignalKind::AggregatorChanged.as_str(),
            change.id,
            change.old,
            change.new
        ),
        event => println!("{label}: kind={}", event.kind_str()),
    }
}

fn to_boxed_error(error: impl ToString) -> Box<dyn Error> {
    std::io::Error::other(error.to_string()).into()
}
