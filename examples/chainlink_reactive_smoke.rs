use std::{
    collections::BTreeMap,
    error::Error,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use evm_oracle_state::alloy_network::{AnyNetwork, Ethereum, TransactionBuilder};
use alloy_primitives::{Address, B256, Bytes, U256, address};
use evm_oracle_state::alloy_provider::{Provider, RootProvider};
use evm_oracle_state::alloy_rpc_client::RpcClient;
use evm_oracle_state::alloy_rpc_types_eth::{Filter, Log as RpcLog, TransactionRequest};
use alloy_sol_types::SolCall;
use evm_oracle_state::alloy_provider::mock::Asserter;
use evm_fork_cache::{
    StateUpdate,
    cache::EvmCache,
    reactive::{
        BlockRef, ChainStatus, InputSource, ReactiveConfig, ReactiveInput, ReactiveInputBatch,
        ReactiveInputRecord, ReactiveRuntime,
    },
    state_update::PurgeScope,
};
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, AggregatorLayout, ChainlinkFeedProvider, FeedConfig,
    OCR1_NEW_TRANSMISSION_TOPIC, OCR2_NEW_TRANSMISSION_TOPIC, OracleError, OracleReactiveHandler,
    OracleRegistry, OracleRoundStatus, OracleTracker, OracleValueSource, ProviderFuture, RoundData,
    StalenessPolicy,
};

alloy_sol_types::sol! {
    function decimals() external view returns (uint8);
    function description() external view returns (string);
    function version() external view returns (uint256);
    function latestRoundData() external view returns (
        uint80 roundId,
        int256 answer,
        uint256 startedAt,
        uint256 updatedAt,
        uint80 answeredInRound
    );
    function aggregator() external view returns (address);
    function typeAndVersion() external view returns (string);
}

const FEEDS: &[(&str, Address)] = &[
    (
        "ETH/USD",
        address!("5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"),
    ),
    (
        "BTC/USD",
        address!("F4030086522a5bEEa4988F8cA5B36dbC97Bee88c"),
    ),
    (
        "LINK/USD",
        address!("2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c"),
    ),
    (
        "AAVE/USD",
        address!("547a514d5e3769680Ce22B2361c10Ea13619e8a9"),
    ),
    (
        "UNI/USD",
        address!("553303d460EE0afB37EdFf9bE42922D8FF63220e"),
    ),
    (
        "USDC/USD",
        address!("8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"),
    ),
    (
        "USDT/USD",
        address!("3E7d1eAB13ad0104d2750B8863b489D65364e32D"),
    ),
    (
        "DAI/USD",
        address!("Aed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9"),
    ),
    (
        "SOL/USD",
        address!("4ffC43a60e009B551865A93d232E33Fce9f01507"),
    ),
    (
        // Canonical mainnet MATIC/USD proxy (post-POL-migration the feed keeps
        // its Chainlink name and tracks POL 1:1). Verified on-chain 2026-07-10:
        // description() == "MATIC / USD", decimals == 8, fresh latestRoundData.
        "MATIC/USD",
        address!("7bAC85A8a13A4BcD8abb3eB7d6b4d632c5a57676"),
    ),
];

const CACHE_SENTINEL_SLOT: U256 = U256::from_limbs([0x0c0ffee, 0, 0, 0]);
const CACHE_SENTINEL_VALUE: U256 = U256::from_limbs([0x1234_5678, 0, 0, 0]);

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let Ok(rpc_url) = std::env::var("ETH_RPC_URL").or_else(|_| std::env::var("E2E_RPC_URL")) else {
        println!("ETH_RPC_URL/E2E_RPC_URL unset; skipping reactive smoke.");
        return Ok(());
    };

    let duration = env_u64("SMOKE_DURATION_SECS").unwrap_or(600);
    let poll_interval = Duration::from_secs(env_u64("SMOKE_POLL_SECS").unwrap_or(12));
    let historical_lookback = env_u64("SMOKE_HISTORY_BLOCKS").unwrap_or(7_200);
    let provider = RootProvider::<AnyNetwork>::connect(&rpc_url).await?;
    let chain_id = provider.get_chain_id().await.ok();
    let feed_provider = RpcChainlinkProvider {
        provider: provider.clone(),
    };
    let mut registry = OracleRegistry::new_at_timestamp(now_secs()?);
    let mut registered = Vec::new();

    println!("reactive smoke starting: duration={duration}s poll={poll_interval:?}");
    for (label, proxy) in selected_feeds() {
        match registry
            .register_chainlink_feed(
                &feed_provider,
                FeedConfig {
                    proxy,
                    label: Some(label.to_string()),
                    staleness: StalenessPolicy::max_age(7 * 24 * 60 * 60),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(id) => {
                let snapshot = registry.latest(proxy).expect("registered snapshot");
                println!(
                    "cold_start ok label={label} proxy={:?} aggregator={:?} answer={} updated_at={} round_status={:?} value_status={:?} source={:?}",
                    snapshot.proxy,
                    snapshot.aggregator,
                    snapshot.round.answer,
                    snapshot.round.updated_at,
                    snapshot.round_status,
                    snapshot.value_status,
                    snapshot.source
                );
                if matches!(snapshot.round_status, OracleRoundStatus::Fresh)
                    && let Some(aggregator) = snapshot.aggregator
                {
                    let registration = registry
                        .registration(id)
                        .expect("registration should be stored");
                    let topic = if registration
                        .aggregator_layout
                        .as_ref()
                        .is_some_and(|evidence| {
                            evidence.layout == AggregatorLayout::ChainlinkOcr2V1
                        }) {
                        OCR2_NEW_TRANSMISSION_TOPIC
                    } else if registration
                        .aggregator_layout
                        .as_ref()
                        .is_some_and(|evidence| evidence.is_chainlink_ocr1())
                    {
                        OCR1_NEW_TRANSMISSION_TOPIC
                    } else {
                        ANSWER_UPDATED_TOPIC
                    };
                    registered.push((label.to_string(), proxy, aggregator, topic));
                }
            }
            Err(error) => {
                println!("cold_start skipped label={label} proxy={proxy:?} error={error}");
            }
        }
    }

    if registered.is_empty() {
        return Err("no feeds registered with current aggregators".into());
    }

    let mut tracker = OracleTracker::new(registry);
    let mut runtime = ReactiveRuntime::<Ethereum>::new(ReactiveConfig::default());
    runtime.register_handler(Arc::new(OracleReactiveHandler::new(
        tracker.registrations().collect::<Vec<_>>(),
    )))?;
    let mut cache = mocked_cache().await;

    println!(
        "registered {} feed(s); reactive interests={}",
        registered.len(),
        runtime.interests().len()
    );

    let latest = provider.get_block_number().await?;
    if let Some(log) = most_recent_log(
        &provider,
        registered
            .iter()
            .map(|(_, _, aggregator, topic)| (*aggregator, *topic))
            .collect(),
        latest.saturating_sub(historical_lookback),
        latest,
    )
    .await?
    {
        let summary = ingest_log(
            &mut runtime,
            &mut tracker,
            &mut cache,
            log,
            chain_id,
            latest,
        )?;
        println!(
            "historical_control ok block={} head={} lag_blocks={} applied={} invalidations={} purged_records={} typed_source={:?} requires_reconcile={} cache_seed_cleared={}",
            summary.block_number,
            latest,
            latest.saturating_sub(summary.block_number),
            summary.applied,
            summary.invalidations,
            summary.purged_records,
            summary.typed_source,
            summary.requires_reconciliation,
            summary.cache_seed_cleared
        );
    } else {
        println!(
            "historical_control no oracle update logs found in last {historical_lookback} blocks"
        );
    }

    let start = Instant::now();
    let mut next_block = latest.saturating_add(1);
    let mut live_events = 0usize;
    let mut max_lag_blocks = 0u64;
    let mut polls = 0usize;

    while start.elapsed() < Duration::from_secs(duration) {
        tokio::time::sleep(poll_interval).await;
        polls += 1;
        let head = provider.get_block_number().await?;
        if head < next_block {
            println!("poll={polls} head={head} waiting_for_block={next_block}");
            continue;
        }

        let logs = fetch_logs(
            &provider,
            registered
                .iter()
                .map(|(_, _, aggregator, topic)| (*aggregator, *topic))
                .collect(),
            next_block,
            head,
        )
        .await?;
        println!(
            "poll={polls} scanned_blocks={}..{} logs={}",
            next_block,
            head,
            logs.len()
        );
        next_block = head.saturating_add(1);

        for log in logs {
            let summary = ingest_log(&mut runtime, &mut tracker, &mut cache, log, chain_id, head)?;
            live_events += 1;
            max_lag_blocks = max_lag_blocks.max(head.saturating_sub(summary.block_number));
            println!(
                "live_event block={} head={} lag_blocks={} applied={} invalidations={} purged_records={} typed_source={:?} requires_reconcile={} cache_seed_cleared={}",
                summary.block_number,
                head,
                head.saturating_sub(summary.block_number),
                summary.applied,
                summary.invalidations,
                summary.purged_records,
                summary.typed_source,
                summary.requires_reconciliation,
                summary.cache_seed_cleared
            );
        }
    }

    println!(
        "reactive_smoke_summary duration_secs={} polls={} live_events={} max_lag_blocks={} feeds={}",
        duration,
        polls,
        live_events,
        max_lag_blocks,
        registered.len()
    );

    if live_events == 0 {
        println!(
            "reactive_smoke_note no live oracle update logs arrived during the timed window; historical control still exercised runtime/cache path"
        );
    }

    Ok(())
}

struct EventSummary {
    block_number: u64,
    applied: usize,
    invalidations: usize,
    purged_records: usize,
    typed_source: OracleValueSource,
    requires_reconciliation: bool,
    cache_seed_cleared: bool,
}

fn ingest_log(
    runtime: &mut ReactiveRuntime<Ethereum>,
    tracker: &mut OracleTracker,
    cache: &mut EvmCache,
    log: RpcLog,
    chain_id: Option<u64>,
    head: u64,
) -> Result<EventSummary, Box<dyn Error>> {
    let block_number = log.block_number.unwrap_or(head);
    let aggregator = log.address();
    let proxy_before = tracker
        .registrations()
        .find(|registration| registration.current_aggregator == Some(aggregator))
        .map(|registration| registration.proxy)
        .ok_or("log did not route to a registered aggregator")?;

    cache.apply_updates(&[
        StateUpdate::slot(proxy_before, CACHE_SENTINEL_SLOT, CACHE_SENTINEL_VALUE),
        StateUpdate::slot(aggregator, CACHE_SENTINEL_SLOT, CACHE_SENTINEL_VALUE),
    ]);

    let batch = ReactiveInputBatch::new(vec![ReactiveInputRecord::new(
        ReactiveInput::Log(log.clone()),
        context_for_log(&log, chain_id, head),
    )]);
    let report = runtime.ingest_batch(cache, batch)?;
    tracker.apply_batch_report(&report)?;

    let applied = report.applied.len();
    let invalidations = report
        .applied
        .iter()
        .map(|applied| applied.invalidations.len())
        .sum();
    let purged_records = report
        .applied
        .iter()
        .flat_map(|applied| applied.diff.purged.iter())
        .filter(|purge| {
            matches!(purge.scope, PurgeScope::AllStorage)
                && (purge.address == proxy_before || purge.address == aggregator)
        })
        .count();
    let proxy_after = cache.cached_storage_value(proxy_before, CACHE_SENTINEL_SLOT);
    let aggregator_after = cache.cached_storage_value(aggregator, CACHE_SENTINEL_SLOT);
    let cache_seed_cleared =
        proxy_after != Some(CACHE_SENTINEL_VALUE) && aggregator_after != Some(CACHE_SENTINEL_VALUE);
    let snapshot = tracker
        .latest(proxy_before)
        .ok_or("snapshot missing after event")?;

    if applied == 0 || invalidations < 2 || purged_records < 2 || !cache_seed_cleared {
        return Err(format!(
            "event did not update runtime/cache correctly: applied={applied} invalidations={invalidations} purged={purged_records} proxy_after={proxy_after:?} aggregator_after={aggregator_after:?}"
        )
        .into());
    }

    Ok(EventSummary {
        block_number,
        applied,
        invalidations,
        purged_records,
        typed_source: snapshot.source,
        requires_reconciliation: snapshot.requires_reconciliation(),
        cache_seed_cleared,
    })
}

fn context_for_log(
    log: &RpcLog,
    chain_id: Option<u64>,
    fallback_block_number: u64,
) -> evm_fork_cache::reactive::ReactiveContext {
    let block_number = log.block_number.unwrap_or(fallback_block_number);
    let block = BlockRef {
        number: block_number,
        hash: log.block_hash.unwrap_or_default(),
        parent_hash: None,
        timestamp: log.block_timestamp,
    };
    evm_fork_cache::reactive::ReactiveContext {
        chain_id,
        source: InputSource::Poll,
        chain_status: ChainStatus::Included {
            block,
            confirmations: 0,
        },
        block: Some(block),
        transaction_index: log.transaction_index,
        log_index: log.log_index,
    }
}

async fn most_recent_log(
    provider: &RootProvider<AnyNetwork>,
    routes: Vec<(Address, B256)>,
    from_block: u64,
    to_block: u64,
) -> Result<Option<RpcLog>, Box<dyn Error>> {
    let mut logs = fetch_logs(provider, routes, from_block, to_block).await?;
    logs.sort_by_key(|log| {
        (
            log.block_number.unwrap_or_default(),
            log.log_index.unwrap_or_default(),
        )
    });
    Ok(logs.pop())
}

async fn fetch_logs(
    provider: &RootProvider<AnyNetwork>,
    routes: Vec<(Address, B256)>,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<RpcLog>, Box<dyn Error>> {
    if routes.is_empty() || from_block > to_block {
        return Ok(Vec::new());
    }
    let mut by_topic: BTreeMap<B256, Vec<Address>> = BTreeMap::new();
    for (aggregator, topic) in routes {
        by_topic.entry(topic).or_default().push(aggregator);
    }

    let mut logs = Vec::new();
    for (topic, aggregators) in by_topic {
        let filter = Filter::new()
            .address(aggregators)
            .event_signature(topic)
            .from_block(from_block)
            .to_block(to_block);
        logs.extend(provider.get_logs(&filter).await?);
    }
    Ok(logs)
}

async fn mocked_cache() -> EvmCache {
    let asserter = Asserter::new();
    let client = RpcClient::mocked(asserter);
    let provider = RootProvider::<AnyNetwork>::new(client);
    EvmCache::new(Arc::new(provider)).await
}

fn selected_feeds() -> Vec<(&'static str, Address)> {
    if let Ok(proxy) = std::env::var("CHAINLINK_FEED_PROXY")
        && let Ok(address) = proxy.parse::<Address>()
    {
        return vec![("custom", address)];
    }
    FEEDS.to_vec()
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse().ok()
}

fn now_secs() -> Result<u64, Box<dyn Error>> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

struct RpcChainlinkProvider {
    provider: RootProvider<AnyNetwork>,
}

impl ChainlinkFeedProvider for RpcChainlinkProvider {
    fn decimals(&self, proxy: Address) -> ProviderFuture<'_, u8> {
        Box::pin(async move {
            let bytes = eth_call(&self.provider, proxy, decimalsCall {}.abi_encode()).await?;
            decimalsCall::abi_decode_returns(&bytes).map_err(provider_error)
        })
    }

    fn description(&self, proxy: Address) -> ProviderFuture<'_, String> {
        Box::pin(async move {
            let bytes = eth_call(&self.provider, proxy, descriptionCall {}.abi_encode()).await?;
            descriptionCall::abi_decode_returns(&bytes).map_err(provider_error)
        })
    }

    fn version(&self, proxy: Address) -> ProviderFuture<'_, U256> {
        Box::pin(async move {
            let bytes = eth_call(&self.provider, proxy, versionCall {}.abi_encode()).await?;
            versionCall::abi_decode_returns(&bytes).map_err(provider_error)
        })
    }

    fn latest_round_data(&self, proxy: Address) -> ProviderFuture<'_, RoundData> {
        Box::pin(async move {
            let bytes =
                eth_call(&self.provider, proxy, latestRoundDataCall {}.abi_encode()).await?;
            let round = latestRoundDataCall::abi_decode_returns(&bytes).map_err(provider_error)?;
            Ok(RoundData {
                round_id: U256::from(round.roundId),
                answer: round.answer,
                started_at: u64_from_u256(round.startedAt, "startedAt")?,
                updated_at: u64_from_u256(round.updatedAt, "updatedAt")?,
                answered_in_round: U256::from(round.answeredInRound),
            })
        })
    }

    fn aggregator(&self, proxy: Address) -> ProviderFuture<'_, Option<Address>> {
        Box::pin(async move {
            let Ok(bytes) = eth_call(&self.provider, proxy, aggregatorCall {}.abi_encode()).await
            else {
                return Ok(None);
            };
            Ok(aggregatorCall::abi_decode_returns(&bytes).ok())
        })
    }

    fn aggregator_type_and_version(
        &self,
        aggregator: Address,
    ) -> ProviderFuture<'_, Option<String>> {
        Box::pin(async move {
            let Ok(bytes) = eth_call(
                &self.provider,
                aggregator,
                typeAndVersionCall {}.abi_encode(),
            )
            .await
            else {
                return Ok(None);
            };
            Ok(typeAndVersionCall::abi_decode_returns(&bytes).ok())
        })
    }
}

async fn eth_call(
    provider: &RootProvider<AnyNetwork>,
    target: Address,
    calldata: Vec<u8>,
) -> Result<Bytes, OracleError> {
    let tx = TransactionRequest::default()
        .with_to(target)
        .with_input(Bytes::from(calldata));
    provider.call(tx.into()).await.map_err(provider_error)
}

fn u64_from_u256(value: U256, field: &'static str) -> Result<u64, OracleError> {
    u64::try_from(value)
        .map_err(|_| OracleError::Provider(format!("{field} does not fit in u64: {value}")))
}

fn provider_error(error: impl ToString) -> OracleError {
    OracleError::Provider(error.to_string())
}
