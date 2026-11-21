use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    fs,
    hint::black_box,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use evm_oracle_state::alloy_network::{AnyNetwork, TransactionBuilder};
use alloy_primitives::{Address, B256, Bytes, I256, U256};
use evm_oracle_state::alloy_provider::{Provider, RootProvider};
use evm_oracle_state::alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::{SolCall, sol};
use evm_fork_cache::{
    StateView,
    cache::{EvmCache, StorageFetchStrategy},
    multicall::{IMulticall3, MULTICALL3_ADDRESS, execute_batched},
};
use evm_oracle_state::{
    AggregatorLayoutEvidence, AssetId, ChainlinkOcr2StorageAdapter, Denomination, Feed, FeedId,
    FeedMetadata, FeedRegistration, FeedSource, OracleAdapter, OracleFeedStatus, OraclePriceUpdate,
    OracleRoundStatus, OracleRuntime, OracleStorageEffect, OracleStorageSync, OracleValueSource,
    OracleValueStatus, StalenessPolicy,
};

const ETH_USD_PROXY: &str = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419";
const BTC_USD_PROXY: &str = "0xf4030086522a5beea4988f8ca5b36dbc97bee88c";
const LINK_USD_PROXY: &str = "0x2c1d072e956affc0d435cb7ac38ef18d24d9127c";
const USDC_USD_PROXY: &str = "0x8fffffd4afb6115b954bd326cbe7b4ba576818f6";

const SMOKE_FEED_SET: &[StaticBenchFeedSpec] = &[
    StaticBenchFeedSpec::new("ETH/USD", "ETH", "USD", ETH_USD_PROXY),
    StaticBenchFeedSpec::new("BTC/USD", "BTC", "USD", BTC_USD_PROXY),
    StaticBenchFeedSpec::new("LINK/USD", "LINK", "USD", LINK_USD_PROXY),
    StaticBenchFeedSpec::new("USDC/USD", "USDC", "USD", USDC_USD_PROXY),
];

const CURATED_MAINNET_FEED_SET: &[StaticBenchFeedSpec] = &[
    StaticBenchFeedSpec::new("ETH/USD", "ETH", "USD", ETH_USD_PROXY),
    StaticBenchFeedSpec::new("BTC/USD", "BTC", "USD", BTC_USD_PROXY),
    StaticBenchFeedSpec::new("LINK/USD", "LINK", "USD", LINK_USD_PROXY),
    StaticBenchFeedSpec::new("USDC/USD", "USDC", "USD", USDC_USD_PROXY),
    StaticBenchFeedSpec::new(
        "DAI/USD",
        "DAI",
        "USD",
        "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9",
    ),
    StaticBenchFeedSpec::new(
        "USDT/USD",
        "USDT",
        "USD",
        "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D",
    ),
    StaticBenchFeedSpec::new(
        "AAVE/USD",
        "AAVE",
        "USD",
        "0x547a514d5e3769680Ce22B2361c10Ea13619e8a9",
    ),
    StaticBenchFeedSpec::new(
        "UNI/USD",
        "UNI",
        "USD",
        "0x553303d460EE0afb37EdFf9bE42922D8FF63220e",
    ),
    StaticBenchFeedSpec::new(
        "MKR/USD",
        "MKR",
        "USD",
        "0xEC1D1B3b0443256cc3860e24a46f108e699484aa",
    ),
    StaticBenchFeedSpec::new(
        "COMP/USD",
        "COMP",
        "USD",
        "0xdbd020CAeF83eFd542f4De03e3cF0C28A4428bd5",
    ),
    StaticBenchFeedSpec::new(
        "SNX/USD",
        "SNX",
        "USD",
        "0xDC3EA94CD0ac27d9A86C180091e7f78C683d3699",
    ),
    StaticBenchFeedSpec::new(
        "YFI/USD",
        "YFI",
        "USD",
        "0x7c5d4F8345e66f68099581Db340cd65B078C41f4",
    ),
    StaticBenchFeedSpec::new(
        "SUSHI/USD",
        "SUSHI",
        "USD",
        "0xCc70F09A6CC17553b2E31954cD36E4A2d89501f7",
    ),
    StaticBenchFeedSpec::new(
        "CRV/USD",
        "CRV",
        "USD",
        "0xCd627aA160A6fA45Eb793D19Ef54f5062F20f33f",
    ),
    StaticBenchFeedSpec::new(
        "1INCH/USD",
        "1INCH",
        "USD",
        "0xc929ad75B72593967DE83E7F7Cda0493458261D9",
    ),
    StaticBenchFeedSpec::new(
        "BAT/USD",
        "BAT",
        "USD",
        "0x0d16d4528239e9ee52fa531af613AcdB23D88c94",
    ),
    StaticBenchFeedSpec::new(
        "ZRX/USD",
        "ZRX",
        "USD",
        "0x2885d15b8Af22648b98B122b22FDF4D2a56c6023",
    ),
    StaticBenchFeedSpec::new(
        "KNC/USD",
        "KNC",
        "USD",
        "0x656c0544eF4C98A6a98491833A89204Abb045d6b",
    ),
    StaticBenchFeedSpec::new(
        "BNB/USD",
        "BNB",
        "USD",
        "0x14e613AC84A31f709eadbdf89C6CC390fDc9540A",
    ),
    StaticBenchFeedSpec::new(
        "SOL/USD",
        "SOL",
        "USD",
        "0x4ffc43a60e009b551865a93d232e33fce9f01507",
    ),
    StaticBenchFeedSpec::new(
        "AVAX/USD",
        "AVAX",
        "USD",
        "0xFF3eeb22B5E3dE6E705B44749c2559D704923FD7",
    ),
    StaticBenchFeedSpec::new(
        "APE/USD",
        "APE",
        "USD",
        "0xD10aBbC76679a20055E167BB80A24ac851b37056",
    ),
    StaticBenchFeedSpec::new(
        "stETH/ETH",
        "stETH",
        "ETH",
        "0x86392dC19c0b719886221c78AB11eb8Cf5c52812",
    ),
    StaticBenchFeedSpec::new(
        "rETH/ETH",
        "rETH",
        "ETH",
        "0x536218f9E9Eb48863970252233c8F271f554C2d0",
    ),
];

sol! {
    interface OracleBenchChainlinkInterface {
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
        function aggregator() external view returns (address);
        function typeAndVersion() external view returns (string);
    }
}

use OracleBenchChainlinkInterface::{
    aggregatorCall, decimalsCall, descriptionCall, latestRoundDataCall, typeAndVersionCall,
    versionCall,
};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    run_direct_sync_microbench();

    let Some(rpc_url) = env::var("ETH_RPC_URL")
        .ok()
        .or_else(|| env::var("E2E_RPC_URL").ok())
    else {
        println!("live_prewarm_bench skipped: set ETH_RPC_URL or E2E_RPC_URL");
        return Ok(());
    };

    run_live_prewarm_bench(&rpc_url).await
}

fn run_direct_sync_microbench() {
    let iterations = env_u64("ORACLE_BENCH_ITERS").unwrap_or(100_000) as usize;
    let proxy = Address::repeat_byte(0x10);
    let aggregator = Address::repeat_byte(0x20);
    let registration = ocr2_registration(proxy, aggregator);
    let update = price_update(proxy, aggregator);
    let cold_state = MapStateView::default();
    let warm_state = MapStateView::default().with_slot(
        aggregator,
        ChainlinkOcr2StorageAdapter::hot_vars_slot(),
        U256::from(1_u64),
    );
    let sync = OracleStorageSync::chainlink_defaults();

    let cold = time_loop(iterations, || {
        matches!(
            black_box(
                sync.state_effect_for_answer(&registration, &update, &cold_state)
                    .expect("cold storage effect")
            ),
            OracleStorageEffect::FallbackPurge
        )
    });
    let warm = time_loop(iterations, || {
        matches!(
            black_box(
                sync.state_effect_for_answer(&registration, &update, &warm_state)
                    .expect("warm storage effect")
            ),
            OracleStorageEffect::StateUpdates { .. }
        )
    });

    println!(
        "direct_sync_microbench iterations={} cold_fallback_ns={} warm_exact_ns={} warm_exact_cpu_over_cold={:.2}x",
        iterations,
        per_iter_ns(cold, iterations),
        per_iter_ns(warm, iterations),
        ratio(warm, cold)
    );
}

async fn run_live_prewarm_bench(rpc_url: &str) -> Result<(), Box<dyn Error>> {
    let feeds = benchmark_feeds()?;
    println!(
        "live_prewarm_bench feeds={} skip_rpc_breakdown={} skip_fork_breakdown={} skip_sequential={}",
        feeds.len(),
        env_bool("ORACLE_BENCH_SKIP_RPC_BREAKDOWN"),
        env_bool("ORACLE_BENCH_SKIP_FORK_BREAKDOWN"),
        env_bool("ORACLE_BENCH_SKIP_SEQUENTIAL")
    );
    if !env_bool("ORACLE_BENCH_SKIP_RPC_BREAKDOWN") {
        run_direct_rpc_multicall_breakdown(rpc_url, &feeds).await?;
    }
    if !env_bool("ORACLE_BENCH_SKIP_FORK_BREAKDOWN") {
        run_fork_cache_call_breakdown(rpc_url, &feeds).await?;
    }

    let registration_multicall = run_one_live_registration(
        rpc_url,
        StorageFetchStrategy::default(),
        feeds.clone(),
        true,
    )
    .await?;
    println!(
        "live_registration_multicall registered_feeds={} skipped_feeds={} elapsed_ms={}",
        registration_multicall.registered_feeds,
        registration_multicall.skipped_feeds,
        registration_multicall.elapsed.as_millis()
    );

    if env_bool("ORACLE_BENCH_SKIP_SEQUENTIAL") {
        println!("live_registration_sequential skipped=true reason=ORACLE_BENCH_SKIP_SEQUENTIAL");
        println!(
            "live_registration_summary multicall_ms={} sequential_ms=skipped speedup=skipped",
            registration_multicall.elapsed.as_millis()
        );
    } else {
        let registration_sequential = run_one_live_registration(
            rpc_url,
            StorageFetchStrategy::default(),
            feeds.clone(),
            false,
        )
        .await?;
        println!(
            "live_registration_sequential registered_feeds={} skipped_feeds={} elapsed_ms={}",
            registration_sequential.registered_feeds,
            registration_sequential.skipped_feeds,
            registration_sequential.elapsed.as_millis()
        );

        println!(
            "live_registration_summary multicall_ms={} sequential_ms={} speedup={:.2}x",
            registration_multicall.elapsed.as_millis(),
            registration_sequential.elapsed.as_millis(),
            ratio(
                registration_sequential.elapsed,
                registration_multicall.elapsed
            )
        );
    }

    let bulk =
        run_one_live_prewarm(rpc_url, StorageFetchStrategy::default(), feeds.clone()).await?;
    println!(
        "live_prewarm_bulk requested_slots={} loaded_slots={} failed_slots={} elapsed_ms={}",
        bulk.report.requested_slots,
        bulk.report.loaded_slots,
        bulk.report.failed_slots.len(),
        bulk.elapsed.as_millis()
    );

    let point = run_one_live_prewarm(rpc_url, StorageFetchStrategy::PointRead, feeds).await?;
    println!(
        "live_prewarm_point requested_slots={} loaded_slots={} failed_slots={} elapsed_ms={}",
        point.report.requested_slots,
        point.report.loaded_slots,
        point.report.failed_slots.len(),
        point.elapsed.as_millis()
    );

    println!(
        "live_prewarm_summary bulk_ms={} point_ms={} speedup={:.2}x",
        bulk.elapsed.as_millis(),
        point.elapsed.as_millis(),
        ratio(point.elapsed, bulk.elapsed)
    );
    Ok(())
}

async fn run_fork_cache_call_breakdown(
    rpc_url: &str,
    feeds: &[Feed],
) -> Result<(), Box<dyn Error>> {
    let provider = Arc::new(RootProvider::<AnyNetwork>::connect(rpc_url).await?);
    let mut cache = EvmCache::builder(provider)
        .storage_fetch_strategy(StorageFetchStrategy::default())
        .build()
        .await;
    let proxies = feeds
        .iter()
        .filter_map(|feed| feed.proxy())
        .collect::<Vec<_>>();
    let core_calls = chainlink_core_calls(&proxies);

    let started = Instant::now();
    let core_results = execute_batched(&mut cache, core_calls)?;
    let core_elapsed = started.elapsed();
    let core = decode_core_results(&core_results);
    println!(
        "fork_cache_multicall_core proxies={} calls={} successful_feeds={} aggregators={} elapsed_ms={}",
        proxies.len(),
        proxies.len() * 5,
        core.successful,
        core.aggregators.len(),
        core_elapsed.as_millis()
    );

    let started = Instant::now();
    let mut sequential_layouts = 0usize;
    for aggregator in &core.aggregators {
        if cache
            .call_sol(*aggregator, typeAndVersionCall {})
            .map(|_: String| ())
            .is_ok()
        {
            sequential_layouts += 1;
        }
    }
    let sequential_layout_elapsed = started.elapsed();
    println!(
        "fork_cache_sequential_layout aggregators={} successful_layouts={} elapsed_ms={}",
        core.aggregators.len(),
        sequential_layouts,
        sequential_layout_elapsed.as_millis()
    );

    let provider = Arc::new(RootProvider::<AnyNetwork>::connect(rpc_url).await?);
    let mut cache = EvmCache::builder(provider)
        .storage_fetch_strategy(StorageFetchStrategy::default())
        .build()
        .await;
    let _ = execute_batched(&mut cache, chainlink_core_calls(&proxies))?;
    let started = Instant::now();
    let layout_results = execute_batched(
        &mut cache,
        core.aggregators
            .iter()
            .map(|aggregator| multicall_call(*aggregator, typeAndVersionCall {}, true)),
    )?;
    let batched_layout_elapsed = started.elapsed();
    let batched_layouts = layout_results
        .iter()
        .filter(|result| try_decode::<typeAndVersionCall>(result).is_some())
        .count();
    println!(
        "fork_cache_multicall_layout aggregators={} successful_layouts={} elapsed_ms={}",
        core.aggregators.len(),
        batched_layouts,
        batched_layout_elapsed.as_millis()
    );

    println!(
        "fork_cache_call_summary core_ms={} sequential_layout_ms={} batched_layout_ms={} current_total_ms={} batched_total_ms={}",
        core_elapsed.as_millis(),
        sequential_layout_elapsed.as_millis(),
        batched_layout_elapsed.as_millis(),
        (core_elapsed + sequential_layout_elapsed).as_millis(),
        (core_elapsed + batched_layout_elapsed).as_millis()
    );
    Ok(())
}

async fn run_direct_rpc_multicall_breakdown(
    rpc_url: &str,
    feeds: &[Feed],
) -> Result<(), Box<dyn Error>> {
    let provider = RootProvider::<AnyNetwork>::connect(rpc_url).await?;
    let proxies = feeds
        .iter()
        .filter_map(|feed| feed.proxy())
        .collect::<Vec<_>>();

    run_direct_rpc_proxy_group(
        &provider,
        "latest_round",
        &proxies,
        |proxy| [multicall_call(proxy, latestRoundDataCall {}, true)],
        |chunk| try_decode::<latestRoundDataCall>(&chunk[0]).is_some(),
    )
    .await?;
    run_direct_rpc_proxy_group(
        &provider,
        "aggregator",
        &proxies,
        |proxy| [multicall_call(proxy, aggregatorCall {}, true)],
        |chunk| try_decode::<aggregatorCall>(&chunk[0]).is_some(),
    )
    .await?;
    run_direct_rpc_proxy_group(
        &provider,
        "metadata",
        &proxies,
        |proxy| {
            [
                multicall_call(proxy, decimalsCall {}, true),
                multicall_call(proxy, descriptionCall {}, true),
                multicall_call(proxy, versionCall {}, true),
            ]
        },
        |chunk| {
            chunk.len() == 3
                && try_decode::<decimalsCall>(&chunk[0]).is_some()
                && try_decode::<descriptionCall>(&chunk[1]).is_some()
                && try_decode::<versionCall>(&chunk[2]).is_some()
        },
    )
    .await?;

    let started = Instant::now();
    let core = rpc_multicall_chainlink_core(&provider, &proxies).await?;
    let core_elapsed = started.elapsed();
    println!(
        "direct_rpc_multicall_core proxies={} calls={} successful_feeds={} aggregators={} elapsed_ms={}",
        proxies.len(),
        proxies.len() * 5,
        core.successful,
        core.aggregators.len(),
        core_elapsed.as_millis()
    );

    let started = Instant::now();
    let layout = rpc_multicall_type_and_version(&provider, &core.aggregators).await?;
    let layout_elapsed = started.elapsed();
    println!(
        "direct_rpc_multicall_layout aggregators={} calls={} successful_layouts={} elapsed_ms={}",
        core.aggregators.len(),
        core.aggregators.len(),
        layout.successful,
        layout_elapsed.as_millis()
    );

    println!(
        "direct_rpc_multicall_summary core_ms={} layout_ms={} total_ms={}",
        core_elapsed.as_millis(),
        layout_elapsed.as_millis(),
        (core_elapsed + layout_elapsed).as_millis()
    );
    Ok(())
}

async fn run_direct_rpc_proxy_group<const N: usize, F, D>(
    provider: &RootProvider<AnyNetwork>,
    label: &str,
    proxies: &[Address],
    calls_for_proxy: F,
    decode_proxy: D,
) -> Result<(), Box<dyn Error>>
where
    F: Fn(Address) -> [(Address, Bytes, bool); N],
    D: Fn(&[IMulticall3::Result]) -> bool,
{
    let calls = proxies
        .iter()
        .flat_map(|proxy| calls_for_proxy(*proxy))
        .collect::<Vec<_>>();
    let started = Instant::now();
    let results = rpc_multicall(provider, calls).await?;
    let elapsed = started.elapsed();
    let successful = results
        .chunks_exact(N)
        .filter(|chunk| decode_proxy(chunk))
        .count();
    println!(
        "direct_rpc_multicall_group label={} proxies={} calls={} successful={} elapsed_ms={}",
        label,
        proxies.len(),
        proxies.len() * N,
        successful,
        elapsed.as_millis()
    );
    Ok(())
}

fn chainlink_core_calls(proxies: &[Address]) -> Vec<(Address, Bytes, bool)> {
    proxies
        .iter()
        .flat_map(|proxy| {
            [
                multicall_call(*proxy, decimalsCall {}, true),
                multicall_call(*proxy, descriptionCall {}, true),
                multicall_call(*proxy, versionCall {}, true),
                multicall_call(*proxy, latestRoundDataCall {}, true),
                multicall_call(*proxy, aggregatorCall {}, true),
            ]
        })
        .collect::<Vec<_>>()
}

fn decode_core_results(results: &[IMulticall3::Result]) -> RpcCoreBreakdown {
    let mut successful = 0usize;
    let mut aggregators = Vec::new();
    for chunk in results.chunks_exact(5) {
        let decimals = try_decode::<decimalsCall>(&chunk[0]);
        let description = try_decode::<descriptionCall>(&chunk[1]);
        let version = try_decode::<versionCall>(&chunk[2]);
        let round = try_decode::<latestRoundDataCall>(&chunk[3]);
        let aggregator = try_decode::<aggregatorCall>(&chunk[4]);
        if decimals.is_some() && description.is_some() && version.is_some() && round.is_some() {
            successful += 1;
        }
        if let Some(aggregator) = aggregator {
            aggregators.push(aggregator);
        }
    }

    RpcCoreBreakdown {
        successful,
        aggregators,
    }
}

async fn rpc_multicall_chainlink_core(
    provider: &RootProvider<AnyNetwork>,
    proxies: &[Address],
) -> Result<RpcCoreBreakdown, Box<dyn Error>> {
    let calls = chainlink_core_calls(proxies);
    let results = rpc_multicall(provider, calls).await?;
    Ok(decode_core_results(&results))
}

async fn rpc_multicall_type_and_version(
    provider: &RootProvider<AnyNetwork>,
    aggregators: &[Address],
) -> Result<RpcLayoutBreakdown, Box<dyn Error>> {
    let results = rpc_multicall(
        provider,
        aggregators
            .iter()
            .map(|aggregator| multicall_call(*aggregator, typeAndVersionCall {}, true)),
    )
    .await?;
    let successful = results
        .iter()
        .filter(|result| try_decode::<typeAndVersionCall>(result).is_some())
        .count();
    Ok(RpcLayoutBreakdown { successful })
}

async fn rpc_multicall<I>(
    provider: &RootProvider<AnyNetwork>,
    calls: I,
) -> Result<Vec<IMulticall3::Result>, Box<dyn Error>>
where
    I: IntoIterator<Item = (Address, Bytes, bool)>,
{
    let calls = calls
        .into_iter()
        .map(|(target, call_data, allow_failure)| IMulticall3::Call3 {
            target,
            allowFailure: allow_failure,
            callData: call_data,
        })
        .collect::<Vec<_>>();
    if calls.is_empty() {
        return Ok(Vec::new());
    }

    let call = IMulticall3::aggregate3Call { calls };
    let tx = TransactionRequest::default()
        .with_to(MULTICALL3_ADDRESS)
        .with_input(Bytes::from(call.abi_encode()));
    let bytes = provider.call(tx.into()).await?;
    Ok(IMulticall3::aggregate3Call::abi_decode_returns(&bytes)?)
}

fn multicall_call<C: SolCall>(
    target: Address,
    call: C,
    allow_failure: bool,
) -> (Address, Bytes, bool) {
    (target, call.abi_encode().into(), allow_failure)
}

fn try_decode<C: SolCall>(result: &IMulticall3::Result) -> Option<C::Return> {
    if !result.success {
        return None;
    }
    C::abi_decode_returns(&result.returnData).ok()
}

async fn run_one_live_registration(
    rpc_url: &str,
    strategy: StorageFetchStrategy,
    feeds: Vec<Feed>,
    multicall_reads: bool,
) -> Result<LiveRegistrationResult, Box<dyn Error>> {
    let provider = Arc::new(RootProvider::<AnyNetwork>::connect(rpc_url).await?);
    let mut cache = EvmCache::builder(provider)
        .storage_fetch_strategy(strategy)
        .build()
        .await;
    let started = Instant::now();
    let adapter_report = OracleAdapter::builder()
        .feeds(feeds)
        .multicall_reads(multicall_reads)
        .now_timestamp(now_timestamp()?)
        .build_report(&mut cache)
        .await?;
    Ok(LiveRegistrationResult {
        elapsed: started.elapsed(),
        registered_feeds: adapter_report.tracker.registrations().count(),
        skipped_feeds: adapter_report.skipped.len(),
    })
}

async fn run_one_live_prewarm(
    rpc_url: &str,
    strategy: StorageFetchStrategy,
    feeds: Vec<Feed>,
) -> Result<LivePrewarmResult, Box<dyn Error>> {
    let provider = Arc::new(RootProvider::<AnyNetwork>::connect(rpc_url).await?);
    let mut cache = EvmCache::builder(provider)
        .storage_fetch_strategy(strategy)
        .build()
        .await;
    let adapter_report = OracleAdapter::builder()
        .feeds(feeds)
        .now_timestamp(now_timestamp()?)
        .build_report(&mut cache)
        .await?;
    if !adapter_report.skipped.is_empty() {
        println!(
            "live_prewarm_skipped feeds={}",
            adapter_report.skipped.len()
        );
        for skipped in &adapter_report.skipped {
            let label = skipped
                .feed
                .label()
                .map(str::to_string)
                .or_else(|| skipped.feed.id().map(|id| id.to_string()))
                .unwrap_or_else(|| "unknown".to_string());
            println!(
                "live_prewarm_skip label={} proxy={:?} reason={}",
                label, skipped.proxy, skipped.reason
            );
        }
    }
    let runtime = OracleRuntime::from_tracker(adapter_report.tracker);
    let started = Instant::now();
    let report = runtime.prewarm_storage(&mut cache);
    Ok(LivePrewarmResult {
        elapsed: started.elapsed(),
        report,
    })
}

#[derive(Debug)]
struct LiveRegistrationResult {
    elapsed: Duration,
    registered_feeds: usize,
    skipped_feeds: usize,
}

#[derive(Debug)]
struct LivePrewarmResult {
    elapsed: Duration,
    report: evm_oracle_state::OracleStorageWarmupReport,
}

#[derive(Debug)]
struct RpcCoreBreakdown {
    successful: usize,
    aggregators: Vec<Address>,
}

#[derive(Debug)]
struct RpcLayoutBreakdown {
    successful: usize,
}

fn benchmark_feeds() -> Result<Vec<Feed>, Box<dyn Error>> {
    let specs = benchmark_feed_specs()?;
    let mut seen = BTreeSet::new();
    let unique = specs.iter().filter(|spec| seen.insert(spec.proxy)).count();
    println!(
        "live_prewarm_fixture feeds={} unique_feeds={} source={}",
        specs.len(),
        unique,
        benchmark_feed_source()
    );

    Ok(specs
        .into_iter()
        .enumerate()
        .map(|(index, spec)| {
            Feed::proxy(spec.proxy)
                .id(format!("bench-feed-{index}"))
                .label(spec.label)
                .base(AssetId::symbol(spec.base))
                .quote(if spec.quote.eq_ignore_ascii_case("USD") {
                    Denomination::Usd
                } else {
                    Denomination::from(spec.quote)
                })
                .max_age_secs(86_400)
        })
        .collect())
}

fn benchmark_feed_source() -> String {
    if env::var("ORACLE_BENCH_FEEDS").is_ok() {
        return "ORACLE_BENCH_FEEDS".to_string();
    }
    if let Ok(path) = env::var("ORACLE_BENCH_FEEDS_FILE") {
        return format!("file:{path}");
    }
    env::var("ORACLE_BENCH_FEED_SET")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "smoke".to_string())
}

fn benchmark_feed_specs() -> Result<Vec<BenchFeedSpec>, Box<dyn Error>> {
    let mut specs = if let Ok(value) = env::var("ORACLE_BENCH_FEEDS") {
        value
            .split(',')
            .filter(|item| !item.trim().is_empty())
            .enumerate()
            .map(|(index, item)| {
                let proxy = Address::from_str(item.trim())?;
                Ok(BenchFeedSpec {
                    label: format!("Bench feed {index}"),
                    base: format!("ASSET{index}"),
                    quote: "USD".to_string(),
                    proxy,
                })
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?
    } else if let Ok(path) = env::var("ORACLE_BENCH_FEEDS_FILE") {
        benchmark_feed_specs_from_file(&path)?
    } else {
        match env::var("ORACLE_BENCH_FEED_SET").as_deref() {
            Ok("ethereum-mainnet-curated") | Ok("curated") | Ok("large") => {
                static_benchmark_specs(CURATED_MAINNET_FEED_SET)?
            }
            Ok("smoke") | Err(_) => static_benchmark_specs(SMOKE_FEED_SET)?,
            Ok(other) => {
                return Err(format!(
                    "unknown ORACLE_BENCH_FEED_SET `{other}`; use `smoke` or `ethereum-mainnet-curated`"
                )
                .into());
            }
        }
    };

    if let Some(limit) = env_u64("ORACLE_BENCH_FEED_LIMIT") {
        specs.truncate(limit as usize);
    }
    Ok(specs)
}

fn static_benchmark_specs(
    specs: &[StaticBenchFeedSpec],
) -> Result<Vec<BenchFeedSpec>, Box<dyn Error>> {
    specs.iter().copied().map(BenchFeedSpec::try_from).collect()
}

fn benchmark_feed_specs_from_file(path: &str) -> Result<Vec<BenchFeedSpec>, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;
    let mut specs = Vec::new();
    for (line_index, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
        let spec = match fields.as_slice() {
            [proxy] => BenchFeedSpec {
                label: format!("Bench feed {}", specs.len()),
                base: format!("ASSET{}", specs.len()),
                quote: "USD".to_string(),
                proxy: Address::from_str(proxy)?,
            },
            [label, base, quote, proxy] => BenchFeedSpec {
                label: (*label).to_string(),
                base: (*base).to_string(),
                quote: (*quote).to_string(),
                proxy: Address::from_str(proxy)?,
            },
            _ => {
                return Err(format!(
                    "invalid feed fixture line {}: expected `proxy` or `label,base,quote,proxy`",
                    line_index + 1
                )
                .into());
            }
        };
        specs.push(spec);
    }
    Ok(specs)
}

#[derive(Clone, Debug)]
struct BenchFeedSpec {
    label: String,
    base: String,
    quote: String,
    proxy: Address,
}

#[derive(Clone, Copy, Debug)]
struct StaticBenchFeedSpec {
    label: &'static str,
    base: &'static str,
    quote: &'static str,
    proxy: &'static str,
}

impl StaticBenchFeedSpec {
    const fn new(
        label: &'static str,
        base: &'static str,
        quote: &'static str,
        proxy: &'static str,
    ) -> Self {
        Self {
            label,
            base,
            quote,
            proxy,
        }
    }
}

impl TryFrom<StaticBenchFeedSpec> for BenchFeedSpec {
    type Error = Box<dyn Error>;

    fn try_from(value: StaticBenchFeedSpec) -> Result<Self, Self::Error> {
        Ok(Self {
            label: value.label.to_string(),
            base: value.base.to_string(),
            quote: value.quote.to_string(),
            proxy: Address::from_str(value.proxy)?,
        })
    }
}

fn ocr2_registration(proxy: Address, aggregator: Address) -> FeedRegistration {
    FeedRegistration {
        id: FeedId::new("eth-usd"),
        proxy,
        label: Some("ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        staleness: StalenessPolicy::max_age(3_600),
        current_aggregator: Some(aggregator),
        aggregator_layout: Some(AggregatorLayoutEvidence::from_type_and_version(
            aggregator,
            "AccessControlledOCR2Aggregator 1.0.0",
            None,
        )),
        metadata: FeedMetadata {
            decimals: 8,
            description: "ETH / USD".to_string(),
            version: U256::from(4_u64),
        },
        source: FeedSource::Chainlink,
        status: OracleFeedStatus::Ready,
    }
}

fn price_update(proxy: Address, aggregator: Address) -> OraclePriceUpdate {
    OraclePriceUpdate {
        id: FeedId::new("eth-usd"),
        proxy,
        aggregator,
        label: Some("ETH/USD".to_string()),
        base: Some("ETH".to_string()),
        quote: Some("USD".to_string()),
        raw_answer: I256::unchecked_from(2_100_000_000_i64),
        decimals: 8,
        event_round_id: U256::from(101_u64),
        started_at: 1_700_000_100,
        updated_at: 1_700_000_100,
        block_number: Some(120),
        block_hash: Some(B256::repeat_byte(0x12)),
        log_index: Some(0),
        round_status: OracleRoundStatus::Fresh,
        value_status: OracleValueStatus::EventPending,
        source: OracleValueSource::Event,
    }
}

#[derive(Default)]
struct MapStateView {
    slots: BTreeMap<(Address, U256), U256>,
}

impl MapStateView {
    fn with_slot(mut self, address: Address, slot: U256, value: U256) -> Self {
        self.slots.insert((address, slot), value);
        self
    }
}

impl StateView for MapStateView {
    fn storage(&self, address: Address, slot: U256) -> Option<U256> {
        self.slots.get(&(address, slot)).copied()
    }
}

fn time_loop(iterations: usize, mut f: impl FnMut() -> bool) -> Duration {
    let start = Instant::now();
    let mut matches = 0usize;
    for _ in 0..iterations {
        if f() {
            matches += 1;
        }
    }
    black_box(matches);
    start.elapsed()
}

fn per_iter_ns(duration: Duration, iterations: usize) -> u128 {
    duration.as_nanos() / iterations.max(1) as u128
}

fn ratio(numerator: Duration, denominator: Duration) -> f64 {
    let denominator = denominator.as_secs_f64();
    if denominator == 0.0 {
        return 0.0;
    }
    numerator.as_secs_f64() / denominator
}

fn now_timestamp() -> Result<u64, Box<dyn Error>> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn env_u64(key: &str) -> Option<u64> {
    env::var(key).ok()?.parse().ok()
}

fn env_bool(key: &str) -> bool {
    env::var(key)
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}
