#[cfg(all(feature = "morpho", feature = "euler"))]
use std::{
    env,
    error::Error,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(all(feature = "morpho", feature = "euler"))]
use evm_oracle_state::alloy_network::{AnyNetwork, Ethereum};
#[cfg(all(feature = "morpho", feature = "euler"))]
use alloy_primitives::{Address, B256, I256, U256, address};
#[cfg(all(feature = "morpho", feature = "euler"))]
use evm_oracle_state::alloy_provider::{ProviderBuilder, RootProvider, WsConnect};
#[cfg(all(feature = "morpho", feature = "euler"))]
use alloy_sol_types::sol;
#[cfg(all(feature = "morpho", feature = "euler"))]
use evm_fork_cache::{
    cache::EvmCache,
    reactive::{AlloySubscriber, ReactiveRuntime, SubscriberConfig, SubscriberMode},
};
#[cfg(all(feature = "morpho", feature = "euler"))]
use evm_oracle_state::{
    AssetId, Denomination, EulerOracleAdapter, EulerQuotePair, MorphoBlueMarket,
    MorphoBlueOracleAdapter, OracleError, OracleHookEvent, OraclePrice, OraclePriceUpdate,
    OracleRuntime,
};
#[cfg(all(feature = "morpho", feature = "euler"))]
use tokio::time::timeout;

#[cfg(all(feature = "morpho", feature = "euler"))]
sol! {
    interface MorphoBlueInterface {
        function idToMarketParams(bytes32 id) external view returns (
            address loanToken,
            address collateralToken,
            address oracle,
            address irm,
            uint256 lltv
        );
    }

    interface IERC20Metadata {
        function decimals() external view returns (uint8);
    }
}

#[cfg(all(feature = "morpho", feature = "euler"))]
use IERC20Metadata::decimalsCall;
#[cfg(all(feature = "morpho", feature = "euler"))]
use MorphoBlueInterface::idToMarketParamsCall;

#[cfg(all(feature = "morpho", feature = "euler"))]
const DEFAULT_MORPHO_BLUE: Address = address!("BBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb");
#[cfg(all(feature = "morpho", feature = "euler"))]
const DEFAULT_WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
#[cfg(all(feature = "morpho", feature = "euler"))]
const DEFAULT_USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
#[cfg(all(feature = "morpho", feature = "euler"))]
const WAD: u128 = 1_000_000_000_000_000_000;
#[cfg(all(feature = "morpho", feature = "euler"))]
const MORPHO_PRICE_SCALE: u128 = 1_000_000_000_000_000_000_000_000_000_000_000_000;

#[cfg(all(feature = "morpho", feature = "euler"))]
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let Some(rpc_url) = env::var("ETH_RPC_URL")
        .ok()
        .or_else(|| env::var("E2E_RPC_URL").ok())
    else {
        println!("ETH_RPC_URL/E2E_RPC_URL unset; skipping Morpho/Euler reactive value demo.");
        return Ok(());
    };
    let Some(ws_url) = websocket_rpc_url(&rpc_url) else {
        println!(
            "ETH_WS_RPC_URL/WS_RPC_URL unset and RPC URL is not websocket; skipping live tracking."
        );
        return Ok(());
    };

    let provider = Arc::new(RootProvider::<AnyNetwork>::connect(&rpc_url).await?);
    let mut cache = EvmCache::new(provider).await;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let config = DemoConfig::from_env(&mut cache)?;
    let mut builder = OracleRuntime::cache_builder().now_timestamp(now);

    if let Some(adapter) = config.morpho_adapter() {
        builder = builder.install_adapter(adapter);
    }
    if let Some(adapter) = config.euler_adapter() {
        builder = builder.install_adapter(adapter);
    }

    let report = builder.build_report(&mut cache).await?;
    let mut oracle_runtime = report.runtime;
    let mut reactive_runtime = oracle_runtime.reactive_runtime()?;

    println!("Morpho/Euler reactive value demo");
    println!(
        "configured feeds: {}",
        oracle_runtime.tracker().registrations().count()
    );
    println!(
        "reactive interests: {}",
        oracle_runtime.reactive_interests().len()
    );
    for skipped in &report.skipped {
        println!("skipped proxy={:?}: {}", skipped.proxy, skipped.reason);
    }
    for registration in oracle_runtime.tracker().registrations() {
        println!(
            "registered feed={} label=\"{}\" proxy={:?} aggregator={:?}",
            registration.id,
            registration.label.as_deref().unwrap_or(""),
            registration.proxy,
            registration.current_aggregator
        );
    }

    print_value_state(&oracle_runtime, &config, "cold_start")?;

    if oracle_runtime.reactive_interests().is_empty() {
        println!("No reactive interests were registered; nothing to subscribe to.");
        return Ok(());
    }

    run_live_tracking(
        &ws_url,
        &mut cache,
        &mut oracle_runtime,
        &mut reactive_runtime,
        &config,
    )
    .await
}

#[cfg(not(all(feature = "morpho", feature = "euler")))]
fn main() {
    println!(
        "Enable both optional adapters to run this example: cargo run --features morpho,euler --example morpho_euler_reactive_value"
    );
}

#[cfg(all(feature = "morpho", feature = "euler"))]
#[derive(Clone, Debug)]
struct DemoConfig {
    morpho: Option<MorphoConfig>,
    euler: Option<EulerConfig>,
}

#[cfg(all(feature = "morpho", feature = "euler"))]
#[derive(Clone, Debug)]
struct MorphoConfig {
    morpho_blue: Address,
    market_id: Option<B256>,
    oracle: Option<Address>,
    lltv_wad: U256,
    collateral_raw: U256,
    debt_raw: U256,
    feed_id: String,
}

#[cfg(all(feature = "morpho", feature = "euler"))]
#[derive(Clone, Debug)]
struct EulerConfig {
    router: Option<Address>,
    oracle: Option<Address>,
    base: Address,
    quote: Address,
    base_decimals: u8,
    ltv_wad: U256,
    collateral_raw: U256,
    debt_raw: U256,
    feed_id: String,
}

#[cfg(all(feature = "morpho", feature = "euler"))]
impl DemoConfig {
    fn from_env(cache: &mut EvmCache) -> Result<Self, OracleError> {
        let morpho = MorphoConfig::from_env(cache)?;
        let euler = EulerConfig::from_env(cache)?;
        if morpho.is_none() && euler.is_none() {
            println!(
                "No Morpho/Euler feed configured. Set MORPHO_MARKET_ID or MORPHO_ORACLE, and/or EULER_ROUTER or EULER_ORACLE."
            );
        }
        Ok(Self { morpho, euler })
    }

    fn morpho_adapter(&self) -> Option<MorphoBlueOracleAdapter> {
        self.morpho.as_ref().map(|config| {
            if let Some(market_id) = config.market_id {
                MorphoBlueOracleAdapter::new(config.morpho_blue).market(
                    MorphoBlueMarket::market_id(market_id)
                        .id(config.feed_id.clone())
                        .label("Morpho reactive market")
                        .base(AssetId::symbol("collateral"))
                        .quote(Denomination::from("loan"))
                        .max_age_secs(24 * 60 * 60),
                )
            } else {
                MorphoBlueOracleAdapter::default().market(
                    MorphoBlueMarket::oracle(config.oracle.expect("morpho oracle"))
                        .id(config.feed_id.clone())
                        .label("Morpho reactive oracle")
                        .base(AssetId::symbol("collateral"))
                        .quote(Denomination::from("loan"))
                        .max_age_secs(24 * 60 * 60),
                )
            }
        })
    }

    fn euler_adapter(&self) -> Option<EulerOracleAdapter> {
        self.euler.as_ref().map(|config| {
            let pair = if let Some(oracle) = config.oracle {
                EulerQuotePair::with_oracle(config.base, config.quote, oracle)
            } else {
                EulerQuotePair::new(config.base, config.quote)
            }
            .id(config.feed_id.clone())
            .label("Euler reactive quote")
            .base_label(AssetId::symbol("base"))
            .quote_label(Denomination::from("quote"))
            .max_age_secs(24 * 60 * 60);

            if let Some(router) = config.router {
                EulerOracleAdapter::router(router).quote_pair(pair)
            } else {
                EulerOracleAdapter::new().quote_pair(pair)
            }
        })
    }
}

#[cfg(all(feature = "morpho", feature = "euler"))]
impl MorphoConfig {
    fn from_env(cache: &mut EvmCache) -> Result<Option<Self>, OracleError> {
        let market_id = env_b256("MORPHO_MARKET_ID")?;
        let oracle = env_address("MORPHO_ORACLE")?;
        if market_id.is_none() && oracle.is_none() {
            return Ok(None);
        }

        let morpho_blue = env_address("MORPHO_BLUE")?.unwrap_or(DEFAULT_MORPHO_BLUE);
        let lltv_wad = if let Some(lltv) = env_u256("MORPHO_LLTV_WAD")? {
            lltv
        } else if let Some(id) = market_id {
            cache
                .call_sol(morpho_blue, idToMarketParamsCall { id })
                .map_err(provider_error)?
                .lltv
        } else {
            U256::from(860_000_000_000_000_000_u128)
        };

        Ok(Some(Self {
            morpho_blue,
            market_id,
            oracle,
            lltv_wad,
            collateral_raw: env_u256("MORPHO_COLLATERAL_RAW")?
                .unwrap_or(U256::from(1_000_000_000_000_000_000_u128)),
            debt_raw: env_u256("MORPHO_DEBT_RAW")?.unwrap_or(U256::from(1_500_000_000_u64)),
            feed_id: env::var("MORPHO_FEED_ID").unwrap_or_else(|_| "morpho-demo".to_string()),
        }))
    }
}

#[cfg(all(feature = "morpho", feature = "euler"))]
impl EulerConfig {
    fn from_env(cache: &mut EvmCache) -> Result<Option<Self>, OracleError> {
        let router = env_address("EULER_ROUTER")?;
        let oracle = env_address("EULER_ORACLE")?;
        if router.is_none() && oracle.is_none() {
            return Ok(None);
        }
        let base = env_address("EULER_BASE")?.unwrap_or(DEFAULT_WETH);
        let quote = env_address("EULER_QUOTE")?.unwrap_or(DEFAULT_USDC);
        let base_decimals = env_u8("EULER_BASE_DECIMALS")?
            .or_else(|| read_decimals(cache, base).ok())
            .unwrap_or(18);

        Ok(Some(Self {
            router,
            oracle,
            base,
            quote,
            base_decimals,
            ltv_wad: env_u256("EULER_LTV_WAD")?.unwrap_or(U256::from(800_000_000_000_000_000_u128)),
            collateral_raw: env_u256("EULER_COLLATERAL_RAW")?
                .unwrap_or(U256::from(1_000_000_000_000_000_000_u128)),
            debt_raw: env_u256("EULER_DEBT_RAW")?.unwrap_or(U256::from(1_500_000_000_u64)),
            feed_id: env::var("EULER_FEED_ID").unwrap_or_else(|_| "euler-demo".to_string()),
        }))
    }
}

#[cfg(all(feature = "morpho", feature = "euler"))]
async fn run_live_tracking(
    ws_url: &str,
    cache: &mut EvmCache,
    oracle_runtime: &mut OracleRuntime<()>,
    reactive_runtime: &mut ReactiveRuntime<Ethereum>,
    config: &DemoConfig,
) -> Result<(), Box<dyn Error>> {
    let duration = env_u64("MORPHO_EULER_DEMO_DURATION_SECS").unwrap_or(300);
    let idle_timeout = env_u64("MORPHO_EULER_DEMO_IDLE_TIMEOUT_SECS").unwrap_or(30);
    if duration == 0 {
        println!("live tracking skipped because MORPHO_EULER_DEMO_DURATION_SECS=0");
        return Ok(());
    }

    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(ws_url.to_string()))
        .await?;
    let mut subscriber = AlloySubscriber::new(
        provider,
        SubscriberMode::Auto,
        SubscriberConfig {
            max_batch_size: 256,
            ..SubscriberConfig::default()
        },
    );
    oracle_runtime.register_subscriber(&mut subscriber).await?;

    println!("\nReactive Morpho/Euler tracking duration={duration}s idle_timeout={idle_timeout}s");

    let start = Instant::now();
    let mut batches = 0usize;
    let mut live_hooks = 0usize;
    while start.elapsed() < Duration::from_secs(duration) {
        let remaining = Duration::from_secs(duration).saturating_sub(start.elapsed());
        let wait_for = remaining.min(Duration::from_secs(idle_timeout));
        let Some(events) = (match timeout(
            wait_for,
            oracle_runtime.next_subscriber_events(cache, reactive_runtime, &mut subscriber),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                println!(
                    "subscription_wait timed out after {}s without an oracle update batch",
                    wait_for.as_secs()
                );
                continue;
            }
        }) else {
            println!("subscriber ended without another oracle batch");
            break;
        };

        batches += 1;
        live_hooks += events.events.len();
        println!("subscription_batch events={}", events.events.len());
        for event in events.events {
            if let OracleHookEvent::PriceUpdate(update) = event {
                print_price_update(&update, "live_event");
                print_value_state(oracle_runtime, config, "live_event")?;
            }
        }
    }

    println!(
        "reactive_value_summary duration_secs={duration} batches={batches} live_hooks={live_hooks}"
    );
    if live_hooks == 0 {
        println!(
            "reactive_value_note no live Morpho/Euler oracle hooks arrived during this window"
        );
    }
    Ok(())
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn print_value_state(
    oracle_runtime: &OracleRuntime<()>,
    config: &DemoConfig,
    prefix: &str,
) -> Result<(), OracleError> {
    if let Some(morpho) = &config.morpho {
        match oracle_runtime.price(&morpho.feed_id) {
            Ok(price) => print_morpho_health(prefix, &price, morpho)?,
            Err(error) => println!("{prefix} morpho_price unavailable: {error}"),
        }
    }

    if let Some(euler) = &config.euler {
        match oracle_runtime.price(&euler.feed_id) {
            Ok(price) => print_euler_health(prefix, &price, euler)?,
            Err(error) => println!("{prefix} euler_price unavailable: {error}"),
        }
    }

    Ok(())
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn print_morpho_health(
    prefix: &str,
    price: &OraclePrice,
    config: &MorphoConfig,
) -> Result<(), OracleError> {
    let price_u256 = i256_to_u256(price.raw_answer, "morpho price")?;
    let collateral_loan_value = config
        .collateral_raw
        .checked_mul(price_u256)
        .and_then(|value| value.checked_div(U256::from(MORPHO_PRICE_SCALE)))
        .ok_or_else(|| OracleError::Provider("morpho value calculation overflowed".to_string()))?;
    let max_debt = collateral_loan_value
        .checked_mul(config.lltv_wad)
        .and_then(|value| value.checked_div(U256::from(WAD)))
        .ok_or_else(|| OracleError::Provider("morpho health calculation overflowed".to_string()))?;
    let hf = wad_ratio(max_debt, config.debt_raw);
    println!(
        "{prefix} morpho_value feed={} round_status={:?} value_status={:?} source={:?} price={} collateral_raw={} debt_raw={} lltv={} collateral_loan_value_raw={} max_debt_raw={} health_factor={}",
        price.id,
        price.round_status,
        price.value_status,
        price.source,
        price.raw_answer,
        config.collateral_raw,
        config.debt_raw,
        format_wad(config.lltv_wad),
        collateral_loan_value,
        max_debt,
        hf.map(format_wad).unwrap_or_else(|| "inf".to_string())
    );
    Ok(())
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn print_euler_health(
    prefix: &str,
    price: &OraclePrice,
    config: &EulerConfig,
) -> Result<(), OracleError> {
    let price_u256 = i256_to_u256(price.raw_answer, "euler price")?;
    let collateral_quote_value = config
        .collateral_raw
        .checked_mul(price_u256)
        .and_then(|value| value.checked_div(pow10_u256(config.base_decimals)))
        .ok_or_else(|| OracleError::Provider("euler value calculation overflowed".to_string()))?;
    let max_debt = collateral_quote_value
        .checked_mul(config.ltv_wad)
        .and_then(|value| value.checked_div(U256::from(WAD)))
        .ok_or_else(|| OracleError::Provider("euler health calculation overflowed".to_string()))?;
    let hf = wad_ratio(max_debt, config.debt_raw);
    println!(
        "{prefix} euler_value feed={} round_status={:?} value_status={:?} source={:?} quote_per_base={} collateral_raw={} debt_raw={} ltv={} collateral_quote_raw={} max_debt_raw={} health_factor={}",
        price.id,
        price.round_status,
        price.value_status,
        price.source,
        price.raw_answer,
        config.collateral_raw,
        config.debt_raw,
        format_wad(config.ltv_wad),
        collateral_quote_value,
        max_debt,
        hf.map(format_wad).unwrap_or_else(|| "inf".to_string())
    );
    Ok(())
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn print_price_update(update: &OraclePriceUpdate, prefix: &str) {
    println!(
        "{prefix} oracle_price_update feed={} label=\"{}\" proxy={:?} aggregator={:?} raw_answer={} decimals={} updated_at={} block={:?} round_status={:?} value_status={:?} source={:?}",
        update.id,
        update.label.as_deref().unwrap_or(""),
        update.proxy,
        update.aggregator,
        update.raw_answer,
        update.decimals,
        update.updated_at,
        update.block_number,
        update.round_status,
        update.value_status,
        update.source
    );
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn read_decimals(cache: &mut EvmCache, token: Address) -> Result<u8, OracleError> {
    cache
        .call_sol(token, decimalsCall {})
        .map_err(provider_error)
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn i256_to_u256(value: I256, field: &'static str) -> Result<U256, OracleError> {
    if value < I256::ZERO {
        return Err(OracleError::Provider(format!("{field} is negative")));
    }
    U256::from_str(&value.to_string())
        .map_err(|error| OracleError::Provider(format!("{field} does not fit U256: {error}")))
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn wad_ratio(numerator: U256, denominator: U256) -> Option<U256> {
    if denominator == U256::ZERO {
        return None;
    }
    numerator
        .checked_mul(U256::from(WAD))
        .and_then(|value| value.checked_div(denominator))
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn pow10_u256(decimals: u8) -> U256 {
    U256::from(10_u8).pow(U256::from(decimals))
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn format_wad(value: U256) -> String {
    let scaled = value.to_string().parse::<f64>().unwrap_or(f64::INFINITY) / 1e18;
    format!("{scaled:.9}")
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn env_address(key: &str) -> Result<Option<Address>, OracleError> {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse::<Address>()
                .map_err(|error| OracleError::Provider(format!("{key} is invalid: {error}")))
        })
        .transpose()
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn env_b256(key: &str) -> Result<Option<B256>, OracleError> {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse::<B256>()
                .map_err(|error| OracleError::Provider(format!("{key} is invalid: {error}")))
        })
        .transpose()
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn env_u256(key: &str) -> Result<Option<U256>, OracleError> {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            U256::from_str(value.trim())
                .map_err(|error| OracleError::Provider(format!("{key} is invalid: {error}")))
        })
        .transpose()
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn env_u64(key: &str) -> Option<u64> {
    env::var(key).ok()?.parse().ok()
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn env_u8(key: &str) -> Result<Option<u8>, OracleError> {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse::<u8>()
                .map_err(|error| OracleError::Provider(format!("{key} is invalid: {error}")))
        })
        .transpose()
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn websocket_rpc_url(primary_rpc_url: &str) -> Option<String> {
    env::var("ETH_WS_RPC_URL")
        .ok()
        .or_else(|| env::var("WS_RPC_URL").ok())
        .or_else(|| {
            primary_rpc_url
                .strip_prefix("https://")
                .map(|suffix| format!("wss://{suffix}"))
        })
        .or_else(|| {
            primary_rpc_url
                .strip_prefix("http://")
                .map(|suffix| format!("ws://{suffix}"))
        })
        .or_else(|| {
            primary_rpc_url
                .starts_with("ws")
                .then(|| primary_rpc_url.to_string())
        })
}

#[cfg(all(feature = "morpho", feature = "euler"))]
fn provider_error(error: impl std::fmt::Debug) -> OracleError {
    OracleError::Provider(format!("{error:?}"))
}
