#[cfg(feature = "pyth")]
use std::{
    env,
    error::Error,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "pyth")]
use evm_oracle_state::alloy_network::{AnyNetwork, Ethereum};
#[cfg(feature = "pyth")]
use alloy_primitives::{Address, B256};
#[cfg(feature = "pyth")]
use evm_oracle_state::alloy_provider::{ProviderBuilder, RootProvider, WsConnect};
#[cfg(feature = "pyth")]
use evm_fork_cache::{
    cache::EvmCache,
    reactive::{AlloySubscriber, ReactiveRuntime, SubscriberConfig, SubscriberMode},
};
#[cfg(feature = "pyth")]
use evm_oracle_state::{
    OracleHookEvent, OraclePriceUpdate, OracleRuntime, PythFeed, PythOracleAdapter,
};
#[cfg(feature = "pyth")]
use tokio::time::timeout;

#[cfg(feature = "pyth")]
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let Some(rpc_url) = env::var("ETH_RPC_URL")
        .ok()
        .or_else(|| env::var("E2E_RPC_URL").ok())
    else {
        println!("ETH_RPC_URL/E2E_RPC_URL unset; skipping Pyth push demo.");
        return Ok(());
    };
    let Some(ws_url) = websocket_rpc_url(&rpc_url) else {
        println!(
            "ETH_WS_RPC_URL/WS_RPC_URL unset and RPC URL is not websocket; skipping Pyth demo."
        );
        return Ok(());
    };
    let Some(config) = PythDemoConfig::from_env()? else {
        println!("Set PYTH_CONTRACT and PYTH_PRICE_ID to run the Pyth push demo.");
        return Ok(());
    };

    let provider = Arc::new(RootProvider::<AnyNetwork>::connect(&rpc_url).await?);
    let mut cache = EvmCache::new(provider).await;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let feed = PythFeed::new(config.pyth, config.price_id)
        .id(config.feed_id)
        .label(config.label)
        .base(config.base)
        .quote(config.quote)
        .max_age_secs(config.max_age_secs);
    let mut oracle_runtime = OracleRuntime::cache_builder()
        .now_timestamp(now)
        .install_adapter(PythOracleAdapter::new().feed(feed))
        .build(&mut cache)
        .await?;
    let mut reactive_runtime = oracle_runtime.reactive_runtime()?;

    println!("Pyth push oracle demo");
    for registration in oracle_runtime.tracker().registrations() {
        println!(
            "registered feed={} proxy_key={:?} pyth={:?}",
            registration.id, registration.proxy, registration.current_aggregator
        );
        let price = oracle_runtime.price_by_proxy(registration.proxy)?;
        println!(
            "cold_start feed={} answer={} decimals={} updated_at={}",
            price.id, price.raw_answer, price.decimals, price.updated_at
        );
    }

    run_live_tracking(
        &ws_url,
        &mut cache,
        &mut oracle_runtime,
        &mut reactive_runtime,
    )
    .await
}

#[cfg(not(feature = "pyth"))]
fn main() {
    println!(
        "Enable the Pyth adapter to run this example: cargo run --features pyth --example pyth_push_demo"
    );
}

#[cfg(feature = "pyth")]
#[derive(Clone, Debug)]
struct PythDemoConfig {
    pyth: Address,
    price_id: B256,
    feed_id: String,
    label: String,
    base: String,
    quote: String,
    max_age_secs: u64,
}

#[cfg(feature = "pyth")]
impl PythDemoConfig {
    fn from_env() -> Result<Option<Self>, Box<dyn Error>> {
        let Some(pyth) = env::var("PYTH_CONTRACT").ok() else {
            return Ok(None);
        };
        let Some(price_id) = env::var("PYTH_PRICE_ID").ok() else {
            return Ok(None);
        };
        let base = env::var("PYTH_BASE").unwrap_or_else(|_| "ASSET".to_string());
        let quote = env::var("PYTH_QUOTE").unwrap_or_else(|_| "USD".to_string());
        Ok(Some(Self {
            pyth: Address::from_str(&pyth)?,
            price_id: B256::from_str(&price_id)?,
            feed_id: env::var("PYTH_FEED_ID").unwrap_or_else(|_| {
                format!("pyth-{}-{}", base.to_lowercase(), quote.to_lowercase())
            }),
            label: env::var("PYTH_LABEL").unwrap_or_else(|_| format!("Pyth {base}/{quote}")),
            base,
            quote,
            max_age_secs: env_u64("PYTH_MAX_AGE_SECS").unwrap_or(300),
        }))
    }
}

#[cfg(feature = "pyth")]
async fn run_live_tracking(
    ws_url: &str,
    cache: &mut EvmCache,
    oracle_runtime: &mut OracleRuntime<()>,
    reactive_runtime: &mut ReactiveRuntime<Ethereum>,
) -> Result<(), Box<dyn Error>> {
    let duration = env_u64("PYTH_DEMO_DURATION_SECS").unwrap_or(300);
    let idle_timeout = env_u64("PYTH_DEMO_IDLE_TIMEOUT_SECS").unwrap_or(30);
    if duration == 0 {
        println!("live tracking skipped because PYTH_DEMO_DURATION_SECS=0");
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

    println!("Reactive Pyth tracking duration={duration}s idle_timeout={idle_timeout}s");

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
                    "subscription_wait timed out after {}s without a Pyth update batch",
                    wait_for.as_secs()
                );
                continue;
            }
        }) else {
            println!("subscriber ended without another Pyth batch");
            break;
        };

        batches += 1;
        live_hooks += events.events.len();
        println!("subscription_batch events={}", events.events.len());
        for event in events.events {
            if let OracleHookEvent::PriceUpdate(update) = event {
                print_price_update(&update);
            }
        }
    }

    println!(
        "pyth_demo_summary duration_secs={duration} batches={batches} live_hooks={live_hooks}"
    );
    Ok(())
}

#[cfg(feature = "pyth")]
fn print_price_update(update: &OraclePriceUpdate) {
    println!(
        "live_event feed={} pyth={:?} raw_answer={} decimals={} publish_time={} block={:?} log_index={:?} round_status={:?} value_status={:?} source={:?}",
        update.id,
        update.aggregator,
        update.raw_answer,
        update.decimals,
        update.updated_at,
        update.block_number,
        update.log_index,
        update.round_status,
        update.value_status,
        update.source
    );
}

#[cfg(feature = "pyth")]
fn websocket_rpc_url(http_url: &str) -> Option<String> {
    env::var("ETH_WS_RPC_URL")
        .ok()
        .or_else(|| env::var("WS_RPC_URL").ok())
        .or_else(|| {
            http_url
                .strip_prefix("https://")
                .map(|rest| format!("wss://{rest}"))
        })
        .or_else(|| {
            http_url
                .strip_prefix("http://")
                .map(|rest| format!("ws://{rest}"))
        })
        .or_else(|| {
            if http_url.starts_with("ws://") || http_url.starts_with("wss://") {
                Some(http_url.to_string())
            } else {
                None
            }
        })
}

#[cfg(feature = "pyth")]
fn env_u64(key: &str) -> Option<u64> {
    env::var(key).ok()?.parse().ok()
}
