#[cfg(feature = "redstone")]
use std::{
    env,
    error::Error,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "redstone")]
use evm_oracle_state::alloy_network::{AnyNetwork, Ethereum};
#[cfg(feature = "redstone")]
use alloy_primitives::{Address, B256};
#[cfg(feature = "redstone")]
use evm_oracle_state::alloy_provider::{ProviderBuilder, RootProvider, WsConnect};
#[cfg(feature = "redstone")]
use evm_fork_cache::{
    cache::EvmCache,
    reactive::{AlloySubscriber, ReactiveRuntime, SubscriberConfig, SubscriberMode},
};
#[cfg(feature = "redstone")]
use evm_oracle_state::{
    AssetId, Denomination, OracleError, OracleHookEvent, OraclePrice, OraclePriceUpdate,
    OracleRuntime, RedstoneFeed, RedstoneOracleAdapter,
};
#[cfg(feature = "redstone")]
use tokio::time::timeout;

#[cfg(feature = "redstone")]
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let Some(rpc_url) = env::var("ETH_RPC_URL")
        .ok()
        .or_else(|| env::var("E2E_RPC_URL").ok())
    else {
        println!("ETH_RPC_URL/E2E_RPC_URL unset; skipping RedStone push demo.");
        return Ok(());
    };
    let Some(price_feed) = env_address("REDSTONE_PRICE_FEED")? else {
        println!("REDSTONE_PRICE_FEED unset; skipping RedStone push demo.");
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
    let feed = feed_from_env(price_feed)?;
    let adapter = RedstoneOracleAdapter::new().feed(feed);
    let report = OracleRuntime::cache_builder()
        .install_adapter(adapter)
        .now_timestamp(now)
        .build_report(&mut cache)
        .await?;
    let mut oracle_runtime = report.runtime;
    let mut reactive_runtime = oracle_runtime.reactive_runtime()?;

    println!("RedStone push demo");
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
            "registered feed={} label=\"{}\" proxy={:?} adapter={:?}",
            registration.id,
            registration.label.as_deref().unwrap_or(""),
            registration.proxy,
            registration.current_aggregator
        );
        print_price(
            &oracle_runtime.price_by_proxy(registration.proxy)?,
            "cold_start",
        );
    }

    if oracle_runtime.reactive_interests().is_empty() {
        println!("No RedStone reactive interests were registered; nothing to subscribe to.");
        return Ok(());
    }

    run_live_tracking(
        &ws_url,
        &mut cache,
        &mut oracle_runtime,
        &mut reactive_runtime,
    )
    .await
}

#[cfg(not(feature = "redstone"))]
fn main() {
    println!(
        "Enable the RedStone adapter to run this example: cargo run --features redstone --example redstone_push_demo"
    );
}

#[cfg(feature = "redstone")]
fn feed_from_env(price_feed: Address) -> Result<RedstoneFeed, OracleError> {
    let mut feed = RedstoneFeed::new(price_feed);
    if let Some(adapter) = env_address("REDSTONE_ADAPTER")? {
        feed = feed.adapter(adapter);
    }
    if let Some(data_feed_id) = env::var("REDSTONE_DATA_FEED_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        feed = feed.data_feed_id(parse_data_feed_id(&data_feed_id)?);
    }
    if let Some(id) = env::var("REDSTONE_FEED_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        feed = feed.id(id);
    }
    if let Some(label) = env::var("REDSTONE_LABEL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        feed = feed.label(label);
    }
    if let Some(base) = env::var("REDSTONE_BASE")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        feed = feed.base(AssetId::from(base));
    }
    if let Some(quote) = env::var("REDSTONE_QUOTE")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        feed = feed.quote(Denomination::from(quote));
    }
    if let Some(max_age_secs) = env_u64("REDSTONE_MAX_AGE_SECS") {
        feed = feed.max_age_secs(max_age_secs);
    }
    Ok(feed)
}

#[cfg(feature = "redstone")]
async fn run_live_tracking(
    ws_url: &str,
    cache: &mut EvmCache,
    oracle_runtime: &mut OracleRuntime<()>,
    reactive_runtime: &mut ReactiveRuntime<Ethereum>,
) -> Result<(), Box<dyn Error>> {
    let duration = env_u64("REDSTONE_DEMO_DURATION_SECS").unwrap_or(300);
    let idle_timeout = env_u64("REDSTONE_DEMO_IDLE_TIMEOUT_SECS").unwrap_or(30);
    if duration == 0 {
        println!("live tracking skipped because REDSTONE_DEMO_DURATION_SECS=0");
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

    println!("\nReactive RedStone tracking duration={duration}s idle_timeout={idle_timeout}s");

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
            }
        }
    }

    println!(
        "redstone_demo_summary duration_secs={duration} batches={batches} live_hooks={live_hooks}"
    );
    if live_hooks == 0 {
        println!("redstone_demo_note no live RedStone oracle hooks arrived during this window");
    }
    Ok(())
}

#[cfg(feature = "redstone")]
fn print_price(price: &OraclePrice, source: &str) {
    println!(
        "{source} feed={} raw_answer={} decimals={} updated_at={} round_status={:?} value_status={:?} source={:?}",
        price.id,
        price.raw_answer,
        price.decimals,
        price.updated_at,
        price.round_status,
        price.value_status,
        price.source
    );
}

#[cfg(feature = "redstone")]
fn print_price_update(update: &OraclePriceUpdate, source: &str) {
    println!(
        "{source} feed={} proxy={:?} event_source={:?} raw_answer={} decimals={} updated_at={} block={:?} log_index={:?} round_status={:?} value_status={:?} source={:?}",
        update.id,
        update.proxy,
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

#[cfg(feature = "redstone")]
fn env_address(key: &str) -> Result<Option<Address>, OracleError> {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            Address::from_str(&value)
                .map_err(|error| OracleError::Provider(format!("{key} is invalid: {error}")))
        })
        .transpose()
}

#[cfg(feature = "redstone")]
fn parse_data_feed_id(value: &str) -> Result<B256, OracleError> {
    let value = value.trim();
    if value.starts_with("0x") {
        return B256::from_str(value).map_err(|error| {
            OracleError::Provider(format!("REDSTONE_DATA_FEED_ID is invalid: {error}"))
        });
    }
    if value.len() > 32 {
        return Err(OracleError::Provider(
            "REDSTONE_DATA_FEED_ID symbol is longer than 32 bytes".to_string(),
        ));
    }
    let mut bytes = [0_u8; 32];
    bytes[..value.len()].copy_from_slice(value.as_bytes());
    Ok(B256::from(bytes))
}

#[cfg(feature = "redstone")]
fn env_u64(key: &str) -> Option<u64> {
    env::var(key).ok()?.parse().ok()
}

#[cfg(feature = "redstone")]
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
