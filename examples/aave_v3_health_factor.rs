use std::{
    collections::BTreeSet,
    env,
    error::Error,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use evm_oracle_state::alloy_network::{AnyNetwork, Ethereum};
use alloy_primitives::{Address, I256, U256, address};
use evm_oracle_state::alloy_provider::{ProviderBuilder, RootProvider, WsConnect};
use alloy_sol_types::sol;
use evm_fork_cache::{
    cache::EvmCache,
    reactive::{AlloySubscriber, ReactiveRuntime, SubscriberConfig, SubscriberMode},
};
#[cfg(feature = "aave")]
use evm_oracle_state::{AaveAsset, AaveV3OracleAdapter};
use evm_oracle_state::{
    AssetId, Denomination, OracleAdapterBuildReport, OracleAdapterFeedSkip, OracleError,
    OracleHookEvent, OraclePriceUpdate, OracleReactiveHandler, OracleRuntime, OracleTracker,
    PricePolicy,
};
#[cfg(not(feature = "aave"))]
use evm_oracle_state::{Feed, OracleAdapter};
use tokio::time::timeout;

sol! {
    struct ReserveConfigurationMap {
        uint256 data;
    }

    struct ReserveData {
        ReserveConfigurationMap configuration;
        uint128 liquidityIndex;
        uint128 currentLiquidityRate;
        uint128 variableBorrowIndex;
        uint128 currentVariableBorrowRate;
        uint128 currentStableBorrowRate;
        uint40 lastUpdateTimestamp;
        uint16 id;
        address aTokenAddress;
        address stableDebtTokenAddress;
        address variableDebtTokenAddress;
        address interestRateStrategyAddress;
        uint128 accruedToTreasury;
        uint128 unbacked;
        uint128 isolationModeTotalDebt;
    }

    struct EModeCategory {
        uint16 ltv;
        uint16 liquidationThreshold;
        uint16 liquidationBonus;
        address priceSource;
        string label;
    }

    interface IPoolAddressesProvider {
        function getPool() external view returns (address);
        function getPriceOracle() external view returns (address);
    }

    interface IPool {
        function getReservesList() external view returns (address[]);
        function getReserveData(address asset) external view returns (ReserveData memory);
        function getUserConfiguration(address user) external view returns (uint256);
        function getUserEMode(address user) external view returns (uint256);
        function getEModeCategoryData(uint8 id) external view returns (EModeCategory memory);
        function getUserAccountData(address user) external view returns (
            uint256 totalCollateralBase,
            uint256 totalDebtBase,
            uint256 availableBorrowsBase,
            uint256 currentLiquidationThreshold,
            uint256 ltv,
            uint256 healthFactor
        );
    }

    interface IAaveOracle {
        function getAssetPrice(address asset) external view returns (uint256);
        function getSourceOfAsset(address asset) external view returns (address);
        function BASE_CURRENCY_UNIT() external view returns (uint256);
    }

    interface IERC20Metadata {
        function decimals() external view returns (uint8);
        function symbol() external view returns (string);
        function balanceOf(address account) external view returns (uint256);
    }
}

use IAaveOracle::{BASE_CURRENCY_UNITCall, getAssetPriceCall, getSourceOfAssetCall};
use IERC20Metadata::{
    balanceOfCall, decimalsCall as tokenDecimalsCall, symbolCall as tokenSymbolCall,
};
use IPool::{
    getEModeCategoryDataCall, getReserveDataCall, getReservesListCall, getUserAccountDataCall,
    getUserConfigurationCall, getUserEModeCall,
};
use IPoolAddressesProvider::{getPoolCall, getPriceOracleCall};

const ETHEREUM_AAVE_V3_ADDRESSES_PROVIDER: Address =
    address!("2f39d218133AFaB8F2B819B1066c7E434Ad94E9e");
const DEFAULT_SMOKE_USER: Address = address!("cc144c2e9fc40f92257b2cd7cc43e1f82666f09a");
const BPS: u64 = 10_000;
const WAD: u128 = 1_000_000_000_000_000_000;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let Some(rpc_url) = env::var("ETH_RPC_URL")
        .ok()
        .or_else(|| env::var("E2E_RPC_URL").ok())
    else {
        println!("ETH_RPC_URL/E2E_RPC_URL unset; skipping Aave V3 health-factor demo.");
        return Ok(());
    };

    let user = parse_user()?;
    let provider = Arc::new(RootProvider::<AnyNetwork>::connect(&rpc_url).await?);
    let mut cache = EvmCache::new(provider.clone()).await;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let pool = read_pool(&mut cache)?;
    let oracle = read_aave_oracle(&mut cache)?;
    let base_unit = read_base_currency_unit(&mut cache, oracle)?;
    let base_decimals = decimals_from_power_of_ten(base_unit).unwrap_or(8);

    let protocol = read_protocol_account_data(&mut cache, pool, user)?;
    let user_config = read_user_configuration(&mut cache, pool, user)?;
    let user_emode = read_user_emode(&mut cache, pool, user)?;
    let emode = if user_emode == 0 {
        None
    } else {
        Some(read_emode_category(&mut cache, pool, user_emode)?)
    };
    let positions = read_positions(&mut cache, pool, oracle, user, user_config, emode.as_ref())?;
    let computed = compute_health_factor(&positions);

    let registration_report =
        build_oracle_registration_report(&mut cache, oracle, &positions, now).await?;
    let mut oracle_runtime = OracleRuntime::from_tracker(registration_report.tracker);
    let mut reactive_runtime = oracle_runtime.reactive_runtime()?;

    println!("Aave V3 Ethereum health-factor demo");
    println!("user: {user:?}");
    println!("pool: {pool:?}");
    println!("oracle: {oracle:?}");
    println!("base currency unit: {base_unit} ({base_decimals} decimals)");
    if let Some(emode) = &emode {
        println!(
            "user e-mode: {} label=\"{}\" liquidation_threshold={} bps",
            emode.id, emode.label, emode.liquidation_threshold_bps
        );
    } else {
        println!("user e-mode: disabled");
    }

    print_oracle_registrations(oracle_runtime.tracker(), &registration_report.skipped);
    print_positions(&positions, base_decimals);
    print_health_factor(&computed, &protocol, base_decimals);
    println!(
        "\nDeBank check: node /Users/kaialdag/Desktop/dev/egg/debank-portfolio-cli/bin/debank-portfolio.js {user:?} --protocol aave --format json"
    );

    if oracle_runtime.reactive_interests().is_empty() {
        println!(
            "\nReactive tracking disabled: no Chainlink-compatible Aave oracle sources registered."
        );
        return Ok(());
    }

    if let Some(ws_url) = websocket_rpc_url(&rpc_url) {
        run_reactive_tracking(
            &ws_url,
            &mut cache,
            &mut oracle_runtime,
            &mut reactive_runtime,
            HealthContext {
                pool,
                oracle,
                user,
                user_config,
                emode: emode.as_ref(),
                base_decimals,
            },
        )
        .await?;
    } else {
        println!(
            "\nLive oracle subscription skipped: set ETH_WS_RPC_URL or WS_RPC_URL to a websocket endpoint."
        );
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct ReserveConfig {
    liquidation_threshold_bps: u16,
    decimals: u8,
    e_mode_category: u8,
}

#[derive(Clone, Debug)]
struct EModeConfig {
    id: u8,
    liquidation_threshold_bps: u16,
    label: String,
}

#[derive(Clone, Debug)]
struct Position {
    asset: Address,
    symbol: String,
    decimals: u8,
    collateral_balance: U256,
    debt_balance: U256,
    price_base: U256,
    collateral_base: U256,
    weighted_collateral_base: U256,
    debt_base: U256,
    liquidation_threshold_bps: u16,
    use_as_collateral: bool,
    borrowing_enabled: bool,
    oracle_source: Address,
}

#[derive(Clone, Debug)]
struct ComputedHealth {
    total_collateral_base: U256,
    weighted_collateral_base: U256,
    total_debt_base: U256,
    health_factor_wad: U256,
}

#[derive(Clone, Debug)]
struct ProtocolAccountData {
    total_collateral_base: U256,
    total_debt_base: U256,
    current_liquidation_threshold: U256,
    health_factor: U256,
}

#[cfg(not(feature = "aave"))]
fn position_oracle_feeds(positions: &[Position]) -> Vec<Feed> {
    let mut registered_sources = BTreeSet::new();
    let mut feeds = Vec::new();

    for position in positions {
        if position.oracle_source == Address::ZERO
            || !registered_sources.insert(position.oracle_source)
        {
            continue;
        }

        let feed = Feed::proxy(position.oracle_source)
            .id(position_feed_id(position))
            .label(position_feed_label(position))
            .base(AssetId::symbol(position.symbol.clone()))
            .quote(Denomination::Usd)
            .max_age_secs(24 * 60 * 60);

        feeds.push(feed);
    }

    feeds
}

#[cfg(feature = "aave")]
async fn build_oracle_registration_report(
    cache: &mut EvmCache,
    oracle: Address,
    positions: &[Position],
    now_timestamp: u64,
) -> Result<OracleAdapterBuildReport, OracleError> {
    let mut adapter = AaveV3OracleAdapter::new(oracle).now_timestamp(now_timestamp);
    for asset in position_aave_assets(positions) {
        adapter = adapter.asset(asset);
    }
    adapter.build_report(cache).await
}

#[cfg(not(feature = "aave"))]
async fn build_oracle_registration_report(
    cache: &mut EvmCache,
    _oracle: Address,
    positions: &[Position],
    now_timestamp: u64,
) -> Result<OracleAdapterBuildReport, OracleError> {
    OracleAdapter::builder()
        .feeds(position_oracle_feeds(positions))
        .now_timestamp(now_timestamp)
        .build_report(cache)
        .await
}

#[cfg(feature = "aave")]
fn position_aave_assets(positions: &[Position]) -> Vec<AaveAsset> {
    let mut registered_assets = BTreeSet::new();
    let mut assets = Vec::new();

    for position in positions {
        if position.asset == Address::ZERO || !registered_assets.insert(position.asset) {
            continue;
        }

        assets.push(
            AaveAsset::new(position.asset)
                .id(position_feed_id(position))
                .label(position_feed_label(position))
                .base(AssetId::symbol(position.symbol.clone()))
                .quote(Denomination::Usd)
                .max_age_secs(24 * 60 * 60),
        );
    }

    assets
}

fn position_feed_id(position: &Position) -> String {
    format!("aave-v3-{}", position.symbol.to_ascii_lowercase())
}

fn position_feed_label(position: &Position) -> String {
    format!("Aave V3 {} source", position.symbol)
}

fn read_positions(
    cache: &mut EvmCache,
    pool: Address,
    oracle: Address,
    user: Address,
    user_config: U256,
    emode: Option<&EModeConfig>,
) -> Result<Vec<Position>, OracleError> {
    let reserves = read_reserves(cache, pool)?;
    let mut positions = Vec::new();

    for asset in reserves {
        let reserve = read_reserve_data(cache, pool, asset)?;
        let reserve_id = reserve.id;
        let has_debt_bit = user_config_bit(user_config, reserve_id, false);
        let has_collateral_bit = user_config_bit(user_config, reserve_id, true);
        if !has_debt_bit && !has_collateral_bit {
            continue;
        }

        let config = ReserveConfig::from_data(reserve.configuration.data);
        let symbol = read_symbol(cache, asset).unwrap_or_else(|_| format!("{asset:?}"));
        let decimals = read_token_decimals(cache, asset).unwrap_or(config.decimals);
        let collateral_balance = read_balance(cache, reserve.aTokenAddress, user)?;
        let variable_debt = read_balance(cache, reserve.variableDebtTokenAddress, user)?;
        let stable_debt = if reserve.stableDebtTokenAddress == Address::ZERO {
            U256::ZERO
        } else {
            read_balance(cache, reserve.stableDebtTokenAddress, user)?
        };
        let debt_balance = variable_debt
            .checked_add(stable_debt)
            .ok_or_else(|| OracleError::Provider("debt balance overflowed".to_string()))?;
        if collateral_balance == U256::ZERO && debt_balance == U256::ZERO {
            continue;
        }

        let price_base = read_asset_price(cache, oracle, asset)?;
        let oracle_source = read_source_of_asset(cache, oracle, asset).unwrap_or(Address::ZERO);
        let liquidation_threshold_bps = if let Some(emode) = emode
            && config.e_mode_category == emode.id
        {
            emode.liquidation_threshold_bps
        } else {
            config.liquidation_threshold_bps
        };
        let collateral_base = if has_collateral_bit {
            value_in_base(collateral_balance, price_base, decimals)?
        } else {
            U256::ZERO
        };
        let weighted_collateral_base = collateral_base
            .checked_mul(U256::from(liquidation_threshold_bps))
            .and_then(|value| value.checked_div(U256::from(BPS)))
            .ok_or_else(|| OracleError::Provider("weighted collateral overflowed".to_string()))?;
        let debt_base = value_in_base(debt_balance, price_base, decimals)?;

        positions.push(Position {
            asset,
            symbol,
            decimals,
            collateral_balance,
            debt_balance,
            price_base,
            collateral_base,
            weighted_collateral_base,
            debt_base,
            liquidation_threshold_bps,
            use_as_collateral: has_collateral_bit,
            borrowing_enabled: has_debt_bit,
            oracle_source,
        });
    }

    Ok(positions)
}

async fn run_reactive_tracking(
    ws_url: &str,
    cache: &mut EvmCache,
    oracle_runtime: &mut OracleRuntime<()>,
    reactive_runtime: &mut ReactiveRuntime<Ethereum>,
    health: HealthContext<'_>,
) -> Result<(), Box<dyn Error>> {
    let duration = env_u64("AAVE_DEMO_DURATION_SECS").unwrap_or(600);
    let timeout_secs = env_u64("AAVE_DEMO_IDLE_TIMEOUT_SECS").unwrap_or(30);

    println!("\nReactive oracle tracking");
    println!(
        "registered interests: {} duration={}s idle_timeout={}s",
        oracle_runtime.reactive_interests().len(),
        duration,
        timeout_secs
    );

    if duration == 0 {
        println!("live tracking skipped because AAVE_DEMO_DURATION_SECS=0");
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

    let start = Instant::now();
    let mut live_hooks = 0usize;
    let mut batches = 0usize;

    while start.elapsed() < Duration::from_secs(duration) {
        let remaining = Duration::from_secs(duration).saturating_sub(start.elapsed());
        let wait_for = remaining.min(Duration::from_secs(timeout_secs));
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
        println!("subscription_batch events={}", events.events.len());
        live_hooks += events.events.len();
        for event in events.events {
            print_reactive_health(cache, &health, "live_event", event)?;
        }
    }

    println!(
        "reactive_tracking_summary duration_secs={duration} batches={batches} live_hooks={live_hooks}"
    );
    if live_hooks == 0 {
        println!("reactive_tracking_note no live oracle update hooks arrived during this window");
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct HealthContext<'a> {
    pool: Address,
    oracle: Address,
    user: Address,
    user_config: U256,
    emode: Option<&'a EModeConfig>,
    base_decimals: u8,
}

fn print_reactive_health(
    cache: &mut EvmCache,
    health: &HealthContext<'_>,
    prefix: &str,
    event: OracleHookEvent,
) -> Result<(), OracleError> {
    let OracleHookEvent::PriceUpdate(update) = event else {
        return Ok(());
    };
    let positions = read_positions(
        cache,
        health.pool,
        health.oracle,
        health.user,
        health.user_config,
        health.emode,
    )?;
    let computed = compute_health_factor(&positions);
    let protocol = read_protocol_account_data(cache, health.pool, health.user)?;
    print_price_update(&update, prefix);
    println!(
        "{prefix} health_factor computed={} aave_getUserAccountData={} delta={} collateral={} debt={}",
        format_wad(computed.health_factor_wad),
        format_wad(protocol.health_factor),
        format_hf_delta(computed.health_factor_wad, protocol.health_factor),
        format_units(computed.total_collateral_base, health.base_decimals, 6),
        format_units(computed.total_debt_base, health.base_decimals, 6)
    );
    Ok(())
}

fn print_price_update(update: &OraclePriceUpdate, prefix: &str) {
    println!(
        "{prefix} oracle_price_update feed={} label=\"{}\" proxy={:?} aggregator={:?} raw_answer={} price={} updated_at={} block={:?} round_status={:?} value_status={:?} source={:?}",
        update.id,
        update.label.as_deref().unwrap_or(""),
        update.proxy,
        update.aggregator,
        update.raw_answer,
        format_i256_units(update.raw_answer, update.decimals, 8),
        update.updated_at,
        update.block_number,
        update.round_status,
        update.value_status,
        update.source
    );
}

fn print_oracle_registrations(tracker: &OracleTracker, skipped_sources: &[OracleAdapterFeedSkip]) {
    println!("\nOracle source registrations");
    println!("registered sources: {}", tracker.registrations().count());
    println!(
        "reactive log interests: {}",
        OracleReactiveHandler::new(tracker.registrations().collect())
            .interests()
            .len()
    );
    for registration in tracker.registrations() {
        let price = tracker
            .price_by_id(registration.id.clone())
            .and_then(|price| price.require(PricePolicy::liquidation().quote(Denomination::Usd)));
        let checked_price = price
            .as_ref()
            .map(|checked| checked.raw_answer().to_string())
            .unwrap_or_else(|error| format!("unavailable ({error})"));
        println!(
            "  {} proxy={:?} aggregator={:?} status={:?} layout={:?} checked_price={}",
            registration.id,
            registration.proxy,
            registration.current_aggregator,
            registration.status,
            registration
                .aggregator_layout
                .as_ref()
                .map(|layout| layout.layout),
            checked_price
        );
    }
    for skipped in skipped_sources {
        let label = skipped
            .feed
            .label()
            .map(str::to_string)
            .or_else(|| skipped.feed.id().map(|id| id.to_string()))
            .unwrap_or_else(|| "unknown".to_string());
        println!(
            "  skipped \"{label}\" proxy={:?}: {}",
            skipped.proxy, skipped.reason
        );
    }
}

fn print_positions(positions: &[Position], base_decimals: u8) {
    println!("\nPosition rows");
    println!(
        "  {:<8} {:>16} {:>16} {:>14} {:>8} {:>14} {:>14}",
        "asset", "collateral", "debt", "price", "lt", "collateral$", "debt$"
    );
    for position in positions {
        println!(
            "  {:<8} {:>16} {:>16} {:>14} {:>7.2}% {:>14} {:>14}",
            position.symbol,
            format_units(position.collateral_balance, position.decimals, 6),
            format_units(position.debt_balance, position.decimals, 6),
            format_units(position.price_base, base_decimals, 6),
            f64::from(position.liquidation_threshold_bps) / 100.0,
            format_units(position.collateral_base, base_decimals, 4),
            format_units(position.debt_base, base_decimals, 4),
        );
        println!(
            "           asset={:?} source={:?} collateral_enabled={} debt_enabled={}",
            position.asset,
            position.oracle_source,
            position.use_as_collateral,
            position.borrowing_enabled
        );
    }
}

fn print_health_factor(
    computed: &ComputedHealth,
    protocol: &ProtocolAccountData,
    base_decimals: u8,
) {
    println!("\nHealth factor");
    println!(
        "computed collateral base: {}",
        format_units(computed.total_collateral_base, base_decimals, 6)
    );
    println!(
        "computed weighted collateral: {}",
        format_units(computed.weighted_collateral_base, base_decimals, 6)
    );
    println!(
        "computed debt base: {}",
        format_units(computed.total_debt_base, base_decimals, 6)
    );
    println!(
        "computed health factor: {}",
        format_wad(computed.health_factor_wad)
    );
    println!(
        "Aave getUserAccountData collateral: {}",
        format_units(protocol.total_collateral_base, base_decimals, 6)
    );
    println!(
        "Aave getUserAccountData debt: {}",
        format_units(protocol.total_debt_base, base_decimals, 6)
    );
    println!(
        "Aave getUserAccountData current LT: {:.2}%",
        u256_to_f64(protocol.current_liquidation_threshold) / 100.0
    );
    println!(
        "Aave getUserAccountData health factor: {}",
        format_wad(protocol.health_factor)
    );
    println!(
        "delta vs Aave health factor: {}",
        format_hf_delta(computed.health_factor_wad, protocol.health_factor)
    );
}

fn compute_health_factor(positions: &[Position]) -> ComputedHealth {
    let total_collateral_base = positions
        .iter()
        .fold(U256::ZERO, |acc, position| acc + position.collateral_base);
    let weighted_collateral_base = positions.iter().fold(U256::ZERO, |acc, position| {
        acc + position.weighted_collateral_base
    });
    let total_debt_base = positions
        .iter()
        .fold(U256::ZERO, |acc, position| acc + position.debt_base);
    let health_factor_wad = if total_debt_base == U256::ZERO {
        U256::MAX
    } else {
        weighted_collateral_base
            .checked_mul(U256::from(WAD))
            .and_then(|value| value.checked_div(total_debt_base))
            .unwrap_or(U256::MAX)
    };
    ComputedHealth {
        total_collateral_base,
        weighted_collateral_base,
        total_debt_base,
        health_factor_wad,
    }
}

impl ReserveConfig {
    fn from_data(data: U256) -> Self {
        Self {
            liquidation_threshold_bps: extract_u16(data, 16),
            decimals: extract_u8(data, 48),
            e_mode_category: extract_u8(data, 168),
        }
    }
}

fn read_pool(cache: &mut EvmCache) -> Result<Address, OracleError> {
    cache
        .call_sol(ETHEREUM_AAVE_V3_ADDRESSES_PROVIDER, getPoolCall {})
        .map_err(provider_error)
}

fn read_aave_oracle(cache: &mut EvmCache) -> Result<Address, OracleError> {
    cache
        .call_sol(ETHEREUM_AAVE_V3_ADDRESSES_PROVIDER, getPriceOracleCall {})
        .map_err(provider_error)
}

fn read_base_currency_unit(cache: &mut EvmCache, oracle: Address) -> Result<U256, OracleError> {
    cache
        .call_sol(oracle, BASE_CURRENCY_UNITCall {})
        .map_err(provider_error)
}

fn read_reserves(cache: &mut EvmCache, pool: Address) -> Result<Vec<Address>, OracleError> {
    cache
        .call_sol(pool, getReservesListCall {})
        .map_err(provider_error)
}

fn read_reserve_data(
    cache: &mut EvmCache,
    pool: Address,
    asset: Address,
) -> Result<ReserveData, OracleError> {
    cache
        .call_sol(pool, getReserveDataCall { asset })
        .map_err(provider_error)
}

fn read_user_configuration(
    cache: &mut EvmCache,
    pool: Address,
    user: Address,
) -> Result<U256, OracleError> {
    cache
        .call_sol(pool, getUserConfigurationCall { user })
        .map_err(provider_error)
}

fn read_user_emode(cache: &mut EvmCache, pool: Address, user: Address) -> Result<u8, OracleError> {
    let emode = cache
        .call_sol(pool, getUserEModeCall { user })
        .map_err(provider_error)?;
    u8::try_from(emode)
        .map_err(|_| OracleError::Provider(format!("user e-mode does not fit u8: {emode}")))
}

fn read_emode_category(
    cache: &mut EvmCache,
    pool: Address,
    id: u8,
) -> Result<EModeConfig, OracleError> {
    let emode = cache
        .call_sol(pool, getEModeCategoryDataCall { id })
        .map_err(provider_error)?;
    Ok(EModeConfig {
        id,
        liquidation_threshold_bps: emode.liquidationThreshold,
        label: emode.label,
    })
}

fn read_protocol_account_data(
    cache: &mut EvmCache,
    pool: Address,
    user: Address,
) -> Result<ProtocolAccountData, OracleError> {
    let data = cache
        .call_sol(pool, getUserAccountDataCall { user })
        .map_err(provider_error)?;
    Ok(ProtocolAccountData {
        total_collateral_base: data.totalCollateralBase,
        total_debt_base: data.totalDebtBase,
        current_liquidation_threshold: data.currentLiquidationThreshold,
        health_factor: data.healthFactor,
    })
}

fn read_token_decimals(cache: &mut EvmCache, token: Address) -> Result<u8, OracleError> {
    cache
        .call_sol(token, tokenDecimalsCall {})
        .map_err(provider_error)
}

fn read_symbol(cache: &mut EvmCache, token: Address) -> Result<String, OracleError> {
    cache
        .call_sol(token, tokenSymbolCall {})
        .map_err(provider_error)
}

fn read_balance(
    cache: &mut EvmCache,
    token: Address,
    account: Address,
) -> Result<U256, OracleError> {
    if token == Address::ZERO {
        return Ok(U256::ZERO);
    }
    cache
        .call_sol(token, balanceOfCall { account })
        .map_err(provider_error)
}

fn read_asset_price(
    cache: &mut EvmCache,
    oracle: Address,
    asset: Address,
) -> Result<U256, OracleError> {
    cache
        .call_sol(oracle, getAssetPriceCall { asset })
        .map_err(provider_error)
}

fn read_source_of_asset(
    cache: &mut EvmCache,
    oracle: Address,
    asset: Address,
) -> Result<Address, OracleError> {
    cache
        .call_sol(oracle, getSourceOfAssetCall { asset })
        .map_err(provider_error)
}

fn value_in_base(balance: U256, price: U256, token_decimals: u8) -> Result<U256, OracleError> {
    balance
        .checked_mul(price)
        .and_then(|value| value.checked_div(pow10_u256(token_decimals)))
        .ok_or_else(|| OracleError::Provider("base valuation overflowed".to_string()))
}

fn user_config_bit(config: U256, reserve_id: u16, collateral: bool) -> bool {
    let bit = usize::from(reserve_id) * 2 + usize::from(collateral);
    ((config >> bit) & U256::from(1_u8)) == U256::from(1_u8)
}

fn extract_u16(data: U256, offset: usize) -> u16 {
    u16::try_from((data >> offset) & U256::from(u16::MAX)).expect("masked u16")
}

fn extract_u8(data: U256, offset: usize) -> u8 {
    u8::try_from((data >> offset) & U256::from(u8::MAX)).expect("masked u8")
}

fn pow10_u256(decimals: u8) -> U256 {
    U256::from(10_u8).pow(U256::from(decimals))
}

fn decimals_from_power_of_ten(unit: U256) -> Option<u8> {
    let string = unit.to_string();
    if string.starts_with('1') && string[1..].chars().all(|ch| ch == '0') {
        u8::try_from(string.len().saturating_sub(1)).ok()
    } else {
        None
    }
}

fn format_units(value: U256, decimals: u8, precision: usize) -> String {
    let scaled = u256_to_f64(value) / 10_f64.powi(i32::from(decimals));
    format!("{scaled:.precision$}")
}

fn format_i256_units(value: I256, decimals: u8, precision: usize) -> String {
    let scaled =
        value.to_string().parse::<f64>().unwrap_or(f64::NAN) / 10_f64.powi(i32::from(decimals));
    format!("{scaled:.precision$}")
}

fn format_wad(value: U256) -> String {
    if value == U256::MAX {
        return "inf".to_string();
    }
    format!("{:.9}", u256_to_f64(value) / 1e18)
}

fn format_hf_delta(left: U256, right: U256) -> String {
    if left == U256::MAX || right == U256::MAX {
        return "n/a".to_string();
    }
    let delta = (u256_to_f64(left) - u256_to_f64(right)) / 1e18;
    format!("{delta:.9}")
}

fn u256_to_f64(value: U256) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(f64::INFINITY)
}

fn env_u64(key: &str) -> Option<u64> {
    env::var(key).ok()?.parse().ok()
}

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

fn parse_user() -> Result<Address, Box<dyn Error>> {
    if let Some(value) = env::args().nth(1).or_else(|| env::var("AAVE_USER").ok()) {
        return Ok(value.parse()?);
    }
    println!("No user address supplied; using the built-in debt-bearing smoke address.");
    Ok(DEFAULT_SMOKE_USER)
}

fn provider_error(error: impl std::fmt::Debug) -> OracleError {
    OracleError::Provider(format!("{error:?}"))
}
