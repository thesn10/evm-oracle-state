use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, I256, U256};
use alloy_sol_types::sol;
use evm_fork_cache::{cache::EvmCache, reactive::ReactiveHandler};

use crate::{
    AdapterFuture, AssetId, Denomination, EvmCacheChainlinkReader, FeedId, FeedMetadata,
    FeedRegistration, FeedSource, MorphoChainlinkFeed, MorphoChainlinkFeedRole,
    OracleAdapterFeedSkip, OracleAdapterId, OracleAdapterPlugin, OracleAdapterSkipReason,
    OracleDependency, OracleDiscoveredFeed, OracleDiscoveryContext, OracleDiscoveryReport,
    OracleError, OracleFeedStatus, OracleReactiveHandler, OracleStorageSync, RoundData,
    StalenessPolicy,
};

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

    interface MorphoChainlinkOracleV2Interface {
        function BASE_VAULT() external view returns (address);
        function BASE_VAULT_CONVERSION_SAMPLE() external view returns (uint256);
        function QUOTE_VAULT() external view returns (address);
        function QUOTE_VAULT_CONVERSION_SAMPLE() external view returns (uint256);
        function BASE_FEED_1() external view returns (address);
        function BASE_FEED_2() external view returns (address);
        function QUOTE_FEED_1() external view returns (address);
        function QUOTE_FEED_2() external view returns (address);
        function SCALE_FACTOR() external view returns (uint256);
        function price() external view returns (uint256);
    }

    interface Erc4626Like {
        function convertToAssets(uint256 shares) external view returns (uint256);
    }
}

use Erc4626Like::convertToAssetsCall;
use MorphoBlueInterface::idToMarketParamsCall;
use MorphoChainlinkOracleV2Interface::{
    BASE_FEED_1Call, BASE_FEED_2Call, BASE_VAULT_CONVERSION_SAMPLECall, BASE_VAULTCall,
    QUOTE_FEED_1Call, QUOTE_FEED_2Call, QUOTE_VAULT_CONVERSION_SAMPLECall, QUOTE_VAULTCall,
    SCALE_FACTORCall, priceCall,
};

const ADAPTER_ID: &str = "evm-oracle-state.morpho-blue";
const MORPHO_PRICE_DECIMALS: u8 = 36;

/// Morpho Blue market parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MorphoMarketParams {
    /// Loan token.
    pub loan_token: Address,
    /// Collateral token.
    pub collateral_token: Address,
    /// Morpho market oracle.
    pub oracle: Address,
    /// Interest rate model.
    pub irm: Address,
    /// Loan-to-value parameter.
    pub lltv: U256,
}

/// Declarative Morpho Blue market oracle registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MorphoBlueMarket {
    id: Option<B256>,
    params: Option<MorphoMarketParams>,
    oracle: Option<Address>,
    feed_id: Option<FeedId>,
    label: Option<String>,
    base: Option<AssetId>,
    quote: Option<Denomination>,
    staleness: StalenessPolicy,
}

impl MorphoBlueMarket {
    /// Register a market by Morpho Blue market id.
    pub fn market_id(id: B256) -> Self {
        Self {
            id: Some(id),
            params: None,
            oracle: None,
            feed_id: None,
            label: None,
            base: None,
            quote: None,
            staleness: StalenessPolicy::default(),
        }
    }

    /// Register a market by explicit market params.
    pub fn market_params(params: MorphoMarketParams) -> Self {
        Self {
            id: None,
            oracle: Some(params.oracle),
            params: Some(params),
            feed_id: None,
            label: None,
            base: None,
            quote: None,
            staleness: StalenessPolicy::default(),
        }
    }

    /// Register a Morpho oracle address directly.
    pub fn oracle(oracle: Address) -> Self {
        Self {
            id: None,
            params: None,
            oracle: Some(oracle),
            feed_id: None,
            label: None,
            base: None,
            quote: None,
            staleness: StalenessPolicy::default(),
        }
    }

    /// Set a stable feed id.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.feed_id = Some(FeedId::new(id));
        self
    }

    /// Set a stable feed id.
    pub fn feed_id(mut self, id: FeedId) -> Self {
        self.feed_id = Some(id);
        self
    }

    /// Set a human-readable label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the base asset label.
    pub fn base(mut self, base: AssetId) -> Self {
        self.base = Some(base);
        self
    }

    /// Set the quote denomination label.
    pub fn quote(mut self, quote: Denomination) -> Self {
        self.quote = Some(quote);
        self
    }

    /// Set a max-age staleness policy.
    pub fn max_age_secs(mut self, max_age_secs: u64) -> Self {
        self.staleness = StalenessPolicy::max_age(max_age_secs);
        self
    }

    /// Set the full staleness policy.
    pub fn staleness(mut self, staleness: StalenessPolicy) -> Self {
        self.staleness = staleness;
        self
    }
}

/// Cache-backed Morpho Blue oracle discovery adapter.
#[derive(Clone, Debug, Default)]
pub struct MorphoBlueOracleAdapter {
    morpho: Option<Address>,
    markets: Vec<MorphoBlueMarket>,
    now_timestamp: Option<u64>,
}

impl MorphoBlueOracleAdapter {
    /// Create an adapter that can discover market params from a Morpho Blue contract.
    pub fn new(morpho: Address) -> Self {
        Self {
            morpho: Some(morpho),
            markets: Vec::new(),
            now_timestamp: None,
        }
    }

    /// Create an adapter for a direct Morpho oracle address.
    pub fn from_oracle(oracle: Address) -> Self {
        Self {
            morpho: None,
            markets: vec![MorphoBlueMarket::oracle(oracle)],
            now_timestamp: None,
        }
    }

    /// Add one market by id.
    pub fn market_id(self, id: B256) -> Self {
        self.market(MorphoBlueMarket::market_id(id))
    }

    /// Add one market registration.
    pub fn market(mut self, market: MorphoBlueMarket) -> Self {
        self.markets.push(market);
        self
    }

    /// Add multiple market registrations.
    pub fn markets(mut self, markets: impl IntoIterator<Item = MorphoBlueMarket>) -> Self {
        self.markets.extend(markets);
        self
    }

    /// Set a fixed timestamp for deterministic registration status classification.
    pub fn now_timestamp(mut self, now_timestamp: u64) -> Self {
        self.now_timestamp = Some(now_timestamp);
        self
    }

    fn timestamp(&self, fallback: Option<u64>) -> Result<u64, OracleError> {
        if let Some(now_timestamp) = self.now_timestamp.or(fallback) {
            return Ok(now_timestamp);
        }
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(crate::error::clock_error)?
            .as_secs())
    }

    fn discover_markets(
        &self,
        cache: &mut EvmCache,
        now_timestamp: u64,
    ) -> Result<OracleDiscoveryReport, OracleError> {
        let mut report = OracleDiscoveryReport::new();
        for market in &self.markets {
            match self.discover_market(cache, market, now_timestamp) {
                Ok(feed) => report = report.with_feed(feed),
                Err(error) => {
                    report = report.with_skip(OracleAdapterFeedSkip {
                        feed: crate::Feed::proxy(market.oracle.unwrap_or_default()).build(),
                        proxy: market.oracle.unwrap_or_default(),
                        reason: OracleAdapterSkipReason::UnsupportedMorphoSource {
                            error: format!("unsupported Morpho source ({error})"),
                        },
                    });
                }
            }
        }
        Ok(report)
    }

    fn discover_market(
        &self,
        cache: &mut EvmCache,
        market: &MorphoBlueMarket,
        now_timestamp: u64,
    ) -> Result<OracleDiscoveredFeed, OracleError> {
        let params = if let Some(params) = &market.params {
            Some(params.clone())
        } else if let Some(id) = market.id {
            Some(self.read_market_params(cache, id)?)
        } else {
            None
        };
        let oracle = market
            .oracle
            .or_else(|| params.as_ref().map(|params| params.oracle))
            .ok_or_else(|| {
                OracleError::Config(crate::error::OracleConfigError::Other(
                    "Morpho market oracle is missing".to_string(),
                ))
            })?;

        let probed = self.read_chainlink_v2(cache, oracle)?;
        let id = market
            .feed_id
            .clone()
            .unwrap_or_else(|| derive_morpho_feed_id(market.id, oracle));
        let label = market
            .label
            .clone()
            .or_else(|| Some(format!("Morpho {oracle:?}")));
        let base = market.base.clone().map(String::from).or_else(|| {
            params
                .as_ref()
                .map(|params| format!("{:?}", params.collateral_token))
        });
        let quote = market.quote.clone().map(String::from).or_else(|| {
            params
                .as_ref()
                .map(|params| format!("{:?}", params.loan_token))
        });
        let metadata = FeedMetadata {
            decimals: MORPHO_PRICE_DECIMALS,
            description: label
                .clone()
                .unwrap_or_else(|| "MorphoChainlinkOracleV2".to_string()),
            version: U256::ZERO,
        };
        let source = FeedSource::morpho_chainlink_v2(
            oracle,
            probed.base_vault_assets,
            probed.quote_vault_assets,
            probed.scale_factor,
            probed.feeds,
        );
        let registration = FeedRegistration {
            id,
            proxy: oracle,
            label,
            base,
            quote,
            staleness: market.staleness,
            current_aggregator: source.event_aggregators(None).first().copied(),
            aggregator_layout: None,
            metadata,
            source,
            status: OracleFeedStatus::Ready,
        };
        let round = RoundData {
            round_id: U256::ZERO,
            answer: probed.price,
            started_at: now_timestamp,
            updated_at: now_timestamp,
            answered_in_round: U256::ZERO,
        };

        Ok(OracleDiscoveredFeed::new(registration, round))
    }

    fn read_market_params(
        &self,
        cache: &mut EvmCache,
        id: B256,
    ) -> Result<MorphoMarketParams, OracleError> {
        let morpho = self.morpho.ok_or_else(|| {
            OracleError::Config(crate::error::OracleConfigError::Other(
                "Morpho contract is required for market-id discovery".to_string(),
            ))
        })?;
        let params = cache
            .call_sol(morpho, idToMarketParamsCall { id })
            .map_err(provider_error)?;
        Ok(MorphoMarketParams {
            loan_token: params.loanToken,
            collateral_token: params.collateralToken,
            oracle: params.oracle,
            irm: params.irm,
            lltv: params.lltv,
        })
    }

    fn read_chainlink_v2(
        &self,
        cache: &mut EvmCache,
        oracle: Address,
    ) -> Result<MorphoChainlinkV2Read, OracleError> {
        let base_vault = cache
            .call_sol(oracle, BASE_VAULTCall {})
            .map_err(provider_error)?;
        let base_sample = cache
            .call_sol(oracle, BASE_VAULT_CONVERSION_SAMPLECall {})
            .map_err(provider_error)?;
        let quote_vault = cache
            .call_sol(oracle, QUOTE_VAULTCall {})
            .map_err(provider_error)?;
        let quote_sample = cache
            .call_sol(oracle, QUOTE_VAULT_CONVERSION_SAMPLECall {})
            .map_err(provider_error)?;
        let scale_factor = cache
            .call_sol(oracle, SCALE_FACTORCall {})
            .map_err(provider_error)?;
        let price = cache
            .call_sol(oracle, priceCall {})
            .map_err(provider_error)?;
        let base_vault_assets = read_vault_assets(cache, base_vault, base_sample)?;
        let quote_vault_assets = read_vault_assets(cache, quote_vault, quote_sample)?;
        let feed_addresses = [
            (
                MorphoChainlinkFeedRole::BaseFeed1,
                cache
                    .call_sol(oracle, BASE_FEED_1Call {})
                    .map_err(provider_error)?,
            ),
            (
                MorphoChainlinkFeedRole::BaseFeed2,
                cache
                    .call_sol(oracle, BASE_FEED_2Call {})
                    .map_err(provider_error)?,
            ),
            (
                MorphoChainlinkFeedRole::QuoteFeed1,
                cache
                    .call_sol(oracle, QUOTE_FEED_1Call {})
                    .map_err(provider_error)?,
            ),
            (
                MorphoChainlinkFeedRole::QuoteFeed2,
                cache
                    .call_sol(oracle, QUOTE_FEED_2Call {})
                    .map_err(provider_error)?,
            ),
        ];

        let reader = EvmCacheChainlinkReader::new(cache);
        let mut feeds = Vec::new();
        for (role, feed) in feed_addresses {
            if feed == Address::ZERO {
                continue;
            }
            let round = reader.read_latest_round_data(feed)?;
            let aggregator = reader.read_aggregator(feed)?.ok_or_else(|| {
                OracleError::Unsupported("Morpho dependency aggregator() returned none".to_string())
            })?;
            feeds.push(MorphoChainlinkFeed::new(
                role,
                OracleDependency::new(feed, aggregator, round.answer),
            ));
        }

        Ok(MorphoChainlinkV2Read {
            base_vault_assets: i256_from_u256(base_vault_assets, "base vault assets")?,
            quote_vault_assets: i256_from_u256(quote_vault_assets, "quote vault assets")?,
            scale_factor: i256_from_u256(scale_factor, "scale factor")?,
            price: i256_from_u256(price, "price")?,
            feeds,
        })
    }
}

impl OracleAdapterPlugin for MorphoBlueOracleAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        OracleAdapterId::new(ADAPTER_ID)
    }

    fn discover<'a>(
        &'a self,
        ctx: OracleDiscoveryContext<'a>,
    ) -> AdapterFuture<'a, OracleDiscoveryReport> {
        Box::pin(async move {
            let now_timestamp = self.timestamp(Some(ctx.now_timestamp))?;
            self.discover_markets(ctx.cache, now_timestamp)
        })
    }

    fn reactive_handler(
        &self,
        registrations: Vec<FeedRegistration>,
        storage_sync: OracleStorageSync,
    ) -> Arc<dyn ReactiveHandler<Ethereum>> {
        Arc::new(OracleReactiveHandler::with_storage_sync(
            registrations,
            storage_sync,
        ))
    }
}

#[derive(Clone, Debug)]
struct MorphoChainlinkV2Read {
    base_vault_assets: I256,
    quote_vault_assets: I256,
    scale_factor: I256,
    price: I256,
    feeds: Vec<MorphoChainlinkFeed>,
}

fn read_vault_assets(
    cache: &mut EvmCache,
    vault: Address,
    sample: U256,
) -> Result<U256, OracleError> {
    if vault == Address::ZERO {
        return Ok(U256::from(1_u8));
    }
    cache
        .call_sol(vault, convertToAssetsCall { shares: sample })
        .map_err(provider_error)
}

fn derive_morpho_feed_id(id: Option<B256>, oracle: Address) -> FeedId {
    match id {
        Some(id) => FeedId::new(format!("morpho-{id:?}")),
        None => FeedId::new(format!("morpho-{oracle:?}")),
    }
}

fn i256_from_u256(value: U256, field: &'static str) -> Result<I256, OracleError> {
    I256::try_from(value)
        .map_err(|_| OracleError::Unsupported(format!("{field} does not fit int256")))
}

fn provider_error(error: impl ToString) -> OracleError {
    OracleError::Provider(error.to_string())
}
