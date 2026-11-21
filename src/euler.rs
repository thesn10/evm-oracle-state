use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::alloy_network::Ethereum;
use alloy_primitives::{Address, I256, U256, keccak256};
use alloy_sol_types::sol;
use evm_fork_cache::{cache::EvmCache, reactive::ReactiveHandler};

use crate::{
    AdapterFuture, AssetId, Denomination, EulerQuoteLeg, EvmCacheChainlinkReader, FeedId,
    FeedMetadata, FeedRegistration, FeedSource, OracleAdapterFeedSkip, OracleAdapterId,
    OracleAdapterPlugin, OracleAdapterSkipReason, OracleDiscoveredFeed, OracleDiscoveryContext,
    OracleDiscoveryReport, OracleError, OracleFeedStatus, OracleReactiveHandler, OracleStorageSync,
    RoundData, StalenessPolicy,
};

sol! {
    interface EulerRouterInterface {
        function getConfiguredOracle(address base, address quote) external view returns (address);
        function fallbackOracle() external view returns (address);
    }

    interface EulerPriceOracleInterface {
        function name() external view returns (string);
        function getQuote(uint256 inAmount, address base, address quote) external view returns (uint256 outAmount);
    }

    interface EulerChainlinkOracleInterface {
        function base() external view returns (address);
        function quote() external view returns (address);
        function feed() external view returns (address);
        function maxStaleness() external view returns (uint256);
    }

    interface EulerFixedRateOracleInterface {
        function base() external view returns (address);
        function quote() external view returns (address);
        function rate() external view returns (uint256);
    }

    interface EulerRateProviderOracleInterface {
        function base() external view returns (address);
        function quote() external view returns (address);
        function rateProvider() external view returns (address);
    }

    interface EulerCrossAdapterInterface {
        function base() external view returns (address);
        function cross() external view returns (address);
        function quote() external view returns (address);
        function oracleBaseCross() external view returns (address);
        function oracleCrossQuote() external view returns (address);
    }

    interface RateProviderInterface {
        function getRate() external view returns (uint256);
    }

    interface Erc20Metadata {
        function decimals() external view returns (uint8);
    }
}

use Erc20Metadata::decimalsCall;
use EulerChainlinkOracleInterface::{
    baseCall as chainlinkBaseCall, feedCall, maxStalenessCall, quoteCall as chainlinkQuoteCall,
};
use EulerCrossAdapterInterface::{
    baseCall as crossBaseCall, crossCall, oracleBaseCrossCall, oracleCrossQuoteCall,
    quoteCall as crossQuoteCall,
};
use EulerFixedRateOracleInterface::{
    baseCall as fixedBaseCall, quoteCall as fixedQuoteCall, rateCall,
};
use EulerPriceOracleInterface::{getQuoteCall, nameCall};
use EulerRateProviderOracleInterface::{
    baseCall as rateBaseCall, quoteCall as rateQuoteCall, rateProviderCall,
};
use EulerRouterInterface::{fallbackOracleCall, getConfiguredOracleCall};
use RateProviderInterface::getRateCall;

const ADAPTER_ID: &str = "evm-oracle-state.euler";

/// Declarative Euler quote-pair registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EulerQuotePair {
    base: Address,
    quote: Address,
    oracle: Option<Address>,
    state_key: Option<Address>,
    id: Option<FeedId>,
    label: Option<String>,
    base_label: Option<AssetId>,
    quote_label: Option<Denomination>,
    staleness: StalenessPolicy,
}

impl EulerQuotePair {
    /// Register a base/quote pair to be resolved through an adapter-level router.
    pub fn new(base: Address, quote: Address) -> Self {
        Self {
            base,
            quote,
            oracle: None,
            state_key: None,
            id: None,
            label: None,
            base_label: None,
            quote_label: None,
            staleness: StalenessPolicy::default(),
        }
    }

    /// Register a base/quote pair with a direct Euler oracle adapter.
    pub fn with_oracle(base: Address, quote: Address, oracle: Address) -> Self {
        Self {
            oracle: Some(oracle),
            ..Self::new(base, quote)
        }
    }

    /// Set a stable feed id.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(FeedId::new(id));
        self
    }

    /// Set a stable feed id.
    pub fn feed_id(mut self, id: FeedId) -> Self {
        self.id = Some(id);
        self
    }

    /// Set a human-readable label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the base asset label.
    pub fn base_label(mut self, base: AssetId) -> Self {
        self.base_label = Some(base);
        self
    }

    /// Set the quote denomination label.
    pub fn quote_label(mut self, quote: Denomination) -> Self {
        self.quote_label = Some(quote);
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

    /// Set an explicit per-feed state key.
    ///
    /// By default each discovered pair derives a synthetic state key with
    /// [`EulerQuotePair::state_key_for`], so multiple pairs resolved through
    /// one router (or one shared adapter) never collide in the registry.
    pub fn state_key(mut self, state_key: Address) -> Self {
        self.state_key = Some(state_key);
        self
    }

    /// Deterministically derive a synthetic per-feed state key.
    ///
    /// `quote_target` is the user-facing contract the quote is read through:
    /// the router for router-resolved pairs, or the direct oracle adapter.
    /// The derived key is the registry/tracker state key (the registration
    /// `proxy`); the real on-chain addresses stay in [`crate::FeedSource`],
    /// and [`crate::OracleReadOverlay`] serves `getQuote` at the real
    /// `quote_target` address.
    pub fn state_key_for(quote_target: Address, base: Address, quote: Address) -> Address {
        let mut bytes = Vec::with_capacity(ADAPTER_ID.len() + 20 + 20 + 20);
        bytes.extend_from_slice(ADAPTER_ID.as_bytes());
        bytes.extend_from_slice(quote_target.as_slice());
        bytes.extend_from_slice(base.as_slice());
        bytes.extend_from_slice(quote.as_slice());
        let hash = keccak256(bytes);
        Address::from_slice(&hash.as_slice()[12..])
    }
}

/// Cache-backed Euler price oracle discovery adapter.
#[derive(Clone, Debug, Default)]
pub struct EulerOracleAdapter {
    router: Option<Address>,
    pairs: Vec<EulerQuotePair>,
    now_timestamp: Option<u64>,
}

impl EulerOracleAdapter {
    /// Create an adapter with no default router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an adapter that resolves quote pairs through `EulerRouter`.
    pub fn router(router: Address) -> Self {
        Self {
            router: Some(router),
            pairs: Vec::new(),
            now_timestamp: None,
        }
    }

    /// Create an adapter for one direct oracle quote pair.
    pub fn oracle(oracle: Address, base: Address, quote: Address) -> Self {
        Self {
            router: None,
            pairs: vec![EulerQuotePair::with_oracle(base, quote, oracle)],
            now_timestamp: None,
        }
    }

    /// Add one quote pair.
    pub fn quote_pair(mut self, pair: EulerQuotePair) -> Self {
        self.pairs.push(pair);
        self
    }

    /// Add multiple quote pairs.
    pub fn quote_pairs(mut self, pairs: impl IntoIterator<Item = EulerQuotePair>) -> Self {
        self.pairs.extend(pairs);
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

    fn discover_pairs(
        &self,
        cache: &mut EvmCache,
        now_timestamp: u64,
    ) -> Result<OracleDiscoveryReport, OracleError> {
        let mut report = OracleDiscoveryReport::new();
        for pair in &self.pairs {
            match self.discover_pair(cache, pair, now_timestamp) {
                Ok(feed) => report = report.with_feed(feed),
                Err(error) => {
                    // Report the user-facing quote target (direct oracle or
                    // router) so router-pair skips carry a real address.
                    let skip_target = pair.oracle.or(self.router).unwrap_or_default();
                    report = report.with_skip(OracleAdapterFeedSkip {
                        feed: crate::Feed::proxy(skip_target).build(),
                        proxy: skip_target,
                        reason: OracleAdapterSkipReason::UnsupportedEulerSource {
                            error: error.to_string(),
                        },
                    });
                }
            }
        }
        Ok(report)
    }

    fn discover_pair(
        &self,
        cache: &mut EvmCache,
        pair: &EulerQuotePair,
        now_timestamp: u64,
    ) -> Result<OracleDiscoveredFeed, OracleError> {
        let oracle = match pair.oracle {
            Some(oracle) => oracle,
            None => self.resolve_router_oracle(cache, pair.base, pair.quote)?,
        };
        // The user-facing contract quotes are read through: the direct oracle
        // adapter when configured, otherwise the router. Every pair gets its
        // own synthetic registry state key so multiple pairs resolved through
        // one router (or one shared adapter serving both directions) never
        // collide on the registry's unique-proxy invariant.
        let quote_target = pair.oracle.or(self.router).unwrap_or(oracle);
        let proxy = pair
            .state_key
            .unwrap_or_else(|| EulerQuotePair::state_key_for(quote_target, pair.base, pair.quote));
        let base_decimals = read_asset_decimals(cache, pair.base)?;
        let quote_decimals = read_asset_decimals(cache, pair.quote)?;
        let source = self.read_source(
            cache,
            oracle,
            pair.base,
            pair.quote,
            base_decimals,
            quote_decimals,
            0,
        )?;
        // Rewrite the discovered source's user-facing address to the quote
        // target: for router-resolved pairs the router is the address callers
        // (and the read overlay) quote through, while the legs keep the
        // resolved adapter addresses for event routing.
        let source = source.with_euler_user_source(quote_target);
        // Seed the cold-start value from an authoritative on-chain
        // `getQuote(one base unit, base, quote)` read through the quote
        // target, so the seeded snapshot honestly carries
        // `Confirmed`/`Proxy` semantics. Euler adapters enforce their own
        // `maxStaleness` inside `getQuote`, so a successful read implies the
        // oracle currently considers its dependencies fresh; a revert (stale,
        // unsupported, or misconfigured pair) skips the feed with the revert
        // reason instead of seeding a recomputed value.
        let in_amount = one_base_unit(base_decimals)?;
        let quote_out = cache
            .call_sol(
                quote_target,
                getQuoteCall {
                    inAmount: in_amount,
                    base: pair.base,
                    quote: pair.quote,
                },
            )
            .map_err(|error| {
                OracleError::Provider(format!(
                    "Euler getQuote({in_amount}, {:?}, {:?}) via {quote_target:?} failed: {error}",
                    pair.base, pair.quote
                ))
            })?;
        let answer = i256_from_u256(quote_out, "Euler getQuote outAmount")?;
        let id = pair.id.clone().unwrap_or_else(|| {
            FeedId::new(format!(
                "euler-{quote_target:?}-{:?}-{:?}",
                pair.base, pair.quote
            ))
        });
        let label = pair
            .label
            .clone()
            .or_else(|| Some(format!("Euler {:?}/{:?}", pair.base, pair.quote)));
        let metadata = FeedMetadata {
            decimals: quote_decimals,
            description: label.clone().unwrap_or_else(|| "Euler quote".to_string()),
            version: U256::ZERO,
        };
        let registration = FeedRegistration {
            id,
            proxy,
            label,
            base: pair
                .base_label
                .clone()
                .map(String::from)
                .or_else(|| Some(format!("{:?}", pair.base))),
            quote: pair
                .quote_label
                .clone()
                .map(String::from)
                .or_else(|| Some(format!("{:?}", pair.quote))),
            staleness: pair.staleness,
            current_aggregator: source.event_aggregators(None).first().copied(),
            aggregator_layout: None,
            metadata,
            source,
            status: OracleFeedStatus::Ready,
        };
        let round = RoundData {
            round_id: U256::ZERO,
            answer,
            started_at: now_timestamp,
            updated_at: now_timestamp,
            answered_in_round: U256::ZERO,
        };
        Ok(OracleDiscoveredFeed::new(registration, round))
    }

    fn resolve_router_oracle(
        &self,
        cache: &mut EvmCache,
        base: Address,
        quote: Address,
    ) -> Result<Address, OracleError> {
        let router = self.router.ok_or_else(|| {
            OracleError::Config(crate::error::OracleConfigError::Other(
                "Euler router or direct oracle is required".to_string(),
            ))
        })?;
        let configured = cache
            .call_sol(router, getConfiguredOracleCall { base, quote })
            .map_err(provider_error)?;
        if configured != Address::ZERO {
            return Ok(configured);
        }
        let fallback = cache
            .call_sol(router, fallbackOracleCall {})
            .map_err(provider_error)?;
        if fallback != Address::ZERO {
            return Ok(fallback);
        }
        Err(OracleError::Unsupported(
            "Euler router has no configured or fallback oracle for pair".to_string(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn read_source(
        &self,
        cache: &mut EvmCache,
        oracle: Address,
        requested_base: Address,
        requested_quote: Address,
        requested_base_decimals: u8,
        requested_quote_decimals: u8,
        depth: usize,
    ) -> Result<FeedSource, OracleError> {
        if depth > 4 {
            return Err(OracleError::Unsupported(
                "Euler oracle nesting depth exceeded".to_string(),
            ));
        }
        let name = cache
            .call_sol(oracle, nameCall {})
            .map_err(provider_error)?;
        match name.as_str() {
            "ChainlinkOracle" => {
                let leg = self.read_chainlink_leg(
                    cache,
                    oracle,
                    requested_base,
                    requested_quote,
                    requested_quote_decimals,
                )?;
                Ok(FeedSource::euler_quote(
                    oracle,
                    requested_base,
                    requested_quote,
                    requested_base_decimals,
                    requested_quote_decimals,
                    leg,
                ))
            }
            "FixedRateOracle" => {
                let leg = self.read_fixed_rate_leg(
                    cache,
                    oracle,
                    requested_base,
                    requested_quote,
                    requested_base_decimals,
                    requested_quote_decimals,
                )?;
                Ok(FeedSource::euler_quote(
                    oracle,
                    requested_base,
                    requested_quote,
                    requested_base_decimals,
                    requested_quote_decimals,
                    leg,
                ))
            }
            "RateProviderOracle" => {
                let leg = self.read_rate_provider_leg(
                    cache,
                    oracle,
                    requested_base,
                    requested_quote,
                    requested_quote_decimals,
                )?;
                Ok(FeedSource::euler_quote(
                    oracle,
                    requested_base,
                    requested_quote,
                    requested_base_decimals,
                    requested_quote_decimals,
                    leg,
                ))
            }
            "CrossAdapter" => self.read_cross_source(
                cache,
                oracle,
                requested_base,
                requested_quote,
                requested_base_decimals,
                requested_quote_decimals,
                depth,
            ),
            other => Err(OracleError::Unsupported(format!(
                "unsupported Euler oracle adapter {other}"
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn read_cross_source(
        &self,
        cache: &mut EvmCache,
        oracle: Address,
        requested_base: Address,
        requested_quote: Address,
        requested_base_decimals: u8,
        requested_quote_decimals: u8,
        depth: usize,
    ) -> Result<FeedSource, OracleError> {
        let configured_base = cache
            .call_sol(oracle, crossBaseCall {})
            .map_err(provider_error)?;
        let cross = cache
            .call_sol(oracle, crossCall {})
            .map_err(provider_error)?;
        let configured_quote = cache
            .call_sol(oracle, crossQuoteCall {})
            .map_err(provider_error)?;
        let inverse = direction_or_revert(
            requested_base,
            configured_base,
            requested_quote,
            configured_quote,
        )?;
        let oracle_base_cross = cache
            .call_sol(oracle, oracleBaseCrossCall {})
            .map_err(provider_error)?;
        let oracle_cross_quote = cache
            .call_sol(oracle, oracleCrossQuoteCall {})
            .map_err(provider_error)?;
        let cross_decimals = read_asset_decimals(cache, cross)?;
        let (first_oracle, second_oracle) = if inverse {
            (oracle_cross_quote, oracle_base_cross)
        } else {
            (oracle_base_cross, oracle_cross_quote)
        };
        let base_cross = self.read_leg(
            cache,
            first_oracle,
            requested_base,
            cross,
            requested_base_decimals,
            cross_decimals,
            depth + 1,
        )?;
        let cross_quote = self.read_leg(
            cache,
            second_oracle,
            cross,
            requested_quote,
            cross_decimals,
            requested_quote_decimals,
            depth + 1,
        )?;

        Ok(FeedSource::euler_cross(
            oracle,
            requested_base,
            cross,
            requested_quote,
            requested_base_decimals,
            cross_decimals,
            requested_quote_decimals,
            base_cross,
            cross_quote,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn read_leg(
        &self,
        cache: &mut EvmCache,
        oracle: Address,
        requested_base: Address,
        requested_quote: Address,
        requested_base_decimals: u8,
        requested_quote_decimals: u8,
        depth: usize,
    ) -> Result<EulerQuoteLeg, OracleError> {
        let source = self.read_source(
            cache,
            oracle,
            requested_base,
            requested_quote,
            requested_base_decimals,
            requested_quote_decimals,
            depth,
        )?;
        match source {
            FeedSource::EulerQuote { leg, .. } => Ok(leg),
            FeedSource::EulerCross { .. } => Err(OracleError::Unsupported(
                "nested Euler CrossAdapter legs are not supported in this MVP".to_string(),
            )),
            _ => Err(OracleError::Unsupported(
                "Euler child source did not produce a quote leg".to_string(),
            )),
        }
    }

    fn read_chainlink_leg(
        &self,
        cache: &mut EvmCache,
        oracle: Address,
        requested_base: Address,
        requested_quote: Address,
        requested_quote_decimals: u8,
    ) -> Result<EulerQuoteLeg, OracleError> {
        let configured_base = cache
            .call_sol(oracle, chainlinkBaseCall {})
            .map_err(provider_error)?;
        let configured_quote = cache
            .call_sol(oracle, chainlinkQuoteCall {})
            .map_err(provider_error)?;
        let inverse = direction_or_revert(
            requested_base,
            configured_base,
            requested_quote,
            configured_quote,
        )?;
        let feed = cache
            .call_sol(oracle, feedCall {})
            .map_err(provider_error)?;
        let _max_staleness = cache
            .call_sol(oracle, maxStalenessCall {})
            .map_err(provider_error)?;
        let reader = EvmCacheChainlinkReader::new(cache);
        let feed_decimals = reader.read_decimals(feed)?;
        let round = reader.read_latest_round_data(feed)?;
        let aggregator = reader.read_aggregator(feed)?.ok_or_else(|| {
            OracleError::Unsupported(
                "Euler ChainlinkOracle feed aggregator() returned none".to_string(),
            )
        })?;
        Ok(EulerQuoteLeg::Chainlink {
            oracle,
            feed,
            aggregator,
            answer: round.answer,
            feed_decimals,
            inverse,
            quote_decimals: requested_quote_decimals,
        })
    }

    fn read_fixed_rate_leg(
        &self,
        cache: &mut EvmCache,
        oracle: Address,
        requested_base: Address,
        requested_quote: Address,
        requested_base_decimals: u8,
        requested_quote_decimals: u8,
    ) -> Result<EulerQuoteLeg, OracleError> {
        let configured_base = cache
            .call_sol(oracle, fixedBaseCall {})
            .map_err(provider_error)?;
        let configured_quote = cache
            .call_sol(oracle, fixedQuoteCall {})
            .map_err(provider_error)?;
        let inverse = direction_or_revert(
            requested_base,
            configured_base,
            requested_quote,
            configured_quote,
        )?;
        let rate = cache
            .call_sol(oracle, rateCall {})
            .map_err(provider_error)?;
        Ok(EulerQuoteLeg::FixedRate {
            oracle,
            rate: i256_from_u256(rate, "rate")?,
            inverse,
            base_decimals: requested_base_decimals,
            quote_decimals: requested_quote_decimals,
        })
    }

    fn read_rate_provider_leg(
        &self,
        cache: &mut EvmCache,
        oracle: Address,
        requested_base: Address,
        requested_quote: Address,
        requested_quote_decimals: u8,
    ) -> Result<EulerQuoteLeg, OracleError> {
        let configured_base = cache
            .call_sol(oracle, rateBaseCall {})
            .map_err(provider_error)?;
        let configured_quote = cache
            .call_sol(oracle, rateQuoteCall {})
            .map_err(provider_error)?;
        let inverse = direction_or_revert(
            requested_base,
            configured_base,
            requested_quote,
            configured_quote,
        )?;
        let rate_provider = cache
            .call_sol(oracle, rateProviderCall {})
            .map_err(provider_error)?;
        let rate = cache
            .call_sol(rate_provider, getRateCall {})
            .map_err(provider_error)?;
        Ok(EulerQuoteLeg::RateProvider {
            oracle,
            rate_provider,
            rate: i256_from_u256(rate, "rate")?,
            inverse,
            quote_decimals: requested_quote_decimals,
        })
    }
}

impl OracleAdapterPlugin for EulerOracleAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        OracleAdapterId::new(ADAPTER_ID)
    }

    fn discover<'a>(
        &'a self,
        ctx: OracleDiscoveryContext<'a>,
    ) -> AdapterFuture<'a, OracleDiscoveryReport> {
        Box::pin(async move {
            let now_timestamp = self.timestamp(Some(ctx.now_timestamp))?;
            self.discover_pairs(ctx.cache, now_timestamp)
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

fn direction_or_revert(
    requested_base: Address,
    configured_base: Address,
    requested_quote: Address,
    configured_quote: Address,
) -> Result<bool, OracleError> {
    if requested_base == configured_base && requested_quote == configured_quote {
        Ok(false)
    } else if requested_base == configured_quote && requested_quote == configured_base {
        Ok(true)
    } else {
        Err(OracleError::Unsupported(
            "Euler oracle does not support requested base/quote pair".to_string(),
        ))
    }
}

fn read_asset_decimals(cache: &mut EvmCache, asset: Address) -> Result<u8, OracleError> {
    if is_reserved_asset_address(asset) {
        return Ok(18);
    }
    match cache.call_sol(asset, decimalsCall {}) {
        Ok(decimals) => Ok(decimals),
        Err(_) => Ok(18),
    }
}

fn is_reserved_asset_address(asset: Address) -> bool {
    // Euler denominates non-token quotes as ISO 4217 numeric currency codes
    // cast to addresses (e.g. USD = address(840)). Any address whose value
    // fits the reserved 32-bit range — i.e. whose top 16 bytes are zero — is
    // treated as such a pseudo-asset and priced with 18 decimals.
    asset.as_slice()[..16].iter().all(|byte| *byte == 0)
}

/// One base unit (`10^base_decimals`) for authoritative `getQuote` reads.
///
/// Shared with the derived-reconciliation reader so discovery seeding and
/// post-event protocol reads quote exactly the same in-amount.
pub(crate) fn one_base_unit(base_decimals: u8) -> Result<U256, OracleError> {
    U256::from(10_u64)
        .checked_pow(U256::from(base_decimals))
        .ok_or_else(|| {
            OracleError::Unsupported(format!(
                "Euler base decimals {base_decimals} overflow a uint256 unit amount"
            ))
        })
}

fn i256_from_u256(value: U256, field: &'static str) -> Result<I256, OracleError> {
    I256::try_from(value)
        .map_err(|_| OracleError::Unsupported(format!("{field} does not fit int256")))
}

fn provider_error(error: impl ToString) -> OracleError {
    OracleError::Provider(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_keys_are_deterministic_and_distinct_per_pair_and_target() {
        let router = Address::repeat_byte(0x01);
        let other_router = Address::repeat_byte(0x02);
        let weth = Address::repeat_byte(0x0a);
        let usdc = Address::repeat_byte(0x0b);
        let dai = Address::repeat_byte(0x0c);

        let weth_usdc = EulerQuotePair::state_key_for(router, weth, usdc);
        assert_eq!(
            weth_usdc,
            EulerQuotePair::state_key_for(router, weth, usdc),
            "state keys must be deterministic"
        );
        // Distinct pairs on one router, inverse direction on one router, and
        // the same pair on another router must never collide.
        assert_ne!(weth_usdc, EulerQuotePair::state_key_for(router, weth, dai));
        assert_ne!(weth_usdc, EulerQuotePair::state_key_for(router, usdc, weth));
        assert_ne!(
            weth_usdc,
            EulerQuotePair::state_key_for(other_router, weth, usdc)
        );
        // And the key must not shadow the real router address.
        assert_ne!(weth_usdc, router);
    }

    #[test]
    fn reserved_asset_addresses_cover_iso4217_pseudo_assets() {
        // USD = address(840) per Euler's denomination convention.
        let usd = Address::from_slice(&{
            let mut bytes = [0_u8; 20];
            bytes[18] = 0x03;
            bytes[19] = 0x48;
            bytes
        });
        assert!(is_reserved_asset_address(usd));
        assert!(is_reserved_asset_address(Address::ZERO));
        // The full 32-bit reserved range boundary is still reserved.
        let boundary = Address::from_slice(&{
            let mut bytes = [0_u8; 20];
            bytes[16..].copy_from_slice(&0xffff_ffff_u32.to_be_bytes());
            bytes
        });
        assert!(is_reserved_asset_address(boundary));
        // A real token address is not.
        assert!(!is_reserved_asset_address(Address::repeat_byte(0x42)));
    }

    #[test]
    fn one_base_unit_scales_by_decimals() {
        assert_eq!(one_base_unit(0).unwrap(), U256::from(1_u64));
        assert_eq!(
            one_base_unit(18).unwrap(),
            U256::from(1_000_000_000_000_000_000_u128)
        );
        assert!(one_base_unit(78).is_err(), "10^78 overflows uint256");
    }
}
