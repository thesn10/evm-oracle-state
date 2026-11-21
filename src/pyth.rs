use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::alloy_network::Ethereum;
use alloy_primitives::{Address, B256, I256, U256, keccak256};
use crate::alloy_rpc_types_eth::Filter;
use alloy_sol_types::sol;
use evm_fork_cache::{
    StateView,
    cache::EvmCache,
    reactive::{
        ChainStatus, HandlerError, HandlerId, HandlerOutcome, HookSignal, InvalidationReason,
        InvalidationRequest, LogInterest, ReactiveContext, ReactiveEffect, ReactiveHandler,
        ReactiveInput, ReactiveInterest, ReportTag, RouteKeySpec, StateEffectQuality,
    },
    state_update::PurgeScope,
};

use crate::{
    AdapterFuture, FeedId, FeedMetadata, FeedRegistration, FeedSource, ORACLE_SIGNAL_NAMESPACE,
    OracleAdapterId, OracleAdapterPlugin, OracleDiscoveredFeed, OracleDiscoveryContext,
    OracleDiscoveryReport, OracleError, OracleFeedStatus, OraclePriceUpdate, OracleSignalKind,
    OracleStorageSync, OracleValueSource, OracleValueStatus, PYTH_PRICE_FEED_UPDATE_TOPIC,
    RoundData, StalenessPolicy, decode_pyth_price_feed_update, state::classify_round,
};

const ADAPTER_ID: &str = "evm-oracle-state.pyth";
const HANDLER_ID: &str = "evm-oracle-state.pyth";

sol! {
    struct PythPrice {
        int64 price;
        uint64 conf;
        int32 expo;
        uint256 publishTime;
    }

    interface PythInterface {
        function getPriceUnsafe(bytes32 id) external view returns (PythPrice memory price);
    }
}

use PythInterface::getPriceUnsafeCall;

/// Builder-style Pyth EVM feed registration config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PythFeed {
    pyth: Address,
    price_id: B256,
    state_key: Option<Address>,
    id: Option<FeedId>,
    label: Option<String>,
    base: Option<String>,
    quote: Option<String>,
    staleness: StalenessPolicy,
}

impl PythFeed {
    /// Create a Pyth feed config from the shared Pyth contract and price id.
    pub fn new(pyth: Address, price_id: B256) -> Self {
        Self {
            pyth,
            price_id,
            state_key: None,
            id: None,
            label: None,
            base: None,
            quote: None,
            staleness: StalenessPolicy::max_age(300),
        }
    }

    /// Set the typed feed id.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(FeedId::new(id));
        self
    }

    /// Set the human-readable label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the base symbol.
    pub fn base(mut self, base: impl Into<String>) -> Self {
        self.base = Some(base.into());
        self
    }

    /// Set the quote symbol.
    pub fn quote(mut self, quote: impl Into<String>) -> Self {
        self.quote = Some(quote.into());
        self
    }

    /// Set the max-age staleness policy.
    pub fn max_age_secs(mut self, max_age_secs: u64) -> Self {
        self.staleness = StalenessPolicy::max_age(max_age_secs);
        self
    }

    /// Set an explicit per-feed state key.
    pub fn state_key(mut self, state_key: Address) -> Self {
        self.state_key = Some(state_key);
        self
    }

    /// Shared Pyth contract address.
    pub fn pyth(&self) -> Address {
        self.pyth
    }

    /// Pyth price feed id.
    pub fn price_id(&self) -> B256 {
        self.price_id
    }

    /// Synthetic per-feed state key used as the registry proxy.
    pub fn proxy(&self) -> Address {
        self.state_key
            .unwrap_or_else(|| Self::state_key_for(self.pyth, self.price_id))
    }

    /// Deterministically derive a synthetic per-feed state key.
    pub fn state_key_for(pyth: Address, price_id: B256) -> Address {
        let mut bytes = Vec::with_capacity(32 + 20 + 32);
        bytes.extend_from_slice(ADAPTER_ID.as_bytes());
        bytes.extend_from_slice(pyth.as_slice());
        bytes.extend_from_slice(price_id.as_slice());
        let hash = keccak256(bytes);
        Address::from_slice(&hash.as_slice()[12..])
    }

    fn feed_id(&self) -> FeedId {
        self.id.clone().unwrap_or_else(|| {
            let price_id = self.price_id;
            FeedId::new(format!("pyth-{price_id:?}"))
        })
    }

    fn description(&self) -> String {
        self.label
            .clone()
            .unwrap_or_else(|| match (&self.base, &self.quote) {
                (Some(base), Some(quote)) => format!("Pyth {base}/{quote}"),
                _ => {
                    let price_id = self.price_id;
                    format!("Pyth {price_id:?}")
                }
            })
    }
}

/// Pyth EVM oracle adapter plugin.
#[derive(Clone, Debug, Default)]
pub struct PythOracleAdapter {
    feeds: Vec<PythFeed>,
}

impl PythOracleAdapter {
    /// Create an empty Pyth adapter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one Pyth feed config.
    pub fn feed(mut self, feed: PythFeed) -> Self {
        self.feeds.push(feed);
        self
    }

    /// Add multiple Pyth feed configs.
    pub fn feeds(mut self, feeds: impl IntoIterator<Item = PythFeed>) -> Self {
        self.feeds.extend(feeds);
        self
    }
}

impl OracleAdapterPlugin for PythOracleAdapter {
    fn adapter_id(&self) -> OracleAdapterId {
        OracleAdapterId::new(ADAPTER_ID)
    }

    fn discover<'a>(
        &'a self,
        ctx: OracleDiscoveryContext<'a>,
    ) -> AdapterFuture<'a, OracleDiscoveryReport> {
        Box::pin(async move {
            let mut report = OracleDiscoveryReport::new();
            for feed in &self.feeds {
                let price = read_pyth_price(ctx.cache, feed.pyth, feed.price_id)?;
                let (answer, decimals) = scale_pyth_price(price.price, price.expo)?;
                let publish_time = u256_to_u64(price.publishTime, "publishTime")?;
                let id = feed.feed_id();
                let metadata = FeedMetadata {
                    decimals,
                    description: feed.description(),
                    version: U256::ZERO,
                };
                let registration = FeedRegistration {
                    id,
                    proxy: feed.proxy(),
                    label: feed.label.clone(),
                    base: feed.base.clone(),
                    quote: feed.quote.clone(),
                    staleness: feed.staleness,
                    current_aggregator: Some(feed.pyth),
                    aggregator_layout: None,
                    metadata,
                    source: FeedSource::pyth(feed.pyth, feed.price_id, price.expo, price.conf),
                    status: OracleFeedStatus::Ready,
                };
                let round = RoundData {
                    round_id: U256::from(publish_time),
                    answer,
                    started_at: publish_time,
                    updated_at: publish_time,
                    answered_in_round: U256::from(publish_time),
                };
                report = report.with_feed(OracleDiscoveredFeed::new(registration, round));
            }
            Ok(report)
        })
    }

    fn reactive_handler(
        &self,
        registrations: Vec<FeedRegistration>,
        _storage_sync: OracleStorageSync,
    ) -> Arc<dyn ReactiveHandler<Ethereum>> {
        Arc::new(PythReactiveHandler::new(registrations))
    }
}

/// Reactive handler for Pyth EVM `PriceFeedUpdate` logs.
#[derive(Clone, Debug)]
pub struct PythReactiveHandler {
    registrations_by_price: BTreeMap<(Address, B256), Vec<FeedRegistration>>,
    pyth_contracts: BTreeSet<Address>,
}

impl PythReactiveHandler {
    /// Create a Pyth handler from current feed registrations.
    pub fn new(registrations: Vec<FeedRegistration>) -> Self {
        let mut registrations_by_price: BTreeMap<(Address, B256), Vec<FeedRegistration>> =
            BTreeMap::new();
        let mut pyth_contracts = BTreeSet::new();

        for registration in registrations {
            let Some((pyth, price_id, _expo, _conf)) = registration.source.pyth_source() else {
                continue;
            };
            pyth_contracts.insert(pyth);
            registrations_by_price
                .entry((pyth, price_id))
                .or_default()
                .push(registration);
        }

        Self {
            registrations_by_price,
            pyth_contracts,
        }
    }

    /// Stable handler id.
    pub fn id(&self) -> HandlerId {
        HandlerId::new(HANDLER_ID)
    }

    /// Interests for registered Pyth contracts.
    pub fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        self.pyth_contracts
            .iter()
            .map(|pyth| {
                ReactiveInterest::Logs(LogInterest {
                    provider_filter: Filter::new()
                        .address(*pyth)
                        .event_signature(PYTH_PRICE_FEED_UPDATE_TOPIC),
                    local_matcher: None,
                    route_key: Some(RouteKeySpec::EmitterAddress),
                })
            })
            .collect()
    }
}

impl ReactiveHandler<Ethereum> for PythReactiveHandler {
    fn id(&self) -> HandlerId {
        self.id()
    }

    fn interests(&self) -> Vec<ReactiveInterest<Ethereum>> {
        self.interests()
    }

    fn handle(
        &self,
        ctx: &ReactiveContext,
        input: &ReactiveInput<Ethereum>,
        _state: &dyn StateView,
    ) -> Result<HandlerOutcome, HandlerError> {
        let ReactiveInput::Log(log) = input else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };
        if log.removed && !matches!(ctx.chain_status, ChainStatus::Reorged { .. }) {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        }
        if log.topics().first().copied() != Some(PYTH_PRICE_FEED_UPDATE_TOPIC) {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        }

        let event = decode_pyth_price_feed_update(log).map_err(|error| {
            HandlerError::new(format!("decode Pyth PriceFeedUpdate failed: {error}"))
        })?;
        let key = (event.pyth, event.price_id);
        let Some(registrations) = self.registrations_by_price.get(&key) else {
            return Ok(HandlerOutcome::empty(StateEffectQuality::NoStateEffect));
        };

        let block_number = event
            .block_number
            .or_else(|| ctx.block.as_ref().map(|block| block.number));
        let block_hash = log
            .block_hash
            .or_else(|| ctx.block.as_ref().map(|block| block.hash));
        let log_index = event.log_index.or(ctx.log_index);
        let value_status = if log.removed {
            OracleValueStatus::RequiresRepair
        } else {
            OracleValueStatus::EventPending
        };

        let mut effects = vec![ReactiveEffect::Invalidate(InvalidationRequest {
            scope: PurgeScope::AllStorage,
            address: event.pyth,
            reason: InvalidationReason::HandlerRequested,
        })];
        let mut tags = Vec::new();

        for registration in registrations {
            let (_pyth, _price_id, expo, _conf) =
                registration.source.pyth_source().ok_or_else(|| {
                    HandlerError::new("matched Pyth registration missing Pyth source")
                })?;
            let (answer, decimals) = scale_pyth_price(event.price, expo)
                .map_err(|error| HandlerError::new(format!("{error}")))?;
            let round = RoundData {
                round_id: U256::from(event.publish_time),
                answer,
                started_at: event.publish_time,
                updated_at: event.publish_time,
                answered_in_round: U256::from(event.publish_time),
            };
            let round_status = classify_round(&round, event.publish_time, &registration.staleness);
            let hook_tags =
                pyth_hook_tags(registration, event.pyth, event.price_id, expo, event.conf);
            tags.extend(hook_tags.clone());
            effects.push(ReactiveEffect::Hook(HookSignal {
                namespace: Cow::Borrowed(ORACLE_SIGNAL_NAMESPACE),
                kind: Cow::Borrowed(OracleSignalKind::PriceUpdate.as_str()),
                labels: hook_tags,
                payload: Some(Arc::new(OraclePriceUpdate {
                    id: registration.id.clone(),
                    proxy: registration.proxy,
                    aggregator: event.pyth,
                    label: registration.label.clone(),
                    base: registration.base.clone(),
                    quote: registration.quote.clone(),
                    raw_answer: answer,
                    decimals,
                    event_round_id: U256::from(event.publish_time),
                    started_at: event.publish_time,
                    updated_at: event.publish_time,
                    block_number,
                    block_hash,
                    log_index,
                    round_status,
                    value_status,
                    source: OracleValueSource::Event,
                })),
            }));
        }

        Ok(HandlerOutcome {
            effects,
            quality: StateEffectQuality::RequiresRepair,
            tags,
        })
    }
}

fn read_pyth_price(
    cache: &mut EvmCache,
    pyth: Address,
    price_id: B256,
) -> Result<PythPrice, OracleError> {
    cache
        .call_sol(pyth, getPriceUnsafeCall { id: price_id })
        .map_err(|error| OracleError::Provider(format!("Pyth getPriceUnsafe failed: {error}")))
}

pub(crate) fn scale_pyth_price(price: i64, expo: i32) -> Result<(I256, u8), OracleError> {
    let mut answer = I256::unchecked_from(price);
    if expo <= 0 {
        let decimals = expo
            .checked_neg()
            .and_then(|value| u8::try_from(value).ok())
            .ok_or_else(|| OracleError::Unsupported(format!("Pyth expo {expo} is too negative")))?;
        return Ok((answer, decimals));
    }

    let scale = I256::unchecked_from(10_i8)
        .checked_pow(U256::from(expo as u32))
        .ok_or_else(|| OracleError::Unsupported(format!("Pyth expo {expo} scale overflowed")))?;
    answer = answer
        .checked_mul(scale)
        .ok_or_else(|| OracleError::Unsupported("Pyth scaled price overflowed".to_string()))?;
    Ok((answer, 0))
}

fn pyth_hook_tags(
    registration: &FeedRegistration,
    pyth: Address,
    price_id: B256,
    expo: i32,
    conf: u64,
) -> Vec<ReportTag> {
    vec![
        ReportTag::new("feed_id", registration.id.to_string()),
        ReportTag::new("proxy", format!("{:?}", registration.proxy)),
        ReportTag::new("aggregator", format!("{pyth:?}")),
        ReportTag::new("pyth_price_id", format!("{price_id:?}")),
        ReportTag::new("pyth_expo", expo.to_string()),
        ReportTag::new("pyth_conf", conf.to_string()),
    ]
}

fn u256_to_u64(value: U256, field: &'static str) -> Result<u64, OracleError> {
    u64::try_from(value).map_err(|_| {
        OracleError::Decode(crate::ChainlinkEventDecodeError::Uint64Overflow { field, value })
    })
}
