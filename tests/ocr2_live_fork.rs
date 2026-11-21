#![cfg(feature = "live-tests")]

use std::{error::Error, sync::Arc};

use evm_oracle_state::alloy_eips::{BlockId, BlockNumberOrTag};
use evm_oracle_state::alloy_network::{AnyNetwork, Ethereum, TransactionBuilder};
use alloy_primitives::{Address, B256, Bytes, I256, Log as PrimitiveLog, U256, address, b256};
use evm_oracle_state::alloy_provider::{Provider, RootProvider};
use evm_oracle_state::alloy_rpc_types_eth::{Log as RpcLog, TransactionRequest};
use alloy_sol_types::SolCall;
use evm_fork_cache::{
    StateUpdate,
    cache::EvmCache,
    reactive::{
        BlockRef, ChainStatus, InputSource, ReactiveContext, ReactiveEffect, ReactiveHandler,
        ReactiveInput,
    },
};
use evm_oracle_state::{
    AggregatorLayout, ChainlinkFeedProvider, FeedConfig, OCR2_NEW_TRANSMISSION_TOPIC, OracleError,
    OraclePriceUpdate, OracleReactiveHandler, OracleRegistry, OracleSignalKind, ProviderFuture,
    RoundData, StalenessPolicy,
};
use evm_oracle_state::revm::context::result::ExecutionResult;

alloy_sol_types::sol! {
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

const ETH_USD_PROXY: Address = address!("5f4eC3Df9cbd43714FE2740f5E3616155c5b8419");
const ETH_USD_OCR2_AGGREGATOR: Address = address!("7d4E742018fb52E48b08BE73d041C18B21de6Fb5");
const PRE_UPDATE_BLOCK: u64 = 25_395_286;
const UPDATE_BLOCK: u64 = 25_395_287;
const EVENT_LOCAL_ROUND_ID: u64 = 30_837;
const EVENT_ANSWER: i64 = 156_892_558_834;
const EVENT_UPDATED_AT: u64 = 1_782_398_279;
const ACTUAL_STARTED_AT: u64 = 1_782_398_255;
const UPDATE_BLOCK_HASH: B256 =
    b256!("02f17a5c5f813ee0f54f31645b4489ec56ffa24d03b4fc02b8cc1815d2b2a507");

#[tokio::test(flavor = "multi_thread")]
async fn ocr2_listener_replays_real_eth_usd_update() -> Result<(), Box<dyn Error>> {
    // Skip (instead of failing) when no live RPC endpoint is configured, so
    // `cargo test --all-features` stays green offline; the release live smoke
    // runner sets ETH_RPC_URL and exercises the full replay.
    let Ok(rpc_url) = std::env::var("ETH_RPC_URL").or_else(|_| std::env::var("E2E_RPC_URL")) else {
        eprintln!("ETH_RPC_URL/E2E_RPC_URL unset; skipping OCR2 live fork replay test.");
        return Ok(());
    };
    let provider = RootProvider::<AnyNetwork>::connect(&rpc_url).await?;
    let mut pre_cache = EvmCache::at_block(
        Arc::new(provider.clone()),
        BlockId::Number(BlockNumberOrTag::Number(PRE_UPDATE_BLOCK)),
    )
    .await;

    let pre = latest_round_data(&mut pre_cache)?;
    assert_eq!(pre.answer, I256::unchecked_from(157_760_820_000_i64));
    assert_eq!(pre.updated_at, 1_782_398_135);

    let feed_provider = RpcChainlinkProvider {
        provider: provider.clone(),
    };
    let mut registry = OracleRegistry::new_at_timestamp(EVENT_UPDATED_AT);
    let id = registry
        .register_chainlink_feed(
            &feed_provider,
            FeedConfig {
                proxy: ETH_USD_PROXY,
                label: Some("ETH/USD".to_string()),
                base: Some("ETH".to_string()),
                quote: Some("USD".to_string()),
                staleness: StalenessPolicy::max_age(3_600),
                ..Default::default()
            },
        )
        .await?;
    let registration = registry.registration(id).expect("registered feed").clone();
    assert_eq!(
        registration.current_aggregator,
        Some(ETH_USD_OCR2_AGGREGATOR)
    );
    assert_eq!(
        registration
            .aggregator_layout
            .as_ref()
            .map(|evidence| evidence.layout),
        Some(AggregatorLayout::ChainlinkOcr2V1)
    );

    let handler = OracleReactiveHandler::new(vec![registration]);
    let outcome = handler.handle(
        &context(),
        &ReactiveInput::<Ethereum>::Log(real_new_transmission_log()),
        &pre_cache,
    )?;
    let updates = outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ReactiveEffect::StateUpdate(update) => Some(update.clone()),
            _ => None,
        })
        .collect::<Vec<StateUpdate>>();
    assert_eq!(
        updates.len(),
        2,
        "expected hot-vars and transmission writes"
    );
    let hook = outcome
        .effects
        .iter()
        .find_map(|effect| match effect {
            ReactiveEffect::Hook(signal)
                if signal.kind.as_ref() == OracleSignalKind::PriceUpdate.as_str() =>
            {
                signal.payload.as_ref()?.downcast_ref::<OraclePriceUpdate>()
            }
            _ => None,
        })
        .expect("price update hook");
    assert_eq!(hook.started_at, ACTUAL_STARTED_AT);
    assert_eq!(hook.updated_at, EVENT_UPDATED_AT);

    let diff = pre_cache.apply_updates(&updates);
    assert!(
        diff.is_fully_applied(),
        "OCR2 storage writes must not be skipped: {diff:?}"
    );

    let patched = latest_round_data(&mut pre_cache)?;
    let mut post_cache = EvmCache::at_block(
        Arc::new(provider),
        BlockId::Number(BlockNumberOrTag::Number(UPDATE_BLOCK)),
    )
    .await;
    let post = latest_round_data(&mut post_cache)?;

    assert_eq!(post.answer, I256::unchecked_from(EVENT_ANSWER));
    assert_eq!(post.updated_at, EVENT_UPDATED_AT);
    assert_eq!(post.started_at, ACTUAL_STARTED_AT);

    assert_eq!(patched.round_id, post.round_id);
    assert_eq!(patched.answer, post.answer);
    assert_eq!(patched.updated_at, post.updated_at);
    assert_eq!(patched.answered_in_round, post.answered_in_round);

    assert_eq!(patched.started_at, post.started_at);

    Ok(())
}

fn context() -> ReactiveContext {
    let block = BlockRef {
        number: UPDATE_BLOCK,
        hash: UPDATE_BLOCK_HASH,
        parent_hash: None,
        timestamp: Some(EVENT_UPDATED_AT),
    };
    ReactiveContext {
        chain_id: Some(1),
        source: InputSource::Synthetic,
        chain_status: ChainStatus::Included {
            block,
            confirmations: 0,
        },
        block: Some(block),
        transaction_index: Some(46),
        log_index: Some(296),
    }
}

fn real_new_transmission_log() -> RpcLog {
    RpcLog {
        inner: PrimitiveLog::new_unchecked(
            ETH_USD_OCR2_AGGREGATOR,
            vec![
                OCR2_NEW_TRANSMISSION_TOPIC,
                B256::from(U256::from(EVENT_LOCAL_ROUND_ID).to_be_bytes::<32>()),
            ],
            Bytes::from(alloy_primitives::hex::decode("00000000000000000000000000000000000000000000000000000024878675f20000000000000000000000006d53d5e35f5226a1613877e071b81217387ac6b5000000000000000000000000000000000000000000000000000000006a3d3d2f0000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000050000000000000000000000000000000000000000000000000000000000000000000001a447e205c13e30207a14a24a65b97642a997b78faaac2bb16ea192f1440300000000000000000000000000000000000000000000000000000000001a8904000000000000000000000000000000000000000000000000000000000000001f00000000000000000000000000000000000000000000000000000024865c04400000000000000000000000000000000000000000000000000000002486a136e00000000000000000000000000000000000000000000000000000002486ba02c00000000000000000000000000000000000000000000000000000002486ba02c00000000000000000000000000000000000000000000000000000002486ba02c00000000000000000000000000000000000000000000000000000002486ba02c00000000000000000000000000000000000000000000000000000002486ba02c00000000000000000000000000000000000000000000000000000002486ba02c000000000000000000000000000000000000000000000000000000024870ea6b800000000000000000000000000000000000000000000000000000024870ea6b800000000000000000000000000000000000000000000000000000024878675f200000000000000000000000000000000000000000000000000000024878675f200000000000000000000000000000000000000000000000000000024878675f200000000000000000000000000000000000000000000000000000024878675f200000000000000000000000000000000000000000000000000000024878675f200000000000000000000000000000000000000000000000000000024878675f200000000000000000000000000000000000000000000000000000024878675f20000000000000000000000000000000000000000000000000000002487ca3a400000000000000000000000000000000000000000000000000000002487ca3a400000000000000000000000000000000000000000000000000000002487ca3a40000000000000000000000000000000000000000000000000000000248803683b000000000000000000000000000000000000000000000000000000248803683b000000000000000000000000000000000000000000000000000000248803683b000000000000000000000000000000000000000000000000000000248803683b0000000000000000000000000000000000000000000000000000002488516ba00000000000000000000000000000000000000000000000000000002488538e80000000000000000000000000000000000000000000000000000000248a1aa5ad000000000000000000000000000000000000000000000000000000248a1aa5ad000000000000000000000000000000000000000000000000000000248a1aa5ad000000000000000000000000000000000000000000000000000000248a43c222000000000000000000000000000000000000000000000000000000248a6cde98000000000000000000000000000000000000000000000000000000000000001f1e001d1a1917061610121514050e0a11130d0f0903070c0b0801181c021b0400").expect("valid log data")),
        ),
        block_hash: Some(UPDATE_BLOCK_HASH),
        block_number: Some(UPDATE_BLOCK),
        block_timestamp: Some(EVENT_UPDATED_AT),
        transaction_hash: Some(b256!(
            "1a76747998466afa6be51798e96b1030a7aaa3feb9e26e5700170333e1a8d3d7"
        )),
        transaction_index: Some(46),
        log_index: Some(296),
        removed: false,
    }
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

#[derive(Debug)]
struct Round {
    round_id: U256,
    answer: I256,
    started_at: u64,
    updated_at: u64,
    answered_in_round: U256,
}

fn latest_round_data(cache: &mut EvmCache) -> Result<Round, Box<dyn Error>> {
    let result = cache.call_raw(
        Address::ZERO,
        ETH_USD_PROXY,
        Bytes::from(latestRoundDataCall {}.abi_encode()),
        false,
    )?;

    let ExecutionResult::Success { output, .. } = result else {
        return Err(format!("latestRoundData failed: {result:?}").into());
    };
    let decoded = latestRoundDataCall::abi_decode_returns(&output.into_data())?;

    Ok(Round {
        round_id: U256::from(decoded.roundId),
        answer: decoded.answer,
        started_at: u64::try_from(decoded.startedAt)?,
        updated_at: u64::try_from(decoded.updatedAt)?,
        answered_in_round: U256::from(decoded.answeredInRound),
    })
}
