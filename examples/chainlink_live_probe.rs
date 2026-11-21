use std::{
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};

use evm_oracle_state::alloy_network::{AnyNetwork, TransactionBuilder};
use alloy_primitives::{Address, Bytes, U256, address};
use evm_oracle_state::alloy_provider::{Provider, RootProvider};
use evm_oracle_state::alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::SolCall;
use evm_oracle_state::{
    ChainlinkFeedProvider, FeedConfig, OracleError, OracleRegistry, ProviderFuture, RoundData,
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

const MAINNET_ETH_USD: Address = address!("5f4eC3Df9cbd43714FE2740f5E3616155c5b8419");

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let Ok(rpc_url) = std::env::var("ETH_RPC_URL").or_else(|_| std::env::var("E2E_RPC_URL")) else {
        println!("ETH_RPC_URL/E2E_RPC_URL unset; skipping Chainlink live probe.");
        return Ok(());
    };

    let proxy = match std::env::var("CHAINLINK_FEED_PROXY") {
        Ok(value) => value.parse::<Address>()?,
        Err(_) => MAINNET_ETH_USD,
    };
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let provider = RootProvider::<AnyNetwork>::connect(&rpc_url).await?;
    let feed_provider = RpcChainlinkProvider { provider };
    let mut registry = OracleRegistry::new_at_timestamp(now);

    let id = registry
        .register_chainlink_feed(
            &feed_provider,
            FeedConfig {
                proxy,
                label: Some("live-probe".to_string()),
                staleness: StalenessPolicy::max_age(24 * 60 * 60),
                ..Default::default()
            },
        )
        .await?;
    let snapshot = registry.latest(proxy).expect("registered snapshot");

    println!("registered feed: {}", id.as_str());
    println!("proxy: {:?}", snapshot.proxy);
    println!("aggregator: {:?}", snapshot.aggregator);
    println!("description: {}", snapshot.metadata.description);
    println!("decimals: {}", snapshot.metadata.decimals);
    println!("answer: {}", snapshot.round.answer);
    println!("updated_at: {}", snapshot.round.updated_at);
    println!("round_status: {:?}", snapshot.round_status);
    println!("value_status: {:?}", snapshot.value_status);
    println!("source: {:?}", snapshot.source);

    Ok(())
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
