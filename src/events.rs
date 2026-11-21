use alloy_primitives::{Address, B256, FixedBytes, I256, U256, b256};
use crate::alloy_rpc_types_eth::Log as RpcLog;
use alloy_sol_types::SolEvent;

use crate::ChainlinkEventDecodeError;

/// `AnswerUpdated(int256,uint256,uint256)` topic0.
pub const ANSWER_UPDATED_TOPIC: B256 =
    b256!("0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f");

/// `NewRound(uint256,address,uint256)` topic0.
pub const NEW_ROUND_TOPIC: B256 =
    b256!("0109fc6f55cf40689f02fbaad7af7fe7bbac8a3d2186600afc7d3e10cac60271");

/// OCR2 `NewTransmission(uint32,int192,address,uint32,int192[],bytes,int192,bytes32,uint40)` topic0.
pub const OCR2_NEW_TRANSMISSION_TOPIC: B256 =
    b256!("c797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a");

/// OCR1 `NewTransmission(uint32,int192,address,int192[],bytes,bytes32)` topic0.
pub const OCR1_NEW_TRANSMISSION_TOPIC: B256 =
    b256!("f6a97944f31ea060dfde0566e4167c1a1082551e64b60ecb14d599a9d023d451");

/// Pyth `PriceFeedUpdate(bytes32,uint64,int64,uint64)` topic0.
#[cfg(feature = "pyth")]
pub const PYTH_PRICE_FEED_UPDATE_TOPIC: B256 =
    b256!("d06a6b7f4918494b3719217d1802786c1f5112a6c1d88fe2cfec00b4584f6aec");

/// RedStone `ValueUpdate(uint256,bytes32,uint256)` topic0.
#[cfg(feature = "redstone")]
pub const REDSTONE_VALUE_UPDATE_TOPIC: B256 =
    b256!("f36866d965ee70c8632ff558f5cf8d41ee9ca1d0d0bc7700786e57be60747390");

mod ocr1_abi {
    alloy_sol_types::sol! {
        event NewTransmission(
            uint32 indexed aggregatorRoundId,
            int192 answer,
            address transmitter,
            int192[] observations,
            bytes observers,
            bytes32 rawReportContext
        );
    }
}

mod ocr2_abi {
    alloy_sol_types::sol! {
        event NewTransmission(
            uint32 indexed aggregatorRoundId,
            int192 answer,
            address transmitter,
            uint32 observationsTimestamp,
            int192[] observations,
            bytes observers,
            int192 juelsPerFeeCoin,
            bytes32 configDigest,
            uint40 epochAndRound
        );
    }
}

/// Decoded Chainlink OCR1 `NewTransmission` event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ocr1NewTransmission {
    /// Emitting aggregator address.
    pub aggregator: Address,
    /// Aggregator-local round id.
    pub aggregator_round_id: U256,
    /// OCR1 median answer.
    pub answer: I256,
    /// Address that transmitted the report.
    pub transmitter: Address,
    /// OCR1 raw report context.
    pub raw_report_context: B256,
    /// OCR1 config digest extracted from `rawReportContext`.
    pub config_digest: FixedBytes<16>,
    /// OCR1 epoch and round packed as uint40.
    pub epoch_and_round: u64,
    /// Log block number, when present.
    pub block_number: Option<u64>,
    /// Log index, when present.
    pub log_index: Option<u64>,
    /// Whether the provider marked the log as removed.
    pub removed: bool,
}

/// Decoded Chainlink `AnswerUpdated` event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnswerUpdated {
    /// Emitting aggregator address.
    pub aggregator: Address,
    /// Signed answer from the event.
    pub current: I256,
    /// Aggregator-local round id.
    pub round_id: U256,
    /// Event `updatedAt` timestamp.
    pub updated_at: u64,
    /// Log block number, when present.
    pub block_number: Option<u64>,
    /// Log index, when present.
    pub log_index: Option<u64>,
    /// Whether the provider marked the log as removed.
    pub removed: bool,
}

/// Decoded Chainlink OCR2 `NewTransmission` event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ocr2NewTransmission {
    /// Emitting aggregator address.
    pub aggregator: Address,
    /// Aggregator-local round id.
    pub aggregator_round_id: U256,
    /// OCR2 median answer.
    pub answer: I256,
    /// Address that transmitted the report.
    pub transmitter: Address,
    /// OCR2 observations timestamp, used as `startedAt`.
    pub observations_timestamp: u64,
    /// OCR2 config digest for the transmission.
    pub config_digest: B256,
    /// OCR2 epoch and round packed as uint40.
    pub epoch_and_round: u64,
    /// Log block number, when present.
    pub block_number: Option<u64>,
    /// Log index, when present.
    pub log_index: Option<u64>,
    /// Whether the provider marked the log as removed.
    pub removed: bool,
}

/// Decoded Chainlink `NewRound` event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewRound {
    /// Emitting aggregator address.
    pub aggregator: Address,
    /// Aggregator-local round id.
    pub round_id: U256,
    /// Address that started the round.
    pub started_by: Address,
    /// Event `startedAt` timestamp.
    pub started_at: u64,
    /// Log block number, when present.
    pub block_number: Option<u64>,
    /// Log index, when present.
    pub log_index: Option<u64>,
    /// Whether the provider marked the log as removed.
    pub removed: bool,
}

/// Decoded Pyth `PriceFeedUpdate` event.
#[cfg(feature = "pyth")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PythPriceFeedUpdate {
    /// Emitting Pyth contract address.
    pub pyth: Address,
    /// Pyth price feed id.
    pub price_id: B256,
    /// Pyth publish timestamp.
    pub publish_time: u64,
    /// Fixed-point price value.
    pub price: i64,
    /// Pyth confidence interval.
    pub conf: u64,
    /// Log block number, when present.
    pub block_number: Option<u64>,
    /// Log index, when present.
    pub log_index: Option<u64>,
    /// Whether the provider marked the log as removed.
    pub removed: bool,
}

/// Decoded RedStone `ValueUpdate` event.
#[cfg(feature = "redstone")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedstoneValueUpdate {
    /// Emitting RedStone adapter address.
    pub adapter: Address,
    /// RedStone data feed id.
    pub data_feed_id: B256,
    /// Unsigned RedStone value.
    pub value: U256,
    /// Event `updatedAt` timestamp.
    pub updated_at: u64,
    /// Log block number, when present.
    pub block_number: Option<u64>,
    /// Log index, when present.
    pub log_index: Option<u64>,
    /// Whether the provider marked the log as removed.
    pub removed: bool,
}

/// Decode a Chainlink `AnswerUpdated` log.
pub fn decode_answer_updated(log: &RpcLog) -> Result<AnswerUpdated, ChainlinkEventDecodeError> {
    require_topic(log, ANSWER_UPDATED_TOPIC)?;
    require_topic_count(log, 3)?;
    let data = data_word(log, "updated_at")?;
    Ok(AnswerUpdated {
        aggregator: log.address(),
        current: topic_i256(log.topics()[1]),
        round_id: topic_u256(log.topics()[2]),
        updated_at: u256_to_u64(data, "updated_at")?,
        block_number: log.block_number,
        log_index: log.log_index,
        removed: log.removed,
    })
}

/// Decode a RedStone `ValueUpdate` log.
#[cfg(feature = "redstone")]
pub fn decode_redstone_value_update(
    log: &RpcLog,
) -> Result<RedstoneValueUpdate, ChainlinkEventDecodeError> {
    require_topic(log, REDSTONE_VALUE_UPDATE_TOPIC)?;
    require_topic_count(log, 1)?;
    let data = log.inner.data.data.as_ref();
    if data.len() != 96 {
        return Err(ChainlinkEventDecodeError::WrongDataLength {
            expected: 96,
            actual: data.len(),
        });
    }

    let value = U256::from_be_slice(&data[0..32]);
    let data_feed_id = B256::from_slice(&data[32..64]);
    let updated_at = u256_to_u64(U256::from_be_slice(&data[64..96]), "updated_at")?;

    Ok(RedstoneValueUpdate {
        adapter: log.address(),
        data_feed_id,
        value,
        updated_at,
        block_number: log.block_number,
        log_index: log.log_index,
        removed: log.removed,
    })
}

/// Decode a Chainlink OCR1 `NewTransmission` log.
pub fn decode_ocr1_new_transmission(
    log: &RpcLog,
) -> Result<Ocr1NewTransmission, ChainlinkEventDecodeError> {
    require_topic(log, OCR1_NEW_TRANSMISSION_TOPIC)?;
    require_topic_count(log, 2)?;
    let decoded = ocr1_abi::NewTransmission::decode_log_validate(&log.inner).map_err(|error| {
        ChainlinkEventDecodeError::AbiDecode {
            event: "NewTransmission",
            message: error.to_string(),
        }
    })?;
    let data = decoded.data;
    Ok(Ocr1NewTransmission {
        aggregator: log.address(),
        aggregator_round_id: U256::from(data.aggregatorRoundId),
        answer: int192_to_i256(data.answer),
        transmitter: data.transmitter,
        raw_report_context: data.rawReportContext,
        config_digest: ocr1_config_digest(data.rawReportContext),
        epoch_and_round: u40_from_context(data.rawReportContext),
        block_number: log.block_number,
        log_index: log.log_index,
        removed: log.removed,
    })
}

/// Decode a Chainlink OCR2 `NewTransmission` log.
pub fn decode_ocr2_new_transmission(
    log: &RpcLog,
) -> Result<Ocr2NewTransmission, ChainlinkEventDecodeError> {
    require_topic(log, OCR2_NEW_TRANSMISSION_TOPIC)?;
    require_topic_count(log, 2)?;
    let decoded = ocr2_abi::NewTransmission::decode_log_validate(&log.inner).map_err(|error| {
        ChainlinkEventDecodeError::AbiDecode {
            event: "NewTransmission",
            message: error.to_string(),
        }
    })?;
    let data = decoded.data;
    Ok(Ocr2NewTransmission {
        aggregator: log.address(),
        aggregator_round_id: U256::from(data.aggregatorRoundId),
        answer: int192_to_i256(data.answer),
        transmitter: data.transmitter,
        observations_timestamp: u64::from(data.observationsTimestamp),
        config_digest: data.configDigest,
        epoch_and_round: data.epochAndRound.as_limbs()[0],
        block_number: log.block_number,
        log_index: log.log_index,
        removed: log.removed,
    })
}

/// Decode a Chainlink `NewRound` log.
pub fn decode_new_round(log: &RpcLog) -> Result<NewRound, ChainlinkEventDecodeError> {
    require_topic(log, NEW_ROUND_TOPIC)?;
    require_topic_count(log, 3)?;
    let data = data_word(log, "started_at")?;
    Ok(NewRound {
        aggregator: log.address(),
        round_id: topic_u256(log.topics()[1]),
        started_by: topic_address(log.topics()[2]),
        started_at: u256_to_u64(data, "started_at")?,
        block_number: log.block_number,
        log_index: log.log_index,
        removed: log.removed,
    })
}

/// Decode a Pyth `PriceFeedUpdate` log.
#[cfg(feature = "pyth")]
pub fn decode_pyth_price_feed_update(
    log: &RpcLog,
) -> Result<PythPriceFeedUpdate, ChainlinkEventDecodeError> {
    require_topic(log, PYTH_PRICE_FEED_UPDATE_TOPIC)?;
    require_topic_count(log, 2)?;
    let data = log.inner.data.data.as_ref();
    if data.len() != 96 {
        return Err(ChainlinkEventDecodeError::WrongDataLength {
            expected: 96,
            actual: data.len(),
        });
    }

    let publish_time = u256_to_u64(U256::from_be_slice(&data[0..32]), "publish_time")?;
    let price = int64_word(&data[32..64])?;
    let conf = u256_to_u64(U256::from_be_slice(&data[64..96]), "conf")?;

    Ok(PythPriceFeedUpdate {
        pyth: log.address(),
        price_id: log.topics()[1],
        publish_time,
        price,
        conf,
        block_number: log.block_number,
        log_index: log.log_index,
        removed: log.removed,
    })
}

fn require_topic(log: &RpcLog, expected: B256) -> Result<(), ChainlinkEventDecodeError> {
    let actual = log.topics().first().copied();
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(ChainlinkEventDecodeError::WrongTopic { expected, actual })
    }
}

fn require_topic_count(log: &RpcLog, expected: usize) -> Result<(), ChainlinkEventDecodeError> {
    let actual = log.topics().len();
    if actual == expected {
        Ok(())
    } else {
        Err(ChainlinkEventDecodeError::WrongTopicCount { expected, actual })
    }
}

fn data_word(log: &RpcLog, field: &'static str) -> Result<U256, ChainlinkEventDecodeError> {
    let data = log.inner.data.data.as_ref();
    if data.len() != 32 {
        return Err(ChainlinkEventDecodeError::WrongDataLength {
            expected: 32,
            actual: data.len(),
        });
    }

    let _ = field;
    Ok(U256::from_be_slice(data))
}

fn topic_u256(topic: B256) -> U256 {
    U256::from_be_slice(topic.as_slice())
}

fn topic_i256(topic: B256) -> I256 {
    I256::from_raw(topic_u256(topic))
}

fn topic_address(topic: B256) -> Address {
    Address::from_slice(&topic.as_slice()[12..])
}

fn int192_to_i256(value: alloy_primitives::Signed<192, 3>) -> I256 {
    let raw = value.into_raw();
    let mut limbs = [0_u64; 4];
    limbs[..3].copy_from_slice(raw.as_limbs());
    if raw.as_limbs()[2] & (1_u64 << 63) != 0 {
        limbs[3] = u64::MAX;
    }
    I256::from_raw(U256::from_limbs(limbs))
}

fn ocr1_config_digest(raw_report_context: B256) -> FixedBytes<16> {
    let mut digest = [0_u8; 16];
    digest.copy_from_slice(&raw_report_context.as_slice()[11..27]);
    FixedBytes::from(digest)
}

fn u40_from_context(raw_report_context: B256) -> u64 {
    let bytes = raw_report_context.as_slice();
    u64::from_be_bytes([
        0, 0, 0, bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
    ])
}

#[cfg(feature = "pyth")]
fn int64_word(word: &[u8]) -> Result<i64, ChainlinkEventDecodeError> {
    debug_assert_eq!(word.len(), 32);
    let negative = word[24] & 0x80 != 0;
    let expected_prefix = if negative { 0xff } else { 0x00 };
    if word[..24].iter().any(|byte| *byte != expected_prefix) {
        return Err(ChainlinkEventDecodeError::AbiDecode {
            event: "PriceFeedUpdate",
            message: "price does not fit int64".to_string(),
        });
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    Ok(i64::from_be_bytes(bytes))
}

fn u256_to_u64(value: U256, field: &'static str) -> Result<u64, ChainlinkEventDecodeError> {
    u64::try_from(value).map_err(|_| ChainlinkEventDecodeError::Uint64Overflow { field, value })
}
