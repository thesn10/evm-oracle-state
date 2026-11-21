use alloy_primitives::{
    Address, B256, Bytes, FixedBytes, I256, Log as PrimitiveLog, U256, aliases::I192, aliases::U40,
    keccak256,
};
use evm_oracle_state::alloy_rpc_types_eth::Log as RpcLog;
use alloy_sol_types::SolEvent;
use evm_oracle_state::{
    ANSWER_UPDATED_TOPIC, AnswerUpdated, ChainlinkEventDecodeError, NEW_ROUND_TOPIC, NewRound,
    OCR1_NEW_TRANSMISSION_TOPIC, OCR2_NEW_TRANSMISSION_TOPIC, Ocr1NewTransmission,
    Ocr2NewTransmission, decode_answer_updated, decode_new_round, decode_ocr1_new_transmission,
    decode_ocr2_new_transmission,
};

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

mod ocr1_fixture {
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

fn rpc_log(address: Address, topics: Vec<B256>, data: Vec<u8>) -> RpcLog {
    RpcLog {
        inner: PrimitiveLog::new_unchecked(address, topics, Bytes::from(data)),
        block_hash: Some(B256::repeat_byte(0x01)),
        block_number: Some(100),
        block_timestamp: Some(1_700_000_000),
        transaction_hash: Some(B256::repeat_byte(0x02)),
        transaction_index: Some(0),
        log_index: Some(3),
        removed: false,
    }
}

fn word(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn uint_topic(value: U256) -> B256 {
    B256::from(value.to_be_bytes::<32>())
}

fn int_topic(value: I256) -> B256 {
    B256::from(value.into_raw().to_be_bytes::<32>())
}

fn address_topic(address: Address) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    B256::from(bytes)
}

fn raw_report_context(config_digest: [u8; 16], epoch_and_round: u64) -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[11..27].copy_from_slice(&config_digest);
    bytes[27..].copy_from_slice(&epoch_and_round.to_be_bytes()[3..]);
    B256::from(bytes)
}

#[test]
fn exposes_chainlink_event_topics() {
    assert_eq!(
        ANSWER_UPDATED_TOPIC,
        keccak256("AnswerUpdated(int256,uint256,uint256)")
    );
    assert_eq!(
        NEW_ROUND_TOPIC,
        keccak256("NewRound(uint256,address,uint256)")
    );
    assert_eq!(
        OCR2_NEW_TRANSMISSION_TOPIC,
        keccak256(
            "NewTransmission(uint32,int192,address,uint32,int192[],bytes,int192,bytes32,uint40)"
        )
    );
    assert_eq!(
        OCR1_NEW_TRANSMISSION_TOPIC,
        keccak256("NewTransmission(uint32,int192,address,int192[],bytes,bytes32)")
    );
}

#[test]
fn decodes_answer_updated_with_signed_current_answer() {
    let aggregator = Address::repeat_byte(0x11);
    let current = I256::unchecked_from(-42);
    let round_id = U256::from(9_001_u64);
    let updated_at = U256::from(1_700_000_123_u64);
    let log = rpc_log(
        aggregator,
        vec![
            ANSWER_UPDATED_TOPIC,
            int_topic(current),
            uint_topic(round_id),
        ],
        word(updated_at),
    );

    let decoded = decode_answer_updated(&log).expect("valid AnswerUpdated log should decode");

    assert_eq!(
        decoded,
        AnswerUpdated {
            aggregator,
            current,
            round_id,
            updated_at: 1_700_000_123,
            block_number: Some(100),
            log_index: Some(3),
            removed: false,
        }
    );
}

#[test]
fn decodes_new_round_for_observability() {
    let aggregator = Address::repeat_byte(0x22);
    let started_by = Address::repeat_byte(0x33);
    let round_id = U256::from(77_u64);
    let started_at = U256::from(1_700_000_456_u64);
    let log = rpc_log(
        aggregator,
        vec![
            NEW_ROUND_TOPIC,
            uint_topic(round_id),
            address_topic(started_by),
        ],
        word(started_at),
    );

    let decoded = decode_new_round(&log).expect("valid NewRound log should decode");

    assert_eq!(
        decoded,
        NewRound {
            aggregator,
            round_id,
            started_by,
            started_at: 1_700_000_456,
            block_number: Some(100),
            log_index: Some(3),
            removed: false,
        }
    );
}

#[test]
fn decodes_ocr1_new_transmission_with_epoch_and_round_source() {
    let aggregator = Address::repeat_byte(0x55);
    let transmitter = Address::repeat_byte(0x66);
    let raw_context = raw_report_context([0x77; 16], 0x1a_89_04);
    let config_digest = FixedBytes::<16>::from([0x77; 16]);
    let event = ocr1_fixture::NewTransmission {
        aggregatorRoundId: 42,
        answer: I192::try_from(-123_i64).expect("answer fits int192"),
        transmitter,
        observations: vec![I192::try_from(-120_i64).expect("observation fits int192")],
        observers: Bytes::from(vec![0x01]),
        rawReportContext: raw_context,
    };
    let log = rpc_log(
        aggregator,
        vec![OCR1_NEW_TRANSMISSION_TOPIC, uint_topic(U256::from(42_u64))],
        event.encode_data().to_vec(),
    );

    let decoded =
        decode_ocr1_new_transmission(&log).expect("valid OCR1 NewTransmission log should decode");

    assert_eq!(
        decoded,
        Ocr1NewTransmission {
            aggregator,
            aggregator_round_id: U256::from(42_u64),
            answer: I256::unchecked_from(-123_i64),
            transmitter,
            raw_report_context: raw_context,
            config_digest,
            epoch_and_round: 0x1a_89_04,
            block_number: Some(100),
            log_index: Some(3),
            removed: false,
        }
    );
}

#[test]
fn decodes_ocr2_new_transmission_with_started_at_source() {
    let aggregator = Address::repeat_byte(0x55);
    let transmitter = Address::repeat_byte(0x66);
    let config_digest = B256::repeat_byte(0x77);
    let event = NewTransmission {
        aggregatorRoundId: 42,
        answer: I192::try_from(-123_i64).expect("answer fits int192"),
        transmitter,
        observationsTimestamp: 1_700_000_321,
        observations: vec![I192::try_from(-120_i64).expect("observation fits int192")],
        observers: Bytes::from(vec![0x01]),
        juelsPerFeeCoin: I192::ZERO,
        configDigest: config_digest,
        epochAndRound: U40::from(0x1a_89_04_u64),
    };
    let log = rpc_log(
        aggregator,
        vec![OCR2_NEW_TRANSMISSION_TOPIC, uint_topic(U256::from(42_u64))],
        event.encode_data().to_vec(),
    );

    let decoded =
        decode_ocr2_new_transmission(&log).expect("valid OCR2 NewTransmission log should decode");

    assert_eq!(
        decoded,
        Ocr2NewTransmission {
            aggregator,
            aggregator_round_id: U256::from(42_u64),
            answer: I256::unchecked_from(-123_i64),
            transmitter,
            observations_timestamp: 1_700_000_321,
            config_digest,
            epoch_and_round: 0x1a_89_04,
            block_number: Some(100),
            log_index: Some(3),
            removed: false,
        }
    );
}

#[test]
fn malformed_answer_updated_returns_typed_decode_error() {
    let log = rpc_log(
        Address::repeat_byte(0x44),
        vec![ANSWER_UPDATED_TOPIC, int_topic(I256::unchecked_from(1))],
        Vec::new(),
    );

    let err = decode_answer_updated(&log).expect_err("missing topic/data should fail");

    assert!(matches!(
        err,
        ChainlinkEventDecodeError::WrongTopicCount { .. }
            | ChainlinkEventDecodeError::WrongDataLength { .. }
    ));
}
