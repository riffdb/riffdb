// req: REP-005
use super::*;
use prost::Message;
use riffdb_proto::{
    durable::{READABLE_RECORD_SCHEMAS, readable_record_registry, readable_record_schema},
    envelope::{RecordRegistry, STORAGE_FORMAT_VERSION_V1, payload_crc32c},
    storage::v1 as wire,
};
use riffdb_storage_api::proto_codec::{
    decode_promotion_administration_v1 as decode, encode_promotion_administration_v1 as encode,
};
const RECORD: &str = "riffdb.storage.v1.StoredPromotionAdministrationV1";

fn record() -> Administration {
    Administration::new(
        pending(),
        Timestamp::new(1235, 0).unwrap(),
        ServiceIngressKindV1::Grpc,
    )
    .unwrap()
}
fn raw(payload: Vec<u8>) -> Vec<u8> {
    wire::StoredEnvelope {
        storage_format_version: STORAGE_FORMAT_VERSION_V1,
        record_type: RECORD.to_owned(),
        schema_hash: readable_record_schema(RECORD)
            .unwrap()
            .schema_hash()
            .as_bytes()
            .to_vec(),
        payload_crc32c: payload_crc32c(&payload),
        payload,
    }
    .encode_to_vec()
}
fn wire_record() -> wire::StoredPromotionAdministrationV1 {
    let encoded = encode(&record()).unwrap();
    let envelope = readable_record_registry()
        .decode(encoded.as_bytes())
        .unwrap();
    wire::StoredPromotionAdministrationV1::decode(envelope.payload()).unwrap()
}

#[test]
fn promotion_administration_vectors_preserve_exact_attempts_and_refuse_old_readers() {
    let old_schemas = READABLE_RECORD_SCHEMAS
        .iter()
        .copied()
        .filter(|s| s.record_type() != RECORD)
        .collect::<Vec<_>>();
    let old = RecordRegistry::new(&old_schemas).unwrap();
    let schema = readable_record_schema(RECORD).unwrap();
    assert_eq!(
        (
            schema.compact_tag(),
            schema.schema_revision(),
            schema.max_payload_bytes()
        ),
        (78, 1, 8192)
    );
    let mut uncertain = pending();
    uncertain
        .advance(Step::Uncertain(
            riffdb_storage_api::ReplicationPromotionFailureV1::StorageUnavailable,
        ))
        .unwrap();
    uncertain
        .advance(Step::Uncertain(
            riffdb_storage_api::ReplicationPromotionFailureV1::ValidationFailed,
        ))
        .unwrap();
    let uncertain = Administration::new(
        uncertain,
        Timestamp::new(1236, 0).unwrap(),
        ServiceIngressKindV1::Grpc,
    )
    .unwrap();
    let mut vectors = String::new();
    for (name, original) in [("cutover", record()), ("uncertain-attempt", uncertain)] {
        let encoded = encode(&original).unwrap();
        assert!(old.decode(encoded.as_bytes()).is_err());
        let decoded = decode(encoded.as_bytes()).unwrap();
        assert_eq!(decoded.value(), &original);
        assert_eq!(
            decoded.encoded_content_charge(),
            encoded.encoded_content_charge()
        );
        assert_eq!(
            encode(decoded.value()).unwrap().as_bytes(),
            encoded.as_bytes()
        );
        vectors.push_str(name);
        vectors.push(' ');
        for byte in encoded.as_bytes() {
            use std::fmt::Write;
            write!(vectors, "{byte:02x}").unwrap();
        }
        vectors.push('\n');
        for offset in 0..encoded.as_bytes().len() {
            assert!(decode(&encoded.as_bytes()[..offset]).is_err());
            let mut changed = encoded.as_bytes().to_vec();
            changed[offset] ^= 0x80;
            assert!(decode(&changed).is_err());
        }
    }
    if let Some(path) = std::env::var_os("RIFFDB_PROMOTION_ADMINISTRATION_VECTOR_OUTPUT") {
        std::fs::write(path, vectors).unwrap();
    } else {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/replication/promotion-administration-v1.hex");
        assert_eq!(std::fs::read_to_string(path).unwrap(), vectors);
    }
}

#[test]
fn checksum_correct_promotion_corruption_cannot_reselect_or_complete_a_phase() {
    for case in 0..31 {
        let mut value = wire_record();
        match case {
            0 => value.administration_sequence = 0,
            1 => value.administration_sequence += 1,
            2 => value.timestamp = None,
            3 => value.timestamp.as_mut().unwrap().nanos = 1_000_000_000,
            4 => value.promotion_operation_id = vec![0; 16],
            5 => value.request_id = vec![0; 16],
            6 => value.principal = None,
            7 => value.principal.as_mut().unwrap().capability_revision = 0,
            8 => value.principal.as_mut().unwrap().principal_id = "x".repeat(257),
            9 => value.approval_id = Some("x".repeat(257)),
            10 => value.attempted_at = None,
            11 => value.fence = None,
            12 => value.fence.as_mut().unwrap().registration_generation = 0,
            13 => value.applied = None,
            14 => {
                value.applied.as_mut().unwrap().history_hash.pop().unwrap();
            }
            15 => value.applied.as_mut().unwrap().application_sequence = 12,
            16 => value.source_history = None,
            17 => value.source_history.as_mut().unwrap().catalog_digest[0] ^= 1,
            18 => value.source_history.as_mut().unwrap().history_incarnation += 1,
            19 => value.source_history.as_mut().unwrap().leadership_epoch += 1,
            20 => value.published_incarnation += 1,
            21 => value.published_epoch += 1,
            22 => value.application_rpo += 1,
            23 => value.attempt_steps.clear(),
            24 => {
                value.attempt_steps.remove(2);
            }
            25 => value.attempt_steps.push(6), // No committed/validated phase in this record.
            26 => value.attempt_steps.push(255),
            27 => value.attempt_steps = vec![1; 17],
            28 => value.ingress = 2, // MCP
            29 => value.ingress = 999,
            30 => {
                value
                    .source_history
                    .as_mut()
                    .unwrap()
                    .tail
                    .as_mut()
                    .unwrap()
                    .application_sequence = 12
            }
            _ => unreachable!(),
        }
        assert!(decode(&raw(value.encode_to_vec())).is_err(), "case {case}");
    }
    let mut payload = wire_record().encode_to_vec();
    payload.extend_from_slice(&[8, 6]); // Duplicate scalar, even with the same value.
    assert!(decode(&raw(payload)).is_err());
    let mut payload = wire_record().encode_to_vec();
    payload.extend_from_slice(&[0x80, 1, 1]); // Unknown field 16.
    assert!(decode(&raw(payload)).is_err());
    assert!(decode(&raw(vec![0; 8193])).is_err());
}

#[test]
fn promotion_maximum_attempt_and_nested_identities_fit_the_original_bound() {
    let mut value = wire_record();
    let principal = value.principal.as_mut().unwrap();
    principal.principal_id = "x".repeat(256);
    principal.capability_revision = u64::MAX;
    value.approval_id = Some("y".repeat(256));
    let fence = value.fence.as_mut().unwrap();
    fence.principal.as_mut().unwrap().principal_id = "z".repeat(256);
    fence.approval_id = Some("w".repeat(256));
    while value.attempt_steps.len() < riffdb_storage_api::MAX_REPLICATION_PROMOTION_STEPS_V1 {
        let next = if value.attempt_steps.last() == Some(&129) {
            130
        } else {
            129
        };
        value.attempt_steps.push(next);
    }
    let bytes = raw(value.encode_to_vec());
    assert!(value.encoded_len() <= 8192);
    let decoded = decode(&bytes).unwrap();
    assert_eq!(decoded.value().attempt().steps().len(), 16);
    assert_eq!(
        decode(encode(decoded.value()).unwrap().as_bytes())
            .unwrap()
            .value(),
        decoded.value()
    );
}
