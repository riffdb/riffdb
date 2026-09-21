// req: REP-005, STO-012, REC-001
use super::*;
use prost::Message;
use riffdb_proto::envelope::{RecordRegistry, STORAGE_FORMAT_VERSION_V1, payload_crc32c};
use riffdb_proto::{
    durable::{READABLE_RECORD_SCHEMAS, readable_record_registry, readable_record_schema},
    storage::v1 as wire,
};
use riffdb_storage_api::proto_codec::{
    decode_primary_fence_administration_v1 as decode_fence,
    decode_replication_primary_admission_v1 as decode_admission,
    encode_primary_fence_administration_v1 as encode_fence,
    encode_replication_primary_admission_v1 as encode_admission,
};
const ADMISSION: &str = "riffdb.storage.v1.ReplicationPrimaryAdmissionV1";
const FENCE: &str = "riffdb.storage.v1.StoredPrimaryFenceAdministrationV1";

fn raw(name: &str, payload: Vec<u8>) -> Vec<u8> {
    wire::StoredEnvelope {
        storage_format_version: STORAGE_FORMAT_VERSION_V1,
        record_type: name.to_owned(),
        schema_hash: readable_record_schema(name)
            .unwrap()
            .schema_hash()
            .as_bytes()
            .to_vec(),
        payload_crc32c: payload_crc32c(&payload),
        payload,
    }
    .encode_to_vec()
}
fn wire_fence() -> wire::StoredPrimaryFenceAdministrationV1 {
    let bytes = encode_fence(&fence(point(9, 5, 2), 4, 3, 7).unwrap()).unwrap();
    let envelope = readable_record_registry().decode(bytes.as_bytes()).unwrap();
    wire::StoredPrimaryFenceAdministrationV1::decode(envelope.payload()).unwrap()
}

#[test]
fn primary_fence_canonical_vectors_preserve_roles_and_refuse_old_readers() {
    let schemas = READABLE_RECORD_SCHEMAS
        .iter()
        .copied()
        .filter(|s| !matches!(s.record_type(), ADMISSION | FENCE))
        .collect::<Vec<_>>();
    let old = RecordRegistry::new(&schemas).unwrap();
    let receipt = fence(point(9, 5, 2), 4, 3, 7).unwrap();
    let fenced = Admission::fenced(receipt.clone());
    let active = Admission::active(lineage(2, 3)).unwrap();
    let mut vectors = String::new();
    for (name, encoded) in [
        ("active", encode_admission(&active).unwrap()),
        ("fenced", encode_admission(&fenced).unwrap()),
        ("receipt", encode_fence(&receipt).unwrap()),
    ] {
        assert!(old.decode(encoded.as_bytes()).is_err());
        if name == "receipt" {
            assert_eq!(decode_fence(encoded.as_bytes()).unwrap().value(), &receipt);
            assert!(decode_admission(encoded.as_bytes()).is_err());
        } else {
            let decoded = decode_admission(encoded.as_bytes()).unwrap();
            assert_eq!(
                decoded.value(),
                if name == "active" { &active } else { &fenced }
            );
            assert_eq!(
                decoded.encoded_content_charge(),
                encoded.encoded_content_charge()
            );
            assert!(decode_fence(encoded.as_bytes()).is_err());
        }
        vectors.push_str(name);
        vectors.push(' ');
        for byte in encoded.as_bytes() {
            use std::fmt::Write;
            write!(vectors, "{byte:02x}").unwrap();
        }
        vectors.push('\n');
        for offset in 0..encoded.as_bytes().len() {
            let mut corrupt = encoded.as_bytes().to_vec();
            corrupt[offset] ^= 0x80;
            for bytes in [&encoded.as_bytes()[..offset], corrupt.as_slice()] {
                assert!(decode_admission(bytes).is_err());
                assert!(decode_fence(bytes).is_err());
            }
        }
        let mut trailing = encoded.as_bytes().to_vec();
        trailing.push(0);
        assert!(decode_admission(&trailing).is_err());
        assert!(decode_fence(&trailing).is_err());
    }
    if let Some(path) = std::env::var_os("RIFFDB_PRIMARY_FENCE_VECTOR_OUTPUT") {
        std::fs::write(path, vectors).unwrap();
    } else {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/replication/primary-fence-v1.hex");
        assert_eq!(std::fs::read_to_string(path).unwrap(), vectors);
    }
}

#[test]
fn checksum_correct_malformed_fence_receipts_refuse_both_roles() {
    use wire::replication_primary_admission_v1::State;
    for case in 0..23 {
        let mut value = wire_fence();
        match case {
            0 => value.administration_sequence = 0,
            1 => value.administration_sequence += 1,
            2 => value.timestamp = None,
            3 => value.timestamp.as_mut().unwrap().nanos = 1_000_000_000,
            4 => value.operation_id = vec![0; 16],
            5 => value.request_id = vec![0; 16],
            6 => value.principal = None,
            7 => value.principal.as_mut().unwrap().capability_revision = 0,
            8 => value.principal.as_mut().unwrap().capability_id = vec![0; 16],
            9 => value.principal.as_mut().unwrap().principal_id = "x".repeat(257),
            10 => value.approval_id = Some("x".repeat(257)),
            11 => value.target = None,
            12 => value.target.as_mut().unwrap().database_id = vec![0; 16],
            13 => value.target.as_mut().unwrap().history_incarnation = 0,
            14 => value.target.as_mut().unwrap().leadership_epoch = 0,
            15 => value.target.as_mut().unwrap().hold_id = vec![0; 16],
            16 => value.registration_generation = 0,
            17 => value.registration_generation = 10,
            18 => value.observed = None,
            19 => value.observed.as_mut().unwrap().history_hash = vec![0; 31],
            20 => value.principal.as_mut().unwrap().actor_kind = 999,
            21 => value.approval_id = Some(String::new()),
            22 => {
                value.observed.as_mut().unwrap().administration_sequence = 0;
                value.administration_sequence = 1;
            }
            _ => unreachable!(),
        }
        assert!(
            decode_fence(&raw(FENCE, value.encode_to_vec())).is_err(),
            "case {case}"
        );
        let metadata = wire::ReplicationPrimaryAdmissionV1 {
            state: Some(State::Fenced(Box::new(value))),
        };
        assert!(
            decode_admission(&raw(ADMISSION, metadata.encode_to_vec())).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn admission_refuses_absence_ambiguous_states_unknown_fields_and_bad_active_lineage() {
    use wire::replication_primary_admission_v1::{Active, State};
    assert!(decode_admission(&[]).is_err());
    assert!(decode_admission(&raw(ADMISSION, vec![])).is_err());
    for case in 0..4 {
        let source = lineage(2, 3);
        let mut active = Active {
            database_id: source.database_id().as_bytes().to_vec(),
            history_incarnation: 2,
            leadership_epoch: 3,
        };
        match case {
            0 => active.database_id = vec![0; 16],
            1 => active.history_incarnation = 0,
            2 => active.leadership_epoch = 0,
            3 => active.database_id.push(0),
            _ => unreachable!(),
        }
        let value = wire::ReplicationPrimaryAdmissionV1 {
            state: Some(State::Active(active)),
        };
        assert!(decode_admission(&raw(ADMISSION, value.encode_to_vec())).is_err());
    }
    let encoded = encode_admission(&Admission::active(lineage(2, 3)).unwrap()).unwrap();
    let envelope = readable_record_registry()
        .decode(encoded.as_bytes())
        .unwrap();
    let active = envelope.payload();
    let fenced = wire::ReplicationPrimaryAdmissionV1 {
        state: Some(State::Fenced(Box::new(wire_fence()))),
    }
    .encode_to_vec();
    for tail in [active, fenced.as_slice(), &[0x18, 0x01]] {
        let mut payload = active.to_vec();
        payload.extend_from_slice(tail);
        assert!(decode_admission(&raw(ADMISSION, payload)).is_err());
    }
    // Duplicated scalar and oversized root also have correct outer checksums.
    let mut payload = wire_fence().encode_to_vec();
    payload.extend_from_slice(&[0x08, 3]);
    assert!(decode_fence(&raw(FENCE, payload)).is_err());
    assert!(decode_admission(&raw(ADMISSION, vec![0; 1025])).is_err());
}

#[test]
fn full_width_fence_values_fit_the_bound_and_origin_substitution_breaks_linkage() {
    use wire::replication_primary_admission_v1::State;
    let mut value = wire_fence();
    value.principal.as_mut().unwrap().principal_id = "x".repeat(256);
    value.principal.as_mut().unwrap().capability_revision = u64::MAX;
    value.approval_id = Some("y".repeat(256));
    value.administration_sequence = u64::MAX;
    value.registration_generation = u64::MAX - 2;
    value.target.as_mut().unwrap().history_incarnation = u64::MAX;
    value.target.as_mut().unwrap().leadership_epoch = u64::MAX;
    let observed = value.observed.as_mut().unwrap();
    observed.transaction_sequence = u64::MAX - 1;
    observed.administration_sequence = u64::MAX - 1;
    observed.application_sequence = u64::MAX;
    let receipt = decode_fence(&raw(FENCE, value.encode_to_vec()))
        .unwrap()
        .into_parts()
        .0;
    let metadata = Admission::fenced(receipt.clone());
    assert_eq!(
        decode_fence(encode_fence(&receipt).unwrap().as_bytes())
            .unwrap()
            .value(),
        &receipt
    );
    assert_eq!(
        decode_admission(encode_admission(&metadata).unwrap().as_bytes())
            .unwrap()
            .value(),
        &metadata
    );
    assert!(value.encoded_len() <= 1000);
    let wire_metadata = wire::ReplicationPrimaryAdmissionV1 {
        state: Some(State::Fenced(Box::new(value.clone()))),
    };
    assert!(wire_metadata.encoded_len() <= 1024);
    value.principal.as_mut().unwrap().capability_revision -= 1;
    let substituted = decode_fence(&raw(FENCE, value.encode_to_vec()))
        .unwrap()
        .into_parts()
        .0;
    assert!(
        metadata
            .validate_source_evidence(
                receipt.lineage(),
                receipt.final_application_head(),
                Some(&substituted)
            )
            .is_err()
    );
}
