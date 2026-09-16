//! Follower-target audit compatibility and malformed-input evidence.
// req: REP-005, REP-006, REC-001, STO-012

use super::{super::*, sample};
use crate::StoredServiceAuditRecordV1;
use prost::Message;
use riffdb_proto::{
    durable::{READABLE_RECORD_SCHEMAS, readable_record_registry, readable_record_schema},
    envelope::{RecordRegistry, STORAGE_FORMAT_VERSION_V1, payload_crc32c},
    storage::v1 as wire,
};
use riffdb_types::{
    EventConsumerIdentityHash, LeadershipEpochV1, ReactiveModuleHash, ReactiveOperationName,
    ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1, ServiceAuditPhaseV1,
    ServiceAuditTargetV1, ServiceAuditTargetsV1,
};

const RECORD: &str = "riffdb.storage.v1.ServiceAuditRecordV3";

fn record() -> StoredServiceAuditRecordV1 {
    let base = sample::service_audit_record();
    let mut targets = base.targets().as_slice().to_vec();
    targets.push(ServiceAuditTargetV1::EventConsumer {
        lineage: sample::lineage(),
        module_hash: ReactiveModuleHash::from_bytes([0x55; 32]),
        operation_name: ReactiveOperationName::new("Observe").unwrap(),
        consumer_identity_hash: EventConsumerIdentityHash::from_bytes([0x66; 32]),
    });
    targets.push(ServiceAuditTargetV1::ReplicationFollower(
        ReplicationFollowerAuditTargetV1::new(
            sample::database_id(),
            7,
            LeadershipEpochV1::new(9).unwrap(),
            ReplicationSourceHoldIdV1::new([0x81; 16]).unwrap(),
        )
        .unwrap(),
    ));
    StoredServiceAuditRecordV1::from_stored_parts(
        base.administration_sequence(),
        base.request_id(),
        base.timestamp(),
        base.operation(),
        base.phase(),
        base.principal().cloned(),
        base.ingress(),
        ServiceAuditTargetsV1::new(targets).unwrap(),
        base.approval_id().cloned(),
        base.link(),
    )
    .unwrap()
}

fn wire_record() -> wire::ServiceAuditRecordV3 {
    let encoded = encode_service_audit_record_v3(&record()).unwrap();
    let envelope = readable_record_registry()
        .decode(encoded.as_bytes())
        .unwrap();
    wire::ServiceAuditRecordV3::decode(envelope.payload()).unwrap()
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

#[test]
fn follower_audit_v3_roundtrips_all_target_kinds_and_mixed_generations() {
    let value = record();
    assert_eq!(value.targets().len(), 11);
    let v3 = encode_service_audit_record_v3(&value).unwrap();
    assert_eq!(
        decode_service_audit_record(v3.as_bytes()).unwrap().value(),
        &value
    );
    assert!(decode_service_audit_record_v1(v3.as_bytes()).is_err());
    assert!(decode_service_audit_record_v2(v3.as_bytes()).is_err());
    assert!(encode_service_audit_record_v2(&value).is_err());
    let old = sample::service_audit_record();
    for bytes in [
        encode_service_audit_record_legacy_v1(&old).unwrap(),
        encode_service_audit_record_v2(&old).unwrap(),
    ] {
        assert_eq!(
            decode_service_audit_record(bytes.as_bytes())
                .unwrap()
                .value(),
            &old
        );
    }
    assert!(
        encode_service_audit_record_v3(&old).is_err(),
        "old audits stay V2"
    );
    for phase in [
        ServiceAuditPhaseV1::Started,
        ServiceAuditPhaseV1::Succeeded,
        ServiceAuditPhaseV1::Denied,
        ServiceAuditPhaseV1::Cancelled,
        ServiceAuditPhaseV1::Failed,
        ServiceAuditPhaseV1::OutcomeUncertain,
    ] {
        let mut wire = wire_record();
        wire.phase = i32::from(phase.tag());
        let decoded = decode_service_audit_record_v3(&raw(wire.encode_to_vec())).unwrap();
        assert_eq!(decoded.value().targets(), value.targets());
        assert_eq!(decoded.value().phase(), phase);
    }
}

#[test]
fn follower_audit_v3_refuses_zero_foreign_shape_order_duplicates_and_overflow() {
    use wire::service_audit_target_v3::Target;
    for case in 0..12 {
        let mut message = wire_record();
        let Target::ReplicationFollower(target) =
            message.targets.last_mut().unwrap().target.as_mut().unwrap()
        else {
            panic!("follower target");
        };
        match case {
            0 => target.database_id.clear(),
            1 => target.database_id = vec![0; 16],
            2 => target.database_id.push(0),
            3 => target.history_incarnation = 0,
            4 => target.leadership_epoch = 0,
            5 => target.hold_id.clear(),
            6 => target.hold_id = vec![0; 16],
            7 => target.hold_id.push(0),
            8 => message.targets.reverse(),
            9 => message
                .targets
                .push(message.targets.last().unwrap().clone()),
            10 => message.targets = vec![message.targets.last().unwrap().clone(); 17],
            11 => message.targets.last_mut().unwrap().target = None,
            _ => unreachable!(),
        }
        assert!(
            decode_service_audit_record_v3(&raw(message.encode_to_vec())).is_err(),
            "case {case}"
        );
    }
    // A checksum-correct unknown target field must not disappear during prost decoding.
    let mut unknown_target = vec![0x62, 0]; // field 12, length-delimited empty message
    let mut field = vec![0x42, 2];
    field.append(&mut unknown_target);
    let mut payload = wire_record().encode_to_vec();
    // Insert alongside existing field-8 targets, before the required result link.
    let link = wire_record().link.unwrap().encode_to_vec();
    let link_bytes = [vec![0x52, u8::try_from(link.len()).unwrap()], link].concat();
    let at = payload.len() - link_bytes.len();
    assert_eq!(&payload[at..], link_bytes);
    payload.splice(at..at, field);
    assert!(decode_service_audit_record_v3(&raw(payload)).is_err());
}

#[test]
fn follower_audit_v3_has_its_own_identity_and_old_registry_refuses_it() {
    let schema = readable_record_schema(RECORD).unwrap();
    assert_eq!((schema.compact_tag(), schema.schema_revision()), (22, 3));
    let old = READABLE_RECORD_SCHEMAS
        .iter()
        .copied()
        .filter(|schema| schema.record_type() != RECORD)
        .collect::<Vec<_>>();
    let registry = RecordRegistry::new(&old).unwrap();
    let bytes = encode_service_audit_record_v3(&record()).unwrap();
    assert!(registry.decode(bytes.as_bytes()).is_err());
    let prior = encode_service_audit_record_v2(&sample::service_audit_record()).unwrap();
    assert!(registry.decode(prior.as_bytes()).is_ok());
    for offset in 0..bytes.as_bytes().len() {
        let mut corrupted = bytes.as_bytes().to_vec();
        corrupted[offset] ^= 0x80;
        assert!(
            decode_service_audit_record_v3(&corrupted).is_err(),
            "offset {offset}"
        );
    }
    let mut trailing = bytes.as_bytes().to_vec();
    trailing.push(0);
    assert!(decode_service_audit_record_v3(&trailing).is_err());
}

#[test]
fn follower_audit_v3_canonical_fixture_preserves_the_complete_target_list() {
    let value = encode_service_audit_record_v3(&record()).unwrap();
    let hex = value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        + "\n";
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/proto/durable-service-audit-v3-all-targets.hex");
    if std::env::var_os("RIFFDB_UPDATE_AUDIT_V3_FIXTURES").is_some() {
        std::fs::write(&path, &hex).unwrap();
    }
    assert_eq!(std::fs::read_to_string(path).unwrap(), hex);
}
