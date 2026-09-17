//! Canonical audit bytes for physical receipt and checkpoint fixtures.

use riffdb_types::AdministrationSequence;

pub(crate) fn canonical_denied_service_audit(sequence: AdministrationSequence) -> Vec<u8> {
    use riffdb_storage_api::{AuditPrincipalV1, StoredServiceAuditRecordV1};
    use riffdb_types::{
        ActorId, ActorKind, CapabilityId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
        ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, Timestamp,
    };
    let record = StoredServiceAuditRecordV1::from_stored_parts(
        sequence,
        RequestId::from_unix_milliseconds_and_random(1_700_000_000_000 + sequence.get(), [1; 10])
            .unwrap(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        Some(AuditPrincipalV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1_700_000_000_000, [2; 10]).unwrap(),
            std::num::NonZeroU64::new(1).unwrap(),
        )),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::new(Vec::new()).unwrap(),
        None,
        ServiceAuditLinkV1::None,
    )
    .unwrap();
    riffdb_storage_api::proto_codec::encode_service_audit_record_v2(&record)
        .unwrap()
        .into_bytes()
}
