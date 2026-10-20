// req: REP-005, STO-012, REC-001
use super::*;

#[test]
fn checksum_correct_successor_wire_owners_refuse_unknown_duplicate_and_oversized_fields() {
    let (_, _, source) = evidence(2, 7, false);
    let value = catalog_fencing_v2_to_wire(source.fence());
    let mut duplicate_catalog = value.encode_to_vec();
    duplicate_catalog.extend_from_slice(&[0x52, 0x20]);
    duplicate_catalog.extend_from_slice(&AuthoritativeStateCatalogV3.digest());
    let mut unknown_scalar = value.encode_to_vec();
    unknown_scalar.extend_from_slice(&[0x58, 0x01]);
    for payload in [duplicate_catalog, unknown_scalar] {
        let encoded = raw(FENCE_V2, payload);
        assert!(decode_primary_fence_administration(&encoded).is_err());
        assert!(decode_primary_fence_administration_v2(&encoded).is_err());
    }
    for mutation in 0..4 {
        let mut value = value.clone();
        match mutation {
            0 => value.principal.as_mut().unwrap().principal_id = "a".repeat(257),
            1 => value.approval_id = Some("p".repeat(257)),
            2 => value.target.as_mut().unwrap().database_id = vec![1; 15],
            3 => value.observed.as_mut().unwrap().history_hash = vec![1; 31],
            _ => unreachable!(),
        }
        assert!(
            decode_primary_fence_administration(&raw(FENCE_V2, value.encode_to_vec())).is_err()
        );
    }
    let active = encode_replication_primary_admission_v2(
        &Admission::active_catalog_bound(lineage(2, 7)).unwrap(),
    )
    .unwrap();
    let active = readable_record_registry()
        .decode(active.as_bytes())
        .unwrap();
    let fenced = wire::ReplicationPrimaryAdmissionV2 {
        state: Some(wire::replication_primary_admission_v2::State::Fenced(
            Box::new(value),
        )),
    }
    .encode_to_vec();
    let mut contradictory = active.payload().to_vec();
    contradictory.extend_from_slice(&fenced);
    assert!(decode_replication_primary_admission(&raw(ADMISSION_V2, contradictory)).is_err());
    assert!(decode_replication_primary_admission(&raw(ADMISSION_V2, vec![])).is_err());
}
