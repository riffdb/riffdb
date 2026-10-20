// req: REP-005, STO-012, REC-001
use super::*;

#[test]
fn catalog_bound_constructors_refuse_foreign_catalog_and_target() {
    let (_, _, source) = evidence(2, 7, false);
    let fence = source.fence();
    for catalog in [
        AuthoritativeStateCatalogV2.digest(),
        crate::AuthoritativeStateCatalogV1.digest(),
    ] {
        let old = ChangelogLineageV3::new_with_catalog(
            fence.target().database_id(),
            2,
            LeadershipEpochV1::new(7).unwrap(),
            catalog,
        )
        .unwrap();
        assert!(Admission::active_catalog_bound(old).is_err());
        assert!(
            Fence::new_catalog_bound(
                old,
                fence.administration_sequence(),
                fence.timestamp(),
                fence.operation_id(),
                fence.request_id(),
                fence.principal().clone(),
                fence.approval_id().cloned(),
                fence.target(),
                fence.generation(),
                fence.observed(),
            )
            .is_err()
        );
    }
    for changed in [lineage(3, 7), lineage(2, 8)] {
        assert!(
            Fence::new_catalog_bound(
                changed,
                fence.administration_sequence(),
                fence.timestamp(),
                fence.operation_id(),
                fence.request_id(),
                fence.principal().clone(),
                fence.approval_id().cloned(),
                fence.target(),
                fence.generation(),
                fence.observed(),
            )
            .is_err()
        );
    }
}

#[test]
fn checksum_correct_active_and_fence_catalog_substitutions_refuse() {
    use wire::replication_primary_admission_v2::{Active, State};
    for digest in [
        vec![],
        vec![3; 31],
        vec![3; 33],
        vec![3; 32],
        AuthoritativeStateCatalogV2.digest().to_vec(),
    ] {
        let source = lineage(2, 7);
        let active = wire::ReplicationPrimaryAdmissionV2 {
            state: Some(State::Active(Active {
                database_id: source.database_id().as_bytes().to_vec(),
                history_incarnation: 2,
                leadership_epoch: 7,
                catalog_digest: digest.clone(),
            })),
        };
        assert!(
            decode_replication_primary_admission(&raw(ADMISSION_V2, active.encode_to_vec()))
                .is_err()
        );
        let (_, _, source) = evidence(2, 7, false);
        let mut fence = catalog_fencing_v2_to_wire(source.fence());
        fence.catalog_digest = digest;
        assert!(
            decode_primary_fence_administration(&raw(FENCE_V2, fence.encode_to_vec())).is_err()
        );
        let admission = wire::ReplicationPrimaryAdmissionV2 {
            state: Some(State::Fenced(Box::new(fence))),
        };
        assert!(
            decode_replication_primary_admission(&raw(ADMISSION_V2, admission.encode_to_vec()))
                .is_err()
        );
    }
}

#[test]
fn checksum_correct_promotion_catalog_identity_and_result_substitutions_refuse() {
    for case in 0..17 {
        let mut value = wire_promotion();
        match case {
            0 => value.published_catalog_digest.clear(),
            1 => value.published_catalog_digest = AuthoritativeStateCatalogV2.digest().to_vec(),
            2 => {
                value.fence.as_mut().unwrap().catalog_digest =
                    AuthoritativeStateCatalogV2.digest().to_vec()
            }
            3 => {
                value.source_history.as_mut().unwrap().catalog_digest =
                    AuthoritativeStateCatalogV2.digest().to_vec()
            }
            4 => value.published_incarnation += 1,
            5 => value.published_epoch += 1,
            6 => value.application_rpo += 1,
            7 => value.source_history.as_mut().unwrap().history_incarnation += 1,
            8 => value.fence.as_mut().unwrap().registration_generation = 0,
            9 => {
                value
                    .fence
                    .as_mut()
                    .unwrap()
                    .target
                    .as_mut()
                    .unwrap()
                    .leadership_epoch += 1
            }
            10 => value.applied.as_mut().unwrap().application_sequence = 12,
            11 => value.attempt_steps.push(255),
            12 => value.attempt_steps = vec![1; MAX_REPLICATION_PROMOTION_STEPS_V1 + 1],
            13 => value.ingress = i32::from(ServiceIngressKindV1::McpHttp.tag()),
            14 => value.administration_sequence += 1,
            15 => value.fence.as_mut().unwrap().administration_sequence += 1,
            16 => value.fence = None,
            _ => unreachable!(),
        }
        let encoded = raw(PROMOTION_V2, value.encode_to_vec());
        assert!(
            decode_promotion_administration(&encoded).is_err(),
            "case {case}"
        );
        assert!(
            decode_promotion_administration_v2(&encoded).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn known_role_corruption_never_falls_back_and_old_catalog_encoders_stay_closed() {
    let (_, _, source) = evidence(2, 7, false);
    let active = Admission::active_catalog_bound(lineage(2, 7)).unwrap();
    let promotion = promotion(false);
    let values = [
        encode_replication_primary_admission_v2(&active).unwrap(),
        encode_primary_fence_administration_v2(source.fence()).unwrap(),
        encode_promotion_administration_v2(&promotion).unwrap(),
    ];
    for encoded in values {
        let mut corrupted = encoded.as_bytes().to_vec();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0x80;
        assert!(decode_replication_primary_admission(&corrupted).is_err());
        assert!(decode_primary_fence_administration(&corrupted).is_err());
        assert!(decode_promotion_administration(&corrupted).is_err());
        let mut trailing = encoded.as_bytes().to_vec();
        trailing.push(0);
        assert!(decode_replication_primary_admission(&trailing).is_err());
        assert!(decode_primary_fence_administration(&trailing).is_err());
        assert!(decode_promotion_administration(&trailing).is_err());
    }
    let mut malformed = wire_promotion();
    malformed.published_catalog_digest = vec![0; 32];
    let encoded = raw(PROMOTION_V2, malformed.encode_to_vec());
    assert_eq!(
        decode_promotion_administration(&encoded)
            .unwrap_err()
            .kind(),
        decode_promotion_administration_v2(&encoded)
            .unwrap_err()
            .kind()
    );
}

#[test]
fn catalog_bound_promotion_refuses_incarnation_or_epoch_exhaustion() {
    for (incarnation, epoch) in [(u64::MAX, 7), (2, u64::MAX)] {
        let (request, follower, source) = evidence(incarnation, epoch, false);
        assert!(Selection::new(request, follower, source).is_err());
    }
}
