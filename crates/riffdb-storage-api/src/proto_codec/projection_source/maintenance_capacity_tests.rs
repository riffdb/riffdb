//! Sizing-only structural controls; no Source candidate or publication is forged.
use super::*;
use riffdb_types::{AdministrationSequence, CommitSequence, DualFrontier, HashDomain, hash};

fn initial(with_prior: bool, wide_prior: bool) -> ProjectionSourceMaintenanceV1 {
    let base = descriptor();
    let prior = with_prior.then(|| {
        let ordinary = root();
        if !wide_prior {
            return ordinary;
        }
        // Legal structural maximal-width binding; not a durable publication.
        let application = CommitSequence::new(u64::MAX).unwrap();
        let previous = AdministrationSequence::new(u64::MAX - 1).unwrap();
        let assigned = AdministrationSequence::new(u64::MAX).unwrap();
        let binding = AuthoritativeTransactionBindingV3 {
            predecessor: Some(crate::ChangelogTransactionSequence::new(u64::MAX - 1).unwrap()),
            sequence: crate::ChangelogTransactionSequence::new(u64::MAX).unwrap(),
            predecessor_frontier: DualFrontier::new(Some(application), Some(previous)),
            covered_frontier: DualFrontier::new(Some(application), Some(assigned)),
            ..ordinary.publication_binding()
        };
        ProjectionSourceRootV1::new(ordinary.reference().clone(), binding, Some(assigned)).unwrap()
    });
    let target = ProjectionSourceDescriptorV1::new(crate::ProjectionSourceDescriptorPartsV1 {
        identity: base.reference().identity().clone(),
        locator: locator(2),
        applied_through: base.applied_through(),
        captured_source: base.captured_source(),
        origin: base.reference().origin(),
        prior_source_digest: prior.as_ref().map(|p| p.reference().digest()),
        row_count: crate::MAX_PROJECTION_SOURCE_ROWS,
        canonical_row_bytes: crate::MAX_PROJECTION_SOURCE_OBJECT_BYTES,
        row_root: base.row_root(),
    })
    .unwrap();
    ProjectionSourceMaintenanceV1::initial(target, prior).unwrap()
}

// req: STO-020, STO-012, PRJ-004
#[test]
fn future_maintenance_capacity_bounds_every_intermediate_counter_and_key_varint_width() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    for (has_prior, wide_prior) in [(false, false), (true, false), (true, true)] {
        let initial = initial(has_prior, wide_prior);
        for maximum in [1, 56, 127, 128, 256] {
            let bound = projection_source_maintenance_future_encoded_bound_admitted_v1(
                &initial, maximum, &budget,
            )
            .unwrap();
            assert!(
                encode_projection_source_maintenance_v1(&initial)
                    .unwrap()
                    .as_bytes()
                    .len()
                    <= bound.get()
            );
            // Exercise every permitted row-count varint transition, a native
            // byte varint transition, and the complete final exact totals.
            let states = [
                (1, 1),
                (1, 127),
                (1, 128),
                (127, 16_383),
                (128, 16_384),
                (16_383, 2_097_151),
                (16_384, 2_097_152),
                (
                    crate::MAX_PROJECTION_SOURCE_ROWS - 1,
                    crate::MAX_PROJECTION_SOURCE_OBJECT_BYTES - 1,
                ),
                (
                    crate::MAX_PROJECTION_SOURCE_ROWS,
                    crate::MAX_PROJECTION_SOURCE_OBJECT_BYTES,
                ),
            ];
            for (rows, bytes) in states {
                for length in [1, maximum] {
                    let progress = ProjectionSourceMaintenanceV1::from_stored_parts(
                        initial.descriptor().clone(),
                        initial.expected_prior_root().cloned(),
                        rows,
                        bytes,
                        Some(vec![0x51; length]),
                        hash(
                            HashDomain::ProjectionSourceCopyProgress,
                            b"structural size-only progress",
                        ),
                    )
                    .unwrap();
                    let encoded = encode_projection_source_maintenance_v1(&progress).unwrap();
                    assert!(encoded.as_bytes().len() <= bound.get());
                    if rows == crate::MAX_PROJECTION_SOURCE_ROWS && length == maximum {
                        assert_eq!(encoded.as_bytes().len(), bound.get());
                    }
                }
            }
        }
    }
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

// req: STO-020, STO-012, PRJ-004
#[test]
fn future_maintenance_capacity_refuses_supported_large_native_key_before_any_progress() {
    let initial = initial(false, false);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    assert!(encode_projection_source_maintenance_v1(&initial).is_ok());
    let before = budget.observation().unwrap();
    let error =
        projection_source_maintenance_future_encoded_bound_admitted_v1(&initial, 4079, &budget)
            .unwrap_err();
    assert!(
        matches!(error,ProjectionSourceRecordDecodeErrorV1::Codec(error)if error.kind()==DurableCodecErrorKind::LimitExceeded)
    );
    assert_eq!(initial.copied_rows(), 0);
    assert_eq!(initial.copied_canonical_row_bytes(), 0);
    assert!(initial.last_copied_key().is_none());
    let after = budget.observation().unwrap();
    assert_eq!(after.owned_bytes, before.owned_bytes);
    assert_eq!(after.reserved_bytes, before.reserved_bytes);
}

// req: STO-020, STO-012, PRJ-004
#[test]
fn future_maintenance_capacity_uses_actual_source_admission_and_rejects_invalid_summary_context() {
    let initial = initial(true, false);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let observation = budget.observation().unwrap();
    let remainder = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - observation.fixed_owner_bytes
        - observation.owned_bytes
        - observation.reserved_bytes;
    let pressure = budget.reserve(remainder).unwrap();
    let saturated = budget.observation().unwrap();
    assert!(matches!(
        projection_source_maintenance_future_encoded_bound_admitted_v1(&initial, 56, &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
    ));
    assert_eq!(budget.observation().unwrap(), saturated);
    drop(pressure);
    let error =
        projection_source_maintenance_future_encoded_bound_admitted_v1(&initial, 0, &budget)
            .unwrap_err();
    assert!(
        matches!(error,ProjectionSourceRecordDecodeErrorV1::Codec(error)if error.kind()==DurableCodecErrorKind::CorruptData)
    );
}
