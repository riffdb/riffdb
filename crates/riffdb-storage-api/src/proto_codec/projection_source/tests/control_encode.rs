//! Owning control encoding preserves bytes and admits construction first.
use super::*;

fn maintenance_with_prior() -> ProjectionSourceMaintenanceV1 {
    let old = descriptor();
    let prior = root();
    let next = ProjectionSourceDescriptorV1::new(ProjectionSourceDescriptorPartsV1 {
        identity: old.reference().identity().clone(),
        locator: locator(2),
        applied_through: old.applied_through(),
        captured_source: old.captured_source(),
        origin: old.reference().origin(),
        prior_source_digest: Some(prior.reference().digest()),
        row_count: old.row_count(),
        canonical_row_bytes: old.canonical_row_bytes(),
        row_root: old.row_root(),
    })
    .unwrap();
    ProjectionSourceMaintenanceV1::initial(next, Some(prior)).unwrap()
}

fn check_owned<T>(
    value: &T,
    ordinary: fn(&T) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError>,
    admitted: fn(
        &T,
        &ProjectionSourceWorkingBudgetV1,
    ) -> Result<
        ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>,
        ProjectionSourceRecordDecodeErrorV1,
    >,
) {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let initial = budget.observation().unwrap();
    let bytes = admitted(value, &budget).unwrap();
    assert_eq!(bytes.as_bytes(), ordinary(value).unwrap().as_bytes());
    assert!(bytes.belongs_to(&budget));
    assert_eq!(bytes.encoded_content_charge().get(), bytes.as_bytes().len());
    assert!(budget.observation().unwrap().owned_bytes > initial.owned_bytes);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    let retained = bytes.clone();
    drop(bytes);
    assert!(budget.observation().unwrap().owned_bytes > initial.owned_bytes);
    drop(retained);
    assert_eq!(
        budget.observation().unwrap().owned_bytes,
        initial.owned_bytes
    );
    let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES - initial.fixed_owner_bytes;
    let pressure = budget.reserve(free).unwrap();
    let saturated = budget.observation().unwrap();
    assert_eq!(
        admitted(value, &budget)
            .err()
            .expect("Source capacity refusal"),
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable
    );
    assert_eq!(budget.observation().unwrap(), saturated);
    drop(pressure);
}

// req: STO-020, PRJ-004, REC-003
#[test]
fn source_control_owning_encoding_preserves_all_four_roles_and_final_buffer_custody() {
    for lifecycle in 1..=6 {
        check_owned(
            &ProjectionSourceObjectV1::new(
                descriptor(),
                ProjectionSourceLifecycleV1::from_tag(lifecycle).unwrap(),
            ),
            encode_projection_source_object_v1,
            encode_projection_source_object_admitted_v1,
        );
    }
    check_owned(
        &root(),
        encode_projection_source_root_v1,
        encode_projection_source_root_admitted_v1,
    );
    check_owned(
        &maintenance_with_prior(),
        encode_projection_source_maintenance_v1,
        encode_projection_source_maintenance_admitted_v1,
    );
    for position in [
        ProjectionCheckpointOrdinalPositionV1::Next(
            ProjectionCheckpointOrdinal::new(u64::MAX).unwrap(),
        ),
        ProjectionCheckpointOrdinalPositionV1::Exhausted,
    ] {
        check_owned(
            &ProjectionCheckpointOrdinalAllocatorV1::new(u64::MAX, position).unwrap(),
            encode_projection_checkpoint_ordinal_allocator_v1,
            encode_projection_checkpoint_ordinal_allocator_admitted_v1,
        );
    }
}

// req: STO-020, PRJ-004
#[test]
fn source_control_wire_preflight_covers_all_nested_buffer_allocations() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let mut census = budget
        .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let object = ProjectionSourceObjectV1::new(descriptor(), ProjectionSourceLifecycleV1::Copying);
    let message = object_to_wire(&object);
    let report = riffdb_types::OwnedCapacityInspectionV1::preview(
        &message,
        census.inspection_scope().unwrap(),
    )
    .unwrap();
    assert!(report.shared_allocations().is_empty());
    assert!(
        report.unshared_bytes()
            <= super::super::control_encode::descriptor_heap(object.descriptor()).unwrap()
    );
    drop(report);
    let root = root();
    let message = root_to_wire(&root);
    let report = riffdb_types::OwnedCapacityInspectionV1::preview(
        &message,
        census.inspection_scope().unwrap(),
    )
    .unwrap();
    assert!(report.unshared_bytes() <= super::super::control_encode::root_heap(&root).unwrap());
    drop(report);
    let maintenance = maintenance_with_prior();
    for key_bytes in [1, 127, 128, riffdb_types::MAX_KEY_BYTES] {
        let value = ProjectionSourceMaintenanceV1::from_stored_parts(
            maintenance.descriptor().clone(),
            maintenance.expected_prior_root().cloned(),
            1,
            900,
            Some(vec![7; key_bytes]),
            hash(
                HashDomain::ProjectionSourceCopyProgress,
                b"synthetic generated buffer census",
            ),
        )
        .unwrap();
        let message = maintenance_to_wire(&value);
        let report = riffdb_types::OwnedCapacityInspectionV1::preview(
            &message,
            census.inspection_scope().unwrap(),
        )
        .unwrap();
        assert!(
            report.unshared_bytes()
                <= super::super::control_encode::maintenance_heap(&value).unwrap()
        );
    }
}

// req: STO-020, PRJ-004
#[test]
fn source_control_encoder_borrows_foreign_semantic_owner_without_reassigning_it() {
    let input_budget = ProjectionSourceWorkingBudgetV1::new();
    let input = input_budget
        .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 128 * 1024)
        .unwrap()
        .settle_shared_value(ProjectionSourceObjectV1::new(
            descriptor(),
            ProjectionSourceLifecycleV1::Copying,
        ))
        .unwrap();
    let held = input_budget.observation().unwrap();
    let output_budget = ProjectionSourceWorkingBudgetV1::new();
    let output = encode_projection_source_object_admitted_v1(&input, &output_budget).unwrap();
    assert_eq!(input_budget.observation().unwrap(), held);
    assert!(input.belongs_to(&input_budget));
    assert!(output.belongs_to(&output_budget));
    assert!(!output.belongs_to(&input_budget));
    assert_eq!(
        output.as_bytes(),
        encode_projection_source_object_v1(&input)
            .unwrap()
            .as_bytes()
    );
    drop(output);
    assert_eq!(output_budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(input_budget.observation().unwrap(), held);
}

// req: STO-020, PRJ-004, STO-012
#[test]
fn source_control_owning_encoder_preserves_four_kib_refusal_and_exact_admission_boundary() {
    let value = ProjectionSourceMaintenanceV1::from_stored_parts(
        descriptor(),
        None,
        1,
        900,
        Some(vec![7; riffdb_types::MAX_KEY_BYTES]),
        hash(
            HashDomain::ProjectionSourceCopyProgress,
            b"synthetic full control key",
        ),
    )
    .unwrap();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    assert!(
        matches!(encode_projection_source_maintenance_admitted_v1(&value, &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::Codec(error)) if error.kind()==super::super::super::DurableCodecErrorKind::LimitExceeded)
    );
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    let object = ProjectionSourceObjectV1::new(descriptor(), ProjectionSourceLifecycleV1::Copying);
    let bound = super::super::control_encode::object_work_bound(&object).unwrap();
    let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - budget.observation().unwrap().fixed_owner_bytes;
    let pressure = budget.reserve(free - bound + 1).unwrap();
    assert_eq!(
        encode_projection_source_object_admitted_v1(&object, &budget)
            .err()
            .expect("Source capacity refusal"),
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable
    );
    drop(pressure);
    let pressure = budget.reserve(free - bound).unwrap();
    let encoded = encode_projection_source_object_admitted_v1(&object, &budget).unwrap();
    assert!(encoded.as_bytes().len() <= MAX_PROJECTION_SOURCE_CONTROL_BYTES);
    drop(encoded);
    drop(pressure);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

// req: STO-020, PRJ-004, STO-012
#[test]
fn source_history_control_encoding_preserves_ordinary_bytes_and_retained_admission() {
    let history = descriptor().captured_source();
    check_owned(
        &history,
        |value| super::super::super::changelog::encode_changelog_history_state_v3(*value),
        |value, budget| encode_changelog_history_state_source_admitted_v3(*value, budget),
    );
    for allocator in [
        history.expected_allocator(),
        crate::ChangelogTransactionAllocator::Exhausted,
    ] {
        check_owned(
            &allocator,
            |value| {
                super::super::super::metadata::encode_changelog_transaction_allocator_v3(*value)
            },
            |value, budget| {
                encode_changelog_transaction_allocator_source_admitted_v3(*value, budget)
            },
        );
    }
}
