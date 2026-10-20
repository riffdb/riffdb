//! Actual sidecar row counterexamples; these setup writes are NOT a Source seeder.
//! No constructed Base certifies canonical rows, source closure or publication.
use super::*;
use riffdb_storage_api::{
    GenerationSourceBaseV1, ProjectionApplyBatchResult, ProjectionApplyBatchV1,
    ProjectionGenerationPosition, ProjectionSourceReferenceV1, encode_generation_source_base_v1,
};
use riffdb_types::{HashDomain, ProjectionCheckpointOrdinal, ProjectionSourceLocatorV1, hash};

// One coherent observation; historical release counters legitimately advance.
fn live_work_balance(budget: &riffdb_types::ProjectionSourceWorkingBudgetV1) -> (u64, u64) {
    let observation = budget.observation().unwrap();
    (observation.reserved_bytes, observation.owned_bytes)
}

fn base(schema: &riffdb_storage_api::CheckedProjectionSchema, at: u64) -> GenerationSourceBaseV1 {
    GenerationSourceBaseV1::new(
        ProjectionGeneration::first(),
        ProjectionSourceReferenceV1::new(
            schema.identity().clone(),
            ProjectionSourceLocatorV1::new(database_id(), 1, ProjectionCheckpointOrdinal::first())
                .unwrap(),
            hash(
                HashDomain::ProjectionSourceDescriptor,
                b"structural base fixture",
            ),
            hash(
                HashDomain::ProjectionSourceOrigin,
                b"structural origin fixture",
            ),
        )
        .unwrap(),
        CommitSequence::new(at).unwrap(),
    )
}
fn control(
    schema: &riffdb_storage_api::CheckedProjectionSchema,
    at: u64,
    ready: bool,
) -> StoredProjectionControlV1 {
    let position = ProjectionGenerationPosition::new(
        ProjectionGeneration::first(),
        FrontierPosition::AppliedThrough(CommitSequence::new(at).unwrap()),
    );
    StoredProjectionControlV1::new(
        schema.identity().clone(),
        ProjectionGeneration::first(),
        ready.then_some(position),
        (!ready).then_some(position),
        ready.then_some(riffdb_storage_api::PublishedApplyModeV1::Enabled),
        if ready {
            ProjectionLifecycleV1::Ready
        } else {
            ProjectionLifecycleV1::CatchingUp
        },
        None,
    )
    .unwrap()
}
fn install_base(
    ports: &RedbOperationalPorts,
    physical: &ProjectionIdentity,
    value: &GenerationSourceBaseV1,
) {
    let encoded = encode_generation_source_base_v1(value).unwrap();
    let mut key = [0; riffdb_types::MAX_KEY_BYTES];
    let len = crate::source_keys::encode_generation_source_base_key_slice_into(
        &mut key,
        physical,
        value.generation(),
    )
    .unwrap();
    let access = ports.begin_derived_write().unwrap();
    access
        .transaction()
        .unwrap()
        .open_table(super::super::source_base::BASES)
        .unwrap()
        .insert(&key[..len], encoded.as_bytes())
        .unwrap();
    access
        .commit_for(RedbTestOperation::ProjectionMutation)
        .unwrap();
}
fn request(
    schema: &riffdb_storage_api::CheckedProjectionSchema,
    at: u64,
) -> ProjectionApplyRequestV1 {
    ProjectionApplyRequestV1::new(
        schema.clone(),
        ProjectionGeneration::first(),
        CommitSequence::new(at).unwrap(),
        if at == 1 {
            FrontierPosition::BeforeFirst
        } else {
            FrontierPosition::AppliedThrough(CommitSequence::new(at - 1).unwrap())
        },
        Vec::new(),
    )
    .unwrap()
}
fn marker(
    ports: &RedbOperationalPorts,
    request: &ProjectionApplyRequestV1,
) -> StoredProjectionApplyV1 {
    let marker = StoredProjectionApplyV1::new(
        ProjectionApplyKey::new(
            request.identity().clone(),
            request.generation(),
            request.sequence(),
        ),
        request.apply_hash(),
    );
    let encoded = encode_projection_apply_v1(&marker).unwrap();
    let access = ports.begin_derived_write().unwrap();
    access
        .transaction()
        .unwrap()
        .open_table(PROJECTION_APPLIED)
        .unwrap()
        .insert(marker.key().as_bytes(), encoded.as_bytes())
        .unwrap();
    access
        .commit_for(RedbTestOperation::ProjectionMutation)
        .unwrap();
    marker
}

// req: PRJ-004, REC-003, STO-012
#[test]
fn seeded_apply_refuses_base_prefix_without_primary_history_or_manufactured_markers() {
    let (_path, mut ports) = operational("source-base-prefix-refusal");
    let schema = recovery_projection_schema();
    seed_command(&ports, CommitSequence::new(3).unwrap(), 0);
    let expected = control(&schema, 3, false);
    install_control(&ports, schema.identity(), &expected);
    install_base(&ports, schema.identity(), &base(&schema, 3));
    let budget = ports.shared.database.source_working_budget().clone();
    let before = live_work_balance(&budget);
    for at in [1, 3] {
        assert_eq!(
            ports.apply_projection(&request(&schema, at)).unwrap(),
            ProjectionApplyResult::NotAppliedInGeneration {
                applied_through: CommitSequence::new(3).unwrap()
            }
        );
        assert_eq!(live_work_balance(&budget), before);
    }
    let read = ports.begin_derived_read().unwrap();
    assert_eq!(
        read_projection_control(
            &read.transaction.open_table(PROJECTION_FRONTIER).unwrap(),
            schema.identity()
        )
        .unwrap(),
        Some(expected)
    );
    assert_eq!(
        read.transaction
            .open_table(PROJECTION_APPLIED)
            .unwrap()
            .len()
            .unwrap(),
        0
    );
    assert_eq!(
        read.transaction
            .open_table(PROJECTION_STATE)
            .unwrap()
            .len()
            .unwrap(),
        0
    );
}

// req: PRJ-004, REC-003, STO-012
#[test]
fn seeded_tail_equality_remains_exact_but_base_does_not_authorize_query_or_new_apply() {
    let (_path, mut ports) = operational("source-base-tail-equality");
    let schema = recovery_projection_schema();
    seed_command(&ports, CommitSequence::first(), 0);
    seed_command(&ports, CommitSequence::new(2).unwrap(), 0);
    let current = control(&schema, 2, true);
    install_control(&ports, schema.identity(), &current);
    install_base(&ports, schema.identity(), &base(&schema, 1));
    let tail = request(&schema, 2);
    let expected = marker(&ports, &tail);
    assert_eq!(
        ports.apply_projection(&tail).unwrap(),
        ProjectionApplyResult::AlreadyApplied(expected.clone())
    );
    let key = schema
        .group_key(
            ProjectionGeneration::first(),
            &[CanonicalValue::string("group-a").unwrap()],
        )
        .unwrap();
    let unequal = ProjectionApplyRequestV1::new(
        schema.clone(),
        ProjectionGeneration::first(),
        CommitSequence::new(2).unwrap(),
        FrontierPosition::AppliedThrough(CommitSequence::first()),
        vec![
            ProjectionRowUpdateV1::new(
                &schema,
                key,
                ProjectionRowPrior::Absent,
                recovery_measures(1),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert_ne!(unequal.apply_hash(), tail.apply_hash());
    assert_eq!(
        ports.apply_projection(&unequal).unwrap_err().kind(),
        StorageErrorKind::CorruptData
    );
    let query = ProjectionQueryRequest::new(
        ProjectionQuerySelector::new(schema.clone(), Vec::new()).unwrap(),
        NonZeroU16::MIN,
        None,
    )
    .unwrap();
    assert_eq!(
        ports.query_projection(&query).unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    seed_command(&ports, CommitSequence::new(3).unwrap(), 0);
    assert_eq!(
        ports
            .apply_projection(&request(&schema, 3))
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    let read = ports.begin_derived_read().unwrap();
    assert_eq!(
        read_projection_marker(
            &read.transaction.open_table(PROJECTION_APPLIED).unwrap(),
            expected.key()
        )
        .unwrap(),
        Some(expected)
    );
    assert_eq!(
        read.transaction
            .open_table(PROJECTION_APPLIED)
            .unwrap()
            .len()
            .unwrap(),
        1
    );
    assert_eq!(
        read_projection_control(
            &read.transaction.open_table(PROJECTION_FRONTIER).unwrap(),
            schema.identity()
        )
        .unwrap(),
        Some(current)
    );
}

// req: PRJ-004, REC-003, STO-012
#[test]
fn seeded_restart_and_retention_require_actual_source_owner_and_base_mismatch_refunds() {
    let (_path, mut ports) = operational("source-base-restart-retention");
    let schema = recovery_projection_schema();
    seed_command(&ports, CommitSequence::first(), 0);
    let current = control(&schema, 1, true);
    install_control(&ports, schema.identity(), &current);
    install_base(&ports, schema.identity(), &base(&schema, 1));
    let validation = ProjectionRecoveryValidationRequestV1::new(
        schema.clone(),
        current,
        FrontierPosition::AppliedThrough(CommitSequence::first()),
        ProjectionGeneration::first(),
        ProjectionRecoveryPageLimit::new(NonZeroU16::MIN).unwrap(),
        None,
        ProjectionRecoveryExpectedPageV1::Markers(Vec::new()),
    )
    .unwrap();
    assert_eq!(
        ports
            .validate_projection_recovery_page(&validation)
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    let read = ports.begin_derived_read().unwrap();
    assert_eq!(
        crate::retention::derived_projection_durable_frontier(
            &read.transaction,
            &std::collections::BTreeSet::new()
        )
        .unwrap_err()
        .kind(),
        StorageErrorKind::Unavailable
    );
    drop(read);
    let foreign_schema = recovery_projection_schema();
    let wrong = GenerationSourceBaseV1::new(
        ProjectionGeneration::first(),
        ProjectionSourceReferenceV1::new(
            projection_identity(0xe7),
            base(&foreign_schema, 1).reference().locator(),
            hash(HashDomain::ProjectionSourceDescriptor, b"other descriptor"),
            hash(HashDomain::ProjectionSourceOrigin, b"other origin"),
        )
        .unwrap(),
        CommitSequence::first(),
    );
    install_base(&ports, schema.identity(), &wrong);
    let budget = ports.shared.database.source_working_budget().clone();
    let before = live_work_balance(&budget);
    assert_eq!(
        ports
            .apply_projection(&request(&schema, 1))
            .unwrap_err()
            .kind(),
        StorageErrorKind::CorruptData
    );
    assert_eq!(live_work_balance(&budget), before);
}

// req: PRJ-004, REC-003, STO-012
#[test]
fn seeded_batch_and_unknown_resolution_refuse_base_prefix_without_equal_success() {
    let (_path, mut ports) = operational("source-base-batch-prefix");
    let schema = recovery_projection_schema();
    seed_command(&ports, CommitSequence::new(3).unwrap(), 0);
    let current = control(&schema, 2, false);
    install_control(&ports, schema.identity(), &current);
    install_base(&ports, schema.identity(), &base(&schema, 2));
    let before = StoredProjectionControlV1::new(
        schema.identity().clone(),
        ProjectionGeneration::first(),
        None,
        Some(ProjectionGenerationPosition::new(
            ProjectionGeneration::first(),
            FrontierPosition::BeforeFirst,
        )),
        None,
        ProjectionLifecycleV1::CatchingUp,
        None,
    )
    .unwrap();
    let batch = ProjectionApplyBatchV1::new(
        before,
        vec![request(&schema, 1), request(&schema, 2)],
        Vec::new(),
    )
    .unwrap();
    let refusal = ProjectionApplyBatchResult::NotAppliedInGeneration {
        applied_through: CommitSequence::new(2).unwrap(),
    };
    assert_eq!(ports.apply_projection_batch(&batch).unwrap(), refusal);
    assert_eq!(ports.resolve_projection_batch(&batch).unwrap(), refusal);
    let read = ports.begin_derived_read().unwrap();
    assert_eq!(
        read.transaction
            .open_table(PROJECTION_APPLIED)
            .unwrap()
            .len()
            .unwrap(),
        0
    );
    assert_eq!(
        read_projection_control(
            &read.transaction.open_table(PROJECTION_FRONTIER).unwrap(),
            schema.identity()
        )
        .unwrap(),
        Some(current)
    );
}
