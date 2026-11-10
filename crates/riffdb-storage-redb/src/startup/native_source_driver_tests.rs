//! Genuine sealed adapter proofs; no synthetic command or publication success.
use super::*;
use crate::source_checkpoint_adapter::NativeSourceCheckpointBackend;
use crate::source_evaluation_work::SourceEvaluationWorkOwner;
use riffdb_catalog::ActiveCatalogSnapshot;
use riffdb_projection::source_driver::{
    SourceCheckpointCandidate, SourceCheckpointMaintenanceCoordinator,
    SourceCheckpointStreamSummary, SourceDriverError, SourceDriverPlan,
    SourceNativePublicationConfirmation, drive_source_checkpoint,
};
use riffdb_types::{CanonicalRecord, CanonicalValue, ProjectionGeneration, ProjectionIdentity};
use std::{
    marker::PhantomData,
    sync::{Arc, atomic::AtomicBool},
};

fn fixture(
    label: &str,
) -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
) {
    let root = crate::test_path::ScopedDirectory::new(label);
    let mut store = RedbStore::open(root.join("db.redb")).unwrap();
    let database =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    store
        .initialize_projection_source_fixture(database)
        .unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("genuine Fresh Source fixture must pass normal startup");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    native_source_command_fixture::activate_source_projection(&mut ports).unwrap();
    native_source_command_fixture::append_source_projection_journaled(&ports, 1).unwrap();
    native_source_command_fixture::append_source_projection_journaled(&ports, 2).unwrap();
    (root, ports)
}
fn projection_identity() -> ProjectionIdentity {
    let bundle = native_source_command_fixture::source_projection_bundle();
    let plan = &bundle.bundle().projections()[0];
    ProjectionIdentity::new(
        bundle.lineage().clone(),
        plan.projection_id(),
        plan.plan_hash(),
    )
}
struct RefuseAfterVerification<'work> {
    expected: Option<SourceCheckpointStreamSummary>,
    calls: usize,
    _backend: PhantomData<fn() -> NativeSourceCheckpointBackend<'work>>,
}
impl<'work> RefuseAfterVerification<'work> {
    fn new(expected: Option<SourceCheckpointStreamSummary>) -> Self {
        Self {
            expected,
            calls: 0,
            _backend: PhantomData,
        }
    }
}
impl<'work> SourceCheckpointMaintenanceCoordinator for RefuseAfterVerification<'work> {
    type Backend = NativeSourceCheckpointBackend<'work>;
    fn maintain<'candidate, 'stage, 'slot, 'session>(
        &mut self,
        candidate: SourceCheckpointCandidate<'candidate, 'stage, 'slot, 'session, Self::Backend>,
        _native: &mut <Self::Backend as riffdb_projection::source_driver::SourceCheckpointDriverBackend>::MaintenancePort<'session>,
    ) -> Result<SourceNativePublicationConfirmation<'session, Self::Backend>, SourceDriverError>
    where
        Self::Backend: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        self.calls += 1;
        assert_eq!(candidate.applied_through().get(), 2);
        if let Some(expected) = self.expected {
            assert_eq!(candidate.summary(), expected);
        }
        // This component test refuses after actual complete verification. It
        // neither fabricates native confirmation nor claims durable publication.
        Err(SourceDriverError::SourceUnavailable)
    }
}

// req: DEP-001, DEP-006, PRJ-004, STO-020, REC-003
#[test]
fn source_driver_repeats_same_input_after_physical_first_pass_teardown() {
    let (_root, ports) = fixture("source-driver-two-passes");
    let catalog = ActiveCatalogSnapshot::read(&ports).unwrap().unwrap();
    let prefix = ports.capture_original_prefix_retention().unwrap();
    let locator = prefix.tentative_source_locator().unwrap();
    let history = prefix.captured_history();
    let budget = prefix.originating_budget().clone();
    let capture = catalog
        .capture_projection_source_admitted_v1(&projection_identity(), &budget)
        .unwrap();
    let plan = SourceDriverPlan::new(&capture);
    // Independent expected rows from the actual event contract: one count for
    // each of IDs7 and8. No oracle row is supplied to the opaque native adapter.
    let rows = [7_u64, 8].map(|id| {
        let key = plan
            .schema()
            .group_key(ProjectionGeneration::first(), &[CanonicalValue::U64(id)])
            .unwrap();
        let measures =
            CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(1))]).unwrap();
        let row = riffdb_storage_api::ProjectionSourceRowV1::new(
            plan.schema(),
            locator,
            CommitSequence::new(2).unwrap(),
            key.clone(),
            measures,
            CommitSequence::new(id - 6).unwrap(),
        )
        .unwrap();
        (key, row)
    });
    let semantic_bytes = rows
        .iter()
        .map(|(_, row)| riffdb_projection::projection_source_row_preimage_bytes_v1(row).unwrap())
        .sum();
    let mut hash = riffdb_projection::ProjectionSourceRowRootV1::new(
        plan.identity(),
        Some(locator),
        CommitSequence::new(2).unwrap(),
        2,
        semantic_bytes,
    )
    .unwrap();
    let mut canonical_bytes = 0;
    let mut maximum_native_key_bytes = 0;
    let mut scratch = Vec::new();
    for (key, row) in &rows {
        scratch.resize(
            riffdb_projection::projection_source_row_scratch_bytes_v1(row).unwrap(),
            0,
        );
        hash.append(key, row, &mut scratch).unwrap();
        let key_bytes = crate::source_keys::source_row_key_len(key).unwrap();
        maximum_native_key_bytes = maximum_native_key_bytes.max(key_bytes);
        canonical_bytes += key_bytes as u64
            + riffdb_storage_api::proto_codec::projection_source_row_encoded_charge_v1(row)
                .unwrap()
                .get() as u64;
    }
    let expected = SourceCheckpointStreamSummary {
        row_count: 2,
        semantic_row_bytes: semantic_bytes,
        canonical_row_bytes: canonical_bytes,
        maximum_native_key_bytes,
        row_root: hash.finish().unwrap(),
    };
    drop(rows);
    drop(scratch);
    // Genuine later command must neither replace captured history nor enter
    // either complete evaluation. Both passes reuse the identical Original pin.
    native_source_command_fixture::append_source_projection_journaled(&ports, 3).unwrap();
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let directory = crate::test_path::ScopedDirectory::new("source-driver-same-slot");
    let parent = directory.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let before = budget.observation().unwrap();
    let mut adapter =
        NativeSourceCheckpointBackend::capture(&ports, &mut work, &plan, &parent).unwrap();
    let mut coordinator = RefuseAfterVerification::new(Some(expected));
    assert!(matches!(
        drive_source_checkpoint(&mut adapter, &mut coordinator),
        Err(SourceDriverError::SourceUnavailable)
    ));
    assert_eq!(
        coordinator.calls, 1,
        "only two complete equal genuine passes may reach candidate"
    );
    // Both maps use the exact SAME create-new scratch directory. Reaching the
    // coordinator proves first physical database/file/directory teardown before
    // second creation, rather than selecting a different staging path.
    assert!(!parent.join("source-staging-v1").exists());
    drop(adapter);
    let after = budget.observation().unwrap();
    assert_eq!(after.reserved_bytes, before.reserved_bytes);
    assert_eq!(after.owned_bytes, before.owned_bytes);
    assert!(after.released_allocations >= before.released_allocations);
    let (captured, slot) = work.split_for_stage();
    assert_eq!(captured.captured_history(), history);
    drop(slot);
    drop(work);
    assert!(budget.try_acquire_worker().is_ok());
}

// req: DEP-001, DEP-006, PRJ-004, STO-020, REC-003
#[test]
fn source_driver_rejects_fabricated_and_cross_backend_input() {
    let (_left_root, left) = fixture("source-driver-left");
    let (_right_root, right) = fixture("source-driver-right");
    let left_catalog = ActiveCatalogSnapshot::read(&left).unwrap().unwrap();
    let right_catalog = ActiveCatalogSnapshot::read(&right).unwrap().unwrap();
    let left_prefix = left.capture_original_prefix_retention().unwrap();
    let right_prefix = right.capture_original_prefix_retention().unwrap();
    assert_eq!(
        left_prefix.captured_history(),
        right_prefix.captured_history()
    );
    let left_budget = left_prefix.originating_budget().clone();
    let right_budget = right_prefix.originating_budget().clone();
    let identity = projection_identity();
    let left_capture = left_catalog
        .capture_projection_source_admitted_v1(&identity, &left_budget)
        .unwrap();
    let right_capture = right_catalog
        .capture_projection_source_admitted_v1(&identity, &right_budget)
        .unwrap();
    let left_plan = SourceDriverPlan::new(&left_capture);
    let right_plan = SourceDriverPlan::new(&right_capture);
    let mut left_work =
        SourceEvaluationWorkOwner::new(left_prefix, Arc::new(AtomicBool::new(false)));
    let mut right_work =
        SourceEvaluationWorkOwner::new(right_prefix, Arc::new(AtomicBool::new(false)));
    let directory = crate::test_path::ScopedDirectory::new("source-driver-crossed");
    let left_parent = directory.join("left");
    let right_parent = directory.join("right");
    std::fs::create_dir(&left_parent).unwrap();
    std::fs::create_dir(&right_parent).unwrap();
    let mut a =
        NativeSourceCheckpointBackend::capture(&left, &mut left_work, &left_plan, &left_parent)
            .unwrap();
    let mut b =
        NativeSourceCheckpointBackend::capture(&right, &mut right_work, &right_plan, &right_parent)
            .unwrap();
    let foreign_before = right_budget.observation().unwrap();
    a.swap_input_for_test(&mut b);
    let mut coordinator = RefuseAfterVerification::new(None);
    assert!(matches!(
        drive_source_checkpoint(&mut a, &mut coordinator),
        Err(SourceDriverError::ForeignCustody)
    ));
    assert_eq!(coordinator.calls, 0);
    assert_eq!(
        right_budget.observation().unwrap(),
        foreign_before,
        "foreign graph was never admitted or read"
    );
    assert!(!left_parent.join("source-staging-v1").exists());
}

// req: DEP-001, DEP-006, PRJ-004, STO-020, REC-003
#[test]
fn native_source_adapter_keeps_projection_semantics_in_projection() {
    let adapter = include_str!("../source_checkpoint_adapter.rs");
    let staging = include_str!("../source_checkpoint_adapter/stage.rs");
    for source in [adapter, staging] {
        for forbidden in [
            "evaluate_projection_commit",
            "source_evaluator",
            "riffdb_projection_ir",
            "riffdb_expression_ir",
            "ProjectionStageApply",
            "projection_plan()",
        ] {
            assert!(
                !source.contains(forbidden),
                "native adapter crosses semantic boundary: {forbidden}"
            );
        }
    }
    assert!(!include_str!("../maintenance/source_staging_map.rs").contains("mod evaluate;"));
    assert!(include_str!("../../Cargo.toml").contains("riffdb-projection = { version = \"0.2.2\", path = \"../riffdb-projection\", default-features = false }"));
}

#[path = "native_source_step_evidence_tests.rs"]
mod step_evidence;
