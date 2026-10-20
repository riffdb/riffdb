//! Genuine runtime and native step-evidence/publication proofs for `WP-825`.
use super::*;
use crate::source_checkpoint_adapter::{
    NativeSourceCheckpointBackend,
    intent::{
        NativeVerifiedSourceStep,
        builder::NativeSourceIntentBuilder,
        inspect_physical_source_image, prepare_allocation_intent, prepare_copy_intent,
        publication_recipe::prepare_publication_intent,
        step_evidence::{
            NativePreparedPublicationStep, prepare_publication_step, prepare_step_progress,
        },
    },
    publication::verify_publication_copy,
};
use crate::source_evaluation_work::SourceEvaluationWorkOwner;
use crate::source_keys::{
    encode_source_root_key_into, source_maintenance_key, source_object_key, source_root_key_len,
};
use crate::source_usage_domain::SourceUsageCensus;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use riffdb_catalog::ActiveCatalogSnapshot;
use riffdb_projection::source_driver::{
    SourceCheckpointCandidate, SourceCheckpointContinuation, SourceCheckpointInstruction,
    SourceCheckpointMaintenanceCoordinator, SourceCheckpointNativeMaintenancePort,
    SourceCheckpointOperationKind, SourceDriverError, SourceDriverPlan,
    SourceGatePacingObservation, SourceNativePublicationConfirmation,
    SourceNativePublicationEvidence, SourceNativeStepEvidence, drive_source_checkpoint,
};
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, AuthoritativeTransactionBindingV3,
    ProjectionCheckpointOrdinalAllocatorV1, ProjectionCheckpointOrdinalPositionV1,
    ProjectionSourceLifecycleV1, ProjectionSourceMaintenanceV1, ProjectionSourceObjectV1,
    ProjectionSourceRootV1, StorageErrorKind,
    proto_codec::{
        decode_projection_checkpoint_ordinal_allocator_inline_v1,
        decode_projection_source_maintenance_admitted_v1,
        decode_projection_source_object_admitted_v1, decode_projection_source_root_admitted_v1,
        encode_projection_source_maintenance_admitted_v1,
        encode_projection_source_object_admitted_v1,
    },
};
use riffdb_types::{
    ProjectionCheckpointOrdinal, ProjectionSourceLocatorV1, ProjectionSourceWorkingBudgetV1,
};
use std::sync::{Arc, atomic::AtomicBool};

fn finish_census<'ports>(
    mut census: SourceUsageCensus<'ports>,
) -> crate::source_usage_domain::SourceUsageReconstruction<'ports> {
    for _ in 0..64 {
        let step = census.step().unwrap();
        assert!(step.rows <= 128 && step.bytes <= 4 * 1024 * 1024);
        if step.done {
            return census.finish().unwrap();
        }
    }
    panic!("bounded SourceUsageCensus must complete within finite steps");
}

fn immediate_fixture(
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
    native_source_command_fixture::append_runtime_immediate(&ports, 1).unwrap();
    native_source_command_fixture::append_runtime_immediate(&ports, 2).unwrap();
    (root, ports)
}

fn authorize_step<'candidate, 'stage, 'slot, 'session, B>(
    mut instruction: SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>,
) -> SourceCheckpointInstruction<'candidate, 'stage, 'slot, 'session, B>
where
    B: riffdb_projection::source_driver::SourceCheckpointDriverBackend + 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    if let Some((allocator, database, incarnation)) = instruction.observed_ordinal() {
        let assignment = allocator.allocate_one(database, incarnation).unwrap();
        instruction = instruction.authorize_ordinal(assignment).unwrap();
    }
    if let Some(history) = instruction.observed_transaction_history() {
        let sequence = history.expected_allocator().allocate_one().unwrap().0;
        let assigned = AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence,
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: history.tail().frontier(),
            prior_history_hash: history.tail().history_hash(),
        };
        instruction = instruction.authorize_transaction_binding(assigned).unwrap();
    }
    instruction
}

fn read_stored_allocator(
    ports: &crate::RedbOperationalPorts,
) -> ProjectionCheckpointOrdinalAllocatorV1 {
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(crate::layout::META).unwrap();
    let key = N::ProjectionSourceAllocator.metadata_key().unwrap();
    let value = meta.get(key).unwrap().unwrap();
    *decode_projection_checkpoint_ordinal_allocator_inline_v1(value.value())
        .unwrap()
        .value()
}

fn table_entry_count(ports: &crate::RedbOperationalPorts, namespace: N) -> usize {
    let read = ports.shared.database.begin_read().unwrap();
    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
    let table = read.open_table(definition).unwrap();
    table.iter().unwrap().count()
}

fn read_stored_maintenance(
    ports: &crate::RedbOperationalPorts,
    locator: ProjectionSourceLocatorV1,
) -> Option<ProjectionSourceMaintenanceV1> {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let read = ports.shared.database.begin_read().unwrap();
    let definition: TableDefinition<&[u8], &[u8]> =
        TableDefinition::new(N::ProjectionSourceMaintenance.table());
    let table = read.open_table(definition).unwrap();
    let key = source_maintenance_key(locator);
    table.get(key.as_slice()).unwrap().map(|val| {
        (**decode_projection_source_maintenance_admitted_v1(val.value(), &budget)
            .unwrap()
            .value())
        .clone()
    })
}

fn read_stored_object(
    ports: &crate::RedbOperationalPorts,
    locator: ProjectionSourceLocatorV1,
) -> Option<ProjectionSourceObjectV1> {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let read = ports.shared.database.begin_read().unwrap();
    let definition: TableDefinition<&[u8], &[u8]> =
        TableDefinition::new(N::ProjectionSourceObjects.table());
    let table = read.open_table(definition).unwrap();
    let key = source_object_key(locator);
    table.get(key.as_slice()).unwrap().map(|val| {
        (**decode_projection_source_object_admitted_v1(val.value(), &budget)
            .unwrap()
            .value())
        .clone()
    })
}

fn read_stored_root(
    ports: &crate::RedbOperationalPorts,
    identity: &ProjectionIdentity,
    locator: ProjectionSourceLocatorV1,
) -> Option<ProjectionSourceRootV1> {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let read = ports.shared.database.begin_read().unwrap();
    let definition: TableDefinition<&[u8], &[u8]> =
        TableDefinition::new(N::ProjectionSourceRoots.table());
    let table = read.open_table(definition).unwrap();
    let mut key = Vec::with_capacity(source_root_key_len(identity));
    encode_source_root_key_into(&mut key, identity, locator).unwrap();
    table.get(key.as_slice()).unwrap().map(|val| {
        (**decode_projection_source_root_admitted_v1(val.value(), &budget)
            .unwrap()
            .value())
        .clone()
    })
}

// req: DEP-006, PRJ-004, STO-020, REC-003
#[test]
fn native_prepared_progress_owns_fresh_postimage_without_adopting_catalog_identity() {
    let (_root, ports) = immediate_fixture("step-evidence-fresh-postimage");
    let (_foreign_root, foreign_ports) = immediate_fixture("step-evidence-fresh-postimage-foreign");
    let foreign_budget = foreign_ports.shared.database.source_working_budget();

    let completion = finish_census(SourceUsageCensus::start(&ports).unwrap());
    let initial_usage = completion.usage();
    let prefix = ports
        .capture_original_prefix_from_usage(completion)
        .unwrap();
    let budget = prefix.originating_budget().clone();
    let catalog = ActiveCatalogSnapshot::read(&ports).unwrap().unwrap();
    let capture = catalog
        .capture_projection_source_admitted_v1(&projection_identity(), &budget)
        .unwrap();
    let plan = SourceDriverPlan::new(&capture);
    let mut work = SourceEvaluationWorkOwner::new(prefix, Arc::new(AtomicBool::new(false)));
    let directory = crate::test_path::ScopedDirectory::new("step-evidence-fresh-postimage-stage");
    let parent = directory.join("parent");
    std::fs::create_dir(&parent).unwrap();
    let pre_adapter_baseline = budget.observation().unwrap();
    let mut adapter =
        NativeSourceCheckpointBackend::capture(&ports, &mut work, &plan, &parent).unwrap();

    struct InspectPreparedAllocate<'a, 'work> {
        budget: &'a riffdb_types::ProjectionSourceWorkingBudgetV1,
        foreign_budget: &'a riffdb_types::ProjectionSourceWorkingBudgetV1,
        catalog_lineage_ptr: *const u8,
        inspected: bool,
        _backend: PhantomData<fn() -> NativeSourceCheckpointBackend<'work>>,
    }
    impl<'work> SourceCheckpointMaintenanceCoordinator for InspectPreparedAllocate<'_, 'work> {
        type Backend = NativeSourceCheckpointBackend<'work>;
        fn maintain<'candidate, 'stage, 'slot, 'session>(
            &mut self,
            candidate: SourceCheckpointCandidate<
                'candidate,
                'stage,
                'slot,
                'session,
                Self::Backend,
            >,
            native: &mut <Self::Backend as riffdb_projection::source_driver::SourceCheckpointDriverBackend>::MaintenancePort<'session>,
        ) -> Result<SourceNativePublicationConfirmation<'session, Self::Backend>, SourceDriverError>
        where
            Self::Backend: 'session,
            'session: 'slot,
            'slot: 'stage,
            'stage: 'candidate,
        {
            let instruction = candidate.into_prepare_instruction()?;
            let response = native.apply(instruction)?;
            if let Some(obs) = response.pacing_observation() {
                native.await_maintenance_turn(obs)?;
            }
            let SourceCheckpointContinuation::Instruction(instruction) = response.prepare_next()?
            else {
                panic!("PrepareAllocation must yield Allocate instruction");
            };
            assert_eq!(
                instruction.operation(),
                SourceCheckpointOperationKind::Allocate
            );
            let before_authorize = self.budget.observation().unwrap();
            let (allocator, database, incarnation) = instruction.observed_ordinal().unwrap();
            let assignment = allocator.allocate_one(database, incarnation).unwrap();
            let instruction = instruction.authorize_ordinal(assignment)?;

            // Inspect inside Native preparation after canonical Allocate intent and
            // prepare_step_progress, before usage.begin.
            let checked = native.checked_request_for_test(&instruction)?;
            let entry_baseline = self.budget.observation().unwrap();
            let intent = prepare_allocation_intent(&checked)?;
            let prepared_progress = prepare_step_progress(&checked, &intent)?;
            let allocated = prepared_progress.allocated_progress_for_test().unwrap();
            let expected_after = instruction.after_progress().unwrap();
            assert_eq!(&**allocated, expected_after);
            assert!(allocated.belongs_to(self.budget));
            assert!(!allocated.belongs_to(self.foreign_budget));
            assert!(intent.belongs_to(self.budget));
            assert!(!intent.belongs_to(self.foreign_budget));

            // Fresh semantic graph: neither the Maintenance record nor its inner
            // ContractLineage allocation aliases the candidate or catalog identity.
            assert!(!std::ptr::eq(&**allocated, expected_after));
            let fresh_lineage_ptr = allocated
                .descriptor()
                .reference()
                .identity()
                .contract_lineage()
                .as_str()
                .as_ptr();
            let candidate_lineage_ptr = expected_after
                .descriptor()
                .reference()
                .identity()
                .contract_lineage()
                .as_str()
                .as_ptr();
            assert_ne!(fresh_lineage_ptr, candidate_lineage_ptr);
            assert_ne!(fresh_lineage_ptr, self.catalog_lineage_ptr);

            let live_during_probe = self.budget.observation().unwrap();
            assert!(live_during_probe.owned_bytes > entry_baseline.owned_bytes);
            assert!(live_during_probe.reserved_bytes > entry_baseline.reserved_bytes);

            // Drop prepared progress and temporary intent owners while keeping
            // original catalog snapshot and candidate identity alive.
            drop(prepared_progress);
            drop(intent);
            let after_probe_drop = self.budget.observation().unwrap();
            assert_eq!(
                after_probe_drop.reserved_bytes,
                entry_baseline.reserved_bytes
            );
            assert_eq!(after_probe_drop.owned_bytes, entry_baseline.owned_bytes);
            assert!(after_probe_drop.released_allocations > entry_baseline.released_allocations);

            // Also verify the prepared step attached by authorize_ordinal returns
            // live accounting to before_authorize when dropped while candidate is alive.
            let attached_alloc = instruction
                .prepared_step()
                .unwrap()
                .progress_for_test()
                .unwrap();
            assert_eq!(&**attached_alloc, expected_after);
            assert!(attached_alloc.belongs_to(self.budget));
            drop(checked);
            let instruction = authorize_step(instruction);
            let stage_brand = instruction.native_stage().brand;
            let (Some(prepared_step), _continuation) =
                instruction.into_native_operation(native.brand_for_test(), stage_brand)?
            else {
                unreachable!()
            };
            drop(prepared_step);
            let after_step_drop = self.budget.observation().unwrap();
            assert_eq!(
                after_step_drop.reserved_bytes,
                before_authorize.reserved_bytes
            );
            assert_eq!(after_step_drop.owned_bytes, before_authorize.owned_bytes);
            assert!(after_step_drop.released_allocations > before_authorize.released_allocations);

            self.inspected = true;
            // Stop with typed preparation refusal before usage.begin / native apply.
            Err(SourceDriverError::SourceUnavailable)
        }
    }

    let catalog_lineage_ptr = plan.identity().contract_lineage().as_str().as_ptr();
    let mut coordinator = InspectPreparedAllocate {
        budget: &budget,
        foreign_budget,
        catalog_lineage_ptr,
        inspected: false,
        _backend: PhantomData,
    };
    assert!(matches!(
        drive_source_checkpoint(&mut adapter, &mut coordinator),
        Err(SourceDriverError::SourceUnavailable)
    ));
    assert!(coordinator.inspected);
    drop(adapter);

    let post_adapter = budget.observation().unwrap();
    assert_eq!(
        post_adapter.reserved_bytes,
        pre_adapter_baseline.reserved_bytes
    );
    assert_eq!(post_adapter.owned_bytes, pre_adapter_baseline.owned_bytes);
    assert!(post_adapter.released_allocations > pre_adapter_baseline.released_allocations);

    // Stored allocator, roots, objects, maintenance, rows and usage domain remain unchanged.
    assert_eq!(
        read_stored_allocator(&ports).position(),
        ProjectionCheckpointOrdinalPositionV1::Next(ProjectionCheckpointOrdinal::first())
    );
    assert_eq!(table_entry_count(&ports, N::ProjectionSourceRoots), 0);
    assert_eq!(table_entry_count(&ports, N::ProjectionSourceObjects), 0);
    assert_eq!(table_entry_count(&ports, N::ProjectionSourceMaintenance), 0);
    assert_eq!(table_entry_count(&ports, N::ProjectionSourceRows), 0);
    assert_eq!(
        ports
            .shared
            .database
            .source_usage_domain()
            .complete_observation()
            .unwrap(),
        initial_usage
    );
}

// req: DEP-006, PRJ-004, STO-020, REC-003
#[test]
fn native_step_progress_rejects_foreign_intent_and_unequal_postimage() {
    let (_left_root, left_ports) = immediate_fixture("step-evidence-reject-left");
    let (_right_root, right_ports) = immediate_fixture("step-evidence-reject-right");

    let left_completion = finish_census(SourceUsageCensus::start(&left_ports).unwrap());
    let right_completion = finish_census(SourceUsageCensus::start(&right_ports).unwrap());
    let left_prefix = left_ports
        .capture_original_prefix_from_usage(left_completion)
        .unwrap();
    let right_prefix = right_ports
        .capture_original_prefix_from_usage(right_completion)
        .unwrap();
    assert_eq!(
        left_prefix.captured_history(),
        right_prefix.captured_history()
    );
    assert_eq!(
        left_prefix.tentative_source_locator(),
        right_prefix.tentative_source_locator()
    );

    let left_budget = left_prefix.originating_budget().clone();
    let right_budget = right_prefix.originating_budget().clone();
    let identity = projection_identity();
    let left_catalog = ActiveCatalogSnapshot::read(&left_ports).unwrap().unwrap();
    let right_catalog = ActiveCatalogSnapshot::read(&right_ports).unwrap().unwrap();
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
    let directory = crate::test_path::ScopedDirectory::new("step-evidence-reject-stages");
    let left_parent = directory.join("left");
    let right_parent = directory.join("right");
    std::fs::create_dir(&left_parent).unwrap();
    std::fs::create_dir(&right_parent).unwrap();

    // First extract a genuine Allocate intent from right_ports (attached to right_budget,
    // with identical durable IDs and canonical postimage bytes).
    struct ExtractAllocateIntent<'work> {
        intent: Option<crate::source_checkpoint_adapter::NativeSourceStepIntent>,
        _backend: PhantomData<fn() -> NativeSourceCheckpointBackend<'work>>,
    }
    impl<'work> SourceCheckpointMaintenanceCoordinator for ExtractAllocateIntent<'work> {
        type Backend = NativeSourceCheckpointBackend<'work>;
        fn maintain<'candidate, 'stage, 'slot, 'session>(
            &mut self,
            candidate: SourceCheckpointCandidate<
                'candidate,
                'stage,
                'slot,
                'session,
                Self::Backend,
            >,
            native: &mut <Self::Backend as riffdb_projection::source_driver::SourceCheckpointDriverBackend>::MaintenancePort<'session>,
        ) -> Result<SourceNativePublicationConfirmation<'session, Self::Backend>, SourceDriverError>
        where
            Self::Backend: 'session,
            'session: 'slot,
            'slot: 'stage,
            'stage: 'candidate,
        {
            let instruction = candidate.into_prepare_instruction()?;
            let response = native.apply(instruction)?;
            if let Some(obs) = response.pacing_observation() {
                native.await_maintenance_turn(obs)?;
            }
            let SourceCheckpointContinuation::Instruction(instruction) = response.prepare_next()?
            else {
                unreachable!()
            };
            let (allocator, database, incarnation) = instruction.observed_ordinal().unwrap();
            let assignment = allocator.allocate_one(database, incarnation).unwrap();
            let instruction = instruction.authorize_ordinal(assignment)?;
            let checked = native.checked_request_for_test(&instruction)?;
            self.intent = Some(prepare_allocation_intent(&checked)?);
            Err(SourceDriverError::SourceUnavailable)
        }
    }

    let mut right_adapter = NativeSourceCheckpointBackend::capture(
        &right_ports,
        &mut right_work,
        &right_plan,
        &right_parent,
    )
    .unwrap();
    let mut right_extractor = ExtractAllocateIntent {
        intent: None,
        _backend: PhantomData,
    };
    assert!(matches!(
        drive_source_checkpoint(&mut right_adapter, &mut right_extractor),
        Err(SourceDriverError::SourceUnavailable)
    ));
    drop(right_adapter);
    let foreign_intent = right_extractor.intent.take().unwrap();
    assert!(foreign_intent.belongs_to(&right_budget));
    assert!(!foreign_intent.belongs_to(&left_budget));

    struct RejectMalformedProgress<'a, 'work> {
        left_budget: &'a riffdb_types::ProjectionSourceWorkingBudgetV1,
        right_budget: &'a riffdb_types::ProjectionSourceWorkingBudgetV1,
        foreign_intent: &'a crate::source_checkpoint_adapter::NativeSourceStepIntent,
        verified_all: bool,
        _backend: PhantomData<fn() -> NativeSourceCheckpointBackend<'work>>,
    }
    impl<'work> SourceCheckpointMaintenanceCoordinator for RejectMalformedProgress<'_, 'work> {
        type Backend = NativeSourceCheckpointBackend<'work>;
        fn maintain<'candidate, 'stage, 'slot, 'session>(
            &mut self,
            candidate: SourceCheckpointCandidate<
                'candidate,
                'stage,
                'slot,
                'session,
                Self::Backend,
            >,
            native: &mut <Self::Backend as riffdb_projection::source_driver::SourceCheckpointDriverBackend>::MaintenancePort<'session>,
        ) -> Result<SourceNativePublicationConfirmation<'session, Self::Backend>, SourceDriverError>
        where
            Self::Backend: 'session,
            'session: 'slot,
            'slot: 'stage,
            'stage: 'candidate,
        {
            let instruction = candidate.into_prepare_instruction()?;
            let response = native.apply(instruction)?;
            if let Some(obs) = response.pacing_observation() {
                native.await_maintenance_turn(obs)?;
            }
            let SourceCheckpointContinuation::Instruction(instruction) = response.prepare_next()?
            else {
                unreachable!()
            };
            let (allocator, database, incarnation) = instruction.observed_ordinal().unwrap();
            let assignment = allocator.allocate_one(database, incarnation).unwrap();
            let instruction = instruction.authorize_ordinal(assignment)?;
            let checked = native.checked_request_for_test(&instruction)?;
            let genuine_intent = prepare_allocation_intent(&checked)?;
            assert_eq!(
                genuine_intent.canonical_receipt(),
                self.foreign_intent.canonical_receipt(),
                "equal durable IDs produce identical canonical receipt bytes"
            );

            let baseline_left = self.left_budget.observation().unwrap();
            let baseline_right = self.right_budget.observation().unwrap();

            // 1. Equal durable IDs with an intent attached to another engine ledger
            // refuse before decode/retention.
            let foreign_err = prepare_step_progress(&checked, self.foreign_intent)
                .err()
                .unwrap();
            assert_eq!(foreign_err.kind(), StorageErrorKind::CorruptData);
            assert_eq!(self.left_budget.observation().unwrap(), baseline_left);
            assert_eq!(self.right_budget.observation().unwrap(), baseline_right);

            // Prepare structural malformed proposals on left_budget using the genuine checked request.
            let locator = checked.captured_locator();
            let other_locator = ProjectionSourceLocatorV1::new(
                locator.database_id(),
                locator.allocation_history_incarnation(),
                ProjectionCheckpointOrdinal::new(2).unwrap(),
            )
            .unwrap();
            let object_key = source_object_key(locator);
            let maintenance_key = source_maintenance_key(locator);
            let wrong_maintenance_key = source_maintenance_key(other_locator);
            let descriptor = checked.descriptor().unwrap();
            let expected_after = checked.after_progress().unwrap();

            // 2a. Absent Maintenance postimage refuses.
            {
                let clone_work = self
                    .left_budget
                    .reserve(descriptor.clone_work_bound_v1().unwrap())
                    .unwrap();
                let object = ProjectionSourceObjectV1::new(
                    descriptor.clone(),
                    ProjectionSourceLifecycleV1::Copying,
                );
                let object_bytes =
                    encode_projection_source_object_admitted_v1(&object, self.left_budget).unwrap();
                drop(object);
                drop(clone_work);
                let mut builder =
                    NativeSourceIntentBuilder::new(&checked, checked.captured_history()).unwrap();
                builder
                    .put(
                        N::ProjectionSourceObjects,
                        &object_key,
                        None,
                        object_bytes.as_bytes(),
                    )
                    .unwrap();
                let absent_intent = builder.finish().unwrap();
                drop(object_bytes);
                assert_eq!(
                    prepare_step_progress(&checked, &absent_intent)
                        .err()
                        .unwrap()
                        .kind(),
                    StorageErrorKind::CorruptData
                );
            }

            // 2b. Removal Maintenance postimage refuses.
            {
                let maintenance_bytes = encode_projection_source_maintenance_admitted_v1(
                    expected_after,
                    self.left_budget,
                )
                .unwrap();
                let mut builder =
                    NativeSourceIntentBuilder::new(&checked, checked.captured_history()).unwrap();
                builder
                    .remove(
                        N::ProjectionSourceMaintenance,
                        &maintenance_key,
                        maintenance_bytes.as_bytes(),
                    )
                    .unwrap();
                let removal_intent = builder.finish().unwrap();
                drop(maintenance_bytes);
                assert_eq!(
                    prepare_step_progress(&checked, &removal_intent)
                        .err()
                        .unwrap()
                        .kind(),
                    StorageErrorKind::CorruptData
                );
            }

            // 2c. Wrong-key Maintenance postimage refuses.
            {
                let maintenance_bytes = encode_projection_source_maintenance_admitted_v1(
                    expected_after,
                    self.left_budget,
                )
                .unwrap();
                let mut builder =
                    NativeSourceIntentBuilder::new(&checked, checked.captured_history()).unwrap();
                builder
                    .put(
                        N::ProjectionSourceMaintenance,
                        &wrong_maintenance_key,
                        None,
                        maintenance_bytes.as_bytes(),
                    )
                    .unwrap();
                let wrong_key_intent = builder.finish().unwrap();
                drop(maintenance_bytes);
                assert_eq!(
                    prepare_step_progress(&checked, &wrong_key_intent)
                        .err()
                        .unwrap()
                        .kind(),
                    StorageErrorKind::CorruptData
                );
            }

            // 2d. Duplicate Maintenance postimage refuses.
            {
                let maintenance_bytes = encode_projection_source_maintenance_admitted_v1(
                    expected_after,
                    self.left_budget,
                )
                .unwrap();
                let mut builder =
                    NativeSourceIntentBuilder::new(&checked, checked.captured_history()).unwrap();
                builder
                    .put(
                        N::ProjectionSourceMaintenance,
                        &maintenance_key,
                        None,
                        maintenance_bytes.as_bytes(),
                    )
                    .unwrap();
                builder
                    .put(
                        N::ProjectionSourceMaintenance,
                        &wrong_maintenance_key,
                        None,
                        maintenance_bytes.as_bytes(),
                    )
                    .unwrap();
                let duplicate_intent = builder.finish().unwrap();
                drop(maintenance_bytes);
                assert_eq!(
                    prepare_step_progress(&checked, &duplicate_intent)
                        .err()
                        .unwrap()
                        .kind(),
                    StorageErrorKind::CorruptData
                );
            }

            // 2e. Same key with unequal complete progress refuses.
            {
                let clone_work = self
                    .left_budget
                    .reserve(descriptor.clone_work_bound_v1().unwrap())
                    .unwrap();
                let unequal_progress = ProjectionSourceMaintenanceV1::from_stored_parts(
                    descriptor.clone(),
                    expected_after.expected_prior_root().cloned(),
                    1,
                    16,
                    Some(vec![0x01]),
                    expected_after.rolling_hash(),
                )
                .unwrap();
                let unequal_bytes = encode_projection_source_maintenance_admitted_v1(
                    &unequal_progress,
                    self.left_budget,
                )
                .unwrap();
                drop(unequal_progress);
                drop(clone_work);
                let mut builder =
                    NativeSourceIntentBuilder::new(&checked, checked.captured_history()).unwrap();
                builder
                    .put(
                        N::ProjectionSourceMaintenance,
                        &maintenance_key,
                        None,
                        unequal_bytes.as_bytes(),
                    )
                    .unwrap();
                let unequal_intent = builder.finish().unwrap();
                drop(unequal_bytes);
                assert_eq!(
                    prepare_step_progress(&checked, &unequal_intent)
                        .err()
                        .unwrap()
                        .kind(),
                    StorageErrorKind::CorruptData
                );
            }

            // 3. Admission refusal leaves all original intent/candidate/catalog owners
            // and live accounting unchanged.
            let after_refusals_left = self.left_budget.observation().unwrap();
            assert_eq!(
                after_refusals_left.reserved_bytes,
                baseline_left.reserved_bytes
            );
            assert_eq!(after_refusals_left.owned_bytes, baseline_left.owned_bytes);
            assert_eq!(self.right_budget.observation().unwrap(), baseline_right);

            let valid_progress = prepare_step_progress(&checked, &genuine_intent).unwrap();
            assert_eq!(
                &**valid_progress.allocated_progress_for_test().unwrap(),
                expected_after
            );
            drop(valid_progress);
            drop(genuine_intent);

            self.verified_all = true;
            Err(SourceDriverError::SourceUnavailable)
        }
    }

    let mut left_adapter = NativeSourceCheckpointBackend::capture(
        &left_ports,
        &mut left_work,
        &left_plan,
        &left_parent,
    )
    .unwrap();
    let mut reject_coordinator = RejectMalformedProgress {
        left_budget: &left_budget,
        right_budget: &right_budget,
        foreign_intent: &foreign_intent,
        verified_all: false,
        _backend: PhantomData,
    };
    assert!(matches!(
        drive_source_checkpoint(&mut left_adapter, &mut reject_coordinator),
        Err(SourceDriverError::SourceUnavailable)
    ));
    assert!(reject_coordinator.verified_all);
    drop(left_adapter);
    drop(foreign_intent);
}

// req: DEP-006, PRJ-004, STO-020, REC-003
#[test]
fn native_step_evidence_requires_actual_same_intent_exact_after() {
    let (_left_root, left_ports) = immediate_fixture("step-evidence-exact-after-left");
    let (_right_root, right_ports) = immediate_fixture("step-evidence-exact-after-right");

    let left_completion = finish_census(SourceUsageCensus::start(&left_ports).unwrap());
    let right_completion = finish_census(SourceUsageCensus::start(&right_ports).unwrap());
    let left_prefix = left_ports
        .capture_original_prefix_from_usage(left_completion)
        .unwrap();
    let right_prefix = right_ports
        .capture_original_prefix_from_usage(right_completion)
        .unwrap();

    let left_budget = left_prefix.originating_budget().clone();
    let right_budget = right_prefix.originating_budget().clone();
    let identity = projection_identity();
    let left_catalog = ActiveCatalogSnapshot::read(&left_ports).unwrap().unwrap();
    let right_catalog = ActiveCatalogSnapshot::read(&right_ports).unwrap().unwrap();
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
    let directory = crate::test_path::ScopedDirectory::new("step-evidence-exact-after-stages");
    let left_parent = directory.join("left");
    let right_parent = directory.join("right");
    std::fs::create_dir(&left_parent).unwrap();
    std::fs::create_dir(&right_parent).unwrap();

    // Drive right_ports through Allocate to obtain a genuine foreign ExactAfter
    // with equal durable IDs.
    struct ApplyRightAllocate<'a, 'work> {
        right_ports: &'a crate::RedbOperationalPorts,
        left_allocate_progress: Option<
            crate::source_checkpoint_adapter::intent::step_evidence::NativePreparedStepProgress<'a>,
        >,
        rejected_foreign: bool,
        _backend: PhantomData<fn() -> NativeSourceCheckpointBackend<'work>>,
    }
    impl<'work> SourceCheckpointMaintenanceCoordinator for ApplyRightAllocate<'_, 'work> {
        type Backend = NativeSourceCheckpointBackend<'work>;
        fn maintain<'candidate, 'stage, 'slot, 'session>(
            &mut self,
            candidate: SourceCheckpointCandidate<
                'candidate,
                'stage,
                'slot,
                'session,
                Self::Backend,
            >,
            native: &mut <Self::Backend as riffdb_projection::source_driver::SourceCheckpointDriverBackend>::MaintenancePort<'session>,
        ) -> Result<SourceNativePublicationConfirmation<'session, Self::Backend>, SourceDriverError>
        where
            Self::Backend: 'session,
            'session: 'slot,
            'slot: 'stage,
            'stage: 'candidate,
        {
            let instruction = candidate.into_prepare_instruction()?;
            let response = native.apply(instruction)?;
            if let Some(obs) = response.pacing_observation() {
                native.await_maintenance_turn(obs)?;
            }
            let SourceCheckpointContinuation::Instruction(instruction) = response.prepare_next()?
            else {
                unreachable!()
            };
            let instruction = authorize_step(instruction);
            let stage_brand = instruction.native_stage().brand;
            let (Some(prepared), continuation) =
                instruction.into_native_operation(native.brand_for_test(), stage_brand)?
            else {
                unreachable!()
            };
            let checked = native.checked_request_for_test(continuation.instruction())?;
            let (intent, usage, controls, Some(right_progress), None) =
                prepared.into_parts_for_test()
            else {
                unreachable!()
            };
            let lease = self
                .right_ports
                .shared
                .mutation_gate
                .acquire_source()?
                .unwrap();
            let Ok(preinstalled) = controls.preinstall(&checked, &lease) else {
                panic!("right Allocate preinstall must succeed");
            };
            let mutation = usage.begin_under_source_lease(self.right_ports, &lease)?;
            assert!(preinstalled.apply(&mutation).is_ok());
            drop(lease);
            drop(checked);
            let observed = inspect_physical_source_image(self.right_ports, &mutation, &intent)?;
            let reconcile_lease = self
                .right_ports
                .shared
                .mutation_gate
                .acquire_source()?
                .unwrap();
            let right_verified_after = observed.into_verified(&reconcile_lease)?;
            assert!(!right_verified_after.is_exact_before());

            // Left's prepared progress must reject right's genuinely applied ExactAfter.
            let left_progress = self.left_allocate_progress.take().unwrap();
            assert_eq!(
                left_progress
                    .confirm(&right_verified_after)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            let right_applied = right_progress.confirm(&right_verified_after)?;
            assert_eq!(
                right_applied.operation(),
                SourceCheckpointOperationKind::Allocate
            );
            self.right_ports
                .shared
                .database
                .source_usage_domain()
                .reconcile_native_step(self.right_ports, &reconcile_lease, right_verified_after)?;
            drop(reconcile_lease);
            drop(mutation);
            self.rejected_foreign = true;
            Err(SourceDriverError::SourceUnavailable)
        }
    }

    struct DriveAllocateAndCopy<'a, 'work> {
        left_ports: &'a crate::RedbOperationalPorts,
        right_ports: &'a crate::RedbOperationalPorts,
        right_work: &'a mut SourceEvaluationWorkOwner,
        right_plan: &'a SourceDriverPlan<'a>,
        right_parent: &'a std::path::Path,
        completed_allocate_and_copy: bool,
        _backend: PhantomData<fn() -> NativeSourceCheckpointBackend<'work>>,
    }
    impl<'work> SourceCheckpointMaintenanceCoordinator for DriveAllocateAndCopy<'_, 'work> {
        type Backend = NativeSourceCheckpointBackend<'work>;
        fn maintain<'candidate, 'stage, 'slot, 'session>(
            &mut self,
            candidate: SourceCheckpointCandidate<
                'candidate,
                'stage,
                'slot,
                'session,
                Self::Backend,
            >,
            native: &mut <Self::Backend as riffdb_projection::source_driver::SourceCheckpointDriverBackend>::MaintenancePort<'session>,
        ) -> Result<SourceNativePublicationConfirmation<'session, Self::Backend>, SourceDriverError>
        where
            Self::Backend: 'session,
            'session: 'slot,
            'slot: 'stage,
            'stage: 'candidate,
        {
            // 1. PrepareAllocation
            let instruction = candidate.into_prepare_instruction()?;
            let response = native.apply(instruction)?;
            if let Some(obs) = response.pacing_observation() {
                native.await_maintenance_turn(obs)?;
            }
            let SourceCheckpointContinuation::Instruction(instruction) = response.prepare_next()?
            else {
                unreachable!()
            };
            assert_eq!(
                instruction.operation(),
                SourceCheckpointOperationKind::Allocate
            );

            // 2. Allocate: inspect ExactBefore, foreign engine ExactAfter, distinct intent allocation,
            // and actual same-intent ExactAfter.
            let (allocator, database, incarnation) = instruction.observed_ordinal().unwrap();
            let assignment = allocator.allocate_one(database, incarnation).unwrap();
            let instruction = instruction.authorize_ordinal(assignment)?;
            let expected_allocate_progress = instruction.after_progress().unwrap().clone();
            let locator = instruction.captured_locator();

            // Before physical submit, no AppliedStepEvidence exists and stored tables are empty.
            assert!(read_stored_maintenance(self.left_ports, locator).is_none());
            assert!(read_stored_object(self.left_ports, locator).is_none());

            let checked_for_probes = native.checked_request_for_test(&instruction)?;
            let distinct_allocate_intent = prepare_allocation_intent(&checked_for_probes)?;
            let distinct_allocate_progress =
                prepare_step_progress(&checked_for_probes, &distinct_allocate_intent)?;
            let foreign_probe_progress =
                prepare_step_progress(&checked_for_probes, &distinct_allocate_intent)?;
            drop(checked_for_probes);

            // Test against another genuinely applied engine (right_ports) with equal durable IDs.
            {
                let mut right_adapter = NativeSourceCheckpointBackend::capture(
                    self.right_ports,
                    self.right_work,
                    self.right_plan,
                    self.right_parent,
                )
                .unwrap();
                let mut right_coord = ApplyRightAllocate {
                    right_ports: self.right_ports,
                    left_allocate_progress: Some(foreign_probe_progress),
                    rejected_foreign: false,
                    _backend: PhantomData,
                };
                assert!(matches!(
                    drive_source_checkpoint(&mut right_adapter, &mut right_coord),
                    Err(SourceDriverError::SourceUnavailable)
                ));
                assert!(right_coord.rejected_foreign);
            }

            let instruction = authorize_step(instruction);
            let stage_brand = instruction.native_stage().brand;
            let (Some(prepared), continuation) =
                instruction.into_native_operation(native.brand_for_test(), stage_brand)?
            else {
                unreachable!()
            };
            let checked = native.checked_request_for_test(continuation.instruction())?;
            let (allocate_intent, usage, controls, Some(allocate_progress), None) =
                prepared.into_parts_for_test()
            else {
                unreachable!()
            };
            let same_intent_before_probe = prepare_step_progress(&checked, &allocate_intent)?;
            let same_intent_cross_step_probe = prepare_step_progress(&checked, &allocate_intent)?;

            let lease = self
                .left_ports
                .shared
                .mutation_gate
                .acquire_source()?
                .unwrap();
            let Ok(preinstalled) = controls.preinstall(&checked, &lease) else {
                panic!("left Allocate preinstall must succeed");
            };
            let mutation = usage.begin_under_source_lease(self.left_ports, &lease)?;

            // Before physical submit: actual physical+logical read-back classifies as ExactBefore,
            // and confirmation must refuse ExactBefore even with the identical intent and ports.
            let observed_before =
                inspect_physical_source_image(self.left_ports, &mutation, &allocate_intent)?;
            let verified_before: NativeVerifiedSourceStep<'_> =
                observed_before.into_verified(&lease)?;
            assert!(verified_before.is_exact_before());
            assert_eq!(
                same_intent_before_probe
                    .confirm(&verified_before)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            drop(verified_before);

            // Physical + logical submit of Allocate.
            assert!(preinstalled.apply(&mutation).is_ok());
            drop(lease);
            drop(checked);

            let observed_after =
                inspect_physical_source_image(self.left_ports, &mutation, &allocate_intent)?;
            let reconcile_lease = self
                .left_ports
                .shared
                .mutation_gate
                .acquire_source()?
                .unwrap();
            let verified_allocate_after = observed_after.into_verified(&reconcile_lease)?;
            assert!(!verified_allocate_after.is_exact_before());

            // Same ports and equal bytes, but distinct intent allocation, must refuse confirmation.
            assert_eq!(
                distinct_allocate_progress
                    .confirm(&verified_allocate_after)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            drop(distinct_allocate_intent);

            // Actual ExactAfter with same ports and identical admitted intent allocation confirms.
            let applied_allocate = allocate_progress.confirm(&verified_allocate_after)?;
            assert_eq!(
                applied_allocate.operation(),
                SourceCheckpointOperationKind::Allocate
            );
            assert_eq!(
                applied_allocate.progress(),
                Some(&expected_allocate_progress)
            );
            assert_eq!(
                read_stored_maintenance(self.left_ports, locator).as_ref(),
                Some(&expected_allocate_progress)
            );
            let stored_obj = read_stored_object(self.left_ports, locator).unwrap();
            assert_eq!(stored_obj.lifecycle(), ProjectionSourceLifecycleV1::Copying);
            assert_eq!(
                read_stored_allocator(self.left_ports).position(),
                ProjectionCheckpointOrdinalPositionV1::Next(
                    ProjectionCheckpointOrdinal::new(2).unwrap()
                )
            );

            self.left_ports
                .shared
                .database
                .source_usage_domain()
                .reconcile_native_step(
                    self.left_ports,
                    &reconcile_lease,
                    verified_allocate_after,
                )?;
            drop(reconcile_lease);
            drop(mutation);

            let pacing = SourceGatePacingObservation::from_native_gate_cost(1)?;
            let response = continuation.applied(
                native.brand_for_test(),
                stage_brand,
                applied_allocate,
                pacing,
            )?;
            if let Some(obs) = response.pacing_observation() {
                native.await_maintenance_turn(obs)?;
            }
            let SourceCheckpointContinuation::Instruction(instruction) = response.prepare_next()?
            else {
                panic!("Allocate must transition to Copy");
            };
            assert_eq!(instruction.operation(), SourceCheckpointOperationKind::Copy);

            // 3. Copy: verify ExactBefore refusal, cross-step intent refusal, and actual
            // same-intent ExactAfter confirmation.
            let expected_copy_progress = instruction.after_progress().unwrap().clone();
            assert!(expected_copy_progress.copy_complete());
            assert_eq!(expected_copy_progress.copied_rows(), 2);

            let copy_probe_checked = native.checked_request_for_test(&instruction)?;
            let distinct_copy_intent = prepare_copy_intent(&copy_probe_checked)?;
            let distinct_copy_progress =
                prepare_step_progress(&copy_probe_checked, &distinct_copy_intent)?;
            drop(copy_probe_checked);

            let instruction = authorize_step(instruction);
            let stage_brand = instruction.native_stage().brand;
            let (Some(prepared_copy), continuation) =
                instruction.into_native_operation(native.brand_for_test(), stage_brand)?
            else {
                unreachable!()
            };
            let copy_checked = native.checked_request_for_test(continuation.instruction())?;
            let (copy_intent, copy_usage, copy_controls, Some(copy_progress), None) =
                prepared_copy.into_parts_for_test()
            else {
                unreachable!()
            };
            let copy_before_probe = prepare_step_progress(&copy_checked, &copy_intent)?;

            let copy_lease = self
                .left_ports
                .shared
                .mutation_gate
                .acquire_source()?
                .unwrap();
            let Ok(copy_preinstalled) = copy_controls.preinstall(&copy_checked, &copy_lease) else {
                panic!("Copy preinstall must succeed");
            };
            let copy_mutation =
                copy_usage.begin_under_source_lease(self.left_ports, &copy_lease)?;

            // Before physical submit of Copy, read-back is ExactBefore and refuses confirmation.
            let copy_obs_before =
                inspect_physical_source_image(self.left_ports, &copy_mutation, &copy_intent)?;
            let copy_verified_before = copy_obs_before.into_verified(&copy_lease)?;
            assert!(copy_verified_before.is_exact_before());
            assert_eq!(
                copy_before_probe
                    .confirm(&copy_verified_before)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            drop(copy_verified_before);

            assert!(copy_preinstalled.apply(&copy_mutation).is_ok());
            drop(copy_lease);
            drop(copy_checked);

            let copy_obs_after =
                inspect_physical_source_image(self.left_ports, &copy_mutation, &copy_intent)?;
            let copy_reconcile_lease = self
                .left_ports
                .shared
                .mutation_gate
                .acquire_source()?
                .unwrap();
            let copy_verified_after = copy_obs_after.into_verified(&copy_reconcile_lease)?;
            assert!(!copy_verified_after.is_exact_before());

            // Allocate's prepared progress and a distinct Copy intent allocation both fail confirmation.
            assert_eq!(
                same_intent_cross_step_probe
                    .confirm(&copy_verified_after)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            assert_eq!(
                distinct_copy_progress
                    .confirm(&copy_verified_after)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            drop(distinct_copy_intent);

            // Actual ExactAfter with identical Copy intent allocation confirms.
            let applied_copy = copy_progress.confirm(&copy_verified_after)?;
            assert_eq!(
                applied_copy.operation(),
                SourceCheckpointOperationKind::Copy
            );
            assert_eq!(applied_copy.progress(), Some(&expected_copy_progress));
            assert_eq!(
                read_stored_maintenance(self.left_ports, locator).as_ref(),
                Some(&expected_copy_progress)
            );
            assert_eq!(
                table_entry_count(self.left_ports, N::ProjectionSourceRows),
                2
            );

            self.left_ports
                .shared
                .database
                .source_usage_domain()
                .reconcile_native_step(
                    self.left_ports,
                    &copy_reconcile_lease,
                    copy_verified_after,
                )?;
            drop(copy_reconcile_lease);
            drop(copy_mutation);

            self.completed_allocate_and_copy = true;
            Err(SourceDriverError::SourceUnavailable)
        }
    }

    let mut left_adapter = NativeSourceCheckpointBackend::capture(
        &left_ports,
        &mut left_work,
        &left_plan,
        &left_parent,
    )
    .unwrap();
    let mut coordinator = DriveAllocateAndCopy {
        left_ports: &left_ports,
        right_ports: &right_ports,
        right_work: &mut right_work,
        right_plan: &right_plan,
        right_parent: &right_parent,
        completed_allocate_and_copy: false,
        _backend: PhantomData,
    };
    assert!(matches!(
        drive_source_checkpoint(&mut left_adapter, &mut coordinator),
        Err(SourceDriverError::SourceUnavailable)
    ));
    assert!(coordinator.completed_allocate_and_copy);
    drop(left_adapter);
    drop(left_work);

    // Reconciled Allocate + Copy usage matches an independent bounded census, while
    // RetainedOriginalSourceInputs remains held in source_usage_domain because Publish
    // has not yet completed.
    let domain_usage = left_ports
        .shared
        .database
        .source_usage_domain()
        .complete_observation()
        .unwrap();
    let census_usage = finish_census(SourceUsageCensus::start(&left_ports).unwrap()).usage();
    assert_eq!(domain_usage, census_usage);
    assert_eq!(
        left_ports
            .shared
            .database
            .source_prefix_pins()
            .prepare_prune_in_incarnation(1, 1)
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::HistoryPruned
    );
}

#[path = "native_source_publication_step_tests.rs"]
mod publication_step;
