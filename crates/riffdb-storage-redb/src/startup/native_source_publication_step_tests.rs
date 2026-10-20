//! Genuine end-to-end native publication step proofs for `WP-825`.
#![allow(clippy::let_unit_value)]
use super::*;

// req: DEP-006, PRJ-004, STO-020, REC-003
#[test]
fn native_publication_step_requires_same_exact_after_without_synthetic_progress() {
    // Verify EmptyDrop on NativePreparedPublicationStep retains the actual ports borrow
    // through physical intent/control destruction (compile-fail E0505 under dropck).
    assert!(std::mem::needs_drop::<NativePreparedPublicationStep<'_>>());
    let step_evidence_src = include_str!("../source_checkpoint_adapter/intent/step_evidence.rs");
    assert!(step_evidence_src.contains("impl Drop for NativePreparedPublicationStep<'_>"));

    let (_left_root, left_ports) = immediate_fixture("step-evidence-publish-left");
    let (_right_root, right_ports) = immediate_fixture("step-evidence-publish-right");

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
    let directory = crate::test_path::ScopedDirectory::new("step-evidence-publish-stages");
    let left_parent = directory.join("left");
    let right_parent = directory.join("right");
    std::fs::create_dir(&left_parent).unwrap();
    std::fs::create_dir(&right_parent).unwrap();
    let pre_adapter_baseline = left_budget.observation().unwrap();

    // Helper coordinator on right_ports to test foreign ExactAfter rejection against
    // left's prepared Publish step and publication owner.
    struct RejectForeignOnPublish<'a, 'work> {
        right_ports: &'a crate::RedbOperationalPorts,
        left_pub_step: Option<NativePreparedPublicationStep<'a>>,
        left_pub_owner: Option<
            crate::source_checkpoint_adapter::intent::publication_recipe::NativePreparedPublication<
                'a,
            >,
        >,
        rejected: bool,
        _backend: PhantomData<fn() -> NativeSourceCheckpointBackend<'work>>,
    }
    impl<'work> SourceCheckpointMaintenanceCoordinator for RejectForeignOnPublish<'_, 'work> {
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
            // Drive right_ports through PrepareAllocation -> Allocate -> Copy -> Publish
            let mut instruction = candidate.into_prepare_instruction()?;
            for _ in 0..3 {
                instruction = authorize_step(instruction);
                let response = native.apply(instruction)?;
                if let Some(obs) = response.pacing_observation() {
                    native.await_maintenance_turn(obs)?;
                }
                let SourceCheckpointContinuation::Instruction(next) = response.prepare_next()?
                else {
                    unreachable!()
                };
                instruction = next;
            }
            assert_eq!(
                instruction.operation(),
                SourceCheckpointOperationKind::Publish
            );
            let instruction = authorize_step(instruction);
            let stage_brand = instruction.native_stage().brand;
            let (Some(prepared), continuation) =
                instruction.into_native_operation(native.brand_for_test(), stage_brand)?
            else {
                unreachable!()
            };
            let checked = native.checked_request_for_test(continuation.instruction())?;
            let (intent, usage, controls, None, Some(right_publication)) =
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
                panic!("right Publish preinstall must succeed");
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

            // Foreign ports (even on a genuinely applied Publish step with equal durable IDs)
            // must refuse both left's NativePreparedPublicationStep and NativePreparedPublication.
            assert_eq!(
                self.left_pub_step
                    .take()
                    .unwrap()
                    .confirm(&right_verified_after)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            assert!(
                self.left_pub_owner
                    .take()
                    .unwrap()
                    .confirm(&right_verified_after)
                    .is_err()
            );

            let (right_step, _right_pub) = right_publication.confirm(&right_verified_after)?;
            assert_eq!(
                right_step.operation(),
                SourceCheckpointOperationKind::Publish
            );
            assert!(right_step.progress().is_none());
            self.right_ports
                .shared
                .database
                .source_usage_domain()
                .reconcile_native_step(self.right_ports, &reconcile_lease, right_verified_after)?;
            drop(reconcile_lease);
            drop(mutation);
            self.rejected = true;
            Err(SourceDriverError::SourceUnavailable)
        }
    }

    struct VerifyEndToEndPublish<'a, 'work> {
        left_ports: &'a crate::RedbOperationalPorts,
        left_budget: &'a riffdb_types::ProjectionSourceWorkingBudgetV1,
        right_ports: &'a crate::RedbOperationalPorts,
        right_work: &'a mut SourceEvaluationWorkOwner,
        right_plan: &'a SourceDriverPlan<'a>,
        right_parent: &'a std::path::Path,
        catalog_lineage_ptr: *const u8,
        verified_publish: bool,
        _backend: PhantomData<fn() -> NativeSourceCheckpointBackend<'work>>,
    }
    impl<'work> SourceCheckpointMaintenanceCoordinator for VerifyEndToEndPublish<'_, 'work> {
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
            // Run PrepareAllocation, Allocate, and Copy through the genuine native port.
            let mut instruction = candidate.into_prepare_instruction()?;
            for expected_op in [
                SourceCheckpointOperationKind::PrepareAllocation,
                SourceCheckpointOperationKind::Allocate,
                SourceCheckpointOperationKind::Copy,
            ] {
                assert_eq!(instruction.operation(), expected_op);
                instruction = authorize_step(instruction);
                let response = native.apply(instruction)?;
                if let Some(obs) = response.pacing_observation() {
                    native.await_maintenance_turn(obs)?;
                }
                let SourceCheckpointContinuation::Instruction(next) = response.prepare_next()?
                else {
                    unreachable!()
                };
                instruction = next;
            }
            assert_eq!(
                instruction.operation(),
                SourceCheckpointOperationKind::Publish
            );
            assert!(instruction.after_progress().is_none());
            let locator = instruction.captured_locator();

            // 1. Before physical submit of Publish, there is neither applied Publish evidence
            // nor final publication evidence: Root is absent, Object is still Copying, and
            // Maintenance is still present.
            assert!(read_stored_root(self.left_ports, &projection_identity(), locator).is_none());
            assert_eq!(
                read_stored_object(self.left_ports, locator)
                    .unwrap()
                    .lifecycle(),
                ProjectionSourceLifecycleV1::Copying
            );
            assert!(read_stored_maintenance(self.left_ports, locator).is_some());

            // Inspect real Publish preparation path before physical submit.
            let publish_checked_probe = native.checked_request_for_test(&instruction)?;
            let probe_baseline = self.left_budget.observation().unwrap();
            let probe_obs = verify_publication_copy(&publish_checked_probe)?;
            let probe_pub = prepare_publication_intent(&publish_checked_probe, probe_obs)?;
            let probe_intent = probe_pub.intent().clone();

            // Publish has no Maintenance postimage and refuses prepare_step_progress.
            assert_eq!(
                prepare_step_progress(&publish_checked_probe, &probe_intent)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );

            // An otherwise-unused prepared publication-step owner retains its reservation
            // until physical drop.
            let before_unused_step = self.left_budget.observation().unwrap();
            let unused_step = prepare_publication_step(&publish_checked_probe, &probe_intent)?;
            assert!(
                self.left_budget.observation().unwrap().reserved_bytes
                    > before_unused_step.reserved_bytes
            );
            drop(unused_step);
            assert_eq!(
                self.left_budget.observation().unwrap().reserved_bytes,
                before_unused_step.reserved_bytes
            );

            // Prepare foreign-rejection probes on the real Publish preparation path.
            let foreign_step_probe =
                prepare_publication_step(&publish_checked_probe, &probe_intent)?;
            let foreign_obs = verify_publication_copy(&publish_checked_probe)?;
            let foreign_pub_probe =
                prepare_publication_intent(&publish_checked_probe, foreign_obs)?;
            {
                let mut right_adapter = NativeSourceCheckpointBackend::capture(
                    self.right_ports,
                    self.right_work,
                    self.right_plan,
                    self.right_parent,
                )
                .unwrap();
                let mut right_coord = RejectForeignOnPublish {
                    right_ports: self.right_ports,
                    left_pub_step: Some(foreign_step_probe),
                    left_pub_owner: Some(foreign_pub_probe),
                    rejected: false,
                    _backend: PhantomData,
                };
                assert!(matches!(
                    drive_source_checkpoint(&mut right_adapter, &mut right_coord),
                    Err(SourceDriverError::SourceUnavailable)
                ));
                assert!(right_coord.rejected);
            }

            // Drop probe owners while keeping catalog and candidate alive; accounting returns
            // to probe_baseline.
            drop(probe_intent);
            drop(probe_pub);
            drop(publish_checked_probe);
            let after_probe_drop = self.left_budget.observation().unwrap();
            assert_eq!(
                after_probe_drop.reserved_bytes,
                probe_baseline.reserved_bytes
            );
            assert_eq!(after_probe_drop.owned_bytes, probe_baseline.owned_bytes);
            assert!(after_probe_drop.released_allocations > probe_baseline.released_allocations);

            // Now execute the actual Publish step while testing ExactBefore and distinct-intent
            // refusals before confirming with the genuine ExactAfter.
            let checked_before_auth = native.checked_request_for_test(&instruction)?;
            let pub_before_obs = verify_publication_copy(&checked_before_auth)?;
            let pub_before_probe =
                prepare_publication_intent(&checked_before_auth, pub_before_obs)?;
            let distinct_pub_obs = verify_publication_copy(&checked_before_auth)?;
            let distinct_pub_probe =
                prepare_publication_intent(&checked_before_auth, distinct_pub_obs)?;
            let distinct_step_probe =
                prepare_publication_step(&checked_before_auth, distinct_pub_probe.intent())?;
            drop(checked_before_auth);

            let instruction = authorize_step(instruction);
            let stage_brand = instruction.native_stage().brand;
            let (Some(prepared), continuation) =
                instruction.into_native_operation(native.brand_for_test(), stage_brand)?
            else {
                unreachable!()
            };
            let checked = native.checked_request_for_test(continuation.instruction())?;
            let (publish_intent, usage, controls, None, Some(publication)) =
                prepared.into_parts_for_test()
            else {
                unreachable!()
            };

            let step_before_probe = prepare_publication_step(&checked, &publish_intent)?;

            let lease = self
                .left_ports
                .shared
                .mutation_gate
                .acquire_source()?
                .unwrap();
            let Ok(preinstalled) = controls.preinstall(&checked, &lease) else {
                panic!("left Publish preinstall must succeed");
            };
            let mutation = usage.begin_under_source_lease(self.left_ports, &lease)?;

            // Real ExactBefore before physical submit refuses both NativePreparedPublicationStep
            // and NativePreparedPublication.
            let obs_before =
                inspect_physical_source_image(self.left_ports, &mutation, &publish_intent)?;
            let verified_before = obs_before.into_verified(&lease)?;
            assert!(verified_before.is_exact_before());
            assert_eq!(
                step_before_probe
                    .confirm(&verified_before)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            assert!(pub_before_probe.confirm(&verified_before).is_err());
            drop(verified_before);

            // Physical + logical submit of Publish.
            assert!(preinstalled.apply(&mutation).is_ok());
            drop(lease);

            let candidate_descriptor = checked.descriptor().unwrap().clone();
            let candidate_lineage_ptr = checked
                .descriptor()
                .unwrap()
                .reference()
                .identity()
                .contract_lineage()
                .as_str()
                .as_ptr();
            drop(checked);

            let obs_after =
                inspect_physical_source_image(self.left_ports, &mutation, &publish_intent)?;
            let reconcile_lease = self
                .left_ports
                .shared
                .mutation_gate
                .acquire_source()?
                .unwrap();
            let verified_after = obs_after.into_verified(&reconcile_lease)?;
            assert!(!verified_after.is_exact_before());
            assert!(verified_after.ends_input_obligation());

            // Another prepared Publish intent allocation on the same ports refuses both outputs.
            assert_eq!(
                distinct_step_probe
                    .confirm(&verified_after)
                    .err()
                    .unwrap()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            assert!(distinct_pub_probe.confirm(&verified_after).is_err());

            // Same genuine ExactAfter releases Publish operation with progress=None and
            // fresh descriptor/root values matching actual stored Root and Published object.
            let (applied_step, published_evidence) = publication.confirm(&verified_after)?;
            assert_eq!(
                applied_step.operation(),
                SourceCheckpointOperationKind::Publish
            );
            assert!(applied_step.progress().is_none());
            assert_eq!(published_evidence.descriptor(), &candidate_descriptor);
            assert_eq!(
                published_evidence.root().reference(),
                published_evidence.descriptor().reference()
            );
            assert_eq!(
                published_evidence.root().publication_binding(),
                publish_intent.receipt().binding()
            );

            // Fresh publication values reconstructed ContractLineage/ProjectionIdentity/Descriptor
            // without adopting catalog or candidate identity allocations.
            let published_lineage_ptr = published_evidence
                .descriptor()
                .reference()
                .identity()
                .contract_lineage()
                .as_str()
                .as_ptr();
            assert_ne!(published_lineage_ptr, candidate_lineage_ptr);
            assert_ne!(published_lineage_ptr, self.catalog_lineage_ptr);

            // Canonical fields equal actual stored Root and Published object; Maintenance removed.
            let stored_root =
                read_stored_root(self.left_ports, &projection_identity(), locator).unwrap();
            assert_eq!(&stored_root, published_evidence.root());
            let stored_object = read_stored_object(self.left_ports, locator).unwrap();
            assert_eq!(
                stored_object.lifecycle(),
                ProjectionSourceLifecycleV1::Published
            );
            assert_eq!(stored_object.descriptor(), published_evidence.descriptor());
            assert!(read_stored_maintenance(self.left_ports, locator).is_none());
            assert_eq!(
                table_entry_count(self.left_ports, N::ProjectionSourceRows),
                2
            );

            self.left_ports
                .shared
                .database
                .source_usage_domain()
                .reconcile_native_step(self.left_ports, &reconcile_lease, verified_after)?;
            drop(reconcile_lease);
            drop(mutation);

            let pacing = SourceGatePacingObservation::from_native_gate_cost(1)?;
            let response = continuation.published(
                native.brand_for_test(),
                stage_brand,
                applied_step,
                published_evidence,
                pacing,
            )?;
            if let Some(obs) = response.pacing_observation() {
                native.await_maintenance_turn(obs)?;
            }
            let SourceCheckpointContinuation::Published(confirmation) = response.prepare_next()?
            else {
                panic!("Publish must produce SourceNativePublicationConfirmation");
            };
            self.verified_publish = true;
            Ok(confirmation)
        }
    }

    let catalog_lineage_ptr = left_plan.identity().contract_lineage().as_str().as_ptr();
    let mut left_adapter = NativeSourceCheckpointBackend::capture(
        &left_ports,
        &mut left_work,
        &left_plan,
        &left_parent,
    )
    .unwrap();
    let mut coordinator = VerifyEndToEndPublish {
        left_ports: &left_ports,
        left_budget: &left_budget,
        right_ports: &right_ports,
        right_work: &mut right_work,
        right_plan: &right_plan,
        right_parent: &right_parent,
        catalog_lineage_ptr,
        verified_publish: false,
        _backend: PhantomData,
    };
    let confirmation = drive_source_checkpoint(&mut left_adapter, &mut coordinator).unwrap();
    assert!(coordinator.verified_publish);

    // Drop evidence/result and adapter owners while keeping original catalog and plan owners alive;
    // live reserved/owned accounting returns to pre_adapter_baseline and released_allocations is monotone.
    let _ = confirmation;
    drop(left_adapter);
    let post_publish_budget = left_budget.observation().unwrap();
    assert_eq!(
        post_publish_budget.reserved_bytes,
        pre_adapter_baseline.reserved_bytes
    );
    assert_eq!(
        post_publish_budget.owned_bytes,
        pre_adapter_baseline.owned_bytes
    );
    assert!(post_publish_budget.released_allocations > pre_adapter_baseline.released_allocations);

    // Also verify end-to-end native.apply through PrepareAllocation -> Allocate -> Copy -> Publish
    // on a fresh immediate fixture.
    {
        struct FullPortPublishCoordinator<'work>(
            PhantomData<fn() -> NativeSourceCheckpointBackend<'work>>,
        );
        impl<'work> SourceCheckpointMaintenanceCoordinator for FullPortPublishCoordinator<'work> {
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
            ) -> Result<
                SourceNativePublicationConfirmation<'session, Self::Backend>,
                SourceDriverError,
            >
            where
                Self::Backend: 'session,
                'session: 'slot,
                'slot: 'stage,
                'stage: 'candidate,
            {
                let mut instruction = candidate.into_prepare_instruction()?;
                loop {
                    instruction = authorize_step(instruction);
                    let response = native.apply(instruction)?;
                    if let Some(obs) = response.pacing_observation() {
                        native.await_maintenance_turn(obs)?;
                    }
                    match response.prepare_next()? {
                        SourceCheckpointContinuation::Instruction(next) => instruction = next,
                        SourceCheckpointContinuation::Published(confirmation) => {
                            return Ok(confirmation);
                        }
                    }
                }
            }
        }
        let (_commit_root, commit_ports) = immediate_fixture("step-evidence-full-port-publish");
        let commit_completion = finish_census(SourceUsageCensus::start(&commit_ports).unwrap());
        let commit_prefix = commit_ports
            .capture_original_prefix_from_usage(commit_completion)
            .unwrap();
        let commit_budget = commit_prefix.originating_budget().clone();
        let commit_catalog = ActiveCatalogSnapshot::read(&commit_ports).unwrap().unwrap();
        let commit_capture = commit_catalog
            .capture_projection_source_admitted_v1(&identity, &commit_budget)
            .unwrap();
        let commit_plan = SourceDriverPlan::new(&commit_capture);
        let mut commit_work =
            SourceEvaluationWorkOwner::new(commit_prefix, Arc::new(AtomicBool::new(false)));
        let commit_parent = directory.join("commit-coord");
        std::fs::create_dir(&commit_parent).unwrap();
        let commit_baseline = commit_budget.observation().unwrap();
        let mut commit_adapter = NativeSourceCheckpointBackend::capture(
            &commit_ports,
            &mut commit_work,
            &commit_plan,
            &commit_parent,
        )
        .unwrap();
        let mut prod_coordinator = FullPortPublishCoordinator(PhantomData);
        let prod_confirmation =
            drive_source_checkpoint(&mut commit_adapter, &mut prod_coordinator).unwrap();
        let _ = prod_confirmation;
        drop(commit_adapter);
        let commit_after = commit_budget.observation().unwrap();
        assert_eq!(commit_after.reserved_bytes, commit_baseline.reserved_bytes);
        assert_eq!(commit_after.owned_bytes, commit_baseline.owned_bytes);
        assert!(commit_after.released_allocations > commit_baseline.released_allocations);
    }

    // Reconciled Publish usage matches an independent bounded census, and dropping
    // left_work releases the prefix pin (Publish ExactAfter ended the input obligation).
    drop(left_work);
    let domain_usage = left_ports
        .shared
        .database
        .source_usage_domain()
        .complete_observation()
        .unwrap();
    let census_usage = finish_census(SourceUsageCensus::start(&left_ports).unwrap()).usage();
    assert_eq!(domain_usage, census_usage);
    assert!(
        left_ports
            .shared
            .database
            .source_prefix_pins()
            .prepare_prune_in_incarnation(1, 1)
            .is_ok()
    );
}
