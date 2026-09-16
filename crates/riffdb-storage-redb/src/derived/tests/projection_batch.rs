// req: PRJ-001, PRJ-002, PRJ-003, PRJ-004
use super::*;
use riffdb_storage_api::{ProjectionApplyBatchResult, ProjectionApplyBatchV1};

fn initial(schema: &riffdb_storage_api::CheckedProjectionSchema) -> StoredProjectionControlV1 {
    StoredProjectionControlV1::new(
        schema.identity().clone(),
        ProjectionGeneration::first(),
        None,
        Some(riffdb_storage_api::ProjectionGenerationPosition::new(
            ProjectionGeneration::first(),
            FrontierPosition::BeforeFirst,
        )),
        None,
        ProjectionLifecycleV1::CatchingUp,
        None,
    )
    .unwrap()
}
fn batch(
    control: StoredProjectionControlV1,
    schema: &riffdb_storage_api::CheckedProjectionSchema,
    first: u64,
    last: u64,
    with_rows: bool,
) -> ProjectionApplyBatchV1 {
    let generation = ProjectionGeneration::first();
    let key = schema
        .group_key(generation, &[CanonicalValue::string("group-a").unwrap()])
        .unwrap();
    let mut frontier = control.frontier_for(generation).unwrap();
    let mut prior = ProjectionRowPrior::Absent;
    let mut members = Vec::new();
    for value in first..=last {
        let sequence = CommitSequence::new(value).unwrap();
        let updates = if with_rows {
            vec![
                ProjectionRowUpdateV1::new(schema, key.clone(), prior, recovery_measures(value))
                    .unwrap(),
            ]
        } else {
            vec![]
        };
        members.push(
            ProjectionApplyRequestV1::new(schema.clone(), generation, sequence, frontier, updates)
                .unwrap(),
        );
        frontier = FrontierPosition::AppliedThrough(sequence);
        prior = ProjectionRowPrior::Present(sequence);
    }
    ProjectionApplyBatchV1::new(
        control,
        members,
        if with_rows {
            vec![ProjectionApplyRowObservation::Absent(key)]
        } else {
            vec![]
        },
    )
    .unwrap()
}

#[test]
fn projection_batch_rejects_gaps_missing_evidence_and_aggregate_count_overflow() {
    let schema = recovery_projection_schema();
    let control = initial(&schema);
    let valid = batch(control.clone(), &schema, 1, 64, true);
    let mut missing_member = valid.members().to_vec();
    missing_member.remove(31);
    assert_eq!(
        ProjectionApplyBatchV1::new(
            control.clone(),
            missing_member,
            valid.observations().to_vec()
        ),
        Err(StorageValueError::IdentityMismatch)
    );
    assert_eq!(
        ProjectionApplyBatchV1::new(control.clone(), valid.members().to_vec(), vec![]),
        Err(StorageValueError::InvalidShape)
    );
    let extra = batch(control.clone(), &schema, 1, 64, false);
    assert_eq!(
        ProjectionApplyBatchV1::new(
            control.clone(),
            extra.members().to_vec(),
            valid.observations().to_vec()
        ),
        Err(StorageValueError::InvalidShape)
    );
    let mut too_many = extra.members().to_vec();
    too_many.push(
        ProjectionApplyRequestV1::new(
            schema.clone(),
            ProjectionGeneration::first(),
            CommitSequence::new(65).unwrap(),
            FrontierPosition::AppliedThrough(CommitSequence::new(64).unwrap()),
            vec![],
        )
        .unwrap(),
    );
    assert_eq!(
        ProjectionApplyBatchV1::new(control.clone(), too_many, vec![]),
        Err(StorageValueError::LimitExceeded)
    );

    let keys = (0..65)
        .map(|n| {
            schema
                .group_key(
                    ProjectionGeneration::first(),
                    &[CanonicalValue::string(format!("group-{n:02}")).unwrap()],
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let observations = keys
        .iter()
        .cloned()
        .map(ProjectionApplyRowObservation::Absent)
        .collect::<Vec<_>>();
    let members = |last_count: usize| {
        (1..=64)
            .map(|n| {
                let prior = if n == 1 {
                    ProjectionRowPrior::Absent
                } else {
                    ProjectionRowPrior::Present(CommitSequence::new(n - 1).unwrap())
                };
                let count = if n == 64 { last_count } else { 64 };
                let updates = keys[..count]
                    .iter()
                    .enumerate()
                    .map(|(i, key)| {
                        ProjectionRowUpdateV1::new(
                            &schema,
                            key.clone(),
                            if i == 64 {
                                ProjectionRowPrior::Absent
                            } else {
                                prior
                            },
                            recovery_measures(n),
                        )
                        .unwrap()
                    })
                    .collect();
                ProjectionApplyRequestV1::new(
                    schema.clone(),
                    ProjectionGeneration::first(),
                    CommitSequence::new(n).unwrap(),
                    if n == 1 {
                        FrontierPosition::BeforeFirst
                    } else {
                        FrontierPosition::AppliedThrough(CommitSequence::new(n - 1).unwrap())
                    },
                    updates,
                )
                .unwrap()
            })
            .collect::<Vec<_>>()
    };
    assert!(
        ProjectionApplyBatchV1::new(control.clone(), members(64), observations[..64].to_vec())
            .is_ok()
    );
    assert_eq!(
        ProjectionApplyBatchV1::new(control, members(65), observations),
        Err(StorageValueError::LimitExceeded)
    );
}

#[test]
fn projection_batch_charges_combined_evidence_members_and_postimages_before_storage() {
    let schema = riffdb_storage_api::CheckedProjectionSchema::new(
        compile_contract_source("contract BatchBytes version 1 {
          event Source { group: string<3500> }
          projection Totals { source event Source key (group) measure total = count() frontier transactionally_ordered }
        }").unwrap().bound_projection_group_schema(ProjectionId::first()).unwrap()
    );
    let control = initial(&schema);
    let generation = ProjectionGeneration::first();
    let keys = (0..1024)
        .map(|n| {
            schema
                .group_key(
                    generation,
                    &[CanonicalValue::string(format!("{n:04}{}", "x".repeat(3000))).unwrap()],
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let observations = keys
        .iter()
        .cloned()
        .map(ProjectionApplyRowObservation::Absent)
        .collect::<Vec<_>>();
    let member = |n| {
        ProjectionApplyRequestV1::new(
            schema.clone(),
            generation,
            CommitSequence::new(n).unwrap(),
            if n == 1 {
                FrontierPosition::BeforeFirst
            } else {
                FrontierPosition::AppliedThrough(CommitSequence::first())
            },
            keys.iter()
                .map(|key| {
                    ProjectionRowUpdateV1::new(
                        &schema,
                        key.clone(),
                        if n == 1 {
                            ProjectionRowPrior::Absent
                        } else {
                            ProjectionRowPrior::Present(CommitSequence::first())
                        },
                        recovery_measures(n),
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap()
    };
    let first = member(1);
    assert!(
        ProjectionApplyBatchV1::new(control.clone(), vec![first.clone()], observations.clone())
            .is_ok()
    );
    assert_eq!(
        ProjectionApplyBatchV1::new(control, vec![first, member(2)], observations),
        Err(StorageValueError::LimitExceeded)
    );
}

#[test]
fn projection_batch_retry_and_unknown_resolution_reject_missing_or_substituted_markers() {
    for substitute in [false, true] {
        let (_path, mut ports) = operational("projection-batch-marker-corruption");
        let schema = recovery_projection_schema();
        let control = initial(&schema);
        install_control(&ports, schema.identity(), &control);
        for n in 1..=2 {
            seed_command(&ports, CommitSequence::new(n).unwrap(), 0);
        }
        let request = batch(control, &schema, 1, 2, true);
        ports.apply_projection_batch(&request).unwrap();
        let key = ProjectionApplyKey::new(
            schema.identity().clone(),
            ProjectionGeneration::first(),
            CommitSequence::first(),
        );
        let access = ports.begin_write().unwrap();
        {
            let mut markers = access
                .transaction()
                .unwrap()
                .open_table(PROJECTION_APPLIED)
                .unwrap();
            if substitute {
                let wrong = StoredProjectionApplyV1::new(
                    key.clone(),
                    riffdb_types::ProjectionApplyHash::from_bytes([0; 32]),
                );
                let encoded = encode_projection_apply_v1(&wrong).unwrap();
                markers
                    .insert(encode_projection_apply_key(&key), encoded.as_bytes())
                    .unwrap();
            } else {
                markers.remove(encode_projection_apply_key(&key)).unwrap();
            }
        }
        access
            .commit_for(RedbTestOperation::Initialization)
            .unwrap();
        assert_eq!(
            ports.resolve_projection_batch(&request).unwrap_err().kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            ports.apply_projection_batch(&request).unwrap_err().kind(),
            StorageErrorKind::CorruptData
        );
    }
}

#[test]
fn projection_batch_1024_irrelevant_commits_use_16_transactions_and_preserve_every_marker() {
    let (_path, mut ports) = operational("projection-batch-irrelevant");
    for value in 1..=1024 {
        seed_command(&ports, CommitSequence::new(value).unwrap(), 0);
    }
    let schema = recovery_projection_schema();
    let mut control = initial(&schema);
    install_control(&ports, schema.identity(), &control);
    let tickets = ports.mutation_gate_tickets();
    let mut expected_markers = Vec::new();
    for first in (1..=1024).step_by(64) {
        let request = batch(control.clone(), &schema, first, first + 63, false);
        expected_markers.extend(
            request
                .members()
                .iter()
                .map(|member| (member.sequence(), member.apply_hash())),
        );
        let ProjectionApplyBatchResult::Applied(next) =
            ports.apply_projection_batch(&request).unwrap()
        else {
            panic!("batch applies");
        };
        control = next;
    }
    assert_eq!(ports.mutation_gate_tickets() - tickets, 16);
    assert_eq!(
        control.frontier_for(ProjectionGeneration::first()),
        Some(FrontierPosition::AppliedThrough(
            CommitSequence::new(1024).unwrap()
        ))
    );
    let access = ports.begin_composite_read().unwrap();
    let markers = access.open_table(PROJECTION_APPLIED).unwrap();
    for (sequence, hash) in expected_markers {
        let marker = read_projection_marker(
            &markers,
            &ProjectionApplyKey::new(
                schema.identity().clone(),
                ProjectionGeneration::first(),
                sequence,
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(marker.canonical_hash(), hash);
    }
}

#[test]
fn projection_batch_overlap_matches_single_commit_reference_and_keeps_pinned_base() {
    let (_path, mut ports) = operational("projection-batch-overlap");
    let (_oracle_path, mut oracle) = operational("projection-batch-oracle");
    for value in 1..=64 {
        for target in [&ports, &oracle] {
            seed_command(target, CommitSequence::new(value).unwrap(), 0);
        }
    }
    let schema = recovery_projection_schema();
    let control = initial(&schema);
    for target in [&ports, &oracle] {
        install_control(target, schema.identity(), &control);
    }
    let request = batch(control.clone(), &schema, 1, 64, true);
    let snapshot = ports
        .capture_apply_batch_snapshot(schema.identity())
        .unwrap();
    let tickets = ports.mutation_gate_tickets();
    let ProjectionApplyBatchResult::Applied(actual) =
        ports.apply_projection_batch(&request).unwrap()
    else {
        panic!("batch applies");
    };
    assert_eq!(ports.mutation_gate_tickets() - tickets, 1);
    for member in request.members() {
        assert!(matches!(
            oracle.apply_projection(member).unwrap(),
            ProjectionApplyResult::Applied { .. }
        ));
    }
    assert_eq!(
        oracle
            .capture_apply_batch_snapshot(schema.identity())
            .unwrap()
            .control(),
        &actual
    );
    let keys = request
        .observations()
        .iter()
        .map(|row| row.key().clone())
        .collect();
    let read =
        ProjectionApplySnapshotRequest::new(schema.clone(), ProjectionGeneration::first(), keys)
            .unwrap();
    assert_eq!(
        ports.read_apply_snapshot(&read).unwrap(),
        oracle.read_apply_snapshot(&read).unwrap()
    );
    let old = snapshot.read_apply_snapshot(&read).unwrap();
    assert_eq!(old.expected_frontier(), FrontierPosition::BeforeFirst);
    assert!(matches!(
        old.rows(),
        [ProjectionApplyRowObservation::Absent(_)]
    ));
    assert_eq!(
        ports.apply_projection_batch(&request).unwrap(),
        ProjectionApplyBatchResult::AlreadyApplied
    );
}

#[test]
fn projection_batch_mixed_prefix_and_missing_source_never_apply_a_suffix() {
    let (_path, mut ports) = operational("projection-batch-prefix");
    let schema = recovery_projection_schema();
    let control = initial(&schema);
    install_control(&ports, schema.identity(), &control);
    for value in 1..=3 {
        seed_command(&ports, CommitSequence::new(value).unwrap(), 0);
    }
    let request = batch(control.clone(), &schema, 1, 4, true);
    assert_eq!(
        ports.apply_projection_batch(&request).unwrap_err().kind(),
        StorageErrorKind::CorruptData
    );
    assert_eq!(
        ports
            .capture_apply_batch_snapshot(schema.identity())
            .unwrap()
            .control(),
        &control
    );
    seed_command(&ports, CommitSequence::new(4).unwrap(), 0);
    assert!(matches!(
        ports.apply_projection(&request.members()[0]).unwrap(),
        ProjectionApplyResult::Applied { .. }
    ));
    let before = ports.read_projection_status(schema.identity()).unwrap();
    assert_eq!(
        ports.apply_projection_batch(&request).unwrap(),
        ProjectionApplyBatchResult::StateChanged
    );
    assert_eq!(
        ports.read_projection_status(schema.identity()).unwrap(),
        before
    );
    let access = ports.begin_composite_read().unwrap();
    let markers = access.open_table(PROJECTION_APPLIED).unwrap();
    assert!(
        read_projection_marker(
            &markers,
            &ProjectionApplyKey::new(
                schema.identity().clone(),
                ProjectionGeneration::first(),
                CommitSequence::new(2).unwrap()
            )
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn projection_batch_retirement_requires_fresh_preparation_without_applying_old_generation() {
    let (_path, mut ports) = operational("projection-batch-retired-generation");
    let schema = recovery_projection_schema();
    let request = batch(initial(&schema), &schema, 1, 2, true);
    let next = ProjectionGeneration::first().checked_next().unwrap();
    let replacement = StoredProjectionControlV1::new(
        schema.identity().clone(),
        next,
        None,
        Some(riffdb_storage_api::ProjectionGenerationPosition::new(
            next,
            FrontierPosition::BeforeFirst,
        )),
        None,
        ProjectionLifecycleV1::CatchingUp,
        None,
    )
    .unwrap();
    install_control(&ports, schema.identity(), &replacement);
    assert_eq!(
        ports.apply_projection_batch(&request).unwrap(),
        ProjectionApplyBatchResult::StateChanged
    );
    assert_eq!(
        ports.resolve_projection_batch(&request).unwrap(),
        ProjectionApplyBatchResult::StateChanged
    );
    assert_eq!(
        ports
            .capture_apply_batch_snapshot(schema.identity())
            .unwrap()
            .control(),
        &replacement
    );
}

#[test]
fn projection_batch_crash_and_unknown_outcome_recover_only_complete_prefixes() {
    const MODE: &str = "RIFFDB_PROJECTION_BATCH_CRASH_MODE";
    const PATH: &str = "RIFFDB_PROJECTION_BATCH_CRASH_PATH";
    const TEST: &str = "derived::tests::projection_batch::projection_batch_crash_and_unknown_outcome_recover_only_complete_prefixes";
    if let Ok(mode) = std::env::var(MODE) {
        let path = PathBuf::from(std::env::var_os(PATH).unwrap());
        let controller = match mode.as_str() {
            "before" => crate::hooks::RedbTestController::abort_before_commit(
                RedbTestOperation::ProjectionMutation,
            ),
            "after" => crate::hooks::RedbTestController::abort_after_commit(
                RedbTestOperation::ProjectionMutation,
            ),
            "unknown" => crate::hooks::RedbTestController::return_unknown_after_commit(
                RedbTestOperation::ProjectionMutation,
            ),
            _ => panic!("closed mode"),
        };
        let store = RedbStore::open_with_test_controller(path, controller).unwrap();
        let dormant = crate::store::RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        };
        let ports = dormant.into_operational_after_catalog_validation().unwrap();
        let schema = recovery_projection_schema();
        let request = batch(initial(&schema), &schema, 1, 64, true);
        let registry = riffdb_projection::ProjectionSchemaRegistry::new([schema]).unwrap();
        let notifier = riffdb_projection::ProjectionNotifier::from_registry(&registry);
        let mut controller = riffdb_projection::ProjectionController::new(ports, notifier);
        let result = controller.apply_batch(&request).unwrap();
        assert_eq!(mode, "unknown", "crash modes must not return");
        assert_eq!(
            result,
            ProjectionApplyBatchResult::AlreadyApplied,
            "unknown commit reconciles every exact marker"
        );
        return;
    }
    for mode in ["before", "after", "unknown"] {
        let (path, ports) = operational(&format!("projection-batch-crash-{mode}"));
        let schema = recovery_projection_schema();
        for value in 1..=64 {
            seed_command(&ports, CommitSequence::new(value).unwrap(), 0);
        }
        let control = initial(&schema);
        install_control(&ports, schema.identity(), &control);
        drop(ports);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(TEST)
            .env(MODE, mode)
            .env(PATH, &path.0)
            .stdin(std::process::Stdio::null())
            .output()
            .unwrap();
        let status = output.status;
        if mode == "unknown" {
            assert!(
                status.success(),
                "{} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            assert_eq!(status.code(), None, "abort at {mode}");
        }
        let store = RedbStore::open(&path.0).unwrap();
        let dormant = crate::store::RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        };
        let ports = dormant.into_operational_after_catalog_validation().unwrap();
        let base = ports
            .capture_apply_batch_snapshot(schema.identity())
            .unwrap();
        let committed = mode != "before";
        assert_eq!(
            base.control().frontier_for(ProjectionGeneration::first()),
            Some(if committed {
                FrontierPosition::AppliedThrough(CommitSequence::new(64).unwrap())
            } else {
                FrontierPosition::BeforeFirst
            })
        );
        let request = batch(control, &schema, 1, 64, true);
        let access = ports.begin_composite_read().unwrap();
        let markers = access.open_table(PROJECTION_APPLIED).unwrap();
        for member in request.members() {
            let marker = read_projection_marker(
                &markers,
                &ProjectionApplyKey::new(
                    schema.identity().clone(),
                    ProjectionGeneration::first(),
                    member.sequence(),
                ),
            )
            .unwrap();
            assert_eq!(
                marker.is_some(),
                committed,
                "{mode}: no partial marker prefix"
            );
            if let Some(marker) = marker {
                assert_eq!(marker.canonical_hash(), member.apply_hash());
            }
        }
        let rows = access.open_table(PROJECTION_STATE).unwrap();
        let row = read_projection_state(&rows, &schema, request.observations()[0].key()).unwrap();
        assert_eq!(row.is_some(), committed);
        if let Some(row) = row {
            assert_eq!(row.measures(), &recovery_measures(64));
        }
    }
}

#[test]
fn projection_batch_checks_complete_base_rows_and_full_control_not_only_frontier() {
    let (_path, mut ports) = operational("projection-batch-exact-base");
    let schema = recovery_projection_schema();
    let generation = ProjectionGeneration::first();
    for value in 1..=2 {
        seed_command(&ports, CommitSequence::new(value).unwrap(), 0);
    }
    let control = initial(&schema);
    install_control(&ports, schema.identity(), &control);
    let first = batch(control, &schema, 1, 1, true);
    let ProjectionApplyBatchResult::Applied(control) =
        ports.apply_projection_batch(&first).unwrap()
    else {
        panic!("first");
    };
    let key = first.observations()[0].key().clone();
    let old = StoredProjectionStateV1::new(
        &schema,
        key.clone(),
        recovery_measures(1),
        CommitSequence::first(),
    )
    .unwrap();
    let update = ProjectionRowUpdateV1::new(
        &schema,
        key.clone(),
        ProjectionRowPrior::Present(CommitSequence::first()),
        recovery_measures(2),
    )
    .unwrap();
    let member = ProjectionApplyRequestV1::new(
        schema.clone(),
        generation,
        CommitSequence::new(2).unwrap(),
        FrontierPosition::AppliedThrough(CommitSequence::first()),
        vec![update],
    )
    .unwrap();
    let request = ProjectionApplyBatchV1::new(
        control.clone(),
        vec![member],
        vec![ProjectionApplyRowObservation::Present(old.clone())],
    )
    .unwrap();
    // Same last_changed_sequence, substituted measures: a version-only check
    // would admit this write and hide the changed evidence.
    let changed = StoredProjectionStateV1::new(
        &schema,
        key.clone(),
        recovery_measures(99),
        CommitSequence::first(),
    )
    .unwrap();
    for row in [&changed, &old] {
        let access = ports.begin_write().unwrap();
        let encoded = encode_projection_state_v1(row).unwrap();
        access
            .transaction()
            .unwrap()
            .open_table(PROJECTION_STATE)
            .unwrap()
            .insert(encode_projection_group_key(&key), encoded.as_bytes())
            .unwrap();
        access
            .commit_for(RedbTestOperation::Initialization)
            .unwrap();
        if row == &changed {
            assert_eq!(
                ports.apply_projection_batch(&request).unwrap(),
                ProjectionApplyBatchResult::StateChanged
            );
            assert_eq!(
                ports
                    .capture_apply_batch_snapshot(schema.identity())
                    .unwrap()
                    .control(),
                &control
            );
        }
    }
    // Change lifecycle/failure while retaining the same generation and frontier.
    let operation = ProjectionControlOperation::RecordFailure {
        expected: control.clone(),
        failure: riffdb_storage_api::ProjectionFailureV1::new(
            generation,
            riffdb_storage_api::ProjectionFailureCodeV1::ProjectionStateIntegrity,
            Some(CommitSequence::new(2).unwrap()),
        ),
    };
    assert!(matches!(
        ports.transition_projection_control(operation).unwrap(),
        ProjectionControlResult::Updated(_)
    ));
    assert_eq!(
        ports.apply_projection_batch(&request).unwrap(),
        ProjectionApplyBatchResult::StateChanged
    );
}
