//! Derived replay, independent pure-engine validation, and process crashes over
//! isolated typed fixtures; complete catalog/startup integration is separate.
// req: REP-002, REP-003, PRJ-001, REC-001

#[cfg(test)]
mod tests {
    use super::super::*;
    use redb::ReadableDatabase;
    use riffdb_catalog::{ActiveCatalogSnapshot, ResolvedProjectionPlan, ValidatedContractBundle};
    use riffdb_projection::{
        ProjectionGenerationValidationOutcome, evaluate_and_prepare_projection_commit,
        validate_projection_generation,
    };
    use riffdb_storage_api::{
        ActiveCatalogPointerV1, AuthoritativeScanReader, CatalogRepository,
        CheckedProjectionSchema, CommitScanPageV1, CommitScanRequest, StorageScanLimit,
        StoredContractBundleV1,
    };

    struct ManualBootstrap {
        path: TestDatabasePath,
        ports: RedbOperationalPorts,
        candidate: crate::RedbBootstrapCandidate,
        expected: StoredProjectionControlV1,
        requests: Vec<ProjectionApplyRequestV1>,
    }

    fn manual_bootstrap() -> ManualBootstrap {
        use riffdb_storage_api::{
            ChangelogLineageV3, LeadershipEpochV1, ReplicationSourceHoldIdV1,
        };
        let (path, mut ports) = operational("bootstrap-projection-rebuild");
        for n in 1..=2 {
            seed_command(&ports, CommitSequence::new(n).unwrap(), 0);
        }
        let schema = recovery_projection_schema();
        let generation = ProjectionGeneration::first();
        let initial = StoredProjectionControlV1::new(
            schema.identity().clone(),
            generation,
            None,
            Some(riffdb_storage_api::ProjectionGenerationPosition::new(
                generation,
                FrontierPosition::BeforeFirst,
            )),
            None,
            ProjectionLifecycleV1::CatchingUp,
            None,
        )
        .unwrap();
        install_control(&ports, schema.identity(), &initial);
        let key = schema
            .group_key(generation, &[CanonicalValue::string("group-a").unwrap()])
            .unwrap();
        let mut requests = Vec::new();
        let mut expected = initial;
        for (n, count) in [(1, 1), (2, 3)] {
            let sequence = CommitSequence::new(n).unwrap();
            let prior = if n == 1 {
                ProjectionRowPrior::Absent
            } else {
                ProjectionRowPrior::Present(CommitSequence::first())
            };
            let request = ProjectionApplyRequestV1::new(
                schema.clone(),
                generation,
                sequence,
                if n == 1 {
                    FrontierPosition::BeforeFirst
                } else {
                    FrontierPosition::AppliedThrough(CommitSequence::first())
                },
                vec![
                    ProjectionRowUpdateV1::new(
                        &schema,
                        key.clone(),
                        prior,
                        recovery_measures(count),
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            let ProjectionApplyResult::Applied { control, .. } =
                ports.apply_projection(&request).unwrap()
            else {
                panic!("source projection apply");
            };
            expected = control;
            requests.push(request);
        }
        crate::changelog_v3_activation::activate_validated(
            ports.shared.database.begin_write().unwrap(),
            ChangelogLineageV3::new(database_id(), 1, LeadershipEpochV1::initial()).unwrap(),
            riffdb_types::DualFrontier::new(Some(CommitSequence::new(2).unwrap()), None),
        )
        .unwrap();
        let directory = path.0.parent().unwrap();
        let source = ports
            .prepare_replication_bootstrap_v3(
                &directory.join("source-transfer"),
                ReplicationSourceHoldIdV1::new([0x79; 16]).unwrap(),
            )
            .unwrap();
        let manifest = source.manifest();
        let mut receiver =
            crate::RedbBootstrapStage::create(&directory.join("receiver-transfer"), manifest)
                .unwrap();
        for ordinal in 1..=manifest.page_count() {
            receiver
                .append(&source.read_page(ordinal).unwrap().encode().unwrap())
                .unwrap();
        }
        let mut materializer = crate::RedbBootstrapMaterializer::create(
            &directory.join("candidate"),
            receiver.into_materialization_input().unwrap(),
        )
        .unwrap();
        while materializer.copy_next_page().unwrap().is_some() {}
        let candidate = materializer.finish().unwrap();
        ManualBootstrap {
            path,
            ports,
            candidate,
            expected,
            requests,
        }
    }

    #[test]
    fn bootstrap_projection_rebuild_preserves_source_controls_and_restores_exact_derived_rows() {
        let ManualBootstrap {
            path,
            ports,
            mut candidate,
            expected,
            requests,
        } = manual_bootstrap();
        let manifest = candidate.manifest();
        let directory = path.0.parent().unwrap();
        let schema = recovery_projection_schema();
        let generation = ProjectionGeneration::first();
        let key = schema
            .group_key(generation, &[CanonicalValue::string("group-a").unwrap()])
            .unwrap();
        let snapshot_request =
            ProjectionApplySnapshotRequest::new(schema.clone(), generation, vec![key.clone()])
                .unwrap();
        assert_eq!(
            candidate
                .read_apply_snapshot(&snapshot_request)
                .unwrap()
                .expected_frontier(),
            FrontierPosition::BeforeFirst
        );
        candidate
            .rebuild_projection_commit(&expected, &requests[0])
            .unwrap();
        drop(candidate);
        let input = crate::RedbBootstrapStage::open(&directory.join("receiver-transfer"), manifest)
            .unwrap()
            .into_materialization_input()
            .unwrap();
        let mut candidate =
            crate::RedbBootstrapMaterializer::open(&directory.join("candidate"), input)
                .unwrap()
                .finish()
                .unwrap();
        assert_eq!(
            candidate
                .read_apply_snapshot(&snapshot_request)
                .unwrap()
                .expected_frontier(),
            FrontierPosition::AppliedThrough(CommitSequence::first())
        );
        let retry = candidate
            .rebuild_projection_commit(&expected, &requests[0])
            .unwrap();
        assert_eq!(retry.canonical_hash(), requests[0].apply_hash());
        candidate
            .rebuild_projection_commit(&expected, &requests[1])
            .unwrap();
        let snapshot = candidate.read_apply_snapshot(&snapshot_request).unwrap();
        let ProjectionApplyRowObservation::Present(row) = &snapshot.rows()[0] else {
            panic!("rebuilt group");
        };
        assert_eq!(row.measures(), &recovery_measures(3));
        drop(candidate);
        let follower = redb::Database::open(directory.join("candidate/follower.redb")).unwrap();
        let follower_read = follower.begin_read().unwrap();
        let source_read = ports
            .shared
            .derived_database()
            .unwrap()
            .begin_read()
            .unwrap();
        for definition in [PROJECTION_STATE, PROJECTION_APPLIED] {
            let values = |read: &redb::ReadTransaction| {
                read.open_table(definition)
                    .unwrap()
                    .iter()
                    .unwrap()
                    .map(|entry| {
                        let (k, v) = entry.unwrap();
                        (k.value().to_vec(), v.value().to_vec())
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(values(&follower_read), values(&source_read));
        }
    }

    const EVALUATOR_CONTRACT: &str = r#"
contract ProjectionEvaluation version 1 {
  entity Row {
    key (id: i64)
    field seen: u64
  }

  event Added {
    group: i64
    amount: i64
  }

  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }

  command Add {
    input request_key: string<128>
    input id: i64
    input group: i64
    input amount: i64
    idempotency_key request_key
    create Row(id) as row else Exists { id: id }
    set row.seen = 1
    emit Added { group: group, amount: amount }
    return AddedOutcome { row: row }
  }

  projection Totals {
    source event Added
    where group == 10
    key (group)
    measure item_count = count()
    measure total = sum(amount)
    frontier transactionally_ordered
  }
}
"#;

    struct ProjectionCatalogRepository {
        active: ActiveCatalogPointerV1,
        bundle: StoredContractBundleV1,
    }

    impl CatalogRepository for ProjectionCatalogRepository {
        fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
            Ok(Some(self.active.clone()))
        }

        fn read_contract_bundle(
            &self,
            lineage: &ContractLineage,
            version: riffdb_types::ContractVersion,
        ) -> Result<Option<StoredContractBundleV1>, StorageError> {
            Ok(
                (self.bundle.lineage() == lineage && self.bundle.contract_version() == version)
                    .then(|| self.bundle.clone()),
            )
        }
    }

    struct EvaluatorFixture {
        bundle: ValidatedContractBundle,
        resolved: ResolvedProjectionPlan,
        schema: CheckedProjectionSchema,
        group_field: FieldId,
        amount_field: FieldId,
        measure_fields: Vec<(FieldId, bool)>,
    }

    fn evaluator_fixture() -> EvaluatorFixture {
        let compiled = compile_contract_source(EVALUATOR_CONTRACT).expect("evaluator contract");
        let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("validated catalog bundle");
        let stored = bundle.to_stored().expect("stored bundle");
        let repository = ProjectionCatalogRepository {
            active: ActiveCatalogPointerV1::from_bundle(&stored),
            bundle: stored,
        };
        let active = ActiveCatalogSnapshot::read(&repository)
            .expect("catalog read")
            .expect("active catalog");
        let projection = bundle
            .bundle()
            .projections()
            .first()
            .expect("projection plan");
        let identity = ProjectionIdentity::new(
            bundle.lineage().clone(),
            projection.projection_id(),
            projection.plan_hash(),
        );
        let resolved = active
            .resolve_projection(&identity)
            .expect("resolved projection");
        let schema = CheckedProjectionSchema::new(
            bundle
                .bundle()
                .bound_projection_group_schema(projection.projection_id())
                .expect("bound group schema"),
        );
        let event = bundle
            .bundle()
            .schema()
            .event(projection.source_event())
            .expect("source event");
        let field = |name: &str| {
            event
                .payload()
                .fields()
                .iter()
                .find(|field| field.name() == name)
                .map(|field| field.id())
                .expect("event field")
        };
        let group_field = field("group");
        let amount_field = field("amount");
        let count_field = projection
            .measures()
            .iter()
            .find(|measure| measure.expression().is_none())
            .expect("count")
            .field()
            .id();
        let measure_fields = projection
            .group_schema()
            .measures()
            .fields()
            .iter()
            .map(|field| (field.id(), field.id() == count_field))
            .collect();
        EvaluatorFixture {
            bundle,
            resolved,
            schema,
            group_field,
            amount_field,
            measure_fields,
        }
    }

    fn evaluator_uuid_bytes(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn evaluator_commit(
        fixture: &EvaluatorFixture,
        sequence: CommitSequence,
        values: &[(i64, i64)],
    ) -> StoredCommitRecordV1 {
        let command = fixture
            .bundle
            .bundle()
            .commands()
            .first()
            .expect("writer command");
        let plan = ExecutablePlanRef::new(
            fixture.bundle.lineage().clone(),
            fixture.bundle.contract_version(),
            fixture.bundle.bundle_hash(),
            command.command_id(),
            command.plan_hash(),
        );
        let events = values
            .iter()
            .enumerate()
            .map(|(ordinal, (group, amount))| {
                let event_id = EventId::new(
                    sequence,
                    u32::try_from(ordinal).expect("bounded event ordinal"),
                );
                let payload = CanonicalRecord::new(vec![
                    (fixture.group_field, CanonicalValue::I64(*group)),
                    (fixture.amount_field, CanonicalValue::I64(*amount)),
                ])
                .expect("event payload");
                let event_type = fixture.resolved.projection_plan().source_event();
                let event_hash =
                    derive_event_hash_v1(event_id, event_type, &payload).expect("event hash");
                StoredDurableEventV1::new(event_id, event_type, payload, event_hash)
                    .expect("stored event")
            })
            .collect::<Vec<_>>();
        let event_ids = events
            .iter()
            .map(StoredDurableEventV1::event_id)
            .collect::<Vec<_>>();
        let actor = AdmittedActorContext::new(
            ActorId::new("projection-worker-fixture").expect("actor"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        );
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_i64(1).expect("partition component");
        let partition_hash =
            hash_partition_key(partition.finish().expect("partition key").as_bytes());
        let dependencies = StoredReadDependenciesV1::from_live(
            &ReadDependencies::new(Vec::new()).expect("empty dependencies"),
        )
        .expect("stored dependencies");
        StoredCommitRecordV1::new(
            sequence,
            RequestId::from_bytes(evaluator_uuid_bytes(
                u8::try_from(sequence.get()).unwrap_or(0x21),
            ))
            .expect("request"),
            plan,
            CanonicalInputHash::from_bytes([0x41; 32]),
            actor,
            LogicalTime::new(Timestamp::new(2 * 86_400, 123).expect("logical time")),
            partition_hash,
            Vec::new(),
            dependencies,
            Vec::new(),
            events,
            DeclaredOutcome::new(
                OutcomeId::first(),
                CanonicalRecord::new(Vec::new()).expect("outcome payload"),
            )
            .expect("outcome"),
            ProvenanceId::from_bytes(evaluator_uuid_bytes(
                u8::try_from(sequence.get())
                    .unwrap_or(0x21)
                    .wrapping_add(0x40),
            ))
            .expect("provenance"),
            event_ids,
            DurabilityMode::Memory,
        )
        .expect("stored commit")
    }

    #[test]
    fn bootstrap_projection_replays_real_events_and_independently_validates_semantics() {
        use riffdb_storage_api::{
            ChangelogLineageV3, LeadershipEpochV1, ReplicationSourceHoldIdV1,
        };
        let fixture = evaluator_fixture();
        let (path, mut ports) = operational("bootstrap-projection-events");
        let generation = ProjectionGeneration::first();
        let initial = StoredProjectionControlV1::new(
            fixture.schema.identity().clone(),
            generation,
            None,
            Some(riffdb_storage_api::ProjectionGenerationPosition::new(
                generation,
                FrontierPosition::BeforeFirst,
            )),
            None,
            ProjectionLifecycleV1::CatchingUp,
            None,
        )
        .unwrap();
        install_control(&ports, fixture.schema.identity(), &initial);
        // Includes filtered events, a zero-update commit, repeated groups, and a
        // negative amount: byte equality alone cannot prove these semantics.
        let values: &[&[(i64, i64)]] = &[&[(10, 7), (20, 999)], &[], &[(10, -2), (10, 6)]];
        let mut expected = initial;
        for (index, values) in values.iter().enumerate() {
            let commit = evaluator_commit(
                &fixture,
                CommitSequence::new(index as u64 + 1).unwrap(),
                values,
            );
            seed_commit(&ports, &commit);
            let request = evaluate_and_prepare_projection_commit(
                &fixture.resolved,
                fixture.schema.clone(),
                generation,
                &commit,
                &ports,
            )
            .unwrap();
            let ProjectionApplyResult::Applied { control, .. } =
                ports.apply_projection(&request).unwrap()
            else {
                panic!("source apply");
            };
            expected = control;
        }
        crate::changelog_v3_activation::activate_validated(
            ports.shared.database.begin_write().unwrap(),
            ChangelogLineageV3::new(database_id(), 1, LeadershipEpochV1::initial()).unwrap(),
            riffdb_types::DualFrontier::new(Some(CommitSequence::new(3).unwrap()), None),
        )
        .unwrap();
        let directory = path.0.parent().unwrap();
        let source = ports
            .prepare_replication_bootstrap_v3(
                &directory.join("source-transfer"),
                ReplicationSourceHoldIdV1::new([0x7a; 16]).unwrap(),
            )
            .unwrap();
        let manifest = source.manifest();
        let mut receiver =
            crate::RedbBootstrapStage::create(&directory.join("receiver-transfer"), manifest)
                .unwrap();
        for ordinal in 1..=manifest.page_count() {
            receiver
                .append(&source.read_page(ordinal).unwrap().encode().unwrap())
                .unwrap();
        }
        let mut materializer = crate::RedbBootstrapMaterializer::create(
            &directory.join("candidate"),
            receiver.into_materialization_input().unwrap(),
        )
        .unwrap();
        while materializer.copy_next_page().unwrap().is_some() {}
        let mut candidate = materializer.finish().unwrap();
        let head = FrontierPosition::AppliedThrough(CommitSequence::new(3).unwrap());
        let limit = ProjectionRecoveryPageLimit::new(NonZeroU16::new(1).unwrap()).unwrap();
        assert!(matches!(
            validate_projection_generation(
                &candidate,
                &fixture.resolved,
                fixture.schema.clone(),
                &expected,
                head,
                generation,
                limit
            )
            .unwrap(),
            ProjectionGenerationValidationOutcome::Finding(_)
                | ProjectionGenerationValidationOutcome::FenceChanged
        ));
        let mut scan = CommitScanRequest::initial(StorageScanLimit::new(1).unwrap());
        loop {
            let page = candidate.scan_commits(scan).unwrap();
            for item in page.records() {
                let request = evaluate_and_prepare_projection_commit(
                    &fixture.resolved,
                    fixture.schema.clone(),
                    generation,
                    item.value(),
                    &candidate,
                )
                .unwrap();
                candidate
                    .rebuild_projection_commit(&expected, &request)
                    .unwrap();
            }
            match page {
                CommitScanPageV1::Page {
                    next_after,
                    inclusive_upper: FrontierPosition::AppliedThrough(upper),
                    ..
                } => scan = CommitScanRequest::continuing(next_after, upper, scan.limit()).unwrap(),
                CommitScanPageV1::ExactEnd { .. } => break,
                _ => panic!("nonempty page requires head"),
            }
        }
        assert!(matches!(
            validate_projection_generation(
                &candidate,
                &fixture.resolved,
                fixture.schema.clone(),
                &expected,
                head,
                generation,
                limit
            )
            .unwrap(),
            ProjectionGenerationValidationOutcome::Clean(_)
        ));
        let keys = [10, 20].map(|group| {
            fixture
                .schema
                .group_key(generation, &[CanonicalValue::I64(group)])
                .unwrap()
        });
        let snapshot = candidate
            .read_apply_snapshot(
                &ProjectionApplySnapshotRequest::new(
                    fixture.schema.clone(),
                    generation,
                    keys.to_vec(),
                )
                .unwrap(),
            )
            .unwrap();
        let ProjectionApplyRowObservation::Present(row) = &snapshot.rows()[0] else {
            panic!("group 10");
        };
        let expected_measures = CanonicalRecord::new(
            fixture
                .measure_fields
                .iter()
                .map(|(field, count)| {
                    (
                        *field,
                        if *count {
                            CanonicalValue::U64(3)
                        } else {
                            CanonicalValue::I64(11)
                        },
                    )
                })
                .collect(),
        )
        .unwrap();
        assert_eq!(row.measures(), &expected_measures);
        assert!(matches!(
            snapshot.rows()[1],
            ProjectionApplyRowObservation::Absent(_)
        ));
        drop(candidate);
        // A valid checksum and matching source frontier do not prove the measures.
        // Reopening construction still verifies authority; the independent engine
        // must refuse this otherwise well-formed derived row before publication.
        let database = redb::Database::open(directory.join("candidate/follower.redb")).unwrap();
        let write = database.begin_write().unwrap();
        let forged = StoredProjectionStateV1::new(
            &fixture.schema,
            keys[0].clone(),
            CanonicalRecord::new(
                fixture
                    .measure_fields
                    .iter()
                    .map(|(field, count)| {
                        (
                            *field,
                            if *count {
                                CanonicalValue::U64(3)
                            } else {
                                CanonicalValue::I64(999)
                            },
                        )
                    })
                    .collect(),
            )
            .unwrap(),
            CommitSequence::new(3).unwrap(),
        )
        .unwrap();
        write
            .open_table(PROJECTION_STATE)
            .unwrap()
            .insert(
                encode_projection_group_key(forged.key()),
                encode_projection_state_v1(&forged).unwrap().as_bytes(),
            )
            .unwrap();
        write.commit().unwrap();
        drop(database);
        let input = crate::RedbBootstrapStage::open(&directory.join("receiver-transfer"), manifest)
            .unwrap()
            .into_materialization_input()
            .unwrap();
        let candidate = crate::RedbBootstrapMaterializer::open(&directory.join("candidate"), input)
            .unwrap()
            .finish()
            .unwrap();
        assert!(matches!(
            validate_projection_generation(
                &candidate,
                &fixture.resolved,
                fixture.schema.clone(),
                &expected,
                head,
                generation,
                limit
            )
            .unwrap(),
            ProjectionGenerationValidationOutcome::Finding(_)
                | ProjectionGenerationValidationOutcome::FenceChanged
        ));
    }

    fn reopen_manual(directory: &std::path::Path) -> crate::RedbBootstrapCandidate {
        let manifest = riffdb_storage_api::ReplicationBootstrapManifestV1::decode(
            &std::fs::read(directory.join("manifest.bin")).unwrap(),
        )
        .unwrap();
        let input = crate::RedbBootstrapStage::open(&directory.join("receiver-transfer"), manifest)
            .unwrap()
            .into_materialization_input()
            .unwrap();
        crate::RedbBootstrapMaterializer::open(&directory.join("candidate"), input)
            .unwrap()
            .finish()
            .unwrap()
    }

    #[test]
    fn bootstrap_projection_process_child() {
        let Some(directory) = std::env::var_os("RIFFDB_BOOTSTRAP_PROJECTION_PATH") else {
            return;
        };
        let directory = std::path::Path::new(&directory);
        let expected = crate::codec::decode_projection_control_v1(
            &std::fs::read(directory.join("control.bin")).unwrap(),
        )
        .unwrap()
        .into_parts()
        .0;
        let mut candidate = reopen_manual(directory);
        let schema = recovery_projection_schema();
        let generation = ProjectionGeneration::first();
        let key = schema
            .group_key(generation, &[CanonicalValue::string("group-a").unwrap()])
            .unwrap();
        let request = ProjectionApplyRequestV1::new(
            schema.clone(),
            generation,
            CommitSequence::first(),
            FrontierPosition::BeforeFirst,
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
        candidate
            .rebuild_projection_commit(&expected, &request)
            .unwrap();
        panic!("requested rebuild crash did not fire");
    }

    #[test]
    fn bootstrap_projection_crashes_keep_rows_and_markers_atomic_and_resume_without_double_apply() {
        for edge in ["derived-staged", "derived-committed"] {
            let ManualBootstrap {
                path,
                ports,
                candidate,
                expected,
                requests,
            } = manual_bootstrap();
            let directory = path.0.parent().unwrap();
            std::fs::write(
                directory.join("manifest.bin"),
                candidate.manifest().encode().unwrap(),
            )
            .unwrap();
            std::fs::write(
                directory.join("control.bin"),
                encode_projection_control_v1(&expected).unwrap().as_bytes(),
            )
            .unwrap();
            drop(candidate);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "derived::tests::bootstrap_projection::tests::bootstrap_projection_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_BOOTSTRAP_PROJECTION_PATH", directory)
            .env("RIFFDB_BOOTSTRAP_PROJECTION_EDGE", edge)
            .status()
            .unwrap();
            assert_eq!(status.code(), Some(93), "edge {edge}");
            let mut candidate = reopen_manual(directory);
            let request = ProjectionApplySnapshotRequest::new(
                requests[0].schema().clone(),
                requests[0].generation(),
                vec![requests[0].row_updates()[0].key().clone()],
            )
            .unwrap();
            let snapshot = candidate.read_apply_snapshot(&request).unwrap();
            assert_eq!(
                snapshot.expected_frontier(),
                if edge == "derived-staged" {
                    FrontierPosition::BeforeFirst
                } else {
                    FrontierPosition::AppliedThrough(CommitSequence::first())
                }
            );
            assert_eq!(
                matches!(
                    snapshot.rows()[0],
                    ProjectionApplyRowObservation::Present(_)
                ),
                edge == "derived-committed"
            );
            candidate
                .rebuild_projection_commit(&expected, &requests[0])
                .unwrap();
            candidate
                .rebuild_projection_commit(&expected, &requests[1])
                .unwrap();
            let rebuilt = candidate.read_apply_snapshot(&request).unwrap();
            let source = ports.read_apply_snapshot(&request).unwrap();
            assert_eq!(rebuilt, source);
        }
    }

    #[test]
    fn bootstrap_projection_rejects_gaps_stale_controls_and_changed_retries_without_partial_writes()
    {
        for violation in ["gap", "control", "retry", "frontier", "prior"] {
            let ManualBootstrap {
                path,
                ports: _,
                mut candidate,
                expected,
                requests,
            } = manual_bootstrap();
            let directory = path.0.parent().unwrap();
            std::fs::write(
                directory.join("manifest.bin"),
                candidate.manifest().encode().unwrap(),
            )
            .unwrap();
            let mut wrong = requests[0].clone();
            let mut control = expected.clone();
            match violation {
                "gap" => wrong = requests[1].clone(),
                "control" => {
                    control = StoredProjectionControlV1::new(
                        expected.identity().clone(),
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
                "retry" => {
                    candidate
                        .rebuild_projection_commit(&expected, &requests[0])
                        .unwrap();
                    wrong = ProjectionApplyRequestV1::new(
                        wrong.schema().clone(),
                        wrong.generation(),
                        wrong.sequence(),
                        wrong.expected_frontier(),
                        vec![
                            ProjectionRowUpdateV1::new(
                                wrong.schema(),
                                wrong.row_updates()[0].key().clone(),
                                ProjectionRowPrior::Absent,
                                recovery_measures(999),
                            )
                            .unwrap(),
                        ],
                    )
                    .unwrap();
                }
                "frontier" => {
                    wrong = ProjectionApplyRequestV1::new(
                        wrong.schema().clone(),
                        wrong.generation(),
                        CommitSequence::new(3).unwrap(),
                        FrontierPosition::AppliedThrough(CommitSequence::new(2).unwrap()),
                        vec![],
                    )
                    .unwrap()
                }
                "prior" => {
                    candidate
                        .rebuild_projection_commit(&expected, &requests[0])
                        .unwrap();
                    wrong = ProjectionApplyRequestV1::new(
                        wrong.schema().clone(),
                        wrong.generation(),
                        requests[1].sequence(),
                        requests[1].expected_frontier(),
                        vec![
                            ProjectionRowUpdateV1::new(
                                wrong.schema(),
                                wrong.row_updates()[0].key().clone(),
                                ProjectionRowPrior::Absent,
                                recovery_measures(999),
                            )
                            .unwrap(),
                        ],
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            assert_eq!(
                candidate
                    .rebuild_projection_commit(&control, &wrong)
                    .unwrap_err()
                    .kind(),
                StorageErrorKind::CorruptData,
                "{violation}"
            );
            assert!(
                candidate
                    .rebuild_projection_commit(&expected, &requests[0])
                    .is_err(),
                "failure fuses owner"
            );
            drop(candidate);
            let mut candidate = reopen_manual(directory);
            candidate
                .rebuild_projection_commit(&expected, &requests[0])
                .unwrap();
            candidate
                .rebuild_projection_commit(&expected, &requests[1])
                .unwrap();
        }
    }
}
