use riffdb_catalog::ValidatedContractBundle;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_storage_api as shutdown_api;
use std::sync::Barrier;
use std::sync::atomic::AtomicUsize;

const SHUTDOWN_CONTRACT: &str = r#"
contract ProjectionShutdown version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Ping {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    create Row(id) as row else Exists {}
    return Done {}
  }
  event Added { group: i64 amount: i64 }
  projection Totals {
    source event Added
    key (group)
    measure item_count = count()
    measure total = sum(amount)
    frontier transactionally_ordered
  }
}
"#;

struct ShutdownCatalog {
    bundle: shutdown_api::StoredContractBundleV1,
}
impl CatalogRepository for ShutdownCatalog {
    fn read_active_catalog(
        &self,
    ) -> Result<Option<shutdown_api::ActiveCatalogPointerV1>, StorageError> {
        Ok(Some(shutdown_api::ActiveCatalogPointerV1::from_bundle(
            &self.bundle,
        )))
    }
    fn read_contract_bundle(
        &self,
        lineage: &riffdb_types::ContractLineage,
        version: riffdb_types::ContractVersion,
    ) -> Result<Option<shutdown_api::StoredContractBundleV1>, StorageError> {
        Ok(
            (self.bundle.lineage() == lineage && self.bundle.contract_version() == version)
                .then(|| self.bundle.clone()),
        )
    }
}

#[derive(Clone, Copy)]
enum ShutdownFirstResult {
    Status,
    Error(shutdown_api::StorageErrorKind),
}

#[derive(Clone)]
struct ShutdownRepository {
    control: StoredProjectionControlV1,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
    calls: Arc<AtomicUsize>,
    mutations: Arc<AtomicUsize>,
    first_result: ShutdownFirstResult,
    clean_second_call: bool,
    replay: Option<Arc<Vec<shutdown_api::EncodedPageItem<shutdown_api::StoredCommitRecordV1>>>>,
    recovery_calls: Arc<AtomicUsize>,
}

fn shutdown_sentinel() -> StorageError {
    StorageError::new(shutdown_api::StorageErrorKind::Unavailable, None)
}

impl ProjectionQueryReader for ShutdownRepository {
    fn query_projection(
        &self,
        _: &shutdown_api::ProjectionQueryRequest,
    ) -> Result<shutdown_api::ProjectionQueryResult, StorageError> {
        unreachable!("empty generation recovery does not query projection rows")
    }
    fn read_projection_status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<shutdown_api::ProjectionStatus, StorageError> {
        assert_eq!(identity, self.control.identity());
        assert_eq!(self.calls.fetch_add(1, Ordering::AcqRel), 0);
        self.entered.wait();
        self.release.wait();
        match self.first_result {
            ShutdownFirstResult::Status => Ok(shutdown_api::ProjectionStatus::from_control(
                &self.control,
                FrontierPosition::BeforeFirst,
            )),
            ShutdownFirstResult::Error(kind) => Err(StorageError::new(kind, None)),
        }
    }
}
impl shutdown_api::ProjectionRecoveryRepository for ShutdownRepository {
    fn scan_projection_controls(
        &self,
        _: Option<&ProjectionIdentity>,
        _: ProjectionRecoveryPageLimit,
    ) -> Result<shutdown_api::ProjectionControlScanV1, StorageError> {
        unreachable!("this generation is already selected by its checked schema")
    }
    fn validate_projection_recovery_page(
        &self,
        request: &shutdown_api::ProjectionRecoveryValidationRequestV1,
    ) -> Result<shutdown_api::ProjectionRecoveryValidationResultV1, StorageError> {
        self.recovery_calls.fetch_add(1, Ordering::AcqRel);
        assert_eq!(self.calls.fetch_add(1, Ordering::AcqRel), 1);
        assert_eq!(request.expected_control(), &self.control);
        assert_eq!(
            request.expected_authoritative_head(),
            FrontierPosition::BeforeFirst
        );
        // The uncorrected worker reaches exactly this second bounded operation.
        // Refusing here makes the RED terminate rather than requiring a timeout.
        if !self.clean_second_call {
            return Err(shutdown_sentinel());
        }
        Ok(
            shutdown_api::ProjectionRecoveryValidationResultV1::ExactEnd(
                shutdown_api::CleanProjectionGenerationV1::new(
                    self.control.identity().clone(),
                    ProjectionGenerationPosition::new(
                        ProjectionGeneration::first(),
                        FrontierPosition::BeforeFirst,
                    ),
                    0,
                ),
            ),
        )
    }
}
impl AuthoritativeScanReader for ShutdownRepository {
    fn scan_index(
        &self,
        _: shutdown_api::AuthoritativeIndexScanRequest,
    ) -> Result<shutdown_api::AuthoritativeIndexScanPage, StorageError> {
        unreachable!("before-first projection has no authoritative rows to replay")
    }
    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        let replay = self
            .replay
            .as_ref()
            .expect("only the genuine prefix schedule reads commits");
        if self.calls.fetch_add(1, Ordering::AcqRel) != 0 {
            // Identity RED reaches a second full-prefix scan for marker two.
            // This bounded sentinel makes its failure terminate deterministically.
            return Err(shutdown_sentinel());
        }
        self.entered.wait();
        self.release.wait();
        assert!(request.after().is_none());
        CommitScanPageV1::page(
            request,
            FrontierPosition::AppliedThrough(CommitSequence::new(501).expect("head")),
            replay[..500].to_vec(),
            CommitSequence::new(500).expect("page end"),
        )
        .map_err(|_| StorageError::new(shutdown_api::StorageErrorKind::InvariantViolation, None))
    }
}
impl ProjectionMutationRepository for ShutdownRepository {
    fn apply_projection(
        &mut self,
        _: &shutdown_api::ProjectionApplyRequestV1,
    ) -> Result<shutdown_api::ProjectionApplyResult, StorageError> {
        self.mutations.fetch_add(1, Ordering::AcqRel);
        Err(StorageError::new(
            shutdown_api::StorageErrorKind::InvariantViolation,
            None,
        ))
    }
    fn transition_projection_control(
        &mut self,
        _: shutdown_api::ProjectionControlOperation,
    ) -> Result<ProjectionControlResult, StorageError> {
        self.mutations.fetch_add(1, Ordering::AcqRel);
        Err(StorageError::new(
            shutdown_api::StorageErrorKind::InvariantViolation,
            None,
        ))
    }
}

struct ShutdownObservation {
    result: Result<ProjectionRecoveryOutcome, ProjectionWorkerError>,
    own_stop: bool,
    calls: usize,
    mutations: usize,
    witnessed: bool,
    readiness: ProjectionWorkerReadiness,
    continue_pass: bool,
}

fn shutdown_recovery_schedule(
    stop_between_calls: bool,
    first_result: ShutdownFirstResult,
    clean_second_call: bool,
) -> ShutdownObservation {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(SHUTDOWN_CONTRACT).expect("projection contract"),
    )
    .expect("validated bundle");
    let catalog = ShutdownCatalog {
        bundle: bundle.to_stored().expect("stored bundle"),
    };
    let active = ActiveCatalogSnapshot::read(&catalog)
        .expect("catalog read")
        .expect("active catalog");
    let plan = bundle.bundle().projections().first().expect("projection");
    let schema = CheckedProjectionSchema::new(
        bundle
            .bundle()
            .bound_projection_group_schema(plan.projection_id())
            .expect("bound group schema"),
    );
    let resolved = active
        .resolve_projection(schema.identity())
        .expect("resolved projection");
    let control = StoredProjectionControlV1::initial(schema.identity().clone());
    let registry = ProjectionSchemaRegistry::new(vec![schema.clone()]).expect("schema registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let mutations = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(StopState {
        requested: Mutex::new(false),
        changed: Condvar::new(),
        interrupted: AtomicBool::new(false),
    });
    let repository = ProjectionWorkerRepository::new(
        ShutdownRepository {
            control,
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            calls: Arc::clone(&calls),
            mutations: Arc::clone(&mutations),
            first_result,
            clean_second_call,
            replay: None,
            recovery_calls: Arc::new(AtomicUsize::new(0)),
        },
        Arc::clone(&stop),
    );
    let worker_stop = Arc::clone(&stop);
    let task = thread::spawn(move || {
        let mut controller = ProjectionController::new(repository, notifier);
        let result = (|| {
            let status = controller
                .repository()
                .read_projection_status(schema.identity())
                .map_err(ProjectionWorkerError::Storage)?;
            let expected = control_from_status(&status)?;
            validate_and_recover_projection_generation(
                &mut controller,
                &resolved,
                schema,
                &expected,
                FrontierPosition::BeforeFirst,
                ProjectionGeneration::first(),
                ProjectionRecoveryPageLimit::new(NonZeroU16::MIN).expect("one-row page"),
            )
            .map_err(ProjectionWorkerError::Recovery)
        })();
        let own_stop = result
            .as_ref()
            .is_err_and(|error| error.is_own_stop(&worker_stop));
        let status = ProjectionWorkerStatus::new();
        let continue_pass =
            finish_projection_pass(result.as_ref().map(|_| ()), &worker_stop, &status);
        (result, own_stop, status.readiness(), continue_pass)
    });
    entered.wait();
    if stop_between_calls {
        request_stop(&stop).expect("request stop");
    }
    release.wait();
    let (result, own_stop, readiness, continue_pass) =
        task.join().expect("bounded repository schedule");
    ShutdownObservation {
        result,
        own_stop,
        readiness,
        continue_pass,
        calls: calls.load(Ordering::Acquire),
        mutations: mutations.load(Ordering::Acquire),
        witnessed: stop.interrupted.load(Ordering::Acquire),
    }
}

// req: PRJ-004, REC-001
#[test]
fn shutdown_between_projection_recovery_operations_skips_next_call_without_clean_or_mutation() {
    let observation = shutdown_recovery_schedule(true, ShutdownFirstResult::Status, false);
    assert_eq!(
        observation.calls, 1,
        "shutdown must stop before the next bounded repository operation"
    );
    assert_eq!(
        observation.mutations, 0,
        "partial validation must not mutate control or allocate a rebuild"
    );
    assert!(observation.witnessed);
    assert!(!observation.continue_pass);
    assert_eq!(
        observation.readiness,
        ProjectionWorkerReadiness::Starting,
        "partial validation publishes neither Ready nor Degraded"
    );
    assert!(
        observation.own_stop,
        "the worker must stop quietly instead of publishing Ready or Degraded"
    );
    assert!(
        matches!(observation.result, Err(ProjectionWorkerError::Recovery(error)) if error.kind() == riffdb_projection::ProjectionRecoveryErrorKind::Evaluation(riffdb_projection::ProjectionEvaluationErrorKind::StorageUnavailable))
    );
}

// req: PRJ-004
#[test]
fn shutdown_racing_inflight_projection_corruption_preserves_integrity() {
    let observation = shutdown_recovery_schedule(
        true,
        ShutdownFirstResult::Error(shutdown_api::StorageErrorKind::CorruptData),
        false,
    );
    assert_eq!(observation.calls, 1);
    assert_eq!(observation.mutations, 0);
    assert!(!observation.witnessed);
    assert!(
        !observation.own_stop,
        "requesting Stop must not hide a real storage corruption"
    );
    assert!(observation.continue_pass);
    assert_eq!(observation.readiness, ProjectionWorkerReadiness::Degraded);
    assert!(
        matches!(observation.result, Err(ProjectionWorkerError::Storage(error)) if error.kind() == shutdown_api::StorageErrorKind::CorruptData)
    );
}

// req: PRJ-004
#[test]
fn unavailable_projection_operation_without_shutdown_has_no_cancellation_witness() {
    let observation = shutdown_recovery_schedule(
        false,
        ShutdownFirstResult::Error(shutdown_api::StorageErrorKind::Unavailable),
        false,
    );
    assert_eq!(observation.calls, 1);
    assert_eq!(observation.mutations, 0);
    assert!(!observation.witnessed);
    assert!(!observation.own_stop);
    assert!(
        matches!(observation.result, Err(ProjectionWorkerError::Storage(error)) if error.kind() == shutdown_api::StorageErrorKind::Unavailable)
    );
}

// req: PRJ-004, REC-001
#[test]
fn uninterrupted_projection_recovery_preserves_complete_validation() {
    let observation = shutdown_recovery_schedule(false, ShutdownFirstResult::Status, true);
    assert_eq!(observation.calls, 2);
    assert_eq!(observation.mutations, 0);
    assert!(observation.continue_pass);
    assert_eq!(observation.readiness, ProjectionWorkerReadiness::Ready);
    assert!(!observation.witnessed);
    assert!(!observation.own_stop);
    assert!(
        matches!(observation.result, Ok(ProjectionRecoveryOutcome::Clean(clean)) if clean.position().frontier() == FrontierPosition::BeforeFirst && clean.state_rows() == 0)
    );
}

// req: PRJ-004
#[test]
fn unavailable_projection_recovery_without_shutdown_keeps_existing_degraded_classification() {
    let observation = shutdown_recovery_schedule(false, ShutdownFirstResult::Status, false);
    assert_eq!(observation.calls, 2);
    assert_eq!(observation.mutations, 0);
    assert!(!observation.witnessed);
    assert!(!observation.own_stop);
    assert!(observation.continue_pass);
    assert_eq!(observation.readiness, ProjectionWorkerReadiness::Degraded);
    assert!(
        matches!(observation.result, Err(ProjectionWorkerError::Recovery(error)) if error.kind() == riffdb_projection::ProjectionRecoveryErrorKind::Evaluation(riffdb_projection::ProjectionEvaluationErrorKind::StorageUnavailable))
    );
}

fn shutdown_replay_commit(
    bundle: &ValidatedContractBundle,
    sequence: CommitSequence,
) -> (
    shutdown_api::StoredCommitRecordV1,
    shutdown_api::CommittedEntityMutationV1,
) {
    let command = &bundle.bundle().commands()[0];
    let plan = shutdown_api::ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    );
    let entity = &bundle.bundle().schema().entities()[0];
    let row_id =
        riffdb_types::RequestId::from_unix_milliseconds_and_random(sequence.get(), [9; 10])
            .expect("distinct Row id");
    let mut key = riffdb_types::EntityKeyBuilder::new(entity.id());
    key.push_uuid(row_id.as_bytes()).expect("Row key component");
    let target = shutdown_api::EntityTarget::new(entity.id(), key.finish().expect("Row key"))
        .expect("Row target");
    let fields = riffdb_types::CanonicalRecord::new(vec![(
        entity.primary_key_fields()[0],
        riffdb_types::CanonicalValue::Uuid(*row_id.as_bytes()),
    )])
    .expect("complete Row fields");
    let post_image = shutdown_api::StoredEntityRecordV1::new(
        target.clone(),
        riffdb_types::EntityVersion::first(),
        bundle.contract_version(),
        shutdown_api::DurableKeySchemaBindingV1::from_plan(&plan),
        fields,
    )
    .expect("first-version Row post-image");
    let mutation = shutdown_api::CommittedEntityMutationV1::new(
        shutdown_api::ExpectedEntityState::Absent,
        post_image,
    )
    .expect("genuine create-Row mutation");
    let references = vec![
        shutdown_api::CommittedEntityReferenceV2::from_mutation(&mutation)
            .expect("exact post-image reference"),
    ];
    let dependencies =
        shutdown_api::ReadDependencies::new([shutdown_api::ReadDependency::EntityObservation {
            target,
            expected: shutdown_api::ExpectedEntityState::Absent,
        }])
        .expect("create absence observation");
    let mut partition =
        riffdb_types::PartitionKeyBuilder::new(bundle.bundle().schema().aggregates()[0].id());
    partition
        .push_uuid(row_id.as_bytes())
        .expect("Row partition component");
    let partition_hash =
        riffdb_types::hash_partition_key(partition.finish().expect("Row partition").as_bytes());
    let mut conflict =
        riffdb_types::ConflictKeyBuilder::new(bundle.bundle().schema().aggregates()[0].id());
    conflict
        .push_uuid(row_id.as_bytes())
        .expect("Row conflict component");
    let conflict_hash =
        riffdb_types::hash_conflict_key(conflict.finish().expect("Row conflict key").as_bytes());
    let input_id = command
        .input()
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "id")
        .expect("Row input field")
        .id();
    // Canonical command hashing omits the idempotency-key field.
    let input = riffdb_types::CanonicalRecord::new(vec![(
        input_id,
        riffdb_types::CanonicalValue::Uuid(*row_id.as_bytes()),
    )])
    .expect("canonical command input");
    let input_hash = riffdb_types::hash_command_input(
        &riffdb_types::encode_canonical_record(&input).expect("canonical input bytes"),
    );
    let commit = shutdown_api::StoredCommitRecordV1::new(
        sequence,
        riffdb_types::RequestId::from_unix_milliseconds_and_random(sequence.get(), [5; 10])
            .expect("request"),
        plan,
        input_hash,
        riffdb_types::AdmittedActorContext::new(
            riffdb_types::ActorId::new("projection-stop-test").expect("actor"),
            riffdb_types::ActorKind::Service,
            riffdb_types::TenantScope::Global,
            None,
        ),
        riffdb_types::LogicalTime::new(
            riffdb_types::Timestamp::new(1_000, 0).expect("logical time"),
        ),
        partition_hash,
        vec![conflict_hash],
        shutdown_api::StoredReadDependenciesV1::from_live(&dependencies)
            .expect("stored create dependency"),
        references,
        Vec::new(),
        shutdown_api::DeclaredOutcome::new(
            command.success_outcome(),
            riffdb_types::CanonicalRecord::new(Vec::new()).expect("Done payload"),
        )
        .expect("successful Done outcome"),
        riffdb_types::ProvenanceId::from_unix_milliseconds_and_random(sequence.get(), [8; 10])
            .expect("provenance"),
        Vec::new(),
        shutdown_api::DurabilityMode::Sync,
    )
    .expect("complete canonical create-Row commit");
    (commit, mutation)
}

// req: PRJ-004, REC-001
#[test]
fn shutdown_during_projection_marker_prefix_replay_skips_next_internal_scan() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(SHUTDOWN_CONTRACT).expect("projection contract"),
    )
    .expect("validated bundle");
    let catalog = ShutdownCatalog {
        bundle: bundle.to_stored().expect("stored bundle"),
    };
    let active = ActiveCatalogSnapshot::read(&catalog)
        .expect("catalog read")
        .expect("active catalog");
    let plan = bundle.bundle().projections().first().expect("projection");
    let schema = CheckedProjectionSchema::new(
        bundle
            .bundle()
            .bound_projection_group_schema(plan.projection_id())
            .expect("bound schema"),
    );
    let resolved = active
        .resolve_projection(schema.identity())
        .expect("resolved projection");
    let head = FrontierPosition::AppliedThrough(CommitSequence::new(501).expect("head"));
    let control = StoredProjectionControlV1::new(
        schema.identity().clone(),
        ProjectionGeneration::first(),
        None,
        Some(ProjectionGenerationPosition::new(
            ProjectionGeneration::first(),
            head,
        )),
        None,
        ProjectionLifecycleV1::CatchingUp,
        None,
    )
    .expect("retained nonempty generation");
    let (records, source_mutations): (Vec<_>, Vec<_>) = (1..=501)
        .map(|ordinal| {
            let (record, mutation) =
                shutdown_replay_commit(&bundle, CommitSequence::new(ordinal).expect("sequence"));
            (
                shutdown_api::EncodedPageItem::new(
                    record,
                    shutdown_api::EncodedContentCharge::new(1).expect("in-memory synthetic charge"),
                ),
                mutation,
            )
        })
        .unzip();
    // Retain the source post-images, not merely commit references to unstaged
    // writes. Each Ping creates a distinct first-version Row and returns Done.
    assert_eq!(source_mutations.len(), 501);
    for (record, mutation) in records.iter().zip(&source_mutations) {
        assert_eq!(
            record.value().entity_references(),
            &[
                shutdown_api::CommittedEntityReferenceV2::from_mutation(mutation)
                    .expect("reference reciprocity")
            ]
        );
        assert_eq!(
            record.value().declared_outcome().outcome_id(),
            bundle.bundle().commands()[0].success_outcome()
        );
    }
    let replay = Arc::new(records);
    let registry = ProjectionSchemaRegistry::new(vec![schema.clone()]).expect("schema registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let mutations = Arc::new(AtomicUsize::new(0));
    let recovery_calls = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(StopState {
        requested: Mutex::new(false),
        changed: Condvar::new(),
        interrupted: AtomicBool::new(false),
    });
    let repository = ProjectionWorkerRepository::new(
        ShutdownRepository {
            control: control.clone(),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            calls: Arc::clone(&calls),
            mutations: Arc::clone(&mutations),
            first_result: ShutdownFirstResult::Status,
            clean_second_call: false,
            replay: Some(replay),
            recovery_calls: Arc::clone(&recovery_calls),
        },
        Arc::clone(&stop),
    );
    let worker_stop = Arc::clone(&stop);
    let task = thread::spawn(move || {
        let mut controller = ProjectionController::new(repository, notifier);
        // This executes unchanged marker_page -> find_commit -> scan_through;
        // it does not invoke the native recovery-page comparison before Stop.
        let result = validate_and_recover_projection_generation(
            &mut controller,
            &resolved,
            schema,
            &control,
            head,
            ProjectionGeneration::first(),
            ProjectionRecoveryPageLimit::new(NonZeroU16::new(500).expect("page rows"))
                .expect("bounded limit"),
        )
        .map_err(ProjectionWorkerError::Recovery);
        let status = ProjectionWorkerStatus::new();
        let continued = finish_projection_pass(result.as_ref().map(|_| ()), &worker_stop, &status);
        (result, continued, status.readiness())
    });
    entered.wait();
    request_stop(&stop).expect("request stop during prefix replay");
    release.wait();
    let (result, continued, readiness) = task.join().expect("bounded prefix schedule");
    assert_eq!(source_mutations.len(), 501);
    assert_eq!(
        calls.load(Ordering::Acquire),
        1,
        "every internal prefix scan must pass the stop boundary"
    );
    assert_eq!(
        recovery_calls.load(Ordering::Acquire),
        0,
        "no complete marker page was validated"
    );
    assert_eq!(mutations.load(Ordering::Acquire), 0);
    assert!(stop.interrupted.load(Ordering::Acquire));
    assert!(!continued);
    assert_eq!(readiness, ProjectionWorkerReadiness::Starting);
    assert!(
        matches!(result, Err(ProjectionWorkerError::Recovery(error)) if error.kind() == riffdb_projection::ProjectionRecoveryErrorKind::Evaluation(riffdb_projection::ProjectionEvaluationErrorKind::StorageUnavailable))
    );
}
