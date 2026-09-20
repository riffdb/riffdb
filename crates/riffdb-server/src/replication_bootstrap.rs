//! Bootstrap, recovery and continuous projection replay under follower custody.
//! These owners never grant source writers or publish partially rebuilt state.
#[path = "replication_projection_tail.rs"]
mod projection_tail;
#[path = "replication_bootstrap_receiver.rs"]
mod receiver;
#[path = "replication_bootstrap_source.rs"]
mod source;
pub use receiver::{
    BootstrapReceiverBuildJob, BootstrapReceiverConnection, BootstrapReceiverJobs,
    BootstrapReceiverTransferJob, FollowerReadSnapshots, FollowerReadView, FollowerReceiver,
    RunningFollowerReceiver,
};
pub use source::{BootstrapSourceJob, BootstrapSourceJobs, HeldBootstrapSourceJob};

use riffdb_catalog::{
    CatalogHistoryOutcome, ResolvedProjectionPlan, ValidatedCatalogHistory,
    validate_catalog_history,
};
use riffdb_projection::{evaluate_and_prepare_projection_commit, validate_projection_generation};
use riffdb_storage_api::{
    AuthoritativeScanReader, CheckedProjectionSchema, CommitScanRequest,
    ProjectionApplySnapshotReader, ProjectionApplySnapshotRequest, ProjectionControlScanV1,
    ProjectionGenerationPosition, ProjectionLifecycleV1, ProjectionRecoveryPageLimit,
    ProjectionRecoveryRepository, StartupValidationInputs, StorageError, StorageErrorKind,
    StorageScanLimit, StoredProjectionControlV1, StructuralEvidenceSession,
};
use riffdb_storage_redb::{
    RedbBootstrapCandidate, RedbPublishedBootstrapCandidate, RedbValidatedBootstrapCandidate,
};
use riffdb_types::{CommitSequence, FrontierPosition, ProjectionGeneration, ProjectionIdentity};
use std::num::NonZeroU16;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Move-only bootstrap rebuild owner. All retained published/candidate positions
/// are enumerated, including historical plans, before final complete scrub.
pub struct BootstrapProjectionRebuild {
    cancellation: Arc<AtomicBool>,
    candidate: RedbBootstrapCandidate,
    history: ValidatedCatalogHistory,
    inputs: StartupValidationInputs,
    progress: ProjectionReplayProgress,
    failed: bool,
}
/// Fully rebuilt and scrubbed private candidate. Only the rebuild worker can
/// construct this result; a storage-only scrub cannot substitute for replay.
pub struct BootstrapRebuiltCandidate {
    candidate: RedbValidatedBootstrapCandidate,
}
impl BootstrapRebuiltCandidate {
    /// Exact source manifest retained through reconstruction and validation.
    pub fn manifest(&self) -> riffdb_storage_api::ReplicationBootstrapManifestV1 {
        self.candidate.manifest()
    }

    /// Publishes only after complete semantic replay and scrub. The returned
    /// owner keeps engine exclusion through the durable replacement boundary.
    pub fn publish(
        self,
        path: &std::path::Path,
    ) -> Result<BootstrapPublishedCandidate, StorageError> {
        Ok(BootstrapPublishedCandidate {
            candidate: self.candidate.publish(path)?,
            path: path.to_path_buf(),
        })
    }
}

/// Published, fully rebuilt bootstrap with the actual engine lock retained.
/// Only this module's sealed replay result can construct it in server code.
pub struct BootstrapPublishedCandidate {
    candidate: RedbPublishedBootstrapCandidate,
    path: std::path::PathBuf,
}
impl BootstrapPublishedCandidate {
    /// Releases publication exclusion, runs the same production evidence driver
    /// and proof join, and binds the sole applier to this exact source manifest.
    /// A caller must persist its local acknowledgement before reporting attachment.
    pub fn activate(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<riffdb_storage_redb::RedbFollowerApplier, StorageError> {
        self.activate_cancellable(inputs, Arc::new(AtomicBool::new(false)))
            .map(|checked| checked.applier)
    }

    fn activate_cancellable(
        self,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<crate::startup::CheckedRedbFollowerStartup, StorageError> {
        check_cancel(&cancellation)?;
        let manifest = self.candidate.release_for_startup()?;
        let checked = crate::startup::open_redb_follower_startup_cancellable(
            &self.path,
            inputs,
            Arc::clone(&cancellation),
        )
        .map_err(|error| match error {
            crate::startup::RedbStartupError::Storage(error) => error,
            _ => corrupt(),
        })?;
        check_cancel(&cancellation)?;
        if checked.database_id != manifest.fence().history().lineage().database_id()
            || checked.applier.durable_history()? != manifest.fence().history()
        {
            return Err(corrupt());
        }
        Ok(checked)
    }

    /// Releases physical publication exclusion for unchanged follower startup.
    /// No source acknowledgement or serving readiness is inferred here.
    pub fn release_for_startup(
        self,
    ) -> Result<riffdb_storage_api::ReplicationBootstrapManifestV1, StorageError> {
        self.candidate.release_for_startup()
    }
}

struct CurrentProjection {
    control: StoredProjectionControlV1,
    resolved: ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    positions: [Option<ProjectionGenerationPosition>; 2],
    next: usize,
}
impl BootstrapProjectionRebuild {
    /// Validates the candidate's real historical evidence before evaluating rows.
    /// This proof is joined only to the exact session that produced the candidate.
    pub fn new(
        candidate: RedbBootstrapCandidate,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Self, StorageError> {
        check_cancel(&cancellation)?;
        let mut session = candidate
            .begin_catalog_preflight(inputs.clone())?
            .with_cancellation(Arc::clone(&cancellation));
        let database = session.database_id();
        let session_id = session.open_session_id();
        let result = validate_catalog_history(&mut session);
        check_cancel(&cancellation)?;
        let (history, end) = result.map_err(|_| corrupt())?.into_parts();
        let CatalogHistoryOutcome::Ready(history) = history else {
            return Err(corrupt());
        };
        if !history.matches(database, session_id) {
            return Err(corrupt());
        }
        let candidate = session.finish_preflight(end)?;
        if candidate.catalog_validation_session() != Some(session_id)
            || candidate
                .manifest()
                .fence()
                .history()
                .lineage()
                .database_id()
                != database
        {
            return Err(corrupt());
        }
        Ok(Self {
            cancellation,
            candidate,
            history,
            inputs,
            progress: ProjectionReplayProgress::default(),
            failed: false,
        })
    }

    /// Copies at most one commit's derived effects, or independently validates
    /// one finished generation with bounded pages. Returns true only after the
    /// exact end of all controls. Failure prevents further use of this owner.
    pub fn advance(&mut self) -> Result<bool, StorageError> {
        if self.failed {
            return Err(corrupt());
        }
        self.failed = true;
        check_cancel(&self.cancellation)?;
        let result = self.advance_inner()?;
        check_cancel(&self.cancellation)?;
        self.failed = false;
        Ok(result)
    }

    fn advance_inner(&mut self) -> Result<bool, StorageError> {
        let head = self
            .candidate
            .manifest()
            .fence()
            .history()
            .tail()
            .frontier()
            .application()
            .map_or(
                FrontierPosition::BeforeFirst,
                FrontierPosition::AppliedThrough,
            );
        self.progress
            .advance(&mut self.candidate, &self.history, head, &self.cancellation)
    }

    /// Runs the unchanged complete structural/catalog scrub only after every
    /// generation matched independent replay. Publication remains a separate owner.
    pub fn finish(self) -> Result<BootstrapRebuiltCandidate, StorageError> {
        if self.failed || !self.progress.complete {
            return Err(corrupt());
        }
        check_cancel(&self.cancellation)?;
        let candidate = self
            .candidate
            .validate_cancellable(self.inputs, self.cancellation)?;
        Ok(BootstrapRebuiltCandidate { candidate })
    }
}

trait ProjectionReplayStorage:
    AuthoritativeScanReader + ProjectionApplySnapshotReader + ProjectionRecoveryRepository
{
    fn persist_replay(
        &mut self,
        control: &StoredProjectionControlV1,
        request: &riffdb_storage_api::ProjectionApplyRequestV1,
    ) -> Result<(), StorageError>;
}
impl ProjectionReplayStorage for RedbBootstrapCandidate {
    fn persist_replay(
        &mut self,
        control: &StoredProjectionControlV1,
        request: &riffdb_storage_api::ProjectionApplyRequestV1,
    ) -> Result<(), StorageError> {
        self.rebuild_projection_commit(control, request).map(|_| ())
    }
}
impl ProjectionReplayStorage for riffdb_storage_redb::RedbFollowerProjectionRecovery {
    fn persist_replay(
        &mut self,
        control: &StoredProjectionControlV1,
        request: &riffdb_storage_api::ProjectionApplyRequestV1,
    ) -> Result<(), StorageError> {
        self.rebuild_projection_commit(control, request)?;
        follower_recovery_edge("replay-committed");
        Ok(())
    }
}
#[derive(Default)]
struct ProjectionReplayProgress {
    after: Option<ProjectionIdentity>,
    current: Option<CurrentProjection>,
    complete: bool,
}
impl ProjectionReplayProgress {
    fn advance<R: ProjectionReplayStorage>(
        &mut self,
        owner: &mut R,
        history: &ValidatedCatalogHistory,
        head: FrontierPosition,
        cancellation: &AtomicBool,
    ) -> Result<bool, StorageError> {
        if self.complete {
            return Ok(true);
        }
        if self.current.is_none() {
            let Some(control) = catalog_projection_after(history, self.after.as_ref(), head)?
            else {
                self.complete = true;
                return Ok(true);
            };
            let resolved = history
                .resolve_projection(control.identity())
                .map_err(|_| corrupt())?;
            let schema = resolved.checked_group_schema().map_err(|_| corrupt())?;
            let positions = [control.published(), control.candidate()];
            self.current = Some(CurrentProjection {
                control,
                resolved,
                schema,
                positions,
                next: 0,
            });
            return Ok(false);
        }
        let current = self.current.as_mut().ok_or_else(corrupt)?;
        let Some(position) = current.positions.get(current.next).copied() else {
            self.after = Some(current.control.identity().clone());
            self.current = None;
            return Ok(false);
        };
        let Some(position) = position else {
            current.next += 1;
            return Ok(false);
        };
        if !replay_projection_commit(
            owner,
            &current.resolved,
            &current.schema,
            &current.control,
            position,
            head,
        )? {
            return Ok(false);
        }
        let reader = CancellableRecovery {
            candidate: &*owner,
            cancellation,
        };
        let result = validate_projection_generation(
            &reader,
            &current.resolved,
            current.schema.clone(),
            &current.control,
            head,
            position.generation(),
            one_page()?,
        );
        check_cancel(cancellation)?;
        let _ = result.map_err(|_| corrupt())?;
        current.next += 1;
        Ok(false)
    }
}

fn catalog_projection_after(
    history: &ValidatedCatalogHistory,
    after: Option<&ProjectionIdentity>,
    head: FrontierPosition,
) -> Result<Option<StoredProjectionControlV1>, StorageError> {
    let Some(bundle) = history.active() else {
        return Ok(None);
    };
    let mut identities: Vec<ProjectionIdentity> = bundle
        .bundle()
        .projections()
        .iter()
        .map(|plan| {
            ProjectionIdentity::new(
                bundle.lineage().clone(),
                plan.projection_id(),
                plan.plan_hash(),
            )
        })
        .collect();
    identities.sort();
    let next = identities.into_iter().find(|identity| match after {
        None => true,
        Some(after) => identity > after,
    });
    let Some(identity) = next else {
        return Ok(None);
    };
    StoredProjectionControlV1::new(
        identity,
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
    .map(Some)
    .map_err(|_| corrupt())
}

/// Replays at most one missing commit. True means the generation is already at
/// its source target; callers own either independent startup validation or the
/// inductive live-prefix proof. This never advances a source control.
fn replay_projection_commit<R: ProjectionReplayStorage>(
    owner: &mut R,
    resolved: &ResolvedProjectionPlan,
    schema: &CheckedProjectionSchema,
    control: &StoredProjectionControlV1,
    position: ProjectionGenerationPosition,
    head: FrontierPosition,
) -> Result<bool, StorageError> {
    if position.frontier() > head {
        return Err(corrupt());
    }
    let local = owner
        .read_apply_snapshot(
            &ProjectionApplySnapshotRequest::new(schema.clone(), position.generation(), vec![])
                .map_err(|_| corrupt())?,
        )?
        .expected_frontier();
    if local > position.frontier() {
        return Err(corrupt());
    }
    if local == position.frontier() {
        return Ok(true);
    }
    let limit = StorageScanLimit::new(1).ok_or_else(corrupt)?;
    let (request, next) = match local {
        FrontierPosition::BeforeFirst => {
            (CommitScanRequest::initial(limit), CommitSequence::first())
        }
        FrontierPosition::AppliedThrough(sequence) => (
            CommitScanRequest::initial_after(sequence, limit),
            sequence.checked_next().ok_or_else(corrupt)?,
        ),
    };
    let page = owner.scan_commits(request)?;
    if page.inclusive_upper() != head || page.records().len() != 1 {
        return Err(corrupt());
    }
    let commit = page.records().first().ok_or_else(corrupt)?.value();
    if commit.commit_sequence() != next {
        return Err(corrupt());
    }
    let apply = evaluate_and_prepare_projection_commit(
        resolved,
        schema.clone(),
        position.generation(),
        commit,
        &*owner,
    )
    .map_err(|_| corrupt())?;
    owner.persist_replay(control, &apply)?;
    Ok(false)
}
/// Replays required local generations while retaining the original locked engine.
/// The returned store grants no readiness; ordinary full startup must follow.
pub(crate) fn recover_follower_projections(
    store: riffdb_storage_redb::RedbFollowerStore,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
) -> Result<riffdb_storage_redb::RedbFollowerStore, StorageError> {
    check_cancel(&cancellation)?;
    let session = store.begin_projection_recovery(inputs, Arc::clone(&cancellation))?;
    let owner = rebuild_follower_projection_session(session, cancellation)?;
    let store = owner.finish_rebuild()?;
    follower_recovery_edge("rebuilt");
    Ok(store)
}

/// Shared private replay under catalog proof; callers retain the final scrub gate.
pub(crate) fn rebuild_follower_projection_session(
    mut session: riffdb_storage_redb::RedbFollowerRecoveryCatalogSession,
    cancellation: Arc<AtomicBool>,
) -> Result<riffdb_storage_redb::RedbFollowerProjectionRecovery, StorageError> {
    check_cancel(&cancellation)?;
    let database = session.database_id();
    let session_id = session.open_session_id();
    let (outcome, end) = validate_catalog_history(&mut session)
        .map_err(|_| corrupt())?
        .into_parts();
    let CatalogHistoryOutcome::Ready(history) = outcome else {
        return Err(corrupt());
    };
    if !history.matches(database, session_id) {
        return Err(corrupt());
    }
    let mut owner = session.finish_preflight(end)?;
    if owner.catalog_validation_session() != session_id
        || owner.durable_history()?.lineage().database_id() != database
    {
        return Err(corrupt());
    }
    let head = owner
        .durable_history()?
        .tail()
        .frontier()
        .application()
        .map_or(
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough,
        );
    let mut progress = ProjectionReplayProgress::default();
    loop {
        check_cancel(&cancellation)?;
        if progress.advance(&mut owner, &history, head, &cancellation)? {
            break;
        }
    }
    check_cancel(&cancellation)?;
    Ok(owner)
}

fn follower_recovery_edge(_edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_FOLLOWER_PROJECTION_RECOVERY_EDGE").as_deref() == Ok(_edge) {
        std::process::exit(93);
    }
}
fn one_page() -> Result<ProjectionRecoveryPageLimit, StorageError> {
    ProjectionRecoveryPageLimit::new(NonZeroU16::MIN).map_err(|_| corrupt())
}
fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}

pub(crate) fn check_cancel(flag: &AtomicBool) -> Result<(), StorageError> {
    if flag.load(Ordering::Acquire) {
        return Err(StorageError::new(StorageErrorKind::Unavailable, None));
    }
    Ok(())
}
struct CancellableRecovery<'a, R> {
    candidate: &'a R,
    cancellation: &'a AtomicBool,
}
impl<R: AuthoritativeScanReader> AuthoritativeScanReader for CancellableRecovery<'_, R> {
    fn scan_commits(
        &self,
        request: CommitScanRequest,
    ) -> Result<riffdb_storage_api::CommitScanPageV1, StorageError> {
        check_cancel(self.cancellation)?;
        self.candidate.scan_commits(request)
    }
    fn scan_entity_partition(
        &self,
        request: riffdb_storage_api::AuthoritativeEntityPartitionScanRequest,
    ) -> Result<riffdb_storage_api::AuthoritativeEntityPartitionScanPage, StorageError> {
        check_cancel(self.cancellation)?;
        self.candidate.scan_entity_partition(request)
    }
    fn scan_index(
        &self,
        request: riffdb_storage_api::AuthoritativeIndexScanRequest,
    ) -> Result<riffdb_storage_api::AuthoritativeIndexScanPage, StorageError> {
        check_cancel(self.cancellation)?;
        self.candidate.scan_index(request)
    }
}
impl<R: ProjectionRecoveryRepository> ProjectionRecoveryRepository for CancellableRecovery<'_, R> {
    fn scan_projection_controls(
        &self,
        after: Option<&ProjectionIdentity>,
        limit: ProjectionRecoveryPageLimit,
    ) -> Result<ProjectionControlScanV1, StorageError> {
        check_cancel(self.cancellation)?;
        self.candidate.scan_projection_controls(after, limit)
    }
    fn validate_projection_recovery_page(
        &self,
        request: &riffdb_storage_api::ProjectionRecoveryValidationRequestV1,
    ) -> Result<riffdb_storage_api::ProjectionRecoveryValidationResultV1, StorageError> {
        check_cancel(self.cancellation)?;
        self.candidate.validate_projection_recovery_page(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_catalog::ValidatedContractBundle;
    use riffdb_storage_api::*;
    use riffdb_storage_redb::{RedbBootstrapMaterializer, RedbBootstrapStage};
    use riffdb_types::*;
    use std::num::NonZeroU64;

    pub(super) fn inputs() -> StartupValidationInputs {
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
        StartupValidationInputs::new(
            Timestamp::new(1_000, 0).unwrap(),
            ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        )
    }

    pub(super) fn bootstrap(
        ports: &mut riffdb_storage_redb::RedbOperationalPorts,
        database: DatabaseId,
    ) {
        let capability = CapabilityId::from_unix_milliseconds_and_random(1, [0x31; 10]).unwrap();
        let digest =
            CapabilityTokenDigest::from_hmac_bytes(DigestKeyId::new(1).unwrap(), [0x11; 32]);
        let time = Timestamp::new(100, 0).unwrap();
        let requested = CapabilityRequestedRecordV1::new(
            database,
            Environment::new("test").unwrap(),
            ActorId::new("bootstrap-test").unwrap(),
            ActorKind::Human,
            std::num::NonZeroU32::new(60).unwrap(),
            vec![Audience::new("riffdb-test").unwrap()],
            CapabilityGrantV1::new(
                TenantScope::Global,
                PartitionScopeV1::All,
                CapabilityPermissionsV1::new(vec![
                    CapabilityPermissionV1::unparameterized(
                        CapabilityPermissionKindV1::AdministerCapabilities,
                    )
                    .unwrap(),
                ])
                .unwrap(),
                vec![],
                NonZeroU16::MIN,
                vec![],
            )
            .unwrap(),
        )
        .unwrap();
        let start = BootstrapServiceAuditStartV1::new(
            RequestId::from_unix_milliseconds_and_random(1, [0x20; 10]).unwrap(),
            time,
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(capability)]).unwrap(),
            None,
        )
        .unwrap();
        let result = ports
            .bootstrap_capability(
                &CapabilityBootstrapIntentV1::new(
                    capability,
                    requested,
                    BootstrapDigestCandidatesV1::new(vec![digest], digest).unwrap(),
                    time,
                    Timestamp::new(160, 0).unwrap(),
                    start,
                )
                .unwrap(),
            )
            .unwrap();
        assert!(matches!(
            result,
            CapabilityBootstrapResult::BootstrapCreated { .. }
        ));
    }

    // req: REP-002, REP-003, REC-001
    #[test]
    fn bootstrap_worker_uses_real_catalog_evidence_and_scrubs_all_retained_projections() {
        let (_scope, path) =
            crate::real_storage_support::temporary_database_scope("bootstrap-projection-worker");
        let startup = crate::startup::open_redb_startup(
            &path,
            inputs(),
            &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
        )
        .unwrap();
        let (database, _, _, _, _, mut ports) = startup.into_parts();
        bootstrap(&mut ports, database);
        let bundle = ValidatedContractBundle::from_compiler_bundle(riffdb_contract_compiler::compile_contract_source(
            "contract BootstrapProjection version 1 { event Added { group: i64 } projection Totals { source event Added key (group) measure n = count() frontier transactionally_ordered } projection Others { source event Added where group == 2 key (group) measure n = count() frontier transactionally_ordered } }").unwrap()).unwrap();
        assert!(matches!(
            ports
                .activate_catalog(&CatalogActivationIntentV1::new(
                    None,
                    bundle.to_stored().unwrap(),
                    RequestId::from_unix_milliseconds_and_random(1, [0x21; 10]).unwrap(),
                    AuditPrincipalV1::new(
                        ActorId::new("bootstrap-test").unwrap(),
                        ActorKind::Human,
                        CapabilityId::from_unix_milliseconds_and_random(1, [0x31; 10]).unwrap(),
                        NonZeroU64::MIN
                    ),
                    Timestamp::new(100, 0).unwrap(),
                    None
                ))
                .unwrap(),
            CatalogActivationResult::Activated { .. }
        ));
        for plan in bundle.bundle().projections() {
            let schema = CheckedProjectionSchema::new(
                bundle
                    .bundle()
                    .bound_projection_group_schema(plan.projection_id())
                    .unwrap(),
            );
            let ProjectionControlResult::Updated(expected) = ports
                .transition_projection_control(ProjectionControlOperation::CreateInitial { schema })
                .unwrap()
            else {
                panic!("create control");
            };
            let ProjectionControlResult::Updated(expected) = ports
                .transition_projection_control(ProjectionControlOperation::StartInitialScan {
                    expected,
                })
                .unwrap()
            else {
                panic!("start control");
            };
            let ProjectionControlResult::Updated(expected) = ports
                .transition_projection_control(ProjectionControlOperation::PublishCandidate {
                    expected,
                })
                .unwrap()
            else {
                panic!("publish");
            };
            assert!(matches!(
                ports
                    .transition_projection_control(ProjectionControlOperation::AllocateRebuild {
                        expected
                    })
                    .unwrap(),
                ProjectionControlResult::Updated(_)
            ));
        }
        let directory = path.parent().unwrap();
        let held = ports
            .prepare_replication_bootstrap_v3(
                &directory.join("source-transfer"),
                ReplicationSourceHoldIdV1::new([0x63; 16]).unwrap(),
            )
            .unwrap();
        let manifest = held.manifest();
        let source_controls = ports
            .scan_projection_controls(
                None,
                ProjectionRecoveryPageLimit::new(NonZeroU16::new(10).unwrap()).unwrap(),
            )
            .unwrap();
        let mut stage =
            RedbBootstrapStage::create(&directory.join("receiver-transfer"), manifest).unwrap();
        for ordinal in 1..=manifest.page_count() {
            stage
                .append(&held.read_page(ordinal).unwrap().encode().unwrap())
                .unwrap();
        }
        let mut materializer = RedbBootstrapMaterializer::create(
            &directory.join("candidate"),
            stage.into_materialization_input().unwrap(),
        )
        .unwrap();
        while materializer.copy_next_page().unwrap().is_some() {}
        let cancellation = Arc::new(AtomicBool::new(false));
        let mut worker = BootstrapProjectionRebuild::new(
            materializer.finish().unwrap(),
            inputs(),
            Arc::clone(&cancellation),
        )
        .unwrap();
        assert!(!worker.advance().unwrap());
        cancellation.store(true, Ordering::Release);
        assert_eq!(
            worker.advance().unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
        assert!(worker.finish().is_err());
        let input = RedbBootstrapStage::open(&directory.join("receiver-transfer"), manifest)
            .unwrap()
            .into_materialization_input()
            .unwrap();
        let candidate = RedbBootstrapMaterializer::open(&directory.join("candidate"), input)
            .unwrap()
            .finish()
            .unwrap();
        let mut worker =
            BootstrapProjectionRebuild::new(candidate, inputs(), Arc::new(AtomicBool::new(false)))
                .unwrap();
        assert!(worker.history.active().is_some());
        let mut steps = 0;
        while !worker.advance().unwrap() {
            steps += 1;
            assert!(steps < 30);
        }
        assert!(steps >= 8, "both retained projections traversed");
        let _ = source_controls;
        // ADR-0248: do not pin source control bytes on the candidate. The
        // worker must still have walked every catalog projection.
        let rebuilt = worker.finish().unwrap();
        assert_eq!(rebuilt.manifest(), manifest);
        let target = directory.join("follower.redb");
        rebuilt
            .publish(&target)
            .unwrap()
            .release_for_startup()
            .unwrap();
        let recovered = recover_follower_projections(
            riffdb_storage_redb::RedbFollowerStore::open(&target).unwrap(),
            inputs(),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        assert!(
            riffdb_storage_redb::RedbFollowerStore::open(&target).is_err(),
            "recovery keeps engine custody through dormant handoff"
        );
        drop(recovered);
        let checked = crate::startup::open_redb_follower_startup(&target, inputs()).unwrap();
        assert_eq!(
            checked.applier.durable_history().unwrap(),
            manifest.fence().history()
        );
        assert_eq!(
            bundle.bundle().projections().len(),
            2,
            "both catalog projections must remain declared"
        );
        let _ = source_controls;
    }
}

#[cfg(test)]
#[path = "replication_projection_recovery_tests.rs"]
mod projection_recovery_tests;
