//! Projection prefix, generation, duplicate-apply, and recovery acceptance test.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::num::NonZeroU16;
use std::time::{Duration, Instant};

use riffdb_catalog::{ActiveCatalogSnapshot, ResolvedProjectionPlan, ValidatedContractBundle};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_projection::{
    MAX_PROJECTION_WAITERS, ProjectionController, ProjectionEvaluationErrorKind,
    ProjectionGenerationValidationOutcome, ProjectionInitializationResult, ProjectionNotifier,
    ProjectionReadOutcome, ProjectionReadSource, ProjectionRecoveryOutcome,
    ProjectionSchemaRegistry, ProjectionWaitCancellation, ProjectionWaitRegistration,
    ProjectionWake, evaluate_projection_commit, prepare_projection_apply,
    validate_and_recover_projection_generation, validate_projection_generation,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest,
    AuthoritativeScanReader, CatalogRepository, CheckedProjectionSchema,
    CleanProjectionGenerationV1, CommitScanPageV1, CommitScanRequest, DeclaredOutcome,
    DurabilityMode, EncodedContentCharge, EncodedPageItem, ExecutablePlanRef,
    ProjectionApplyRequestV1, ProjectionApplyResult, ProjectionApplyRowObservation,
    ProjectionApplySnapshot, ProjectionApplySnapshotReader, ProjectionApplySnapshotRequest,
    ProjectionControlOperation, ProjectionControlResult, ProjectionControlScanV1,
    ProjectionFailureCodeV1, ProjectionLifecycleV1, ProjectionLowerContinuation,
    ProjectionMutationRepository, ProjectionQueryReader, ProjectionQueryRequest,
    ProjectionQueryResult, ProjectionRecoveryContinuationV1, ProjectionRecoveryExpectedPageV1,
    ProjectionRecoveryFindingCodeV1, ProjectionRecoveryFindingV1, ProjectionRecoveryPageLimit,
    ProjectionRecoveryRepository, ProjectionRecoveryValidationRequestV1,
    ProjectionRecoveryValidationResultV1, ProjectionRowPrior, ProjectionRowUpdateV1,
    ProjectionStatus, ProjectionUnavailableReason, ReadDependencies, StorageError,
    StorageErrorKind, StoredCommitRecordV1, StoredContractBundleV1, StoredDurableEventV1,
    StoredProjectionApplyV1, StoredProjectionControlV1, StoredProjectionStateV1,
    StoredReadDependenciesV1, derive_event_hash_v1, evaluate_projection_control_operation,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash, CanonicalRecord,
    CanonicalValue, CommitSequence, ContractLineage, EventId, FieldId, FrontierPosition,
    LogicalTime, OutcomeId, PartitionKeyBuilder, ProjectionApplyKey, ProjectionGeneration,
    ProjectionGroupKey, ProjectionIdentity, ProvenanceId, RequestId, TenantScope, Timestamp,
    hash_partition_key,
};

const CONTRACT: &str = r#"
contract ProjectionAcceptance version 1 {
  event Added {
    group: i64
    amount: i64
  }

  projection Totals {
    source event Added
    key (group)
    measure item_count = count()
    measure total = sum(amount)
    frontier transactionally_ordered
  }
}
"#;

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

#[derive(Clone)]
struct SemanticRepository {
    head: FrontierPosition,
    control: Option<StoredProjectionControlV1>,
    rows: BTreeMap<ProjectionGroupKey, StoredProjectionStateV1>,
    markers: BTreeMap<ProjectionApplyKey, StoredProjectionApplyV1>,
    commits: Vec<EncodedPageItem<StoredCommitRecordV1>>,
}

impl Default for SemanticRepository {
    fn default() -> Self {
        Self {
            head: FrontierPosition::BeforeFirst,
            control: None,
            rows: BTreeMap::new(),
            markers: BTreeMap::new(),
            commits: Vec::new(),
        }
    }
}

impl SemanticRepository {
    fn with_head(head: CommitSequence) -> Self {
        Self {
            head: FrontierPosition::AppliedThrough(head),
            ..Self::default()
        }
    }

    fn set_head(&mut self, head: CommitSequence) {
        self.head = FrontierPosition::AppliedThrough(head);
    }

    fn install_commits(&mut self, commits: impl IntoIterator<Item = StoredCommitRecordV1>) {
        self.commits = commits
            .into_iter()
            .map(|commit| {
                EncodedPageItem::new(
                    commit,
                    EncodedContentCharge::new(1).expect("synthetic charge"),
                )
            })
            .collect();
        self.head = self
            .commits
            .last()
            .map_or(FrontierPosition::BeforeFirst, |commit| {
                FrontierPosition::AppliedThrough(commit.value().commit_sequence())
            });
    }
}

impl riffdb_storage_api::ProjectionBatchSnapshot for SemanticRepository {
    fn control(&self) -> &StoredProjectionControlV1 {
        self.control.as_ref().expect("captured control")
    }
}

impl ProjectionApplySnapshotReader for SemanticRepository {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        let control = self.control.as_ref().ok_or_else(integrity)?;
        let frontier = control
            .frontier_for(request.generation())
            .ok_or_else(integrity)?;
        let rows = request
            .group_keys()
            .iter()
            .map(|key| {
                self.rows.get(key).cloned().map_or_else(
                    || ProjectionApplyRowObservation::Absent(key.clone()),
                    ProjectionApplyRowObservation::Present,
                )
            })
            .collect();
        ProjectionApplySnapshot::new(request, frontier, rows).map_err(|_| integrity())
    }
}

impl ProjectionMutationRepository for SemanticRepository {
    fn apply_projection(
        &mut self,
        request: &ProjectionApplyRequestV1,
    ) -> Result<ProjectionApplyResult, StorageError> {
        let Some(control) = self.control.as_ref() else {
            return Ok(ProjectionApplyResult::StateChanged);
        };
        let Some(frontier) = control.frontier_for(request.generation()) else {
            return Ok(ProjectionApplyResult::StateChanged);
        };
        let marker_key = ProjectionApplyKey::new(
            request.identity().clone(),
            request.generation(),
            request.sequence(),
        );
        if frontier >= FrontierPosition::AppliedThrough(request.sequence()) {
            let marker = self.markers.get(&marker_key).ok_or_else(integrity)?;
            if marker.canonical_hash() != request.apply_hash() {
                return Err(integrity());
            }
            return Ok(ProjectionApplyResult::AlreadyApplied(marker.clone()));
        }
        if frontier != request.expected_frontier()
            || !control.permits_application(request.generation())
        {
            return Ok(ProjectionApplyResult::StateChanged);
        }
        for update in request.row_updates() {
            let prior_matches = match (update.prior(), self.rows.get(update.key())) {
                (ProjectionRowPrior::Absent, None) => true,
                (ProjectionRowPrior::Present(expected), Some(current)) => {
                    current.last_changed_sequence() == expected
                }
                _ => false,
            };
            if !prior_matches {
                return Ok(ProjectionApplyResult::StateChanged);
            }
        }
        let next_control = control
            .after_apply(
                request.generation(),
                request.expected_frontier(),
                request.sequence(),
            )
            .map_err(|_| integrity())?;
        let marker = StoredProjectionApplyV1::new(marker_key.clone(), request.apply_hash());
        for update in request.row_updates() {
            let row = StoredProjectionStateV1::new(
                request.schema(),
                update.key().clone(),
                update.measures().clone(),
                request.sequence(),
            )
            .map_err(|_| integrity())?;
            self.rows.insert(update.key().clone(), row);
        }
        self.markers.insert(marker_key, marker.clone());
        self.control = Some(next_control.clone());
        Ok(ProjectionApplyResult::Applied {
            marker,
            control: next_control,
        })
    }

    fn transition_projection_control(
        &mut self,
        operation: ProjectionControlOperation,
    ) -> Result<ProjectionControlResult, StorageError> {
        let result =
            evaluate_projection_control_operation(self.control.as_ref(), &operation, self.head)
                .map_err(|_| integrity())?;
        if let ProjectionControlResult::Updated(control) = &result {
            self.control = Some(control.clone());
        }
        Ok(result)
    }
}

impl ProjectionQueryReader for SemanticRepository {
    fn query_projection(
        &self,
        request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError> {
        let Some(control) = self.control.as_ref() else {
            return Ok(ProjectionQueryResult::Degraded {
                generation: None,
                current: FrontierPosition::BeforeFirst,
                reason: ProjectionUnavailableReason::Building,
            });
        };
        match control.lifecycle() {
            ProjectionLifecycleV1::Building | ProjectionLifecycleV1::CatchingUp => {
                return Ok(ProjectionQueryResult::Degraded {
                    generation: control.candidate().map(|value| value.generation()),
                    current: control
                        .candidate()
                        .map_or(FrontierPosition::BeforeFirst, |value| value.frontier()),
                    reason: ProjectionUnavailableReason::Building,
                });
            }
            ProjectionLifecycleV1::Rebuilding => {
                return Ok(ProjectionQueryResult::Degraded {
                    generation: control.candidate().map(|value| value.generation()),
                    current: control
                        .candidate()
                        .map_or(FrontierPosition::BeforeFirst, |value| value.frontier()),
                    reason: ProjectionUnavailableReason::Rebuilding,
                });
            }
            ProjectionLifecycleV1::Degraded => {
                let failure = control.failure().ok_or_else(integrity)?;
                let current = control
                    .frontier_for(failure.generation())
                    .ok_or_else(integrity)?;
                return Ok(ProjectionQueryResult::Degraded {
                    generation: Some(failure.generation()),
                    current,
                    reason: ProjectionUnavailableReason::Failure(failure.code()),
                });
            }
            ProjectionLifecycleV1::Invalid => {
                return Ok(ProjectionQueryResult::Invalid {
                    generation: control.failure().ok_or_else(integrity)?.generation(),
                    current: control
                        .failure()
                        .and_then(|failure| control.frontier_for(failure.generation()))
                        .ok_or_else(integrity)?,
                    reason: control.failure().ok_or_else(integrity)?.code(),
                });
            }
            ProjectionLifecycleV1::Ready => {}
        }
        let published = control.published().ok_or_else(integrity)?;
        let prefix = request
            .selector()
            .schema()
            .group_prefix(
                published.generation(),
                request.selector().leading_components(),
            )
            .map_err(|_| integrity())?;
        if request.continuation().is_some_and(|continuation| {
            continuation.identity() != request.selector().identity()
                || continuation.generation() != published.generation()
                || continuation.prefix() != prefix.as_bytes()
                || continuation.observed_frontier() != published.frontier()
        }) {
            return Ok(ProjectionQueryResult::ContinuationInvalidated);
        }
        let after = request
            .continuation()
            .map(ProjectionLowerContinuation::exclusive_last_key);
        let mut matching = self
            .rows
            .values()
            .filter(|row| {
                row.generation() == published.generation()
                    && row.key().as_bytes().starts_with(prefix.as_bytes())
                    && after.is_none_or(|after| row.key() > after)
            })
            .cloned()
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| left.key().cmp(right.key()));
        let limit = usize::from(request.limit().get());
        let has_more = matching.len() > limit;
        matching.truncate(limit);
        let rows = matching
            .iter()
            .cloned()
            .map(|row| {
                EncodedPageItem::new(row, EncodedContentCharge::new(1).expect("synthetic charge"))
            })
            .collect::<Vec<_>>();
        let next = has_more
            .then(|| {
                ProjectionLowerContinuation::new(
                    request.selector(),
                    published.generation(),
                    matching
                        .last()
                        .expect("nonempty limited page")
                        .key()
                        .clone(),
                    published.frontier(),
                )
            })
            .transpose()
            .map_err(|_| integrity())?;
        ProjectionQueryResult::ready(
            request,
            published.generation(),
            published.frontier(),
            rows,
            next,
        )
        .map_err(|_| integrity())
    }

    fn read_projection_status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        Ok(self.control.as_ref().map_or_else(
            || ProjectionStatus::uninitialized(identity.clone(), self.head),
            |control| ProjectionStatus::from_control(control, self.head),
        ))
    }
}

impl ProjectionQueryReader for &SemanticRepository {
    fn query_projection(
        &self,
        request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError> {
        <SemanticRepository as ProjectionQueryReader>::query_projection(self, request)
    }

    fn read_projection_status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        <SemanticRepository as ProjectionQueryReader>::read_projection_status(self, identity)
    }
}

impl AuthoritativeScanReader for SemanticRepository {
    fn scan_index(
        &self,
        _request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        Err(integrity())
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        let inclusive_upper = request
            .inclusive_upper()
            .map_or(self.head, FrontierPosition::AppliedThrough);
        let after = request.after();
        let start = self.commits.partition_point(|commit| {
            after.is_some_and(|after| commit.value().commit_sequence() <= after)
        });
        let upper_end = self.commits.partition_point(|commit| {
            FrontierPosition::AppliedThrough(commit.value().commit_sequence()) <= inclusive_upper
        });
        let end = start
            .saturating_add(usize::from(request.limit().get()))
            .min(upper_end);
        let records = self.commits[start..end].to_vec();
        if end < upper_end {
            CommitScanPageV1::page(
                request,
                inclusive_upper,
                records,
                self.commits[end - 1].value().commit_sequence(),
            )
            .map_err(|_| integrity())
        } else {
            CommitScanPageV1::exact_end(request, inclusive_upper, records).map_err(|_| integrity())
        }
    }
}

impl ProjectionRecoveryRepository for SemanticRepository {
    fn scan_projection_controls(
        &self,
        after: Option<&ProjectionIdentity>,
        _limit: ProjectionRecoveryPageLimit,
    ) -> Result<ProjectionControlScanV1, StorageError> {
        let controls = self
            .control
            .as_ref()
            .filter(|control| after.is_none_or(|after| control.identity() > after))
            .cloned()
            .map(|control| {
                EncodedPageItem::new(
                    control,
                    EncodedContentCharge::new(1).expect("synthetic charge"),
                )
            })
            .into_iter()
            .collect();
        ProjectionControlScanV1::page(controls, false).map_err(|_| integrity())
    }

    fn validate_projection_recovery_page(
        &self,
        request: &ProjectionRecoveryValidationRequestV1,
    ) -> Result<ProjectionRecoveryValidationResultV1, StorageError> {
        if self.control.as_ref() != Some(request.expected_control())
            || self.head != request.expected_authoritative_head()
        {
            return Ok(ProjectionRecoveryValidationResultV1::FenceChanged);
        }
        let finding = |code| {
            ProjectionRecoveryValidationResultV1::Finding(ProjectionRecoveryFindingV1::new(
                request.schema().identity().clone(),
                request.generation(),
                code,
            ))
        };
        let markers = self
            .markers
            .values()
            .filter(|marker| {
                marker.key().identity() == request.schema().identity()
                    && marker.key().generation() == request.generation()
            })
            .cloned()
            .collect::<Vec<_>>();
        let rows = self
            .rows
            .values()
            .filter(|row| {
                row.identity() == request.schema().identity()
                    && row.generation() == request.generation()
            })
            .cloned()
            .collect::<Vec<_>>();
        if request.expected_position().frontier() == FrontierPosition::BeforeFirst {
            return if markers.is_empty() && rows.is_empty() {
                Ok(ProjectionRecoveryValidationResultV1::ExactEnd(
                    CleanProjectionGenerationV1::new(
                        request.schema().identity().clone(),
                        request.expected_position(),
                        0,
                    ),
                ))
            } else {
                Ok(finding(
                    ProjectionRecoveryFindingCodeV1::BeforeFirstNotEmpty,
                ))
            };
        }

        match request.expected_page() {
            ProjectionRecoveryExpectedPageV1::Markers(expected) => {
                let after = match request.continuation() {
                    None => None,
                    Some(ProjectionRecoveryContinuationV1::Markers { after }) => Some(*after),
                    Some(ProjectionRecoveryContinuationV1::Rows { .. }) => {
                        return Err(integrity());
                    }
                };
                let start = markers.partition_point(|marker| {
                    after.is_some_and(|after| marker.key().commit_sequence() <= after)
                });
                let end = start
                    .saturating_add(usize::from(request.limit().get().get()))
                    .min(markers.len());
                let actual = &markers[start..end];
                if actual != expected {
                    return Ok(finding(ProjectionRecoveryFindingCodeV1::MarkerMismatch));
                }
                let FrontierPosition::AppliedThrough(frontier) =
                    request.expected_position().frontier()
                else {
                    return Err(integrity());
                };
                if markers
                    .get(end)
                    .is_some_and(|marker| marker.key().commit_sequence() > frontier)
                {
                    return Ok(finding(
                        ProjectionRecoveryFindingCodeV1::MarkerAboveFrontier,
                    ));
                }
                if end < markers.len() {
                    let last = actual.last().ok_or_else(integrity)?;
                    return Ok(ProjectionRecoveryValidationResultV1::Page {
                        continuation: ProjectionRecoveryContinuationV1::markers(
                            last.key().commit_sequence(),
                        ),
                    });
                }
                if actual
                    .last()
                    .map(|marker| marker.key().commit_sequence())
                    .or(after)
                    != Some(frontier)
                {
                    return Ok(finding(
                        ProjectionRecoveryFindingCodeV1::MarkerSequenceMismatch,
                    ));
                }
                Ok(ProjectionRecoveryValidationResultV1::Page {
                    continuation: ProjectionRecoveryContinuationV1::rows(None, 0),
                })
            }
            ProjectionRecoveryExpectedPageV1::Rows(expected) => {
                let (after, validated_rows) = match request.continuation() {
                    Some(ProjectionRecoveryContinuationV1::Rows {
                        after,
                        validated_rows,
                    }) => (after.as_ref(), *validated_rows),
                    None | Some(ProjectionRecoveryContinuationV1::Markers { .. }) => {
                        return Err(integrity());
                    }
                };
                let start =
                    rows.partition_point(|row| after.is_some_and(|after| row.key() <= after));
                let end = start
                    .saturating_add(usize::from(request.limit().get().get()))
                    .min(rows.len());
                let actual = &rows[start..end];
                if actual != expected {
                    return Ok(finding(ProjectionRecoveryFindingCodeV1::StateMismatch));
                }
                let observed = u64::try_from(actual.len()).map_err(|_| integrity())?;
                let validated_rows = validated_rows.checked_add(observed).ok_or_else(integrity)?;
                if end < rows.len() {
                    let last = actual.last().ok_or_else(integrity)?;
                    return Ok(ProjectionRecoveryValidationResultV1::Page {
                        continuation: ProjectionRecoveryContinuationV1::rows(
                            Some(last.key().clone()),
                            validated_rows,
                        ),
                    });
                }
                Ok(ProjectionRecoveryValidationResultV1::ExactEnd(
                    CleanProjectionGenerationV1::new(
                        request.schema().identity().clone(),
                        request.expected_position(),
                        validated_rows,
                    ),
                ))
            }
        }
    }
}

enum ScriptedSignal {
    Notify(ProjectionNotifier, ProjectionIdentity),
    Cancel(ProjectionWaitCancellation),
}

struct ScriptedQueryReader {
    responses: RefCell<VecDeque<ProjectionQueryResult>>,
    after_first_read: RefCell<Option<ScriptedSignal>>,
    status: ProjectionStatus,
}

impl ScriptedQueryReader {
    fn new(
        responses: impl IntoIterator<Item = ProjectionQueryResult>,
        status: ProjectionStatus,
        after_first_read: Option<ScriptedSignal>,
    ) -> Self {
        Self {
            responses: RefCell::new(responses.into_iter().collect()),
            after_first_read: RefCell::new(after_first_read),
            status,
        }
    }
}

impl ProjectionQueryReader for ScriptedQueryReader {
    fn query_projection(
        &self,
        _request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError> {
        let response = self
            .responses
            .borrow_mut()
            .pop_front()
            .ok_or_else(integrity)?;
        if let Some(signal) = self.after_first_read.borrow_mut().take() {
            match signal {
                ScriptedSignal::Notify(notifier, identity) => {
                    notifier.notify(&identity).map_err(|_| integrity())?;
                }
                ScriptedSignal::Cancel(cancellation) => {
                    cancellation.cancel().map_err(|_| integrity())?;
                }
            }
        }
        Ok(response)
    }

    fn read_projection_status(
        &self,
        _identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        Ok(self.status.clone())
    }
}

struct TimeoutCapacityProbeReader {
    responses: RefCell<VecDeque<ProjectionQueryResult>>,
    status: ProjectionStatus,
    notifier: ProjectionNotifier,
    identity: ProjectionIdentity,
    query_count: Cell<usize>,
    final_reread_registration: RefCell<Option<ProjectionWaitRegistration>>,
}

impl TimeoutCapacityProbeReader {
    fn new(
        responses: impl IntoIterator<Item = ProjectionQueryResult>,
        status: ProjectionStatus,
        notifier: ProjectionNotifier,
        identity: ProjectionIdentity,
    ) -> Self {
        Self {
            responses: RefCell::new(responses.into_iter().collect()),
            status,
            notifier,
            identity,
            query_count: Cell::new(0),
            final_reread_registration: RefCell::new(None),
        }
    }
}

impl ProjectionQueryReader for TimeoutCapacityProbeReader {
    fn query_projection(
        &self,
        _request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError> {
        let query_count = self.query_count.get() + 1;
        self.query_count.set(query_count);
        if query_count == 2 {
            let registration = self
                .notifier
                .register(self.identity.clone())
                .map_err(|_| integrity())?;
            self.final_reread_registration
                .borrow_mut()
                .replace(registration);
        }
        self.responses
            .borrow_mut()
            .pop_front()
            .ok_or_else(integrity)
    }

    fn read_projection_status(
        &self,
        _identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        Ok(self.status.clone())
    }
}

fn integrity() -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}

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
    let bundle =
        ValidatedContractBundle::from_compiler_bundle(compiled).expect("validated catalog bundle");
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
            .map(riffdb_contract_ir::FieldSchema::id)
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

fn uuid_bytes(seed: u8) -> [u8; 16] {
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
    let partition_hash = hash_partition_key(partition.finish().expect("partition key").as_bytes());
    let dependencies = StoredReadDependenciesV1::from_live(
        &ReadDependencies::new(Vec::new()).expect("empty dependencies"),
    )
    .expect("stored dependencies");
    StoredCommitRecordV1::new(
        sequence,
        RequestId::from_bytes(uuid_bytes(u8::try_from(sequence.get()).unwrap_or(0x21)))
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
        ProvenanceId::from_bytes(uuid_bytes(
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

fn fixture() -> (
    CheckedProjectionSchema,
    Vec<(riffdb_types::FieldId, bool)>,
    ProjectionIdentity,
) {
    let bundle = compile_contract_source(CONTRACT).expect("compile projection contract");
    let plan = bundle.projections().first().expect("projection");
    let count_field = plan
        .measures()
        .iter()
        .find(|measure| measure.expression().is_none())
        .expect("count measure")
        .field()
        .id();
    let bound = bundle
        .bound_projection_group_schema(plan.projection_id())
        .expect("bound schema");
    let measure_fields = bound
        .schema()
        .measures()
        .fields()
        .iter()
        .map(|field| (field.id(), field.id() == count_field))
        .collect();
    let schema = CheckedProjectionSchema::new(bound);
    let identity = schema.identity().clone();
    (schema, measure_fields, identity)
}

fn sequence(value: u64) -> CommitSequence {
    CommitSequence::new(value).expect("nonzero sequence")
}

fn measures(fields: &[(riffdb_types::FieldId, bool)], count: u64, total: i64) -> CanonicalRecord {
    CanonicalRecord::new(
        fields
            .iter()
            .map(|(field, is_count)| {
                (
                    *field,
                    if *is_count {
                        CanonicalValue::U64(count)
                    } else {
                        CanonicalValue::I64(total)
                    },
                )
            })
            .collect(),
    )
    .expect("measure record")
}

fn empty_ready(
    schema: &CheckedProjectionSchema,
    generation: ProjectionGeneration,
    frontier: FrontierPosition,
) -> ProjectionQueryResult {
    let selector = riffdb_storage_api::ProjectionQuerySelector::new(schema.clone(), Vec::new())
        .expect("selector");
    let request = ProjectionQueryRequest::new(selector, NonZeroU16::new(10).expect("limit"), None)
        .expect("query request");
    ProjectionQueryResult::ready(&request, generation, frontier, Vec::new(), None)
        .expect("ready result")
}

fn apply_row(
    controller: &mut ProjectionController<SemanticRepository>,
    schema: &CheckedProjectionSchema,
    generation: ProjectionGeneration,
    sequence: CommitSequence,
    group: i64,
    values: CanonicalRecord,
) -> ProjectionApplyRequestV1 {
    let key = schema
        .group_key(generation, &[CanonicalValue::I64(group)])
        .expect("group key");
    let snapshot_request =
        ProjectionApplySnapshotRequest::new(schema.clone(), generation, vec![key.clone()])
            .expect("snapshot request");
    let snapshot = controller
        .repository()
        .read_apply_snapshot(&snapshot_request)
        .expect("snapshot");
    let prior = match &snapshot.rows()[0] {
        ProjectionApplyRowObservation::Absent(_) => ProjectionRowPrior::Absent,
        ProjectionApplyRowObservation::Present(row) => {
            ProjectionRowPrior::Present(row.last_changed_sequence())
        }
    };
    let update =
        ProjectionRowUpdateV1::new(schema, key, prior, values).expect("checked row update");
    let request = ProjectionApplyRequestV1::new(
        schema.clone(),
        generation,
        sequence,
        snapshot.expected_frontier(),
        vec![update],
    )
    .expect("checked apply");
    assert!(matches!(
        controller.apply(&request).expect("atomic apply"),
        ProjectionApplyResult::Applied { .. }
    ));
    request
}

#[test]
fn catalog_normalized_evaluator_is_identical_for_live_and_rebuild_and_marks_empty_commits() {
    let fixture = evaluator_fixture();
    let identity = fixture.schema.identity().clone();
    let registry =
        ProjectionSchemaRegistry::new([fixture.schema.clone()]).expect("projection registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let mut controller =
        ProjectionController::new(SemanticRepository::with_head(sequence(3)), notifier);
    controller
        .initialize(fixture.schema.clone())
        .expect("initialize");
    controller
        .start_initial_catch_up(&identity)
        .expect("initial catch-up");

    let commits = [
        evaluator_commit(&fixture, sequence(1), &[(10, 7), (10, 3)]),
        evaluator_commit(&fixture, sequence(2), &[]),
        evaluator_commit(&fixture, sequence(3), &[(20, 4)]),
    ];
    let generation_one = ProjectionGeneration::first();
    let mut first_hashes = Vec::new();
    for commit in &commits {
        let evaluated = evaluate_projection_commit(
            &fixture.resolved,
            fixture.schema.clone(),
            generation_one,
            commit,
        )
        .expect("checked live evaluation");
        let request =
            prepare_projection_apply(&evaluated, controller.repository()).expect("live apply");
        if commit.commit_sequence() == sequence(2) {
            assert!(
                request.row_updates().is_empty(),
                "an empty commit still produces one marker-only apply"
            );
        }
        if commit.commit_sequence() == sequence(3) {
            assert_eq!(
                request.row_updates().len(),
                0,
                "the filtered event cannot create a group"
            );
        }
        first_hashes.push(request.apply_hash());
        assert!(matches!(
            controller.apply(&request).expect("atomic live apply"),
            ProjectionApplyResult::Applied { .. }
        ));
    }
    controller
        .publish_candidate(&identity)
        .expect("publish live generation");

    let live_rows = controller
        .repository()
        .rows
        .values()
        .filter(|row| row.generation() == generation_one)
        .map(|row| (row.group_values().to_vec(), row.measures().clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        live_rows,
        vec![(
            vec![CanonicalValue::I64(10)],
            measures(&fixture.measure_fields, 2, 10),
        ),]
    );

    let ProjectionControlResult::Updated(rebuilding) = controller
        .allocate_rebuild(&identity)
        .expect("allocate rebuild")
    else {
        panic!("rebuild control");
    };
    let generation_two = rebuilding.candidate().expect("candidate").generation();
    for commit in &commits {
        let evaluated = evaluate_projection_commit(
            &fixture.resolved,
            fixture.schema.clone(),
            generation_two,
            commit,
        )
        .expect("checked rebuild evaluation");
        let request =
            prepare_projection_apply(&evaluated, controller.repository()).expect("rebuild apply");
        assert!(matches!(
            controller.apply(&request).expect("atomic rebuild apply"),
            ProjectionApplyResult::Applied { .. }
        ));
        assert!(matches!(
            controller.apply(&request).expect("equal duplicate"),
            ProjectionApplyResult::AlreadyApplied(_)
        ));
    }
    let rebuilt_rows = controller
        .repository()
        .rows
        .values()
        .filter(|row| row.generation() == generation_two)
        .map(|row| (row.group_values().to_vec(), row.measures().clone()))
        .collect::<Vec<_>>();
    assert_eq!(rebuilt_rows, live_rows);
    assert_eq!(first_hashes.len(), 3);
}

#[test]
fn grouped_sum_overflow_fails_before_state_or_frontier_changes() {
    let fixture = evaluator_fixture();
    let commit = evaluator_commit(
        &fixture,
        CommitSequence::first(),
        &[(10, i64::MAX), (10, 1)],
    );
    assert_eq!(
        evaluate_projection_commit(
            &fixture.resolved,
            fixture.schema,
            ProjectionGeneration::first(),
            &commit,
        )
        .expect_err("checked aggregate overflow")
        .kind(),
        ProjectionEvaluationErrorKind::ArithmeticOverflow
    );
}

#[test]
fn fenced_recovery_detects_hash_mismatch_and_allocates_a_new_generation() {
    let fixture = evaluator_fixture();
    let identity = fixture.schema.identity().clone();
    let commits = vec![
        evaluator_commit(&fixture, sequence(1), &[(10, 7)]),
        evaluator_commit(&fixture, sequence(2), &[]),
        evaluator_commit(&fixture, sequence(3), &[(10, 3)]),
    ];
    let mut repository = SemanticRepository::default();
    repository.install_commits(commits.clone());
    let registry =
        ProjectionSchemaRegistry::new([fixture.schema.clone()]).expect("projection registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let mut controller = ProjectionController::new(repository, notifier);
    controller
        .initialize(fixture.schema.clone())
        .expect("initialize");
    controller
        .start_initial_catch_up(&identity)
        .expect("start catch-up");
    let generation = ProjectionGeneration::first();
    for commit in &commits {
        let evaluated = evaluate_projection_commit(
            &fixture.resolved,
            fixture.schema.clone(),
            generation,
            commit,
        )
        .expect("replayable evaluation");
        let request =
            prepare_projection_apply(&evaluated, controller.repository()).expect("apply request");
        controller.apply(&request).expect("atomic apply");
    }
    let ProjectionControlResult::Updated(ready) =
        controller.publish_candidate(&identity).expect("publish")
    else {
        panic!("ready control");
    };
    let page_limit = ProjectionRecoveryPageLimit::new(NonZeroU16::new(2).expect("limit"))
        .expect("checked limit");
    assert!(matches!(
        validate_projection_generation(
            controller.repository(),
            &fixture.resolved,
            fixture.schema.clone(),
            &ready,
            FrontierPosition::AppliedThrough(sequence(3)),
            generation,
            page_limit,
        )
        .expect("clean validation"),
        ProjectionGenerationValidationOutcome::Clean(_)
    ));

    let ProjectionControlResult::Updated(rebuilding) = controller
        .allocate_rebuild(&identity)
        .expect("operator rebuild")
    else {
        panic!("rebuilding control");
    };
    assert!(matches!(
        validate_projection_generation(
            controller.repository(),
            &fixture.resolved,
            fixture.schema.clone(),
            &ready,
            FrontierPosition::AppliedThrough(sequence(3)),
            generation,
            page_limit,
        )
        .expect("stale fence classification"),
        ProjectionGenerationValidationOutcome::FenceChanged
    ));

    let marker_key = ProjectionApplyKey::new(identity.clone(), generation, sequence(2));
    let original_hash = controller
        .repository()
        .markers
        .get(&marker_key)
        .expect("marker")
        .canonical_hash();
    let corrupt_hash = riffdb_types::ProjectionApplyHash::from_bytes([0xe1; 32]);
    assert_ne!(corrupt_hash, original_hash);
    controller.repository_mut().markers.insert(
        marker_key.clone(),
        StoredProjectionApplyV1::new(marker_key, corrupt_hash),
    );
    let outcome = validate_and_recover_projection_generation(
        &mut controller,
        &fixture.resolved,
        fixture.schema,
        &rebuilding,
        FrontierPosition::AppliedThrough(sequence(3)),
        generation,
        page_limit,
    )
    .expect("mismatch recovery");
    let ProjectionRecoveryOutcome::RebuildAllocated {
        failed_generation,
        replacement_generation,
    } = outcome
    else {
        panic!("fresh rebuild allocation");
    };
    assert_eq!(failed_generation, generation);
    assert!(replacement_generation > failed_generation);
    assert!(
        controller
            .repository()
            .rows
            .values()
            .any(|row| row.generation() == generation),
        "published state is retained rather than repaired or deleted"
    );
    assert_eq!(
        controller
            .repository()
            .markers
            .get(&ProjectionApplyKey::new(identity, generation, sequence(2)))
            .expect("corrupt marker remains quarantined")
            .canonical_hash(),
        corrupt_hash
    );
}

#[test]
fn prefix_frontier_duplicate_rebuild_and_recovery_are_generation_safe() {
    let (schema, measure_fields, identity) = fixture();
    let registry = ProjectionSchemaRegistry::new([schema.clone()]).expect("registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let repository = SemanticRepository::with_head(sequence(2));
    let mut controller = ProjectionController::new(repository, notifier.clone());

    assert!(matches!(
        controller.initialize(schema.clone()).expect("initialize"),
        ProjectionInitializationResult::Initialized(_)
    ));
    controller
        .start_initial_catch_up(&identity)
        .expect("start initial catch-up");
    let generation_one = ProjectionGeneration::first();
    let first_request = apply_row(
        &mut controller,
        &schema,
        generation_one,
        sequence(1),
        10,
        measures(&measure_fields, 1, 7),
    );
    assert!(matches!(
        controller.apply(&first_request).expect("equal duplicate"),
        ProjectionApplyResult::AlreadyApplied(_)
    ));
    apply_row(
        &mut controller,
        &schema,
        generation_one,
        sequence(2),
        20,
        measures(&measure_fields, 1, 9),
    );
    controller
        .publish_candidate(&identity)
        .expect("publish initial generation");

    let source =
        ProjectionReadSource::new(controller.repository(), registry.clone(), notifier.clone());
    let ProjectionQueryResult::Ready {
        generation,
        frontier,
        rows,
        next,
    } = source
        .query(
            &identity,
            Vec::new(),
            NonZeroU16::new(1).expect("limit"),
            None,
        )
        .expect("prefix query")
    else {
        panic!("ready prefix page");
    };
    assert_eq!(generation, generation_one);
    assert_eq!(frontier, FrontierPosition::AppliedThrough(sequence(2)));
    assert_eq!(rows.len(), 1);
    let continuation = *next.expect("lower continuation");
    drop(source);

    let ProjectionControlResult::Updated(rebuilding) = controller
        .allocate_rebuild(&identity)
        .expect("allocate rebuild")
    else {
        panic!("rebuild allocated");
    };
    let generation_two = rebuilding.candidate().expect("candidate").generation();
    assert!(generation_two > generation_one);
    apply_row(
        &mut controller,
        &schema,
        generation_two,
        sequence(1),
        10,
        measures(&measure_fields, 1, 7),
    );
    apply_row(
        &mut controller,
        &schema,
        generation_two,
        sequence(2),
        20,
        measures(&measure_fields, 1, 9),
    );
    controller
        .publish_candidate(&identity)
        .expect("publish rebuilt generation");

    let source = ProjectionReadSource::new(controller.repository(), registry, notifier.clone());
    assert!(matches!(
        source
            .query(
                &identity,
                Vec::new(),
                NonZeroU16::new(1).expect("limit"),
                Some(continuation),
            )
            .expect("old continuation invalidates"),
        ProjectionQueryResult::ContinuationInvalidated
    ));
    let ProjectionQueryResult::Ready {
        generation,
        frontier,
        rows,
        ..
    } = source
        .query(
            &identity,
            vec![CanonicalValue::I64(10)],
            NonZeroU16::new(10).expect("limit"),
            None,
        )
        .expect("exact prefix query")
    else {
        panic!("rebuilt ready query");
    };
    assert_eq!(generation, generation_two);
    assert_eq!(frontier, FrontierPosition::AppliedThrough(sequence(2)));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].value().measures(), &measures(&measure_fields, 1, 7));
    drop(source);

    controller.repository_mut().set_head(sequence(3));
    let registration = notifier.register(identity.clone()).expect("waiter");
    let ProjectionControlResult::Updated(degraded) = controller
        .record_failure(
            &identity,
            generation_two,
            ProjectionFailureCodeV1::ArithmeticOverflow,
            Some(sequence(3)),
        )
        .expect("record failure")
    else {
        panic!("degraded transition");
    };
    assert_eq!(degraded.lifecycle(), ProjectionLifecycleV1::Degraded);
    assert_eq!(
        registration.wait(Instant::now()).expect("notified"),
        ProjectionWake::Notified
    );
    let ProjectionControlResult::Updated(recovering) = controller
        .recover_degraded(&identity)
        .expect("recover into a new generation")
    else {
        panic!("recovery transition");
    };
    assert_eq!(recovering.lifecycle(), ProjectionLifecycleV1::Rebuilding);
    assert!(
        recovering.candidate().expect("replacement").generation() > generation_two,
        "a degraded published generation is never repaired in place"
    );
}

#[test]
fn after_sequence_wait_returns_to_the_service_on_notification_or_cancellation() {
    let (schema, _, identity) = fixture();
    let status = ProjectionStatus::uninitialized(
        identity.clone(),
        FrontierPosition::AppliedThrough(sequence(2)),
    );
    let registry = ProjectionSchemaRegistry::new([schema.clone()]).expect("registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let cancellation = notifier.cancellation();
    let reader = ScriptedQueryReader::new(
        [empty_ready(
            &schema,
            ProjectionGeneration::first(),
            FrontierPosition::AppliedThrough(sequence(1)),
        )],
        status.clone(),
        Some(ScriptedSignal::Notify(notifier.clone(), identity.clone())),
    );
    let source = ProjectionReadSource::new(reader, registry, notifier.clone());
    assert_eq!(
        source
            .observe_after_sequence(
                &identity,
                Vec::new(),
                Some(sequence(2)),
                Instant::now() + Duration::from_secs(30),
                NonZeroU16::new(10).expect("limit"),
                None,
                &cancellation,
            )
            .expect("notified observation"),
        ProjectionReadOutcome::PendingObservation {
            generation: ProjectionGeneration::first(),
            frontier: FrontierPosition::AppliedThrough(sequence(1)),
        }
    );

    let registry = ProjectionSchemaRegistry::new([schema.clone()]).expect("registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let cancellation = notifier.cancellation();
    let reader = ScriptedQueryReader::new(
        [empty_ready(
            &schema,
            ProjectionGeneration::first(),
            FrontierPosition::AppliedThrough(sequence(1)),
        )],
        status,
        Some(ScriptedSignal::Cancel(cancellation.clone())),
    );
    let source = ProjectionReadSource::new(reader, registry, notifier);
    assert_eq!(
        source
            .observe_after_sequence(
                &identity,
                Vec::new(),
                Some(sequence(2)),
                Instant::now() + Duration::from_secs(30),
                NonZeroU16::new(10).expect("limit"),
                None,
                &cancellation,
            )
            .expect("cancelled observation"),
        ProjectionReadOutcome::Cancelled
    );
}

#[test]
fn timeout_uses_a_final_atomic_reread_across_generation_and_lifecycle_changes() {
    let (schema, _, identity) = fixture();
    let status = ProjectionStatus::uninitialized(
        identity.clone(),
        FrontierPosition::AppliedThrough(sequence(3)),
    );
    let first_generation = ProjectionGeneration::first();
    let next_generation = first_generation.checked_next().expect("next generation");

    let registry = ProjectionSchemaRegistry::new([schema.clone()]).expect("registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let cancellation = notifier.cancellation();
    let reader = ScriptedQueryReader::new(
        [
            empty_ready(
                &schema,
                first_generation,
                FrontierPosition::AppliedThrough(sequence(1)),
            ),
            empty_ready(
                &schema,
                next_generation,
                FrontierPosition::AppliedThrough(sequence(2)),
            ),
        ],
        status.clone(),
        None,
    );
    let source = ProjectionReadSource::new(reader, registry, notifier);
    assert_eq!(
        source
            .observe_after_sequence(
                &identity,
                Vec::new(),
                Some(sequence(3)),
                Instant::now(),
                NonZeroU16::new(10).expect("limit"),
                None,
                &cancellation,
            )
            .expect("timeout observation"),
        ProjectionReadOutcome::WaitTimedOut {
            required: sequence(3),
            generation: next_generation,
            current: FrontierPosition::AppliedThrough(sequence(2)),
        }
    );

    let registry = ProjectionSchemaRegistry::new([schema.clone()]).expect("registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let cancellation = notifier.cancellation();
    let reader = ScriptedQueryReader::new(
        [
            empty_ready(
                &schema,
                first_generation,
                FrontierPosition::AppliedThrough(sequence(1)),
            ),
            ProjectionQueryResult::Degraded {
                generation: Some(next_generation),
                current: FrontierPosition::AppliedThrough(sequence(2)),
                reason: ProjectionUnavailableReason::Rebuilding,
            },
        ],
        status,
        None,
    );
    let source = ProjectionReadSource::new(reader, registry, notifier);
    assert_eq!(
        source
            .observe_after_sequence(
                &identity,
                Vec::new(),
                Some(sequence(3)),
                Instant::now(),
                NonZeroU16::new(10).expect("limit"),
                None,
                &cancellation,
            )
            .expect("lifecycle observation"),
        ProjectionReadOutcome::Degraded {
            generation: Some(next_generation),
            current: FrontierPosition::AppliedThrough(sequence(2)),
            reason: ProjectionUnavailableReason::Rebuilding,
        }
    );
}

#[test]
fn timeout_final_reread_does_not_consume_waiter_capacity() {
    let (schema, _, identity) = fixture();
    let registry = ProjectionSchemaRegistry::new([schema.clone()]).expect("registry");
    let notifier = ProjectionNotifier::from_registry(&registry);
    let cancellation = notifier.cancellation();
    let held_registrations = (0..MAX_PROJECTION_WAITERS - 1)
        .map(|_| notifier.register(identity.clone()).expect("held waiter"))
        .collect::<Vec<_>>();
    let behind = FrontierPosition::AppliedThrough(sequence(1));
    let reader = TimeoutCapacityProbeReader::new(
        [
            empty_ready(&schema, ProjectionGeneration::first(), behind),
            empty_ready(&schema, ProjectionGeneration::first(), behind),
        ],
        ProjectionStatus::uninitialized(
            identity.clone(),
            FrontierPosition::AppliedThrough(sequence(2)),
        ),
        notifier.clone(),
        identity.clone(),
    );
    let source = ProjectionReadSource::new(reader, registry, notifier);

    assert_eq!(
        source
            .observe_after_sequence(
                &identity,
                Vec::new(),
                Some(sequence(2)),
                Instant::now(),
                NonZeroU16::new(10).expect("limit"),
                None,
                &cancellation,
            )
            .expect("timeout observation"),
        ProjectionReadOutcome::WaitTimedOut {
            required: sequence(2),
            generation: ProjectionGeneration::first(),
            current: behind,
        }
    );

    let reader = source.into_reader();
    assert_eq!(reader.query_count.get(), 2);
    assert!(
        reader.final_reread_registration.borrow().is_some(),
        "the final storage reread must retain the last free capacity slot"
    );
    drop(held_registrations);
}

// req: PRJ-001, PRJ-002, PRJ-003, PRJ-004
#[test]
fn projection_batch_builder_matches_sequential_catalog_evaluation_and_flushes_at_64() {
    let fixture = evaluator_fixture();
    let identity = fixture.schema.identity().clone();
    let registry = ProjectionSchemaRegistry::new([fixture.schema.clone()]).expect("registry");
    let mut oracle = ProjectionController::new(
        SemanticRepository::with_head(sequence(65)),
        ProjectionNotifier::from_registry(&registry),
    );
    oracle
        .initialize(fixture.schema.clone())
        .expect("initialize");
    oracle.start_initial_catch_up(&identity).expect("catch up");
    let mut builder = riffdb_projection::ProjectionBatchBuilder::new(
        Box::new(oracle.repository().clone()),
        fixture.schema.clone(),
        ProjectionGeneration::first(),
    )
    .expect("pinned batch");
    let mut expected = Vec::new();
    for value in 1..=65 {
        let commit = evaluator_commit(
            &fixture,
            sequence(value),
            if value % 3 == 0 {
                &[]
            } else {
                &[(10, -3), (10, 7), (20, 100)]
            },
        );
        let evaluated = evaluate_projection_commit(
            &fixture.resolved,
            fixture.schema.clone(),
            ProjectionGeneration::first(),
            &commit,
        )
        .expect("evaluate");
        if value == 65 {
            assert!(!builder.try_push(&evaluated).expect("count flush"));
            assert_eq!(
                builder.frontier(),
                FrontierPosition::AppliedThrough(sequence(64))
            );
        } else {
            let request = prepare_projection_apply(&evaluated, oracle.repository())
                .expect("reference request");
            assert!(builder.try_push(&evaluated).expect("bounded member"));
            oracle.apply(&request).expect("reference apply");
            expected.push(request);
        }
    }
    let batch = builder.finish().expect("checked batch");
    assert_eq!(
        batch.members(),
        expected,
        "each prior, post-image and canonical marker matches the per-commit oracle"
    );
    assert_eq!(batch.observations().len(), 1);
    assert!(matches!(
        batch.observations(),
        [ProjectionApplyRowObservation::Absent(_)]
    ));
}
