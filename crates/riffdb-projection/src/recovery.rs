//! Fenced derived-state validation and new-generation recovery.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU16;

use riffdb_catalog::ResolvedProjectionPlan;
use riffdb_storage_api::{
    AuthoritativeScanReader, CheckedProjectionSchema, CleanProjectionGenerationV1,
    CommitScanPageV1, CommitScanRequest, ProjectionControlResult, ProjectionMutationRepository,
    ProjectionQueryReader, ProjectionRecoveryContinuationV1, ProjectionRecoveryExpectedPageV1,
    ProjectionRecoveryFindingV1, ProjectionRecoveryPageLimit, ProjectionRecoveryRepository,
    ProjectionRecoveryValidationRequestV1, ProjectionRecoveryValidationResultV1,
    ProjectionRowPrior, ProjectionRowUpdateV1, StorageScanLimit, StoredCommitRecordV1,
    StoredProjectionApplyV1, StoredProjectionControlV1, StoredProjectionStateV1,
};
use riffdb_types::{
    CanonicalRecord, CommitSequence, FrontierPosition, ProjectionApplyKey, ProjectionGeneration,
    ProjectionGroupKey,
};

use crate::{
    EvaluatedProjectionCommit, ProjectionController, ProjectionCoreErrorKind,
    ProjectionEvaluationError, ProjectionEvaluationErrorKind, ProjectionHooks, add_measure_records,
    evaluate_projection_commit, map_storage_error, map_storage_value_error,
};

const REPLAY_SCAN_PAGE_ROWS: u16 = 500;

/// Closed recovery-orchestration failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProjectionRecoveryErrorKind {
    /// Authoritative replay or checked projection evaluation failed.
    Evaluation(ProjectionEvaluationErrorKind),
    /// A lifecycle/storage controller operation failed.
    Control(ProjectionCoreErrorKind),
    /// A storage result violated the closed recovery state machine.
    Integrity,
}

impl ProjectionRecoveryErrorKind {
    const fn safe_message(self) -> &'static str {
        match self {
            Self::Evaluation(_) => "projection recovery replay failed",
            Self::Control(_) => "projection recovery control failed",
            Self::Integrity => "projection recovery state is inconsistent",
        }
    }
}

/// Redaction-safe recovery failure.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ProjectionRecoveryError {
    kind: ProjectionRecoveryErrorKind,
}

impl ProjectionRecoveryError {
    const fn new(kind: ProjectionRecoveryErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the closed failure classification.
    #[must_use]
    pub const fn kind(self) -> ProjectionRecoveryErrorKind {
        self.kind
    }
}

impl fmt::Debug for ProjectionRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionRecoveryError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for ProjectionRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for ProjectionRecoveryError {}

impl From<ProjectionEvaluationError> for ProjectionRecoveryError {
    fn from(error: ProjectionEvaluationError) -> Self {
        Self::new(ProjectionRecoveryErrorKind::Evaluation(error.kind()))
    }
}

/// Result of validating and, when necessary, recovering one retained generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionRecoveryOutcome {
    /// Replay marker hashes, final rows, and the fenced control all matched.
    Clean(CleanProjectionGenerationV1),
    /// The authoritative head or complete control changed; validation must restart.
    FenceChanged,
    /// A mismatch was degraded and a disjoint replacement generation was allocated.
    RebuildAllocated {
        /// Retained generation whose evidence did not match replay.
        failed_generation: ProjectionGeneration,
        /// Fresh never-reused candidate generation.
        replacement_generation: ProjectionGeneration,
    },
}

/// Fenced validation result before any recovery control transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionGenerationValidationOutcome {
    /// Replay marker hashes and final rows matched through exact end.
    Clean(CleanProjectionGenerationV1),
    /// The complete control or authoritative head changed during validation.
    FenceChanged,
    /// Derived marker or row evidence differed from authoritative replay.
    Finding(ProjectionRecoveryFindingV1),
}

/// Replays and validates one retained generation under exact control/head fences.
///
/// Marker expectations are paged in sequence order. Final state is derived one
/// next canonical key at a time, so recovery never retains the complete group
/// namespace in memory.
pub fn validate_projection_generation<R>(
    repository: &R,
    resolved: &ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    expected_control: &StoredProjectionControlV1,
    expected_authoritative_head: FrontierPosition,
    generation: ProjectionGeneration,
    marker_page_limit: ProjectionRecoveryPageLimit,
) -> Result<ProjectionGenerationValidationOutcome, ProjectionRecoveryError>
where
    R: AuthoritativeScanReader + ProjectionRecoveryRepository,
{
    validate_generation(
        repository,
        resolved,
        schema,
        expected_control,
        expected_authoritative_head,
        generation,
        marker_page_limit,
    )
}

/// Validates a generation and allocates a fresh rebuild on a derived mismatch.
///
/// A fence race returns `FenceChanged` without mutating control. A finding first
/// records a closed integrity failure and then uses the accepted degraded
/// recovery transition; the failed generation is never repaired in place.
pub fn validate_and_recover_projection_generation<R, H>(
    controller: &mut ProjectionController<R, H>,
    resolved: &ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    expected_control: &StoredProjectionControlV1,
    expected_authoritative_head: FrontierPosition,
    generation: ProjectionGeneration,
    marker_page_limit: ProjectionRecoveryPageLimit,
) -> Result<ProjectionRecoveryOutcome, ProjectionRecoveryError>
where
    R: AuthoritativeScanReader
        + ProjectionRecoveryRepository
        + ProjectionMutationRepository
        + ProjectionQueryReader,
    H: ProjectionHooks,
{
    let validation = validate_generation(
        controller.repository(),
        resolved,
        schema,
        expected_control,
        expected_authoritative_head,
        generation,
        marker_page_limit,
    )?;
    let finding = match validation {
        ProjectionGenerationValidationOutcome::Clean(clean) => {
            return Ok(ProjectionRecoveryOutcome::Clean(clean));
        }
        ProjectionGenerationValidationOutcome::FenceChanged => {
            return Ok(ProjectionRecoveryOutcome::FenceChanged);
        }
        ProjectionGenerationValidationOutcome::Finding(finding) => finding,
    };

    let degraded = controller
        .record_failure(
            finding.identity(),
            finding.generation(),
            riffdb_storage_api::ProjectionFailureCodeV1::ProjectionStateIntegrity,
            None,
        )
        .map_err(|error| {
            ProjectionRecoveryError::new(ProjectionRecoveryErrorKind::Control(error.kind()))
        })?;
    if matches!(degraded, ProjectionControlResult::StateChanged) {
        return Ok(ProjectionRecoveryOutcome::FenceChanged);
    }
    if !matches!(degraded, ProjectionControlResult::Updated(_)) {
        return Err(ProjectionRecoveryError::new(
            ProjectionRecoveryErrorKind::Integrity,
        ));
    }

    let recovered = controller
        .recover_degraded(finding.identity())
        .map_err(|error| {
            ProjectionRecoveryError::new(ProjectionRecoveryErrorKind::Control(error.kind()))
        })?;
    let ProjectionControlResult::Updated(control) = recovered else {
        if matches!(recovered, ProjectionControlResult::StateChanged) {
            return Ok(ProjectionRecoveryOutcome::FenceChanged);
        }
        return Err(ProjectionRecoveryError::new(
            ProjectionRecoveryErrorKind::Integrity,
        ));
    };
    let replacement = control
        .candidate()
        .map(|position| position.generation())
        .filter(|replacement| *replacement > finding.generation())
        .ok_or_else(|| ProjectionRecoveryError::new(ProjectionRecoveryErrorKind::Integrity))?;
    Ok(ProjectionRecoveryOutcome::RebuildAllocated {
        failed_generation: finding.generation(),
        replacement_generation: replacement,
    })
}

fn validate_generation<R>(
    repository: &R,
    resolved: &ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    expected_control: &StoredProjectionControlV1,
    expected_authoritative_head: FrontierPosition,
    generation: ProjectionGeneration,
    marker_page_limit: ProjectionRecoveryPageLimit,
) -> Result<ProjectionGenerationValidationOutcome, ProjectionRecoveryError>
where
    R: AuthoritativeScanReader + ProjectionRecoveryRepository,
{
    if expected_control.identity() != schema.identity()
        || resolved.identity() != schema.identity()
        || expected_control.frontier_for(generation).is_none()
    {
        return Err(ProjectionRecoveryError::new(
            ProjectionRecoveryErrorKind::Integrity,
        ));
    }
    let retained_frontier = expected_control
        .frontier_for(generation)
        .ok_or_else(|| ProjectionRecoveryError::new(ProjectionRecoveryErrorKind::Integrity))?;
    if retained_frontier > expected_authoritative_head {
        return Err(ProjectionRecoveryError::new(
            ProjectionRecoveryErrorKind::Integrity,
        ));
    }
    let replay = FencedReplay {
        repository,
        resolved,
        schema: &schema,
        generation,
        expected_authoritative_head,
        retained_frontier,
    };

    let mut continuation = None;
    loop {
        let expected_page = match continuation.as_ref() {
            None | Some(ProjectionRecoveryContinuationV1::Markers { .. }) => {
                ProjectionRecoveryExpectedPageV1::markers(
                    replay.marker_page(continuation.as_ref(), marker_page_limit)?,
                )
            }
            Some(ProjectionRecoveryContinuationV1::Rows { after, .. }) => {
                ProjectionRecoveryExpectedPageV1::rows(
                    replay.next_state_row(after.as_ref())?.into_iter().collect(),
                )
            }
        };
        let page_limit = match &expected_page {
            ProjectionRecoveryExpectedPageV1::Markers(markers) => {
                nonempty_recovery_limit(markers.len(), marker_page_limit)?
            }
            ProjectionRecoveryExpectedPageV1::Rows(_) => one_recovery_limit()?,
        };
        let request = ProjectionRecoveryValidationRequestV1::new(
            schema.clone(),
            expected_control.clone(),
            expected_authoritative_head,
            generation,
            page_limit,
            continuation.clone(),
            expected_page,
        )
        .map_err(map_storage_value_error)?;
        match repository
            .validate_projection_recovery_page(&request)
            .map_err(map_storage_error)?
        {
            ProjectionRecoveryValidationResultV1::Page { continuation: next } => {
                if !continuation_progresses(continuation.as_ref(), &next) {
                    return Err(ProjectionRecoveryError::new(
                        ProjectionRecoveryErrorKind::Integrity,
                    ));
                }
                continuation = Some(next);
            }
            ProjectionRecoveryValidationResultV1::ExactEnd(clean) => {
                return Ok(ProjectionGenerationValidationOutcome::Clean(clean));
            }
            ProjectionRecoveryValidationResultV1::Finding(finding) => {
                return Ok(ProjectionGenerationValidationOutcome::Finding(finding));
            }
            ProjectionRecoveryValidationResultV1::FenceChanged => {
                return Ok(ProjectionGenerationValidationOutcome::FenceChanged);
            }
        }
    }
}

struct FencedReplay<'a, R> {
    repository: &'a R,
    resolved: &'a ResolvedProjectionPlan,
    schema: &'a CheckedProjectionSchema,
    generation: ProjectionGeneration,
    expected_authoritative_head: FrontierPosition,
    retained_frontier: FrontierPosition,
}

impl<R> FencedReplay<'_, R>
where
    R: AuthoritativeScanReader,
{
    fn marker_page(
        &self,
        continuation: Option<&ProjectionRecoveryContinuationV1>,
        limit: ProjectionRecoveryPageLimit,
    ) -> Result<Vec<StoredProjectionApplyV1>, ProjectionRecoveryError> {
        let FrontierPosition::AppliedThrough(frontier) = self.retained_frontier else {
            return Ok(Vec::new());
        };
        let mut next = match continuation {
            None => Some(CommitSequence::first()),
            Some(ProjectionRecoveryContinuationV1::Markers { after }) => after.checked_next(),
            Some(ProjectionRecoveryContinuationV1::Rows { .. }) => {
                return Err(ProjectionRecoveryError::new(
                    ProjectionRecoveryErrorKind::Integrity,
                ));
            }
        };
        let maximum = usize::from(limit.get().get());
        let mut markers = Vec::with_capacity(maximum);
        while let Some(sequence) = next
            && sequence <= frontier
            && markers.len() < maximum
        {
            let request = self.replay_apply_request(sequence)?;
            markers.push(StoredProjectionApplyV1::new(
                ProjectionApplyKey::new(self.schema.identity().clone(), self.generation, sequence),
                request.apply_hash(),
            ));
            next = sequence.checked_next();
        }
        Ok(markers)
    }

    fn next_state_row(
        &self,
        after: Option<&ProjectionGroupKey>,
    ) -> Result<Option<StoredProjectionStateV1>, ProjectionRecoveryError> {
        let Some(next_key) = self.next_group_key(after)? else {
            return Ok(None);
        };
        let Some((measures, last_changed)) =
            self.replay_key_through(&next_key, self.retained_frontier)?
        else {
            return Err(ProjectionRecoveryError::new(
                ProjectionRecoveryErrorKind::Integrity,
            ));
        };
        StoredProjectionStateV1::new(self.schema, next_key, measures, last_changed)
            .map(Some)
            .map_err(map_storage_value_error)
            .map_err(ProjectionRecoveryError::from)
    }

    fn next_group_key(
        &self,
        after: Option<&ProjectionGroupKey>,
    ) -> Result<Option<ProjectionGroupKey>, ProjectionRecoveryError> {
        let mut next: Option<ProjectionGroupKey> = None;
        self.scan_through(self.retained_frontier, |commit| {
            let evaluated = self.evaluate(commit)?;
            for key in evaluated.grouped_deltas().keys() {
                if after.is_none_or(|after| key > after)
                    && next.as_ref().is_none_or(|candidate| key < candidate)
                {
                    next = Some(key.clone());
                }
            }
            Ok(ScanDirective::Continue)
        })?;
        Ok(next)
    }

    fn replay_apply_request(
        &self,
        sequence: CommitSequence,
    ) -> Result<riffdb_storage_api::ProjectionApplyRequestV1, ProjectionRecoveryError> {
        let commit = self.find_commit(sequence)?;
        let evaluated = self.evaluate(&commit)?;
        let expected_frontier = predecessor(sequence);
        let mut updates = Vec::with_capacity(evaluated.changed_group_count());
        for (key, delta) in evaluated.grouped_deltas() {
            let prior = self.replay_key_through(key, expected_frontier)?;
            let (prior, measures) = match prior {
                None => (ProjectionRowPrior::Absent, delta.clone()),
                Some((prior_measures, last_changed)) => (
                    ProjectionRowPrior::Present(last_changed),
                    add_measure_records(&prior_measures, delta)?,
                ),
            };
            updates.push(
                ProjectionRowUpdateV1::new(self.schema, key.clone(), prior, measures)
                    .map_err(map_storage_value_error)?,
            );
        }
        riffdb_storage_api::ProjectionApplyRequestV1::new(
            self.schema.clone(),
            self.generation,
            sequence,
            expected_frontier,
            updates,
        )
        .map_err(map_storage_value_error)
        .map_err(ProjectionRecoveryError::from)
    }

    fn replay_key_through(
        &self,
        key: &ProjectionGroupKey,
        frontier: FrontierPosition,
    ) -> Result<Option<(CanonicalRecord, CommitSequence)>, ProjectionRecoveryError> {
        let mut state: Option<(CanonicalRecord, CommitSequence)> = None;
        self.scan_through(frontier, |commit| {
            let evaluated = self.evaluate(commit)?;
            if let Some(delta) = evaluated.grouped_deltas().get(key) {
                let measures = state.as_ref().map_or_else(
                    || Ok(delta.clone()),
                    |(current, _)| add_measure_records(current, delta),
                )?;
                state = Some((measures, commit.commit_sequence()));
            }
            Ok(ScanDirective::Continue)
        })?;
        Ok(state)
    }

    fn find_commit(
        &self,
        target: CommitSequence,
    ) -> Result<StoredCommitRecordV1, ProjectionRecoveryError> {
        let mut found = None;
        self.scan_through(FrontierPosition::AppliedThrough(target), |commit| {
            if commit.commit_sequence() == target {
                found = Some(commit.clone());
                Ok(ScanDirective::Stop)
            } else {
                Ok(ScanDirective::Continue)
            }
        })?;
        found.ok_or_else(|| {
            ProjectionRecoveryError::from(ProjectionEvaluationError::new(
                ProjectionEvaluationErrorKind::MissingCommit,
            ))
        })
    }

    fn evaluate(
        &self,
        commit: &StoredCommitRecordV1,
    ) -> Result<EvaluatedProjectionCommit, ProjectionRecoveryError> {
        evaluate_projection_commit(self.resolved, self.schema.clone(), self.generation, commit)
            .map_err(ProjectionRecoveryError::from)
    }

    fn scan_through(
        &self,
        target: FrontierPosition,
        mut visit: impl FnMut(&StoredCommitRecordV1) -> Result<ScanDirective, ProjectionRecoveryError>,
    ) -> Result<(), ProjectionRecoveryError> {
        if target == FrontierPosition::BeforeFirst {
            return Ok(());
        }
        if target > self.expected_authoritative_head {
            return Err(ProjectionRecoveryError::new(
                ProjectionRecoveryErrorKind::Integrity,
            ));
        }
        let limit = StorageScanLimit::new(REPLAY_SCAN_PAGE_ROWS)
            .ok_or_else(|| ProjectionRecoveryError::new(ProjectionRecoveryErrorKind::Integrity))?;
        let mut request = CommitScanRequest::initial(limit);
        loop {
            let page = self
                .repository
                .scan_commits(request)
                .map_err(map_storage_error)?;
            if page.inclusive_upper() != self.expected_authoritative_head {
                return Err(ProjectionRecoveryError::new(
                    ProjectionRecoveryErrorKind::Integrity,
                ));
            }
            for charged in page.records() {
                let commit = charged.value();
                if FrontierPosition::AppliedThrough(commit.commit_sequence()) > target {
                    return Ok(());
                }
                if matches!(visit(commit)?, ScanDirective::Stop)
                    || FrontierPosition::AppliedThrough(commit.commit_sequence()) == target
                {
                    return Ok(());
                }
            }
            match page {
                CommitScanPageV1::Page {
                    next_after,
                    inclusive_upper,
                    ..
                } => {
                    let FrontierPosition::AppliedThrough(inclusive_upper) = inclusive_upper else {
                        return Err(ProjectionRecoveryError::new(
                            ProjectionRecoveryErrorKind::Integrity,
                        ));
                    };
                    request = CommitScanRequest::continuing(next_after, inclusive_upper, limit)
                        .map_err(map_storage_value_error)?;
                }
                CommitScanPageV1::ExactEnd { .. } => {
                    return Err(ProjectionRecoveryError::from(
                        ProjectionEvaluationError::new(
                            ProjectionEvaluationErrorKind::MissingCommit,
                        ),
                    ));
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ScanDirective {
    Continue,
    Stop,
}

fn predecessor(sequence: CommitSequence) -> FrontierPosition {
    if sequence == CommitSequence::first() {
        FrontierPosition::BeforeFirst
    } else {
        FrontierPosition::AppliedThrough(
            CommitSequence::new(sequence.get() - 1).expect("non-first predecessor is nonzero"),
        )
    }
}

fn nonempty_recovery_limit(
    count: usize,
    fallback: ProjectionRecoveryPageLimit,
) -> Result<ProjectionRecoveryPageLimit, ProjectionRecoveryError> {
    if count == 0 {
        return Ok(fallback);
    }
    let count = u16::try_from(count)
        .ok()
        .and_then(NonZeroU16::new)
        .ok_or_else(|| ProjectionRecoveryError::new(ProjectionRecoveryErrorKind::Integrity))?;
    ProjectionRecoveryPageLimit::new(count)
        .map_err(map_storage_value_error)
        .map_err(ProjectionRecoveryError::from)
}

fn one_recovery_limit() -> Result<ProjectionRecoveryPageLimit, ProjectionRecoveryError> {
    ProjectionRecoveryPageLimit::new(NonZeroU16::MIN)
        .map_err(map_storage_value_error)
        .map_err(ProjectionRecoveryError::from)
}

fn continuation_progresses(
    prior: Option<&ProjectionRecoveryContinuationV1>,
    next: &ProjectionRecoveryContinuationV1,
) -> bool {
    match (prior, next) {
        (None, ProjectionRecoveryContinuationV1::Markers { .. })
        | (None, ProjectionRecoveryContinuationV1::Rows { after: None, .. })
        | (
            Some(ProjectionRecoveryContinuationV1::Markers { .. }),
            ProjectionRecoveryContinuationV1::Rows { after: None, .. },
        ) => true,
        (
            Some(ProjectionRecoveryContinuationV1::Markers { after: prior }),
            ProjectionRecoveryContinuationV1::Markers { after: next },
        ) => next > prior,
        (
            Some(ProjectionRecoveryContinuationV1::Rows {
                after: prior,
                validated_rows: prior_count,
            }),
            ProjectionRecoveryContinuationV1::Rows {
                after: next,
                validated_rows: next_count,
            },
        ) => {
            next_count > prior_count
                && match (prior, next) {
                    (None, Some(_)) => true,
                    (Some(prior), Some(next)) => next > prior,
                    (None | Some(_), None) => false,
                }
        }
        _ => false,
    }
}
