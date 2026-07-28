//! Durable projection lifecycle and atomic-apply orchestration.

use riffdb_storage_api::{
    CheckedProjectionSchema, ProjectionApplyRequestV1, ProjectionApplyResult,
    ProjectionControlOperation, ProjectionControlResult, ProjectionFailureCodeV1,
    ProjectionFailureV1, ProjectionGenerationPosition, ProjectionLifecycleV1,
    ProjectionMutationRepository, ProjectionQueryReader, ProjectionStatus, PublishedApplyModeV1,
    StoredProjectionControlV1,
};
use riffdb_types::{CommitSequence, ProjectionGeneration, ProjectionIdentity};

use crate::{
    NoopProjectionHooks, ProjectionCoreError, ProjectionCoreErrorKind, ProjectionFailpoint,
    ProjectionHooks, ProjectionNotifier, ProjectionTelemetryEvent,
};

/// Result of initialization when another actor may have won the same CAS.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionInitializationResult {
    /// Generation one was durably created.
    Initialized(StoredProjectionControlV1),
    /// An exact control record was already present.
    Existing(StoredProjectionControlV1),
    /// State changed between the status read and mutation.
    StateChanged,
}

/// Projection lifecycle controller over least-authority storage ports.
pub struct ProjectionController<R, H = NoopProjectionHooks> {
    repository: R,
    notifier: ProjectionNotifier,
    hooks: H,
}

impl<R> ProjectionController<R, NoopProjectionHooks> {
    /// Constructs a controller with no-op telemetry and failpoints.
    #[must_use]
    pub fn new(repository: R, notifier: ProjectionNotifier) -> Self {
        Self {
            repository,
            notifier,
            hooks: NoopProjectionHooks,
        }
    }
}

impl<R, H> ProjectionController<R, H>
where
    R: ProjectionMutationRepository + ProjectionQueryReader,
    H: ProjectionHooks,
{
    /// Constructs a controller with explicit redaction-safe hooks.
    #[must_use]
    pub fn with_hooks(repository: R, notifier: ProjectionNotifier, hooks: H) -> Self {
        Self {
            repository,
            notifier,
            hooks,
        }
    }

    /// Returns the process-local durable-transition notifier.
    #[must_use]
    pub const fn notifier(&self) -> &ProjectionNotifier {
        &self.notifier
    }

    /// Borrows the underlying semantic port bundle.
    #[must_use]
    pub const fn repository(&self) -> &R {
        &self.repository
    }

    /// Mutably borrows the underlying semantic port bundle.
    #[must_use]
    pub const fn repository_mut(&mut self) -> &mut R {
        &mut self.repository
    }

    /// Consumes the controller and returns its repository and hooks.
    #[must_use]
    pub fn into_parts(self) -> (R, H) {
        (self.repository, self.hooks)
    }

    /// Creates generation one if no control record exists.
    pub fn initialize(
        &mut self,
        schema: CheckedProjectionSchema,
    ) -> Result<ProjectionInitializationResult, ProjectionCoreError> {
        self.before_control()?;
        let result = self
            .repository
            .transition_projection_control(ProjectionControlOperation::CreateInitial { schema })?;
        self.after_control()?;
        match result {
            ProjectionControlResult::Updated(control) => {
                self.hooks.record(ProjectionTelemetryEvent::Initialized);
                self.notify(control.identity())?;
                Ok(ProjectionInitializationResult::Initialized(control))
            }
            ProjectionControlResult::AlreadyInitialized(control) => {
                Ok(ProjectionInitializationResult::Existing(control))
            }
            ProjectionControlResult::StateChanged => {
                Ok(ProjectionInitializationResult::StateChanged)
            }
            ProjectionControlResult::GenerationExhausted => Err(ProjectionCoreError::new(
                ProjectionCoreErrorKind::GenerationExhausted,
            )),
        }
    }

    /// Moves an initialized generation-one candidate into contiguous catch-up.
    pub fn start_initial_catch_up(
        &mut self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionControlResult, ProjectionCoreError> {
        let expected = self.read_control(identity)?;
        self.transition(
            ProjectionControlOperation::StartInitialScan { expected },
            ProjectionTelemetryEvent::InitialCatchUpStarted,
        )
    }

    /// Allocates a disjoint same-plan rebuild generation beside a ready one.
    pub fn allocate_rebuild(
        &mut self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionControlResult, ProjectionCoreError> {
        let expected = self.read_control(identity)?;
        self.transition(
            ProjectionControlOperation::AllocateRebuild { expected },
            ProjectionTelemetryEvent::RebuildAllocated,
        )
    }

    /// Publishes a caught-up candidate at the transaction-current head.
    pub fn publish_candidate(
        &mut self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionControlResult, ProjectionCoreError> {
        let expected = self.read_control(identity)?;
        self.transition(
            ProjectionControlOperation::PublishCandidate { expected },
            ProjectionTelemetryEvent::CandidatePublished,
        )
    }

    /// Records one closed generation failure without advancing its frontier.
    pub fn record_failure(
        &mut self,
        identity: &ProjectionIdentity,
        generation: ProjectionGeneration,
        code: ProjectionFailureCodeV1,
        at_sequence: Option<CommitSequence>,
    ) -> Result<ProjectionControlResult, ProjectionCoreError> {
        let expected = self.read_control(identity)?;
        let failure = ProjectionFailureV1::new(generation, code, at_sequence);
        self.transition(
            ProjectionControlOperation::RecordFailure { expected, failure },
            ProjectionTelemetryEvent::GenerationDegraded,
        )
    }

    /// Applies the exact degraded-recovery transition into a new generation.
    pub fn recover_degraded(
        &mut self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionControlResult, ProjectionCoreError> {
        let expected = self.read_control(identity)?;
        self.transition(
            ProjectionControlOperation::RecoverDegraded { expected },
            ProjectionTelemetryEvent::RecoveryStarted,
        )
    }

    /// Marks a degraded projection invalid only after external no-rebuild proof.
    pub fn mark_invalid(
        &mut self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionControlResult, ProjectionCoreError> {
        let expected = self.read_control(identity)?;
        self.transition(
            ProjectionControlOperation::MarkInvalid { expected },
            ProjectionTelemetryEvent::MarkedInvalid,
        )
    }

    /// Submits one already checked atomic state/marker/frontier request.
    ///
    /// Raw durable events cannot enter this boundary. The catalog-backed
    /// evaluator constructs the checked request before calling this method.
    pub fn apply(
        &mut self,
        request: &ProjectionApplyRequestV1,
    ) -> Result<ProjectionApplyResult, ProjectionCoreError> {
        self.hooks
            .failpoint(ProjectionFailpoint::BeforeStateAndFrontierApply)?;
        let result = self.repository.apply_projection(request)?;
        match &result {
            ProjectionApplyResult::Applied { control, .. } => {
                self.hooks
                    .failpoint(ProjectionFailpoint::AfterStateAndFrontierApply)?;
                self.hooks.record(ProjectionTelemetryEvent::SequenceApplied);
                self.notify(control.identity())?;
            }
            ProjectionApplyResult::AlreadyApplied(_) => {
                self.hooks
                    .record(ProjectionTelemetryEvent::DuplicateApplyConfirmed);
            }
            ProjectionApplyResult::StateChanged => {}
        }
        Ok(result)
    }

    fn transition(
        &mut self,
        operation: ProjectionControlOperation,
        event: ProjectionTelemetryEvent,
    ) -> Result<ProjectionControlResult, ProjectionCoreError> {
        self.before_control()?;
        let result = self.repository.transition_projection_control(operation)?;
        self.after_control()?;
        match &result {
            ProjectionControlResult::Updated(control) => {
                self.hooks.record(event);
                self.notify(control.identity())?;
            }
            ProjectionControlResult::GenerationExhausted => {
                return Err(ProjectionCoreError::new(
                    ProjectionCoreErrorKind::GenerationExhausted,
                ));
            }
            ProjectionControlResult::StateChanged
            | ProjectionControlResult::AlreadyInitialized(_) => {}
        }
        Ok(result)
    }

    fn before_control(&mut self) -> Result<(), ProjectionCoreError> {
        self.hooks
            .failpoint(ProjectionFailpoint::BeforeControlTransition)
    }

    fn after_control(&mut self) -> Result<(), ProjectionCoreError> {
        self.hooks
            .failpoint(ProjectionFailpoint::AfterControlTransition)
    }

    fn notify(&mut self, identity: &ProjectionIdentity) -> Result<(), ProjectionCoreError> {
        self.notifier.notify(identity)?;
        self.hooks.record(ProjectionTelemetryEvent::WaitersNotified);
        Ok(())
    }

    fn read_control(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<StoredProjectionControlV1, ProjectionCoreError> {
        let status = self.repository.read_projection_status(identity)?;
        control_from_status(&status)
    }
}

fn control_from_status(
    status: &ProjectionStatus,
) -> Result<StoredProjectionControlV1, ProjectionCoreError> {
    let published = status.published();
    let candidate = status.candidate();
    let highest = [published, candidate]
        .into_iter()
        .flatten()
        .map(ProjectionGenerationPosition::generation)
        .max()
        .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?;
    StoredProjectionControlV1::new(
        status.identity().clone(),
        highest,
        published,
        candidate,
        status.published_apply_mode(),
        status.lifecycle(),
        status.failure().cloned(),
    )
    .map_err(ProjectionCoreError::from)
}

#[allow(dead_code)]
fn _closed_control_types(
    _: ProjectionLifecycleV1,
    _: PublishedApplyModeV1,
    _: ProjectionGenerationPosition,
) {
}
