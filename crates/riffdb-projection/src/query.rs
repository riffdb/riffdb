//! Exact-identity bounded query, status, and waiter registration.

use std::num::NonZeroU16;
use std::time::Instant;

use riffdb_storage_api::{
    EncodedPageItem, ProjectionLowerContinuation, ProjectionQueryReader, ProjectionQueryRequest,
    ProjectionQueryResult, ProjectionQuerySelector, ProjectionStatus, ProjectionUnavailableReason,
    StoredProjectionStateV1,
};
use riffdb_types::{
    CanonicalValue, CommitSequence, FrontierPosition, ProjectionGeneration, ProjectionIdentity,
};

use crate::{
    ProjectionCoreError, ProjectionCoreErrorKind, ProjectionNotifier, ProjectionSchemaRegistry,
    ProjectionWaitCancellation, ProjectionWake, RegisteredProjectionObservation,
};

/// One bounded lower projection observation between service policy safe points.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionReadOutcome {
    /// Published rows whose frontier includes the requested sequence, if any.
    Ready {
        /// Selected published generation.
        generation: ProjectionGeneration,
        /// Frontier observed atomically with the rows.
        frontier: FrontierPosition,
        /// Complete bounded rows in canonical key order.
        rows: Vec<EncodedPageItem<StoredProjectionStateV1>>,
        /// Lower continuation fenced to this exact generation and frontier.
        next: Option<Box<ProjectionLowerContinuation>>,
    },
    /// A real durable-transition notification requires a service policy safe point.
    PendingObservation {
        /// Generation from the pre-wait atomic observation.
        generation: ProjectionGeneration,
        /// Behind frontier from the pre-wait atomic observation.
        frontier: FrontierPosition,
    },
    /// The deadline elapsed and a final atomic reread remained behind.
    WaitTimedOut {
        /// Required nonzero application sequence.
        required: CommitSequence,
        /// Published generation observed by the final reread.
        generation: ProjectionGeneration,
        /// Published frontier observed by the final reread.
        current: FrontierPosition,
    },
    /// No rows are exposed while the lifecycle is unavailable.
    Degraded {
        /// Exact affected generation, absent only for a control-less identity.
        generation: Option<ProjectionGeneration>,
        /// Visible affected-generation position.
        current: FrontierPosition,
        /// Closed safe unavailable reason.
        reason: ProjectionUnavailableReason,
    },
    /// Rebuild is impossible from retained authoritative history.
    Invalid {
        /// Exact failed retained generation.
        generation: ProjectionGeneration,
        /// Frontier of the failed retained generation.
        current: FrontierPosition,
        /// Closed safe failure reason.
        reason: riffdb_storage_api::ProjectionFailureCodeV1,
    },
    /// Publication or frontier advancement invalidated the lower fence.
    ContinuationInvalidated,
    /// Process-local request cancellation won the wait.
    Cancelled,
}

/// Storage-backed query source over an immutable catalog-checked schema registry.
pub struct ProjectionReadSource<R> {
    reader: R,
    schemas: ProjectionSchemaRegistry,
    notifier: ProjectionNotifier,
}

impl<R> ProjectionReadSource<R>
where
    R: ProjectionQueryReader,
{
    /// Constructs a least-authority read source.
    #[must_use]
    pub const fn new(
        reader: R,
        schemas: ProjectionSchemaRegistry,
        notifier: ProjectionNotifier,
    ) -> Self {
        Self {
            reader,
            schemas,
            notifier,
        }
    }

    /// Reads one complete/prefix page and lifecycle from one storage snapshot.
    pub fn query(
        &self,
        identity: &ProjectionIdentity,
        leading_components: Vec<CanonicalValue>,
        limit: NonZeroU16,
        continuation: Option<ProjectionLowerContinuation>,
    ) -> Result<ProjectionQueryResult, ProjectionCoreError> {
        let request = self.query_request(identity, leading_components, limit, continuation)?;
        self.reader
            .query_projection(&request)
            .map_err(ProjectionCoreError::from)
    }

    /// Performs one bounded read/wait observation for `after_sequence`.
    ///
    /// Registration precedes the first atomic storage read. A durable wake
    /// returns `PendingObservation` without a second read so the service can
    /// reauthorize before asking for the next observation. A timeout performs
    /// one final atomic reread and never releases rows whose frontier remains
    /// behind. Condition-variable wakeups without an epoch change are ignored
    /// by [`crate::ProjectionWaitRegistration`].
    #[allow(clippy::too_many_arguments)]
    pub fn observe_after_sequence(
        &self,
        identity: &ProjectionIdentity,
        leading_components: Vec<CanonicalValue>,
        required_sequence: Option<CommitSequence>,
        deadline: Instant,
        limit: NonZeroU16,
        continuation: Option<ProjectionLowerContinuation>,
        cancellation: &ProjectionWaitCancellation,
    ) -> Result<ProjectionReadOutcome, ProjectionCoreError> {
        if !cancellation.belongs_to(&self.notifier) {
            return Err(ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity));
        }
        if cancellation.is_cancelled() {
            return Ok(ProjectionReadOutcome::Cancelled);
        }
        let request = self.query_request(identity, leading_components, limit, continuation)?;
        let registration = self.notifier.register(identity.clone())?;
        let observed = self
            .reader
            .query_projection(&request)
            .map_err(ProjectionCoreError::from)?;
        if cancellation.is_cancelled() {
            return Ok(ProjectionReadOutcome::Cancelled);
        }
        let Some(required) = required_sequence else {
            return Ok(map_complete_observation(observed));
        };
        let ProjectionQueryResult::Ready {
            generation,
            frontier,
            rows,
            next,
        } = observed
        else {
            return Ok(map_complete_observation(observed));
        };
        if frontier >= FrontierPosition::AppliedThrough(required) {
            return Ok(ProjectionReadOutcome::Ready {
                generation,
                frontier,
                rows,
                next,
            });
        }

        match registration.wait_controlled(deadline, cancellation)? {
            ProjectionWake::Notified => Ok(ProjectionReadOutcome::PendingObservation {
                generation,
                frontier,
            }),
            ProjectionWake::Cancelled => Ok(ProjectionReadOutcome::Cancelled),
            ProjectionWake::TimedOut => {
                if cancellation.is_cancelled() {
                    return Ok(ProjectionReadOutcome::Cancelled);
                }
                // The deadline has elapsed, so this branch will not wait again.
                // One final atomic storage read determines the returned state
                // without consuming another process-local waiter slot.
                let final_observed = self
                    .reader
                    .query_projection(&request)
                    .map_err(ProjectionCoreError::from)?;
                if cancellation.is_cancelled() {
                    return Ok(ProjectionReadOutcome::Cancelled);
                }
                Ok(map_timeout_observation(required, final_observed))
            }
        }
    }

    /// Registers before reading, preventing a durable transition from being lost
    /// between a behind-frontier observation and waiter installation.
    pub fn query_registered(
        &self,
        identity: &ProjectionIdentity,
        leading_components: Vec<CanonicalValue>,
        limit: NonZeroU16,
        continuation: Option<ProjectionLowerContinuation>,
    ) -> Result<RegisteredProjectionObservation<ProjectionQueryResult>, ProjectionCoreError> {
        let registration = self.notifier.register(identity.clone())?;
        let observation = self.query(identity, leading_components, limit, continuation)?;
        Ok(RegisteredProjectionObservation::new(
            observation,
            registration,
        ))
    }

    /// Reads exact lifecycle plus authoritative head in one storage transaction.
    pub fn status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, ProjectionCoreError> {
        if self.schemas.get(identity).is_none() {
            return Err(ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity));
        }
        self.reader
            .read_projection_status(identity)
            .map_err(ProjectionCoreError::from)
    }

    /// Borrows the exact schema registry.
    #[must_use]
    pub const fn schemas(&self) -> &ProjectionSchemaRegistry {
        &self.schemas
    }

    /// Returns the process-local notifier used by this source.
    #[must_use]
    pub const fn notifier(&self) -> &ProjectionNotifier {
        &self.notifier
    }

    /// Consumes the source and returns its read port.
    #[must_use]
    pub fn into_reader(self) -> R {
        self.reader
    }

    fn query_request(
        &self,
        identity: &ProjectionIdentity,
        leading_components: Vec<CanonicalValue>,
        limit: NonZeroU16,
        continuation: Option<ProjectionLowerContinuation>,
    ) -> Result<ProjectionQueryRequest, ProjectionCoreError> {
        let schema = self
            .schemas
            .get(identity)
            .ok_or_else(|| ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity))?
            .clone();
        let selector = ProjectionQuerySelector::new(schema, leading_components)?;
        ProjectionQueryRequest::new(selector, limit, continuation)
            .map_err(ProjectionCoreError::from)
    }
}

fn map_complete_observation(observed: ProjectionQueryResult) -> ProjectionReadOutcome {
    match observed {
        ProjectionQueryResult::Ready {
            generation,
            frontier,
            rows,
            next,
        } => ProjectionReadOutcome::Ready {
            generation,
            frontier,
            rows,
            next,
        },
        ProjectionQueryResult::Degraded {
            generation,
            current,
            reason,
        } => ProjectionReadOutcome::Degraded {
            generation,
            current,
            reason,
        },
        ProjectionQueryResult::Invalid {
            generation,
            current,
            reason,
        } => ProjectionReadOutcome::Invalid {
            generation,
            current,
            reason,
        },
        ProjectionQueryResult::ContinuationInvalidated => {
            ProjectionReadOutcome::ContinuationInvalidated
        }
    }
}

fn map_timeout_observation(
    required: CommitSequence,
    observed: ProjectionQueryResult,
) -> ProjectionReadOutcome {
    match observed {
        ProjectionQueryResult::Ready {
            generation,
            frontier,
            rows,
            next,
        } if frontier >= FrontierPosition::AppliedThrough(required) => {
            ProjectionReadOutcome::Ready {
                generation,
                frontier,
                rows,
                next,
            }
        }
        ProjectionQueryResult::Ready {
            generation,
            frontier,
            ..
        } => ProjectionReadOutcome::WaitTimedOut {
            required,
            generation,
            current: frontier,
        },
        other => map_complete_observation(other),
    }
}
