//! Mechanical projection adapter over the shared checked catalog and storage.

use std::fmt;

use riffdb_catalog::{ActiveCatalogSnapshot, CatalogError, CatalogErrorKind};
use riffdb_projection::{
    ProjectionCoreError, ProjectionCoreErrorKind, ProjectionNotifier, ProjectionReadOutcome,
    ProjectionReadSource, ProjectionSchemaRegistry,
};
use riffdb_service::{
    BoxPortCapacityPermit, PortAdmissionError, PortFuture, ProjectionContinuation,
    ProjectionFailure, ProjectionFailureCode, ProjectionGenerationFrontier, ProjectionLifecycle,
    ProjectionPageFence, ProjectionPortError, ProjectionPortReady, ProjectionPortRequest,
    ProjectionPortResult, ProjectionQueryPort, ProjectionRow, ProjectionStateFence,
    ProjectionStatusSnapshot, ProjectionUnavailableReason, PublishedApplyMode, RequestControl,
};
use riffdb_storage_api::{
    CatalogRepository, CheckedProjectionSchema, ProjectionFailureCodeV1, ProjectionLifecycleV1,
    ProjectionLowerContinuation, ProjectionQueryReader, ProjectionQuerySelector, ProjectionStatus,
    ProjectionUnavailableReason as StorageProjectionUnavailableReason, PublishedApplyModeV1,
    StorageErrorKind,
};
use riffdb_types::ProjectionIdentity;

use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};

/// Storage-backed projection source driven by the existing bounded port driver.
pub(crate) struct ServerProjectionQueryPort {
    query: BlockingPortExecutor<ProjectionPortRequest, ProjectionPortResult, ProjectionPortError>,
    status: BlockingPortExecutor<
        ProjectionIdentity,
        Option<ProjectionStatusSnapshot>,
        ProjectionPortError,
    >,
}

impl ServerProjectionQueryPort {
    /// Binds the checked active catalog and exact projection notifier.
    pub(crate) fn new<
        S: CatalogRepository + ProjectionQueryReader + Clone + Send + Sync + 'static,
    >(
        storage: S,
        notifier: ProjectionNotifier,
        driver: &BlockingPortDriver,
    ) -> Self {
        let query_storage = storage.clone();
        let query_notifier = notifier.clone();
        let query = driver
            .executor(move |request| query_projection(&query_storage, &query_notifier, request));
        let status =
            driver.executor(move |identity| read_projection_status(&storage, &notifier, &identity));
        Self { query, status }
    }
}

impl ProjectionQueryPort for ServerProjectionQueryPort {
    fn reserve_query_projection<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<ProjectionPortRequest, ProjectionPortResult, ProjectionPortError>,
        PortAdmissionError,
    > {
        Box::pin(self.query.reserve_async(control))
    }

    fn reserve_projection_status<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<
            ProjectionIdentity,
            Option<ProjectionStatusSnapshot>,
            ProjectionPortError,
        >,
        PortAdmissionError,
    > {
        Box::pin(self.status.reserve_async(control))
    }
}

impl fmt::Debug for ServerProjectionQueryPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerProjectionQueryPort([CHECKED_DERIVED_STATE])")
    }
}

fn query_projection<S: CatalogRepository + ProjectionQueryReader + Clone>(
    storage: &S,
    notifier: &ProjectionNotifier,
    request: ProjectionPortRequest,
) -> Result<ProjectionPortResult, ProjectionPortError> {
    let registry = active_projection_registry(storage)?;
    notifier
        .synchronize_registry(&registry)
        .map_err(map_projection_error)?;
    let schema = registry
        .get(request.identity())
        .cloned()
        .ok_or(ProjectionPortError::Integrity)?;
    let continuation = request
        .continuation()
        .map(|continuation| lower_continuation(&schema, &request, continuation))
        .transpose()?;
    let cancellation = notifier.cancellation();
    let source = ProjectionReadSource::new(storage.clone(), registry, notifier.clone());
    let observed = source
        .observe_after_sequence(
            request.identity(),
            request.leading_components().to_vec(),
            request.required_sequence(),
            request.deadline(),
            request.limit().get(),
            continuation,
            &cancellation,
        )
        .map_err(map_projection_error)?;
    map_query_outcome(&schema, &request, observed)
}

fn read_projection_status<S: CatalogRepository + ProjectionQueryReader + Clone>(
    storage: &S,
    notifier: &ProjectionNotifier,
    identity: &ProjectionIdentity,
) -> Result<Option<ProjectionStatusSnapshot>, ProjectionPortError> {
    let registry = active_projection_registry(storage)?;
    notifier
        .synchronize_registry(&registry)
        .map_err(map_projection_error)?;
    if registry.get(identity).is_none() {
        return Ok(None);
    }
    let source = ProjectionReadSource::new(storage.clone(), registry, notifier.clone());
    source
        .status(identity)
        .map_err(map_projection_error)
        .and_then(map_status)
        .map(Some)
}

pub(crate) fn active_projection_registry(
    storage: &impl CatalogRepository,
) -> Result<ProjectionSchemaRegistry, ProjectionPortError> {
    let Some(active) = ActiveCatalogSnapshot::read(storage).map_err(map_catalog_error)? else {
        return ProjectionSchemaRegistry::new(Vec::new()).map_err(map_projection_error);
    };
    let bundle = active.bundle().bundle();
    let schemas = bundle
        .projections()
        .iter()
        .map(|projection| {
            bundle
                .bound_projection_group_schema(projection.projection_id())
                .map(CheckedProjectionSchema::new)
                .ok_or(ProjectionPortError::Integrity)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ProjectionSchemaRegistry::new(schemas).map_err(map_projection_error)
}

fn lower_continuation(
    schema: &CheckedProjectionSchema,
    request: &ProjectionPortRequest,
    continuation: &ProjectionContinuation,
) -> Result<ProjectionLowerContinuation, ProjectionPortError> {
    let selector =
        ProjectionQuerySelector::new(schema.clone(), request.leading_components().to_vec())
            .map_err(|_| ProjectionPortError::Integrity)?;
    ProjectionLowerContinuation::new(
        &selector,
        continuation.generation(),
        continuation.exclusive_last_key().clone(),
        continuation.observed_frontier(),
    )
    .map_err(|_| ProjectionPortError::Integrity)
}

fn map_query_outcome(
    schema: &CheckedProjectionSchema,
    request: &ProjectionPortRequest,
    observed: ProjectionReadOutcome,
) -> Result<ProjectionPortResult, ProjectionPortError> {
    match observed {
        ProjectionReadOutcome::Ready {
            generation,
            frontier,
            rows,
            next,
        } => {
            let rows = rows
                .into_iter()
                .map(|charged| {
                    let (row, _) = charged.into_parts();
                    ProjectionRow::new(row.group_values().to_vec(), row.measures().clone())
                        .map_err(|_| ProjectionPortError::Integrity)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let continuation = next
                .map(|next| {
                    let prefix = schema
                        .group_prefix(generation, request.leading_components())
                        .map_err(|_| ProjectionPortError::Integrity)?;
                    ProjectionContinuation::from_provider(
                        request.identity().clone(),
                        generation,
                        prefix,
                        next.exclusive_last_key().clone(),
                        frontier,
                    )
                    .map_err(|_| ProjectionPortError::Integrity)
                })
                .transpose()?;
            ProjectionPortReady::from_provider(request, generation, frontier, rows, continuation)
                .map(ProjectionPortResult::Ready)
                .map_err(|_| ProjectionPortError::Integrity)
        }
        ProjectionReadOutcome::PendingObservation {
            generation,
            frontier,
        } => Ok(ProjectionPortResult::PendingObservation {
            fence: ProjectionPageFence::new(request.identity().clone(), generation, frontier),
        }),
        ProjectionReadOutcome::WaitTimedOut {
            required,
            generation,
            current,
        } => Ok(ProjectionPortResult::WaitTimedOut {
            required,
            fence: ProjectionPageFence::new(request.identity().clone(), generation, current),
        }),
        ProjectionReadOutcome::Degraded {
            generation,
            current,
            reason,
        } => Ok(ProjectionPortResult::Degraded {
            fence: ProjectionStateFence::new(request.identity().clone(), generation, current)
                .map_err(|_| ProjectionPortError::Integrity)?,
            reason: map_unavailable_reason(reason),
        }),
        ProjectionReadOutcome::Invalid {
            generation,
            current,
            reason,
        } => Ok(ProjectionPortResult::Invalid {
            fence: ProjectionPageFence::new(request.identity().clone(), generation, current),
            reason: map_failure_code(reason),
        }),
        ProjectionReadOutcome::ContinuationInvalidated => {
            Ok(ProjectionPortResult::ContinuationInvalidated)
        }
        ProjectionReadOutcome::Cancelled => Err(ProjectionPortError::Unavailable),
    }
}

fn map_status(status: ProjectionStatus) -> Result<ProjectionStatusSnapshot, ProjectionPortError> {
    let identity = status.identity().clone();
    if status.published().is_none() && status.candidate().is_none() {
        return Ok(ProjectionStatusSnapshot::uninitialized(
            identity,
            status.authoritative_head(),
        ));
    }
    let highest = [status.published(), status.candidate()]
        .into_iter()
        .flatten()
        .map(|position| position.generation())
        .max()
        .ok_or(ProjectionPortError::Integrity)?;
    let published = status.published().map(|position| {
        ProjectionGenerationFrontier::new(position.generation(), position.frontier())
    });
    let candidate = status.candidate().map(|position| {
        ProjectionGenerationFrontier::new(position.generation(), position.frontier())
    });
    let failure = status.failure().map(|failure| {
        ProjectionFailure::new(
            failure.generation(),
            map_failure_code(failure.code()),
            failure.at_sequence(),
        )
    });
    ProjectionStatusSnapshot::from_initialized_control(
        identity,
        highest,
        map_lifecycle(status.lifecycle()),
        published,
        candidate,
        status.published_apply_mode().map(map_apply_mode),
        failure,
        status.authoritative_head(),
    )
    .map_err(|_| ProjectionPortError::Integrity)
}

const fn map_lifecycle(lifecycle: ProjectionLifecycleV1) -> ProjectionLifecycle {
    match lifecycle {
        ProjectionLifecycleV1::Building => ProjectionLifecycle::Building,
        ProjectionLifecycleV1::CatchingUp => ProjectionLifecycle::CatchingUp,
        ProjectionLifecycleV1::Ready => ProjectionLifecycle::Ready,
        ProjectionLifecycleV1::Rebuilding => ProjectionLifecycle::Rebuilding,
        ProjectionLifecycleV1::Degraded => ProjectionLifecycle::Degraded,
        ProjectionLifecycleV1::Invalid => ProjectionLifecycle::Invalid,
    }
}

const fn map_apply_mode(mode: PublishedApplyModeV1) -> PublishedApplyMode {
    match mode {
        PublishedApplyModeV1::Enabled => PublishedApplyMode::Enabled,
        PublishedApplyModeV1::Suspended => PublishedApplyMode::Suspended,
    }
}

const fn map_failure_code(code: ProjectionFailureCodeV1) -> ProjectionFailureCode {
    match code {
        ProjectionFailureCodeV1::ArithmeticOverflow => ProjectionFailureCode::ArithmeticOverflow,
        ProjectionFailureCodeV1::MalformedDurableEvent => {
            ProjectionFailureCode::MalformedDurableEvent
        }
        ProjectionFailureCodeV1::MissingCommit => ProjectionFailureCode::MissingCommit,
        ProjectionFailureCodeV1::PlanOrSchemaUnavailable => {
            ProjectionFailureCode::PlanOrSchemaUnavailable
        }
        ProjectionFailureCodeV1::ProjectionStateIntegrity => {
            ProjectionFailureCode::StateIntegrityFailure
        }
        ProjectionFailureCodeV1::HardLimitExceeded => ProjectionFailureCode::HardLimitExceeded,
    }
}

const fn map_unavailable_reason(
    reason: StorageProjectionUnavailableReason,
) -> ProjectionUnavailableReason {
    match reason {
        StorageProjectionUnavailableReason::Building => ProjectionUnavailableReason::Building,
        StorageProjectionUnavailableReason::Rebuilding => ProjectionUnavailableReason::Rebuilding,
        StorageProjectionUnavailableReason::Failure(code) => {
            ProjectionUnavailableReason::Failure(map_failure_code(code))
        }
    }
}

fn map_catalog_error(error: CatalogError) -> ProjectionPortError {
    match (error.kind(), error.storage_kind()) {
        (
            CatalogErrorKind::Storage,
            Some(StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown),
        ) => ProjectionPortError::Unavailable,
        _ => ProjectionPortError::Integrity,
    }
}

const fn map_projection_error(error: ProjectionCoreError) -> ProjectionPortError {
    match error.kind() {
        ProjectionCoreErrorKind::StorageUnavailable
        | ProjectionCoreErrorKind::CommitStatusUnknown
        | ProjectionCoreErrorKind::WaiterCapacityExceeded => ProjectionPortError::Unavailable,
        ProjectionCoreErrorKind::Integrity
        | ProjectionCoreErrorKind::LimitExceeded
        | ProjectionCoreErrorKind::GenerationExhausted => ProjectionPortError::Integrity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_failure_registry_maps_exhaustively() {
        assert_eq!(
            map_failure_code(ProjectionFailureCodeV1::ProjectionStateIntegrity),
            ProjectionFailureCode::StateIntegrityFailure
        );
        assert_eq!(
            map_unavailable_reason(StorageProjectionUnavailableReason::Failure(
                ProjectionFailureCodeV1::HardLimitExceeded,
            )),
            ProjectionUnavailableReason::Failure(ProjectionFailureCode::HardLimitExceeded)
        );
    }

    #[test]
    fn debug_output_exposes_no_projection_state() {
        assert_eq!(
            std::any::type_name::<ServerProjectionQueryPort>(),
            "riffdb_server::projection_adapter::ServerProjectionQueryPort"
        );
    }
}
