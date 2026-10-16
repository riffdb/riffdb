//! Initial dependency admission owns no operation or durable audit work.
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{PortFuture, RequestControl, RiffDbServiceInner, ServiceFailure, ServiceResult};
use riffdb_errors::PublicError;
use riffdb_types::ServiceOperationV1;

/// Closed first-party native admission failure, before any protected work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitialNativeDependencyError {
    /// Pending construction or its waiter cannot be admitted under existing bounds.
    Unavailable,
    /// The exact source or its construction proof failed integrity validation.
    Integrity,
}

/// A complete current source, or one admitted construction completion wait.
pub enum InitialNativeDependencyAdmission<'a> {
    /// The source is complete; ordinary prestart cancellation audit still applies.
    Ready,
    /// Dropping this future cancels only its admitted waiter, never shared construction.
    Pending(PortFuture<'a, (), InitialNativeDependencyError>),
}

/// Trusted live-source composition capability; callers cannot select this phase.
pub trait InitialNativeDependencyPort: Send + Sync {
    /// Probe the exact current source before inspecting cancellation or expiry.
    /// Pending completion must recapture and verify the current source before success.
    fn begin_initial_admission<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> Result<InitialNativeDependencyAdmission<'a>, InitialNativeDependencyError>;
}

impl RiffDbServiceInner {
    pub(crate) async fn admit_initial_native_dependency(
        &self,
        control: &RequestControl,
        operation: Option<ServiceOperationV1>,
    ) -> ServiceResult<()> {
        match crate::service::catch_maintenance_future_panic(
            self.wait_initial_native_dependency(control, operation),
        )
        .await
        {
            Ok(result) => result,
            Err(()) => {
                self.providers
                    .health
                    .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
                Err(match operation {
                    Some(operation) => {
                        self.internal_failure(operation, crate::InternalDefect::Panic)
                    }
                    None => self.maintenance_internal_failure(
                        crate::service::MaintenanceInternalDefect::Panic,
                    ),
                })
            }
        }
    }

    async fn wait_initial_native_dependency(
        &self,
        control: &RequestControl,
        operation: Option<ServiceOperationV1>,
    ) -> ServiceResult<()> {
        let Some(port) = self.providers.initial_native_dependency.as_ref() else {
            // Follower/static and semantic test compositions have no native writer source.
            return Ok(());
        };
        let admitted = match port.begin_initial_admission(control) {
            Ok(InitialNativeDependencyAdmission::Ready) => return Ok(()),
            Ok(InitialNativeDependencyAdmission::Pending(wait)) => {
                match wait_with_control(control, self.providers.deadline_scheduler.as_ref(), wait)
                    .await
                {
                    Ok(result) => result,
                    Err(ControlledWaitError::Cancelled) => return Err(ServiceFailure::Cancelled),
                    Err(ControlledWaitError::DeadlineExceeded) => {
                        return Err(ServiceFailure::DeadlineExceeded);
                    }
                }
            }
            Err(error) => Err(error),
        };
        match admitted {
            Ok(()) => Ok(()),
            Err(InitialNativeDependencyError::Unavailable) => {
                // This closed port error proves initial Pending was identified;
                // unknown/invalid native state is Integrity, never this exception.
                if control.is_cancelled() {
                    Err(ServiceFailure::Cancelled)
                } else if control.is_deadline_exceeded() {
                    Err(ServiceFailure::DeadlineExceeded)
                } else {
                    Err(PublicError::storage_unavailable().into())
                }
            }
            Err(InitialNativeDependencyError::Integrity) => {
                self.providers
                    .health
                    .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
                Err(match operation {
                    Some(operation) => {
                        self.internal_failure(operation, crate::InternalDefect::LowerIntegrity)
                    }
                    None => self.maintenance_internal_failure(
                        crate::service::MaintenanceInternalDefect::LowerIntegrity,
                    ),
                })
            }
        }
    }
}
