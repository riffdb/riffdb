//! Mechanical payload-free outbox status adapter for the shared service.

use std::fmt;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use riffdb_commit::{ApplicationCommitNotificationError, ApplicationCommitNotificationSink};
use riffdb_outbox::{
    OutboxStatusPageRequest, OutboxStatusSource, OutboxStatusSourceError, OutboxStatusState,
};
use riffdb_service::{
    BoxPortCapacityPermit, OutboxDeliveryState, OutboxDeliverySummary, OutboxStatusPort,
    OutboxStatusPortError, OutboxStatusRequest, OutboxStatusSnapshot, PortAdmissionError,
    PortFuture, RequestControl,
};
use riffdb_storage_api::{OutboxPageLimit, StorageErrorKind};
use riffdb_types::CommitSequence;

use crate::notifications::FirstCommitNotificationHub;
use crate::operational_status::OutboxRecoveryReadiness;
use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::storage::SharedRedbOperationalPorts;

/// Closed no-destination state used by aggregate health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboxDerivedReadiness {
    Ready,
    Degraded,
}

/// Payload-free dynamic health cache for the no-destination composition.
#[derive(Clone)]
pub(crate) struct NoDestinationOutboxHealth {
    state: Arc<AtomicU8>,
}

impl NoDestinationOutboxHealth {
    /// Seeds recovery readiness before the first complete undelivered scan.
    pub(crate) fn new(recovery: OutboxRecoveryReadiness) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(match recovery {
                OutboxRecoveryReadiness::Ready => 0,
                OutboxRecoveryReadiness::Degraded => 1,
            })),
        }
    }

    /// Refreshes the cache from one bounded payload-free exact source page.
    pub(crate) fn refresh(&self, source: &impl OutboxStatusSource) {
        let limit = OutboxPageLimit::new(NonZeroU16::MIN)
            .expect("the fixed no-destination health page is valid");
        let degraded = source
            .read_outbox_status_page(OutboxStatusPageRequest::new(None, limit))
            .map_or(true, |page| !page.items().is_empty());
        self.state.store(u8::from(degraded), Ordering::Release);
    }

    /// Returns the current payload-free aggregate classification.
    pub(crate) fn readiness(&self) -> OutboxDerivedReadiness {
        if self.state.load(Ordering::Acquire) == 0 {
            OutboxDerivedReadiness::Ready
        } else {
            OutboxDerivedReadiness::Degraded
        }
    }
}

impl fmt::Debug for NoDestinationOutboxHealth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NoDestinationOutboxHealth")
            .field("readiness", &self.readiness())
            .finish()
    }
}

/// Publishes authoritative sequence hints and then samples derived outbox health.
#[derive(Clone)]
pub(crate) struct ServerCommitNotificationSink {
    authoritative: FirstCommitNotificationHub,
    storage: SharedRedbOperationalPorts,
    outbox: NoDestinationOutboxHealth,
}

impl ServerCommitNotificationSink {
    pub(crate) fn new(
        authoritative: FirstCommitNotificationHub,
        storage: SharedRedbOperationalPorts,
        outbox: NoDestinationOutboxHealth,
    ) -> Self {
        Self {
            authoritative,
            storage,
            outbox,
        }
    }
}

impl ApplicationCommitNotificationSink for ServerCommitNotificationSink {
    fn publish_first_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<(), ApplicationCommitNotificationError> {
        self.authoritative.publish_first_commit(sequence)?;
        self.outbox.refresh(&self.storage);
        Ok(())
    }
}

impl fmt::Debug for ServerCommitNotificationSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerCommitNotificationSink([AUTHORITATIVE_AND_DERIVED])")
    }
}

/// Payload-free service port driven by the outbox-owned source mapping.
pub(crate) struct ServerOutboxStatusPort {
    status: BlockingPortExecutor<OutboxStatusRequest, OutboxStatusSnapshot, OutboxStatusPortError>,
}

impl ServerOutboxStatusPort {
    /// Binds the exact activated repository to the existing bounded driver.
    pub(crate) fn new(storage: SharedRedbOperationalPorts, driver: &BlockingPortDriver) -> Self {
        let status = driver.executor(move |request| read_status_page(&storage, request));
        Self { status }
    }
}

impl OutboxStatusPort for ServerOutboxStatusPort {
    fn reserve_pending_status(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<OutboxStatusRequest, OutboxStatusSnapshot, OutboxStatusPortError>,
        PortAdmissionError,
    > {
        let reservation = self.status.reserve(control);
        Box::pin(async move { reservation })
    }
}

impl fmt::Debug for ServerOutboxStatusPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerOutboxStatusPort([PAYLOAD_FREE])")
    }
}

fn read_status_page(
    source: &impl OutboxStatusSource,
    request: OutboxStatusRequest,
) -> Result<OutboxStatusSnapshot, OutboxStatusPortError> {
    let limit = OutboxPageLimit::new(request.limit().get())
        .map_err(|_| OutboxStatusPortError::Integrity)?;
    let page = source
        .read_outbox_status_page(OutboxStatusPageRequest::new(request.after(), limit))
        .map_err(map_source_error)?;
    let items = page
        .items()
        .iter()
        .copied()
        .map(|summary| {
            OutboxDeliverySummary::new(
                summary.event_id(),
                map_state(summary.state()),
                summary.attempts(),
                summary.next_attempt_at(),
            )
        })
        .collect();
    OutboxStatusSnapshot::new(request, items, page.next_after())
        .map_err(|_| OutboxStatusPortError::Integrity)
}

const fn map_state(state: OutboxStatusState) -> OutboxDeliveryState {
    match state {
        OutboxStatusState::Pending => OutboxDeliveryState::Pending,
        OutboxStatusState::RetryScheduled => OutboxDeliveryState::RetryScheduled,
        OutboxStatusState::Delivering => OutboxDeliveryState::Delivering,
        OutboxStatusState::DeadLetter => OutboxDeliveryState::DeadLetter,
    }
}

fn map_source_error(error: OutboxStatusSourceError) -> OutboxStatusPortError {
    match error {
        OutboxStatusSourceError::Storage(error) => match error.kind() {
            StorageErrorKind::Unavailable | StorageErrorKind::CommitStatusUnknown => {
                OutboxStatusPortError::Unavailable
            }
            StorageErrorKind::CorruptData
            | StorageErrorKind::IncompatibleFormat
            | StorageErrorKind::LimitExceeded
            | StorageErrorKind::InvariantViolation
            | StorageErrorKind::SequenceExhausted => OutboxStatusPortError::Integrity,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_outbox_state_maps_without_a_fallback() {
        assert_eq!(
            map_state(OutboxStatusState::Pending),
            OutboxDeliveryState::Pending
        );
        assert_eq!(
            map_state(OutboxStatusState::RetryScheduled),
            OutboxDeliveryState::RetryScheduled
        );
        assert_eq!(
            map_state(OutboxStatusState::Delivering),
            OutboxDeliveryState::Delivering
        );
        assert_eq!(
            map_state(OutboxStatusState::DeadLetter),
            OutboxDeliveryState::DeadLetter
        );
    }

    #[test]
    fn debug_output_names_no_repository_or_status() {
        assert_eq!(
            std::any::type_name::<ServerOutboxStatusPort>(),
            "riffdb_server::outbox_adapter::ServerOutboxStatusPort"
        );
    }
}
