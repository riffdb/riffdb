//! Mechanical payload-free outbox status adapter for the shared service.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use riffdb_commit::{ApplicationCommitNotificationError, ApplicationCommitNotificationSink};
use riffdb_outbox::{
    OutboxStatusPageRequest, OutboxStatusSource, OutboxStatusSourceError, OutboxStatusState,
};
use riffdb_service::{
    BoxPortCapacityPermit, OutboxDeliveryState, OutboxDeliverySummary, OutboxStatusPort,
    OutboxStatusPortError, OutboxStatusRequest, OutboxStatusSnapshot, PortAdmissionError,
    PortFuture, RequestControl,
};
use riffdb_storage_api::{OutboxPageLimit, OutboxRepository, StorageErrorKind};
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
    state: Arc<AtomicU64>,
    destination_unready: Arc<AtomicU8>,
}

impl NoDestinationOutboxHealth {
    /// Seeds recovery readiness before the first complete undelivered scan.
    pub(crate) fn new(recovery: OutboxRecoveryReadiness) -> Self {
        Self {
            destination_unready: Arc::new(AtomicU8::new(0)),
            state: Arc::new(AtomicU64::new(match recovery {
                OutboxRecoveryReadiness::Ready => 0,
                OutboxRecoveryReadiness::Degraded => 1,
            })),
        }
    }

    /// Refreshes the cache from exact commit-coupled undelivered membership.
    pub(crate) fn refresh(&self, source: &impl OutboxRepository) {
        self.refresh_snapshot(|| source.has_undelivered_outbox().unwrap_or(true));
    }

    fn refresh_snapshot(&self, inspect: impl FnOnce() -> bool) {
        // The low bit is degraded; upper bits are an ephemeral observation
        // generation. Mark the latest capture unknown before inspecting it.
        // A delayed older capture can never clear or replace a newer result.
        // Reserve MAX as permanently degraded exhaustion, never wrap identity.
        let generation = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |prior| {
                prior
                    .checked_add(2)
                    .map(|next| next | 1)
                    .filter(|next| *next < u64::MAX)
            });
        let Ok(prior) = generation else {
            self.state.store(u64::MAX, Ordering::Release);
            return;
        };
        let admitted = (prior + 2) | 1;
        let observed = (admitted & !1) | u64::from(inspect());
        let _ =
            self.state
                .compare_exchange(admitted, observed, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Configured destination recovery/refusal independently fences readiness.
    pub(crate) fn destination_ready(&self, ready: bool) {
        self.destination_unready
            .store(u8::from(!ready), Ordering::Release);
    }

    /// Returns the current payload-free aggregate classification.
    pub(crate) fn readiness(&self) -> OutboxDerivedReadiness {
        if self.state.load(Ordering::Acquire) & 1 == 0
            && self.destination_unready.load(Ordering::Acquire) == 0
        {
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

    fn publish_first_commit_group(
        &self,
        sequences: &[CommitSequence],
    ) -> Result<(), ApplicationCommitNotificationError> {
        for sequence in sequences {
            self.authoritative.publish_first_commit(*sequence)?;
        }
        if !sequences.is_empty() {
            self.outbox.refresh(&self.storage);
        }
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
    fn reserve_pending_status<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> PortFuture<
        'a,
        BoxPortCapacityPermit<OutboxStatusRequest, OutboxStatusSnapshot, OutboxStatusPortError>,
        PortAdmissionError,
    > {
        Box::pin(self.status.reserve_async(control))
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
            | StorageErrorKind::SequenceExhausted
            | StorageErrorKind::HistoryPruned => OutboxStatusPortError::Integrity,
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

    // req: EFF-002
    #[test]
    fn commit_health_updates_cannot_clear_a_destination_refusal_in_either_order() {
        for destination_first in [false, true] {
            let health = NoDestinationOutboxHealth::new(OutboxRecoveryReadiness::Ready);
            if destination_first {
                health.destination_ready(false);
            }
            health.state.store(0, Ordering::Release);
            if !destination_first {
                health.destination_ready(false);
            }
            assert_eq!(health.readiness(), OutboxDerivedReadiness::Degraded);
            health.destination_ready(true);
            assert_eq!(health.readiness(), OutboxDerivedReadiness::Ready);
            health.state.store(1, Ordering::Release);
            health.destination_ready(true);
            assert_eq!(health.readiness(), OutboxDerivedReadiness::Degraded);
        }
    }
    // req: EFF-002, END-007, END-008
    #[test]
    fn older_health_refresh_cannot_overwrite_a_completed_newer_snapshot() {
        for older_pending in [false, true] {
            let health = NoDestinationOutboxHealth::new(OutboxRecoveryReadiness::Ready);
            let previous = health.clone();
            let (captured, captured_rx) = std::sync::mpsc::sync_channel(1);
            let (resume, resume_rx) = std::sync::mpsc::sync_channel(1);
            let thread = std::thread::spawn(move || {
                previous.refresh_snapshot(|| {
                    captured.send(()).unwrap();
                    resume_rx.recv().unwrap();
                    older_pending
                });
            });
            captured_rx.recv().unwrap();
            health.refresh_snapshot(|| !older_pending);
            let expected = if older_pending {
                OutboxDerivedReadiness::Ready
            } else {
                OutboxDerivedReadiness::Degraded
            };
            assert_eq!(health.readiness(), expected);
            resume.send(()).unwrap();
            thread.join().unwrap();
            assert_eq!(
                health.readiness(),
                expected,
                "the delayed older observation cannot replace the newer completed state"
            );
        }
    }

    // req: EFF-002, END-007, END-008
    #[test]
    fn in_flight_health_refresh_and_exhaustion_remain_degraded() {
        let health = NoDestinationOutboxHealth::new(OutboxRecoveryReadiness::Ready);
        health.refresh_snapshot(|| {
            assert_eq!(health.readiness(), OutboxDerivedReadiness::Degraded);
            false
        });
        assert_eq!(health.readiness(), OutboxDerivedReadiness::Ready);
        health.state.store(u64::MAX - 3, Ordering::Release);
        let mut inspected = false;
        health.refresh_snapshot(|| {
            inspected = true;
            false
        });
        assert!(!inspected);
        assert_eq!(health.state.load(Ordering::Acquire), u64::MAX);
        assert_eq!(health.readiness(), OutboxDerivedReadiness::Degraded);
        health.refresh_snapshot(|| false);
        assert_eq!(health.readiness(), OutboxDerivedReadiness::Degraded);
    }
    // req: EFF-002, END-007, END-008
    #[test]
    fn exhaustion_cannot_be_cleared_by_the_last_admitted_delayed_refresh() {
        let health = NoDestinationOutboxHealth::new(OutboxRecoveryReadiness::Ready);
        health.state.store(u64::MAX - 5, Ordering::Release);
        let previous = health.clone();
        let (captured, captured_rx) = std::sync::mpsc::sync_channel(1);
        let (resume, resume_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || {
            previous.refresh_snapshot(|| {
                captured.send(()).unwrap();
                resume_rx.recv().unwrap();
                false
            });
        });
        captured_rx.recv().unwrap();
        health.refresh_snapshot(|| false);
        assert_eq!(health.state.load(Ordering::Acquire), u64::MAX);
        resume.send(()).unwrap();
        thread.join().unwrap();
        assert_eq!(health.state.load(Ordering::Acquire), u64::MAX);
        assert_eq!(health.readiness(), OutboxDerivedReadiness::Degraded);
    }
}
