//! One-page bounded dispatcher over durable pending intents.

use std::num::NonZeroU32;

use riffdb_storage_api::{
    OutboxClaimV1, OutboxDeadLetterV1, OutboxDeliveryStateV1, OutboxRetryV1, OutboxSafeErrorV1,
    OutboxStatusObservationV1, OutboxSucceedV1, OutboxTransitionResultV1, PendingOutboxItemV1,
    PendingOutboxScanV1, StoredOutboxStatusV1,
};
use riffdb_types::{EventId, Timestamp};

use crate::{
    ConnectorDisposition, DeliveryAttempt, DeliveryPolicy, NoOutboxFailpoints, NoOutboxTelemetry,
    OutboxClock, OutboxConnector, OutboxFailpoint, OutboxFailpoints, OutboxTelemetry,
    OutboxTelemetryEvent, OutboxTransitionPhase, OutboxWorkerError, OutboxWorkerRepository,
    RecoveredOutbox, interrupt,
};

const ATTEMPT_LIMIT_SAFE_ERROR: &str = "delivery attempt limit exhausted";

/// Bounded evidence from one pending-scan page.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutboxDispatchReport {
    scanned: u16,
    deferred: u16,
    claims: u16,
    delivered: u16,
    retries_scheduled: u16,
    dead_lettered: u16,
    state_changes: u16,
    next_after: Option<EventId>,
}

impl OutboxDispatchReport {
    fn increment(value: &mut u16) -> Result<(), OutboxWorkerError> {
        *value = value
            .checked_add(1)
            .ok_or(riffdb_storage_api::StorageValueError::SizeOverflow)?;
        Ok(())
    }

    /// Returns pending items examined.
    #[must_use]
    pub const fn scanned(self) -> u16 {
        self.scanned
    }

    /// Returns explicit backoff items not yet eligible.
    #[must_use]
    pub const fn deferred(self) -> u16 {
        self.deferred
    }

    /// Returns durable delivery claims created.
    #[must_use]
    pub const fn claims(self) -> u16 {
        self.claims
    }

    /// Returns statuses durably completed as delivered.
    #[must_use]
    pub const fn delivered(self) -> u16 {
        self.delivered
    }

    /// Returns retry transitions made durable.
    #[must_use]
    pub const fn retries_scheduled(self) -> u16 {
        self.retries_scheduled
    }

    /// Returns terminal dead-letter transitions made durable.
    #[must_use]
    pub const fn dead_lettered(self) -> u16 {
        self.dead_lettered
    }

    /// Returns exact claims or completions lost to a concurrent status change.
    #[must_use]
    pub const fn state_changes(self) -> u16 {
        self.state_changes
    }

    /// Returns the next exclusive scanner position, if this was not exact end.
    #[must_use]
    pub const fn next_after(self) -> Option<EventId> {
        self.next_after
    }
}

/// Dispatcher constructible only from completed startup recovery.
pub struct OutboxDispatcher<R, C, K, F = NoOutboxFailpoints, T = NoOutboxTelemetry> {
    repository: R,
    connector: C,
    clock: K,
    failpoints: F,
    telemetry: T,
    policy: DeliveryPolicy,
    scan_after: Option<EventId>,
}

impl<R, C, K, F, T> OutboxDispatcher<R, C, K, F, T> {
    /// Wires one recovered repository to explicit connector and worker providers.
    #[must_use]
    pub fn new(
        recovered: RecoveredOutbox<R>,
        connector: C,
        clock: K,
        failpoints: F,
        telemetry: T,
        policy: DeliveryPolicy,
    ) -> Self {
        Self {
            repository: recovered.into_inner(),
            connector,
            clock,
            failpoints,
            telemetry,
            policy,
            scan_after: None,
        }
    }

    /// Borrows the repository for payload-free operational status.
    #[must_use]
    pub const fn repository(&self) -> &R {
        &self.repository
    }

    /// Borrows deterministic connector state.
    #[must_use]
    pub const fn connector(&self) -> &C {
        &self.connector
    }

    /// Separates owned components during controlled shutdown or crash simulation.
    #[must_use]
    pub fn into_parts(self) -> (R, C, K, F, T, DeliveryPolicy) {
        (
            self.repository,
            self.connector,
            self.clock,
            self.failpoints,
            self.telemetry,
            self.policy,
        )
    }
}

impl<R, C, K, F, T> OutboxDispatcher<R, C, K, F, T>
where
    R: OutboxWorkerRepository,
    C: OutboxConnector,
    K: OutboxClock,
    F: OutboxFailpoints,
    T: OutboxTelemetry,
{
    /// Scans and processes at most one configured storage page.
    ///
    /// Connector I/O begins only after `claim_outbox` returns `Applied`.
    /// Exact status transitions make uncertain completion visible as a future
    /// retry rather than silently claiming exactly-once delivery.
    pub fn dispatch_once(&mut self) -> Result<OutboxDispatchReport, OutboxWorkerError> {
        let scan = self
            .repository
            .scan_pending_outbox(self.scan_after, self.policy.scan_limit())
            .map_err(|error| {
                self.telemetry
                    .record(OutboxTelemetryEvent::StorageFailure { kind: error.kind() });
                OutboxWorkerError::Storage(error)
            })?;
        let (items, next_after) = match scan {
            PendingOutboxScanV1::Page { items, next_after } => (items, Some(next_after)),
            PendingOutboxScanV1::ExactEnd { items } => (items, None),
        };
        let mut report = OutboxDispatchReport {
            next_after,
            ..OutboxDispatchReport::default()
        };

        for encoded in items {
            OutboxDispatchReport::increment(&mut report.scanned)?;
            let (item, _) = encoded.into_parts();
            self.dispatch_item(&item, &mut report)?;
            // An interrupted item and the rest of its page remain reachable.
            self.scan_after = Some(item.event_id());
        }
        self.scan_after = next_after;
        Ok(report)
    }

    fn dispatch_item(
        &mut self,
        item: &PendingOutboxItemV1,
        report: &mut OutboxDispatchReport,
    ) -> Result<(), OutboxWorkerError> {
        let event_id = item.event_id();
        let started_at = self.clock.now()?;
        if !is_eligible(item.status(), started_at) {
            OutboxDispatchReport::increment(&mut report.deferred)?;
            return Ok(());
        }
        if !self.policy.permits_attempt_after(item.status().attempts()) {
            return self.dead_letter_pending(item, started_at, report);
        }

        let claim = OutboxClaimV1::new(
            event_id,
            item.status().clone(),
            self.policy.destination_id().clone(),
            started_at,
            self.policy.lease_deadline(started_at)?,
        )?;
        let delivering = match self.repository.claim_outbox(&claim) {
            Ok(OutboxTransitionResultV1::Applied(status)) => status,
            Ok(OutboxTransitionResultV1::StateChanged(_)) => {
                self.telemetry
                    .record(OutboxTelemetryEvent::StateChanged { event_id });
                OutboxDispatchReport::increment(&mut report.state_changes)?;
                return Ok(());
            }
            Ok(OutboxTransitionResultV1::AuthoritativeIntentMissing) => {
                return Err(OutboxWorkerError::AuthoritativeIntentMissing);
            }
            Err(error) => return self.storage_failure(error),
        };
        let attempt = delivering_attempt(&delivering)?;
        OutboxDispatchReport::increment(&mut report.claims)?;
        self.telemetry
            .record(OutboxTelemetryEvent::DeliveryClaimed {
                event_id,
                attempt: attempt.get(),
            });
        self.stop_at(OutboxFailpoint::DeliveryAfterClaimBeforeConnector, event_id)?;

        let disposition = self.connector.deliver(DeliveryAttempt::new(
            item.event(),
            attempt,
            self.policy.destination_id(),
            self.policy.delivery_timeout_seconds(),
        ));
        self.telemetry
            .record(OutboxTelemetryEvent::ConnectorResult {
                event_id,
                attempt: attempt.get(),
                class: disposition.class(),
            });
        match disposition {
            ConnectorDisposition::Accepted => {
                self.stop_at(
                    OutboxFailpoint::DeliveryAfterConnectorAcceptedBeforeSuccess,
                    event_id,
                )?;
                self.complete_success(delivering, attempt, report)
            }
            ConnectorDisposition::Retryable { safe_error } => {
                self.stop_at(
                    OutboxFailpoint::DeliveryAfterConnectorRetryableBeforeRetry,
                    event_id,
                )?;
                self.complete_retry(delivering, attempt, safe_error, report)
            }
            ConnectorDisposition::PermanentFailure { safe_error } => {
                self.stop_at(
                    OutboxFailpoint::DeliveryAfterConnectorRejectedBeforeDeadLetter,
                    event_id,
                )?;
                self.complete_dead_letter(delivering, attempt, safe_error, report)
            }
        }
    }

    fn complete_success(
        &mut self,
        delivering: StoredOutboxStatusV1,
        attempt: NonZeroU32,
        report: &mut OutboxDispatchReport,
    ) -> Result<(), OutboxWorkerError> {
        let event_id = delivering.event_id();
        let transition = OutboxSucceedV1::new(delivering, self.clock.now()?)?;
        match self.repository.succeed_outbox(&transition) {
            Ok(OutboxTransitionResultV1::Applied(_)) => {
                OutboxDispatchReport::increment(&mut report.delivered)?;
                self.telemetry.record(OutboxTelemetryEvent::Delivered {
                    event_id,
                    attempt: attempt.get(),
                });
                Ok(())
            }
            Ok(OutboxTransitionResultV1::StateChanged(_)) => {
                self.completion_state_changed(event_id, OutboxTransitionPhase::Succeed, report)
            }
            Ok(OutboxTransitionResultV1::AuthoritativeIntentMissing) => {
                Err(OutboxWorkerError::AuthoritativeIntentMissing)
            }
            Err(error) => self.storage_failure(error),
        }
    }

    fn complete_retry(
        &mut self,
        delivering: StoredOutboxStatusV1,
        attempt: NonZeroU32,
        safe_error: Option<OutboxSafeErrorV1>,
        report: &mut OutboxDispatchReport,
    ) -> Result<(), OutboxWorkerError> {
        let now = self.clock.now()?;
        let next_attempt_at = self.policy.retry_at(now, attempt)?;
        if next_attempt_at.is_none() {
            return self.complete_dead_letter(delivering, attempt, safe_error, report);
        }
        let event_id = delivering.event_id();
        let transition = OutboxRetryV1::new(delivering, next_attempt_at, safe_error)?;
        match self.repository.retry_outbox(&transition) {
            Ok(OutboxTransitionResultV1::Applied(_)) => {
                OutboxDispatchReport::increment(&mut report.retries_scheduled)?;
                self.telemetry.record(OutboxTelemetryEvent::RetryScheduled {
                    event_id,
                    attempt: attempt.get(),
                });
                Ok(())
            }
            Ok(OutboxTransitionResultV1::StateChanged(_)) => {
                self.completion_state_changed(event_id, OutboxTransitionPhase::Retry, report)
            }
            Ok(OutboxTransitionResultV1::AuthoritativeIntentMissing) => {
                Err(OutboxWorkerError::AuthoritativeIntentMissing)
            }
            Err(error) => self.storage_failure(error),
        }
    }

    fn complete_dead_letter(
        &mut self,
        delivering: StoredOutboxStatusV1,
        attempt: NonZeroU32,
        safe_error: Option<OutboxSafeErrorV1>,
        report: &mut OutboxDispatchReport,
    ) -> Result<(), OutboxWorkerError> {
        let event_id = delivering.event_id();
        let transition = OutboxDeadLetterV1::new(
            event_id,
            OutboxStatusObservationV1::Present(delivering),
            None,
            self.clock.now()?,
            safe_error,
        )?;
        match self.repository.dead_letter_outbox(&transition) {
            Ok(OutboxTransitionResultV1::Applied(_)) => {
                OutboxDispatchReport::increment(&mut report.dead_lettered)?;
                self.telemetry.record(OutboxTelemetryEvent::DeadLettered {
                    event_id,
                    attempts: attempt.get(),
                });
                Ok(())
            }
            Ok(OutboxTransitionResultV1::StateChanged(_)) => {
                self.completion_state_changed(event_id, OutboxTransitionPhase::DeadLetter, report)
            }
            Ok(OutboxTransitionResultV1::AuthoritativeIntentMissing) => {
                Err(OutboxWorkerError::AuthoritativeIntentMissing)
            }
            Err(error) => self.storage_failure(error),
        }
    }

    fn dead_letter_pending(
        &mut self,
        item: &PendingOutboxItemV1,
        failed_at: Timestamp,
        report: &mut OutboxDispatchReport,
    ) -> Result<(), OutboxWorkerError> {
        let event_id = item.event_id();
        let safe_error = OutboxSafeErrorV1::new(ATTEMPT_LIMIT_SAFE_ERROR)?;
        let initial_destination = matches!(
            item.status(),
            OutboxStatusObservationV1::AbsentInitialPending
        )
        .then(|| self.policy.destination_id().clone());
        let transition = OutboxDeadLetterV1::new(
            event_id,
            item.status().clone(),
            initial_destination,
            failed_at,
            Some(safe_error),
        )?;
        match self.repository.dead_letter_outbox(&transition) {
            Ok(OutboxTransitionResultV1::Applied(status)) => {
                OutboxDispatchReport::increment(&mut report.dead_lettered)?;
                self.telemetry.record(OutboxTelemetryEvent::DeadLettered {
                    event_id,
                    attempts: status.state().attempts(),
                });
                Ok(())
            }
            Ok(OutboxTransitionResultV1::StateChanged(_)) => {
                self.telemetry
                    .record(OutboxTelemetryEvent::StateChanged { event_id });
                OutboxDispatchReport::increment(&mut report.state_changes)
            }
            Ok(OutboxTransitionResultV1::AuthoritativeIntentMissing) => {
                Err(OutboxWorkerError::AuthoritativeIntentMissing)
            }
            Err(error) => self.storage_failure(error),
        }
    }

    fn completion_state_changed(
        &mut self,
        event_id: EventId,
        phase: OutboxTransitionPhase,
        report: &mut OutboxDispatchReport,
    ) -> Result<(), OutboxWorkerError> {
        self.telemetry
            .record(OutboxTelemetryEvent::StateChanged { event_id });
        OutboxDispatchReport::increment(&mut report.state_changes)?;
        Err(OutboxWorkerError::StateChanged { phase })
    }

    fn stop_at(
        &mut self,
        failpoint: OutboxFailpoint,
        event_id: EventId,
    ) -> Result<(), OutboxWorkerError> {
        if interrupt(
            &mut self.failpoints,
            &mut self.telemetry,
            failpoint,
            Some(event_id),
        ) {
            Err(OutboxWorkerError::Interrupted { failpoint })
        } else {
            Ok(())
        }
    }

    fn storage_failure<U>(
        &mut self,
        error: riffdb_storage_api::StorageError,
    ) -> Result<U, OutboxWorkerError> {
        self.telemetry
            .record(OutboxTelemetryEvent::StorageFailure { kind: error.kind() });
        Err(OutboxWorkerError::Storage(error))
    }
}

fn delivering_attempt(delivering: &StoredOutboxStatusV1) -> Result<NonZeroU32, OutboxWorkerError> {
    match delivering.state() {
        OutboxDeliveryStateV1::Delivering { attempt, .. } => Ok(*attempt),
        OutboxDeliveryStateV1::Pending(_)
        | OutboxDeliveryStateV1::Delivered { .. }
        | OutboxDeliveryStateV1::DeadLetter { .. } => {
            Err(riffdb_storage_api::StorageValueError::InvalidShape.into())
        }
    }
}

fn is_eligible(status: &OutboxStatusObservationV1, now: Timestamp) -> bool {
    match status {
        OutboxStatusObservationV1::AbsentInitialPending => true,
        OutboxStatusObservationV1::Present(status) => match status.state() {
            OutboxDeliveryStateV1::Pending(metadata) => metadata
                .next_attempt_at()
                .is_none_or(|retry_at| retry_at <= now),
            OutboxDeliveryStateV1::Delivering { .. }
            | OutboxDeliveryStateV1::Delivered { .. }
            | OutboxDeliveryStateV1::DeadLetter { .. } => false,
        },
    }
}
