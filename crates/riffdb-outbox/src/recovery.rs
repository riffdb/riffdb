//! Startup type state for interrupted-delivery normalization.

use riffdb_storage_api::{
    OutboxDeliveryStateV1, OutboxRetryV1, OutboxSafeErrorV1, OutboxTransitionResultV1,
    UndeliveredOutboxStatusScanRequestV1,
};
use riffdb_types::EventId;

use crate::{
    DeliveryPolicy, OutboxClock, OutboxFailpoint, OutboxFailpoints, OutboxTelemetry,
    OutboxTelemetryEvent, OutboxTransitionPhase, OutboxWorkerError, OutboxWorkerRepository,
    delivering_status,
};

/// Maximum detailed findings retained by one recovery run.
pub const MAX_OUTBOX_RECOVERY_FINDINGS: usize = 256;

const INTERRUPTED_SAFE_ERROR: &str = "delivery interrupted before durable completion";

/// Bounded recovery finding classification, separate from structural evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxRecoveryFindingCode {
    /// Exact status changed after the recovery scan.
    StateChanged,
    /// Reciprocal authoritative event/intent was unexpectedly absent.
    AuthoritativeIntentMissing,
    /// Storage failed while scanning or normalizing derived status.
    StorageFailure,
    /// A deterministic recovery failpoint interrupted the run.
    Interrupted,
}

/// One payload-free bounded recovery finding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboxRecoveryFinding {
    event_id: Option<EventId>,
    code: OutboxRecoveryFindingCode,
}

impl OutboxRecoveryFinding {
    fn new(event_id: Option<EventId>, code: OutboxRecoveryFindingCode) -> Self {
        Self { event_id, code }
    }

    /// Returns the affected stable event identity, when one was known.
    #[must_use]
    pub const fn event_id(self) -> Option<EventId> {
        self.event_id
    }

    /// Returns the closed recovery finding code.
    #[must_use]
    pub const fn code(self) -> OutboxRecoveryFindingCode {
        self.code
    }
}

/// Bounded report produced independently of WP-070 structural findings.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OutboxRecoveryReport {
    scanned: u64,
    normalized: u64,
    findings: Vec<OutboxRecoveryFinding>,
    findings_truncated: bool,
}

impl OutboxRecoveryReport {
    fn observe_scan(&mut self) -> Result<(), OutboxWorkerError> {
        self.scanned = self
            .scanned
            .checked_add(1)
            .ok_or(riffdb_storage_api::StorageValueError::SizeOverflow)?;
        Ok(())
    }

    fn observe_normalized(&mut self) -> Result<(), OutboxWorkerError> {
        self.normalized = self
            .normalized
            .checked_add(1)
            .ok_or(riffdb_storage_api::StorageValueError::SizeOverflow)?;
        Ok(())
    }

    fn observe_finding(&mut self, finding: OutboxRecoveryFinding) {
        if self.findings.len() < MAX_OUTBOX_RECOVERY_FINDINGS {
            self.findings.push(finding);
        } else {
            self.findings_truncated = true;
        }
    }

    /// Returns the number of undelivered statuses examined.
    #[must_use]
    pub const fn scanned(&self) -> u64 {
        self.scanned
    }

    /// Returns the number of interrupted statuses durably normalized.
    #[must_use]
    pub const fn normalized(&self) -> u64 {
        self.normalized
    }

    /// Borrows retained payload-free findings.
    #[must_use]
    pub fn findings(&self) -> &[OutboxRecoveryFinding] {
        &self.findings
    }

    /// Returns whether additional findings were omitted.
    #[must_use]
    pub const fn findings_truncated(&self) -> bool {
        self.findings_truncated
    }
}

/// Repository that has not yet completed interrupted-delivery recovery.
pub struct RecoveringOutbox<R> {
    repository: R,
}

impl<R> RecoveringOutbox<R> {
    /// Begins the worker-owned recovery phase after authoritative readiness.
    #[must_use]
    pub const fn after_authoritative_readiness(repository: R) -> Self {
        Self { repository }
    }

    /// Returns the repository without claiming outbox readiness.
    #[must_use]
    pub fn into_repository(self) -> R {
        self.repository
    }
}

/// Repository proven to have reached exact end after normalization.
pub struct RecoveredOutbox<R> {
    repository: R,
}

impl<R> RecoveredOutbox<R> {
    /// Borrows the recovered repository for payload-free status reads.
    #[must_use]
    pub const fn repository(&self) -> &R {
        &self.repository
    }

    /// Returns the repository for controlled composition or shutdown.
    #[must_use]
    pub fn into_repository(self) -> R {
        self.repository
    }

    pub(crate) fn into_inner(self) -> R {
        self.repository
    }
}

/// Recovery completed or remained degraded without discarding the repository.
pub enum OutboxRecoveryResult<R> {
    /// Every scan reached exact end and interrupted statuses were normalized.
    Ready {
        /// Type-state proof required to construct a dispatcher.
        recovered: RecoveredOutbox<R>,
        /// Bounded recovery evidence.
        report: OutboxRecoveryReport,
    },
    /// Recovery failed; authoritative readiness is unaffected but outbox stays degraded.
    Degraded {
        /// Repository retained for explicit retry or shutdown.
        recovering: RecoveringOutbox<R>,
        /// Bounded partial recovery evidence.
        report: OutboxRecoveryReport,
        /// Closed safe failure.
        error: OutboxWorkerError,
    },
}

impl<R> OutboxRecoveryResult<R> {
    /// Returns the bounded report for either terminal branch.
    #[must_use]
    pub const fn report(&self) -> &OutboxRecoveryReport {
        match self {
            Self::Ready { report, .. } | Self::Degraded { report, .. } => report,
        }
    }
}

impl<R> RecoveringOutbox<R>
where
    R: OutboxWorkerRepository,
{
    /// Normalizes every interrupted `Delivering` row before releasing readiness.
    ///
    /// Callers must enforce one recovery owner and keep the dispatcher stopped
    /// for this repository until `Ready` is returned.
    pub fn recover<C, F, T>(
        mut self,
        policy: &DeliveryPolicy,
        clock: &mut C,
        failpoints: &mut F,
        telemetry: &mut T,
    ) -> OutboxRecoveryResult<R>
    where
        C: OutboxClock,
        F: OutboxFailpoints,
        T: OutboxTelemetry,
    {
        telemetry.record(OutboxTelemetryEvent::RecoveryStarted);
        let mut report = OutboxRecoveryReport::default();
        let mut request = UndeliveredOutboxStatusScanRequestV1::initial(None, policy.scan_limit());

        loop {
            let scan = match self.repository.scan_undelivered_outbox_statuses(request) {
                Ok(scan) => scan,
                Err(error) => {
                    telemetry.record(OutboxTelemetryEvent::StorageFailure { kind: error.kind() });
                    report.observe_finding(OutboxRecoveryFinding::new(
                        None,
                        OutboxRecoveryFindingCode::StorageFailure,
                    ));
                    return degraded(self, report, error.into());
                }
            };

            for item in scan.items() {
                if let Err(error) = report.observe_scan() {
                    return degraded(self, report, error);
                }
                let Some(delivering) = delivering_status(item.value()) else {
                    continue;
                };
                let event_id = delivering.event_id();
                if interrupt(
                    failpoints,
                    telemetry,
                    OutboxFailpoint::RecoveryAfterScanBeforeNormalize,
                    Some(event_id),
                ) {
                    report.observe_finding(OutboxRecoveryFinding::new(
                        Some(event_id),
                        OutboxRecoveryFindingCode::Interrupted,
                    ));
                    return degraded(
                        self,
                        report,
                        OutboxWorkerError::Interrupted {
                            failpoint: OutboxFailpoint::RecoveryAfterScanBeforeNormalize,
                        },
                    );
                }

                let OutboxDeliveryStateV1::Delivering { attempt, .. } = delivering.state() else {
                    return degraded(
                        self,
                        report,
                        OutboxWorkerError::StateChanged {
                            phase: OutboxTransitionPhase::Recovery,
                        },
                    );
                };
                let attempt = *attempt;
                let now = match clock.now() {
                    Ok(now) => now,
                    Err(error) => return degraded(self, report, error.into()),
                };
                let next_attempt_at = match policy.retry_at(now, attempt) {
                    Ok(value) => value,
                    Err(error) => return degraded(self, report, error.into()),
                };
                let safe_error = match OutboxSafeErrorV1::new(INTERRUPTED_SAFE_ERROR) {
                    Ok(value) => value,
                    Err(error) => return degraded(self, report, error.into()),
                };
                let retry = match OutboxRetryV1::new(delivering, next_attempt_at, Some(safe_error))
                {
                    Ok(retry) => retry,
                    Err(error) => return degraded(self, report, error.into()),
                };
                match self.repository.retry_outbox(&retry) {
                    Ok(OutboxTransitionResultV1::Applied(_)) => {
                        if let Err(error) = report.observe_normalized() {
                            return degraded(self, report, error);
                        }
                        telemetry.record(OutboxTelemetryEvent::RecoveryNormalized {
                            event_id,
                            attempt: attempt.get(),
                        });
                    }
                    Ok(OutboxTransitionResultV1::StateChanged(_)) => {
                        telemetry.record(OutboxTelemetryEvent::StateChanged { event_id });
                        report.observe_finding(OutboxRecoveryFinding::new(
                            Some(event_id),
                            OutboxRecoveryFindingCode::StateChanged,
                        ));
                        return degraded(
                            self,
                            report,
                            OutboxWorkerError::StateChanged {
                                phase: OutboxTransitionPhase::Recovery,
                            },
                        );
                    }
                    Ok(OutboxTransitionResultV1::AuthoritativeIntentMissing) => {
                        report.observe_finding(OutboxRecoveryFinding::new(
                            Some(event_id),
                            OutboxRecoveryFindingCode::AuthoritativeIntentMissing,
                        ));
                        return degraded(
                            self,
                            report,
                            OutboxWorkerError::AuthoritativeIntentMissing,
                        );
                    }
                    Err(error) => {
                        telemetry
                            .record(OutboxTelemetryEvent::StorageFailure { kind: error.kind() });
                        report.observe_finding(OutboxRecoveryFinding::new(
                            Some(event_id),
                            OutboxRecoveryFindingCode::StorageFailure,
                        ));
                        return degraded(self, report, error.into());
                    }
                }

                if interrupt(
                    failpoints,
                    telemetry,
                    OutboxFailpoint::RecoveryAfterNormalize,
                    Some(event_id),
                ) {
                    report.observe_finding(OutboxRecoveryFinding::new(
                        Some(event_id),
                        OutboxRecoveryFindingCode::Interrupted,
                    ));
                    return degraded(
                        self,
                        report,
                        OutboxWorkerError::Interrupted {
                            failpoint: OutboxFailpoint::RecoveryAfterNormalize,
                        },
                    );
                }
            }

            match scan.continuation() {
                Some(continuation) => {
                    request = UndeliveredOutboxStatusScanRequestV1::continuing(
                        continuation,
                        policy.scan_limit(),
                    );
                }
                None => break,
            }
        }

        if interrupt(
            failpoints,
            telemetry,
            OutboxFailpoint::RecoveryBeforeReady,
            None,
        ) {
            report.observe_finding(OutboxRecoveryFinding::new(
                None,
                OutboxRecoveryFindingCode::Interrupted,
            ));
            return degraded(
                self,
                report,
                OutboxWorkerError::Interrupted {
                    failpoint: OutboxFailpoint::RecoveryBeforeReady,
                },
            );
        }
        telemetry.record(OutboxTelemetryEvent::RecoveryReady {
            normalized: report.normalized,
        });
        OutboxRecoveryResult::Ready {
            recovered: RecoveredOutbox {
                repository: self.repository,
            },
            report,
        }
    }
}

fn degraded<R>(
    recovering: RecoveringOutbox<R>,
    report: OutboxRecoveryReport,
    error: OutboxWorkerError,
) -> OutboxRecoveryResult<R> {
    OutboxRecoveryResult::Degraded {
        recovering,
        report,
        error,
    }
}

pub(crate) fn interrupt<F, T>(
    failpoints: &mut F,
    telemetry: &mut T,
    failpoint: OutboxFailpoint,
    event_id: Option<EventId>,
) -> bool
where
    F: OutboxFailpoints,
    T: OutboxTelemetry,
{
    if failpoints.should_interrupt(failpoint, event_id) {
        telemetry.record(OutboxTelemetryEvent::Interrupted {
            failpoint,
            event_id,
        });
        true
    } else {
        false
    }
}
