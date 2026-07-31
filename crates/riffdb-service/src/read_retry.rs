//! Bounded internal retry for transient authoritative-read conditions.
//!
//! The retryable set is closed: backend unavailability, blocking-port admission
//! unavailability, and cursor-registration transients. Stale or invalid
//! continuations, integrity faults, response-size faults, and authorization
//! failures are never retried.

use std::time::{Duration, Instant};

use riffdb_types::ServiceOperationV1;

use crate::ports::{RequestDeadlineScheduler, ServiceTelemetry, ServiceTelemetryEvent};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{RequestControl, ServiceFailure};

/// Maximum attempts for one internal read retry sequence (including the first try).
pub(crate) const MAX_READ_ATTEMPTS: u32 = 3;

/// Minimum remaining request budget required to start another attempt.
///
/// Bound to the server port-admission deadline margin (`P1_PORT_ADMISSION_DEADLINE_MARGIN`
/// = 25ms). Keeping this ≥ the margin ensures a near-deadline request surfaces
/// the client's own deadline rather than being retried into `storage_unavailable`.
pub(crate) const READ_RETRY_MIN_REMAINING: Duration = Duration::from_millis(25);

/// Closed set of transient failures eligible for internal retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::enum_variant_names,
    reason = "closed set mirrors public fault classes"
)]
pub(crate) enum RetryableReadFault {
    /// Backend reported transient unavailability.
    BackendUnavailable,
    /// Port admission reported temporary unavailability.
    PortUnavailable,
    /// Cursor token source, clock, or registry poison failed closed.
    CursorUnavailable,
}

/// Runs `operation` up to [`MAX_READ_ATTEMPTS`] times with bounded backoff.
///
/// `operation` returns `Ok(T)` on success, `Err(Ok(fault))` for a closed
/// retryable fault, and `Err(Err(failure))` for a terminal typed failure.
pub(crate) async fn with_read_retry<T, F, Fut>(
    control: &RequestControl,
    deadline_scheduler: &dyn RequestDeadlineScheduler,
    telemetry: &dyn ServiceTelemetry,
    operation_name: ServiceOperationV1,
    mut operation: F,
) -> Result<T, ServiceFailure>
where
    F: FnMut(u32) -> Fut,
    Fut: std::future::Future<Output = Result<T, Result<RetryableReadFault, ServiceFailure>>>,
{
    let mut attempt = 0_u32;
    loop {
        attempt = attempt.saturating_add(1);
        // Emit only for actual retries (attempt > 1); attempt 1 is the ordinary path.
        if attempt > 1 {
            telemetry.record(ServiceTelemetryEvent::ReadRetryAttempt {
                operation: operation_name,
                attempt,
            });
        }
        match operation(attempt).await {
            Ok(value) => return Ok(value),
            Err(Err(failure)) => return Err(failure),
            Err(Ok(_fault)) => {
                if attempt >= MAX_READ_ATTEMPTS {
                    telemetry.record(ServiceTelemetryEvent::ReadRetryExhausted {
                        operation: operation_name,
                    });
                    return Err(ServiceFailure::from(
                        riffdb_errors::PublicError::storage_unavailable(),
                    ));
                }
                if control.is_cancelled() {
                    return Err(ServiceFailure::Cancelled);
                }
                if control.is_deadline_exceeded() {
                    return Err(ServiceFailure::DeadlineExceeded);
                }
                let remaining = control
                    .deadline()
                    .checked_duration_since(Instant::now())
                    .unwrap_or(Duration::ZERO);
                if remaining < READ_RETRY_MIN_REMAINING {
                    // Not exhausted-retry: the client's own deadline cannot fit
                    // another attempt (including admission margin).
                    return Err(ServiceFailure::DeadlineExceeded);
                }
                let backoff = retry_backoff(attempt);
                let wake_at = Instant::now()
                    .checked_add(backoff)
                    .unwrap_or_else(Instant::now);
                let wake_at = wake_at.min(control.deadline());
                match wait_with_control(
                    control,
                    deadline_scheduler,
                    deadline_scheduler.wait_until(wake_at),
                )
                .await
                {
                    Ok(()) => {}
                    Err(ControlledWaitError::Cancelled) => return Err(ServiceFailure::Cancelled),
                    Err(ControlledWaitError::DeadlineExceeded) => {
                        return Err(ServiceFailure::DeadlineExceeded);
                    }
                }
            }
        }
    }
}

fn retry_backoff(attempt: u32) -> Duration {
    // Closed backoff schedule in [2ms, 8ms].
    match attempt {
        1 => Duration::from_millis(2),
        2 => Duration::from_millis(8),
        _ => Duration::from_millis(8),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    struct TestScheduler;

    impl RequestDeadlineScheduler for TestScheduler {
        fn wait_until(&self, deadline: Instant) -> crate::RequestDeadlineFuture<'_> {
            Box::pin(async move {
                let now = Instant::now();
                if deadline > now {
                    tokio::time::sleep(deadline.saturating_duration_since(now)).await;
                }
            })
        }
    }

    struct RecordingTelemetry {
        attempts: Mutex<Vec<u32>>,
        exhausted: AtomicU32,
    }

    impl ServiceTelemetry for RecordingTelemetry {
        fn record(&self, event: ServiceTelemetryEvent) {
            match event {
                ServiceTelemetryEvent::ReadRetryAttempt { attempt, .. } => {
                    self.attempts.lock().expect("lock").push(attempt);
                }
                ServiceTelemetryEvent::ReadRetryExhausted { .. } => {
                    self.exhausted.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime")
            .block_on(future)
    }

    #[test]
    fn backoff_stays_within_closed_bounds() {
        assert_eq!(retry_backoff(1), Duration::from_millis(2));
        assert_eq!(retry_backoff(2), Duration::from_millis(8));
        assert_eq!(retry_backoff(3), Duration::from_millis(8));
    }

    #[test]
    fn two_transient_failures_then_success_records_three_attempts() {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("deadline");
        let (control, _cancel) = RequestControl::new(deadline);
        let telemetry = RecordingTelemetry {
            attempts: Mutex::new(Vec::new()),
            exhausted: AtomicU32::new(0),
        };
        let tries = AtomicU32::new(0);
        let value = block_on(with_read_retry(
            &control,
            &TestScheduler,
            &telemetry,
            ServiceOperationV1::GetEntity,
            |_attempt| {
                let n = tries.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 2 {
                        Err(Ok(RetryableReadFault::BackendUnavailable))
                    } else {
                        Ok(42_u32)
                    }
                }
            },
        ))
        .expect("eventual success");
        assert_eq!(value, 42);
        // Attempts 2 and 3 are retries; attempt 1 is not counted as a retry.
        assert_eq!(telemetry.attempts.lock().expect("lock").as_slice(), &[2, 3]);
        assert_eq!(telemetry.exhausted.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn exhausted_retries_surface_storage_unavailable() {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("deadline");
        let (control, _cancel) = RequestControl::new(deadline);
        let telemetry = RecordingTelemetry {
            attempts: Mutex::new(Vec::new()),
            exhausted: AtomicU32::new(0),
        };
        let err = block_on(with_read_retry(
            &control,
            &TestScheduler,
            &telemetry,
            ServiceOperationV1::ScanIndex,
            |_attempt| async { Err::<u32, _>(Ok(RetryableReadFault::PortUnavailable)) },
        ))
        .expect_err("exhausted");
        let public = match err {
            ServiceFailure::Public(error) => error,
            other => panic!("expected public storage unavailable, got {other:?}"),
        };
        assert_eq!(
            public.kind(),
            riffdb_errors::PublicErrorKind::StorageUnavailable
        );
        assert_eq!(public.code(), "storage_unavailable");
        assert_eq!(telemetry.attempts.lock().expect("lock").as_slice(), &[2, 3]);
        assert_eq!(telemetry.exhausted.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn remaining_budget_below_min_returns_deadline_not_storage_unavailable() {
        let deadline = Instant::now()
            .checked_add(READ_RETRY_MIN_REMAINING - Duration::from_millis(1))
            .expect("deadline");
        let (control, _cancel) = RequestControl::new(deadline);
        let telemetry = RecordingTelemetry {
            attempts: Mutex::new(Vec::new()),
            exhausted: AtomicU32::new(0),
        };
        let err = block_on(with_read_retry(
            &control,
            &TestScheduler,
            &telemetry,
            ServiceOperationV1::GetEntity,
            |_attempt| async { Err::<u32, _>(Ok(RetryableReadFault::BackendUnavailable)) },
        ))
        .expect_err("near-deadline");
        assert!(
            matches!(err, ServiceFailure::DeadlineExceeded),
            "expected client deadline, got {err:?}"
        );
        assert_eq!(telemetry.exhausted.load(Ordering::SeqCst), 0);
    }
}
