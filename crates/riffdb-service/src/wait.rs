//! Cancellation- and deadline-aware admission waits.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;
use std::time::Instant;

use crate::{RequestControl, RequestDeadlineScheduler};

/// Closed reason a service wait ended before its protected operation completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlledWaitError {
    Cancelled,
    DeadlineExceeded,
}

/// Closed reason a pre-admission capacity wait ended without a reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionWaitError {
    /// Request cancellation was proven while still unadmitted.
    Cancelled,
    /// The absolute admission wait cap elapsed while still unadmitted.
    AdmissionCapExceeded,
}

/// Races one cancellation-safe lower future against request cancellation and deadline.
pub(crate) async fn wait_with_control<F>(
    control: &RequestControl,
    deadline_scheduler: &dyn RequestDeadlineScheduler,
    future: F,
) -> Result<F::Output, ControlledWaitError>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let mut cancelled = Box::pin(control.cancelled());
    let mut deadline = deadline_scheduler.wait_until(control.deadline());

    poll_fn(|context| {
        if control.is_cancelled() || Pin::as_mut(&mut cancelled).poll(context).is_ready() {
            return Poll::Ready(Err(ControlledWaitError::Cancelled));
        }
        if control.is_deadline_exceeded() || Pin::as_mut(&mut deadline).poll(context).is_ready() {
            return Poll::Ready(Err(ControlledWaitError::DeadlineExceeded));
        }
        Pin::as_mut(&mut future).poll(context).map(Ok)
    })
    .await
}

/// Bounded pre-admission wait: cancellation remains cancellation; the absolute
/// cap (not the raw request deadline) maps to overload.
///
/// Does not hold service-owned mutable state across the await. The admission
/// deadline is an absolute `Instant` already reduced by the service edge.
pub(crate) async fn wait_for_admission_capacity<F>(
    control: &RequestControl,
    deadline_scheduler: &dyn RequestDeadlineScheduler,
    admission_deadline: Instant,
    future: F,
) -> Result<F::Output, AdmissionWaitError>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let mut cancelled = Box::pin(control.cancelled());
    let mut admission = deadline_scheduler.wait_until(admission_deadline);

    poll_fn(|context| {
        if control.is_cancelled() || Pin::as_mut(&mut cancelled).poll(context).is_ready() {
            return Poll::Ready(Err(AdmissionWaitError::Cancelled));
        }
        // Also treat an already-elapsed absolute cap as ready without waiting
        // for the scheduler future (covers clocks that only fire via poll of
        // a long-lived sleep registered against a later Instant).
        if control.is_deadline_exceeded()
            || Instant::now() >= admission_deadline
            || Pin::as_mut(&mut admission).poll(context).is_ready()
        {
            return Poll::Ready(Err(AdmissionWaitError::AdmissionCapExceeded));
        }
        Pin::as_mut(&mut future).poll(context).map(Ok)
    })
    .await
}
