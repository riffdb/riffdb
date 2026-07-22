//! Cancellation- and deadline-aware admission waits.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;

use crate::{RequestControl, RequestDeadlineScheduler};

/// Closed reason a service wait ended before its protected operation completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlledWaitError {
    Cancelled,
    DeadlineExceeded,
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
