//! Monotonic time used only by coordinator formation decisions.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

/// Monotonic time port for bounded coordinator coalescing.
///
/// The daemon installs the crate-private wall-clock implementation. The
/// simulator may implement this port only through its development-only
/// composition, so no request or runtime configuration can select time.
#[doc(hidden)]
pub trait CoordinatorMonotonicClock: Send + Sync + 'static {
    /// Samples the monotonic instant used to form a deadline.
    fn now(&self) -> Instant;

    /// Waits until the supplied monotonic deadline.
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Computes elapsed monotonic time without permitting a negative value.
    fn elapsed_since(&self, started: Instant) -> Duration {
        self.now().saturating_duration_since(started)
    }
}

pub(crate) struct WallCoordinatorMonotonicClock;

impl CoordinatorMonotonicClock for WallCoordinatorMonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
            deadline,
        )))
    }
}
