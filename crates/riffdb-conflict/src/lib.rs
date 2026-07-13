#![forbid(unsafe_code)]

//! Bounded, cancellation-safe acquisition of exclusive logical conflict keys.

mod cancellation;
mod manager;
mod telemetry;
#[cfg(any(test, feature = "loom", feature = "shuttle"))]
mod testing;

pub use cancellation::CancellationToken;
pub use manager::{
    AcquisitionFuture, ConflictError, ConflictManager, ConflictManagerBuildError,
    ConflictManagerConfig, ConflictManagerConfigError, MutationLease, ShardedConflictManager,
};
pub use telemetry::{ConflictEvent, ConflictEventKind, ConflictObserver, NoopConflictObserver};

#[cfg(any(test, feature = "loom", feature = "shuttle"))]
#[doc(hidden)]
pub use manager::ConflictTestDriver;
#[cfg(any(test, feature = "loom", feature = "shuttle"))]
#[doc(hidden)]
pub use testing::{ConflictSchedulePoint, DeterministicConflictScheduler};
