//! Closed worker failures that contain no event payload or connector response.

use std::error::Error;
use std::fmt;

use riffdb_storage_api::{StorageError, StorageValueError};

use crate::OutboxFailpoint;

/// Failure to obtain a canonical worker timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxClockError {
    /// The clock source was temporarily unavailable.
    Unavailable,
    /// The clock returned a value outside its supported range.
    InvalidValue,
}

impl fmt::Display for OutboxClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "outbox clock is unavailable",
            Self::InvalidValue => "outbox clock returned an invalid value",
        })
    }
}

impl Error for OutboxClockError {}

/// Phase of an exact delivery-status compare-and-transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxTransitionPhase {
    /// Pending status was being claimed for delivery.
    Claim,
    /// An accepted external delivery was being made durable.
    Succeed,
    /// A retryable failure was being scheduled.
    Retry,
    /// A terminal failure was being moved to dead letter.
    DeadLetter,
    /// Interrupted delivery was being normalized during startup.
    Recovery,
}

/// Closed, redaction-safe outbox worker failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxWorkerError {
    /// The specialized storage port failed.
    Storage(StorageError),
    /// A worker-selected semantic value could not be constructed.
    InvalidWorkerValue(StorageValueError),
    /// The injected worker clock failed.
    Clock(OutboxClockError),
    /// Another actor changed an exact status observation.
    StateChanged {
        /// Transition whose expected state no longer matched.
        phase: OutboxTransitionPhase,
    },
    /// Authoritative event/intent reciprocity was absent after readiness.
    AuthoritativeIntentMissing,
    /// A named deterministic failpoint interrupted processing.
    Interrupted {
        /// Exact failpoint reached.
        failpoint: OutboxFailpoint,
    },
}

impl fmt::Display for OutboxWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "{error}"),
            Self::InvalidWorkerValue(error) => write!(formatter, "{error}"),
            Self::Clock(error) => write!(formatter, "{error}"),
            Self::StateChanged { phase } => {
                write!(formatter, "outbox status changed during {phase:?}")
            }
            Self::AuthoritativeIntentMissing => {
                formatter.write_str("authoritative outbox intent is missing")
            }
            Self::Interrupted { failpoint } => {
                write!(
                    formatter,
                    "outbox worker interrupted at {}",
                    failpoint.name()
                )
            }
        }
    }
}

impl Error for OutboxWorkerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::InvalidWorkerValue(error) => Some(error),
            Self::Clock(error) => Some(error),
            Self::StateChanged { .. }
            | Self::AuthoritativeIntentMissing
            | Self::Interrupted { .. } => None,
        }
    }
}

impl From<StorageError> for OutboxWorkerError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<StorageValueError> for OutboxWorkerError {
    fn from(error: StorageValueError) -> Self {
        Self::InvalidWorkerValue(error)
    }
}

impl From<OutboxClockError> for OutboxWorkerError {
    fn from(error: OutboxClockError) -> Self {
        Self::Clock(error)
    }
}
