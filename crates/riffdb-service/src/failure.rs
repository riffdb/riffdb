//! Closed API-neutral service failures.

use std::{error::Error, fmt};

use riffdb_errors::{EmergencyInternalFailure, PublicError};

/// Result returned by every API-neutral service operation.
pub type ServiceResult<T> = Result<T, ServiceFailure>;

/// The only control failures that may cross the application-service boundary.
pub enum ServiceFailure {
    /// A fully checked caller-safe semantic failure.
    Public(PublicError),
    /// Cancellation was proven before the operation's applicable admission boundary.
    Cancelled,
    /// The request deadline elapsed before a protected result could be released.
    DeadlineExceeded,
    /// One complete authorized semantic result cannot fit the response ceiling.
    ResponseTooLarge,
    /// Internal containment could not obtain a fresh incident identifier.
    EmergencyInternal(EmergencyInternalFailure),
}

impl ServiceFailure {
    /// Returns the public semantic failure, when this is one.
    #[must_use]
    pub const fn public_error(&self) -> Option<&PublicError> {
        match self {
            Self::Public(error) => Some(error),
            Self::Cancelled
            | Self::DeadlineExceeded
            | Self::ResponseTooLarge
            | Self::EmergencyInternal(_) => None,
        }
    }
}

impl From<PublicError> for ServiceFailure {
    fn from(error: PublicError) -> Self {
        Self::Public(error)
    }
}

impl From<EmergencyInternalFailure> for ServiceFailure {
    fn from(error: EmergencyInternalFailure) -> Self {
        Self::EmergencyInternal(error)
    }
}

impl fmt::Debug for ServiceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Public(_) => "ServiceFailure::Public([REDACTED])",
            Self::Cancelled => "ServiceFailure::Cancelled",
            Self::DeadlineExceeded => "ServiceFailure::DeadlineExceeded",
            Self::ResponseTooLarge => "ServiceFailure::ResponseTooLarge",
            Self::EmergencyInternal(_) => "ServiceFailure::EmergencyInternal",
        })
    }
}

impl fmt::Display for ServiceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Public(error) => error.safe_message(),
            Self::Cancelled => "request was cancelled",
            Self::DeadlineExceeded => "request deadline elapsed",
            Self::ResponseTooLarge => "response exceeds the service limit",
            Self::EmergencyInternal(error) => error.safe_message(),
        })
    }
}

impl Error for ServiceFailure {}
