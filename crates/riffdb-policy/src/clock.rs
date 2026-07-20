//! Current-policy wall-clock boundary.

use std::{error::Error, fmt};

use riffdb_types::Timestamp;

/// Synchronous wall clock sampled independently at every authorization safe point.
pub trait AuthorizationClock: Send + Sync {
    /// Returns one fresh canonical authorization timestamp.
    fn now(&self) -> Result<Timestamp, AuthorizationClockError>;
}

/// A redaction-safe failure to obtain current authorization time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthorizationClockError;

impl fmt::Display for AuthorizationClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("authorization clock is unavailable")
    }
}

impl Error for AuthorizationClockError {}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedClock;

    impl AuthorizationClock for FixedClock {
        fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
            Timestamp::new(17, 23).map_err(|_| AuthorizationClockError)
        }
    }

    #[test]
    fn clock_is_an_injected_synchronous_value_source() {
        assert_eq!(
            FixedClock.now().expect("fixed timestamp"),
            Timestamp::new(17, 23).expect("valid timestamp")
        );
        assert_eq!(
            AuthorizationClockError.to_string(),
            "authorization clock is unavailable"
        );
    }
}
