//! Checked worker-local delivery, retry, and lease policy.

use std::error::Error;
use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

use riffdb_storage_api::{OutboxDestinationIdV1, OutboxPageLimit};
use riffdb_types::Timestamp;

use crate::OutboxClockError;

/// Maximum delivery attempts retained by the POC worker policy.
pub const MAX_OUTBOX_DELIVERY_ATTEMPTS: u32 = 32;
/// Maximum connector timeout in seconds.
pub const MAX_OUTBOX_DELIVERY_TIMEOUT_SECONDS: u32 = 3_600;
/// Maximum lease or individual retry delay in seconds.
pub const MAX_OUTBOX_POLICY_DELAY_SECONDS: u32 = 86_400;

/// Invalid worker configuration detected before the dispatcher starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryPolicyError {
    /// A configured value exceeds a fixed POC hard bound.
    LimitExceeded,
    /// Retry schedule length does not match the maximum attempts.
    RetryScheduleMismatch,
    /// The delivery lease is shorter than the connector timeout.
    LeaseShorterThanTimeout,
}

impl fmt::Display for DeliveryPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::LimitExceeded => "outbox delivery policy exceeds a hard limit",
            Self::RetryScheduleMismatch => {
                "outbox retry schedule must contain one delay between each attempt"
            }
            Self::LeaseShorterThanTimeout => {
                "outbox delivery lease must cover the connector timeout"
            }
        })
    }
}

impl Error for DeliveryPolicyError {}

/// Checked, deterministic policy for one configured outbox destination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryPolicy {
    destination_id: OutboxDestinationIdV1,
    delivery_timeout_seconds: NonZeroU32,
    lease_seconds: NonZeroU32,
    max_attempts: NonZeroU32,
    retry_delays_seconds: Vec<NonZeroU32>,
    scan_limit: OutboxPageLimit,
}

impl DeliveryPolicy {
    /// Validates explicit retry, timeout, lease, and scan bounds.
    pub fn new(
        destination_id: OutboxDestinationIdV1,
        delivery_timeout_seconds: NonZeroU32,
        lease_seconds: NonZeroU32,
        max_attempts: NonZeroU32,
        retry_delays_seconds: Vec<NonZeroU32>,
        scan_limit: OutboxPageLimit,
    ) -> Result<Self, DeliveryPolicyError> {
        if delivery_timeout_seconds.get() > MAX_OUTBOX_DELIVERY_TIMEOUT_SECONDS
            || lease_seconds.get() > MAX_OUTBOX_POLICY_DELAY_SECONDS
            || max_attempts.get() > MAX_OUTBOX_DELIVERY_ATTEMPTS
            || retry_delays_seconds
                .iter()
                .any(|delay| delay.get() > MAX_OUTBOX_POLICY_DELAY_SECONDS)
        {
            return Err(DeliveryPolicyError::LimitExceeded);
        }
        let expected_delays = usize::try_from(max_attempts.get() - 1)
            .map_err(|_| DeliveryPolicyError::LimitExceeded)?;
        if retry_delays_seconds.len() != expected_delays {
            return Err(DeliveryPolicyError::RetryScheduleMismatch);
        }
        if lease_seconds < delivery_timeout_seconds {
            return Err(DeliveryPolicyError::LeaseShorterThanTimeout);
        }
        Ok(Self {
            destination_id,
            delivery_timeout_seconds,
            lease_seconds,
            max_attempts,
            retry_delays_seconds,
            scan_limit,
        })
    }

    /// Borrows the storage-safe destination configuration reference.
    #[must_use]
    pub const fn destination_id(&self) -> &OutboxDestinationIdV1 {
        &self.destination_id
    }

    /// Returns the connector timeout in seconds.
    #[must_use]
    pub const fn delivery_timeout_seconds(&self) -> NonZeroU32 {
        self.delivery_timeout_seconds
    }

    /// Returns the in-flight lease duration in seconds.
    #[must_use]
    pub const fn lease_seconds(&self) -> NonZeroU32 {
        self.lease_seconds
    }

    /// Returns the maximum number of connector calls.
    #[must_use]
    pub const fn max_attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }

    /// Returns the checked storage scan limit used by each worker pass.
    #[must_use]
    pub const fn scan_limit(&self) -> OutboxPageLimit {
        self.scan_limit
    }

    /// Returns whether another connector attempt is permitted.
    #[must_use]
    pub const fn permits_attempt_after(&self, attempts: u32) -> bool {
        attempts < self.max_attempts.get()
    }

    /// Computes the retry instant after a failed nonzero attempt.
    pub fn retry_at(
        &self,
        from: Timestamp,
        failed_attempt: NonZeroU32,
    ) -> Result<Option<Timestamp>, OutboxClockError> {
        if !self.permits_attempt_after(failed_attempt.get()) {
            return Ok(None);
        }
        let index = usize::try_from(failed_attempt.get() - 1)
            .map_err(|_| OutboxClockError::InvalidValue)?;
        let delay = self
            .retry_delays_seconds
            .get(index)
            .ok_or(OutboxClockError::InvalidValue)?;
        add_seconds(from, delay.get()).map(Some)
    }

    /// Computes a lease deadline from one sampled attempt start.
    pub fn lease_deadline(&self, started_at: Timestamp) -> Result<Timestamp, OutboxClockError> {
        add_seconds(started_at, self.lease_seconds.get())
    }
}

/// Injected wall-clock source used only by the outbox worker.
pub trait OutboxClock {
    /// Returns one canonical current timestamp.
    fn now(&mut self) -> Result<Timestamp, OutboxClockError>;
}

fn add_seconds(timestamp: Timestamp, seconds: u32) -> Result<Timestamp, OutboxClockError> {
    let seconds = timestamp
        .seconds()
        .checked_add(i64::from(seconds))
        .ok_or(OutboxClockError::InvalidValue)?;
    Timestamp::new(seconds, timestamp.nanoseconds()).map_err(|_| OutboxClockError::InvalidValue)
}

/// Convenience policy used by deterministic examples and tests.
pub fn deterministic_test_policy() -> DeliveryPolicy {
    let destination =
        OutboxDestinationIdV1::new("test/deterministic").expect("static destination is valid");
    let timeout = NonZeroU32::new(5).expect("static timeout is nonzero");
    let lease = NonZeroU32::new(10).expect("static lease is nonzero");
    let attempts = NonZeroU32::new(3).expect("static attempts are nonzero");
    let delays = vec![
        NonZeroU32::new(1).expect("static delay is nonzero"),
        NonZeroU32::new(2).expect("static delay is nonzero"),
    ];
    let scan_limit =
        OutboxPageLimit::new(NonZeroU16::new(50).expect("static scan limit is nonzero"))
            .expect("static scan limit is bounded");
    DeliveryPolicy::new(destination, timeout, lease, attempts, delays, scan_limit)
        .expect("static policy is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_requires_an_explicit_delay_between_attempts() {
        let result = DeliveryPolicy::new(
            OutboxDestinationIdV1::new("test").expect("destination"),
            NonZeroU32::new(1).expect("timeout"),
            NonZeroU32::new(2).expect("lease"),
            NonZeroU32::new(3).expect("attempts"),
            vec![NonZeroU32::new(1).expect("delay")],
            OutboxPageLimit::new(NonZeroU16::new(1).expect("limit")).expect("bounded"),
        );

        assert_eq!(result, Err(DeliveryPolicyError::RetryScheduleMismatch));
    }

    #[test]
    fn timestamp_addition_fails_closed_at_the_edge() {
        let policy = deterministic_test_policy();
        let edge = Timestamp::new(i64::MAX, 0).expect("timestamp");

        assert_eq!(
            policy.lease_deadline(edge),
            Err(OutboxClockError::InvalidValue)
        );
    }
}
