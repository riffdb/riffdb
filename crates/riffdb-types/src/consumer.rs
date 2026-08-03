//! Stable bounded identities used by durable event consumers.

use std::error::Error;
use std::fmt;
use std::num::{NonZeroU8, NonZeroU64};

use crate::{
    DatabaseId, EventConsumerIdentityHash, QueryParameterHash, ReactiveModuleHash,
    ReactiveOperationName, hash_event_consumer_identity,
};

/// Maximum visible-ASCII bytes in one durable consumer name.
pub const MAX_EVENT_CONSUMER_NAME_BYTES: usize = 64;
/// Exact opaque bytes in one attempt-specific lease token.
pub const EVENT_LEASE_TOKEN_BYTES: usize = 32;
/// Terminal failed-attempt count for one event delivery.
pub const MAX_EVENT_DELIVERY_ATTEMPTS: u8 = 10;

/// Hashes the one canonical complete durable consumer identity tuple.
#[must_use]
pub fn event_consumer_identity_hash(
    database_id: DatabaseId,
    module_hash: ReactiveModuleHash,
    operation_name: &ReactiveOperationName,
    parameter_hash: QueryParameterHash,
    consumer_name: &EventConsumerName,
) -> EventConsumerIdentityHash {
    let mut bytes = Vec::with_capacity(
        16 + 32 + 4 + operation_name.as_str().len() + 32 + 4 + consumer_name.as_bytes().len(),
    );
    bytes.extend_from_slice(database_id.as_bytes());
    bytes.extend_from_slice(module_hash.as_bytes());
    append_bytes(&mut bytes, operation_name.as_str().as_bytes());
    bytes.extend_from_slice(parameter_hash.as_bytes());
    append_bytes(&mut bytes, consumer_name.as_bytes());
    hash_event_consumer_identity(&bytes)
}

fn append_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    output.extend_from_slice(bytes);
}

/// One exact bounded application-selected durable consumer name.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventConsumerName(String);

impl EventConsumerName {
    /// Checks `[A-Za-z][A-Za-z0-9_-]{0,63}` and retains the exact bytes.
    pub fn new(value: impl Into<String>) -> Result<Self, EventConsumerNameError> {
        let value = value.into();
        let bytes = value.as_bytes();
        if bytes.is_empty()
            || bytes.len() > MAX_EVENT_CONSUMER_NAME_BYTES
            || !bytes[0].is_ascii_alphabetic()
            || !bytes[1..]
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(EventConsumerNameError);
        }
        Ok(Self(value))
    }

    /// Borrows the exact consumer name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the exact visible-ASCII bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for EventConsumerName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EventConsumerName([REDACTED])")
    }
}

/// A consumer name was empty, malformed, or exceeded its fixed bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventConsumerNameError;

impl fmt::Display for EventConsumerNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid event consumer name")
    }
}

impl Error for EventConsumerNameError {}

/// Monotonic compare-and-transition revision local to one consumer record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventConsumerRevision(NonZeroU64);

impl EventConsumerRevision {
    /// Constructs a nonzero revision.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the first revision.
    #[must_use]
    pub const fn first() -> Self {
        Self(NonZeroU64::MIN)
    }

    /// Returns the integer value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Advances without wrapping.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

/// One attempt number in the closed range `1..=10`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventDeliveryAttempt(NonZeroU8);

impl EventDeliveryAttempt {
    /// Checks the fixed attempt range.
    #[must_use]
    pub const fn new(value: u8) -> Option<Self> {
        match NonZeroU8::new(value) {
            Some(value) if value.get() <= MAX_EVENT_DELIVERY_ATTEMPTS => Some(Self(value)),
            _ => None,
        }
    }

    /// Returns the first delivery attempt.
    #[must_use]
    pub const fn first() -> Self {
        Self(NonZeroU8::MIN)
    }

    /// Returns the attempt number.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0.get()
    }

    /// Advances while another attempt remains.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        Self::new(self.0.get().saturating_add(1))
    }
}

/// One opaque attempt-specific lease token. Possession is not authority.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventLeaseToken([u8; EVENT_LEASE_TOKEN_BYTES]);

impl EventLeaseToken {
    /// Retains exactly 32 generated bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; EVENT_LEASE_TOKEN_BYTES]) -> Self {
        Self(bytes)
    }

    /// Borrows the exact opaque bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; EVENT_LEASE_TOKEN_BYTES] {
        &self.0
    }
}

impl fmt::Debug for EventLeaseToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EventLeaseToken([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_name_grammar_is_exact() {
        for accepted in ["a", "Worker_1", "worker-2", &"z".repeat(64)] {
            assert!(EventConsumerName::new(accepted).is_ok(), "{accepted}");
        }
        for rejected in ["", "1worker", "has.dot", "has space", &"z".repeat(65)] {
            assert!(EventConsumerName::new(rejected).is_err(), "{rejected}");
        }
    }

    #[test]
    fn revisions_and_attempts_never_wrap() {
        assert_eq!(EventConsumerRevision::first().get(), 1);
        assert!(
            EventConsumerRevision::new(u64::MAX)
                .expect("nonzero")
                .checked_next()
                .is_none()
        );
        assert_eq!(EventDeliveryAttempt::first().get(), 1);
        assert_eq!(
            EventDeliveryAttempt::new(10)
                .expect("bounded")
                .checked_next(),
            None
        );
        assert!(EventDeliveryAttempt::new(11).is_none());
    }
}
