//! Platform-independent date, timestamp, and logical-time values.

use std::error::Error;
use std::fmt;

/// The number of nanoseconds in one second.
pub const NANOS_PER_SECOND: u32 = 1_000_000_000;

/// A UTC instant represented without a timezone or operating-system clock.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp {
    seconds: i64,
    nanoseconds: u32,
}

impl Timestamp {
    /// Validates and creates a Unix timestamp.
    pub const fn new(seconds: i64, nanoseconds: u32) -> Result<Self, TimestampError> {
        if nanoseconds >= NANOS_PER_SECOND {
            return Err(TimestampError::NanosecondsOutOfRange { nanoseconds });
        }
        Ok(Self {
            seconds,
            nanoseconds,
        })
    }

    /// Returns whole signed seconds since the Unix epoch.
    #[must_use]
    pub const fn seconds(self) -> i64 {
        self.seconds
    }

    /// Returns the fractional nanosecond component.
    #[must_use]
    pub const fn nanoseconds(self) -> u32 {
        self.nanoseconds
    }

    /// Returns the fractional nanosecond component.
    #[must_use]
    pub const fn nanos(self) -> u32 {
        self.nanoseconds
    }
}

/// A safe timestamp construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampError {
    /// The fractional component is not in `0..1_000_000_000`.
    NanosecondsOutOfRange {
        /// The invalid nanosecond component.
        nanoseconds: u32,
    },
}

impl fmt::Display for TimestampError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NanosecondsOutOfRange { nanoseconds } => write!(
                formatter,
                "timestamp nanoseconds {nanoseconds} are outside 0..1_000_000_000"
            ),
        }
    }
}

impl Error for TimestampError {}

/// A calendar date represented as signed days since the Unix epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Date(i32);

impl Date {
    /// Creates a date from its canonical day count.
    #[must_use]
    pub const fn new(days_since_unix_epoch: i32) -> Self {
        Self(days_since_unix_epoch)
    }

    /// Creates a date from its canonical day count.
    #[must_use]
    pub const fn from_days_since_unix_epoch(days: i32) -> Self {
        Self::new(days)
    }

    /// Returns signed days since the Unix epoch.
    #[must_use]
    pub const fn days_since_unix_epoch(self) -> i32 {
        self.0
    }
}

/// Coordinator-supplied deterministic transaction time.
///
/// This wrapper intentionally has no wall-clock constructor. Admission code must
/// supply a validated [`Timestamp`], and deterministic command code receives only
/// the resulting logical value.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogicalTime(Timestamp);

impl LogicalTime {
    /// Wraps a coordinator-supplied timestamp as deterministic logical time.
    #[must_use]
    pub const fn new(timestamp: Timestamp) -> Self {
        Self(timestamp)
    }

    /// Returns the underlying UTC timestamp.
    #[must_use]
    pub const fn timestamp(self) -> Timestamp {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_accepts_only_canonical_nanoseconds() {
        assert!(Timestamp::new(i64::MIN, 0).is_ok());
        assert!(Timestamp::new(i64::MAX, NANOS_PER_SECOND - 1).is_ok());
        assert_eq!(
            Timestamp::new(0, NANOS_PER_SECOND),
            Err(TimestampError::NanosecondsOutOfRange {
                nanoseconds: NANOS_PER_SECOND
            })
        );
    }

    #[test]
    fn date_and_logical_time_are_exact_wrappers() {
        let date = Date::from_days_since_unix_epoch(-1);
        assert_eq!(date.days_since_unix_epoch(), -1);

        let timestamp = Timestamp::new(42, 7).expect("valid timestamp");
        let logical = LogicalTime::new(timestamp);
        assert_eq!(logical.timestamp(), timestamp);
    }
}
