//! Coordinator-owned wall-clock consumer ports.

use std::error::Error;
use std::fmt;

use riffdb_types::Timestamp;

/// Synchronous wall clock used only to freeze command logical time.
///
/// The coordinator samples this port exactly once before creating a new
/// mutating admission and once for each unjournaled read-only invocation. It
/// does not sample the port when resuming a pending admission or replaying a
/// terminal result. The coordinator converts the returned canonical timestamp
/// to logical time without clamping, rounding, retrying, or substituting a
/// value. Production providers belong to server composition.
pub trait AdmissionClock: Send + Sync {
    /// Returns one fresh canonical timestamp for admission processing.
    fn now(&self) -> Result<Timestamp, AdmissionClockError>;
}

/// A redaction-safe failure to obtain command admission time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionClockError;

impl fmt::Display for AdmissionClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("admission clock is unavailable")
    }
}

impl Error for AdmissionClockError {}

/// Synchronous wall clock used only for coordinator-observed administration time.
///
/// The coordinator samples this port exactly once for each standalone service
/// audit, normal `started` audit, normal terminal audit, and catalog transition.
/// A new-bootstrap compound transition shares one sample between its `started`
/// and capability-administration records; exact bootstrap replay uses one
/// sample for its new `started` record, and either bootstrap path obtains a new
/// sample for its terminal record. Capability create and revoke instead reuse
/// their checked transaction-current authorization timestamp. Samples are not
/// required to be monotonic and never establish record order. Production
/// providers belong to server composition.
pub trait AdministrationClock: Send + Sync {
    /// Returns one fresh canonical timestamp for administration processing.
    fn now(&self) -> Result<Timestamp, AdministrationClockError>;
}

/// A redaction-safe failure to obtain administration time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdministrationClockError;

impl fmt::Display for AdministrationClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("administration clock is unavailable")
    }
}

impl Error for AdministrationClockError {}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedAdmissionClock(Timestamp);

    impl AdmissionClock for FixedAdmissionClock {
        fn now(&self) -> Result<Timestamp, AdmissionClockError> {
            Ok(self.0)
        }
    }

    struct FixedAdministrationClock(Timestamp);

    impl AdministrationClock for FixedAdministrationClock {
        fn now(&self) -> Result<Timestamp, AdministrationClockError> {
            Ok(self.0)
        }
    }

    fn read_admission_clock(clock: &dyn AdmissionClock) -> Result<Timestamp, AdmissionClockError> {
        clock.now()
    }

    fn read_administration_clock(
        clock: &dyn AdministrationClock,
    ) -> Result<Timestamp, AdministrationClockError> {
        clock.now()
    }

    #[test]
    fn clock_ports_are_synchronous_object_safe_value_sources() {
        let admission = Timestamp::new(-7, 999_999_999).expect("canonical timestamp");
        let administration = Timestamp::new(11, 23).expect("canonical timestamp");

        assert_eq!(
            read_admission_clock(&FixedAdmissionClock(admission)),
            Ok(admission)
        );
        assert_eq!(
            read_administration_clock(&FixedAdministrationClock(administration)),
            Ok(administration)
        );
    }

    #[test]
    fn clock_failures_expose_only_static_safe_text() {
        assert_eq!(
            AdmissionClockError.to_string(),
            "admission clock is unavailable"
        );
        assert_eq!(
            AdministrationClockError.to_string(),
            "administration clock is unavailable"
        );
        assert_eq!(format!("{AdmissionClockError:?}"), "AdmissionClockError");
        assert_eq!(
            format!("{AdministrationClockError:?}"),
            "AdministrationClockError"
        );
        assert!(AdmissionClockError.source().is_none());
        assert!(AdministrationClockError.source().is_none());
    }
}
