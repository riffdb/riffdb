//! Distinct production wall-clock adapters for semantic consumer ports.

// These providers are consumed by the WP-130 production graph assembled in this crate.
#![allow(dead_code)]

use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use riffdb_auth::{AuthenticationClock, AuthenticationClockError};
use riffdb_commit::{
    AdministrationClock, AdministrationClockError, AdmissionClock, AdmissionClockError,
};
use riffdb_outbox::{OutboxClock, OutboxClockError};
use riffdb_policy::{AuthorizationClock, AuthorizationClockError};
use riffdb_types::Timestamp;

trait WallTimeSource: Send + Sync {
    fn now(&self) -> Result<SystemTime, CanonicalWallClockError>;
}

struct OperatingSystemWallTime;

impl WallTimeSource for OperatingSystemWallTime {
    fn now(&self) -> Result<SystemTime, CanonicalWallClockError> {
        Ok(SystemTime::now())
    }
}

struct CanonicalWallClock {
    source: Arc<dyn WallTimeSource>,
}

impl CanonicalWallClock {
    fn production() -> Self {
        Self {
            source: Arc::new(OperatingSystemWallTime),
        }
    }

    fn now(&self) -> Result<Timestamp, CanonicalWallClockError> {
        timestamp_from_system_time(self.source.now()?)
    }
}

impl fmt::Debug for CanonicalWallClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CanonicalWallClock([REDACTED])")
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct CanonicalWallClockError;

impl fmt::Debug for CanonicalWallClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CanonicalWallClockError([REDACTED])")
    }
}

fn timestamp_from_system_time(time: SystemTime) -> Result<Timestamp, CanonicalWallClockError> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => {
            let seconds = i64::try_from(elapsed.as_secs()).map_err(|_| CanonicalWallClockError)?;
            Timestamp::new(seconds, elapsed.subsec_nanos()).map_err(|_| CanonicalWallClockError)
        }
        Err(before_epoch) => {
            let elapsed = before_epoch.duration();
            let nanos = elapsed.subsec_nanos();
            let seconds = negative_timestamp_seconds(elapsed.as_secs(), nanos)?;
            let canonical_nanos = if nanos == 0 { 0 } else { 1_000_000_000 - nanos };
            Timestamp::new(seconds, canonical_nanos).map_err(|_| CanonicalWallClockError)
        }
    }
}

fn negative_timestamp_seconds(
    elapsed_seconds: u64,
    subsecond_nanos: u32,
) -> Result<i64, CanonicalWallClockError> {
    if subsecond_nanos == 0 && elapsed_seconds == (i64::MAX as u64) + 1 {
        return Ok(i64::MIN);
    }
    let seconds = i64::try_from(elapsed_seconds).map_err(|_| CanonicalWallClockError)?;
    if subsecond_nanos == 0 {
        seconds.checked_neg().ok_or(CanonicalWallClockError)
    } else {
        seconds
            .checked_add(1)
            .and_then(i64::checked_neg)
            .ok_or(CanonicalWallClockError)
    }
}

/// Type-distinct clocks backed by one stateless canonical wall-clock implementation.
pub(crate) struct ProductionWallClocks {
    shared: Arc<CanonicalWallClock>,
}

impl ProductionWallClocks {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(CanonicalWallClock::production()),
        }
    }

    pub(crate) fn authentication(&self) -> ServerAuthenticationClock {
        ServerAuthenticationClock(Arc::clone(&self.shared))
    }

    pub(crate) fn authorization(&self) -> ServerAuthorizationClock {
        ServerAuthorizationClock(Arc::clone(&self.shared))
    }

    pub(crate) fn admission(&self) -> ServerAdmissionClock {
        ServerAdmissionClock(Arc::clone(&self.shared))
    }

    pub(crate) fn administration(&self) -> ServerAdministrationClock {
        ServerAdministrationClock(Arc::clone(&self.shared))
    }

    pub(crate) fn outbox(&self) -> ServerOutboxClock {
        ServerOutboxClock(Arc::clone(&self.shared))
    }

    /// Samples server-owned process metadata without borrowing a semantic consumer port.
    pub(crate) fn process_time(&self) -> Result<Timestamp, ServerProcessClockError> {
        self.shared.now().map_err(|_| ServerProcessClockError)
    }
}

impl Default for ProductionWallClocks {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ProductionWallClocks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionWallClocks([REDACTED])")
    }
}

/// Closed server-owned clock failure for immutable process metadata.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct ServerProcessClockError;

impl fmt::Debug for ServerProcessClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerProcessClockError([REDACTED])")
    }
}

impl fmt::Display for ServerProcessClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("process wall-clock sample failed")
    }
}

impl std::error::Error for ServerProcessClockError {}

/// A settable wall-time source used only by this crate's tests.
///
/// Production composition never reaches this: `ProductionWallClocks::new` is
/// the sole constructor outside `cfg(test)`, and the whole type is compiled out
/// of normal builds.
#[cfg(test)]
struct SettableWallTime(Arc<std::sync::atomic::AtomicI64>);

#[cfg(test)]
impl WallTimeSource for SettableWallTime {
    fn now(&self) -> Result<SystemTime, CanonicalWallClockError> {
        let seconds = self.0.load(std::sync::atomic::Ordering::Acquire);
        let seconds = u64::try_from(seconds).map_err(|_| CanonicalWallClockError)?;
        Ok(UNIX_EPOCH + std::time::Duration::from_secs(seconds))
    }
}

#[cfg(test)]
impl ProductionWallClocks {
    /// Builds clocks whose every consumer reads one settable second counter.
    pub(crate) fn settable(seconds: Arc<std::sync::atomic::AtomicI64>) -> Self {
        Self {
            shared: Arc::new(CanonicalWallClock {
                source: Arc::new(SettableWallTime(seconds)),
            }),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ServerAuthenticationClock(Arc<CanonicalWallClock>);

impl AuthenticationClock for ServerAuthenticationClock {
    fn now(&self) -> Result<Timestamp, AuthenticationClockError> {
        self.0.now().map_err(|_| AuthenticationClockError)
    }
}

impl fmt::Debug for ServerAuthenticationClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerAuthenticationClock([REDACTED])")
    }
}

#[derive(Clone)]
pub(crate) struct ServerAuthorizationClock(Arc<CanonicalWallClock>);

impl AuthorizationClock for ServerAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        self.0.now().map_err(|_| AuthorizationClockError)
    }
}

impl fmt::Debug for ServerAuthorizationClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerAuthorizationClock([REDACTED])")
    }
}

#[derive(Clone)]
pub(crate) struct ServerAdmissionClock(Arc<CanonicalWallClock>);

impl AdmissionClock for ServerAdmissionClock {
    fn now(&self) -> Result<Timestamp, AdmissionClockError> {
        self.0.now().map_err(|_| AdmissionClockError)
    }
}

impl fmt::Debug for ServerAdmissionClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerAdmissionClock([REDACTED])")
    }
}

#[derive(Clone)]
pub(crate) struct ServerAdministrationClock(Arc<CanonicalWallClock>);

impl AdministrationClock for ServerAdministrationClock {
    fn now(&self) -> Result<Timestamp, AdministrationClockError> {
        self.0.now().map_err(|_| AdministrationClockError)
    }
}

impl riffdb_service::EventConsumerClock for ServerAdministrationClock {
    fn now(&self) -> Result<Timestamp, riffdb_service::EventConsumerClockError> {
        self.0
            .now()
            .map_err(|_| riffdb_service::EventConsumerClockError)
    }
}

impl fmt::Debug for ServerAdministrationClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerAdministrationClock([REDACTED])")
    }
}

pub(crate) struct ServerOutboxClock(Arc<CanonicalWallClock>);

impl OutboxClock for ServerOutboxClock {
    fn now(&mut self) -> Result<Timestamp, OutboxClockError> {
        self.0.now().map_err(|_| OutboxClockError::Unavailable)
    }
}

impl fmt::Debug for ServerOutboxClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerOutboxClock([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;

    struct ScriptedWallTime {
        values: Mutex<VecDeque<Result<SystemTime, CanonicalWallClockError>>>,
    }

    impl WallTimeSource for ScriptedWallTime {
        fn now(&self) -> Result<SystemTime, CanonicalWallClockError> {
            self.values
                .lock()
                .expect("scripted wall clock")
                .pop_front()
                .expect("one scripted sample")
        }
    }

    fn shared_clock(
        values: impl IntoIterator<Item = Result<SystemTime, CanonicalWallClockError>>,
    ) -> Arc<CanonicalWallClock> {
        Arc::new(CanonicalWallClock {
            source: Arc::new(ScriptedWallTime {
                values: Mutex::new(values.into_iter().collect()),
            }),
        })
    }

    #[test]
    fn canonical_conversion_preserves_positive_and_pre_epoch_instants() {
        let positive = UNIX_EPOCH + Duration::new(17, 23);
        let negative_fraction = UNIX_EPOCH - Duration::new(2, 250_000_000);
        let negative_whole = UNIX_EPOCH - Duration::new(2, 0);

        assert_eq!(
            timestamp_from_system_time(positive),
            Ok(Timestamp::new(17, 23).expect("timestamp"))
        );
        assert_eq!(
            timestamp_from_system_time(negative_fraction),
            Ok(Timestamp::new(-3, 750_000_000).expect("timestamp"))
        );
        assert_eq!(
            timestamp_from_system_time(negative_whole),
            Ok(Timestamp::new(-2, 0).expect("timestamp"))
        );
    }

    #[test]
    fn four_consumer_wrappers_remain_type_distinct_and_sample_independently() {
        let shared = shared_clock([
            Ok(UNIX_EPOCH + Duration::new(1, 1)),
            Ok(UNIX_EPOCH + Duration::new(2, 2)),
            Ok(UNIX_EPOCH + Duration::new(3, 3)),
            Ok(UNIX_EPOCH + Duration::new(4, 4)),
        ]);
        let authentication = ServerAuthenticationClock(Arc::clone(&shared));
        let authorization = ServerAuthorizationClock(Arc::clone(&shared));
        let admission = ServerAdmissionClock(Arc::clone(&shared));
        let administration = ServerAdministrationClock(shared);

        assert_eq!(
            authentication.now(),
            Ok(Timestamp::new(1, 1).expect("timestamp"))
        );
        assert_eq!(
            authorization.now(),
            Ok(Timestamp::new(2, 2).expect("timestamp"))
        );
        assert_eq!(
            admission.now(),
            Ok(Timestamp::new(3, 3).expect("timestamp"))
        );
        assert_eq!(
            administration.now(),
            Ok(Timestamp::new(4, 4).expect("timestamp"))
        );
    }

    #[test]
    fn process_metadata_uses_a_server_owned_sample_and_error() {
        let clocks = ProductionWallClocks {
            shared: shared_clock([Ok(UNIX_EPOCH + Duration::new(9, 8))]),
        };
        assert_eq!(
            clocks.process_time(),
            Ok(Timestamp::new(9, 8).expect("timestamp"))
        );

        let failing = ProductionWallClocks {
            shared: shared_clock([Err(CanonicalWallClockError)]),
        };
        assert_eq!(failing.process_time(), Err(ServerProcessClockError));
        assert_eq!(
            format!("{:?}", ServerProcessClockError),
            "ServerProcessClockError([REDACTED])"
        );
    }

    #[test]
    fn each_wrapper_maps_source_failure_to_its_consumer_owned_error() {
        let shared = shared_clock([
            Err(CanonicalWallClockError),
            Err(CanonicalWallClockError),
            Err(CanonicalWallClockError),
            Err(CanonicalWallClockError),
        ]);
        assert_eq!(
            ServerAuthenticationClock(Arc::clone(&shared)).now(),
            Err(AuthenticationClockError)
        );
        assert_eq!(
            ServerAuthorizationClock(Arc::clone(&shared)).now(),
            Err(AuthorizationClockError)
        );
        assert_eq!(
            ServerAdmissionClock(Arc::clone(&shared)).now(),
            Err(AdmissionClockError)
        );
        assert_eq!(
            ServerAdministrationClock(shared).now(),
            Err(AdministrationClockError)
        );
    }

    #[test]
    fn debug_output_redacts_clock_state() {
        let clocks = ProductionWallClocks::new();
        assert_eq!(format!("{clocks:?}"), "ProductionWallClocks([REDACTED])");
        assert_eq!(
            format!("{:?}", clocks.authentication()),
            "ServerAuthenticationClock([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", clocks.authorization()),
            "ServerAuthorizationClock([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", clocks.admission()),
            "ServerAdmissionClock([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", clocks.administration()),
            "ServerAdministrationClock([REDACTED])"
        );
    }
}
