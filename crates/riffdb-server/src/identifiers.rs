//! Server-owned production UUIDv7 providers.

// These providers are consumed by the WP-130 production graph assembled in this crate.
#![allow(dead_code)]

use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use riffdb_commit::{ProvenanceIdSource, ProvenanceIdSourceError};
use riffdb_errors::{IncidentIdSource, IncidentIdSourceError};
use riffdb_types::{DatabaseId, IncidentId, ProvenanceId, RequestId, UuidV7ConstructionError};

const UUID_V7_MAX_UNIX_MILLISECONDS: u64 = 0xffff_ffff_ffff;

trait UuidV7Clock: Send + Sync {
    fn sample(&self) -> Result<SystemTime, SystemUuidV7Error>;
}

trait UuidV7Entropy: Send + Sync {
    fn fill_random(&self, destination: &mut [u8; 10]) -> Result<(), SystemUuidV7Error>;
}

struct OperatingSystemUuidClock;

impl UuidV7Clock for OperatingSystemUuidClock {
    fn sample(&self) -> Result<SystemTime, SystemUuidV7Error> {
        Ok(SystemTime::now())
    }
}

struct OperatingSystemUuidEntropy;

impl UuidV7Entropy for OperatingSystemUuidEntropy {
    fn fill_random(&self, destination: &mut [u8; 10]) -> Result<(), SystemUuidV7Error> {
        getrandom::fill(destination).map_err(|_| SystemUuidV7Error::Entropy)
    }
}

fn unix_milliseconds(sample: SystemTime) -> Result<u64, SystemUuidV7Error> {
    let elapsed = sample
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SystemUuidV7Error::Clock)?;
    let milliseconds = u64::try_from(elapsed.as_millis()).map_err(|_| SystemUuidV7Error::Clock)?;
    if milliseconds > UUID_V7_MAX_UNIX_MILLISECONDS {
        return Err(SystemUuidV7Error::Clock);
    }
    Ok(milliseconds)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SystemUuidV7Error {
    Clock,
    Entropy,
    Construction,
}

impl fmt::Debug for SystemUuidV7Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SystemUuidV7Error([REDACTED])")
    }
}

struct SystemUuidV7Source {
    clock: Arc<dyn UuidV7Clock>,
    entropy: Arc<dyn UuidV7Entropy>,
}

impl SystemUuidV7Source {
    fn production() -> Self {
        Self {
            clock: Arc::new(OperatingSystemUuidClock),
            entropy: Arc::new(OperatingSystemUuidEntropy),
        }
    }

    fn next<T>(
        &self,
        constructor: fn(u64, [u8; 10]) -> Result<T, UuidV7ConstructionError>,
    ) -> Result<T, SystemUuidV7Error> {
        // ADR-0018 fixes this order and permits no retry or fallback.
        let milliseconds = unix_milliseconds(self.clock.sample()?)?;
        let mut random = [0_u8; 10];
        self.entropy.fill_random(&mut random)?;
        constructor(milliseconds, random).map_err(|_| SystemUuidV7Error::Construction)
    }
}

impl fmt::Debug for SystemUuidV7Source {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SystemUuidV7Source([REDACTED])")
    }
}

/// The complete server-owned UUID source set for one production component graph.
pub(crate) struct ProductionIdentifierSources {
    source: Arc<SystemUuidV7Source>,
}

impl ProductionIdentifierSources {
    pub(crate) fn new() -> Self {
        Self {
            source: Arc::new(SystemUuidV7Source::production()),
        }
    }

    pub(crate) fn database_ids(&self) -> DatabaseIdCandidateSource {
        DatabaseIdCandidateSource(Arc::clone(&self.source))
    }

    pub(crate) fn provenance_ids(&self) -> ServerProvenanceIdSource {
        ServerProvenanceIdSource(Arc::clone(&self.source))
    }

    pub(crate) fn request_ids(&self) -> ServerRequestIdSource {
        ServerRequestIdSource(Arc::clone(&self.source))
    }

    pub(crate) fn incident_ids(&self) -> ServerIncidentIdSource {
        ServerIncidentIdSource(Arc::clone(&self.source))
    }
}

impl Default for ProductionIdentifierSources {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ProductionIdentifierSources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionIdentifierSources([REDACTED])")
    }
}

/// Source used only after the source-free identity probe reports an empty database.
#[derive(Clone)]
pub(crate) struct DatabaseIdCandidateSource(Arc<SystemUuidV7Source>);

impl DatabaseIdCandidateSource {
    pub(crate) fn next_database_id(&self) -> Result<DatabaseId, ServerIdentifierSourceError> {
        self.0
            .next(DatabaseId::from_unix_milliseconds_and_random)
            .map_err(|_| ServerIdentifierSourceError)
    }
}

impl fmt::Debug for DatabaseIdCandidateSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DatabaseIdCandidateSource([REDACTED])")
    }
}

/// Server-side request IDs for hosted transports that own a consumer port.
#[derive(Clone)]
pub(crate) struct ServerRequestIdSource(Arc<SystemUuidV7Source>);

impl ServerRequestIdSource {
    pub(crate) fn next_request_id(&self) -> Result<RequestId, ServerIdentifierSourceError> {
        self.0
            .next(RequestId::from_unix_milliseconds_and_random)
            .map_err(|_| ServerIdentifierSourceError)
    }
}

impl riffdb_api_mcp::RequestIdSource for ServerRequestIdSource {
    fn next_request_id(
        &self,
    ) -> Result<riffdb_api_mcp::McpRequestId, riffdb_api_mcp::RequestIdSourceError> {
        let request_id = ServerRequestIdSource::next_request_id(self)
            .map_err(|_| riffdb_api_mcp::RequestIdSourceError)?;
        riffdb_api_mcp::McpRequestId::from_public_bytes(&request_id.into_bytes())
    }
}

impl fmt::Debug for ServerRequestIdSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerRequestIdSource([REDACTED])")
    }
}

/// Production adapter for the commit-owned provenance consumer port.
#[derive(Clone)]
pub(crate) struct ServerProvenanceIdSource(Arc<SystemUuidV7Source>);

impl ProvenanceIdSource for ServerProvenanceIdSource {
    fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError> {
        self.0
            .next(ProvenanceId::from_unix_milliseconds_and_random)
            .map_err(|_| ProvenanceIdSourceError)
    }
}

impl fmt::Debug for ServerProvenanceIdSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerProvenanceIdSource([REDACTED])")
    }
}

/// Production adapter for the errors-owned incident consumer port.
#[derive(Clone)]
pub(crate) struct ServerIncidentIdSource(Arc<SystemUuidV7Source>);

impl IncidentIdSource for ServerIncidentIdSource {
    fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError> {
        self.0
            .next(IncidentId::from_unix_milliseconds_and_random)
            .map_err(|_| IncidentIdSourceError)
    }
}

impl fmt::Debug for ServerIncidentIdSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerIncidentIdSource([REDACTED])")
    }
}

/// Closed server-composition failure before a checked database or request ID exists.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct ServerIdentifierSourceError;

impl fmt::Debug for ServerIdentifierSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerIdentifierSourceError([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;

    struct RecordingClock {
        result: Result<SystemTime, SystemUuidV7Error>,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl UuidV7Clock for RecordingClock {
        fn sample(&self) -> Result<SystemTime, SystemUuidV7Error> {
            self.calls.lock().expect("clock call log").push("clock");
            self.result
        }
    }

    struct RecordingEntropy {
        result: Result<[u8; 10], SystemUuidV7Error>,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl UuidV7Entropy for RecordingEntropy {
        fn fill_random(&self, destination: &mut [u8; 10]) -> Result<(), SystemUuidV7Error> {
            self.calls.lock().expect("entropy call log").push("entropy");
            *destination = self.result?;
            Ok(())
        }
    }

    fn source(clock: Arc<RecordingClock>, entropy: Arc<RecordingEntropy>) -> SystemUuidV7Source {
        SystemUuidV7Source { clock, entropy }
    }

    fn time_at_unix_milliseconds(milliseconds: u64) -> SystemTime {
        UNIX_EPOCH
            .checked_add(Duration::from_millis(milliseconds))
            .expect("test time must fit SystemTime")
    }

    #[test]
    fn source_samples_clock_then_fills_once_and_matches_the_uuid_golden() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let clock = Arc::new(RecordingClock {
            result: Ok(time_at_unix_milliseconds(0x0123_4567_89ab)),
            calls: Arc::clone(&calls),
        });
        let entropy = Arc::new(RecordingEntropy {
            result: Ok([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            calls: Arc::clone(&calls),
        });

        let request = source(Arc::clone(&clock), Arc::clone(&entropy))
            .next(RequestId::from_unix_milliseconds_and_random)
            .expect("checked request ID");

        assert_eq!(
            request.into_bytes(),
            [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0x70, 0x01, 0x82, 0x03, 0x04, 0x05, 0x06, 0x07,
                0x08, 0x09,
            ]
        );
        assert_eq!(
            *calls.lock().expect("source call order"),
            ["clock", "entropy"]
        );
    }

    #[test]
    fn clock_and_entropy_failures_do_not_retry_or_fabricate_an_id() {
        let clock = Arc::new(RecordingClock {
            result: Err(SystemUuidV7Error::Clock),
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let entropy = Arc::new(RecordingEntropy {
            result: Ok([7; 10]),
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        assert_eq!(
            source(Arc::clone(&clock), Arc::clone(&entropy))
                .next(DatabaseId::from_unix_milliseconds_and_random),
            Err(SystemUuidV7Error::Clock)
        );
        assert_eq!(*clock.calls.lock().expect("clock calls"), ["clock"]);
        assert!(entropy.calls.lock().expect("entropy calls").is_empty());

        let clock = Arc::new(RecordingClock {
            result: Ok(time_at_unix_milliseconds(1)),
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let entropy = Arc::new(RecordingEntropy {
            result: Err(SystemUuidV7Error::Entropy),
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        assert_eq!(
            source(Arc::clone(&clock), Arc::clone(&entropy))
                .next(DatabaseId::from_unix_milliseconds_and_random),
            Err(SystemUuidV7Error::Entropy)
        );
        assert_eq!(*clock.calls.lock().expect("clock calls"), ["clock"]);
        assert_eq!(*entropy.calls.lock().expect("entropy calls"), ["entropy"]);
    }

    #[test]
    fn exact_timestamp_limit_is_accepted_with_one_sample_and_fill() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let clock = Arc::new(RecordingClock {
            result: Ok(time_at_unix_milliseconds(UUID_V7_MAX_UNIX_MILLISECONDS)),
            calls: Arc::clone(&calls),
        });
        let entropy = Arc::new(RecordingEntropy {
            result: Ok([9; 10]),
            calls: Arc::clone(&calls),
        });

        let request = source(clock, entropy)
            .next(RequestId::from_unix_milliseconds_and_random)
            .expect("maximum 48-bit timestamp");

        assert_eq!(&request.into_bytes()[..6], &[0xff; 6]);
        assert_eq!(
            *calls.lock().expect("source call order"),
            ["clock", "entropy"]
        );
    }

    #[test]
    fn timestamp_above_48_bits_is_rejected_before_entropy() {
        let clock = Arc::new(RecordingClock {
            result: Ok(time_at_unix_milliseconds(UUID_V7_MAX_UNIX_MILLISECONDS + 1)),
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let entropy = Arc::new(RecordingEntropy {
            result: Ok([9; 10]),
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        assert_eq!(
            source(Arc::clone(&clock), Arc::clone(&entropy))
                .next(RequestId::from_unix_milliseconds_and_random),
            Err(SystemUuidV7Error::Clock)
        );
        assert_eq!(*clock.calls.lock().expect("clock calls"), ["clock"]);
        assert!(entropy.calls.lock().expect("entropy calls").is_empty());
    }

    #[test]
    fn pre_epoch_clock_is_rejected_before_entropy() {
        let clock = Arc::new(RecordingClock {
            result: Ok(UNIX_EPOCH
                .checked_sub(Duration::from_millis(1))
                .expect("test time must fit SystemTime")),
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let entropy = Arc::new(RecordingEntropy {
            result: Ok([9; 10]),
            calls: Arc::new(Mutex::new(Vec::new())),
        });

        assert_eq!(
            source(Arc::clone(&clock), Arc::clone(&entropy))
                .next(RequestId::from_unix_milliseconds_and_random),
            Err(SystemUuidV7Error::Clock)
        );
        assert_eq!(*clock.calls.lock().expect("clock calls"), ["clock"]);
        assert!(entropy.calls.lock().expect("entropy calls").is_empty());
    }

    #[test]
    fn wrappers_keep_domain_types_distinct_and_propagate_closed_errors() {
        let shared = Arc::new(source(
            Arc::new(RecordingClock {
                result: Ok(time_at_unix_milliseconds(7)),
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
            Arc::new(RecordingEntropy {
                result: Ok([8; 10]),
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
        ));
        let database = DatabaseIdCandidateSource(Arc::clone(&shared));
        let request = ServerRequestIdSource(Arc::clone(&shared));
        let provenance = ServerProvenanceIdSource(Arc::clone(&shared));
        let incident = ServerIncidentIdSource(shared);

        assert!(database.next_database_id().is_ok());
        assert!(request.next_request_id().is_ok());
        assert!(provenance.next_provenance_id().is_ok());
        assert!(incident.next_incident_id().is_ok());

        let failed = Arc::new(source(
            Arc::new(RecordingClock {
                result: Err(SystemUuidV7Error::Clock),
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
            Arc::new(RecordingEntropy {
                result: Ok([0; 10]),
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
        ));
        assert_eq!(
            ServerProvenanceIdSource(Arc::clone(&failed)).next_provenance_id(),
            Err(ProvenanceIdSourceError)
        );
        assert_eq!(
            ServerIncidentIdSource(failed).next_incident_id(),
            Err(IncidentIdSourceError)
        );
    }

    #[test]
    fn debug_output_redacts_provider_state() {
        let sources = ProductionIdentifierSources::new();
        assert_eq!(
            format!("{sources:?}"),
            "ProductionIdentifierSources([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", sources.database_ids()),
            "DatabaseIdCandidateSource([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", sources.request_ids()),
            "ServerRequestIdSource([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", sources.provenance_ids()),
            "ServerProvenanceIdSource([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", sources.incident_ids()),
            "ServerIncidentIdSource([REDACTED])"
        );
        assert_eq!(
            format!("{:?}", ServerIdentifierSourceError),
            "ServerIdentifierSourceError([REDACTED])"
        );
    }
}
