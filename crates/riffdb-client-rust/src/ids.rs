//! Stateless client-side UUIDv7 generation.

use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use riffdb_types::{
    AgentSessionId, ApplicationExportOperationId, CapabilityId, ContractMigrationOperationId,
    OfflineMaintenanceOperationId, RequestId, UuidV7ConstructionError,
};

const MAX_UUID_V7_UNIX_MILLISECONDS: u128 = 0xffff_ffff_ffff;

/// A safe local failure while generating one client identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifierGenerationError {
    /// The host clock is before the Unix epoch.
    ClockBeforeUnixEpoch,
    /// The host clock cannot be represented by the UUIDv7 48-bit timestamp.
    TimestampOutOfRange,
    /// The operating-system entropy provider failed.
    EntropyUnavailable,
}

impl fmt::Display for IdentifierGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ClockBeforeUnixEpoch => "system clock is before the Unix epoch",
            Self::TimestampOutOfRange => "system clock is outside the UUIDv7 range",
            Self::EntropyUnavailable => "operating-system entropy is unavailable",
        })
    }
}

impl Error for IdentifierGenerationError {}

/// Stateless production source for client-owned UUIDv7 values.
///
/// Every method samples the system clock once, performs one ten-byte entropy
/// fill, and retains no ordering or uniqueness state.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemIdSource;

impl SystemIdSource {
    /// Constructs the stateless production source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Generates one fresh outer transport request identifier.
    pub fn request_id(self) -> Result<RequestId, IdentifierGenerationError> {
        generate_with(
            SystemTime::now,
            fill_system_random,
            RequestId::from_unix_milliseconds_and_random,
        )
    }

    /// Generates one caller-selected capability identifier.
    pub fn capability_id(self) -> Result<CapabilityId, IdentifierGenerationError> {
        generate_with(
            SystemTime::now,
            fill_system_random,
            CapabilityId::from_unix_milliseconds_and_random,
        )
    }

    /// Generates one optional caller agent-session identifier.
    pub fn agent_session_id(self) -> Result<AgentSessionId, IdentifierGenerationError> {
        generate_with(
            SystemTime::now,
            fill_system_random,
            AgentSessionId::from_unix_milliseconds_and_random,
        )
    }

    /// Generates one caller-stable offline-maintenance operation identifier.
    pub fn offline_maintenance_operation_id(
        self,
    ) -> Result<OfflineMaintenanceOperationId, IdentifierGenerationError> {
        generate_with(
            SystemTime::now,
            fill_system_random,
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random,
        )
    }

    /// Generates one caller-stable contract-migration operation identifier.
    pub fn contract_migration_operation_id(
        self,
    ) -> Result<ContractMigrationOperationId, IdentifierGenerationError> {
        generate_with(
            SystemTime::now,
            fill_system_random,
            ContractMigrationOperationId::from_unix_milliseconds_and_random,
        )
    }

    /// Generates one caller-stable symbolic-export operation identifier.
    pub fn application_export_operation_id(
        self,
    ) -> Result<ApplicationExportOperationId, IdentifierGenerationError> {
        generate_with(
            SystemTime::now,
            fill_system_random,
            ApplicationExportOperationId::from_unix_milliseconds_and_random,
        )
    }
}

/// Generates one fresh outer transport request identifier.
pub fn generate_request_id() -> Result<RequestId, IdentifierGenerationError> {
    SystemIdSource::new().request_id()
}

/// Generates one caller-selected capability identifier.
pub fn generate_capability_id() -> Result<CapabilityId, IdentifierGenerationError> {
    SystemIdSource::new().capability_id()
}

/// Generates one optional caller agent-session identifier.
pub fn generate_agent_session_id() -> Result<AgentSessionId, IdentifierGenerationError> {
    SystemIdSource::new().agent_session_id()
}

/// Generates one caller-stable offline-maintenance operation identifier.
pub fn generate_offline_maintenance_operation_id()
-> Result<OfflineMaintenanceOperationId, IdentifierGenerationError> {
    SystemIdSource::new().offline_maintenance_operation_id()
}

/// Generates one caller-stable contract-migration operation identifier.
pub fn generate_contract_migration_operation_id()
-> Result<ContractMigrationOperationId, IdentifierGenerationError> {
    SystemIdSource::new().contract_migration_operation_id()
}

/// Generates one caller-stable symbolic-export operation identifier.
pub fn generate_application_export_operation_id()
-> Result<ApplicationExportOperationId, IdentifierGenerationError> {
    SystemIdSource::new().application_export_operation_id()
}

fn fill_system_random(output: &mut [u8; 10]) -> Result<(), IdentifierGenerationError> {
    getrandom::fill(output).map_err(|_| IdentifierGenerationError::EntropyUnavailable)
}

fn generate_with<T>(
    now: impl FnOnce() -> SystemTime,
    fill: impl FnOnce(&mut [u8; 10]) -> Result<(), IdentifierGenerationError>,
    construct: fn(u64, [u8; 10]) -> Result<T, UuidV7ConstructionError>,
) -> Result<T, IdentifierGenerationError> {
    let elapsed = now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| IdentifierGenerationError::ClockBeforeUnixEpoch)?;
    let milliseconds = elapsed.as_millis();
    if milliseconds > MAX_UUID_V7_UNIX_MILLISECONDS {
        return Err(IdentifierGenerationError::TimestampOutOfRange);
    }
    let milliseconds =
        u64::try_from(milliseconds).map_err(|_| IdentifierGenerationError::TimestampOutOfRange)?;

    let mut random = [0_u8; 10];
    fill(&mut random)?;
    construct(milliseconds, random).map_err(|_| IdentifierGenerationError::TimestampOutOfRange)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[test]
    fn one_candidate_samples_clock_then_fills_exactly_ten_bytes_once() {
        let calls = Mutex::new(Vec::new());
        let clock_calls = AtomicUsize::new(0);
        let entropy_calls = AtomicUsize::new(0);

        let request_id = generate_with(
            || {
                clock_calls.fetch_add(1, Ordering::SeqCst);
                calls.lock().expect("call log").push("clock");
                UNIX_EPOCH + Duration::from_millis(0x0123_4567_89ab)
            },
            |output| {
                entropy_calls.fetch_add(1, Ordering::SeqCst);
                calls.lock().expect("call log").push("entropy");
                assert_eq!(output.len(), 10);
                output.copy_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
                Ok(())
            },
            RequestId::from_unix_milliseconds_and_random,
        )
        .expect("candidate");

        assert_eq!(clock_calls.load(Ordering::SeqCst), 1);
        assert_eq!(entropy_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*calls.lock().expect("call log"), ["clock", "entropy"]);
        assert_eq!(
            request_id.to_string(),
            "01234567-89ab-7001-8203-040506070809"
        );
    }

    #[test]
    fn invalid_clock_never_requests_entropy() {
        let entropy_calls = AtomicUsize::new(0);
        let result = generate_with(
            || {
                UNIX_EPOCH
                    .checked_sub(Duration::from_millis(1))
                    .expect("time")
            },
            |_| {
                entropy_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            CapabilityId::from_unix_milliseconds_and_random,
        );
        assert_eq!(result, Err(IdentifierGenerationError::ClockBeforeUnixEpoch));
        assert_eq!(entropy_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn entropy_failure_returns_no_partial_identifier_and_does_not_retry() {
        let entropy_calls = AtomicUsize::new(0);
        let result = generate_with(
            || UNIX_EPOCH,
            |_| {
                entropy_calls.fetch_add(1, Ordering::SeqCst);
                Err(IdentifierGenerationError::EntropyUnavailable)
            },
            AgentSessionId::from_unix_milliseconds_and_random,
        );
        assert_eq!(result, Err(IdentifierGenerationError::EntropyUnavailable));
        assert_eq!(entropy_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn timestamp_above_uuid_v7_range_never_requests_entropy() {
        let entropy_calls = AtomicUsize::new(0);
        let duration = Duration::from_millis(0x1_0000_0000_0000);
        let result = generate_with(
            || UNIX_EPOCH + duration,
            |_| {
                entropy_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            RequestId::from_unix_milliseconds_and_random,
        );
        assert_eq!(result, Err(IdentifierGenerationError::TimestampOutOfRange));
        assert_eq!(entropy_calls.load(Ordering::SeqCst), 0);
    }
}
