//! Storage-hidden authentication and current-capability fixtures.

use std::{error::Error, fmt, num::NonZeroU64, sync::Mutex};

use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationClock, AuthenticationClockError, AuthenticationContext,
    CapabilityAuthenticator, CapabilityDigestKeyProvider, CapabilityReaderCurrentResolver,
    CredentialAuthenticator, CurrentCapabilityResolver, NoopAuthenticationTelemetry,
    OpaqueCredential, RawCapabilityToken,
};
use riffdb_storage_api::{
    CapabilityLifecycleV1, CapabilityLookupResult, CapabilityReader, RevocationReasonCodeV1,
    StorageError, StorageErrorKind, StoredCapabilityRecordV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, Audience, CapabilityGrantV1, CapabilityId,
    DatabaseId, Environment, RequestId, Timestamp,
};

const TEST_TOKEN: &[u8] = b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
const TEST_KEY_DOCUMENT: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";

/// Explicit wall-clock facts used to build one authorization fixture.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct AuthorizationFixtureTimes {
    issued_at: Timestamp,
    expires_at: Timestamp,
    authentication_time: Timestamp,
}

impl AuthorizationFixtureTimes {
    /// Constructs exact issue, exclusive-expiry, and initial-authentication times.
    #[must_use]
    pub const fn new(
        issued_at: Timestamp,
        expires_at: Timestamp,
        authentication_time: Timestamp,
    ) -> Self {
        Self {
            issued_at,
            expires_at,
            authentication_time,
        }
    }

    /// Returns the configured issue time.
    #[must_use]
    pub const fn issued_at(self) -> Timestamp {
        self.issued_at
    }

    /// Returns the configured exclusive expiry time.
    #[must_use]
    pub const fn expires_at(self) -> Timestamp {
        self.expires_at
    }

    /// Returns the clock sample used only for initial authentication.
    #[must_use]
    pub const fn authentication_time(self) -> Timestamp {
        self.authentication_time
    }
}

impl fmt::Debug for AuthorizationFixtureTimes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizationFixtureTimes([REDACTED])")
    }
}

/// High-level identity, boundary, validity, and grant inputs for a fixture.
pub struct AuthorizationFixtureConfig {
    database_id: DatabaseId,
    environment: Environment,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audience: Audience,
    times: AuthorizationFixtureTimes,
    grant: CapabilityGrantV1,
}

impl AuthorizationFixtureConfig {
    /// Constructs one complete test authorization configuration.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        database_id: DatabaseId,
        environment: Environment,
        principal_id: ActorId,
        actor_kind: ActorKind,
        audience: Audience,
        times: AuthorizationFixtureTimes,
        grant: CapabilityGrantV1,
    ) -> Self {
        Self {
            database_id,
            environment,
            principal_id,
            actor_kind,
            audience,
            times,
            grant,
        }
    }
}

impl fmt::Debug for AuthorizationFixtureConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizationFixtureConfig([REDACTED])")
    }
}

/// A closed, redaction-safe fixture construction or mutation failure.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum AuthorizationFixtureError {
    /// Supplied identity, validity, or grant facts could not form a checked record.
    InvalidConfiguration,
    /// The real authentication boundary rejected the configured fixture.
    AuthenticationFailed,
    /// The fixture's synchronized current state could not be accessed.
    StateUnavailable,
}

impl fmt::Debug for AuthorizationFixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizationFixtureError([REDACTED])")
    }
}

impl fmt::Display for AuthorizationFixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "authorization fixture configuration is invalid",
            Self::AuthenticationFailed => "authorization fixture authentication failed",
            Self::StateUnavailable => "authorization fixture state is unavailable",
        })
    }
}

impl Error for AuthorizationFixtureError {}

/// One authenticated principal paired with a mutable current-capability reader.
///
/// The fixture authenticates through [`CapabilityAuthenticator`] during
/// construction. Its resolver is the production
/// [`CapabilityReaderCurrentResolver`], while all storage record types remain
/// private to this module.
pub struct AuthorizationFixture {
    principal: AuthenticatedPrincipal,
    reader: FixtureCapabilityReader,
}

impl AuthorizationFixture {
    /// Builds checked current state and authenticates the deterministic test token.
    pub fn new(config: AuthorizationFixtureConfig) -> Result<Self, AuthorizationFixtureError> {
        let keys = CapabilityDigestKeyProvider::parse_document(TEST_KEY_DOCUMENT)
            .map_err(|_| AuthorizationFixtureError::InvalidConfiguration)?;
        let token = RawCapabilityToken::parse_canonical(TEST_TOKEN)
            .map_err(|_| AuthorizationFixtureError::InvalidConfiguration)?;
        let token_digest = keys.current_digest(&token);
        let capability_id = fixture_capability_id();
        let record = StoredCapabilityRecordV1::from_stored_parts(
            capability_id,
            NonZeroU64::MIN,
            token_digest,
            config.database_id,
            config.environment.clone(),
            config.principal_id,
            config.actor_kind,
            vec![config.audience.clone()],
            config.times.issued_at,
            config.times.expires_at,
            AdministrationSequence::first(),
            fixture_request_id(),
            config.grant,
            CapabilityLifecycleV1::Active,
        )
        .map_err(|_| AuthorizationFixtureError::InvalidConfiguration)?;
        let reader = FixtureCapabilityReader::new(record);
        let clock = FixedAuthenticationClock(config.times.authentication_time);
        let context =
            AuthenticationContext::new(config.database_id, config.environment, config.audience);
        let principal =
            CapabilityAuthenticator::new(&reader, &keys, &clock, &NoopAuthenticationTelemetry)
                .authenticate(OpaqueCredential::new(TEST_TOKEN), &context)
                .map_err(|_| AuthorizationFixtureError::AuthenticationFailed)?;

        Ok(Self { principal, reader })
    }

    /// Borrows the principal produced by the real credential authenticator.
    #[must_use]
    pub const fn authenticated_principal(&self) -> &AuthenticatedPrincipal {
        &self.principal
    }

    /// Returns a real non-caching current-capability resolver over hidden storage.
    #[must_use]
    pub fn current_capability_resolver(&self) -> impl CurrentCapabilityResolver + '_ {
        CapabilityReaderCurrentResolver::new(&self.reader)
    }

    /// Applies the only valid active-to-revoked transition to current test state.
    pub fn revoke_current(&self, revoked_at: Timestamp) -> Result<(), AuthorizationFixtureError> {
        self.reader.update_record(|record| {
            record
                .revoked(
                    record.revision(),
                    revoked_at,
                    AdministrationSequence::try_from(2_u64)
                        .expect("fixture revocation sequence is nonzero"),
                    RevocationReasonCodeV1::Requested,
                )
                .map_err(|_| AuthorizationFixtureError::InvalidConfiguration)
        })
    }

    /// Makes current resolution return not-found without discarding checked state.
    pub fn make_current_missing(&self) -> Result<(), AuthorizationFixtureError> {
        self.reader.set_mode(CurrentResolutionMode::Missing)
    }

    /// Makes current resolution return a generic repository failure.
    pub fn make_current_unavailable(&self) -> Result<(), AuthorizationFixtureError> {
        self.reader.set_mode(CurrentResolutionMode::Unavailable)
    }

    /// Restores current resolution after a missing or unavailable simulation.
    pub fn make_current_available(&self) -> Result<(), AuthorizationFixtureError> {
        self.reader.set_mode(CurrentResolutionMode::Available)
    }
}

impl fmt::Debug for AuthorizationFixture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizationFixture([REDACTED])")
    }
}

struct FixedAuthenticationClock(Timestamp);

impl AuthenticationClock for FixedAuthenticationClock {
    fn now(&self) -> Result<Timestamp, AuthenticationClockError> {
        Ok(self.0)
    }
}

#[derive(Clone, Copy)]
enum CurrentResolutionMode {
    Available,
    Missing,
    Unavailable,
}

struct FixtureReaderState {
    record: StoredCapabilityRecordV1,
    mode: CurrentResolutionMode,
}

struct FixtureCapabilityReader {
    state: Mutex<FixtureReaderState>,
}

impl FixtureCapabilityReader {
    fn new(record: StoredCapabilityRecordV1) -> Self {
        Self {
            state: Mutex::new(FixtureReaderState {
                record,
                mode: CurrentResolutionMode::Available,
            }),
        }
    }

    fn update_record(
        &self,
        update: impl FnOnce(
            &StoredCapabilityRecordV1,
        ) -> Result<StoredCapabilityRecordV1, AuthorizationFixtureError>,
    ) -> Result<(), AuthorizationFixtureError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| AuthorizationFixtureError::StateUnavailable)?;
        state.record = update(&state.record)?;
        Ok(())
    }

    fn set_mode(&self, mode: CurrentResolutionMode) -> Result<(), AuthorizationFixtureError> {
        self.state
            .lock()
            .map_err(|_| AuthorizationFixtureError::StateUnavailable)?
            .mode = mode;
        Ok(())
    }

    fn read_state(
        &self,
    ) -> Result<(StoredCapabilityRecordV1, CurrentResolutionMode), StorageError> {
        let state = self.state.lock().map_err(|_| fixture_storage_error())?;
        Ok((state.record.clone(), state.mode))
    }
}

impl CapabilityReader for FixtureCapabilityReader {
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        let (record, mode) = self.read_state()?;
        match mode {
            CurrentResolutionMode::Available if record.capability_id() == capability_id => {
                Ok(Some(record))
            }
            CurrentResolutionMode::Available | CurrentResolutionMode::Missing => Ok(None),
            CurrentResolutionMode::Unavailable => Err(fixture_storage_error()),
        }
    }

    fn resolve_capability_digests(
        &self,
        candidates: &[riffdb_types::CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        let (record, mode) = self.read_state()?;
        match mode {
            CurrentResolutionMode::Available if candidates.contains(&record.token_digest()) => {
                Ok(CapabilityLookupResult::Found(Box::new(record)))
            }
            CurrentResolutionMode::Available | CurrentResolutionMode::Missing => {
                Ok(CapabilityLookupResult::NotFound)
            }
            CurrentResolutionMode::Unavailable => Err(fixture_storage_error()),
        }
    }
}

fn fixture_storage_error() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

fn fixture_capability_id() -> CapabilityId {
    CapabilityId::from_bytes(fixture_uuid_bytes(0x21)).expect("fixture capability ID is UUIDv7")
}

fn fixture_request_id() -> RequestId {
    RequestId::from_bytes(fixture_uuid_bytes(0x31)).expect("fixture request ID is UUIDv7")
}

fn fixture_uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use riffdb_auth::{CurrentCapabilityActivity, CurrentCapabilityResolutionError};
    use riffdb_types::{
        CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
        PartitionScopeV1, TenantScope,
    };

    use super::*;

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 7).expect("valid timestamp")
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_bytes(fixture_uuid_bytes(0x41)).expect("fixture database ID is UUIDv7")
    }

    fn grant(max_scan_rows: u16) -> CapabilityGrantV1 {
        CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                    .expect("unparameterized permission"),
            ])
            .expect("permission set"),
            Vec::new(),
            NonZeroU16::new(max_scan_rows).expect("nonzero row limit"),
            Vec::new(),
        )
        .expect("valid grant")
    }

    fn config_with(max_scan_rows: u16, expires_at: Timestamp) -> AuthorizationFixtureConfig {
        AuthorizationFixtureConfig::new(
            database_id(),
            Environment::new("test").expect("environment"),
            ActorId::new("authorization-fixture-principal").expect("actor ID"),
            ActorKind::Agent,
            Audience::new("riffdb-test").expect("audience"),
            AuthorizationFixtureTimes::new(timestamp(100), expires_at, timestamp(150)),
            grant(max_scan_rows),
        )
    }

    fn config() -> AuthorizationFixtureConfig {
        config_with(37, timestamp(200))
    }

    #[test]
    fn authenticates_through_real_boundary_and_resolves_current_state() {
        let fixture = AuthorizationFixture::new(config()).expect("fixture");
        let principal = fixture.authenticated_principal();
        assert_eq!(principal.capability_id(), fixture_capability_id());
        assert_eq!(principal.actor_kind(), ActorKind::Agent);
        assert_eq!(principal.authenticated_at(), timestamp(150));

        let current = fixture
            .current_capability_resolver()
            .resolve_current(principal)
            .expect("current capability");
        assert_eq!(current.capability_id(), principal.capability_id());
        assert_eq!(current.revision(), NonZeroU64::MIN);
        assert_eq!(current.activity(), CurrentCapabilityActivity::Active);
        assert_eq!(current.expires_at(), timestamp(200));
        assert_eq!(current.grant().max_scan_rows().get(), 37);
    }

    #[test]
    fn immutable_grant_and_expiry_are_selected_by_separate_fixture_configuration() {
        let fixture = AuthorizationFixture::new(config_with(11, timestamp(175))).expect("fixture");
        let current = fixture
            .current_capability_resolver()
            .resolve_current(fixture.authenticated_principal())
            .expect("configured current capability");
        assert_eq!(current.grant().max_scan_rows().get(), 11);
        assert_eq!(current.expires_at(), timestamp(175));
    }

    #[test]
    fn revocation_is_the_only_live_record_mutation() {
        let fixture = AuthorizationFixture::new(config()).expect("fixture");
        fixture.revoke_current(timestamp(160)).expect("revoke");
        let revoked = fixture
            .current_capability_resolver()
            .resolve_current(fixture.authenticated_principal())
            .expect("revoked current capability still resolves");
        assert_eq!(revoked.revision().get(), 2);
        assert_eq!(revoked.activity(), CurrentCapabilityActivity::Revoked);
    }

    #[test]
    fn missing_and_unavailable_current_state_fail_closed_and_can_be_restored() {
        let fixture = AuthorizationFixture::new(config()).expect("fixture");

        fixture.make_current_missing().expect("set missing");
        assert_eq!(
            fixture
                .current_capability_resolver()
                .resolve_current(fixture.authenticated_principal()),
            Err(CurrentCapabilityResolutionError)
        );

        fixture.make_current_unavailable().expect("set unavailable");
        assert_eq!(
            fixture
                .current_capability_resolver()
                .resolve_current(fixture.authenticated_principal()),
            Err(CurrentCapabilityResolutionError)
        );

        fixture.make_current_available().expect("restore available");
        assert!(
            fixture
                .current_capability_resolver()
                .resolve_current(fixture.authenticated_principal())
                .is_ok()
        );
    }

    #[test]
    fn fixture_surfaces_are_redacted_and_invalid_time_fails_safely() {
        let config = config();
        let config_debug = format!("{config:?}");
        assert_eq!(config_debug, "AuthorizationFixtureConfig([REDACTED])");

        let fixture = AuthorizationFixture::new(config).expect("fixture");
        assert_eq!(format!("{fixture:?}"), "AuthorizationFixture([REDACTED])");
        assert_eq!(
            format!("{:?}", AuthorizationFixtureError::AuthenticationFailed),
            "AuthorizationFixtureError([REDACTED])"
        );

        let invalid = AuthorizationFixtureConfig::new(
            database_id(),
            Environment::new("test-secret-environment").expect("environment"),
            ActorId::new("secret-principal").expect("actor ID"),
            ActorKind::Human,
            Audience::new("secret-audience").expect("audience"),
            AuthorizationFixtureTimes::new(timestamp(200), timestamp(100), timestamp(150)),
            grant(10),
        );
        assert_eq!(
            AuthorizationFixture::new(invalid).unwrap_err(),
            AuthorizationFixtureError::InvalidConfiguration
        );
    }
}
