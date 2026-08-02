//! API-neutral capability credential authentication.

use std::{error::Error, fmt};

use riffdb_storage_api::{
    CapabilityLifecycleV1, CapabilityLookupResult, CapabilityReader, StorageErrorKind,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityId, DatabaseId, Environment, TenantScope, Timestamp,
};
use zeroize::Zeroizing;

use crate::{CapabilityDigestKeyProvider, RawCapabilityToken};

const RETAINED_OPAQUE_CREDENTIAL_BYTES: usize = 43;

/// A borrowed opaque credential supplied by a transport adapter.
///
/// The adapter remains responsible for removing its protocol-specific bearer
/// scheme. This value deliberately performs no syntax validation so malformed
/// and missing credentials enter the same generic authentication boundary.
pub struct OpaqueCredential<'a> {
    bytes: &'a [u8],
}

impl<'a> OpaqueCredential<'a> {
    /// Borrows one complete credential after protocol-specific extraction.
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    const fn as_bytes(&self) -> &[u8] {
        self.bytes
    }
}

impl fmt::Debug for OpaqueCredential<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpaqueCredential([REDACTED])")
    }
}

impl fmt::Display for OpaqueCredential<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("opaque credential [REDACTED]")
    }
}

/// Auth-owned bounded retention for an opaque credential presentation.
///
/// Construction only copies an already transport-extracted byte slice. It does
/// not validate credential syntax or establish an authentication result.
pub struct RetainedOpaqueCredential {
    bytes: Zeroizing<[u8; RETAINED_OPAQUE_CREDENTIAL_BYTES]>,
    len: u8,
}

impl RetainedOpaqueCredential {
    /// Copies an opaque credential when it fits the retained presentation bound.
    pub fn new(bytes: &[u8]) -> Result<Self, RetainedOpaqueCredentialError> {
        if bytes.len() > RETAINED_OPAQUE_CREDENTIAL_BYTES {
            return Err(RetainedOpaqueCredentialError);
        }

        let mut retained = Zeroizing::new([0; RETAINED_OPAQUE_CREDENTIAL_BYTES]);
        retained[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            bytes: retained,
            len: u8::try_from(bytes.len()).map_err(|_| RetainedOpaqueCredentialError)?,
        })
    }

    /// Borrows the exact retained prefix for [`CredentialAuthenticator`].
    #[must_use]
    pub fn borrow(&self) -> OpaqueCredential<'_> {
        OpaqueCredential::new(&self.bytes[..usize::from(self.len)])
    }
}

impl fmt::Debug for RetainedOpaqueCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetainedOpaqueCredential([REDACTED])")
    }
}

impl fmt::Display for RetainedOpaqueCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("retained opaque credential [REDACTED]")
    }
}

/// Safe failure to retain an over-bound opaque credential.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedOpaqueCredentialError;

impl fmt::Display for RetainedOpaqueCredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("credential exceeds the retained presentation limit")
    }
}

impl Error for RetainedOpaqueCredentialError {}

/// Trusted configured identity against which one credential is resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticationContext {
    database_id: DatabaseId,
    environment: Environment,
    audience: Audience,
}

impl AuthenticationContext {
    /// Constructs the exact server-configured authentication boundary.
    #[must_use]
    pub const fn new(
        database_id: DatabaseId,
        environment: Environment,
        audience: Audience,
    ) -> Self {
        Self {
            database_id,
            environment,
            audience,
        }
    }

    /// Returns the permanent database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the configured environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the exact trusted transport audience.
    #[must_use]
    pub const fn audience(&self) -> &Audience {
        &self.audience
    }
}

/// A privately constructed principal produced by successful authentication.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthenticatedPrincipal {
    capability_id: CapabilityId,
    capability_revision: std::num::NonZeroU64,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audience: Audience,
    tenant_scope: TenantScope,
    authenticated_at: Timestamp,
}

impl AuthenticatedPrincipal {
    /// Returns the stable capability identity used for current-policy reloads.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the capability revision observed during authentication.
    #[must_use]
    pub const fn capability_revision(&self) -> std::num::NonZeroU64 {
        self.capability_revision
    }

    /// Returns the stable authenticated principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the trusted actor classification.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the exact audience used for this authentication.
    #[must_use]
    pub const fn audience(&self) -> &Audience {
        &self.audience
    }

    /// Returns the capability's authorization-resolved tenant scope.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.tenant_scope
    }

    /// Returns initial authentication time evidence.
    ///
    /// Current authorization must sample its separate clock and must never
    /// reuse this value for a later expiry check.
    #[must_use]
    pub const fn authenticated_at(&self) -> Timestamp {
        self.authenticated_at
    }
}

impl fmt::Debug for AuthenticatedPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthenticatedPrincipal([REDACTED])")
    }
}

/// Synchronous initial-authentication clock outside deterministic execution.
pub trait AuthenticationClock: Send + Sync {
    /// Obtains one fresh canonical wall-clock value.
    fn now(&self) -> Result<Timestamp, AuthenticationClockError>;
}

/// A redaction-safe failure to obtain initial authentication time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticationClockError;

impl fmt::Display for AuthenticationClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("authentication clock is unavailable")
    }
}

impl Error for AuthenticationClockError {}

/// The only caller-visible authentication failure classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationFailure {
    /// The credential did not establish an authenticated principal.
    Unauthenticated,
    /// Authentication could not safely determine a result.
    Internal,
}

impl fmt::Display for AuthenticationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unauthenticated => "credential was not accepted",
            Self::Internal => "authentication is unavailable",
        })
    }
}

impl Error for AuthenticationFailure {}

/// Redaction-safe reason retained only by trusted authentication telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationRejection {
    /// The credential is absent or not canonical token syntax.
    MalformedCredential,
    /// No configured digest candidate resolved.
    NoMatch,
    /// The resolved capability is no longer active.
    InactiveCapability,
    /// Database, environment, or audience binding does not match.
    BoundaryMismatch,
    /// Authentication time is outside the capability's half-open interval.
    OutsideValidityInterval,
}

/// Redaction-safe internal defect retained only by trusted telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationDefect {
    /// The synchronous authentication clock failed.
    ClockUnavailable,
    /// The capability repository could not complete the lookup.
    RepositoryUnavailable,
    /// The repository reported corrupt or incompatible capability state.
    RepositoryIntegrity,
    /// More than one readable digest candidate resolved.
    MultipleMatches,
    /// A supposedly reciprocal record does not match exactly one candidate.
    ReciprocalLinkMismatch,
}

/// One bounded authentication telemetry signal without credential data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthenticationTelemetryEvent {
    /// Caller-controlled authentication rejection.
    Rejected(AuthenticationRejection),
    /// Internal authentication defect.
    Defect(AuthenticationDefect),
}

/// Trusted redaction-safe authentication telemetry sink.
pub trait AuthenticationTelemetry: Send + Sync {
    /// Records one closed event without principal, credential, digest, or key data.
    fn record(&self, event: AuthenticationTelemetryEvent);
}

/// A no-op telemetry sink for compositions that do not install one.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAuthenticationTelemetry;

impl AuthenticationTelemetry for NoopAuthenticationTelemetry {
    fn record(&self, _event: AuthenticationTelemetryEvent) {}
}

/// API-neutral credential authentication used by every transport adapter.
pub trait CredentialAuthenticator: Send + Sync {
    /// Resolves one extracted opaque credential under trusted server context.
    fn authenticate(
        &self,
        credential: OpaqueCredential<'_>,
        context: &AuthenticationContext,
    ) -> Result<AuthenticatedPrincipal, AuthenticationFailure>;
}

/// Opaque capability authenticator over injected semantic dependencies.
pub struct CapabilityAuthenticator<'a, R: ?Sized, C: ?Sized, T: ?Sized> {
    reader: &'a R,
    keys: &'a CapabilityDigestKeyProvider,
    clock: &'a C,
    telemetry: &'a T,
}

impl<'a, R: ?Sized, C: ?Sized, T: ?Sized> CapabilityAuthenticator<'a, R, C, T> {
    /// Wires one non-caching authenticator from its disjoint dependencies.
    #[must_use]
    pub const fn new(
        reader: &'a R,
        keys: &'a CapabilityDigestKeyProvider,
        clock: &'a C,
        telemetry: &'a T,
    ) -> Self {
        Self {
            reader,
            keys,
            clock,
            telemetry,
        }
    }
}

impl<R, C, T> CredentialAuthenticator for CapabilityAuthenticator<'_, R, C, T>
where
    R: CapabilityReader + Sync + ?Sized,
    C: AuthenticationClock + ?Sized,
    T: AuthenticationTelemetry + ?Sized,
{
    fn authenticate(
        &self,
        credential: OpaqueCredential<'_>,
        context: &AuthenticationContext,
    ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
        let raw_token = RawCapabilityToken::parse_canonical(credential.as_bytes())
            .map_err(|_| reject(self.telemetry, AuthenticationRejection::MalformedCredential))?;
        let candidates = self.keys.digest_candidates(&raw_token);
        let record = match self
            .reader
            .resolve_capability_digests(candidates.as_slice())
        {
            Ok(CapabilityLookupResult::Found(record)) => record,
            Ok(CapabilityLookupResult::NotFound) => {
                return Err(reject(self.telemetry, AuthenticationRejection::NoMatch));
            }
            Ok(CapabilityLookupResult::MultipleMatches) => {
                return Err(defect(
                    self.telemetry,
                    AuthenticationDefect::MultipleMatches,
                ));
            }
            Err(error) => {
                let category = match error.kind() {
                    StorageErrorKind::CorruptData
                    | StorageErrorKind::IncompatibleFormat
                    | StorageErrorKind::InvariantViolation => {
                        AuthenticationDefect::RepositoryIntegrity
                    }
                    StorageErrorKind::Unavailable
                    | StorageErrorKind::CommitStatusUnknown
                    | StorageErrorKind::LimitExceeded
                    | StorageErrorKind::SequenceExhausted
                    | StorageErrorKind::HistoryPruned => {
                        AuthenticationDefect::RepositoryUnavailable
                    }
                };
                return Err(defect(self.telemetry, category));
            }
        };

        let reciprocal_matches = candidates
            .as_slice()
            .iter()
            .filter(|candidate| **candidate == record.token_digest())
            .count();
        if reciprocal_matches != 1 {
            return Err(defect(
                self.telemetry,
                AuthenticationDefect::ReciprocalLinkMismatch,
            ));
        }

        if !matches!(record.lifecycle(), CapabilityLifecycleV1::Active) {
            return Err(reject(
                self.telemetry,
                AuthenticationRejection::InactiveCapability,
            ));
        }
        if record.database_id() != context.database_id
            || record.environment() != &context.environment
            || !record.audiences().contains(&context.audience)
        {
            return Err(reject(
                self.telemetry,
                AuthenticationRejection::BoundaryMismatch,
            ));
        }

        let authenticated_at = self
            .clock
            .now()
            .map_err(|_| defect(self.telemetry, AuthenticationDefect::ClockUnavailable))?;
        if authenticated_at < record.issued_at() || authenticated_at >= record.expires_at() {
            return Err(reject(
                self.telemetry,
                AuthenticationRejection::OutsideValidityInterval,
            ));
        }

        Ok(AuthenticatedPrincipal {
            capability_id: record.capability_id(),
            capability_revision: record.revision(),
            principal_id: record.principal_id().clone(),
            actor_kind: record.actor_kind(),
            audience: context.audience.clone(),
            tenant_scope: record.grant().tenant_scope().clone(),
            authenticated_at,
        })
    }
}

fn reject<T: AuthenticationTelemetry + ?Sized>(
    telemetry: &T,
    reason: AuthenticationRejection,
) -> AuthenticationFailure {
    telemetry.record(AuthenticationTelemetryEvent::Rejected(reason));
    AuthenticationFailure::Unauthenticated
}

fn defect<T: AuthenticationTelemetry + ?Sized>(
    telemetry: &T,
    reason: AuthenticationDefect,
) -> AuthenticationFailure {
    telemetry.record(AuthenticationTelemetryEvent::Defect(reason));
    AuthenticationFailure::Internal
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU16, NonZeroU32, NonZeroU64},
        sync::Mutex,
    };

    use riffdb_storage_api::{
        CapabilityGrantV1, CapabilityPermissionV1, CapabilityPermissionsV1,
        CapabilityRequestedRecordV1, PartitionScopeV1, RevocationReasonCodeV1, StorageError,
        StoredCapabilityRecordV1,
    };
    use riffdb_types::{AdministrationSequence, CapabilityTokenDigest, DigestKeyId, RequestId};

    use super::*;

    const TOKEN: &[u8] = b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    const KEY_ONE: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const KEY_TWO: &str = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f";

    struct FakeReader {
        result: Result<CapabilityLookupResult, StorageError>,
        candidates: Mutex<Vec<Vec<CapabilityTokenDigest>>>,
    }

    impl FakeReader {
        fn new(result: Result<CapabilityLookupResult, StorageError>) -> Self {
            Self {
                result,
                candidates: Mutex::new(Vec::new()),
            }
        }
    }

    impl CapabilityReader for FakeReader {
        fn read_capability(
            &self,
            _capability_id: CapabilityId,
        ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
            panic!("credential authentication must use only the bounded digest lookup")
        }

        fn resolve_capability_digests(
            &self,
            candidates: &[CapabilityTokenDigest],
        ) -> Result<CapabilityLookupResult, StorageError> {
            self.candidates
                .lock()
                .expect("candidate capture mutex")
                .push(candidates.to_vec());
            self.result.clone()
        }
    }

    struct SequenceClock {
        values: Mutex<Vec<Result<Timestamp, AuthenticationClockError>>>,
    }

    impl SequenceClock {
        fn new(values: Vec<Result<Timestamp, AuthenticationClockError>>) -> Self {
            Self {
                values: Mutex::new(values.into_iter().rev().collect()),
            }
        }

        fn remaining(&self) -> usize {
            self.values.lock().expect("clock mutex").len()
        }
    }

    impl AuthenticationClock for SequenceClock {
        fn now(&self) -> Result<Timestamp, AuthenticationClockError> {
            self.values
                .lock()
                .expect("clock mutex")
                .pop()
                .expect("test provided enough clock values")
        }
    }

    #[derive(Default)]
    struct RecordingTelemetry(Mutex<Vec<AuthenticationTelemetryEvent>>);

    impl RecordingTelemetry {
        fn events(&self) -> Vec<AuthenticationTelemetryEvent> {
            self.0.lock().expect("telemetry mutex").clone()
        }
    }

    impl AuthenticationTelemetry for RecordingTelemetry {
        fn record(&self, event: AuthenticationTelemetryEvent) {
            self.0.lock().expect("telemetry mutex").push(event);
        }
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 7).expect("canonical timestamp")
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn capability_id() -> CapabilityId {
        CapabilityId::from_bytes(uuid_bytes(0x21)).expect("UUIDv7 capability ID")
    }

    fn request_id() -> RequestId {
        RequestId::from_bytes(uuid_bytes(0x31)).expect("UUIDv7 request ID")
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_bytes(uuid_bytes(0x41)).expect("UUIDv7 database ID")
    }

    fn environment() -> Environment {
        Environment::new("test").expect("environment")
    }

    fn audience() -> Audience {
        Audience::new("riffdb-test").expect("audience")
    }

    fn context() -> AuthenticationContext {
        AuthenticationContext::new(database_id(), environment(), audience())
    }

    fn key_provider() -> CapabilityDigestKeyProvider {
        CapabilityDigestKeyProvider::parse_document(
            format!("riffdb-capability-digest-keys-v1\n2:{KEY_TWO}\n1:{KEY_ONE}\n").as_bytes(),
        )
        .expect("valid test key document")
    }

    fn digest_for_key(keys: &CapabilityDigestKeyProvider, index: usize) -> CapabilityTokenDigest {
        let raw = RawCapabilityToken::parse_canonical(TOKEN).expect("canonical token");
        keys.digest_candidates(&raw).as_slice()[index]
    }

    fn active_record(
        token_digest: CapabilityTokenDigest,
        database: DatabaseId,
        environment: Environment,
        audiences: Vec<Audience>,
    ) -> StoredCapabilityRecordV1 {
        let permissions = CapabilityPermissionsV1::new(Vec::<CapabilityPermissionV1>::new())
            .expect("empty permission set");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            permissions,
            Vec::new(),
            NonZeroU16::new(50).expect("nonzero row limit"),
            Vec::new(),
        )
        .expect("grant");
        let requested = CapabilityRequestedRecordV1::new(
            database,
            environment,
            ActorId::new("test-principal").expect("actor ID"),
            ActorKind::Agent,
            NonZeroU32::new(100).expect("duration"),
            audiences,
            grant,
        )
        .expect("requested record");
        StoredCapabilityRecordV1::active(
            capability_id(),
            token_digest,
            requested,
            timestamp(100),
            timestamp(200),
            AdministrationSequence::first(),
            request_id(),
        )
        .expect("active record")
    }

    fn authenticate_with(
        result: Result<CapabilityLookupResult, StorageError>,
        clock_values: Vec<Result<Timestamp, AuthenticationClockError>>,
        credential: &[u8],
        authentication_context: &AuthenticationContext,
    ) -> (
        Result<AuthenticatedPrincipal, AuthenticationFailure>,
        FakeReader,
        SequenceClock,
        RecordingTelemetry,
    ) {
        let keys = key_provider();
        let reader = FakeReader::new(result);
        let clock = SequenceClock::new(clock_values);
        let telemetry = RecordingTelemetry::default();
        let result = CapabilityAuthenticator::new(&reader, &keys, &clock, &telemetry)
            .authenticate(OpaqueCredential::new(credential), authentication_context);
        (result, reader, clock, telemetry)
    }

    #[test]
    fn authenticates_one_reciprocal_active_record_using_every_readable_key() {
        let keys = key_provider();
        let record = active_record(
            digest_for_key(&keys, 1),
            database_id(),
            environment(),
            vec![audience()],
        );
        let reader = FakeReader::new(Ok(CapabilityLookupResult::Found(Box::new(record))));
        let clock = SequenceClock::new(vec![Ok(timestamp(100))]);
        let telemetry = RecordingTelemetry::default();
        let principal = CapabilityAuthenticator::new(&reader, &keys, &clock, &telemetry)
            .authenticate(OpaqueCredential::new(TOKEN), &context())
            .expect("credential authenticates");

        assert_eq!(principal.capability_id(), capability_id());
        assert_eq!(principal.capability_revision(), NonZeroU64::MIN);
        assert_eq!(principal.principal_id().as_str(), "test-principal");
        assert_eq!(principal.actor_kind(), ActorKind::Agent);
        assert_eq!(principal.audience(), &audience());
        assert_eq!(principal.tenant_scope(), &TenantScope::Global);
        assert_eq!(principal.authenticated_at(), timestamp(100));
        assert_eq!(clock.remaining(), 0);
        assert!(telemetry.events().is_empty());

        let calls = reader.candidates.lock().expect("candidate capture mutex");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].len(), 2);
        assert_eq!(calls[0][1], digest_for_key(&keys, 1));
    }

    #[test]
    fn caller_controlled_failures_share_one_public_class() {
        let keys = key_provider();
        let active = active_record(
            digest_for_key(&keys, 0),
            database_id(),
            environment(),
            vec![audience()],
        );
        let revoked = active
            .revoked(
                NonZeroU64::MIN,
                timestamp(150),
                AdministrationSequence::new(2).expect("sequence two"),
                RevocationReasonCodeV1::Requested,
            )
            .expect("revoked record");
        let wrong_database = DatabaseId::from_bytes(uuid_bytes(0x51)).expect("UUIDv7 database ID");
        let wrong_database_context =
            AuthenticationContext::new(wrong_database, environment(), audience());
        let wrong_environment_context = AuthenticationContext::new(
            database_id(),
            Environment::new("other").expect("environment"),
            audience(),
        );
        let wrong_audience_context = AuthenticationContext::new(
            database_id(),
            environment(),
            Audience::new("other-audience").expect("audience"),
        );

        let cases = [
            (
                Ok(CapabilityLookupResult::NotFound),
                vec![],
                TOKEN,
                context(),
                AuthenticationRejection::NoMatch,
            ),
            (
                Ok(CapabilityLookupResult::Found(Box::new(revoked))),
                vec![],
                TOKEN,
                context(),
                AuthenticationRejection::InactiveCapability,
            ),
            (
                Ok(CapabilityLookupResult::Found(Box::new(active.clone()))),
                vec![],
                TOKEN,
                wrong_database_context,
                AuthenticationRejection::BoundaryMismatch,
            ),
            (
                Ok(CapabilityLookupResult::Found(Box::new(active.clone()))),
                vec![],
                TOKEN,
                wrong_environment_context,
                AuthenticationRejection::BoundaryMismatch,
            ),
            (
                Ok(CapabilityLookupResult::Found(Box::new(active.clone()))),
                vec![],
                TOKEN,
                wrong_audience_context,
                AuthenticationRejection::BoundaryMismatch,
            ),
            (
                Ok(CapabilityLookupResult::Found(Box::new(active.clone()))),
                vec![Ok(timestamp(99))],
                TOKEN,
                context(),
                AuthenticationRejection::OutsideValidityInterval,
            ),
            (
                Ok(CapabilityLookupResult::Found(Box::new(active))),
                vec![Ok(timestamp(200))],
                TOKEN,
                context(),
                AuthenticationRejection::OutsideValidityInterval,
            ),
        ];

        for (lookup, times, token, authentication_context, reason) in cases {
            let (result, _reader, _clock, telemetry) =
                authenticate_with(lookup, times, token, &authentication_context);
            assert_eq!(result, Err(AuthenticationFailure::Unauthenticated));
            assert_eq!(
                telemetry.events(),
                vec![AuthenticationTelemetryEvent::Rejected(reason)]
            );
        }

        let (result, reader, clock, telemetry) = authenticate_with(
            Ok(CapabilityLookupResult::NotFound),
            vec![],
            b"not-a-token",
            &context(),
        );
        assert_eq!(result, Err(AuthenticationFailure::Unauthenticated));
        assert!(
            reader
                .candidates
                .lock()
                .expect("candidate capture mutex")
                .is_empty()
        );
        assert_eq!(clock.remaining(), 0);
        assert_eq!(
            telemetry.events(),
            vec![AuthenticationTelemetryEvent::Rejected(
                AuthenticationRejection::MalformedCredential
            )]
        );
    }

    #[test]
    fn infrastructure_and_reciprocity_defects_are_redacted_internal_failures() {
        let keys = key_provider();
        let valid = active_record(
            digest_for_key(&keys, 0),
            database_id(),
            environment(),
            vec![audience()],
        );
        let wrong_digest = CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(7).expect("key ID"),
            [0x5a; 32],
        );
        let mismatched =
            active_record(wrong_digest, database_id(), environment(), vec![audience()]);
        let storage_unavailable = StorageError::new(StorageErrorKind::Unavailable, None);
        let storage_corrupt = StorageError::new(StorageErrorKind::CorruptData, None);
        let cases = [
            (
                Err(storage_unavailable),
                vec![],
                AuthenticationDefect::RepositoryUnavailable,
            ),
            (
                Err(storage_corrupt),
                vec![],
                AuthenticationDefect::RepositoryIntegrity,
            ),
            (
                Ok(CapabilityLookupResult::MultipleMatches),
                vec![],
                AuthenticationDefect::MultipleMatches,
            ),
            (
                Ok(CapabilityLookupResult::Found(Box::new(mismatched))),
                vec![],
                AuthenticationDefect::ReciprocalLinkMismatch,
            ),
            (
                Ok(CapabilityLookupResult::Found(Box::new(valid))),
                vec![Err(AuthenticationClockError)],
                AuthenticationDefect::ClockUnavailable,
            ),
        ];

        for (lookup, times, reason) in cases {
            let (result, _reader, _clock, telemetry) =
                authenticate_with(lookup, times, TOKEN, &context());
            assert_eq!(result, Err(AuthenticationFailure::Internal));
            assert_eq!(
                telemetry.events(),
                vec![AuthenticationTelemetryEvent::Defect(reason)]
            );
            assert_eq!(
                AuthenticationFailure::Internal.to_string(),
                "authentication is unavailable"
            );
        }
    }

    #[test]
    fn every_authentication_reloads_and_resamples_without_a_positive_cache() {
        let keys = key_provider();
        let record = active_record(
            digest_for_key(&keys, 0),
            database_id(),
            environment(),
            vec![audience()],
        );
        let reader = FakeReader::new(Ok(CapabilityLookupResult::Found(Box::new(record))));
        let clock = SequenceClock::new(vec![Ok(timestamp(150)), Ok(timestamp(200))]);
        let telemetry = RecordingTelemetry::default();
        let authenticator = CapabilityAuthenticator::new(&reader, &keys, &clock, &telemetry);

        assert!(
            authenticator
                .authenticate(OpaqueCredential::new(TOKEN), &context())
                .is_ok()
        );
        assert_eq!(
            authenticator.authenticate(OpaqueCredential::new(TOKEN), &context()),
            Err(AuthenticationFailure::Unauthenticated)
        );
        assert_eq!(
            reader
                .candidates
                .lock()
                .expect("candidate capture mutex")
                .len(),
            2
        );
        assert_eq!(clock.remaining(), 0);
    }

    #[test]
    fn formatting_never_exposes_the_credential_or_key_material() {
        let credential = OpaqueCredential::new(TOKEN);
        let retained = RetainedOpaqueCredential::new(TOKEN).expect("bounded retained credential");
        let keys = key_provider();
        let record = active_record(
            digest_for_key(&keys, 0),
            database_id(),
            environment(),
            vec![audience()],
        );
        let reader = FakeReader::new(Ok(CapabilityLookupResult::Found(Box::new(record))));
        let clock = SequenceClock::new(vec![Ok(timestamp(150))]);
        let telemetry = RecordingTelemetry::default();
        let principal = CapabilityAuthenticator::new(&reader, &keys, &clock, &telemetry)
            .authenticate(OpaqueCredential::new(TOKEN), &context())
            .expect("credential authenticates");
        let rendered = format!(
            "{credential:?} {credential} {retained:?} {retained} {principal:?} {:?} {} {keys:?}",
            AuthenticationFailure::Unauthenticated,
            AuthenticationFailure::Unauthenticated,
        );

        assert!(!rendered.contains("AAECAwQF"));
        assert!(!rendered.contains(KEY_ONE));
        assert!(!rendered.contains(KEY_TWO));
        assert!(rendered.contains("[REDACTED]"));
    }

    #[test]
    fn retained_credential_bounds_and_copies_without_validating_syntax() {
        struct BorrowCheckingAuthenticator {
            expected: Vec<u8>,
        }

        impl CredentialAuthenticator for BorrowCheckingAuthenticator {
            fn authenticate(
                &self,
                credential: OpaqueCredential<'_>,
                _context: &AuthenticationContext,
            ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
                assert_eq!(credential.as_bytes(), self.expected);
                Err(AuthenticationFailure::Unauthenticated)
            }
        }

        for len in 0..=RETAINED_OPAQUE_CREDENTIAL_BYTES {
            let mut source = vec![b'!'; len];
            let retained =
                RetainedOpaqueCredential::new(&source).expect("at-bound input is retained");
            source.fill(b'?');

            let authenticator = BorrowCheckingAuthenticator {
                expected: vec![b'!'; len],
            };
            assert_eq!(
                authenticator.authenticate(retained.borrow(), &context()),
                Err(AuthenticationFailure::Unauthenticated)
            );
        }

        let over_bound = vec![b'x'; RETAINED_OPAQUE_CREDENTIAL_BYTES + 1];
        assert_eq!(
            RetainedOpaqueCredential::new(&over_bound).unwrap_err(),
            RetainedOpaqueCredentialError
        );
        assert_eq!(
            RetainedOpaqueCredentialError.to_string(),
            "credential exceeds the retained presentation limit"
        );
        assert!(std::mem::needs_drop::<RetainedOpaqueCredential>());
    }
}
