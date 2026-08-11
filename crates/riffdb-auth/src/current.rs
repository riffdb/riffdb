//! Digest-free current capability reloads for authorization safe points.

use std::{error::Error, fmt};

use riffdb_storage_api::{CapabilityLifecycleV1, CapabilityReader, StoredCapabilityRecordV1};
use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityGrantV1, CapabilityId, DatabaseId, Environment,
    Timestamp,
};

use crate::{AuthenticatedPrincipal, PrincipalFactBindingError, PrincipalFactBindingV1};

/// Current irreversible lifecycle activity relevant to policy evaluation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CurrentCapabilityActivity {
    /// The capability remains eligible for policy evaluation.
    Active,
    /// The capability was irreversibly revoked.
    Revoked,
}

/// One digest-free checked current capability snapshot.
///
/// Construction is confined to the storage-backed resolver. This value carries
/// no credential, lookup digest, storage key, reader, clock, or policy result.
#[derive(Clone, Eq, PartialEq)]
pub struct CurrentCapability {
    capability_id: CapabilityId,
    revision: std::num::NonZeroU64,
    activity: CurrentCapabilityActivity,
    database_id: DatabaseId,
    environment: Environment,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audiences: Vec<Audience>,
    issued_at: Timestamp,
    expires_at: Timestamp,
    grant: CapabilityGrantV1,
}

impl CurrentCapability {
    fn from_record(record: &StoredCapabilityRecordV1) -> Self {
        let activity = match record.lifecycle() {
            CapabilityLifecycleV1::Active => CurrentCapabilityActivity::Active,
            CapabilityLifecycleV1::Revoked { .. } => CurrentCapabilityActivity::Revoked,
        };
        Self {
            capability_id: record.capability_id(),
            revision: record.revision(),
            activity,
            database_id: record.database_id(),
            environment: record.environment().clone(),
            principal_id: record.principal_id().clone(),
            actor_kind: record.actor_kind(),
            audiences: record.audiences().to_vec(),
            issued_at: record.issued_at(),
            expires_at: record.expires_at(),
            grant: record.grant().clone(),
        }
    }

    /// Returns the stable capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the transaction-current nonzero lifecycle revision.
    #[must_use]
    pub const fn revision(&self) -> std::num::NonZeroU64 {
        self.revision
    }

    /// Returns the current active or irreversibly revoked state.
    #[must_use]
    pub const fn activity(&self) -> CurrentCapabilityActivity {
        self.activity
    }

    /// Returns the permanent database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the exact configured environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Returns the stable principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the trusted actor classification.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the complete canonical configured audience set.
    #[must_use]
    pub fn audiences(&self) -> &[Audience] {
        &self.audiences
    }

    /// Returns the authoritative issue time.
    #[must_use]
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }

    /// Returns the authoritative exclusive expiry time.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the complete checked grant for policy evaluation.
    #[must_use]
    pub const fn grant(&self) -> &CapabilityGrantV1 {
        &self.grant
    }

    /// Reconstructs the only trusted principal-fact binding for a V4 capability.
    ///
    /// The fact set is loaded from the transaction-current durable record; no
    /// request or cached authentication result can supply or override it.
    pub fn row_policy_principal_binding(
        &self,
    ) -> Option<Result<PrincipalFactBindingV1, PrincipalFactBindingError>> {
        self.grant.internal_row_policy().map(|extension| {
            PrincipalFactBindingV1::new(
                self.capability_id,
                self.revision,
                self.database_id,
                self.environment.clone(),
                self.principal_id.clone(),
                self.actor_kind,
                self.audiences.clone(),
                self.grant.tenant_scope().clone(),
                self.issued_at,
                self.expires_at,
                extension.internal_principal_facts().clone(),
            )
        })
    }
}

impl fmt::Debug for CurrentCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CurrentCapability([REDACTED])")
    }
}

/// A closed redacted failure to reload retained current capability state.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CurrentCapabilityResolutionError;

impl fmt::Debug for CurrentCapabilityResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CurrentCapabilityResolutionError([REDACTED])")
    }
}

impl fmt::Display for CurrentCapabilityResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("current capability resolution failed")
    }
}

impl Error for CurrentCapabilityResolutionError {}

/// Synchronous current-capability source used at service authorization points.
pub trait CurrentCapabilityResolver: Send + Sync {
    /// Reloads by the authenticated stable capability ID on every call.
    fn resolve_current(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<CurrentCapability, CurrentCapabilityResolutionError>;
}

/// Mechanical resolver over the least-authority semantic capability reader.
pub struct CapabilityReaderCurrentResolver<'a, R: ?Sized> {
    reader: &'a R,
}

impl<'a, R: ?Sized> CapabilityReaderCurrentResolver<'a, R> {
    /// Wires one non-caching current-capability resolver.
    #[must_use]
    pub const fn new(reader: &'a R) -> Self {
        Self { reader }
    }
}

impl<R> CurrentCapabilityResolver for CapabilityReaderCurrentResolver<'_, R>
where
    R: CapabilityReader + Sync + ?Sized,
{
    fn resolve_current(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<CurrentCapability, CurrentCapabilityResolutionError> {
        let expected_id = principal.capability_id();
        let record = self
            .reader
            .read_capability(expected_id)
            .map_err(|_| CurrentCapabilityResolutionError)?
            .ok_or(CurrentCapabilityResolutionError)?;
        if record.capability_id() != expected_id {
            return Err(CurrentCapabilityResolutionError);
        }
        Ok(CurrentCapability::from_record(&record))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        num::{NonZeroU16, NonZeroU32, NonZeroU64},
        sync::Mutex,
    };

    use riffdb_storage_api::{
        CapabilityLookupResult, CapabilityPermissionV1, CapabilityPermissionsV1,
        CapabilityRequestedRecordV1, PartitionScopeV1, RevocationReasonCodeV1, StorageError,
        StorageErrorKind,
    };
    use riffdb_types::{AdministrationSequence, CapabilityTokenDigest, RequestId, TenantScope};

    use crate::{
        AuthenticationClock, AuthenticationClockError, AuthenticationContext,
        CapabilityAuthenticator, CapabilityDigestKeyProvider, CredentialAuthenticator,
        NoopAuthenticationTelemetry, OpaqueCredential, RawCapabilityToken,
    };

    use super::*;

    const TOKEN: &[u8] = b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    const KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    struct FixedClock;

    impl AuthenticationClock for FixedClock {
        fn now(&self) -> Result<Timestamp, AuthenticationClockError> {
            Ok(timestamp(150))
        }
    }

    struct AuthenticationReader {
        record: StoredCapabilityRecordV1,
    }

    impl CapabilityReader for AuthenticationReader {
        fn read_capability(
            &self,
            _capability_id: CapabilityId,
        ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
            panic!("initial authentication must use the digest lookup")
        }

        fn resolve_capability_digests(
            &self,
            _candidates: &[CapabilityTokenDigest],
        ) -> Result<CapabilityLookupResult, StorageError> {
            Ok(CapabilityLookupResult::Found(Box::new(self.record.clone())))
        }
    }

    struct SequencedReader {
        results: Mutex<VecDeque<Result<Option<StoredCapabilityRecordV1>, StorageError>>>,
        requested_ids: Mutex<Vec<CapabilityId>>,
    }

    impl SequencedReader {
        fn new(results: Vec<Result<Option<StoredCapabilityRecordV1>, StorageError>>) -> Self {
            Self {
                results: Mutex::new(results.into()),
                requested_ids: Mutex::new(Vec::new()),
            }
        }
    }

    impl CapabilityReader for SequencedReader {
        fn read_capability(
            &self,
            capability_id: CapabilityId,
        ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
            self.requested_ids
                .lock()
                .expect("requested-ID mutex")
                .push(capability_id);
            self.results
                .lock()
                .expect("result mutex")
                .pop_front()
                .expect("test supplied one result per call")
        }

        fn resolve_capability_digests(
            &self,
            _candidates: &[CapabilityTokenDigest],
        ) -> Result<CapabilityLookupResult, StorageError> {
            panic!("current resolution must never perform a digest lookup")
        }
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 9).expect("canonical timestamp")
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn capability_id() -> CapabilityId {
        CapabilityId::from_bytes(uuid_bytes(0x11)).expect("UUIDv7 capability ID")
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_bytes(uuid_bytes(0x22)).expect("UUIDv7 database ID")
    }

    fn request_id() -> RequestId {
        RequestId::from_bytes(uuid_bytes(0x33)).expect("UUIDv7 request ID")
    }

    fn environment() -> Environment {
        Environment::new("test").expect("environment")
    }

    fn audience() -> Audience {
        Audience::new("riffdb-test").expect("audience")
    }

    fn keys() -> CapabilityDigestKeyProvider {
        CapabilityDigestKeyProvider::parse_document(
            format!("riffdb-capability-digest-keys-v1\n1:{KEY}\n").as_bytes(),
        )
        .expect("key document")
    }

    fn active_record(keys: &CapabilityDigestKeyProvider) -> StoredCapabilityRecordV1 {
        let raw = RawCapabilityToken::parse_canonical(TOKEN).expect("canonical token");
        let lookup_value = keys.digest_candidates(&raw).current();
        let permissions = CapabilityPermissionsV1::new(Vec::<CapabilityPermissionV1>::new())
            .expect("permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            permissions,
            Vec::new(),
            NonZeroU16::new(37).expect("row limit"),
            Vec::new(),
        )
        .expect("grant");
        let requested = CapabilityRequestedRecordV1::new(
            database_id(),
            environment(),
            ActorId::new("current-principal").expect("actor ID"),
            ActorKind::Service,
            NonZeroU32::new(100).expect("duration"),
            vec![audience()],
            grant,
        )
        .expect("requested record");
        StoredCapabilityRecordV1::active(
            capability_id(),
            lookup_value,
            requested,
            timestamp(100),
            timestamp(200),
            AdministrationSequence::first(),
            request_id(),
        )
        .expect("active record")
    }

    fn principal(
        keys: &CapabilityDigestKeyProvider,
        record: &StoredCapabilityRecordV1,
    ) -> AuthenticatedPrincipal {
        let reader = AuthenticationReader {
            record: record.clone(),
        };
        CapabilityAuthenticator::new(&reader, keys, &FixedClock, &NoopAuthenticationTelemetry)
            .authenticate(
                OpaqueCredential::new(TOKEN),
                &AuthenticationContext::new(database_id(), environment(), audience()),
            )
            .expect("initial authentication")
    }

    #[test]
    fn reloads_every_call_and_exposes_revocation_as_current_policy_data() {
        let keys = keys();
        let active = active_record(&keys);
        let revoked = active
            .revoked(
                NonZeroU64::MIN,
                timestamp(160),
                AdministrationSequence::new(2).expect("sequence two"),
                RevocationReasonCodeV1::Requested,
            )
            .expect("revoked record");
        let principal = principal(&keys, &active);
        let reader = SequencedReader::new(vec![Ok(Some(active)), Ok(Some(revoked))]);
        let resolver = CapabilityReaderCurrentResolver::new(&reader);

        let first = resolver
            .resolve_current(&principal)
            .expect("first current state");
        let second = resolver
            .resolve_current(&principal)
            .expect("second current state");

        assert_eq!(first.activity(), CurrentCapabilityActivity::Active);
        assert_eq!(first.revision(), NonZeroU64::MIN);
        assert_eq!(second.activity(), CurrentCapabilityActivity::Revoked);
        assert_eq!(second.revision(), NonZeroU64::new(2).expect("revision two"));
        assert_eq!(first.database_id(), database_id());
        assert_eq!(first.environment(), &environment());
        assert_eq!(first.principal_id().as_str(), "current-principal");
        assert_eq!(first.actor_kind(), ActorKind::Service);
        assert_eq!(first.audiences(), &[audience()]);
        assert_eq!(first.issued_at(), timestamp(100));
        assert_eq!(first.expires_at(), timestamp(200));
        assert_eq!(first.grant().max_scan_rows().get(), 37);
        assert_eq!(
            *reader.requested_ids.lock().expect("requested-ID mutex"),
            vec![capability_id(), capability_id()]
        );
    }

    #[test]
    fn missing_and_repository_failure_have_one_redacted_internal_shape() {
        let keys = keys();
        let active = active_record(&keys);
        let principal = principal(&keys, &active);
        let reader = SequencedReader::new(vec![
            Ok(None),
            Err(StorageError::new(StorageErrorKind::Unavailable, None)),
        ]);
        let resolver = CapabilityReaderCurrentResolver::new(&reader);

        let missing = resolver.resolve_current(&principal).unwrap_err();
        let unavailable = resolver.resolve_current(&principal).unwrap_err();

        assert_eq!(missing, CurrentCapabilityResolutionError);
        assert_eq!(unavailable, CurrentCapabilityResolutionError);
        assert_eq!(missing.to_string(), "current capability resolution failed");
        assert_eq!(
            format!("{missing:?}"),
            "CurrentCapabilityResolutionError([REDACTED])"
        );
        assert!(missing.source().is_none());
    }

    #[test]
    fn current_value_and_production_api_contain_no_lookup_material() {
        let keys = keys();
        let active = active_record(&keys);
        let principal = principal(&keys, &active);
        let reader = SequencedReader::new(vec![Ok(Some(active))]);
        let current = CapabilityReaderCurrentResolver::new(&reader)
            .resolve_current(&principal)
            .expect("current state");
        let debug = format!("{current:?}");
        let production_source = include_str!("current.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source prefix");

        assert_eq!(debug, "CurrentCapability([REDACTED])");
        assert!(!debug.contains(KEY));
        assert!(!production_source.contains("token_digest"));
        assert!(!production_source.contains("CapabilityTokenDigest"));
        assert!(!production_source.contains("resolve_capability_digests"));
    }
}
