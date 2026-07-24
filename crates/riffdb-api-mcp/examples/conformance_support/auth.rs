use std::{
    num::{NonZeroU16, NonZeroU32},
    sync::atomic::{AtomicUsize, Ordering},
};

use riffdb_auth::{
    AuthenticationClock, AuthenticationClockError, AuthenticationContext, AuthenticationFailure,
    CapabilityAuthenticator, CapabilityDigestKeyProvider, CredentialAuthenticator,
    NoopAuthenticationTelemetry, OpaqueCredential, RawCapabilityToken,
};
use riffdb_storage_api::{
    CapabilityGrantV1, CapabilityLookupResult, CapabilityPermissionV1, CapabilityPermissionsV1,
    CapabilityReader, CapabilityRequestedRecordV1, PartitionScopeV1, StorageError,
    StoredCapabilityRecordV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, Audience, CapabilityId, CapabilityTokenDigest,
    DatabaseId, Environment, RequestId, TenantScope, Timestamp,
};

use super::backend::CAPABILITY_TOKEN;

const DIGEST_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

pub(crate) struct ConformanceAuthenticator {
    context: AuthenticationContext,
    keys: CapabilityDigestKeyProvider,
    record: StoredCapabilityRecordV1,
    calls: AtomicUsize,
}

impl ConformanceAuthenticator {
    pub(crate) fn new(protected_resource: String) -> Self {
        let context = AuthenticationContext::new(
            database_id(),
            Environment::new("conformance").expect("static environment"),
            Audience::new(protected_resource).expect("loopback MCP audience"),
        );
        let keys = CapabilityDigestKeyProvider::parse_document(
            format!("riffdb-capability-digest-keys-v1\n1:{DIGEST_KEY}\n").as_bytes(),
        )
        .expect("static conformance digest key");
        let raw = RawCapabilityToken::parse_canonical(CAPABILITY_TOKEN.as_bytes())
            .expect("static canonical capability token");
        let record = active_record(keys.current_digest(&raw), &context);
        Self {
            context,
            keys,
            record,
            calls: AtomicUsize::new(0),
        }
    }

    pub(crate) fn context(&self) -> AuthenticationContext {
        self.context.clone()
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
}

impl CredentialAuthenticator for ConformanceAuthenticator {
    fn authenticate(
        &self,
        credential: OpaqueCredential<'_>,
        context: &AuthenticationContext,
    ) -> Result<riffdb_auth::AuthenticatedPrincipal, AuthenticationFailure> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        CapabilityAuthenticator::new(self, &self.keys, self, &NoopAuthenticationTelemetry)
            .authenticate(credential, context)
    }
}

impl CapabilityReader for ConformanceAuthenticator {
    fn read_capability(
        &self,
        capability_id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        Ok((capability_id == self.record.capability_id()).then(|| self.record.clone()))
    }

    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        Ok(if candidates.contains(&self.record.token_digest()) {
            CapabilityLookupResult::Found(Box::new(self.record.clone()))
        } else {
            CapabilityLookupResult::NotFound
        })
    }
}

impl AuthenticationClock for ConformanceAuthenticator {
    fn now(&self) -> Result<Timestamp, AuthenticationClockError> {
        Ok(timestamp(150))
    }
}

fn active_record(
    token_digest: CapabilityTokenDigest,
    context: &AuthenticationContext,
) -> StoredCapabilityRecordV1 {
    let permissions = CapabilityPermissionsV1::new(Vec::<CapabilityPermissionV1>::new())
        .expect("empty conformance permission set");
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        permissions,
        Vec::new(),
        NonZeroU16::new(50).expect("nonzero row limit"),
        Vec::new(),
    )
    .expect("conformance grant");
    let requested = CapabilityRequestedRecordV1::new(
        context.database_id(),
        context.environment().clone(),
        ActorId::new("conformance-agent").expect("static actor"),
        ActorKind::Agent,
        NonZeroU32::new(100).expect("nonzero lifetime"),
        vec![context.audience().clone()],
        grant,
    )
    .expect("conformance capability request");
    StoredCapabilityRecordV1::active(
        capability_id(),
        token_digest,
        requested,
        timestamp(100),
        timestamp(200),
        AdministrationSequence::first(),
        request_id(),
    )
    .expect("active conformance capability")
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

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("canonical timestamp")
}
