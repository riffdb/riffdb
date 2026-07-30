//! Durable database initialization and canonical idempotency identity.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;
use std::str;

use riffdb_types::{
    ActorId, CommandId, ContractLineage, DIGEST_SCHEME_V1, DatabaseId, DigestKeyId, Environment,
    MAX_KEY_BYTES, TenantId, TenantScope,
};

use crate::{
    ActiveCatalogPointerV1, AdministrationSequenceAllocator, ApplicationSequenceAllocator,
    CapabilityBootstrapMarkerV1, StorageError, StorageValueError,
};

/// The nonzero semantic storage-format version retained in database metadata.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageFormatVersion(NonZeroU32);

impl StorageFormatVersion {
    /// The initial POC storage format.
    pub const V1: Self = Self(NonZeroU32::MIN);
    /// Compact tagged durable records with a database-bound registry digest.
    pub const V2: Self = Self(NonZeroU32::new(2).expect("two is nonzero"));

    /// Reconstructs only a storage format supported by this semantic API.
    #[must_use]
    pub const fn from_supported(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            _ => None,
        }
    }

    /// Returns the nonzero numeric format version.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// The exact six-category POC retained operational metadata surface.
///
/// Absence of the active pointer and bootstrap marker is canonical during the
/// corresponding initialization phases. No node identity, shutdown marker, or
/// persisted integrity result is represented.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedMetadataV1 {
    storage_format_version: StorageFormatVersion,
    database_id: DatabaseId,
    application_sequence: ApplicationSequenceAllocator,
    administration_sequence: AdministrationSequenceAllocator,
    active_catalog: Option<ActiveCatalogPointerV1>,
    capability_bootstrap: Option<CapabilityBootstrapMarkerV1>,
}

impl RetainedMetadataV1 {
    /// Constructs the complete retained set and checks database-bound links.
    pub fn new(
        storage_format_version: StorageFormatVersion,
        database_id: DatabaseId,
        application_sequence: ApplicationSequenceAllocator,
        administration_sequence: AdministrationSequenceAllocator,
        active_catalog: Option<ActiveCatalogPointerV1>,
        capability_bootstrap: Option<CapabilityBootstrapMarkerV1>,
    ) -> Result<Self, StorageValueError> {
        if capability_bootstrap
            .as_ref()
            .is_some_and(|marker| marker.database_id() != database_id)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            storage_format_version,
            database_id,
            application_sequence,
            administration_sequence,
            active_catalog,
            capability_bootstrap,
        })
    }

    /// Constructs complete initial metadata for one newly installed database.
    #[must_use]
    pub const fn initial(database_id: DatabaseId) -> Self {
        Self {
            storage_format_version: StorageFormatVersion::V2,
            database_id,
            application_sequence: ApplicationSequenceAllocator::initial(),
            administration_sequence: AdministrationSequenceAllocator::initial(),
            active_catalog: None,
            capability_bootstrap: None,
        }
    }

    /// Returns the durable storage-format identity.
    #[must_use]
    pub const fn storage_format_version(&self) -> StorageFormatVersion {
        self.storage_format_version
    }

    /// Returns the permanent database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the next-or-exhausted application allocator state.
    #[must_use]
    pub const fn application_sequence(&self) -> ApplicationSequenceAllocator {
        self.application_sequence
    }

    /// Returns the independent next-or-exhausted administration allocator state.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequenceAllocator {
        self.administration_sequence
    }

    /// Borrows the optional active-contract consistency pointer.
    #[must_use]
    pub const fn active_catalog(&self) -> Option<&ActiveCatalogPointerV1> {
        self.active_catalog.as_ref()
    }

    /// Returns the optional singleton capability-bootstrap cross-link.
    #[must_use]
    pub const fn capability_bootstrap(&self) -> Option<CapabilityBootstrapMarkerV1> {
        self.capability_bootstrap
    }
}

/// The immutable idempotency-key purpose and format prefix.
pub const IDEMPOTENCY_IDENTITY_KEY_V1_PREFIX: [u8; 2] = [0x59, 0x01];
/// The exact largest valid v1 idempotency identity key.
pub const MAX_IDEMPOTENCY_IDENTITY_KEY_V1_BYTES: usize = 908;

/// The source-free durable database-identity probe result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseIdentityProbe {
    /// Durable metadata already owns this identity; later validation may still fail.
    Existing(DatabaseId),
    /// The store is proven truly empty and may be initialized.
    NeedsInitialization,
}

/// The closed result of atomically installing a checked database identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseInitializationResult {
    /// This transition installed the supplied candidate.
    Installed(DatabaseId),
    /// Another initializer durably won after the source-free probe.
    ConcurrentWinner(DatabaseId),
}

impl DatabaseInitializationResult {
    /// Returns the one durable database identity after the transition.
    #[must_use]
    pub const fn database_id(self) -> DatabaseId {
        match self {
            Self::Installed(database_id) | Self::ConcurrentWinner(database_id) => database_id,
        }
    }
}

/// Source-free identity inspection implemented by a backend while dormant.
pub trait DatabaseIdentityProbePort {
    /// Proves either complete existing identity or true initialization need.
    fn probe_database_identity(&self) -> Result<DatabaseIdentityProbe, StorageError>;
}

/// Narrow authoritative database-initialization transition.
pub trait DatabaseInitializationPort {
    /// Re-proves emptiness and atomically installs complete initial metadata.
    fn initialize_database(
        &mut self,
        candidate: DatabaseId,
    ) -> Result<DatabaseInitializationResult, StorageError>;
}

/// A domain-typed v1 digest of one caller idempotency key.
///
/// This value is deliberately non-substitutable for a capability-token digest
/// even though both use the accepted keyed frame and SHA-256 output width.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKeyDigest {
    key_id: DigestKeyId,
    bytes: [u8; 32],
}

impl IdempotencyKeyDigest {
    /// Constructs a v1 idempotency-key digest from checked HMAC output.
    #[must_use]
    pub const fn from_hmac_bytes(key_id: DigestKeyId, bytes: [u8; 32]) -> Self {
        Self { key_id, bytes }
    }

    /// Returns the immutable v1 digest scheme.
    #[must_use]
    pub const fn scheme(self) -> u8 {
        DIGEST_SCHEME_V1
    }

    /// Returns the nonsecret digest-key identity.
    #[must_use]
    pub const fn key_id(self) -> DigestKeyId {
        self.key_id
    }

    /// Explicitly borrows the digest bytes for a reviewed storage lookup.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl fmt::Debug for IdempotencyKeyDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdempotencyKeyDigest")
            .field("scheme", &DIGEST_SCHEME_V1)
            .field("key_id", &self.key_id)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

/// The complete semantic tuple used for durable command idempotency lookup.
#[derive(Clone, Eq, PartialEq)]
pub struct IdempotencyIdentity {
    database_id: DatabaseId,
    environment: Environment,
    tenant_scope: TenantScope,
    principal_id: ActorId,
    contract_lineage: ContractLineage,
    command_id: CommandId,
    caller_key_digest: IdempotencyKeyDigest,
}

impl IdempotencyIdentity {
    /// Constructs a checked identity from already authorization-resolved values.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        database_id: DatabaseId,
        environment: Environment,
        tenant_scope: TenantScope,
        principal_id: ActorId,
        contract_lineage: ContractLineage,
        command_id: CommandId,
        caller_key_digest: IdempotencyKeyDigest,
    ) -> Self {
        Self {
            database_id,
            environment,
            tenant_scope,
            principal_id,
            contract_lineage,
            command_id,
            caller_key_digest,
        }
    }

    /// Returns the durable database scope.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Borrows the configured environment scope.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Borrows the authorization-resolved tenant scope.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.tenant_scope
    }

    /// Borrows the stable principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Borrows the exact contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Returns the lineage-scoped stable command identity.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    /// Returns the nonsecret versioned keyed digest.
    #[must_use]
    pub const fn caller_key_digest(&self) -> IdempotencyKeyDigest {
        self.caller_key_digest
    }

    /// Encodes the exact ADR-0005 v1 storage key.
    pub fn storage_key(&self) -> Result<IdempotencyIdentityKey, IdempotencyKeyError> {
        IdempotencyIdentityKey::from_identity(self)
    }
}

impl fmt::Debug for IdempotencyIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IdempotencyIdentity([REDACTED])")
    }
}

/// A bounded opaque canonical v1 idempotency storage key.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyIdentityKey(Vec<u8>);

impl IdempotencyIdentityKey {
    fn from_identity(identity: &IdempotencyIdentity) -> Result<Self, IdempotencyKeyError> {
        let environment = identity.environment.as_bytes();
        let principal = identity.principal_id.as_str().as_bytes();
        let lineage = identity.contract_lineage.as_bytes();
        let tenant_bytes = match &identity.tenant_scope {
            TenantScope::Global => 0,
            TenantScope::Tenant(tenant_id) => 4 + tenant_id.as_bytes().len(),
        };
        let length = 2usize
            .checked_add(16)
            .and_then(|value| value.checked_add(4 + environment.len()))
            .and_then(|value| value.checked_add(1 + tenant_bytes))
            .and_then(|value| value.checked_add(4 + principal.len()))
            .and_then(|value| value.checked_add(4 + lineage.len()))
            .and_then(|value| value.checked_add(4 + 1 + 4 + 32))
            .ok_or(IdempotencyKeyError::TooLong)?;
        if length > MAX_KEY_BYTES || length > MAX_IDEMPOTENCY_IDENTITY_KEY_V1_BYTES {
            return Err(IdempotencyKeyError::TooLong);
        }

        let mut bytes = Vec::with_capacity(length);
        bytes.extend_from_slice(&IDEMPOTENCY_IDENTITY_KEY_V1_PREFIX);
        bytes.extend_from_slice(identity.database_id.as_bytes());
        push_length_bytes(&mut bytes, environment);
        match &identity.tenant_scope {
            TenantScope::Global => bytes.push(0),
            TenantScope::Tenant(tenant_id) => {
                bytes.push(1);
                push_length_bytes(&mut bytes, tenant_id.as_bytes());
            }
        }
        push_length_bytes(&mut bytes, principal);
        push_length_bytes(&mut bytes, lineage);
        bytes.extend_from_slice(&identity.command_id.to_be_bytes());
        bytes.push(identity.caller_key_digest.scheme());
        bytes.extend_from_slice(&identity.caller_key_digest.key_id().to_be_bytes());
        bytes.extend_from_slice(identity.caller_key_digest.as_bytes());
        debug_assert_eq!(bytes.len(), length);
        Ok(Self(bytes))
    }

    /// Decodes and fully validates one complete v1 key before returning its identity.
    pub fn decode(bytes: &[u8]) -> Result<IdempotencyIdentity, IdempotencyKeyError> {
        if bytes.len() > MAX_KEY_BYTES || bytes.len() > MAX_IDEMPOTENCY_IDENTITY_KEY_V1_BYTES {
            return Err(IdempotencyKeyError::TooLong);
        }
        let mut cursor = Cursor::new(bytes);
        if cursor.take(2)? != IDEMPOTENCY_IDENTITY_KEY_V1_PREFIX {
            return Err(IdempotencyKeyError::UnsupportedEnvelope);
        }
        let database_id = DatabaseId::from_bytes(cursor.array_16()?)
            .map_err(|_| IdempotencyKeyError::InvalidComponent)?;
        let environment =
            Environment::new(cursor.text()?).map_err(|_| IdempotencyKeyError::InvalidComponent)?;
        let tenant_scope = match cursor.byte()? {
            0 => TenantScope::Global,
            1 => TenantScope::Tenant(
                TenantId::new(cursor.text()?).map_err(|_| IdempotencyKeyError::InvalidComponent)?,
            ),
            _ => return Err(IdempotencyKeyError::InvalidComponent),
        };
        let principal_id =
            ActorId::new(cursor.text()?).map_err(|_| IdempotencyKeyError::InvalidComponent)?;
        let contract_lineage = ContractLineage::new(cursor.text()?)
            .map_err(|_| IdempotencyKeyError::InvalidComponent)?;
        let command_id =
            CommandId::new(cursor.u32()?).ok_or(IdempotencyKeyError::InvalidComponent)?;
        if cursor.byte()? != DIGEST_SCHEME_V1 {
            return Err(IdempotencyKeyError::UnsupportedDigestScheme);
        }
        let key_id =
            DigestKeyId::new(cursor.u32()?).ok_or(IdempotencyKeyError::InvalidComponent)?;
        let digest = IdempotencyKeyDigest::from_hmac_bytes(key_id, cursor.array_32()?);
        if !cursor.is_empty() {
            return Err(IdempotencyKeyError::TrailingBytes);
        }
        Ok(IdempotencyIdentity::new(
            database_id,
            environment,
            tenant_scope,
            principal_id,
            contract_lineage,
            command_id,
            digest,
        ))
    }

    /// Borrows the complete canonical bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for IdempotencyIdentityKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdempotencyIdentityKey")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.0.len())
            .finish()
    }
}

/// Safe failure to construct or decode a canonical idempotency storage key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotencyKeyError {
    /// The complete input exceeds the accepted key bound.
    TooLong,
    /// The purpose or version bytes are unsupported.
    UnsupportedEnvelope,
    /// A component is truncated, malformed, zero, or outside its type bound.
    InvalidComponent,
    /// The keyed-digest scheme is unsupported.
    UnsupportedDigestScheme,
    /// Bytes remain after the exact final digest.
    TrailingBytes,
}

impl fmt::Display for IdempotencyKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TooLong => "idempotency identity key exceeds its bound",
            Self::UnsupportedEnvelope => "unsupported idempotency identity key envelope",
            Self::InvalidComponent => "invalid idempotency identity key component",
            Self::UnsupportedDigestScheme => "unsupported idempotency digest scheme",
            Self::TrailingBytes => "idempotency identity key contains trailing bytes",
        })
    }
}

impl Error for IdempotencyKeyError {}

fn push_length_bytes(output: &mut Vec<u8>, value: &[u8]) {
    let length = u32::try_from(value.len()).expect("foundational text bounds fit u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
}

struct Cursor<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], IdempotencyKeyError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(IdempotencyKeyError::InvalidComponent)?;
        let value = self
            .input
            .get(self.position..end)
            .ok_or(IdempotencyKeyError::InvalidComponent)?;
        self.position = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, IdempotencyKeyError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, IdempotencyKeyError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| IdempotencyKeyError::InvalidComponent)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn text(&mut self) -> Result<&'a str, IdempotencyKeyError> {
        let length =
            usize::try_from(self.u32()?).map_err(|_| IdempotencyKeyError::InvalidComponent)?;
        let bytes = self.take(length)?;
        str::from_utf8(bytes).map_err(|_| IdempotencyKeyError::InvalidComponent)
    }

    fn array_16(&mut self) -> Result<[u8; 16], IdempotencyKeyError> {
        self.take(16)?
            .try_into()
            .map_err(|_| IdempotencyKeyError::InvalidComponent)
    }

    fn array_32(&mut self) -> Result<[u8; 32], IdempotencyKeyError> {
        self.take(32)?
            .try_into()
            .map_err(|_| IdempotencyKeyError::InvalidComponent)
    }

    const fn is_empty(&self) -> bool {
        self.position == self.input.len()
    }
}
