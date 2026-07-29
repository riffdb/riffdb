//! Stable identifiers and opaque key material.

use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use crate::limits::{
    MAX_ACTOR_ID_BYTES, MAX_CONTRACT_LINEAGE_BYTES, MAX_ENVIRONMENT_BYTES,
    MAX_IDEMPOTENCY_KEY_BYTES, MAX_KEY_BYTES, MAX_TENANT_ID_BYTES,
};

/// A numeric identifier was reconstructed from its reserved zero value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZeroNumericIdError;

impl fmt::Display for ZeroNumericIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("numeric identifier must be nonzero")
    }
}

impl Error for ZeroNumericIdError {}

macro_rules! nonzero_id {
    ($(#[$meta:meta])* $name:ident, $inner:ty, $nonzero:ty) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($nonzero);

        impl $name {
            /// Creates an identifier from its numeric representation.
            #[must_use]
            pub const fn new(value: $inner) -> Option<Self> {
                match <$nonzero>::new(value) {
                    Some(value) => Some(Self(value)),
                    None => None,
                }
            }

            /// Returns the numeric representation.
            #[must_use]
            pub const fn get(self) -> $inner {
                self.0.get()
            }

            /// Returns the canonical big-endian representation.
            #[must_use]
            pub const fn to_be_bytes(self) -> [u8; size_of::<$inner>()] {
                self.get().to_be_bytes()
            }
        }

        impl TryFrom<$inner> for $name {
            type Error = ZeroNumericIdError;

            fn try_from(value: $inner) -> Result<Self, Self::Error> {
                Self::new(value).ok_or(ZeroNumericIdError)
            }
        }

        impl From<$name> for $inner {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.get().fmt(formatter)
            }
        }
    };
}

macro_rules! allocatable_nonzero_id {
    ($(#[$meta:meta])* $name:ident, $inner:ty, $nonzero:ty) => {
        nonzero_id!($(#[$meta])* $name, $inner, $nonzero);

        impl $name {
            /// Returns the first value in this one-based allocation space.
            #[must_use]
            pub const fn first() -> Self {
                Self(<$nonzero>::MIN)
            }

            /// Returns the next value, or `None` when the allocation space is exhausted.
            #[must_use]
            pub const fn checked_next(self) -> Option<Self> {
                match self.get().checked_add(1) {
                    Some(value) => Self::new(value),
                    None => None,
                }
            }
        }
    };
}

nonzero_id!(
    /// An application contract version.
    ContractVersion,
    u64,
    NonZeroU64
);
allocatable_nonzero_id!(
    /// A single-node application commit sequence.
    CommitSequence,
    u64,
    NonZeroU64
);
allocatable_nonzero_id!(
    /// The monotonically increasing version of an entity record.
    EntityVersion,
    u64,
    NonZeroU64
);

allocatable_nonzero_id!(
    /// A compiler-assigned stable entity type identifier.
    EntityTypeId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable field identifier.
    FieldId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable command identifier.
    CommandId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable outcome identifier.
    OutcomeId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable projection identifier.
    ProjectionId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable event type identifier.
    EventTypeId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable index identifier.
    IndexId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable enum type identifier.
    EnumTypeId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable enum variant identifier.
    EnumVariantId,
    u32,
    NonZeroU32
);
nonzero_id!(
    /// The identifier of a keyed-digest configuration.
    DigestKeyId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable aggregate type identifier.
    AggregateTypeId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// A compiler-assigned stable invariant identifier.
    InvariantId,
    u32,
    NonZeroU32
);
allocatable_nonzero_id!(
    /// The epoch of a derived index representation.
    IndexEpoch,
    u64,
    NonZeroU64
);

/// Empty or assigned state of one index-range epoch bucket.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IndexEpochPosition {
    /// No mutation has affected this exact prefix bucket.
    BeforeFirst,
    /// The bucket has this nonzero assigned epoch.
    Value(IndexEpoch),
}

allocatable_nonzero_id!(
    /// A sequence assigned to an administrative change.
    AdministrationSequence,
    u64,
    NonZeroU64
);

/// The contiguous authoritative application-commit frontier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FrontierPosition {
    /// No application commit has been applied.
    BeforeFirst,
    /// Every application commit through this sequence has been applied.
    AppliedThrough(CommitSequence),
}

/// The durable identity of one event within a committed command.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventId {
    commit_sequence: CommitSequence,
    event_ordinal: u32,
}

impl EventId {
    /// Creates an event identifier from its commit sequence and zero-based ordinal.
    #[must_use]
    pub const fn new(commit_sequence: CommitSequence, event_ordinal: u32) -> Self {
        Self {
            commit_sequence,
            event_ordinal,
        }
    }

    /// Decodes the canonical big-endian representation.
    #[must_use]
    pub const fn from_be_bytes(bytes: [u8; 12]) -> Option<Self> {
        let commit_sequence = match CommitSequence::new(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ])) {
            Some(value) => value,
            None => return None,
        };
        let event_ordinal = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        Some(Self::new(commit_sequence, event_ordinal))
    }

    /// Returns the commit containing the event.
    #[must_use]
    pub const fn commit_sequence(self) -> CommitSequence {
        self.commit_sequence
    }

    /// Returns the event's zero-based ordinal within its commit.
    #[must_use]
    pub const fn event_ordinal(self) -> u32 {
        self.event_ordinal
    }

    /// Returns the canonical big-endian representation.
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 12] {
        let sequence = self.commit_sequence.to_be_bytes();
        let ordinal = self.event_ordinal.to_be_bytes();
        [
            sequence[0],
            sequence[1],
            sequence[2],
            sequence[3],
            sequence[4],
            sequence[5],
            sequence[6],
            sequence[7],
            ordinal[0],
            ordinal[1],
            ordinal[2],
            ordinal[3],
        ]
    }
}

macro_rules! hash_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Creates a hash identifier from its 32 digest bytes.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            /// Borrows the digest bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }

            /// Returns the digest bytes.
            #[must_use]
            pub const fn into_bytes(self) -> [u8; 32] {
                self.0
            }
        }
    };
}

hash_id!(
    /// The hash of one canonical value document.
    CanonicalValueHash
);
hash_id!(
    /// The hash of an executable command plan.
    PlanHash
);
hash_id!(
    /// The hash of one validated projection plan.
    ProjectionPlanHash
);
hash_id!(
    /// The hash of one closed RiffQL query access program.
    QueryPlanHash
);
hash_id!(
    /// The identity of one canonical immutable query module.
    QueryModuleHash
);
hash_id!(
    /// The hash of one exact RiffQL source document.
    QuerySourceHash
);
hash_id!(
    /// The hash of one canonical name-addressed RiffQL parameter set.
    QueryParameterHash
);
hash_id!(
    /// The hash of a contract bundle's ordered semantic plan set.
    ContractPlanRootHash
);
hash_id!(
    /// The hash of canonical contract source.
    SourceHash
);
hash_id!(
    /// The hash of an immutable contract bundle.
    ContractBundleHash
);
hash_id!(
    /// The hash of canonical command input.
    CanonicalInputHash
);
hash_id!(
    /// The hash of a canonical durable event.
    EventHash
);
hash_id!(
    /// The hash of a canonical entity key.
    EntityKeyHash
);
hash_id!(
    /// The hash of a canonical conflict key.
    ConflictKeyHash
);
hash_id!(
    /// The hash of a canonical logical partition key.
    PartitionKeyHash
);
hash_id!(
    /// The hash of a public or durable schema.
    SchemaHash
);
hash_id!(
    /// The stable semantic-input hash of one offline maintenance operation.
    OfflineMaintenanceInputHash
);

/// A validation failure for a UUIDv7 identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UuidV7Error {
    /// The UUID does not use the RFC 4122/RFC 9562 variant.
    InvalidVariant,
    /// The UUID does not use version 7.
    InvalidVersion,
}

impl fmt::Display for UuidV7Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidVariant => formatter.write_str("identifier has an invalid UUID variant"),
            Self::InvalidVersion => formatter.write_str("identifier is not UUID version 7"),
        }
    }
}

impl Error for UuidV7Error {}

/// A safe failure while assembling a UUIDv7 value from explicit inputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UuidV7ConstructionError {
    /// The Unix-millisecond timestamp cannot fit UUIDv7's 48-bit timestamp field.
    UnixMillisecondsOutOfRange,
}

impl fmt::Display for UuidV7ConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UUIDv7 Unix-millisecond timestamp is out of range")
    }
}

impl Error for UuidV7ConstructionError {}

const UUID_V7_MAX_UNIX_MILLISECONDS: u64 = 0xffff_ffff_ffff;

const fn assemble_uuid_v7(
    unix_milliseconds: u64,
    random: [u8; 10],
) -> Result<[u8; 16], UuidV7ConstructionError> {
    if unix_milliseconds > UUID_V7_MAX_UNIX_MILLISECONDS {
        return Err(UuidV7ConstructionError::UnixMillisecondsOutOfRange);
    }

    let timestamp = unix_milliseconds.to_be_bytes();
    Ok([
        timestamp[2],
        timestamp[3],
        timestamp[4],
        timestamp[5],
        timestamp[6],
        timestamp[7],
        0x70 | (random[0] & 0x0f),
        random[1],
        0x80 | (random[2] & 0x3f),
        random[3],
        random[4],
        random[5],
        random[6],
        random[7],
        random[8],
        random[9],
    ])
}

fn validate_uuid_v7(bytes: &[u8; 16]) -> Result<(), UuidV7Error> {
    if bytes[8] & 0b1100_0000 != 0b1000_0000 {
        return Err(UuidV7Error::InvalidVariant);
    }
    if bytes[6] >> 4 != 7 {
        return Err(UuidV7Error::InvalidVersion);
    }
    Ok(())
}

fn fmt_uuid(bytes: &[u8; 16], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
        formatter,
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

macro_rules! uuid_v7_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 16]);

        impl $name {
            /// Assembles a UUIDv7 from an explicit 48-bit Unix-millisecond value and random bytes.
            pub const fn from_unix_milliseconds_and_random(
                unix_milliseconds: u64,
                random: [u8; 10],
            ) -> Result<Self, UuidV7ConstructionError> {
                match assemble_uuid_v7(unix_milliseconds, random) {
                    Ok(bytes) => Ok(Self(bytes)),
                    Err(error) => Err(error),
                }
            }

            /// Validates and creates an identifier from network-order UUID bytes.
            pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, UuidV7Error> {
                validate_uuid_v7(&bytes)?;
                Ok(Self(bytes))
            }

            /// Borrows the network-order UUID bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            /// Returns the network-order UUID bytes.
            #[must_use]
            pub const fn into_bytes(self) -> [u8; 16] {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt_uuid(&self.0, formatter)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                fmt_uuid(&self.0, formatter)?;
                formatter.write_str(")")
            }
        }
    };
}

uuid_v7_id!(
    /// The identifier of one transport submission.
    RequestId
);
uuid_v7_id!(
    /// The identifier of an agent session.
    AgentSessionId
);
uuid_v7_id!(
    /// A public correlation identifier for an internal failure.
    IncidentId
);
uuid_v7_id!(
    /// The durable identifier of a database instance.
    DatabaseId
);
uuid_v7_id!(
    /// The identifier of an authorization capability.
    CapabilityId
);
uuid_v7_id!(
    /// The identifier of a durable provenance record.
    ProvenanceId
);
uuid_v7_id!(
    /// The caller-stable identifier of one offline maintenance operation.
    OfflineMaintenanceOperationId
);

/// A safe validation failure for a bounded textual identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextIdError {
    /// The identifier is empty.
    Empty,
    /// The UTF-8 representation exceeds its byte limit.
    TooLong {
        /// The applicable maximum byte length.
        maximum: usize,
        /// The supplied byte length.
        actual: usize,
    },
    /// The identifier contains a byte outside its permitted alphabet.
    InvalidCharacter {
        /// The zero-based byte index of the invalid character.
        index: usize,
    },
}

impl fmt::Display for TextIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("identifier must not be empty"),
            Self::TooLong { maximum, actual } => write!(
                formatter,
                "identifier is {actual} bytes but the maximum is {maximum} bytes"
            ),
            Self::InvalidCharacter { index } => {
                write!(
                    formatter,
                    "identifier has an invalid character at byte {index}"
                )
            }
        }
    }
}

/// A bounded exact contract lineage name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContractLineage(String);

impl ContractLineage {
    /// Validates and creates a contract lineage without normalizing its text.
    pub fn new(value: impl Into<String>) -> Result<Self, TextIdError> {
        let value = value.into();
        validate_text_id(&value, MAX_CONTRACT_LINEAGE_BYTES)?;
        Ok(Self(value))
    }

    /// Borrows the exact contract lineage name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrows the exact UTF-8 identity bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for ContractLineage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A bounded ASCII environment slug.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Environment(String);

impl Environment {
    /// Validates and creates an environment slug without changing its spelling.
    pub fn new(value: impl Into<String>) -> Result<Self, TextIdError> {
        let value = value.into();
        validate_text_id(&value, MAX_ENVIRONMENT_BYTES)?;

        if let Some(index) = value
            .bytes()
            .position(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(TextIdError::InvalidCharacter { index });
        }

        Ok(Self(value))
    }

    /// Borrows the exact environment slug.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrows the exact ASCII identity bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A bounded exact tenant identifier.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TenantId(String);

impl TenantId {
    /// Validates and creates a tenant identifier without normalizing its text.
    pub fn new(value: impl Into<String>) -> Result<Self, TextIdError> {
        let value = value.into();
        validate_text_id(&value, MAX_TENANT_ID_BYTES)?;
        Ok(Self(value))
    }

    /// Borrows the exact tenant identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrows the exact UTF-8 identity bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for TenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TenantId([REDACTED])")
    }
}

/// The tenant partition against which an operation is evaluated.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TenantScope {
    /// The operation is not scoped to one tenant.
    Global,
    /// The operation is scoped to the specified tenant.
    Tenant(TenantId),
}

impl TenantScope {
    /// Returns the immutable idempotency-identity component encoding.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        match self {
            Self::Global => vec![0],
            Self::Tenant(tenant_id) => {
                let tenant_bytes = tenant_id.as_bytes();
                let mut bytes = Vec::with_capacity(1 + 4 + tenant_bytes.len());
                bytes.push(1);
                bytes.extend_from_slice(&(tenant_bytes.len() as u32).to_be_bytes());
                bytes.extend_from_slice(tenant_bytes);
                bytes
            }
        }
    }
}

impl fmt::Debug for TenantScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Global => formatter.write_str("TenantScope::Global"),
            Self::Tenant(_) => formatter.write_str("TenantScope::Tenant([REDACTED])"),
        }
    }
}

impl Error for TextIdError {}

fn validate_text_id(value: &str, maximum: usize) -> Result<(), TextIdError> {
    if value.is_empty() {
        return Err(TextIdError::Empty);
    }
    if value.len() > maximum {
        return Err(TextIdError::TooLong {
            maximum,
            actual: value.len(),
        });
    }
    Ok(())
}

/// A bounded stable principal identifier.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ActorId(String);

impl ActorId {
    /// Validates and creates an actor identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, TextIdError> {
        let value = value.into();
        validate_text_id(&value, MAX_ACTOR_ID_BYTES)?;
        Ok(Self(value))
    }

    /// Borrows the identifier text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ActorId([REDACTED])")
    }
}

/// A bounded caller-provided key for uncertainty recovery.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Validates and creates an idempotency key.
    pub fn new(value: impl Into<String>) -> Result<Self, TextIdError> {
        let value = value.into();
        validate_text_id(&value, MAX_IDEMPOTENCY_KEY_BYTES)?;
        Ok(Self(value))
    }

    /// Explicitly exposes the caller key for keyed digest calculation.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IdempotencyKey([REDACTED])")
    }
}

/// Immutable entity-key namespace and v1 encoding prefix.
pub const ENTITY_KEY_V1_PREFIX: [u8; 2] = [0x45, 0x01];

/// Immutable conflict-key namespace and v1 encoding prefix.
pub const CONFLICT_KEY_V1_PREFIX: [u8; 2] = [0x43, 0x01];

/// Immutable partition-key namespace and v1 encoding prefix.
pub const PARTITION_KEY_V1_PREFIX: [u8; 2] = [0x50, 0x01];

/// Immutable index-entry-key namespace and v1 encoding prefix.
pub const INDEX_ENTRY_KEY_V1_PREFIX: [u8; 2] = [0x49, 0x01];

const TYPED_KEY_ENVELOPE_BYTES: usize = 6;
const INDEX_ENTRY_KEY_MIN_BYTES: usize = TYPED_KEY_ENVELOPE_BYTES + 4 + TYPED_KEY_ENVELOPE_BYTES;

/// A safe validation failure for bounded canonical key bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyBytesError {
    /// The encoded key exceeds the v1 hard limit.
    TooLong {
        /// Supplied encoded length.
        actual: usize,
        /// Maximum encoded length.
        maximum: usize,
    },
    /// The encoded key cannot contain the required prefix and type identity.
    TooShort {
        /// Supplied encoded length.
        actual: usize,
        /// Minimum encoded envelope length.
        minimum: usize,
    },
    /// The key uses another purpose namespace.
    WrongPurpose {
        /// Required purpose byte.
        expected: u8,
        /// Supplied purpose byte.
        actual: u8,
    },
    /// The key uses an unsupported format version.
    UnsupportedVersion {
        /// Supplied version byte.
        version: u8,
    },
    /// The key envelope contains the reserved zero owner identity.
    ZeroTypeIdentity,
}

impl fmt::Display for KeyBytesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong { actual, maximum } => {
                write!(
                    formatter,
                    "key is {actual} bytes but the maximum is {maximum} bytes"
                )
            }
            Self::TooShort { actual, minimum } => {
                write!(
                    formatter,
                    "key is {actual} bytes but the minimum envelope is {minimum} bytes"
                )
            }
            Self::WrongPurpose { expected, actual } => write!(
                formatter,
                "key purpose byte {actual} does not match expected purpose {expected}"
            ),
            Self::UnsupportedVersion { version } => {
                write!(formatter, "unsupported key encoding version {version}")
            }
            Self::ZeroTypeIdentity => formatter.write_str("key type identity must be nonzero"),
        }
    }
}

impl Error for KeyBytesError {}

macro_rules! bounded_key {
    ($(#[$meta:meta])* $name:ident, $type_id:ident, $type_id_method:ident, $prefix:ident, $minimum:expr) => {
        $(#[$meta])*
        #[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Vec<u8>);

        impl $name {
            /// Validates a key's purpose/version envelope and hard size bounds.
            ///
            /// Component-level validation requires the exact compiler-produced schema.
            pub fn new(bytes: Vec<u8>) -> Result<Self, KeyBytesError> {
                if bytes.len() > MAX_KEY_BYTES {
                    return Err(KeyBytesError::TooLong {
                        actual: bytes.len(),
                        maximum: MAX_KEY_BYTES,
                    });
                }
                if bytes.len() < $minimum {
                    return Err(KeyBytesError::TooShort {
                        actual: bytes.len(),
                        minimum: $minimum,
                    });
                }
                if bytes[0] != $prefix[0] {
                    return Err(KeyBytesError::WrongPurpose {
                        expected: $prefix[0],
                        actual: bytes[0],
                    });
                }
                if bytes[1] != $prefix[1] {
                    return Err(KeyBytesError::UnsupportedVersion { version: bytes[1] });
                }
                if bytes[2..TYPED_KEY_ENVELOPE_BYTES] == [0; 4] {
                    return Err(KeyBytesError::ZeroTypeIdentity);
                }
                Ok(Self(bytes))
            }

            /// Validates a key's purpose/version envelope and hard size bounds.
            pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, KeyBytesError> {
                Self::new(bytes)
            }

            /// Returns the stable type identity encoded in the key envelope.
            #[must_use]
            pub fn $type_id_method(&self) -> $type_id {
                let value = u32::from_be_bytes([
                    self.0[2], self.0[3], self.0[4], self.0[5],
                ]);
                $type_id::new(value).expect("validated key contains a nonzero type identity")
            }

            pub(crate) fn from_validated_bytes(bytes: Vec<u8>) -> Self {
                debug_assert!(bytes.len() >= $minimum);
                debug_assert!(bytes.len() <= MAX_KEY_BYTES);
                debug_assert_eq!(bytes[..2], $prefix);
                Self(bytes)
            }

            /// Borrows the canonical key bytes.
            #[must_use]
            pub fn as_bytes(&self) -> &[u8] {
                &self.0
            }

            /// Returns the canonical key bytes.
            #[must_use]
            pub fn into_bytes(self) -> Vec<u8> {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("bytes", &"[REDACTED]")
                    .field("length", &self.0.len())
                    .finish()
            }
        }
    };
}

bounded_key!(
    /// An opaque canonical entity key.
    EntityKey,
    EntityTypeId,
    entity_type_id,
    ENTITY_KEY_V1_PREFIX,
    TYPED_KEY_ENVELOPE_BYTES
);
bounded_key!(
    /// An opaque canonical logical conflict key.
    ConflictKey,
    AggregateTypeId,
    aggregate_type_id,
    CONFLICT_KEY_V1_PREFIX,
    TYPED_KEY_ENVELOPE_BYTES
);
bounded_key!(
    /// An opaque canonical logical partition key.
    PartitionKey,
    AggregateTypeId,
    aggregate_type_id,
    PARTITION_KEY_V1_PREFIX,
    TYPED_KEY_ENVELOPE_BYTES
);
bounded_key!(
    /// A complete canonical local-index entry key.
    IndexEntryKey,
    IndexId,
    index_id,
    INDEX_ENTRY_KEY_V1_PREFIX,
    INDEX_ENTRY_KEY_MIN_BYTES
);

/// A 32-byte key used by a reviewed keyed-digest construction.
pub struct DigestKey([u8; 32]);

impl DigestKey {
    /// Creates a digest key from secret key bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Explicitly exposes the secret bytes to a keyed digest implementation.
    #[must_use]
    pub const fn expose_secret(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for DigestKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DigestKey([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const UUID_V7: [u8; 16] = [
        0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x03,
    ];

    #[test]
    fn commit_sequence_next_is_checked() {
        let forty_one = CommitSequence::new(41).expect("nonzero sequence");
        assert_eq!(forty_one.checked_next(), CommitSequence::new(42));
        assert_eq!(
            CommitSequence::new(u64::MAX)
                .expect("nonzero sequence")
                .checked_next(),
            None
        );
    }

    #[test]
    fn event_id_has_stable_big_endian_ordering() {
        let first = EventId::new(
            CommitSequence::new(0x0102_0304_0506_0708).expect("nonzero sequence"),
            0x090a_0b0c,
        );
        let next_event = EventId::new(first.commit_sequence(), 0x090a_0b0d);
        let next_commit = EventId::new(
            CommitSequence::new(0x0102_0304_0506_0709).expect("nonzero sequence"),
            0,
        );

        assert_eq!(first.to_be_bytes(), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(EventId::from_be_bytes(first.to_be_bytes()), Some(first));
        assert_eq!(EventId::from_be_bytes([0; 12]), None);
        assert!(first < next_event);
        assert!(next_event < next_commit);
        assert_eq!(first.event_ordinal(), 0x090a_0b0c);
    }

    #[test]
    fn assigned_and_compiler_ids_are_one_based_and_exhaustion_is_checked() {
        macro_rules! assert_u64_id {
            ($type:ty) => {{
                assert_eq!(<$type>::new(0), None);
                assert_eq!(<$type>::first().get(), 1);
                assert_eq!(<$type>::first().checked_next().map(<$type>::get), Some(2));
                assert_eq!(
                    <$type>::new(u64::MAX).expect("nonzero").checked_next(),
                    None
                );
                assert_eq!(<$type>::try_from(0), Err(ZeroNumericIdError));
            }};
        }

        macro_rules! assert_u32_id {
            ($type:ty) => {{
                assert_eq!(<$type>::new(0), None);
                assert_eq!(<$type>::first().get(), 1);
                assert_eq!(<$type>::first().checked_next().map(<$type>::get), Some(2));
                assert_eq!(
                    <$type>::new(u32::MAX).expect("nonzero").checked_next(),
                    None
                );
                assert_eq!(<$type>::try_from(0), Err(ZeroNumericIdError));
            }};
        }

        assert_u64_id!(CommitSequence);
        assert_u64_id!(AdministrationSequence);
        assert_u64_id!(EntityVersion);
        assert_u64_id!(IndexEpoch);
        assert_u32_id!(EntityTypeId);
        assert_u32_id!(FieldId);
        assert_u32_id!(CommandId);
        assert_u32_id!(OutcomeId);
        assert_u32_id!(ProjectionId);
        assert_u32_id!(EventTypeId);
        assert_u32_id!(IndexId);
        assert_u32_id!(EnumTypeId);
        assert_u32_id!(EnumVariantId);
        assert_u32_id!(AggregateTypeId);
        assert_u32_id!(InvariantId);

        assert_eq!(ContractVersion::new(0), None);
        assert_eq!(DigestKeyId::new(0), None);
        assert_eq!(ContractVersion::try_from(0), Err(ZeroNumericIdError));
        assert_eq!(DigestKeyId::try_from(0), Err(ZeroNumericIdError));
    }

    #[test]
    fn uuid_v7_identifiers_validate_variant_and_version() {
        let mut invalid_version = UUID_V7;
        invalid_version[6] = 0x40;
        let mut invalid_variant = UUID_V7;
        invalid_variant[8] = 0xc0;

        macro_rules! assert_decoder {
            ($type:ty) => {{
                let value = <$type>::from_bytes(UUID_V7).expect("valid UUIDv7");
                assert_eq!(value.as_bytes(), &UUID_V7);
                assert_eq!(
                    <$type>::from_bytes(invalid_version),
                    Err(UuidV7Error::InvalidVersion)
                );
                assert_eq!(
                    <$type>::from_bytes(invalid_variant),
                    Err(UuidV7Error::InvalidVariant)
                );
            }};
        }

        assert_decoder!(RequestId);
        assert_decoder!(AgentSessionId);
        assert_decoder!(IncidentId);
        assert_decoder!(DatabaseId);
        assert_decoder!(CapabilityId);
        assert_decoder!(ProvenanceId);

        let request = RequestId::from_bytes(UUID_V7).expect("valid UUIDv7");
        assert_eq!(request.to_string(), "018f0000-0000-7001-8002-000000000003");
    }

    #[test]
    fn uuid_v7_pure_construction_matches_accepted_goldens_for_every_id_type() {
        const RANDOM: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        const PRIMARY: [u8; 16] = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0x70, 0x01, 0x82, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09,
        ];
        const ZERO: [u8; 16] = [0, 0, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 0];
        const MAXIMUM: [u8; 16] = [
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xbf, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff,
        ];

        macro_rules! assert_constructor {
            ($type:ty) => {{
                let primary = <$type>::from_unix_milliseconds_and_random(0x0123_4567_89ab, RANDOM)
                    .expect("48-bit timestamp");
                assert_eq!(primary.into_bytes(), PRIMARY);
                assert_eq!(primary.to_string(), "01234567-89ab-7001-8203-040506070809");
                assert_eq!(
                    <$type>::from_unix_milliseconds_and_random(0, [0; 10])
                        .expect("lower bound")
                        .into_bytes(),
                    ZERO
                );
                assert_eq!(
                    <$type>::from_unix_milliseconds_and_random(
                        UUID_V7_MAX_UNIX_MILLISECONDS,
                        [0xff; 10],
                    )
                    .expect("upper bound")
                    .into_bytes(),
                    MAXIMUM
                );
                assert_eq!(
                    <$type>::from_unix_milliseconds_and_random(0x1_0000_0000_0000, [0xa5; 10]),
                    Err(UuidV7ConstructionError::UnixMillisecondsOutOfRange)
                );
            }};
        }

        assert_constructor!(RequestId);
        assert_constructor!(AgentSessionId);
        assert_constructor!(IncidentId);
        assert_constructor!(DatabaseId);
        assert_constructor!(CapabilityId);
        assert_constructor!(ProvenanceId);

        let error = UuidV7ConstructionError::UnixMillisecondsOutOfRange;
        assert_eq!(
            error.to_string(),
            "UUIDv7 Unix-millisecond timestamp is out of range"
        );
        assert!(!format!("{error:?}").contains("a5"));
    }

    proptest! {
        #[test]
        fn uuid_v7_construction_preserves_fields_and_round_trips(
            unix_milliseconds in 0u64..=UUID_V7_MAX_UNIX_MILLISECONDS,
            random in any::<[u8; 10]>(),
        ) {
            let id = RequestId::from_unix_milliseconds_and_random(unix_milliseconds, random)
                .expect("generated timestamp is in range");
            let bytes = id.into_bytes();
            let timestamp = unix_milliseconds.to_be_bytes();
            let canonical_text = id.to_string();

            prop_assert_eq!(&bytes[..6], &timestamp[2..]);
            prop_assert_eq!(bytes[6] >> 4, 7);
            prop_assert_eq!(bytes[6] & 0x0f, random[0] & 0x0f);
            prop_assert_eq!(bytes[7], random[1]);
            prop_assert_eq!(bytes[8] >> 6, 0b10);
            prop_assert_eq!(bytes[8] & 0x3f, random[2] & 0x3f);
            prop_assert_eq!(&bytes[9..], &random[3..]);
            prop_assert_eq!(canonical_text.len(), 36);
            let canonical_text_is_valid =
                canonical_text.bytes().enumerate().all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    byte == b'-'
                } else {
                    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
                }
            });
            prop_assert!(canonical_text_is_valid);

            macro_rules! assert_id {
                ($type:ty) => {{
                    let typed = <$type>::from_unix_milliseconds_and_random(
                        unix_milliseconds,
                        random,
                    )
                    .expect("generated timestamp is in range");
                    prop_assert_eq!(typed.into_bytes(), bytes);
                    prop_assert_eq!(<$type>::from_bytes(bytes), Ok(typed));
                    prop_assert_eq!(typed.to_string(), canonical_text.as_str());
                }};
            }

            assert_id!(RequestId);
            assert_id!(AgentSessionId);
            assert_id!(IncidentId);
            assert_id!(DatabaseId);
            assert_id!(CapabilityId);
            assert_id!(ProvenanceId);
        }

        #[test]
        fn uuid_v7_network_order_tracks_timestamp_for_equal_randomness(
            left in 0u64..=UUID_V7_MAX_UNIX_MILLISECONDS,
            right in 0u64..=UUID_V7_MAX_UNIX_MILLISECONDS,
            random in any::<[u8; 10]>(),
        ) {
            let left_id = RequestId::from_unix_milliseconds_and_random(left, random)
                .expect("generated timestamp is in range");
            let right_id = RequestId::from_unix_milliseconds_and_random(right, random)
                .expect("generated timestamp is in range");
            prop_assert_eq!(left.cmp(&right), left_id.cmp(&right_id));

            macro_rules! assert_order {
                ($type:ty) => {{
                    let left_id = <$type>::from_unix_milliseconds_and_random(left, random)
                        .expect("generated timestamp is in range");
                    let right_id = <$type>::from_unix_milliseconds_and_random(right, random)
                        .expect("generated timestamp is in range");
                    prop_assert_eq!(left.cmp(&right), left_id.cmp(&right_id));
                }};
            }

            assert_order!(AgentSessionId);
            assert_order!(IncidentId);
            assert_order!(DatabaseId);
            assert_order!(CapabilityId);
            assert_order!(ProvenanceId);
        }
    }

    #[test]
    fn uuid_identifier_newtypes_are_distinct() {
        use std::any::TypeId;

        let types = [
            TypeId::of::<RequestId>(),
            TypeId::of::<AgentSessionId>(),
            TypeId::of::<IncidentId>(),
            TypeId::of::<DatabaseId>(),
            TypeId::of::<CapabilityId>(),
            TypeId::of::<ProvenanceId>(),
        ];
        for (index, current) in types.iter().enumerate() {
            assert!(!types[..index].contains(current));
        }
    }

    #[test]
    fn bounded_names_preserve_exact_text_and_validate_environment_alphabet() {
        let lineage = ContractLineage::new("Inventory.v2").expect("valid lineage");
        let environment = Environment::new("Prod_US-2.1").expect("valid environment");
        let tenant = TenantId::new("tenant-\u{00e9}").expect("valid tenant");

        assert_eq!(lineage.as_str(), "Inventory.v2");
        assert_eq!(environment.as_str(), "Prod_US-2.1");
        assert_eq!(tenant.as_str(), "tenant-\u{00e9}");
        assert_eq!(
            Environment::new("prod/us"),
            Err(TextIdError::InvalidCharacter { index: 4 })
        );
        assert_eq!(
            Environment::new("prod \u{00e9}"),
            Err(TextIdError::InvalidCharacter { index: 4 })
        );
        assert_eq!(Environment::new(""), Err(TextIdError::Empty));
    }

    #[test]
    fn tenant_debug_output_is_redacted() {
        let tenant = TenantId::new("customer-secret").expect("valid tenant");
        let scope = TenantScope::Tenant(tenant.clone());

        let output = format!("{tenant:?} {scope:?}");
        assert!(!output.contains("customer-secret"));
        assert_eq!(format!("{:?}", TenantScope::Global), "TenantScope::Global");
    }

    #[test]
    fn tenant_scope_identity_encoding_is_stable() {
        assert_eq!(TenantScope::Global.to_canonical_bytes(), [0]);

        let scope = TenantScope::Tenant(TenantId::new("tenant-é").expect("valid tenant"));
        assert_eq!(
            scope.to_canonical_bytes(),
            [&[1, 0, 0, 0, 9][..], "tenant-é".as_bytes(),].concat()
        );
    }

    #[test]
    fn sensitive_identifier_debug_output_is_redacted() {
        let actor = ActorId::new("principal-secret").expect("valid actor ID");
        let idempotency = IdempotencyKey::new("caller-secret").expect("valid key");
        let entity_key = EntityKey::from_bytes(
            [
                ENTITY_KEY_V1_PREFIX.as_slice(),
                &EntityTypeId::new(7).expect("nonzero").to_be_bytes(),
                b"business-key",
            ]
            .concat(),
        )
        .expect("valid key");
        let partition_key = PartitionKey::from_bytes(
            [
                PARTITION_KEY_V1_PREFIX.as_slice(),
                &AggregateTypeId::new(8).expect("nonzero").to_be_bytes(),
                b"business-partition",
            ]
            .concat(),
        )
        .expect("valid partition envelope");
        let index_key = IndexEntryKey::from_bytes(
            [
                INDEX_ENTRY_KEY_V1_PREFIX.as_slice(),
                &IndexId::new(9).expect("nonzero").to_be_bytes(),
                &[0, 0, 0, 6],
                ENTITY_KEY_V1_PREFIX.as_slice(),
                &EntityTypeId::new(7).expect("nonzero").to_be_bytes(),
            ]
            .concat(),
        )
        .expect("structurally bounded index envelope");
        let digest_key = DigestKey::from_bytes([0x5a; 32]);

        let output = format!(
            "{actor:?} {idempotency:?} {entity_key:?} {partition_key:?} {index_key:?} {digest_key:?}"
        );
        assert!(!output.contains("principal-secret"));
        assert!(!output.contains("caller-secret"));
        assert!(!output.contains("business-key"));
        assert!(!output.contains("business-partition"));
        assert!(!output.contains("5a"));
    }

    #[test]
    fn bounded_identifiers_reject_empty_and_oversized_values() {
        assert_eq!(ActorId::new(""), Err(TextIdError::Empty));
        assert!(matches!(
            IdempotencyKey::new("x".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1)),
            Err(TextIdError::TooLong { .. })
        ));
        assert!(matches!(
            ContractLineage::new("x".repeat(MAX_CONTRACT_LINEAGE_BYTES + 1)),
            Err(TextIdError::TooLong { .. })
        ));
        assert!(matches!(
            Environment::new("x".repeat(MAX_ENVIRONMENT_BYTES + 1)),
            Err(TextIdError::TooLong { .. })
        ));
        assert!(matches!(
            TenantId::new("x".repeat(MAX_TENANT_ID_BYTES + 1)),
            Err(TextIdError::TooLong { .. })
        ));
        assert_eq!(
            ConflictKey::from_bytes(vec![0; MAX_KEY_BYTES + 1]),
            Err(KeyBytesError::TooLong {
                actual: MAX_KEY_BYTES + 1,
                maximum: MAX_KEY_BYTES,
            })
        );
    }

    #[test]
    fn typed_key_envelopes_reject_malformed_or_cross_purpose_bytes() {
        assert_eq!(
            EntityKey::from_bytes(Vec::new()),
            Err(KeyBytesError::TooShort {
                actual: 0,
                minimum: TYPED_KEY_ENVELOPE_BYTES,
            })
        );
        assert_eq!(
            EntityKey::from_bytes(vec![0x45, 0x02, 0, 0, 0, 1]),
            Err(KeyBytesError::UnsupportedVersion { version: 2 })
        );
        assert_eq!(
            EntityKey::from_bytes(vec![0x45, 0x01, 0, 0, 0, 0]),
            Err(KeyBytesError::ZeroTypeIdentity)
        );
        assert_eq!(
            EntityKey::from_bytes(vec![0x43, 0x01, 0, 0, 0, 1]),
            Err(KeyBytesError::WrongPurpose {
                expected: 0x45,
                actual: 0x43,
            })
        );
        assert_eq!(
            IndexEntryKey::from_bytes(vec![0x49, 0x01, 0, 0, 0, 1]),
            Err(KeyBytesError::TooShort {
                actual: TYPED_KEY_ENVELOPE_BYTES,
                minimum: INDEX_ENTRY_KEY_MIN_BYTES,
            })
        );

        let entity =
            EntityKey::from_bytes(vec![0x45, 0x01, 1, 2, 3, 4]).expect("valid entity envelope");
        let conflict =
            ConflictKey::from_bytes(vec![0x43, 0x01, 5, 6, 7, 8]).expect("valid conflict envelope");
        let partition = PartitionKey::from_bytes(vec![0x50, 0x01, 9, 10, 11, 12])
            .expect("valid partition envelope");
        let index = IndexEntryKey::from_bytes(vec![
            0x49, 0x01, 13, 14, 15, 16, 0, 0, 0, 6, 0x45, 0x01, 0, 0, 0, 1,
        ])
        .expect("structurally bounded index envelope");
        assert_eq!(
            entity.entity_type_id(),
            EntityTypeId::new(0x0102_0304).expect("nonzero")
        );
        assert_eq!(
            conflict.aggregate_type_id(),
            AggregateTypeId::new(0x0506_0708).expect("nonzero")
        );
        assert_eq!(
            partition.aggregate_type_id(),
            AggregateTypeId::new(0x090a_0b0c).expect("nonzero")
        );
        assert_eq!(
            index.index_id(),
            IndexId::new(0x0d0e_0f10).expect("nonzero")
        );
    }
}
