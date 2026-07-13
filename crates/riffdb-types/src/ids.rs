//! Stable identifiers and opaque key material.

use std::error::Error;
use std::fmt;

use crate::limits::{
    MAX_ACTOR_ID_BYTES, MAX_CONTRACT_LINEAGE_BYTES, MAX_ENVIRONMENT_BYTES,
    MAX_IDEMPOTENCY_KEY_BYTES, MAX_KEY_BYTES, MAX_TENANT_ID_BYTES,
};

macro_rules! unsigned_id {
    ($(#[$meta:meta])* $name:ident, $inner:ty) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name($inner);

        impl $name {
            /// Creates an identifier from its numeric representation.
            #[must_use]
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            /// Returns the numeric representation.
            #[must_use]
            pub const fn get(self) -> $inner {
                self.0
            }

            /// Returns the canonical big-endian representation.
            #[must_use]
            pub const fn to_be_bytes(self) -> [u8; size_of::<$inner>()] {
                self.0.to_be_bytes()
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self::new(value)
            }
        }

        impl From<$name> for $inner {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

unsigned_id!(
    /// An application contract version.
    ContractVersion,
    u64
);
unsigned_id!(
    /// A single-node application commit sequence.
    CommitSequence,
    u64
);
unsigned_id!(
    /// The monotonically increasing version of an entity record.
    EntityVersion,
    u64
);

impl CommitSequence {
    /// Returns the next sequence, or `None` at the numeric limit.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

unsigned_id!(
    /// A compiler-assigned stable entity type identifier.
    EntityTypeId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable field identifier.
    FieldId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable command identifier.
    CommandId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable outcome identifier.
    OutcomeId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable projection identifier.
    ProjectionId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable event type identifier.
    EventTypeId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable index identifier.
    IndexId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable enum type identifier.
    EnumTypeId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable enum variant identifier.
    EnumVariantId,
    u32
);
unsigned_id!(
    /// The identifier of a keyed-digest configuration.
    DigestKeyId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable aggregate type identifier.
    AggregateTypeId,
    u32
);
unsigned_id!(
    /// A compiler-assigned stable invariant identifier.
    InvariantId,
    u32
);
unsigned_id!(
    /// The epoch of a derived index representation.
    IndexEpoch,
    u64
);
unsigned_id!(
    /// A sequence assigned to an administrative change.
    AdministrationSequence,
    u64
);

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
    pub const fn from_be_bytes(bytes: [u8; 12]) -> Self {
        let commit_sequence = CommitSequence::new(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]));
        let event_ordinal = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        Self::new(commit_sequence, event_ordinal)
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
    /// The hash of a public or durable schema.
    SchemaHash
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

const TYPED_KEY_ENVELOPE_BYTES: usize = 6;

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
        }
    }
}

impl Error for KeyBytesError {}

macro_rules! bounded_key {
    ($(#[$meta:meta])* $name:ident, $type_id:ident, $type_id_method:ident, $prefix:ident) => {
        $(#[$meta])*
        #[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Vec<u8>);

        impl $name {
            /// Validates and creates a key from canonical bytes.
            pub fn new(bytes: Vec<u8>) -> Result<Self, KeyBytesError> {
                if bytes.len() > MAX_KEY_BYTES {
                    return Err(KeyBytesError::TooLong {
                        actual: bytes.len(),
                        maximum: MAX_KEY_BYTES,
                    });
                }
                if bytes.len() < TYPED_KEY_ENVELOPE_BYTES {
                    return Err(KeyBytesError::TooShort {
                        actual: bytes.len(),
                        minimum: TYPED_KEY_ENVELOPE_BYTES,
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
                Ok(Self(bytes))
            }

            /// Validates and creates a key from canonical bytes.
            pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, KeyBytesError> {
                Self::new(bytes)
            }

            /// Returns the stable type identity encoded in the key envelope.
            #[must_use]
            pub fn $type_id_method(&self) -> $type_id {
                $type_id::new(u32::from_be_bytes([
                    self.0[2], self.0[3], self.0[4], self.0[5],
                ]))
            }

            pub(crate) fn from_validated_bytes(bytes: Vec<u8>) -> Self {
                debug_assert!(bytes.len() >= TYPED_KEY_ENVELOPE_BYTES);
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
    ENTITY_KEY_V1_PREFIX
);
bounded_key!(
    /// An opaque canonical logical conflict key.
    ConflictKey,
    AggregateTypeId,
    aggregate_type_id,
    CONFLICT_KEY_V1_PREFIX
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

    const UUID_V7: [u8; 16] = [
        0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x03,
    ];

    #[test]
    fn commit_sequence_next_is_checked() {
        assert_eq!(
            CommitSequence::new(41).checked_next(),
            Some(CommitSequence::new(42))
        );
        assert_eq!(CommitSequence::new(u64::MAX).checked_next(), None);
    }

    #[test]
    fn event_id_has_stable_big_endian_ordering() {
        let first = EventId::new(CommitSequence::new(0x0102_0304_0506_0708), 0x090a_0b0c);
        let next_event = EventId::new(first.commit_sequence(), 0x090a_0b0d);
        let next_commit = EventId::new(CommitSequence::new(0x0102_0304_0506_0709), 0);

        assert_eq!(first.to_be_bytes(), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(EventId::from_be_bytes(first.to_be_bytes()), first);
        assert!(first < next_event);
        assert!(next_event < next_commit);
        assert_eq!(first.event_ordinal(), 0x090a_0b0c);
    }

    #[test]
    fn uuid_v7_identifiers_validate_variant_and_version() {
        let request = RequestId::from_bytes(UUID_V7).expect("valid UUIDv7");
        assert_eq!(request.as_bytes(), &UUID_V7);
        assert_eq!(request.to_string(), "018f0000-0000-7001-8002-000000000003");

        let mut invalid_version = UUID_V7;
        invalid_version[6] = 0x40;
        assert_eq!(
            RequestId::from_bytes(invalid_version),
            Err(UuidV7Error::InvalidVersion)
        );

        let mut invalid_variant = UUID_V7;
        invalid_variant[8] = 0xc0;
        assert_eq!(
            AgentSessionId::from_bytes(invalid_variant),
            Err(UuidV7Error::InvalidVariant)
        );

        assert!(DatabaseId::from_bytes(UUID_V7).is_ok());
        assert!(CapabilityId::from_bytes(UUID_V7).is_ok());
        assert!(ProvenanceId::from_bytes(UUID_V7).is_ok());
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
                &EntityTypeId::new(7).to_be_bytes(),
                b"business-key",
            ]
            .concat(),
        )
        .expect("valid key");
        let digest_key = DigestKey::from_bytes([0x5a; 32]);

        let output = format!("{actor:?} {idempotency:?} {entity_key:?} {digest_key:?}");
        assert!(!output.contains("principal-secret"));
        assert!(!output.contains("caller-secret"));
        assert!(!output.contains("business-key"));
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
            EntityKey::from_bytes(vec![0x43, 0x01, 0, 0, 0, 1]),
            Err(KeyBytesError::WrongPurpose {
                expected: 0x45,
                actual: 0x43,
            })
        );

        let entity =
            EntityKey::from_bytes(vec![0x45, 0x01, 1, 2, 3, 4]).expect("valid entity envelope");
        let conflict =
            ConflictKey::from_bytes(vec![0x43, 0x01, 5, 6, 7, 8]).expect("valid conflict envelope");
        assert_eq!(entity.entity_type_id(), EntityTypeId::new(0x0102_0304));
        assert_eq!(
            conflict.aggregate_type_id(),
            AggregateTypeId::new(0x0506_0708)
        );
    }
}
