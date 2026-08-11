//! Foundational projection identities, generations, keys, and hash values.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::num::NonZeroU64;

use crate::limits::{MAX_CONTRACT_LINEAGE_BYTES, MAX_KEY_BYTES, MAX_PROJECTION_GROUP_COMPONENTS};
use crate::{
    CanonicalValue, CommitSequence, ContractLineage, ProjectionId, ProjectionPlanHash,
    decode_canonical_value, encode_canonical_value,
};

/// Immutable v1 projection apply-marker key prefix.
pub const PROJECTION_APPLY_KEY_V1_PREFIX: [u8; 2] = [0x41, 0x01];

/// Immutable v1 projection frontier/control key prefix.
pub const PROJECTION_FRONTIER_KEY_V1_PREFIX: [u8; 2] = [0x46, 0x01];

/// Immutable v1 projection group-state key prefix.
pub const PROJECTION_GROUP_KEY_V1_PREFIX: [u8; 2] = [0x47, 0x01];

/// The exact lineage, projection declaration, and semantic plan identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProjectionIdentity {
    contract_lineage: ContractLineage,
    projection_id: ProjectionId,
    plan_hash: ProjectionPlanHash,
}

impl ProjectionIdentity {
    /// Creates an identity from already-validated foundational components.
    #[must_use]
    pub const fn new(
        contract_lineage: ContractLineage,
        projection_id: ProjectionId,
        plan_hash: ProjectionPlanHash,
    ) -> Self {
        Self {
            contract_lineage,
            projection_id,
            plan_hash,
        }
    }

    /// Decodes one complete canonical v1 projection-identity payload.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ProjectionKeyError> {
        let (identity, consumed) = decode_identity_prefix(bytes)?;
        if consumed != bytes.len() {
            return Err(ProjectionKeyError::TrailingBytes);
        }
        Ok(identity)
    }

    /// Returns the exact contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Returns the stable nonzero projection ID.
    #[must_use]
    pub const fn projection_id(&self) -> ProjectionId {
        self.projection_id
    }

    /// Returns the exact projection-plan hash.
    #[must_use]
    pub const fn plan_hash(&self) -> ProjectionPlanHash {
        self.plan_hash
    }

    /// Encodes the canonical v1 projection-identity payload.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let lineage = self.contract_lineage.as_bytes();
        let mut bytes = Vec::with_capacity(4 + lineage.len() + 4 + 32);
        bytes.extend_from_slice(&(lineage.len() as u32).to_be_bytes());
        bytes.extend_from_slice(lineage);
        bytes.extend_from_slice(&self.projection_id.to_be_bytes());
        bytes.extend_from_slice(self.plan_hash.as_bytes());
        bytes
    }
}

/// A nonzero, never-reused build generation for one projection identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProjectionGeneration(NonZeroU64);

impl ProjectionGeneration {
    /// Creates a generation, rejecting the semantic zero sentinel.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the first generation for one exact projection identity.
    #[must_use]
    pub const fn first() -> Self {
        Self(NonZeroU64::MIN)
    }

    /// Returns the numeric generation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the canonical big-endian representation.
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.get().to_be_bytes()
    }

    /// Returns the next generation, or `None` at the numeric limit.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

impl TryFrom<u64> for ProjectionGeneration {
    type Error = ProjectionGenerationError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(ProjectionGenerationError)
    }
}

impl From<ProjectionGeneration> for u64 {
    fn from(value: ProjectionGeneration) -> Self {
        value.get()
    }
}

/// A safe failure to construct a nonzero projection generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionGenerationError;

impl fmt::Display for ProjectionGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("projection generation must be nonzero")
    }
}

impl Error for ProjectionGenerationError {}

/// A typed hash of one complete checked projection apply request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProjectionApplyHash([u8; 32]);

impl ProjectionApplyHash {
    /// Creates a typed projection-apply hash from its 32 digest bytes.
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

/// A complete canonical projection group-state key.
#[derive(Clone)]
pub struct ProjectionGroupKey {
    bytes: Vec<u8>,
    identity: ProjectionIdentity,
    generation: ProjectionGeneration,
    components: Vec<CanonicalValue>,
}

impl ProjectionGroupKey {
    /// Parses a syntactically canonical complete group key.
    ///
    /// Exact component types and count still require the matching checked
    /// projection group schema.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, ProjectionKeyError> {
        let parts = decode_group_parts(&bytes, true)?;
        Ok(Self {
            bytes,
            identity: parts.identity,
            generation: parts.generation,
            components: parts.components,
        })
    }

    /// Borrows the canonical key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the canonical key bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Returns the projection identity encoded by the key.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }

    /// Returns the generation encoded by the key.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the syntactically validated canonical scalar components.
    #[must_use]
    pub fn components(&self) -> &[CanonicalValue] {
        &self.components
    }
}

/// A transient canonical prefix for one published projection generation.
#[derive(Clone)]
pub struct ProjectionGroupPrefix {
    bytes: Vec<u8>,
    identity: ProjectionIdentity,
    generation: ProjectionGeneration,
    components: Vec<CanonicalValue>,
}

impl ProjectionGroupPrefix {
    /// Parses a syntactically canonical zero-or-more-component prefix.
    ///
    /// Exact leading-component validation still requires the matching checked
    /// projection group schema.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, ProjectionKeyError> {
        let parts = decode_group_parts(&bytes, false)?;
        Ok(Self {
            bytes,
            identity: parts.identity,
            generation: parts.generation,
            components: parts.components,
        })
    }

    /// Borrows the canonical prefix bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the projection identity encoded by the prefix.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }

    /// Returns the generation encoded by the prefix.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the syntactically validated complete leading components.
    #[must_use]
    pub fn components(&self) -> &[CanonicalValue] {
        &self.components
    }

    /// Returns the lexicographic exclusive successor for a prefix range.
    ///
    /// A valid prefix always begins with `0x47`, so a successor always exists.
    #[must_use]
    pub fn exclusive_successor(&self) -> Vec<u8> {
        let mut successor = self.bytes.clone();
        for index in (0..successor.len()).rev() {
            if successor[index] != u8::MAX {
                successor[index] += 1;
                successor.truncate(index + 1);
                return successor;
            }
        }
        unreachable!("a projection group prefix begins with 0x47")
    }
}

/// Builder for one syntactically canonical complete projection group key.
#[derive(Clone)]
pub struct ProjectionGroupKeyBuilder(ProjectionComponentsBuilder);

impl ProjectionGroupKeyBuilder {
    /// Starts a key with its exact identity and nonzero generation.
    #[must_use]
    pub fn new(identity: ProjectionIdentity, generation: ProjectionGeneration) -> Self {
        Self(ProjectionComponentsBuilder::new(identity, generation))
    }

    /// Appends one complete canonical scalar component.
    pub fn push_component(
        &mut self,
        value: CanonicalValue,
    ) -> Result<&mut Self, ProjectionKeyError> {
        self.0.push_component(value)?;
        Ok(self)
    }

    /// Borrows the currently encoded key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.bytes
    }

    /// Finishes a nonempty syntactically canonical complete key.
    pub fn finish(self) -> Result<ProjectionGroupKey, ProjectionKeyError> {
        if self.0.components.is_empty() {
            return Err(ProjectionKeyError::EmptyGroupKey);
        }
        Ok(ProjectionGroupKey {
            bytes: self.0.bytes,
            identity: self.0.identity,
            generation: self.0.generation,
            components: self.0.components,
        })
    }
}

/// Builder for a syntactically canonical projection group prefix.
#[derive(Clone)]
pub struct ProjectionGroupPrefixBuilder(ProjectionComponentsBuilder);

impl ProjectionGroupPrefixBuilder {
    /// Starts a prefix with its exact identity and nonzero generation.
    #[must_use]
    pub fn new(identity: ProjectionIdentity, generation: ProjectionGeneration) -> Self {
        Self(ProjectionComponentsBuilder::new(identity, generation))
    }

    /// Appends one complete canonical leading scalar component.
    pub fn push_component(
        &mut self,
        value: CanonicalValue,
    ) -> Result<&mut Self, ProjectionKeyError> {
        self.0.push_component(value)?;
        Ok(self)
    }

    /// Borrows the currently encoded prefix bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.bytes
    }

    /// Finishes a zero-or-more-component syntactically canonical prefix.
    #[must_use]
    pub fn finish(self) -> ProjectionGroupPrefix {
        ProjectionGroupPrefix {
            bytes: self.0.bytes,
            identity: self.0.identity,
            generation: self.0.generation,
            components: self.0.components,
        }
    }
}

#[derive(Clone)]
struct ProjectionComponentsBuilder {
    bytes: Vec<u8>,
    identity: ProjectionIdentity,
    generation: ProjectionGeneration,
    components: Vec<CanonicalValue>,
}

impl ProjectionComponentsBuilder {
    fn new(identity: ProjectionIdentity, generation: ProjectionGeneration) -> Self {
        let identity_bytes = identity.to_canonical_bytes();
        let mut bytes = Vec::with_capacity(2 + identity_bytes.len() + 8);
        bytes.extend_from_slice(&PROJECTION_GROUP_KEY_V1_PREFIX);
        bytes.extend_from_slice(&identity_bytes);
        bytes.extend_from_slice(&generation.to_be_bytes());
        Self {
            bytes,
            identity,
            generation,
            components: Vec::new(),
        }
    }

    fn push_component(&mut self, value: CanonicalValue) -> Result<(), ProjectionKeyError> {
        if self.components.len() == MAX_PROJECTION_GROUP_COMPONENTS {
            return Err(ProjectionKeyError::TooManyComponents);
        }
        let encoded_length = projection_scalar_encoded_length(&value)?;
        let additional = 4usize
            .checked_add(encoded_length)
            .ok_or(ProjectionKeyError::TooLong {
                actual: usize::MAX,
                maximum: MAX_KEY_BYTES,
            })?;
        let actual =
            self.bytes
                .len()
                .checked_add(additional)
                .ok_or(ProjectionKeyError::TooLong {
                    actual: usize::MAX,
                    maximum: MAX_KEY_BYTES,
                })?;
        if actual > MAX_KEY_BYTES {
            return Err(ProjectionKeyError::TooLong {
                actual,
                maximum: MAX_KEY_BYTES,
            });
        }

        let encoded = encode_canonical_value(&value)
            .map_err(|_| ProjectionKeyError::InvalidCanonicalComponent)?;
        if encoded.len() != encoded_length {
            return Err(ProjectionKeyError::InvalidCanonicalComponent);
        }
        let encoded_length = u32::try_from(encoded.len())
            .map_err(|_| ProjectionKeyError::InvalidCanonicalComponent)?;
        self.bytes.extend_from_slice(&encoded_length.to_be_bytes());
        self.bytes.extend_from_slice(&encoded);
        self.components.push(value);
        Ok(())
    }
}

/// The generation-neutral control/frontier key for one projection identity.
#[derive(Clone)]
pub struct ProjectionFrontierKey {
    bytes: Vec<u8>,
    identity: ProjectionIdentity,
}

impl ProjectionFrontierKey {
    /// Constructs the exact v1 frontier/control key.
    #[must_use]
    pub fn new(identity: ProjectionIdentity) -> Self {
        let identity_bytes = identity.to_canonical_bytes();
        let mut bytes = Vec::with_capacity(2 + identity_bytes.len());
        bytes.extend_from_slice(&PROJECTION_FRONTIER_KEY_V1_PREFIX);
        bytes.extend_from_slice(&identity_bytes);
        Self { bytes, identity }
    }

    /// Parses one exact v1 frontier/control key.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, ProjectionKeyError> {
        validate_key_bound(&bytes)?;
        validate_prefix(&bytes, PROJECTION_FRONTIER_KEY_V1_PREFIX)?;
        let (identity, consumed) = decode_identity_prefix(&bytes[2..])?;
        if 2 + consumed != bytes.len() {
            return Err(ProjectionKeyError::TrailingBytes);
        }
        Ok(Self { bytes, identity })
    }

    /// Borrows the canonical key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the projection identity encoded by the key.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }
}

/// One generation- and sequence-specific projection apply-marker key.
#[derive(Clone)]
pub struct ProjectionApplyKey {
    bytes: Vec<u8>,
    identity: ProjectionIdentity,
    generation: ProjectionGeneration,
    commit_sequence: CommitSequence,
}

impl ProjectionApplyKey {
    /// Constructs the exact v1 projection apply-marker key.
    #[must_use]
    pub fn new(
        identity: ProjectionIdentity,
        generation: ProjectionGeneration,
        commit_sequence: CommitSequence,
    ) -> Self {
        let identity_bytes = identity.to_canonical_bytes();
        let mut bytes = Vec::with_capacity(2 + identity_bytes.len() + 16);
        bytes.extend_from_slice(&PROJECTION_APPLY_KEY_V1_PREFIX);
        bytes.extend_from_slice(&identity_bytes);
        bytes.extend_from_slice(&generation.to_be_bytes());
        bytes.extend_from_slice(&commit_sequence.to_be_bytes());
        Self {
            bytes,
            identity,
            generation,
            commit_sequence,
        }
    }

    /// Parses one exact v1 projection apply-marker key.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, ProjectionKeyError> {
        validate_key_bound(&bytes)?;
        validate_prefix(&bytes, PROJECTION_APPLY_KEY_V1_PREFIX)?;
        let (identity, consumed) = decode_identity_prefix(&bytes[2..])?;
        let mut position = 2 + consumed;
        if bytes.len().saturating_sub(position) != 16 {
            return Err(ProjectionKeyError::TruncatedOrTrailing);
        }
        let generation = decode_generation(read_array::<8>(&bytes, &mut position)?)?;
        let commit_sequence = decode_commit_sequence(read_array::<8>(&bytes, &mut position)?)?;
        Ok(Self {
            bytes,
            identity,
            generation,
            commit_sequence,
        })
    }

    /// Borrows the canonical key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the projection identity encoded by the key.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }

    /// Returns the projection generation encoded by the key.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the applied commit sequence encoded by the key.
    #[must_use]
    pub const fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }
}

macro_rules! byte_key_traits {
    ($type:ty) => {
        impl PartialEq for $type {
            fn eq(&self, other: &Self) -> bool {
                self.bytes == other.bytes
            }
        }

        impl Eq for $type {}

        impl PartialOrd for $type {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $type {
            fn cmp(&self, other: &Self) -> Ordering {
                self.bytes.cmp(&other.bytes)
            }
        }

        impl Hash for $type {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.bytes.hash(state);
            }
        }
    };
}

byte_key_traits!(ProjectionGroupKey);
byte_key_traits!(ProjectionGroupPrefix);
byte_key_traits!(ProjectionFrontierKey);
byte_key_traits!(ProjectionApplyKey);

macro_rules! redacted_key_debug {
    ($type:ty, $name:literal) => {
        impl fmt::Debug for $type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct($name)
                    .field("bytes", &"[REDACTED]")
                    .field("length", &self.bytes.len())
                    .finish()
            }
        }
    };
}

redacted_key_debug!(ProjectionGroupKey, "ProjectionGroupKey");
redacted_key_debug!(ProjectionGroupPrefix, "ProjectionGroupPrefix");
redacted_key_debug!(ProjectionFrontierKey, "ProjectionFrontierKey");
redacted_key_debug!(ProjectionApplyKey, "ProjectionApplyKey");

impl TryFrom<Vec<u8>> for ProjectionGroupKey {
    type Error = ProjectionKeyError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

impl TryFrom<Vec<u8>> for ProjectionGroupPrefix {
    type Error = ProjectionKeyError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

impl TryFrom<Vec<u8>> for ProjectionFrontierKey {
    type Error = ProjectionKeyError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

impl TryFrom<Vec<u8>> for ProjectionApplyKey {
    type Error = ProjectionKeyError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

/// A safe failure to construct or decode a projection identity or key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionKeyError {
    /// The complete key exceeds the durable key hard limit.
    TooLong {
        /// Attempted encoded byte length.
        actual: usize,
        /// Maximum encoded byte length.
        maximum: usize,
    },
    /// The input ends before a required field is complete.
    Truncated,
    /// The input contains bytes after its complete fixed shape.
    TrailingBytes,
    /// The fixed shape has either missing or trailing bytes.
    TruncatedOrTrailing,
    /// The key belongs to another purpose namespace.
    WrongPurpose,
    /// The key uses an unsupported key format version.
    UnsupportedVersion,
    /// The projection lineage or identity payload is invalid.
    InvalidIdentity,
    /// A projection ID uses the forbidden zero sentinel.
    ZeroProjectionId,
    /// A projection generation uses the forbidden zero sentinel.
    ZeroGeneration,
    /// A commit sequence uses the forbidden zero sentinel.
    ZeroCommitSequence,
    /// A complete group key contains no component.
    EmptyGroupKey,
    /// A framed component has a forbidden zero byte length.
    EmptyComponent,
    /// The component count exceeds the v1 hard limit.
    TooManyComponents,
    /// Null, list, or record is not a projection group scalar.
    NonScalarComponent,
    /// A component is not one exact canonical scalar encoding.
    InvalidCanonicalComponent,
}

impl fmt::Display for ProjectionKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TooLong { .. } => "projection key exceeds the hard byte limit",
            Self::Truncated => "projection key is truncated",
            Self::TrailingBytes => "projection key has trailing bytes",
            Self::TruncatedOrTrailing => "projection key has the wrong fixed length",
            Self::WrongPurpose => "projection key has the wrong purpose",
            Self::UnsupportedVersion => "projection key version is unsupported",
            Self::InvalidIdentity => "projection identity is invalid",
            Self::ZeroProjectionId => "projection ID must be nonzero",
            Self::ZeroGeneration => "projection generation must be nonzero",
            Self::ZeroCommitSequence => "commit sequence must be nonzero",
            Self::EmptyGroupKey => "complete projection group key has no components",
            Self::EmptyComponent => "projection group component is empty",
            Self::TooManyComponents => "projection group has too many components",
            Self::NonScalarComponent => "projection group component is not a scalar",
            Self::InvalidCanonicalComponent => {
                "projection group component is not canonically encoded"
            }
        })
    }
}

impl Error for ProjectionKeyError {}

struct ProjectionGroupParts {
    identity: ProjectionIdentity,
    generation: ProjectionGeneration,
    components: Vec<CanonicalValue>,
}

fn decode_group_parts(
    bytes: &[u8],
    require_nonempty: bool,
) -> Result<ProjectionGroupParts, ProjectionKeyError> {
    validate_key_bound(bytes)?;
    validate_prefix(bytes, PROJECTION_GROUP_KEY_V1_PREFIX)?;
    let (identity, consumed) = decode_identity_prefix(&bytes[2..])?;
    let mut position = 2 + consumed;
    let generation = decode_generation(read_array::<8>(bytes, &mut position)?)?;
    let mut components = Vec::new();
    while position < bytes.len() {
        if components.len() == MAX_PROJECTION_GROUP_COMPONENTS {
            return Err(ProjectionKeyError::TooManyComponents);
        }
        let length = u32::from_be_bytes(read_array::<4>(bytes, &mut position)?) as usize;
        if length == 0 {
            return Err(ProjectionKeyError::EmptyComponent);
        }
        let end = position
            .checked_add(length)
            .ok_or(ProjectionKeyError::Truncated)?;
        let encoded = bytes
            .get(position..end)
            .ok_or(ProjectionKeyError::Truncated)?;
        let value = decode_canonical_value(encoded)
            .map_err(|_| ProjectionKeyError::InvalidCanonicalComponent)?;
        projection_scalar_encoded_length(&value)?;
        let canonical = encode_canonical_value(&value)
            .map_err(|_| ProjectionKeyError::InvalidCanonicalComponent)?;
        if canonical != encoded {
            return Err(ProjectionKeyError::InvalidCanonicalComponent);
        }
        components.push(value);
        position = end;
    }
    if require_nonempty && components.is_empty() {
        return Err(ProjectionKeyError::EmptyGroupKey);
    }
    Ok(ProjectionGroupParts {
        identity,
        generation,
        components,
    })
}

fn decode_identity_prefix(bytes: &[u8]) -> Result<(ProjectionIdentity, usize), ProjectionKeyError> {
    let mut position = 0;
    let lineage_length = u32::from_be_bytes(read_array::<4>(bytes, &mut position)?) as usize;
    if lineage_length == 0 || lineage_length > MAX_CONTRACT_LINEAGE_BYTES {
        return Err(ProjectionKeyError::InvalidIdentity);
    }
    let lineage_end = position
        .checked_add(lineage_length)
        .ok_or(ProjectionKeyError::InvalidIdentity)?;
    let lineage_bytes = bytes
        .get(position..lineage_end)
        .ok_or(ProjectionKeyError::Truncated)?;
    let lineage =
        std::str::from_utf8(lineage_bytes).map_err(|_| ProjectionKeyError::InvalidIdentity)?;
    let lineage = ContractLineage::new(lineage).map_err(|_| ProjectionKeyError::InvalidIdentity)?;
    position = lineage_end;

    let projection_raw = u32::from_be_bytes(read_array::<4>(bytes, &mut position)?);
    if projection_raw == 0 {
        return Err(ProjectionKeyError::ZeroProjectionId);
    }
    let projection_id =
        ProjectionId::try_from(projection_raw).map_err(|_| ProjectionKeyError::ZeroProjectionId)?;
    let plan_hash = ProjectionPlanHash::from_bytes(read_array::<32>(bytes, &mut position)?);
    Ok((
        ProjectionIdentity::new(lineage, projection_id, plan_hash),
        position,
    ))
}

fn projection_scalar_encoded_length(value: &CanonicalValue) -> Result<usize, ProjectionKeyError> {
    match value {
        CanonicalValue::Null | CanonicalValue::List(_) | CanonicalValue::Record(_)
        | CanonicalValue::Vector(_) => {
            Err(ProjectionKeyError::NonScalarComponent)
        }
        CanonicalValue::Bool(_) => Ok(3),
        CanonicalValue::I64(_) | CanonicalValue::U64(_) => Ok(10),
        CanonicalValue::Decimal(_) => Ok(20),
        CanonicalValue::Money(_) => Ok(23),
        CanonicalValue::String(value) => value
            .len()
            .checked_add(6)
            .ok_or(ProjectionKeyError::InvalidCanonicalComponent),
        CanonicalValue::Bytes(value) => value
            .len()
            .checked_add(6)
            .ok_or(ProjectionKeyError::InvalidCanonicalComponent),
        CanonicalValue::Timestamp(_) => Ok(14),
        CanonicalValue::Date(_) => Ok(6),
        CanonicalValue::Uuid(_) => Ok(18),
        CanonicalValue::Enum { .. } => Ok(10),
    }
}

fn validate_key_bound(bytes: &[u8]) -> Result<(), ProjectionKeyError> {
    if bytes.len() > MAX_KEY_BYTES {
        return Err(ProjectionKeyError::TooLong {
            actual: bytes.len(),
            maximum: MAX_KEY_BYTES,
        });
    }
    Ok(())
}

fn validate_prefix(bytes: &[u8], expected: [u8; 2]) -> Result<(), ProjectionKeyError> {
    let purpose = *bytes.first().ok_or(ProjectionKeyError::Truncated)?;
    if purpose != expected[0] {
        return Err(ProjectionKeyError::WrongPurpose);
    }
    let version = *bytes.get(1).ok_or(ProjectionKeyError::Truncated)?;
    if version != expected[1] {
        return Err(ProjectionKeyError::UnsupportedVersion);
    }
    Ok(())
}

fn decode_generation(bytes: [u8; 8]) -> Result<ProjectionGeneration, ProjectionKeyError> {
    ProjectionGeneration::new(u64::from_be_bytes(bytes)).ok_or(ProjectionKeyError::ZeroGeneration)
}

fn decode_commit_sequence(bytes: [u8; 8]) -> Result<CommitSequence, ProjectionKeyError> {
    let value = u64::from_be_bytes(bytes);
    if value == 0 {
        return Err(ProjectionKeyError::ZeroCommitSequence);
    }
    CommitSequence::try_from(value).map_err(|_| ProjectionKeyError::ZeroCommitSequence)
}

fn read_array<const N: usize>(
    bytes: &[u8],
    position: &mut usize,
) -> Result<[u8; N], ProjectionKeyError> {
    let end = position
        .checked_add(N)
        .ok_or(ProjectionKeyError::Truncated)?;
    let value = bytes
        .get(*position..end)
        .ok_or(ProjectionKeyError::Truncated)?;
    *position = end;
    value.try_into().map_err(|_| ProjectionKeyError::Truncated)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn projection_id(value: u32) -> ProjectionId {
        ProjectionId::try_from(value).expect("nonzero projection ID")
    }

    fn commit_sequence(value: u64) -> CommitSequence {
        CommitSequence::try_from(value).expect("nonzero commit sequence")
    }

    fn identity() -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new("budget").expect("valid lineage"),
            projection_id(0x0102_0304),
            ProjectionPlanHash::from_bytes(std::array::from_fn(|index| index as u8)),
        )
    }

    fn generation() -> ProjectionGeneration {
        ProjectionGeneration::new(7).expect("nonzero generation")
    }

    fn hex(input: &str) -> Vec<u8> {
        input
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).expect("ASCII hex");
                u8::from_str_radix(pair, 16).expect("valid hex")
            })
            .collect()
    }

    #[test]
    fn identity_and_fixed_key_vectors_are_stable() {
        let identity = identity();
        let identity_bytes = hex(
            "0000000662756467657401020304000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        );
        assert_eq!(identity.to_canonical_bytes(), identity_bytes);
        assert_eq!(
            ProjectionIdentity::from_canonical_bytes(&identity_bytes),
            Ok(identity.clone())
        );

        let frontier = ProjectionFrontierKey::new(identity.clone());
        let mut expected_frontier = vec![0x46, 0x01];
        expected_frontier.extend_from_slice(&identity_bytes);
        assert_eq!(frontier.as_bytes(), expected_frontier);
        assert_eq!(
            ProjectionFrontierKey::from_bytes(expected_frontier),
            Ok(frontier)
        );

        let apply = ProjectionApplyKey::new(identity, generation(), commit_sequence(9));
        let mut expected_apply = vec![0x41, 0x01];
        expected_apply.extend_from_slice(&identity_bytes);
        expected_apply.extend_from_slice(&7_u64.to_be_bytes());
        expected_apply.extend_from_slice(&9_u64.to_be_bytes());
        assert_eq!(apply.as_bytes(), expected_apply);
        assert_eq!(ProjectionApplyKey::from_bytes(expected_apply), Ok(apply));
    }

    #[test]
    fn complete_and_prefix_group_vectors_are_stable() {
        let mut complete = ProjectionGroupKeyBuilder::new(identity(), generation());
        complete
            .push_component(CanonicalValue::Bool(true))
            .expect("bounded")
            .push_component(CanonicalValue::U64(5))
            .expect("bounded");
        let complete = complete.finish().expect("nonempty complete key");

        let mut expected = vec![0x47, 0x01];
        expected.extend_from_slice(&identity().to_canonical_bytes());
        expected.extend_from_slice(&7_u64.to_be_bytes());
        expected.extend_from_slice(&3_u32.to_be_bytes());
        expected.extend_from_slice(&[0x01, 0x01, 0x01]);
        expected.extend_from_slice(&10_u32.to_be_bytes());
        expected.extend_from_slice(&[0x01, 0x03]);
        expected.extend_from_slice(&5_u64.to_be_bytes());
        assert_eq!(complete.as_bytes(), expected);
        assert_eq!(
            ProjectionGroupKey::from_bytes(expected),
            Ok(complete.clone())
        );

        let empty_prefix = ProjectionGroupPrefixBuilder::new(identity(), generation()).finish();
        assert!(complete.as_bytes().starts_with(empty_prefix.as_bytes()));

        let mut one_prefix = ProjectionGroupPrefixBuilder::new(identity(), generation());
        one_prefix
            .push_component(CanonicalValue::Bool(true))
            .expect("bounded");
        let one_prefix = one_prefix.finish();
        assert!(complete.as_bytes().starts_with(one_prefix.as_bytes()));
        assert!(complete.as_bytes() < one_prefix.exclusive_successor().as_slice());
        assert_eq!(
            ProjectionGroupPrefix::from_bytes(one_prefix.as_bytes().to_vec()),
            Ok(one_prefix)
        );
    }

    #[test]
    fn every_projection_scalar_has_a_complete_key_and_prefix_golden() {
        let decimal_spec = crate::DecimalSpec::new(5, 2).expect("valid decimal type");
        let decimal = crate::Decimal::new(decimal_spec, -1234).expect("bounded decimal");
        let values = vec![
            CanonicalValue::Bool(true),
            CanonicalValue::I64(-1),
            CanonicalValue::U64(u64::MAX),
            CanonicalValue::Decimal(decimal),
            CanonicalValue::Money(crate::Money::new(
                crate::CurrencyCode::new("USD").expect("valid currency"),
                decimal,
            )),
            CanonicalValue::string("é").expect("bounded string"),
            CanonicalValue::bytes([0, 0xff]).expect("bounded bytes"),
            CanonicalValue::Timestamp(
                crate::Timestamp::new(-1, 999_999_999).expect("valid timestamp"),
            ),
            CanonicalValue::Date(crate::Date::from_days_since_unix_epoch(-1)),
            CanonicalValue::Uuid([
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ]),
            CanonicalValue::Enum {
                type_id: crate::EnumTypeId::new(42).expect("nonzero enum type ID"),
                variant_id: crate::EnumVariantId::new(7).expect("nonzero enum variant ID"),
            },
        ];
        let encoded_components = [
            "010101",
            "0102ffffffffffffffff",
            "0103ffffffffffffffff",
            "01040502fffffffffffffffffffffffffffffb2e",
            "01055553440502fffffffffffffffffffffffffffffb2e",
            "010600000002c3a9",
            "01070000000200ff",
            "0108ffffffffffffffff3b9ac9ff",
            "0109ffffffff",
            "010a000102030405060708090a0b0c0d0e0f",
            "010b0000002a00000007",
        ]
        .map(hex);
        assert_eq!(
            encoded_components.each_ref().map(|value| value.len()),
            [3, 10, 10, 20, 23, 8, 8, 14, 6, 18, 10]
        );

        let mut complete = ProjectionGroupKeyBuilder::new(identity(), generation());
        let mut prefix = ProjectionGroupPrefixBuilder::new(identity(), generation());
        for value in &values {
            complete
                .push_component(value.clone())
                .expect("scalar key remains bounded");
            prefix
                .push_component(value.clone())
                .expect("scalar prefix remains bounded");
        }
        let complete = complete.finish().expect("nonempty complete key");
        let prefix = prefix.finish();

        let mut expected = vec![0x47, 0x01];
        expected.extend_from_slice(&identity().to_canonical_bytes());
        expected.extend_from_slice(&generation().to_be_bytes());
        for component in encoded_components {
            expected.extend_from_slice(&(component.len() as u32).to_be_bytes());
            expected.extend_from_slice(&component);
        }
        assert_eq!(complete.as_bytes(), expected);
        assert_eq!(prefix.as_bytes(), expected);
        assert_eq!(complete.components(), values);
        assert_eq!(prefix.components(), values);
        assert_eq!(
            ProjectionGroupKey::from_bytes(expected.clone()),
            Ok(complete)
        );
        assert_eq!(ProjectionGroupPrefix::from_bytes(expected), Ok(prefix));
    }

    #[test]
    fn generations_are_nonzero_checked_and_never_wrap() {
        assert_eq!(ProjectionGeneration::new(0), None);
        assert_eq!(ProjectionGeneration::first().get(), 1);
        assert_eq!(
            ProjectionGeneration::first().checked_next(),
            ProjectionGeneration::new(2)
        );
        assert_eq!(
            ProjectionGeneration::new(u64::MAX)
                .expect("nonzero")
                .checked_next(),
            None
        );
    }

    #[test]
    fn group_builder_rejects_non_scalars_and_oversized_components_without_mutation() {
        let mut builder = ProjectionGroupKeyBuilder::new(identity(), generation());
        let before = builder.as_bytes().to_vec();
        assert!(matches!(
            builder.push_component(CanonicalValue::Null),
            Err(ProjectionKeyError::NonScalarComponent)
        ));
        assert_eq!(builder.as_bytes(), before);

        let oversized = CanonicalValue::string("x".repeat(MAX_KEY_BYTES))
            .expect("within canonical document limit");
        assert!(matches!(
            builder.push_component(oversized),
            Err(ProjectionKeyError::TooLong { .. })
        ));
        assert_eq!(builder.as_bytes(), before);
        assert_eq!(builder.finish(), Err(ProjectionKeyError::EmptyGroupKey));
    }

    #[test]
    fn complete_group_key_honors_the_exact_four_kibibyte_boundary() {
        let base = ProjectionGroupKeyBuilder::new(identity(), generation())
            .as_bytes()
            .len();
        let exact_string_bytes = MAX_KEY_BYTES - base - 4 - 6;

        let mut exact = ProjectionGroupKeyBuilder::new(identity(), generation());
        exact
            .push_component(
                CanonicalValue::string("x".repeat(exact_string_bytes)).expect("bounded value"),
            )
            .expect("exactly four KiB");
        let exact = exact.finish().expect("complete key");
        assert_eq!(exact.as_bytes().len(), MAX_KEY_BYTES);
        assert_eq!(
            ProjectionGroupKey::from_bytes(exact.as_bytes().to_vec()),
            Ok(exact)
        );

        let mut too_large = ProjectionGroupKeyBuilder::new(identity(), generation());
        assert!(matches!(
            too_large.push_component(
                CanonicalValue::string("x".repeat(exact_string_bytes + 1)).expect("bounded value"),
            ),
            Err(ProjectionKeyError::TooLong {
                actual: 4_097,
                maximum: MAX_KEY_BYTES,
            })
        ));
    }

    #[test]
    fn decoders_reject_zero_and_malformed_fields() {
        let identity_bytes = identity().to_canonical_bytes();
        let projection_id_offset = 4 + "budget".len();
        let mut zero_id = identity_bytes.clone();
        zero_id[projection_id_offset..projection_id_offset + 4].fill(0);
        assert_eq!(
            ProjectionIdentity::from_canonical_bytes(&zero_id),
            Err(ProjectionKeyError::ZeroProjectionId)
        );

        let mut zero_generation = ProjectionGroupPrefixBuilder::new(identity(), generation())
            .finish()
            .as_bytes()
            .to_vec();
        let generation_offset = 2 + identity_bytes.len();
        zero_generation[generation_offset..generation_offset + 8].fill(0);
        assert_eq!(
            ProjectionGroupPrefix::from_bytes(zero_generation),
            Err(ProjectionKeyError::ZeroGeneration)
        );

        let mut zero_sequence =
            ProjectionApplyKey::new(identity(), generation(), commit_sequence(9))
                .as_bytes()
                .to_vec();
        let sequence_offset = 2 + identity_bytes.len() + 8;
        zero_sequence[sequence_offset..sequence_offset + 8].fill(0);
        assert_eq!(
            ProjectionApplyKey::from_bytes(zero_sequence),
            Err(ProjectionKeyError::ZeroCommitSequence)
        );

        let mut wrong_purpose = ProjectionFrontierKey::new(identity()).as_bytes().to_vec();
        wrong_purpose[0] = 0x45;
        assert_eq!(
            ProjectionFrontierKey::from_bytes(wrong_purpose),
            Err(ProjectionKeyError::WrongPurpose)
        );
    }

    #[test]
    fn group_key_formatting_never_exposes_application_values() {
        let mut builder = ProjectionGroupKeyBuilder::new(identity(), generation());
        builder
            .push_component(CanonicalValue::string("TOP_SECRET").expect("bounded"))
            .expect("bounded key");
        let key = builder.finish().expect("complete key");
        let debug = format!("{key:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("TOP_SECRET"));
    }

    proptest! {
        #[test]
        fn canonical_group_keys_round_trip_for_bounded_scalar_sequences(
            values in proptest::collection::vec(any::<bool>(), 1..64),
        ) {
            let mut builder = ProjectionGroupKeyBuilder::new(identity(), generation());
            for value in values {
                builder
                    .push_component(CanonicalValue::Bool(value))
                    .expect("generated key is bounded");
            }
            let key = builder.finish().expect("nonempty complete key");
            prop_assert_eq!(
                ProjectionGroupKey::from_bytes(key.as_bytes().to_vec()),
                Ok(key)
            );
        }

        #[test]
        fn arbitrary_projection_key_bytes_never_panic(
            bytes in proptest::collection::vec(any::<u8>(), 0..5_000),
        ) {
            let _ = ProjectionGroupKey::from_bytes(bytes.clone());
            let _ = ProjectionGroupPrefix::from_bytes(bytes.clone());
            let _ = ProjectionFrontierKey::from_bytes(bytes.clone());
            let _ = ProjectionApplyKey::from_bytes(bytes);
        }
    }
}
