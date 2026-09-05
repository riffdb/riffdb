//! Canonical complete-inventory root for one immutable V2 generation.

use std::collections::BTreeSet;
use std::fmt;

use riffdb_types::{
    CommitSequence, FrontierPosition, HashDomain, ProjectionGeneration, decode_canonical_value,
    encode_canonical_value, hash,
};

use crate::{
    COLUMNAR_ENCODING_REGISTRY_VERSION_V1, COLUMNAR_LAYOUT_VERSION_V2,
    COLUMNAR_MANIFEST_FORMAT_VERSION_V2, COLUMNAR_SEGMENT_FORMAT_VERSION_V2, DefinitionFingerprint,
    MAX_COLUMNAR_MANIFEST_V2_BYTES, OrgKey,
};

/// Durable generation-root format identity introduced by ADR-0190.
pub const COLUMNAR_GENERATION_ROOT_FORMAT_VERSION_V1: u16 = 1;
/// Canonical filename inside one immutable generation directory.
pub const COLUMNAR_GENERATION_ROOT_FILE_NAME_V1: &str = "ROOT-V1";
/// Maximum complete encoded root bytes.
pub const MAX_COLUMNAR_GENERATION_ROOT_V1_BYTES: usize = 16 * 1024 * 1024;
/// Maximum organization partitions named by one root.
pub const MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS: usize = 4_096;

const MAGIC: &[u8; 8] = b"RDBCGRT\0";
const PHYSICAL_FINGERPRINT_PREFIX: &[u8] = b"riffdb.columnar.physical-generation/v1\0";
const CHECKSUM_BYTES: usize = 32;
const MAX_ORG_KEY_BYTES: usize = 1024 * 1024;

/// Root-local digest binding the frozen physical-format tuple without changing
/// the legacy definition fingerprint carried by Segment V2 and Manifest V2.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PhysicalGenerationFingerprintV1([u8; 32]);

impl PhysicalGenerationFingerprintV1 {
    /// Computes the accepted root-only physical tuple fingerprint.
    #[must_use]
    pub fn compute(definition_fingerprint: DefinitionFingerprint) -> Self {
        let mut payload = Vec::with_capacity(
            PHYSICAL_FINGERPRINT_PREFIX.len() + std::mem::size_of::<[u8; 32]>() + 10,
        );
        payload.extend_from_slice(PHYSICAL_FINGERPRINT_PREFIX);
        payload.extend_from_slice(definition_fingerprint.as_bytes());
        payload.extend_from_slice(&COLUMNAR_LAYOUT_VERSION_V2.to_be_bytes());
        payload.extend_from_slice(&COLUMNAR_SEGMENT_FORMAT_VERSION_V2.to_be_bytes());
        payload.extend_from_slice(&COLUMNAR_ENCODING_REGISTRY_VERSION_V1.to_be_bytes());
        payload.extend_from_slice(&COLUMNAR_MANIFEST_FORMAT_VERSION_V2.to_be_bytes());
        Self(*hash(HashDomain::CanonicalValue, &payload).as_bytes())
    }

    /// Exact digest bytes stored in the root and common control.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Consumes the wrapper into its exact digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// One complete partition-manifest member of a generation root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnarGenerationRootV1Entry {
    organization: OrgKey,
    manifest_length: u64,
    manifest_checksum: [u8; 32],
}

impl ColumnarGenerationRootV1Entry {
    /// Validates one exact partition inventory member.
    pub fn new(
        organization: OrgKey,
        manifest_length: u64,
        manifest_checksum: [u8; 32],
    ) -> Result<Self, ColumnarGenerationRootError> {
        if organization.as_bytes().is_empty() || organization.as_bytes().len() > MAX_ORG_KEY_BYTES {
            return Err(ColumnarGenerationRootError::BoundExceeded);
        }
        let decoded = decode_canonical_value(organization.as_bytes())
            .map_err(|_| ColumnarGenerationRootError::NonCanonical)?;
        let canonical = encode_canonical_value(&decoded)
            .map_err(|_| ColumnarGenerationRootError::NonCanonical)?;
        if canonical != organization.as_bytes() {
            return Err(ColumnarGenerationRootError::NonCanonical);
        }
        if manifest_length == 0 || manifest_length > MAX_COLUMNAR_MANIFEST_V2_BYTES as u64 {
            return Err(ColumnarGenerationRootError::BoundExceeded);
        }
        Ok(Self {
            organization,
            manifest_length,
            manifest_checksum,
        })
    }

    /// Canonical organization partition bytes.
    #[must_use]
    pub const fn organization(&self) -> &OrgKey {
        &self.organization
    }

    /// Exact complete Manifest V2 byte length.
    #[must_use]
    pub const fn manifest_length(&self) -> u64 {
        self.manifest_length
    }

    /// Exact complete Manifest V2 checksum.
    #[must_use]
    pub const fn manifest_checksum(&self) -> &[u8; 32] {
        &self.manifest_checksum
    }

    /// Sole relative filename derived from the manifest checksum.
    #[must_use]
    pub fn file_name(&self) -> String {
        let mut name = String::with_capacity("partition-".len() + 64 + ".manifest-v2".len());
        name.push_str("partition-");
        for byte in self.manifest_checksum {
            use std::fmt::Write as _;
            let _ = write!(name, "{byte:02x}");
        }
        name.push_str(".manifest-v2");
        name
    }
}

/// Complete canonical physical inventory for one V2 projection generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnarGenerationRootV1 {
    definition_fingerprint: DefinitionFingerprint,
    physical_generation_fingerprint: PhysicalGenerationFingerprintV1,
    history_incarnation: u64,
    generation: ProjectionGeneration,
    frontier: FrontierPosition,
    total_segments: u64,
    total_rows: u64,
    partitions: Vec<ColumnarGenerationRootV1Entry>,
    encoded_length: u64,
}

impl ColumnarGenerationRootV1 {
    /// Validates a complete inventory and computes its root-only physical tuple.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        definition_fingerprint: DefinitionFingerprint,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        frontier: FrontierPosition,
        total_segments: u64,
        total_rows: u64,
        partitions: Vec<ColumnarGenerationRootV1Entry>,
    ) -> Result<Self, ColumnarGenerationRootError> {
        let physical_generation_fingerprint =
            PhysicalGenerationFingerprintV1::compute(definition_fingerprint);
        let encoded_length = encoded_length(&partitions)?;
        let root = Self {
            definition_fingerprint,
            physical_generation_fingerprint,
            history_incarnation,
            generation,
            frontier,
            total_segments,
            total_rows,
            partitions,
            encoded_length,
        };
        root.validate()?;
        Ok(root)
    }

    fn validate(&self) -> Result<(), ColumnarGenerationRootError> {
        if self.history_incarnation == 0
            || self.partitions.len() > MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS
            || self.encoded_length as usize > MAX_COLUMNAR_GENERATION_ROOT_V1_BYTES
        {
            return Err(ColumnarGenerationRootError::BoundExceeded);
        }
        if self.physical_generation_fingerprint
            != PhysicalGenerationFingerprintV1::compute(self.definition_fingerprint)
        {
            return Err(ColumnarGenerationRootError::PhysicalFingerprintMismatch);
        }
        if self.partitions.is_empty() {
            if self.total_segments != 0 || self.total_rows != 0 {
                return Err(ColumnarGenerationRootError::InvalidInventory);
            }
        } else if self.total_segments < self.partitions.len() as u64
            || self.total_rows < self.total_segments
        {
            return Err(ColumnarGenerationRootError::InvalidInventory);
        }
        if self
            .partitions
            .windows(2)
            .any(|pair| pair[0].organization.as_bytes() >= pair[1].organization.as_bytes())
        {
            return Err(ColumnarGenerationRootError::NonCanonical);
        }
        let mut filenames = BTreeSet::new();
        for entry in &self.partitions {
            if !filenames.insert(entry.file_name()) {
                return Err(ColumnarGenerationRootError::InvalidInventory);
            }
        }
        Ok(())
    }

    /// Encodes stable bytes ending in the accepted domain-separated checksum.
    pub fn encode(&self) -> Result<Vec<u8>, ColumnarGenerationRootError> {
        self.validate()?;
        let expected_length = encoded_length(&self.partitions)?;
        if expected_length != self.encoded_length {
            return Err(ColumnarGenerationRootError::NonCanonical);
        }
        let mut out = Vec::with_capacity(expected_length as usize);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&COLUMNAR_GENERATION_ROOT_FORMAT_VERSION_V1.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&expected_length.to_be_bytes());
        out.extend_from_slice(self.definition_fingerprint.as_bytes());
        out.extend_from_slice(self.physical_generation_fingerprint.as_bytes());
        out.extend_from_slice(&self.history_incarnation.to_be_bytes());
        out.extend_from_slice(&self.generation.to_be_bytes());
        encode_frontier(&mut out, self.frontier);
        out.extend_from_slice(&COLUMNAR_LAYOUT_VERSION_V2.to_be_bytes());
        out.extend_from_slice(&COLUMNAR_SEGMENT_FORMAT_VERSION_V2.to_be_bytes());
        out.extend_from_slice(&COLUMNAR_ENCODING_REGISTRY_VERSION_V1.to_be_bytes());
        out.extend_from_slice(&COLUMNAR_MANIFEST_FORMAT_VERSION_V2.to_be_bytes());
        out.extend_from_slice(&(self.partitions.len() as u64).to_be_bytes());
        out.extend_from_slice(&self.total_segments.to_be_bytes());
        out.extend_from_slice(&self.total_rows.to_be_bytes());
        out.extend_from_slice(&(self.partitions.len() as u32).to_be_bytes());
        for entry in &self.partitions {
            let organization_length = u32::try_from(entry.organization.as_bytes().len())
                .map_err(|_| ColumnarGenerationRootError::BoundExceeded)?;
            out.extend_from_slice(&organization_length.to_be_bytes());
            out.extend_from_slice(entry.organization.as_bytes());
            out.extend_from_slice(&entry.manifest_length.to_be_bytes());
            out.extend_from_slice(&entry.manifest_checksum);
        }
        if out.len() + CHECKSUM_BYTES != expected_length as usize {
            return Err(ColumnarGenerationRootError::NonCanonical);
        }
        let checksum = *hash(HashDomain::CanonicalValue, &out).as_bytes();
        out.extend_from_slice(&checksum);
        Ok(out)
    }

    /// Decodes, fully consumes, validates, and canonically re-encodes one root.
    pub fn decode(bytes: &[u8]) -> Result<Self, ColumnarGenerationRootError> {
        if bytes.len() > MAX_COLUMNAR_GENERATION_ROOT_V1_BYTES
            || bytes.len() < MINIMUM_ENCODED_LENGTH
        {
            return Err(ColumnarGenerationRootError::BoundExceeded);
        }
        let (body, checksum) = bytes
            .split_at_checked(bytes.len() - CHECKSUM_BYTES)
            .ok_or(ColumnarGenerationRootError::Truncated)?;
        if hash(HashDomain::CanonicalValue, body).as_bytes() != checksum {
            return Err(ColumnarGenerationRootError::ChecksumMismatch);
        }
        let mut reader = Reader::new(body);
        if reader.read_exact(MAGIC.len())? != MAGIC {
            return Err(ColumnarGenerationRootError::UnknownIdentity);
        }
        if reader.read_u16()? != COLUMNAR_GENERATION_ROOT_FORMAT_VERSION_V1
            || reader.read_u16()? != 0
        {
            return Err(ColumnarGenerationRootError::UnknownIdentity);
        }
        let encoded_length = reader.read_u64()?;
        if encoded_length != bytes.len() as u64 {
            return Err(ColumnarGenerationRootError::NonCanonical);
        }
        let definition_fingerprint = DefinitionFingerprint::from_bytes(reader.read_array()?);
        let physical_generation_fingerprint = PhysicalGenerationFingerprintV1(reader.read_array()?);
        let history_incarnation = reader.read_u64()?;
        let generation = ProjectionGeneration::new(reader.read_u64()?)
            .ok_or(ColumnarGenerationRootError::InvalidInventory)?;
        let frontier = decode_frontier(&mut reader)?;
        if reader.read_u32()? != COLUMNAR_LAYOUT_VERSION_V2
            || reader.read_u16()? != COLUMNAR_SEGMENT_FORMAT_VERSION_V2
            || reader.read_u16()? != COLUMNAR_ENCODING_REGISTRY_VERSION_V1
            || reader.read_u16()? != COLUMNAR_MANIFEST_FORMAT_VERSION_V2
        {
            return Err(ColumnarGenerationRootError::UnknownIdentity);
        }
        let partition_total = reader.read_u64()?;
        let total_segments = reader.read_u64()?;
        let total_rows = reader.read_u64()?;
        let count = reader.read_u32()? as usize;
        if count > MAX_COLUMNAR_GENERATION_ROOT_V1_PARTITIONS || partition_total != count as u64 {
            return Err(ColumnarGenerationRootError::BoundExceeded);
        }
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let organization_length = reader.read_u32()? as usize;
            if organization_length == 0 || organization_length > MAX_ORG_KEY_BYTES {
                return Err(ColumnarGenerationRootError::BoundExceeded);
            }
            let organization =
                OrgKey::from_encoded_bytes(reader.read_exact(organization_length)?.to_vec());
            let manifest_length = reader.read_u64()?;
            let manifest_checksum = reader.read_array()?;
            partitions.push(ColumnarGenerationRootV1Entry::new(
                organization,
                manifest_length,
                manifest_checksum,
            )?);
        }
        if !reader.is_empty() {
            return Err(ColumnarGenerationRootError::NonCanonical);
        }
        let root = Self {
            definition_fingerprint,
            physical_generation_fingerprint,
            history_incarnation,
            generation,
            frontier,
            total_segments,
            total_rows,
            partitions,
            encoded_length,
        };
        root.validate()?;
        if root.encode()?.as_slice() != bytes {
            return Err(ColumnarGenerationRootError::NonCanonical);
        }
        Ok(root)
    }

    /// Complete encoded byte length, including checksum.
    #[must_use]
    pub const fn encoded_length(&self) -> u64 {
        self.encoded_length
    }

    /// Frozen registered-definition fingerprint retained by members.
    #[must_use]
    pub const fn definition_fingerprint(&self) -> DefinitionFingerprint {
        self.definition_fingerprint
    }

    /// Root-only physical tuple digest.
    #[must_use]
    pub const fn physical_generation_fingerprint(&self) -> PhysicalGenerationFingerprintV1 {
        self.physical_generation_fingerprint
    }

    /// Authoritative history incarnation bound to every member.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Never-reused projection generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Matched authoritative frontier.
    #[must_use]
    pub const fn frontier(&self) -> FrontierPosition {
        self.frontier
    }

    /// Explicit selected layout identity.
    #[must_use]
    pub const fn layout_version(&self) -> u32 {
        COLUMNAR_LAYOUT_VERSION_V2
    }

    /// Explicit segment-format identity.
    #[must_use]
    pub const fn segment_format_version(&self) -> u16 {
        COLUMNAR_SEGMENT_FORMAT_VERSION_V2
    }

    /// Explicit encoding-registry identity.
    #[must_use]
    pub const fn encoding_registry_version(&self) -> u16 {
        COLUMNAR_ENCODING_REGISTRY_VERSION_V1
    }

    /// Explicit partition-manifest identity.
    #[must_use]
    pub const fn manifest_format_version(&self) -> u16 {
        COLUMNAR_MANIFEST_FORMAT_VERSION_V2
    }

    /// Exact complete segment total, validated against members at open.
    #[must_use]
    pub const fn total_segments(&self) -> u64 {
        self.total_segments
    }

    /// Exact complete row total, validated against members at open.
    #[must_use]
    pub const fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Complete canonical partition inventory.
    #[must_use]
    pub fn partitions(&self) -> &[ColumnarGenerationRootV1Entry] {
        &self.partitions
    }

    /// Canonical generation directory name.
    #[must_use]
    pub fn directory_name(&self) -> String {
        format!("generation-{:016x}", self.generation.get())
    }

    /// Canonical temporary generation directory name.
    #[must_use]
    pub fn temporary_directory_name(&self) -> String {
        format!("{}.tmp", self.directory_name())
    }
}

const MINIMUM_ENCODED_LENGTH: usize =
    8 + 2 + 2 + 8 + 32 + 32 + 8 + 8 + 9 + 4 + 2 + 2 + 2 + 8 + 8 + 8 + 4 + CHECKSUM_BYTES;

fn encoded_length(
    partitions: &[ColumnarGenerationRootV1Entry],
) -> Result<u64, ColumnarGenerationRootError> {
    let mut length = MINIMUM_ENCODED_LENGTH;
    for entry in partitions {
        length = length
            .checked_add(4)
            .and_then(|value| value.checked_add(entry.organization.as_bytes().len()))
            .and_then(|value| value.checked_add(8 + 32))
            .ok_or(ColumnarGenerationRootError::BoundExceeded)?;
    }
    if length > MAX_COLUMNAR_GENERATION_ROOT_V1_BYTES {
        return Err(ColumnarGenerationRootError::BoundExceeded);
    }
    u64::try_from(length).map_err(|_| ColumnarGenerationRootError::BoundExceeded)
}

fn encode_frontier(out: &mut Vec<u8>, frontier: FrontierPosition) {
    match frontier {
        FrontierPosition::BeforeFirst => {
            out.push(0);
            out.extend_from_slice(&0u64.to_be_bytes());
        }
        FrontierPosition::AppliedThrough(sequence) => {
            out.push(1);
            out.extend_from_slice(&sequence.to_be_bytes());
        }
    }
}

fn decode_frontier(
    reader: &mut Reader<'_>,
) -> Result<FrontierPosition, ColumnarGenerationRootError> {
    let tag = reader.read_u8()?;
    let sequence = reader.read_u64()?;
    match (tag, sequence) {
        (0, 0) => Ok(FrontierPosition::BeforeFirst),
        (1, sequence) => CommitSequence::new(sequence)
            .map(FrontierPosition::AppliedThrough)
            .ok_or(ColumnarGenerationRootError::NonCanonical),
        _ => Err(ColumnarGenerationRootError::NonCanonical),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], ColumnarGenerationRootError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ColumnarGenerationRootError::Truncated)?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or(ColumnarGenerationRootError::Truncated)?;
        self.offset = end;
        Ok(result)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ColumnarGenerationRootError> {
        self.read_exact(N)?
            .try_into()
            .map_err(|_| ColumnarGenerationRootError::Truncated)
    }

    fn read_u8(&mut self) -> Result<u8, ColumnarGenerationRootError> {
        self.read_exact(1)?
            .first()
            .copied()
            .ok_or(ColumnarGenerationRootError::Truncated)
    }

    fn read_u16(&mut self) -> Result<u16, ColumnarGenerationRootError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, ColumnarGenerationRootError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, ColumnarGenerationRootError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

/// Closed root codec rejection. Durable details remain internal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnarGenerationRootError {
    /// A fixed byte or count ceiling was exceeded.
    BoundExceeded,
    /// Bytes ended before a complete field.
    Truncated,
    /// A version, layout, registry, or reserved field is unknown.
    UnknownIdentity,
    /// The complete root checksum disagrees.
    ChecksumMismatch,
    /// The root-only physical tuple digest disagrees.
    PhysicalFingerprintMismatch,
    /// The complete inventory is internally contradictory.
    InvalidInventory,
    /// Ordering, presence, full consumption, or re-encoding is noncanonical.
    NonCanonical,
}

impl fmt::Display for ColumnarGenerationRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid columnar generation root")
    }
}

impl std::error::Error for ColumnarGenerationRootError {}
