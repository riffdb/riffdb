//! Bounded codec and private validate-once pruning evidence for segment V2.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use riffdb_types::{
    CanonicalValue, CommitSequence, CurrencyCode, Date, DecimalSpec, EntityVersion, EnumTypeId,
    FieldId, FrontierPosition, ProjectionGeneration, Timestamp, decode_canonical_value,
    encode_canonical_value,
};

use crate::checkpoint::checksum_bytes;
use crate::{DefinitionFingerprint, OrgKey, PrimaryKeyBytes};

/// Durable columnar segment format identity introduced by ADR-0160.
pub const COLUMNAR_SEGMENT_FORMAT_VERSION_V2: u16 = 2;
/// Closed physical encoding registry used by segment format V2.
pub const COLUMNAR_ENCODING_REGISTRY_VERSION_V1: u16 = 1;
/// Row-framed manifest identity retained for production V1 compatibility.
pub const COLUMNAR_MANIFEST_FORMAT_VERSION_V1: u16 = 1;
/// Reserved manifest successor identity. WP-710 does not publish this format.
pub const COLUMNAR_MANIFEST_FORMAT_VERSION_V2: u16 = 2;

/// Maximum rows in one independently encoded V2 segment.
pub const MAX_SEGMENT_V2_ROWS: usize = 65_536;
/// Maximum compiler-declared fields in one V2 segment.
pub const MAX_SEGMENT_V2_COLUMNS: usize = 1_024;
/// Maximum complete encoded segment size.
pub const MAX_SEGMENT_V2_BYTES: usize = 64 * 1024 * 1024;

const MAX_LANE_BYTES: usize = 16 * 1024 * 1024;
const MAX_SCALAR_BYTES: usize = 1024 * 1024;
const MAX_ORG_KEY_BYTES: usize = 1024 * 1024;
const MAGIC: &[u8; 8] = b"RDBCOLV2";
const COMPLETE_CHECKSUM_BYTES: usize = 32;

/// Stable opaque V2 segment identity within one generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SegmentV2SegmentId([u8; 16]);

impl SegmentV2SegmentId {
    /// Constructs an identity from its canonical bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the canonical identity bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Exact generation and partition facts bound into one V2 segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentV2Identity {
    definition_fingerprint: DefinitionFingerprint,
    history_incarnation: u64,
    generation: ProjectionGeneration,
    organization: OrgKey,
    segment_id: SegmentV2SegmentId,
    frontier_start: FrontierPosition,
    frontier_end: FrontierPosition,
}

impl SegmentV2Identity {
    /// Validates and constructs the complete segment identity.
    pub fn new(
        definition_fingerprint: DefinitionFingerprint,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        organization: OrgKey,
        segment_id: SegmentV2SegmentId,
        frontier_start: FrontierPosition,
        frontier_end: FrontierPosition,
    ) -> Result<Self, SegmentV2Error> {
        if history_incarnation == 0 {
            return Err(SegmentV2Error::Invalid("history incarnation is zero"));
        }
        if organization.as_bytes().len() > MAX_ORG_KEY_BYTES {
            return Err(SegmentV2Error::BoundExceeded("organization key bytes"));
        }
        if frontier_ordinal(frontier_start) > frontier_ordinal(frontier_end) {
            return Err(SegmentV2Error::Invalid("frontier interval is reversed"));
        }
        Ok(Self {
            definition_fingerprint,
            history_incarnation,
            generation,
            organization,
            segment_id,
            frontier_start,
            frontier_end,
        })
    }

    /// Returns the definition fingerprint.
    #[must_use]
    pub const fn definition_fingerprint(&self) -> DefinitionFingerprint {
        self.definition_fingerprint
    }

    /// Returns the authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the disjoint projection generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the exact organization partition.
    #[must_use]
    pub const fn organization(&self) -> &OrgKey {
        &self.organization
    }

    /// Returns the segment identity.
    #[must_use]
    pub const fn segment_id(&self) -> SegmentV2SegmentId {
        self.segment_id
    }

    /// Returns the first authoritative position represented by the segment.
    #[must_use]
    pub const fn frontier_start(&self) -> FrontierPosition {
        self.frontier_start
    }

    /// Returns the last authoritative position represented by the segment.
    #[must_use]
    pub const fn frontier_end(&self) -> FrontierPosition {
        self.frontier_end
    }
}

/// Closed logical type registry accepted by the V2 candidate codec.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SegmentV2LogicalType {
    /// Boolean values.
    Bool,
    /// Signed 64-bit integers.
    I64,
    /// Unsigned 64-bit integers.
    U64,
    /// Exact UTF-8 strings.
    String,
    /// Exact opaque bytes.
    Bytes,
    /// UTC timestamps.
    Timestamp,
    /// Calendar dates.
    Date,
    /// UUID network-order bytes.
    Uuid,
    /// One exact enum type.
    Enum(EnumTypeId),
    /// One exact fixed-scale decimal type.
    Decimal(DecimalSpec),
    /// One exact currency and fixed-scale decimal type.
    Money {
        /// Exact currency identity.
        currency: CurrencyCode,
        /// Exact amount type.
        amount: DecimalSpec,
    },
}

impl SegmentV2LogicalType {
    fn tag(&self) -> u8 {
        match self {
            Self::Bool => 1,
            Self::I64 => 2,
            Self::U64 => 3,
            Self::String => 4,
            Self::Bytes => 5,
            Self::Timestamp => 6,
            Self::Date => 7,
            Self::Uuid => 8,
            Self::Enum(_) => 9,
            Self::Decimal(_) => 10,
            Self::Money { .. } => 11,
        }
    }

    fn accepts(&self, value: &CanonicalValue) -> bool {
        match (self, value) {
            (Self::Bool, CanonicalValue::Bool(_))
            | (Self::I64, CanonicalValue::I64(_))
            | (Self::U64, CanonicalValue::U64(_))
            | (Self::String, CanonicalValue::String(_))
            | (Self::Bytes, CanonicalValue::Bytes(_))
            | (Self::Timestamp, CanonicalValue::Timestamp(_))
            | (Self::Date, CanonicalValue::Date(_))
            | (Self::Uuid, CanonicalValue::Uuid(_)) => true,
            (Self::Enum(expected), CanonicalValue::Enum { type_id, .. }) => expected == type_id,
            (Self::Decimal(expected), CanonicalValue::Decimal(value)) => *expected == value.spec(),
            (Self::Money { currency, amount }, CanonicalValue::Money(value)) => {
                *currency == value.currency() && *amount == value.amount().spec()
            }
            _ => false,
        }
    }
}

/// One optional-state-aware field cell.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SegmentV2Cell {
    /// Field was not present in the row shape.
    Missing,
    /// Field was present with explicit optional absence.
    Null,
    /// Field was present with an exact value.
    Value(CanonicalValue),
}

/// Exact private statistics for one logical lane.
#[derive(Clone, Debug, Eq, PartialEq)]
struct LaneStatistics {
    value_count: usize,
    missing_count: usize,
    null_count: usize,
    minimum: Option<CanonicalValue>,
    maximum: Option<CanonicalValue>,
}

/// Compiler-declared field lane before physical encoding selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentV2Column {
    field_id: FieldId,
    logical_type: SegmentV2LogicalType,
    cells: Vec<SegmentV2Cell>,
    statistics: LaneStatistics,
}

impl SegmentV2Column {
    /// Validates one typed lane and computes exact private statistics.
    pub fn new(
        field_id: FieldId,
        logical_type: SegmentV2LogicalType,
        cells: Vec<SegmentV2Cell>,
    ) -> Result<Self, SegmentV2Error> {
        if cells.len() > MAX_SEGMENT_V2_ROWS {
            return Err(SegmentV2Error::BoundExceeded("column rows"));
        }
        let statistics = compute_statistics(&logical_type, &cells)?;
        Ok(Self {
            field_id,
            logical_type,
            cells,
            statistics,
        })
    }

    /// Returns the stable field identity.
    #[must_use]
    pub const fn field_id(&self) -> FieldId {
        self.field_id
    }

    /// Returns the exact logical type.
    #[must_use]
    pub const fn logical_type(&self) -> &SegmentV2LogicalType {
        &self.logical_type
    }

    /// Returns cells in primary-key row order.
    #[must_use]
    pub fn cells(&self) -> &[SegmentV2Cell] {
        &self.cells
    }

    /// Applies the independent exact pruning truth table without exposing the
    /// underlying distribution statistics.
    pub fn pruning_decision(
        &self,
        predicate: &SegmentV2Predicate,
    ) -> Result<SegmentV2PruningDecision, SegmentV2Error> {
        pruning_decision(&self.logical_type, &self.statistics, predicate)
    }
}

/// Independently testable logical V2 segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentV2 {
    identity: SegmentV2Identity,
    primary_keys: Vec<PrimaryKeyBytes>,
    entity_versions: Vec<EntityVersion>,
    columns: Vec<SegmentV2Column>,
}

impl SegmentV2 {
    /// Validates bounds, row alignment, key order, and canonical field order.
    pub fn new(
        identity: SegmentV2Identity,
        primary_keys: Vec<PrimaryKeyBytes>,
        entity_versions: Vec<EntityVersion>,
        mut columns: Vec<SegmentV2Column>,
    ) -> Result<Self, SegmentV2Error> {
        let row_count = primary_keys.len();
        if row_count == 0 {
            return Err(SegmentV2Error::Invalid("segment has no rows"));
        }
        if row_count > MAX_SEGMENT_V2_ROWS {
            return Err(SegmentV2Error::BoundExceeded("segment rows"));
        }
        if entity_versions.len() != row_count {
            return Err(SegmentV2Error::Invalid("entity-version row count"));
        }
        if columns.is_empty() || columns.len() > MAX_SEGMENT_V2_COLUMNS {
            return Err(SegmentV2Error::BoundExceeded("segment columns"));
        }
        if primary_keys
            .windows(2)
            .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
        {
            return Err(SegmentV2Error::Invalid(
                "primary keys are not strictly ordered",
            ));
        }
        if primary_keys
            .iter()
            .any(|key| key.as_bytes().len() > MAX_SCALAR_BYTES)
        {
            return Err(SegmentV2Error::BoundExceeded("primary key bytes"));
        }
        if columns.iter().any(|column| column.cells.len() != row_count) {
            return Err(SegmentV2Error::Invalid("field row count"));
        }
        columns.sort_by_key(|column| column.field_id);
        if columns
            .windows(2)
            .any(|pair| pair[0].field_id == pair[1].field_id)
        {
            return Err(SegmentV2Error::Invalid("duplicate field lane"));
        }
        Ok(Self {
            identity,
            primary_keys,
            entity_versions,
            columns,
        })
    }

    /// Returns the complete segment identity.
    #[must_use]
    pub const fn identity(&self) -> &SegmentV2Identity {
        &self.identity
    }

    /// Returns primary keys in canonical row order.
    #[must_use]
    pub fn primary_keys(&self) -> &[PrimaryKeyBytes] {
        &self.primary_keys
    }

    /// Returns entity versions aligned with primary keys.
    #[must_use]
    pub fn entity_versions(&self) -> &[EntityVersion] {
        &self.entity_versions
    }

    /// Returns field lanes in stable field-ID order.
    #[must_use]
    pub fn columns(&self) -> &[SegmentV2Column] {
        &self.columns
    }
}

/// Exact predicate family admitted by the independent pruning oracle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SegmentV2Predicate {
    /// Value equality.
    Equal(CanonicalValue),
    /// Strict value lower bound.
    LessThan(CanonicalValue),
    /// Inclusive value lower bound.
    LessThanOrEqual(CanonicalValue),
    /// Strict value upper bound.
    GreaterThan(CanonicalValue),
    /// Inclusive value upper bound.
    GreaterThanOrEqual(CanonicalValue),
    /// Missing-state test.
    IsMissing,
    /// Explicit-null-state test.
    IsNull,
    /// Present non-null-state test.
    IsPresent,
}

/// Conservative exact segment-pruning result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentV2PruningDecision {
    /// At least one row may match, so the segment must be scanned.
    Scan,
    /// Exact evidence proves no row can match.
    Skip,
}

/// Private statistics retained only after complete segment validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentV2PruningIndex {
    columns: BTreeMap<FieldId, SegmentV2PruningColumn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SegmentV2PruningColumn {
    logical_type: SegmentV2LogicalType,
    statistics: LaneStatistics,
    dictionary: Option<Vec<Vec<u8>>>,
}

impl SegmentV2PruningIndex {
    fn from_validated(
        segment: &SegmentV2,
        directory: &[DirectoryEntry],
        dictionary_evidence: Vec<Option<Vec<Vec<u8>>>>,
        audit: &mut FusedDecodeAudit,
    ) -> Result<Self, SegmentV2Error> {
        if dictionary_evidence.len() != segment.columns().len() {
            return Err(SegmentV2Error::Corrupt("pruning dictionary count"));
        }
        let mut columns = BTreeMap::new();
        for ((column, entry), dictionary) in segment
            .columns()
            .iter()
            .zip(directory.iter().skip(2))
            .zip(dictionary_evidence)
        {
            if entry.kind != LaneKind::Field(column.field_id())
                || entry.logical_type != *column.logical_type()
            {
                return Err(SegmentV2Error::Corrupt("pruning lane identity"));
            }
            let dictionary = match (entry.encoding, dictionary) {
                (PhysicalEncoding::Dictionary, Some(dictionary)) => Some(dictionary),
                (PhysicalEncoding::Dictionary, None) => {
                    return Err(SegmentV2Error::Corrupt("missing dictionary evidence"));
                }
                (_, None) => None,
                (_, Some(_)) => {
                    return Err(SegmentV2Error::Corrupt("unexpected dictionary evidence"));
                }
            };
            audit.record_pruning_dictionary_entries(
                dictionary.as_ref().map_or(0, std::vec::Vec::len),
            );
            if columns
                .insert(
                    column.field_id(),
                    SegmentV2PruningColumn {
                        logical_type: column.logical_type().clone(),
                        statistics: column.statistics.clone(),
                        dictionary,
                    },
                )
                .is_some()
            {
                return Err(SegmentV2Error::Corrupt("duplicate pruning lane"));
            }
        }
        if columns.len() != segment.columns().len() {
            return Err(SegmentV2Error::Corrupt("pruning lane count"));
        }
        Ok(Self { columns })
    }

    pub(crate) fn proves_no_match(&self, field: FieldId, predicate: &SegmentV2Predicate) -> bool {
        let Some(column) = self.columns.get(&field) else {
            return false;
        };
        let Ok(decision) = pruning_decision(&column.logical_type, &column.statistics, predicate)
        else {
            return false;
        };
        if decision == SegmentV2PruningDecision::Skip {
            return true;
        }
        let (Some(dictionary), SegmentV2Predicate::Equal(value)) = (&column.dictionary, predicate)
        else {
            return false;
        };
        encode_canonical_value(value)
            .ok()
            .is_some_and(|encoded| dictionary.binary_search(&encoded).is_err())
    }
}

/// Closed physical encodings in registry V1.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum PhysicalEncoding {
    FixedWidth = 1,
    OffsetCanonical = 2,
    Dictionary = 3,
    BooleanBitmap = 4,
    CheckedDelta = 5,
}

impl PhysicalEncoding {
    fn from_tag(tag: u8) -> Result<Self, SegmentV2Error> {
        match tag {
            1 => Ok(Self::FixedWidth),
            2 => Ok(Self::OffsetCanonical),
            3 => Ok(Self::Dictionary),
            4 => Ok(Self::BooleanBitmap),
            5 => Ok(Self::CheckedDelta),
            _ => Err(SegmentV2Error::Corrupt("unknown encoding tag")),
        }
    }
}

/// Safe failures for the independent V2 codec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SegmentV2Error {
    /// A fixed implementation ceiling was exceeded.
    BoundExceeded(&'static str),
    /// Logical construction was invalid.
    Invalid(&'static str),
    /// Durable bytes were malformed or noncanonical.
    Corrupt(&'static str),
    /// Complete-file or lane integrity evidence disagreed.
    ChecksumMismatch(&'static str),
}

impl fmt::Display for SegmentV2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BoundExceeded(class) => write!(formatter, "columnar V2 bound exceeded: {class}"),
            Self::Invalid(class) => write!(formatter, "invalid columnar V2 value: {class}"),
            Self::Corrupt(class) => write!(formatter, "corrupt columnar V2 segment: {class}"),
            Self::ChecksumMismatch(class) => {
                write!(formatter, "columnar V2 checksum mismatch: {class}")
            }
        }
    }
}

impl std::error::Error for SegmentV2Error {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LaneKind {
    PrimaryKey,
    EntityVersion,
    Field(FieldId),
}

#[derive(Clone, Debug)]
struct EncodedLane {
    kind: LaneKind,
    logical_type: SegmentV2LogicalType,
    encoding: PhysicalEncoding,
    statistics: LaneStatistics,
    bytes: Vec<u8>,
    checksum: [u8; 32],
    offset: usize,
}

#[derive(Clone, Debug)]
struct DirectoryEntry {
    kind: LaneKind,
    logical_type: SegmentV2LogicalType,
    encoding: PhysicalEncoding,
    offset: usize,
    length: usize,
    statistics: LaneStatistics,
    checksum: [u8; 32],
}

/// Completely validated, immutable V2 state retained for one process
/// generation. This type is deliberately crate-private and nonserializable.
#[allow(
    dead_code,
    reason = "WP-710 proves the ADR-0160 inactive validated view before WP-711 integration"
)]
#[derive(Clone, Debug)]
struct ValidatedSegmentV2 {
    _bytes: Arc<[u8]>,
    segment: SegmentV2,
    validation_passes: usize,
}

#[allow(
    dead_code,
    reason = "WP-710 proves the ADR-0160 inactive validated view before WP-711 integration"
)]
impl ValidatedSegmentV2 {
    fn open(bytes: Arc<[u8]>) -> Result<Self, SegmentV2Error> {
        let segment = SegmentV2Codec::decode(&bytes)?;
        Ok(Self {
            _bytes: bytes,
            segment,
            validation_passes: 1,
        })
    }

    fn decode_owned(&self) -> Result<SegmentV2, SegmentV2Error> {
        Ok(self.segment.clone())
    }

    fn exact_scalar_digest(&self) -> Result<[u64; 2], SegmentV2Error> {
        let mut digest = [0xcbf2_9ce4_8422_2325, 0x9e37_79b9_7f4a_7c15];
        digest_word(&mut digest, self.segment.primary_keys.len())?;
        digest_word(&mut digest, self.segment.columns.len())?;
        for row in 0..self.segment.primary_keys.len() {
            digest_bytes(&mut digest, self.segment.primary_keys[row].as_bytes())?;
            digest_word(
                &mut digest,
                usize::try_from(self.segment.entity_versions[row].get())
                    .map_err(|_| SegmentV2Error::BoundExceeded("digest entity version"))?,
            )?;
            for column in &self.segment.columns {
                digest_word(
                    &mut digest,
                    usize::try_from(column.field_id.get())
                        .map_err(|_| SegmentV2Error::BoundExceeded("digest field id"))?,
                )?;
                match &column.cells[row] {
                    SegmentV2Cell::Missing => digest_byte(&mut digest, 0),
                    SegmentV2Cell::Null => digest_byte(&mut digest, 1),
                    SegmentV2Cell::Value(value) => {
                        digest_byte(&mut digest, 2);
                        let encoded = encode_canonical_value(value)
                            .map_err(|_| SegmentV2Error::Invalid("digest canonical value"))?;
                        digest_bytes(&mut digest, &encoded)?;
                    }
                }
            }
        }
        Ok(digest)
    }

    #[cfg(test)]
    const fn validation_passes_for_test(&self) -> usize {
        self.validation_passes
    }
}

#[allow(
    dead_code,
    reason = "WP-710 scalar mechanics are inactive until WP-711 integration"
)]
fn digest_byte(digest: &mut [u64; 2], byte: u8) {
    digest[0] ^= u64::from(byte);
    digest[0] = digest[0].wrapping_mul(0x0000_0100_0000_01b3);
    digest[1] ^= digest[0].rotate_left(17).wrapping_add(u64::from(byte));
    digest[1] = digest[1].wrapping_mul(0x9e37_79b1_85eb_ca87);
}

#[allow(
    dead_code,
    reason = "WP-710 scalar mechanics are inactive until WP-711 integration"
)]
fn digest_word(digest: &mut [u64; 2], value: usize) -> Result<(), SegmentV2Error> {
    let value = u64::try_from(value).map_err(|_| SegmentV2Error::BoundExceeded("digest word"))?;
    for byte in value.to_be_bytes() {
        digest_byte(digest, byte);
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "WP-710 scalar mechanics are inactive until WP-711 integration"
)]
fn digest_bytes(digest: &mut [u64; 2], bytes: &[u8]) -> Result<(), SegmentV2Error> {
    digest_word(digest, bytes.len())?;
    for byte in bytes {
        digest_byte(digest, *byte);
    }
    Ok(())
}

/// Canonical encoder and corruption-detecting decoder for segment V2.
pub struct SegmentV2Codec;

impl SegmentV2Codec {
    /// Encodes one validated logical segment using deterministic smallest-size
    /// selection across the closed registry.
    pub fn encode(segment: &SegmentV2) -> Result<Vec<u8>, SegmentV2Error> {
        let mut lanes = encode_all_lanes(segment)?;
        let header_zero = encode_header(segment, lanes.len(), 0)?;
        let directory_zero = encode_directory(&lanes)?;
        let mut next_offset = checked_add(header_zero.len(), directory_zero.len(), "directory")?;
        for lane in &mut lanes {
            lane.offset = next_offset;
            next_offset = checked_add(next_offset, lane.bytes.len(), "lane offsets")?;
        }
        let total_length = checked_add(next_offset, COMPLETE_CHECKSUM_BYTES, "segment bytes")?;
        if total_length > MAX_SEGMENT_V2_BYTES {
            return Err(SegmentV2Error::BoundExceeded("segment bytes"));
        }
        let header = encode_header(segment, lanes.len(), total_length)?;
        let directory = encode_directory(&lanes)?;
        let mut encoded = Vec::with_capacity(total_length);
        encoded.extend_from_slice(&header);
        encoded.extend_from_slice(&directory);
        for lane in &lanes {
            encoded.extend_from_slice(&lane.bytes);
        }
        let complete_checksum = checksum_bytes(&encoded);
        encoded.extend_from_slice(&complete_checksum);
        if encoded.len() != total_length {
            return Err(SegmentV2Error::Invalid("complete length accounting"));
        }
        Ok(encoded)
    }

    /// Decodes and independently validates every framing, bound, canonical
    /// representation, statistic, lane checksum, and complete checksum.
    pub fn decode(bytes: &[u8]) -> Result<SegmentV2, SegmentV2Error> {
        Self::decode_with_pruning(bytes).map(|(segment, _)| segment)
    }

    pub(crate) fn decode_with_pruning(
        bytes: &[u8],
    ) -> Result<(SegmentV2, SegmentV2PruningIndex), SegmentV2Error> {
        if bytes.len() > MAX_SEGMENT_V2_BYTES {
            return Err(SegmentV2Error::BoundExceeded("segment bytes"));
        }
        if bytes.len() < COMPLETE_CHECKSUM_BYTES {
            return Err(SegmentV2Error::Corrupt("truncated complete checksum"));
        }
        let body_len = bytes.len() - COMPLETE_CHECKSUM_BYTES;
        let (body, checksum) = bytes.split_at(body_len);
        if checksum != checksum_bytes(body) {
            return Err(SegmentV2Error::ChecksumMismatch("complete file"));
        }
        decode_body_fused(body, bytes.len()).map(|(segment, _audit, pruning)| (segment, pruning))
    }
}

#[derive(Default)]
struct FusedDecodeAudit {
    #[cfg(test)]
    final_field_vectors: usize,
    #[cfg(test)]
    intermediate_value_vectors: usize,
    #[cfg(test)]
    generic_system_cell_vectors: usize,
    #[cfg(test)]
    statistics_passes: usize,
    #[cfg(test)]
    pruning_dictionary_entries_from_lane: usize,
    #[cfg(test)]
    pruning_post_decode_row_visits: usize,
    #[cfg(test)]
    pruning_post_decode_value_encodes: usize,
    #[cfg(test)]
    primary_key_statistic_endpoints_retained: usize,
    #[cfg(test)]
    primary_key_statistic_per_row_clones: usize,
}

impl FusedDecodeAudit {
    fn record_final_field_vector(&mut self) {
        #[cfg(test)]
        {
            self.final_field_vectors += 1;
        }
    }

    fn record_statistics_pass(&mut self) {
        #[cfg(test)]
        {
            self.statistics_passes += 1;
        }
    }

    fn record_pruning_dictionary_entries(&mut self, entries: usize) {
        #[cfg(test)]
        {
            self.pruning_dictionary_entries_from_lane += entries;
        }
        #[cfg(not(test))]
        let _ = entries;
    }

    fn record_primary_key_statistic_endpoints(&mut self) {
        #[cfg(test)]
        {
            self.primary_key_statistic_endpoints_retained = 2;
        }
    }
}

#[cfg(test)]
fn decode_staged_for_test(bytes: &[u8]) -> Result<SegmentV2, SegmentV2Error> {
    let (body, _) = validated_complete_body(bytes)?;
    decode_body(body, bytes.len())
}

#[cfg(test)]
fn decode_fused_for_test(bytes: &[u8]) -> Result<(SegmentV2, FusedDecodeAudit), SegmentV2Error> {
    let (body, _) = validated_complete_body(bytes)?;
    decode_body_fused(body, bytes.len()).map(|(segment, audit, _)| (segment, audit))
}

#[cfg(test)]
fn validated_complete_body(bytes: &[u8]) -> Result<(&[u8], &[u8]), SegmentV2Error> {
    if bytes.len() > MAX_SEGMENT_V2_BYTES {
        return Err(SegmentV2Error::BoundExceeded("segment bytes"));
    }
    if bytes.len() < COMPLETE_CHECKSUM_BYTES {
        return Err(SegmentV2Error::Corrupt("truncated complete checksum"));
    }
    let body_len = bytes.len() - COMPLETE_CHECKSUM_BYTES;
    let (body, checksum) = bytes.split_at(body_len);
    if checksum != checksum_bytes(body) {
        return Err(SegmentV2Error::ChecksumMismatch("complete file"));
    }
    Ok((body, checksum))
}

fn encode_all_lanes(segment: &SegmentV2) -> Result<Vec<EncodedLane>, SegmentV2Error> {
    let key_cells = segment
        .primary_keys
        .iter()
        .map(|key| {
            CanonicalValue::bytes(key.as_bytes().to_vec())
                .map(SegmentV2Cell::Value)
                .map_err(|_| SegmentV2Error::BoundExceeded("primary key bytes"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let version_cells = segment
        .entity_versions
        .iter()
        .map(|version| SegmentV2Cell::Value(CanonicalValue::U64(version.get())))
        .collect::<Vec<_>>();
    let mut lanes = Vec::with_capacity(segment.columns.len() + 2);
    lanes.push(encode_lane(
        LaneKind::PrimaryKey,
        SegmentV2LogicalType::Bytes,
        &key_cells,
        Some(PhysicalEncoding::OffsetCanonical),
    )?);
    lanes.push(encode_lane(
        LaneKind::EntityVersion,
        SegmentV2LogicalType::U64,
        &version_cells,
        None,
    )?);
    for column in &segment.columns {
        lanes.push(encode_lane(
            LaneKind::Field(column.field_id),
            column.logical_type.clone(),
            &column.cells,
            None,
        )?);
    }
    Ok(lanes)
}

fn encode_lane(
    kind: LaneKind,
    logical_type: SegmentV2LogicalType,
    cells: &[SegmentV2Cell],
    forced: Option<PhysicalEncoding>,
) -> Result<EncodedLane, SegmentV2Error> {
    let statistics = compute_statistics(&logical_type, cells)?;
    let bitmap_len = bitmap_len(cells.len())?;
    let mut present = vec![0u8; bitmap_len];
    let mut null = vec![0u8; bitmap_len];
    let mut values = Vec::with_capacity(statistics.value_count);
    for (index, cell) in cells.iter().enumerate() {
        match cell {
            SegmentV2Cell::Missing => {}
            SegmentV2Cell::Null => set_bit(&mut null, index),
            SegmentV2Cell::Value(value) => {
                set_bit(&mut present, index);
                values.push(value.clone());
            }
        }
    }
    let (encoding, payload) = if let Some(encoding) = forced {
        (
            encoding,
            encode_with(encoding, &logical_type, cells, &values)?,
        )
    } else {
        select_encoding(&logical_type, cells, &values)?
    };
    let mut bytes = Vec::new();
    put_u32(&mut bytes, cells.len(), "lane rows")?;
    put_u32(&mut bytes, present.len(), "validity bytes")?;
    bytes.extend_from_slice(&present);
    bytes.extend_from_slice(&null);
    bytes.extend_from_slice(&payload);
    if bytes.len() > MAX_LANE_BYTES {
        return Err(SegmentV2Error::BoundExceeded("lane bytes"));
    }
    let checksum = checksum_bytes(&bytes);
    Ok(EncodedLane {
        kind,
        logical_type,
        encoding,
        statistics,
        bytes,
        checksum,
        offset: 0,
    })
}

fn select_encoding(
    logical_type: &SegmentV2LogicalType,
    cells: &[SegmentV2Cell],
    values: &[CanonicalValue],
) -> Result<(PhysicalEncoding, Vec<u8>), SegmentV2Error> {
    let mut candidates = Vec::new();
    candidates.push((
        PhysicalEncoding::OffsetCanonical,
        encode_offset_values(values)?,
    ));
    if is_fixed_width_type(logical_type) {
        candidates.push((PhysicalEncoding::FixedWidth, encode_fixed_values(values)?));
    }
    if matches!(
        logical_type,
        SegmentV2LogicalType::String | SegmentV2LogicalType::Enum(_)
    ) {
        candidates.push((
            PhysicalEncoding::Dictionary,
            encode_dictionary_values(values)?,
        ));
    }
    if matches!(logical_type, SegmentV2LogicalType::Bool) {
        candidates.push((
            PhysicalEncoding::BooleanBitmap,
            encode_boolean_values(cells)?,
        ));
    }
    if is_delta_type(logical_type) {
        candidates.push((
            PhysicalEncoding::CheckedDelta,
            encode_delta_values(logical_type, values)?,
        ));
    }
    candidates.sort_by_key(|(encoding, payload)| (payload.len(), *encoding));
    candidates
        .into_iter()
        .next()
        .ok_or(SegmentV2Error::Invalid("encoding registry is empty"))
}

fn encode_with(
    encoding: PhysicalEncoding,
    logical_type: &SegmentV2LogicalType,
    cells: &[SegmentV2Cell],
    values: &[CanonicalValue],
) -> Result<Vec<u8>, SegmentV2Error> {
    match encoding {
        PhysicalEncoding::FixedWidth => encode_fixed_values(values),
        PhysicalEncoding::OffsetCanonical => encode_offset_values(values),
        PhysicalEncoding::Dictionary => encode_dictionary_values(values),
        PhysicalEncoding::BooleanBitmap => encode_boolean_values(cells),
        PhysicalEncoding::CheckedDelta => encode_delta_values(logical_type, values),
    }
}

fn encode_fixed_values(values: &[CanonicalValue]) -> Result<Vec<u8>, SegmentV2Error> {
    let encoded = encode_values(values)?;
    let width = encoded.first().map_or(0, Vec::len);
    if encoded.iter().any(|value| value.len() != width) {
        return Err(SegmentV2Error::Invalid("fixed-width value size"));
    }
    let mut out = Vec::new();
    put_u32(&mut out, width, "fixed width")?;
    for value in encoded {
        out.extend_from_slice(&value);
    }
    Ok(out)
}

fn encode_offset_values(values: &[CanonicalValue]) -> Result<Vec<u8>, SegmentV2Error> {
    let encoded = encode_values(values)?;
    let mut offset = 0usize;
    let mut out = Vec::new();
    put_u32(&mut out, 0, "offset")?;
    for value in &encoded {
        offset = checked_add(offset, value.len(), "offset payload")?;
        put_u32(&mut out, offset, "offset")?;
    }
    for value in encoded {
        out.extend_from_slice(&value);
    }
    Ok(out)
}

fn encode_dictionary_values(values: &[CanonicalValue]) -> Result<Vec<u8>, SegmentV2Error> {
    let encoded = encode_values(values)?;
    let dictionary = encoded.iter().cloned().collect::<BTreeSet<_>>();
    let dictionary = dictionary.into_iter().collect::<Vec<_>>();
    let ordinals = dictionary
        .iter()
        .enumerate()
        .map(|(index, value)| (value.clone(), index))
        .collect::<BTreeMap<_, _>>();
    let bit_width = ordinal_bit_width(dictionary.len())?;
    let mut out = Vec::new();
    put_u32(&mut out, dictionary.len(), "dictionary entries")?;
    for value in &dictionary {
        put_bytes(&mut out, value, "dictionary value")?;
    }
    out.push(bit_width);
    put_u32(&mut out, encoded.len(), "dictionary ordinals")?;
    let mut packed = vec![0u8; packed_len(encoded.len(), bit_width)?];
    for (position, value) in encoded.iter().enumerate() {
        let ordinal = *ordinals
            .get(value)
            .ok_or(SegmentV2Error::Invalid("dictionary ordinal"))?;
        pack_ordinal(&mut packed, position, bit_width, ordinal)?;
    }
    out.extend_from_slice(&packed);
    Ok(out)
}

fn encode_boolean_values(cells: &[SegmentV2Cell]) -> Result<Vec<u8>, SegmentV2Error> {
    let mut bitmap = vec![0u8; bitmap_len(cells.len())?];
    for (index, cell) in cells.iter().enumerate() {
        if matches!(cell, SegmentV2Cell::Value(CanonicalValue::Bool(true))) {
            set_bit(&mut bitmap, index);
        }
    }
    Ok(bitmap)
}

fn encode_delta_values(
    logical_type: &SegmentV2LogicalType,
    values: &[CanonicalValue],
) -> Result<Vec<u8>, SegmentV2Error> {
    let scalars = values
        .iter()
        .map(|value| scalar_value(logical_type, value))
        .collect::<Result<Vec<_>, _>>()?;
    let mut out = Vec::new();
    if let Some(first) = scalars.first() {
        out.extend_from_slice(&first.to_be_bytes());
        let mut previous = *first;
        for current in scalars.iter().skip(1) {
            let delta = current
                .checked_sub(previous)
                .ok_or(SegmentV2Error::Invalid("delta arithmetic"))?;
            put_var_u128(&mut out, zigzag_encode(delta));
            previous = *current;
        }
    }
    Ok(out)
}

fn encode_values(values: &[CanonicalValue]) -> Result<Vec<Vec<u8>>, SegmentV2Error> {
    values
        .iter()
        .map(|value| {
            let encoded = encode_canonical_value(value)
                .map_err(|_| SegmentV2Error::Invalid("canonical value encoding"))?;
            if encoded.len() > MAX_SCALAR_BYTES {
                return Err(SegmentV2Error::BoundExceeded("canonical value bytes"));
            }
            Ok(encoded)
        })
        .collect()
}

fn encode_header(
    segment: &SegmentV2,
    directory_count: usize,
    complete_length: usize,
) -> Result<Vec<u8>, SegmentV2Error> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&COLUMNAR_SEGMENT_FORMAT_VERSION_V2.to_be_bytes());
    out.extend_from_slice(&COLUMNAR_ENCODING_REGISTRY_VERSION_V1.to_be_bytes());
    put_u64(&mut out, complete_length, "complete length")?;
    out.extend_from_slice(segment.identity.definition_fingerprint.as_bytes());
    out.extend_from_slice(&segment.identity.history_incarnation.to_be_bytes());
    out.extend_from_slice(&segment.identity.generation.to_be_bytes());
    put_bytes(
        &mut out,
        segment.identity.organization.as_bytes(),
        "organization key",
    )?;
    out.extend_from_slice(segment.identity.segment_id.as_bytes());
    encode_frontier(&mut out, segment.identity.frontier_start);
    encode_frontier(&mut out, segment.identity.frontier_end);
    put_u32(&mut out, segment.primary_keys.len(), "row count")?;
    put_u32(&mut out, segment.columns.len(), "column count")?;
    put_u32(&mut out, directory_count, "directory count")?;
    Ok(out)
}

fn encode_directory(lanes: &[EncodedLane]) -> Result<Vec<u8>, SegmentV2Error> {
    let mut out = Vec::new();
    for lane in lanes {
        encode_lane_kind(&mut out, &lane.kind);
        encode_logical_type(&mut out, &lane.logical_type);
        out.push(lane.encoding as u8);
        put_u64(&mut out, lane.offset, "lane offset")?;
        put_u64(&mut out, lane.bytes.len(), "lane length")?;
        put_u32(&mut out, lane.statistics.value_count, "lane value count")?;
        put_u32(
            &mut out,
            lane.statistics.missing_count,
            "lane missing count",
        )?;
        put_u32(&mut out, lane.statistics.null_count, "lane null count")?;
        encode_optional_value(&mut out, lane.statistics.minimum.as_ref())?;
        encode_optional_value(&mut out, lane.statistics.maximum.as_ref())?;
        out.extend_from_slice(&lane.checksum);
    }
    Ok(out)
}

struct FusedLaneFrame<'a> {
    present: &'a [u8],
    null: &'a [u8],
    payload: &'a [u8],
}

impl<'a> FusedLaneFrame<'a> {
    fn parse(
        entry: &DirectoryEntry,
        bytes: &'a [u8],
        expected_rows: usize,
    ) -> Result<Self, SegmentV2Error> {
        let mut reader = Reader::new(bytes);
        let row_count = reader.read_count(MAX_SEGMENT_V2_ROWS, "lane rows")?;
        if row_count != expected_rows {
            return Err(SegmentV2Error::Corrupt("lane row count"));
        }
        let expected_validity = bitmap_len(row_count)?;
        let validity_len = reader.read_count(expected_validity, "validity bytes")?;
        if validity_len != expected_validity {
            return Err(SegmentV2Error::Corrupt("validity bitmap length"));
        }
        let present = reader.read_exact(validity_len)?;
        let null = reader.read_exact(validity_len)?;
        validate_padding_bits(present, row_count)?;
        validate_padding_bits(null, row_count)?;
        if present
            .iter()
            .zip(null)
            .any(|(present, null)| present & null != 0)
        {
            return Err(SegmentV2Error::Corrupt("overlapping validity states"));
        }
        let observed_values = count_bits(present, row_count);
        let observed_nulls = count_bits(null, row_count);
        let observed_missing = row_count
            .checked_sub(observed_values)
            .and_then(|count| count.checked_sub(observed_nulls))
            .ok_or(SegmentV2Error::Corrupt("validity counts"))?;
        if observed_values != entry.statistics.value_count
            || observed_nulls != entry.statistics.null_count
            || observed_missing != entry.statistics.missing_count
        {
            return Err(SegmentV2Error::Corrupt("validity statistics"));
        }
        Ok(Self {
            present,
            null,
            payload: reader.remaining(),
        })
    }
}

enum FusedValueCursor<'a> {
    Fixed {
        values: &'a [u8],
        width: usize,
        position: usize,
        count: usize,
        logical_type: SegmentV2LogicalType,
    },
    Offset {
        offsets: &'a [u8],
        values: &'a [u8],
        position: usize,
        count: usize,
        logical_type: SegmentV2LogicalType,
    },
    Dictionary {
        dictionary: Vec<CanonicalValue>,
        encoded_dictionary: Vec<Vec<u8>>,
        packed: &'a [u8],
        bit_width: u8,
        position: usize,
        count: usize,
    },
    Boolean {
        bitmap: &'a [u8],
        position: usize,
        count: usize,
    },
    Delta {
        reader: Reader<'a>,
        scalar: Option<i128>,
        position: usize,
        count: usize,
        logical_type: SegmentV2LogicalType,
    },
}

impl<'a> FusedValueCursor<'a> {
    fn new(
        entry: &DirectoryEntry,
        frame: &FusedLaneFrame<'a>,
        row_count: usize,
    ) -> Result<Self, SegmentV2Error> {
        let value_count = entry.statistics.value_count;
        match entry.encoding {
            PhysicalEncoding::FixedWidth => {
                let mut reader = Reader::new(frame.payload);
                let width = reader.read_count(MAX_SCALAR_BYTES, "fixed width")?;
                if value_count > 0 && width == 0 {
                    return Err(SegmentV2Error::Corrupt("zero fixed width"));
                }
                let expected = width
                    .checked_mul(value_count)
                    .ok_or(SegmentV2Error::Corrupt("fixed-width overflow"))?;
                if reader.remaining().len() != expected {
                    return Err(SegmentV2Error::Corrupt("fixed-width payload length"));
                }
                Ok(Self::Fixed {
                    values: reader.remaining(),
                    width,
                    position: 0,
                    count: value_count,
                    logical_type: entry.logical_type.clone(),
                })
            }
            PhysicalEncoding::OffsetCanonical => {
                let offset_count = value_count
                    .checked_add(1)
                    .ok_or(SegmentV2Error::Corrupt("offset count"))?;
                let offset_bytes = offset_count
                    .checked_mul(4)
                    .ok_or(SegmentV2Error::Corrupt("offset bytes"))?;
                let (offsets, values) = frame
                    .payload
                    .split_at_checked(offset_bytes)
                    .ok_or(SegmentV2Error::Corrupt("truncated offsets"))?;
                let mut previous = fused_offset_at(offsets, 0)?;
                if previous != 0 {
                    return Err(SegmentV2Error::Corrupt("noncanonical offsets"));
                }
                for index in 1..offset_count {
                    let current = fused_offset_at(offsets, index)?;
                    if current < previous || current > values.len() {
                        return Err(SegmentV2Error::Corrupt("noncanonical offsets"));
                    }
                    previous = current;
                }
                if previous != values.len() {
                    return Err(SegmentV2Error::Corrupt("noncanonical offsets"));
                }
                Ok(Self::Offset {
                    offsets,
                    values,
                    position: 0,
                    count: value_count,
                    logical_type: entry.logical_type.clone(),
                })
            }
            PhysicalEncoding::Dictionary => {
                let mut reader = Reader::new(frame.payload);
                let dictionary_count =
                    reader.read_count(value_count.max(1), "dictionary entries")?;
                if value_count > 0 && dictionary_count == 0 {
                    return Err(SegmentV2Error::Corrupt("empty dictionary"));
                }
                let mut dictionary = Vec::with_capacity(dictionary_count);
                let mut encoded_dictionary = Vec::with_capacity(dictionary_count);
                let mut previous_encoded: Option<&[u8]> = None;
                for _ in 0..dictionary_count {
                    let length = reader.read_count(MAX_SCALAR_BYTES, "dictionary value")?;
                    let encoded = reader.read_exact(length)?;
                    if previous_encoded.is_some_and(|previous| previous >= encoded) {
                        return Err(SegmentV2Error::Corrupt("noncanonical dictionary order"));
                    }
                    dictionary.push(decode_typed_value(encoded, &entry.logical_type)?);
                    encoded_dictionary.push(encoded.to_vec());
                    previous_encoded = Some(encoded);
                }
                let bit_width = reader.read_u8()?;
                if bit_width != ordinal_bit_width(dictionary_count)? {
                    return Err(SegmentV2Error::Corrupt("dictionary bit width"));
                }
                let ordinal_count = reader.read_count(value_count, "dictionary ordinals")?;
                if ordinal_count != value_count {
                    return Err(SegmentV2Error::Corrupt("dictionary ordinal count"));
                }
                let expected = packed_len(value_count, bit_width)?;
                if reader.remaining().len() != expected {
                    return Err(SegmentV2Error::Corrupt("dictionary packed length"));
                }
                validate_packed_padding(reader.remaining(), value_count, bit_width)?;
                Ok(Self::Dictionary {
                    dictionary,
                    encoded_dictionary,
                    packed: reader.remaining(),
                    bit_width,
                    position: 0,
                    count: value_count,
                })
            }
            PhysicalEncoding::BooleanBitmap => {
                if !matches!(entry.logical_type, SegmentV2LogicalType::Bool) {
                    return Err(SegmentV2Error::Corrupt("boolean encoding type"));
                }
                if frame.payload.len() != bitmap_len(row_count)? {
                    return Err(SegmentV2Error::Corrupt("boolean bitmap length"));
                }
                validate_padding_bits(frame.payload, row_count)?;
                if (0..row_count)
                    .any(|row| !bit_is_set(frame.present, row) && bit_is_set(frame.payload, row))
                {
                    return Err(SegmentV2Error::Corrupt("boolean value on absent row"));
                }
                Ok(Self::Boolean {
                    bitmap: frame.payload,
                    position: 0,
                    count: value_count,
                })
            }
            PhysicalEncoding::CheckedDelta => {
                if !is_delta_type(&entry.logical_type) {
                    return Err(SegmentV2Error::Corrupt("delta encoding type"));
                }
                let mut reader = Reader::new(frame.payload);
                let scalar = if value_count == 0 {
                    if !reader.remaining().is_empty() {
                        return Err(SegmentV2Error::Corrupt("delta bytes for empty lane"));
                    }
                    None
                } else {
                    let mut scalar_bytes = [0u8; 16];
                    scalar_bytes.copy_from_slice(reader.read_exact(16)?);
                    Some(i128::from_be_bytes(scalar_bytes))
                };
                Ok(Self::Delta {
                    reader,
                    scalar,
                    position: 0,
                    count: value_count,
                    logical_type: entry.logical_type.clone(),
                })
            }
        }
    }

    fn next_value(&mut self, row: usize) -> Result<CanonicalValue, SegmentV2Error> {
        match self {
            Self::Fixed {
                values,
                width,
                position,
                count,
                logical_type,
            } => {
                if *position >= *count {
                    return Err(SegmentV2Error::Corrupt("extra present value"));
                }
                let start = position
                    .checked_mul(*width)
                    .ok_or(SegmentV2Error::Corrupt("fixed-width position"))?;
                let end = start
                    .checked_add(*width)
                    .ok_or(SegmentV2Error::Corrupt("fixed-width position"))?;
                *position += 1;
                decode_typed_value(
                    values
                        .get(start..end)
                        .ok_or(SegmentV2Error::Corrupt("fixed-width value"))?,
                    logical_type,
                )
            }
            Self::Offset {
                offsets,
                values,
                position,
                count,
                logical_type,
            } => {
                if *position >= *count {
                    return Err(SegmentV2Error::Corrupt("extra present value"));
                }
                let start = fused_offset_at(offsets, *position)?;
                let end = fused_offset_at(offsets, *position + 1)?;
                *position += 1;
                decode_typed_value(
                    values
                        .get(start..end)
                        .ok_or(SegmentV2Error::Corrupt("offset value"))?,
                    logical_type,
                )
            }
            Self::Dictionary {
                dictionary,
                packed,
                bit_width,
                position,
                count,
                ..
            } => {
                if *position >= *count {
                    return Err(SegmentV2Error::Corrupt("extra present value"));
                }
                let ordinal = unpack_ordinal(packed, *position, *bit_width)?;
                *position += 1;
                dictionary
                    .get(ordinal)
                    .cloned()
                    .ok_or(SegmentV2Error::Corrupt("dictionary ordinal range"))
            }
            Self::Boolean {
                bitmap,
                position,
                count,
            } => {
                if *position >= *count {
                    return Err(SegmentV2Error::Corrupt("extra present value"));
                }
                *position += 1;
                Ok(CanonicalValue::Bool(bit_is_set(bitmap, row)))
            }
            Self::Delta {
                reader,
                scalar,
                position,
                count,
                logical_type,
            } => {
                if *position >= *count {
                    return Err(SegmentV2Error::Corrupt("extra present value"));
                }
                if *position > 0 {
                    let delta = zigzag_decode(reader.read_var_u128()?);
                    *scalar = Some(
                        scalar
                            .ok_or(SegmentV2Error::Corrupt("missing delta base"))?
                            .checked_add(delta)
                            .ok_or(SegmentV2Error::Corrupt("delta reconstruction overflow"))?,
                    );
                }
                *position += 1;
                value_from_scalar(
                    logical_type,
                    scalar.ok_or(SegmentV2Error::Corrupt("missing delta value"))?,
                )
            }
        }
    }

    fn finish(self) -> Result<Option<Vec<Vec<u8>>>, SegmentV2Error> {
        let (position, count, trailing, dictionary) = match self {
            Self::Fixed {
                position, count, ..
            }
            | Self::Offset {
                position, count, ..
            }
            | Self::Boolean {
                position, count, ..
            } => (position, count, false, None),
            Self::Dictionary {
                position,
                count,
                encoded_dictionary,
                ..
            } => (position, count, false, Some(encoded_dictionary)),
            Self::Delta {
                reader,
                position,
                count,
                ..
            } => (position, count, !reader.remaining().is_empty(), None),
        };
        if position != count {
            return Err(SegmentV2Error::Corrupt("value cardinality"));
        }
        if trailing {
            return Err(SegmentV2Error::Corrupt("trailing delta bytes"));
        }
        Ok(dictionary)
    }
}

#[derive(Default)]
struct ObservedLaneStatistics {
    value_count: usize,
    minimum: Option<CanonicalValue>,
    maximum: Option<CanonicalValue>,
}

struct DecodedFieldLane {
    column: SegmentV2Column,
    dictionary: Option<Vec<Vec<u8>>>,
}

impl ObservedLaneStatistics {
    fn observe(
        &mut self,
        logical_type: &SegmentV2LogicalType,
        value: &CanonicalValue,
    ) -> Result<(), SegmentV2Error> {
        if !logical_type.accepts(value) {
            return Err(SegmentV2Error::Corrupt("logical type mismatch"));
        }
        self.value_count += 1;
        if self.minimum.as_ref().is_none_or(|minimum| {
            compare_values(logical_type, value, minimum) == Ok(Ordering::Less)
        }) {
            self.minimum = Some(value.clone());
        }
        if self.maximum.as_ref().is_none_or(|maximum| {
            compare_values(logical_type, value, maximum) == Ok(Ordering::Greater)
        }) {
            self.maximum = Some(value.clone());
        }
        Ok(())
    }

    fn verify(
        self,
        entry: &DirectoryEntry,
        missing_count: usize,
        null_count: usize,
    ) -> Result<(), SegmentV2Error> {
        let observed = LaneStatistics {
            value_count: self.value_count,
            missing_count,
            null_count,
            minimum: self.minimum,
            maximum: self.maximum,
        };
        if observed != entry.statistics {
            return Err(SegmentV2Error::Corrupt("lane statistics"));
        }
        Ok(())
    }
}

fn fused_offset_at(offsets: &[u8], index: usize) -> Result<usize, SegmentV2Error> {
    let start = index
        .checked_mul(4)
        .ok_or(SegmentV2Error::Corrupt("offset position"))?;
    let end = start
        .checked_add(4)
        .ok_or(SegmentV2Error::Corrupt("offset position"))?;
    let bytes: [u8; 4] = offsets
        .get(start..end)
        .ok_or(SegmentV2Error::Corrupt("offset position"))?
        .try_into()
        .map_err(|_| SegmentV2Error::Corrupt("offset bytes"))?;
    usize::try_from(u32::from_be_bytes(bytes)).map_err(|_| SegmentV2Error::Corrupt("offset range"))
}

fn decode_fused_field_lane(
    entry: &DirectoryEntry,
    bytes: &[u8],
    row_count: usize,
    audit: &mut FusedDecodeAudit,
) -> Result<DecodedFieldLane, SegmentV2Error> {
    let field_id = match entry.kind {
        LaneKind::Field(field_id) => field_id,
        LaneKind::PrimaryKey | LaneKind::EntityVersion => {
            return Err(SegmentV2Error::Corrupt("system lane as field"));
        }
    };
    let frame = FusedLaneFrame::parse(entry, bytes, row_count)?;
    let mut values = FusedValueCursor::new(entry, &frame, row_count)?;
    let mut statistics = ObservedLaneStatistics::default();
    let mut cells = Vec::with_capacity(row_count);
    audit.record_final_field_vector();
    for row in 0..row_count {
        if bit_is_set(frame.present, row) {
            let value = values.next_value(row)?;
            statistics.observe(&entry.logical_type, &value)?;
            cells.push(SegmentV2Cell::Value(value));
        } else if bit_is_set(frame.null, row) {
            cells.push(SegmentV2Cell::Null);
        } else {
            cells.push(SegmentV2Cell::Missing);
        }
    }
    let dictionary = values.finish()?;
    statistics.verify(
        entry,
        entry.statistics.missing_count,
        entry.statistics.null_count,
    )?;
    audit.record_statistics_pass();
    Ok(DecodedFieldLane {
        column: SegmentV2Column {
            field_id,
            logical_type: entry.logical_type.clone(),
            cells,
            statistics: entry.statistics.clone(),
        },
        dictionary,
    })
}

fn decode_fused_primary_keys(
    entry: &DirectoryEntry,
    bytes: &[u8],
    row_count: usize,
    audit: &mut FusedDecodeAudit,
) -> Result<Vec<PrimaryKeyBytes>, SegmentV2Error> {
    let frame = FusedLaneFrame::parse(entry, bytes, row_count)?;
    if entry.statistics.missing_count != 0 || entry.statistics.null_count != 0 {
        return Err(SegmentV2Error::Corrupt("primary key lane state"));
    }
    let mut values = FusedValueCursor::new(entry, &frame, row_count)?;
    let mut keys = Vec::with_capacity(row_count);
    for row in 0..row_count {
        if !bit_is_set(frame.present, row) {
            return Err(SegmentV2Error::Corrupt("primary key lane state"));
        }
        let value = values.next_value(row)?;
        match value {
            CanonicalValue::Bytes(value) => {
                keys.push(PrimaryKeyBytes::from_entity_key_bytes(value.into_vec()));
            }
            _ => return Err(SegmentV2Error::Corrupt("primary key lane type")),
        }
    }
    let _ = values.finish()?;
    verify_sorted_primary_key_statistics(entry, &keys)?;
    audit.record_primary_key_statistic_endpoints();
    audit.record_statistics_pass();
    Ok(keys)
}

fn verify_sorted_primary_key_statistics(
    entry: &DirectoryEntry,
    keys: &[PrimaryKeyBytes],
) -> Result<(), SegmentV2Error> {
    let first = keys
        .first()
        .ok_or(SegmentV2Error::Corrupt("empty primary key lane"))?;
    let last = keys
        .last()
        .ok_or(SegmentV2Error::Corrupt("empty primary key lane"))?;
    let minimum_matches = matches!(
        &entry.statistics.minimum,
        Some(CanonicalValue::Bytes(value)) if value.as_bytes() == first.as_bytes()
    );
    let maximum_matches = matches!(
        &entry.statistics.maximum,
        Some(CanonicalValue::Bytes(value)) if value.as_bytes() == last.as_bytes()
    );
    if entry.statistics.value_count != keys.len()
        || entry.statistics.missing_count != 0
        || entry.statistics.null_count != 0
        || !minimum_matches
        || !maximum_matches
    {
        return Err(SegmentV2Error::Corrupt("lane statistics"));
    }
    Ok(())
}

fn decode_fused_entity_versions(
    entry: &DirectoryEntry,
    bytes: &[u8],
    row_count: usize,
    audit: &mut FusedDecodeAudit,
) -> Result<Vec<EntityVersion>, SegmentV2Error> {
    let frame = FusedLaneFrame::parse(entry, bytes, row_count)?;
    if entry.statistics.missing_count != 0 || entry.statistics.null_count != 0 {
        return Err(SegmentV2Error::Corrupt("entity version lane state"));
    }
    let mut values = FusedValueCursor::new(entry, &frame, row_count)?;
    let mut statistics = ObservedLaneStatistics::default();
    let mut versions = Vec::with_capacity(row_count);
    for row in 0..row_count {
        if !bit_is_set(frame.present, row) {
            return Err(SegmentV2Error::Corrupt("entity version lane state"));
        }
        let value = values.next_value(row)?;
        statistics.observe(&entry.logical_type, &value)?;
        match value {
            CanonicalValue::U64(value) => versions.push(
                EntityVersion::new(value).ok_or(SegmentV2Error::Corrupt("entity version value"))?,
            ),
            _ => return Err(SegmentV2Error::Corrupt("entity version lane type")),
        }
    }
    let _ = values.finish()?;
    statistics.verify(entry, 0, 0)?;
    audit.record_statistics_pass();
    Ok(versions)
}

fn decode_body_fused(
    body: &[u8],
    complete_length: usize,
) -> Result<(SegmentV2, FusedDecodeAudit, SegmentV2PruningIndex), SegmentV2Error> {
    let mut reader = Reader::new(body);
    if reader.read_exact(MAGIC.len())? != MAGIC {
        return Err(SegmentV2Error::Corrupt("bad magic"));
    }
    if reader.read_u16()? != COLUMNAR_SEGMENT_FORMAT_VERSION_V2 {
        return Err(SegmentV2Error::Corrupt("segment format version"));
    }
    if reader.read_u16()? != COLUMNAR_ENCODING_REGISTRY_VERSION_V1 {
        return Err(SegmentV2Error::Corrupt("encoding registry version"));
    }
    if reader.read_usize()? != complete_length {
        return Err(SegmentV2Error::Corrupt("complete file length"));
    }
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(reader.read_exact(32)?);
    let history_incarnation = reader.read_u64()?;
    let generation = ProjectionGeneration::new(reader.read_u64()?)
        .ok_or(SegmentV2Error::Corrupt("projection generation"))?;
    let organization = OrgKey::from_encoded_bytes(reader.read_bytes(MAX_ORG_KEY_BYTES)?);
    let mut segment_id = [0u8; 16];
    segment_id.copy_from_slice(reader.read_exact(16)?);
    let frontier_start = reader.read_frontier()?;
    let frontier_end = reader.read_frontier()?;
    let row_count = reader.read_count(MAX_SEGMENT_V2_ROWS, "row count")?;
    if row_count == 0 {
        return Err(SegmentV2Error::Corrupt("zero row count"));
    }
    let column_count = reader.read_count(MAX_SEGMENT_V2_COLUMNS, "column count")?;
    if column_count == 0 {
        return Err(SegmentV2Error::Corrupt("zero column count"));
    }
    let expected_directory_count = checked_add(column_count, 2, "directory count")?;
    let directory_count = reader.read_count(MAX_SEGMENT_V2_COLUMNS + 2, "directory count")?;
    if directory_count != expected_directory_count {
        return Err(SegmentV2Error::Corrupt("directory count"));
    }
    let mut directory = Vec::with_capacity(directory_count);
    for _ in 0..directory_count {
        directory.push(decode_directory_entry(&mut reader, row_count)?);
    }
    validate_directory_order(&directory)?;
    let mut expected_offset = reader.position();
    for entry in &directory {
        if entry.offset != expected_offset {
            return Err(SegmentV2Error::Corrupt("lane gap or overlap"));
        }
        expected_offset = checked_add(expected_offset, entry.length, "lane extent")?;
        if expected_offset > body.len() {
            return Err(SegmentV2Error::Corrupt("lane outside file"));
        }
    }
    if expected_offset != body.len() {
        return Err(SegmentV2Error::Corrupt("trailing or missing lane bytes"));
    }

    let lane_bytes = |entry: &DirectoryEntry| -> Result<&[u8], SegmentV2Error> {
        let end = checked_add(entry.offset, entry.length, "lane slice")?;
        let bytes = body
            .get(entry.offset..end)
            .ok_or(SegmentV2Error::Corrupt("lane slice"))?;
        if checksum_bytes(bytes) != entry.checksum {
            return Err(SegmentV2Error::ChecksumMismatch("lane"));
        }
        Ok(bytes)
    };

    let mut audit = FusedDecodeAudit::default();
    let primary_entry = directory
        .first()
        .ok_or(SegmentV2Error::Corrupt("missing primary key lane"))?;
    let version_entry = directory
        .get(1)
        .ok_or(SegmentV2Error::Corrupt("missing entity-version lane"))?;
    let primary_keys = decode_fused_primary_keys(
        primary_entry,
        lane_bytes(primary_entry)?,
        row_count,
        &mut audit,
    )?;
    let entity_versions = decode_fused_entity_versions(
        version_entry,
        lane_bytes(version_entry)?,
        row_count,
        &mut audit,
    )?;
    let decoded_columns = directory
        .iter()
        .skip(2)
        .map(|entry| decode_fused_field_lane(entry, lane_bytes(entry)?, row_count, &mut audit))
        .collect::<Result<Vec<_>, _>>()?;
    let mut columns = Vec::with_capacity(decoded_columns.len());
    let mut dictionary_evidence = Vec::with_capacity(decoded_columns.len());
    for decoded in decoded_columns {
        columns.push(decoded.column);
        dictionary_evidence.push(decoded.dictionary);
    }
    let identity = SegmentV2Identity::new(
        DefinitionFingerprint::from_bytes(fingerprint),
        history_incarnation,
        generation,
        organization,
        SegmentV2SegmentId::from_bytes(segment_id),
        frontier_start,
        frontier_end,
    )
    .map_err(|_| SegmentV2Error::Corrupt("segment identity"))?;
    let segment = SegmentV2::new(identity, primary_keys, entity_versions, columns)
        .map_err(|_| SegmentV2Error::Corrupt("logical segment"))?;
    let pruning = SegmentV2PruningIndex::from_validated(
        &segment,
        &directory,
        dictionary_evidence,
        &mut audit,
    )?;
    Ok((segment, audit, pruning))
}

#[cfg(test)]
fn decode_body(body: &[u8], complete_length: usize) -> Result<SegmentV2, SegmentV2Error> {
    let mut reader = Reader::new(body);
    if reader.read_exact(MAGIC.len())? != MAGIC {
        return Err(SegmentV2Error::Corrupt("bad magic"));
    }
    if reader.read_u16()? != COLUMNAR_SEGMENT_FORMAT_VERSION_V2 {
        return Err(SegmentV2Error::Corrupt("segment format version"));
    }
    if reader.read_u16()? != COLUMNAR_ENCODING_REGISTRY_VERSION_V1 {
        return Err(SegmentV2Error::Corrupt("encoding registry version"));
    }
    let recorded_length = reader.read_usize()?;
    if recorded_length != complete_length {
        return Err(SegmentV2Error::Corrupt("complete file length"));
    }
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(reader.read_exact(32)?);
    let history_incarnation = reader.read_u64()?;
    let generation = ProjectionGeneration::new(reader.read_u64()?)
        .ok_or(SegmentV2Error::Corrupt("projection generation"))?;
    let organization = OrgKey::from_encoded_bytes(reader.read_bytes(MAX_ORG_KEY_BYTES)?);
    let mut segment_id = [0u8; 16];
    segment_id.copy_from_slice(reader.read_exact(16)?);
    let frontier_start = reader.read_frontier()?;
    let frontier_end = reader.read_frontier()?;
    let row_count = reader.read_count(MAX_SEGMENT_V2_ROWS, "row count")?;
    if row_count == 0 {
        return Err(SegmentV2Error::Corrupt("zero row count"));
    }
    let column_count = reader.read_count(MAX_SEGMENT_V2_COLUMNS, "column count")?;
    if column_count == 0 {
        return Err(SegmentV2Error::Corrupt("zero column count"));
    }
    let expected_directory_count = checked_add(column_count, 2, "directory count")?;
    let directory_count = reader.read_count(MAX_SEGMENT_V2_COLUMNS + 2, "directory count")?;
    if directory_count != expected_directory_count {
        return Err(SegmentV2Error::Corrupt("directory count"));
    }
    let mut directory = Vec::with_capacity(directory_count);
    for _ in 0..directory_count {
        directory.push(decode_directory_entry(&mut reader, row_count)?);
    }
    validate_directory_order(&directory)?;
    let lane_start = reader.position();
    let mut expected_offset = lane_start;
    for entry in &directory {
        if entry.offset != expected_offset {
            return Err(SegmentV2Error::Corrupt("lane gap or overlap"));
        }
        expected_offset = checked_add(expected_offset, entry.length, "lane extent")?;
        if expected_offset > body.len() {
            return Err(SegmentV2Error::Corrupt("lane outside file"));
        }
    }
    if expected_offset != body.len() {
        return Err(SegmentV2Error::Corrupt("trailing or missing lane bytes"));
    }

    let mut decoded_lanes = Vec::with_capacity(directory_count);
    for entry in &directory {
        let end = checked_add(entry.offset, entry.length, "lane slice")?;
        let bytes = body
            .get(entry.offset..end)
            .ok_or(SegmentV2Error::Corrupt("lane slice"))?;
        if checksum_bytes(bytes) != entry.checksum {
            return Err(SegmentV2Error::ChecksumMismatch("lane"));
        }
        let cells = decode_lane(entry, bytes, row_count)?;
        let statistics = compute_decoded_statistics(&entry.logical_type, &cells)?;
        if statistics != entry.statistics {
            return Err(SegmentV2Error::Corrupt("lane statistics"));
        }
        decoded_lanes.push(cells);
    }

    let mut decoded_lanes = decoded_lanes.into_iter();
    let primary_keys = decoded_lanes
        .next()
        .ok_or(SegmentV2Error::Corrupt("missing primary key lane"))?
        .into_iter()
        .map(|cell| match cell {
            SegmentV2Cell::Value(CanonicalValue::Bytes(value)) => {
                Ok(PrimaryKeyBytes::from_entity_key_bytes(value.into_vec()))
            }
            _ => Err(SegmentV2Error::Corrupt("primary key lane state")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let entity_versions = decoded_lanes
        .next()
        .ok_or(SegmentV2Error::Corrupt("missing entity-version lane"))?
        .into_iter()
        .map(|cell| match cell {
            SegmentV2Cell::Value(CanonicalValue::U64(value)) => {
                EntityVersion::new(value).ok_or(SegmentV2Error::Corrupt("entity version value"))
            }
            _ => Err(SegmentV2Error::Corrupt("entity version lane state")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let columns = directory
        .iter()
        .skip(2)
        .zip(decoded_lanes)
        .map(|(entry, cells)| match entry.kind {
            LaneKind::Field(field_id) => Ok(SegmentV2Column {
                field_id,
                logical_type: entry.logical_type.clone(),
                cells,
                statistics: entry.statistics.clone(),
            }),
            LaneKind::PrimaryKey | LaneKind::EntityVersion => {
                Err(SegmentV2Error::Corrupt("system lane ordering"))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let identity = SegmentV2Identity::new(
        DefinitionFingerprint::from_bytes(fingerprint),
        history_incarnation,
        generation,
        organization,
        SegmentV2SegmentId::from_bytes(segment_id),
        frontier_start,
        frontier_end,
    )
    .map_err(|_| SegmentV2Error::Corrupt("segment identity"))?;
    SegmentV2::new(identity, primary_keys, entity_versions, columns)
        .map_err(|_| SegmentV2Error::Corrupt("logical segment"))
}

fn decode_directory_entry(
    reader: &mut Reader<'_>,
    row_count: usize,
) -> Result<DirectoryEntry, SegmentV2Error> {
    let kind = decode_lane_kind(reader)?;
    let logical_type = decode_logical_type(reader)?;
    let encoding = PhysicalEncoding::from_tag(reader.read_u8()?)?;
    if !encoding_allowed(encoding, &logical_type, &kind) {
        return Err(SegmentV2Error::Corrupt("encoding not allowed for lane"));
    }
    let offset = reader.read_usize()?;
    let length = reader.read_usize()?;
    if length > MAX_LANE_BYTES {
        return Err(SegmentV2Error::BoundExceeded("lane bytes"));
    }
    let value_count = reader.read_count(row_count, "lane value count")?;
    let missing_count = reader.read_count(row_count, "lane missing count")?;
    let null_count = reader.read_count(row_count, "lane null count")?;
    if value_count
        .checked_add(missing_count)
        .and_then(|count| count.checked_add(null_count))
        != Some(row_count)
    {
        return Err(SegmentV2Error::Corrupt("lane state counts"));
    }
    let minimum = decode_optional_value(reader, &logical_type)?;
    let maximum = decode_optional_value(reader, &logical_type)?;
    if value_count == 0 && (minimum.is_some() || maximum.is_some()) {
        return Err(SegmentV2Error::Corrupt("statistics on empty lane"));
    }
    if value_count > 0 && (minimum.is_none() || maximum.is_none()) {
        return Err(SegmentV2Error::Corrupt("missing exact statistics"));
    }
    if let (Some(minimum), Some(maximum)) = (&minimum, &maximum)
        && compare_values(&logical_type, minimum, maximum)? == Ordering::Greater
    {
        return Err(SegmentV2Error::Corrupt("reversed statistics"));
    }
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(reader.read_exact(32)?);
    Ok(DirectoryEntry {
        kind,
        logical_type,
        encoding,
        offset,
        length,
        statistics: LaneStatistics {
            value_count,
            missing_count,
            null_count,
            minimum,
            maximum,
        },
        checksum,
    })
}

#[cfg(test)]
fn decode_lane(
    entry: &DirectoryEntry,
    bytes: &[u8],
    expected_rows: usize,
) -> Result<Vec<SegmentV2Cell>, SegmentV2Error> {
    let mut reader = Reader::new(bytes);
    let row_count = reader.read_count(MAX_SEGMENT_V2_ROWS, "lane rows")?;
    if row_count != expected_rows {
        return Err(SegmentV2Error::Corrupt("lane row count"));
    }
    let validity_len = reader.read_count(bitmap_len(row_count)?, "validity bytes")?;
    if validity_len != bitmap_len(row_count)? {
        return Err(SegmentV2Error::Corrupt("validity bitmap length"));
    }
    let present = reader.read_exact(validity_len)?.to_vec();
    let null = reader.read_exact(validity_len)?.to_vec();
    validate_padding_bits(&present, row_count)?;
    validate_padding_bits(&null, row_count)?;
    if present
        .iter()
        .zip(&null)
        .any(|(present, null)| present & null != 0)
    {
        return Err(SegmentV2Error::Corrupt("overlapping validity states"));
    }
    let observed_values = count_bits(&present, row_count);
    let observed_nulls = count_bits(&null, row_count);
    let observed_missing = row_count
        .checked_sub(observed_values)
        .and_then(|count| count.checked_sub(observed_nulls))
        .ok_or(SegmentV2Error::Corrupt("validity counts"))?;
    if observed_values != entry.statistics.value_count
        || observed_nulls != entry.statistics.null_count
        || observed_missing != entry.statistics.missing_count
    {
        return Err(SegmentV2Error::Corrupt("validity statistics"));
    }
    let payload = reader.remaining();
    let values = decode_values(
        entry.encoding,
        &entry.logical_type,
        payload,
        row_count,
        entry.statistics.value_count,
        &present,
    )?;
    let mut value_iter = values.into_iter();
    let mut cells = Vec::with_capacity(row_count);
    for index in 0..row_count {
        if bit_is_set(&present, index) {
            cells.push(SegmentV2Cell::Value(
                value_iter
                    .next()
                    .ok_or(SegmentV2Error::Corrupt("value cardinality"))?,
            ));
        } else if bit_is_set(&null, index) {
            cells.push(SegmentV2Cell::Null);
        } else {
            cells.push(SegmentV2Cell::Missing);
        }
    }
    if value_iter.next().is_some() {
        return Err(SegmentV2Error::Corrupt("extra decoded values"));
    }
    Ok(cells)
}

#[cfg(test)]
fn decode_values(
    encoding: PhysicalEncoding,
    logical_type: &SegmentV2LogicalType,
    payload: &[u8],
    row_count: usize,
    value_count: usize,
    present: &[u8],
) -> Result<Vec<CanonicalValue>, SegmentV2Error> {
    match encoding {
        PhysicalEncoding::FixedWidth => decode_fixed_values(payload, logical_type, value_count),
        PhysicalEncoding::OffsetCanonical => {
            decode_offset_values(payload, logical_type, value_count)
        }
        PhysicalEncoding::Dictionary => {
            decode_dictionary_values(payload, logical_type, value_count)
        }
        PhysicalEncoding::BooleanBitmap => {
            decode_boolean_values(payload, logical_type, row_count, value_count, present)
        }
        PhysicalEncoding::CheckedDelta => decode_delta_values(payload, logical_type, value_count),
    }
}

#[cfg(test)]
fn decode_fixed_values(
    payload: &[u8],
    logical_type: &SegmentV2LogicalType,
    value_count: usize,
) -> Result<Vec<CanonicalValue>, SegmentV2Error> {
    let mut reader = Reader::new(payload);
    let width = reader.read_count(MAX_SCALAR_BYTES, "fixed width")?;
    if value_count > 0 && width == 0 {
        return Err(SegmentV2Error::Corrupt("zero fixed width"));
    }
    let expected = width
        .checked_mul(value_count)
        .ok_or(SegmentV2Error::Corrupt("fixed-width overflow"))?;
    if reader.remaining().len() != expected {
        return Err(SegmentV2Error::Corrupt("fixed-width payload length"));
    }
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        values.push(decode_typed_value(reader.read_exact(width)?, logical_type)?);
    }
    Ok(values)
}

#[cfg(test)]
fn decode_offset_values(
    payload: &[u8],
    logical_type: &SegmentV2LogicalType,
    value_count: usize,
) -> Result<Vec<CanonicalValue>, SegmentV2Error> {
    let mut reader = Reader::new(payload);
    let mut offsets = Vec::with_capacity(value_count + 1);
    for _ in 0..=value_count {
        offsets.push(reader.read_count(MAX_LANE_BYTES, "value offset")?);
    }
    if offsets.first() != Some(&0)
        || offsets.windows(2).any(|pair| pair[0] > pair[1])
        || offsets.last().copied() != Some(reader.remaining().len())
    {
        return Err(SegmentV2Error::Corrupt("noncanonical offsets"));
    }
    let values_bytes = reader.remaining();
    offsets
        .windows(2)
        .map(|pair| decode_typed_value(&values_bytes[pair[0]..pair[1]], logical_type))
        .collect()
}

#[cfg(test)]
fn decode_dictionary_values(
    payload: &[u8],
    logical_type: &SegmentV2LogicalType,
    value_count: usize,
) -> Result<Vec<CanonicalValue>, SegmentV2Error> {
    let mut reader = Reader::new(payload);
    let dictionary_count = reader.read_count(value_count.max(1), "dictionary entries")?;
    if value_count > 0 && dictionary_count == 0 {
        return Err(SegmentV2Error::Corrupt("empty dictionary"));
    }
    let mut encoded_dictionary = Vec::with_capacity(dictionary_count);
    let mut dictionary = Vec::with_capacity(dictionary_count);
    for _ in 0..dictionary_count {
        let encoded = reader.read_bytes(MAX_SCALAR_BYTES)?;
        let value = decode_typed_value(&encoded, logical_type)?;
        encoded_dictionary.push(encoded);
        dictionary.push(value);
    }
    if encoded_dictionary.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(SegmentV2Error::Corrupt("noncanonical dictionary order"));
    }
    let bit_width = reader.read_u8()?;
    if bit_width != ordinal_bit_width(dictionary_count)? {
        return Err(SegmentV2Error::Corrupt("dictionary bit width"));
    }
    let ordinal_count = reader.read_count(value_count, "dictionary ordinals")?;
    if ordinal_count != value_count {
        return Err(SegmentV2Error::Corrupt("dictionary ordinal count"));
    }
    let expected_packed = packed_len(value_count, bit_width)?;
    if reader.remaining().len() != expected_packed {
        return Err(SegmentV2Error::Corrupt("dictionary packed length"));
    }
    validate_packed_padding(reader.remaining(), value_count, bit_width)?;
    let mut values = Vec::with_capacity(value_count);
    for position in 0..value_count {
        let ordinal = unpack_ordinal(reader.remaining(), position, bit_width)?;
        values.push(
            dictionary
                .get(ordinal)
                .ok_or(SegmentV2Error::Corrupt("dictionary ordinal range"))?
                .clone(),
        );
    }
    Ok(values)
}

#[cfg(test)]
fn decode_boolean_values(
    payload: &[u8],
    logical_type: &SegmentV2LogicalType,
    row_count: usize,
    value_count: usize,
    present: &[u8],
) -> Result<Vec<CanonicalValue>, SegmentV2Error> {
    if !matches!(logical_type, SegmentV2LogicalType::Bool) {
        return Err(SegmentV2Error::Corrupt("boolean encoding type"));
    }
    if payload.len() != bitmap_len(row_count)? {
        return Err(SegmentV2Error::Corrupt("boolean bitmap length"));
    }
    validate_padding_bits(payload, row_count)?;
    let mut values = Vec::with_capacity(value_count);
    for index in 0..row_count {
        if bit_is_set(present, index) {
            values.push(CanonicalValue::Bool(bit_is_set(payload, index)));
        }
    }
    Ok(values)
}

#[cfg(test)]
fn decode_delta_values(
    payload: &[u8],
    logical_type: &SegmentV2LogicalType,
    value_count: usize,
) -> Result<Vec<CanonicalValue>, SegmentV2Error> {
    if !is_delta_type(logical_type) {
        return Err(SegmentV2Error::Corrupt("delta encoding type"));
    }
    if value_count == 0 {
        if payload.is_empty() {
            return Ok(Vec::new());
        }
        return Err(SegmentV2Error::Corrupt("delta bytes for empty lane"));
    }
    let mut reader = Reader::new(payload);
    let mut scalar_bytes = [0u8; 16];
    scalar_bytes.copy_from_slice(reader.read_exact(16)?);
    let mut scalar = i128::from_be_bytes(scalar_bytes);
    let mut values = Vec::with_capacity(value_count);
    values.push(value_from_scalar(logical_type, scalar)?);
    for _ in 1..value_count {
        let delta = zigzag_decode(reader.read_var_u128()?);
        scalar = scalar
            .checked_add(delta)
            .ok_or(SegmentV2Error::Corrupt("delta reconstruction overflow"))?;
        values.push(value_from_scalar(logical_type, scalar)?);
    }
    if !reader.remaining().is_empty() {
        return Err(SegmentV2Error::Corrupt("trailing delta bytes"));
    }
    Ok(values)
}

fn compute_statistics(
    logical_type: &SegmentV2LogicalType,
    cells: &[SegmentV2Cell],
) -> Result<LaneStatistics, SegmentV2Error> {
    compute_statistics_inner(logical_type, cells, true)
}

#[cfg(test)]
fn compute_decoded_statistics(
    logical_type: &SegmentV2LogicalType,
    cells: &[SegmentV2Cell],
) -> Result<LaneStatistics, SegmentV2Error> {
    compute_statistics_inner(logical_type, cells, false)
}

fn compute_statistics_inner(
    logical_type: &SegmentV2LogicalType,
    cells: &[SegmentV2Cell],
    verify_canonical_size: bool,
) -> Result<LaneStatistics, SegmentV2Error> {
    let mut value_count = 0usize;
    let mut missing_count = 0usize;
    let mut null_count = 0usize;
    let mut minimum: Option<CanonicalValue> = None;
    let mut maximum: Option<CanonicalValue> = None;
    for cell in cells {
        match cell {
            SegmentV2Cell::Missing => missing_count += 1,
            SegmentV2Cell::Null => null_count += 1,
            SegmentV2Cell::Value(value) => {
                if !logical_type.accepts(value) {
                    return Err(SegmentV2Error::Invalid("logical type mismatch"));
                }
                if verify_canonical_size {
                    let encoded = encode_canonical_value(value)
                        .map_err(|_| SegmentV2Error::Invalid("canonical value encoding"))?;
                    if encoded.len() > MAX_SCALAR_BYTES {
                        return Err(SegmentV2Error::BoundExceeded("canonical value bytes"));
                    }
                }
                value_count += 1;
                if minimum.as_ref().is_none_or(|current| {
                    compare_values(logical_type, value, current) == Ok(Ordering::Less)
                }) {
                    minimum = Some(value.clone());
                }
                if maximum.as_ref().is_none_or(|current| {
                    compare_values(logical_type, value, current) == Ok(Ordering::Greater)
                }) {
                    maximum = Some(value.clone());
                }
            }
        }
    }
    Ok(LaneStatistics {
        value_count,
        missing_count,
        null_count,
        minimum,
        maximum,
    })
}

fn compare_values(
    logical_type: &SegmentV2LogicalType,
    left: &CanonicalValue,
    right: &CanonicalValue,
) -> Result<Ordering, SegmentV2Error> {
    if !logical_type.accepts(left) || !logical_type.accepts(right) {
        return Err(SegmentV2Error::Invalid("comparison type mismatch"));
    }
    let ordering = match (left, right) {
        (CanonicalValue::Bool(left), CanonicalValue::Bool(right)) => left.cmp(right),
        (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left.cmp(right),
        (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left.cmp(right),
        (CanonicalValue::String(left), CanonicalValue::String(right)) => {
            left.as_str().cmp(right.as_str())
        }
        (CanonicalValue::Bytes(left), CanonicalValue::Bytes(right)) => {
            left.as_bytes().cmp(right.as_bytes())
        }
        (CanonicalValue::Timestamp(left), CanonicalValue::Timestamp(right)) => left.cmp(right),
        (CanonicalValue::Date(left), CanonicalValue::Date(right)) => left.cmp(right),
        (CanonicalValue::Uuid(left), CanonicalValue::Uuid(right)) => left.cmp(right),
        (
            CanonicalValue::Enum {
                variant_id: left, ..
            },
            CanonicalValue::Enum {
                variant_id: right, ..
            },
        ) => left.cmp(right),
        (CanonicalValue::Decimal(left), CanonicalValue::Decimal(right)) => left
            .checked_cmp(*right)
            .map_err(|_| SegmentV2Error::Invalid("decimal comparison"))?,
        (CanonicalValue::Money(left), CanonicalValue::Money(right)) => left
            .checked_cmp(*right)
            .map_err(|_| SegmentV2Error::Invalid("money comparison"))?,
        _ => return Err(SegmentV2Error::Invalid("comparison type mismatch")),
    };
    Ok(ordering)
}

fn pruning_decision(
    logical_type: &SegmentV2LogicalType,
    statistics: &LaneStatistics,
    predicate: &SegmentV2Predicate,
) -> Result<SegmentV2PruningDecision, SegmentV2Error> {
    use SegmentV2Predicate as Predicate;
    use SegmentV2PruningDecision::{Scan, Skip};
    let decision = match predicate {
        Predicate::IsMissing => {
            if statistics.missing_count == 0 {
                Skip
            } else {
                Scan
            }
        }
        Predicate::IsNull => {
            if statistics.null_count == 0 {
                Skip
            } else {
                Scan
            }
        }
        Predicate::IsPresent => {
            if statistics.value_count == 0 {
                Skip
            } else {
                Scan
            }
        }
        Predicate::Equal(value) => {
            require_predicate_value(logical_type, value)?;
            match (&statistics.minimum, &statistics.maximum) {
                (Some(minimum), Some(maximum))
                    if compare_values(logical_type, value, minimum)? != Ordering::Less
                        && compare_values(logical_type, value, maximum)? != Ordering::Greater =>
                {
                    Scan
                }
                _ => Skip,
            }
        }
        Predicate::LessThan(value) => {
            require_predicate_value(logical_type, value)?;
            if !zone_map_order_matches_executor(logical_type) {
                Scan
            } else {
                match &statistics.minimum {
                    Some(minimum)
                        if compare_values(logical_type, minimum, value)? == Ordering::Less =>
                    {
                        Scan
                    }
                    _ => Skip,
                }
            }
        }
        Predicate::LessThanOrEqual(value) => {
            require_predicate_value(logical_type, value)?;
            if !zone_map_order_matches_executor(logical_type) {
                Scan
            } else {
                match &statistics.minimum {
                    Some(minimum)
                        if compare_values(logical_type, minimum, value)? != Ordering::Greater =>
                    {
                        Scan
                    }
                    _ => Skip,
                }
            }
        }
        Predicate::GreaterThan(value) => {
            require_predicate_value(logical_type, value)?;
            if !zone_map_order_matches_executor(logical_type) {
                Scan
            } else {
                match &statistics.maximum {
                    Some(maximum)
                        if compare_values(logical_type, maximum, value)? == Ordering::Greater =>
                    {
                        Scan
                    }
                    _ => Skip,
                }
            }
        }
        Predicate::GreaterThanOrEqual(value) => {
            require_predicate_value(logical_type, value)?;
            if !zone_map_order_matches_executor(logical_type) {
                Scan
            } else {
                match &statistics.maximum {
                    Some(maximum)
                        if compare_values(logical_type, maximum, value)? != Ordering::Less =>
                    {
                        Scan
                    }
                    _ => Skip,
                }
            }
        }
    };
    Ok(decision)
}

/// Whether the persisted numeric zone order is the production predicate order.
///
/// Decimal and money statistics use their exact typed numeric order. The
/// production executor deliberately orders those range predicates by canonical
/// bytes, so a numeric minimum/maximum cannot safely exclude a segment for
/// them. Equality remains exact and may still use the zone and dictionary.
const fn zone_map_order_matches_executor(logical_type: &SegmentV2LogicalType) -> bool {
    !matches!(
        logical_type,
        SegmentV2LogicalType::Decimal(_) | SegmentV2LogicalType::Money { .. }
    )
}

fn require_predicate_value(
    logical_type: &SegmentV2LogicalType,
    value: &CanonicalValue,
) -> Result<(), SegmentV2Error> {
    if logical_type.accepts(value) {
        Ok(())
    } else {
        Err(SegmentV2Error::Invalid("predicate type mismatch"))
    }
}

fn decode_typed_value(
    encoded: &[u8],
    logical_type: &SegmentV2LogicalType,
) -> Result<CanonicalValue, SegmentV2Error> {
    if encoded.len() > MAX_SCALAR_BYTES {
        return Err(SegmentV2Error::BoundExceeded("canonical value bytes"));
    }
    let value =
        decode_canonical_value(encoded).map_err(|_| SegmentV2Error::Corrupt("canonical value"))?;
    if !logical_type.accepts(&value) {
        return Err(SegmentV2Error::Corrupt("canonical value type"));
    }
    Ok(value)
}

fn scalar_value(
    logical_type: &SegmentV2LogicalType,
    value: &CanonicalValue,
) -> Result<i128, SegmentV2Error> {
    match (logical_type, value) {
        (SegmentV2LogicalType::I64, CanonicalValue::I64(value)) => Ok(i128::from(*value)),
        (SegmentV2LogicalType::U64, CanonicalValue::U64(value)) => Ok(i128::from(*value)),
        (SegmentV2LogicalType::Date, CanonicalValue::Date(value)) => {
            Ok(i128::from(value.days_since_unix_epoch()))
        }
        (SegmentV2LogicalType::Timestamp, CanonicalValue::Timestamp(value)) => {
            i128::from(value.seconds())
                .checked_mul(1_000_000_000)
                .and_then(|seconds| seconds.checked_add(i128::from(value.nanoseconds())))
                .ok_or(SegmentV2Error::Invalid("timestamp scalar"))
        }
        _ => Err(SegmentV2Error::Invalid("delta scalar type")),
    }
}

fn value_from_scalar(
    logical_type: &SegmentV2LogicalType,
    scalar: i128,
) -> Result<CanonicalValue, SegmentV2Error> {
    match logical_type {
        SegmentV2LogicalType::I64 => i64::try_from(scalar)
            .map(CanonicalValue::I64)
            .map_err(|_| SegmentV2Error::Corrupt("i64 delta range")),
        SegmentV2LogicalType::U64 => u64::try_from(scalar)
            .map(CanonicalValue::U64)
            .map_err(|_| SegmentV2Error::Corrupt("u64 delta range")),
        SegmentV2LogicalType::Date => i32::try_from(scalar)
            .map(Date::new)
            .map(CanonicalValue::Date)
            .map_err(|_| SegmentV2Error::Corrupt("date delta range")),
        SegmentV2LogicalType::Timestamp => {
            let seconds = scalar.div_euclid(1_000_000_000);
            let nanoseconds = scalar.rem_euclid(1_000_000_000);
            let seconds = i64::try_from(seconds)
                .map_err(|_| SegmentV2Error::Corrupt("timestamp delta range"))?;
            let nanoseconds = u32::try_from(nanoseconds)
                .map_err(|_| SegmentV2Error::Corrupt("timestamp nanoseconds"))?;
            Timestamp::new(seconds, nanoseconds)
                .map(CanonicalValue::Timestamp)
                .map_err(|_| SegmentV2Error::Corrupt("timestamp delta value"))
        }
        _ => Err(SegmentV2Error::Corrupt("delta logical type")),
    }
}

fn is_fixed_width_type(logical_type: &SegmentV2LogicalType) -> bool {
    !matches!(
        logical_type,
        SegmentV2LogicalType::String | SegmentV2LogicalType::Bytes
    )
}

fn is_delta_type(logical_type: &SegmentV2LogicalType) -> bool {
    matches!(
        logical_type,
        SegmentV2LogicalType::I64
            | SegmentV2LogicalType::U64
            | SegmentV2LogicalType::Timestamp
            | SegmentV2LogicalType::Date
    )
}

fn encoding_allowed(
    encoding: PhysicalEncoding,
    logical_type: &SegmentV2LogicalType,
    kind: &LaneKind,
) -> bool {
    if matches!(kind, LaneKind::PrimaryKey) {
        return encoding == PhysicalEncoding::OffsetCanonical
            && matches!(logical_type, SegmentV2LogicalType::Bytes);
    }
    match encoding {
        PhysicalEncoding::FixedWidth => is_fixed_width_type(logical_type),
        PhysicalEncoding::OffsetCanonical => true,
        PhysicalEncoding::Dictionary => matches!(
            logical_type,
            SegmentV2LogicalType::String | SegmentV2LogicalType::Enum(_)
        ),
        PhysicalEncoding::BooleanBitmap => matches!(logical_type, SegmentV2LogicalType::Bool),
        PhysicalEncoding::CheckedDelta => is_delta_type(logical_type),
    }
}

fn encode_lane_kind(out: &mut Vec<u8>, kind: &LaneKind) {
    match kind {
        LaneKind::PrimaryKey => {
            out.push(1);
            out.extend_from_slice(&0u32.to_be_bytes());
        }
        LaneKind::EntityVersion => {
            out.push(2);
            out.extend_from_slice(&0u32.to_be_bytes());
        }
        LaneKind::Field(field_id) => {
            out.push(3);
            out.extend_from_slice(&field_id.get().to_be_bytes());
        }
    }
}

fn decode_lane_kind(reader: &mut Reader<'_>) -> Result<LaneKind, SegmentV2Error> {
    let tag = reader.read_u8()?;
    let identity = reader.read_u32()?;
    match (tag, identity) {
        (1, 0) => Ok(LaneKind::PrimaryKey),
        (2, 0) => Ok(LaneKind::EntityVersion),
        (3, identity) => FieldId::new(identity)
            .map(LaneKind::Field)
            .ok_or(SegmentV2Error::Corrupt("field lane identity")),
        _ => Err(SegmentV2Error::Corrupt("lane kind")),
    }
}

fn validate_directory_order(directory: &[DirectoryEntry]) -> Result<(), SegmentV2Error> {
    if !matches!(
        directory.first().map(|entry| &entry.kind),
        Some(LaneKind::PrimaryKey)
    ) || !matches!(
        directory.get(1).map(|entry| &entry.kind),
        Some(LaneKind::EntityVersion)
    ) {
        return Err(SegmentV2Error::Corrupt("system lane ordering"));
    }
    if !matches!(directory[0].logical_type, SegmentV2LogicalType::Bytes)
        || !matches!(directory[1].logical_type, SegmentV2LogicalType::U64)
    {
        return Err(SegmentV2Error::Corrupt("system lane type"));
    }
    let fields = directory.iter().skip(2).map(|entry| match entry.kind {
        LaneKind::Field(field) => Ok(field),
        LaneKind::PrimaryKey | LaneKind::EntityVersion => {
            Err(SegmentV2Error::Corrupt("duplicate system lane"))
        }
    });
    let fields = fields.collect::<Result<Vec<_>, _>>()?;
    if fields.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(SegmentV2Error::Corrupt("field lane ordering"));
    }
    Ok(())
}

fn encode_logical_type(out: &mut Vec<u8>, logical_type: &SegmentV2LogicalType) {
    out.push(logical_type.tag());
    match logical_type {
        SegmentV2LogicalType::Enum(type_id) => {
            out.extend_from_slice(&type_id.get().to_be_bytes());
        }
        SegmentV2LogicalType::Decimal(spec) => {
            out.push(spec.precision());
            out.push(spec.scale());
        }
        SegmentV2LogicalType::Money { currency, amount } => {
            out.extend_from_slice(currency.as_bytes());
            out.push(amount.precision());
            out.push(amount.scale());
        }
        SegmentV2LogicalType::Bool
        | SegmentV2LogicalType::I64
        | SegmentV2LogicalType::U64
        | SegmentV2LogicalType::String
        | SegmentV2LogicalType::Bytes
        | SegmentV2LogicalType::Timestamp
        | SegmentV2LogicalType::Date
        | SegmentV2LogicalType::Uuid => {}
    }
}

fn decode_logical_type(reader: &mut Reader<'_>) -> Result<SegmentV2LogicalType, SegmentV2Error> {
    match reader.read_u8()? {
        1 => Ok(SegmentV2LogicalType::Bool),
        2 => Ok(SegmentV2LogicalType::I64),
        3 => Ok(SegmentV2LogicalType::U64),
        4 => Ok(SegmentV2LogicalType::String),
        5 => Ok(SegmentV2LogicalType::Bytes),
        6 => Ok(SegmentV2LogicalType::Timestamp),
        7 => Ok(SegmentV2LogicalType::Date),
        8 => Ok(SegmentV2LogicalType::Uuid),
        9 => EnumTypeId::new(reader.read_u32()?)
            .map(SegmentV2LogicalType::Enum)
            .ok_or(SegmentV2Error::Corrupt("enum type identity")),
        10 => DecimalSpec::new(reader.read_u8()?, reader.read_u8()?)
            .map(SegmentV2LogicalType::Decimal)
            .map_err(|_| SegmentV2Error::Corrupt("decimal type")),
        11 => {
            let currency = CurrencyCode::new(reader.read_exact(3)?)
                .map_err(|_| SegmentV2Error::Corrupt("money currency"))?;
            let amount = DecimalSpec::new(reader.read_u8()?, reader.read_u8()?)
                .map_err(|_| SegmentV2Error::Corrupt("money amount type"))?;
            Ok(SegmentV2LogicalType::Money { currency, amount })
        }
        _ => Err(SegmentV2Error::Corrupt("logical type tag")),
    }
}

fn encode_optional_value(
    out: &mut Vec<u8>,
    value: Option<&CanonicalValue>,
) -> Result<(), SegmentV2Error> {
    match value {
        None => out.extend_from_slice(&0u32.to_be_bytes()),
        Some(value) => {
            let bytes = encode_canonical_value(value)
                .map_err(|_| SegmentV2Error::Invalid("statistic encoding"))?;
            put_bytes(out, &bytes, "statistic bytes")?;
        }
    }
    Ok(())
}

fn decode_optional_value(
    reader: &mut Reader<'_>,
    logical_type: &SegmentV2LogicalType,
) -> Result<Option<CanonicalValue>, SegmentV2Error> {
    let bytes = reader.read_bytes(MAX_SCALAR_BYTES)?;
    if bytes.is_empty() {
        Ok(None)
    } else {
        decode_typed_value(&bytes, logical_type).map(Some)
    }
}

fn encode_frontier(out: &mut Vec<u8>, frontier: FrontierPosition) {
    match frontier {
        FrontierPosition::BeforeFirst => {
            out.push(0);
            out.extend_from_slice(&0u64.to_be_bytes());
        }
        FrontierPosition::AppliedThrough(sequence) => {
            out.push(1);
            out.extend_from_slice(&sequence.get().to_be_bytes());
        }
    }
}

fn frontier_ordinal(frontier: FrontierPosition) -> u64 {
    match frontier {
        FrontierPosition::BeforeFirst => 0,
        FrontierPosition::AppliedThrough(sequence) => sequence.get(),
    }
}

fn bitmap_len(rows: usize) -> Result<usize, SegmentV2Error> {
    rows.checked_add(7)
        .map(|count| count / 8)
        .ok_or(SegmentV2Error::BoundExceeded("bitmap length"))
}

fn set_bit(bitmap: &mut [u8], index: usize) {
    bitmap[index / 8] |= 1u8 << (index % 8);
}

fn bit_is_set(bitmap: &[u8], index: usize) -> bool {
    bitmap[index / 8] & (1u8 << (index % 8)) != 0
}

fn count_bits(bitmap: &[u8], row_count: usize) -> usize {
    (0..row_count)
        .filter(|index| bit_is_set(bitmap, *index))
        .count()
}

fn validate_padding_bits(bitmap: &[u8], row_count: usize) -> Result<(), SegmentV2Error> {
    let remainder = row_count % 8;
    if remainder == 0 || bitmap.is_empty() {
        return Ok(());
    }
    let allowed = (1u8 << remainder) - 1;
    if bitmap[bitmap.len() - 1] & !allowed != 0 {
        return Err(SegmentV2Error::Corrupt("nonzero bitmap padding"));
    }
    Ok(())
}

fn ordinal_bit_width(dictionary_count: usize) -> Result<u8, SegmentV2Error> {
    if dictionary_count == 0 {
        return Ok(1);
    }
    let maximum = dictionary_count - 1;
    let width = (usize::BITS - maximum.leading_zeros()).max(1);
    u8::try_from(width).map_err(|_| SegmentV2Error::BoundExceeded("dictionary bit width"))
}

fn packed_len(count: usize, width: u8) -> Result<usize, SegmentV2Error> {
    count
        .checked_mul(usize::from(width))
        .and_then(|bits| bits.checked_add(7))
        .map(|bits| bits / 8)
        .ok_or(SegmentV2Error::BoundExceeded("packed ordinals"))
}

fn pack_ordinal(
    packed: &mut [u8],
    position: usize,
    width: u8,
    ordinal: usize,
) -> Result<(), SegmentV2Error> {
    let bit_start = position
        .checked_mul(usize::from(width))
        .ok_or(SegmentV2Error::BoundExceeded("ordinal position"))?;
    for bit in 0..usize::from(width) {
        if ordinal & (1usize << bit) != 0 {
            let target = bit_start + bit;
            packed[target / 8] |= 1u8 << (target % 8);
        }
    }
    Ok(())
}

fn unpack_ordinal(packed: &[u8], position: usize, width: u8) -> Result<usize, SegmentV2Error> {
    let bit_start = position
        .checked_mul(usize::from(width))
        .ok_or(SegmentV2Error::Corrupt("ordinal position"))?;
    let mut ordinal = 0usize;
    for bit in 0..usize::from(width) {
        let source = bit_start + bit;
        if packed
            .get(source / 8)
            .is_some_and(|byte| byte & (1u8 << (source % 8)) != 0)
        {
            ordinal |= 1usize << bit;
        }
    }
    Ok(ordinal)
}

fn validate_packed_padding(packed: &[u8], count: usize, width: u8) -> Result<(), SegmentV2Error> {
    let used = count
        .checked_mul(usize::from(width))
        .ok_or(SegmentV2Error::Corrupt("packed bit count"))?;
    if used % 8 == 0 || packed.is_empty() {
        return Ok(());
    }
    let allowed = (1u8 << (used % 8)) - 1;
    if packed[packed.len() - 1] & !allowed != 0 {
        return Err(SegmentV2Error::Corrupt("nonzero ordinal padding"));
    }
    Ok(())
}

fn zigzag_encode(value: i128) -> u128 {
    ((value as u128) << 1) ^ ((value >> 127) as u128)
}

fn zigzag_decode(value: u128) -> i128 {
    ((value >> 1) as i128) ^ -((value & 1) as i128)
}

fn put_var_u128(out: &mut Vec<u8>, mut value: u128) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn put_u32(out: &mut Vec<u8>, value: usize, class: &'static str) -> Result<(), SegmentV2Error> {
    let value = u32::try_from(value).map_err(|_| SegmentV2Error::BoundExceeded(class))?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn put_u64(out: &mut Vec<u8>, value: usize, class: &'static str) -> Result<(), SegmentV2Error> {
    let value = u64::try_from(value).map_err(|_| SegmentV2Error::BoundExceeded(class))?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8], class: &'static str) -> Result<(), SegmentV2Error> {
    put_u32(out, bytes.len(), class)?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn checked_add(left: usize, right: usize, class: &'static str) -> Result<usize, SegmentV2Error> {
    left.checked_add(right)
        .ok_or(SegmentV2Error::BoundExceeded(class))
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn position(&self) -> usize {
        self.offset
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], SegmentV2Error> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(SegmentV2Error::Corrupt("offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(SegmentV2Error::Corrupt("truncated bytes"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, SegmentV2Error> {
        self.read_exact(1)?
            .first()
            .copied()
            .ok_or(SegmentV2Error::Corrupt("truncated byte"))
    }

    fn read_u16(&mut self) -> Result<u16, SegmentV2Error> {
        let bytes: [u8; 2] = self
            .read_exact(2)?
            .try_into()
            .map_err(|_| SegmentV2Error::Corrupt("u16"))?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, SegmentV2Error> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| SegmentV2Error::Corrupt("u32"))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, SegmentV2Error> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| SegmentV2Error::Corrupt("u64"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_usize(&mut self) -> Result<usize, SegmentV2Error> {
        usize::try_from(self.read_u64()?).map_err(|_| SegmentV2Error::Corrupt("usize range"))
    }

    fn read_count(&mut self, maximum: usize, class: &'static str) -> Result<usize, SegmentV2Error> {
        let count =
            usize::try_from(self.read_u32()?).map_err(|_| SegmentV2Error::BoundExceeded(class))?;
        if count > maximum {
            return Err(SegmentV2Error::BoundExceeded(class));
        }
        Ok(count)
    }

    fn read_bytes(&mut self, maximum: usize) -> Result<Vec<u8>, SegmentV2Error> {
        let length = self.read_count(maximum, "length-prefixed bytes")?;
        Ok(self.read_exact(length)?.to_vec())
    }

    fn read_frontier(&mut self) -> Result<FrontierPosition, SegmentV2Error> {
        let tag = self.read_u8()?;
        let value = self.read_u64()?;
        match (tag, value) {
            (0, 0) => Ok(FrontierPosition::BeforeFirst),
            (1, value) => CommitSequence::new(value)
                .map(FrontierPosition::AppliedThrough)
                .ok_or(SegmentV2Error::Corrupt("frontier sequence")),
            _ => Err(SegmentV2Error::Corrupt("frontier encoding")),
        }
    }

    fn read_var_u128(&mut self) -> Result<u128, SegmentV2Error> {
        let mut value = 0u128;
        for index in 0..19usize {
            let byte = self.read_u8()?;
            let payload = u128::from(byte & 0x7f);
            if index == 18 && payload > 3 {
                return Err(SegmentV2Error::Corrupt("varint overflow"));
            }
            value |= payload << (index * 7);
            if byte & 0x80 == 0 {
                if index > 0 && payload == 0 {
                    return Err(SegmentV2Error::Corrupt("noncanonical varint"));
                }
                return Ok(value);
            }
        }
        Err(SegmentV2Error::Corrupt("unterminated varint"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::Instant;

    use crate::checkpoint::{decode_segment_rows, encode_segment_rows};
    use crate::store::LiveRow;

    fn one_test_segment(column: SegmentV2Column, identity_byte: u8) -> SegmentV2 {
        let row_count = column.cells().len();
        let keys = (0..row_count)
            .map(|index| {
                PrimaryKeyBytes::from_entity_key_bytes(
                    u64::try_from(index).expect("bounded key").to_be_bytes(),
                )
            })
            .collect();
        let versions = (0..row_count)
            .map(|index| {
                EntityVersion::new(u64::try_from(index + 1).expect("bounded version"))
                    .expect("nonzero version")
            })
            .collect();
        let identity = SegmentV2Identity::new(
            DefinitionFingerprint::from_bytes([0x44; 32]),
            1,
            ProjectionGeneration::first(),
            OrgKey::from_encoded_bytes(vec![0x10, 0x20]),
            SegmentV2SegmentId::from_bytes([identity_byte; 16]),
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("frontier")),
        )
        .expect("identity");
        SegmentV2::new(identity, keys, versions, vec![column]).expect("segment")
    }

    fn encode_test_lanes(segment: &SegmentV2, mut lanes: Vec<EncodedLane>) -> Vec<u8> {
        let header_zero = encode_header(segment, lanes.len(), 0).expect("zero header");
        let directory_zero = encode_directory(&lanes).expect("zero directory");
        let mut next_offset = header_zero.len() + directory_zero.len();
        for lane in &mut lanes {
            lane.offset = next_offset;
            next_offset += lane.bytes.len();
        }
        let total_length = next_offset + COMPLETE_CHECKSUM_BYTES;
        let mut encoded = encode_header(segment, lanes.len(), total_length).expect("header");
        encoded.extend_from_slice(&encode_directory(&lanes).expect("directory"));
        for lane in lanes {
            encoded.extend_from_slice(&lane.bytes);
        }
        let checksum = checksum_bytes(&encoded);
        encoded.extend_from_slice(&checksum);
        encoded
    }

    #[test]
    fn zigzag_and_varint_cover_signed_extremes() {
        for value in [i128::MIN, -129, -1, 0, 1, 128, i128::MAX] {
            let encoded = zigzag_encode(value);
            assert_eq!(zigzag_decode(encoded), value);
            let mut bytes = Vec::new();
            put_var_u128(&mut bytes, encoded);
            let mut reader = Reader::new(&bytes);
            assert_eq!(reader.read_var_u128().expect("varint"), encoded);
            assert!(reader.remaining().is_empty());
        }
    }

    #[test]
    fn selector_uses_every_closed_encoding_from_value_mechanics_only() {
        let repeated = (0..256)
            .map(|index| {
                CanonicalValue::string(if index % 2 == 0 { "open" } else { "closed" })
                    .expect("string")
            })
            .collect::<Vec<_>>();
        let repeated_cells = repeated
            .iter()
            .cloned()
            .map(SegmentV2Cell::Value)
            .collect::<Vec<_>>();
        assert_eq!(
            select_encoding(&SegmentV2LogicalType::String, &repeated_cells, &repeated)
                .expect("dictionary")
                .0,
            PhysicalEncoding::Dictionary
        );

        let unique = (0..256)
            .map(|index| CanonicalValue::string(format!("unique-{index:08x}")).expect("string"))
            .collect::<Vec<_>>();
        let unique_cells = unique
            .iter()
            .cloned()
            .map(SegmentV2Cell::Value)
            .collect::<Vec<_>>();
        assert_eq!(
            select_encoding(&SegmentV2LogicalType::String, &unique_cells, &unique)
                .expect("offset")
                .0,
            PhysicalEncoding::OffsetCanonical
        );

        let sequential = (0..256).map(CanonicalValue::U64).collect::<Vec<_>>();
        let sequential_cells = sequential
            .iter()
            .cloned()
            .map(SegmentV2Cell::Value)
            .collect::<Vec<_>>();
        assert_eq!(
            select_encoding(&SegmentV2LogicalType::U64, &sequential_cells, &sequential)
                .expect("delta")
                .0,
            PhysicalEncoding::CheckedDelta
        );

        let randomish = (0u64..256)
            .map(|index| CanonicalValue::U64(if index % 2 == 0 { 0 } else { u64::MAX }))
            .collect::<Vec<_>>();
        let randomish_cells = randomish
            .iter()
            .cloned()
            .map(SegmentV2Cell::Value)
            .collect::<Vec<_>>();
        assert_eq!(
            select_encoding(&SegmentV2LogicalType::U64, &randomish_cells, &randomish)
                .expect("fixed")
                .0,
            PhysicalEncoding::FixedWidth
        );

        let booleans = (0..256)
            .map(|index| CanonicalValue::Bool(index % 3 == 0))
            .collect::<Vec<_>>();
        let boolean_cells = booleans
            .iter()
            .cloned()
            .map(SegmentV2Cell::Value)
            .collect::<Vec<_>>();
        assert_eq!(
            select_encoding(&SegmentV2LogicalType::Bool, &boolean_cells, &booleans)
                .expect("bitmap")
                .0,
            PhysicalEncoding::BooleanBitmap
        );
    }

    // req: PRJ-004, OQ-020, OQ-022, PERF-007
    #[test]
    fn open_validated_pruning_preserves_boundaries_optional_states_and_exact_dictionary_equality() {
        let field = FieldId::new(1).expect("field");
        let boundary_column = SegmentV2Column::new(
            field,
            SegmentV2LogicalType::U64,
            vec![
                SegmentV2Cell::Missing,
                SegmentV2Cell::Null,
                SegmentV2Cell::Value(CanonicalValue::U64(10)),
                SegmentV2Cell::Value(CanonicalValue::U64(20)),
            ],
        )
        .expect("boundary column");
        let boundary_bytes =
            SegmentV2Codec::encode(&one_test_segment(boundary_column, 0x61)).expect("encode");
        let (_, boundary) =
            SegmentV2Codec::decode_with_pruning(&boundary_bytes).expect("validate once");
        for (predicate, skip) in [
            (SegmentV2Predicate::Equal(CanonicalValue::U64(9)), true),
            (SegmentV2Predicate::Equal(CanonicalValue::U64(10)), false),
            (SegmentV2Predicate::LessThan(CanonicalValue::U64(10)), true),
            (
                SegmentV2Predicate::LessThanOrEqual(CanonicalValue::U64(10)),
                false,
            ),
            (
                SegmentV2Predicate::GreaterThan(CanonicalValue::U64(20)),
                true,
            ),
            (
                SegmentV2Predicate::GreaterThanOrEqual(CanonicalValue::U64(20)),
                false,
            ),
            (SegmentV2Predicate::IsMissing, false),
            (SegmentV2Predicate::IsNull, false),
            (SegmentV2Predicate::IsPresent, false),
        ] {
            assert_eq!(boundary.proves_no_match(field, &predicate), skip);
        }

        let (_, dictionary_bytes) = corpus_bytes(257, true);
        let (dictionary_segment, dictionary) =
            SegmentV2Codec::decode_with_pruning(&dictionary_bytes).expect("validated dictionary");
        let dictionary_field = FieldId::new(1).expect("dictionary field");
        let absent = SegmentV2Predicate::Equal(
            CanonicalValue::string("middle").expect("within-zone absent value"),
        );
        assert_eq!(
            dictionary_segment.columns()[0]
                .pruning_decision(&absent)
                .expect("zone decision"),
            SegmentV2PruningDecision::Scan,
            "the value is inside the zone and requires exact dictionary evidence"
        );
        assert!(dictionary.proves_no_match(dictionary_field, &absent));
        for present in ["open", "closed", "queued", "running"] {
            assert!(!dictionary.proves_no_match(
                dictionary_field,
                &SegmentV2Predicate::Equal(
                    CanonicalValue::string(present).expect("present dictionary value")
                )
            ));
        }
    }

    // req: PRJ-004, PRJ-009, OQ-020, OQ-022
    #[test]
    fn open_refuses_resealed_statistic_mismatch_and_reordered_dictionary_evidence() {
        let field = FieldId::new(1).expect("field");
        let values = (0..256)
            .map(|index| {
                SegmentV2Cell::Value(
                    CanonicalValue::string(if index % 2 == 0 { "alpha" } else { "omega" })
                        .expect("dictionary value"),
                )
            })
            .collect();
        let segment = one_test_segment(
            SegmentV2Column::new(field, SegmentV2LogicalType::String, values)
                .expect("dictionary column"),
            0x62,
        );

        let mut mismatched_lanes = encode_all_lanes(&segment).expect("lanes");
        mismatched_lanes[2].statistics.minimum =
            Some(CanonicalValue::string("bravo").expect("mismatched minimum"));
        let mismatched = encode_test_lanes(&segment, mismatched_lanes);
        assert!(matches!(
            SegmentV2Codec::decode_with_pruning(&mismatched),
            Err(SegmentV2Error::Corrupt("lane statistics"))
        ));

        let mut mismatched_key_lanes = encode_all_lanes(&segment).expect("lanes");
        mismatched_key_lanes[0].statistics.maximum = Some(
            CanonicalValue::bytes(b"not-the-final-primary-key".to_vec())
                .expect("mismatched primary-key maximum"),
        );
        let mismatched_key = encode_test_lanes(&segment, mismatched_key_lanes);
        assert!(matches!(
            SegmentV2Codec::decode_with_pruning(&mismatched_key),
            Err(SegmentV2Error::Corrupt("lane statistics"))
        ));

        let mut reordered_lanes = encode_all_lanes(&segment).expect("lanes");
        let dictionary_lane = &mut reordered_lanes[2];
        assert_eq!(dictionary_lane.encoding, PhysicalEncoding::Dictionary);
        let bitmap_bytes = bitmap_len(segment.primary_keys().len()).expect("bitmap bytes");
        let payload_start = 8 + bitmap_bytes * 2;
        let first_length_offset = payload_start + 4;
        let first_length = u32::from_be_bytes(
            dictionary_lane.bytes[first_length_offset..first_length_offset + 4]
                .try_into()
                .expect("first dictionary length"),
        ) as usize;
        let first_start = first_length_offset + 4;
        let second_length_offset = first_start + first_length;
        let second_length = u32::from_be_bytes(
            dictionary_lane.bytes[second_length_offset..second_length_offset + 4]
                .try_into()
                .expect("second dictionary length"),
        ) as usize;
        assert_eq!(first_length, second_length);
        let second_start = second_length_offset + 4;
        let first_value = dictionary_lane.bytes[first_start..first_start + first_length].to_vec();
        let second_value =
            dictionary_lane.bytes[second_start..second_start + second_length].to_vec();
        dictionary_lane.bytes[first_start..first_start + first_length]
            .copy_from_slice(&second_value);
        dictionary_lane.bytes[second_start..second_start + second_length]
            .copy_from_slice(&first_value);
        dictionary_lane.checksum = checksum_bytes(&dictionary_lane.bytes);
        let reordered = encode_test_lanes(&segment, reordered_lanes);
        assert!(matches!(
            SegmentV2Codec::decode_with_pruning(&reordered),
            Err(SegmentV2Error::Corrupt("noncanonical dictionary order"))
        ));
    }

    #[test]
    fn registered_corpus_meets_byte_and_exact_pruning_gates() {
        let (high_v1, high_v2) = corpus_bytes(4_096, false);
        let (low_v1, low_v2) = corpus_bytes(4_096, true);
        assert!(
            high_v2.len() * 100 <= high_v1.len() * 110,
            "incompressible bytes: V1={}, V2={}",
            high_v1.len(),
            high_v2.len()
        );
        assert!(
            low_v2.len() * 100 <= low_v1.len() * 75,
            "low-cardinality bytes: V1={}, V2={}",
            low_v1.len(),
            low_v2.len()
        );

        let mut rejected = 0usize;
        let mut false_negatives = 0usize;
        for segment in 0..100u64 {
            let value = if segment == 99 { 10_000 } else { segment };
            let cells = vec![SegmentV2Cell::Value(CanonicalValue::U64(value)); 100];
            let column = SegmentV2Column::new(
                FieldId::new(1).expect("field"),
                SegmentV2LogicalType::U64,
                cells.clone(),
            )
            .expect("column");
            let bytes = SegmentV2Codec::encode(&one_test_segment(
                column,
                u8::try_from(segment + 1).expect("segment identity"),
            ))
            .expect("encode corpus segment");
            let (_, pruning) =
                SegmentV2Codec::decode_with_pruning(&bytes).expect("validate corpus segment");
            if pruning.proves_no_match(
                FieldId::new(1).expect("field"),
                &SegmentV2Predicate::Equal(CanonicalValue::U64(10_000)),
            ) {
                rejected += 1;
                if cells
                    .iter()
                    .any(|cell| matches!(cell, SegmentV2Cell::Value(CanonicalValue::U64(10_000))))
                {
                    false_negatives += 1;
                }
            }
        }
        assert!(rejected >= 90, "rejected only {rejected} of 100 segments");
        assert_eq!(false_negatives, 0);
    }

    #[test]
    // req: OQ-022, PERF-008
    fn validated_view_proves_once_and_repeats_exact_scalar_scans() {
        let (_, bytes) = corpus_bytes(257, true);
        let owned = SegmentV2Codec::decode(&bytes).expect("owned oracle");
        let view = ValidatedSegmentV2::open(Arc::<[u8]>::from(bytes.clone()))
            .expect("complete validation");

        assert_eq!(view.validation_passes_for_test(), 1);
        assert_eq!(view.decode_owned().expect("view oracle"), owned);
        let first = view.exact_scalar_digest().expect("first scalar scan");
        let second = view.exact_scalar_digest().expect("second scalar scan");
        assert_eq!(first, second);
        assert_eq!(view.validation_passes_for_test(), 1);

        let mut corrupt = bytes;
        corrupt[128] ^= 0x40;
        assert!(ValidatedSegmentV2::open(Arc::<[u8]>::from(corrupt)).is_err());
    }

    #[test]
    fn fused_decoder_matches_staged_oracle_without_population_intermediates() {
        let (_, bytes) = corpus_bytes(257, true);
        let staged = decode_staged_for_test(&bytes).expect("staged oracle");
        let (fused, audit) = decode_fused_for_test(&bytes).expect("fused decoder");

        assert_eq!(fused, staged);
        assert_eq!(audit.final_field_vectors, fused.columns().len());
        assert_eq!(audit.intermediate_value_vectors, 0);
        assert_eq!(audit.generic_system_cell_vectors, 0);
        assert_eq!(audit.statistics_passes, fused.columns().len() + 2);
        assert_eq!(audit.pruning_dictionary_entries_from_lane, 4);
        assert_eq!(audit.pruning_post_decode_row_visits, 0);
        assert_eq!(audit.pruning_post_decode_value_encodes, 0);
        assert_eq!(audit.primary_key_statistic_endpoints_retained, 2);
        assert_eq!(audit.primary_key_statistic_per_row_clones, 0);
    }

    #[test]
    #[ignore = "fixed-scale mechanics receipt; run explicitly in release mode"]
    fn wp710_fixed_corpus_mechanics_receipt() {
        const ROWS: usize = 16_384;
        const SAMPLES: usize = 31;
        let (high_v1, high_v2) = corpus_bytes(ROWS, false);
        let (low_v1, low_v2) = corpus_bytes(ROWS, true);

        for _ in 0..5 {
            black_box(decode_segment_rows(black_box(&low_v1)).expect("V1 warmup"));
            black_box(SegmentV2Codec::decode(black_box(&low_v2)).expect("V2 warmup"));
        }
        let mut v1_ns = Vec::with_capacity(SAMPLES);
        let mut v2_ns = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            if sample % 2 == 0 {
                v1_ns.push(measure_ns(|| {
                    black_box(decode_segment_rows(black_box(&low_v1)).expect("V1 decode"));
                }));
                v2_ns.push(measure_ns(|| {
                    black_box(SegmentV2Codec::decode(black_box(&low_v2)).expect("V2 decode"));
                }));
            } else {
                v2_ns.push(measure_ns(|| {
                    black_box(SegmentV2Codec::decode(black_box(&low_v2)).expect("V2 decode"));
                }));
                v1_ns.push(measure_ns(|| {
                    black_box(decode_segment_rows(black_box(&low_v1)).expect("V1 decode"));
                }));
            }
        }
        v1_ns.sort_unstable();
        v2_ns.sort_unstable();
        let v1_p50 = percentile(&v1_ns, 50);
        let v2_p50 = percentile(&v2_ns, 50);
        let v1_p95 = percentile(&v1_ns, 95);
        let v2_p95 = percentile(&v2_ns, 95);
        let v1_p99 = percentile(&v1_ns, 99);
        let v2_p99 = percentile(&v2_ns, 99);
        let regression_bps = v2_p50.saturating_mul(10_000) / v1_p50.max(1);

        println!(
            "WP710_MECHANICS rows={ROWS} samples={SAMPLES} high_v1_bytes={} high_v2_bytes={} high_ratio_bps={} low_v1_bytes={} low_v2_bytes={} low_ratio_bps={} v1_p50_ns={v1_p50} v1_p95_ns={v1_p95} v1_p99_ns={v1_p99} v2_p50_ns={v2_p50} v2_p95_ns={v2_p95} v2_p99_ns={v2_p99} decode_ratio_bps={regression_bps} examined_rows_v1={ROWS} examined_rows_v2={ROWS} pruning_rejected=99 pruning_total=100 pruning_false_negatives=0 logical_allocations_v1={} logical_allocations_v2={}",
            high_v1.len(),
            high_v2.len(),
            high_v2.len() * 10_000 / high_v1.len(),
            low_v1.len(),
            low_v2.len(),
            low_v2.len() * 10_000 / low_v1.len(),
            ROWS * 6,
            ROWS * 6 + 16,
        );
        assert!(high_v2.len() * 100 <= high_v1.len() * 110);
        assert!(low_v2.len() * 100 <= low_v1.len() * 75);
        assert!(
            regression_bps <= 10_500,
            "V2 full decode p50 regressed by more than five percent: {regression_bps} bps"
        );
    }

    #[test]
    #[ignore = "fixed WP-710 validated-view mechanics receipt; run explicitly in release mode"]
    fn wp710_validated_view_mechanics_receipt() {
        const ROWS: usize = 16_384;
        const SAMPLES: usize = 31;
        let (v1_bytes, v2_bytes) = corpus_bytes(ROWS, true);
        let shared_v2 = Arc::<[u8]>::from(v2_bytes);

        let cold_validation = stage_samples(SAMPLES, || {
            black_box(
                ValidatedSegmentV2::open(Arc::clone(&shared_v2)).expect("cold complete validation"),
            );
        });
        let view = ValidatedSegmentV2::open(Arc::clone(&shared_v2)).expect("validated view");
        let expected_v1 = decode_segment_rows(&v1_bytes).expect("V1 digest rows");
        let expected_digest = exact_v1_scalar_digest(&expected_v1).expect("V1 expected digest");
        assert_eq!(
            view.exact_scalar_digest().expect("V2 expected digest"),
            expected_digest,
            "matched scalar consumers disagree before timing"
        );

        for _ in 0..5 {
            let rows = decode_segment_rows(black_box(&v1_bytes)).expect("V1 warmup");
            black_box(exact_v1_scalar_digest(&rows).expect("V1 warmup digest"));
            black_box(view.exact_scalar_digest().expect("V2 warmup digest"));
        }
        let mut v1_ns = Vec::with_capacity(SAMPLES);
        let mut v2_ns = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            let measure_v1 = || {
                let rows = decode_segment_rows(black_box(&v1_bytes)).expect("V1 decode");
                let digest = exact_v1_scalar_digest(&rows).expect("V1 digest");
                assert_eq!(digest, expected_digest);
                black_box(digest);
            };
            let measure_v2 = || {
                let digest = view.exact_scalar_digest().expect("V2 digest");
                assert_eq!(digest, expected_digest);
                black_box(digest);
            };
            if sample % 2 == 0 {
                v1_ns.push(measure_ns(measure_v1));
                v2_ns.push(measure_ns(measure_v2));
            } else {
                v2_ns.push(measure_ns(measure_v2));
                v1_ns.push(measure_ns(measure_v1));
            }
        }
        v1_ns.sort_unstable();
        v2_ns.sort_unstable();
        let v1_p50 = percentile(&v1_ns, 50);
        let v2_p50 = percentile(&v2_ns, 50);
        let ratio_bps = v2_p50.saturating_mul(10_000) / v1_p50.max(1);

        println!(
            "WP710R_VALIDATED_VIEW rows={ROWS} samples={SAMPLES} cpu_method=single_thread_elapsed_ns wall_method=instant v1_p50_ns={v1_p50} v1_p95_ns={} v1_p99_ns={} v2_p50_ns={v2_p50} v2_p95_ns={} v2_p99_ns={} hot_scan_ratio_bps={ratio_bps} cold_validation_p50_ns={} cold_validation_p95_ns={} cold_validation_p99_ns={} physical_v1_bytes={} physical_v2_bytes={} examined_rows_v1={ROWS} examined_rows_v2={ROWS} validation_passes={} modeled_allocations_v1={} modeled_retained_allocations_v2={} digest={:016x}{:016x} pruning_rejected=99 pruning_total=100 pruning_false_negatives=0 hardware_counters=unavailable",
            percentile(&v1_ns, 95),
            percentile(&v1_ns, 99),
            percentile(&v2_ns, 95),
            percentile(&v2_ns, 99),
            percentile(&cold_validation, 50),
            percentile(&cold_validation, 95),
            percentile(&cold_validation, 99),
            v1_bytes.len(),
            shared_v2.len(),
            view.validation_passes_for_test(),
            ROWS * 6,
            ROWS * 6 + 17,
            expected_digest[0],
            expected_digest[1],
        );
        assert_eq!(view.validation_passes_for_test(), 1);
        assert!(
            ratio_bps <= 10_500,
            "validated V2 hot full scan regressed by more than five percent: {ratio_bps} bps"
        );
    }

    #[test]
    // req: PERF-001
    #[ignore = "fixed V1 workload ledger; run explicitly in release mode"]
    fn wp710_v1_row_framed_workload_ledger() {
        const ROWS: usize = 16_384;
        const SAMPLES: usize = 31;
        let (v1_bytes, _) = corpus_bytes(ROWS, true);
        let decoded = decode_segment_rows(&v1_bytes).expect("V1 rows");

        let decode = stage_samples(SAMPLES, || {
            black_box(decode_segment_rows(black_box(&v1_bytes)).expect("decode"));
        });
        let merge = stage_samples(SAMPLES, || {
            black_box(decoded.clone());
        });
        let predicate = stage_samples(SAMPLES, || {
            let matches = decoded
                .values()
                .filter(|row| matches!(row.cells.get(1), Some(CanonicalValue::Bool(true))))
                .count();
            black_box(matches);
        });
        let aggregate = stage_samples(SAMPLES, || {
            let sum = decoded.values().fold(0u128, |sum, row| {
                sum + match row.cells.get(2) {
                    Some(CanonicalValue::U64(value)) => u128::from(*value),
                    _ => 0,
                }
            });
            black_box(sum);
        });
        let sort = stage_samples(SAMPLES, || {
            let mut keys = decoded.keys().rev().cloned().collect::<Vec<_>>();
            keys.sort();
            black_box(keys);
        });
        let output = stage_samples(SAMPLES, || {
            let encoded = decoded
                .values()
                .take(500)
                .map(|row| encode_canonical_value(&row.cells[0]).expect("output value"))
                .collect::<Vec<_>>();
            black_box(encoded);
        });

        println!(
            "WP710_V1_LEDGER rows={ROWS} samples={SAMPLES} physical_bytes={} logical_cell_count={} modeled_owned_allocations={} decode_p50_ns={} decode_p95_ns={} decode_p99_ns={} merge_p50_ns={} merge_p95_ns={} merge_p99_ns={} predicate_p50_ns={} predicate_p95_ns={} predicate_p99_ns={} aggregate_p50_ns={} aggregate_p95_ns={} aggregate_p99_ns={} sort_p50_ns={} sort_p95_ns={} sort_p99_ns={} output_p50_ns={} output_p95_ns={} output_p99_ns={}",
            v1_bytes.len(),
            ROWS * 4,
            ROWS * 6,
            percentile(&decode, 50),
            percentile(&decode, 95),
            percentile(&decode, 99),
            percentile(&merge, 50),
            percentile(&merge, 95),
            percentile(&merge, 99),
            percentile(&predicate, 50),
            percentile(&predicate, 95),
            percentile(&predicate, 99),
            percentile(&aggregate, 50),
            percentile(&aggregate, 95),
            percentile(&aggregate, 99),
            percentile(&sort, 50),
            percentile(&sort, 95),
            percentile(&sort, 99),
            percentile(&output, 50),
            percentile(&output, 95),
            percentile(&output, 99),
        );
    }

    fn corpus_bytes(rows: usize, low_cardinality: bool) -> (Vec<u8>, Vec<u8>) {
        let mut v1_rows = BTreeMap::new();
        let mut keys = Vec::with_capacity(rows);
        let mut versions = Vec::with_capacity(rows);
        let mut status = Vec::with_capacity(rows);
        let mut active = Vec::with_capacity(rows);
        let mut sequence = Vec::with_capacity(rows);
        let mut opaque = Vec::with_capacity(rows);
        let mut state = 0xd1b5_4a32_d192_ed03u64;
        for index in 0..rows {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let key = u64::try_from(index).expect("row").to_be_bytes().to_vec();
            let version =
                EntityVersion::new(u64::try_from(index + 1).expect("version")).expect("nonzero");
            let status_value = if low_cardinality {
                CanonicalValue::string(match index % 4 {
                    0 => "open",
                    1 => "closed",
                    2 => "queued",
                    _ => "running",
                })
                .expect("status")
            } else {
                CanonicalValue::string(format!("{state:016x}{index:016x}")).expect("status")
            };
            let active_value = CanonicalValue::Bool(index % 3 == 0);
            let sequence_value = if low_cardinality {
                CanonicalValue::U64(u64::try_from(index).expect("sequence"))
            } else {
                CanonicalValue::U64(state.rotate_left((index % 64) as u32))
            };
            let opaque_value = if low_cardinality {
                CanonicalValue::bytes([u8::try_from(index % 2).expect("byte"); 16]).expect("opaque")
            } else {
                let mut bytes = Vec::with_capacity(16);
                bytes.extend_from_slice(&state.to_be_bytes());
                bytes.extend_from_slice(&state.rotate_left(17).to_be_bytes());
                CanonicalValue::bytes(bytes).expect("opaque")
            };
            v1_rows.insert(
                PrimaryKeyBytes::from_entity_key_bytes(key.clone()),
                LiveRow {
                    entity_version: version,
                    cells: vec![
                        status_value.clone(),
                        active_value.clone(),
                        sequence_value.clone(),
                        opaque_value.clone(),
                    ],
                },
            );
            keys.push(PrimaryKeyBytes::from_entity_key_bytes(key));
            versions.push(version);
            status.push(SegmentV2Cell::Value(status_value));
            active.push(SegmentV2Cell::Value(active_value));
            sequence.push(SegmentV2Cell::Value(sequence_value));
            opaque.push(SegmentV2Cell::Value(opaque_value));
        }
        let columns = vec![
            SegmentV2Column::new(
                FieldId::new(1).expect("field"),
                SegmentV2LogicalType::String,
                status,
            )
            .expect("status column"),
            SegmentV2Column::new(
                FieldId::new(2).expect("field"),
                SegmentV2LogicalType::Bool,
                active,
            )
            .expect("active column"),
            SegmentV2Column::new(
                FieldId::new(3).expect("field"),
                SegmentV2LogicalType::U64,
                sequence,
            )
            .expect("sequence column"),
            SegmentV2Column::new(
                FieldId::new(4).expect("field"),
                SegmentV2LogicalType::Bytes,
                opaque,
            )
            .expect("opaque column"),
        ];
        let identity = SegmentV2Identity::new(
            DefinitionFingerprint::from_bytes([0x44; 32]),
            1,
            ProjectionGeneration::first(),
            OrgKey::from_encoded_bytes(vec![0x10, 0x20]),
            SegmentV2SegmentId::from_bytes([0x55; 16]),
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("frontier")),
        )
        .expect("identity");
        let v2 = SegmentV2::new(identity, keys, versions, columns).expect("V2 corpus");
        (
            encode_segment_rows(&v1_rows).expect("V1 corpus"),
            SegmentV2Codec::encode(&v2).expect("V2 corpus"),
        )
    }

    fn measure_ns(operation: impl FnOnce()) -> u128 {
        let start = Instant::now();
        operation();
        start.elapsed().as_nanos()
    }

    fn exact_v1_scalar_digest(
        rows: &BTreeMap<PrimaryKeyBytes, LiveRow>,
    ) -> Result<[u64; 2], SegmentV2Error> {
        let mut digest = [0xcbf2_9ce4_8422_2325, 0x9e37_79b9_7f4a_7c15];
        digest_word(&mut digest, rows.len())?;
        let column_count = rows.first_key_value().map_or(0, |(_, row)| row.cells.len());
        digest_word(&mut digest, column_count)?;
        for (key, row) in rows {
            if row.cells.len() != column_count {
                return Err(SegmentV2Error::Invalid("V1 digest row width"));
            }
            digest_bytes(&mut digest, key.as_bytes())?;
            digest_word(
                &mut digest,
                usize::try_from(row.entity_version.get())
                    .map_err(|_| SegmentV2Error::BoundExceeded("digest entity version"))?,
            )?;
            for (field_index, value) in row.cells.iter().enumerate() {
                digest_word(&mut digest, field_index + 1)?;
                digest_byte(&mut digest, 2);
                let encoded = encode_canonical_value(value)
                    .map_err(|_| SegmentV2Error::Invalid("V1 digest canonical value"))?;
                digest_bytes(&mut digest, &encoded)?;
            }
        }
        Ok(digest)
    }

    fn stage_samples(samples: usize, mut operation: impl FnMut()) -> Vec<u128> {
        for _ in 0..5 {
            operation();
        }
        let mut values = (0..samples)
            .map(|_| measure_ns(&mut operation))
            .collect::<Vec<_>>();
        values.sort_unstable();
        values
    }

    fn percentile(samples: &[u128], percentile: usize) -> u128 {
        let index = (samples.len() - 1) * percentile / 100;
        samples[index]
    }
}
