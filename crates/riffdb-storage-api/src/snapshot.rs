//! Owned command snapshots, observations, and canonical read dependencies.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

#[cfg(test)]
use riffdb_types::IndexEpoch;
use riffdb_types::{
    CanonicalRecord, CommitSequence, Date, EntityKey, EntityTypeId, EntityVersion, EnumVariantId,
    IndexEntryKey, IndexEntryKeyBuilder, IndexEpochPosition, IndexId, KeyEncodingError,
    MAX_KEY_BYTES, Timestamp, encode_canonical_record,
};

use crate::{
    AffectedIndexEpochTargets, ExecutablePlanRef, MAX_COMMAND_READ_TARGETS, MAX_READ_DEPENDENCIES,
    MAX_READ_SNAPSHOT_BYTES, MAX_SCAN_PAGE_BYTES, MAX_SCAN_PAGE_ENTRIES, StorageError,
    StorageValueError, StoredEntityRecordV1, canonical_codec_storage_error,
};

/// One complete canonical entity identity.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntityTarget {
    entity_type_id: EntityTypeId,
    key: EntityKey,
}

impl EntityTarget {
    /// Constructs a target whose explicit type matches its key envelope.
    pub fn new(entity_type_id: EntityTypeId, key: EntityKey) -> Result<Self, StorageValueError> {
        if key.entity_type_id() != entity_type_id {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            entity_type_id,
            key,
        })
    }

    /// Returns the stable entity type.
    #[must_use]
    pub const fn entity_type_id(&self) -> EntityTypeId {
        self.entity_type_id
    }

    /// Borrows the complete canonical key.
    #[must_use]
    pub const fn key(&self) -> &EntityKey {
        &self.key
    }

    pub(crate) fn canonical_target_key(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(1 + 4 + 4 + self.key.as_bytes().len());
        output.push(0x01);
        output.extend_from_slice(&self.entity_type_id.to_be_bytes());
        let length =
            u32::try_from(self.key.as_bytes().len()).expect("entity key hard bound fits u32");
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(self.key.as_bytes());
        output
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.key
            .as_bytes()
            .len()
            .checked_add(4 + 4)
            .ok_or(StorageValueError::SizeOverflow)
    }
}

impl fmt::Debug for EntityTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EntityTarget")
            .field("entity_type_id", &self.entity_type_id)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// A storage-owned structurally checked index scan prefix.
///
/// Values can be produced only by [`IndexRangePrefixBuilder`], which appends
/// complete canonical key components. Choosing component methods that match the
/// historical plan remains the coordinator's private IR-aware responsibility.
#[derive(Clone)]
pub struct IndexRangePrefix {
    index_id: IndexId,
    bytes: Vec<u8>,
}

impl IndexRangePrefix {
    /// Returns the exact stable index identity.
    #[must_use]
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Borrows the exact canonical prefix bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl PartialEq for IndexRangePrefix {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for IndexRangePrefix {}

impl PartialOrd for IndexRangePrefix {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexRangePrefix {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl Hash for IndexRangePrefix {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

/// Component-aware producer for one exact leading-component index prefix.
#[derive(Clone)]
pub struct IndexRangePrefixBuilder {
    index_id: IndexId,
    complete_component_count: u16,
    key: IndexEntryKeyBuilder,
}

macro_rules! range_prefix_component_method {
    ($(#[$meta:meta])* $name:ident($value:ident: $type:ty) => $delegate:ident) => {
        $(#[$meta])*
        pub fn $name(&mut self, $value: $type) -> Result<&mut Self, KeyEncodingError> {
            self.key.$delegate($value)?;
            self.complete_component_count = self
                .complete_component_count
                .checked_add(1)
                .expect("the 4 KiB key bound prevents u16 component-count overflow");
            Ok(self)
        }
    };
}

impl IndexRangePrefixBuilder {
    /// Starts the empty exact-prefix bucket for one index.
    #[must_use]
    pub fn new(index_id: IndexId) -> Self {
        Self {
            index_id,
            complete_component_count: 0,
            key: IndexEntryKeyBuilder::new(index_id),
        }
    }

    range_prefix_component_method!(
        /// Appends one complete canonical Boolean component.
        push_bool(value: bool) => push_bool
    );
    range_prefix_component_method!(
        /// Appends one complete canonical unsigned 64-bit component.
        push_u64(value: u64) => push_u64
    );
    range_prefix_component_method!(
        /// Appends one complete canonical signed 64-bit component.
        push_i64(value: i64) => push_i64
    );
    range_prefix_component_method!(
        /// Appends one complete canonical timestamp component.
        push_timestamp(value: Timestamp) => push_timestamp
    );
    range_prefix_component_method!(
        /// Appends one complete canonical date component.
        push_date(value: Date) => push_date
    );
    range_prefix_component_method!(
        /// Appends one complete canonical enum-variant component.
        push_enum_variant(value: EnumVariantId) => push_enum_variant
    );
    range_prefix_component_method!(
        /// Appends one complete canonical UUID component.
        push_uuid(value: &[u8; 16]) => push_uuid
    );
    range_prefix_component_method!(
        /// Appends one complete length-delimited byte component.
        push_bytes(value: &[u8]) => push_bytes
    );
    range_prefix_component_method!(
        /// Appends one complete length-delimited UTF-8 component.
        push_str(value: &str) => push_str
    );

    /// Finishes the opaque prefix after only complete components were appended.
    #[must_use]
    pub fn finish(self) -> IndexRangePrefix {
        IndexRangePrefix {
            index_id: self.index_id,
            bytes: self.key.as_bytes().to_vec(),
        }
    }
}

impl fmt::Debug for IndexRangePrefixBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexRangePrefixBuilder")
            .field("index_id", &self.index_id)
            .field("complete_component_count", &self.complete_component_count)
            .field("bytes", &"[REDACTED]")
            .field("length", &self.key.as_bytes().len())
            .finish()
    }
}

impl fmt::Debug for IndexRangePrefix {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexRangePrefix")
            .field("index_id", &self.index_id)
            .field("bytes", &"[REDACTED]")
            .field("length", &self.bytes.len())
            .finish()
    }
}

/// One exact index-range dependency target.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexRangeTarget(IndexRangePrefix);

impl IndexRangeTarget {
    /// Wraps a structurally checked complete-component prefix.
    #[must_use]
    pub const fn new(prefix: IndexRangePrefix) -> Self {
        Self(prefix)
    }

    /// Borrows the exact prefix.
    #[must_use]
    pub const fn prefix(&self) -> &IndexRangePrefix {
        &self.0
    }

    pub(crate) fn canonical_target_key(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(1 + 4 + 4 + self.0.bytes.len());
        output.push(0x02);
        output.extend_from_slice(&self.0.index_id.to_be_bytes());
        let length = u32::try_from(self.0.bytes.len()).expect("range prefix bound fits u32");
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(&self.0.bytes);
        output
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.0
            .bytes
            .len()
            .checked_add(4 + 4)
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// The current state expected for one entity dependency.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ExpectedEntityState {
    /// The entity must remain absent.
    Absent,
    /// The entity must retain this exact nonzero version.
    Present(EntityVersion),
}

/// One complete source-binding or root-validation observation.
#[derive(Clone, Eq, PartialEq)]
pub enum EntityObservation {
    /// The exact target was absent in the owned read view.
    Absent(EntityTarget),
    /// The exact target had this complete canonical record.
    Present(StoredEntityRecordV1),
}

impl EntityObservation {
    /// Borrows the observed target.
    #[must_use]
    pub fn target(&self) -> &EntityTarget {
        match self {
            Self::Absent(target) => target,
            Self::Present(record) => record.target(),
        }
    }

    /// Returns the corresponding canonical dependency expectation.
    #[must_use]
    pub const fn expected_state(&self) -> ExpectedEntityState {
        match self {
            Self::Absent(_) => ExpectedEntityState::Absent,
            Self::Present(record) => ExpectedEntityState::Present(record.entity_version()),
        }
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        match self {
            Self::Absent(target) => target
                .semantic_bytes()?
                .checked_add(1)
                .ok_or(StorageValueError::SizeOverflow),
            Self::Present(record) => {
                let record_bytes = record.semantic_bytes()?;
                record_bytes
                    .checked_add(1)
                    .ok_or(StorageValueError::SizeOverflow)
            }
        }
    }
}

/// One bounded ordered index entry observed in a range.
#[derive(Clone, Eq, PartialEq)]
pub struct IndexRangeEntry {
    key: IndexEntryKey,
    covered_values: CanonicalRecord,
}

impl IndexRangeEntry {
    /// Constructs one entry whose key belongs to the supplied index.
    pub fn new(
        index_id: IndexId,
        key: IndexEntryKey,
        covered_values: CanonicalRecord,
    ) -> Result<Self, StorageValueError> {
        if key.index_id() != index_id {
            return Err(StorageValueError::IdentityMismatch);
        }
        let encoded = encode_canonical_record(&covered_values)
            .map_err(|error| canonical_codec_storage_error(&error))?;
        if encoded.len() > riffdb_types::MAX_CANONICAL_DOCUMENT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            key,
            covered_values,
        })
    }

    /// Borrows the complete canonical index entry key.
    #[must_use]
    pub const fn key(&self) -> &IndexEntryKey {
        &self.key
    }

    /// Borrows the canonical covered-value record.
    #[must_use]
    pub const fn covered_values(&self) -> &CanonicalRecord {
        &self.covered_values
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let encoded = encode_canonical_record(&self.covered_values)
            .map_err(|error| canonical_codec_storage_error(&error))?;
        self.key
            .as_bytes()
            .len()
            .checked_add(4)
            .and_then(|value| value.checked_add(encoded.len()))
            .and_then(|value| value.checked_add(4))
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// One range observation from the same consistent view as all snapshot entities.
#[derive(Clone, Eq, PartialEq)]
pub struct IndexRangeObservation {
    target: IndexRangeTarget,
    epoch: IndexEpochPosition,
    entries: Vec<IndexRangeEntry>,
}

impl IndexRangeObservation {
    /// Validates entry identity, canonical order, row count, and page bytes.
    pub fn new(
        target: IndexRangeTarget,
        epoch: IndexEpochPosition,
        entries: Vec<IndexRangeEntry>,
    ) -> Result<Self, StorageValueError> {
        if entries.len() > MAX_SCAN_PAGE_ENTRIES {
            return Err(StorageValueError::LimitExceeded);
        }
        let mut total = 0usize;
        let mut prior: Option<&[u8]> = None;
        for entry in &entries {
            if entry.key.index_id() != target.prefix().index_id() {
                return Err(StorageValueError::IdentityMismatch);
            }
            if !entry.key.as_bytes().starts_with(target.prefix().as_bytes()) {
                return Err(StorageValueError::IdentityMismatch);
            }
            if prior.is_some_and(|prior| prior >= entry.key.as_bytes()) {
                return Err(StorageValueError::NonCanonicalOrder);
            }
            total = total
                .checked_add(entry.semantic_bytes()?)
                .ok_or(StorageValueError::SizeOverflow)?;
            if total > MAX_SCAN_PAGE_BYTES {
                return Err(StorageValueError::LimitExceeded);
            }
            prior = Some(entry.key.as_bytes());
        }
        Ok(Self {
            target,
            epoch,
            entries,
        })
    }

    /// Borrows the exact range target.
    #[must_use]
    pub const fn target(&self) -> &IndexRangeTarget {
        &self.target
    }

    /// Returns the observed exact epoch position.
    #[must_use]
    pub const fn epoch(&self) -> IndexEpochPosition {
        self.epoch
    }

    /// Borrows entries in canonical complete-key order.
    #[must_use]
    pub fn entries(&self) -> &[IndexRangeEntry] {
        &self.entries
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let epoch_bytes = match self.epoch {
            IndexEpochPosition::BeforeFirst => 1,
            IndexEpochPosition::Value(_) => 1 + 8,
        };
        self.entries.iter().try_fold(
            self.target
                .semantic_bytes()?
                .checked_add(epoch_bytes + 4)
                .ok_or(StorageValueError::SizeOverflow)?,
            |total, entry| {
                total
                    .checked_add(entry.semantic_bytes()?)
                    .ok_or(StorageValueError::SizeOverflow)
            },
        )
    }
}

/// Closed canonical dependency registry.
#[derive(Clone, Eq, PartialEq)]
pub enum ReadDependency {
    /// One exact entity absence or version observation.
    EntityObservation {
        /// Complete entity target.
        target: EntityTarget,
        /// Expected absence or version.
        expected: ExpectedEntityState,
    },
    /// One exact index-prefix epoch observation.
    IndexRangeEpoch {
        /// Complete-component prefix target.
        target: IndexRangeTarget,
        /// Expected epoch position.
        expected: IndexEpochPosition,
    },
}

impl ReadDependency {
    /// Builds the dependency implied by one entity observation.
    #[must_use]
    pub fn from_entity(observation: &EntityObservation) -> Self {
        Self::EntityObservation {
            target: observation.target().clone(),
            expected: observation.expected_state(),
        }
    }

    /// Builds the dependency implied by one range observation.
    #[must_use]
    pub fn from_range(observation: &IndexRangeObservation) -> Self {
        Self::IndexRangeEpoch {
            target: observation.target().clone(),
            expected: observation.epoch(),
        }
    }

    fn target_key(&self) -> Vec<u8> {
        match self {
            Self::EntityObservation { target, .. } => target.canonical_target_key(),
            Self::IndexRangeEpoch { target, .. } => target.canonical_target_key(),
        }
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        match self {
            Self::EntityObservation { target, expected } => {
                let expected_bytes = match expected {
                    ExpectedEntityState::Absent => 1,
                    ExpectedEntityState::Present(_) => 1 + 8,
                };
                target
                    .semantic_bytes()?
                    .checked_add(1 + expected_bytes)
                    .ok_or(StorageValueError::SizeOverflow)
            }
            Self::IndexRangeEpoch { target, expected } => {
                let expected_bytes = match expected {
                    IndexEpochPosition::BeforeFirst => 1,
                    IndexEpochPosition::Value(_) => 1 + 8,
                };
                target
                    .semantic_bytes()?
                    .checked_add(1 + expected_bytes)
                    .ok_or(StorageValueError::SizeOverflow)
            }
        }
    }
}

/// Canonically ordered, duplicate-free read dependencies.
#[derive(Clone, Eq, PartialEq)]
pub struct ReadDependencies(Vec<ReadDependency>);

impl ReadDependencies {
    /// Canonicalizes dependencies and collapses only equal duplicate observations.
    pub fn new(
        dependencies: impl IntoIterator<Item = ReadDependency>,
    ) -> Result<Self, StorageValueError> {
        let mut keyed = Vec::new();
        for dependency in dependencies {
            if keyed.len() == MAX_READ_DEPENDENCIES {
                return Err(StorageValueError::LimitExceeded);
            }
            keyed.push((dependency.target_key(), dependency));
        }
        keyed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut canonical: Vec<ReadDependency> = Vec::with_capacity(keyed.len());
        let mut prior_key: Option<Vec<u8>> = None;
        for (key, dependency) in keyed {
            if prior_key.as_ref() == Some(&key) {
                if canonical.last() != Some(&dependency) {
                    return Err(StorageValueError::IdentityMismatch);
                }
                continue;
            }
            prior_key = Some(key);
            canonical.push(dependency);
        }
        Ok(Self(canonical))
    }

    /// Constructs an empty dependency set.
    #[must_use]
    pub const fn empty() -> Self {
        Self(Vec::new())
    }

    /// Borrows dependencies in canonical target-byte order.
    #[must_use]
    pub fn as_slice(&self) -> &[ReadDependency] {
        &self.0
    }

    /// Returns the exact expected state for one entity target when present.
    #[must_use]
    pub fn expected_entity_state(&self, requested: &EntityTarget) -> Option<ExpectedEntityState> {
        self.0.iter().find_map(|dependency| match dependency {
            ReadDependency::EntityObservation { target, expected } if target == requested => {
                Some(*expected)
            }
            ReadDependency::EntityObservation { .. } | ReadDependency::IndexRangeEpoch { .. } => {
                None
            }
        })
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.0.iter().try_fold(4usize, |total, dependency| {
            total
                .checked_add(dependency.semantic_bytes()?)
                .ok_or(StorageValueError::SizeOverflow)
        })
    }
}

/// A bounded, envelope-checked persisted index-prefix value.
///
/// This type proves only purpose, format, owner identity, and byte bounds. It
/// does **not** claim that the bytes end at a complete component boundary.
/// Catalog startup validation must check the exact historical `KeySchema`.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StructurallyDecodedIndexRangePrefixV1 {
    index_id: IndexId,
    bytes: Vec<u8>,
}

impl StructurallyDecodedIndexRangePrefixV1 {
    /// Checks only the IR-opaque structural envelope and hard byte bound.
    pub fn new(index_id: IndexId, bytes: Vec<u8>) -> Result<Self, StorageValueError> {
        if bytes.len() < 6 || bytes.len() > MAX_KEY_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        if bytes[..2] != [0x49, 0x01] {
            return Err(StorageValueError::InvalidShape);
        }
        if bytes[2..6] != index_id.to_be_bytes() {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self { index_id, bytes })
    }

    /// Copies a live builder-produced prefix into its durable structural form.
    #[must_use]
    pub fn from_live(prefix: &IndexRangePrefix) -> Self {
        Self {
            index_id: prefix.index_id,
            bytes: prefix.bytes.clone(),
        }
    }

    /// Returns the owner identity repeated in the prefix envelope.
    #[must_use]
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Borrows the exact persisted bytes for storage or catalog validation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.bytes
            .len()
            .checked_add(4 + 4)
            .ok_or(StorageValueError::SizeOverflow)
    }
}

impl fmt::Debug for StructurallyDecodedIndexRangePrefixV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StructurallyDecodedIndexRangePrefixV1")
            .field("index_id", &self.index_id)
            .field("bytes", &"[REDACTED]")
            .field("length", &self.bytes.len())
            .finish()
    }
}

/// One IR-opaque dependency retained in a durable commit record.
#[derive(Clone, Eq, PartialEq)]
pub enum StoredReadDependencyV1 {
    /// One structurally decoded entity target and expected state.
    EntityObservation {
        /// Complete opaque entity target.
        target: EntityTarget,
        /// Expected absence or nonzero version.
        expected: ExpectedEntityState,
    },
    /// One structurally decoded prefix and expected epoch.
    IndexRangeEpoch {
        /// Prefix whose component completeness is catalog-validated at startup.
        target: StructurallyDecodedIndexRangePrefixV1,
        /// Expected epoch position.
        expected: IndexEpochPosition,
    },
}

impl StoredReadDependencyV1 {
    fn target_key(&self) -> Vec<u8> {
        match self {
            Self::EntityObservation { target, .. } => target.canonical_target_key(),
            Self::IndexRangeEpoch { target, .. } => {
                let mut output = Vec::with_capacity(1 + 4 + 4 + target.bytes.len());
                output.push(0x02);
                output.extend_from_slice(&target.index_id.to_be_bytes());
                output.extend_from_slice(
                    &u32::try_from(target.bytes.len())
                        .expect("range-prefix hard bound fits u32")
                        .to_be_bytes(),
                );
                output.extend_from_slice(&target.bytes);
                output
            }
        }
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        match self {
            Self::EntityObservation { target, expected } => {
                let expected_bytes = match expected {
                    ExpectedEntityState::Absent => 1,
                    ExpectedEntityState::Present(_) => 1 + 8,
                };
                target
                    .semantic_bytes()?
                    .checked_add(1 + expected_bytes)
                    .ok_or(StorageValueError::SizeOverflow)
            }
            Self::IndexRangeEpoch { target, expected } => {
                let expected_bytes = match expected {
                    IndexEpochPosition::BeforeFirst => 1,
                    IndexEpochPosition::Value(_) => 1 + 8,
                };
                target
                    .semantic_bytes()?
                    .checked_add(1 + expected_bytes)
                    .ok_or(StorageValueError::SizeOverflow)
            }
        }
    }
}

/// Canonically ordered IR-opaque dependencies retained in one commit record.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredReadDependenciesV1(Vec<StoredReadDependencyV1>);

impl StoredReadDependenciesV1 {
    /// Validates decoded structural dependencies without claiming IR semantics.
    pub fn new(mut dependencies: Vec<StoredReadDependencyV1>) -> Result<Self, StorageValueError> {
        if dependencies.len() > MAX_READ_DEPENDENCIES {
            return Err(StorageValueError::LimitExceeded);
        }
        dependencies.sort_by_cached_key(StoredReadDependencyV1::target_key);
        for pair in dependencies.windows(2) {
            if pair[0].target_key() == pair[1].target_key() {
                return Err(if pair[0] == pair[1] {
                    StorageValueError::Duplicate
                } else {
                    StorageValueError::IdentityMismatch
                });
            }
        }
        Ok(Self(dependencies))
    }

    /// Performs the one-way live-to-durable structural conversion.
    pub fn from_live(dependencies: &ReadDependencies) -> Result<Self, StorageValueError> {
        Self::new(
            dependencies
                .as_slice()
                .iter()
                .map(|dependency| match dependency {
                    ReadDependency::EntityObservation { target, expected } => {
                        StoredReadDependencyV1::EntityObservation {
                            target: target.clone(),
                            expected: *expected,
                        }
                    }
                    ReadDependency::IndexRangeEpoch { target, expected } => {
                        StoredReadDependencyV1::IndexRangeEpoch {
                            target: StructurallyDecodedIndexRangePrefixV1::from_live(
                                target.prefix(),
                            ),
                            expected: *expected,
                        }
                    }
                })
                .collect(),
        )
    }

    /// Borrows dependencies in canonical tag/target-byte order.
    #[must_use]
    pub fn as_slice(&self) -> &[StoredReadDependencyV1] {
        &self.0
    }

    /// Returns an entity expectation retained in this durable dependency set.
    #[must_use]
    pub fn expected_entity_state(&self, requested: &EntityTarget) -> Option<ExpectedEntityState> {
        self.0.iter().find_map(|dependency| match dependency {
            StoredReadDependencyV1::EntityObservation { target, expected }
                if target == requested =>
            {
                Some(*expected)
            }
            StoredReadDependencyV1::EntityObservation { .. }
            | StoredReadDependencyV1::IndexRangeEpoch { .. } => None,
        })
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.0.iter().try_fold(4usize, |total, dependency| {
            total
                .checked_add(dependency.semantic_bytes()?)
                .ok_or(StorageValueError::SizeOverflow)
        })
    }
}

impl fmt::Debug for StoredReadDependencyV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoredReadDependencyV1([REDACTED])")
    }
}

impl fmt::Debug for StoredReadDependenciesV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoredReadDependenciesV1([REDACTED])")
    }
}

/// Exact structurally checked targets for one owned command snapshot.
#[derive(Clone, Eq, PartialEq)]
pub struct SnapshotRequest {
    plan: ExecutablePlanRef,
    binding_targets: Vec<EntityTarget>,
    root_validation_targets: Vec<EntityTarget>,
    range_targets: Vec<IndexRangeTarget>,
}

impl SnapshotRequest {
    /// Validates total count and canonical range-target order.
    pub fn new(
        plan: ExecutablePlanRef,
        binding_targets: Vec<EntityTarget>,
        root_validation_targets: Vec<EntityTarget>,
        mut range_targets: Vec<IndexRangeTarget>,
    ) -> Result<Self, StorageValueError> {
        let total = binding_targets
            .len()
            .checked_add(root_validation_targets.len())
            .and_then(|value| value.checked_add(range_targets.len()))
            .ok_or(StorageValueError::SizeOverflow)?;
        if total > MAX_COMMAND_READ_TARGETS {
            return Err(StorageValueError::LimitExceeded);
        }
        range_targets.sort_unstable();
        if range_targets.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self {
            plan,
            binding_targets,
            root_validation_targets,
            range_targets,
        })
    }

    /// Borrows the exact historical plan reference.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Borrows targets in dense plan-local binding position order.
    #[must_use]
    pub fn binding_targets(&self) -> &[EntityTarget] {
        &self.binding_targets
    }

    /// Borrows targets in dense plan-local root-validation position order.
    #[must_use]
    pub fn root_validation_targets(&self) -> &[EntityTarget] {
        &self.root_validation_targets
    }

    /// Borrows ranges in canonical target-byte order.
    #[must_use]
    pub fn range_targets(&self) -> &[IndexRangeTarget] {
        &self.range_targets
    }
}

/// Dense entity-observation position visited during a snapshot transformation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EntityObservationPosition {
    /// One source binding at its plan-local position.
    Binding(usize),
    /// One internal root validation at its plan-local position.
    RootValidation(usize),
}

/// A fully owned bounded snapshot with no live engine handle.
#[derive(Clone, Eq, PartialEq)]
pub struct ReadSnapshot {
    plan: ExecutablePlanRef,
    observed_through: Option<CommitSequence>,
    bindings: Vec<EntityObservation>,
    root_validations: Vec<EntityObservation>,
    ranges: Vec<IndexRangeObservation>,
    read_dependencies: ReadDependencies,
    semantic_bytes: usize,
}

/// Incremental adapter-facing construction of one bounded owned snapshot.
///
/// Concrete storage engines use this builder while copying records out of one
/// consistent read view. Every observation is charged before the builder retains
/// it, so rejecting the 4 MiB per-range or 16 MiB aggregate ceiling never first
/// materializes an over-limit snapshot.
pub struct ReadSnapshotBuilder<'request> {
    request: &'request SnapshotRequest,
    observed_through: Option<CommitSequence>,
    bindings: Vec<EntityObservation>,
    root_validations: Vec<EntityObservation>,
    ranges: Vec<IndexRangeObservation>,
    dependencies: Vec<ReadDependency>,
    semantic_bytes: usize,
}

/// Incremental materialization of one range inside a [`ReadSnapshotBuilder`].
///
/// Dropping this value without calling [`finish`](Self::finish) leaves the parent
/// incomplete, so the parent cannot produce a snapshot.
pub struct ReadSnapshotRangeBuilder<'builder, 'request> {
    snapshot: &'builder mut ReadSnapshotBuilder<'request>,
    target: IndexRangeTarget,
    epoch: IndexEpochPosition,
    entries: Vec<IndexRangeEntry>,
    entry_semantic_bytes: usize,
    observation_semantic_bytes: usize,
    dependency: ReadDependency,
    prepared_dependency: PreparedDependencyInsert,
}

#[derive(Clone, Copy)]
enum PreparedDependencyInsert {
    Existing,
    Insert { index: usize, semantic_bytes: usize },
}

impl<'request> ReadSnapshotBuilder<'request> {
    /// Starts exact bounded materialization for one request and read-view head.
    pub fn new(
        request: &'request SnapshotRequest,
        observed_through: Option<CommitSequence>,
    ) -> Result<Self, StorageValueError> {
        let semantic_bytes = snapshot_fixed_semantic_bytes(request, observed_through)?
            .checked_add(4)
            .ok_or(StorageValueError::SizeOverflow)?;
        if semantic_bytes > MAX_READ_SNAPSHOT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            request,
            observed_through,
            bindings: Vec::new(),
            root_validations: Vec::new(),
            ranges: Vec::new(),
            dependencies: Vec::new(),
            semantic_bytes,
        })
    }

    /// Retains the next source-binding observation after charging it.
    pub fn push_binding(
        &mut self,
        observation: EntityObservation,
    ) -> Result<(), StorageValueError> {
        let expected = self
            .request
            .binding_targets()
            .get(self.bindings.len())
            .ok_or(StorageValueError::IdentityMismatch)?;
        if observation.target() != expected {
            return Err(StorageValueError::IdentityMismatch);
        }
        self.retain_entity_observation(observation, true)
    }

    /// Retains the next root-validation observation after charging it.
    pub fn push_root_validation(
        &mut self,
        observation: EntityObservation,
    ) -> Result<(), StorageValueError> {
        if self.bindings.len() != self.request.binding_targets().len() {
            return Err(StorageValueError::IdentityMismatch);
        }
        let expected = self
            .request
            .root_validation_targets()
            .get(self.root_validations.len())
            .ok_or(StorageValueError::IdentityMismatch)?;
        if observation.target() != expected {
            return Err(StorageValueError::IdentityMismatch);
        }
        self.retain_entity_observation(observation, false)
    }

    /// Starts the next canonically ordered range observation.
    pub fn begin_range(
        &mut self,
        target: IndexRangeTarget,
        epoch: IndexEpochPosition,
    ) -> Result<ReadSnapshotRangeBuilder<'_, 'request>, StorageValueError> {
        if self.bindings.len() != self.request.binding_targets().len()
            || self.root_validations.len() != self.request.root_validation_targets().len()
            || self.request.range_targets().get(self.ranges.len()) != Some(&target)
        {
            return Err(StorageValueError::IdentityMismatch);
        }

        let dependency = ReadDependency::IndexRangeEpoch {
            target: target.clone(),
            expected: epoch,
        };
        let prepared_dependency = self.prepare_dependency(&dependency)?;
        let epoch_bytes = match epoch {
            IndexEpochPosition::BeforeFirst => 1,
            IndexEpochPosition::Value(_) => 1 + 8,
        };
        let observation_bytes = target
            .semantic_bytes()?
            .checked_add(epoch_bytes + 4)
            .ok_or(StorageValueError::SizeOverflow)?;
        self.preview_charge(observation_bytes, prepared_dependency)?;

        Ok(ReadSnapshotRangeBuilder {
            snapshot: self,
            target,
            epoch,
            entries: Vec::new(),
            entry_semantic_bytes: 0,
            observation_semantic_bytes: observation_bytes,
            dependency,
            prepared_dependency,
        })
    }

    /// Finishes only after every requested semantic position was retained.
    pub fn finish(self) -> Result<ReadSnapshot, StorageValueError> {
        if self.bindings.len() != self.request.binding_targets().len()
            || self.root_validations.len() != self.request.root_validation_targets().len()
            || self.ranges.len() != self.request.range_targets().len()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(ReadSnapshot {
            plan: self.request.plan.clone(),
            observed_through: self.observed_through,
            bindings: self.bindings,
            root_validations: self.root_validations,
            ranges: self.ranges,
            read_dependencies: ReadDependencies(self.dependencies),
            semantic_bytes: self.semantic_bytes,
        })
    }

    fn retain_entity_observation(
        &mut self,
        observation: EntityObservation,
        binding: bool,
    ) -> Result<(), StorageValueError> {
        let dependency = ReadDependency::from_entity(&observation);
        let prepared_dependency = self.prepare_dependency(&dependency)?;
        self.retain_charge(observation.semantic_bytes()?, prepared_dependency)?;
        self.install_dependency(dependency, prepared_dependency);
        if binding {
            self.bindings.push(observation);
        } else {
            self.root_validations.push(observation);
        }
        Ok(())
    }

    fn prepare_dependency(
        &self,
        dependency: &ReadDependency,
    ) -> Result<PreparedDependencyInsert, StorageValueError> {
        match self
            .dependencies
            .binary_search_by(|current| compare_dependency_targets(current, dependency))
        {
            Ok(index) if self.dependencies[index] == *dependency => {
                Ok(PreparedDependencyInsert::Existing)
            }
            Ok(_) => Err(StorageValueError::IdentityMismatch),
            Err(index) => {
                if self.dependencies.len() == MAX_READ_DEPENDENCIES {
                    return Err(StorageValueError::LimitExceeded);
                }
                Ok(PreparedDependencyInsert::Insert {
                    index,
                    semantic_bytes: dependency.semantic_bytes()?,
                })
            }
        }
    }

    fn retain_charge(
        &mut self,
        observation_bytes: usize,
        dependency: PreparedDependencyInsert,
    ) -> Result<(), StorageValueError> {
        self.semantic_bytes = self.preview_charge(observation_bytes, dependency)?;
        Ok(())
    }

    fn preview_charge(
        &self,
        observation_bytes: usize,
        dependency: PreparedDependencyInsert,
    ) -> Result<usize, StorageValueError> {
        let dependency_bytes = match dependency {
            PreparedDependencyInsert::Existing => 0,
            PreparedDependencyInsert::Insert { semantic_bytes, .. } => semantic_bytes,
        };
        let next = self
            .semantic_bytes
            .checked_add(observation_bytes)
            .and_then(|total| total.checked_add(dependency_bytes))
            .ok_or(StorageValueError::SizeOverflow)?;
        if next > MAX_READ_SNAPSHOT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(next)
    }

    fn install_dependency(
        &mut self,
        dependency: ReadDependency,
        prepared: PreparedDependencyInsert,
    ) {
        if let PreparedDependencyInsert::Insert { index, .. } = prepared {
            self.dependencies.insert(index, dependency);
        }
    }
}

impl ReadSnapshotRangeBuilder<'_, '_> {
    /// Retains the next canonical range entry after both byte ceilings pass.
    pub fn push_entry(&mut self, entry: IndexRangeEntry) -> Result<(), StorageValueError> {
        if self.entries.len() == MAX_SCAN_PAGE_ENTRIES {
            return Err(StorageValueError::LimitExceeded);
        }
        if entry.key.index_id() != self.target.prefix().index_id()
            || !entry
                .key
                .as_bytes()
                .starts_with(self.target.prefix().as_bytes())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        if let Some(prior) = self.entries.last()
            && prior.key.as_bytes() >= entry.key.as_bytes()
        {
            return Err(if prior.key.as_bytes() == entry.key.as_bytes() {
                StorageValueError::Duplicate
            } else {
                StorageValueError::NonCanonicalOrder
            });
        }
        let entry_bytes = entry.semantic_bytes()?;
        let range_bytes = self
            .entry_semantic_bytes
            .checked_add(entry_bytes)
            .ok_or(StorageValueError::SizeOverflow)?;
        if range_bytes > MAX_SCAN_PAGE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        let observation_semantic_bytes = self
            .observation_semantic_bytes
            .checked_add(entry_bytes)
            .ok_or(StorageValueError::SizeOverflow)?;
        self.snapshot
            .preview_charge(observation_semantic_bytes, self.prepared_dependency)?;
        self.entry_semantic_bytes = range_bytes;
        self.observation_semantic_bytes = observation_semantic_bytes;
        self.entries.push(entry);
        Ok(())
    }

    /// Retains the complete range observation in its parent snapshot.
    pub fn finish(self) -> Result<(), StorageValueError> {
        self.snapshot
            .retain_charge(self.observation_semantic_bytes, self.prepared_dependency)?;
        self.snapshot
            .install_dependency(self.dependency, self.prepared_dependency);
        self.snapshot.ranges.push(IndexRangeObservation {
            target: self.target,
            epoch: self.epoch,
            entries: self.entries,
        });
        Ok(())
    }
}

fn compare_dependency_targets(left: &ReadDependency, right: &ReadDependency) -> Ordering {
    match (left, right) {
        (
            ReadDependency::EntityObservation { target: left, .. },
            ReadDependency::EntityObservation { target: right, .. },
        ) => left
            .entity_type_id()
            .cmp(&right.entity_type_id())
            .then_with(|| {
                left.key()
                    .as_bytes()
                    .len()
                    .cmp(&right.key().as_bytes().len())
            })
            .then_with(|| left.key().as_bytes().cmp(right.key().as_bytes())),
        (ReadDependency::EntityObservation { .. }, ReadDependency::IndexRangeEpoch { .. }) => {
            Ordering::Less
        }
        (ReadDependency::IndexRangeEpoch { .. }, ReadDependency::EntityObservation { .. }) => {
            Ordering::Greater
        }
        (
            ReadDependency::IndexRangeEpoch { target: left, .. },
            ReadDependency::IndexRangeEpoch { target: right, .. },
        ) => left
            .prefix()
            .index_id()
            .cmp(&right.prefix().index_id())
            .then_with(|| {
                left.prefix()
                    .as_bytes()
                    .len()
                    .cmp(&right.prefix().as_bytes().len())
            })
            .then_with(|| left.prefix().as_bytes().cmp(right.prefix().as_bytes())),
    }
}

impl ReadSnapshot {
    /// Validates request/observation positions, bounds, and complete dependencies.
    pub fn new(
        request: &SnapshotRequest,
        observed_through: Option<CommitSequence>,
        bindings: Vec<EntityObservation>,
        root_validations: Vec<EntityObservation>,
        ranges: Vec<IndexRangeObservation>,
    ) -> Result<Self, StorageValueError> {
        validate_entity_positions(request.binding_targets(), &bindings)?;
        validate_entity_positions(request.root_validation_targets(), &root_validations)?;
        if request.range_targets().len() != ranges.len()
            || request
                .range_targets()
                .iter()
                .zip(&ranges)
                .any(|(target, observation)| target != observation.target())
        {
            return Err(StorageValueError::IdentityMismatch);
        }

        let read_dependencies = ReadDependencies::new(
            bindings
                .iter()
                .chain(&root_validations)
                .map(ReadDependency::from_entity)
                .chain(ranges.iter().map(ReadDependency::from_range)),
        )?;
        let semantic_bytes = read_snapshot_semantic_bytes(
            &request.plan,
            observed_through,
            &bindings,
            &root_validations,
            &ranges,
            &read_dependencies,
        )?;
        Ok(Self {
            plan: request.plan.clone(),
            observed_through,
            bindings,
            root_validations,
            ranges,
            read_dependencies,
            semantic_bytes,
        })
    }

    /// Borrows the exact historical plan reference.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Returns the authoritative application prefix observed by this read view.
    #[must_use]
    pub const fn observed_through(&self) -> Option<CommitSequence> {
        self.observed_through
    }

    /// Borrows source-binding observations in dense plan-local order.
    #[must_use]
    pub fn bindings(&self) -> &[EntityObservation] {
        &self.bindings
    }

    /// Borrows internal root observations in dense plan-local order.
    #[must_use]
    pub fn root_validations(&self) -> &[EntityObservation] {
        &self.root_validations
    }

    /// Borrows canonical range observations.
    #[must_use]
    pub fn ranges(&self) -> &[IndexRangeObservation] {
        &self.ranges
    }

    /// Borrows complete canonical dependency evidence.
    #[must_use]
    pub const fn read_dependencies(&self) -> &ReadDependencies {
        &self.read_dependencies
    }

    /// Returns the exact checked bytes retained by this owned snapshot.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }

    /// Transforms present entity records without exposing snapshot structure.
    ///
    /// Present bindings are visited in dense binding order, followed by present
    /// root validations in dense root-validation order. Absences and range
    /// observations are retained unchanged. A replacement may change only the
    /// record fields; target, entity version, writer, and schema binding must
    /// remain exact. The original read dependencies are retained verbatim and
    /// the aggregate snapshot byte bound is checked again before returning.
    pub fn try_map_present_records<E, F>(self, mut mapper: F) -> Result<Self, E>
    where
        E: From<StorageValueError>,
        F: FnMut(
            EntityObservationPosition,
            StoredEntityRecordV1,
        ) -> Result<StoredEntityRecordV1, E>,
    {
        let Self {
            plan,
            observed_through,
            bindings,
            root_validations,
            ranges,
            read_dependencies,
            semantic_bytes: _,
        } = self;
        let bindings = try_map_present_observations(bindings, false, &mut mapper)?;
        let root_validations = try_map_present_observations(root_validations, true, &mut mapper)?;
        let semantic_bytes = read_snapshot_semantic_bytes(
            &plan,
            observed_through,
            &bindings,
            &root_validations,
            &ranges,
            &read_dependencies,
        )
        .map_err(E::from)?;
        Ok(Self {
            plan,
            observed_through,
            bindings,
            root_validations,
            ranges,
            read_dependencies,
            semantic_bytes,
        })
    }

    /// Constructs the transaction-current read request for this exact snapshot.
    #[must_use]
    pub fn validation_request(&self) -> ValidationReadRequest {
        ValidationReadRequest {
            plan: self.plan.clone(),
            binding_targets: self
                .bindings
                .iter()
                .map(|item| item.target().clone())
                .collect(),
            root_validation_targets: self
                .root_validations
                .iter()
                .map(|item| item.target().clone())
                .collect(),
            range_targets: self
                .ranges
                .iter()
                .map(|item| item.target().clone())
                .collect(),
        }
    }
}

fn snapshot_fixed_semantic_bytes(
    request: &SnapshotRequest,
    observed_through: Option<CommitSequence>,
) -> Result<usize, StorageValueError> {
    snapshot_fixed_semantic_bytes_for_plan(&request.plan, observed_through)
}

fn snapshot_fixed_semantic_bytes_for_plan(
    plan: &ExecutablePlanRef,
    observed_through: Option<CommitSequence>,
) -> Result<usize, StorageValueError> {
    let observed_bytes = match observed_through {
        None => 1,
        Some(_) => 1 + 8,
    };
    plan.semantic_bytes()
        .and_then(|value| value.checked_add(observed_bytes + 4 + 4 + 4))
        .ok_or(StorageValueError::SizeOverflow)
}

fn read_snapshot_semantic_bytes(
    plan: &ExecutablePlanRef,
    observed_through: Option<CommitSequence>,
    bindings: &[EntityObservation],
    root_validations: &[EntityObservation],
    ranges: &[IndexRangeObservation],
    read_dependencies: &ReadDependencies,
) -> Result<usize, StorageValueError> {
    let observation_bytes =
        bindings
            .iter()
            .chain(root_validations)
            .try_fold(0usize, |sum, observation| {
                sum.checked_add(observation.semantic_bytes()?)
                    .ok_or(StorageValueError::SizeOverflow)
            })?;
    let observation_bytes = ranges
        .iter()
        .try_fold(observation_bytes, |sum, observation| {
            sum.checked_add(observation.semantic_bytes()?)
                .ok_or(StorageValueError::SizeOverflow)
        })?;
    let fixed_bytes = snapshot_fixed_semantic_bytes_for_plan(plan, observed_through)?;
    let total = observation_bytes
        .checked_add(read_dependencies.semantic_bytes()?)
        .and_then(|value| value.checked_add(fixed_bytes))
        .ok_or(StorageValueError::SizeOverflow)?;
    if total > MAX_READ_SNAPSHOT_BYTES {
        return Err(StorageValueError::LimitExceeded);
    }
    Ok(total)
}

/// Structurally checked transaction-current read targets.
#[derive(Clone, Eq, PartialEq)]
pub struct ValidationReadRequest {
    plan: ExecutablePlanRef,
    binding_targets: Vec<EntityTarget>,
    root_validation_targets: Vec<EntityTarget>,
    range_targets: Vec<IndexRangeTarget>,
}

impl ValidationReadRequest {
    /// Borrows the exact historical plan reference.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Borrows source-binding targets in dense plan-local order.
    #[must_use]
    pub fn binding_targets(&self) -> &[EntityTarget] {
        &self.binding_targets
    }

    /// Borrows root-validation targets in dense plan-local order.
    #[must_use]
    pub fn root_validation_targets(&self) -> &[EntityTarget] {
        &self.root_validation_targets
    }

    /// Borrows canonical range targets.
    #[must_use]
    pub fn range_targets(&self) -> &[IndexRangeTarget] {
        &self.range_targets
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        semantic_request_bytes(
            &self.plan,
            &self.binding_targets,
            &self.root_validation_targets,
            &self.range_targets,
        )
    }
}

/// One current index-range epoch read inside the short write transaction.
#[derive(Clone, Eq, PartialEq)]
pub struct CurrentRangeObservation {
    target: IndexRangeTarget,
    epoch: IndexEpochPosition,
}

impl CurrentRangeObservation {
    /// Constructs one exact current epoch observation.
    #[must_use]
    pub const fn new(target: IndexRangeTarget, epoch: IndexEpochPosition) -> Self {
        Self { target, epoch }
    }

    /// Borrows its exact target.
    #[must_use]
    pub const fn target(&self) -> &IndexRangeTarget {
        &self.target
    }

    /// Returns the current epoch position.
    #[must_use]
    pub const fn epoch(&self) -> IndexEpochPosition {
        self.epoch
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let epoch_bytes = match self.epoch {
            IndexEpochPosition::BeforeFirst => 1,
            IndexEpochPosition::Value(_) => 1 + 8,
        };
        self.target
            .semantic_bytes()?
            .checked_add(epoch_bytes)
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// Exact current positions for mutation-derived epoch buckets read after validation.
#[derive(Clone, Eq, PartialEq)]
pub struct AffectedEpochCurrentState {
    observations: Vec<CurrentRangeObservation>,
    semantic_bytes: usize,
}

/// Incremental adapter-facing construction of mutation-affected epoch state.
///
/// Storage engines charge every observation before retaining it so the final
/// affected target cannot transiently push owned current state over the
/// aggregate snapshot byte bound.
pub struct AffectedEpochCurrentStateBuilder<'expected> {
    expected: &'expected AffectedIndexEpochTargets,
    observations: Vec<CurrentRangeObservation>,
    semantic_bytes: usize,
}

impl<'expected> AffectedEpochCurrentStateBuilder<'expected> {
    /// Starts exact bounded materialization for one affected target set.
    #[must_use]
    pub fn new(expected: &'expected AffectedIndexEpochTargets) -> Self {
        Self {
            expected,
            observations: Vec::new(),
            semantic_bytes: affected_epoch_current_fixed_semantic_bytes(),
        }
    }

    /// Retains the next canonical epoch observation after charging it.
    pub fn push(&mut self, observation: CurrentRangeObservation) -> Result<(), StorageValueError> {
        let expected = self
            .expected
            .as_slice()
            .get(self.observations.len())
            .ok_or(StorageValueError::IdentityMismatch)?;
        if observation.target() != expected {
            return Err(StorageValueError::IdentityMismatch);
        }
        let next = self
            .semantic_bytes
            .checked_add(observation.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)?;
        if next > MAX_READ_SNAPSHOT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        self.semantic_bytes = next;
        self.observations.push(observation);
        Ok(())
    }

    /// Finishes only after every affected target position was retained.
    pub fn finish(self) -> Result<AffectedEpochCurrentState, StorageValueError> {
        if self.observations.len() != self.expected.as_slice().len() {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(AffectedEpochCurrentState {
            observations: self.observations,
            semantic_bytes: self.semantic_bytes,
        })
    }
}

impl AffectedEpochCurrentState {
    /// Validates one-to-one canonical coverage of the retained affected target set.
    pub fn new(
        expected: &AffectedIndexEpochTargets,
        observations: Vec<CurrentRangeObservation>,
    ) -> Result<Self, StorageValueError> {
        let mut builder = AffectedEpochCurrentStateBuilder::new(expected);
        for observation in observations {
            builder.push(observation)?;
        }
        builder.finish()
    }

    /// Borrows observations in exact affected-target order.
    #[must_use]
    pub fn observations(&self) -> &[CurrentRangeObservation] {
        &self.observations
    }

    /// Returns the bounded transient observation charge.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }
}

const fn affected_epoch_current_fixed_semantic_bytes() -> usize {
    4
}

/// Complete transaction-current values for every requested validation target.
#[derive(Clone, Eq, PartialEq)]
pub struct TransactionCurrentState {
    bindings: Vec<EntityObservation>,
    root_validations: Vec<EntityObservation>,
    ranges: Vec<CurrentRangeObservation>,
    semantic_bytes: usize,
}

/// Incremental adapter-facing construction of transaction-current validation data.
///
/// Storage engines charge each complete entity observation before retaining it,
/// preventing a structurally bounded 4,096-target request from first cloning a
/// multi-gigabyte over-limit current-state value.
pub struct TransactionCurrentStateBuilder<'request> {
    request: &'request ValidationReadRequest,
    bindings: Vec<EntityObservation>,
    root_validations: Vec<EntityObservation>,
    ranges: Vec<CurrentRangeObservation>,
    semantic_bytes: usize,
}

impl<'request> TransactionCurrentStateBuilder<'request> {
    /// Starts exact bounded materialization for one validation request.
    #[must_use]
    pub fn new(request: &'request ValidationReadRequest) -> Self {
        Self {
            request,
            bindings: Vec::new(),
            root_validations: Vec::new(),
            ranges: Vec::new(),
            semantic_bytes: transaction_current_fixed_semantic_bytes(),
        }
    }

    /// Retains the next source-binding observation after charging it.
    pub fn push_binding(
        &mut self,
        observation: EntityObservation,
    ) -> Result<(), StorageValueError> {
        let expected = self
            .request
            .binding_targets()
            .get(self.bindings.len())
            .ok_or(StorageValueError::IdentityMismatch)?;
        if observation.target() != expected {
            return Err(StorageValueError::IdentityMismatch);
        }
        self.retain_charge(observation.semantic_bytes()?)?;
        self.bindings.push(observation);
        Ok(())
    }

    /// Retains the next root-validation observation after charging it.
    pub fn push_root_validation(
        &mut self,
        observation: EntityObservation,
    ) -> Result<(), StorageValueError> {
        if self.bindings.len() != self.request.binding_targets().len() {
            return Err(StorageValueError::IdentityMismatch);
        }
        let expected = self
            .request
            .root_validation_targets()
            .get(self.root_validations.len())
            .ok_or(StorageValueError::IdentityMismatch)?;
        if observation.target() != expected {
            return Err(StorageValueError::IdentityMismatch);
        }
        self.retain_charge(observation.semantic_bytes()?)?;
        self.root_validations.push(observation);
        Ok(())
    }

    /// Retains the next canonical range-epoch observation after charging it.
    pub fn push_range(
        &mut self,
        observation: CurrentRangeObservation,
    ) -> Result<(), StorageValueError> {
        if self.bindings.len() != self.request.binding_targets().len()
            || self.root_validations.len() != self.request.root_validation_targets().len()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let expected = self
            .request
            .range_targets()
            .get(self.ranges.len())
            .ok_or(StorageValueError::IdentityMismatch)?;
        if observation.target() != expected {
            return Err(StorageValueError::IdentityMismatch);
        }
        self.retain_charge(observation.semantic_bytes()?)?;
        self.ranges.push(observation);
        Ok(())
    }

    /// Finishes only after every validation position was retained.
    pub fn finish(self) -> Result<TransactionCurrentState, StorageValueError> {
        if self.bindings.len() != self.request.binding_targets().len()
            || self.root_validations.len() != self.request.root_validation_targets().len()
            || self.ranges.len() != self.request.range_targets().len()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(TransactionCurrentState {
            bindings: self.bindings,
            root_validations: self.root_validations,
            ranges: self.ranges,
            semantic_bytes: self.semantic_bytes,
        })
    }

    fn retain_charge(&mut self, observation_bytes: usize) -> Result<(), StorageValueError> {
        let next = self
            .semantic_bytes
            .checked_add(observation_bytes)
            .ok_or(StorageValueError::SizeOverflow)?;
        if next > MAX_READ_SNAPSHOT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        self.semantic_bytes = next;
        Ok(())
    }
}

impl TransactionCurrentState {
    /// Validates exact positional coverage of one validation request.
    pub fn new(
        request: &ValidationReadRequest,
        bindings: Vec<EntityObservation>,
        root_validations: Vec<EntityObservation>,
        ranges: Vec<CurrentRangeObservation>,
    ) -> Result<Self, StorageValueError> {
        validate_entity_positions(request.binding_targets(), &bindings)?;
        validate_entity_positions(request.root_validation_targets(), &root_validations)?;
        if request.range_targets().len() != ranges.len()
            || request
                .range_targets()
                .iter()
                .zip(&ranges)
                .any(|(target, observation)| target != observation.target())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let semantic_bytes =
            transaction_current_semantic_bytes(&bindings, &root_validations, &ranges)?;
        Ok(Self {
            bindings,
            root_validations,
            ranges,
            semantic_bytes,
        })
    }

    /// Borrows transaction-current binding observations.
    #[must_use]
    pub fn bindings(&self) -> &[EntityObservation] {
        &self.bindings
    }

    /// Borrows transaction-current root observations.
    #[must_use]
    pub fn root_validations(&self) -> &[EntityObservation] {
        &self.root_validations
    }

    /// Borrows transaction-current range epochs.
    #[must_use]
    pub fn ranges(&self) -> &[CurrentRangeObservation] {
        &self.ranges
    }

    /// Returns checked bytes owned by this transaction-current materialization.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }

    /// Transforms present current records while preserving validation structure.
    ///
    /// Present bindings are visited in dense binding order, followed by present
    /// root validations in dense root-validation order. Absences and range
    /// epochs are retained unchanged. A replacement may change only fields;
    /// target, entity version, writer, and schema binding must remain exact.
    /// The aggregate current-state byte bound is checked again before returning.
    pub fn try_map_present_records<E, F>(self, mut mapper: F) -> Result<Self, E>
    where
        E: From<StorageValueError>,
        F: FnMut(
            EntityObservationPosition,
            StoredEntityRecordV1,
        ) -> Result<StoredEntityRecordV1, E>,
    {
        let Self {
            bindings,
            root_validations,
            ranges,
            semantic_bytes: _,
        } = self;
        let bindings = try_map_present_observations(bindings, false, &mut mapper)?;
        let root_validations = try_map_present_observations(root_validations, true, &mut mapper)?;
        let semantic_bytes =
            transaction_current_semantic_bytes(&bindings, &root_validations, &ranges)
                .map_err(E::from)?;
        Ok(Self {
            bindings,
            root_validations,
            ranges,
            semantic_bytes,
        })
    }
}

fn try_map_present_observations<E, F>(
    observations: Vec<EntityObservation>,
    root_validation: bool,
    mapper: &mut F,
) -> Result<Vec<EntityObservation>, E>
where
    E: From<StorageValueError>,
    F: FnMut(EntityObservationPosition, StoredEntityRecordV1) -> Result<StoredEntityRecordV1, E>,
{
    observations
        .into_iter()
        .enumerate()
        .map(|(index, observation)| match observation {
            EntityObservation::Absent(target) => Ok(EntityObservation::Absent(target)),
            EntityObservation::Present(record) => {
                let target = record.target().clone();
                let entity_version = record.entity_version();
                let written_by_contract = record.written_by_contract();
                let schema_binding = record.schema_binding().clone();
                let position = if root_validation {
                    EntityObservationPosition::RootValidation(index)
                } else {
                    EntityObservationPosition::Binding(index)
                };
                let replacement = mapper(position, record)?;
                if replacement.target() != &target
                    || replacement.entity_version() != entity_version
                    || replacement.written_by_contract() != written_by_contract
                    || replacement.schema_binding() != &schema_binding
                {
                    return Err(E::from(StorageValueError::IdentityMismatch));
                }
                Ok(EntityObservation::Present(replacement))
            }
        })
        .collect()
}

fn transaction_current_semantic_bytes(
    bindings: &[EntityObservation],
    root_validations: &[EntityObservation],
    ranges: &[CurrentRangeObservation],
) -> Result<usize, StorageValueError> {
    let entity_bytes = bindings.iter().chain(root_validations).try_fold(
        transaction_current_fixed_semantic_bytes(),
        |total, observation| {
            total
                .checked_add(observation.semantic_bytes()?)
                .ok_or(StorageValueError::SizeOverflow)
        },
    )?;
    let total = ranges.iter().try_fold(entity_bytes, |total, range| {
        total
            .checked_add(range.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    if total > MAX_READ_SNAPSHOT_BYTES {
        return Err(StorageValueError::LimitExceeded);
    }
    Ok(total)
}

const fn transaction_current_fixed_semantic_bytes() -> usize {
    4 + 4 + 4
}

/// Narrow synchronous command snapshot reader.
pub trait SnapshotReader {
    /// Materializes a complete owned snapshot and closes its engine read view.
    fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError>;
}

fn validate_entity_positions(
    expected: &[EntityTarget],
    observed: &[EntityObservation],
) -> Result<(), StorageValueError> {
    if expected.len() != observed.len()
        || expected
            .iter()
            .zip(observed)
            .any(|(target, observation)| target != observation.target())
    {
        return Err(StorageValueError::IdentityMismatch);
    }
    Ok(())
}

fn semantic_request_bytes(
    plan: &ExecutablePlanRef,
    binding_targets: &[EntityTarget],
    root_validation_targets: &[EntityTarget],
    range_targets: &[IndexRangeTarget],
) -> Result<usize, StorageValueError> {
    let initial = plan
        .semantic_bytes()
        .and_then(|value| value.checked_add(4 + 4 + 4))
        .ok_or(StorageValueError::SizeOverflow)?;
    let entities = binding_targets
        .iter()
        .chain(root_validation_targets)
        .try_fold(initial, |total, target| {
            total
                .checked_add(target.semantic_bytes()?)
                .ok_or(StorageValueError::SizeOverflow)
        })?;
    range_targets.iter().try_fold(entities, |total, target| {
        total
            .checked_add(target.semantic_bytes()?)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

macro_rules! redacted_debug {
    ($($type:ty),+ $(,)?) => {
        $(
            impl fmt::Debug for $type {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(concat!(stringify!($type), "([REDACTED])"))
                }
            }
        )+
    };
}

redacted_debug!(
    IndexRangeTarget,
    EntityObservation,
    IndexRangeEntry,
    IndexRangeObservation,
    ReadDependency,
    ReadDependencies,
    SnapshotRequest,
    ReadSnapshot,
    ValidationReadRequest,
    CurrentRangeObservation,
    AffectedEpochCurrentState,
    TransactionCurrentState,
);

#[cfg(test)]
mod builder_tests {
    use riffdb_types::{
        CanonicalRecord, CanonicalValue, CommandId, ContractBundleHash, ContractLineage,
        ContractVersion, EntityKeyBuilder, EntityTypeId, EntityVersion, FieldId,
        IndexEntryKeyBuilder, IndexId, PlanHash,
    };

    use super::*;
    use crate::DurableKeySchemaBindingV1;

    const BULK_BYTES: usize = 900_000;

    fn plan() -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            ContractLineage::new("snapshot-builder").expect("lineage"),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([0x21; 32]),
            CommandId::first(),
            PlanHash::from_bytes([0x22; 32]),
        )
    }

    fn target(value: u64) -> EntityTarget {
        let entity_type = EntityTypeId::first();
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(value).expect("entity key component");
        EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("entity target")
    }

    fn record(payload_bytes: usize) -> CanonicalRecord {
        CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::bytes(vec![0xa5; payload_bytes]).expect("bounded bytes"),
        )])
        .expect("canonical record")
    }

    fn stored(
        plan: &ExecutablePlanRef,
        target: EntityTarget,
        payload: usize,
    ) -> StoredEntityRecordV1 {
        StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            plan.contract_version(),
            DurableKeySchemaBindingV1::from_plan(plan),
            record(payload),
        )
        .expect("stored entity")
    }

    fn present(
        plan: &ExecutablePlanRef,
        target: EntityTarget,
        payload: usize,
    ) -> EntityObservation {
        EntityObservation::Present(stored(plan, target, payload))
    }

    fn with_fields(source: StoredEntityRecordV1, fields: CanonicalRecord) -> StoredEntityRecordV1 {
        StoredEntityRecordV1::new(
            source.target().clone(),
            source.entity_version(),
            source.written_by_contract(),
            source.schema_binding().clone(),
            fields,
        )
        .expect("replacement fields")
    }

    fn range_target(index: IndexId) -> IndexRangeTarget {
        IndexRangeTarget::new(IndexRangePrefixBuilder::new(index).finish())
    }

    fn affected_targets_for_state_bytes(total: usize) -> AffectedIndexEpochTargets {
        let target_count = crate::MAX_AFFECTED_INDEX_EPOCH_TARGETS;
        let desired_prefix_bytes = total
            .checked_sub(affected_epoch_current_fixed_semantic_bytes() + target_count * 9)
            .expect("requested state total covers fixed observation bytes");
        let mut reduction = target_count * MAX_KEY_BYTES - desired_prefix_bytes;
        let mut targets = Vec::with_capacity(target_count);
        for ordinal in 1..=target_count {
            let reduction_for_target = reduction.min(MAX_KEY_BYTES - 6);
            let prefix_bytes = MAX_KEY_BYTES - reduction_for_target;
            reduction -= reduction_for_target;

            let index = IndexId::new(u32::try_from(ordinal).expect("bounded index ordinal"))
                .expect("nonzero index ID");
            let mut prefix = IndexRangePrefixBuilder::new(index);
            if prefix_bytes > 6 {
                let payload_bytes = prefix_bytes
                    .checked_sub(10)
                    .expect("partial reduction leaves one complete byte component");
                prefix
                    .push_bytes(&vec![0xa5; payload_bytes])
                    .expect("bounded prefix payload");
            }
            targets.push(IndexRangeTarget::new(prefix.finish()));
        }
        assert_eq!(reduction, 0);
        AffectedIndexEpochTargets::new(targets).expect("bounded affected target set")
    }

    #[test]
    fn affected_epoch_semantic_framing_matches_the_checked_plan_estimator() {
        let index = IndexId::first();
        let whole = range_target(index);
        assert_eq!(whole.semantic_bytes(), Ok(14));

        let expected =
            AffectedIndexEpochTargets::new(vec![whole.clone()]).expect("one whole-index target");
        let current = AffectedEpochCurrentState::new(
            &expected,
            vec![CurrentRangeObservation::new(
                whole,
                IndexEpochPosition::Value(IndexEpoch::first()),
            )],
        )
        .expect("one current epoch");
        assert_eq!(current.semantic_bytes(), 4 + 14 + 9);

        let mut component_prefix = IndexRangePrefixBuilder::new(index);
        component_prefix
            .push_u64(u64::MAX)
            .expect("fixed-width component");
        assert_eq!(
            IndexRangeTarget::new(component_prefix.finish()).semantic_bytes(),
            Ok(14 + 8)
        );
    }

    fn range_entry(index: IndexId, order: u64, payload: usize) -> IndexRangeEntry {
        let mut key = IndexEntryKeyBuilder::new(index);
        key.push_u64(order).expect("index component");
        IndexRangeEntry::new(
            index,
            key.finish(target(order).key().clone())
                .expect("index entry key"),
            record(payload),
        )
        .expect("range entry")
    }

    fn push_snapshot_bulk(
        builder: &mut ReadSnapshotBuilder<'_>,
        plan: &ExecutablePlanRef,
        targets: &[EntityTarget],
    ) {
        for target in &targets[..18] {
            builder
                .push_binding(present(plan, target.clone(), BULK_BYTES))
                .expect("bulk observation fits");
        }
    }

    fn push_current_bulk(
        builder: &mut TransactionCurrentStateBuilder<'_>,
        plan: &ExecutablePlanRef,
        targets: &[EntityTarget],
    ) {
        for target in &targets[..18] {
            builder
                .push_binding(present(plan, target.clone(), BULK_BYTES))
                .expect("bulk current observation fits");
        }
    }

    #[test]
    fn snapshot_builder_accepts_exact_aggregate_bound_and_rejects_one_over_before_retention() {
        let plan = plan();
        let targets = (1..=19).map(target).collect::<Vec<_>>();
        let request = SnapshotRequest::new(plan.clone(), targets.clone(), Vec::new(), Vec::new())
            .expect("snapshot request");

        let mut exact = ReadSnapshotBuilder::new(&request, None).expect("snapshot builder");
        push_snapshot_bulk(&mut exact, &plan, &targets);
        let baseline = present(&plan, targets[18].clone(), 0);
        let dependency = ReadDependency::from_entity(&baseline);
        let baseline_addition = baseline
            .semantic_bytes()
            .expect("baseline observation bytes")
            .checked_add(
                dependency
                    .semantic_bytes()
                    .expect("baseline dependency bytes"),
            )
            .expect("baseline semantic bytes");
        let remaining = MAX_READ_SNAPSHOT_BYTES - exact.semantic_bytes;
        let final_payload = remaining
            .checked_sub(baseline_addition)
            .expect("bulk leaves room for one bounded payload");
        let final_observation = present(&plan, targets[18].clone(), final_payload);
        exact
            .push_binding(final_observation)
            .expect("exact snapshot bound");
        assert_eq!(exact.semantic_bytes, MAX_READ_SNAPSHOT_BYTES);
        let exact = exact.finish().expect("complete exact snapshot");
        assert_eq!(exact.semantic_bytes(), MAX_READ_SNAPSHOT_BYTES);
        assert_eq!(
            exact.try_map_present_records::<StorageValueError, _>(|position, source| {
                Ok(match position {
                    EntityObservationPosition::Binding(18) => {
                        with_fields(source, record(final_payload + 1))
                    }
                    EntityObservationPosition::Binding(_)
                    | EntityObservationPosition::RootValidation(_) => source,
                })
            }),
            Err(StorageValueError::LimitExceeded)
        );

        let mut over = ReadSnapshotBuilder::new(&request, None).expect("snapshot builder");
        push_snapshot_bulk(&mut over, &plan, &targets);
        let retained = over.bindings.len();
        assert_eq!(
            over.push_binding(present(&plan, targets[18].clone(), final_payload + 1,)),
            Err(StorageValueError::LimitExceeded)
        );
        assert_eq!(over.bindings.len(), retained);
    }

    #[test]
    fn range_builder_accepts_exact_page_bound_rejects_one_over_and_drop_is_retryable() {
        let plan = plan();
        let index = IndexId::first();
        let range = range_target(index);
        let request = SnapshotRequest::new(plan, Vec::new(), Vec::new(), vec![range.clone()])
            .expect("range request");

        let mut exact = ReadSnapshotBuilder::new(&request, None).expect("snapshot builder");
        let mut exact_range = exact
            .begin_range(range.clone(), IndexEpochPosition::BeforeFirst)
            .expect("range builder");
        for order in 1..=4 {
            exact_range
                .push_entry(range_entry(index, order, BULK_BYTES))
                .expect("bulk range entry fits");
        }
        let baseline = range_entry(index, 5, 0);
        let remaining = MAX_SCAN_PAGE_BYTES - exact_range.entry_semantic_bytes;
        let final_payload = remaining
            .checked_sub(baseline.semantic_bytes().expect("baseline bytes"))
            .expect("bulk leaves bounded range space");
        exact_range
            .push_entry(range_entry(index, 5, final_payload))
            .expect("exact range bound");
        assert_eq!(exact_range.entry_semantic_bytes, MAX_SCAN_PAGE_BYTES);
        exact_range.finish().expect("retain exact range");
        assert!(exact.finish().is_ok());

        let mut over = ReadSnapshotBuilder::new(&request, None).expect("snapshot builder");
        let parent_bytes = over.semantic_bytes;
        {
            let mut failed = over
                .begin_range(range.clone(), IndexEpochPosition::BeforeFirst)
                .expect("first range attempt");
            for order in 1..=4 {
                failed
                    .push_entry(range_entry(index, order, BULK_BYTES))
                    .expect("bulk range entry fits");
            }
            let retained = failed.entries.len();
            assert_eq!(
                failed.push_entry(range_entry(index, 5, final_payload + 1)),
                Err(StorageValueError::LimitExceeded)
            );
            assert_eq!(failed.entries.len(), retained);
        }
        assert_eq!(over.semantic_bytes, parent_bytes);
        assert!(over.dependencies.is_empty());
        over.begin_range(range, IndexEpochPosition::BeforeFirst)
            .expect("retry range")
            .finish()
            .expect("retain retry");
        assert!(over.finish().is_ok());
    }

    #[test]
    fn mixed_builder_matches_constructor_and_canonicalizes_equal_dependencies() {
        let plan = plan();
        let entity = target(1);
        let index = IndexId::first();
        let range = range_target(index);
        let request = SnapshotRequest::new(
            plan,
            vec![entity.clone()],
            vec![entity.clone()],
            vec![range.clone()],
        )
        .expect("mixed request");
        let absent = EntityObservation::Absent(entity);
        let entry = range_entry(index, 1, 16);
        let range_observation = IndexRangeObservation::new(
            range.clone(),
            IndexEpochPosition::BeforeFirst,
            vec![entry.clone()],
        )
        .expect("range observation");
        let direct = ReadSnapshot::new(
            &request,
            None,
            vec![absent.clone()],
            vec![absent.clone()],
            vec![range_observation],
        )
        .expect("direct snapshot");

        let mut builder = ReadSnapshotBuilder::new(&request, None).expect("snapshot builder");
        builder
            .push_binding(absent.clone())
            .expect("binding observation");
        builder
            .push_root_validation(absent)
            .expect("equal root observation");
        assert_eq!(builder.dependencies.len(), 1);
        let mut range_builder = builder
            .begin_range(range, IndexEpochPosition::BeforeFirst)
            .expect("range builder");
        range_builder.push_entry(entry).expect("range entry");
        range_builder.finish().expect("retain range");
        assert_eq!(builder.finish().expect("built snapshot"), direct);
    }

    #[test]
    fn snapshot_map_visits_only_present_entities_and_preserves_dependency_evidence() {
        let plan = plan();
        let binding_absent = target(1);
        let binding_present = target(2);
        let root_present = target(3);
        let root_absent = target(4);
        let range = range_target(IndexId::first());
        let request = SnapshotRequest::new(
            plan.clone(),
            vec![binding_absent.clone(), binding_present.clone()],
            vec![root_present.clone(), root_absent.clone()],
            vec![range.clone()],
        )
        .expect("request");
        let ranges = vec![
            IndexRangeObservation::new(
                range,
                IndexEpochPosition::Value(IndexEpoch::first()),
                Vec::new(),
            )
            .expect("range observation"),
        ];
        let snapshot = ReadSnapshot::new(
            &request,
            Some(CommitSequence::first()),
            vec![
                EntityObservation::Absent(binding_absent),
                present(&plan, binding_present, 1),
            ],
            vec![
                present(&plan, root_present, 2),
                EntityObservation::Absent(root_absent),
            ],
            ranges.clone(),
        )
        .expect("snapshot");
        let dependencies = snapshot.read_dependencies().clone();
        let original_bytes = snapshot.semantic_bytes();
        let mut visited = Vec::new();

        let mapped = snapshot
            .try_map_present_records::<StorageValueError, _>(|position, source| {
                visited.push(position);
                let payload = match position {
                    EntityObservationPosition::Binding(1) => 16,
                    EntityObservationPosition::RootValidation(0) => 32,
                    _ => panic!("unexpected present position"),
                };
                Ok(with_fields(source, record(payload)))
            })
            .expect("field-only mapping");

        assert_eq!(
            visited,
            vec![
                EntityObservationPosition::Binding(1),
                EntityObservationPosition::RootValidation(0),
            ]
        );
        assert_eq!(mapped.read_dependencies(), &dependencies);
        assert_eq!(mapped.ranges(), ranges);
        assert!(matches!(mapped.bindings()[0], EntityObservation::Absent(_)));
        assert!(matches!(
            mapped.root_validations()[1],
            EntityObservation::Absent(_)
        ));
        assert!(mapped.semantic_bytes() > original_bytes);
    }

    #[test]
    fn snapshot_map_rejects_every_structural_identity_change() {
        let plan = plan();
        let source_target = target(1);
        let request = SnapshotRequest::new(
            plan.clone(),
            vec![source_target.clone()],
            Vec::new(),
            Vec::new(),
        )
        .expect("request");
        let other_version = ContractVersion::new(2).expect("contract version");
        let replacements = vec![
            StoredEntityRecordV1::new(
                target(2),
                EntityVersion::first(),
                plan.contract_version(),
                DurableKeySchemaBindingV1::from_plan(&plan),
                record(0),
            )
            .expect("changed target"),
            StoredEntityRecordV1::new(
                source_target.clone(),
                EntityVersion::new(2).expect("entity version"),
                plan.contract_version(),
                DurableKeySchemaBindingV1::from_plan(&plan),
                record(0),
            )
            .expect("changed entity version"),
            StoredEntityRecordV1::new(
                source_target.clone(),
                EntityVersion::first(),
                other_version,
                DurableKeySchemaBindingV1::new(
                    plan.contract_lineage().clone(),
                    other_version,
                    ContractBundleHash::from_bytes([0x91; 32]),
                ),
                record(0),
            )
            .expect("changed writer"),
            StoredEntityRecordV1::new(
                source_target.clone(),
                EntityVersion::first(),
                plan.contract_version(),
                DurableKeySchemaBindingV1::new(
                    plan.contract_lineage().clone(),
                    plan.contract_version(),
                    ContractBundleHash::from_bytes([0x92; 32]),
                ),
                record(0),
            )
            .expect("changed schema binding"),
        ];

        for replacement in replacements {
            let snapshot = ReadSnapshot::new(
                &request,
                None,
                vec![present(&plan, source_target.clone(), 0)],
                Vec::new(),
                Vec::new(),
            )
            .expect("snapshot");
            assert_eq!(
                snapshot.try_map_present_records::<StorageValueError, _>(|_, _| {
                    Ok(replacement.clone())
                }),
                Err(StorageValueError::IdentityMismatch)
            );
        }
    }

    #[test]
    fn snapshot_map_short_circuits_callback_errors() {
        let plan = plan();
        let absent = target(1);
        let binding = target(2);
        let root = target(3);
        let range = range_target(IndexId::first());
        let request = SnapshotRequest::new(
            plan.clone(),
            vec![absent.clone(), binding.clone()],
            vec![root.clone()],
            vec![range.clone()],
        )
        .expect("request");
        let snapshot = ReadSnapshot::new(
            &request,
            None,
            vec![
                EntityObservation::Absent(absent),
                present(&plan, binding, 0),
            ],
            vec![present(&plan, root, 0)],
            vec![
                IndexRangeObservation::new(range, IndexEpochPosition::BeforeFirst, Vec::new())
                    .expect("range observation"),
            ],
        )
        .expect("snapshot");
        let mut calls = 0;

        let result = snapshot.try_map_present_records::<StorageValueError, _>(|position, _| {
            calls += 1;
            assert_eq!(position, EntityObservationPosition::Binding(1));
            Err(StorageValueError::InvalidShape)
        });

        assert_eq!(result, Err(StorageValueError::InvalidShape));
        assert_eq!(calls, 1);
    }

    #[test]
    fn transaction_current_map_uses_the_same_sealed_position_order() {
        let plan = plan();
        let binding_absent = target(1);
        let binding_present = target(2);
        let root_present = target(3);
        let range = range_target(IndexId::first());
        let snapshot_request = SnapshotRequest::new(
            plan.clone(),
            vec![binding_absent.clone(), binding_present.clone()],
            vec![root_present.clone()],
            vec![range.clone()],
        )
        .expect("snapshot request");
        let snapshot = ReadSnapshot::new(
            &snapshot_request,
            None,
            vec![
                EntityObservation::Absent(binding_absent.clone()),
                present(&plan, binding_present.clone(), 0),
            ],
            vec![present(&plan, root_present.clone(), 0)],
            vec![
                IndexRangeObservation::new(
                    range.clone(),
                    IndexEpochPosition::BeforeFirst,
                    Vec::new(),
                )
                .expect("range observation"),
            ],
        )
        .expect("snapshot");
        let validation = snapshot.validation_request();
        let current_ranges = vec![CurrentRangeObservation::new(
            range,
            IndexEpochPosition::Value(IndexEpoch::first()),
        )];
        let current = TransactionCurrentState::new(
            &validation,
            vec![
                EntityObservation::Absent(binding_absent),
                present(&plan, binding_present, 1),
            ],
            vec![present(&plan, root_present, 2)],
            current_ranges.clone(),
        )
        .expect("transaction current state");
        let original_bytes = current.semantic_bytes();
        let mut visited = Vec::new();

        let mapped = current
            .try_map_present_records::<StorageValueError, _>(|position, source| {
                visited.push(position);
                Ok(with_fields(source, record(32)))
            })
            .expect("field-only mapping");

        assert_eq!(
            visited,
            vec![
                EntityObservationPosition::Binding(1),
                EntityObservationPosition::RootValidation(0),
            ]
        );
        assert_eq!(mapped.ranges(), current_ranges);
        assert!(matches!(mapped.bindings()[0], EntityObservation::Absent(_)));
        assert!(mapped.semantic_bytes() > original_bytes);
    }

    #[test]
    fn snapshot_builder_rejects_incomplete_order_and_conflicting_duplicate_dependency() {
        let plan = plan();
        let entity = target(1);
        let request = SnapshotRequest::new(
            plan.clone(),
            vec![entity.clone()],
            vec![entity.clone()],
            Vec::new(),
        )
        .expect("request");
        let mut out_of_order = ReadSnapshotBuilder::new(&request, None).expect("builder");
        assert_eq!(
            out_of_order.push_root_validation(EntityObservation::Absent(entity.clone())),
            Err(StorageValueError::IdentityMismatch)
        );
        assert_eq!(
            out_of_order.finish(),
            Err(StorageValueError::IdentityMismatch)
        );

        let mut conflicting = ReadSnapshotBuilder::new(&request, None).expect("builder");
        conflicting
            .push_binding(EntityObservation::Absent(entity.clone()))
            .expect("absent binding");
        let retained_bytes = conflicting.semantic_bytes;
        assert_eq!(
            conflicting.push_root_validation(present(&plan, entity, 0)),
            Err(StorageValueError::IdentityMismatch)
        );
        assert_eq!(conflicting.semantic_bytes, retained_bytes);
        assert!(conflicting.root_validations.is_empty());
    }

    #[test]
    fn transaction_current_builder_accepts_exact_bound_and_rejects_one_over_before_retention() {
        let plan = plan();
        let targets = (1..=19).map(target).collect::<Vec<_>>();
        let snapshot_request =
            SnapshotRequest::new(plan.clone(), targets.clone(), Vec::new(), Vec::new())
                .expect("snapshot request");
        let validation = ReadSnapshot::new(
            &snapshot_request,
            None,
            targets
                .iter()
                .cloned()
                .map(EntityObservation::Absent)
                .collect(),
            Vec::new(),
            Vec::new(),
        )
        .expect("small snapshot")
        .validation_request();

        let mut exact = TransactionCurrentStateBuilder::new(&validation);
        push_current_bulk(&mut exact, &plan, &targets);
        let baseline = present(&plan, targets[18].clone(), 0);
        let remaining = MAX_READ_SNAPSHOT_BYTES - exact.semantic_bytes;
        let final_payload = remaining
            .checked_sub(baseline.semantic_bytes().expect("baseline bytes"))
            .expect("bulk leaves bounded current-state space");
        exact
            .push_binding(present(&plan, targets[18].clone(), final_payload))
            .expect("exact current-state bound");
        assert_eq!(exact.semantic_bytes, MAX_READ_SNAPSHOT_BYTES);
        assert!(exact.finish().is_ok());

        let mut over = TransactionCurrentStateBuilder::new(&validation);
        push_current_bulk(&mut over, &plan, &targets);
        let retained = over.bindings.len();
        assert_eq!(
            over.push_binding(present(&plan, targets[18].clone(), final_payload + 1,)),
            Err(StorageValueError::LimitExceeded)
        );
        assert_eq!(over.bindings.len(), retained);

        let incomplete = TransactionCurrentStateBuilder::new(&validation);
        assert_eq!(
            incomplete.finish(),
            Err(StorageValueError::IdentityMismatch)
        );
    }

    #[test]
    fn affected_epoch_builder_accepts_exact_aggregate_bound() {
        let expected = affected_targets_for_state_bytes(MAX_READ_SNAPSHOT_BYTES);
        let mut builder = AffectedEpochCurrentStateBuilder::new(&expected);
        for target in expected.as_slice() {
            builder
                .push(CurrentRangeObservation::new(
                    target.clone(),
                    IndexEpochPosition::BeforeFirst,
                ))
                .expect("observation through exact bound");
        }
        assert_eq!(builder.semantic_bytes, MAX_READ_SNAPSHOT_BYTES);
        assert_eq!(
            builder
                .finish()
                .expect("complete exact state")
                .semantic_bytes(),
            MAX_READ_SNAPSHOT_BYTES
        );
    }

    #[test]
    fn affected_epoch_builder_rejects_one_over_before_retention() {
        let expected = affected_targets_for_state_bytes(MAX_READ_SNAPSHOT_BYTES + 1);
        let mut builder = AffectedEpochCurrentStateBuilder::new(&expected);
        let (last, prefix) = expected.as_slice().split_last().expect("nonempty targets");
        for target in prefix {
            builder
                .push(CurrentRangeObservation::new(
                    target.clone(),
                    IndexEpochPosition::BeforeFirst,
                ))
                .expect("prefix remains below bound");
        }
        let retained = builder.observations.len();
        assert_eq!(
            builder.push(CurrentRangeObservation::new(
                last.clone(),
                IndexEpochPosition::BeforeFirst,
            )),
            Err(StorageValueError::LimitExceeded)
        );
        assert_eq!(builder.observations.len(), retained);
    }

    #[test]
    fn affected_epoch_builder_rejects_incomplete_state() {
        let expected = AffectedIndexEpochTargets::new(vec![range_target(IndexId::first())])
            .expect("affected targets");
        assert_eq!(
            AffectedEpochCurrentStateBuilder::new(&expected).finish(),
            Err(StorageValueError::IdentityMismatch)
        );
    }
}
