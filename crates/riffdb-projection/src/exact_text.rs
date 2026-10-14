//! Partition-scoped exact binary UTF-8 derived index (ADR-0131).

#[path = "exact_text_admission.rs"]
mod admission;
pub use admission::ExactTextRebuildAdmissionErrorV1;
#[path = "exact_text_checkpoint_admission.rs"]
mod checkpoint_admission;
#[path = "exact_text_filtered_admission.rs"]
mod filtered_admission;
#[path = "exact_text_successor_admission.rs"]
mod successor_admission;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU16;

use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, EntityKey, EntityKeyHash, ExactTextNeedleV1,
    ExactTextOperatorV1, ExactTextOrderV1, ExactTextProfileV1, MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1,
    MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1, MAX_EXACT_TEXT_VALUE_BYTES_V1, MAX_KEY_BYTES,
    PartitionKeyHash, ProjectionGeneration, canonical_record_encoded_len,
    canonical_value_encoded_len, decode_canonical_value, encode_canonical_record,
    encode_canonical_value, hash_entity_key,
};

/// Provider-owned rebuildable checkpoint format identity.
pub const EXACT_TEXT_PROVIDER_STATE_FORMAT_VERSION_V1: u16 = 1;
/// Activated provider checkpoint carrying canonical entity keys for hydration.
pub const EXACT_TEXT_PROVIDER_STATE_FORMAT_VERSION_V2: u16 = 2;
/// Additive provider checkpoint carrying one compiler-bound typed equality dimension.
pub const EXACT_TEXT_PROVIDER_STATE_FORMAT_VERSION_V3: u16 = 3;
/// Exact maximum canonical checkpoint size under V1 row/value bounds.
pub const MAX_EXACT_TEXT_CHECKPOINT_BYTES_V1: usize = 4
    + 2
    + 1
    + 1
    + 32
    + 8
    + 8
    + 4
    + MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 * (32 + 2 + MAX_EXACT_TEXT_VALUE_BYTES_V1);
/// Exact maximum canonical checkpoint size under V2 row/value bounds.
pub const MAX_EXACT_TEXT_CHECKPOINT_BYTES_V2: usize = 4
    + 2
    + 1
    + 1
    + 32
    + 8
    + 8
    + 4
    + MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1
        * (2 + MAX_KEY_BYTES
            + 2
            + MAX_EXACT_TEXT_VALUE_BYTES_V1
            + 4
            + MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1);
/// Exact maximum V3 wrapper bytes around one V2 state plus one typed filter value per row.
pub const MAX_EXACT_TEXT_CHECKPOINT_BYTES_V3: usize = 18
    + MAX_EXACT_TEXT_CHECKPOINT_BYTES_V2
    + MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 * (32 + 4 + MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1);

/// Maximum rows released by one exact result window.
pub const MAX_EXACT_TEXT_PAGE_ROWS_V1: u16 = 500;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PostingList {
    /// Entity identities in `(value ASC, entity-key ASC)` order.
    ascending: Vec<EntityKeyHash>,
    /// Indices into `ascending` in `(value DESC, entity-key ASC)` order.
    descending: Vec<u16>,
}

type PostingMap = BTreeMap<Vec<u8>, PostingList>;
type ExactTermSets<'a> = (BTreeSet<&'a [u8]>, BTreeSet<&'a [u8]>, BTreeSet<&'a [u8]>);

/// One bounded exact result window and its whole admitted-population count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextResultPageV1 {
    rows: Vec<EntityKeyHash>,
    exact_total: u64,
}

impl ExactTextResultPageV1 {
    /// Directly selected row identities in the compiler-declared total order.
    #[must_use]
    pub fn rows(&self) -> &[EntityKeyHash] {
        &self.rows
    }

    /// Exact count before offset, limit, or output shaping.
    #[must_use]
    pub const fn exact_total(&self) -> u64 {
        self.exact_total
    }
}

/// One checked row update for a single partition index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactTextIndexMutationV1 {
    /// Insert or replace one exact UTF-8 value.
    Upsert {
        /// Exact authoritative row identity.
        row: EntityKeyHash,
        /// Canonical bounded UTF-8 post-image.
        value: String,
    },
    /// Remove one row from every posting.
    Delete {
        /// Exact authoritative row identity.
        row: EntityKeyHash,
    },
}

impl ExactTextIndexMutationV1 {
    /// Constructs a bounded exact-value upsert.
    pub fn upsert(row: EntityKeyHash, value: &str) -> Result<Self, ExactTextProviderErrorV1> {
        if value.len() > MAX_EXACT_TEXT_VALUE_BYTES_V1 {
            return Err(ExactTextProviderErrorV1::ValueTooLong);
        }
        Ok(Self::Upsert {
            row,
            value: value.to_owned(),
        })
    }

    /// Constructs a row deletion.
    #[must_use]
    pub const fn delete(row: EntityKeyHash) -> Self {
        Self::Delete { row }
    }

    const fn row(&self) -> EntityKeyHash {
        match self {
            Self::Upsert { row, .. } | Self::Delete { row } => *row,
        }
    }
}

/// Exact provider state for one complete capability/policy partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextPartitionIndexV1 {
    partition: PartitionKeyHash,
    generation: ProjectionGeneration,
    frontier: Option<CommitSequence>,
    profile: ExactTextProfileV1,
    rows: BTreeMap<EntityKeyHash, String>,
    equals: PostingMap,
    prefixes: PostingMap,
    suffixes: PostingMap,
    contains: PostingMap,
}

impl ExactTextPartitionIndexV1 {
    /// Creates an empty provider generation for one exact partition.
    #[must_use]
    pub fn new(
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
        profile: ExactTextProfileV1,
    ) -> Self {
        Self {
            partition,
            generation,
            frontier: None,
            profile,
            rows: BTreeMap::new(),
            equals: BTreeMap::new(),
            prefixes: BTreeMap::new(),
            suffixes: BTreeMap::new(),
            contains: BTreeMap::new(),
        }
    }

    /// Applies one complete epoch atomically to this bounded derived state.
    pub fn apply(
        &mut self,
        epoch: CommitSequence,
        mutations: &[ExactTextIndexMutationV1],
    ) -> Result<(), ExactTextProviderErrorV1> {
        if self.frontier.is_some_and(|frontier| epoch <= frontier) {
            return Err(ExactTextProviderErrorV1::NonAdvancingEpoch);
        }
        let mut unique = BTreeSet::new();
        if mutations
            .iter()
            .any(|mutation| !unique.insert(mutation.row()))
        {
            return Err(ExactTextProviderErrorV1::DuplicateRowMutation);
        }
        let mut next_count = self.rows.len();
        for mutation in mutations {
            match mutation {
                ExactTextIndexMutationV1::Upsert { row, value } => {
                    // ADR-0172: the profile transform runs BEFORE the bound
                    // check. Folding can expand a value, so a limit measured on
                    // the source would not bound what is actually stored.
                    if self.profile.matched_form(value).len() > MAX_EXACT_TEXT_VALUE_BYTES_V1 {
                        return Err(ExactTextProviderErrorV1::ValueTooLong);
                    }
                    if !self.rows.contains_key(row) {
                        next_count = next_count.saturating_add(1);
                    }
                }
                ExactTextIndexMutationV1::Delete { row } => {
                    if self.rows.contains_key(row) {
                        next_count = next_count.saturating_sub(1);
                    }
                }
            }
        }
        if next_count > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextProviderErrorV1::PartitionRowLimit);
        }
        let mut prepared = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let row = mutation.row();
            if let Some(previous) = self.rows.remove(&row) {
                remove_value_from_postings_sorted(
                    row,
                    &previous,
                    &mut self.equals,
                    &mut self.prefixes,
                    &mut self.suffixes,
                    &mut self.contains,
                );
            }
            if let ExactTextIndexMutationV1::Upsert { value, .. } = mutation {
                // Postings, ordering, and the stored row must all be the SAME
                // representation. Building terms from the source while storing
                // the folded form would index text the needle can never match.
                prepared.push((row, self.profile.matched_form(value)));
            }
        }
        for (row, matched) in prepared {
            add_value_to_postings_v1_sorted(
                row,
                &matched,
                &self.rows,
                &mut self.equals,
                &mut self.prefixes,
                &mut self.suffixes,
                &mut self.contains,
            );
            self.rows.insert(row, matched);
        }
        self.frontier = Some(epoch);
        Ok(())
    }

    /// Rebuilds a disjoint generation at one exact authoritative frontier.
    pub fn rebuild(
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
        profile: ExactTextProfileV1,
        rows: &BTreeMap<EntityKeyHash, String>,
    ) -> Result<Self, ExactTextProviderErrorV1> {
        if rows.len() > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1
            || rows
                .values()
                .any(|value| value.len() > MAX_EXACT_TEXT_VALUE_BYTES_V1)
        {
            return Err(ExactTextProviderErrorV1::PartitionRowLimit);
        }
        let (equals, prefixes, suffixes, contains) = build_postings(rows);
        Ok(Self {
            partition,
            generation,
            frontier: Some(frontier),
            profile,
            rows: rows.clone(),
            equals,
            prefixes,
            suffixes,
            contains,
        })
    }

    /// Exact posting lookup; work is independent of offset or nonmatching rows.
    #[must_use]
    pub fn lookup(
        &self,
        operator: ExactTextOperatorV1,
        needle: &ExactTextNeedleV1,
    ) -> &[EntityKeyHash] {
        let postings = match operator {
            ExactTextOperatorV1::Equals => &self.equals,
            ExactTextOperatorV1::StartsWith => &self.prefixes,
            ExactTextOperatorV1::EndsWith => &self.suffixes,
            ExactTextOperatorV1::Contains => &self.contains,
        };
        postings
            .get(self.profile.matched_form(needle.as_str()).as_bytes())
            .map_or(&[], |posting| posting.ascending.as_slice())
    }

    /// Returns one exact whole-result count and indexed ordinal window.
    ///
    /// The maintained posting carries both declared total orders. Selection is
    /// a direct bounded slice and never walks or discards earlier result rows.
    pub fn result_page(
        &self,
        operator: ExactTextOperatorV1,
        needle: &ExactTextNeedleV1,
        order: ExactTextOrderV1,
        offset: u32,
        limit: NonZeroU16,
    ) -> Result<ExactTextResultPageV1, ExactTextProviderErrorV1> {
        let postings = match operator {
            ExactTextOperatorV1::Equals => &self.equals,
            ExactTextOperatorV1::StartsWith => &self.prefixes,
            ExactTextOperatorV1::EndsWith => &self.suffixes,
            ExactTextOperatorV1::Contains => &self.contains,
        };
        let Some(posting) = postings.get(self.profile.matched_form(needle.as_str()).as_bytes())
        else {
            return Ok(ExactTextResultPageV1 {
                rows: Vec::new(),
                exact_total: 0,
            });
        };
        let exact_total = u64::try_from(posting.ascending.len())
            .map_err(|_| ExactTextProviderErrorV1::CardinalityOverflow)?;
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        if start >= posting.ascending.len() {
            return Ok(ExactTextResultPageV1 {
                rows: Vec::new(),
                exact_total,
            });
        }
        let end = start
            .saturating_add(usize::from(limit.get()))
            .min(posting.ascending.len());
        let rows = match order {
            ExactTextOrderV1::ValueAscEntityKey => posting.ascending[start..end].to_vec(),
            ExactTextOrderV1::ValueDescEntityKey => posting.descending[start..end]
                .iter()
                .map(|position| posting.ascending[usize::from(*position)])
                .collect(),
        };
        Ok(ExactTextResultPageV1 { rows, exact_total })
    }

    /// Canonical source rows retained for deterministic rebuild/reference checks.
    #[must_use]
    pub const fn rows(&self) -> &BTreeMap<EntityKeyHash, String> {
        &self.rows
    }

    /// Exact partition identity of this complete policy-aligned state.
    #[must_use]
    pub const fn partition(&self) -> PartitionKeyHash {
        self.partition
    }

    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Latest completely applied provider epoch, when initialized.
    #[must_use]
    pub const fn frontier(&self) -> Option<CommitSequence> {
        self.frontier
    }

    /// Rebuilds compact postings without changing logical state or frontier.
    pub fn compact(&mut self) {
        let (equals, prefixes, suffixes, contains) = build_postings(&self.rows);
        self.equals = equals;
        self.prefixes = prefixes;
        self.suffixes = suffixes;
        self.contains = contains;
    }

    /// Canonical provider-owned checkpoint; postings are derived on recovery.
    pub fn to_checkpoint_bytes(&self) -> Result<Vec<u8>, ExactTextProviderErrorV1> {
        let frontier = self
            .frontier
            .ok_or(ExactTextProviderErrorV1::MissingFrontier)?;
        let mut bytes = Vec::with_capacity(60 + self.rows.len() * 48);
        bytes.extend_from_slice(b"RXTS");
        bytes.extend_from_slice(&EXACT_TEXT_PROVIDER_STATE_FORMAT_VERSION_V1.to_be_bytes());
        bytes.push(self.profile as u8);
        bytes.push(0);
        bytes.extend_from_slice(self.partition.as_bytes());
        bytes.extend_from_slice(&self.generation.get().to_be_bytes());
        bytes.extend_from_slice(&frontier.get().to_be_bytes());
        bytes.extend_from_slice(&(self.rows.len() as u32).to_be_bytes());
        for (row, value) in &self.rows {
            bytes.extend_from_slice(row.as_bytes());
            bytes.extend_from_slice(&(value.len() as u16).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        if bytes.len() > MAX_EXACT_TEXT_CHECKPOINT_BYTES_V1 {
            return Err(ExactTextProviderErrorV1::CheckpointTooLarge);
        }
        Ok(bytes)
    }

    /// Strictly recovers V1 or refuses mixed/unknown state before use.
    pub fn from_checkpoint_bytes(bytes: &[u8]) -> Result<Self, ExactTextProviderErrorV1> {
        if bytes.len() < 60 || bytes.len() > MAX_EXACT_TEXT_CHECKPOINT_BYTES_V1 {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        if &bytes[..4] != b"RXTS" || u16::from_be_bytes([bytes[4], bytes[5]]) != 1 {
            return Err(ExactTextProviderErrorV1::UnsupportedFormat);
        }
        let profile = ExactTextProfileV1::from_discriminant(bytes[6])
            .ok_or(ExactTextProviderErrorV1::UnsupportedFormat)?;
        if bytes[7] != 0 {
            return Err(ExactTextProviderErrorV1::UnsupportedFormat);
        }
        let partition = PartitionKeyHash::from_bytes(read_array(bytes, 8)?);
        let generation = ProjectionGeneration::new(read_u64(bytes, 40)?)
            .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let frontier = CommitSequence::new(read_u64(bytes, 48)?)
            .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let count = usize::try_from(read_u32(bytes, 56)?)
            .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
        if count > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextProviderErrorV1::PartitionRowLimit);
        }
        let mut cursor = 60;
        let mut rows = BTreeMap::new();
        for _ in 0..count {
            let row = EntityKeyHash::from_bytes(read_array(bytes, cursor)?);
            cursor += 32;
            let length = usize::from(read_u16(bytes, cursor)?);
            cursor += 2;
            let end = cursor
                .checked_add(length)
                .filter(|end| *end <= bytes.len())
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            if length > MAX_EXACT_TEXT_VALUE_BYTES_V1 {
                return Err(ExactTextProviderErrorV1::ValueTooLong);
            }
            let value = std::str::from_utf8(&bytes[cursor..end])
                .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?
                .to_owned();
            if rows.insert(row, value).is_some() {
                return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
            }
            cursor = end;
        }
        if cursor != bytes.len() {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        Self::rebuild(partition, generation, frontier, profile, &rows)
    }
}

/// Closed provider failures with no values or partition cardinalities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactTextProviderErrorV1 {
    /// Indexed UTF-8 value exceeds its compiled maximum.
    ValueTooLong,
    /// Policy partition exceeds the fixed V1 row ceiling.
    PartitionRowLimit,
    /// One epoch attempted two writes for the same row.
    DuplicateRowMutation,
    /// Derived epochs must advance strictly.
    NonAdvancingEpoch,
    /// A checkpoint cannot be emitted before the first applied epoch.
    MissingFrontier,
    /// Checkpoint framing or contents are malformed.
    InvalidCheckpoint,
    /// Checkpoint belongs to an unknown/mixed format or semantic profile.
    UnsupportedFormat,
    /// Canonical checkpoint exceeds its fixed maximum.
    CheckpointTooLarge,
    /// Requested output page exceeds the fixed V1 release bound.
    PageLimit,
    /// A platform could not represent the maintained exact cardinality.
    CardinalityOverflow,
    /// Two distinct canonical keys produced the same immutable tie-breaker hash.
    EntityKeyHashCollision,
    /// Compiler-shaped output is not one canonical record.
    OutputInvalid,
    /// Compiler-shaped output exceeds the exact provider's per-row bound.
    OutputTooLarge,
}

impl fmt::Display for ExactTextProviderErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "exact text provider unavailable: {self:?}")
    }
}

impl Error for ExactTextProviderErrorV1 {}

/// One bounded exact result window carrying authoritative entity keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextResultRowV2 {
    key: EntityKey,
    output: CanonicalRecord,
}

impl ExactTextResultRowV2 {
    /// Canonical authoritative entity key.
    #[must_use]
    pub const fn key(&self) -> &EntityKey {
        &self.key
    }

    /// Compiler-shaped canonical output, already policy partitioned.
    #[must_use]
    pub const fn output(&self) -> &CanonicalRecord {
        &self.output
    }
}

/// One bounded exact result window with compiler-shaped output rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextResultPageV2 {
    rows: Vec<ExactTextResultRowV2>,
    exact_total: u64,
}

impl ExactTextResultPageV2 {
    /// Directly selected canonical entity keys in the declared total order.
    #[must_use]
    pub fn rows(&self) -> &[ExactTextResultRowV2] {
        &self.rows
    }

    /// Exact count before offset, limit, or output shaping.
    #[must_use]
    pub const fn exact_total(&self) -> u64 {
        self.exact_total
    }
}

/// One checked V2 row update. The hash tie-breaker is always derived by RiffDB.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactTextIndexMutationV2 {
    /// Insert or replace one exact UTF-8 value.
    Upsert {
        /// Canonical authoritative entity key.
        row: EntityKey,
        /// Canonical bounded UTF-8 post-image.
        value: String,
        /// Compiler-shaped canonical output record for this policy partition.
        output: CanonicalRecord,
    },
    /// Remove one row from every posting.
    Delete {
        /// Canonical authoritative entity key.
        row: EntityKey,
    },
}

impl ExactTextIndexMutationV2 {
    /// Constructs a bounded exact-value upsert.
    pub fn upsert(
        row: EntityKey,
        value: &str,
        output: CanonicalRecord,
    ) -> Result<Self, ExactTextProviderErrorV1> {
        if value.len() > MAX_EXACT_TEXT_VALUE_BYTES_V1 {
            return Err(ExactTextProviderErrorV1::ValueTooLong);
        }
        if canonical_record_encoded_len(&output)
            .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?
            > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1
        {
            return Err(ExactTextProviderErrorV1::OutputTooLarge);
        }
        Ok(Self::Upsert {
            row,
            value: value.to_owned(),
            output,
        })
    }

    /// Constructs a row deletion.
    #[must_use]
    pub const fn delete(row: EntityKey) -> Self {
        Self::Delete { row }
    }

    fn row(&self) -> &EntityKey {
        match self {
            Self::Upsert { row, .. } | Self::Delete { row } => row,
        }
    }
}

/// Activated exact provider state retaining keys needed for typed row hydration.
///
/// V1 remains byte-frozen and readable. V2 wraps the same bounded posting
/// semantics, derives every tie-breaker hash from its canonical key, and
/// persists the key so recovery never needs a scan or hash reversal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextPartitionIndexV2 {
    index: ExactTextPartitionIndexV1,
    keys: BTreeMap<EntityKeyHash, EntityKey>,
    outputs: BTreeMap<EntityKeyHash, CanonicalRecord>,
    equals: PostingMap,
    prefixes: PostingMap,
    suffixes: PostingMap,
    contains: PostingMap,
}

impl ExactTextPartitionIndexV2 {
    /// Creates an empty activated provider generation for one exact partition.
    #[must_use]
    pub fn new(
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
        profile: ExactTextProfileV1,
    ) -> Self {
        Self {
            index: ExactTextPartitionIndexV1::new(partition, generation, profile),
            keys: BTreeMap::new(),
            outputs: BTreeMap::new(),
            equals: BTreeMap::new(),
            prefixes: BTreeMap::new(),
            suffixes: BTreeMap::new(),
            contains: BTreeMap::new(),
        }
    }

    /// Applies one complete epoch atomically after deriving and checking hashes.
    pub fn apply(
        &mut self,
        epoch: CommitSequence,
        mutations: &[ExactTextIndexMutationV2],
    ) -> Result<(), ExactTextProviderErrorV1> {
        let mut inner = Vec::with_capacity(mutations.len());
        let mut seen = BTreeMap::<EntityKeyHash, &EntityKey>::new();
        for mutation in mutations {
            let key = mutation.row();
            let hash = hash_entity_key(key.as_bytes());
            if let Some(previous) = seen.insert(hash, key) {
                return Err(if previous == key {
                    ExactTextProviderErrorV1::DuplicateRowMutation
                } else {
                    ExactTextProviderErrorV1::EntityKeyHashCollision
                });
            }
            if self.keys.get(&hash).is_some_and(|stored| stored != key) {
                return Err(ExactTextProviderErrorV1::EntityKeyHashCollision);
            }
            match mutation {
                ExactTextIndexMutationV2::Upsert { value, output, .. } => {
                    let encoded_len = canonical_record_encoded_len(output)
                        .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
                    if encoded_len > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
                        return Err(ExactTextProviderErrorV1::OutputTooLarge);
                    }
                    inner.push(ExactTextIndexMutationV1::upsert(hash, value)?);
                }
                ExactTextIndexMutationV2::Delete { .. } => {
                    inner.push(ExactTextIndexMutationV1::delete(hash));
                }
            }
        }
        if self
            .index
            .frontier
            .is_some_and(|frontier| epoch <= frontier)
        {
            return Err(ExactTextProviderErrorV1::NonAdvancingEpoch);
        }
        let mut next_count = self.index.rows.len();
        for mutation in &inner {
            match mutation {
                ExactTextIndexMutationV1::Upsert { row, value } => {
                    if self.index.profile.matched_form(value).len() > MAX_EXACT_TEXT_VALUE_BYTES_V1
                    {
                        return Err(ExactTextProviderErrorV1::ValueTooLong);
                    }
                    if !self.index.rows.contains_key(row) {
                        next_count = next_count.saturating_add(1);
                    }
                }
                ExactTextIndexMutationV1::Delete { row } => {
                    if self.index.rows.contains_key(row) {
                        next_count = next_count.saturating_sub(1);
                    }
                }
            }
        }
        if next_count > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextProviderErrorV1::PartitionRowLimit);
        }
        if self.index.rows.len() != self.keys.len()
            || self
                .index
                .rows
                .keys()
                .any(|row| !self.keys.contains_key(row))
        {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        for (mutation, inner_mutation) in mutations.iter().zip(&inner) {
            let hash = inner_mutation.row();
            if let Some(previous) = self.index.rows.get(&hash) {
                remove_value_from_postings_sorted(
                    hash,
                    previous,
                    &mut self.equals,
                    &mut self.prefixes,
                    &mut self.suffixes,
                    &mut self.contains,
                );
            }
            if matches!(mutation, ExactTextIndexMutationV2::Delete { .. }) {
                self.keys.remove(&hash);
                self.outputs.remove(&hash);
            }
        }
        self.index.apply(epoch, &inner)?;
        for (mutation, inner_mutation) in mutations.iter().zip(&inner) {
            if let ExactTextIndexMutationV2::Upsert {
                row: key, output, ..
            } = mutation
            {
                let hash = inner_mutation.row();
                self.keys.insert(hash, key.clone());
                self.outputs.insert(hash, output.clone());
                let matched = self
                    .index
                    .rows
                    .get(&hash)
                    .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
                add_value_to_postings_v2_sorted(
                    hash,
                    matched,
                    key.as_bytes(),
                    &self.index.rows,
                    &self.keys,
                    &mut self.equals,
                    &mut self.prefixes,
                    &mut self.suffixes,
                    &mut self.contains,
                )?;
            }
        }
        Ok(())
    }

    /// Rebuilds a disjoint V2 generation from complete authoritative source rows.
    pub fn rebuild(
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
        profile: ExactTextProfileV1,
        rows: &BTreeMap<EntityKey, (String, CanonicalRecord)>,
    ) -> Result<Self, ExactTextProviderErrorV1> {
        if rows.len() > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextProviderErrorV1::PartitionRowLimit);
        }
        let mut hashed_rows = BTreeMap::new();
        let mut keys = BTreeMap::new();
        let mut outputs = BTreeMap::new();
        for (key, (value, output)) in rows {
            if value.len() > MAX_EXACT_TEXT_VALUE_BYTES_V1 {
                return Err(ExactTextProviderErrorV1::ValueTooLong);
            }
            let hash = hash_entity_key(key.as_bytes());
            if keys.insert(hash, key.clone()).is_some() {
                return Err(ExactTextProviderErrorV1::EntityKeyHashCollision);
            }
            hashed_rows.insert(hash, value.clone());
            let encoded_len = canonical_record_encoded_len(output)
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
            if encoded_len > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
                return Err(ExactTextProviderErrorV1::OutputTooLarge);
            }
            outputs.insert(hash, output.clone());
        }
        let index = ExactTextPartitionIndexV1::rebuild(
            partition,
            generation,
            frontier,
            profile,
            &hashed_rows,
        )?;
        let (equals, prefixes, suffixes, contains) = build_v2_postings(&index.rows, &keys)?;
        Ok(Self {
            index,
            keys,
            outputs,
            equals,
            prefixes,
            suffixes,
            contains,
        })
    }

    /// Returns one whole-result count and direct ordinal window with real keys.
    pub fn result_page(
        &self,
        operator: ExactTextOperatorV1,
        needle: &ExactTextNeedleV1,
        order: ExactTextOrderV1,
        offset: u32,
        limit: NonZeroU16,
    ) -> Result<ExactTextResultPageV2, ExactTextProviderErrorV1> {
        if limit.get() > MAX_EXACT_TEXT_PAGE_ROWS_V1 {
            return Err(ExactTextProviderErrorV1::PageLimit);
        }
        let postings = match operator {
            ExactTextOperatorV1::Equals => &self.equals,
            ExactTextOperatorV1::StartsWith => &self.prefixes,
            ExactTextOperatorV1::EndsWith => &self.suffixes,
            ExactTextOperatorV1::Contains => &self.contains,
        };
        result_page_from_postings(
            postings,
            &self.keys,
            &self.outputs,
            needle,
            order,
            offset,
            limit,
        )
    }

    /// Exact partition identity of this complete policy-aligned state.
    #[must_use]
    pub const fn partition(&self) -> PartitionKeyHash {
        self.index.partition()
    }

    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.index.generation()
    }

    /// Latest completely applied provider epoch, when initialized.
    #[must_use]
    pub const fn frontier(&self) -> Option<CommitSequence> {
        self.index.frontier()
    }

    /// Rebuilds compact postings without changing logical state or frontier.
    pub fn compact(&mut self) -> Result<(), ExactTextProviderErrorV1> {
        self.index.compact();
        let (equals, prefixes, suffixes, contains) =
            build_v2_postings(&self.index.rows, &self.keys)?;
        self.equals = equals;
        self.prefixes = prefixes;
        self.suffixes = suffixes;
        self.contains = contains;
        Ok(())
    }

    /// Canonical V2 checkpoint. Rows are ordered by their derived key hash.
    pub fn to_checkpoint_bytes(&self) -> Result<Vec<u8>, ExactTextProviderErrorV1> {
        let frontier = self
            .index
            .frontier
            .ok_or(ExactTextProviderErrorV1::MissingFrontier)?;
        let mut bytes = Vec::with_capacity(60 + self.keys.len() * 64);
        bytes.extend_from_slice(b"RXTS");
        bytes.extend_from_slice(&EXACT_TEXT_PROVIDER_STATE_FORMAT_VERSION_V2.to_be_bytes());
        bytes.push(self.index.profile as u8);
        bytes.push(0);
        bytes.extend_from_slice(self.index.partition.as_bytes());
        bytes.extend_from_slice(&self.index.generation.get().to_be_bytes());
        bytes.extend_from_slice(&frontier.get().to_be_bytes());
        bytes.extend_from_slice(&(self.keys.len() as u32).to_be_bytes());
        for (hash, key) in &self.keys {
            let value = self
                .index
                .rows
                .get(hash)
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            let output = self
                .outputs
                .get(hash)
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            let output_bytes = encode_canonical_record(output)
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
            if output_bytes.len() > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
                return Err(ExactTextProviderErrorV1::OutputTooLarge);
            }
            let key_length = u16::try_from(key.as_bytes().len())
                .map_err(|_| ExactTextProviderErrorV1::CheckpointTooLarge)?;
            bytes.extend_from_slice(&key_length.to_be_bytes());
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(&(value.len() as u16).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
            bytes.extend_from_slice(&(output_bytes.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&output_bytes);
        }
        if bytes.len() > MAX_EXACT_TEXT_CHECKPOINT_BYTES_V2 {
            return Err(ExactTextProviderErrorV1::CheckpointTooLarge);
        }
        Ok(bytes)
    }

    /// Strictly recovers V2 and refuses unknown, mixed, or non-canonical bytes.
    pub fn from_checkpoint_bytes(bytes: &[u8]) -> Result<Self, ExactTextProviderErrorV1> {
        if bytes.len() < 60 || bytes.len() > MAX_EXACT_TEXT_CHECKPOINT_BYTES_V2 {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        if &bytes[..4] != b"RXTS" || u16::from_be_bytes([bytes[4], bytes[5]]) != 2 {
            return Err(ExactTextProviderErrorV1::UnsupportedFormat);
        }
        let profile = ExactTextProfileV1::from_discriminant(bytes[6])
            .ok_or(ExactTextProviderErrorV1::UnsupportedFormat)?;
        if bytes[7] != 0 {
            return Err(ExactTextProviderErrorV1::UnsupportedFormat);
        }
        let partition = PartitionKeyHash::from_bytes(read_array(bytes, 8)?);
        let generation = ProjectionGeneration::new(read_u64(bytes, 40)?)
            .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let frontier = CommitSequence::new(read_u64(bytes, 48)?)
            .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let count = usize::try_from(read_u32(bytes, 56)?)
            .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
        if count > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextProviderErrorV1::PartitionRowLimit);
        }
        let mut cursor = 60;
        let mut rows = BTreeMap::new();
        for _ in 0..count {
            let key_length = usize::from(read_u16(bytes, cursor)?);
            cursor += 2;
            let key_end = cursor
                .checked_add(key_length)
                .filter(|end| *end <= bytes.len())
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            let key = EntityKey::from_bytes(bytes[cursor..key_end].to_vec())
                .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
            cursor = key_end;
            let value_length = usize::from(read_u16(bytes, cursor)?);
            cursor += 2;
            let value_end = cursor
                .checked_add(value_length)
                .filter(|end| *end <= bytes.len())
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            if value_length > MAX_EXACT_TEXT_VALUE_BYTES_V1 {
                return Err(ExactTextProviderErrorV1::ValueTooLong);
            }
            let value = std::str::from_utf8(&bytes[cursor..value_end])
                .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?
                .to_owned();
            cursor = value_end;
            let output_length = usize::try_from(read_u32(bytes, cursor)?)
                .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
            cursor += 4;
            let output_end = cursor
                .checked_add(output_length)
                .filter(|end| *end <= bytes.len())
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            if output_length > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
                return Err(ExactTextProviderErrorV1::OutputTooLarge);
            }
            let CanonicalValue::Record(output) = decode_canonical_value(&bytes[cursor..output_end])
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?
            else {
                return Err(ExactTextProviderErrorV1::OutputInvalid);
            };
            if rows.insert(key, (value, output)).is_some() {
                return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
            }
            cursor = output_end;
        }
        if cursor != bytes.len() {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        let recovered = Self::rebuild(partition, generation, frontier, profile, &rows)?;
        if recovered.to_checkpoint_bytes()? != bytes {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        Ok(recovered)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ExactTextFilterPostingsV3 {
    equals: PostingMap,
    prefixes: PostingMap,
    suffixes: PostingMap,
    contains: PostingMap,
}

/// Additive exact provider state with one compiler-bound typed equality dimension.
///
/// Every distinct canonical filter value owns disjoint posting maps. Query-time
/// filtering therefore selects one already-maintained posting family before
/// exact count and ordinal selection; it never scans or post-filters matches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextPartitionIndexV3 {
    base: ExactTextPartitionIndexV2,
    filter_field: riffdb_types::FieldId,
    filters: BTreeMap<EntityKeyHash, CanonicalValue>,
    filtered: BTreeMap<Vec<u8>, ExactTextFilterPostingsV3>,
}

/// Complete checked source change for a filtered binary-text provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactTextIndexMutationV3 {
    /// Replace text, filter and output together; no old posting survives.
    Upsert {
        /// Exact canonical entity key.
        row: EntityKey,
        /// Bounded source text.
        value: String,
        /// Complete canonical filter value, including explicit null.
        filter: CanonicalValue,
        /// Complete compiler-shaped output.
        output: CanonicalRecord,
    },
    /// Remove the row and its old filter membership.
    Delete(EntityKey),
}

impl ExactTextPartitionIndexV3 {
    /// Applies one complete validated epoch to a private successor. Failed
    /// validation leaves text, filters, outputs and the frontier unchanged.
    pub fn apply(
        &mut self,
        epoch: CommitSequence,
        mutations: &[ExactTextIndexMutationV3],
    ) -> Result<(), ExactTextProviderErrorV1> {
        if mutations.len() > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 * 2 {
            return Err(ExactTextProviderErrorV1::PartitionRowLimit);
        }
        let mut base_updates = Vec::with_capacity(mutations.len());
        let mut mutation_filters = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            match mutation {
                ExactTextIndexMutationV3::Upsert {
                    row,
                    value,
                    filter,
                    output,
                } => {
                    let encoded = encode_canonical_value(filter)
                        .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
                    if encoded.len() > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
                        return Err(ExactTextProviderErrorV1::OutputTooLarge);
                    }
                    base_updates.push(ExactTextIndexMutationV2::upsert(
                        row.clone(),
                        value,
                        output.clone(),
                    )?);
                    mutation_filters.push(Some((filter, encoded)));
                }
                ExactTextIndexMutationV3::Delete(row) => {
                    base_updates.push(ExactTextIndexMutationV2::delete(row.clone()));
                    mutation_filters.push(None);
                }
            }
        }
        if self.filters.len() != self.base.keys.len()
            || self
                .filters
                .keys()
                .any(|row| !self.base.keys.contains_key(row))
        {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        for filter in self.filters.values() {
            canonical_value_encoded_len(filter)
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
        }
        let mut old_row_states = Vec::with_capacity(mutations.len());
        for base in &base_updates {
            let row = hash_entity_key(base.row().as_bytes());
            let old_state = if let Some(old_filter) = self.filters.get(&row) {
                let old_text = self
                    .base
                    .index
                    .rows
                    .get(&row)
                    .cloned()
                    .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
                let old_encoded = encode_canonical_value(old_filter)
                    .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
                Some((old_text, old_encoded))
            } else {
                None
            };
            old_row_states.push((row, old_state));
        }
        self.base.apply(epoch, &base_updates)?;
        for (row, old_state) in old_row_states {
            if let Some((old_text, old_filter_encoded)) = old_state {
                self.filters.remove(&row);
                let empty = if let Some(family) = self.filtered.get_mut(&old_filter_encoded) {
                    remove_value_from_postings_sorted(
                        row,
                        &old_text,
                        &mut family.equals,
                        &mut family.prefixes,
                        &mut family.suffixes,
                        &mut family.contains,
                    );
                    family.equals.is_empty()
                } else {
                    false
                };
                if empty {
                    self.filtered.remove(&old_filter_encoded);
                }
            }
        }
        for (mutation, filter_info) in mutations.iter().zip(mutation_filters) {
            if let (
                ExactTextIndexMutationV3::Upsert { row: key, .. },
                Some((filter, new_filter_encoded)),
            ) = (mutation, filter_info)
            {
                let row = hash_entity_key(key.as_bytes());
                self.filters.insert(row, filter.clone());
                let matched = self
                    .base
                    .index
                    .rows
                    .get(&row)
                    .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
                let family = self.filtered.entry(new_filter_encoded).or_default();
                add_value_to_postings_v2_sorted(
                    row,
                    matched,
                    key.as_bytes(),
                    &self.base.index.rows,
                    &self.base.keys,
                    &mut family.equals,
                    &mut family.prefixes,
                    &mut family.suffixes,
                    &mut family.contains,
                )?;
            }
        }
        // Preserve the complete checkpoint/state ceiling before publication.
        self.validate_checkpoint_bounds()?;
        Ok(())
    }

    fn validate_checkpoint_bounds(&self) -> Result<(), ExactTextProviderErrorV1> {
        let _frontier = self
            .base
            .index
            .frontier
            .ok_or(ExactTextProviderErrorV1::MissingFrontier)?;
        let mut base_bytes = 60usize;
        for (hash, key) in &self.base.keys {
            let value = self
                .base
                .index
                .rows
                .get(hash)
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            let output = self
                .base
                .outputs
                .get(hash)
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            let output_len = canonical_record_encoded_len(output)
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
            if output_len > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
                return Err(ExactTextProviderErrorV1::OutputTooLarge);
            }
            u16::try_from(key.as_bytes().len())
                .map_err(|_| ExactTextProviderErrorV1::CheckpointTooLarge)?;
            base_bytes = base_bytes
                .checked_add(2 + key.as_bytes().len() + 2 + value.len() + 4 + output_len)
                .ok_or(ExactTextProviderErrorV1::CheckpointTooLarge)?;
        }
        if base_bytes > MAX_EXACT_TEXT_CHECKPOINT_BYTES_V2 {
            return Err(ExactTextProviderErrorV1::CheckpointTooLarge);
        }
        u32::try_from(base_bytes).map_err(|_| ExactTextProviderErrorV1::CheckpointTooLarge)?;
        let mut total_bytes = 18usize
            .checked_add(base_bytes)
            .ok_or(ExactTextProviderErrorV1::CheckpointTooLarge)?;
        for filter in self.filters.values() {
            let encoded_len = canonical_value_encoded_len(filter)
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
            u32::try_from(encoded_len).map_err(|_| ExactTextProviderErrorV1::CheckpointTooLarge)?;
            total_bytes = total_bytes
                .checked_add(36 + encoded_len)
                .ok_or(ExactTextProviderErrorV1::CheckpointTooLarge)?;
        }
        if total_bytes > MAX_EXACT_TEXT_CHECKPOINT_BYTES_V3 {
            return Err(ExactTextProviderErrorV1::CheckpointTooLarge);
        }
        Ok(())
    }

    /// Rebuilds one complete V3 generation from authoritative rows.
    pub fn rebuild(
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
        profile: ExactTextProfileV1,
        filter_field: riffdb_types::FieldId,
        rows: &BTreeMap<EntityKey, (String, CanonicalValue, CanonicalRecord)>,
    ) -> Result<Self, ExactTextProviderErrorV1> {
        let base_rows = rows
            .iter()
            .map(|(key, (text, _, output))| (key.clone(), (text.clone(), output.clone())))
            .collect::<BTreeMap<_, _>>();
        let base = ExactTextPartitionIndexV2::rebuild(
            partition, generation, frontier, profile, &base_rows,
        )?;
        let mut filters = BTreeMap::new();
        for (key, (_, filter, _)) in rows {
            let encoded_len = canonical_value_encoded_len(filter)
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
            if encoded_len > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
                return Err(ExactTextProviderErrorV1::OutputTooLarge);
            }
            let hash = hash_entity_key(key.as_bytes());
            if filters.insert(hash, filter.clone()).is_some() {
                return Err(ExactTextProviderErrorV1::EntityKeyHashCollision);
            }
        }
        let filtered = build_filter_postings_v3(&base, &filters)?;
        Ok(Self {
            base,
            filter_field,
            filters,
            filtered,
        })
    }

    /// Returns one exact page from either the whole set or one typed filter partition.
    pub fn result_page(
        &self,
        operator: ExactTextOperatorV1,
        needle: &ExactTextNeedleV1,
        filter: Option<&CanonicalValue>,
        order: ExactTextOrderV1,
        offset: u32,
        limit: NonZeroU16,
    ) -> Result<ExactTextResultPageV2, ExactTextProviderErrorV1> {
        let postings = match filter {
            None => match operator {
                ExactTextOperatorV1::Equals => &self.base.equals,
                ExactTextOperatorV1::StartsWith => &self.base.prefixes,
                ExactTextOperatorV1::EndsWith => &self.base.suffixes,
                ExactTextOperatorV1::Contains => &self.base.contains,
            },
            Some(filter) => {
                let encoded = encode_canonical_value(filter)
                    .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
                let Some(filtered) = self.filtered.get(&encoded) else {
                    return Ok(ExactTextResultPageV2 {
                        rows: Vec::new(),
                        exact_total: 0,
                    });
                };
                match operator {
                    ExactTextOperatorV1::Equals => &filtered.equals,
                    ExactTextOperatorV1::StartsWith => &filtered.prefixes,
                    ExactTextOperatorV1::EndsWith => &filtered.suffixes,
                    ExactTextOperatorV1::Contains => &filtered.contains,
                }
            }
        };
        result_page_from_postings(
            postings,
            &self.base.keys,
            &self.base.outputs,
            needle,
            order,
            offset,
            limit,
        )
    }

    /// Compiler-resolved equality field bound into this state identity.
    #[must_use]
    pub const fn filter_field(&self) -> riffdb_types::FieldId {
        self.filter_field
    }

    /// Exact partition identity.
    #[must_use]
    pub const fn partition(&self) -> PartitionKeyHash {
        self.base.partition()
    }

    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.base.generation()
    }

    /// Latest completely applied provider epoch.
    #[must_use]
    pub const fn frontier(&self) -> Option<CommitSequence> {
        self.base.frontier()
    }

    /// Canonical V3 wrapper; the embedded V2 bytes remain independently exact.
    pub fn to_checkpoint_bytes(&self) -> Result<Vec<u8>, ExactTextProviderErrorV1> {
        let base = self.base.to_checkpoint_bytes()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RXTF");
        bytes.extend_from_slice(&EXACT_TEXT_PROVIDER_STATE_FORMAT_VERSION_V3.to_be_bytes());
        bytes.extend_from_slice(&self.filter_field.to_be_bytes());
        bytes.extend_from_slice(
            &u32::try_from(base.len())
                .map_err(|_| ExactTextProviderErrorV1::CheckpointTooLarge)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&base);
        bytes.extend_from_slice(&(self.filters.len() as u32).to_be_bytes());
        for (row, filter) in &self.filters {
            let encoded = encode_canonical_value(filter)
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
            bytes.extend_from_slice(row.as_bytes());
            bytes.extend_from_slice(
                &u32::try_from(encoded.len())
                    .map_err(|_| ExactTextProviderErrorV1::CheckpointTooLarge)?
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(&encoded);
        }
        if bytes.len() > MAX_EXACT_TEXT_CHECKPOINT_BYTES_V3 {
            return Err(ExactTextProviderErrorV1::CheckpointTooLarge);
        }
        Ok(bytes)
    }

    /// Strictly recovers V3 and rejects unknown, mixed, or non-canonical bytes.
    pub fn from_checkpoint_bytes(bytes: &[u8]) -> Result<Self, ExactTextProviderErrorV1> {
        if bytes.len() < 18
            || bytes.len() > MAX_EXACT_TEXT_CHECKPOINT_BYTES_V3
            || &bytes[..4] != b"RXTF"
            || u16::from_be_bytes([bytes[4], bytes[5]])
                != EXACT_TEXT_PROVIDER_STATE_FORMAT_VERSION_V3
        {
            return Err(ExactTextProviderErrorV1::UnsupportedFormat);
        }
        let filter_field = riffdb_types::FieldId::new(read_u32(bytes, 6)?)
            .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let base_len = usize::try_from(read_u32(bytes, 10)?)
            .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let base_end = 14_usize
            .checked_add(base_len)
            .filter(|end| end.saturating_add(4) <= bytes.len())
            .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
        let base = ExactTextPartitionIndexV2::from_checkpoint_bytes(&bytes[14..base_end])?;
        let count = usize::try_from(read_u32(bytes, base_end)?)
            .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
        if count != base.keys.len() || count > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        let mut cursor = base_end + 4;
        let mut filters = BTreeMap::new();
        for _ in 0..count {
            let row = EntityKeyHash::from_bytes(read_array(bytes, cursor)?);
            cursor += 32;
            let length = usize::try_from(read_u32(bytes, cursor)?)
                .map_err(|_| ExactTextProviderErrorV1::InvalidCheckpoint)?;
            cursor += 4;
            let end = cursor
                .checked_add(length)
                .filter(|end| *end <= bytes.len())
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            let filter = decode_canonical_value(&bytes[cursor..end])
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
            if !base.keys.contains_key(&row) || filters.insert(row, filter).is_some() {
                return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
            }
            cursor = end;
        }
        if cursor != bytes.len() {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        let filtered = build_filter_postings_v3(&base, &filters)?;
        let recovered = Self {
            base,
            filter_field,
            filters,
            filtered,
        };
        if recovered.to_checkpoint_bytes()? != bytes {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        Ok(recovered)
    }
}

fn build_filter_postings_v3(
    base: &ExactTextPartitionIndexV2,
    filters: &BTreeMap<EntityKeyHash, CanonicalValue>,
) -> Result<BTreeMap<Vec<u8>, ExactTextFilterPostingsV3>, ExactTextProviderErrorV1> {
    if filters.len() != base.keys.len() || filters.keys().any(|row| !base.keys.contains_key(row)) {
        return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
    }
    build_filter_subset_postings_v3(base, filters)
}

fn build_filter_subset_postings_v3(
    base: &ExactTextPartitionIndexV2,
    filters: &BTreeMap<EntityKeyHash, CanonicalValue>,
) -> Result<BTreeMap<Vec<u8>, ExactTextFilterPostingsV3>, ExactTextProviderErrorV1> {
    if filters.keys().any(|row| !base.keys.contains_key(row)) {
        return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
    }
    let mut members = BTreeMap::<Vec<u8>, BTreeSet<EntityKeyHash>>::new();
    for (row, filter) in filters {
        let encoded =
            encode_canonical_value(filter).map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
        members.entry(encoded).or_default().insert(*row);
    }
    members
        .into_iter()
        .map(|(filter, members)| {
            let (equals, prefixes, suffixes, contains) =
                build_v2_postings_for_members(&base.index.rows, &base.keys, &members)?;
            Ok((
                filter,
                ExactTextFilterPostingsV3 {
                    equals,
                    prefixes,
                    suffixes,
                    contains,
                },
            ))
        })
        .collect()
}

fn build_v2_postings_for_members(
    rows: &BTreeMap<EntityKeyHash, String>,
    keys: &BTreeMap<EntityKeyHash, EntityKey>,
    members: &BTreeSet<EntityKeyHash>,
) -> Result<(PostingMap, PostingMap, PostingMap, PostingMap), ExactTextProviderErrorV1> {
    let mut equals = PostingMap::new();
    let mut prefixes = PostingMap::new();
    let mut suffixes = PostingMap::new();
    let mut contains = PostingMap::new();
    for row in members {
        let value = rows
            .get(row)
            .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
        if !keys.contains_key(row) {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        add_value_to_postings(
            *row,
            value,
            &mut equals,
            &mut prefixes,
            &mut suffixes,
            &mut contains,
        );
    }
    order_v2_postings(&mut equals, rows, keys)?;
    order_v2_postings(&mut prefixes, rows, keys)?;
    order_v2_postings(&mut suffixes, rows, keys)?;
    order_v2_postings(&mut contains, rows, keys)?;
    Ok((equals, prefixes, suffixes, contains))
}

fn result_page_from_postings(
    postings: &PostingMap,
    keys: &BTreeMap<EntityKeyHash, EntityKey>,
    outputs: &BTreeMap<EntityKeyHash, CanonicalRecord>,
    needle: &ExactTextNeedleV1,
    order: ExactTextOrderV1,
    offset: u32,
    limit: NonZeroU16,
) -> Result<ExactTextResultPageV2, ExactTextProviderErrorV1> {
    if limit.get() > MAX_EXACT_TEXT_PAGE_ROWS_V1 {
        return Err(ExactTextProviderErrorV1::PageLimit);
    }
    let Some(posting) = postings.get(needle.as_str().as_bytes()) else {
        return Ok(ExactTextResultPageV2 {
            rows: Vec::new(),
            exact_total: 0,
        });
    };
    let exact_total = u64::try_from(posting.ascending.len())
        .map_err(|_| ExactTextProviderErrorV1::CardinalityOverflow)?;
    let start = usize::try_from(offset).unwrap_or(usize::MAX);
    if start >= posting.ascending.len() {
        return Ok(ExactTextResultPageV2 {
            rows: Vec::new(),
            exact_total,
        });
    }
    let end = start
        .saturating_add(usize::from(limit.get()))
        .min(posting.ascending.len());
    let hashes = match order {
        ExactTextOrderV1::ValueAscEntityKey => posting.ascending[start..end].to_vec(),
        ExactTextOrderV1::ValueDescEntityKey => posting.descending[start..end]
            .iter()
            .map(|position| posting.ascending[usize::from(*position)])
            .collect(),
    };
    let rows = hashes
        .iter()
        .map(|hash| {
            let key = keys
                .get(hash)
                .cloned()
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            let output = outputs
                .get(hash)
                .cloned()
                .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?;
            Ok(ExactTextResultRowV2 { key, output })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ExactTextResultPageV2 { rows, exact_total })
}

fn build_v2_postings(
    rows: &BTreeMap<EntityKeyHash, String>,
    keys: &BTreeMap<EntityKeyHash, EntityKey>,
) -> Result<(PostingMap, PostingMap, PostingMap, PostingMap), ExactTextProviderErrorV1> {
    if rows.len() != keys.len() || rows.keys().any(|row| !keys.contains_key(row)) {
        return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
    }
    let mut equals = PostingMap::new();
    let mut prefixes = PostingMap::new();
    let mut suffixes = PostingMap::new();
    let mut contains = PostingMap::new();
    for (row, value) in rows {
        add_value_to_postings(
            *row,
            value,
            &mut equals,
            &mut prefixes,
            &mut suffixes,
            &mut contains,
        );
    }
    order_v2_postings(&mut equals, rows, keys)?;
    order_v2_postings(&mut prefixes, rows, keys)?;
    order_v2_postings(&mut suffixes, rows, keys)?;
    order_v2_postings(&mut contains, rows, keys)?;
    Ok((equals, prefixes, suffixes, contains))
}

fn order_v2_postings(
    postings: &mut PostingMap,
    rows: &BTreeMap<EntityKeyHash, String>,
    keys: &BTreeMap<EntityKeyHash, EntityKey>,
) -> Result<(), ExactTextProviderErrorV1> {
    for posting in postings.values_mut() {
        order_v2_posting(posting, rows, keys)?;
    }
    Ok(())
}

fn order_v2_posting(
    posting: &mut PostingList,
    rows: &BTreeMap<EntityKeyHash, String>,
    keys: &BTreeMap<EntityKeyHash, EntityKey>,
) -> Result<(), ExactTextProviderErrorV1> {
    let mut ascending = posting
        .ascending
        .iter()
        .map(|row| {
            Ok((
                *row,
                rows.get(row)
                    .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?
                    .as_bytes(),
                keys.get(row)
                    .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?
                    .as_bytes(),
            ))
        })
        .collect::<Result<Vec<_>, ExactTextProviderErrorV1>>()?;
    ascending.sort_unstable_by(|left, right| left.1.cmp(right.1).then_with(|| left.2.cmp(right.2)));
    posting.ascending = ascending.iter().map(|(row, _, _)| *row).collect();

    if ascending.len() > usize::from(u16::MAX) + 1 {
        return Err(ExactTextProviderErrorV1::PartitionRowLimit);
    }
    let mut descending = Vec::with_capacity(ascending.len());
    let mut end = ascending.len();
    while end > 0 {
        let value = ascending[end - 1].1;
        let mut start = end - 1;
        while start > 0 && ascending[start - 1].1 == value {
            start -= 1;
        }
        for position in start..end {
            descending.push(
                u16::try_from(position).map_err(|_| ExactTextProviderErrorV1::PartitionRowLimit)?,
            );
        }
        end = start;
    }
    posting.descending = descending;
    Ok(())
}

fn build_postings(
    rows: &BTreeMap<EntityKeyHash, String>,
) -> (PostingMap, PostingMap, PostingMap, PostingMap) {
    let mut equals = PostingMap::new();
    let mut prefixes = PostingMap::new();
    let mut suffixes = PostingMap::new();
    let mut contains = PostingMap::new();
    for (row, value) in rows {
        add_value_to_postings(
            *row,
            value,
            &mut equals,
            &mut prefixes,
            &mut suffixes,
            &mut contains,
        );
    }
    order_postings(&mut equals, rows);
    order_postings(&mut prefixes, rows);
    order_postings(&mut suffixes, rows);
    order_postings(&mut contains, rows);
    (equals, prefixes, suffixes, contains)
}

fn add_value_to_postings(
    row: EntityKeyHash,
    value: &str,
    equals: &mut PostingMap,
    prefixes: &mut PostingMap,
    suffixes: &mut PostingMap,
    contains: &mut PostingMap,
) {
    let (prefix_terms, suffix_terms, contains_terms) = terms(value);
    posting_insert(equals, value.as_bytes(), row);
    for term in prefix_terms {
        posting_insert(prefixes, term, row);
    }
    for term in suffix_terms {
        posting_insert(suffixes, term, row);
    }
    for term in contains_terms {
        posting_insert(contains, term, row);
    }
}

fn add_value_to_postings_v1_sorted(
    row: EntityKeyHash,
    value: &str,
    rows: &BTreeMap<EntityKeyHash, String>,
    equals: &mut PostingMap,
    prefixes: &mut PostingMap,
    suffixes: &mut PostingMap,
    contains: &mut PostingMap,
) {
    let value_bytes = value.as_bytes();
    let (prefix_terms, suffix_terms, contains_terms) = terms(value);
    posting_insert_v1_sorted(equals, value_bytes, row, value_bytes, rows);
    for term in prefix_terms {
        posting_insert_v1_sorted(prefixes, term, row, value_bytes, rows);
    }
    for term in suffix_terms {
        posting_insert_v1_sorted(suffixes, term, row, value_bytes, rows);
    }
    for term in contains_terms {
        posting_insert_v1_sorted(contains, term, row, value_bytes, rows);
    }
}

#[allow(clippy::too_many_arguments)]
fn add_value_to_postings_v2_sorted(
    row: EntityKeyHash,
    value: &str,
    key_bytes: &[u8],
    rows: &BTreeMap<EntityKeyHash, String>,
    keys: &BTreeMap<EntityKeyHash, EntityKey>,
    equals: &mut PostingMap,
    prefixes: &mut PostingMap,
    suffixes: &mut PostingMap,
    contains: &mut PostingMap,
) -> Result<(), ExactTextProviderErrorV1> {
    let value_bytes = value.as_bytes();
    let (prefix_terms, suffix_terms, contains_terms) = terms(value);
    posting_insert_v2_sorted(equals, value_bytes, row, value_bytes, key_bytes, rows, keys)?;
    for term in prefix_terms {
        posting_insert_v2_sorted(prefixes, term, row, value_bytes, key_bytes, rows, keys)?;
    }
    for term in suffix_terms {
        posting_insert_v2_sorted(suffixes, term, row, value_bytes, key_bytes, rows, keys)?;
    }
    for term in contains_terms {
        posting_insert_v2_sorted(contains, term, row, value_bytes, key_bytes, rows, keys)?;
    }
    Ok(())
}

fn remove_value_from_postings_sorted(
    row: EntityKeyHash,
    value: &str,
    equals: &mut PostingMap,
    prefixes: &mut PostingMap,
    suffixes: &mut PostingMap,
    contains: &mut PostingMap,
) {
    let (prefix_terms, suffix_terms, contains_terms) = terms(value);
    posting_remove_sorted(equals, value.as_bytes(), row);
    for term in prefix_terms {
        posting_remove_sorted(prefixes, term, row);
    }
    for term in suffix_terms {
        posting_remove_sorted(suffixes, term, row);
    }
    for term in contains_terms {
        posting_remove_sorted(contains, term, row);
    }
}

fn terms(value: &str) -> ExactTermSets<'_> {
    let boundaries: Vec<usize> = value
        .char_indices()
        .map(|(offset, _)| offset)
        .chain(std::iter::once(value.len()))
        .collect();
    let prefixes = boundaries
        .iter()
        .skip(1)
        .map(|end| &value.as_bytes()[..*end])
        .collect();
    let suffixes = boundaries
        .iter()
        .take(boundaries.len().saturating_sub(1))
        .map(|start| &value.as_bytes()[*start..])
        .collect();
    let mut contains = BTreeSet::new();
    for (start_index, &start) in boundaries.iter().enumerate() {
        for &end in boundaries.iter().skip(start_index + 1) {
            contains.insert(&value.as_bytes()[start..end]);
        }
    }
    (prefixes, suffixes, contains)
}

fn posting_insert(postings: &mut PostingMap, term: &[u8], row: EntityKeyHash) {
    let posting = if let Some(existing) = postings.get_mut(term) {
        existing
    } else {
        postings.entry(term.to_vec()).or_default()
    };
    if !posting.ascending.contains(&row) {
        posting.ascending.push(row);
    }
}

fn posting_insert_v1_sorted(
    postings: &mut PostingMap,
    term: &[u8],
    row: EntityKeyHash,
    value_bytes: &[u8],
    rows: &BTreeMap<EntityKeyHash, String>,
) {
    let posting = if let Some(existing) = postings.get_mut(term) {
        existing
    } else {
        postings.entry(term.to_vec()).or_default()
    };
    let old_len = posting.ascending.len();
    let run_start = posting
        .ascending
        .partition_point(|candidate| rows[candidate].as_bytes() < value_bytes);
    let (run_end, asc_pos) =
        if run_start < old_len && rows[&posting.ascending[run_start]].as_bytes() == value_bytes {
            let eq_len = posting.ascending[run_start..]
                .partition_point(|candidate| rows[candidate].as_bytes() <= value_bytes);
            let run_end = run_start + eq_len;
            let tie_offset =
                posting.ascending[run_start..run_end].partition_point(|candidate| *candidate < row);
            (run_end, run_start + tie_offset)
        } else {
            (run_start, run_start)
        };
    let desc_pos = (old_len - run_end) + (asc_pos - run_start);
    let asc_pos_u16 = u16::try_from(asc_pos).expect("partition row bound fits u16");
    for idx in &mut posting.descending {
        if *idx >= asc_pos_u16 {
            *idx += 1;
        }
    }
    posting.ascending.insert(asc_pos, row);
    posting.descending.insert(desc_pos, asc_pos_u16);
}

fn posting_insert_v2_sorted(
    postings: &mut PostingMap,
    term: &[u8],
    row: EntityKeyHash,
    value_bytes: &[u8],
    key_bytes: &[u8],
    rows: &BTreeMap<EntityKeyHash, String>,
    keys: &BTreeMap<EntityKeyHash, EntityKey>,
) -> Result<(), ExactTextProviderErrorV1> {
    let posting = if let Some(existing) = postings.get_mut(term) {
        existing
    } else {
        postings.entry(term.to_vec()).or_default()
    };
    let old_len = posting.ascending.len();
    let run_start = posting
        .ascending
        .partition_point(|candidate| rows[candidate].as_bytes() < value_bytes);
    let (run_end, asc_pos) =
        if run_start < old_len && rows[&posting.ascending[run_start]].as_bytes() == value_bytes {
            let eq_len = posting.ascending[run_start..]
                .partition_point(|candidate| rows[candidate].as_bytes() <= value_bytes);
            let run_end = run_start + eq_len;
            let tie_offset = posting.ascending[run_start..run_end]
                .partition_point(|candidate| keys[candidate].as_bytes() < key_bytes);
            (run_end, run_start + tie_offset)
        } else {
            (run_start, run_start)
        };
    let desc_pos = (old_len - run_end) + (asc_pos - run_start);
    let asc_pos_u16 =
        u16::try_from(asc_pos).map_err(|_| ExactTextProviderErrorV1::PartitionRowLimit)?;
    for idx in &mut posting.descending {
        if *idx >= asc_pos_u16 {
            *idx += 1;
        }
    }
    posting.ascending.insert(asc_pos, row);
    posting.descending.insert(desc_pos, asc_pos_u16);
    Ok(())
}

fn posting_remove_sorted(postings: &mut PostingMap, term: &[u8], row: EntityKeyHash) {
    let remove_term = if let Some(posting) = postings.get_mut(term) {
        if let Some(position) = posting
            .ascending
            .iter()
            .position(|candidate| *candidate == row)
        {
            posting.ascending.remove(position);
            if let Ok(pos_u16) = u16::try_from(position) {
                if let Some(desc_idx) = posting.descending.iter().position(|&idx| idx == pos_u16) {
                    posting.descending.remove(desc_idx);
                }
                for idx in &mut posting.descending {
                    if *idx > pos_u16 {
                        *idx -= 1;
                    }
                }
            }
        }
        posting.ascending.is_empty()
    } else {
        false
    };
    if remove_term {
        postings.remove(term);
    }
}

fn order_postings(postings: &mut PostingMap, rows: &BTreeMap<EntityKeyHash, String>) {
    for posting in postings.values_mut() {
        order_posting(posting, rows);
    }
}

fn order_posting(posting: &mut PostingList, rows: &BTreeMap<EntityKeyHash, String>) {
    let mut ascending: Vec<(EntityKeyHash, &[u8])> = posting
        .ascending
        .iter()
        .map(|row| (*row, rows[row].as_bytes()))
        .collect();
    ascending
        .sort_unstable_by(|left, right| left.1.cmp(right.1).then_with(|| left.0.cmp(&right.0)));
    for (dst, (row, _)) in posting.ascending.iter_mut().zip(&ascending) {
        *dst = *row;
    }
    let mut descending = Vec::with_capacity(ascending.len());
    let mut end = ascending.len();
    while end > 0 {
        let value = ascending[end - 1].1;
        let mut start = end - 1;
        while start > 0 && ascending[start - 1].1 == value {
            start -= 1;
        }
        for position in start..end {
            descending.push(u16::try_from(position).expect("partition row bound fits u16"));
        }
        end = start;
    }
    posting.descending = descending;
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, ExactTextProviderErrorV1> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, ExactTextProviderErrorV1> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, ExactTextProviderErrorV1> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_array<const N: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<[u8; N], ExactTextProviderErrorV1> {
    bytes
        .get(offset..offset + N)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)
}

// Exhaustive non-durable ownership inventories for retained provider data.
riffdb_types::inspect_owned_capacity_struct_v1!(ExactTextPartitionIndexV2 {
    owned: [index, keys, outputs, equals, prefixes, suffixes, contains],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(ExactTextPartitionIndexV1 {
    owned: [rows, equals, prefixes, suffixes, contains],
    inline: [partition, generation, frontier, profile]
});

riffdb_types::inspect_owned_capacity_struct_v1!(ExactTextPartitionIndexV3 {
    owned: [base, filter_field, filters, filtered],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(ExactTextFilterPostingsV3 {
    owned: [equals, prefixes, suffixes, contains],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(PostingList {
    owned: [ascending, descending],
    inline: []
});

#[cfg(test)]
mod physical_capacity_tests {
    use super::*;
    // req: OQ-040
    #[test]
    fn empty_posting_lists_charge_both_retained_order_buffers() {
        let posting = PostingList {
            ascending: Vec::with_capacity(128),
            descending: Vec::with_capacity(256),
        };
        let report = riffdb_types::OwnedCapacityInspectionV1::inspect(&posting).unwrap();
        let retained = posting.ascending.capacity() * std::mem::size_of::<EntityKeyHash>()
            + posting.descending.capacity() * std::mem::size_of::<u16>();
        assert!(report.unshared_bytes() >= u64::try_from(retained).unwrap());
        assert!(posting.ascending.is_empty());
        assert!(posting.descending.is_empty());
    }
}
