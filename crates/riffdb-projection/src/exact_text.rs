//! Partition-scoped exact binary UTF-8 derived index (ADR-0131).

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU16;

use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, EntityKey, EntityKeyHash, ExactTextNeedleV1,
    ExactTextOperatorV1, ExactTextOrderV1, ExactTextProfileV1, MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1,
    MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1, MAX_EXACT_TEXT_VALUE_BYTES_V1, MAX_KEY_BYTES,
    PartitionKeyHash, ProjectionGeneration, decode_canonical_value, encode_canonical_record,
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
        let mut touched_equals = BTreeSet::new();
        let mut touched_prefixes = BTreeSet::new();
        let mut touched_suffixes = BTreeSet::new();
        let mut touched_contains = BTreeSet::new();
        for mutation in mutations {
            let row = mutation.row();
            if let Some(previous) = self.rows.remove(&row) {
                record_terms(
                    &previous,
                    &mut touched_equals,
                    &mut touched_prefixes,
                    &mut touched_suffixes,
                    &mut touched_contains,
                );
                remove_value_from_postings(
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
                let matched = self.profile.matched_form(value);
                record_terms(
                    &matched,
                    &mut touched_equals,
                    &mut touched_prefixes,
                    &mut touched_suffixes,
                    &mut touched_contains,
                );
                add_value_to_postings(
                    row,
                    &matched,
                    &mut self.equals,
                    &mut self.prefixes,
                    &mut self.suffixes,
                    &mut self.contains,
                );
                self.rows.insert(row, matched);
            }
        }
        order_touched_postings(&mut self.equals, &touched_equals, &self.rows);
        order_touched_postings(&mut self.prefixes, &touched_prefixes, &self.rows);
        order_touched_postings(&mut self.suffixes, &touched_suffixes, &self.rows);
        order_touched_postings(&mut self.contains, &touched_contains, &self.rows);
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
        if encode_canonical_record(&output)
            .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?
            .len()
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
        let mut next = self.clone();
        let mut inner = Vec::with_capacity(mutations.len());
        let mut seen = BTreeMap::<EntityKeyHash, &EntityKey>::new();
        let mut touched_equals = BTreeSet::new();
        let mut touched_prefixes = BTreeSet::new();
        let mut touched_suffixes = BTreeSet::new();
        let mut touched_contains = BTreeSet::new();
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
            if next.keys.get(&hash).is_some_and(|stored| stored != key) {
                return Err(ExactTextProviderErrorV1::EntityKeyHashCollision);
            }
            if let Some(previous) = next.index.rows.get(&hash) {
                record_terms(
                    previous,
                    &mut touched_equals,
                    &mut touched_prefixes,
                    &mut touched_suffixes,
                    &mut touched_contains,
                );
                remove_value_from_postings(
                    hash,
                    previous,
                    &mut next.equals,
                    &mut next.prefixes,
                    &mut next.suffixes,
                    &mut next.contains,
                );
            }
            match mutation {
                ExactTextIndexMutationV2::Upsert { value, output, .. } => {
                    let encoded = encode_canonical_record(output)
                        .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
                    if encoded.len() > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
                        return Err(ExactTextProviderErrorV1::OutputTooLarge);
                    }
                    next.keys.insert(hash, key.clone());
                    next.outputs.insert(hash, output.clone());
                    record_terms(
                        value,
                        &mut touched_equals,
                        &mut touched_prefixes,
                        &mut touched_suffixes,
                        &mut touched_contains,
                    );
                    add_value_to_postings(
                        hash,
                        value,
                        &mut next.equals,
                        &mut next.prefixes,
                        &mut next.suffixes,
                        &mut next.contains,
                    );
                    inner.push(ExactTextIndexMutationV1::upsert(hash, value)?);
                }
                ExactTextIndexMutationV2::Delete { .. } => {
                    next.keys.remove(&hash);
                    next.outputs.remove(&hash);
                    inner.push(ExactTextIndexMutationV1::delete(hash));
                }
            }
        }
        next.index.apply(epoch, &inner)?;
        order_v2_touched_postings(
            &mut next.equals,
            &touched_equals,
            &next.index.rows,
            &next.keys,
        )?;
        order_v2_touched_postings(
            &mut next.prefixes,
            &touched_prefixes,
            &next.index.rows,
            &next.keys,
        )?;
        order_v2_touched_postings(
            &mut next.suffixes,
            &touched_suffixes,
            &next.index.rows,
            &next.keys,
        )?;
        order_v2_touched_postings(
            &mut next.contains,
            &touched_contains,
            &next.index.rows,
            &next.keys,
        )?;
        *self = next;
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
            let encoded = encode_canonical_record(output)
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
            if encoded.len() > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
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
        for mutation in mutations {
            base_updates.push(match mutation {
                ExactTextIndexMutationV3::Upsert {
                    row,
                    value,
                    filter,
                    output,
                } => {
                    if encode_canonical_value(filter)
                        .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?
                        .len()
                        > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1
                    {
                        return Err(ExactTextProviderErrorV1::OutputTooLarge);
                    }
                    ExactTextIndexMutationV2::upsert(row.clone(), value, output.clone())?
                }
                ExactTextIndexMutationV3::Delete(row) => {
                    ExactTextIndexMutationV2::delete(row.clone())
                }
            });
        }
        let mut next = self.clone();
        next.base.apply(epoch, &base_updates)?;
        let mut affected = BTreeSet::new();
        for (mutation, base) in mutations.iter().zip(&base_updates) {
            let row = hash_entity_key(base.row().as_bytes());
            if let Some(old) = next.filters.remove(&row) {
                affected.insert(
                    encode_canonical_value(&old)
                        .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?,
                );
            }
            if let ExactTextIndexMutationV3::Upsert { filter, .. } = mutation {
                affected.insert(
                    encode_canonical_value(filter)
                        .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?,
                );
                next.filters.insert(row, filter.clone());
            }
        }
        // Rebuild only changed filter families. Untouched families retain their
        // checked posting order; the base already applied replacements atomically.
        let filters = next
            .filters
            .iter()
            .filter_map(|(row, value)| match encode_canonical_value(value) {
                Ok(encoded) if affected.contains(&encoded) => Some(Ok((*row, value.clone()))),
                Ok(_) => None,
                Err(_) => Some(Err(ExactTextProviderErrorV1::OutputInvalid)),
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        for filter in affected {
            next.filtered.remove(&filter);
        }
        if next.filters.len() != next.base.keys.len()
            || next
                .filters
                .keys()
                .any(|row| !next.base.keys.contains_key(row))
        {
            return Err(ExactTextProviderErrorV1::InvalidCheckpoint);
        }
        next.filtered
            .extend(build_filter_subset_postings_v3(&next.base, &filters)?);
        // Preserve the complete checkpoint/state ceiling before publication.
        next.to_checkpoint_bytes()?;
        *self = next;
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
            let encoded = encode_canonical_value(filter)
                .map_err(|_| ExactTextProviderErrorV1::OutputInvalid)?;
            if encoded.len() > MAX_EXACT_TEXT_OUTPUT_ROW_BYTES_V1 {
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
            let rows = base
                .index
                .rows
                .iter()
                .filter(|(row, _)| members.contains(row))
                .map(|(row, value)| (*row, value.clone()))
                .collect::<BTreeMap<_, _>>();
            let keys = base
                .keys
                .iter()
                .filter(|(row, _)| members.contains(row))
                .map(|(row, key)| (*row, key.clone()))
                .collect::<BTreeMap<_, _>>();
            let (equals, prefixes, suffixes, contains) = build_v2_postings(&rows, &keys)?;
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

fn order_v2_touched_postings(
    postings: &mut PostingMap,
    touched: &BTreeSet<Vec<u8>>,
    rows: &BTreeMap<EntityKeyHash, String>,
    keys: &BTreeMap<EntityKeyHash, EntityKey>,
) -> Result<(), ExactTextProviderErrorV1> {
    for term in touched {
        if let Some(posting) = postings.get_mut(term) {
            order_v2_posting(posting, rows, keys)?;
        }
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
                    .as_bytes()
                    .to_vec(),
                keys.get(row)
                    .ok_or(ExactTextProviderErrorV1::InvalidCheckpoint)?
                    .as_bytes()
                    .to_vec(),
            ))
        })
        .collect::<Result<Vec<_>, ExactTextProviderErrorV1>>()?;
    ascending
        .sort_unstable_by(|left, right| left.1.cmp(&right.1).then_with(|| left.2.cmp(&right.2)));
    posting.ascending = ascending.iter().map(|(row, _, _)| *row).collect();

    let mut descending = ascending
        .into_iter()
        .enumerate()
        .map(|(position, (_, value, key))| {
            Ok((
                u16::try_from(position).map_err(|_| ExactTextProviderErrorV1::PartitionRowLimit)?,
                value,
                key,
            ))
        })
        .collect::<Result<Vec<_>, ExactTextProviderErrorV1>>()?;
    descending
        .sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.2.cmp(&right.2)));
    posting.descending = descending
        .into_iter()
        .map(|(position, _, _)| position)
        .collect();
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

fn remove_value_from_postings(
    row: EntityKeyHash,
    value: &str,
    equals: &mut PostingMap,
    prefixes: &mut PostingMap,
    suffixes: &mut PostingMap,
    contains: &mut PostingMap,
) {
    let (prefix_terms, suffix_terms, contains_terms) = terms(value);
    posting_remove(equals, value.as_bytes(), row);
    for term in prefix_terms {
        posting_remove(prefixes, term, row);
    }
    for term in suffix_terms {
        posting_remove(suffixes, term, row);
    }
    for term in contains_terms {
        posting_remove(contains, term, row);
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

fn record_terms(
    value: &str,
    equals: &mut BTreeSet<Vec<u8>>,
    prefixes: &mut BTreeSet<Vec<u8>>,
    suffixes: &mut BTreeSet<Vec<u8>>,
    contains: &mut BTreeSet<Vec<u8>>,
) {
    let (prefix_terms, suffix_terms, contains_terms) = terms(value);
    equals.insert(value.as_bytes().to_vec());
    prefixes.extend(prefix_terms.into_iter().map(<[u8]>::to_vec));
    suffixes.extend(suffix_terms.into_iter().map(<[u8]>::to_vec));
    contains.extend(contains_terms.into_iter().map(<[u8]>::to_vec));
}

fn posting_insert(postings: &mut PostingMap, term: &[u8], row: EntityKeyHash) {
    // The source row map is authoritative for values, so rebuild the compact
    // order vectors after each bounded mutation below. Incremental mutation
    // only records membership here and cannot admit duplicates.
    let posting = postings.entry(term.to_vec()).or_default();
    if !posting.ascending.contains(&row) {
        posting.ascending.push(row);
    }
}

fn posting_remove(postings: &mut PostingMap, term: &[u8], row: EntityKeyHash) {
    let remove_term = if let Some(posting) = postings.get_mut(term) {
        if let Some(position) = posting
            .ascending
            .iter()
            .position(|candidate| *candidate == row)
        {
            posting.ascending.remove(position);
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

fn order_touched_postings(
    postings: &mut PostingMap,
    touched: &BTreeSet<Vec<u8>>,
    rows: &BTreeMap<EntityKeyHash, String>,
) {
    for term in touched {
        if let Some(posting) = postings.get_mut(term) {
            order_posting(posting, rows);
        }
    }
}

fn order_posting(posting: &mut PostingList, rows: &BTreeMap<EntityKeyHash, String>) {
    posting.ascending.sort_unstable_by(|left, right| {
        rows[left]
            .as_bytes()
            .cmp(rows[right].as_bytes())
            .then_with(|| left.cmp(right))
    });
    let mut positions: Vec<u16> = (0..posting.ascending.len())
        .map(|position| u16::try_from(position).expect("partition row bound fits u16"))
        .collect();
    positions.sort_unstable_by(|left, right| {
        let left_row = posting.ascending[usize::from(*left)];
        let right_row = posting.ascending[usize::from(*right)];
        rows[&right_row]
            .as_bytes()
            .cmp(rows[&left_row].as_bytes())
            .then_with(|| left_row.cmp(&right_row))
    });
    posting.descending = positions;
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
