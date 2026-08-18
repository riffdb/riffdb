//! Partition-scoped exact binary UTF-8 derived index (ADR-0131).

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use riffdb_types::{
    CommitSequence, EntityKeyHash, ExactTextNeedleV1, ExactTextOperatorV1, ExactTextProfileV1,
    MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1, MAX_EXACT_TEXT_VALUE_BYTES_V1, PartitionKeyHash,
    ProjectionGeneration,
};

/// Provider-owned rebuildable checkpoint format identity.
pub const EXACT_TEXT_PROVIDER_STATE_FORMAT_VERSION_V1: u16 = 1;
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

type PostingMap = BTreeMap<Vec<u8>, Vec<EntityKeyHash>>;
type ExactTermSets = (BTreeSet<Vec<u8>>, BTreeSet<Vec<u8>>, BTreeSet<Vec<u8>>);

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
                    if value.len() > MAX_EXACT_TEXT_VALUE_BYTES_V1 {
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
        for mutation in mutations {
            let row = mutation.row();
            if let Some(previous) = self.rows.remove(&row) {
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
                add_value_to_postings(
                    row,
                    value,
                    &mut self.equals,
                    &mut self.prefixes,
                    &mut self.suffixes,
                    &mut self.contains,
                );
                self.rows.insert(row, value.clone());
            }
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
            .get(needle.as_str().as_bytes())
            .map_or(&[], Vec::as_slice)
    }

    /// Canonical source rows retained for deterministic rebuild/reference checks.
    #[must_use]
    pub const fn rows(&self) -> &BTreeMap<EntityKeyHash, String> {
        &self.rows
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
        if bytes[6] != ExactTextProfileV1::BinaryUtf8V1 as u8 || bytes[7] != 0 {
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
        Self::rebuild(
            partition,
            generation,
            frontier,
            ExactTextProfileV1::BinaryUtf8V1,
            &rows,
        )
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
}

impl fmt::Display for ExactTextProviderErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "exact text provider unavailable: {self:?}")
    }
}

impl Error for ExactTextProviderErrorV1 {}

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
    posting_insert(equals, value.as_bytes().to_vec(), row);
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
        posting_remove(prefixes, &term, row);
    }
    for term in suffix_terms {
        posting_remove(suffixes, &term, row);
    }
    for term in contains_terms {
        posting_remove(contains, &term, row);
    }
}

fn terms(value: &str) -> ExactTermSets {
    let boundaries: Vec<usize> = value
        .char_indices()
        .map(|(offset, _)| offset)
        .chain(std::iter::once(value.len()))
        .collect();
    let prefixes = boundaries
        .iter()
        .skip(1)
        .map(|end| value.as_bytes()[..*end].to_vec())
        .collect();
    let suffixes = boundaries
        .iter()
        .take(boundaries.len().saturating_sub(1))
        .map(|start| value.as_bytes()[*start..].to_vec())
        .collect();
    let mut contains = BTreeSet::new();
    for (start_index, &start) in boundaries.iter().enumerate() {
        for &end in boundaries.iter().skip(start_index + 1) {
            contains.insert(value.as_bytes()[start..end].to_vec());
        }
    }
    (prefixes, suffixes, contains)
}

fn posting_insert(postings: &mut PostingMap, term: Vec<u8>, row: EntityKeyHash) {
    let rows = postings.entry(term).or_default();
    if let Err(position) = rows.binary_search(&row) {
        rows.insert(position, row);
    }
}

fn posting_remove(postings: &mut PostingMap, term: &[u8], row: EntityKeyHash) {
    let remove_term = if let Some(rows) = postings.get_mut(term) {
        if let Ok(position) = rows.binary_search(&row) {
            rows.remove(position);
        }
        rows.is_empty()
    } else {
        false
    };
    if remove_term {
        postings.remove(term);
    }
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
