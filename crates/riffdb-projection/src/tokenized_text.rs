//! Durable partition-scoped tokenized-text provider (ADR-0173).

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, EntityKey, EntityKeyHash, FieldId,
    PartitionKeyHash, ProjectionGeneration, TextAnalyzerV1, decode_canonical_value,
    encode_canonical_record, hash_entity_key,
};

/// Byte-frozen tokenized provider-state version.
pub const TOKENIZED_TEXT_STATE_VERSION_V1: u16 = 1;
/// Maximum indexed documents in one policy-aligned partition.
pub const MAX_TOKENIZED_DOCUMENTS_V1: usize = 4_096;
/// Maximum source bytes admitted for one field value.
pub const MAX_TOKENIZED_FIELD_BYTES_V1: usize = 65_535;
/// Maximum analyzed terms retained for one document across all fields.
pub const MAX_TOKENIZED_TERMS_PER_DOCUMENT_V1: usize = 65_536;
/// Maximum compiler-shaped output bytes retained per document.
pub const MAX_TOKENIZED_OUTPUT_BYTES_V1: usize = 64 * 1024;
/// Maximum canonical checkpoint accepted before allocation.
pub const MAX_TOKENIZED_CHECKPOINT_BYTES_V1: usize = 256 * 1024 * 1024;

/// One compiler-owned source field and relevance weight.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TokenizedTextFieldV1 {
    field: FieldId,
    weight: u16,
}

impl TokenizedTextFieldV1 {
    /// Constructs one bounded source field.
    pub const fn new(field: FieldId, weight: u16) -> Result<Self, TokenizedTextErrorV1> {
        if weight == 0 || weight > 10_000 {
            return Err(TokenizedTextErrorV1::InvalidConfiguration);
        }
        Ok(Self { field, weight })
    }

    /// Stable source field identity.
    #[must_use]
    pub const fn field(self) -> FieldId {
        self.field
    }

    /// Compiler-owned relevance weight.
    #[must_use]
    pub const fn weight(self) -> u16 {
        self.weight
    }
}

/// Immutable compiled identity required to open one tokenized segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenizedTextConfigV1 {
    index_identity: [u8; 32],
    analyzer: TextAnalyzerV1,
    fields: Vec<TokenizedTextFieldV1>,
}

impl TokenizedTextConfigV1 {
    /// Validates and canonically orders one compiled configuration.
    pub fn new(
        index_identity: [u8; 32],
        analyzer: TextAnalyzerV1,
        mut fields: Vec<TokenizedTextFieldV1>,
    ) -> Result<Self, TokenizedTextErrorV1> {
        if fields.is_empty() || fields.len() > 1_024 || index_identity == [0; 32] {
            return Err(TokenizedTextErrorV1::InvalidConfiguration);
        }
        fields.sort_unstable_by_key(|field| field.field.get());
        if fields.windows(2).any(|pair| pair[0].field == pair[1].field) {
            return Err(TokenizedTextErrorV1::InvalidConfiguration);
        }
        Ok(Self {
            index_identity,
            analyzer,
            fields,
        })
    }

    /// Contract-derived index identity.
    #[must_use]
    pub const fn index_identity(&self) -> [u8; 32] {
        self.index_identity
    }

    /// Frozen analyzer.
    #[must_use]
    pub const fn analyzer(&self) -> TextAnalyzerV1 {
        self.analyzer
    }

    /// Canonically ordered source fields.
    #[must_use]
    pub fn fields(&self) -> &[TokenizedTextFieldV1] {
        &self.fields
    }

    fn admits(&self, field: FieldId) -> bool {
        self.fields
            .binary_search_by_key(&field.get(), |candidate| candidate.field.get())
            .is_ok()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AnalyzedFieldV1 {
    length: u32,
    terms: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DocumentV1 {
    key: EntityKey,
    output: CanonicalRecord,
    fields: BTreeMap<FieldId, AnalyzedFieldV1>,
}

/// One immutable posting member; frequency and positions are stored explicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenizedPostingV1 {
    frequency: u32,
    positions: Vec<u32>,
}

impl TokenizedPostingV1 {
    /// Number of occurrences in this field.
    #[must_use]
    pub const fn frequency(&self) -> u32 {
        self.frequency
    }

    /// Zero-based token positions in ascending order.
    #[must_use]
    pub fn positions(&self) -> &[u32] {
        &self.positions
    }
}

type PostingDocumentsV1 = BTreeMap<EntityKeyHash, TokenizedPostingV1>;
type PostingMapV1 = BTreeMap<(FieldId, String), PostingDocumentsV1>;

/// One checked row update for a tokenized provider partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TokenizedTextMutationV1 {
    /// Replace one complete indexed post-image.
    Upsert {
        /// Canonical authoritative entity key.
        key: EntityKey,
        /// Present bounded source fields; omitted optional fields produce no terms.
        fields: Vec<(FieldId, String)>,
        /// Compiler-shaped policy-aligned output.
        output: CanonicalRecord,
    },
    /// Remove one document from all postings.
    Delete {
        /// Canonical authoritative entity key.
        key: EntityKey,
    },
}

impl TokenizedTextMutationV1 {
    /// Constructs one bounded post-image before analyzer expansion checks.
    pub fn upsert(
        key: EntityKey,
        mut fields: Vec<(FieldId, String)>,
        output: CanonicalRecord,
    ) -> Result<Self, TokenizedTextErrorV1> {
        if fields.len() > 1_024
            || fields
                .iter()
                .any(|(_, value)| value.len() > MAX_TOKENIZED_FIELD_BYTES_V1)
        {
            return Err(TokenizedTextErrorV1::DocumentLimit);
        }
        fields.sort_unstable_by_key(|(field, _)| field.get());
        if fields.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(TokenizedTextErrorV1::DuplicateField);
        }
        let output_bytes =
            encode_canonical_record(&output).map_err(|_| TokenizedTextErrorV1::OutputInvalid)?;
        if output_bytes.len() > MAX_TOKENIZED_OUTPUT_BYTES_V1 {
            return Err(TokenizedTextErrorV1::OutputLimit);
        }
        Ok(Self::Upsert {
            key,
            fields,
            output,
        })
    }

    /// Constructs one deletion.
    #[must_use]
    pub const fn delete(key: EntityKey) -> Self {
        Self::Delete { key }
    }

    fn key(&self) -> &EntityKey {
        match self {
            Self::Upsert { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Complete derived state for one text index and authorization partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenizedTextPartitionIndexV1 {
    config: TokenizedTextConfigV1,
    partition: PartitionKeyHash,
    generation: ProjectionGeneration,
    frontier: Option<CommitSequence>,
    documents: BTreeMap<EntityKeyHash, DocumentV1>,
    postings: PostingMapV1,
}

impl TokenizedTextPartitionIndexV1 {
    /// Creates one empty unpublished generation.
    #[must_use]
    pub fn new(
        config: TokenizedTextConfigV1,
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
    ) -> Self {
        Self {
            config,
            partition,
            generation,
            frontier: None,
            documents: BTreeMap::new(),
            postings: BTreeMap::new(),
        }
    }

    /// Applies a whole commit epoch atomically or leaves this state byte-equivalent.
    pub fn apply(
        &mut self,
        epoch: CommitSequence,
        mutations: &[TokenizedTextMutationV1],
    ) -> Result<(), TokenizedTextErrorV1> {
        if self.frontier.is_some_and(|frontier| epoch <= frontier) {
            return Err(TokenizedTextErrorV1::NonAdvancingEpoch);
        }
        let mut seen = BTreeSet::new();
        if mutations
            .iter()
            .any(|mutation| !seen.insert(hash_entity_key(mutation.key().as_bytes())))
        {
            return Err(TokenizedTextErrorV1::DuplicateDocumentMutation);
        }
        let mut next = self.documents.clone();
        for mutation in mutations {
            let hash = hash_entity_key(mutation.key().as_bytes());
            match mutation {
                TokenizedTextMutationV1::Delete { .. } => {
                    next.remove(&hash);
                }
                TokenizedTextMutationV1::Upsert {
                    key,
                    fields,
                    output,
                } => {
                    if fields.iter().any(|(field, _)| !self.config.admits(*field)) {
                        return Err(TokenizedTextErrorV1::UnknownField);
                    }
                    if next.get(&hash).is_some_and(|document| document.key != *key) {
                        return Err(TokenizedTextErrorV1::EntityKeyHashCollision);
                    }
                    let analyzed = analyze_fields(self.config.analyzer, fields)?;
                    next.insert(
                        hash,
                        DocumentV1 {
                            key: key.clone(),
                            output: output.clone(),
                            fields: analyzed,
                        },
                    );
                }
            }
        }
        if next.len() > MAX_TOKENIZED_DOCUMENTS_V1 {
            return Err(TokenizedTextErrorV1::PartitionLimit);
        }
        let postings = build_postings(&next)?;
        self.documents = next;
        self.postings = postings;
        self.frontier = Some(epoch);
        Ok(())
    }

    /// Builds a disjoint generation at one exact authoritative frontier.
    pub fn rebuild(
        config: TokenizedTextConfigV1,
        partition: PartitionKeyHash,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
        documents: &[TokenizedTextMutationV1],
    ) -> Result<Self, TokenizedTextErrorV1> {
        if documents
            .iter()
            .any(|mutation| matches!(mutation, TokenizedTextMutationV1::Delete { .. }))
        {
            return Err(TokenizedTextErrorV1::InvalidRebuildInput);
        }
        let mut index = Self::new(config, partition, generation);
        index.apply(frontier, documents)?;
        Ok(index)
    }

    /// Rebuilds only derived posting order without changing identity or frontier.
    pub fn compact(&mut self) -> Result<(), TokenizedTextErrorV1> {
        self.postings = build_postings(&self.documents)?;
        Ok(())
    }

    /// Exact current provider frontier.
    #[must_use]
    pub const fn frontier(&self) -> Option<CommitSequence> {
        self.frontier
    }

    /// Never-reused generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Exact authorization partition.
    #[must_use]
    pub const fn partition(&self) -> PartitionKeyHash {
        self.partition
    }

    /// Immutable compiled configuration.
    #[must_use]
    pub const fn config(&self) -> &TokenizedTextConfigV1 {
        &self.config
    }

    /// Number of policy-admitted documents.
    #[must_use]
    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    /// Exact posting lookup without an entity scan.
    #[must_use]
    pub fn posting(
        &self,
        field: FieldId,
        term: &str,
    ) -> Option<&BTreeMap<EntityKeyHash, TokenizedPostingV1>> {
        self.postings.get(&(field, term.to_owned()))
    }

    /// Field token count used by deterministic length normalization.
    #[must_use]
    pub fn field_length(&self, row: EntityKeyHash, field: FieldId) -> Option<u32> {
        self.documents
            .get(&row)?
            .fields
            .get(&field)
            .map(|value| value.length)
    }

    /// Canonical row key and already policy-shaped output.
    #[must_use]
    pub fn row(&self, row: EntityKeyHash) -> Option<(&EntityKey, &CanonicalRecord)> {
        self.documents
            .get(&row)
            .map(|document| (&document.key, &document.output))
    }

    /// Emits canonical V1 bytes including both document norms and explicit postings.
    pub fn to_checkpoint_bytes(&self) -> Result<Vec<u8>, TokenizedTextErrorV1> {
        let frontier = self.frontier.ok_or(TokenizedTextErrorV1::MissingFrontier)?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RTTS");
        bytes.extend_from_slice(&TOKENIZED_TEXT_STATE_VERSION_V1.to_be_bytes());
        bytes.push(self.config.analyzer as u8);
        bytes.push(0);
        bytes.extend_from_slice(&self.config.index_identity);
        bytes.extend_from_slice(self.partition.as_bytes());
        bytes.extend_from_slice(&self.generation.get().to_be_bytes());
        bytes.extend_from_slice(&frontier.get().to_be_bytes());
        push_u16(&mut bytes, self.config.fields.len())?;
        for field in &self.config.fields {
            bytes.extend_from_slice(&field.field.get().to_be_bytes());
            bytes.extend_from_slice(&field.weight.to_be_bytes());
        }
        push_u32(&mut bytes, self.documents.len())?;
        for (hash, document) in &self.documents {
            bytes.extend_from_slice(hash.as_bytes());
            push_bytes_u16(&mut bytes, document.key.as_bytes())?;
            let output = encode_canonical_record(&document.output)
                .map_err(|_| TokenizedTextErrorV1::OutputInvalid)?;
            push_bytes_u32(&mut bytes, &output)?;
            push_u16(&mut bytes, document.fields.len())?;
            for (field, analyzed) in &document.fields {
                bytes.extend_from_slice(&field.get().to_be_bytes());
                bytes.extend_from_slice(&analyzed.length.to_be_bytes());
                push_u32(&mut bytes, analyzed.terms.len())?;
                for (position, term) in analyzed.terms.iter().enumerate() {
                    bytes.extend_from_slice(
                        &u32::try_from(position)
                            .map_err(|_| TokenizedTextErrorV1::DocumentLimit)?
                            .to_be_bytes(),
                    );
                    push_bytes_u16(&mut bytes, term.as_bytes())?;
                }
            }
        }
        push_u32(&mut bytes, self.postings.len())?;
        for ((field, term), documents) in &self.postings {
            bytes.extend_from_slice(&field.get().to_be_bytes());
            push_bytes_u16(&mut bytes, term.as_bytes())?;
            push_u32(&mut bytes, documents.len())?;
            for (row, posting) in documents {
                bytes.extend_from_slice(row.as_bytes());
                bytes.extend_from_slice(&posting.frequency.to_be_bytes());
                push_u32(&mut bytes, posting.positions.len())?;
                for position in &posting.positions {
                    bytes.extend_from_slice(&position.to_be_bytes());
                }
            }
        }
        if bytes.len() > MAX_TOKENIZED_CHECKPOINT_BYTES_V1 {
            return Err(TokenizedTextErrorV1::CheckpointLimit);
        }
        Ok(bytes)
    }

    /// Strictly restores canonical V1 bytes and verifies redundant postings.
    pub fn from_checkpoint_bytes(bytes: &[u8]) -> Result<Self, TokenizedTextErrorV1> {
        if bytes.len() < 90 || bytes.len() > MAX_TOKENIZED_CHECKPOINT_BYTES_V1 {
            return Err(TokenizedTextErrorV1::InvalidCheckpoint);
        }
        let mut reader = Reader::new(bytes);
        if reader.take(4)? != b"RTTS" || reader.u16()? != TOKENIZED_TEXT_STATE_VERSION_V1 {
            return Err(TokenizedTextErrorV1::UnsupportedFormat);
        }
        let analyzer = TextAnalyzerV1::from_discriminant(reader.u8()?)
            .ok_or(TokenizedTextErrorV1::UnsupportedFormat)?;
        if reader.u8()? != 0 {
            return Err(TokenizedTextErrorV1::UnsupportedFormat);
        }
        let index_identity = reader.array32()?;
        let partition = PartitionKeyHash::from_bytes(reader.array32()?);
        let generation = ProjectionGeneration::new(reader.u64()?)
            .ok_or(TokenizedTextErrorV1::InvalidCheckpoint)?;
        let frontier =
            CommitSequence::new(reader.u64()?).ok_or(TokenizedTextErrorV1::InvalidCheckpoint)?;
        let field_count = usize::from(reader.u16()?);
        let mut fields = Vec::with_capacity(field_count.min(1_024));
        for _ in 0..field_count {
            let field =
                FieldId::new(reader.u32()?).ok_or(TokenizedTextErrorV1::InvalidCheckpoint)?;
            fields.push(TokenizedTextFieldV1::new(field, reader.u16()?)?);
        }
        let config = TokenizedTextConfigV1::new(index_identity, analyzer, fields)?;
        let document_count =
            usize::try_from(reader.u32()?).map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?;
        if document_count > MAX_TOKENIZED_DOCUMENTS_V1 {
            return Err(TokenizedTextErrorV1::PartitionLimit);
        }
        let mut documents = BTreeMap::new();
        for _ in 0..document_count {
            let hash = EntityKeyHash::from_bytes(reader.array32()?);
            let key = EntityKey::from_bytes(reader.bytes_u16()?.to_vec())
                .map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?;
            if hash_entity_key(key.as_bytes()) != hash {
                return Err(TokenizedTextErrorV1::InvalidCheckpoint);
            }
            let output_bytes = reader.bytes_u32()?;
            if output_bytes.len() > MAX_TOKENIZED_OUTPUT_BYTES_V1 {
                return Err(TokenizedTextErrorV1::OutputLimit);
            }
            let CanonicalValue::Record(output) = decode_canonical_value(output_bytes)
                .map_err(|_| TokenizedTextErrorV1::OutputInvalid)?
            else {
                return Err(TokenizedTextErrorV1::OutputInvalid);
            };
            let analyzed_field_count = usize::from(reader.u16()?);
            let mut analyzed_fields = BTreeMap::new();
            let mut terms = 0usize;
            for _ in 0..analyzed_field_count {
                let field =
                    FieldId::new(reader.u32()?).ok_or(TokenizedTextErrorV1::InvalidCheckpoint)?;
                if !config.admits(field) {
                    return Err(TokenizedTextErrorV1::UnknownField);
                }
                let length = reader.u32()?;
                let count = usize::try_from(reader.u32()?)
                    .map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?;
                terms = terms
                    .checked_add(count)
                    .ok_or(TokenizedTextErrorV1::DocumentLimit)?;
                if terms > MAX_TOKENIZED_TERMS_PER_DOCUMENT_V1
                    || usize::try_from(length).ok() != Some(count)
                {
                    return Err(TokenizedTextErrorV1::DocumentLimit);
                }
                let mut analyzed_terms = Vec::with_capacity(count);
                for expected_position in 0..count {
                    if usize::try_from(reader.u32()?).ok() != Some(expected_position) {
                        return Err(TokenizedTextErrorV1::InvalidCheckpoint);
                    }
                    let term = std::str::from_utf8(reader.bytes_u16()?)
                        .map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?
                        .to_owned();
                    analyzed_terms.push(term);
                }
                if analyzed_fields
                    .insert(
                        field,
                        AnalyzedFieldV1 {
                            length,
                            terms: analyzed_terms,
                        },
                    )
                    .is_some()
                {
                    return Err(TokenizedTextErrorV1::DuplicateField);
                }
            }
            if documents
                .insert(
                    hash,
                    DocumentV1 {
                        key,
                        output,
                        fields: analyzed_fields,
                    },
                )
                .is_some()
            {
                return Err(TokenizedTextErrorV1::InvalidCheckpoint);
            }
        }
        let encoded_postings = decode_postings(&mut reader, &config)?;
        if !reader.is_finished() {
            return Err(TokenizedTextErrorV1::InvalidCheckpoint);
        }
        let postings = build_postings(&documents)?;
        if postings != encoded_postings {
            return Err(TokenizedTextErrorV1::InvalidCheckpoint);
        }
        Ok(Self {
            config,
            partition,
            generation,
            frontier: Some(frontier),
            documents,
            postings,
        })
    }
}

fn analyze_fields(
    analyzer: TextAnalyzerV1,
    fields: &[(FieldId, String)],
) -> Result<BTreeMap<FieldId, AnalyzedFieldV1>, TokenizedTextErrorV1> {
    let mut total = 0usize;
    let mut analyzed = BTreeMap::new();
    for (field, value) in fields {
        let terms = analyzer
            .analyze(value)
            .map(|term| term.term().to_owned())
            .collect::<Vec<_>>();
        if terms.iter().any(|term| term.len() > u16::MAX as usize) {
            return Err(TokenizedTextErrorV1::DocumentLimit);
        }
        total = total
            .checked_add(terms.len())
            .ok_or(TokenizedTextErrorV1::DocumentLimit)?;
        if total > MAX_TOKENIZED_TERMS_PER_DOCUMENT_V1 {
            return Err(TokenizedTextErrorV1::DocumentLimit);
        }
        let length = u32::try_from(terms.len()).map_err(|_| TokenizedTextErrorV1::DocumentLimit)?;
        analyzed.insert(*field, AnalyzedFieldV1 { length, terms });
    }
    Ok(analyzed)
}

fn build_postings(
    documents: &BTreeMap<EntityKeyHash, DocumentV1>,
) -> Result<PostingMapV1, TokenizedTextErrorV1> {
    let mut postings = PostingMapV1::new();
    for (row, document) in documents {
        for (field, analyzed) in &document.fields {
            for (position, term) in analyzed.terms.iter().enumerate() {
                let position =
                    u32::try_from(position).map_err(|_| TokenizedTextErrorV1::DocumentLimit)?;
                postings
                    .entry((*field, term.clone()))
                    .or_default()
                    .entry(*row)
                    .or_insert_with(|| TokenizedPostingV1 {
                        frequency: 0,
                        positions: Vec::new(),
                    })
                    .positions
                    .push(position);
            }
        }
    }
    for documents in postings.values_mut() {
        for posting in documents.values_mut() {
            posting.frequency = u32::try_from(posting.positions.len())
                .map_err(|_| TokenizedTextErrorV1::DocumentLimit)?;
        }
    }
    Ok(postings)
}

fn decode_postings(
    reader: &mut Reader<'_>,
    config: &TokenizedTextConfigV1,
) -> Result<PostingMapV1, TokenizedTextErrorV1> {
    let count =
        usize::try_from(reader.u32()?).map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?;
    let mut postings = PostingMapV1::new();
    for _ in 0..count {
        let field = FieldId::new(reader.u32()?).ok_or(TokenizedTextErrorV1::InvalidCheckpoint)?;
        if !config.admits(field) {
            return Err(TokenizedTextErrorV1::UnknownField);
        }
        let term = std::str::from_utf8(reader.bytes_u16()?)
            .map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?
            .to_owned();
        let document_count =
            usize::try_from(reader.u32()?).map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?;
        if document_count > MAX_TOKENIZED_DOCUMENTS_V1 {
            return Err(TokenizedTextErrorV1::PartitionLimit);
        }
        let mut documents = BTreeMap::new();
        for _ in 0..document_count {
            let row = EntityKeyHash::from_bytes(reader.array32()?);
            let frequency = reader.u32()?;
            let position_count = usize::try_from(reader.u32()?)
                .map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?;
            if frequency == 0 || usize::try_from(frequency).ok() != Some(position_count) {
                return Err(TokenizedTextErrorV1::InvalidCheckpoint);
            }
            let mut positions = Vec::with_capacity(position_count);
            for _ in 0..position_count {
                let position = reader.u32()?;
                if positions.last().is_some_and(|last| position <= *last) {
                    return Err(TokenizedTextErrorV1::InvalidCheckpoint);
                }
                positions.push(position);
            }
            if documents
                .insert(
                    row,
                    TokenizedPostingV1 {
                        frequency,
                        positions,
                    },
                )
                .is_some()
            {
                return Err(TokenizedTextErrorV1::InvalidCheckpoint);
            }
        }
        if postings.insert((field, term), documents).is_some() {
            return Err(TokenizedTextErrorV1::InvalidCheckpoint);
        }
    }
    Ok(postings)
}

fn push_u16(bytes: &mut Vec<u8>, value: usize) -> Result<(), TokenizedTextErrorV1> {
    bytes.extend_from_slice(
        &u16::try_from(value)
            .map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?
            .to_be_bytes(),
    );
    Ok(())
}

fn push_u32(bytes: &mut Vec<u8>, value: usize) -> Result<(), TokenizedTextErrorV1> {
    bytes.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?
            .to_be_bytes(),
    );
    Ok(())
}

fn push_bytes_u16(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), TokenizedTextErrorV1> {
    push_u16(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn push_bytes_u32(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), TokenizedTextErrorV1> {
    push_u32(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], TokenizedTextErrorV1> {
        let end = self
            .cursor
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(TokenizedTextErrorV1::InvalidCheckpoint)?;
        let value = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, TokenizedTextErrorV1> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, TokenizedTextErrorV1> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, TokenizedTextErrorV1> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, TokenizedTextErrorV1> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?,
        ))
    }

    fn array32(&mut self) -> Result<[u8; 32], TokenizedTextErrorV1> {
        self.take(32)?
            .try_into()
            .map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)
    }

    fn bytes_u16(&mut self) -> Result<&'a [u8], TokenizedTextErrorV1> {
        let length = usize::from(self.u16()?);
        self.take(length)
    }

    fn bytes_u32(&mut self) -> Result<&'a [u8], TokenizedTextErrorV1> {
        let length =
            usize::try_from(self.u32()?).map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?;
        self.take(length)
    }

    const fn is_finished(&self) -> bool {
        self.cursor == self.bytes.len()
    }
}

/// Closed failures safe to expose without indexed values or cardinalities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenizedTextErrorV1 {
    /// Compiled identity or source configuration is invalid.
    InvalidConfiguration,
    /// Mutation names an undeclared source field.
    UnknownField,
    /// One document repeats a source field.
    DuplicateField,
    /// Source/analyzed document exceeds its static bound.
    DocumentLimit,
    /// Policy partition exceeds its static document bound.
    PartitionLimit,
    /// One epoch contains two changes for one entity.
    DuplicateDocumentMutation,
    /// Epochs must advance strictly.
    NonAdvancingEpoch,
    /// Rebuild input must contain complete post-images only.
    InvalidRebuildInput,
    /// Two distinct canonical keys produced one tie-breaker hash.
    EntityKeyHashCollision,
    /// Compiler-shaped output is not canonical.
    OutputInvalid,
    /// Compiler-shaped output exceeds its bound.
    OutputLimit,
    /// Checkpoint requires one published frontier.
    MissingFrontier,
    /// Checkpoint framing, canonical order, or redundant contents are invalid.
    InvalidCheckpoint,
    /// Checkpoint format or analyzer is unsupported.
    UnsupportedFormat,
    /// Checkpoint exceeds its static maximum.
    CheckpointLimit,
}

impl fmt::Display for TokenizedTextErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "tokenized text provider unavailable: {self:?}")
    }
}

impl Error for TokenizedTextErrorV1 {}
