//! Durable partition-scoped tokenized-text provider (ADR-0173).

#[path = "tokenized_text_admission.rs"]
mod admission;
pub use admission::TokenizedTextAdmissionErrorV1;

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, EntityKey, EntityKeyHash, FieldId,
    PartitionKeyHash, ProjectionGeneration, TextAnalyzerV1, canonical_record_encoded_len,
    decode_canonical_value, encode_canonical_record, hash_entity_key,
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
        let output_len = canonical_record_encoded_len(&output)
            .map_err(|_| TokenizedTextErrorV1::OutputInvalid)?;
        if output_len > MAX_TOKENIZED_OUTPUT_BYTES_V1 {
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
    field_totals: BTreeMap<FieldId, u64>,
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
            field_totals: BTreeMap::new(),
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
        let mut prepared = Vec::with_capacity(mutations.len());
        let mut next_count = self.documents.len();
        for mutation in mutations {
            let hash = hash_entity_key(mutation.key().as_bytes());
            match mutation {
                TokenizedTextMutationV1::Delete { .. } => {
                    if self.documents.contains_key(&hash) {
                        next_count = next_count.saturating_sub(1);
                    }
                    prepared.push((hash, None));
                }
                TokenizedTextMutationV1::Upsert {
                    key,
                    fields,
                    output,
                } => {
                    if fields.iter().any(|(field, _)| !self.config.admits(*field)) {
                        return Err(TokenizedTextErrorV1::UnknownField);
                    }
                    if self
                        .documents
                        .get(&hash)
                        .is_some_and(|document| document.key != *key)
                    {
                        return Err(TokenizedTextErrorV1::EntityKeyHashCollision);
                    }
                    let analyzed = analyze_fields(self.config.analyzer, fields)?;
                    if !self.documents.contains_key(&hash) {
                        next_count = next_count.saturating_add(1);
                    }
                    prepared.push((
                        hash,
                        Some(DocumentV1 {
                            key: key.clone(),
                            output: output.clone(),
                            fields: analyzed,
                        }),
                    ));
                }
            }
        }
        if next_count > MAX_TOKENIZED_DOCUMENTS_V1 {
            return Err(TokenizedTextErrorV1::PartitionLimit);
        }
        let old_frontier = self.frontier;
        let mut rollback = Vec::with_capacity(prepared.len());
        for (hash, next_doc) in prepared {
            let old_doc = self.documents.remove(&hash);
            if let Some(ref doc) = old_doc {
                remove_document_from_postings(&mut self.postings, hash, doc);
                remove_document_from_field_totals(&mut self.field_totals, doc);
            }
            if let Some(doc) = next_doc {
                if let Err(err) = add_document_to_postings(&mut self.postings, hash, &doc)
                    .and_then(|()| add_document_to_field_totals(&mut self.field_totals, &doc))
                {
                    if let Some(prev) = old_doc {
                        let _ = add_document_to_postings(&mut self.postings, hash, &prev);
                        let _ = add_document_to_field_totals(&mut self.field_totals, &prev);
                        self.documents.insert(hash, prev);
                    }
                    for (prev_hash, prev_doc) in rollback.into_iter().rev() {
                        if let Some(current) = self.documents.remove(&prev_hash) {
                            remove_document_from_postings(&mut self.postings, prev_hash, &current);
                            remove_document_from_field_totals(&mut self.field_totals, &current);
                        }
                        if let Some(prev) = prev_doc {
                            let _ = add_document_to_postings(&mut self.postings, prev_hash, &prev);
                            let _ = add_document_to_field_totals(&mut self.field_totals, &prev);
                            self.documents.insert(prev_hash, prev);
                        }
                    }
                    return Err(err);
                }
                self.documents.insert(hash, doc);
            }
            rollback.push((hash, old_doc));
        }
        self.frontier = Some(epoch);
        // A successful epoch must always remain representable by the durable
        // boundary. Validate before publication so checkpoint overflow cannot
        // strand an acknowledged in-memory state.
        if let Err(err) = self.validate_checkpoint_bounds() {
            self.frontier = old_frontier;
            for (prev_hash, prev_doc) in rollback.into_iter().rev() {
                if let Some(current) = self.documents.remove(&prev_hash) {
                    remove_document_from_postings(&mut self.postings, prev_hash, &current);
                    remove_document_from_field_totals(&mut self.field_totals, &current);
                }
                if let Some(prev) = prev_doc {
                    let _ = add_document_to_postings(&mut self.postings, prev_hash, &prev);
                    let _ = add_document_to_field_totals(&mut self.field_totals, &prev);
                    self.documents.insert(prev_hash, prev);
                }
            }
            return Err(err);
        }
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
        self.field_totals = build_field_totals(&self.documents)?;
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

    /// Complete token count for one field over this policy-aligned snapshot.
    #[doc(hidden)]
    pub fn total_field_length(&self, field: FieldId) -> Result<u64, TokenizedTextErrorV1> {
        Ok(self.field_totals.get(&field).copied().unwrap_or(0))
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
        if field_count == 0 || field_count > 1_024 {
            return Err(TokenizedTextErrorV1::InvalidConfiguration);
        }
        let mut fields = Vec::with_capacity(field_count);
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
            if analyzed_field_count > config.fields().len() {
                return Err(TokenizedTextErrorV1::InvalidCheckpoint);
            }
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
        let field_totals = build_field_totals(&documents)?;
        Ok(Self {
            config,
            partition,
            generation,
            frontier: Some(frontier),
            documents,
            postings,
            field_totals,
        })
    }
    fn validate_checkpoint_bounds(&self) -> Result<(), TokenizedTextErrorV1> {
        let _frontier = self.frontier.ok_or(TokenizedTextErrorV1::MissingFrontier)?;
        u16::try_from(self.config.fields.len())
            .map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
        u32::try_from(self.documents.len()).map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
        let mut total = 4 + 2 + 1 + 1 + 32 + 32 + 8 + 8 + 2 + self.config.fields.len() * 6 + 4;
        for document in self.documents.values() {
            u16::try_from(document.key.as_bytes().len())
                .map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
            let output_len = canonical_record_encoded_len(&document.output)
                .map_err(|_| TokenizedTextErrorV1::OutputInvalid)?;
            u32::try_from(output_len).map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
            u16::try_from(document.fields.len())
                .map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
            total = total
                .checked_add(32 + 2 + document.key.as_bytes().len() + 4 + output_len + 2)
                .ok_or(TokenizedTextErrorV1::CheckpointLimit)?;
            for analyzed in document.fields.values() {
                u32::try_from(analyzed.terms.len())
                    .map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
                total = total
                    .checked_add(4 + 4 + 4)
                    .ok_or(TokenizedTextErrorV1::CheckpointLimit)?;
                for (position, term) in analyzed.terms.iter().enumerate() {
                    u32::try_from(position).map_err(|_| TokenizedTextErrorV1::DocumentLimit)?;
                    u16::try_from(term.len()).map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
                    total = total
                        .checked_add(4 + 2 + term.len())
                        .ok_or(TokenizedTextErrorV1::CheckpointLimit)?;
                }
            }
        }
        u32::try_from(self.postings.len()).map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
        total = total
            .checked_add(4)
            .ok_or(TokenizedTextErrorV1::CheckpointLimit)?;
        for ((_, term), documents) in &self.postings {
            u16::try_from(term.len()).map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
            u32::try_from(documents.len()).map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
            total = total
                .checked_add(4 + 2 + term.len() + 4)
                .ok_or(TokenizedTextErrorV1::CheckpointLimit)?;
            for posting in documents.values() {
                u32::try_from(posting.positions.len())
                    .map_err(|_| TokenizedTextErrorV1::CheckpointLimit)?;
                total = total
                    .checked_add(32 + 4 + 4 + posting.positions.len() * 4)
                    .ok_or(TokenizedTextErrorV1::CheckpointLimit)?;
            }
        }
        if total > MAX_TOKENIZED_CHECKPOINT_BYTES_V1 {
            return Err(TokenizedTextErrorV1::CheckpointLimit);
        }
        Ok(())
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
        add_document_to_postings(&mut postings, *row, document)?;
    }
    Ok(postings)
}

fn build_field_totals(
    documents: &BTreeMap<EntityKeyHash, DocumentV1>,
) -> Result<BTreeMap<FieldId, u64>, TokenizedTextErrorV1> {
    let mut totals = BTreeMap::new();
    for document in documents.values() {
        add_document_to_field_totals(&mut totals, document)?;
    }
    Ok(totals)
}

fn add_document_to_field_totals(
    totals: &mut BTreeMap<FieldId, u64>,
    document: &DocumentV1,
) -> Result<(), TokenizedTextErrorV1> {
    for (field, analyzed) in &document.fields {
        if analyzed.length == 0 {
            continue;
        }
        let next = totals
            .get(field)
            .copied()
            .unwrap_or(0)
            .checked_add(u64::from(analyzed.length))
            .ok_or(TokenizedTextErrorV1::DocumentLimit)?;
        totals.insert(*field, next);
    }
    Ok(())
}

fn remove_document_from_field_totals(totals: &mut BTreeMap<FieldId, u64>, document: &DocumentV1) {
    for (field, analyzed) in &document.fields {
        if analyzed.length == 0 {
            continue;
        }
        let remove_entry = if let Some(total) = totals.get_mut(field) {
            *total = total.saturating_sub(u64::from(analyzed.length));
            *total == 0
        } else {
            false
        };
        if remove_entry {
            totals.remove(field);
        }
    }
}

fn add_document_to_postings(
    postings: &mut PostingMapV1,
    row: EntityKeyHash,
    document: &DocumentV1,
) -> Result<(), TokenizedTextErrorV1> {
    for (field, analyzed) in &document.fields {
        for (position, term) in analyzed.terms.iter().enumerate() {
            let position =
                u32::try_from(position).map_err(|_| TokenizedTextErrorV1::DocumentLimit)?;
            let posting = postings
                .entry((*field, term.clone()))
                .or_default()
                .entry(row)
                .or_insert_with(|| TokenizedPostingV1 {
                    frequency: 0,
                    positions: Vec::new(),
                });
            posting.positions.push(position);
            posting.frequency = u32::try_from(posting.positions.len())
                .map_err(|_| TokenizedTextErrorV1::DocumentLimit)?;
        }
    }
    Ok(())
}

fn remove_document_from_postings(
    postings: &mut PostingMapV1,
    row: EntityKeyHash,
    document: &DocumentV1,
) {
    for (field, analyzed) in &document.fields {
        for term in &analyzed.terms {
            let key = (*field, term.clone());
            let remove_term = if let Some(documents) = postings.get_mut(&key) {
                documents.remove(&row);
                documents.is_empty()
            } else {
                false
            };
            if remove_term {
                postings.remove(&key);
            }
        }
    }
}

fn decode_postings(
    reader: &mut Reader<'_>,
    config: &TokenizedTextConfigV1,
) -> Result<PostingMapV1, TokenizedTextErrorV1> {
    let count =
        usize::try_from(reader.u32()?).map_err(|_| TokenizedTextErrorV1::InvalidCheckpoint)?;
    // Each entry needs at least a field, empty term length, and document count.
    // Reject impossible declared counts before any per-entry allocation/work.
    if count > reader.remaining() / 10 {
        return Err(TokenizedTextErrorV1::InvalidCheckpoint);
    }
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
            if frequency == 0
                || position_count > MAX_TOKENIZED_TERMS_PER_DOCUMENT_V1
                || usize::try_from(frequency).ok() != Some(position_count)
                || position_count > reader.remaining() / 4
            {
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

    const fn remaining(&self) -> usize {
        self.bytes.len() - self.cursor
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

// Exhaustive non-durable ownership inventories for retained provider data.
riffdb_types::inspect_owned_capacity_struct_v1!(TokenizedTextPartitionIndexV1 {
    owned: [config, documents, postings, field_totals],
    inline: [partition, generation, frontier]
});

riffdb_types::inspect_owned_capacity_struct_v1!(TokenizedTextConfigV1 {
    owned: [fields],
    inline: [index_identity, analyzer]
});

riffdb_types::inspect_owned_capacity_copy_v1!(TokenizedTextFieldV1);

riffdb_types::inspect_owned_capacity_struct_v1!(DocumentV1 {
    owned: [key, output, fields],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(AnalyzedFieldV1 {
    owned: [terms],
    inline: [length]
});

riffdb_types::inspect_owned_capacity_struct_v1!(TokenizedPostingV1 {
    owned: [positions],
    inline: [frequency]
});

#[cfg(test)]
mod physical_capacity_tests {
    use super::*;
    // req: OQ-040
    #[test]
    fn token_positions_charge_capacity_beyond_the_semantic_position_count() {
        let mut positions = Vec::with_capacity(1024);
        positions.push(0);
        let posting = TokenizedPostingV1 {
            frequency: 1,
            positions,
        };
        let report = riffdb_types::OwnedCapacityInspectionV1::inspect(&posting).unwrap();
        assert!(
            report.unshared_bytes()
                >= u64::try_from(posting.positions.capacity() * std::mem::size_of::<u32>())
                    .unwrap()
        );
        assert_eq!(posting.positions(), &[0]);
        assert_eq!(posting.frequency(), 1);
    }

    // req: OQ-023
    #[test]
    fn total_field_length_tracks_upserts_updates_deletes_compaction_and_checkpoints() {
        use riffdb_types::{EntityKeyBuilder, EntityTypeId};

        let f1 = FieldId::new(1).unwrap();
        let f2 = FieldId::new(2).unwrap();
        let undeclared = FieldId::new(9).unwrap();
        let config = TokenizedTextConfigV1::new(
            [7; 32],
            TextAnalyzerV1::StandardV1,
            vec![
                TokenizedTextFieldV1::new(f1, 2).unwrap(),
                TokenizedTextFieldV1::new(f2, 1).unwrap(),
            ],
        )
        .unwrap();
        let mut index = TokenizedTextPartitionIndexV1::new(
            config,
            PartitionKeyHash::from_bytes([1; 32]),
            ProjectionGeneration::new(1).unwrap(),
        );
        let key = |id: u64| {
            let mut builder = EntityKeyBuilder::new(EntityTypeId::first());
            builder.push_u64(id).unwrap();
            builder.finish().unwrap()
        };
        let empty_out = || CanonicalRecord::new(Vec::new()).unwrap();

        assert_eq!(index.total_field_length(f1), Ok(0));
        assert_eq!(index.total_field_length(f2), Ok(0));
        assert_eq!(index.total_field_length(undeclared), Ok(0));

        // Epoch 1: insert two documents
        index
            .apply(
                CommitSequence::new(1).unwrap(),
                &[
                    TokenizedTextMutationV1::upsert(
                        key(1),
                        vec![
                            (f1, "alpha beta gamma".to_owned()),
                            (f2, "one two".to_owned()),
                        ],
                        empty_out(),
                    )
                    .unwrap(),
                    TokenizedTextMutationV1::upsert(
                        key(2),
                        vec![(f1, "delta".to_owned()), (f2, "three four five".to_owned())],
                        empty_out(),
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
        assert_eq!(index.total_field_length(f1), Ok(4));
        assert_eq!(index.total_field_length(f2), Ok(5));

        // Epoch 2: update key(1) with a shorter f1 and empty f2, and delete key(2)
        index
            .apply(
                CommitSequence::new(2).unwrap(),
                &[
                    TokenizedTextMutationV1::upsert(
                        key(1),
                        vec![(f1, "alpha".to_owned()), (f2, String::new())],
                        empty_out(),
                    )
                    .unwrap(),
                    TokenizedTextMutationV1::delete(key(2)),
                ],
            )
            .unwrap();
        assert_eq!(index.total_field_length(f1), Ok(1));
        assert_eq!(index.total_field_length(f2), Ok(0));

        // Checkpoint round-trip preserves exact field totals
        let checkpoint = index.to_checkpoint_bytes().unwrap();
        let restored =
            TokenizedTextPartitionIndexV1::from_checkpoint_bytes(checkpoint.as_slice()).unwrap();
        assert_eq!(restored.total_field_length(f1), Ok(1));
        assert_eq!(restored.total_field_length(f2), Ok(0));

        // Compaction preserves exact field totals
        index.compact().unwrap();
        assert_eq!(index.total_field_length(f1), Ok(1));
        assert_eq!(index.total_field_length(f2), Ok(0));
    }
}
