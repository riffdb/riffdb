//! Compiler-sealed tokenized text plan identity (ADR-0173).

use std::error::Error;
use std::fmt;

use riffdb_riffql_syntax::Span;
use riffdb_types::{
    EntityTypeId, FieldId, IndexId, ProjectionProviderCapabilitiesV1,
    ProjectionProviderDescriptorV1, ProjectionProviderKindV1, TextAnalyzerV1,
};

/// Byte-frozen tokenized text plan version.
pub const TOKENIZED_TEXT_PLAN_VERSION_V1: u16 = 1;
/// Maximum compiler-sealed proximity distance.
pub const MAX_TOKENIZED_PROXIMITY_V1: u16 = 1_024;

/// One weighted source retained in plan identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TokenizedTextPlanFieldV1 {
    field: FieldId,
    weight: u16,
}

impl TokenizedTextPlanFieldV1 {
    /// Constructs one bounded field weight.
    pub const fn new(field: FieldId, weight: u16) -> Result<Self, TokenizedTextPlanErrorV1> {
        if weight == 0 || weight > 10_000 {
            return Err(TokenizedTextPlanErrorV1::InvalidBound);
        }
        Ok(Self { field, weight })
    }

    /// Stable field ID.
    #[must_use]
    pub const fn field(self) -> FieldId {
        self.field
    }

    /// Compiler-owned weight.
    #[must_use]
    pub const fn weight(self) -> u16 {
        self.weight
    }
}

/// Closed compile-time boolean match structure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenizedMatchKindV1 {
    /// All analyzed terms occur.
    Conjunction,
    /// At least one analyzed term occurs.
    Disjunction,
    /// Consecutive ordered positions in one field.
    Phrase,
    /// Ordered positions whose adjacent gaps do not exceed this distance.
    Proximity(u16),
}

/// Closed provider-owned tokenized result order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenizedRankingV1 {
    /// Canonical entity-key ascending order.
    Boolean,
    /// Frozen `riff_bm25_v1` score descending, then entity key ascending.
    RiffBm25V1,
}

impl TokenizedMatchKindV1 {
    fn tag(self) -> u8 {
        match self {
            Self::Conjunction => 1,
            Self::Disjunction => 2,
            Self::Phrase => 3,
            Self::Proximity(_) => 4,
        }
    }

    /// Compile-time proximity distance, zero for non-proximity shapes.
    #[must_use]
    pub const fn distance(self) -> u16 {
        match self {
            Self::Proximity(distance) => distance,
            Self::Conjunction | Self::Disjunction | Self::Phrase => 0,
        }
    }
}

/// Complete immutable plan for one named tokenized match operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenizedTextPlanV1 {
    entity: EntityTypeId,
    index: IndexId,
    index_identity: [u8; 32],
    analyzer: TextAnalyzerV1,
    fields: Vec<TokenizedTextPlanFieldV1>,
    kind: TokenizedMatchKindV1,
    ranking: TokenizedRankingV1,
    max_terms: u32,
    max_candidates: u32,
    max_results: u32,
    descriptor: ProjectionProviderDescriptorV1,
    span: Span,
}

impl TokenizedTextPlanV1 {
    /// Constructs one canonical compiler-owned plan.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        entity: EntityTypeId,
        index: IndexId,
        index_identity: [u8; 32],
        analyzer: TextAnalyzerV1,
        mut fields: Vec<TokenizedTextPlanFieldV1>,
        kind: TokenizedMatchKindV1,
        ranking: TokenizedRankingV1,
        max_terms: u32,
        max_candidates: u32,
        max_results: u32,
        descriptor: ProjectionProviderDescriptorV1,
        span: Span,
    ) -> Result<Self, TokenizedTextPlanErrorV1> {
        fields.sort_unstable_by_key(|field| field.field.get());
        if index_identity == [0; 32]
            || fields.is_empty()
            || fields.len() > 1_024
            || fields.windows(2).any(|pair| pair[0].field == pair[1].field)
            || max_terms == 0
            || max_candidates == 0
            || max_results == 0
            || max_results > max_candidates
            || matches!(kind, TokenizedMatchKindV1::Proximity(0))
            || matches!(kind, TokenizedMatchKindV1::Proximity(value) if value > MAX_TOKENIZED_PROXIMITY_V1)
        {
            return Err(TokenizedTextPlanErrorV1::InvalidBound);
        }
        let required = ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::FILTER
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT;
        if descriptor.kind() != ProjectionProviderKindV1::TokenizedText
            || !descriptor.capabilities().contains(required)
            || descriptor.max_candidates() != max_candidates
            || descriptor.max_output_rows() != max_results
        {
            return Err(TokenizedTextPlanErrorV1::ProviderMismatch);
        }
        Ok(Self {
            entity,
            index,
            index_identity,
            analyzer,
            fields,
            kind,
            ranking,
            max_terms,
            max_candidates,
            max_results,
            descriptor,
            span,
        })
    }

    /// Owning entity.
    #[must_use]
    pub const fn entity(&self) -> EntityTypeId {
        self.entity
    }
    /// Stable text-index identity.
    #[must_use]
    pub const fn index(&self) -> IndexId {
        self.index
    }
    /// Contract-derived text-index identity checked against durable state.
    #[must_use]
    pub const fn index_identity(&self) -> [u8; 32] {
        self.index_identity
    }
    /// Frozen analyzer.
    #[must_use]
    pub const fn analyzer(&self) -> TextAnalyzerV1 {
        self.analyzer
    }
    /// Canonical weighted source fields.
    #[must_use]
    pub fn fields(&self) -> &[TokenizedTextPlanFieldV1] {
        &self.fields
    }
    /// Closed match structure.
    #[must_use]
    pub const fn kind(&self) -> TokenizedMatchKindV1 {
        self.kind
    }
    /// Provider-owned result order.
    #[must_use]
    pub const fn ranking(&self) -> TokenizedRankingV1 {
        self.ranking
    }
    /// Maximum analyzed terms.
    #[must_use]
    pub const fn max_terms(&self) -> u32 {
        self.max_terms
    }
    /// Maximum candidate documents.
    #[must_use]
    pub const fn max_candidates(&self) -> u32 {
        self.max_candidates
    }
    /// Maximum result rows.
    #[must_use]
    pub const fn max_results(&self) -> u32 {
        self.max_results
    }
    /// Exact provider descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &ProjectionProviderDescriptorV1 {
        &self.descriptor
    }
    /// Source span of the match clause.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }

    /// Canonical plan bytes used by query-module identity.
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(180 + self.fields.len() * 6);
        bytes.extend_from_slice(b"RTQP");
        bytes.extend_from_slice(&TOKENIZED_TEXT_PLAN_VERSION_V1.to_be_bytes());
        bytes.extend_from_slice(&self.entity.get().to_be_bytes());
        bytes.extend_from_slice(&self.index.get().to_be_bytes());
        bytes.extend_from_slice(&self.index_identity);
        bytes.push(self.analyzer as u8);
        bytes.push(self.kind.tag());
        bytes.push(match self.ranking {
            TokenizedRankingV1::Boolean => 1,
            TokenizedRankingV1::RiffBm25V1 => 2,
        });
        bytes.extend_from_slice(&self.kind.distance().to_be_bytes());
        bytes.extend_from_slice(&(self.fields.len() as u16).to_be_bytes());
        for field in &self.fields {
            bytes.extend_from_slice(&field.field.get().to_be_bytes());
            bytes.extend_from_slice(&field.weight.to_be_bytes());
        }
        bytes.extend_from_slice(&self.max_terms.to_be_bytes());
        bytes.extend_from_slice(&self.max_candidates.to_be_bytes());
        bytes.extend_from_slice(&self.max_results.to_be_bytes());
        bytes.extend_from_slice(&self.descriptor.to_canonical_bytes());
        bytes.extend_from_slice(&self.span.start.to_be_bytes());
        bytes.extend_from_slice(&self.span.end.to_be_bytes());
        bytes
    }
}

/// Closed plan construction failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenizedTextPlanErrorV1 {
    /// A declared count, weight, or proximity is invalid.
    InvalidBound,
    /// Provider kind, capability, or static bound does not match the plan.
    ProviderMismatch,
}

impl fmt::Display for TokenizedTextPlanErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("tokenized text plan is invalid")
    }
}

impl Error for TokenizedTextPlanErrorV1 {}
