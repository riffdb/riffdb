use std::fmt;

use crate::MAX_IDENTIFIER_BYTES;

/// Original named-query language version retained for V1 source.
pub const RIFFQL_LANGUAGE_VERSION: u32 = 1;
/// Operational-predicate language version.
pub const RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1: u32 = 2;
/// Exact secret-output declaration language version.
pub const RIFFQL_LANGUAGE_VERSION_SECRET_OUTPUT_V1: u32 = 3;
/// Exact indexed result-set language version.
pub const RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1: u32 = 4;
/// Compiler-owned projected vector source and freshness declarations.
pub const RIFFQL_LANGUAGE_VERSION_PROJECTED_VECTOR_V1: u32 = 5;
/// Compiler-declared exact predicate and independent-order families.
pub const RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1: u32 = 6;
/// Compiler-declared nullable total-order placement.
pub const RIFFQL_LANGUAGE_VERSION_NULLABLE_EXACT_ORDER_V1: u32 = 7;
/// Additive exact aggregate core function family.
pub const RIFFQL_LANGUAGE_VERSION_EXACT_AGGREGATE_V1: u32 = 8;
/// Compiler-declared bounded runtime page-limit language version.
pub const RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1: u32 = 9;
/// Compiler-sealed tokenized boolean matching.
pub const RIFFQL_LANGUAGE_VERSION_TOKENIZED_TEXT_V1: u32 = 10;
/// Candidate and independently widened bounded-result pipeline language.
pub const RIFFQL_LANGUAGE_VERSION_BOUNDED_RESULT_PIPELINE_V1: u32 = 11;
/// Finite compiler-owned result-order family selected by a contract enum.
pub const RIFFQL_LANGUAGE_VERSION_ORDER_FAMILY_V1: u32 = 12;
/// Maximum compiler-declared causal projection wait.
pub const MAX_PROJECTED_CAUSAL_WAIT_MS: u32 = 30_000;
/// Maximum compiler-declared bounded projection lag.
pub const MAX_PROJECTED_LAG_MS: u64 = 86_400_000;

/// Checked half-open UTF-8 byte span.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Span {
    /// Inclusive start byte.
    pub start: u32,
    /// Exclusive end byte.
    pub end: u32,
}

impl Span {
    pub(crate) fn checked(start: usize, end: usize) -> Option<Self> {
        Some(Self {
            start: u32::try_from(start).ok()?,
            end: u32::try_from(end).ok()?,
        })
    }
}

/// A source value paired with its source span.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Spanned<T> {
    /// Parsed value.
    pub value: T,
    /// Source span.
    pub span: Span,
}

/// Exact case-sensitive ASCII identifier.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Identifier(String);

impl Identifier {
    pub(crate) fn new(value: &str) -> Option<Self> {
        if value.is_empty()
            || value.len() > MAX_IDENTIFIER_BYTES
            || !value.is_ascii()
            || !value.bytes().enumerate().all(|(index, byte)| {
                byte == b'_'
                    || byte.is_ascii_alphanumeric() && (index > 0 || !byte.is_ascii_digit())
            })
        {
            return None;
        }
        Some(Self(value.to_owned()))
    }

    /// Returns the exact spelling.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Identifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Complete parsed source document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Document {
    /// Fixed language version.
    pub language_version: u32,
    /// Optional named declaration; absence is an ad-hoc query.
    pub name: Option<Spanned<Identifier>>,
    /// Declared typed parameters.
    pub parameters: Vec<Parameter>,
    /// Exact compiler-owned projection source and freshness policy.
    pub projected_source: Option<ProjectedSource>,
    /// Query body.
    pub body: QueryBody,
}

/// One explicit vector-projection source selected by symbolic contract path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedSource {
    /// Exact `Entity.field` source path.
    pub path: Spanned<Path>,
    /// Compiler-owned freshness behavior; never a request parameter.
    pub freshness: Spanned<ProjectedFreshness>,
}

/// Closed source-level projection freshness registry (ADR-0136).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectedFreshness {
    /// Serve the currently published generation and report its frontier.
    Available,
    /// Require the inherited session commit when present, with a bounded wait.
    Causal {
        /// Whether the generated client/session commit is inherited.
        inherit_session_commit: bool,
        /// Compiler-bounded wait in milliseconds.
        max_wait_ms: u32,
    },
    /// Require a trusted elapsed-time lag observation within this bound.
    Bounded {
        /// Maximum elapsed projection lag in milliseconds.
        max_lag_ms: u64,
    },
}

/// Typed query parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Parameter {
    /// Parameter name without `$`.
    pub name: Spanned<Identifier>,
    /// Declared source type.
    pub ty: Spanned<TypeReference>,
    /// Optional literal default.
    pub default: Option<Spanned<Literal>>,
}

/// Source-level parameter type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeReference {
    /// Contract type or `Entity.field` reference.
    Named(Path),
    /// Query-only optional.
    Optional(Box<Spanned<Self>>),
    /// Query-only bounded submitted set.
    Set(Box<Spanned<Self>>),
    /// Opaque cursor token.
    Cursor,
    /// Positive row limit.
    Limit,
    /// Positive row limit with a compiler-declared inclusive maximum.
    BoundedLimit(u64),
}

/// Ordered query body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBody {
    /// Compiler-owned, non-output complete candidate bindings.
    pub candidates: Vec<CandidateBinding>,
    /// Cardinality-declared bindings.
    pub bindings: Vec<Binding>,
    /// Bounded exact aggregate declarations over prior collection bindings.
    pub aggregates: Vec<AggregateBinding>,
    /// Returned outcome name, if declared.
    pub outcome: Option<Spanned<Identifier>>,
    /// Returned record.
    pub selection: Selection,
    /// Closed declared outcome union.
    pub outcomes: Vec<Spanned<Identifier>>,
}

/// One bounded, non-output set of complete root keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateBinding {
    /// Query-local candidate name.
    pub name: Spanned<Identifier>,
    /// Complete root-key field produced by every source.
    pub root_key: Spanned<Path>,
    /// Closed compiler-visible set expression.
    pub expression: CandidateSetExpression,
    /// Maximum distinct keys before whole-binding refusal.
    pub within: u16,
    /// Outcome returned on refusal.
    pub refusal_outcome: Spanned<Identifier>,
    /// Complete declaration span.
    pub span: Span,
}

/// Closed V1 candidate-set algebra.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CandidateSetExpression {
    /// One complete source.
    Single(CandidateSource),
    /// Intersection of two through eight complete sources.
    Intersection(Vec<CandidateSource>),
    /// Union of two through eight complete sources.
    Union(Vec<CandidateSource>),
    /// One positive authorized universe minus one or more complete sources.
    Difference {
        /// Compiler-proven positive authorized universe.
        positive: CandidateSource,
        /// Complete negative sources.
        negative: Vec<CandidateSource>,
    },
}

/// One compiler-sealed ordinary or provider candidate source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateSource {
    /// Projected root-key field (`Entity.field`).
    pub projected_key: Spanned<Path>,
    /// Declared access/provider source name.
    pub access: Spanned<Identifier>,
    /// Compiler-checked source predicate.
    pub predicate: Spanned<Expression>,
    /// Complete source span.
    pub span: Span,
}

/// One bounded aggregate result derived from a prior collection binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateBinding {
    /// Query-local aggregate result name.
    pub name: Spanned<Identifier>,
    /// Earlier collection binding being folded.
    pub source: Spanned<Identifier>,
    /// Declared grouping fields, empty for one whole-set result.
    pub group_by: Vec<Spanned<Path>>,
    /// Closed exact measures.
    pub measures: Vec<AggregateMeasure>,
}

/// One named exact aggregate measure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateMeasure {
    /// Closed aggregate function.
    pub function: Spanned<AggregateFunction>,
    /// Input field for `sum`, `min`, and `max`; absent for `count`.
    pub field: Option<Spanned<Path>>,
    /// Returned field name.
    pub alias: Spanned<Identifier>,
}

/// Closed exact aggregate function registry shared with WP-492 semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateFunction {
    /// Exact row count.
    Count,
    /// Exact count over the complete admitted population before windowing.
    ExactCount,
    /// Checked exact sum.
    Sum,
    /// Minimum value or the shared empty-set absence.
    Min,
    /// Maximum value or the shared empty-set absence.
    Max,
    /// Exact count of rows whose field is not `NoValue`.
    CountPresent,
    /// Exact count of distinct canonical typed values, including `NoValue`.
    CountDistinct,
    /// Exact count of distinct canonical typed values excluding `NoValue`.
    CountDistinctPresent,
    /// Exact mergeable total and contributing-row count.
    Mean,
    /// Boolean disjunction over a required field.
    Any,
    /// Boolean conjunction over a required field.
    All,
}

/// Expected binding cardinality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cardinality {
    /// Exactly one row.
    One,
    /// Zero or one row.
    Maybe,
    /// Bounded list.
    Many,
}

/// One ordered source binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Binding {
    /// Declared cardinality.
    pub cardinality: Spanned<Cardinality>,
    /// Query-local binding name.
    pub name: Spanned<Identifier>,
    /// Contract entity symbol.
    pub entity: Spanned<Identifier>,
    /// Required predicate.
    pub predicate: Spanned<Expression>,
    /// Stable ordering.
    pub order: Vec<OrderTerm>,
    /// Optional finite compiler-owned order family. Exactly one enum value
    /// selects one complete immutable order; callers never submit structure.
    pub order_family: Option<OrderFamily>,
    /// Explicit bound for `many`, optional cursor for all cardinalities.
    pub take: Option<Take>,
    /// Nearest-neighbor search clause (ADR-0091). When present, replaces
    /// `order by` + `take` for a `many` binding.
    pub nearest: Option<NearestClause>,
    /// Compiler-sealed tokenized text match clause.
    pub tokenized_match: Option<TokenizedMatchClause>,
    /// Declared absence or missing-target outcome.
    ///
    /// This is required by `one`; the planner also requires it for a bounded
    /// dependent point batch sourced from an earlier `many`.
    pub absence_outcome: Option<Spanned<Identifier>>,
}

/// One finite order family selected by a typed contract-enum parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderFamily {
    /// Enum parameter selecting a precompiled member.
    pub parameter: Spanned<Identifier>,
    /// Complete closed variant inventory.
    pub variants: Vec<OrderFamilyVariant>,
    /// Complete clause span.
    pub span: Span,
}

/// One enum variant and its immutable total order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderFamilyVariant {
    /// Exact contract enum variant name.
    pub variant: Spanned<Identifier>,
    /// Complete fixed order, including the unique tie-breaker.
    pub order: Vec<OrderTerm>,
}

/// One compiler-owned tokenized match over a declared text index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenizedMatchClause {
    /// Contract text-index source name.
    pub index: Spanned<Identifier>,
    /// Closed compile-time match structure.
    pub kind: Spanned<TokenizedMatchKind>,
    /// Typed bounded string parameter analyzed by the declared index.
    pub query: Spanned<Identifier>,
    /// Provider-owned result order; boolean key order when omitted.
    pub ranking: TokenizedRanking,
    /// Complete clause span.
    pub span: Span,
}

/// Closed tokenized result ordering vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenizedRanking {
    /// Canonical entity-key order with no score computation.
    Boolean,
    /// Frozen provider-owned fixed-point relevance order.
    RiffBm25V1,
}

/// Closed tokenized boolean vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenizedMatchKind {
    /// Every analyzed query term must occur.
    Conjunction,
    /// At least one analyzed query term must occur.
    Disjunction,
    /// Terms must occupy consecutive positions in one field.
    Phrase,
    /// Ordered adjacent terms must be within the compiled distance.
    Proximity(u16),
}

/// Bounded page clause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Take {
    /// Positive literal or `Limit` parameter.
    pub limit: Spanned<Expression>,
    /// Optional cursor parameter.
    pub after: Option<Spanned<Identifier>>,
    /// Optional zero-based ordinal parameter or literal; mutually exclusive with cursor paging.
    pub offset: Option<Spanned<Expression>>,
}

/// Nearest-neighbor search clause (ADR-0091).
///
/// Replaces `order by` + `take` for vector similarity queries. The bound `k`
/// is mandatory and acts as the implicit page limit; no cursor pagination is
/// supported for nearest queries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NearestClause {
    /// The declared vector field on the entity.
    pub field: Spanned<Identifier>,
    /// The query vector parameter (`$name`).
    pub vector: Spanned<Identifier>,
    /// Maximum results (K). Positive integer literal.
    pub k: Spanned<Expression>,
    /// Span of the entire `nearest(...)` clause.
    pub span: Span,
}

/// One source order term.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderTerm {
    /// Field path.
    pub path: Spanned<Path>,
    /// Direction.
    pub direction: Spanned<Direction>,
    /// Explicit placement for missing and null values.
    pub null_placement: Option<Spanned<NullPlacement>>,
}

/// Closed source-level placement for missing and null order values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NullPlacement {
    /// Place the shared no-value class before every present value.
    First,
    /// Place the shared no-value class after every present value.
    Last,
}

/// Ordering direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    /// Ascending.
    Ascending,
    /// Descending.
    Descending,
}

/// Nested returned record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Selection {
    /// Ordered returned fields.
    pub fields: Vec<FieldSelection>,
}

/// One returned field or nested record/list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldSelection {
    /// Optional output alias.
    pub alias: Option<Spanned<Identifier>>,
    /// Selected source path.
    pub source: Spanned<Path>,
    /// Exact stored secret field intentionally returned by this leaf.
    pub reveals: Vec<Spanned<Path>>,
    /// Optional nested projection.
    pub nested: Option<Selection>,
}

/// Identifier path.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Path(pub Vec<Spanned<Identifier>>);

/// Predicate/value expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expression {
    /// Query parameter.
    Parameter(Spanned<Identifier>),
    /// Symbolic path or enum value.
    Path(Path),
    /// Literal.
    Literal(Literal),
    /// Predicate enabled only when one declared optional parameter is present.
    PresenceGuard {
        /// Optional parameter controlling membership in the finite plan family.
        parameter: Spanned<Identifier>,
        /// Predicate compiled into members where the parameter is present.
        predicate: Box<Spanned<Self>>,
    },
    /// Closed null/existence predicate.
    Unary {
        /// Predicate operator.
        operator: Spanned<UnaryOperator>,
        /// Symbolic field path being tested.
        operand: Box<Spanned<Self>>,
    },
    /// Binary expression.
    Binary {
        /// Operator.
        operator: Spanned<BinaryOperator>,
        /// Left operand.
        left: Box<Spanned<Self>>,
        /// Right operand.
        right: Box<Spanned<Self>>,
    },
}

/// Closed RiffQL v2 unary predicate set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    /// Field value is explicitly null.
    IsNull,
    /// Field value is present and non-null.
    IsNotNull,
    /// Field is present, including an explicitly null value.
    Exists,
}

/// Closed RiffQL v1 binary operator set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    /// Equality.
    Equal,
    /// Inequality.
    NotEqual,
    /// Less than.
    Less,
    /// Less than or equal.
    LessEqual,
    /// Greater than.
    Greater,
    /// Greater than or equal.
    GreaterEqual,
    /// Membership in a submitted set.
    In,
    /// Non-membership in a submitted set within the authorized universe.
    NotIn,
    /// Canonical leading-byte text-key match.
    Prefix,
    /// Exact binary UTF-8 leading-byte match through an ADR-0131 provider.
    StartsWith,
    /// Exact binary UTF-8 trailing-byte match through an ADR-0131 provider.
    EndsWith,
    /// Exact binary UTF-8 contiguous-byte match through an ADR-0131 provider.
    Contains,
    /// Boolean conjunction.
    And,
    /// Boolean disjunction.
    Or,
}

/// Source literal retained without numeric coercion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Literal {
    /// Unsigned integer spelling.
    Unsigned(String),
    /// Decoded UTF-8 string.
    String(String),
    /// Boolean.
    Boolean(bool),
    /// Null.
    Null,
}
