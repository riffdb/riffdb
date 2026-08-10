use std::fmt;

use crate::MAX_IDENTIFIER_BYTES;

/// Original named-query language version retained for V1 source.
pub const RIFFQL_LANGUAGE_VERSION: u32 = 1;
/// Operational-predicate language version.
pub const RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1: u32 = 2;

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
    /// Query body.
    pub body: QueryBody,
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
}

/// Ordered query body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBody {
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
    /// Checked exact sum.
    Sum,
    /// Minimum value or the shared empty-set absence.
    Min,
    /// Maximum value or the shared empty-set absence.
    Max,
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
    /// Explicit bound for `many`, optional cursor for all cardinalities.
    pub take: Option<Take>,
    /// Declared absence or missing-target outcome.
    ///
    /// This is required by `one`; the planner also requires it for a bounded
    /// dependent point batch sourced from an earlier `many`.
    pub absence_outcome: Option<Spanned<Identifier>>,
}

/// Bounded page clause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Take {
    /// Positive literal or `Limit` parameter.
    pub limit: Spanned<Expression>,
    /// Optional cursor parameter.
    pub after: Option<Spanned<Identifier>>,
}

/// One source order term.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderTerm {
    /// Field path.
    pub path: Spanned<Path>,
    /// Direction.
    pub direction: Spanned<Direction>,
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
    /// Canonical leading-byte text-key match.
    Prefix,
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
