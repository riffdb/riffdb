//! Span-preserving reactive syntax tree.

use crate::Span;

/// One grammar-v1 reactive module.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Module {
    pub(crate) name: String,
    pub(crate) version: u64,
    pub(crate) streams: Vec<Stream>,
    pub(crate) watches: Vec<Watch>,
    pub(crate) subscriptions: Vec<Subscription>,
    pub(crate) span: Span,
}

impl Module {
    /// Symbolic module name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Positive module version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }
    /// Streams in source order.
    #[must_use]
    pub fn streams(&self) -> &[Stream] {
        &self.streams
    }
    /// Watches in source order.
    #[must_use]
    pub fn watches(&self) -> &[Watch] {
        &self.watches
    }
    /// Contextual subscriptions in source order.
    #[must_use]
    pub fn subscriptions(&self) -> &[Subscription] {
        &self.subscriptions
    }
    /// Complete module span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// One typed symbolic parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Parameter {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) span: Span,
}

impl Parameter {
    /// Parameter name without `$`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Symbolic type name.
    #[must_use]
    pub fn type_name(&self) -> &str {
        &self.type_name
    }
    /// Declaration span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// One stream partition-field binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionBinding {
    pub(crate) field: String,
    pub(crate) parameter: String,
    pub(crate) span: Span,
}

impl PartitionBinding {
    /// Event partition field.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }
    /// Bound stream parameter without `$`.
    #[must_use]
    pub fn parameter(&self) -> &str {
        &self.parameter
    }
    /// Binding span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// One explicitly selected event type and payload-field set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventSelection {
    pub(crate) event: String,
    pub(crate) fields: Vec<String>,
    pub(crate) span: Span,
}

impl EventSelection {
    /// Event type name.
    #[must_use]
    pub fn event(&self) -> &str {
        &self.event
    }
    /// Selected payload fields in source order.
    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }
    /// Selection span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// One named partition-local symbolic event stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stream {
    pub(crate) name: String,
    pub(crate) parameters: Vec<Parameter>,
    pub(crate) partition: Vec<PartitionBinding>,
    pub(crate) events: Vec<EventSelection>,
    pub(crate) predicate: Option<Expression>,
    pub(crate) span: Span,
}

impl Stream {
    /// Stream operation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Typed input parameters.
    #[must_use]
    pub fn parameters(&self) -> &[Parameter] {
        &self.parameters
    }
    /// Complete ordered partition binding.
    #[must_use]
    pub fn partition(&self) -> &[PartitionBinding] {
        &self.partition
    }
    /// Explicit event selections.
    #[must_use]
    pub fn events(&self) -> &[EventSelection] {
        &self.events
    }
    /// Optional checked predicate.
    #[must_use]
    pub const fn predicate(&self) -> Option<&Expression> {
        self.predicate.as_ref()
    }
    /// Definition span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// Requested public update behavior for a named query watch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateMode {
    /// Keyed patches are required.
    Patch,
    /// Complete bounded resets are used.
    Reset,
}

/// One named live-query definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Watch {
    pub(crate) name: String,
    pub(crate) parameters: Vec<Parameter>,
    pub(crate) query: String,
    pub(crate) update_mode: UpdateMode,
    pub(crate) span: Span,
}

impl Watch {
    /// Watch operation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Typed parameters.
    #[must_use]
    pub fn parameters(&self) -> &[Parameter] {
        &self.parameters
    }
    /// Exact symbolic named query.
    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }
    /// Requested update mode.
    #[must_use]
    pub const fn update_mode(&self) -> UpdateMode {
        self.update_mode
    }
    /// Definition span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// One named argument binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Argument {
    pub(crate) name: String,
    pub(crate) value: Operand,
    pub(crate) span: Span,
}

impl Argument {
    /// Callee parameter name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Bound source operand.
    #[must_use]
    pub const fn value(&self) -> &Operand {
        &self.value
    }
    /// Binding span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// Exact stream invocation inside a subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamBinding {
    pub(crate) stream: String,
    pub(crate) arguments: Vec<Argument>,
    pub(crate) span: Span,
}
impl StreamBinding {
    /// Referenced stream name.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }
    /// Exact named argument bindings.
    #[must_use]
    pub fn arguments(&self) -> &[Argument] {
        &self.arguments
    }
    /// Binding span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// One named hydration query invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hydration {
    pub(crate) name: String,
    pub(crate) query: String,
    pub(crate) arguments: Vec<Argument>,
    pub(crate) span: Span,
}
impl Hydration {
    /// Context member name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Exact named query.
    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }
    /// Exact named argument bindings.
    #[must_use]
    pub fn arguments(&self) -> &[Argument] {
        &self.arguments
    }
    /// Declaration span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// One named reaction helper declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reaction {
    pub(crate) name: String,
    pub(crate) command: String,
    pub(crate) span: Span,
}
impl Reaction {
    /// Retry-stable reaction name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Exact target command.
    #[must_use]
    pub fn command(&self) -> &str {
        &self.command
    }
    /// Declaration span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// Explicit contextual delivery limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub(crate) batch: u8,
    pub(crate) in_flight: u8,
    pub(crate) lease_seconds: u16,
    pub(crate) span: Span,
}
impl Limits {
    /// Maximum events returned together.
    #[must_use]
    pub const fn batch(self) -> u8 {
        self.batch
    }
    /// Maximum concurrently leased work items.
    #[must_use]
    pub const fn in_flight(self) -> u8 {
        self.in_flight
    }
    /// Lease duration in seconds.
    #[must_use]
    pub const fn lease_seconds(self) -> u16 {
        self.lease_seconds
    }
    /// Declaration span.
    #[must_use]
    pub const fn span(self) -> Span {
        self.span
    }
}

/// One contextual subscription definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Subscription {
    pub(crate) name: String,
    pub(crate) parameters: Vec<Parameter>,
    pub(crate) stream: StreamBinding,
    pub(crate) hydrations: Vec<Hydration>,
    pub(crate) reactions: Vec<Reaction>,
    pub(crate) limits: Limits,
    pub(crate) span: Span,
}
impl Subscription {
    /// Subscription operation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Typed parameters.
    #[must_use]
    pub fn parameters(&self) -> &[Parameter] {
        &self.parameters
    }
    /// Exact stream binding.
    #[must_use]
    pub const fn stream(&self) -> &StreamBinding {
        &self.stream
    }
    /// Hydration query declarations.
    #[must_use]
    pub fn hydrations(&self) -> &[Hydration] {
        &self.hydrations
    }
    /// Named command reactions.
    #[must_use]
    pub fn reactions(&self) -> &[Reaction] {
        &self.reactions
    }
    /// Delivery limits.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }
    /// Definition span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// Predicate or argument operand.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Operand {
    /// `event.<field>`.
    EventField(String, Span),
    /// `$parameter`.
    Parameter(String, Span),
    /// Canonical literal.
    Literal(Literal, Span),
}

impl Operand {
    /// Operand span.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::EventField(_, span) | Self::Parameter(_, span) | Self::Literal(_, span) => *span,
        }
    }
}

/// Closed predicate literal set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Literal {
    /// Signed decimal integer.
    Integer(i64),
    /// Boolean.
    Boolean(bool),
    /// Bounded string.
    String(String),
    /// Qualified enum symbol such as `Status.Open`.
    Symbol(String),
}

/// Closed binary predicate operator.
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
    /// Boolean conjunction.
    And,
    /// Boolean disjunction.
    Or,
}

/// Bounded predicate expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expression {
    /// One operand.
    Operand(Operand),
    /// One binary expression.
    Binary {
        /// Left operand.
        left: Box<Expression>,
        /// Closed operator.
        operator: BinaryOperator,
        /// Right operand.
        right: Box<Expression>,
        /// Complete expression span.
        span: Span,
    },
}

impl Expression {
    /// Complete expression span.
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Operand(value) => value.span(),
            Self::Binary { span, .. } => *span,
        }
    }
}
