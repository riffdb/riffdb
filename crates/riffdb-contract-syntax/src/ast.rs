//! Source-oriented syntax tree for contract grammar version 1.

pub use crate::span::{Span, Spanned};

/// One parsed source document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractDocument {
    /// The document's single top-level contract.
    pub contract: Spanned<Contract>,
}

/// A contract declaration retaining source order and application version lexeme.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Contract {
    /// The contract's source-spelled identifier.
    pub name: Spanned<String>,
    /// The unsigned application-version lexeme.
    pub version: Spanned<String>,
    /// Top-level declarations in source order.
    pub declarations: Vec<Spanned<Declaration>>,
}

/// Grammar-version-1 top-level declarations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Declaration {
    /// A persistent entity schema.
    Entity(EntityDeclaration),
    /// A durable event schema.
    Event(EventDeclaration),
    /// A named enumeration.
    Enum(EnumDeclaration),
    /// An aggregate ownership declaration.
    Aggregate(AggregateDeclaration),
    /// A typed command declaration.
    Command(CommandDeclaration),
    /// An event-derived projection declaration.
    Projection(ProjectionDeclaration),
}

/// A persistent entity declaration with source-ordered items.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityDeclaration {
    /// The entity identifier.
    pub name: Spanned<String>,
    /// Entity items in source order.
    pub items: Vec<Spanned<EntityItem>>,
}

/// One item accepted inside an entity declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntityItem {
    /// The entity's key declaration.
    Key(KeyDeclaration),
    /// A stored entity field.
    Field(TypedField),
    /// A named entity invariant.
    Invariant(InvariantDeclaration),
    /// An exact-prefix index declaration.
    Index(IndexDeclaration),
}

/// The typed fields forming an entity key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyDeclaration {
    /// Key fields in source order.
    pub fields: Vec<Spanned<TypedField>>,
}

/// A source-spelled field name and unresolved type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedField {
    /// The field identifier.
    pub name: Spanned<String>,
    /// The unresolved source type.
    pub ty: Spanned<TypeExpression>,
}

/// A named invariant and its untyped expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantDeclaration {
    /// The invariant identifier.
    pub name: Spanned<String>,
    /// The invariant expression.
    pub expression: Spanned<Expression>,
}

/// A named entity index over source-spelled fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDeclaration {
    /// The index identifier.
    pub name: Spanned<String>,
    /// Indexed field names in declared order.
    pub fields: Vec<Spanned<String>>,
}

/// A durable event declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventDeclaration {
    /// The event identifier.
    pub name: Spanned<String>,
    /// Event payload fields in source order.
    pub fields: Vec<Spanned<TypedField>>,
}

/// A named enumeration declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumDeclaration {
    /// The enumeration identifier.
    pub name: Spanned<String>,
    /// Variant identifiers in source order.
    pub variants: Vec<Spanned<String>>,
}

/// An aggregate ownership declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateDeclaration {
    /// The aggregate identifier.
    pub name: Spanned<String>,
    /// Aggregate items in source order.
    pub items: Vec<Spanned<AggregateItem>>,
}

/// One item accepted inside an aggregate declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateItem {
    /// The root entity name.
    Root(Spanned<String>),
    /// A child entity name.
    Child(Spanned<String>),
    /// The aggregate partition expression.
    PartitionBy(Spanned<Expression>),
    /// Conflict-key components in source order.
    ConflictKey(Vec<Spanned<Expression>>),
    /// A named aggregate invariant.
    Invariant(InvariantDeclaration),
}

/// Source type syntax. Bounds and name resolution belong to WP-040.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeExpression {
    /// The `bool` scalar type.
    Bool,
    /// The signed 64-bit integer scalar type.
    I64,
    /// The unsigned 64-bit integer scalar type.
    U64,
    /// The transaction timestamp scalar type.
    Timestamp,
    /// The calendar date scalar type.
    Date,
    /// The UUID scalar type.
    Uuid,
    /// A fixed-scale decimal type.
    Decimal {
        /// The unsigned precision lexeme.
        precision: Spanned<String>,
        /// The unsigned scale lexeme.
        scale: Spanned<String>,
    },
    /// A fixed-currency money type.
    Money {
        /// The three-letter currency lexeme.
        currency: Spanned<String>,
    },
    /// A bounded UTF-8 string type.
    String {
        /// The unsigned maximum-byte-length lexeme.
        maximum: Spanned<String>,
    },
    /// A bounded byte-string type.
    Bytes {
        /// The unsigned maximum-length lexeme.
        maximum: Spanned<String>,
    },
    /// An explicitly nullable nested type.
    Optional(Box<Spanned<TypeExpression>>),
    /// A bounded homogeneous list type.
    List {
        /// The unresolved element type.
        element: Box<Spanned<TypeExpression>>,
        /// The unsigned maximum-item-count lexeme.
        maximum: Spanned<String>,
    },
    /// An unresolved named type.
    Named(Spanned<String>),
}

/// A command declaration split into the grammar's fixed phases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandDeclaration {
    /// The command identifier.
    pub name: Spanned<String>,
    /// Input declarations in source order.
    pub inputs: Vec<Spanned<InputDeclaration>>,
    /// The optional idempotency declaration.
    pub idempotency: Option<Spanned<IdempotencyClause>>,
    /// Up-front entity bindings in source order.
    pub bindings: Vec<Spanned<Binding>>,
    /// Business preconditions in source order.
    pub requirements: Vec<Spanned<Requirement>>,
    /// Interleaved state and event effects in source order.
    pub effects: Vec<Spanned<Effect>>,
    /// The command's final success outcome.
    pub return_clause: Spanned<ReturnClause>,
}

/// A command input declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputDeclaration {
    /// The input field syntax.
    pub field: TypedField,
}

/// The expression supplying a command's idempotency key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdempotencyClause {
    /// The untyped source expression.
    pub expression: Spanned<Expression>,
}

/// An up-front entity binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Binding {
    /// A read-only existing-entity binding.
    Read(EntityBinding),
    /// A mutable existing-entity binding.
    Mutate(EntityBinding),
    /// A mutable new-entity binding with a duplicate outcome.
    Create(CreateBinding),
}

/// The common syntax of `read` and `mutate` bindings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityBinding {
    /// The unresolved entity name.
    pub entity: Spanned<String>,
    /// Key expressions in source order.
    pub arguments: Vec<Spanned<Expression>>,
    /// The local binding name.
    pub binding: Spanned<String>,
}

/// A new-entity binding and its declared duplicate outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateBinding {
    /// The unresolved entity name.
    pub entity: Spanned<String>,
    /// Key expressions in source order.
    pub arguments: Vec<Spanned<Expression>>,
    /// The local mutable binding name.
    pub binding: Spanned<String>,
    /// The outcome returned when the entity already exists.
    pub duplicate: Spanned<OutcomeExpression>,
}

/// A named business precondition and rejection outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Requirement {
    /// The requirement identifier.
    pub name: Spanned<String>,
    /// The Boolean condition expression.
    pub condition: Spanned<Expression>,
    /// The outcome returned when the condition is false.
    pub rejection: Spanned<OutcomeExpression>,
}

/// One effect in a command's ordered effect phase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    /// Assign a value to a bound entity path.
    Set(SetEffect),
    /// Emit one durable typed event.
    Emit(EmitEffect),
}

/// A source-level field assignment effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetEffect {
    /// The assignment target path.
    pub target: Spanned<Path>,
    /// The assigned value expression.
    pub value: Spanned<Expression>,
}

/// A source-level durable event emission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmitEffect {
    /// The unresolved event name.
    pub event: Spanned<String>,
    /// The event payload object.
    pub payload: Spanned<ObjectLiteral>,
}

/// The final success return clause of a command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReturnClause {
    /// The declared success outcome.
    pub outcome: Spanned<OutcomeExpression>,
}

/// A typed outcome name and source payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutcomeExpression {
    /// The unresolved outcome name.
    pub name: Spanned<String>,
    /// The outcome payload object.
    pub payload: Spanned<ObjectLiteral>,
}

/// An object literal accepted only in outcome and event positions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectLiteral {
    /// Object fields in source order.
    pub fields: Vec<Spanned<ObjectField>>,
}

/// One source field in an object literal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectField {
    /// The field name.
    pub name: Spanned<String>,
    /// The field value expression.
    pub value: Spanned<Expression>,
}

/// A bounded event-derived projection declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionDeclaration {
    /// The projection identifier.
    pub name: Spanned<String>,
    /// The unresolved source event name.
    pub source_event: Spanned<String>,
    /// The optional source-event filter.
    pub filter: Option<Spanned<Expression>>,
    /// Group-key expressions in source order.
    pub key: Vec<Spanned<Expression>>,
    /// Projection measures in source order.
    pub measures: Vec<Spanned<Measure>>,
    /// The declared projection frontier mode.
    pub frontier: Spanned<Frontier>,
}

/// A named projection aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Measure {
    /// The measure identifier.
    pub name: Spanned<String>,
    /// The aggregation syntax.
    pub aggregation: Spanned<Aggregation>,
}

/// A grammar-version-1 projection aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Aggregation {
    /// Count matching source events.
    Count,
    /// Sum an expression over matching source events.
    Sum(Spanned<Expression>),
}

/// A grammar-version-1 projection frontier declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Frontier {
    /// Process source commits in transaction order.
    TransactionallyOrdered,
}

/// An untyped, source-oriented expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expression {
    /// A literal lexeme.
    Literal(Spanned<Literal>),
    /// An identifier path.
    Path(Spanned<Path>),
    /// A parenthesized expression retained as source shape.
    Parenthesized(Box<Spanned<Expression>>),
    /// A unary operation.
    Unary {
        /// The source operator.
        operator: Spanned<UnaryOperator>,
        /// The operand expression.
        operand: Box<Spanned<Expression>>,
    },
    /// A binary operation.
    Binary {
        /// The left operand.
        left: Box<Spanned<Expression>>,
        /// The source operator.
        operator: Spanned<BinaryOperator>,
        /// The right operand.
        right: Box<Spanned<Expression>>,
    },
}

/// A source literal whose numeric and string forms remain unresolved lexemes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Literal {
    /// A Boolean literal.
    Bool(bool),
    /// The explicit null literal.
    Null,
    /// An unsigned base-10 integer lexeme.
    UInt(String),
    /// A fixed decimal lexeme.
    FixedDecimal(String),
    /// A JSON string lexeme, including source quotes and escapes.
    String(String),
}

/// A nonempty source identifier path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Path {
    /// Path segments in source order.
    pub segments: Vec<Spanned<String>>,
}

/// A grammar-version-1 unary operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    /// Boolean negation (`!`).
    Not,
    /// Numeric negation (`-`).
    Negate,
}

/// A grammar-version-1 binary operator in parsed precedence order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    /// Multiplication (`*`).
    Multiply,
    /// Division (`/`).
    Divide,
    /// Addition (`+`).
    Add,
    /// Subtraction (`-`).
    Subtract,
    /// Equality (`==`).
    Equal,
    /// Inequality (`!=`).
    NotEqual,
    /// Strictly less than (`<`).
    Less,
    /// Less than or equal (`<=`).
    LessEqual,
    /// Strictly greater than (`>`).
    Greater,
    /// Greater than or equal (`>=`).
    GreaterEqual,
    /// Boolean conjunction (`&&`).
    And,
    /// Boolean disjunction (`||`).
    Or,
}
