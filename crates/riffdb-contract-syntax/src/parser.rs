//! Parser facade, bounded semantic actions, and AST-bound validation.

use lalrpop_util::ParseError;

use crate::ast::*;
use crate::diagnostic::{SyntaxDiagnostic, SyntaxDiagnosticCode, SyntaxDiagnostics};
use crate::grammar;
use crate::lexer::{SpannedToken, Token, lex};
use crate::limits::{
    MAX_AST_NODES, MAX_DECLARATION_ITEMS, MAX_EXPECTED_TOKENS, MAX_LIST_ITEMS, MAX_NESTING_DEPTH,
};
use crate::span::{Span, Spanned};

pub(crate) type GrammarError = ParseError<usize, Token, SyntaxDiagnostic>;

/// Parses one bounded grammar-version-1 contract document.
///
/// The result is source-oriented syntax only. It has not been name-resolved,
/// type-checked, canonicalized, or compiled for execution.
pub fn parse_contract(source: &str) -> Result<ContractDocument, SyntaxDiagnostics> {
    let tokens = lex(source)?;
    validate_collection_bounds(&tokens).map_err(SyntaxDiagnostics::single)?;
    let input = tokens.into_iter().map(|token| {
        Ok((
            token.span.start() as usize,
            token.value,
            token.span.end() as usize,
        ))
    });

    let document = grammar::ContractDocumentParser::new()
        .parse(input)
        .map_err(|error| SyntaxDiagnostics::single(map_parse_error(error, source.len())))?;
    validate_document(&document).map_err(SyntaxDiagnostics::single)?;
    Ok(document)
}

/// Validates UTF-8 and parses one bounded grammar-version-1 source document.
///
/// Invalid UTF-8 is reported as `RDB-S003` at the first invalid byte sequence.
/// Callers that already hold a Rust string can use [`parse_contract`].
pub fn parse_contract_bytes(source: &[u8]) -> Result<ContractDocument, SyntaxDiagnostics> {
    if source.len() > crate::limits::MAX_SOURCE_BYTES {
        return Err(SyntaxDiagnostics::single(SyntaxDiagnostic::new(
            SyntaxDiagnosticCode::SourceLimit,
            span(0, crate::limits::MAX_SOURCE_BYTES),
        )));
    }

    match std::str::from_utf8(source) {
        Ok(source) => parse_contract(source),
        Err(error) => {
            let start = error.valid_up_to();
            let end = error
                .error_len()
                .map_or(source.len(), |length| start.saturating_add(length));
            Err(SyntaxDiagnostics::single(SyntaxDiagnostic::new(
                SyntaxDiagnosticCode::InvalidToken,
                span(start, end.min(source.len())),
            )))
        }
    }
}

pub(crate) fn span(start: usize, end: usize) -> Span {
    Span::new(start, end).unwrap_or(Span::ZERO)
}

pub(crate) fn spanned<T>(value: T, start: usize, end: usize) -> Spanned<T> {
    Spanned::new(value, span(start, end))
}

pub(crate) fn grammar_result<T>(result: Result<T, SyntaxDiagnostic>) -> Result<T, GrammarError> {
    result.map_err(|error| ParseError::User { error })
}

pub(crate) fn currency_type(
    currency: Spanned<String>,
    outer_span: Span,
) -> Result<Spanned<TypeExpression>, SyntaxDiagnostic> {
    let is_currency =
        currency.value.len() == 3 && currency.value.bytes().all(|byte| byte.is_ascii_uppercase());
    if !is_currency {
        return Err(SyntaxDiagnostic::new(
            SyntaxDiagnosticCode::InvalidToken,
            currency.span,
        ));
    }
    Ok(Spanned::new(TypeExpression::Money { currency }, outer_span))
}

/// A temporary parser value that enforces expression bounds before boxing.
pub(crate) struct ParsedExpression {
    syntax: Spanned<Expression>,
    depth: usize,
    nodes: usize,
}

impl ParsedExpression {
    pub(crate) fn into_syntax(self) -> Spanned<Expression> {
        self.syntax
    }
}

pub(crate) fn literal_expression(literal: Spanned<Literal>) -> ParsedExpression {
    let span = literal.span;
    ParsedExpression {
        syntax: Spanned::new(Expression::Literal(literal), span),
        depth: 0,
        nodes: 2,
    }
}

pub(crate) fn path_expression(path: Spanned<Path>) -> ParsedExpression {
    let span = path.span;
    let nodes = path.value.segments.len().saturating_add(2);
    ParsedExpression {
        syntax: Spanned::new(Expression::Path(path), span),
        depth: 0,
        nodes,
    }
}

pub(crate) fn parenthesized_expression(
    expression: ParsedExpression,
    outer_span: Span,
) -> Result<ParsedExpression, SyntaxDiagnostic> {
    bounded_expression(
        Spanned::new(
            Expression::Parenthesized(Box::new(expression.syntax)),
            outer_span,
        ),
        expression.depth.saturating_add(1),
        expression.nodes.saturating_add(1),
    )
}

pub(crate) fn unary_expression(
    operator: Spanned<UnaryOperator>,
    operand: ParsedExpression,
    outer_span: Span,
) -> Result<ParsedExpression, SyntaxDiagnostic> {
    bounded_expression(
        Spanned::new(
            Expression::Unary {
                operator,
                operand: Box::new(operand.syntax),
            },
            outer_span,
        ),
        operand.depth.saturating_add(1),
        operand.nodes.saturating_add(2),
    )
}

pub(crate) fn binary_expression(
    left: ParsedExpression,
    operator: Spanned<BinaryOperator>,
    right: ParsedExpression,
    outer_span: Span,
) -> Result<ParsedExpression, SyntaxDiagnostic> {
    let depth = left.depth.max(right.depth).saturating_add(1);
    let nodes = left.nodes.saturating_add(right.nodes).saturating_add(2);
    bounded_expression(
        Spanned::new(
            Expression::Binary {
                left: Box::new(left.syntax),
                operator,
                right: Box::new(right.syntax),
            },
            outer_span,
        ),
        depth,
        nodes,
    )
}

pub(crate) fn binary_chain(
    mut left: ParsedExpression,
    rest: Vec<(Spanned<BinaryOperator>, ParsedExpression)>,
) -> Result<ParsedExpression, SyntaxDiagnostic> {
    for (operator, right) in rest {
        let outer_span = left.syntax.span.cover(right.syntax.span);
        left = binary_expression(left, operator, right, outer_span)?;
    }
    Ok(left)
}

fn bounded_expression(
    syntax: Spanned<Expression>,
    depth: usize,
    nodes: usize,
) -> Result<ParsedExpression, SyntaxDiagnostic> {
    if depth > MAX_NESTING_DEPTH {
        return Err(SyntaxDiagnostic::new(
            SyntaxDiagnosticCode::NestingLimit,
            syntax.span,
        ));
    }
    if nodes > MAX_AST_NODES {
        return Err(SyntaxDiagnostic::new(
            SyntaxDiagnosticCode::NodeLimit,
            syntax.span,
        ));
    }
    Ok(ParsedExpression {
        syntax,
        depth,
        nodes,
    })
}

fn map_parse_error(error: GrammarError, source_end: usize) -> SyntaxDiagnostic {
    match error {
        ParseError::InvalidToken { location } => {
            SyntaxDiagnostic::new(SyntaxDiagnosticCode::InvalidToken, span(location, location))
        }
        ParseError::UnrecognizedEof {
            location: _,
            expected,
        } => diagnostic_with_expected(
            SyntaxDiagnosticCode::UnexpectedEnd,
            span(source_end, source_end),
            expected,
        ),
        ParseError::UnrecognizedToken {
            token: (start, _, end),
            expected,
        } => diagnostic_with_expected(
            SyntaxDiagnosticCode::UnexpectedToken,
            span(start, end),
            expected,
        ),
        ParseError::ExtraToken {
            token: (start, _, end),
        } => SyntaxDiagnostic::new(SyntaxDiagnosticCode::UnexpectedToken, span(start, end)),
        ParseError::User { error } => error,
    }
}

#[derive(Clone, Copy)]
enum DeclarationKind {
    Entity,
    Event,
    Enum,
    Aggregate,
    Command,
    Projection,
}

impl DeclarationKind {
    fn from_token(token: &Token) -> Option<Self> {
        match token {
            Token::Entity => Some(Self::Entity),
            Token::Event => Some(Self::Event),
            Token::Enum => Some(Self::Enum),
            Token::Aggregate => Some(Self::Aggregate),
            Token::Command => Some(Self::Command),
            Token::Projection => Some(Self::Projection),
            _ => None,
        }
    }

    fn starts_item(self, token: &Token) -> bool {
        match self {
            Self::Entity => matches!(
                token,
                Token::Key | Token::Field | Token::Invariant | Token::Index | Token::Reference
            ),
            Self::Event => matches!(token, Token::Colon),
            Self::Enum => matches!(token, Token::Identifier(_) | Token::IdempotencyKey),
            Self::Aggregate => matches!(
                token,
                Token::Root
                    | Token::Child
                    | Token::PartitionBy
                    | Token::ConflictKey
                    | Token::Invariant
            ),
            Self::Command => matches!(
                token,
                Token::Input
                    | Token::IdempotencyKey
                    | Token::Read
                    | Token::Mutate
                    | Token::Create
                    | Token::Require
                    | Token::Set
                    | Token::Emit
                    | Token::Return
            ),
            Self::Projection => matches!(
                token,
                Token::Source | Token::Where | Token::Key | Token::Measure | Token::Frontier
            ),
        }
    }
}

#[derive(Default)]
struct ListFrame {
    completed: usize,
    active: bool,
}

fn validate_collection_bounds(tokens: &[SpannedToken]) -> Result<(), SyntaxDiagnostic> {
    let mut brace_depth = 0_usize;
    let mut declaration_count = 0_usize;
    let mut pending_declaration: Option<DeclarationKind> = None;
    let mut active_declaration: Option<DeclarationKind> = None;
    let mut declaration_items = 0_usize;
    let mut parenthesized_lists = Vec::<ListFrame>::new();
    let mut object_fields = Vec::<(usize, usize)>::new();
    let mut type_argument_depth = 0_usize;
    let mut previous: Option<&Token> = None;

    for token in tokens {
        let opens_type_arguments = matches!(token.value, Token::Less)
            && previous.is_some_and(|previous| {
                matches!(
                    previous,
                    Token::Decimal
                        | Token::Money
                        | Token::String
                        | Token::Bytes
                        | Token::Optional
                        | Token::List
                )
            });
        if opens_type_arguments {
            type_argument_depth = type_argument_depth.saturating_add(1);
        }

        if type_argument_depth == 0 {
            match &token.value {
                Token::LeftParen => {
                    start_list_entry(&mut parenthesized_lists, token.span)?;
                    parenthesized_lists.push(ListFrame::default());
                }
                Token::Comma => {
                    if let Some(frame) = parenthesized_lists.last_mut()
                        && frame.active
                    {
                        frame.completed = frame.completed.saturating_add(1);
                        frame.active = false;
                    }
                }
                Token::RightParen => {
                    parenthesized_lists.pop();
                }
                _ => start_list_entry(&mut parenthesized_lists, token.span)?,
            }
        }

        if brace_depth == 1
            && active_declaration.is_none()
            && let Some(kind) = DeclarationKind::from_token(&token.value)
        {
            increment_collection(&mut declaration_count, token.span)?;
            pending_declaration = Some(kind);
        }

        if brace_depth == 2
            && let Some(kind) = active_declaration
            && kind.starts_item(&token.value)
        {
            if matches!(kind, DeclarationKind::Enum) {
                increment_list(&mut declaration_items, token.span)?;
            } else {
                increment_collection(&mut declaration_items, token.span)?;
            }
        }

        if matches!(token.value, Token::Colon)
            && brace_depth >= 3
            && let Some((_, fields)) = object_fields.last_mut()
        {
            increment_list(fields, token.span)?;
        }

        match &token.value {
            Token::LeftBrace => {
                brace_depth = brace_depth.saturating_add(1);
                if brace_depth == 2
                    && let Some(kind) = pending_declaration.take()
                {
                    active_declaration = Some(kind);
                    declaration_items = 0;
                } else if brace_depth >= 3 {
                    object_fields.push((brace_depth, 0));
                }
            }
            Token::RightBrace => {
                if object_fields
                    .last()
                    .is_some_and(|(depth, _)| *depth == brace_depth)
                {
                    object_fields.pop();
                }
                if brace_depth == 2 {
                    active_declaration = None;
                    declaration_items = 0;
                }
                brace_depth = brace_depth.saturating_sub(1);
            }
            _ => {}
        }

        if type_argument_depth > 0 && matches!(token.value, Token::Greater) {
            type_argument_depth -= 1;
        }
        previous = Some(&token.value);
    }

    Ok(())
}

fn start_list_entry(frames: &mut [ListFrame], span: Span) -> Result<(), SyntaxDiagnostic> {
    if let Some(frame) = frames.last_mut()
        && !frame.active
    {
        if frame.completed == MAX_LIST_ITEMS {
            return Err(SyntaxDiagnostic::new(
                SyntaxDiagnosticCode::CollectionLimit,
                span,
            ));
        }
        frame.active = true;
    }
    Ok(())
}

fn increment_collection(count: &mut usize, span: Span) -> Result<(), SyntaxDiagnostic> {
    if *count == MAX_DECLARATION_ITEMS {
        return Err(SyntaxDiagnostic::new(
            SyntaxDiagnosticCode::CollectionLimit,
            span,
        ));
    }
    *count += 1;
    Ok(())
}

fn increment_list(count: &mut usize, span: Span) -> Result<(), SyntaxDiagnostic> {
    if *count == MAX_LIST_ITEMS {
        return Err(SyntaxDiagnostic::new(
            SyntaxDiagnosticCode::CollectionLimit,
            span,
        ));
    }
    *count += 1;
    Ok(())
}

fn diagnostic_with_expected(
    code: SyntaxDiagnosticCode,
    span: Span,
    expected: Vec<String>,
) -> SyntaxDiagnostic {
    let mut names: Vec<_> = expected
        .iter()
        .filter_map(|name| expected_name(name))
        .collect();
    names.sort_unstable();
    names.dedup();
    names.truncate(MAX_EXPECTED_TOKENS);
    SyntaxDiagnostic::with_expected(code, span, names)
        .unwrap_or_else(|_| SyntaxDiagnostic::new(code, span))
}

fn expected_name(rendered: &str) -> Option<&'static str> {
    let unquoted = rendered
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(rendered);
    EXPECTED_TOKEN_NAMES
        .iter()
        .copied()
        .find(|candidate| *candidate == unquoted)
}

const EXPECTED_TOKEN_NAMES: &[&str] = &[
    "contract",
    "version",
    "entity",
    "key",
    "field",
    "invariant",
    "index",
    "reference",
    "event",
    "enum",
    "aggregate",
    "root",
    "child",
    "partition_by",
    "conflict_key",
    "projection",
    "source",
    "where",
    "measure",
    "count",
    "sum",
    "frontier",
    "transactionally_ordered",
    "command",
    "input",
    "idempotency_key",
    "read",
    "mutate",
    "create",
    "as",
    "else",
    "require",
    "set",
    "emit",
    "return",
    "bool",
    "i64",
    "u64",
    "timestamp",
    "date",
    "uuid",
    "decimal",
    "money",
    "string",
    "bytes",
    "optional",
    "list",
    "true",
    "false",
    "null",
    "{",
    "}",
    "(",
    ")",
    "<=",
    ">=",
    "==",
    "!=",
    "&&",
    "||",
    "<",
    ">",
    ",",
    ":",
    ".",
    "=",
    "!",
    "-",
    "*",
    "/",
    "+",
    "fixed decimal literal",
    "unsigned integer literal",
    "string literal",
    "identifier",
];

fn validate_document(document: &ContractDocument) -> Result<(), SyntaxDiagnostic> {
    let mut counter = NodeCounter::default();
    counter.add(3, document.contract.span)?;
    let contract = &document.contract.value;
    counter.collection(
        contract.declarations.len(),
        MAX_DECLARATION_ITEMS,
        document.contract.span,
    )?;
    counter.name(&contract.name)?;
    counter.name(&contract.version)?;
    for declaration in &contract.declarations {
        counter.declaration(declaration)?;
    }
    Ok(())
}

#[derive(Default)]
struct NodeCounter {
    nodes: usize,
}

impl NodeCounter {
    fn add(&mut self, amount: usize, span: Span) -> Result<(), SyntaxDiagnostic> {
        self.nodes = self.nodes.saturating_add(amount);
        if self.nodes > MAX_AST_NODES {
            return Err(SyntaxDiagnostic::new(SyntaxDiagnosticCode::NodeLimit, span));
        }
        Ok(())
    }

    fn collection(&self, length: usize, limit: usize, span: Span) -> Result<(), SyntaxDiagnostic> {
        if length > limit {
            return Err(SyntaxDiagnostic::new(
                SyntaxDiagnosticCode::CollectionLimit,
                span,
            ));
        }
        Ok(())
    }

    fn declaration(&mut self, declaration: &Spanned<Declaration>) -> Result<(), SyntaxDiagnostic> {
        self.add(2, declaration.span)?;
        match &declaration.value {
            Declaration::Entity(entity) => {
                self.add(1, declaration.span)?;
                self.name(&entity.name)?;
                self.collection(entity.items.len(), MAX_DECLARATION_ITEMS, declaration.span)?;
                for item in &entity.items {
                    self.add(2, item.span)?;
                    match &item.value {
                        EntityItem::Key(key) => {
                            self.add(1, item.span)?;
                            self.typed_fields(&key.fields, item.span)?;
                        }
                        EntityItem::Field(field) => self.typed_field(field)?,
                        EntityItem::Invariant(invariant) => self.invariant(invariant)?,
                        EntityItem::Index(index) => {
                            self.add(1, item.span)?;
                            self.name(&index.name)?;
                            self.names(&index.fields, item.span)?;
                        }
                        EntityItem::Reference(reference) => {
                            self.add(1, item.span)?;
                            self.name(&reference.name)?;
                            self.names(&reference.source_fields, item.span)?;
                            self.name(&reference.target_entity)?;
                            self.names(&reference.target_fields, item.span)?;
                        }
                    }
                }
            }
            Declaration::Event(event) => {
                self.add(1, declaration.span)?;
                self.name(&event.name)?;
                self.collection(event.fields.len(), MAX_DECLARATION_ITEMS, declaration.span)?;
                for field in &event.fields {
                    self.add(1, field.span)?;
                    self.typed_field(&field.value)?;
                }
            }
            Declaration::Enum(enumeration) => {
                self.add(1, declaration.span)?;
                self.name(&enumeration.name)?;
                self.names(&enumeration.variants, declaration.span)?;
            }
            Declaration::Aggregate(aggregate) => {
                self.add(1, declaration.span)?;
                self.name(&aggregate.name)?;
                self.collection(
                    aggregate.items.len(),
                    MAX_DECLARATION_ITEMS,
                    declaration.span,
                )?;
                for item in &aggregate.items {
                    self.add(2, item.span)?;
                    match &item.value {
                        AggregateItem::Root(name) | AggregateItem::Child(name) => {
                            self.name(name)?
                        }
                        AggregateItem::PartitionBy(expression) => self.expression(expression)?,
                        AggregateItem::ConflictKey(expressions) => {
                            self.expressions(expressions, item.span)?;
                        }
                        AggregateItem::Invariant(invariant) => self.invariant(invariant)?,
                    }
                }
            }
            Declaration::Command(command) => {
                self.add(1, declaration.span)?;
                self.command(command, declaration.span)?;
            }
            Declaration::Projection(projection) => {
                self.add(1, declaration.span)?;
                self.projection(projection, declaration.span)?;
            }
        }
        Ok(())
    }

    fn command(
        &mut self,
        command: &CommandDeclaration,
        span: Span,
    ) -> Result<(), SyntaxDiagnostic> {
        self.name(&command.name)?;
        let items = command
            .inputs
            .len()
            .saturating_add(usize::from(command.idempotency.is_some()))
            .saturating_add(command.bindings.len())
            .saturating_add(command.requirements.len())
            .saturating_add(command.effects.len())
            .saturating_add(1);
        self.collection(items, MAX_DECLARATION_ITEMS, span)?;
        for input in &command.inputs {
            self.add(2, input.span)?;
            self.typed_field(&input.value.field)?;
        }
        if let Some(idempotency) = &command.idempotency {
            self.add(2, idempotency.span)?;
            self.expression(&idempotency.value.expression)?;
        }
        for binding in &command.bindings {
            self.add(2, binding.span)?;
            match &binding.value {
                Binding::Read(entity) | Binding::Mutate(entity) | Binding::Create(entity) => {
                    self.entity_binding(entity, binding.span)?;
                }
            }
        }
        for requirement in &command.requirements {
            self.add(2, requirement.span)?;
            self.name(&requirement.value.name)?;
            self.expression(&requirement.value.condition)?;
            self.outcome(&requirement.value.rejection)?;
        }
        for effect in &command.effects {
            self.add(2, effect.span)?;
            match &effect.value {
                Effect::Set(set) => {
                    self.add(1, effect.span)?;
                    self.path(&set.target)?;
                    self.expression(&set.value)?;
                }
                Effect::Emit(emit) => {
                    self.add(1, effect.span)?;
                    self.name(&emit.event)?;
                    self.object(&emit.payload)?;
                }
            }
        }
        self.add(2, command.return_clause.span)?;
        self.outcome(&command.return_clause.value.outcome)
    }

    fn projection(
        &mut self,
        projection: &ProjectionDeclaration,
        span: Span,
    ) -> Result<(), SyntaxDiagnostic> {
        self.name(&projection.name)?;
        self.name(&projection.source_event)?;
        let items = 3_usize
            .saturating_add(usize::from(projection.filter.is_some()))
            .saturating_add(projection.measures.len());
        self.collection(items, MAX_DECLARATION_ITEMS, span)?;
        if let Some(filter) = &projection.filter {
            self.expression(filter)?;
        }
        self.expressions(&projection.key, span)?;
        for measure in &projection.measures {
            self.add(2, measure.span)?;
            self.name(&measure.value.name)?;
            self.add(2, measure.value.aggregation.span)?;
            if let Aggregation::Sum(expression) = &measure.value.aggregation.value {
                self.expression(expression)?;
            }
        }
        self.add(2, projection.frontier.span)
    }

    fn entity_binding(
        &mut self,
        binding: &EntityBinding,
        span: Span,
    ) -> Result<(), SyntaxDiagnostic> {
        self.add(1, span)?;
        self.name(&binding.entity)?;
        self.expressions(&binding.arguments, span)?;
        self.name(&binding.binding)?;
        self.outcome(&binding.failure)
    }

    fn outcome(&mut self, outcome: &Spanned<OutcomeExpression>) -> Result<(), SyntaxDiagnostic> {
        self.add(2, outcome.span)?;
        self.name(&outcome.value.name)?;
        self.object(&outcome.value.payload)
    }

    fn object(&mut self, object: &Spanned<ObjectLiteral>) -> Result<(), SyntaxDiagnostic> {
        self.add(2, object.span)?;
        self.collection(object.value.fields.len(), MAX_LIST_ITEMS, object.span)?;
        for field in &object.value.fields {
            self.add(2, field.span)?;
            self.name(&field.value.name)?;
            self.expression(&field.value.value)?;
        }
        Ok(())
    }

    fn invariant(&mut self, invariant: &InvariantDeclaration) -> Result<(), SyntaxDiagnostic> {
        self.add(1, invariant.name.span)?;
        self.name(&invariant.name)?;
        self.expression(&invariant.expression)
    }

    fn typed_fields(
        &mut self,
        fields: &[Spanned<TypedField>],
        span: Span,
    ) -> Result<(), SyntaxDiagnostic> {
        self.collection(fields.len(), MAX_LIST_ITEMS, span)?;
        for field in fields {
            self.add(1, field.span)?;
            self.typed_field(&field.value)?;
        }
        Ok(())
    }

    fn typed_field(&mut self, field: &TypedField) -> Result<(), SyntaxDiagnostic> {
        self.add(1, field.name.span)?;
        self.name(&field.name)?;
        self.type_expression(&field.ty)
    }

    fn type_expression(&mut self, ty: &Spanned<TypeExpression>) -> Result<(), SyntaxDiagnostic> {
        self.add(2, ty.span)?;
        match &ty.value {
            TypeExpression::Decimal { precision, scale } => {
                self.name(precision)?;
                self.name(scale)
            }
            TypeExpression::Money { currency }
            | TypeExpression::String { maximum: currency }
            | TypeExpression::Bytes { maximum: currency } => self.add(1, currency.span),
            TypeExpression::Optional(inner) => self.type_expression(inner),
            TypeExpression::List { element, maximum } => {
                self.type_expression(element)?;
                self.add(1, maximum.span)
            }
            TypeExpression::Named(name) => self.name(name),
            TypeExpression::Bool
            | TypeExpression::I64
            | TypeExpression::U64
            | TypeExpression::Timestamp
            | TypeExpression::Date
            | TypeExpression::Uuid => Ok(()),
        }
    }

    fn expressions(
        &mut self,
        expressions: &[Spanned<Expression>],
        span: Span,
    ) -> Result<(), SyntaxDiagnostic> {
        self.collection(expressions.len(), MAX_LIST_ITEMS, span)?;
        for expression in expressions {
            self.expression(expression)?;
        }
        Ok(())
    }

    fn expression(&mut self, expression: &Spanned<Expression>) -> Result<(), SyntaxDiagnostic> {
        self.add(2, expression.span)?;
        match &expression.value {
            Expression::Literal(literal) => self.add(2, literal.span),
            Expression::Path(path) => self.path(path),
            Expression::Parenthesized(inner) => self.expression(inner),
            Expression::Unary { operator, operand } => {
                self.add(2, operator.span)?;
                self.expression(operand)
            }
            Expression::Binary {
                left,
                operator,
                right,
            } => {
                self.expression(left)?;
                self.add(2, operator.span)?;
                self.expression(right)
            }
        }
    }

    fn path(&mut self, path: &Spanned<Path>) -> Result<(), SyntaxDiagnostic> {
        self.add(2, path.span)?;
        for segment in &path.value.segments {
            self.name(segment)?;
        }
        Ok(())
    }

    fn names(&mut self, names: &[Spanned<String>], span: Span) -> Result<(), SyntaxDiagnostic> {
        self.collection(names.len(), MAX_LIST_ITEMS, span)?;
        for name in names {
            self.name(name)?;
        }
        Ok(())
    }

    fn name(&mut self, name: &Spanned<String>) -> Result<(), SyntaxDiagnostic> {
        self.add(1, name.span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ast_node_budget_accepts_the_limit_and_rejects_the_next_node() {
        let mut counter = NodeCounter::default();
        counter
            .add(MAX_AST_NODES, Span::ZERO)
            .expect("the exact AST-node limit is valid");
        let diagnostic = counter
            .add(1, Span::ZERO)
            .expect_err("one node above the AST limit must fail");
        assert_eq!(diagnostic.code(), SyntaxDiagnosticCode::NodeLimit);
    }
}
