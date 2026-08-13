//! Separate bounded parser and formatter for migration grammar version 1.

use std::fmt::Write as _;

use crate::ast::{BinaryOperator, Expression, Literal, Path, Span, Spanned, UnaryOperator};
use crate::diagnostic::{SyntaxDiagnostic, SyntaxDiagnosticCode, SyntaxDiagnostics};
use crate::lexer::{SpannedToken, Token, lex};
use crate::limits::{MAX_DECLARATION_ITEMS, MAX_LIST_ITEMS, MAX_MIGRATION_SOURCE_BYTES};

/// One parsed migration source document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationDocument {
    /// The document's single top-level migration.
    pub migration: Spanned<Migration>,
}

/// A direct parent-to-successor migration declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Migration {
    /// Contract lineage.
    pub lineage: Spanned<String>,
    /// Positive parent contract-version lexeme.
    pub from: Spanned<String>,
    /// Positive successor contract-version lexeme.
    pub to: Spanned<String>,
    /// Source-ordered migration declarations.
    pub declarations: Vec<Spanned<MigrationDeclaration>>,
}

/// Closed migration declaration families in grammar version 1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationDeclaration {
    /// Semantic-preserving identity rename.
    Rename(MigrationRename),
    /// Logical identity retirement.
    Retire(MigrationRetirement),
    /// Entity-local row transformation.
    Transform(MigrationTransform),
    /// Exhaustive enum mapping.
    EnumMap(MigrationEnumMap),
    /// Explicit acknowledgement for a gate-C semantic change.
    Acknowledge(MigrationAcknowledgement),
}

/// Stable identity kinds addressable by rename or retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationIdentityKind {
    /// Entity identity.
    Entity,
    /// Entity, event, input, outcome, or projection-result field identity.
    Field,
    /// Enumeration identity.
    Enum,
    /// Enum variant identity.
    EnumVariant,
    /// Command identity.
    Command,
    /// Durable event identity.
    Event,
    /// Command outcome identity.
    Outcome,
    /// Projection identity.
    Projection,
}

/// One semantic-preserving rename.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationRename {
    /// Stable identity kind.
    pub kind: Spanned<MigrationIdentityKind>,
    /// Qualified predecessor name.
    pub old_path: Vec<Spanned<String>>,
    /// Successor leaf name.
    pub new_name: Spanned<String>,
}

/// One logical retirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationRetirement {
    /// Stable identity kind.
    pub kind: Spanned<MigrationIdentityKind>,
    /// Qualified predecessor name.
    pub path: Vec<Spanned<String>>,
}

/// One row-local entity transform.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationTransform {
    /// Entity name in the predecessor contract.
    pub entity: Spanned<String>,
    /// Ordered clauses. Execution order is compiler-derived after dependency checks.
    pub clauses: Vec<Spanned<MigrationTransformClause>>,
}

/// Closed entity-transform clauses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationTransformClause {
    /// Sets one successor field from a deterministic expression.
    Set {
        /// Successor field name.
        field: Spanned<String>,
        /// Row-local expression.
        expression: Spanned<Expression>,
    },
    /// Replaces one field through a named checked conversion.
    Replace {
        /// Predecessor field.
        old_field: Spanned<String>,
        /// Successor field.
        new_field: Spanned<String>,
        /// Closed conversion-registry name.
        conversion: Spanned<String>,
    },
    /// Requires a row-local Boolean assertion.
    Require(Spanned<Expression>),
    /// Computes a replacement primary key.
    Rekey(Vec<Spanned<Expression>>),
}

/// One exhaustive enumeration mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationEnumMap {
    /// Enumeration name.
    pub enumeration: Spanned<String>,
    /// Source-ordered mappings; the compiler canonicalizes by predecessor stable ID.
    pub mappings: Vec<Spanned<MigrationEnumMapping>>,
}

/// One predecessor-to-successor enum-variant mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationEnumMapping {
    /// Predecessor variant.
    pub from: Spanned<String>,
    /// Successor variant.
    pub to: Spanned<String>,
}

/// Gate-C acknowledgement kinds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationAcknowledgementKind {
    /// Partition-key derivation changes.
    Repartition,
    /// Aggregate membership changes.
    Aggregate,
    /// Conflict-domain derivation changes.
    Conflict,
}

/// One explicit acknowledgement of a compiler-derived semantic change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationAcknowledgement {
    /// Acknowledged change kind.
    pub kind: Spanned<MigrationAcknowledgementKind>,
    /// Affected aggregate name.
    pub aggregate: Spanned<String>,
}

/// Parses one bounded migration grammar-version-1 document.
pub fn parse_migration(source: &str) -> Result<MigrationDocument, SyntaxDiagnostics> {
    if source.len() > MAX_MIGRATION_SOURCE_BYTES {
        return Err(failure(
            SyntaxDiagnosticCode::SourceLimit,
            Span::new(0, MAX_MIGRATION_SOURCE_BYTES).unwrap_or(Span::ZERO),
        ));
    }
    let tokens = lex(source)?;
    Parser::new(tokens).parse()
}

/// Validates UTF-8 and parses one bounded migration source document.
pub fn parse_migration_bytes(source: &[u8]) -> Result<MigrationDocument, SyntaxDiagnostics> {
    if source.len() > MAX_MIGRATION_SOURCE_BYTES {
        return Err(failure(
            SyntaxDiagnosticCode::SourceLimit,
            Span::new(0, MAX_MIGRATION_SOURCE_BYTES).unwrap_or(Span::ZERO),
        ));
    }
    match std::str::from_utf8(source) {
        Ok(source) => parse_migration(source),
        Err(error) => {
            let start = error.valid_up_to();
            let end = error
                .error_len()
                .map_or(source.len(), |length| start.saturating_add(length));
            Err(failure(
                SyntaxDiagnosticCode::InvalidToken,
                Span::new(start, end.min(source.len())).unwrap_or(Span::ZERO),
            ))
        }
    }
}

struct Parser {
    tokens: Vec<SpannedToken>,
    cursor: usize,
}

impl Parser {
    const fn new(tokens: Vec<SpannedToken>) -> Self {
        Self { tokens, cursor: 0 }
    }

    fn parse(mut self) -> Result<MigrationDocument, SyntaxDiagnostics> {
        let start = self.expect_word("migration")?.span;
        let lineage = self.identifier()?;
        self.expect_word("from")?;
        let from = self.unsigned()?;
        self.expect_word("to")?;
        let to = self.unsigned()?;
        self.expect_simple(TokenKind::LeftBrace)?;
        let mut declarations = Vec::new();
        while !self.at(TokenKind::RightBrace) {
            if declarations.len() == MAX_DECLARATION_ITEMS {
                return Err(failure(SyntaxDiagnosticCode::CollectionLimit, self.span()));
            }
            declarations.push(self.declaration()?);
        }
        let end = self.expect_simple(TokenKind::RightBrace)?.span;
        if self.cursor != self.tokens.len() {
            return Err(failure(SyntaxDiagnosticCode::UnexpectedToken, self.span()));
        }
        Ok(MigrationDocument {
            migration: Spanned::new(
                Migration {
                    lineage,
                    from,
                    to,
                    declarations,
                },
                start.cover(end),
            ),
        })
    }

    fn declaration(&mut self) -> Result<Spanned<MigrationDeclaration>, SyntaxDiagnostics> {
        let start = self.span();
        let value = if self.take_word("rename") {
            MigrationDeclaration::Rename(self.rename()?)
        } else if self.take_word("retire") {
            MigrationDeclaration::Retire(self.retirement()?)
        } else if self.take_word("transform") {
            MigrationDeclaration::Transform(self.transform()?)
        } else if self.take_word("map") {
            MigrationDeclaration::EnumMap(self.enum_map()?)
        } else if self.take_word("acknowledge") {
            MigrationDeclaration::Acknowledge(self.acknowledgement()?)
        } else {
            return Err(failure(SyntaxDiagnosticCode::UnexpectedToken, start));
        };
        let end = self.previous_span();
        Ok(Spanned::new(value, start.cover(end)))
    }

    fn rename(&mut self) -> Result<MigrationRename, SyntaxDiagnostics> {
        let kind = self.identity_kind()?;
        let old_path = self.qualified_path_until("to")?;
        self.expect_word("to")?;
        let new_name = self.identifier()?;
        Ok(MigrationRename {
            kind,
            old_path,
            new_name,
        })
    }

    fn retirement(&mut self) -> Result<MigrationRetirement, SyntaxDiagnostics> {
        let kind = self.identity_kind()?;
        let path = self.qualified_path()?;
        Ok(MigrationRetirement { kind, path })
    }

    fn transform(&mut self) -> Result<MigrationTransform, SyntaxDiagnostics> {
        let entity = self.identifier()?;
        self.expect_simple(TokenKind::LeftBrace)?;
        let mut clauses = Vec::new();
        while !self.at(TokenKind::RightBrace) {
            if clauses.len() == MAX_DECLARATION_ITEMS {
                return Err(failure(SyntaxDiagnosticCode::CollectionLimit, self.span()));
            }
            let start = self.span();
            let value = if self.take_simple(TokenKind::Set) {
                let field = self.identifier()?;
                self.expect_simple(TokenKind::Equal)?;
                MigrationTransformClause::Set {
                    field,
                    expression: self.expression_until(Self::at_transform_boundary)?,
                }
            } else if self.take_word("replace") {
                let old_field = self.identifier()?;
                self.expect_word("with")?;
                let new_field = self.identifier()?;
                self.expect_word("using")?;
                let conversion = self.identifier()?;
                MigrationTransformClause::Replace {
                    old_field,
                    new_field,
                    conversion,
                }
            } else if self.take_simple(TokenKind::Require) {
                MigrationTransformClause::Require(
                    self.expression_until(Self::at_transform_boundary)?,
                )
            } else if self.take_word("rekey") {
                self.expect_simple(TokenKind::LeftParen)?;
                let expressions = self.expression_list()?;
                self.expect_simple(TokenKind::RightParen)?;
                MigrationTransformClause::Rekey(expressions)
            } else {
                return Err(failure(SyntaxDiagnosticCode::UnexpectedToken, start));
            };
            clauses.push(Spanned::new(value, start.cover(self.previous_span())));
        }
        self.expect_simple(TokenKind::RightBrace)?;
        if clauses.is_empty() {
            return Err(failure(
                SyntaxDiagnosticCode::UnexpectedEnd,
                self.previous_span(),
            ));
        }
        Ok(MigrationTransform { entity, clauses })
    }

    fn enum_map(&mut self) -> Result<MigrationEnumMap, SyntaxDiagnostics> {
        self.expect_simple(TokenKind::Enum)?;
        let enumeration = self.identifier()?;
        self.expect_simple(TokenKind::LeftBrace)?;
        let mut mappings = Vec::new();
        while !self.at(TokenKind::RightBrace) {
            if mappings.len() == MAX_LIST_ITEMS {
                return Err(failure(SyntaxDiagnosticCode::CollectionLimit, self.span()));
            }
            let from = self.identifier()?;
            self.expect_simple(TokenKind::Arrow)?;
            let to = self.identifier()?;
            let span = from.span.cover(to.span);
            mappings.push(Spanned::new(MigrationEnumMapping { from, to }, span));
        }
        self.expect_simple(TokenKind::RightBrace)?;
        if mappings.is_empty() {
            return Err(failure(
                SyntaxDiagnosticCode::UnexpectedEnd,
                self.previous_span(),
            ));
        }
        Ok(MigrationEnumMap {
            enumeration,
            mappings,
        })
    }

    fn acknowledgement(&mut self) -> Result<MigrationAcknowledgement, SyntaxDiagnostics> {
        let token = self.next()?;
        let kind = match token_word(&token.value) {
            Some("repartition") => MigrationAcknowledgementKind::Repartition,
            Some("aggregate") => MigrationAcknowledgementKind::Aggregate,
            Some("conflict") => MigrationAcknowledgementKind::Conflict,
            _ => return Err(failure(SyntaxDiagnosticCode::UnexpectedToken, token.span)),
        };
        Ok(MigrationAcknowledgement {
            kind: Spanned::new(kind, token.span),
            aggregate: self.identifier()?,
        })
    }

    fn identity_kind(&mut self) -> Result<Spanned<MigrationIdentityKind>, SyntaxDiagnostics> {
        let token = self.next()?;
        let kind = match token_word(&token.value) {
            Some("entity") => MigrationIdentityKind::Entity,
            Some("field") => MigrationIdentityKind::Field,
            Some("enum") => MigrationIdentityKind::Enum,
            Some("variant") => MigrationIdentityKind::EnumVariant,
            Some("command") => MigrationIdentityKind::Command,
            Some("event") => MigrationIdentityKind::Event,
            Some("outcome") => MigrationIdentityKind::Outcome,
            Some("projection") => MigrationIdentityKind::Projection,
            _ => return Err(failure(SyntaxDiagnosticCode::UnexpectedToken, token.span)),
        };
        Ok(Spanned::new(kind, token.span))
    }

    fn qualified_path_until(
        &mut self,
        terminal: &str,
    ) -> Result<Vec<Spanned<String>>, SyntaxDiagnostics> {
        let path = self.qualified_path()?;
        if !self.at_word(terminal) {
            return Err(failure(SyntaxDiagnosticCode::UnexpectedToken, self.span()));
        }
        Ok(path)
    }

    fn qualified_path(&mut self) -> Result<Vec<Spanned<String>>, SyntaxDiagnostics> {
        let mut segments = vec![self.identifier()?];
        while self.take_simple(TokenKind::Dot) {
            if segments.len() == MAX_LIST_ITEMS {
                return Err(failure(SyntaxDiagnosticCode::CollectionLimit, self.span()));
            }
            segments.push(self.identifier()?);
        }
        Ok(segments)
    }

    fn expression_list(&mut self) -> Result<Vec<Spanned<Expression>>, SyntaxDiagnostics> {
        let mut expressions = Vec::new();
        if self.at(TokenKind::RightParen) {
            return Err(failure(SyntaxDiagnosticCode::UnexpectedToken, self.span()));
        }
        loop {
            expressions.push(self.expression_until(|parser| {
                parser.at(TokenKind::Comma) || parser.at(TokenKind::RightParen)
            })?);
            if !self.take_simple(TokenKind::Comma) {
                break;
            }
            if expressions.len() == MAX_LIST_ITEMS {
                return Err(failure(SyntaxDiagnosticCode::CollectionLimit, self.span()));
            }
        }
        Ok(expressions)
    }

    fn expression_until(
        &mut self,
        boundary: fn(&Self) -> bool,
    ) -> Result<Spanned<Expression>, SyntaxDiagnostics> {
        let start = self.cursor;
        let mut depth = 0_usize;
        while self.cursor < self.tokens.len() {
            if depth == 0 && boundary(self) {
                break;
            }
            match self.tokens[self.cursor].value {
                Token::LeftParen => depth = depth.saturating_add(1),
                Token::RightParen if depth > 0 => depth -= 1,
                Token::RightParen => break,
                _ => {}
            }
            self.cursor += 1;
        }
        if self.cursor == start {
            return Err(failure(SyntaxDiagnosticCode::UnexpectedToken, self.span()));
        }
        ExpressionParser::new(&self.tokens[start..self.cursor]).parse()
    }

    fn at_transform_boundary(parser: &Self) -> bool {
        parser.at(TokenKind::RightBrace)
            || parser.at(TokenKind::Set)
            || parser.at(TokenKind::Require)
            || parser.at_word("replace")
            || parser.at_word("rekey")
    }

    fn identifier(&mut self) -> Result<Spanned<String>, SyntaxDiagnostics> {
        let token = self.next()?;
        token_identifier(&token.value)
            .map(|value| Spanned::new(value.to_owned(), token.span))
            .ok_or_else(|| failure(SyntaxDiagnosticCode::UnexpectedToken, token.span))
    }

    fn unsigned(&mut self) -> Result<Spanned<String>, SyntaxDiagnostics> {
        let token = self.next()?;
        match token.value {
            Token::UIntLiteral(value) if value != "0" => Ok(Spanned::new(value, token.span)),
            _ => Err(failure(SyntaxDiagnosticCode::InvalidToken, token.span)),
        }
    }

    fn expect_word(&mut self, word: &str) -> Result<SpannedToken, SyntaxDiagnostics> {
        let token = self.next()?;
        if token_word(&token.value) == Some(word) {
            Ok(token)
        } else {
            Err(failure(SyntaxDiagnosticCode::UnexpectedToken, token.span))
        }
    }

    fn take_word(&mut self, word: &str) -> bool {
        if self.at_word(word) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn at_word(&self, word: &str) -> bool {
        self.tokens
            .get(self.cursor)
            .and_then(|token| token_word(&token.value))
            == Some(word)
    }

    fn expect_simple(&mut self, kind: TokenKind) -> Result<SpannedToken, SyntaxDiagnostics> {
        let token = self.next()?;
        if token_kind(&token.value) == Some(kind) {
            Ok(token)
        } else {
            Err(failure(SyntaxDiagnosticCode::UnexpectedToken, token.span))
        }
    }

    fn take_simple(&mut self, kind: TokenKind) -> bool {
        if self.at(kind) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn at(&self, kind: TokenKind) -> bool {
        self.tokens
            .get(self.cursor)
            .and_then(|token| token_kind(&token.value))
            == Some(kind)
    }

    fn next(&mut self) -> Result<SpannedToken, SyntaxDiagnostics> {
        let token =
            self.tokens.get(self.cursor).cloned().ok_or_else(|| {
                failure(SyntaxDiagnosticCode::UnexpectedEnd, self.previous_span())
            })?;
        self.cursor += 1;
        Ok(token)
    }

    fn span(&self) -> Span {
        self.tokens
            .get(self.cursor)
            .map_or_else(|| self.previous_span(), |token| token.span)
    }

    fn previous_span(&self) -> Span {
        self.cursor
            .checked_sub(1)
            .and_then(|index| self.tokens.get(index))
            .map_or(Span::ZERO, |token| token.span)
    }
}

struct ExpressionParser<'a> {
    tokens: &'a [SpannedToken],
    cursor: usize,
}

impl<'a> ExpressionParser<'a> {
    const fn new(tokens: &'a [SpannedToken]) -> Self {
        Self { tokens, cursor: 0 }
    }

    fn parse(mut self) -> Result<Spanned<Expression>, SyntaxDiagnostics> {
        let expression = self.precedence(0)?;
        if self.cursor != self.tokens.len() {
            return Err(failure(
                SyntaxDiagnosticCode::UnexpectedToken,
                self.tokens[self.cursor].span,
            ));
        }
        Ok(expression)
    }

    fn precedence(&mut self, minimum: u8) -> Result<Spanned<Expression>, SyntaxDiagnostics> {
        let mut left = self.prefix()?;
        while let Some((operator, precedence)) = self.binary() {
            if precedence < minimum {
                break;
            }
            let operator_span = self.tokens[self.cursor].span;
            self.cursor += 1;
            let right = self.precedence(precedence.saturating_add(1))?;
            let span = left.span.cover(right.span);
            left = Spanned::new(
                Expression::Binary {
                    left: Box::new(left),
                    operator: Spanned::new(operator, operator_span),
                    right: Box::new(right),
                },
                span,
            );
        }
        Ok(left)
    }

    fn prefix(&mut self) -> Result<Spanned<Expression>, SyntaxDiagnostics> {
        let token = self
            .tokens
            .get(self.cursor)
            .cloned()
            .ok_or_else(|| failure(SyntaxDiagnosticCode::UnexpectedEnd, Span::ZERO))?;
        match token.value {
            Token::Bang | Token::Minus => {
                self.cursor += 1;
                let operator = if matches!(token.value, Token::Bang) {
                    UnaryOperator::Not
                } else {
                    UnaryOperator::Negate
                };
                let operand = self.prefix()?;
                let span = token.span.cover(operand.span);
                Ok(Spanned::new(
                    Expression::Unary {
                        operator: Spanned::new(operator, token.span),
                        operand: Box::new(operand),
                    },
                    span,
                ))
            }
            Token::LeftParen => {
                self.cursor += 1;
                let expression = self.precedence(0)?;
                let close = self
                    .tokens
                    .get(self.cursor)
                    .ok_or_else(|| failure(SyntaxDiagnosticCode::UnexpectedEnd, token.span))?;
                if !matches!(close.value, Token::RightParen) {
                    return Err(failure(SyntaxDiagnosticCode::UnexpectedToken, close.span));
                }
                self.cursor += 1;
                let span = token.span.cover(close.span);
                Ok(Spanned::new(
                    Expression::Parenthesized(Box::new(expression)),
                    span,
                ))
            }
            _ if token_identifier(&token.value).is_some() => self.path(),
            Token::True
            | Token::False
            | Token::Null
            | Token::UIntLiteral(_)
            | Token::FixedDecimalLiteral(_)
            | Token::StringLiteral(_) => {
                self.cursor += 1;
                let literal = match token.value {
                    Token::True => Literal::Bool(true),
                    Token::False => Literal::Bool(false),
                    Token::Null => Literal::Null,
                    Token::UIntLiteral(value) => Literal::UInt(value),
                    Token::FixedDecimalLiteral(value) => Literal::FixedDecimal(value),
                    Token::StringLiteral(value) => Literal::String(value),
                    _ => unreachable!("literal alternatives are exhaustive"),
                };
                Ok(Spanned::new(
                    Expression::Literal(Spanned::new(literal, token.span)),
                    token.span,
                ))
            }
            _ => Err(failure(SyntaxDiagnosticCode::UnexpectedToken, token.span)),
        }
    }

    fn path(&mut self) -> Result<Spanned<Expression>, SyntaxDiagnostics> {
        let start = self.tokens[self.cursor].span;
        let mut segments = Vec::new();
        loop {
            let token = self
                .tokens
                .get(self.cursor)
                .cloned()
                .ok_or_else(|| failure(SyntaxDiagnosticCode::UnexpectedEnd, start))?;
            let value = token_identifier(&token.value)
                .ok_or_else(|| failure(SyntaxDiagnosticCode::UnexpectedToken, token.span))?;
            segments.push(Spanned::new(value.to_owned(), token.span));
            self.cursor += 1;
            if !self
                .tokens
                .get(self.cursor)
                .is_some_and(|token| matches!(token.value, Token::Dot))
            {
                break;
            }
            self.cursor += 1;
        }
        let end = segments.last().map_or(start, |segment| segment.span);
        let span = start.cover(end);
        Ok(Spanned::new(
            Expression::Path(Spanned::new(Path { segments }, span)),
            span,
        ))
    }

    fn binary(&self) -> Option<(BinaryOperator, u8)> {
        let token = &self.tokens.get(self.cursor)?.value;
        Some(match token {
            Token::OrOr => (BinaryOperator::Or, 1),
            Token::AndAnd => (BinaryOperator::And, 2),
            Token::EqualEqual => (BinaryOperator::Equal, 3),
            Token::BangEqual => (BinaryOperator::NotEqual, 3),
            Token::Less => (BinaryOperator::Less, 4),
            Token::LessEqual => (BinaryOperator::LessEqual, 4),
            Token::Greater => (BinaryOperator::Greater, 4),
            Token::GreaterEqual => (BinaryOperator::GreaterEqual, 4),
            Token::Plus => (BinaryOperator::Add, 5),
            Token::Minus => (BinaryOperator::Subtract, 5),
            Token::Star => (BinaryOperator::Multiply, 6),
            Token::Slash => (BinaryOperator::Divide, 6),
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TokenKind {
    LeftBrace,
    RightBrace,
    LeftParen,
    RightParen,
    Comma,
    Dot,
    Equal,
    Arrow,
    Set,
    Require,
    Enum,
}

const fn token_kind(token: &Token) -> Option<TokenKind> {
    match token {
        Token::LeftBrace => Some(TokenKind::LeftBrace),
        Token::RightBrace => Some(TokenKind::RightBrace),
        Token::LeftParen => Some(TokenKind::LeftParen),
        Token::RightParen => Some(TokenKind::RightParen),
        Token::Comma => Some(TokenKind::Comma),
        Token::Dot => Some(TokenKind::Dot),
        Token::Equal => Some(TokenKind::Equal),
        Token::Arrow => Some(TokenKind::Arrow),
        Token::Set => Some(TokenKind::Set),
        Token::Require => Some(TokenKind::Require),
        Token::Enum => Some(TokenKind::Enum),
        _ => None,
    }
}

fn token_word(token: &Token) -> Option<&str> {
    match token {
        Token::Identifier(value) => Some(value),
        Token::Entity => Some("entity"),
        Token::Field => Some("field"),
        Token::Enum => Some("enum"),
        Token::Command => Some("command"),
        Token::Event => Some("event"),
        Token::Projection => Some("projection"),
        Token::Aggregate => Some("aggregate"),
        _ => token_identifier(token),
    }
}

fn token_identifier(token: &Token) -> Option<&str> {
    match token {
        Token::Identifier(value) => Some(value.as_str()),
        Token::Workflow => Some("workflow"),
        Token::Initial => Some("initial"),
        Token::State => Some("state"),
        Token::Transition => Some("transition"),
        Token::From => Some("from"),
        Token::To => Some("to"),
        Token::Lease => Some("lease"),
        Token::Claim => Some("claim"),
        Token::Renew => Some("renew"),
        Token::Release => Some("release"),
        Token::Expire => Some("expire"),
        Token::Fence => Some("fence"),
        Token::Owner => Some("owner"),
        Token::ExpiresAt => Some("expires_at"),
        Token::FencingToken => Some("fencing_token"),
        Token::Attempts => Some("attempts"),
        Token::DurationSeconds => Some("duration_seconds"),
        Token::Service => Some("service"),
        Token::UuidV7 => Some("uuid_v7"),
        Token::TransactionTime => Some("transaction_time"),
        Token::On => Some("on"),
        Token::Revision => Some("revision"),
        Token::Stale => Some("stale"),
        Token::Illegal => Some("illegal"),
        Token::Unavailable => Some("unavailable"),
        Token::Invalid => Some("invalid"),
        Token::Exhausted => Some("exhausted"),
        Token::Expired => Some("expired"),
        Token::Active => Some("active"),
        _ => None,
    }
}

fn failure(code: SyntaxDiagnosticCode, span: Span) -> SyntaxDiagnostics {
    SyntaxDiagnostics::single(SyntaxDiagnostic::new(code, span))
}

/// Renders one parsed migration into the canonical grammar-v1 source spelling.
#[must_use]
pub fn format_migration(document: &MigrationDocument) -> String {
    let migration = &document.migration.value;
    let mut output = format!(
        "migration {} from {} to {} {{\n",
        migration.lineage.value, migration.from.value, migration.to.value
    );
    for declaration in &migration.declarations {
        format_declaration(&mut output, &declaration.value);
    }
    output.push_str("}\n");
    output
}

fn format_declaration(output: &mut String, declaration: &MigrationDeclaration) {
    match declaration {
        MigrationDeclaration::Rename(rename) => {
            let _ = writeln!(
                output,
                "  rename {} {} to {}",
                identity_kind(rename.kind.value),
                path(&rename.old_path),
                rename.new_name.value
            );
        }
        MigrationDeclaration::Retire(retirement) => {
            let _ = writeln!(
                output,
                "  retire {} {}",
                identity_kind(retirement.kind.value),
                path(&retirement.path)
            );
        }
        MigrationDeclaration::Transform(transform) => {
            let _ = writeln!(output, "  transform {} {{", transform.entity.value);
            for clause in &transform.clauses {
                match &clause.value {
                    MigrationTransformClause::Set { field, expression } => {
                        let _ = writeln!(
                            output,
                            "    set {} = {}",
                            field.value,
                            format_expression(&expression.value)
                        );
                    }
                    MigrationTransformClause::Replace {
                        old_field,
                        new_field,
                        conversion,
                    } => {
                        let _ = writeln!(
                            output,
                            "    replace {} with {} using {}",
                            old_field.value, new_field.value, conversion.value
                        );
                    }
                    MigrationTransformClause::Require(expression) => {
                        let _ = writeln!(
                            output,
                            "    require {}",
                            format_expression(&expression.value)
                        );
                    }
                    MigrationTransformClause::Rekey(expressions) => {
                        let joined = expressions
                            .iter()
                            .map(|expression| format_expression(&expression.value))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let _ = writeln!(output, "    rekey ({joined})");
                    }
                }
            }
            output.push_str("  }\n");
        }
        MigrationDeclaration::EnumMap(mapping) => {
            let _ = writeln!(output, "  map enum {} {{", mapping.enumeration.value);
            for item in &mapping.mappings {
                let _ = writeln!(
                    output,
                    "    {} -> {}",
                    item.value.from.value, item.value.to.value
                );
            }
            output.push_str("  }\n");
        }
        MigrationDeclaration::Acknowledge(acknowledgement) => {
            let kind = match acknowledgement.kind.value {
                MigrationAcknowledgementKind::Repartition => "repartition",
                MigrationAcknowledgementKind::Aggregate => "aggregate",
                MigrationAcknowledgementKind::Conflict => "conflict",
            };
            let _ = writeln!(
                output,
                "  acknowledge {kind} {}",
                acknowledgement.aggregate.value
            );
        }
    }
}

const fn identity_kind(kind: MigrationIdentityKind) -> &'static str {
    match kind {
        MigrationIdentityKind::Entity => "entity",
        MigrationIdentityKind::Field => "field",
        MigrationIdentityKind::Enum => "enum",
        MigrationIdentityKind::EnumVariant => "variant",
        MigrationIdentityKind::Command => "command",
        MigrationIdentityKind::Event => "event",
        MigrationIdentityKind::Outcome => "outcome",
        MigrationIdentityKind::Projection => "projection",
    }
}

fn path(segments: &[Spanned<String>]) -> String {
    segments
        .iter()
        .map(|segment| segment.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

fn format_expression(expression: &Expression) -> String {
    match expression {
        Expression::Literal(literal) => match &literal.value {
            Literal::Bool(value) => value.to_string(),
            Literal::Null => "null".to_owned(),
            Literal::UInt(value) | Literal::FixedDecimal(value) | Literal::String(value) => {
                value.clone()
            }
        },
        Expression::Path(path_value) => path(&path_value.value.segments),
        Expression::Parenthesized(inner) => format!("({})", format_expression(&inner.value)),
        Expression::Unary { operator, operand } => format!(
            "{}{}",
            match operator.value {
                UnaryOperator::Not => "!",
                UnaryOperator::Negate => "-",
            },
            format_expression(&operand.value)
        ),
        Expression::Binary {
            left,
            operator,
            right,
        } => format!(
            "{} {} {}",
            format_expression(&left.value),
            binary_operator(operator.value),
            format_expression(&right.value)
        ),
    }
}

const fn binary_operator(operator: BinaryOperator) -> &'static str {
    match operator {
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Add => "+",
        BinaryOperator::Subtract => "-",
        BinaryOperator::Equal => "==",
        BinaryOperator::NotEqual => "!=",
        BinaryOperator::Less => "<",
        BinaryOperator::LessEqual => "<=",
        BinaryOperator::Greater => ">",
        BinaryOperator::GreaterEqual => ">=",
        BinaryOperator::And => "&&",
        BinaryOperator::Or => "||",
    }
}
