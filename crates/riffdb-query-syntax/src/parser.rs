//! Bounded lexer and recursive-descent parser.

use std::collections::BTreeSet;

use crate::{
    Argument, BinaryOperator, Diagnostic, DiagnosticCode, EventSelection, Expression, Hydration,
    Limits, Literal, MAX_PREDICATE_NODES, MAX_REACTIVE_DEFINITIONS, MAX_REACTIVE_NESTING,
    MAX_REACTIVE_PARAMETERS, MAX_REACTIVE_SOURCE_BYTES, MAX_REACTIVE_TOKENS,
    MAX_STREAM_EVENT_TYPES, MAX_STREAM_SELECTED_FIELDS, MAX_SUBSCRIPTION_HYDRATIONS,
    MAX_SUBSCRIPTION_REACTIONS, Module, Operand, Parameter, PartitionBinding, Reaction, Span,
    Stream, StreamBinding, Subscription, UpdateMode, Watch,
};

#[derive(Clone, Debug, Eq, PartialEq)]
enum TokenKind {
    Identifier(String),
    Parameter(String),
    Integer(String),
    String(String),
    LeftBrace,
    RightBrace,
    LeftParen,
    RightParen,
    Comma,
    Colon,
    Semicolon,
    Dot,
    Equal,
    EqualEqual,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
    End,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Token {
    kind: TokenKind,
    span: Span,
}

/// Parses one complete grammar-v1 reactive module.
pub fn parse_module(source: &str) -> Result<Module, Vec<Diagnostic>> {
    if source.is_empty() || source.len() > MAX_REACTIVE_SOURCE_BYTES {
        return Err(vec![Diagnostic::new(
            DiagnosticCode::LimitExceeded,
            Span::new(0, source.len()),
        )]);
    }
    let tokens = lex(source)?;
    Parser {
        tokens,
        current: 0,
        depth: 0,
        predicate_nodes: 0,
    }
    .module()
    .map_err(|diagnostic| vec![diagnostic])
}

fn lex(source: &str) -> Result<Vec<Token>, Vec<Diagnostic>> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        if tokens.len() >= MAX_REACTIVE_TOKENS {
            return Err(vec![Diagnostic::new(
                DiagnosticCode::LimitExceeded,
                Span::new(offset, offset),
            )]);
        }
        let byte = bytes[offset];
        if byte.is_ascii_whitespace() {
            offset += 1;
            continue;
        }
        if byte == b'/' && bytes.get(offset + 1) == Some(&b'/') {
            offset += 2;
            while bytes.get(offset).is_some_and(|byte| *byte != b'\n') {
                offset += 1;
            }
            continue;
        }
        let start = offset;
        let kind = match byte {
            b'{' => {
                offset += 1;
                TokenKind::LeftBrace
            }
            b'}' => {
                offset += 1;
                TokenKind::RightBrace
            }
            b'(' => {
                offset += 1;
                TokenKind::LeftParen
            }
            b')' => {
                offset += 1;
                TokenKind::RightParen
            }
            b',' => {
                offset += 1;
                TokenKind::Comma
            }
            b':' => {
                offset += 1;
                TokenKind::Colon
            }
            b';' => {
                offset += 1;
                TokenKind::Semicolon
            }
            b'.' => {
                offset += 1;
                TokenKind::Dot
            }
            b'=' if bytes.get(offset + 1) == Some(&b'=') => {
                offset += 2;
                TokenKind::EqualEqual
            }
            b'=' => {
                offset += 1;
                TokenKind::Equal
            }
            b'!' if bytes.get(offset + 1) == Some(&b'=') => {
                offset += 2;
                TokenKind::NotEqual
            }
            b'<' if bytes.get(offset + 1) == Some(&b'=') => {
                offset += 2;
                TokenKind::LessEqual
            }
            b'<' => {
                offset += 1;
                TokenKind::Less
            }
            b'>' if bytes.get(offset + 1) == Some(&b'=') => {
                offset += 2;
                TokenKind::GreaterEqual
            }
            b'>' => {
                offset += 1;
                TokenKind::Greater
            }
            b'&' if bytes.get(offset + 1) == Some(&b'&') => {
                offset += 2;
                TokenKind::And
            }
            b'|' if bytes.get(offset + 1) == Some(&b'|') => {
                offset += 2;
                TokenKind::Or
            }
            b'$' => {
                offset += 1;
                let name_start = offset;
                consume_identifier(bytes, &mut offset);
                if offset == name_start {
                    return Err(vec![Diagnostic::new(
                        DiagnosticCode::UnexpectedToken,
                        Span::new(start, offset),
                    )]);
                }
                TokenKind::Parameter(source[name_start..offset].to_owned())
            }
            b'"' => {
                offset += 1;
                let mut value = String::new();
                let mut closed = false;
                while offset < bytes.len() {
                    match bytes[offset] {
                        b'"' => {
                            offset += 1;
                            closed = true;
                            break;
                        }
                        b'\\' => {
                            offset += 1;
                            let Some(escaped) = bytes.get(offset).copied() else {
                                break;
                            };
                            value.push(match escaped {
                                b'"' => '"',
                                b'\\' => '\\',
                                b'n' => '\n',
                                b'r' => '\r',
                                b't' => '\t',
                                _ => {
                                    return Err(vec![Diagnostic::new(
                                        DiagnosticCode::InvalidLiteral,
                                        Span::new(start, offset + 1),
                                    )]);
                                }
                            });
                            offset += 1;
                        }
                        byte if byte.is_ascii_control() => {
                            return Err(vec![Diagnostic::new(
                                DiagnosticCode::InvalidLiteral,
                                Span::new(start, offset + 1),
                            )]);
                        }
                        _ => {
                            let Some(character) = source[offset..].chars().next() else {
                                break;
                            };
                            value.push(character);
                            offset += character.len_utf8();
                        }
                    }
                }
                if !closed || value.len() > 65_535 {
                    return Err(vec![Diagnostic::new(
                        DiagnosticCode::InvalidLiteral,
                        Span::new(start, offset),
                    )]);
                }
                TokenKind::String(value)
            }
            b'-' | b'0'..=b'9' => {
                if byte == b'-' {
                    offset += 1;
                }
                let digits = offset;
                while bytes.get(offset).is_some_and(u8::is_ascii_digit) {
                    offset += 1;
                }
                if offset == digits {
                    return Err(vec![Diagnostic::new(
                        DiagnosticCode::InvalidCharacter,
                        Span::new(start, offset),
                    )]);
                }
                TokenKind::Integer(source[start..offset].to_owned())
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                consume_identifier(bytes, &mut offset);
                TokenKind::Identifier(source[start..offset].to_owned())
            }
            _ => {
                return Err(vec![Diagnostic::new(
                    DiagnosticCode::InvalidCharacter,
                    Span::new(start, start + 1),
                )]);
            }
        };
        tokens.push(Token {
            kind,
            span: Span::new(start, offset),
        });
    }
    tokens.push(Token {
        kind: TokenKind::End,
        span: Span::new(source.len(), source.len()),
    });
    Ok(tokens)
}

fn consume_identifier(bytes: &[u8], offset: &mut usize) {
    while bytes
        .get(*offset)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        *offset += 1;
    }
}

struct Parser {
    tokens: Vec<Token>,
    current: usize,
    depth: usize,
    predicate_nodes: usize,
}

impl Parser {
    fn module(mut self) -> Result<Module, Diagnostic> {
        let start = self.expect_word("reactive")?.start();
        let (name, _) = self.identifier()?;
        self.expect_word("version")?;
        let version_token = self.advance().clone();
        let version = match version_token.kind {
            TokenKind::Integer(value) => value
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    Diagnostic::new(DiagnosticCode::InvalidLiteral, version_token.span)
                })?,
            _ => {
                return Err(Diagnostic::new(
                    DiagnosticCode::UnexpectedToken,
                    version_token.span,
                ));
            }
        };
        if version != 1 {
            return Err(Diagnostic::new(
                DiagnosticCode::UnsupportedVersion,
                version_token.span,
            ));
        }
        self.expect(TokenKind::LeftBrace)?;
        let mut streams = Vec::new();
        let mut watches = Vec::new();
        let mut subscriptions = Vec::new();
        let mut names = BTreeSet::new();
        while !self.check(&TokenKind::RightBrace) {
            if streams.len() + watches.len() + subscriptions.len() >= MAX_REACTIVE_DEFINITIONS {
                return Err(Diagnostic::new(
                    DiagnosticCode::LimitExceeded,
                    self.peek().span,
                ));
            }
            let word = self.peek_word().ok_or_else(|| {
                Diagnostic::new(DiagnosticCode::UnexpectedToken, self.peek().span)
            })?;
            let (operation_name, span) = match word {
                "stream" => {
                    let value = self.stream()?;
                    let pair = (value.name.clone(), value.span);
                    streams.push(value);
                    pair
                }
                "watch" => {
                    let value = self.watch()?;
                    let pair = (value.name.clone(), value.span);
                    watches.push(value);
                    pair
                }
                "subscription" => {
                    let value = self.subscription()?;
                    let pair = (value.name.clone(), value.span);
                    subscriptions.push(value);
                    pair
                }
                _ => {
                    return Err(Diagnostic::new(
                        DiagnosticCode::UnsupportedForm,
                        self.peek().span,
                    ));
                }
            };
            if !names.insert(operation_name) {
                return Err(Diagnostic::new(DiagnosticCode::DuplicateName, span));
            }
        }
        let end = self.expect(TokenKind::RightBrace)?.end();
        self.expect(TokenKind::End)?;
        Ok(Module {
            name,
            version,
            streams,
            watches,
            subscriptions,
            span: Span::new(start, end),
        })
    }

    fn stream(&mut self) -> Result<Stream, Diagnostic> {
        let start = self.expect_word("stream")?.start();
        let (name, _) = self.identifier()?;
        let parameters = self.parameters()?;
        self.expect(TokenKind::LeftBrace)?;
        self.expect_word("partition")?;
        let partition = self.partition_bindings()?;
        self.expect(TokenKind::Semicolon)?;
        let mut events = Vec::new();
        let mut event_names = BTreeSet::new();
        let mut selected_fields = 0usize;
        while self.peek_word() == Some("event") {
            if events.len() >= MAX_STREAM_EVENT_TYPES {
                return Err(Diagnostic::new(
                    DiagnosticCode::LimitExceeded,
                    self.peek().span,
                ));
            }
            let event = self.event_selection()?;
            selected_fields = selected_fields
                .checked_add(event.fields.len())
                .filter(|value| *value <= MAX_STREAM_SELECTED_FIELDS)
                .ok_or_else(|| Diagnostic::new(DiagnosticCode::LimitExceeded, event.span))?;
            if !event_names.insert(event.event.clone()) {
                return Err(Diagnostic::new(DiagnosticCode::DuplicateName, event.span));
            }
            events.push(event);
        }
        if events.is_empty() {
            return Err(Diagnostic::new(
                DiagnosticCode::UnexpectedToken,
                self.peek().span,
            ));
        }
        let predicate = if self.peek_word() == Some("where") {
            self.advance();
            self.predicate_nodes = 0;
            let value = self.expression()?;
            self.expect(TokenKind::Semicolon)?;
            Some(value)
        } else {
            None
        };
        let end = self.expect(TokenKind::RightBrace)?.end();
        Ok(Stream {
            name,
            parameters,
            partition,
            events,
            predicate,
            span: Span::new(start, end),
        })
    }

    fn watch(&mut self) -> Result<Watch, Diagnostic> {
        let start = self.expect_word("watch")?.start();
        let (name, _) = self.identifier()?;
        let parameters = self.parameters()?;
        self.expect_word("query")?;
        let (query, _) = self.identifier()?;
        self.expect_word("updates")?;
        let mode = self.advance().clone();
        let update_mode = match mode.kind {
            TokenKind::Identifier(value) if value == "patch" => UpdateMode::Patch,
            TokenKind::Identifier(value) if value == "reset" => UpdateMode::Reset,
            _ => return Err(Diagnostic::new(DiagnosticCode::UnexpectedToken, mode.span)),
        };
        let end = self.expect(TokenKind::Semicolon)?.end();
        Ok(Watch {
            name,
            parameters,
            query,
            update_mode,
            span: Span::new(start, end),
        })
    }

    fn subscription(&mut self) -> Result<Subscription, Diagnostic> {
        let start = self.expect_word("subscription")?.start();
        let (name, _) = self.identifier()?;
        let parameters = self.parameters()?;
        self.expect(TokenKind::LeftBrace)?;
        let stream_start = self.expect_word("stream")?.start();
        let (stream_name, _) = self.identifier()?;
        let stream_arguments = self.arguments()?;
        let stream_end = self.expect(TokenKind::Semicolon)?.end();
        let stream = StreamBinding {
            stream: stream_name,
            arguments: stream_arguments,
            span: Span::new(stream_start, stream_end),
        };
        let mut hydrations = Vec::new();
        let mut hydration_names = BTreeSet::new();
        while self.peek_word() == Some("hydrate") {
            if hydrations.len() >= MAX_SUBSCRIPTION_HYDRATIONS {
                return Err(Diagnostic::new(
                    DiagnosticCode::LimitExceeded,
                    self.peek().span,
                ));
            }
            let hydration = self.hydration()?;
            if !hydration_names.insert(hydration.name.clone()) {
                return Err(Diagnostic::new(
                    DiagnosticCode::DuplicateName,
                    hydration.span,
                ));
            }
            hydrations.push(hydration);
        }
        let mut reactions = Vec::new();
        let mut reaction_names = BTreeSet::new();
        while self.peek_word() == Some("reaction") {
            if reactions.len() >= MAX_SUBSCRIPTION_REACTIONS {
                return Err(Diagnostic::new(
                    DiagnosticCode::LimitExceeded,
                    self.peek().span,
                ));
            }
            let reaction = self.reaction()?;
            if !reaction_names.insert(reaction.name.clone()) {
                return Err(Diagnostic::new(
                    DiagnosticCode::DuplicateName,
                    reaction.span,
                ));
            }
            reactions.push(reaction);
        }
        self.expect_word("limits")?;
        let limits_start = self.expect(TokenKind::LeftBrace)?.start();
        self.expect_word("batch")?;
        let batch = self.positive_u16()?;
        self.expect(TokenKind::Semicolon)?;
        self.expect_word("in_flight")?;
        let in_flight = self.positive_u16()?;
        self.expect(TokenKind::Semicolon)?;
        self.expect_word("lease_seconds")?;
        let lease_seconds = self.positive_u16()?;
        self.expect(TokenKind::Semicolon)?;
        let limits_end = self.expect(TokenKind::RightBrace)?.end();
        if !(1..=8).contains(&batch)
            || !(1..=8).contains(&in_flight)
            || !(5..=900).contains(&lease_seconds)
        {
            return Err(Diagnostic::new(
                DiagnosticCode::LimitExceeded,
                Span::new(limits_start, limits_end),
            ));
        }
        let limits = Limits {
            batch: u8::try_from(batch).map_err(|_| {
                Diagnostic::new(
                    DiagnosticCode::LimitExceeded,
                    Span::new(limits_start, limits_end),
                )
            })?,
            in_flight: u8::try_from(in_flight).map_err(|_| {
                Diagnostic::new(
                    DiagnosticCode::LimitExceeded,
                    Span::new(limits_start, limits_end),
                )
            })?,
            lease_seconds,
            span: Span::new(limits_start, limits_end),
        };
        let end = self.expect(TokenKind::RightBrace)?.end();
        Ok(Subscription {
            name,
            parameters,
            stream,
            hydrations,
            reactions,
            limits,
            span: Span::new(start, end),
        })
    }

    fn event_selection(&mut self) -> Result<EventSelection, Diagnostic> {
        let start = self.expect_word("event")?.start();
        let (event, _) = self.identifier()?;
        self.expect_word("select")?;
        self.expect(TokenKind::LeftParen)?;
        let mut fields = Vec::new();
        let mut names = BTreeSet::new();
        loop {
            let (field, span) = self.identifier()?;
            if !names.insert(field.clone()) {
                return Err(Diagnostic::new(DiagnosticCode::DuplicateName, span));
            }
            fields.push(field);
            if self.take(&TokenKind::Comma).is_none() {
                break;
            }
        }
        self.expect(TokenKind::RightParen)?;
        let end = self.expect(TokenKind::Semicolon)?.end();
        Ok(EventSelection {
            event,
            fields,
            span: Span::new(start, end),
        })
    }

    fn hydration(&mut self) -> Result<Hydration, Diagnostic> {
        let start = self.expect_word("hydrate")?.start();
        let (name, _) = self.identifier()?;
        self.expect_word("query")?;
        let (query, _) = self.identifier()?;
        let arguments = self.arguments()?;
        let end = self.expect(TokenKind::Semicolon)?.end();
        Ok(Hydration {
            name,
            query,
            arguments,
            span: Span::new(start, end),
        })
    }

    fn reaction(&mut self) -> Result<Reaction, Diagnostic> {
        let start = self.expect_word("reaction")?.start();
        let (name, _) = self.identifier()?;
        self.expect_word("command")?;
        let (command, _) = self.identifier()?;
        let end = self.expect(TokenKind::Semicolon)?.end();
        Ok(Reaction {
            name,
            command,
            span: Span::new(start, end),
        })
    }

    fn parameters(&mut self) -> Result<Vec<Parameter>, Diagnostic> {
        self.expect(TokenKind::LeftParen)?;
        let mut parameters = Vec::new();
        let mut names = BTreeSet::new();
        if !self.check(&TokenKind::RightParen) {
            loop {
                if parameters.len() >= MAX_REACTIVE_PARAMETERS {
                    return Err(Diagnostic::new(
                        DiagnosticCode::LimitExceeded,
                        self.peek().span,
                    ));
                }
                let token = self.advance().clone();
                let name = match token.kind {
                    TokenKind::Parameter(value) => value,
                    _ => return Err(Diagnostic::new(DiagnosticCode::UnexpectedToken, token.span)),
                };
                self.expect(TokenKind::Colon)?;
                let (type_name, end) = self.qualified_identifier()?;
                if !names.insert(name.clone()) {
                    return Err(Diagnostic::new(DiagnosticCode::DuplicateName, token.span));
                }
                parameters.push(Parameter {
                    name,
                    type_name,
                    span: Span::new(token.span.start(), end.end()),
                });
                if self.take(&TokenKind::Comma).is_none() {
                    break;
                }
            }
        }
        self.expect(TokenKind::RightParen)?;
        Ok(parameters)
    }

    fn partition_bindings(&mut self) -> Result<Vec<PartitionBinding>, Diagnostic> {
        self.expect(TokenKind::LeftParen)?;
        let mut bindings = Vec::new();
        let mut fields = BTreeSet::new();
        loop {
            if bindings.len() >= MAX_REACTIVE_PARAMETERS {
                return Err(Diagnostic::new(
                    DiagnosticCode::LimitExceeded,
                    self.peek().span,
                ));
            }
            let (field, start) = self.identifier()?;
            self.expect(TokenKind::Equal)?;
            let token = self.advance().clone();
            let parameter = match token.kind {
                TokenKind::Parameter(value) => value,
                _ => return Err(Diagnostic::new(DiagnosticCode::UnexpectedToken, token.span)),
            };
            if !fields.insert(field.clone()) {
                return Err(Diagnostic::new(DiagnosticCode::DuplicateName, start));
            }
            bindings.push(PartitionBinding {
                field,
                parameter,
                span: Span::new(start.start(), token.span.end()),
            });
            if self.take(&TokenKind::Comma).is_none() {
                break;
            }
        }
        self.expect(TokenKind::RightParen)?;
        Ok(bindings)
    }

    fn arguments(&mut self) -> Result<Vec<Argument>, Diagnostic> {
        self.expect(TokenKind::LeftParen)?;
        let mut arguments = Vec::new();
        let mut names = BTreeSet::new();
        if !self.check(&TokenKind::RightParen) {
            loop {
                if arguments.len() >= MAX_REACTIVE_PARAMETERS {
                    return Err(Diagnostic::new(
                        DiagnosticCode::LimitExceeded,
                        self.peek().span,
                    ));
                }
                let (name, start) = self.identifier()?;
                self.expect(TokenKind::Equal)?;
                let value = self.operand()?;
                let span = Span::new(start.start(), value.span().end());
                if !names.insert(name.clone()) {
                    return Err(Diagnostic::new(DiagnosticCode::DuplicateName, span));
                }
                arguments.push(Argument { name, value, span });
                if self.take(&TokenKind::Comma).is_none() {
                    break;
                }
            }
        }
        self.expect(TokenKind::RightParen)?;
        Ok(arguments)
    }

    fn expression(&mut self) -> Result<Expression, Diagnostic> {
        self.or_expression()
    }
    fn or_expression(&mut self) -> Result<Expression, Diagnostic> {
        let mut left = self.and_expression()?;
        while self.take(&TokenKind::Or).is_some() {
            let right = self.and_expression()?;
            left = self.binary(left, BinaryOperator::Or, right)?;
        }
        Ok(left)
    }
    fn and_expression(&mut self) -> Result<Expression, Diagnostic> {
        let mut left = self.comparison()?;
        while self.take(&TokenKind::And).is_some() {
            let right = self.comparison()?;
            left = self.binary(left, BinaryOperator::And, right)?;
        }
        Ok(left)
    }
    fn comparison(&mut self) -> Result<Expression, Diagnostic> {
        if self.take(&TokenKind::LeftParen).is_some() {
            self.depth += 1;
            if self.depth > MAX_REACTIVE_NESTING {
                return Err(Diagnostic::new(
                    DiagnosticCode::LimitExceeded,
                    self.peek().span,
                ));
            }
            let value = self.expression()?;
            self.expect(TokenKind::RightParen)?;
            self.depth -= 1;
            return Ok(value);
        }
        let left = self.expression_operand()?;
        let operator = match &self.peek().kind {
            TokenKind::EqualEqual => BinaryOperator::Equal,
            TokenKind::NotEqual => BinaryOperator::NotEqual,
            TokenKind::Less => BinaryOperator::Less,
            TokenKind::LessEqual => BinaryOperator::LessEqual,
            TokenKind::Greater => BinaryOperator::Greater,
            TokenKind::GreaterEqual => BinaryOperator::GreaterEqual,
            _ => {
                return Err(Diagnostic::new(
                    DiagnosticCode::UnexpectedToken,
                    self.peek().span,
                ));
            }
        };
        self.advance();
        let right = self.expression_operand()?;
        self.binary(left, operator, right)
    }
    fn expression_operand(&mut self) -> Result<Expression, Diagnostic> {
        self.predicate_nodes += 1;
        if self.predicate_nodes > MAX_PREDICATE_NODES {
            return Err(Diagnostic::new(
                DiagnosticCode::LimitExceeded,
                self.peek().span,
            ));
        }
        Ok(Expression::Operand(self.operand()?))
    }
    fn binary(
        &mut self,
        left: Expression,
        operator: BinaryOperator,
        right: Expression,
    ) -> Result<Expression, Diagnostic> {
        self.predicate_nodes += 1;
        if self.predicate_nodes > MAX_PREDICATE_NODES {
            return Err(Diagnostic::new(DiagnosticCode::LimitExceeded, left.span()));
        }
        let span = Span::new(left.span().start(), right.span().end());
        Ok(Expression::Binary {
            left: Box::new(left),
            operator,
            right: Box::new(right),
            span,
        })
    }

    fn operand(&mut self) -> Result<Operand, Diagnostic> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Parameter(value) => Ok(Operand::Parameter(value, token.span)),
            TokenKind::Integer(value) => value
                .parse::<i64>()
                .map(|value| Operand::Literal(Literal::Integer(value), token.span))
                .map_err(|_| Diagnostic::new(DiagnosticCode::InvalidLiteral, token.span)),
            TokenKind::String(value) => Ok(Operand::Literal(Literal::String(value), token.span)),
            TokenKind::Identifier(value) if value == "true" => {
                Ok(Operand::Literal(Literal::Boolean(true), token.span))
            }
            TokenKind::Identifier(value) if value == "false" => {
                Ok(Operand::Literal(Literal::Boolean(false), token.span))
            }
            TokenKind::Identifier(first) => {
                if self.take(&TokenKind::Dot).is_none() {
                    return Err(Diagnostic::new(DiagnosticCode::UnexpectedToken, token.span));
                }
                let (second, end) = self.identifier()?;
                let span = Span::new(token.span.start(), end.end());
                if first == "event" {
                    Ok(Operand::EventField(second, span))
                } else {
                    Ok(Operand::Literal(
                        Literal::Symbol(format!("{first}.{second}")),
                        span,
                    ))
                }
            }
            _ => Err(Diagnostic::new(DiagnosticCode::UnexpectedToken, token.span)),
        }
    }

    fn positive_u16(&mut self) -> Result<u16, Diagnostic> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Integer(value) => value
                .parse::<u16>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| Diagnostic::new(DiagnosticCode::InvalidLiteral, token.span)),
            _ => Err(Diagnostic::new(DiagnosticCode::UnexpectedToken, token.span)),
        }
    }

    fn qualified_identifier(&mut self) -> Result<(String, Span), Diagnostic> {
        let (mut value, start) = self.identifier()?;
        let mut end = start;
        while self.take(&TokenKind::Dot).is_some() {
            let (component, component_span) = self.identifier()?;
            value.push('.');
            value.push_str(&component);
            end = component_span;
        }
        Ok((value, Span::new(start.start(), end.end())))
    }
    fn identifier(&mut self) -> Result<(String, Span), Diagnostic> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Identifier(value) => Ok((value, token.span)),
            _ => Err(Diagnostic::new(DiagnosticCode::UnexpectedToken, token.span)),
        }
    }
    fn expect_word(&mut self, expected: &str) -> Result<Span, Diagnostic> {
        let token = self.advance().clone();
        match token.kind {
            TokenKind::Identifier(value) if value == expected => Ok(token.span),
            _ => Err(Diagnostic::new(DiagnosticCode::UnexpectedToken, token.span)),
        }
    }
    fn peek_word(&self) -> Option<&str> {
        match &self.peek().kind {
            TokenKind::Identifier(value) => Some(value),
            _ => None,
        }
    }
    fn peek(&self) -> &Token {
        &self.tokens[self.current]
    }
    fn advance(&mut self) -> &Token {
        let index = self.current;
        if !matches!(self.tokens[index].kind, TokenKind::End) {
            self.current += 1;
        }
        &self.tokens[index]
    }
    fn check(&self, expected: &TokenKind) -> bool {
        std::mem::discriminant(&self.peek().kind) == std::mem::discriminant(expected)
    }
    fn take(&mut self, expected: &TokenKind) -> Option<Span> {
        self.check(expected).then(|| self.advance().span)
    }
    fn expect(&mut self, expected: TokenKind) -> Result<Span, Diagnostic> {
        self.take(&expected)
            .ok_or_else(|| Diagnostic::new(DiagnosticCode::UnexpectedToken, self.peek().span))
    }
}
