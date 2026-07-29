use crate::lexer::{Token, TokenKind, lex};
use crate::{
    BinaryOperator, Binding, Cardinality, DiagnosticCode, Direction, Document, Expression,
    FieldSelection, Identifier, Literal, MAX_BINDINGS, MAX_COLLECTION_ITEMS, MAX_NESTING,
    MAX_SYNTAX_ITEMS, OrderTerm, Parameter, ParseDiagnostic, ParseDiagnostics, Path, QueryBody,
    RIFFQL_LANGUAGE_VERSION, Selection, Span, Spanned, Take, TypeReference,
};

/// Parses one UTF-8 RiffQL v1 source document.
pub fn parse_query(source: &str) -> Result<Document, ParseDiagnostics> {
    let trimmed = source.trim_start();
    let leading_whitespace = source.len() - trimmed.len();
    let first_word = trimmed
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .next()
        .unwrap_or_default();
    if matches!(
        first_word.to_ascii_lowercase().as_str(),
        "run" | "create" | "update" | "delete" | "insert" | "select" | "with"
    ) {
        return Err(ParseDiagnostics::one(ParseDiagnostic::new(
            DiagnosticCode::UnsupportedForm,
            Span::checked(leading_whitespace, leading_whitespace + first_word.len()).unwrap_or(
                Span {
                    start: 0,
                    end: u32::MAX,
                },
            ),
            "mutation, SQL, and natural-language forms are not RiffQL",
            Some("use a query declaration with one, maybe, or bounded many bindings"),
        )));
    }
    Parser::new(lex(source)?, source.len()).document()
}

/// Validates UTF-8 and parses one RiffQL v1 source document.
pub fn parse_query_bytes(source: &[u8]) -> Result<Document, ParseDiagnostics> {
    let source = std::str::from_utf8(source).map_err(|_| {
        ParseDiagnostics::one(ParseDiagnostic::new(
            DiagnosticCode::InvalidToken,
            Span {
                start: 0,
                end: u32::try_from(source.len()).unwrap_or(u32::MAX),
            },
            "RiffQL source is not UTF-8",
            None,
        ))
    })?;
    parse_query(source)
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
    source_len: usize,
    nodes: usize,
    nesting: usize,
}

impl Parser {
    const fn new(tokens: Vec<Token>, source_len: usize) -> Self {
        Self {
            tokens,
            index: 0,
            source_len,
            nodes: 0,
            nesting: 0,
        }
    }

    fn document(mut self) -> Result<Document, ParseDiagnostics> {
        self.reject_forbidden_lead()?;
        let (name, parameters) = if self.take_word("query").is_some() {
            let name = Some(self.identifier()?);
            self.expect(TokenKind::LeftParen)?;
            let parameters = self.comma_list(TokenKind::RightParen, |parser| parser.parameter())?;
            (name, parameters)
        } else {
            (None, Vec::new())
        };
        self.expect(TokenKind::LeftBrace)?;
        let mut bindings = Vec::new();
        while self.peek_word("one") || self.peek_word("maybe") || self.peek_word("many") {
            if bindings.len() == MAX_BINDINGS {
                return Err(self.error(
                    DiagnosticCode::TooManyItems,
                    "query binding limit exceeded",
                    Some("split the query into bounded named operations"),
                ));
            }
            bindings.push(self.binding()?);
        }
        self.expect_word("return")?;
        let outcome = if matches!(self.peek_kind(), Some(TokenKind::Ident(_)))
            && matches!(
                self.tokens.get(self.index + 1).map(|token| &token.kind),
                Some(TokenKind::LeftBrace)
            ) {
            Some(self.identifier()?)
        } else {
            None
        };
        let selection = self.selection()?;
        let outcomes = if self.take_word("outcomes").is_some() {
            self.pipe_list()?
        } else {
            Vec::new()
        };
        self.expect(TokenKind::RightBrace)?;
        if self.index != self.tokens.len() {
            return Err(self.error(
                DiagnosticCode::UnexpectedToken,
                "unexpected token after query",
                None,
            ));
        }
        Ok(Document {
            language_version: RIFFQL_LANGUAGE_VERSION,
            name,
            parameters,
            body: QueryBody {
                bindings,
                outcome,
                selection,
                outcomes,
            },
        })
    }

    fn parameter(&mut self) -> Result<Parameter, ParseDiagnostics> {
        let name = self.parameter_name()?;
        self.expect(TokenKind::Colon)?;
        let ty = self.type_reference()?;
        let default = if self.take(TokenKind::Equal).is_some() {
            Some(self.literal()?)
        } else {
            None
        };
        self.node()?;
        Ok(Parameter { name, ty, default })
    }

    fn type_reference(&mut self) -> Result<Spanned<TypeReference>, ParseDiagnostics> {
        let start = self.current_start();
        let mut value = if self.take_word("Set").is_some() {
            self.expect(TokenKind::Less)?;
            let inner = self.type_reference()?;
            self.expect(TokenKind::Greater)?;
            TypeReference::Set(Box::new(inner))
        } else if self.take_word("Cursor").is_some() {
            TypeReference::Cursor
        } else if self.take_word("Limit").is_some() {
            TypeReference::Limit
        } else {
            TypeReference::Named(self.path()?)
        };
        if self.take(TokenKind::Question).is_some() {
            let span = self.span_from(start);
            value = TypeReference::Optional(Box::new(Spanned { value, span }));
        }
        self.node()?;
        Ok(Spanned {
            value,
            span: self.span_from(start),
        })
    }

    fn binding(&mut self) -> Result<Binding, ParseDiagnostics> {
        let cardinality = if let Some(span) = self.take_word("one") {
            Spanned {
                value: Cardinality::One,
                span,
            }
        } else if let Some(span) = self.take_word("maybe") {
            Spanned {
                value: Cardinality::Maybe,
                span,
            }
        } else {
            let span = self.expect_word("many")?;
            Spanned {
                value: Cardinality::Many,
                span,
            }
        };
        let name = self.identifier()?;
        self.expect_word("from")?;
        let entity = self.identifier()?;
        self.expect_word("where")?;
        let predicate = self.expression(0)?;
        let mut order = Vec::new();
        if self.take_word("order").is_some() {
            self.expect_word("by")?;
            loop {
                if order.len() == MAX_COLLECTION_ITEMS {
                    return Err(self.too_many());
                }
                let path_start = self.current_start();
                let path = self.path()?;
                let path = Spanned {
                    value: path,
                    span: self.span_from(path_start),
                };
                let direction = if let Some(span) = self.take_word("asc") {
                    Spanned {
                        value: Direction::Ascending,
                        span,
                    }
                } else if let Some(span) = self.take_word("desc") {
                    Spanned {
                        value: Direction::Descending,
                        span,
                    }
                } else {
                    return Err(self.error(
                        DiagnosticCode::UnexpectedToken,
                        "order term requires asc or desc",
                        None,
                    ));
                };
                order.push(OrderTerm { path, direction });
                if self.take(TokenKind::Comma).is_none() {
                    break;
                }
            }
        }
        let take = if self.take_word("take").is_some() {
            let limit = self.take_limit()?;
            let after = if self.take_word("after").is_some() {
                Some(self.parameter_name()?)
            } else {
                None
            };
            Some(Take { limit, after })
        } else {
            None
        };
        let absence_outcome = if self.take_word("else").is_some() {
            Some(self.identifier()?)
        } else {
            None
        };
        if cardinality.value == Cardinality::Many && take.is_none() {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnboundedMany,
                cardinality.span,
                "many binding requires an explicit take clause",
                Some("add take with a positive literal or Limit parameter"),
            )));
        }
        if cardinality.value == Cardinality::One && absence_outcome.is_none() {
            return Err(self.error(
                DiagnosticCode::UnexpectedToken,
                "one binding requires a declared else outcome",
                Some("add else followed by an outcome name"),
            ));
        }
        self.node()?;
        Ok(Binding {
            cardinality,
            name,
            entity,
            predicate,
            order,
            take,
            absence_outcome,
        })
    }

    fn take_limit(&mut self) -> Result<Spanned<Expression>, ParseDiagnostics> {
        let start = self.current_start();
        let value = match self.peek_kind() {
            Some(TokenKind::Parameter(_)) => Expression::Parameter(self.parameter_name()?),
            Some(TokenKind::Unsigned(value)) if value.bytes().any(|digit| digit != b'0') => {
                Expression::Literal(self.literal()?.value)
            }
            _ => {
                return Err(self.error(
                    DiagnosticCode::UnexpectedToken,
                    "take requires a positive integer or $Limit parameter",
                    None,
                ));
            }
        };
        self.node()?;
        Ok(Spanned {
            value,
            span: self.span_from(start),
        })
    }

    fn selection(&mut self) -> Result<Selection, ParseDiagnostics> {
        self.enter_nesting()?;
        self.expect(TokenKind::LeftBrace)?;
        let mut fields = Vec::new();
        while !self.peek(TokenKind::RightBrace) {
            if fields.len() == MAX_COLLECTION_ITEMS {
                return Err(self.too_many());
            }
            let first = self.identifier()?;
            let (alias, source) = if self.take(TokenKind::Colon).is_some() {
                (Some(first), self.spanned_path()?)
            } else {
                let start = first.span.start as usize;
                let mut segments = vec![first];
                while self.take(TokenKind::Dot).is_some() {
                    segments.push(self.identifier()?);
                }
                (
                    None,
                    Spanned {
                        value: Path(segments),
                        span: self.span_from(start),
                    },
                )
            };
            let nested = if self.peek(TokenKind::LeftBrace) {
                Some(self.selection()?)
            } else {
                None
            };
            fields.push(FieldSelection {
                alias,
                source,
                nested,
            });
            let _ = self.take(TokenKind::Comma);
        }
        self.expect(TokenKind::RightBrace)?;
        self.leave_nesting();
        self.node()?;
        Ok(Selection { fields })
    }

    fn expression(
        &mut self,
        minimum_precedence: u8,
    ) -> Result<Spanned<Expression>, ParseDiagnostics> {
        let start = self.current_start();
        let mut left = self.primary()?;
        while let Some((operator, precedence, span)) = self.binary_operator() {
            if precedence < minimum_precedence {
                break;
            }
            self.index += 1;
            let right = self.expression(precedence + 1)?;
            self.node()?;
            left = Spanned {
                value: Expression::Binary {
                    operator: Spanned {
                        value: operator,
                        span,
                    },
                    left: Box::new(left),
                    right: Box::new(right),
                },
                span: self.span_from(start),
            };
        }
        Ok(left)
    }

    fn primary(&mut self) -> Result<Spanned<Expression>, ParseDiagnostics> {
        let start = self.current_start();
        let value = match self.peek_kind() {
            Some(TokenKind::Parameter(_)) => Expression::Parameter(self.parameter_name()?),
            Some(TokenKind::Unsigned(_)) | Some(TokenKind::String(_)) => {
                Expression::Literal(self.literal()?.value)
            }
            Some(TokenKind::Ident(word)) if word == "true" || word == "false" || word == "null" => {
                Expression::Literal(self.literal()?.value)
            }
            Some(TokenKind::Ident(_))
                if matches!(
                    self.tokens.get(self.index + 1).map(|token| &token.kind),
                    Some(TokenKind::LeftParen)
                ) =>
            {
                return Err(self.error(
                    DiagnosticCode::UnsupportedForm,
                    "function calls and recursion are not RiffQL",
                    None,
                ));
            }
            Some(TokenKind::Ident(_)) => Expression::Path(self.path()?),
            Some(TokenKind::LeftParen) => {
                self.enter_nesting()?;
                self.index += 1;
                let expression = self.expression(0)?;
                self.expect(TokenKind::RightParen)?;
                self.leave_nesting();
                return Ok(Spanned {
                    value: expression.value,
                    span: self.span_from(start),
                });
            }
            Some(_) => {
                return Err(self.error(
                    DiagnosticCode::UnexpectedToken,
                    "expected a RiffQL expression",
                    None,
                ));
            }
            None => return Err(self.unexpected_end()),
        };
        self.node()?;
        Ok(Spanned {
            value,
            span: self.span_from(start),
        })
    }

    fn literal(&mut self) -> Result<Spanned<Literal>, ParseDiagnostics> {
        let token = self.next()?.clone();
        let value = match token.kind {
            TokenKind::Unsigned(value) => Literal::Unsigned(value),
            TokenKind::String(value) => Literal::String(value),
            TokenKind::Ident(value) if value == "true" => Literal::Boolean(true),
            TokenKind::Ident(value) if value == "false" => Literal::Boolean(false),
            TokenKind::Ident(value) if value == "null" => Literal::Null,
            _ => {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::UnexpectedToken,
                    token.span,
                    "expected a literal",
                    None,
                )));
            }
        };
        Ok(Spanned {
            value,
            span: token.span,
        })
    }

    fn path(&mut self) -> Result<Path, ParseDiagnostics> {
        let mut segments = vec![self.identifier()?];
        while self.take(TokenKind::Dot).is_some() {
            if segments.len() == MAX_COLLECTION_ITEMS {
                return Err(self.too_many());
            }
            segments.push(self.identifier()?);
        }
        Ok(Path(segments))
    }

    fn spanned_path(&mut self) -> Result<Spanned<Path>, ParseDiagnostics> {
        let start = self.current_start();
        let value = self.path()?;
        Ok(Spanned {
            value,
            span: self.span_from(start),
        })
    }

    fn comma_list<T>(
        &mut self,
        end: TokenKind,
        mut parse: impl FnMut(&mut Self) -> Result<T, ParseDiagnostics>,
    ) -> Result<Vec<T>, ParseDiagnostics> {
        let mut values = Vec::new();
        if self.take(end.clone()).is_some() {
            return Ok(values);
        }
        loop {
            if values.len() == MAX_COLLECTION_ITEMS {
                return Err(self.too_many());
            }
            values.push(parse(self)?);
            if self.take(TokenKind::Comma).is_some() {
                if self.take(end.clone()).is_some() {
                    break;
                }
            } else {
                self.expect(end)?;
                break;
            }
        }
        Ok(values)
    }

    fn pipe_list(&mut self) -> Result<Vec<Spanned<Identifier>>, ParseDiagnostics> {
        let mut values = vec![self.identifier()?];
        while self.take(TokenKind::Pipe).is_some() {
            values.push(self.identifier()?);
        }
        Ok(values)
    }

    fn identifier(&mut self) -> Result<Spanned<Identifier>, ParseDiagnostics> {
        let token = self.next()?.clone();
        let TokenKind::Ident(value) = token.kind else {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnexpectedToken,
                token.span,
                "expected an identifier",
                None,
            )));
        };
        if reserved(&value) {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnexpectedToken,
                token.span,
                "reserved RiffQL word cannot be used as an identifier",
                None,
            )));
        }
        let Some(value) = Identifier::new(&value) else {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::InvalidToken,
                token.span,
                "invalid or oversized identifier",
                None,
            )));
        };
        Ok(Spanned {
            value,
            span: token.span,
        })
    }

    fn parameter_name(&mut self) -> Result<Spanned<Identifier>, ParseDiagnostics> {
        let token = self.next()?.clone();
        let TokenKind::Parameter(value) = token.kind else {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnexpectedToken,
                token.span,
                "expected a $parameter",
                None,
            )));
        };
        let Some(value) = Identifier::new(&value) else {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::InvalidToken,
                token.span,
                "invalid or oversized parameter name",
                None,
            )));
        };
        Ok(Spanned {
            value,
            span: token.span,
        })
    }

    fn binary_operator(&self) -> Option<(BinaryOperator, u8, Span)> {
        let token = self.tokens.get(self.index)?;
        let pair = match &token.kind {
            TokenKind::OrOr => (BinaryOperator::Or, 1),
            TokenKind::AndAnd => (BinaryOperator::And, 2),
            TokenKind::EqualEqual => (BinaryOperator::Equal, 3),
            TokenKind::BangEqual => (BinaryOperator::NotEqual, 3),
            TokenKind::Less => (BinaryOperator::Less, 3),
            TokenKind::LessEqual => (BinaryOperator::LessEqual, 3),
            TokenKind::Greater => (BinaryOperator::Greater, 3),
            TokenKind::GreaterEqual => (BinaryOperator::GreaterEqual, 3),
            TokenKind::Ident(word) if word == "in" => (BinaryOperator::In, 3),
            _ => return None,
        };
        Some((pair.0, pair.1, token.span))
    }

    fn reject_forbidden_lead(&self) -> Result<(), ParseDiagnostics> {
        if let Some(Token {
            kind: TokenKind::Ident(word),
            span,
        }) = self.tokens.first()
            && matches!(
                word.to_ascii_lowercase().as_str(),
                "run" | "create" | "update" | "delete" | "insert" | "select" | "with"
            )
        {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnsupportedForm,
                *span,
                "mutation, SQL, and natural-language forms are not RiffQL",
                Some("use a query declaration with one, maybe, or bounded many bindings"),
            )));
        }
        Ok(())
    }

    fn enter_nesting(&mut self) -> Result<(), ParseDiagnostics> {
        if self.nesting == MAX_NESTING {
            return Err(self.error(
                DiagnosticCode::NestingTooDeep,
                "RiffQL nesting limit exceeded",
                None,
            ));
        }
        self.nesting += 1;
        Ok(())
    }

    fn leave_nesting(&mut self) {
        self.nesting -= 1;
    }

    fn node(&mut self) -> Result<(), ParseDiagnostics> {
        if self.nodes == MAX_SYNTAX_ITEMS {
            return Err(self.error(
                DiagnosticCode::TooManySyntaxItems,
                "RiffQL AST node limit exceeded",
                None,
            ));
        }
        self.nodes += 1;
        Ok(())
    }

    fn peek_word(&self, word: &str) -> bool {
        matches!(self.peek_kind(), Some(TokenKind::Ident(value)) if value == word)
    }

    fn take_word(&mut self, word: &str) -> Option<Span> {
        if self.peek_word(word) {
            let span = self.tokens[self.index].span;
            self.index += 1;
            Some(span)
        } else {
            None
        }
    }

    fn expect_word(&mut self, word: &str) -> Result<Span, ParseDiagnostics> {
        self.take_word(word).ok_or_else(|| {
            self.error(
                DiagnosticCode::UnexpectedToken,
                "expected a RiffQL clause keyword",
                None,
            )
        })
    }

    fn peek(&self, kind: TokenKind) -> bool {
        self.peek_kind().is_some_and(|value| *value == kind)
    }

    fn take(&mut self, kind: TokenKind) -> Option<Span> {
        if self.peek(kind) {
            let span = self.tokens[self.index].span;
            self.index += 1;
            Some(span)
        } else {
            None
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<Span, ParseDiagnostics> {
        self.take(kind).ok_or_else(|| {
            self.error(
                DiagnosticCode::UnexpectedToken,
                "unexpected RiffQL token",
                None,
            )
        })
    }

    fn next(&mut self) -> Result<&Token, ParseDiagnostics> {
        let token = self
            .tokens
            .get(self.index)
            .ok_or_else(|| self.unexpected_end())?;
        self.index += 1;
        Ok(token)
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.tokens.get(self.index).map(|token| &token.kind)
    }

    fn current_start(&self) -> usize {
        self.tokens
            .get(self.index)
            .map_or(self.source_len, |token| token.span.start as usize)
    }

    fn span_from(&self, start: usize) -> Span {
        let end = self
            .tokens
            .get(self.index.saturating_sub(1))
            .map_or(start, |token| token.span.end as usize);
        Span::checked(start, end).expect("bounded source span")
    }

    fn error(
        &self,
        code: DiagnosticCode,
        summary: &'static str,
        help: Option<&'static str>,
    ) -> ParseDiagnostics {
        let span = self.tokens.get(self.index).map_or(
            Span::checked(self.source_len, self.source_len).expect("bounded source span"),
            |token| token.span,
        );
        ParseDiagnostics::one(ParseDiagnostic::new(code, span, summary, help))
    }

    fn unexpected_end(&self) -> ParseDiagnostics {
        self.error(
            DiagnosticCode::UnexpectedEnd,
            "unexpected end of RiffQL source",
            None,
        )
    }

    fn too_many(&self) -> ParseDiagnostics {
        self.error(
            DiagnosticCode::TooManyItems,
            "RiffQL collection limit exceeded",
            None,
        )
    }
}

fn reserved(value: &str) -> bool {
    matches!(
        value,
        "query"
            | "one"
            | "maybe"
            | "many"
            | "from"
            | "where"
            | "order"
            | "by"
            | "asc"
            | "desc"
            | "take"
            | "after"
            | "else"
            | "return"
            | "outcomes"
            | "in"
            | "true"
            | "false"
            | "null"
    )
}
