use crate::lexer::{Token, TokenKind, lex};
use crate::{
    AggregateBinding, AggregateFunction, AggregateMeasure, BinaryOperator, Binding, Cardinality,
    DiagnosticCode, Direction, Document, Expression, FieldSelection, Identifier, Literal,
    MAX_AGGREGATE_BINDINGS, MAX_AGGREGATE_GROUP_KEYS, MAX_AGGREGATE_MEASURES, MAX_BINDINGS,
    MAX_COLLECTION_ITEMS, MAX_NESTING, MAX_SYNTAX_ITEMS, OrderTerm, Parameter, ParseDiagnostic,
    ParseDiagnostics, Path, QueryBody, RIFFQL_LANGUAGE_VERSION,
    RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1, Selection, Span, Spanned, Take, TypeReference,
    UnaryOperator,
};

/// Parses one UTF-8 RiffQL source document in a supported language version.
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
        let mut aggregates = Vec::new();
        while self.peek_word("aggregate") {
            if aggregates.len() == MAX_AGGREGATE_BINDINGS {
                return Err(self.error(
                    DiagnosticCode::TooManyItems,
                    "query aggregate declaration limit exceeded",
                    Some("split aggregate results into bounded named operations"),
                ));
            }
            aggregates.push(self.aggregate_binding()?);
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
        let body = QueryBody {
            bindings,
            aggregates,
            outcome,
            selection,
            outcomes,
        };
        let language_version = if !body.aggregates.is_empty()
            || body
                .bindings
                .iter()
                .any(|binding| expression_uses_operational_syntax(&binding.predicate.value))
        {
            RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1
        } else {
            RIFFQL_LANGUAGE_VERSION
        };
        Ok(Document {
            language_version,
            name,
            parameters,
            body,
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
        let nearest = if let Some(nearest_kw_span) = self.take_word("nearest") {
            self.expect(TokenKind::LeftParen)?;
            let field = self.identifier()?;
            self.expect(TokenKind::Comma)?;
            let vector = self.parameter_name()?;
            self.expect(TokenKind::Comma)?;
            let k = self.take_limit()?;
            self.expect(TokenKind::RightParen)?;
            let span = self.span_from(nearest_kw_span.start as usize);
            self.node()?;
            Some(crate::NearestClause {
                field,
                vector,
                k,
                span,
            })
        } else {
            None
        };
        let absence_outcome = if self.take_word("else").is_some() {
            Some(self.identifier()?)
        } else {
            None
        };
        if cardinality.value == Cardinality::Many && take.is_none() && nearest.is_none() {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnboundedMany,
                cardinality.span,
                "many binding requires an explicit take clause or nearest clause",
                Some(
                    "add take with a positive literal or Limit parameter, or nearest(field, $vector, k)",
                ),
            )));
        }
        if nearest.is_some() && cardinality.value != Cardinality::Many {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnsupportedForm,
                cardinality.span,
                "nearest clause is only valid on many bindings",
                Some("change cardinality to many"),
            )));
        }
        if nearest.is_some() && !order.is_empty() {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnsupportedForm,
                cardinality.span,
                "nearest clause replaces order by (results are ordered by distance)",
                Some("remove the order by clause"),
            )));
        }
        if nearest.is_some() && take.is_some() {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnsupportedForm,
                cardinality.span,
                "nearest clause includes its own bound (k); take is not allowed",
                Some("remove the take clause; the k argument to nearest is the bound"),
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
            nearest,
            absence_outcome,
        })
    }

    fn aggregate_binding(&mut self) -> Result<AggregateBinding, ParseDiagnostics> {
        self.expect_word("aggregate")?;
        let name = self.identifier()?;
        self.expect_word("from")?;
        let source = self.identifier()?;
        self.enter_nesting()?;
        self.expect(TokenKind::LeftBrace)?;
        let mut group_by = Vec::new();
        if self.take_word("group").is_some() {
            self.expect_word("by")?;
            loop {
                if group_by.len() == MAX_AGGREGATE_GROUP_KEYS {
                    return Err(self.error(
                        DiagnosticCode::TooManyItems,
                        "aggregate grouping-key limit exceeded",
                        Some("reduce grouping dimensions or split the operation"),
                    ));
                }
                group_by.push(self.spanned_path()?);
                if self.take(TokenKind::Comma).is_none() {
                    break;
                }
            }
        }
        let mut measures = Vec::new();
        while !self.peek(TokenKind::RightBrace) {
            if measures.len() == MAX_AGGREGATE_MEASURES {
                return Err(self.error(
                    DiagnosticCode::TooManyItems,
                    "aggregate measure limit exceeded",
                    Some("split measures into another bounded named operation"),
                ));
            }
            measures.push(self.aggregate_measure()?);
            let _ = self.take(TokenKind::Comma);
        }
        if measures.is_empty() {
            return Err(self.error(
                DiagnosticCode::UnexpectedToken,
                "aggregate declaration requires at least one measure",
                Some("add count(), sum(field), min(field), or max(field)"),
            ));
        }
        self.expect(TokenKind::RightBrace)?;
        self.leave_nesting();
        self.node()?;
        Ok(AggregateBinding {
            name,
            source,
            group_by,
            measures,
        })
    }

    fn aggregate_measure(&mut self) -> Result<AggregateMeasure, ParseDiagnostics> {
        let token = self.next()?.clone();
        let (function, requires_field) = match &token.kind {
            TokenKind::Ident(value) if value == "count" => (AggregateFunction::Count, false),
            TokenKind::Ident(value) if value == "sum" => (AggregateFunction::Sum, true),
            TokenKind::Ident(value) if value == "min" => (AggregateFunction::Min, true),
            TokenKind::Ident(value) if value == "max" => (AggregateFunction::Max, true),
            _ => {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::UnexpectedToken,
                    token.span,
                    "unknown aggregate function",
                    Some("use count(), sum(field), min(field), or max(field)"),
                )));
            }
        };
        self.expect(TokenKind::LeftParen)?;
        let field = if requires_field {
            Some(self.spanned_path()?)
        } else {
            None
        };
        self.expect(TokenKind::RightParen)?;
        self.expect_word("as")?;
        let alias = self.identifier()?;
        self.node()?;
        Ok(AggregateMeasure {
            function: Spanned {
                value: function,
                span: token.span,
            },
            field,
            alias,
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
        loop {
            if minimum_precedence <= 3
                && let Some((operator, span)) = self.take_null_operator()
            {
                self.node()?;
                left = Spanned {
                    value: Expression::Unary {
                        operator: Spanned {
                            value: operator,
                            span,
                        },
                        operand: Box::new(left),
                    },
                    span: self.span_from(start),
                };
                continue;
            }
            let Some((operator, precedence, span)) = self.binary_operator() else {
                break;
            };
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
            Some(TokenKind::Ident(word)) if word == "when" => {
                self.index += 1;
                let parameter = self.parameter_name()?;
                self.enter_nesting()?;
                self.expect(TokenKind::LeftBrace)?;
                let predicate = self.expression(0)?;
                self.expect(TokenKind::RightBrace)?;
                self.leave_nesting();
                Expression::PresenceGuard {
                    parameter,
                    predicate: Box::new(predicate),
                }
            }
            Some(TokenKind::Ident(word)) if word == "exists" => {
                let operator_span = self.tokens[self.index].span;
                self.index += 1;
                let operand_start = self.current_start();
                let operand = Spanned {
                    value: Expression::Path(self.path()?),
                    span: self.span_from(operand_start),
                };
                Expression::Unary {
                    operator: Spanned {
                        value: UnaryOperator::Exists,
                        span: operator_span,
                    },
                    operand: Box::new(operand),
                }
            }
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
            TokenKind::Ident(word) if word == "prefix" => (BinaryOperator::Prefix, 3),
            _ => return None,
        };
        Some((pair.0, pair.1, token.span))
    }

    fn take_null_operator(&mut self) -> Option<(UnaryOperator, Span)> {
        if !self.peek_word("is") {
            return None;
        }
        let start = self.tokens[self.index].span.start as usize;
        let (operator, consumed) = match (
            self.tokens.get(self.index + 1).map(|token| &token.kind),
            self.tokens.get(self.index + 2).map(|token| &token.kind),
        ) {
            (Some(TokenKind::Ident(word)), _) if word == "null" => (UnaryOperator::IsNull, 2),
            (Some(TokenKind::Ident(not)), Some(TokenKind::Ident(null)))
                if not == "not" && null == "null" =>
            {
                (UnaryOperator::IsNotNull, 3)
            }
            _ => return None,
        };
        self.index += consumed;
        Some((operator, self.span_from(start)))
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

fn expression_uses_operational_syntax(expression: &Expression) -> bool {
    match expression {
        Expression::PresenceGuard { .. } | Expression::Unary { .. } => true,
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            operator.value == BinaryOperator::Prefix
                || expression_uses_operational_syntax(&left.value)
                || expression_uses_operational_syntax(&right.value)
        }
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => false,
    }
}

// `nearest` is deliberately NOT reserved: the contract language does not
// reserve it, so a contract may legally declare `field nearest: ...`, and
// reserving it here made that field unnameable in any query. The clause
// position is unambiguous — `take_word("nearest")` fires only after the
// binding's predicate/order/take clauses are complete, and a finished
// expression cannot be extended by a bare identifier — so `nearest` is a
// contextual word, exactly like the contract lexer treats `cosine`,
// `euclidean`, `dot_product`, and `staleness_slo`.
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
            | "when"
            | "exists"
            | "is"
            | "not"
            | "prefix"
            | "true"
            | "false"
            | "null"
    )
}
