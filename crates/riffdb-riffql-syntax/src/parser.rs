use crate::lexer::{Token, TokenKind, lex};
use crate::{
    AggregateBinding, AggregateFunction, AggregateMeasure, BinaryOperator, Binding,
    CandidateBinding, CandidateSetExpression, CandidateSource, Cardinality, DiagnosticCode,
    Direction, Document, Expression, FieldSelection, Identifier, Literal, MAX_AGGREGATE_BINDINGS,
    MAX_AGGREGATE_GROUP_KEYS, MAX_AGGREGATE_MEASURES, MAX_BINDINGS, MAX_CANDIDATE_SOURCES,
    MAX_COLLECTION_ITEMS, MAX_NESTING, MAX_PROJECTED_CAUSAL_WAIT_MS, MAX_PROJECTED_LAG_MS,
    MAX_SYNTAX_ITEMS, NullPlacement, OrderTerm, Parameter, ParseDiagnostic, ParseDiagnostics, Path,
    ProjectedFreshness, ProjectedSource, QueryBody, RIFFQL_LANGUAGE_VERSION,
    RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1, RIFFQL_LANGUAGE_VERSION_BOUNDED_RESULT_PIPELINE_V1,
    RIFFQL_LANGUAGE_VERSION_EXACT_AGGREGATE_V1, RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1,
    RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1, RIFFQL_LANGUAGE_VERSION_NULLABLE_EXACT_ORDER_V1,
    RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1, RIFFQL_LANGUAGE_VERSION_PROJECTED_VECTOR_V1,
    RIFFQL_LANGUAGE_VERSION_SECRET_OUTPUT_V1, RIFFQL_LANGUAGE_VERSION_TOKENIZED_TEXT_V1, Selection,
    Span, Spanned, Take, TokenizedMatchClause, TokenizedMatchKind, TokenizedRanking, TypeReference,
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
        let projected_source = if self.peek_word("source") {
            Some(self.projected_source()?)
        } else {
            None
        };
        let mut candidates = Vec::new();
        while self.peek_word("candidates") {
            candidates.push(self.candidate_binding()?);
        }
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
            candidates,
            bindings,
            aggregates,
            outcome,
            selection,
            outcomes,
        };
        let extended_limit = parameters.iter().any(|parameter| {
            matches!(
                parameter.ty.value,
                TypeReference::BoundedLimit(maximum)
                    if maximum > riffdb_types::MAX_APPLICATION_QUERY_PAGE_ROWS_BOUNDED_LIMIT_V1
            )
        });
        let language_version = if extended_limit || !body.candidates.is_empty() {
            RIFFQL_LANGUAGE_VERSION_BOUNDED_RESULT_PIPELINE_V1
        } else if body
            .bindings
            .iter()
            .any(|binding| binding.tokenized_match.is_some())
        {
            RIFFQL_LANGUAGE_VERSION_TOKENIZED_TEXT_V1
        } else if parameters
            .iter()
            .any(|parameter| matches!(parameter.ty.value, TypeReference::BoundedLimit(_)))
        {
            RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1
        } else {
            query_shape_language_version(&body, projected_source.as_ref())
        };
        Ok(Document {
            language_version,
            name,
            parameters,
            projected_source,
            body,
        })
    }

    fn candidate_binding(&mut self) -> Result<CandidateBinding, ParseDiagnostics> {
        let start = self.expect_word("candidates")?.start as usize;
        let name = self.identifier()?;
        self.expect(TokenKind::Colon)?;
        let key_start = self.current_start();
        let root_key = Spanned {
            value: self.path()?,
            span: self.span_from(key_start),
        };
        self.expect_word("from")?;
        let expression = if self.take_word("intersect").is_some() {
            CandidateSetExpression::Intersection(self.candidate_source_list()?)
        } else if self.take_word("union").is_some() {
            CandidateSetExpression::Union(self.candidate_source_list()?)
        } else if self.take_word("difference").is_some() {
            self.expect(TokenKind::LeftBrace)?;
            let positive = self.candidate_source()?;
            self.expect(TokenKind::Semicolon)?;
            let mut negative = Vec::new();
            loop {
                if negative.len() + 1 == MAX_CANDIDATE_SOURCES {
                    return Err(self.error(
                        DiagnosticCode::TooManyItems,
                        "candidate source limit exceeded",
                        Some("use at most eight candidate sources"),
                    ));
                }
                negative.push(self.candidate_source()?);
                if self.take(TokenKind::Comma).is_none() {
                    break;
                }
                if self.peek(TokenKind::RightBrace) {
                    break;
                }
            }
            if negative.is_empty() {
                return Err(self.error(
                    DiagnosticCode::UnexpectedToken,
                    "candidate difference requires a negative source",
                    None,
                ));
            }
            self.expect(TokenKind::RightBrace)?;
            CandidateSetExpression::Difference { positive, negative }
        } else {
            CandidateSetExpression::Single(self.candidate_source()?)
        };
        self.expect_word("within")?;
        let bound = self.next()?.clone();
        let TokenKind::Unsigned(value) = bound.kind else {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnexpectedToken,
                bound.span,
                "candidate bound must be a positive unsigned literal",
                Some("use one canonical literal from 1 through 65535"),
            )));
        };
        let canonical = value == "0" || !value.starts_with('0');
        let within = value
            .parse::<u16>()
            .ok()
            .filter(|value| canonical && *value > 0)
            .ok_or_else(|| {
                ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::InvalidToken,
                    bound.span,
                    "candidate bound is outside the supported range",
                    Some("use one canonical literal from 1 through 65535"),
                ))
            })?;
        self.expect_word("else")?;
        let refusal_outcome = self.identifier()?;
        self.node()?;
        Ok(CandidateBinding {
            name,
            root_key,
            expression,
            within,
            refusal_outcome,
            span: self.span_from(start),
        })
    }

    fn candidate_source_list(&mut self) -> Result<Vec<CandidateSource>, ParseDiagnostics> {
        self.expect(TokenKind::LeftBrace)?;
        let mut sources = Vec::new();
        loop {
            if sources.len() == MAX_CANDIDATE_SOURCES {
                return Err(self.error(
                    DiagnosticCode::TooManyItems,
                    "candidate source limit exceeded",
                    Some("use at most eight candidate sources"),
                ));
            }
            sources.push(self.candidate_source()?);
            if self.take(TokenKind::Comma).is_none() {
                break;
            }
            if self.peek(TokenKind::RightBrace) {
                break;
            }
        }
        self.expect(TokenKind::RightBrace)?;
        if sources.len() < 2 {
            return Err(self.error(
                DiagnosticCode::UnexpectedToken,
                "candidate set operator requires at least two sources",
                Some("use a single source without intersect or union"),
            ));
        }
        Ok(sources)
    }

    fn candidate_source(&mut self) -> Result<CandidateSource, ParseDiagnostics> {
        let start = self.current_start();
        let key_start = self.current_start();
        let projected_key = Spanned {
            value: self.path()?,
            span: self.span_from(key_start),
        };
        self.expect_word("using")?;
        let access = self.identifier()?;
        self.expect_word("where")?;
        let predicate = self.expression(0)?;
        self.node()?;
        Ok(CandidateSource {
            projected_key,
            access,
            predicate,
            span: self.span_from(start),
        })
    }

    fn projected_source(&mut self) -> Result<ProjectedSource, ParseDiagnostics> {
        self.expect_word("source")?;
        self.expect_word("projected")?;
        let path_start = self.current_start();
        let path = self.path()?;
        let path = Spanned {
            value: path,
            span: self.span_from(path_start),
        };
        self.expect_word("freshness")?;
        let freshness_start = self.current_start();
        let freshness = if self.take_word("available").is_some() {
            ProjectedFreshness::Available
        } else if self.take_word("causal").is_some() {
            self.expect_word("inherit_session_commit")?;
            let inherit = self.literal()?;
            let Literal::Boolean(inherit_session_commit) = inherit.value else {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::UnexpectedToken,
                    inherit.span,
                    "inherit_session_commit must be true or false",
                    Some("use `inherit_session_commit true` for generated clients"),
                )));
            };
            self.expect_word("max_wait_ms")?;
            let wait = self.literal()?;
            let wait_span = wait.span;
            let Literal::Unsigned(wait) = wait.value else {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::UnexpectedToken,
                    wait_span,
                    "max_wait_ms must be a positive integer literal",
                    None,
                )));
            };
            let max_wait_ms = wait
                .parse::<u32>()
                .ok()
                .filter(|value| *value > 0 && *value <= MAX_PROJECTED_CAUSAL_WAIT_MS);
            let Some(max_wait_ms) = max_wait_ms else {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::InvalidToken,
                    wait_span,
                    "max_wait_ms exceeds the compiler projection-wait bound",
                    Some("use a value from 1 through 30000"),
                )));
            };
            ProjectedFreshness::Causal {
                inherit_session_commit,
                max_wait_ms,
            }
        } else if self.take_word("bounded").is_some() {
            self.expect_word("max_lag_ms")?;
            let lag = self.literal()?;
            let lag_span = lag.span;
            let Literal::Unsigned(lag) = lag.value else {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::UnexpectedToken,
                    lag_span,
                    "max_lag_ms must be a positive integer literal",
                    None,
                )));
            };
            let max_lag_ms = lag
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0 && *value <= MAX_PROJECTED_LAG_MS);
            let Some(max_lag_ms) = max_lag_ms else {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::InvalidToken,
                    lag_span,
                    "max_lag_ms exceeds the compiler projection-lag bound",
                    Some("use a value from 1 through 86400000"),
                )));
            };
            ProjectedFreshness::Bounded { max_lag_ms }
        } else {
            return Err(self.error(
                DiagnosticCode::UnexpectedToken,
                "expected available, causal, or bounded projection freshness",
                None,
            ));
        };
        Ok(ProjectedSource {
            path,
            freshness: Spanned {
                value: freshness,
                span: self.span_from(freshness_start),
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
            if type_contains_bounded_limit(&inner.value) {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::InvalidToken,
                    inner.span,
                    "bounded Limit cannot be nested in a query type",
                    Some("declare Limit<MAX> directly and use it only for take or nearest"),
                )));
            }
            self.expect(TokenKind::Greater)?;
            TypeReference::Set(Box::new(inner))
        } else if self.take_word("Cursor").is_some() {
            TypeReference::Cursor
        } else if self.take_word("Limit").is_some() {
            if self.take(TokenKind::Less).is_some() {
                let maximum = self.next()?.clone();
                let TokenKind::Unsigned(value) = maximum.kind else {
                    return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                        DiagnosticCode::UnexpectedToken,
                        maximum.span,
                        "bounded Limit maximum must be an unsigned literal",
                        Some("use Limit<MAX> where MAX is from 1 through 65534"),
                    )));
                };
                let canonical = value == "0" || !value.starts_with('0');
                let maximum_value = value.parse::<u64>().ok();
                if !canonical
                    || !maximum_value.is_some_and(|value| {
                        (1..=riffdb_types::MAX_APPLICATION_QUERY_PAGE_ROWS).contains(&value)
                    })
                {
                    return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                        DiagnosticCode::InvalidToken,
                        maximum.span,
                        "bounded Limit maximum is outside the supported range",
                        Some("use one canonical unsigned literal from 1 through 65534"),
                    )));
                }
                self.close_bounded_maximum()?;
                TypeReference::BoundedLimit(maximum_value.expect("checked bounded Limit maximum"))
            } else {
                // An unbounded `Limit` is charged -- and authorized -- at the
                // type maximum of 499 regardless of any declared default, so
                // `Limit = 50` reads as a fifty-row bound while granting
                // authority for 499. Every use is either equivalent to
                // `Limit<499>` or a mistake, so the maximum is now required.
                // Already-compiled modules still decode their unbounded row
                // limits; only source declaring one is rejected.
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::InvalidToken,
                    self.span_from(start),
                    "Limit must declare its maximum",
                    Some(
                        "use Limit<MAX> with MAX from 1 through 65534; the declared maximum is what cost and role authority are charged at, not the default",
                    ),
                )));
            }
        } else {
            TypeReference::Named(self.path()?)
        };
        if self.take(TokenKind::Question).is_some() {
            if type_contains_bounded_limit(&value) {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::InvalidToken,
                    self.span_from(start),
                    "bounded Limit cannot be optional",
                    Some("omit the parameter only by declaring a compiled default"),
                )));
            }
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
        let tokenized_match = if let Some(start) = self.take_word("matching") {
            self.expect(TokenKind::LeftParen)?;
            let index = self.identifier()?;
            self.expect(TokenKind::Comma)?;
            let kind_start = self.current_start();
            let kind = if self.take_word("conjunction").is_some() {
                TokenizedMatchKind::Conjunction
            } else if self.take_word("disjunction").is_some() {
                TokenizedMatchKind::Disjunction
            } else if self.take_word("phrase").is_some() {
                TokenizedMatchKind::Phrase
            } else if self.take_word("proximity").is_some() {
                self.expect(TokenKind::Comma)?;
                let distance = self.literal()?;
                let distance_span = distance.span;
                let Literal::Unsigned(distance) = distance.value else {
                    return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                        DiagnosticCode::UnexpectedToken,
                        distance_span,
                        "tokenized proximity distance must be a positive integer literal",
                        None,
                    )));
                };
                let distance = distance
                    .parse::<u16>()
                    .ok()
                    .filter(|distance| (1..=1_024).contains(distance))
                    .ok_or_else(|| {
                        ParseDiagnostics::one(ParseDiagnostic::new(
                            DiagnosticCode::InvalidToken,
                            distance_span,
                            "tokenized proximity distance exceeds its compiler bound",
                            Some("use a value from 1 through 1024"),
                        ))
                    })?;
                TokenizedMatchKind::Proximity(distance)
            } else {
                return Err(self.error(
                    DiagnosticCode::UnexpectedToken,
                    "unknown tokenized match kind",
                    Some("use conjunction, disjunction, phrase, or proximity"),
                ));
            };
            let kind = Spanned {
                value: kind,
                span: self.span_from(kind_start),
            };
            self.expect(TokenKind::Comma)?;
            let query = self.parameter_name()?;
            let ranking = if self.take(TokenKind::Comma).is_some() {
                if self.take_word("riff_bm25_v1").is_none() {
                    return Err(self.error(
                        DiagnosticCode::UnexpectedToken,
                        "unknown tokenized ranking",
                        Some("use riff_bm25_v1 or omit ranking for canonical key order"),
                    ));
                }
                TokenizedRanking::RiffBm25V1
            } else {
                TokenizedRanking::Boolean
            };
            self.expect(TokenKind::RightParen)?;
            Some(TokenizedMatchClause {
                index,
                kind,
                query,
                ranking,
                span: self.span_from(start.start as usize),
            })
        } else {
            None
        };
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
                let null_placement = if self.take_word("nulls").is_some() {
                    if let Some(span) = self.take_word("first") {
                        Some(Spanned {
                            value: NullPlacement::First,
                            span,
                        })
                    } else if let Some(span) = self.take_word("last") {
                        Some(Spanned {
                            value: NullPlacement::Last,
                            span,
                        })
                    } else {
                        return Err(self.error(
                            DiagnosticCode::UnexpectedToken,
                            "null placement requires first or last",
                            Some("use nulls first or nulls last"),
                        ));
                    }
                } else {
                    None
                };
                order.push(OrderTerm {
                    path,
                    direction,
                    null_placement,
                });
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
            let offset = if self.take_word("offset").is_some() {
                if after.is_some() {
                    return Err(self.error(
                        DiagnosticCode::UnsupportedForm,
                        "cursor and ordinal windows cannot be combined",
                        Some("use either after $cursor or offset $offset"),
                    ));
                }
                Some(self.take_offset()?)
            } else {
                None
            };
            Some(Take {
                limit,
                after,
                offset,
            })
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
        if tokenized_match.is_some() && cardinality.value != Cardinality::Many {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnsupportedForm,
                cardinality.span,
                "tokenized matching is only valid on many bindings",
                Some("change cardinality to many"),
            )));
        }
        if tokenized_match.is_some() && nearest.is_some() {
            return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                DiagnosticCode::UnsupportedForm,
                cardinality.span,
                "tokenized matching cannot be combined with nearest",
                None,
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
            tokenized_match,
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
            TokenKind::Ident(value) if value == "exact_count" => {
                (AggregateFunction::ExactCount, false)
            }
            TokenKind::Ident(value) if value == "sum" => (AggregateFunction::Sum, true),
            TokenKind::Ident(value) if value == "min" => (AggregateFunction::Min, true),
            TokenKind::Ident(value) if value == "max" => (AggregateFunction::Max, true),
            TokenKind::Ident(value) if value == "count_present" => {
                (AggregateFunction::CountPresent, true)
            }
            TokenKind::Ident(value) if value == "count_distinct" => {
                (AggregateFunction::CountDistinct, true)
            }
            TokenKind::Ident(value) if value == "count_distinct_present" => {
                (AggregateFunction::CountDistinctPresent, true)
            }
            TokenKind::Ident(value) if value == "mean" => (AggregateFunction::Mean, true),
            TokenKind::Ident(value) if value == "any" => (AggregateFunction::Any, true),
            TokenKind::Ident(value) if value == "all" => (AggregateFunction::All, true),
            _ => {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::UnexpectedToken,
                    token.span,
                    "unknown aggregate function",
                    Some("use count(), exact_count(), sum(field), min(field), or max(field)"),
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
            let mut reveals = Vec::new();
            while self.peek_secret_reveal_clause() {
                if reveals.len() == MAX_COLLECTION_ITEMS {
                    return Err(self.too_many());
                }
                self.expect_word("reveals")?;
                reveals.push(self.spanned_path()?);
            }
            let nested = if self.peek(TokenKind::LeftBrace) {
                Some(self.selection()?)
            } else {
                None
            };
            fields.push(FieldSelection {
                alias,
                source,
                reveals,
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
            TokenKind::Ident(word) if word == "not_in" => (BinaryOperator::NotIn, 3),
            TokenKind::Ident(word) if word == "prefix" => (BinaryOperator::Prefix, 3),
            TokenKind::Ident(word) if word == "starts_with" => (BinaryOperator::StartsWith, 3),
            TokenKind::Ident(word) if word == "ends_with" => (BinaryOperator::EndsWith, 3),
            TokenKind::Ident(word) if word == "contains" => (BinaryOperator::Contains, 3),
            _ => return None,
        };
        Some((pair.0, pair.1, token.span))
    }

    fn take_offset(&mut self) -> Result<Spanned<Expression>, ParseDiagnostics> {
        let token = self.next()?.clone();
        let value = match token.kind {
            TokenKind::Unsigned(value) => Expression::Literal(Literal::Unsigned(value)),
            TokenKind::Parameter(value) => {
                let name = Identifier::new(&value).ok_or_else(|| {
                    ParseDiagnostics::one(ParseDiagnostic::new(
                        DiagnosticCode::InvalidToken,
                        token.span,
                        "invalid offset parameter",
                        None,
                    ))
                })?;
                Expression::Parameter(Spanned {
                    value: name,
                    span: token.span,
                })
            }
            _ => {
                return Err(ParseDiagnostics::one(ParseDiagnostic::new(
                    DiagnosticCode::UnexpectedToken,
                    token.span,
                    "offset requires a nonnegative literal or u64 parameter",
                    None,
                )));
            }
        };
        self.node()?;
        Ok(Spanned {
            value,
            span: token.span,
        })
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

    fn peek_secret_reveal_clause(&self) -> bool {
        matches!(
            self.tokens.get(self.index).map(|token| &token.kind),
            Some(TokenKind::Ident(value)) if value == "reveals"
        ) && matches!(
            self.tokens.get(self.index + 1).map(|token| &token.kind),
            Some(TokenKind::Ident(_))
        ) && matches!(
            self.tokens.get(self.index + 2).map(|token| &token.kind),
            Some(TokenKind::Dot)
        ) && matches!(
            self.tokens.get(self.index + 3).map(|token| &token.kind),
            Some(TokenKind::Ident(_))
        )
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

    /// Consumes the `>` closing a bounded maximum.
    ///
    /// `Limit<50>=25` lexes its `>=` as one relational token, so closing the
    /// maximum has to split it: the `>` closes the bound and the `=` is left in
    /// place to introduce the default. Without this, the only spelling that
    /// parses is one with a space before the `=`, and the failure reads
    /// "unexpected RiffQL token" with no help — now that a maximum is required
    /// rather than optional, every author would meet it.
    fn close_bounded_maximum(&mut self) -> Result<(), ParseDiagnostics> {
        if self.take(TokenKind::Greater).is_some() {
            return Ok(());
        }
        if self.peek(TokenKind::GreaterEqual) {
            let span = self.tokens[self.index].span;
            self.tokens[self.index] = Token {
                kind: TokenKind::Equal,
                span: Span {
                    start: span.start + 1,
                    end: span.end,
                },
            };
            return Ok(());
        }
        self.expect(TokenKind::Greater).map(|_| ())
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

fn type_contains_bounded_limit(value: &TypeReference) -> bool {
    match value {
        TypeReference::BoundedLimit(_) => true,
        TypeReference::Optional(inner) | TypeReference::Set(inner) => {
            type_contains_bounded_limit(&inner.value)
        }
        TypeReference::Named(_) | TypeReference::Cursor | TypeReference::Limit => false,
    }
}

/// Returns the pre-bounded-limit language family selected by one parsed query.
///
/// `Limit<MAX>` is an additive parameter constraint rather than a replacement
/// for the query's execution family. Consumers that dispatch to specialized
/// exact-result providers use this classifier after parsing V9 source.
#[must_use]
pub fn document_query_shape_language_version(document: &Document) -> u32 {
    query_shape_language_version(&document.body, document.projected_source.as_ref())
}

fn query_shape_language_version(
    body: &QueryBody,
    projected_source: Option<&ProjectedSource>,
) -> u32 {
    if body
        .bindings
        .iter()
        .any(|binding| binding.tokenized_match.is_some())
    {
        RIFFQL_LANGUAGE_VERSION_TOKENIZED_TEXT_V1
    } else if body_uses_exact_aggregate_core(body) {
        RIFFQL_LANGUAGE_VERSION_EXACT_AGGREGATE_V1
    } else if projected_source.is_some() {
        RIFFQL_LANGUAGE_VERSION_PROJECTED_VECTOR_V1
    } else if body
        .bindings
        .iter()
        .flat_map(|binding| &binding.order)
        .any(|term| term.null_placement.is_some())
    {
        RIFFQL_LANGUAGE_VERSION_NULLABLE_EXACT_ORDER_V1
    } else if body_uses_rich_exact_result_set(body) {
        RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1
    } else if body_uses_exact_result_set(body) {
        RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1
    } else if selection_uses_secret_output(&body.selection) {
        RIFFQL_LANGUAGE_VERSION_SECRET_OUTPUT_V1
    } else if !body.aggregates.is_empty()
        || body
            .bindings
            .iter()
            .any(|binding| expression_uses_operational_syntax(&binding.predicate.value))
    {
        RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1
    } else {
        RIFFQL_LANGUAGE_VERSION
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
            matches!(
                operator.value,
                BinaryOperator::Prefix | BinaryOperator::NotIn
            ) || expression_uses_operational_syntax(&left.value)
                || expression_uses_operational_syntax(&right.value)
        }
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => false,
    }
}

fn body_uses_rich_exact_result_set(body: &QueryBody) -> bool {
    if !body_uses_exact_result_set(body) {
        return false;
    }
    body.bindings.iter().any(|binding| {
        expression_uses_rich_exact_predicate(&binding.predicate.value)
            || exact_text_field(&binding.predicate.value).is_some_and(|field| {
                binding
                    .order
                    .first()
                    .and_then(|term| term.path.value.0.last())
                    .is_some_and(|ordered| ordered.value.as_str() != field)
            })
    })
}

fn exact_text_field(expression: &Expression) -> Option<&str> {
    match expression {
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            if matches!(
                operator.value,
                BinaryOperator::StartsWith | BinaryOperator::EndsWith | BinaryOperator::Contains
            ) && let Expression::Path(path) = &left.value
            {
                return path.0.last().map(|segment| segment.value.as_str());
            }
            exact_text_field(&left.value).or_else(|| exact_text_field(&right.value))
        }
        Expression::PresenceGuard { predicate, .. } => exact_text_field(&predicate.value),
        Expression::Unary { operand, .. } => exact_text_field(&operand.value),
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => None,
    }
}

fn expression_uses_rich_exact_predicate(expression: &Expression) -> bool {
    match expression {
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            matches!(
                operator.value,
                BinaryOperator::NotEqual
                    | BinaryOperator::Less
                    | BinaryOperator::LessEqual
                    | BinaryOperator::Greater
                    | BinaryOperator::GreaterEqual
                    | BinaryOperator::In
                    | BinaryOperator::NotIn
                    | BinaryOperator::Prefix
                    | BinaryOperator::Or
            ) || expression_uses_rich_exact_predicate(&left.value)
                || expression_uses_rich_exact_predicate(&right.value)
        }
        Expression::PresenceGuard { predicate, .. } => {
            expression_uses_rich_exact_predicate(&predicate.value)
        }
        Expression::Unary { .. } => true,
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => false,
    }
}

fn body_uses_exact_result_set(body: &QueryBody) -> bool {
    body.bindings.iter().any(|binding| {
        binding
            .take
            .as_ref()
            .is_some_and(|take| take.offset.is_some())
            || expression_uses_exact_text(&binding.predicate.value)
    }) || body.aggregates.iter().any(|aggregate| {
        aggregate
            .measures
            .iter()
            .any(|measure| measure.function.value == AggregateFunction::ExactCount)
    })
}

fn body_uses_exact_aggregate_core(body: &QueryBody) -> bool {
    body.aggregates.iter().any(|aggregate| {
        aggregate.measures.iter().any(|measure| {
            matches!(
                measure.function.value,
                AggregateFunction::CountPresent
                    | AggregateFunction::CountDistinct
                    | AggregateFunction::CountDistinctPresent
                    | AggregateFunction::Mean
                    | AggregateFunction::Any
                    | AggregateFunction::All
            )
        })
    })
}

fn expression_uses_exact_text(expression: &Expression) -> bool {
    match expression {
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            matches!(
                operator.value,
                BinaryOperator::StartsWith | BinaryOperator::EndsWith | BinaryOperator::Contains
            ) || expression_uses_exact_text(&left.value)
                || expression_uses_exact_text(&right.value)
        }
        Expression::PresenceGuard { predicate, .. } => expression_uses_exact_text(&predicate.value),
        Expression::Unary { operand, .. } => expression_uses_exact_text(&operand.value),
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => false,
    }
}

fn selection_uses_secret_output(selection: &Selection) -> bool {
    selection.fields.iter().any(|field| {
        !field.reveals.is_empty()
            || field
                .nested
                .as_ref()
                .is_some_and(selection_uses_secret_output)
    })
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
