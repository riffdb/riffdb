//! Bounded lexical analysis for contract grammar version 3.

use logos::{Lexer, Logos};

use crate::ast::{Span, Spanned};
use crate::diagnostic::{SyntaxDiagnostic, SyntaxDiagnosticCode, SyntaxDiagnostics};
use crate::limits::{MAX_IDENTIFIER_BYTES, MAX_NESTING_DEPTH, MAX_SOURCE_BYTES, MAX_TOKENS};

/// A token paired with its half-open UTF-8 byte span.
pub(crate) type SpannedToken = Spanned<Token>;

/// Reserved syntax which is deliberately absent from grammar version 3.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum DeferredKeyword {
    Module,
    Import,
    Include,
    Query,
    StateMachine,
    Capability,
    Approval,
    Default,
}

/// The complete external token set for contract grammar version 3.
#[derive(Clone, Debug, Eq, Hash, Logos, PartialEq)]
#[allow(clippy::enum_variant_names)] // `fencing_token` is the accepted external keyword.
#[logos(error = LexingError)]
#[logos(skip r"[ \t\r\n\x0C]+")]
#[logos(skip(r"//[^\r\n]*", allow_greedy = true))]
pub(crate) enum Token {
    #[token("contract")]
    Contract,
    #[token("version")]
    Version,
    #[token("entity")]
    Entity,
    #[token("key")]
    Key,
    #[token("field")]
    Field,
    #[token("invariant")]
    Invariant,
    #[token("index")]
    Index,
    #[token("presence")]
    Presence,
    #[token("text_key")]
    TextKey,
    #[token("binary_utf8_v1")]
    BinaryUtf8V1,
    #[token("unicode_fold_v1")]
    UnicodeFoldV1,
    #[token("unique")]
    Unique,
    #[token("reference")]
    Reference,
    #[token("vector_field")]
    VectorField,
    #[token("cosine")]
    Cosine,
    #[token("euclidean")]
    Euclidean,
    #[token("dot_product")]
    DotProduct,
    #[token("staleness_slo")]
    StalenessSlo,
    #[token("event")]
    Event,
    #[token("enum")]
    Enum,
    #[token("aggregate")]
    Aggregate,
    #[token("root")]
    Root,
    #[token("child")]
    Child,
    #[token("partition_by")]
    PartitionBy,
    #[token("conflict_key")]
    ConflictKey,
    #[token("projection")]
    Projection,
    #[token("source")]
    Source,
    #[token("where")]
    Where,
    #[token("measure")]
    Measure,
    #[token("count")]
    Count,
    #[token("sum")]
    Sum,
    #[token("frontier")]
    Frontier,
    #[token("transactionally_ordered")]
    TransactionallyOrdered,
    #[token("command")]
    Command,
    #[token("workflow")]
    Workflow,
    #[token("state")]
    State,
    #[token("transition")]
    Transition,
    #[token("from")]
    From,
    #[token("to")]
    To,
    #[token("lease")]
    Lease,
    #[token("claim")]
    Claim,
    #[token("renew")]
    Renew,
    #[token("release")]
    Release,
    #[token("expire")]
    Expire,
    #[token("fence")]
    Fence,
    #[token("owner")]
    Owner,
    #[token("expires_at")]
    ExpiresAt,
    #[token("fencing_token")]
    FencingToken,
    #[token("attempts")]
    Attempts,
    #[token("duration_seconds")]
    DurationSeconds,
    #[token("service")]
    Service,
    #[token("uuid_v7")]
    UuidV7,
    #[token("transaction_time")]
    TransactionTime,
    #[token("on")]
    On,
    #[token("revision")]
    Revision,
    #[token("stale")]
    Stale,
    #[token("illegal")]
    Illegal,
    #[token("unavailable")]
    Unavailable,
    #[token("invalid")]
    Invalid,
    #[token("exhausted")]
    Exhausted,
    #[token("expired")]
    Expired,
    #[token("active")]
    Active,
    #[token("input")]
    Input,
    #[token("idempotency_key")]
    IdempotencyKey,
    #[token("read")]
    Read,
    #[token("mutate")]
    Mutate,
    #[token("create")]
    Create,
    #[token("as")]
    As,
    #[token("else")]
    Else,
    #[token("require")]
    Require,
    #[token("set")]
    Set,
    #[token("emit")]
    Emit,
    #[token("return")]
    Return,

    #[token("bool")]
    Bool,
    #[token("i64")]
    I64,
    #[token("u64")]
    U64,
    #[token("timestamp")]
    Timestamp,
    #[token("date")]
    Date,
    #[token("uuid")]
    Uuid,
    #[token("decimal")]
    Decimal,
    #[token("money")]
    Money,
    #[token("string")]
    String,
    #[token("bytes")]
    Bytes,
    #[token("optional")]
    Optional,
    #[token("list")]
    List,

    #[token("true")]
    True,
    #[token("false")]
    False,
    #[token("null")]
    Null,

    #[token("{")]
    LeftBrace,
    #[token("}")]
    RightBrace,
    #[token("(")]
    LeftParen,
    #[token(")")]
    RightParen,
    #[token("<=")]
    LessEqual,
    #[token(">=")]
    GreaterEqual,
    #[token("->")]
    Arrow,
    #[token("==")]
    EqualEqual,
    #[token("!=")]
    BangEqual,
    #[token("&&")]
    AndAnd,
    #[token("||")]
    OrOr,
    #[token("<")]
    Less,
    #[token(">")]
    Greater,
    #[token(",")]
    Comma,
    #[token(":")]
    Colon,
    #[token(".")]
    Dot,
    #[token("=")]
    Equal,
    #[token("!")]
    Bang,
    #[token("-")]
    Minus,
    #[token("*")]
    Star,
    #[token("/")]
    Slash,
    #[token("+")]
    Plus,

    #[regex(r"[0-9]+\.[0-9]+", owned_slice)]
    FixedDecimalLiteral(String),
    #[regex(r"[0-9]+", owned_slice)]
    UIntLiteral(String),
    #[token("\"", scan_json_string)]
    StringLiteral(String),
    #[regex(r"[A-Za-z_][A-Za-z0-9_]*", bounded_identifier)]
    Identifier(String),

    #[token("module", |_| DeferredKeyword::Module)]
    #[token("import", |_| DeferredKeyword::Import)]
    #[token("include", |_| DeferredKeyword::Include)]
    #[token("query", |_| DeferredKeyword::Query)]
    #[token("state_machine", |_| DeferredKeyword::StateMachine)]
    #[token("capability", |_| DeferredKeyword::Capability)]
    #[token("approval", |_| DeferredKeyword::Approval)]
    #[token("default", |_| DeferredKeyword::Default)]
    Deferred(DeferredKeyword),

    #[token("/*", consume_block_comment)]
    UnsupportedBlockComment,

    // These patterns consume malformed numeric spellings as one lexical error
    // instead of leaking fragments into parser diagnostics.
    #[regex(r"[0-9]+\.")]
    #[regex(r"\.[0-9]+")]
    #[regex(r"[0-9]+(?:\.[0-9]+)?[A-Za-z_][A-Za-z0-9_]*")]
    InvalidNumericLiteral,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum LexingError {
    #[default]
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Delimiter {
    Brace,
    Paren,
    TypeArgument,
}

/// Lexes one source document and applies all lexer-owned safety bounds.
///
/// Lexical failures are fail-fast. Parser diagnostics may aggregate later, but
/// malformed literal text and Logos errors never become public diagnostic text.
pub(crate) fn lex(source: &str) -> Result<Vec<SpannedToken>, SyntaxDiagnostics> {
    if source.len() > MAX_SOURCE_BYTES {
        return Err(single_diagnostic(
            SyntaxDiagnosticCode::SourceLimit,
            bounded_source_span(source),
        ));
    }

    let mut lexer = Token::lexer(source);
    let mut tokens = Vec::new();
    let mut delimiters = Vec::with_capacity(MAX_NESTING_DEPTH);
    let mut unary_depth = 0_usize;

    while let Some(result) = lexer.next() {
        let logos_span = lexer.span();
        let span = Span::new(logos_span.start, logos_span.end)
            .ok_or_else(|| single_diagnostic(SyntaxDiagnosticCode::SourceLimit, Span::ZERO))?;
        let token = match result {
            Ok(token) => token,
            Err(LexingError::Invalid) => {
                return Err(single_diagnostic(SyntaxDiagnosticCode::InvalidToken, span));
            }
        };

        if matches!(token, Token::Deferred(_) | Token::UnsupportedBlockComment) {
            return Err(single_diagnostic(
                SyntaxDiagnosticCode::UnsupportedSyntax,
                span,
            ));
        }
        if matches!(token, Token::InvalidNumericLiteral) {
            return Err(single_diagnostic(SyntaxDiagnosticCode::InvalidToken, span));
        }
        if tokens.len() == MAX_TOKENS {
            return Err(single_diagnostic(SyntaxDiagnosticCode::NodeLimit, span));
        }

        if matches!(token, Token::Bang | Token::Minus) {
            unary_depth += 1;
            if unary_depth > MAX_NESTING_DEPTH {
                return Err(single_diagnostic(SyntaxDiagnosticCode::NestingLimit, span));
            }
        } else {
            unary_depth = 0;
        }
        update_nesting(&token, tokens.last(), span, &mut delimiters)?;
        tokens.push(Spanned::new(token, span));
    }

    Ok(tokens)
}

fn update_nesting(
    token: &Token,
    previous: Option<&SpannedToken>,
    span: Span,
    delimiters: &mut Vec<Delimiter>,
) -> Result<(), SyntaxDiagnostics> {
    let opening = match token {
        Token::LeftBrace => Some(Delimiter::Brace),
        Token::LeftParen => Some(Delimiter::Paren),
        Token::Less if previous.is_some_and(|previous| opens_type_arguments(&previous.value)) => {
            Some(Delimiter::TypeArgument)
        }
        _ => None,
    };

    if let Some(delimiter) = opening {
        if delimiters.len() == MAX_NESTING_DEPTH {
            return Err(single_diagnostic(SyntaxDiagnosticCode::NestingLimit, span));
        }
        delimiters.push(delimiter);
        return Ok(());
    }

    let closing = match token {
        Token::RightBrace => Some(Delimiter::Brace),
        Token::RightParen => Some(Delimiter::Paren),
        Token::Greater if delimiters.last() == Some(&Delimiter::TypeArgument) => {
            Some(Delimiter::TypeArgument)
        }
        _ => None,
    };
    if closing.is_some_and(|delimiter| delimiters.last() == Some(&delimiter)) {
        delimiters.pop();
    }
    Ok(())
}

const fn opens_type_arguments(token: &Token) -> bool {
    matches!(
        token,
        Token::Decimal
            | Token::Money
            | Token::String
            | Token::Bytes
            | Token::Optional
            | Token::List
    )
}

fn single_diagnostic(code: SyntaxDiagnosticCode, span: Span) -> SyntaxDiagnostics {
    SyntaxDiagnostics::single(SyntaxDiagnostic::new(code, span))
}

fn bounded_source_span(source: &str) -> Span {
    let end = source.len().min(MAX_SOURCE_BYTES);
    Span::new(0, end).unwrap_or(Span::ZERO)
}

fn owned_slice(lexer: &mut Lexer<'_, Token>) -> String {
    lexer.slice().to_owned()
}

fn bounded_identifier(lexer: &mut Lexer<'_, Token>) -> Result<String, LexingError> {
    if lexer.slice().len() > MAX_IDENTIFIER_BYTES {
        return Err(LexingError::Invalid);
    }
    Ok(lexer.slice().to_owned())
}

fn consume_block_comment(lexer: &mut Lexer<'_, Token>) {
    let remainder = lexer.remainder();
    let bytes_to_consume = remainder
        .find("*/")
        .map_or(remainder.len(), |offset| offset + 2);
    lexer.bump(bytes_to_consume);
}

fn scan_json_string(lexer: &mut Lexer<'_, Token>) -> Result<String, LexingError> {
    let remainder = lexer.remainder().as_bytes();
    let mut offset = 0;
    let mut valid = true;

    while offset < remainder.len() {
        match remainder[offset] {
            b'"' => {
                lexer.bump(offset + 1);
                return if valid {
                    Ok(lexer.slice().to_owned())
                } else {
                    Err(LexingError::Invalid)
                };
            }
            b'\\' => {
                let Some(escaped) = remainder.get(offset + 1).copied() else {
                    lexer.bump(remainder.len());
                    return Err(LexingError::Invalid);
                };
                match escaped {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                        offset += 2;
                    }
                    b'u' => {
                        let hex_end = offset.saturating_add(6);
                        if hex_end <= remainder.len()
                            && remainder[offset + 2..hex_end]
                                .iter()
                                .all(u8::is_ascii_hexdigit)
                        {
                            offset = hex_end;
                        } else {
                            valid = false;
                            offset = (offset + 2).min(remainder.len());
                        }
                    }
                    _ => {
                        valid = false;
                        offset += 2;
                    }
                }
            }
            byte if byte <= 0x1f => {
                valid = false;
                offset += 1;
            }
            _ => offset += 1,
        }
    }

    lexer.bump(remainder.len());
    Err(LexingError::Invalid)
}

#[cfg(test)]
mod tests;
