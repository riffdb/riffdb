use super::{Token, lex};
use crate::diagnostic::SyntaxDiagnosticCode;
use crate::limits::{MAX_IDENTIFIER_BYTES, MAX_NESTING_DEPTH, MAX_SOURCE_BYTES, MAX_TOKENS};

fn token_values(source: &str) -> Vec<Token> {
    lex(source)
        .expect("test source should lex")
        .into_iter()
        .map(|token| token.value)
        .collect()
}

fn assert_lex_error(source: &str, code: SyntaxDiagnosticCode) {
    let diagnostics = lex(source).expect_err("test source should fail lexing");
    assert_eq!(diagnostics.as_slice().len(), 1);
    assert_eq!(diagnostics.as_slice()[0].code(), code);
}

#[test]
fn lexes_every_reserved_keyword() {
    let source = concat!(
        "contract version entity key field invariant index presence text_key binary_utf8_v1 ",
        "unicode_fold_v1 unique reference event enum aggregate root child ",
        "partition_by conflict_key projection source where measure count sum frontier ",
        "transactionally_ordered command workflow state transition from to lease claim renew ",
        "release expire fence owner expires_at ",
        "fencing_token attempts duration_seconds service uuid_v7 transaction_time on revision ",
        "stale illegal unavailable invalid exhausted expired active input idempotency_key read mutate create as else ",
        "require set emit return bool i64 u64 timestamp date uuid decimal money string bytes ",
        "optional list true false null"
    );
    let expected = vec![
        Token::Contract,
        Token::Version,
        Token::Entity,
        Token::Key,
        Token::Field,
        Token::Invariant,
        Token::Index,
        Token::Presence,
        Token::TextKey,
        Token::BinaryUtf8V1,
        Token::UnicodeFoldV1,
        Token::Unique,
        Token::Reference,
        Token::Event,
        Token::Enum,
        Token::Aggregate,
        Token::Root,
        Token::Child,
        Token::PartitionBy,
        Token::ConflictKey,
        Token::Projection,
        Token::Source,
        Token::Where,
        Token::Measure,
        Token::Count,
        Token::Sum,
        Token::Frontier,
        Token::TransactionallyOrdered,
        Token::Command,
        Token::Workflow,
        Token::State,
        Token::Transition,
        Token::From,
        Token::To,
        Token::Lease,
        Token::Claim,
        Token::Renew,
        Token::Release,
        Token::Expire,
        Token::Fence,
        Token::Owner,
        Token::ExpiresAt,
        Token::FencingToken,
        Token::Attempts,
        Token::DurationSeconds,
        Token::Service,
        Token::UuidV7,
        Token::TransactionTime,
        Token::On,
        Token::Revision,
        Token::Stale,
        Token::Illegal,
        Token::Unavailable,
        Token::Invalid,
        Token::Exhausted,
        Token::Expired,
        Token::Active,
        Token::Input,
        Token::IdempotencyKey,
        Token::Read,
        Token::Mutate,
        Token::Create,
        Token::As,
        Token::Else,
        Token::Require,
        Token::Set,
        Token::Emit,
        Token::Return,
        Token::Bool,
        Token::I64,
        Token::U64,
        Token::Timestamp,
        Token::Date,
        Token::Uuid,
        Token::Decimal,
        Token::Money,
        Token::String,
        Token::Bytes,
        Token::Optional,
        Token::List,
        Token::True,
        Token::False,
        Token::Null,
    ];

    assert_eq!(token_values(source), expected);
}

#[test]
fn lexes_punctuation_operators_and_literals() {
    let source = r#"{}() <= >= == != && || < > , : . = ! - * / + Name 42 12.50 "line\n\u0021""#;
    let expected = vec![
        Token::LeftBrace,
        Token::RightBrace,
        Token::LeftParen,
        Token::RightParen,
        Token::LessEqual,
        Token::GreaterEqual,
        Token::EqualEqual,
        Token::BangEqual,
        Token::AndAnd,
        Token::OrOr,
        Token::Less,
        Token::Greater,
        Token::Comma,
        Token::Colon,
        Token::Dot,
        Token::Equal,
        Token::Bang,
        Token::Minus,
        Token::Star,
        Token::Slash,
        Token::Plus,
        Token::Identifier("Name".to_owned()),
        Token::UIntLiteral("42".to_owned()),
        Token::FixedDecimalLiteral("12.50".to_owned()),
        Token::StringLiteral(r#""line\n\u0021""#.to_owned()),
    ];

    assert_eq!(token_values(source), expected);
}

#[test]
fn preserves_half_open_utf8_byte_spans() {
    let tokens = lex("\"é\" Name").expect("test source should lex");
    assert_eq!(tokens.len(), 2);
    assert_eq!(tokens[0].span.start(), 0);
    assert_eq!(tokens[0].span.end(), 4);
    assert_eq!(tokens[1].span.start(), 5);
    assert_eq!(tokens[1].span.end(), 9);
}

#[test]
fn skips_whitespace_and_line_comments_only() {
    assert_eq!(
        token_values("// heading\r\ncontract\tName // trailing"),
        vec![Token::Contract, Token::Identifier("Name".to_owned())]
    );
    assert_lex_error(
        "contract /* deferred */ Name",
        SyntaxDiagnosticCode::UnsupportedSyntax,
    );
    assert_lex_error("/* unterminated", SyntaxDiagnosticCode::UnsupportedSyntax);

    let source = "/* unsupported */";
    let diagnostics = lex(source).expect_err("block comments are deferred");
    assert_eq!(diagnostics.as_slice()[0].span().start(), 0);
    assert_eq!(
        diagnostics.as_slice()[0].span().end(),
        u32::try_from(source.len()).expect("small fixture")
    );
}

#[test]
fn rejects_all_closed_deferred_keywords() {
    for keyword in [
        "module",
        "import",
        "include",
        "query",
        "state_machine",
        "capability",
        "approval",
        "default",
    ] {
        assert_lex_error(keyword, SyntaxDiagnosticCode::UnsupportedSyntax);
    }
}

#[test]
fn keywords_are_lowercase_and_identifiers_are_case_sensitive() {
    assert_eq!(
        token_values("contract Contract CONTRACT contract_name"),
        vec![
            Token::Contract,
            Token::Identifier("Contract".to_owned()),
            Token::Identifier("CONTRACT".to_owned()),
            Token::Identifier("contract_name".to_owned()),
        ]
    );
}

#[test]
fn validates_json_string_representation_without_decoding_it() {
    let source = r#""\\\/\b\f\n\r\t\"\u00e9""#;
    assert_eq!(
        token_values(source),
        vec![Token::StringLiteral(source.to_owned())]
    );

    for invalid in [
        "\"unterminated",
        "\"bad\\q\"",
        "\"bad\\u123\"",
        "\"bad\\u12xz\"",
        "\"line\nfeed\"",
    ] {
        assert_lex_error(invalid, SyntaxDiagnosticCode::InvalidToken);
    }

    let source = "\"bad\\q\"";
    let diagnostics = lex(source).expect_err("invalid escape should fail lexing");
    assert_eq!(diagnostics.as_slice()[0].span().start(), 0);
    assert_eq!(
        diagnostics.as_slice()[0].span().end(),
        u32::try_from(source.len()).expect("small fixture")
    );
}

#[test]
fn rejects_invalid_numeric_representations_lexically() {
    for invalid in ["1.", ".1", "1e2", "1E+2", "0x10", "1_000", "1abc", "1.2e3"] {
        assert_lex_error(invalid, SyntaxDiagnosticCode::InvalidToken);
    }
}

#[test]
fn enforces_identifier_source_and_token_bounds() {
    let at_identifier_limit = "a".repeat(MAX_IDENTIFIER_BYTES);
    assert_eq!(
        token_values(&at_identifier_limit),
        vec![Token::Identifier(at_identifier_limit)]
    );
    assert_lex_error(
        &"a".repeat(MAX_IDENTIFIER_BYTES + 1),
        SyntaxDiagnosticCode::InvalidToken,
    );
    assert_lex_error("é", SyntaxDiagnosticCode::InvalidToken);

    let at_source_limit = " ".repeat(MAX_SOURCE_BYTES);
    assert!(
        lex(&at_source_limit)
            .expect("source at limit should lex")
            .is_empty()
    );
    assert_lex_error(
        &" ".repeat(MAX_SOURCE_BYTES + 1),
        SyntaxDiagnosticCode::SourceLimit,
    );

    let at_token_limit = "+ ".repeat(MAX_TOKENS);
    assert_eq!(
        lex(&at_token_limit)
            .expect("token stream at limit should lex")
            .len(),
        MAX_TOKENS
    );
    assert_lex_error(
        &"+ ".repeat(MAX_TOKENS + 1),
        SyntaxDiagnosticCode::NodeLimit,
    );
}

#[test]
fn enforces_delimiter_and_type_constructor_nesting() {
    let at_paren_limit = format!(
        "{}{}",
        "(".repeat(MAX_NESTING_DEPTH),
        ")".repeat(MAX_NESTING_DEPTH)
    );
    assert!(lex(&at_paren_limit).is_ok());
    assert_lex_error(
        &"(".repeat(MAX_NESTING_DEPTH + 1),
        SyntaxDiagnosticCode::NestingLimit,
    );

    let at_brace_limit = format!(
        "{}{}",
        "{".repeat(MAX_NESTING_DEPTH),
        "}".repeat(MAX_NESTING_DEPTH)
    );
    assert!(lex(&at_brace_limit).is_ok());
    assert_lex_error(
        &"{".repeat(MAX_NESTING_DEPTH + 1),
        SyntaxDiagnosticCode::NestingLimit,
    );

    let nested_type = format!(
        "{}u64{}",
        "optional<".repeat(MAX_NESTING_DEPTH),
        ">".repeat(MAX_NESTING_DEPTH)
    );
    assert!(lex(&nested_type).is_ok());
    assert_lex_error(
        &"optional<".repeat(MAX_NESTING_DEPTH + 1),
        SyntaxDiagnosticCode::NestingLimit,
    );

    assert!(lex(&"!".repeat(MAX_NESTING_DEPTH)).is_ok());
    assert_lex_error(
        &"!".repeat(MAX_NESTING_DEPTH + 1),
        SyntaxDiagnosticCode::NestingLimit,
    );

    // Comparison operators are not treated as type delimiters.
    assert!(lex(&"value < ".repeat(MAX_NESTING_DEPTH + 1)).is_ok());
}

#[test]
fn caller_input_never_enters_lexical_diagnostics() {
    let canary = "DO_NOT_EXPOSE_THIS_CANARY";
    let diagnostics =
        lex(&format!("\"bad\\q{canary}\"")).expect_err("malformed escape should fail lexing");
    let rendered = format!("{diagnostics:?}");
    assert!(!rendered.contains(canary));
}
