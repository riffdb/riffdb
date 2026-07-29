//! Parser safety-boundary and diagnostic identity tests.

use riffdb_riffql_syntax::{DiagnosticCode, MAX_SOURCE_BYTES, parse_query, parse_query_bytes};

#[test]
fn source_utf8_identifier_and_positive_many_bounds_fail_closed() {
    let oversized = " ".repeat(MAX_SOURCE_BYTES + 1);
    assert_code(&oversized, DiagnosticCode::SourceTooLong);

    let invalid_utf8 = parse_query_bytes(&[0xff]).expect_err("invalid UTF-8");
    assert_eq!(
        invalid_utf8.as_slice()[0].code(),
        DiagnosticCode::InvalidToken
    );

    let oversized_name = "x".repeat(257);
    assert_code(
        &format!("query {oversized_name}() {{ return {{ value }} }}"),
        DiagnosticCode::InvalidToken,
    );

    assert_code(
        "query Zero() { many rows from Ticket where active == true take 0 return { rows } }",
        DiagnosticCode::UnexpectedToken,
    );
}

#[test]
fn diagnostic_output_is_value_free_and_source_spanned() {
    let source =
        "query Secret() { many rows from Ticket where password == \"canary\" return { rows } }";
    let diagnostics = parse_query(source).expect_err("unbounded many");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), DiagnosticCode::UnboundedMany);
    assert!(diagnostic.span().end > diagnostic.span().start);
    assert!(!diagnostic.summary().contains("canary"));
    assert!(!diagnostic.help().unwrap_or_default().contains("canary"));
}

fn assert_code(source: &str, expected: DiagnosticCode) {
    let diagnostics = parse_query(source).expect_err("source must reject");
    assert_eq!(diagnostics.as_slice()[0].code(), expected);
}
