//! Compiler-checked named-query secret outputs (ADR-0128, WP-634).

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_ir::{
    QUERY_IR_VERSION_SECRET_OUTPUT_V1, QueryDiagnosticCode, SourceSymbolKind, SymbolicCatalog,
    resolve_query_surface,
};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = r#"
contract AuthShape version 1 {
  entity Account {
    key (org_id: uuid)
  }
  entity Session {
    key (org_id: uuid, session_id: uuid)
    field secret token_hash: string<256>
    field expires_at: timestamp
  }
  aggregate AccountRoot {
    root Account
    child Session
    partition_by org_id
    conflict_key (org_id)
  }
}
"#;

fn resolve(
    source: &str,
) -> Result<riffdb_query_ir::ResolvedQueryV1, riffdb_query_ir::QueryDiagnostics> {
    let bundle = compile_contract_source(CONTRACT).expect("compile contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let document = parse_query(source).expect("parse query");
    resolve_query_surface(&document, &catalog)
}

fn query(selection: &str) -> String {
    format!(
        r#"query GetSession($org: Session.org_id, $id: Session.session_id) {{
    one session from Session where org_id == $org && session_id == $id else NotFound
    return Found {{ session: session {{ {selection} }} }}
    outcomes Found | NotFound
}}"#
    )
}

#[test]
fn exact_secret_leaf_is_resolved_into_canonical_identity_and_result_slot() {
    let source = query("token_hash reveals session.token_hash");
    let resolved = resolve(&source).expect("declared secret output resolves");
    assert_eq!(resolved.ir_version(), QUERY_IR_VERSION_SECRET_OUTPUT_V1);
    let requirement = &resolved.secret_outputs()[0];
    assert_eq!(requirement.binding(), "session");
    assert_eq!(requirement.entity(), "Session");
    assert_eq!(requirement.field(), "token_hash");
    assert_eq!(
        requirement.result_path(),
        ["Found", "session", "token_hash"]
    );
    assert!(requirement.declaration_span().end > requirement.declaration_span().start);
    assert!(resolved.source_map().entries().iter().any(|entry| {
        entry.kind() == SourceSymbolKind::SecretOutput
            && entry.symbolic_path() == ["Session", "token_hash"]
    }));
}

#[test]
fn secret_leaf_without_declaration_fails_closed_at_the_projection() {
    let source = query("token_hash");
    let diagnostics = resolve(&source).expect_err("secret output intent is mandatory");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(
        diagnostic.code(),
        QueryDiagnosticCode::SecretOutputDeclaration
    );
    let start = source.find("token_hash").expect("projection span");
    assert_eq!(diagnostic.primary().start as usize, start);
}

#[test]
fn declaration_rejects_ordinary_mismatched_and_duplicate_fields() {
    for selection in [
        "expires_at reveals session.expires_at",
        "token_hash reveals session.expires_at",
        "token_hash reveals other.token_hash",
        "token_hash reveals session.missing",
        "token_hash reveals session.token_hash reveals session.token_hash",
    ] {
        let diagnostics = resolve(&query(selection)).expect_err(selection);
        assert_eq!(
            diagnostics.as_slice()[0].code(),
            QueryDiagnosticCode::SecretOutputDeclaration,
            "{selection}"
        );
    }
}

#[test]
fn declarations_on_records_and_aggregate_results_fail_at_the_declaration() {
    let record_source = query("token_hash").replace(
        "session: session { token_hash }",
        "session: session reveals session.token_hash { token_hash }",
    );
    let diagnostics = resolve(&record_source).expect_err("whole-record declaration");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        QueryDiagnosticCode::SecretOutputDeclaration
    );

    let aggregate = r#"query CountSessions($org: Session.org_id) {
    many sessions from Session where org_id == $org order by session_id asc take 10
    aggregate counts from sessions { count() as total }
    return Found { counts: counts { total reveals counts.total } }
    outcomes Found
}"#;
    let diagnostics = resolve(aggregate).expect_err("aggregate declaration");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        QueryDiagnosticCode::SecretOutputDeclaration
    );
}

#[test]
fn unannotated_ordinary_query_retains_v1_identity() {
    let resolved = resolve(&query("expires_at")).expect("ordinary projection resolves");
    assert_eq!(resolved.ir_version(), riffdb_query_ir::QUERY_IR_VERSION_V1);
    assert!(resolved.secret_outputs().is_empty());
}

#[test]
fn ad_hoc_source_cannot_manufacture_secret_output_intent() {
    let bundle = compile_contract_source(CONTRACT).expect("compile contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let mut document =
        parse_query(&query("token_hash reveals session.token_hash")).expect("parse named source");
    document.name = None;
    let diagnostics = resolve_query_surface(&document, &catalog)
        .expect_err("ad-hoc compilation cannot carry secret output intent");
    assert_eq!(
        diagnostics.as_slice()[0].code(),
        QueryDiagnosticCode::SecretOutputDeclaration
    );
}

#[test]
fn secret_output_requirement_limit_fails_at_the_excess_declaration() {
    let records = (0..513)
        .map(|index| {
            format!(
                "record_{index}: session {{ first_{index}: token_hash reveals session.token_hash second_{index}: token_hash reveals session.token_hash }}"
            )
        })
        .collect::<Vec<_>>()
        .join("\n        ");
    let source = format!(
        r#"query ExcessiveSecrets($org: Session.org_id, $id: Session.session_id) {{
    one session from Session where org_id == $org && session_id == $id else NotFound
    return Found {{
        {records}
    }}
    outcomes Found | NotFound
}}"#
    );

    let diagnostics = resolve(&source).expect_err("the 1,025th secret output must be rejected");
    let diagnostic = &diagnostics.as_slice()[0];
    assert_eq!(diagnostic.code(), QueryDiagnosticCode::ArtifactLimit);
    assert_eq!(
        diagnostic.summary(),
        "secret output requirement limit exceeded"
    );
    let span = diagnostic.primary();
    assert_eq!(
        &source[span.start as usize..span.end as usize],
        "session.token_hash"
    );
}
