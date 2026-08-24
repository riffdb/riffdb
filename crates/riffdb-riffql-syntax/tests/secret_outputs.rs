//! Explicit secret-output syntax and contextual-keyword compatibility.

use riffdb_riffql_syntax::{
    RIFFQL_LANGUAGE_VERSION, RIFFQL_LANGUAGE_VERSION_SECRET_OUTPUT_V1, format_query, parse_query,
};

#[test]
fn exact_secret_output_declaration_selects_language_v3_and_round_trips() {
    let source = r#"query Lookup($id: Session.session_id) {
    one session from Session where session_id == $id else NotFound
    return Found { session: session { token_digest reveals session.token_digest } }
    outcomes Found | NotFound
}"#;

    let document = parse_query(source).expect("exact secret output parses");
    assert_eq!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_SECRET_OUTPUT_V1
    );
    let declaration = document.body.selection.fields[0]
        .nested
        .as_ref()
        .expect("nested record")
        .fields[0]
        .reveals
        .first()
        .expect("secret declaration");
    assert_eq!(declaration.value.0.len(), 2);
    assert_eq!(declaration.value.0[0].value.as_str(), "session");
    assert_eq!(declaration.value.0[1].value.as_str(), "token_digest");

    let canonical = format_query(&document);
    assert!(canonical.contains("token_digest reveals session.token_digest"));
    let reparsed = parse_query(&canonical).expect("canonical secret output reparses");
    assert_eq!(format_query(&reparsed), canonical);
}

#[test]
fn reveals_remains_a_legal_identifier_without_selecting_v3() {
    let source = r#"query Contextual($id: Session.session_id) {
    one reveals from Session where session_id == $id else NotFound
    return Found { reveals: reveals { reveals } }
    outcomes Found | NotFound
}"#;

    let document = parse_query(source).expect("reveals remains contextual");
    assert_eq!(document.language_version, RIFFQL_LANGUAGE_VERSION);
    assert_eq!(document.body.bindings[0].name.value.as_str(), "reveals");
    assert!(document.body.selection.fields[0].reveals.is_empty());
}

#[test]
fn formatter_delimits_a_bare_reveals_field_before_a_dotted_field() {
    let source = r#"query Contextual($id: Session.session_id) {
    one session from Session where session_id == $id else NotFound
    return Found {
        token_digest reveals session.token_digest
        reveals,
        session.user_id
    }
    outcomes Found | NotFound
}"#;

    let document = parse_query(source).expect("contextual identifier parses with delimiter");
    assert_eq!(document.body.selection.fields.len(), 3);

    let canonical = format_query(&document);
    assert!(canonical.contains("        reveals,\n        session.user_id"));
    let reparsed = parse_query(&canonical).expect("canonical contextual identifier reparses");
    assert_eq!(format_query(&reparsed), canonical);
    assert_eq!(reparsed.language_version, document.language_version);
    assert_eq!(reparsed.body.selection.fields.len(), 3);
    assert_eq!(
        reparsed.body.selection.fields[1].source.value.0[0]
            .value
            .as_str(),
        "reveals"
    );
}
