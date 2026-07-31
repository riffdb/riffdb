//! Compiler proof that streamable events carry the exact command partition.

use riffdb_contract_compiler::{
    CompilerDiagnosticCode, compile_contract_source, compile_contract_successor,
    validate_contract_source,
};
use riffdb_contract_ir::{CompatibilityCode, ContractBundle, KeyPurpose};

fn source(event_partition: &str, emitted_partition: &str) -> String {
    format!(
        r#"contract EventPartitionProof version 1 {{
  entity Row {{
    key (organization_id: uuid, row_id: uuid)
    field value: i64
  }}

  event RowChanged {{
    partition_by ({event_partition})
    organization_id: uuid
    row_id: uuid
  }}

  aggregate Rows {{
    root Row
    partition_by organization_id
    conflict_key (organization_id, row_id)
  }}

  command ChangeRow {{
    input request_id: string<128>
    input organization_id: uuid
    input row_id: uuid
    idempotency_key request_id
    mutate Row(organization_id, row_id) as row else Missing {{ row_id: row_id }}
    set row.value = 1
    emit RowChanged {{ organization_id: {emitted_partition}, row_id: row_id }}
    return Changed {{ row: row }}
  }}
}}
"#
    )
}

fn assert_diagnostic(source: &str, code: CompilerDiagnosticCode, selected_source: &str) {
    let error = validate_contract_source(source).expect_err("contract must reject");
    let diagnostic = error
        .semantic()
        .expect("semantic diagnostic")
        .as_slice()
        .iter()
        .find(|diagnostic| diagnostic.code() == code)
        .unwrap_or_else(|| panic!("missing {code:?}: {error}"));
    let selected_start = source.rfind(selected_source).expect("selected source");
    assert_eq!(diagnostic.primary_span().start() as usize, selected_start);
    assert_eq!(
        diagnostic.primary_span().end() as usize,
        selected_start + selected_source.len()
    );
}

#[test]
fn exact_emit_expression_and_aggregate_key_schema_compile() {
    let bundle = compile_contract_source(&source("organization_id", "organization_id"))
        .expect("exact partition compiles");
    let event = bundle.schema().events().first().expect("event");
    let partition = event.partition().expect("streamable partition");
    assert_eq!(partition.fields().len(), 1);
    assert_eq!(
        partition.fields()[0],
        event
            .payload()
            .fields()
            .iter()
            .find(|field| field.name() == "organization_id")
            .expect("partition field")
            .id()
    );
    assert!(matches!(
        partition.key_schema().purpose(),
        KeyPurpose::Partition(_)
    ));
    assert_eq!(
        partition.key_schema(),
        bundle.schema().aggregates()[0].keys().partition_schema()
    );
    let decoded = ContractBundle::decode(bundle.canonical_bytes())
        .expect("canonical partitioned bundle decodes");
    assert_eq!(
        decoded.schema().events()[0].partition(),
        Some(partition),
        "the durable bundle must retain the exact partition proof"
    );
}

#[test]
fn different_emit_expression_fails_on_the_emitted_event_symbol() {
    let source = source("organization_id", "row_id");
    assert_diagnostic(
        &source,
        CompilerDiagnosticCode::CrossPartitionMutation,
        "RowChanged",
    );
}

#[test]
fn unsupported_partition_tuple_fails_on_the_partition_clause() {
    let source = source("organization_id, row_id", "organization_id");
    assert_diagnostic(
        &source,
        CompilerDiagnosticCode::InvalidEvent,
        "partition_by (organization_id, row_id)",
    );
}

#[test]
fn unpartitioned_events_remain_valid_but_are_not_application_streamable() {
    let source = source("organization_id", "organization_id")
        .replace("    partition_by (organization_id)\n", "");
    let bundle = compile_contract_source(&source).expect("legacy event compiles");
    assert!(bundle.schema().events()[0].partition().is_none());
}

#[test]
fn adding_a_proved_partition_is_compatible_but_removal_is_not() {
    let genesis_source = source("organization_id", "organization_id")
        .replace("    partition_by (organization_id)\n", "");
    let genesis = compile_contract_source(&genesis_source).expect("genesis");
    let successor_source =
        source("organization_id", "organization_id").replace("version 1", "version 2");
    let successor = compile_contract_successor(&successor_source, &genesis)
        .expect("proved partition addition is compatible");
    assert!(successor.schema().events()[0].partition().is_some());

    let removal_source = genesis_source.replace("version 1", "version 3");
    let removal = compile_contract_successor(&removal_source, &successor)
        .expect("incompatible candidate still compiles for an exact report");
    assert!(
        removal
            .compatibility()
            .entries()
            .iter()
            .any(|entry| entry.code() == CompatibilityCode::EventChange),
        "a published application partition cannot disappear"
    );
}
