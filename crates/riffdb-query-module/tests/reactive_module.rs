#![forbid(unsafe_code)]

//! Exact reactive-module compiler acceptance tests.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::ReactiveCompileDiagnosticCode;
use riffdb_query_ir::ReactiveOperationPlanV1;
use riffdb_query_module::{
    ReactiveModuleCompilationError, compile_reactive_source, decode_and_validate_reactive_module,
};

const CONTRACT: &str = r#"
contract ReactiveRows version 1 {
  entity Row { key (organization_id: uuid, row_id: uuid) field value: i64 }
  event RowChanged {
    partition_by (organization_id)
    organization_id: uuid
    row_id: uuid
    value: i64
  }
  aggregate Rows { root Row partition_by organization_id conflict_key (organization_id, row_id) }
  command ChangeRow {
    input idempotency_key: string<128>
    input organization_id: uuid
    input row_id: uuid
    idempotency_key idempotency_key
    mutate Row(organization_id, row_id) as row else Missing {}
    set row.value = 1
    emit RowChanged { organization_id: organization_id, row_id: row_id, value: 1 }
    return Changed {}
  }
}
"#;

const SOURCE: &str =
    include_str!("../../../fixtures/reactive/module/reactive-v1/row_activity.riffr");

#[test]
fn exact_stream_module_is_reproducible_and_partition_local() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let first = compile_reactive_source(SOURCE, &contract, &[]).expect("reactive module");
    let second = compile_reactive_source(SOURCE, &contract, &[]).expect("reactive module");
    assert_eq!(first.identity(), second.identity());
    assert_eq!(first.canonical_bytes(), second.canonical_bytes());
    let operation = first.operation("RowChanges").expect("stream");
    assert!(matches!(
        operation.plan(),
        ReactiveOperationPlanV1::Stream { .. }
    ));
    assert_eq!(
        decode_and_validate_reactive_module(first.canonical_bytes(), SOURCE, &contract, &[])
            .expect("strict decode")
            .identity(),
        first.identity()
    );
    let mut corrupt = first.canonical_bytes().to_vec();
    *corrupt.last_mut().expect("artifact byte") ^= 1;
    assert_eq!(
        decode_and_validate_reactive_module(&corrupt, SOURCE, &contract, &[]),
        Err(ReactiveModuleCompilationError::IdentityMismatch)
    );
}

#[test]
fn cross_partition_and_hidden_predicate_fields_fail_before_deployment() {
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let cross_partition = SOURCE.replace(
        "partition (organization_id = $organization_id)",
        "partition (row_id = $organization_id)",
    );
    let error =
        compile_reactive_source(&cross_partition, &contract, &[]).expect_err("wrong partition");
    assert!(
        matches!(error, ReactiveModuleCompilationError::Semantic(ref values)
        if values[0].code() == ReactiveCompileDiagnosticCode::CrossPartition)
    );

    let hidden = SOURCE.replace(
        "event RowChanged select (organization_id, row_id, value);",
        "event RowChanged select (organization_id, row_id);\n    where event.value > 0;",
    );
    let error = compile_reactive_source(&hidden, &contract, &[])
        .expect_err("predicate field must be selected");
    assert!(
        matches!(error, ReactiveModuleCompilationError::Semantic(ref values)
        if values[0].code() == ReactiveCompileDiagnosticCode::TypeMismatch)
    );
}
