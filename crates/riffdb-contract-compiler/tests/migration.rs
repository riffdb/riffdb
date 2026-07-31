//! Exact-parent migration compiler contract tests.

use riffdb_contract_compiler::{
    CompilationError, CompilerDiagnosticCode, compile_contract_source, compile_contract_successor,
    compile_migration_source,
};
use riffdb_contract_ir::{CompatibilityClass, MigrationBundleV1, MigrationStepKindV1};

const GENESIS: &str = r#"
contract Evolution version 1 {
  entity Row {
    key (id: uuid)
    field value: i64
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
}
"#;

const SUCCESSOR: &str = r#"
contract Evolution version 2 {
  entity Row {
    key (id: uuid)
    field value: i64
    field doubled: i64
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
}
"#;

fn contracts() -> (
    riffdb_contract_ir::ContractBundle,
    riffdb_contract_ir::ContractBundle,
) {
    let parent = compile_contract_source(GENESIS).expect("genesis compiles");
    let candidate = compile_contract_successor(SUCCESSOR, &parent).expect("successor compiles");
    assert_eq!(
        candidate.compatibility().overall(),
        CompatibilityClass::RequiresMigration
    );
    (parent, candidate)
}

fn first_code(error: &CompilationError) -> CompilerDiagnosticCode {
    error.semantic().expect("semantic diagnostic").as_slice()[0].code()
}

fn has_code(error: &CompilationError, code: CompilerDiagnosticCode) -> bool {
    error
        .semantic()
        .expect("semantic diagnostic")
        .as_slice()
        .iter()
        .any(|diagnostic| diagnostic.code() == code)
}

#[test]
fn exact_parent_set_proof_compiles_to_canonical_round_trippable_bundle() {
    let (parent, candidate) = contracts();
    let source = r#"
migration Evolution from 1 to 2 {
  transform Row {
    set doubled = old.value + 1
  }
}
"#;
    let bundle = compile_migration_source(source, &parent, &candidate).expect("migration compiles");
    assert_eq!(bundle.parent_bundle_hash(), parent.bundle_hash());
    assert_eq!(bundle.candidate_bundle_hash(), candidate.bundle_hash());
    assert_eq!(bundle.steps().len(), 1);
    assert!(matches!(
        bundle.steps()[0].kind(),
        MigrationStepKindV1::SetField { .. }
    ));
    assert_eq!(
        MigrationBundleV1::decode(bundle.canonical_bytes()).expect("strict round trip"),
        bundle
    );
}

#[test]
fn missing_duplicate_and_unnecessary_proofs_fail_closed() {
    let (parent, candidate) = contracts();
    let missing = "migration Evolution from 1 to 2 {}";
    assert_eq!(
        first_code(&compile_migration_source(missing, &parent, &candidate).unwrap_err()),
        CompilerDiagnosticCode::MissingMigrationProof
    );

    let duplicate = r#"
migration Evolution from 1 to 2 {
  transform Row {
    set doubled = old.value
    set doubled = 1
  }
}
"#;
    assert_eq!(
        first_code(&compile_migration_source(duplicate, &parent, &candidate).unwrap_err()),
        CompilerDiagnosticCode::DuplicateMigrationProof
    );

    let unnecessary = r#"
migration Evolution from 1 to 2 {
  transform Row { set value = old.value }
}
"#;
    assert!(has_code(
        &compile_migration_source(unnecessary, &parent, &candidate).unwrap_err(),
        CompilerDiagnosticCode::UnnecessaryMigrationProof
    ));
}

#[test]
fn wrong_identity_and_unimplemented_steps_have_stable_diagnostics() {
    let (parent, candidate) = contracts();
    let wrong_parent = "migration Evolution from 9 to 2 {}";
    assert_eq!(
        first_code(&compile_migration_source(wrong_parent, &parent, &candidate).unwrap_err()),
        CompilerDiagnosticCode::InvalidMigrationIdentity
    );

    let rename = r#"
migration Evolution from 1 to 2 {
  rename field Row.value to amount
}
"#;
    assert!(has_code(
        &compile_migration_source(rename, &parent, &candidate).unwrap_err(),
        CompilerDiagnosticCode::UnsupportedMigrationStep
    ));
}

#[test]
fn older_supported_parent_targets_the_one_canonical_successor_bundle() {
    let v1 = compile_contract_source(GENESIS).expect("v1");
    let v2_source = GENESIS.replace("version 1", "version 2");
    let v2 = compile_contract_successor(&v2_source, &v1).expect("v2");
    let v3_source = SUCCESSOR.replace("version 2", "version 3");
    let v3 = compile_contract_successor(&v3_source, &v2).expect("canonical v3");
    let from_v1 = r#"
migration Evolution from 1 to 3 {
  transform Row { set doubled = old.value + 1 }
}
"#;
    let from_v2 = r#"
migration Evolution from 2 to 3 {
  transform Row { set doubled = old.value + 1 }
}
"#;
    let first = compile_migration_source(from_v1, &v1, &v3).expect("v1 to v3");
    let second = compile_migration_source(from_v2, &v2, &v3).expect("v2 to v3");
    assert_eq!(first.candidate_bundle_hash(), v3.bundle_hash());
    assert_eq!(second.candidate_bundle_hash(), v3.bundle_hash());
    assert_ne!(first.parent_bundle_hash(), second.parent_bundle_hash());
}

#[test]
fn added_index_requires_a_proof_and_derives_an_index_rebuild_step() {
    let parent = compile_contract_source(GENESIS).expect("parent");
    let candidate_source = GENESIS.replace("version 1", "version 2").replace(
        "field value: i64",
        "field value: i64\n    index by_value (value, id)",
    );
    let candidate =
        compile_contract_successor(&candidate_source, &parent).expect("index successor");
    assert_eq!(
        candidate.compatibility().overall(),
        CompatibilityClass::RequiresMigration
    );

    let source = "migration Evolution from 1 to 2 {}";
    let bundle = compile_migration_source(source, &parent, &candidate).expect("derived proof");
    assert_eq!(bundle.steps().len(), 1);
    assert!(matches!(
        bundle.steps()[0].kind(),
        MigrationStepKindV1::RebuildIndex { .. }
    ));
}
