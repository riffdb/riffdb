//! Exact-parent migration compiler contract tests.

use riffdb_contract_compiler::{
    CompilationError, CompilerDiagnosticCode, compile_contract_migration_successor,
    compile_contract_source, compile_contract_successor, compile_migration_source,
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
        CompilerDiagnosticCode::MissingMigrationProof
    ));
}

#[test]
fn gate_c_rekey_requires_exact_key_and_locality_proofs() {
    let parent = compile_contract_source(GENESIS).expect("parent");
    let successor = r#"
contract Evolution version 2 {
  entity Row {
    key (scope: string<32>, id: uuid)
    field value: i64
  }
  aggregate Rows { root Row partition_by scope conflict_key (scope, id) }
}
"#;
    let candidate = compile_contract_successor(successor, &parent).expect("candidate");
    let migration = r#"
migration Evolution from 1 to 2 {
  transform Row {
    set scope = "default"
    rekey ("default", old.id)
  }
  acknowledge repartition Rows
  acknowledge conflict Rows
}
"#;
    let bundle = compile_migration_source(migration, &parent, &candidate).expect("Gate C");
    assert!(bundle.steps().iter().any(|step| matches!(
        step.kind(),
        MigrationStepKindV1::RekeyEntity { components, .. } if components.len() == 2
    )));
    assert!(bundle.steps().iter().any(|step| matches!(
        step.kind(),
        MigrationStepKindV1::AcknowledgeRepartition { .. }
    )));
    assert!(
        bundle
            .steps()
            .iter()
            .any(|step| matches!(step.kind(), MigrationStepKindV1::AcknowledgeConflict { .. }))
    );

    let missing_acknowledgement = migration.replace("  acknowledge conflict Rows\n", "");
    assert!(has_code(
        &compile_migration_source(&missing_acknowledgement, &parent, &candidate).unwrap_err(),
        CompilerDiagnosticCode::MissingMigrationProof
    ));
}

#[test]
fn gate_c_rejects_an_unrelated_existing_command_plan_change() {
    let parent_source = r#"
contract CoupledPlans version 1 {
  entity Rekeyed { key (id: uuid) }
  entity Unchanged { key (id: uuid) field value: i64 }
  aggregate RekeyedRows { root Rekeyed partition_by id conflict_key (id) }
  aggregate UnchangedRows { root Unchanged partition_by id conflict_key (id) }
  command ChangeUnchanged {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    mutate Unchanged(id) as row else Missing { id: id }
    set row.value = 1
    return Changed { row: row }
  }
}
"#;
    let candidate_source = parent_source
        .replace("version 1", "version 2")
        .replacen("key (id: uuid) }", "key (scope: string<16>, id: uuid) }", 1)
        .replacen(
            "partition_by id conflict_key (id) }",
            "partition_by scope conflict_key (scope, id) }",
            1,
        )
        .replace("set row.value = 1", "set row.value = 2");
    let parent = compile_contract_source(parent_source).expect("parent");
    let candidate =
        compile_contract_successor(&candidate_source, &parent).expect("candidate successor");
    let migration = r#"
migration CoupledPlans from 1 to 2 {
  transform Rekeyed {
    set scope = "default"
    rekey ("default", old.id)
  }
  acknowledge repartition RekeyedRows
  acknowledge conflict RekeyedRows
}
"#;

    assert_eq!(
        first_code(&compile_migration_source(migration, &parent, &candidate).unwrap_err()),
        CompilerDiagnosticCode::UnsupportedMigrationStep
    );
}

#[test]
fn gate_c_accepts_a_command_plan_coupled_to_the_rekeyed_entity() {
    let parent_source = r#"
contract CoupledCommand version 1 {
  entity Row { key (id: uuid) field scope: string<16> field value: i64 }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input idempotency_key: string<128>
    input id: uuid
    input scope: string<16>
    idempotency_key idempotency_key
    mutate Row(id) as row else Missing { id: id }
    set row.value = 1
    return Changed { row: row }
  }
}
"#;
    let candidate_source = parent_source
        .replace("version 1", "version 2")
        .replace(
            "key (id: uuid) field scope: string<16>",
            "key (scope: string<16>, id: uuid)",
        )
        .replace(
            "partition_by id conflict_key (id)",
            "partition_by scope conflict_key (scope, id)",
        )
        .replace("mutate Row(id)", "mutate Row(scope, id)");
    let parent = compile_contract_source(parent_source).expect("parent");
    let candidate =
        compile_contract_successor(&candidate_source, &parent).expect("candidate successor");
    let migration = r#"
migration CoupledCommand from 1 to 2 {
  transform Row { rekey (old.scope, old.id) }
  acknowledge repartition Rows
  acknowledge conflict Rows
}
"#;

    compile_migration_source(migration, &parent, &candidate)
        .expect("the successor command is coupled to the exact Gate C change");
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
fn migration_aware_successor_preserves_renamed_field_id_and_emits_alias() {
    let parent = compile_contract_source(GENESIS).expect("parent");
    let successor = GENESIS
        .replace("version 1", "version 2")
        .replace("field value: i64", "field amount: i64");
    let source = r#"
migration Evolution from 1 to 2 {
  rename field Row.value to amount
}
"#;
    let (candidate, migration) =
        compile_contract_migration_successor(&successor, source, &parent).expect("rename");
    let old = parent.schema().entities()[0].record().fields()[1].id();
    let renamed = candidate.schema().entities()[0].record().fields()[1].id();

    assert_eq!(old, renamed);
    assert_eq!(candidate.ledger().version(), 2);
    assert_eq!(candidate.ledger().aliases().len(), 1);
    assert_eq!(
        candidate.compatibility().overall(),
        CompatibilityClass::RequiresMigration
    );
    assert!(
        candidate
            .compatibility()
            .entries()
            .iter()
            .any(|entry| entry.code().as_str() == "RDB-K036")
    );
    assert!(matches!(
        migration.steps()[0].kind(),
        MigrationStepKindV1::RenameIdentity { stable_id, new_name, .. }
            if *stable_id == old.get() && new_name == "amount"
    ));
    assert_eq!(
        riffdb_contract_ir::ContractBundle::decode(candidate.canonical_bytes())
            .expect("bundle round trip")
            .canonical_bytes(),
        candidate.canonical_bytes()
    );
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

#[test]
fn replacement_retirement_and_exhaustive_enum_map_compile() {
    let parent = compile_contract_source(GENESIS).expect("parent");
    let replacement_source = GENESIS
        .replace("version 1", "version 2")
        .replace("field value: i64", "field amount: u64");
    let replacement =
        compile_contract_successor(&replacement_source, &parent).expect("replacement successor");
    let replacement_migration = r#"
migration Evolution from 1 to 2 {
  transform Row {
    replace value with amount using checked_i64_to_u64
  }
}
"#;
    let replacement = compile_migration_source(replacement_migration, &parent, &replacement)
        .expect("checked replacement");
    assert!(matches!(
        replacement.steps()[0].kind(),
        MigrationStepKindV1::ReplaceField { .. }
    ));

    let retired_source = "contract Evolution version 2 {}";
    let retired =
        compile_contract_successor(retired_source, &parent).expect("retirement successor");
    let retirement = compile_migration_source(
        "migration Evolution from 1 to 2 { retire entity Row }",
        &parent,
        &retired,
    )
    .expect("logical retirement");
    assert!(matches!(
        retirement.steps()[0].kind(),
        MigrationStepKindV1::RetireIdentity { .. }
    ));

    let enum_parent_source = r#"
contract EnumEvolution version 1 {
  enum WorkflowStatus { Open, Closed }
  entity Row {
    key (id: uuid)
    field status: WorkflowStatus
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
}
"#;
    let enum_successor_source = enum_parent_source
        .replace("version 1", "version 2")
        .replace("Open, Closed", "Open, Archived");
    let enum_parent = compile_contract_source(enum_parent_source).expect("enum parent");
    let enum_successor =
        compile_contract_successor(&enum_successor_source, &enum_parent).expect("enum successor");
    let enum_migration = r#"
migration EnumEvolution from 1 to 2 {
  map enum WorkflowStatus {
    Open -> Open
    Closed -> Archived
  }
}
"#;
    let mapped = compile_migration_source(enum_migration, &enum_parent, &enum_successor)
        .expect("exhaustive map");
    assert!(matches!(
        mapped.steps()[0].kind(),
        MigrationStepKindV1::MapEnum { mappings, .. } if mappings.len() == 2
    ));

    let incomplete = r#"
migration EnumEvolution from 1 to 2 {
  map enum WorkflowStatus { Open -> Open }
}
"#;
    assert_eq!(
        first_code(
            &compile_migration_source(incomplete, &enum_parent, &enum_successor).unwrap_err()
        ),
        CompilerDiagnosticCode::MissingMigrationProof
    );
}

#[test]
fn scoped_field_renames_with_equal_numeric_ids_remain_distinct() {
    let parent_source = r#"
contract ScopedRenames version 1 {
  entity Left { key (id: uuid) field value: i64 }
  entity Right { key (id: uuid) field value: i64 }
  aggregate Lefts { root Left partition_by id conflict_key (id) }
  aggregate Rights { root Right partition_by id conflict_key (id) }
}
"#;
    let successor_source = parent_source
        .replace("version 1", "version 2")
        .replace("field value: i64", "field amount: i64");
    let migration_source = r#"
migration ScopedRenames from 1 to 2 {
  rename field Left.value to amount
  rename field Right.value to amount
}
"#;
    let parent = compile_contract_source(parent_source).expect("parent");
    let (candidate, migration) =
        compile_contract_migration_successor(&successor_source, migration_source, &parent)
            .expect("scoped renames");
    let renames = migration
        .steps()
        .iter()
        .filter_map(|step| match step.kind() {
            MigrationStepKindV1::RenameIdentity {
                owner_ids,
                stable_id,
                ..
            } => Some((owner_ids.clone(), *stable_id)),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(renames.len(), 2);
    assert_ne!(renames[0].0, renames[1].0);
    assert_eq!(renames[0].1, renames[1].1);
    assert_eq!(candidate.ledger().aliases().len(), 2);
    assert_eq!(
        MigrationBundleV1::decode(migration.canonical_bytes()).expect("scoped round trip"),
        migration
    );
}

#[test]
fn logical_retirement_closes_owned_identities_and_rebuilds_replacement_projection() {
    let parent_source = r#"
contract RetiredSurface version 1 {
  entity Row { key (id: uuid) }
  event Audit { id: uuid }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Observe {
    input id: uuid
    read Row(id) as row else Missing {}
    return Found {}
  }
  projection OldAudit {
    source event Audit
    key (id)
    measure seen = count()
    frontier transactionally_ordered
  }
}
"#;
    let successor_source = r#"
contract RetiredSurface version 2 {
  entity Row { key (id: uuid) }
  event Audit { id: uuid }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  projection CurrentAudit {
    source event Audit
    key (id)
    measure seen = count()
    frontier transactionally_ordered
  }
}
"#;
    let parent = compile_contract_source(parent_source).expect("parent");
    let candidate =
        compile_contract_successor(successor_source, &parent).expect("retired successor");
    let migration = compile_migration_source(
        r#"
migration RetiredSurface from 1 to 2 {
  retire command Observe
  retire projection OldAudit
}
"#,
        &parent,
        &candidate,
    )
    .expect("logical retirement and replacement projection");

    assert!(migration.steps().iter().any(|step| matches!(
        step.kind(),
        MigrationStepKindV1::RetireIdentity { namespace, .. }
            if *namespace == riffdb_contract_ir::StableIdNamespaceTag::Command
    )));
    assert!(migration.steps().iter().any(|step| matches!(
        step.kind(),
        MigrationStepKindV1::RetireIdentity { namespace, .. }
            if *namespace == riffdb_contract_ir::StableIdNamespaceTag::Projection
    )));
    assert!(
        migration
            .steps()
            .iter()
            .any(|step| matches!(step.kind(), MigrationStepKindV1::RebuildProjection { .. }))
    );
}
