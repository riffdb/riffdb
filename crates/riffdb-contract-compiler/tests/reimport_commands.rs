#![forbid(unsafe_code)]

//! Compiler-owned reimport command boundary (ADR-0119, WP-599).

use riffdb_contract_compiler::CompilerDiagnosticCode;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{BUNDLE_FORMAT_VERSION_V10, ContractBundle, Instruction};

const REIMPORT_CONTRACT: &str = include_str!("../../../fixtures/compiler/reimport/contract.riff");

#[test]
fn lowers_exact_workflow_records_to_a_distinct_create_only_command_class() {
    let bundle = compile_contract_source(REIMPORT_CONTRACT)
        .expect("exact workflow reconstitution must compile");
    let command = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "ReconstituteSessions")
        .expect("reimport plan");

    assert!(command.is_reimport());
    assert_eq!(bundle.format_version(), BUNDLE_FORMAT_VERSION_V10);
    assert!(
        bundle.mcp_command_names().entries().is_empty(),
        "reimport commands must not enter application MCP discovery"
    );
    assert!(command.idempotency_input().is_none());
    assert!(command.bindings().iter().all(|binding| {
        binding.mode() == riffdb_contract_ir::BindingMode::Create
            && binding.entity_type() == bundle.schema().entities()[0].id()
    }));
    assert!(command.collection_expansion().is_some());
    let set_count = command
        .instructions()
        .iter()
        .filter(|instruction| matches!(instruction, Instruction::SetField { .. }))
        .count();
    assert_eq!(set_count, 6, "every non-key field is compiler-constructed");

    let decoded = ContractBundle::decode(bundle.canonical_bytes()).expect("v10 round trip");
    assert_eq!(decoded, bundle);

    let pinned: &[u8] = include_bytes!("../../../fixtures/compiler/reimport/bundle.bin");
    let pinned_hash =
        include_str!("../../../fixtures/compiler/reimport/bundle-hash.txt").trim_end();
    assert_eq!(pinned, bundle.canonical_bytes());
    assert_eq!(
        pinned_hash,
        bundle
            .bundle_hash()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
}

#[test]
fn rejects_a_reimport_source_whose_exact_record_type_does_not_match() {
    let source = REIMPORT_CONTRACT.replace(
        "reconstitute Session from records",
        "reconstitute Other from records",
    );
    let diagnostics = compile_contract_source(&source).expect_err("mismatch must fail");
    let semantic = diagnostics.semantic().expect("semantic diagnostic");
    let diagnostic = semantic.as_slice().first().expect("one diagnostic");
    assert_eq!(diagnostic.code(), CompilerDiagnosticCode::InvalidType);
    assert_eq!(
        diagnostic.primary_span().start() as usize,
        source.find("Other from records").expect("record type")
    );
}

#[test]
fn derives_exact_parent_reads_for_reimported_relationships() {
    let source = r#"
contract PortableChildren version 1 {
  entity Parent {
    key (tenant_id: uuid, parent_id: uuid)
  }
  entity Child {
    key (tenant_id: uuid, parent_id: uuid, child_id: uuid)
    field label: string<64>
    reference parent (tenant_id, parent_id) -> Parent(tenant_id, parent_id)
  }
  aggregate Families {
    root Parent
    child Child
    partition_by tenant_id
    conflict_key (tenant_id, parent_id)
  }
  reimport command ReconstituteChildren {
    input records: list<Child, 1..64>
    reconstitute Child from records else DependencyUnavailable {}
    return ChildrenReconstituted {}
  }
}
"#;
    let bundle = compile_contract_source(source).expect("relationship reimport compiles");
    let command = &bundle.commands()[0];
    assert_eq!(command.bindings().len(), 2);
    assert_eq!(
        command.bindings()[0].mode(),
        riffdb_contract_ir::BindingMode::Read
    );
    assert_eq!(
        command.bindings()[1].mode(),
        riffdb_contract_ir::BindingMode::Create
    );
    assert_eq!(command.relationship_checks().len(), 1);
    assert_eq!(
        command.relationship_checks()[0].relationship_name(),
        "parent"
    );
}
