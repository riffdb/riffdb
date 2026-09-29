//! An indexed pool supplies a command's selected target, not its conflict ownership.
// req: CMP-015, CMP-013
use riffdb_contract_compiler::compile_contract_source;

const SOURCE: &str = include_str!("../../../fixtures/compiler/indexed-selection/contract.riff");

#[test]
fn an_indexed_pool_allocation_compiles_without_a_caller_selected_slot() {
    let bundle = compile_contract_source(SOURCE)
        .expect("the compiler must admit one bounded indexed selection under known pool ownership");
    assert_eq!(bundle.commands().len(), 1);
    assert_eq!(bundle.commands()[0].name(), "Allocate");
    assert_eq!(bundle.commands()[0].input().record().fields().len(), 3);
    assert!(bundle.commands()[0].write_proofs().is_some());
}

#[test]
fn explicit_slot_selection_is_the_existing_checked_control() {
    let source = SOURCE
        .replace(
            "input holder: uuid",
            "input holder: uuid\n    input slot_id: uuid",
        )
        .replace(
            "mutate first Slot.by_available(pool_id, true)",
            "mutate Slot(pool_id, slot_id)",
        );
    let bundle = compile_contract_source(&source).expect("current exact-key command");
    assert_eq!(bundle.ir_version(), 25);
    assert_eq!(bundle.commands()[0].input().record().fields().len(), 4);
}

#[test]
fn selector_round_trip_and_explain_preserve_exact_semantics() {
    let bundle = compile_contract_source(SOURCE).expect("selection");
    assert_eq!(bundle.ir_version(), 26);
    assert_eq!(
        riffdb_contract_ir::ContractBundle::decode(bundle.canonical_bytes()).expect("decode"),
        bundle
    );
    let selection = bundle.commands()[0]
        .indexed_selection()
        .expect("selector")
        .1;
    assert_eq!(selection.prefix_expressions().len(), 2);
    let changed = compile_contract_source(&SOURCE.replace(
        "by_available(pool_id, true)",
        "by_available(pool_id, false)",
    ))
    .expect("other membership");
    assert_ne!(
        bundle.commands()[0].plan_hash(),
        changed.commands()[0].plan_hash()
    );
    let text = riffdb_contract_ir::CommandExplain::from_plan(&bundle.commands()[0]).render_text();
    for expected in [
        "selection:",
        "ascending-complete-index-key",
        "candidate-bound=1",
        "partition-index-generation",
        "evaluation-attempts=3",
    ] {
        assert!(text.contains(expected), "missing explain detail {expected}");
    }
}

#[test]
fn invalid_selection_has_a_safe_exact_source_span() {
    use riffdb_contract_compiler::CompilerDiagnosticCode;
    for (source, token) in [
        (
            SOURCE.replace(
                "Slot.by_available(pool_id, true)",
                "Slot.unknown_index(pool_id, true)",
            ),
            "unknown_index",
        ),
        (
            SOURCE.replace(
                "Slot.by_available(pool_id, true)",
                "Slot.by_available(pool_id, true, holder, holder)",
            ),
            "by_available",
        ),
        (
            SOURCE.replace("conflict_key (pool_id)", "conflict_key (pool_id, slot_id)"),
            "Slot",
        ),
        (
            SOURCE.replace("partition_by pool_id", "partition_by slot_id"),
            "Slot",
        ),
        (
            SOURCE.replace(
                "by_available(pool_id, true)",
                "by_available(pool_id, slot.available)",
            ),
            "Slot",
        ),
    ] {
        let error = compile_contract_source(&source).expect_err("unproved selection");
        let diagnostic = error
            .semantic()
            .unwrap_or_else(|| panic!("{error:?}"))
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidIndexedSelection)
            .unwrap_or_else(|| panic!("{error:?}"));
        assert_eq!(
            &source[diagnostic.primary_span().start() as usize
                ..diagnostic.primary_span().end() as usize],
            token
        );
        assert_eq!(diagnostic.code().as_str(), "RDB-C051");
        assert!(
            diagnostic
                .code()
                .help()
                .expect("guidance")
                .contains("conflict")
        );
    }
}

#[test]
fn selection_does_not_supply_assignment_evidence_or_reserve_first() {
    let unchecked = SOURCE.replace(" checked_by available", "").replace(
        "    require available: slot.available else NoCapacity {}\n",
        "",
    );
    assert!(compile_contract_source(&unchecked).is_err());
    let exact = SOURCE
        .replace(
            "mutate first Slot.by_available(pool_id, true)",
            "mutate Slot(pool_id, holder)",
        )
        .replace("Slot", "first");
    assert!(
        compile_contract_source(&exact).is_ok(),
        "first remains an ordinary entity identifier"
    );
}

#[test]
fn ir_constructor_rejects_forged_prefix_key_proof() {
    use riffdb_contract_ir::{BindingPlan, CommandPlan, IndexedSelectionPlan};
    let bundle = compile_contract_source(SOURCE).expect("selection");
    let plan = &bundle.commands()[0];
    let original = &plan.bindings()[0];
    let selection = original.indexed_selection().expect("selection");
    let mut prefix = selection.prefix_expressions().to_vec();
    // Substitute the discovered slot key for the fixed partition component.
    prefix[0] = original.key_expressions()[1];
    let forged = BindingPlan::new(
        original.id(),
        original.name(),
        original.mode(),
        original.entity_type(),
        original.key_schema().clone(),
        original.key_expressions().to_vec(),
        original.accessed_fields().to_vec(),
        original.complete_record_access(),
        original.failure().expect("empty result").clone(),
    )
    .expect("binding shape")
    .with_indexed_selection(
        IndexedSelectionPlan::new(selection.index(), prefix).expect("descriptor shape"),
    )
    .expect("attachment");
    let result = CommandPlan::new(
        plan.command_id(),
        bundle.lineage().clone(),
        plan.name(),
        bundle.contract_version(),
        plan.input().clone(),
        plan.outcomes().to_vec(),
        plan.success_outcome(),
        plan.idempotency_input(),
        plan.expressions().clone(),
        vec![forged],
        plan.root_validation_reads().to_vec(),
        plan.locality().clone(),
        plan.commit_checks().to_vec(),
        plan.instructions().to_vec(),
        plan.execution_class(),
        bundle.schema(),
    );
    assert!(
        result.is_err(),
        "unchecked IR cannot turn a selected key into conflict ownership"
    );
}

#[test]
fn selected_child_retains_its_discovered_invariant_root_read() {
    let source = SOURCE
        .replace("entity Slot {", "entity Pool { key (pool_id: uuid, root_id: uuid) field enabled: bool }\n  entity Slot {")
        .replace("key (pool_id: uuid, slot_id: uuid)", "key (pool_id: uuid, root_id: uuid, slot_id: uuid)")
        .replace("root Slot", "root Pool\n    child Slot\n    invariant enabled: enabled");
    let bundle = compile_contract_source(&source).expect("selected child with invariant root");
    let plan = &bundle.commands()[0];
    assert_eq!(plan.root_validation_reads().len(), 1);
    assert_eq!(plan.commit_checks().len(), 1);
    assert!(
        !plan
            .expressions()
            .dependencies(plan.root_validation_reads()[0].key_expressions()[1])
            .expect("dependencies")
            .is_input_computable()
    );
    assert_eq!(
        riffdb_contract_ir::ContractBundle::decode(bundle.canonical_bytes()).expect("round trip"),
        bundle
    );
}

#[test]
fn selection_rejects_aliases_multiple_selectors_and_secret_membership() {
    for source in [
        SOURCE.replace("    require available:", "    read Slot(pool_id, holder) as other else NoCapacity {}\n    require available:"),
        SOURCE.replace("    require available:", "    mutate first Slot.by_available(pool_id, false) as other else NoCapacity {}\n    require available:"),
        SOURCE.replace("field available: bool", "field secret available: bool"),
        SOURCE.replace("by_available(pool_id, true)", "by_available(request, true)"),
    ] { assert!(compile_contract_source(&source).is_err(), "selector proof or secret-flow check must reject"); }
}
