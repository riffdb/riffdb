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
