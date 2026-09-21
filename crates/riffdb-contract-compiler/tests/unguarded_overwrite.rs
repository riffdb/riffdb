//! What the command surface does and does not prove about a write's precondition.
//!
//! A design review held that ordinary multi-tenant patterns -- slot allocation,
//! a limit summing rows the command does not name, a rollup -- are inexpressible,
//! so an application must implement them unsafely outside a command. These tests
//! establish what is actually true, because the answer decides whether the gap is
//! expressiveness or something narrower.
//!
//! Two of those patterns are expressible. What is not proven is narrower and
//! sharper: a command may overwrite a field without declaring anything about its
//! prior value, and nothing marks that as last-writer-wins.
// req: POC-003

use riffdb_contract_compiler::{CompilerDiagnosticCode, compile_contract_source};

const CREDIT_LIMIT: &str = r"
contract Billing version 1 {
  entity Customer {
    key (tenant: uuid, customer_id: uuid)
    field credit_limit: i64
    field outstanding: i64
  }
  entity Order {
    key (tenant: uuid, customer_id: uuid, order_id: uuid)
    field amount: i64
  }
  aggregate Customers {
    root Customer
    child Order
    partition_by tenant
    conflict_key (tenant, customer_id)
    invariant within_limit: outstanding <= credit_limit
  }
  command PlaceOrder {
    input request_key: string<128>
    input tenant: uuid
    input customer_id: uuid
    input order_id: uuid
    input amount: i64
    idempotency_key request_key
    mutate Customer(tenant, customer_id) as customer else CustomerMissing {}
    create Order(tenant, customer_id, order_id) as order else OrderExists {}
    set order.amount = amount
    set customer.outstanding = customer.outstanding + amount
    return Placed { order: order }
  }
}
";

fn slot_claim_spelled(guard: &str, spelling: &str) -> String {
    format!(
        r"
contract Booking version 1 {{
  entity Slot {{
    key (tenant: uuid, slot_id: uuid)
    field held_by: uuid
    field free: bool
  }}
  aggregate Slots {{
    root Slot
    partition_by tenant
    conflict_key (tenant, slot_id)
  }}
  command ClaimSlot {{
    input request_key: string<128>
    input tenant: uuid
    input slot_id: uuid
    input holder: uuid
    idempotency_key request_key
    mutate Slot(tenant, slot_id) as slot else SlotMissing {{}}
{guard}    {spelling} slot.free = false
    {spelling} slot.held_by = holder
    return Claimed {{ slot: slot }}
  }}
}}
"
    )
}

const GUARDED: &str = "    require is_free: slot.free else SlotTaken {}\n";

/// The guarded claim: a precondition, and writes that say they have one.
fn slot_claim(guard: &str) -> String {
    slot_claim_spelled(guard, "set")
}

/// The same writes with no precondition, spelled as what they are.
fn declared_overwrite() -> String {
    slot_claim_spelled("", "overwrite")
}

/// A limit over rows the command does not name is expressible: maintain the
/// rollup on the aggregate root in the same command and put the invariant on the
/// root. The conflict key covers the root, so concurrent orders serialize.
// req: POC-003
#[test]
fn a_limit_over_unnamed_rows_is_expressible_as_a_root_rollup() {
    let bundle = compile_contract_source(CREDIT_LIMIT).expect("a maintained rollup compiles");
    let command = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "PlaceOrder")
        .expect("PlaceOrder");
    assert_eq!(
        command.commit_checks().len(),
        1,
        "the root invariant is checked at commit, over the value this command wrote"
    );
}

/// Choosing a target in the application is safe when the command re-checks the
/// condition the choice depended on: the loser gets a declared outcome.
// req: POC-003
#[test]
fn a_claim_that_rechecks_its_precondition_compiles() {
    compile_contract_source(&slot_claim(GUARDED)).expect("a compare-and-set claim compiles");
}

/// The gap this package closed, now pinned inverted.
///
/// Before ADR-0256 the same command with its guard deleted compiled, and the
/// guarded and unguarded forms were indistinguishable in commit checks, in
/// conflict ownership and in the plan. A blind overwrite is not always wrong --
/// overwriting a display name is last-writer-wins by intent -- so it is still
/// expressible. What is no longer possible is expressing it silently.
// req: POC-003
#[test]
fn an_unguarded_overwrite_must_be_declared_and_is_then_distinguishable() {
    let silent = compile_contract_source(&slot_claim(""));
    assert!(
        silent.is_err(),
        "a write with no precondition may not pass as a guarded one"
    );

    let declared = compile_contract_source(&declared_overwrite())
        .expect("the same write, spelled for what it is, compiles");
    let guarded = compile_contract_source(&slot_claim(GUARDED)).expect("guarded compiles");

    let unguarded_writes = |bundle: &riffdb_contract_ir::ContractBundle| {
        riffdb_contract_ir::CommandExplain::from_plan(
            bundle
                .commands()
                .iter()
                .find(|command| command.name() == "ClaimSlot")
                .expect("ClaimSlot"),
        )
        .unguarded_writes()
        .len()
    };
    assert_eq!(
        unguarded_writes(&declared),
        2,
        "both writes are carried in the plan as unguarded"
    );
    assert_eq!(
        unguarded_writes(&guarded),
        0,
        "and a guarded command carries none"
    );
}

/// The classification is in the plan, so it is in the plan hash: the same
/// assignment under the two spellings is not the same command.
///
/// It is also least-sufficient. A contract with no `overwrite` keeps the
/// version identity it had, which is what lets the migration be per contract
/// rather than all at once.
// req: POC-003
#[test]
fn the_classification_selects_its_own_version_and_moves_the_plan_hash() {
    use riffdb_contract_ir::{
        BUNDLE_FORMAT_VERSION_V24, EXECUTABLE_IR_VERSION_V24, GRAMMAR_VERSION_V24,
    };

    let guarded = compile_contract_source(&slot_claim(GUARDED)).expect("guarded compiles");
    let unguarded = compile_contract_source(&declared_overwrite()).expect("unguarded compiles");

    let plan_hash = |bundle: &riffdb_contract_ir::ContractBundle| {
        bundle
            .commands()
            .iter()
            .find(|command| command.name() == "ClaimSlot")
            .expect("ClaimSlot")
            .plan_hash()
    };
    assert_ne!(
        plan_hash(&guarded),
        plan_hash(&unguarded),
        "a write that stops constraining its prior value is a different command"
    );

    assert_eq!(unguarded.format_version(), BUNDLE_FORMAT_VERSION_V24);
    assert_eq!(unguarded.ir_version(), EXECUTABLE_IR_VERSION_V24);
    assert_eq!(unguarded.grammar_version(), GRAMMAR_VERSION_V24);
    assert!(
        guarded.format_version() < BUNDLE_FORMAT_VERSION_V24
            && guarded.ir_version() < EXECUTABLE_IR_VERSION_V24
            && guarded.grammar_version() < GRAMMAR_VERSION_V24,
        "a contract that declares no unguarded write must not be lifted to V24"
    );
}

/// The property the record exists for: deleting a `require` does not quietly
/// change what concurrent callers observe, it stops compiling.
// req: POC-003
#[test]
fn deleting_a_require_fails_to_compile_rather_than_changing_semantics() {
    let deployed = slot_claim(GUARDED);
    compile_contract_source(&deployed).expect("the guarded form compiles");

    let edited = deployed.replace(GUARDED, "");
    let error = compile_contract_source(&edited)
        .expect_err("removing the precondition must be refused, not accepted quietly");
    let riffdb_contract_compiler::CompilationError::Semantic(diagnostics) = error else {
        panic!("expected a semantic rejection");
    };
    assert!(
        diagnostics
            .as_slice()
            .iter()
            .any(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::UnguardedSet),
        "the diagnostic must name the write that lost its precondition"
    );
}

/// And the inverse, so the pair cannot drift into a one-way rule that trains
/// authors to write `overwrite` everywhere.
// req: POC-003
#[test]
fn an_overwrite_that_is_guarded_is_refused() {
    let guarded_but_declared_otherwise =
        slot_claim(GUARDED).replace("    set slot.", "    overwrite slot.");
    let error = compile_contract_source(&guarded_but_declared_otherwise)
        .expect_err("a guarded write may not claim to be unconditional");
    let riffdb_contract_compiler::CompilationError::Semantic(diagnostics) = error else {
        panic!("expected a semantic rejection");
    };
    assert!(
        diagnostics
            .as_slice()
            .iter()
            .any(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::GuardedOverwrite)
    );
}
