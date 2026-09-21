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

use riffdb_contract_compiler::compile_contract_source;

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

fn slot_claim(guard: &str) -> String {
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
{guard}    set slot.free = false
    set slot.held_by = holder
    return Claimed {{ slot: slot }}
  }}
}}
"
    )
}

const GUARDED: &str = "    require is_free: slot.free else SlotTaken {}\n";

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

/// The gap. The same command with the guard removed is a blind overwrite --
/// two concurrent claims both succeed and the second silently replaces the
/// first holder -- and the compiler accepts it with no diagnostic and nothing
/// in the plan distinguishing it from the guarded form.
///
/// This is not always wrong: overwriting a display name is last-writer-wins by
/// intent. What is wrong is that the two are indistinguishable in the source,
/// in the plan, and in review. If this test ever fails because a diagnostic
/// fires, that is the fix landing, not a regression.
// req: POC-003
#[test]
fn an_unguarded_overwrite_is_accepted_and_unmarked() {
    let unguarded =
        compile_contract_source(&slot_claim("")).expect("today a blind overwrite compiles");
    let guarded = compile_contract_source(&slot_claim(GUARDED)).expect("guarded compiles");

    let plan = |bundle: &riffdb_contract_ir::ContractBundle| {
        let command = bundle
            .commands()
            .iter()
            .find(|command| command.name() == "ClaimSlot")
            .expect("ClaimSlot");
        (
            command.commit_checks().len(),
            command.locality().conflict_keys().len(),
        )
    };
    assert_eq!(
        plan(&unguarded),
        plan(&guarded),
        "the guarded and unguarded claims are indistinguishable in commit checks and \
         conflict ownership; only the declared outcome differs, and nothing requires one"
    );
}
