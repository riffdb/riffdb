//! Partition-local cross-aggregate command writes (ADR-0170).
//!
//! A command may write entities from more than one aggregate when every
//! binding derives the same partition route. Its conflict ownership is the
//! union of the keys those bindings derive, and each key is still the owning
//! aggregate's template instantiated for its own binding. A command whose
//! bindings derive *different* routes is still rejected.

use riffdb_contract_compiler::{CompilerDiagnosticCode, compile_contract_source};
use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V19, EXECUTABLE_IR_VERSION_V19, GRAMMAR_VERSION_V19, KeyPurpose,
    RootValidationReadPlan,
};

/// One tenant route, two aggregates, one atomic checkout.
const CROSS_AGGREGATE: &str = r"
contract Storefront version 1 {
  entity Product {
    key (tenant_id: uuid, product_id: uuid)
    field stock: u64
    delete_policy no_inbound
  }
  entity Order {
    key (tenant_id: uuid, order_id: uuid)
    field total: u64
    delete_policy no_inbound
  }
  aggregate ProductData {
    root Product
    partition_by tenant_id
    conflict_key (tenant_id, product_id)
  }
  aggregate OrderData {
    root Order
    partition_by tenant_id
    conflict_key (tenant_id, order_id)
  }
  command Checkout {
    input request_id: uuid
    input tenant_id: uuid
    input product_id: uuid
    input order_id: uuid
    input quantity: u64
    idempotency_key request_id
    mutate Product(tenant_id, product_id) as product else ProductMissing {}
    create Order(tenant_id, order_id) as order else OrderExists {}
    require in_stock: product.stock >= quantity else InsufficientStock {}
    set product.stock = product.stock - quantity
    set order.total = 100
    return CheckoutCompleted {}
  }
}
";

/// The same command over aggregates that derive different partition routes.
fn cross_partition() -> String {
    CROSS_AGGREGATE
        .replace(
            "key (tenant_id: uuid, product_id: uuid)",
            "key (region_id: uuid, product_id: uuid)",
        )
        .replace(
            "partition_by tenant_id\n    conflict_key (tenant_id, product_id)",
            "partition_by region_id\n    conflict_key (region_id, product_id)",
        )
        .replace(
            "    input product_id: uuid",
            "    input region_id: uuid\n    input product_id: uuid",
        )
        .replace(
            "mutate Product(tenant_id, product_id)",
            "mutate Product(region_id, product_id)",
        )
}

#[test]
fn a_command_may_write_two_aggregates_that_share_one_partition_route() {
    let bundle = compile_contract_source(CROSS_AGGREGATE)
        .expect("a partition-local cross-aggregate write compiles");
    let command = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "Checkout")
        .expect("Checkout");
    assert!(
        command.requires_ir_v19(),
        "a command spanning aggregates must require the version that admits it"
    );
}

#[test]
fn its_conflict_ownership_is_the_union_of_the_aggregates_it_writes() {
    let bundle = compile_contract_source(CROSS_AGGREGATE).expect("compiles");
    let command = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "Checkout")
        .expect("Checkout");
    let owners = command
        .locality()
        .conflict_keys()
        .iter()
        .filter_map(|key| match key.schema().purpose() {
            KeyPurpose::Conflict(owner) => Some(owner),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        owners.len(),
        2,
        "one conflict key per written aggregate, not one for the partition owner"
    );
    assert!(
        command.locality().spans_aggregates(),
        "ownership naming an aggregate other than the partition owner is the V19 shape"
    );
}

#[test]
fn writing_two_partition_routes_is_still_rejected() {
    // The distributed-transaction argument covers this case and only this case.
    let error = compile_contract_source(&cross_partition())
        .expect_err("bindings deriving different routes are not partition-local");
    let diagnostics = match error {
        riffdb_contract_compiler::CompilationError::Semantic(diagnostics) => diagnostics,
        other => panic!("expected a semantic rejection, got {other:?}"),
    };
    assert!(
        diagnostics
            .as_slice()
            .iter()
            .any(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::CrossPartitionMutation),
        "a cross-partition write must still raise RDB-C017"
    );
}

#[test]
fn a_single_aggregate_command_keeps_its_older_least_sufficient_identity() {
    // The compatibility claim: nothing that compiles today changes version, so
    // no deployed contract's plan hash or application lock moves.
    let single = CROSS_AGGREGATE
        .replace(
            "    create Order(tenant_id, order_id) as order else OrderExists {}\n",
            "",
        )
        .replace("    set order.total = 100\n", "");
    let bundle = compile_contract_source(&single).expect("single-aggregate command compiles");
    let command = bundle
        .commands()
        .iter()
        .find(|command| command.name() == "Checkout")
        .expect("Checkout");
    assert!(!command.requires_ir_v19());
    assert!(!command.locality().spans_aggregates());
    assert!(
        bundle.format_version() < BUNDLE_FORMAT_VERSION_V19
            && bundle.ir_version() < EXECUTABLE_IR_VERSION_V19
            && bundle.grammar_version() < GRAMMAR_VERSION_V19,
        "a single-aggregate contract must not be lifted to V19"
    );
}

#[test]
fn a_cross_aggregate_bundle_round_trips_through_its_canonical_encoding() {
    // The locality encoding is what feeds the command plan hash, so a decode
    // that loses or reorders conflict ownership would surface much later as a
    // stale application lock rather than as a decode failure.
    let bundle = compile_contract_source(CROSS_AGGREGATE).expect("compiles");
    let bytes = bundle.canonical_bytes().to_vec();
    let decoded = riffdb_contract_ir::ContractBundle::decode(&bytes).expect("decodes");
    assert_eq!(
        decoded.canonical_bytes(),
        bytes.as_slice(),
        "a cross-aggregate bundle must re-encode to the same bytes"
    );
    assert_eq!(decoded.bundle_hash(), bundle.bundle_hash());

    let command = decoded
        .commands()
        .iter()
        .find(|command| command.name() == "Checkout")
        .expect("Checkout survives the round trip");
    assert!(command.locality().spans_aggregates());
    assert_eq!(command.locality().conflict_keys().len(), 2);
}

// A command's invariant enforcement is a property of the aggregates it writes,
// never of the order its bindings are declared in. Before ADR-0252 the commit
// checks and their root-validation reads were both derived from the *first*
// mutating binding's aggregate, so a second aggregate's invariants were lowered
// only when its binding happened to come first, and the IR coverage proof
// validated against the same anchor and could not see the omission.
// req: POC-003
const TWO_AGGREGATE_INVARIANT: &str = r"
contract Ledger version 1 {
  entity Account {
    key (tenant: uuid, account_id: uuid)
    field open: bool
  }
  entity Entry {
    key (tenant: uuid, account_id: uuid, entry_id: uuid)
    field amount: i64
  }
  entity Note {
    key (tenant: uuid, note_id: uuid)
    field body: i64
  }
  aggregate Accounts {
    root Account
    child Entry
    partition_by tenant
    conflict_key (tenant, account_id)
    invariant account_open: open
  }
  aggregate Notes {
    root Note
    partition_by tenant
    conflict_key (tenant, note_id)
  }
  command Post {
    input request_key: string<128>
    input tenant: uuid
    input account_id: uuid
    input entry_id: uuid
    input note_id: uuid
    input amount: i64
    idempotency_key request_key
BINDINGS    overwrite note.body = amount
    overwrite entry.amount = amount
    return Posted { entry: entry }
  }
}
";

const NOTE_BINDING: &str = "    mutate Note(tenant, note_id) as note else NoteMissing {}\n";
const ENTRY_BINDING: &str =
    "    mutate Entry(tenant, account_id, entry_id) as entry else EntryMissing {}\n";

fn two_aggregate_invariant(note_first: bool) -> String {
    let bindings = if note_first {
        format!("{NOTE_BINDING}{ENTRY_BINDING}")
    } else {
        format!("{ENTRY_BINDING}{NOTE_BINDING}")
    };
    TWO_AGGREGATE_INVARIANT.replace("BINDINGS", &bindings)
}

/// The property: permuting the mutating bindings must not change what the
/// command enforces.
///
/// The plan itself is not identical under permutation -- binding identifiers and
/// the instruction stream follow the source order, and that is intended. What
/// must not move is the enforced content: which invariants are checked, and
/// which aggregate roots are read to check them.
// req: POC-003
#[test]
fn declaration_order_of_mutating_bindings_does_not_change_what_is_enforced() {
    let enforced = |note_first: bool| {
        let bundle = compile_contract_source(&two_aggregate_invariant(note_first))
            .expect("a partition-local cross-aggregate write compiles");
        let command = bundle
            .commands()
            .iter()
            .find(|command| command.name() == "Post")
            .expect("Post");
        let mut invariants = command
            .commit_checks()
            .iter()
            .map(|check| check.invariant_id())
            .collect::<Vec<_>>();
        invariants.sort_unstable();
        let mut validated_roots = command
            .root_validation_reads()
            .iter()
            .map(RootValidationReadPlan::entity_type)
            .collect::<Vec<_>>();
        validated_roots.sort_unstable();
        (invariants, validated_roots)
    };
    assert_eq!(
        enforced(true),
        enforced(false),
        "the same writes under a different binding order must enforce the same invariants \
         over the same aggregate roots"
    );
}

/// The soundness claim the property protects: the invariant is enforced even
/// when its aggregate is not the one the first mutating binding belongs to.
// req: POC-003
#[test]
fn an_invariant_is_enforced_for_every_aggregate_the_command_writes() {
    for note_first in [true, false] {
        let bundle = compile_contract_source(&two_aggregate_invariant(note_first))
            .expect("a partition-local cross-aggregate write compiles");
        let command = bundle
            .commands()
            .iter()
            .find(|command| command.name() == "Post")
            .expect("Post");
        let accounts = bundle
            .schema()
            .aggregates()
            .iter()
            .find(|aggregate| aggregate.name() == "Accounts")
            .expect("Accounts");
        let invariant = accounts
            .invariants()
            .first()
            .expect("Accounts declares account_open");

        assert_eq!(
            command
                .commit_checks()
                .iter()
                .filter(|check| check.invariant_id() == invariant.id())
                .count(),
            1,
            "note_first={note_first}: account_open must be checked exactly once"
        );
        assert!(
            command
                .root_validation_reads()
                .iter()
                .all(|read| read.entity_type() == accounts.root()),
            "note_first={note_first}: a root-validation read must read the invariant's own \
             aggregate root, never a key fabricated from another aggregate's binding"
        );
        assert_eq!(
            command.root_validation_reads().len(),
            1,
            "note_first={note_first}: one entry write reaches one account root"
        );
    }
}
