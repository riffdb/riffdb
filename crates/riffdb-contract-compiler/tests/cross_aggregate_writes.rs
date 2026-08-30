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
