//! Compiler and static verification tests for role and saga declarations (ADR-0334).
// req: WF-002, WF-005, WF-011, WF-012, WF-013, ID-001, ID-002, TXN-001

use riffdb_contract_compiler::{
    CompilerBoundResource, CompilerDiagnosticCause, CompilerDiagnosticCode,
    compile_contract_source, validate_contract_source,
};
use riffdb_contract_ir::{
    ContractBundle, SagaBranchTransitionV1, SagaOutcomeEffectClassV1, saga_step_outcome_field_id,
};

const VALID_CHECKOUT_SAGA_CONTRACT: &str = r#"
contract CheckoutService version 1 {
    entity Inventory {
        key (warehouse_id: uuid, sku_id: string<64>)
        field available: i64
        field reserved: i64
    }

    entity Wallet {
        key (customer_id: uuid)
        field balance: i64
    }

    entity Order {
        key (order_id: uuid)
        field customer_id: uuid
        field warehouse_id: uuid
        field sku_id: string<64>
        field quantity: i64
        field charge_amount: i64
        field confirmed: bool
    }

    aggregate Inventories {
        root Inventory
        partition_by warehouse_id
        conflict_key (warehouse_id, sku_id)
    }

    aggregate Wallets {
        root Wallet
        partition_by customer_id
        conflict_key (customer_id)
    }

    aggregate Orders {
        root Order
        partition_by order_id
        conflict_key (order_id)
    }

    command ReserveInventory {
        input request_id: uuid
        input warehouse_id: uuid
        input sku_id: string<64>
        input quantity: i64
        idempotency_key request_id
        mutate Inventory(warehouse_id, sku_id) as inv else MissingSku { sku_id: sku_id }
        require sufficient_stock: inv.available >= quantity else OutOfStock { available: inv.available }
        set inv.available = inv.available - quantity
        set inv.reserved = inv.reserved + quantity
        return Reserved { reserved_qty: quantity }
    }

    command ReleaseInventory {
        input request_id: uuid
        input warehouse_id: uuid
        input sku_id: string<64>
        input quantity: i64
        idempotency_key request_id
        mutate Inventory(warehouse_id, sku_id) as inv else MissingSku { sku_id: sku_id }
        require has_reserved: inv.reserved >= quantity else ReleaseRejected { reserved: inv.reserved }
        set inv.available = inv.available + quantity
        set inv.reserved = inv.reserved - quantity
        return Released { released_qty: quantity }
    }

    command ChargeWallet {
        input request_id: uuid
        input customer_id: uuid
        input amount: i64
        idempotency_key request_id
        mutate Wallet(customer_id) as w else MissingWallet { customer_id: customer_id }
        require enough_funds: w.balance >= amount else InsufficientFunds { balance: w.balance }
        set w.balance = w.balance - amount
        return Charged { charged_amount: amount }
    }

    command RefundWallet {
        input request_id: uuid
        input customer_id: uuid
        input amount: i64
        idempotency_key request_id
        mutate Wallet(customer_id) as w else MissingWallet { customer_id: customer_id }
        set w.balance = w.balance + amount
        return Refunded { refunded_amount: amount }
    }

    command ConfirmOrder {
        input request_id: uuid
        input order_id: uuid
        input customer_id: uuid
        input warehouse_id: uuid
        input sku_id: string<64>
        input quantity: i64
        input charge_amount: i64
        idempotency_key request_id
        create Order(order_id) as ord else OrderAlreadyExists { order_id: order_id }
        set ord.customer_id = customer_id
        set ord.warehouse_id = warehouse_id
        set ord.sku_id = sku_id
        set ord.quantity = quantity
        set ord.charge_amount = charge_amount
        set ord.confirmed = true
        return Confirmed { order_id: order_id }
    }

    role CheckoutOrchestrator {
        commands (
            ReserveInventory,
            ReleaseInventory,
            ChargeWallet,
            RefundWallet,
            ConfirmOrder
        )
    }

    saga CheckoutOrder {
        partition_by order_id
        input request_id: uuid
        input saga_id: uuid
        input order_id: uuid
        input customer_id: uuid
        input warehouse_id: uuid
        input sku_id: string<64>
        input quantity: i64
        input amount: i64
        idempotency_key request_id
        execution_role CheckoutOrchestrator

        step reserve {
            command ReserveInventory(request_id, warehouse_id, sku_id, quantity)
            compensate ReleaseInventory(request_id, warehouse_id, sku_id, reserve.reserved_qty)
            on Reserved (effectful) => next charge
            on MissingSku (no_effect) => abort MissingInventory { order_id: order_id }
            on OutOfStock (no_effect) => abort OutOfStockAbort { order_id: order_id }
        }

        step charge {
            command ChargeWallet(request_id, customer_id, amount)
            compensate RefundWallet(request_id, customer_id, charge.charged_amount)
            on Charged (effectful) => next confirm
            on MissingWallet (no_effect) => compensate PaymentFailed { order_id: order_id, reserved_qty: reserve.reserved_qty }
            on InsufficientFunds (no_effect) => compensate PaymentFailed { order_id: order_id, reserved_qty: reserve.reserved_qty }
        }

        step confirm {
            command ConfirmOrder(request_id, order_id, customer_id, warehouse_id, sku_id, reserve.reserved_qty, charge.charged_amount)
            on Confirmed (effectful) => complete OrderCompleted { order_id: confirm.order_id, charged: charge.charged_amount }
            on OrderAlreadyExists (no_effect) => compensate DuplicateOrder { order_id: order_id }
        }
    }
}
"#;

#[test]
fn valid_multi_partition_saga_compiles_and_roundtrips_bundle_encoding() {
    validate_contract_source(VALID_CHECKOUT_SAGA_CONTRACT).expect("source validates");
    let bundle = compile_contract_source(VALID_CHECKOUT_SAGA_CONTRACT).expect("bundle compiles");

    assert_eq!(bundle.roles().len(), 1);
    let role = bundle.role("CheckoutOrchestrator").expect("role exists");
    assert_eq!(role.commands().len(), 5);

    assert_eq!(bundle.sagas().len(), 1);
    let saga = bundle.saga("CheckoutOrder").expect("saga exists");
    assert_eq!(saga.execution_role(), "CheckoutOrchestrator");
    assert_eq!(saga.steps().len(), 3);
    assert_eq!(saga.steps()[0].name(), "reserve");
    assert_eq!(saga.steps()[1].name(), "charge");
    assert_eq!(saga.steps()[2].name(), "confirm");
    assert!(saga.steps()[0].compensation().is_some());
    assert!(saga.steps()[1].compensation().is_some());
    assert!(saga.steps()[2].compensation().is_none());

    let reserve_cmd = bundle
        .commands()
        .iter()
        .find(|c| c.name() == "ReserveInventory")
        .expect("ReserveInventory");
    assert!(saga_step_outcome_field_id(reserve_cmd, "reserved_qty").is_some());

    let confirm_step = &saga.steps()[2];
    let has_complete = confirm_step.branches().iter().any(|b| {
        b.effect_class() == SagaOutcomeEffectClassV1::Effectful
            && matches!(b.transition(), SagaBranchTransitionV1::Complete(o) if o.outcome_name() == "OrderCompleted")
    });
    assert!(has_complete);

    let encoded = bundle.canonical_bytes();
    let decoded = ContractBundle::decode(encoded).expect("bundle decodes");
    assert_eq!(decoded.roles(), bundle.roles());
    assert_eq!(decoded.sagas(), bundle.sagas());
}

#[test]
fn saga_rejects_missing_outcome_branch() {
    let source = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "on OutOfStock (no_effect) => abort OutOfStockAbort { order_id: order_id }",
        "",
    );
    let err = compile_contract_source(&source).expect_err("missing branch must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::InvalidWorkflow
            && d.cause() == Some(CompilerDiagnosticCause::SagaOutcomeBranchMissing)
    }));
}

#[test]
fn saga_rejects_mismatched_effectful_or_no_effect_classification() {
    let source = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "on Reserved (effectful) => next charge",
        "on Reserved (no_effect) => next charge",
    );
    let err = compile_contract_source(&source).expect_err("effect mismatch must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::InvalidWorkflow
            && d.cause() == Some(CompilerDiagnosticCause::SagaOutcomeEffectMismatch)
    }));
}

#[test]
fn saga_rejects_missing_compensation_when_downstream_compensates() {
    let source = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "compensate ReleaseInventory(request_id, warehouse_id, sku_id, reserve.reserved_qty)",
        "",
    );
    let err = compile_contract_source(&source).expect_err("missing compensation must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::InvalidWorkflow
            && d.cause() == Some(CompilerDiagnosticCause::SagaMissingCompensation)
    }));
}

#[test]
fn saga_rejects_plain_abort_after_effectful_predecessor_but_accepts_abort_uncompensated() {
    let plain_abort = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "on InsufficientFunds (no_effect) => compensate PaymentFailed { order_id: order_id, reserved_qty: reserve.reserved_qty }",
        "on InsufficientFunds (no_effect) => abort UncompensatedDrop { order_id: order_id }",
    );
    let err =
        compile_contract_source(&plain_abort).expect_err("plain abort after effect must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::InvalidWorkflowTransition
            && d.cause() == Some(CompilerDiagnosticCause::SagaUncompensatedAbortAfterEffect)
    }));

    let explicit_uncompensated = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "on InsufficientFunds (no_effect) => compensate PaymentFailed { order_id: order_id, reserved_qty: reserve.reserved_qty }",
        "on InsufficientFunds (no_effect) => abort_uncompensated ExplicitDrop { order_id: order_id }",
    );
    let bundle = compile_contract_source(&explicit_uncompensated)
        .expect("explicit abort_uncompensated compiles");
    let charge_step = &bundle.saga("CheckoutOrder").expect("saga").steps()[1];
    assert!(charge_step.branches().iter().any(|b| {
        matches!(b.transition(), SagaBranchTransitionV1::AbortUncompensated(o) if o.outcome_name() == "ExplicitDrop")
    }));
}

#[test]
fn saga_rejects_step_cycles_and_unreachable_steps() {
    let cyclic = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "on OrderAlreadyExists (no_effect) => compensate DuplicateOrder { order_id: order_id }",
        "on OrderAlreadyExists (no_effect) => next charge",
    );
    let err = compile_contract_source(&cyclic).expect_err("cycle must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::InvalidWorkflowTransition
            && d.cause() == Some(CompilerDiagnosticCause::SagaStepCycle)
    }));

    let unreachable = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "on Reserved (effectful) => next charge",
        "on Reserved (effectful) => next confirm",
    );
    let err = compile_contract_source(&unreachable).expect_err("unreachable step must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::InvalidWorkflowTransition
            && d.cause() == Some(CompilerDiagnosticCause::SagaStepUnreachable)
    }));
}

#[test]
fn saga_rejects_role_missing_command_capability() {
    let missing_role_cmd = VALID_CHECKOUT_SAGA_CONTRACT.replace("RefundWallet,\n", "");
    let err = compile_contract_source(&missing_role_cmd)
        .expect_err("missing compensation command in role must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::InvalidWorkflow
            && d.cause() == Some(CompilerDiagnosticCause::SagaRoleMissingCommandCapability)
    }));
}

#[test]
fn saga_rejects_non_dominating_step_outcome_reference() {
    let non_dominating = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "command ReserveInventory(request_id, warehouse_id, sku_id, quantity)",
        "command ReserveInventory(request_id, warehouse_id, sku_id, charge.charged_amount)",
    );
    let err = compile_contract_source(&non_dominating)
        .expect_err("referencing downstream step must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::InvalidExpression
            && d.cause() == Some(CompilerDiagnosticCause::SagaNonDominatingStepReference)
    }));
}

#[test]
fn saga_rejects_more_than_sixteen_steps() {
    let mut extra_steps = String::new();
    for idx in 1..=16 {
        let next_action = if idx < 16 {
            format!("next step_{}", idx + 1)
        } else {
            "next reserve".to_owned()
        };
        extra_steps.push_str(&format!(
            r#"
        step step_{idx} {{
            command ChargeWallet(request_id, customer_id, amount)
            compensate RefundWallet(request_id, customer_id, step_{idx}.charged_amount)
            on Charged (effectful) => {next_action}
            on MissingWallet (no_effect) => compensate Failed {{ order_id: order_id }}
            on InsufficientFunds (no_effect) => compensate Failed {{ order_id: order_id }}
        }}
"#
        ));
    }
    let too_many_steps = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "        step reserve {",
        &format!("{extra_steps}\n        step reserve {{"),
    );
    let err = compile_contract_source(&too_many_steps).expect_err("19 steps must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::BoundExceeded
            && d.bound().map(|b| b.resource()) == Some(CompilerBoundResource::DeclarationCount)
    }));
}

#[test]
fn saga_rejects_missing_saga_id_and_invalid_partition_by() {
    let missing_saga_id = VALID_CHECKOUT_SAGA_CONTRACT.replace("input saga_id: uuid\n", "");
    let err = compile_contract_source(&missing_saga_id).expect_err("missing saga_id must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::InvalidWorkflow
            && d.cause() == Some(CompilerDiagnosticCause::SagaMissingSagaIdInput)
    }));

    let bad_partition = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "    saga CheckoutOrder {\n        partition_by order_id",
        "    saga CheckoutOrder {\n        partition_by unknown_input",
    );
    let err = compile_contract_source(&bad_partition).expect_err("unknown partition_by must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::UnknownName
            && d.cause() == Some(CompilerDiagnosticCause::SagaPartitionFieldInvalid)
    }));
}

#[test]
fn saga_rejects_accumulated_state_exceeding_64_kib() {
    let oversized_input = VALID_CHECKOUT_SAGA_CONTRACT.replace(
        "input sku_id: string<64>",
        "input sku_id: string<64>\n        input huge_payload: bytes<70000>",
    );
    let err = compile_contract_source(&oversized_input)
        .expect_err("accumulated state > 64 KiB must fail");
    let diags = err.semantic().expect("semantic error");
    assert!(diags.as_slice().iter().any(|d| {
        d.code() == CompilerDiagnosticCode::BoundExceeded
            && d.bound().map(|b| b.resource()) == Some(CompilerBoundResource::ValueBytes)
    }));
}
