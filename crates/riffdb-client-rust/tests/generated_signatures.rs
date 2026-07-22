#![forbid(unsafe_code)]

//! Compile-time signature fixture for the canonical generated contract module.

use riffdb_client_rust::RiffDbClient;
use riffdb_client_rust::generated::GeneratedCommand;
use riffdb_client_rust::generated::legal_spend::{
    ALLOCATE_BUDGET_PLAN_HASH, AllocateBudget, AllocateBudgetOutcome, Amount, Budget,
    CONTRACT_LINEAGE, CONTRACT_VERSION, CREATE_BUDGET_PLAN_HASH, CreateBudget, CreateBudgetOutcome,
};
use riffdb_types::{RequestId, Timestamp};

#[test]
fn legal_spend_public_shapes_remain_source_compatible() {
    fn assert_generated<T: GeneratedCommand>() {}
    assert_generated::<CreateBudget>();
    assert_generated::<AllocateBudget>();

    let amount = Amount::from_minor_units(10_000).expect("amount");
    let budget = Budget {
        organization_id: [1; 16],
        fiscal_year: 2026,
        approved_amount: amount,
        allocated_amount: Amount::from_minor_units(0).expect("zero"),
        updated_at: Timestamp::new(1, 0).expect("timestamp"),
    };
    let create = CreateBudget {
        idempotency_key: "create-1".to_owned(),
        organization_id: budget.organization_id,
        fiscal_year: budget.fiscal_year,
        approved_amount: budget.approved_amount,
    };
    let allocate = AllocateBudget {
        idempotency_key: "allocate-1".to_owned(),
        organization_id: budget.organization_id,
        fiscal_year: budget.fiscal_year,
        matter_id: [2; 16],
        amount,
    };

    assert!(create.idempotent_command().is_ok());
    assert!(allocate.idempotent_command().is_ok());
    let request_id = RequestId::from_unix_milliseconds_and_random(1, [3; 10]).expect("request ID");
    assert!(create.outcome_request(request_id).is_ok());
    assert!(allocate.outcome_request(request_id).is_ok());
    assert_eq!(CONTRACT_LINEAGE, "LegalSpend");
    assert_eq!(CONTRACT_VERSION, 1);
    assert_eq!(CREATE_BUDGET_PLAN_HASH.len(), 32);
    assert_eq!(ALLOCATE_BUDGET_PLAN_HASH.len(), 32);

    consume_create_outcome(CreateBudgetOutcome::BudgetCreated { budget });
    consume_allocate_outcome(AllocateBudgetOutcome::Allocated {
        budget,
        remaining: amount,
    });

    // These references make method removal or renaming a compile-time failure.
    let _ = RiffDbClient::execute_generated::<CreateBudget>;
    let _ = RiffDbClient::wait_for_projection;
}

fn consume_create_outcome(outcome: CreateBudgetOutcome) {
    match outcome {
        CreateBudgetOutcome::BudgetCreated { budget: _ } => {}
        CreateBudgetOutcome::BudgetAlreadyExists {
            organization_id: _,
            fiscal_year: _,
        } => {}
        CreateBudgetOutcome::InvalidApprovedAmount { minimum: _ } => {}
    }
}

fn consume_allocate_outcome(outcome: AllocateBudgetOutcome) {
    match outcome {
        AllocateBudgetOutcome::Allocated {
            budget: _,
            remaining: _,
        } => {}
        AllocateBudgetOutcome::InvalidAmount { minimum: _ } => {}
        AllocateBudgetOutcome::BudgetNotFound {
            organization_id: _,
            fiscal_year: _,
        } => {}
        AllocateBudgetOutcome::InsufficientBudget {
            approved: _,
            allocated: _,
            requested: _,
        } => {}
    }
}
