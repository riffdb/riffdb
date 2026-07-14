//! Canonical workload construction and generated golden fixture set.

use crate::{
    AllocateBudget, Amount, BudgetKey, BudgetOperation, BudgetWorkload, ContentionWorkload,
    CreateBudget, MatterId, OperationId, OrganizationId, ReferenceError, SequentialWorkload,
    WorkloadIdempotencyKey, evaluate_sequential, expected_contention_observation,
    postgres_guarantee_profile, render_contention_observation, render_guarantee_profile,
    render_sequential_observation, render_workload_fixture,
};

/// One deterministic generated file relative to the comparison workspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedFixture {
    /// Workspace-relative path.
    pub relative_path: String,
    /// Complete UTF-8 contents including final newline.
    pub contents: String,
}

/// Returns the version-one canonical long-lived comparison workload.
pub fn canonical_workload() -> BudgetWorkload {
    let primary = trusted_organization("018f22a1-7b3c-7def-8123-456789abcdef");
    let invalid = trusted_organization("018f22a1-7b3c-7def-8123-456789abcd00");
    let missing = trusted_organization("018f22a1-7b3c-7def-8123-456789abcd01");
    let primary_key = BudgetKey {
        organization_id: primary,
        fiscal_year: 2026,
    };

    BudgetWorkload {
        schema_version: 1,
        sequential: SequentialWorkload {
            case_id: trusted_operation_id("budget-sequential-v1"),
            operations: vec![
                BudgetOperation::Create(create(
                    "create-invalid-approval",
                    "wp045-create-invalid",
                    BudgetKey {
                        organization_id: invalid,
                        fiscal_year: 2026,
                    },
                    "-1.00",
                )),
                BudgetOperation::Allocate(allocate(
                    "allocate-missing-budget",
                    "wp045-allocate-missing",
                    BudgetKey {
                        organization_id: missing,
                        fiscal_year: 2026,
                    },
                    "018f22a1-7b3c-7def-8123-456789ab1001",
                    "1.00",
                )),
                BudgetOperation::Create(create(
                    "create-budget-100",
                    "wp045-create-primary",
                    primary_key,
                    "100.00",
                )),
                BudgetOperation::Create(create(
                    "create-duplicate-invalid",
                    "wp045-create-duplicate",
                    primary_key,
                    "-1.00",
                )),
                BudgetOperation::Allocate(allocate(
                    "allocate-invalid-zero",
                    "wp045-allocate-zero",
                    primary_key,
                    "018f22a1-7b3c-7def-8123-456789ab1002",
                    "0.00",
                )),
                BudgetOperation::Allocate(allocate(
                    "allocate-thirty",
                    "wp045-allocate-thirty",
                    primary_key,
                    "018f22a1-7b3c-7def-8123-456789ab1003",
                    "30.00",
                )),
                BudgetOperation::Allocate(allocate(
                    "allocate-insufficient-eighty",
                    "wp045-allocate-eighty",
                    primary_key,
                    "018f22a1-7b3c-7def-8123-456789ab1004",
                    "80.00",
                )),
                BudgetOperation::Allocate(allocate(
                    "allocate-twenty",
                    "wp045-allocate-twenty",
                    primary_key,
                    "018f22a1-7b3c-7def-8123-456789ab1005",
                    "20.00",
                )),
            ],
        },
        contention: ContentionWorkload {
            case_id: trusted_operation_id("budget-contention-v1"),
            seed: create(
                "contention-create-budget-100",
                "wp045-contention-create",
                primary_key,
                "100.00",
            ),
            contenders: [
                allocate(
                    "contention-allocate-a",
                    "wp045-contention-a",
                    primary_key,
                    "018f22a1-7b3c-7def-8123-456789ab2001",
                    "80.00",
                ),
                allocate(
                    "contention-allocate-b",
                    "wp045-contention-b",
                    primary_key,
                    "018f22a1-7b3c-7def-8123-456789ab2002",
                    "80.00",
                ),
            ],
        },
    }
}

/// Generates every checked-in version-one fixture from typed inputs and the model.
pub fn generated_fixtures() -> Result<Vec<GeneratedFixture>, ReferenceError> {
    let workload = canonical_workload();
    let sequential = evaluate_sequential(&workload.sequential)?;
    let contention = expected_contention_observation(&workload.contention)?;
    Ok(vec![
        GeneratedFixture {
            relative_path: String::from("fixtures/workload-v1.json"),
            contents: render_workload_fixture(&workload),
        },
        GeneratedFixture {
            relative_path: String::from("fixtures/sequential-observation-v1.json"),
            contents: render_sequential_observation(&sequential),
        },
        GeneratedFixture {
            relative_path: String::from("fixtures/contention-observation-v1.json"),
            contents: render_contention_observation(&contention),
        },
        GeneratedFixture {
            relative_path: String::from("fixtures/postgres-guarantees-v1.json"),
            contents: render_guarantee_profile(&postgres_guarantee_profile()),
        },
    ])
}

fn create(operation: &str, key: &str, budget: BudgetKey, approved: &str) -> CreateBudget {
    CreateBudget {
        operation_id: trusted_operation_id(operation),
        idempotency_key: trusted_idempotency_key(key),
        key: budget,
        approved_amount: trusted_amount(approved),
    }
}

fn allocate(
    operation: &str,
    key: &str,
    budget: BudgetKey,
    matter: &str,
    amount: &str,
) -> AllocateBudget {
    AllocateBudget {
        operation_id: trusted_operation_id(operation),
        idempotency_key: trusted_idempotency_key(key),
        key: budget,
        matter_id: trusted_matter(matter),
        amount: trusted_amount(amount),
    }
}

fn trusted_operation_id(value: &str) -> OperationId {
    match OperationId::new(value) {
        Ok(value) => value,
        Err(_) => unreachable!("built-in operation ID is valid"),
    }
}

fn trusted_idempotency_key(value: &str) -> WorkloadIdempotencyKey {
    match WorkloadIdempotencyKey::new(value) {
        Ok(value) => value,
        Err(_) => unreachable!("built-in idempotency key is valid"),
    }
}

fn trusted_organization(value: &str) -> OrganizationId {
    match OrganizationId::parse(value) {
        Ok(value) => value,
        Err(_) => unreachable!("built-in organization ID is valid"),
    }
}

fn trusted_matter(value: &str) -> MatterId {
    match MatterId::parse(value) {
        Ok(value) => value,
        Err(_) => unreachable!("built-in matter ID is valid"),
    }
}

fn trusted_amount(value: &str) -> Amount {
    match Amount::parse(value) {
        Ok(value) => value,
        Err(_) => unreachable!("built-in amount is valid"),
    }
}
