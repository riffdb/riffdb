//! Typed workload and normalized observation model.

use crate::{Amount, MatterId, OperationId, OrganizationId, WorkloadIdempotencyKey};

/// The maximum number of sequential operations accepted from a fixture.
pub const MAX_WORKLOAD_OPERATIONS: usize = 1_024;

/// The primary key of one annual budget.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BudgetKey {
    /// Owning organization.
    pub organization_id: OrganizationId,
    /// Fiscal year.
    pub fiscal_year: i64,
}

/// A normalized budget state.
///
/// `updated_at` is deliberately absent because PostgreSQL assigns it from its
/// transaction clock while RiffDB assigns deterministic command logical time.
/// Adapters must still validate that their stored timestamp is present.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BudgetState {
    /// Budget identity.
    pub key: BudgetKey,
    /// Approved amount.
    pub approved_amount: Amount,
    /// Allocated amount.
    pub allocated_amount: Amount,
}

/// Inputs for the canonical `CreateBudget` command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateBudget {
    /// Stable workload operation ID.
    pub operation_id: OperationId,
    /// Contract idempotency-key input.
    pub idempotency_key: WorkloadIdempotencyKey,
    /// Budget identity.
    pub key: BudgetKey,
    /// Requested approval.
    pub approved_amount: Amount,
}

/// Inputs for the canonical `AllocateBudget` command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AllocateBudget {
    /// Stable workload operation ID.
    pub operation_id: OperationId,
    /// Contract idempotency-key input.
    pub idempotency_key: WorkloadIdempotencyKey,
    /// Budget identity.
    pub key: BudgetKey,
    /// Matter recorded by the contract event.
    pub matter_id: MatterId,
    /// Requested allocation.
    pub amount: Amount,
}

/// One command in the shared sequential workload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BudgetOperation {
    /// `CreateBudget` command.
    Create(CreateBudget),
    /// `AllocateBudget` command.
    Allocate(AllocateBudget),
}

impl BudgetOperation {
    /// Returns the stable operation ID.
    pub fn operation_id(&self) -> &OperationId {
        match self {
            Self::Create(operation) => &operation.operation_id,
            Self::Allocate(operation) => &operation.operation_id,
        }
    }

    /// Returns the affected budget key.
    pub const fn key(&self) -> BudgetKey {
        match self {
            Self::Create(operation) => operation.key,
            Self::Allocate(operation) => operation.key,
        }
    }
}

/// The deterministic sequential comparison case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SequentialWorkload {
    /// Stable case ID.
    pub case_id: OperationId,
    /// Ordered commands.
    pub operations: Vec<BudgetOperation>,
}

/// The deterministic two-contender comparison case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentionWorkload {
    /// Stable case ID.
    pub case_id: OperationId,
    /// Command-based initial creation.
    pub seed: CreateBudget,
    /// Two allocations released at one explicit barrier.
    pub contenders: [AllocateBudget; 2],
}

/// Version-one shared budget workload document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BudgetWorkload {
    /// Fixture schema version.
    pub schema_version: u32,
    /// Sequential semantic cases.
    pub sequential: SequentialWorkload,
    /// Concurrent conflict case.
    pub contention: ContentionWorkload,
}

/// One declared command outcome normalized across adapters.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BudgetOutcome {
    /// Successful creation.
    BudgetCreated {
        /// Created budget.
        budget: BudgetState,
    },
    /// Duplicate create binding failure.
    BudgetAlreadyExists {
        /// Existing budget key.
        key: BudgetKey,
    },
    /// Create precondition failure.
    InvalidApprovedAmount {
        /// Smallest accepted approval.
        minimum: Amount,
    },
    /// Missing mutate binding failure.
    BudgetNotFound {
        /// Missing budget key.
        key: BudgetKey,
    },
    /// Allocation precondition failure.
    InvalidAmount {
        /// Smallest accepted allocation.
        minimum: Amount,
    },
    /// Allocation would exceed approval.
    InsufficientBudget {
        /// Approved amount.
        approved: Amount,
        /// Already allocated amount.
        allocated: Amount,
        /// Requested amount.
        requested: Amount,
    },
    /// Successful allocation.
    Allocated {
        /// Updated budget.
        budget: BudgetState,
        /// Exact amount remaining.
        remaining: Amount,
    },
}

/// A command observation retaining sequential operation identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandObservation {
    /// Stable workload operation ID.
    pub operation_id: OperationId,
    /// Declared outcome.
    pub outcome: BudgetOutcome,
}

/// Normalized sequential report, including all final rows addressed by the case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SequentialObservation {
    /// Stable case ID.
    pub case_id: OperationId,
    /// Outcomes in execution order.
    pub outcomes: Vec<CommandObservation>,
    /// Sorted final durable budgets.
    pub final_budgets: Vec<BudgetState>,
}

/// Order-independent observation for the two-contender case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentionObservation {
    /// Stable case ID.
    pub case_id: OperationId,
    /// Sorted outcome multiset; contender identity is intentionally removed.
    pub outcomes: Vec<BudgetOutcome>,
    /// Final durable row.
    pub final_budget: Option<BudgetState>,
}
