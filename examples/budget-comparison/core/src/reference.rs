//! Deterministic in-memory reference semantics and comparison checks.

use crate::{
    AllocateBudget, Amount, AmountError, BudgetKey, BudgetOperation, BudgetOutcome, BudgetState,
    CommandObservation, ContentionObservation, ContentionWorkload, CreateBudget,
    SequentialObservation, SequentialWorkload,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

/// Deterministic reference state for the canonical budget commands.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReferenceModel {
    budgets: BTreeMap<BudgetKey, BudgetState>,
}

impl ReferenceModel {
    /// Creates an empty reference database.
    pub const fn new() -> Self {
        Self {
            budgets: BTreeMap::new(),
        }
    }

    /// Executes one operation with canonical binding and precondition priority.
    pub fn execute(
        &mut self,
        operation: &BudgetOperation,
    ) -> Result<CommandObservation, ReferenceError> {
        let outcome = match operation {
            BudgetOperation::Create(command) => self.create(command)?,
            BudgetOperation::Allocate(command) => self.allocate(command)?,
        };
        Ok(CommandObservation {
            operation_id: operation.operation_id().clone(),
            outcome,
        })
    }

    /// Reads one normalized budget.
    pub fn read_budget(&self, key: BudgetKey) -> Option<BudgetState> {
        self.budgets.get(&key).cloned()
    }

    fn create(&mut self, command: &CreateBudget) -> Result<BudgetOutcome, ReferenceError> {
        if self.budgets.contains_key(&command.key) {
            return Ok(BudgetOutcome::BudgetAlreadyExists { key: command.key });
        }
        if command.approved_amount <= Amount::ZERO {
            return Ok(BudgetOutcome::InvalidApprovedAmount {
                minimum: Amount::MINIMUM_POSITIVE,
            });
        }
        let budget = BudgetState {
            key: command.key,
            approved_amount: command.approved_amount,
            allocated_amount: Amount::ZERO,
        };
        self.budgets.insert(command.key, budget.clone());
        Ok(BudgetOutcome::BudgetCreated { budget })
    }

    fn allocate(&mut self, command: &AllocateBudget) -> Result<BudgetOutcome, ReferenceError> {
        let Some(current) = self.budgets.get(&command.key).cloned() else {
            return Ok(BudgetOutcome::BudgetNotFound { key: command.key });
        };
        if command.amount <= Amount::ZERO {
            return Ok(BudgetOutcome::InvalidAmount {
                minimum: Amount::MINIMUM_POSITIVE,
            });
        }
        let allocated = current
            .allocated_amount
            .checked_add(command.amount)
            .map_err(ReferenceError::Arithmetic)?;
        if allocated > current.approved_amount {
            return Ok(BudgetOutcome::InsufficientBudget {
                approved: current.approved_amount,
                allocated: current.allocated_amount,
                requested: command.amount,
            });
        }
        let budget = BudgetState {
            allocated_amount: allocated,
            ..current
        };
        let remaining = budget
            .approved_amount
            .checked_sub(budget.allocated_amount)
            .map_err(ReferenceError::Arithmetic)?;
        self.budgets.insert(command.key, budget.clone());
        Ok(BudgetOutcome::Allocated { budget, remaining })
    }
}

/// Evaluates the sequential workload against the deterministic reference model.
pub fn evaluate_sequential(
    workload: &SequentialWorkload,
) -> Result<SequentialObservation, ReferenceError> {
    let mut model = ReferenceModel::new();
    let mut keys = BTreeSet::new();
    let mut outcomes = Vec::with_capacity(workload.operations.len());
    for operation in &workload.operations {
        keys.insert(operation.key());
        outcomes.push(model.execute(operation)?);
    }
    let final_budgets = keys
        .into_iter()
        .filter_map(|key| model.read_budget(key))
        .collect();
    Ok(SequentialObservation {
        case_id: workload.case_id.clone(),
        outcomes,
        final_budgets,
    })
}

/// Computes the order-independent expected result of both legal serializations.
pub fn expected_contention_observation(
    workload: &ContentionWorkload,
) -> Result<ContentionObservation, ReferenceError> {
    let first = evaluate_contention_order(workload, [0, 1])?;
    let second = evaluate_contention_order(workload, [1, 0])?;
    if first != second {
        return Err(ReferenceError::OrderDependentFixture);
    }
    Ok(first)
}

fn evaluate_contention_order(
    workload: &ContentionWorkload,
    order: [usize; 2],
) -> Result<ContentionObservation, ReferenceError> {
    let mut model = ReferenceModel::new();
    let seed = BudgetOperation::Create(workload.seed.clone());
    let seed_observation = model.execute(&seed)?;
    if !matches!(
        seed_observation.outcome,
        BudgetOutcome::BudgetCreated { .. }
    ) {
        return Err(ReferenceError::InvalidContentionSeed);
    }

    let mut outcomes = Vec::with_capacity(2);
    for index in order {
        let operation = BudgetOperation::Allocate(workload.contenders[index].clone());
        outcomes.push(model.execute(&operation)?.outcome);
    }
    outcomes.sort();
    Ok(ContentionObservation {
        case_id: workload.case_id.clone(),
        outcomes,
        final_budget: model.read_budget(workload.seed.key),
    })
}

/// Verifies an adapter's sequential report without weakening field equality.
pub fn verify_sequential(
    expected: &SequentialObservation,
    actual: &SequentialObservation,
) -> Result<(), OracleMismatch> {
    if expected == actual {
        Ok(())
    } else {
        Err(OracleMismatch::Sequential)
    }
}

/// Verifies an adapter's normalized contention multiset and final state.
pub fn verify_contention(
    expected: &ContentionObservation,
    actual: &ContentionObservation,
) -> Result<(), OracleMismatch> {
    if expected == actual {
        Ok(())
    } else {
        Err(OracleMismatch::Contention)
    }
}

/// A reference-model evaluation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReferenceError {
    /// Exact decimal arithmetic failed.
    Arithmetic(AmountError),
    /// The contention seed did not produce a created budget.
    InvalidContentionSeed,
    /// The golden contention case is not invariant to contender ordering.
    OrderDependentFixture,
}

impl fmt::Display for ReferenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arithmetic(error) => {
                write!(formatter, "reference decimal arithmetic failed: {error}")
            }
            Self::InvalidContentionSeed => {
                formatter.write_str("contention seed must create one budget")
            }
            Self::OrderDependentFixture => {
                formatter.write_str("contention fixture normalizes differently across legal orders")
            }
        }
    }
}

impl Error for ReferenceError {}

/// A safe comparison failure without embedding backend data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OracleMismatch {
    /// Sequential outcomes or final rows differ.
    Sequential,
    /// Contention outcome multiset or final row differs.
    Contention,
}

impl fmt::Display for OracleMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sequential => {
                formatter.write_str("sequential observation differs from the reference model")
            }
            Self::Contention => {
                formatter.write_str("contention observation differs from the reference model")
            }
        }
    }
}

impl Error for OracleMismatch {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical_workload;

    #[test]
    fn duplicate_binding_precedes_invalid_approval() {
        let workload = canonical_workload();
        let observation = evaluate_sequential(&workload.sequential).expect("reference run");
        assert!(matches!(
            observation.outcomes[3].outcome,
            BudgetOutcome::BudgetAlreadyExists { .. }
        ));
    }

    #[test]
    fn contention_allows_exactly_one_eighty_unit_allocation() {
        let workload = canonical_workload();
        let observation =
            expected_contention_observation(&workload.contention).expect("reference run");
        assert_eq!(observation.outcomes.len(), 2);
        assert_eq!(
            observation
                .outcomes
                .iter()
                .filter(|outcome| matches!(outcome, BudgetOutcome::Allocated { .. }))
                .count(),
            1
        );
        assert_eq!(
            observation
                .final_budget
                .expect("seeded row")
                .allocated_amount,
            Amount::parse("80.00").expect("amount")
        );
    }
}
