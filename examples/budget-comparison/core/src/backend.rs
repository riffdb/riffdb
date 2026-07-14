//! Minimal backend contract consumed by the shared runner.

use crate::{
    BudgetKey, BudgetOperation, BudgetState, CommandObservation, SequentialObservation,
    SequentialWorkload,
};
use std::collections::BTreeSet;
use std::error::Error;

/// API-neutral comparison adapter surface.
///
/// This is evidence-only and is not a RiffDB production service trait.
pub trait BudgetBackend {
    /// Adapter-specific failure.
    type Error: Error;

    /// Executes one compiled-command-equivalent operation.
    fn execute(&self, operation: &BudgetOperation) -> Result<CommandObservation, Self::Error>;

    /// Reads one normalized final budget state.
    fn read_budget(&self, key: BudgetKey) -> Result<Option<BudgetState>, Self::Error>;
}

/// Runs the ordered workload and captures every addressed final row.
pub fn observe_sequential<B: BudgetBackend>(
    backend: &B,
    workload: &SequentialWorkload,
) -> Result<SequentialObservation, B::Error> {
    let mut keys = BTreeSet::new();
    let mut outcomes = Vec::with_capacity(workload.operations.len());
    for operation in &workload.operations {
        keys.insert(operation.key());
        outcomes.push(backend.execute(operation)?);
    }

    let mut final_budgets = Vec::new();
    for key in keys {
        if let Some(budget) = backend.read_budget(key)? {
            final_budgets.push(budget);
        }
    }
    final_budgets.sort();

    Ok(SequentialObservation {
        case_id: workload.case_id.clone(),
        outcomes,
        final_budgets,
    })
}
