//! Source-admitted reuse of ordinary pure projection evaluation.
//!
//! This is not native capture, independent replay closure or publication proof.
use crate::{
    EvaluatedProjectionCommit, ProjectionEvaluationError, ProjectionEvaluationErrorKind,
    evaluate_projection_commit,
};
use riffdb_catalog::ResolvedProjectionPlan;
use riffdb_storage_api::{CheckedProjectionSchema, StoredCommitRecordV1};
use riffdb_types::{
    CanonicalRecord, CommitSequence, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    ProjectionGeneration, ProjectionGroupKey, ProjectionIdentity,
    ProjectionSourceWorkReservationV1, ProjectionSourceWorkingBudgetV1,
};
use std::marker::PhantomData;

#[path = "source_evaluator_capacity.rs"]
mod capacity;

/// Borrowed-lifetime, move-only evaluated deltas. Data dies before its complete
/// work reservation; original plan/schema/command custody cannot be detached.
/// Only read-only per-group borrows are exposed. No source candidate is minted.
///
/// ```compile_fail,E0505
/// use riffdb_catalog::ResolvedProjectionPlan;
/// use riffdb_storage_api::{CheckedProjectionSchema, StoredCommitRecordV1};
/// use riffdb_types::ProjectionSourceWorkingBudgetV1;
/// use riffdb_projection::evaluate_projection_commit_source_admitted_v1;
/// fn cannot_detach_command(resolved: ResolvedProjectionPlan, schema: CheckedProjectionSchema,
///     command: StoredCommitRecordV1, budget: ProjectionSourceWorkingBudgetV1) {
///     let result = evaluate_projection_commit_source_admitted_v1(&resolved, &schema, &command, &budget).unwrap();
///     drop(command);
///     drop(result);
/// }
/// ```
pub struct SourceAdmittedProjectionCommitV1<'input> {
    evaluated: EvaluatedProjectionCommit,
    _inputs: PhantomData<(
        &'input ResolvedProjectionPlan,
        &'input CheckedProjectionSchema,
        &'input StoredCommitRecordV1,
        &'input ProjectionSourceWorkingBudgetV1,
    )>,
    budget: &'input ProjectionSourceWorkingBudgetV1,
    _work: ProjectionSourceWorkReservationV1,
}

impl Drop for SourceAdmittedProjectionCommitV1<'_> {
    fn drop(&mut self) {
        // Keep every input borrow live through physical result destruction.
        // A bare PhantomData reference could end at last use under NLL.
    }
}

impl SourceAdmittedProjectionCommitV1<'_> {
    /// Exact projection identity checked by the unchanged ordinary evaluator.
    #[must_use]
    pub fn identity(&self) -> &ProjectionIdentity {
        self.evaluated.identity()
    }
    /// Includes irrelevant/empty commands; no command progress is inferred.
    #[must_use]
    pub fn sequence(&self) -> CommitSequence {
        self.evaluated.sequence()
    }
    /// Existing bounded complete-command grouping, in canonical native key order.
    pub fn groups(&self) -> impl ExactSizeIterator<Item = (&ProjectionGroupKey, &CanonicalRecord)> {
        self.evaluated.grouped_deltas().iter()
    }
}

/// Reserves all additional evaluator work before normalization/cache/grouping.
/// Inputs require a separate admitted native capture on this originating ledger;
/// this pure helper accepts no callback and creates no ledger or source authority.
#[doc(hidden)]
pub fn evaluate_projection_commit_source_admitted_v1<'input>(
    resolved: &'input ResolvedProjectionPlan,
    schema: &'input CheckedProjectionSchema,
    commit: &'input StoredCommitRecordV1,
    budget: &'input ProjectionSourceWorkingBudgetV1,
) -> Result<SourceAdmittedProjectionCommitV1<'input>, ProjectionEvaluationError> {
    let mut scratch = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .map_err(|_| unavailable())?;
    // A sizing walk is not authoritative semantic validation. It may refuse
    // an unknown writer/unsupported shape as well as a finite census bound;
    // none of those observations may fabricate a durable corruption/limit code.
    // Ordinary evaluation retains all semantic classifications once admitted.
    let bytes = capacity::work_bound(
        resolved,
        schema,
        commit,
        scratch.inspection_scope().map_err(|_| unavailable())?,
    )
    .map_err(|_| unavailable())?;
    let work = budget.reserve(bytes).map_err(|_| unavailable())?;
    drop(scratch);
    // Exactly the ordinary evaluator/arithmetic/normalizer: never stage source
    // events one at a time before the complete-command delta association.
    let evaluated = evaluate_projection_commit(
        resolved,
        schema.clone(),
        ProjectionGeneration::first(),
        commit,
    )?;
    Ok(SourceAdmittedProjectionCommitV1 {
        evaluated,
        _inputs: PhantomData,
        budget,
        _work: work,
    })
}

fn unavailable() -> ProjectionEvaluationError {
    ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::StorageUnavailable)
}

#[path = "source_evaluator_rows.rs"]
mod rows;

#[cfg(test)]
#[path = "source_evaluator_tests.rs"]
pub(crate) mod tests;
