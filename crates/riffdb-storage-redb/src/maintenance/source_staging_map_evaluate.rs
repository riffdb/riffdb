//! Complete-command projection evaluation streamed into disposable staging.
use super::*;
use riffdb_catalog::ProjectionSourcePlanCaptureV1;
use riffdb_projection::{ProjectionEvaluationError, evaluate_projection_commit_source_admitted_v1};
use riffdb_storage_api::StoredCommitRecordV1;

/// Closed internal composition failure; never an evaluation/publication proof.
#[derive(Debug)]
pub(crate) enum SourceStageEvaluationError {
    Storage(StorageError),
    Evaluation(ProjectionEvaluationError),
}
impl From<StorageError> for SourceStageEvaluationError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}
impl From<ProjectionEvaluationError> for SourceStageEvaluationError {
    fn from(value: ProjectionEvaluationError) -> Self {
        Self::Evaluation(value)
    }
}

impl SourceStageMap<'_, '_, '_> {
    pub(crate) fn evaluate_and_apply(
        &mut self,
        capture: &ProjectionSourcePlanCaptureV1<'_>,
        command: &StoredCommitRecordV1,
    ) -> Result<(), SourceStageEvaluationError> {
        let result = self.evaluate_and_apply_inner(capture, command);
        if result.is_err() {
            self.retire();
        }
        result
    }

    fn evaluate_and_apply_inner(
        &mut self,
        capture: &ProjectionSourcePlanCaptureV1<'_>,
        command: &StoredCommitRecordV1,
    ) -> Result<(), SourceStageEvaluationError> {
        self.active()?;
        if !capture.belongs_to(&self.budget) || !std::ptr::eq(capture.schema(), self.schema) {
            return Err(integrity().into());
        }
        let next = self
            .applied
            .map_or(Some(CommitSequence::first()), CommitSequence::checked_next);
        if Some(command.commit_sequence()) != next
            || command.commit_sequence() > self.prefix.captured_through()
        {
            return Err(integrity().into());
        }
        // A clone of this actual map ledger is the same originating owner, not
        // another capacity allowance. Its local borrow permits mutable map use.
        let budget = self.budget.clone();
        let evaluated = evaluate_projection_commit_source_admitted_v1(
            capture.plan(),
            capture.schema(),
            command,
            &budget,
        )?;
        if !evaluated.belongs_to(&self.budget) {
            return Err(integrity().into());
        }
        let locator = self.tentative_locator;
        let mut update = self.begin_update()?;
        for (key, _) in evaluated.groups() {
            let key = evaluated.prepare_source_group_key_v1(key)?;
            let current = update.current(&key)?;
            let row = evaluated.prepare_source_row_v1(
                &key,
                current.as_ref().map(EncodedPageItem::value),
                locator,
            )?;
            // Do not retain decoded previous row graphs during encoding/put.
            drop(current);
            if !update.can_fit_put(&key, &row)? {
                // Chunk durability advances no command progress. Any later
                // failure retires this entire disposable map, not a candidate.
                update.finish(None)?;
                update = self.begin_update()?;
            }
            update.put(&row)?;
        }
        // Includes empty/irrelevant/filter-false commands: exact successor only.
        update.finish(Some(evaluated.sequence()))?;
        Ok(())
    }
}
