//! Sequential preparation over one immutable projection base (ADR-0230).

use crate::evaluator::{map_storage_error, map_storage_value_error};
use crate::{
    EvaluatedProjectionCommit, ProjectionEvaluationError, ProjectionEvaluationErrorKind,
    prepare_projection_apply,
};
use riffdb_storage_api::{
    CheckedProjectionSchema, MAX_PROJECTION_BATCH_MEMBERS, ProjectionApplyBatchV1,
    ProjectionApplyRequestV1, ProjectionApplyRowObservation, ProjectionApplySnapshot,
    ProjectionApplySnapshotReader, ProjectionApplySnapshotRequest, ProjectionBatchSnapshot,
    StorageError, StoredProjectionStateV1,
};
use riffdb_types::{
    FrontierPosition, MAX_PROJECTION_ROW_UPDATES, MAX_PROJECTION_WRITE_SET_BYTES,
    ProjectionGeneration, ProjectionGroupKey,
};
use std::collections::BTreeMap;

/// One bounded private sequence chain; it never mutates its captured base.
pub struct ProjectionBatchBuilder {
    base: Box<dyn ProjectionBatchSnapshot>,
    schema: CheckedProjectionSchema,
    generation: ProjectionGeneration,
    frontier: FrontierPosition,
    members: Vec<ProjectionApplyRequestV1>,
    observations: BTreeMap<ProjectionGroupKey, ProjectionApplyRowObservation>,
    overlay: BTreeMap<ProjectionGroupKey, StoredProjectionStateV1>,
    bytes: usize,
    updates: usize,
}

impl ProjectionBatchBuilder {
    /// Starts from complete control and row evidence in the same immutable view.
    pub fn new(
        base: Box<dyn ProjectionBatchSnapshot>,
        schema: CheckedProjectionSchema,
        generation: ProjectionGeneration,
    ) -> Result<Self, ProjectionEvaluationError> {
        if base.control().identity() != schema.identity()
            || !base.control().permits_application(generation)
        {
            return Err(integrity());
        }
        let frontier = base
            .control()
            .frontier_for(generation)
            .ok_or_else(integrity)?;
        let bytes = ProjectionApplyBatchV1::control_semantic_bytes(schema.identity())
            .map_err(map_storage_value_error)?;
        Ok(Self {
            base,
            schema,
            generation,
            frontier,
            members: Vec::new(),
            observations: BTreeMap::new(),
            overlay: BTreeMap::new(),
            bytes,
            updates: 0,
        })
    }

    /// Last privately prepared frontier; no durable progress is implied.
    #[must_use]
    pub const fn frontier(&self) -> FrontierPosition {
        self.frontier
    }

    /// Whether the fixed sequence count has been reached.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.members.len() == MAX_PROJECTION_BATCH_MEMBERS
    }

    /// Whether no sequence has yet been retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Retains a checked member only after all combined state/count bounds pass.
    /// False means flush this nonempty batch and prepare the member again on the new base.
    pub fn try_push(
        &mut self,
        evaluated: &EvaluatedProjectionCommit,
    ) -> Result<bool, ProjectionEvaluationError> {
        if evaluated.schema() != &self.schema || evaluated.generation() != self.generation {
            return Err(integrity());
        }
        if self.members.len() == MAX_PROJECTION_BATCH_MEMBERS {
            return Ok(false);
        }
        let missing = evaluated
            .grouped_deltas()
            .keys()
            .filter(|key| !self.observations.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>();
        let request =
            ProjectionApplySnapshotRequest::new(self.schema.clone(), self.generation, missing)
                .map_err(map_storage_value_error)?;
        let base_rows = self
            .base
            .read_apply_snapshot(&request)
            .map_err(map_storage_error)?;
        if Some(base_rows.expected_frontier()) != self.base.control().frontier_for(self.generation)
        {
            return Err(integrity());
        }
        let mut new_rows = base_rows.rows().iter();
        let mut rows = Vec::with_capacity(evaluated.changed_group_count());
        for key in evaluated.grouped_deltas().keys() {
            let observation = if let Some(row) = self.overlay.get(key) {
                ProjectionApplyRowObservation::Present(row.clone())
            } else {
                let row = new_rows.next().ok_or_else(integrity)?;
                if row.key() != key {
                    return Err(integrity());
                }
                row.clone()
            };
            rows.push(observation);
        }
        if new_rows.next().is_some() {
            return Err(integrity());
        }
        let requested_keys = evaluated.grouped_deltas().keys().cloned().collect();
        let snapshot_request = ProjectionApplySnapshotRequest::new(
            self.schema.clone(),
            self.generation,
            requested_keys,
        )
        .map_err(map_storage_value_error)?;
        let snapshot = ProjectionApplySnapshot::new(&snapshot_request, self.frontier, rows)
            .map_err(map_storage_value_error)?;
        let member = prepare_projection_apply(evaluated, &PreparedSnapshot(snapshot))?;
        let observation_bytes = base_rows.rows().iter().try_fold(0usize, |total, row| {
            total
                .checked_add(
                    row.semantic_bytes_at(base_rows.expected_frontier())
                        .map_err(map_storage_value_error)?
                        .checked_mul(2)
                        .ok_or_else(limit)?,
                )
                .ok_or_else(limit)
        })?;
        let bytes = self
            .bytes
            .checked_add(observation_bytes)
            .and_then(|bytes| bytes.checked_add(member.semantic_bytes()))
            .and_then(|bytes| bytes.checked_add(member.write_set_semantic_bytes()))
            .ok_or_else(limit)?;
        let updates = self
            .updates
            .checked_add(member.row_updates().len())
            .ok_or_else(limit)?;
        if bytes > MAX_PROJECTION_WRITE_SET_BYTES
            || updates > MAX_PROJECTION_ROW_UPDATES
            || self.observations.len() + base_rows.rows().len() > MAX_PROJECTION_ROW_UPDATES
        {
            return if self.is_empty() {
                Err(limit())
            } else {
                Ok(false)
            };
        }
        for row in base_rows.rows() {
            self.observations.insert(row.key().clone(), row.clone());
        }
        for update in member.row_updates() {
            let row = StoredProjectionStateV1::new(
                &self.schema,
                update.key().clone(),
                update.measures().clone(),
                member.sequence(),
            )
            .map_err(map_storage_value_error)?;
            self.overlay.insert(update.key().clone(), row);
        }
        self.frontier = FrontierPosition::AppliedThrough(member.sequence());
        self.bytes = bytes;
        self.updates = updates;
        self.members.push(member);
        Ok(true)
    }

    /// Freezes the chain and original observations for exact transactional validation.
    pub fn finish(self) -> Result<ProjectionApplyBatchV1, ProjectionEvaluationError> {
        ProjectionApplyBatchV1::new(
            self.base.control().clone(),
            self.members,
            self.observations.into_values().collect(),
        )
        .map_err(map_storage_value_error)
    }
}

struct PreparedSnapshot(ProjectionApplySnapshot);
impl ProjectionApplySnapshotReader for PreparedSnapshot {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        ProjectionApplySnapshot::new(request, self.0.expected_frontier(), self.0.rows().to_vec())
            .map_err(|_| {
                StorageError::new(
                    riffdb_storage_api::StorageErrorKind::InvariantViolation,
                    None,
                )
            })
    }
}
fn integrity() -> ProjectionEvaluationError {
    ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ProjectionStateIntegrity)
}
fn limit() -> ProjectionEvaluationError {
    ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::HardLimitExceeded)
}
