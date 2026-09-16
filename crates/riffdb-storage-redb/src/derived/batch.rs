//! Closed atomic projection batches; source commits remain authoritative.
use super::*;
use riffdb_storage_api::{
    ProjectionApplyBatchResult, ProjectionApplyBatchV1, ProjectionBatchSnapshot,
};
use std::collections::BTreeMap;

struct CapturedBase {
    access: RedbReadAccess,
    control: StoredProjectionControlV1,
}
impl ProjectionBatchSnapshot for CapturedBase {
    fn control(&self) -> &StoredProjectionControlV1 {
        &self.control
    }
}
impl ProjectionApplySnapshotReader for CapturedBase {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        if request.schema().identity() != self.control.identity()
            || !self.control.permits_application(request.generation())
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let frontier = self
            .control
            .frontier_for(request.generation())
            .ok_or_else(corrupt)?;
        let rows = self
            .access
            .open_table(PROJECTION_STATE)
            .map_err(table_error)?;
        let mut snapshot =
            ProjectionApplySnapshotBuilder::new(request, frontier).map_err(stored_value_error)?;
        for key in request.group_keys() {
            let observation = read_projection_state(&rows, request.schema(), key)?.map_or_else(
                || ProjectionApplyRowObservation::Absent(key.clone()),
                ProjectionApplyRowObservation::Present,
            );
            snapshot.push_row(observation).map_err(stored_value_error)?;
        }
        snapshot.finish().map_err(stored_value_error)
    }
}

pub(super) fn capture(
    ports: &RedbOperationalPorts,
    identity: &ProjectionIdentity,
) -> Result<Box<dyn ProjectionBatchSnapshot>, StorageError> {
    let access = ports.begin_composite_read()?;
    let control = read_projection_control(
        &access
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?,
        identity,
    )?
    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    Ok(Box::new(CapturedBase { access, control }))
}

pub(super) fn apply(
    ports: &RedbOperationalPorts,
    batch: &ProjectionApplyBatchV1,
) -> Result<ProjectionApplyBatchResult, StorageError> {
    // Reconstruct the checked boundary, including exact canonical member hashes.
    for member in batch.members() {
        let canonical = ProjectionApplyRequestV1::new(
            member.schema().clone(),
            member.generation(),
            member.sequence(),
            member.expected_frontier(),
            member.row_updates().to_vec(),
        )
        .map_err(request_value_error)?;
        if &canonical != member {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    ProjectionApplyBatchV1::new(
        batch.expected().clone(),
        batch.members().to_vec(),
        batch.observations().to_vec(),
    )
    .map_err(request_value_error)?;
    let first = batch.members().first().ok_or_else(corrupt)?;
    let access = ports
        .begin_attributed_write(riffdb_storage_api::ChangelogAttributionV3::ProjectionControl)?;
    let transaction = access.transaction()?;
    let current = read_projection_control(
        &transaction
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?,
        first.identity(),
    )?;
    let Some(current) = current else {
        access.abort()?;
        return Ok(ProjectionApplyBatchResult::StateChanged);
    };
    let Some(retained) = current.frontier_for(first.generation()) else {
        // Generation retirement is a concurrent control change, not evidence
        // that an otherwise canonical prepared batch is corrupt.
        access.abort()?;
        return Ok(ProjectionApplyBatchResult::StateChanged);
    };
    let mut historical = 0usize;
    {
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let markers = transaction
            .open_table(PROJECTION_APPLIED)
            .map_err(table_error)?;
        for member in batch.members() {
            if read_commit(&commits, &events, member.sequence())?.is_none() {
                return Err(corrupt());
            }
            let key = ProjectionApplyKey::new(
                member.identity().clone(),
                member.generation(),
                member.sequence(),
            );
            let marker = read_projection_marker(&markers, &key)?;
            if sequence_is_at_or_before(member.sequence(), retained) {
                if marker.is_none_or(|marker| marker.canonical_hash() != member.apply_hash()) {
                    return Err(corrupt());
                }
                historical += 1;
            } else if marker.is_some() {
                return Err(corrupt());
            }
        }
    }
    if historical == batch.members().len() {
        access.abort()?;
        return Ok(ProjectionApplyBatchResult::AlreadyApplied);
    }
    if historical != 0 || &current != batch.expected() {
        access.abort()?;
        return Ok(ProjectionApplyBatchResult::StateChanged);
    }
    let mut base = BTreeMap::new();
    {
        let rows = transaction
            .open_table(PROJECTION_STATE)
            .map_err(table_error)?;
        for observed in batch.observations() {
            let row = read_projection_state(&rows, first.schema(), observed.key())?;
            let matches = match (observed, &row) {
                (ProjectionApplyRowObservation::Absent(_), None) => true,
                (ProjectionApplyRowObservation::Present(expected), Some(actual)) => {
                    expected == actual
                }
                _ => false,
            };
            if !matches {
                drop(rows);
                access.abort()?;
                return Ok(ProjectionApplyBatchResult::StateChanged);
            }
            base.insert(observed.key().clone(), row);
        }
    }
    let mut overlay = BTreeMap::new();
    let mut control = current.clone();
    let mut markers = Vec::with_capacity(batch.members().len());
    for member in batch.members() {
        for update in member.row_updates() {
            let prior = overlay
                .get(update.key())
                .or_else(|| base.get(update.key()).and_then(Option::as_ref));
            let matches = match (update.prior(), prior) {
                (ProjectionRowPrior::Absent, None) => true,
                (ProjectionRowPrior::Present(expected), Some(row)) => {
                    row.last_changed_sequence() == expected
                }
                _ => false,
            };
            if !matches {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            overlay.insert(update.key().clone(), projection_post_image(member, update)?);
        }
        control = control
            .after_apply(
                member.generation(),
                member.expected_frontier(),
                member.sequence(),
            )
            .map_err(request_value_error)?;
        let key = ProjectionApplyKey::new(
            member.identity().clone(),
            member.generation(),
            member.sequence(),
        );
        markers.push((
            key.clone(),
            encode_projection_apply_v1(&StoredProjectionApplyV1::new(key, member.apply_hash()))?,
        ));
    }
    let encoded_control = encode_projection_control_v1(&control)?;
    let mut bytes = encoded_control.encoded_content_charge().get();
    for (_, encoded) in &markers {
        bytes = bytes
            .checked_add(encoded.encoded_content_charge().get())
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    }
    let mut prepared = Vec::with_capacity(overlay.len());
    for (key, row) in overlay {
        let encoded = encode_projection_state_v1(&row)?;
        bytes = bytes
            .checked_add(encoded.encoded_content_charge().get())
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        if bytes > MAX_PROJECTION_WRITE_SET_BYTES {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        prepared.push((key, encoded));
    }
    if bytes > MAX_PROJECTION_WRITE_SET_BYTES {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    for (key, _) in &prepared {
        access
            .expect_fresh_locator_byte_insert(PROJECTION_STATE, encode_projection_group_key(key))?;
    }
    for (key, _) in &markers {
        access.expect_fresh_locator_byte_insert(
            PROJECTION_APPLIED,
            encode_projection_apply_key(key),
        )?;
    }
    let frontier_key = ProjectionFrontierKey::new(first.identity().clone());
    let control_key = encode_projection_frontier_key(&frontier_key);
    access.expect_fresh_locator_byte_insert(PROJECTION_FRONTIER, control_key)?;
    access.close_fresh_locator_mutation_expectations()?;
    {
        let mut rows = transaction
            .open_table(PROJECTION_STATE)
            .map_err(table_error)?;
        for (key, encoded) in prepared {
            let physical = encode_projection_group_key(&key);
            access.record_actual_fresh_locator_byte_insert(PROJECTION_STATE, physical)?;
            let previous = rows
                .insert(physical, encoded.as_bytes())
                .map_err(precommit_storage_error)?;
            let actual = previous
                .map(|previous| {
                    decode_projection_state_v1(previous.value(), first.schema())
                        .map(|value| value.into_parts().0)
                })
                .transpose()?;
            if base.get(&key) != Some(&actual) {
                return Err(corrupt());
            }
        }
    }
    {
        let mut table = transaction
            .open_table(PROJECTION_APPLIED)
            .map_err(table_error)?;
        for (key, encoded) in markers {
            let physical = encode_projection_apply_key(&key);
            access.record_actual_fresh_locator_byte_insert(PROJECTION_APPLIED, physical)?;
            if table
                .insert(physical, encoded.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(corrupt());
            }
        }
    }
    {
        let mut controls = transaction
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?;
        access.record_actual_fresh_locator_byte_insert(PROJECTION_FRONTIER, control_key)?;
        let previous = controls
            .insert(control_key, encoded_control.as_bytes())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        if decode_projection_control_v1(previous.value())?
            .into_parts()
            .0
            != current
        {
            return Err(corrupt());
        }
    }
    access.commit_for(RedbTestOperation::ProjectionMutation)?;
    Ok(ProjectionApplyBatchResult::Applied(control))
}

pub(super) fn resolve(
    ports: &RedbOperationalPorts,
    batch: &ProjectionApplyBatchV1,
) -> Result<ProjectionApplyBatchResult, StorageError> {
    // Like columnar selection reconciliation, use one current engine read root;
    // admitting another write after CommitStatusUnknown is prohibited.
    let access = ports.begin_read()?;
    let first = batch.members().first().ok_or_else(corrupt)?;
    let Some(control) = read_projection_control(
        &access
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?,
        first.identity(),
    )?
    else {
        return Ok(ProjectionApplyBatchResult::StateChanged);
    };
    let Some(frontier) = control.frontier_for(first.generation()) else {
        return Ok(ProjectionApplyBatchResult::StateChanged);
    };
    let markers = access.open_table(PROJECTION_APPLIED).map_err(table_error)?;
    let mut present = 0usize;
    for member in batch.members() {
        let key = ProjectionApplyKey::new(
            member.identity().clone(),
            member.generation(),
            member.sequence(),
        );
        let marker = read_projection_marker(&markers, &key)?;
        if sequence_is_at_or_before(member.sequence(), frontier) {
            if marker.is_none_or(|marker| marker.canonical_hash() != member.apply_hash()) {
                return Err(corrupt());
            }
            present += 1;
        } else if marker.is_some() {
            return Err(corrupt());
        }
    }
    Ok(if present == batch.members().len() {
        ProjectionApplyBatchResult::AlreadyApplied
    } else {
        ProjectionApplyBatchResult::StateChanged
    })
}
