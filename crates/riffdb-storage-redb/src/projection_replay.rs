//! Shared derived-only projection replay. Both owners keep exclusive writer
//! custody; this core never commits, changes a source control, or assigns a sequence.
use crate::{
    codec::{decode_projection_apply_v1, encode_projection_apply_v1, encode_projection_state_v1},
    derived::{
        projection_post_image, read_commit, read_projection_control, read_projection_marker,
        read_projection_state,
    },
    error::storage_error,
    keys::{encode_projection_apply_key, encode_projection_group_key},
    layout::{COMMITS, EVENTS, PROJECTION_APPLIED, PROJECTION_FRONTIER, PROJECTION_STATE},
    store::RedbReadAccess,
};
use redb::{ReadableTable, WriteTransaction};
use riffdb_storage_api::{
    CanonicalStoredEnvelopeV1, ProjectionApplyRequestV1, ProjectionApplyRowObservation,
    ProjectionApplySnapshot, ProjectionApplySnapshotBuilder, ProjectionApplySnapshotRequest,
    ProjectionRowPrior, StorageError, StorageErrorKind, StoredProjectionApplyV1,
    StoredProjectionControlV1,
};
use riffdb_types::{
    CommitSequence, FrontierPosition, MAX_PROJECTION_WRITE_SET_BYTES, ProjectionApplyKey,
    ProjectionGeneration, ProjectionIdentity,
};

pub(crate) enum ProjectionReplayStage {
    Existing(StoredProjectionApplyV1),
    Staged(StoredProjectionApplyV1),
}

pub(crate) fn stage_projection_replay(
    write: &WriteTransaction,
    expected: &StoredProjectionControlV1,
    request: &ProjectionApplyRequestV1,
) -> Result<ProjectionReplayStage, StorageError> {
    let control = read_projection_control(
        &write.open_table(PROJECTION_FRONTIER).map_err(invalid)?,
        request.identity(),
    )?
    .ok_or_else(corrupt)?;
    if &control != expected {
        return Err(corrupt());
    }
    let target = control
        .frontier_for(request.generation())
        .ok_or_else(corrupt)?;
    if FrontierPosition::AppliedThrough(request.sequence()) > target
        || read_commit(
            &write.open_table(COMMITS).map_err(invalid)?,
            &write.open_table(EVENTS).map_err(invalid)?,
            request.sequence(),
        )?
        .is_none()
    {
        return Err(corrupt());
    }
    let key = ProjectionApplyKey::new(
        request.identity().clone(),
        request.generation(),
        request.sequence(),
    );
    let local = local_frontier(
        &write.open_table(PROJECTION_APPLIED).map_err(invalid)?,
        request.identity(),
        request.generation(),
        target,
    )?;
    if FrontierPosition::AppliedThrough(request.sequence()) <= local {
        let marker = read_projection_marker(
            &write.open_table(PROJECTION_APPLIED).map_err(invalid)?,
            &key,
        )?
        .ok_or_else(corrupt)?;
        if marker.canonical_hash() != request.apply_hash() {
            return Err(corrupt());
        }
        return Ok(ProjectionReplayStage::Existing(marker));
    }
    if local != request.expected_frontier() {
        return Err(corrupt());
    }
    let marker = StoredProjectionApplyV1::new(key.clone(), request.apply_hash());
    let encoded_marker = encode_projection_apply_v1(&marker)?;
    let mut bytes = encoded_marker.encoded_content_charge().get();
    let mut prepared: Vec<CanonicalStoredEnvelopeV1> =
        Vec::with_capacity(request.row_updates().len());
    {
        let rows = write.open_table(PROJECTION_STATE).map_err(invalid)?;
        for update in request.row_updates() {
            let current = read_projection_state(&rows, request.schema(), update.key())?;
            if !match (update.prior(), current.as_ref()) {
                (ProjectionRowPrior::Absent, None) => true,
                (ProjectionRowPrior::Present(sequence), Some(row)) => {
                    row.last_changed_sequence() == sequence
                }
                _ => false,
            } {
                return Err(corrupt());
            }
            let encoded = encode_projection_state_v1(&projection_post_image(request, update)?)?;
            bytes = bytes
                .checked_add(encoded.encoded_content_charge().get())
                .ok_or_else(corrupt)?;
            if bytes > MAX_PROJECTION_WRITE_SET_BYTES {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            prepared.push(encoded);
        }
    }
    {
        let mut rows = write.open_table(PROJECTION_STATE).map_err(invalid)?;
        for (update, encoded) in request.row_updates().iter().zip(prepared) {
            rows.insert(
                encode_projection_group_key(update.key()),
                encoded.as_bytes(),
            )
            .map_err(unavailable)?;
        }
    }
    if write
        .open_table(PROJECTION_APPLIED)
        .map_err(invalid)?
        .insert(encode_projection_apply_key(&key), encoded_marker.as_bytes())
        .map_err(unavailable)?
        .is_some()
    {
        return Err(corrupt());
    }
    Ok(ProjectionReplayStage::Staged(marker))
}

/// Last locally replayed marker, found with one bounded reverse key lookup.
/// Complete marker/row semantic verification still precedes publication.
pub(crate) fn local_frontier(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    target: FrontierPosition,
) -> Result<FrontierPosition, StorageError> {
    let first = ProjectionApplyKey::new(identity.clone(), generation, CommitSequence::first());
    let last = ProjectionApplyKey::new(
        identity.clone(),
        generation,
        CommitSequence::new(u64::MAX).ok_or_else(corrupt)?,
    );
    let mut range = table
        .range(first.as_bytes()..=last.as_bytes())
        .map_err(unavailable)?;
    let Some(entry) = range.next_back() else {
        return Ok(FrontierPosition::BeforeFirst);
    };
    let (key, value) = entry.map_err(unavailable)?;
    let marker = decode_projection_apply_v1(value.value())?.into_parts().0;
    if marker.key().as_bytes() != key.value()
        || marker.key().identity() != identity
        || marker.key().generation() != generation
    {
        return Err(corrupt());
    }
    let frontier = FrontierPosition::AppliedThrough(marker.key().commit_sequence());
    if frontier > target {
        return Err(corrupt());
    }
    Ok(frontier)
}

pub(crate) fn read_projection_replay_snapshot(
    access: &RedbReadAccess,
    request: &ProjectionApplySnapshotRequest,
) -> Result<ProjectionApplySnapshot, StorageError> {
    let control = read_projection_control(
        &access.open_table(PROJECTION_FRONTIER).map_err(invalid)?,
        request.schema().identity(),
    )?
    .ok_or_else(corrupt)?;
    let target = control
        .frontier_for(request.generation())
        .ok_or_else(corrupt)?;
    let frontier = local_frontier(
        &access.open_table(PROJECTION_APPLIED).map_err(invalid)?,
        request.schema().identity(),
        request.generation(),
        target,
    )?;
    let rows = access.open_table(PROJECTION_STATE).map_err(invalid)?;
    let mut snapshot = ProjectionApplySnapshotBuilder::new(request, frontier).map_err(invalid)?;
    for key in request.group_keys() {
        snapshot
            .push_row(
                read_projection_state(&rows, request.schema(), key)?.map_or_else(
                    || ProjectionApplyRowObservation::Absent(key.clone()),
                    ProjectionApplyRowObservation::Present,
                ),
            )
            .map_err(invalid)?;
    }
    snapshot.finish().map_err(invalid)
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn invalid<T>(_: T) -> StorageError {
    corrupt()
}
fn unavailable<T>(_: T) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
