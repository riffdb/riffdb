//! Primary-resident projection checkpoints and seeded candidate/recovery helpers.
#![allow(clippy::mutable_key_type)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use redb::ReadableTable;
use riffdb_storage_api::{CheckedProjectionSchema, StructurallyDecodedProjectionStateV1};

use super::*;
use crate::codec::{decode_projection_state_structural_v1, encode_projection_state_structural_v1};

#[derive(Clone)]
pub(crate) struct CachedProjectionCheckpoint {
    generation: ProjectionGeneration,
    frontier: CommitSequence,
    rows: Vec<StructurallyDecodedProjectionStateV1>,
    markers: Vec<StoredProjectionApplyV1>,
}

pub(crate) fn derived_path_for_journal_path(journal_path: &Path) -> Option<PathBuf> {
    let text = journal_path.to_str()?;
    let db_path = text
        .strip_suffix(".riffjournal.checkpoint")
        .or_else(|| text.strip_suffix(".riffjournal"))?;
    Some(crate::store::derived_store_path(Path::new(db_path)))
}

pub(crate) fn highest_marker_for_identity<T>(
    markers: &T,
    identity: &ProjectionIdentity,
) -> Result<Option<StoredProjectionApplyV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = super::projection_identity_marker_prefix(identity);
    let mut upper = prefix.clone();
    upper.extend_from_slice(&[0xFF; 16]);
    let mut range = markers
        .range::<&[u8]>(prefix.as_slice()..=upper.as_slice())
        .map_err(precommit_storage_error)?;
    let Some(entry) = range.next_back() else {
        return Ok(None);
    };
    let (key, value) = entry.map_err(precommit_storage_error)?;
    if !key.value().starts_with(&prefix) {
        return Ok(None);
    }
    let marker = decode_projection_apply_v1(value.value())?.into_parts().0;
    if marker.key().as_bytes() != key.value() || marker.key().identity() != identity {
        return Err(corrupt());
    }
    Ok(Some(marker))
}

fn extract_checkpoint_for_generation<S, M>(
    state_table: &S,
    marker_table: &M,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    frontier: CommitSequence,
) -> Result<Option<CachedProjectionCheckpoint>, StorageError>
where
    S: ReadableTable<&'static [u8], &'static [u8]>,
    M: ReadableTable<&'static [u8], &'static [u8]>,
{
    let frontier_key = ProjectionApplyKey::new(identity.clone(), generation, frontier);
    if read_projection_marker(marker_table, &frontier_key)?.is_none() {
        return Ok(None);
    }
    let state_prefix =
        riffdb_types::ProjectionGroupPrefixBuilder::new(identity.clone(), generation).finish();
    let mut rows = Vec::new();
    let mut marker_sequences = BTreeSet::new();
    marker_sequences.insert(frontier);
    for entry in state_table
        .range::<&[u8]>(state_prefix.as_bytes()..)
        .map_err(precommit_storage_error)?
    {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if !key.value().starts_with(state_prefix.as_bytes()) {
            break;
        }
        let Ok(decoded) = decode_projection_state_structural_v1(value.value()) else {
            return Ok(None);
        };
        let row = decoded.into_parts().0;
        if row.key().as_bytes() != key.value()
            || row.identity() != identity
            || row.generation() != generation
            || row.last_changed_sequence() > frontier
        {
            return Ok(None);
        }
        marker_sequences.insert(row.last_changed_sequence());
        rows.push(row);
    }
    let mut markers = Vec::with_capacity(marker_sequences.len());
    for seq in marker_sequences {
        let key = ProjectionApplyKey::new(identity.clone(), generation, seq);
        let Some(marker) = read_projection_marker(marker_table, &key).ok().flatten() else {
            return Ok(None);
        };
        markers.push(marker);
    }
    Ok(Some(CachedProjectionCheckpoint {
        generation,
        frontier,
        rows,
        markers,
    }))
}

pub(crate) fn extract_primary_checkpoint(
    primary: &redb::ReadTransaction,
    identity: &ProjectionIdentity,
) -> Result<Option<CachedProjectionCheckpoint>, StorageError> {
    let controls = match primary.open_table(PROJECTION_FRONTIER) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(table_error(error)),
    };
    if read_projection_control(&controls, identity)?.is_some() {
        return Ok(None);
    }
    let rows = match primary.open_table(PROJECTION_STATE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(table_error(error)),
    };
    let markers = match primary.open_table(PROJECTION_APPLIED) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(table_error(error)),
    };
    let Some(highest) = highest_marker_for_identity(&markers, identity)? else {
        return Ok(None);
    };
    extract_checkpoint_for_generation(
        &rows,
        &markers,
        identity,
        highest.key().generation(),
        highest.key().commit_sequence(),
    )
}

fn delete_prefix(table: &mut redb::Table<&[u8], &[u8]>, prefix: &[u8]) -> Result<(), StorageError> {
    let mut keys = Vec::new();
    for entry in table
        .range::<&[u8]>(prefix..)
        .map_err(precommit_storage_error)?
    {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        if !key.value().starts_with(prefix) {
            break;
        }
        keys.push(key.value().to_vec());
    }
    for key in keys {
        table
            .remove(key.as_slice())
            .map_err(precommit_storage_error)?;
    }
    Ok(())
}

fn write_checkpoint_to_tables(
    state_table: &mut redb::Table<&[u8], &[u8]>,
    marker_table: &mut redb::Table<&[u8], &[u8]>,
    identity: &ProjectionIdentity,
    target_generation: ProjectionGeneration,
    checkpoint: &CachedProjectionCheckpoint,
    clear_entire_identity: bool,
) -> Result<(), StorageError> {
    if clear_entire_identity {
        let state_prefix = super::projection_identity_state_prefix(identity);
        delete_prefix(state_table, &state_prefix)?;
        let marker_prefix = super::projection_identity_marker_prefix(identity);
        delete_prefix(marker_table, &marker_prefix)?;
    } else {
        let state_prefix =
            riffdb_types::ProjectionGroupPrefixBuilder::new(identity.clone(), target_generation)
                .finish();
        delete_prefix(state_table, state_prefix.as_bytes())?;
        let marker_prefix = super::projection_marker_namespace_prefix(identity, target_generation);
        delete_prefix(marker_table, &marker_prefix)?;
    }
    for row in &checkpoint.rows {
        let rekeyed = if row.generation() == target_generation {
            row.clone()
        } else {
            row.clone()
                .with_generation(target_generation)
                .map_err(super::stored_value_error)?
        };
        let encoded = encode_projection_state_structural_v1(&rekeyed)?;
        state_table
            .insert(
                encode_projection_group_key(rekeyed.key()),
                encoded.as_bytes(),
            )
            .map_err(precommit_storage_error)?;
    }
    for marker in &checkpoint.markers {
        let rekeyed = if marker.key().generation() == target_generation {
            marker.clone()
        } else {
            StoredProjectionApplyV1::new(
                ProjectionApplyKey::new(
                    identity.clone(),
                    target_generation,
                    marker.key().commit_sequence(),
                ),
                marker.canonical_hash(),
            )
        };
        let encoded = encode_projection_apply_v1(&rekeyed)?;
        marker_table
            .insert(
                encode_projection_apply_key(rekeyed.key()),
                encoded.as_bytes(),
            )
            .map_err(precommit_storage_error)?;
    }
    Ok(())
}

pub(crate) fn snapshot_derived_into_cache(
    derived_read: &redb::ReadTransaction,
    cache: &Mutex<BTreeMap<ProjectionIdentity, CachedProjectionCheckpoint>>,
) -> Result<(), StorageError> {
    let controls = match derived_read.open_table(PROJECTION_FRONTIER) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(error) => return Err(table_error(error)),
    };
    let rows = match derived_read.open_table(PROJECTION_STATE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(error) => return Err(table_error(error)),
    };
    let markers = match derived_read.open_table(PROJECTION_APPLIED) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(error) => return Err(table_error(error)),
    };
    let mut updates = Vec::new();
    for entry in controls.iter().map_err(precommit_storage_error)? {
        let (_, encoded) = entry.map_err(precommit_storage_error)?;
        let Ok(decoded) = decode_projection_control_v1(encoded.value()) else {
            continue;
        };
        let control = decoded.into_parts().0;
        let identity = control.identity();
        let Some(published) = control.published() else {
            continue;
        };
        let FrontierPosition::AppliedThrough(seq) = published.frontier() else {
            continue;
        };
        if super::source_base::exists(derived_read, identity, published.generation())? {
            continue;
        }
        if let Ok(Some(ckpt)) = extract_checkpoint_for_generation(
            &rows,
            &markers,
            identity,
            published.generation(),
            seq,
        ) {
            updates.push((identity.clone(), ckpt));
        }
    }
    if !updates.is_empty()
        && let Ok(mut guard) = cache.lock()
    {
        for (identity, ckpt) in updates {
            let replace = guard
                .get(&identity)
                .is_none_or(|existing| ckpt.frontier >= existing.frontier);
            if replace {
                guard.insert(identity, ckpt);
            }
        }
    }
    Ok(())
}

fn watermark_in_write(primary_write: &redb::WriteTransaction) -> Result<u64, StorageError> {
    let meta = match primary_write.open_table(crate::layout::META) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
        Err(error) => return Err(table_error(error)),
    };
    let Some(encoded) = meta
        .get(crate::layout::META_RETENTION_WATERMARK)
        .map_err(precommit_storage_error)?
    else {
        return Ok(0);
    };
    let decoded = riffdb_storage_api::proto_codec::decode_retention_watermark_v1(encoded.value())
        .map_err(crate::error::codec_error)?;
    Ok(decoded.value().watermark_sequence())
}

pub(crate) fn flush_derived_to_primary_write(
    derived_read: &redb::ReadTransaction,
    primary_write: &redb::WriteTransaction,
    max_seq: CommitSequence,
    force_latest: bool,
) -> Result<(), StorageError> {
    let watermark = watermark_in_write(primary_write)?;
    if watermark == 0 {
        return Ok(());
    }
    let derived_controls = match derived_read.open_table(PROJECTION_FRONTIER) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(error) => return Err(table_error(error)),
    };
    let derived_rows = match derived_read.open_table(PROJECTION_STATE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(error) => return Err(table_error(error)),
    };
    let derived_markers = match derived_read.open_table(PROJECTION_APPLIED) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(error) => return Err(table_error(error)),
    };
    let primary_controls = primary_write
        .open_table(PROJECTION_FRONTIER)
        .map_err(table_error)?;
    let mut prepared = Vec::new();
    {
        let primary_markers = primary_write
            .open_table(PROJECTION_APPLIED)
            .map_err(table_error)?;
        for entry in derived_controls.iter().map_err(precommit_storage_error)? {
            let (_, encoded) = entry.map_err(precommit_storage_error)?;
            let Ok(decoded) = decode_projection_control_v1(encoded.value()) else {
                continue;
            };
            let control = decoded.into_parts().0;
            let identity = control.identity();
            if read_projection_control(&primary_controls, identity)
                .ok()
                .flatten()
                .is_some()
            {
                continue;
            }
            let Some(published) = control.published() else {
                continue;
            };
            let FrontierPosition::AppliedThrough(seq) = published.frontier() else {
                continue;
            };
            if seq > max_seq
                || super::source_base::exists(derived_read, identity, published.generation())?
            {
                continue;
            }
            if let Ok(Some(existing)) = highest_marker_for_identity(&primary_markers, identity)
                && existing.key().generation() == published.generation()
            {
                let existing_seq = existing.key().commit_sequence().get();
                if existing_seq >= seq.get()
                    || (!force_latest
                        && existing_seq >= watermark
                        && seq.get().saturating_sub(existing_seq) < 4096)
                {
                    continue;
                }
            }
            if let Ok(Some(ckpt)) = extract_checkpoint_for_generation(
                &derived_rows,
                &derived_markers,
                identity,
                published.generation(),
                seq,
            ) {
                prepared.push((identity.clone(), ckpt));
            }
        }
    }
    drop(primary_controls);
    if !prepared.is_empty() {
        let mut primary_rows = primary_write
            .open_table(PROJECTION_STATE)
            .map_err(table_error)?;
        let mut primary_markers = primary_write
            .open_table(PROJECTION_APPLIED)
            .map_err(table_error)?;
        for (identity, ckpt) in prepared {
            write_checkpoint_to_tables(
                &mut primary_rows,
                &mut primary_markers,
                &identity,
                ckpt.generation,
                &ckpt,
                true,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn flush_shared_to_primary_write(
    shared: &crate::store::SharedRedb,
    primary_write: &redb::WriteTransaction,
    max_seq: CommitSequence,
    force_latest: bool,
) -> Result<(), StorageError> {
    let watermark = watermark_in_write(primary_write)?;
    if watermark == 0 {
        return Ok(());
    }
    let needs_derived_refresh = force_latest
        || shared
            .projection_checkpoints
            .lock()
            .map(|guard| {
                guard.is_empty()
                    || guard.values().any(|ckpt| {
                        ckpt.frontier.get() < watermark
                            || max_seq.get().saturating_sub(ckpt.frontier.get()) >= 4096
                    })
            })
            .unwrap_or(true);
    if needs_derived_refresh
        && let Some(derived_engine) = shared.derived_if_open()
        && let Ok(derived_read) = derived_engine.begin_read()
    {
        let _ = snapshot_derived_into_cache(&derived_read, &shared.projection_checkpoints);
    }
    let candidates: Vec<(ProjectionIdentity, ProjectionGeneration, CommitSequence)> = shared
        .projection_checkpoints
        .lock()
        .map(|guard| {
            guard
                .iter()
                .map(|(k, v)| (k.clone(), v.generation, v.frontier))
                .collect()
        })
        .unwrap_or_default();
    if candidates.is_empty() {
        return Ok(());
    }
    let primary_controls = primary_write
        .open_table(PROJECTION_FRONTIER)
        .map_err(table_error)?;
    let mut needed_identities = Vec::new();
    {
        let primary_markers = primary_write
            .open_table(PROJECTION_APPLIED)
            .map_err(table_error)?;
        for (identity, generation, frontier) in candidates {
            if frontier > max_seq {
                continue;
            }
            if read_projection_control(&primary_controls, &identity)
                .ok()
                .flatten()
                .is_some()
            {
                continue;
            }
            if let Ok(Some(existing)) = highest_marker_for_identity(&primary_markers, &identity)
                && existing.key().generation() == generation
            {
                let existing_seq = existing.key().commit_sequence().get();
                if existing_seq >= frontier.get()
                    || (!force_latest
                        && existing_seq >= watermark
                        && frontier.get().saturating_sub(existing_seq) < 4096)
                {
                    continue;
                }
            }
            needed_identities.push(identity);
        }
    }
    drop(primary_controls);
    if needed_identities.is_empty() {
        return Ok(());
    }
    let mut prepared = Vec::with_capacity(needed_identities.len());
    if let Ok(guard) = shared.projection_checkpoints.lock() {
        for identity in needed_identities {
            if let Some(ckpt) = guard.get(&identity)
                && ckpt.frontier <= max_seq
            {
                prepared.push((identity, ckpt.clone()));
            }
        }
    }
    if !prepared.is_empty() {
        let mut primary_rows = primary_write
            .open_table(PROJECTION_STATE)
            .map_err(table_error)?;
        let mut primary_markers = primary_write
            .open_table(PROJECTION_APPLIED)
            .map_err(table_error)?;
        for (identity, ckpt) in prepared {
            write_checkpoint_to_tables(
                &mut primary_rows,
                &mut primary_markers,
                &identity,
                ckpt.generation,
                &ckpt,
                true,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn seed_candidate_from_checkpoint(
    primary: &RedbReadAccess,
    cache: &Mutex<BTreeMap<ProjectionIdentity, CachedProjectionCheckpoint>>,
    derived_write: &redb::WriteTransaction,
    current_control: Option<&StoredProjectionControlV1>,
    identity: &ProjectionIdentity,
    candidate_generation: ProjectionGeneration,
    watermark: u64,
    head: FrontierPosition,
) -> Result<Option<CommitSequence>, StorageError> {
    if watermark == 0 {
        return Ok(None);
    }
    let FrontierPosition::AppliedThrough(head_seq) = head else {
        return Ok(None);
    };
    let mut best: Option<CachedProjectionCheckpoint> = None;
    if let Ok(Some(primary_ckpt)) = extract_primary_checkpoint(primary.deref(), identity)
        && primary_ckpt.frontier.get() >= watermark
        && primary_ckpt.frontier <= head_seq
    {
        best = Some(primary_ckpt);
    }
    if let Ok(guard) = cache.lock()
        && let Some(cached) = guard.get(identity)
        && cached.frontier.get() >= watermark
        && cached.frontier <= head_seq
        && best
            .as_ref()
            .is_none_or(|current| cached.frontier > current.frontier)
    {
        best = Some(cached.clone());
    }
    if let Some(published) = current_control.and_then(StoredProjectionControlV1::published)
        && let FrontierPosition::AppliedThrough(pub_seq) = published.frontier()
        && pub_seq.get() >= watermark
        && pub_seq <= head_seq
        && best
            .as_ref()
            .is_none_or(|current| pub_seq > current.frontier)
        && !super::source_base::exists_write(derived_write, identity, published.generation())?
    {
        let rows = derived_write
            .open_table(PROJECTION_STATE)
            .map_err(table_error)?;
        let markers = derived_write
            .open_table(PROJECTION_APPLIED)
            .map_err(table_error)?;
        if let Ok(Some(derived_ckpt)) = extract_checkpoint_for_generation(
            &rows,
            &markers,
            identity,
            published.generation(),
            pub_seq,
        ) {
            best = Some(derived_ckpt);
        }
    }
    let Some(ckpt) = best else {
        return Ok(None);
    };
    let mut rows = derived_write
        .open_table(PROJECTION_STATE)
        .map_err(table_error)?;
    let mut markers = derived_write
        .open_table(PROJECTION_APPLIED)
        .map_err(table_error)?;
    write_checkpoint_to_tables(
        &mut rows,
        &mut markers,
        identity,
        candidate_generation,
        &ckpt,
        false,
    )?;
    Ok(Some(ckpt.frontier))
}

pub(crate) fn read_recovery_checkpoint_from(
    primary: &RedbReadAccess,
    cache: &Mutex<BTreeMap<ProjectionIdentity, CachedProjectionCheckpoint>>,
    derived: &redb::ReadTransaction,
    schema: &CheckedProjectionSchema,
    generation: ProjectionGeneration,
) -> Result<Option<(CommitSequence, Vec<StoredProjectionStateV1>)>, StorageError> {
    let watermark = crate::retention::watermark_at_access(primary)?;
    if watermark == 0 || super::source_base::exists(derived, schema.identity(), generation)? {
        return Ok(None);
    }
    let controls = derived
        .open_table(PROJECTION_FRONTIER)
        .map_err(table_error)?;
    let Some(control) = read_projection_control(&controls, schema.identity())? else {
        return Ok(None);
    };
    let Some(FrontierPosition::AppliedThrough(retained_frontier)) =
        control.frontier_for(generation)
    else {
        return Ok(None);
    };
    let mut best: Option<CachedProjectionCheckpoint> = None;
    if let Ok(Some(primary_ckpt)) = extract_primary_checkpoint(primary.deref(), schema.identity())
        && primary_ckpt.frontier.get() >= watermark
        && primary_ckpt.frontier <= retained_frontier
    {
        best = Some(primary_ckpt);
    }
    if let Ok(guard) = cache.lock()
        && let Some(cached) = guard.get(schema.identity())
        && cached.frontier.get() >= watermark
        && cached.frontier <= retained_frontier
        && best
            .as_ref()
            .is_none_or(|current| cached.frontier > current.frontier)
    {
        best = Some(cached.clone());
    }
    let Some(ckpt) = best else {
        return Ok(None);
    };
    let mut checked_rows = Vec::with_capacity(ckpt.rows.len());
    for row in ckpt.rows {
        let rekeyed = if row.generation() == generation {
            row
        } else {
            row.with_generation(generation)
                .map_err(super::stored_value_error)?
        };
        let checked = rekeyed
            .into_checked(schema)
            .map_err(super::stored_value_error)?;
        checked_rows.push(checked);
    }
    Ok(Some((ckpt.frontier, checked_rows)))
}
