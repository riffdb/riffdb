//! Bounded follower/source lifecycle separation under ADR-0186 amendment 2.
//! These checks never grant readiness or replace authoritative validation.

use redb::{ReadTransaction, TableError};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, ReplicationFollowerStateV3, StorageError, StorageErrorKind,
    proto_codec::decode_replication_follower_state_v3,
};
use riffdb_types::DatabaseId;

use crate::{
    error::{codec_error, precommit_storage_error, storage_error, table_error},
    layout::{META, META_CLEAN_CLOSE_LIFECYCLE},
};

fn state(
    transaction: &ReadTransaction,
) -> Result<Option<ReplicationFollowerStateV3>, StorageError> {
    let meta = match transaction.open_table(META) {
        Ok(meta) => meta,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(table_error(error)),
    };
    let key = N::ReplicationFollowerState
        .metadata_key()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    let Some(row) = meta.get(key).map_err(precommit_storage_error)? else {
        // Only a routing probe: the existing full V3 validator owns missing-root
        // refusal after activation. Absence here never grants activation.
        return Ok(None);
    };
    Ok(Some(
        *decode_replication_follower_state_v3(row.value())
            .map_err(codec_error)?
            .value(),
    ))
}

/// Conservative source-mode admission probe, before recovery or worker creation.
pub(crate) fn is_attached(transaction: &ReadTransaction) -> Result<bool, StorageError> {
    Ok(state(transaction)?.is_some_and(|state| state.attached_state().is_some()))
}

/// Refuses conflicting source lifecycle evidence and binds the exact applied
/// root before skipping a source-only lifecycle mutation. Caller owns exclusion.
pub(crate) fn preserve_attached_lifecycle(
    transaction: &ReadTransaction,
    database_id: DatabaseId,
    history_incarnation: u64,
) -> Result<bool, StorageError> {
    let Some((lineage, applied, _)) = state(transaction)?.and_then(|state| state.attached_state())
    else {
        return Ok(false);
    };
    let corrupt = || storage_error(StorageErrorKind::CorruptData);
    let history =
        crate::changelog_v3_roots::read_checkpoint_roots(transaction)?.ok_or_else(corrupt)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    if lineage.database_id() != database_id
        || lineage.history_incarnation() != history_incarnation
        || lineage != history.lineage()
        || applied != history.tail()
        || meta
            .get(META_CLEAN_CLOSE_LIFECYCLE)
            .map_err(precommit_storage_error)?
            .is_some()
    {
        return Err(corrupt());
    }
    Ok(true)
}
