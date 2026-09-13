//! Exact physical Immediate receipt application. Operation owners must first
//! validate their command/audit/control records and source attribution. This
//! boundary owns expected-state application and atomic history publication, not
//! application authorization, startup validation, or a follower apply permit.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "WP-772 direct operation owners and startup activation are being integrated"
    )
)]

use std::collections::BTreeSet;

use redb::{Database, Durability, ReadableTable, TableDefinition, TableHandle, WriteTransaction};
use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3,
    ChangelogAttributionV3, ChangelogV3Error, MAX_CHANGELOG_FRAME_BYTES, StorageError,
    StorageErrorKind,
    proto_codec::{encode_changelog_history_state_v3, encode_changelog_transaction_allocator_v3},
};

use crate::{
    changelog_v3_activation::HISTORY,
    changelog_v3_roots::read_checkpoint_roots_for_write,
    error::{codec_error, precommit_storage_error, storage_error, table_error, transaction_error},
    layout::META,
    store::{RedbCommitProfile, SharedRedb},
};

/// Owns the transaction after its exact receipt and all mutations have been
/// staged. No raw transaction or writable table is returned to a caller, so
/// subsequent unreceipted writes cannot be added before commit. Dropping aborts.
pub(crate) struct PreparedImmediateReceipt {
    transaction: WriteTransaction,
}

impl PreparedImmediateReceipt {
    /// Applies a validated operation's complete net mutation plan in a fresh
    /// transaction owned from creation to commit. No earlier unreceipted writes
    /// can enter it. Every expected state is checked before applying any row.
    /// The caller retains the operation's existing exclusive/drained write gate.
    /// Journal checkpointing and lineage/control-only ceremonies have separate
    /// owners and cannot be smuggled through this direct mutation boundary.
    pub(crate) fn apply(
        database: &Database,
        profile: RedbCommitProfile,
        receipt: &AuthoritativeTransactionV3,
    ) -> Result<Self, StorageError> {
        use ChangelogAttributionV3 as A;
        if matches!(
            receipt.attribution(),
            A::JournaledApplicationGroup
                | A::JournaledServiceAudit
                | A::V3Activation
                | A::V3Rotation
                | A::FollowerApply
                | A::Promotion
                | A::RestoreAnchor
                | A::CleanClose
                | A::DirtyActivation
                | A::HistoryReclamation
                | A::ReplicationSourceHold
        ) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let mut transaction = database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(profile.uses_two_phase());
        let history = read_checkpoint_roots_for_write(&transaction)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        let (assigned, allocator) = history
            .expected_allocator()
            .allocate_one()
            .map_err(value_error)?;
        if assigned != receipt.binding().sequence {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let successor = history.advance(receipt).map_err(value_error)?;
        let encoded = receipt.encode().map_err(value_error)?;
        let encoded_history = encode_changelog_history_state_v3(successor).map_err(codec_error)?;
        let encoded_allocator =
            encode_changelog_transaction_allocator_v3(allocator).map_err(codec_error)?;
        let tables = table_inventory(&transaction)?;
        for mutation in receipt.mutations() {
            if !tables.contains(mutation.namespace().table()) {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            check_predecessor(&transaction, mutation)?;
        }
        let position = assigned.get().to_be_bytes();
        if transaction
            .open_table(HISTORY)
            .map_err(table_error)?
            .get(position.as_slice())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        // Preserve the caller's existing commit profile. No extra commit,
        // journal flush, or acknowledgement dependency is introduced.
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        for mutation in receipt.mutations() {
            apply_mutation(&transaction, mutation)?;
        }
        #[cfg(test)]
        crash_edge("mutations");
        transaction
            .open_table(HISTORY)
            .map_err(table_error)?
            .insert(position.as_slice(), encoded.as_slice())
            .map_err(precommit_storage_error)?;
        #[cfg(test)]
        crash_edge("receipt");
        {
            let mut meta = transaction.open_table(META).map_err(table_error)?;
            meta.insert(
                metadata_key(N::NextChangelogTransaction)?,
                encoded_allocator.as_bytes(),
            )
            .map_err(precommit_storage_error)?;
            meta.insert(
                metadata_key(N::ChangelogHistoryState)?,
                encoded_history.as_bytes(),
            )
            .map_err(precommit_storage_error)?;
        }
        #[cfg(test)]
        crash_edge("roots");
        // Recheck complete transaction-current roots, including actual retained
        // application/admin allocators, lineage and the exact staged tail row.
        // A declared frontier cannot diverge from these physical post-images.
        if read_checkpoint_roots_for_write(&transaction)? != Some(successor) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(Self { transaction })
    }

    /// Commits through the existing durable-root publication/fault owner.
    #[expect(
        dead_code,
        reason = "WP-772 production direct callers are not activated yet"
    )]
    pub(crate) fn commit(self, shared: &SharedRedb) -> Result<(), StorageError> {
        shared.commit_durable(self.transaction)
    }

    #[cfg(test)]
    pub(crate) fn commit_for_test(self) -> Result<(), StorageError> {
        self.transaction
            .commit()
            .map_err(crate::error::commit_error)?;
        crash_edge("committed");
        Ok(())
    }
}

#[cfg(test)]
fn crash_edge(edge: &str) {
    if std::env::var("RIFFDB_V3_DIRECT_EDGE").ok().as_deref() == Some(edge) {
        std::process::exit(93);
    }
}

fn metadata_key(namespace: N) -> Result<&'static str, StorageError> {
    namespace
        .metadata_key()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
}

fn value_error(error: ChangelogV3Error) -> StorageError {
    storage_error(match error {
        ChangelogV3Error::LimitExceeded => StorageErrorKind::LimitExceeded,
        ChangelogV3Error::SequenceExhausted => StorageErrorKind::SequenceExhausted,
        _ => StorageErrorKind::CorruptData,
    })
}

fn table_inventory(transaction: &WriteTransaction) -> Result<BTreeSet<&'static str>, StorageError> {
    if transaction
        .list_multimap_tables()
        .map_err(precommit_storage_error)?
        .next()
        .is_some()
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let mut tables = BTreeSet::new();
    for (count, table) in transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .enumerate()
    {
        if count >= N::ALL.len() {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let namespace = N::ALL
            .into_iter()
            .find(|namespace| namespace.table() == table.name())
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        tables.insert(namespace.table());
    }
    Ok(tables)
}

fn check_predecessor(
    transaction: &WriteTransaction,
    mutation: &AuthoritativeMutationV3,
) -> Result<(), StorageError> {
    let check = |before: Option<&[u8]>| {
        if before.is_some_and(|bytes| bytes.len() > MAX_CHANGELOG_FRAME_BYTES) {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        if !mutation.matches_prior(before) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(())
    };
    if let Some(key) = mutation.namespace().metadata_key() {
        let table = transaction.open_table(META).map_err(table_error)?;
        let before = table.get(key).map_err(precommit_storage_error)?;
        check(before.as_ref().map(|row| row.value()))
    } else {
        let definition: TableDefinition<&[u8], &[u8]> =
            TableDefinition::new(mutation.namespace().table());
        let table = transaction.open_table(definition).map_err(table_error)?;
        let before = table.get(mutation.key()).map_err(precommit_storage_error)?;
        check(before.as_ref().map(|row| row.value()))
    }
}

fn apply_mutation(
    transaction: &WriteTransaction,
    mutation: &AuthoritativeMutationV3,
) -> Result<(), StorageError> {
    if let Some(key) = mutation.namespace().metadata_key() {
        let mut table = transaction.open_table(META).map_err(table_error)?;
        if let Some(value) = mutation.value() {
            table.insert(key, value).map_err(precommit_storage_error)?;
        } else {
            table.remove(key).map_err(precommit_storage_error)?;
        }
    } else {
        let definition: TableDefinition<&[u8], &[u8]> =
            TableDefinition::new(mutation.namespace().table());
        let mut table = transaction.open_table(definition).map_err(table_error)?;
        if let Some(value) = mutation.value() {
            table
                .insert(mutation.key(), value)
                .map_err(precommit_storage_error)?;
        } else {
            table
                .remove(mutation.key())
                .map_err(precommit_storage_error)?;
        }
    }
    Ok(())
}
