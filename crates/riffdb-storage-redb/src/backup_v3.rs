//! Prepared V3 lineage reset inside the existing offline incarnation transaction.

use crate::{
    changelog_v3_activation::{HISTORY, SOURCE_HOLDS},
    changelog_v3_roots::validate_retained_history_for_write,
    error::{codec_error, precommit_storage_error, storage_error, table_error},
    layout::{META, META_HISTORY_INCARNATION},
};
use redb::{ReadableTable, WriteTransaction};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3,
    ChangelogAttributionV3, ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ChangelogTransactionAllocator, ReplicationAuthorityClassV1, ReplicationFollowerStateV3,
    ReplicationTransferV1, StorageError, StorageErrorKind, proto_codec::*,
};

pub(super) struct PreparedRestoreAnchor {
    predecessor: ChangelogHistoryStateV3,
    successor: ChangelogHistoryStateV3,
    history: CanonicalStoredEnvelopeV1,
    allocator: CanonicalStoredEnvelopeV1,
    follower: CanonicalStoredEnvelopeV1,
    receipt: Vec<u8>,
}

impl PreparedRestoreAnchor {
    pub(super) fn prepare(
        transaction: &WriteTransaction,
        incarnation: u64,
    ) -> Result<Option<Self>, StorageError> {
        // Same transitional routing as the existing journal and Immediate owners.
        // Final activation still must make total current-registry root erasure
        // unconditionally fatal. Any present V3 control already requires strict
        // validation; this is not permission to repair a partial V3 database.
        if !crate::changelog_v3_journal::has_write_recovery_roots(transaction)? {
            return Ok(None);
        }
        let corrupt = || storage_error(StorageErrorKind::CorruptData);
        let predecessor = validate_retained_history_for_write(transaction)?.ok_or_else(corrupt)?;
        {
            let meta = transaction.open_table(META).map_err(table_error)?;
            if let Some(encoded) = meta
                .get(key(N::CleanCloseLifecycle)?)
                .map_err(precommit_storage_error)?
            {
                let lifecycle = crate::clean_close::CleanCloseLifecycle::decode(encoded.value())
                    .map_err(|_| corrupt())?;
                if lifecycle.database_id() != predecessor.lineage().database_id()
                    || lifecycle.history_incarnation()
                        != predecessor.lineage().history_incarnation()
                {
                    return Err(corrupt());
                }
            }
        }
        if incarnation < predecessor.lineage().history_incarnation() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if incarnation == predecessor.lineage().history_incarnation() {
            return Ok(None);
        }
        let lineage = ChangelogLineageV3::new(
            predecessor.lineage().database_id(),
            incarnation,
            predecessor.lineage().leadership_epoch(),
        )
        .map_err(|_| corrupt())?;
        let (sequence, allocator) = ChangelogTransactionAllocator::initial()
            .allocate_one()
            .map_err(|_| storage_error(StorageErrorKind::SequenceExhausted))?;
        let receipt = AuthoritativeTransactionV3::new(
            AuthoritativeTransactionBindingV3 {
                database_id: lineage.database_id(),
                history_incarnation: incarnation,
                predecessor: None,
                sequence,
                predecessor_frontier: predecessor.tail().frontier(),
                covered_frontier: predecessor.tail().frontier(),
                prior_history_hash: [0; 32],
            },
            ChangelogAttributionV3::RestoreAnchor,
            vec![],
        )
        .map_err(|_| corrupt())?;
        let point = ChangelogHistoryPointV3::from_receipt(&receipt).map_err(|_| corrupt())?;
        let successor =
            ChangelogHistoryStateV3::new(lineage, point, point, point).map_err(|_| corrupt())?;
        Ok(Some(Self {
            predecessor,
            successor,
            history: encode_changelog_history_state_v3(successor).map_err(codec_error)?,
            allocator: encode_changelog_transaction_allocator_v3(allocator).map_err(codec_error)?,
            follower: encode_replication_follower_state_v3(ReplicationFollowerStateV3::detached())
                .map_err(codec_error)?,
            receipt: receipt.encode().map_err(|_| corrupt())?,
        }))
    }

    pub(super) fn stage(self, transaction: &WriteTransaction) -> Result<(), StorageError> {
        let corrupt = || storage_error(StorageErrorKind::CorruptData);
        {
            let meta = transaction.open_table(META).map_err(table_error)?;
            let incarnation = meta
                .get(META_HISTORY_INCARNATION)
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            let old_history = meta
                .get(key(N::ChangelogHistoryState)?)
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            if *decode_history_incarnation_v1(incarnation.value())
                .map_err(codec_error)?
                .value()
                != self.successor.lineage().history_incarnation()
                || *decode_changelog_history_state_v3(old_history.value())
                    .map_err(codec_error)?
                    .value()
                    != self.predecessor
            {
                return Err(corrupt());
            }
        }
        // Exact source-only table targets; no replicated-authoritative table
        // or surviving same-lineage receipt is rewritten by reclamation here.
        if !transaction.delete_table(HISTORY).map_err(table_error)?
            || !transaction
                .delete_table(SOURCE_HOLDS)
                .map_err(table_error)?
        {
            return Err(corrupt());
        }
        drop(transaction.open_table(SOURCE_HOLDS).map_err(table_error)?);
        {
            let mut meta = transaction.open_table(META).map_err(table_error)?;
            for namespace in N::ALL {
                if namespace.class()
                    == ReplicationAuthorityClassV1::ReplicationControl(
                        ReplicationTransferV1::SourceOnly,
                    )
                    && let Some(key) = namespace.metadata_key()
                {
                    meta.remove(key).map_err(precommit_storage_error)?;
                }
            }
            meta.insert(key(N::ReplicationFollowerState)?, self.follower.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        edge("local-reset");
        {
            let mut meta = transaction.open_table(META).map_err(table_error)?;
            meta.insert(key(N::ChangelogHistoryState)?, self.history.as_bytes())
                .map_err(precommit_storage_error)?;
            meta.insert(key(N::NextChangelogTransaction)?, self.allocator.as_bytes())
                .map_err(precommit_storage_error)?;
        }
        transaction
            .open_table(HISTORY)
            .map_err(table_error)?
            .insert(
                self.successor
                    .tail()
                    .sequence()
                    .get()
                    .to_be_bytes()
                    .as_slice(),
                self.receipt.as_slice(),
            )
            .map_err(precommit_storage_error)?;
        if validate_retained_history_for_write(transaction)? != Some(self.successor) {
            return Err(corrupt());
        }
        edge("anchor");
        Ok(())
    }
}

fn key(namespace: N) -> Result<&'static str, StorageError> {
    namespace
        .metadata_key()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
}

pub(super) fn edge(_name: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_WP772_RESTORE_CRASH").as_deref() == Ok(_name) {
        std::process::exit(93);
    }
}
