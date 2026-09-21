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
    admission: Option<CanonicalStoredEnvelopeV1>,
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
            crate::primary_admission_roots::require_unfenced(&meta)?;
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
        Self::at_frontier(predecessor, predecessor.tail().frontier(), incarnation).map(Some)
    }

    pub(super) fn prepare_private(
        transaction: &WriteTransaction,
        binding: crate::maintenance::PrivateArchiveValidationBinding,
        incarnation: u64,
    ) -> Result<Self, StorageError> {
        binding.validate_for_write(transaction)?;
        crate::primary_admission_roots::require_unfenced(
            &transaction.open_table(META).map_err(table_error)?,
        )?;
        if incarnation <= binding.predecessor().lineage().history_incarnation() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        Self::at_frontier(binding.predecessor(), binding.frontier(), incarnation)
    }

    fn at_frontier(
        predecessor: ChangelogHistoryStateV3,
        frontier: riffdb_types::DualFrontier,
        incarnation: u64,
    ) -> Result<Self, StorageError> {
        let corrupt = || storage_error(StorageErrorKind::CorruptData);
        let lineage = ChangelogLineageV3::new_with_catalog(
            predecessor.lineage().database_id(),
            incarnation,
            predecessor.lineage().leadership_epoch(),
            predecessor.lineage().catalog_digest(),
        )
        .map_err(|_| corrupt())?;
        let (sequence, allocator) = ChangelogTransactionAllocator::initial()
            .allocate_one()
            .map_err(|_| storage_error(StorageErrorKind::SequenceExhausted))?;
        let receipt = AuthoritativeTransactionV3::new_for_catalog(
            AuthoritativeTransactionBindingV3 {
                database_id: lineage.database_id(),
                history_incarnation: incarnation,
                predecessor: None,
                sequence,
                predecessor_frontier: frontier,
                covered_frontier: frontier,
                prior_history_hash: [0; 32],
            },
            ChangelogAttributionV3::RestoreAnchor,
            vec![],
            lineage.catalog_digest(),
        )
        .map_err(|_| corrupt())?;
        let point = ChangelogHistoryPointV3::from_receipt(&receipt).map_err(|_| corrupt())?;
        let successor =
            ChangelogHistoryStateV3::new(lineage, point, point, point).map_err(|_| corrupt())?;
        Ok(Self {
            predecessor,
            successor,
            history: encode_changelog_history_state_v3(successor).map_err(codec_error)?,
            allocator: encode_changelog_transaction_allocator_v3(allocator).map_err(codec_error)?,
            follower: encode_replication_follower_state_v3(ReplicationFollowerStateV3::detached())
                .map_err(codec_error)?,
            admission: if lineage.catalog_digest()
                == riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
            {
                Some(
                    encode_replication_primary_admission_v1(
                        &riffdb_storage_api::ReplicationPrimaryAdmissionV1::active(lineage)
                            .map_err(|_| corrupt())?,
                    )
                    .map_err(codec_error)?,
                )
            } else {
                None
            },
            receipt: receipt.encode().map_err(|_| corrupt())?,
        })
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
            if let Some(admission) = &self.admission {
                meta.insert(crate::primary_admission_roots::key()?, admission.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
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

/// The offline watermark owner has no operational validation permit. Validate
/// the original write pin completely, including on a no-op retry. Final fresh
/// activation still owns removal of the shared transitional presence routing.
pub(super) fn watermark_history(
    transaction: &WriteTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    if !crate::changelog_v3_journal::has_write_recovery_roots(transaction)? {
        return Ok(None);
    }
    let history = validate_retained_history_for_write(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    Ok(Some(history))
}

/// Prepare the exact watermark before/after transition before its original
/// mutation. A retention normalization stays in the same lineage and therefore
/// cannot use RestoreAnchor or discard the old receipt chain.
pub(super) fn prepare_watermark_receipt(
    transaction: &WriteTransaction,
    history: Option<ChangelogHistoryStateV3>,
    encoded: &CanonicalStoredEnvelopeV1,
) -> Result<Option<crate::changelog_v3_write::PreparedHistoryAdvance>, StorageError> {
    use crate::changelog_v3_write::{PreparedHistoryAdvance, value_error};
    use riffdb_storage_api::AuthoritativeMutationV3;
    let Some(history) = history else {
        return Ok(None);
    };
    let key = key(N::RetentionWatermark)?;
    let mutation = {
        let meta = transaction.open_table(META).map_err(table_error)?;
        let before = meta.get(key).map_err(precommit_storage_error)?;
        match before {
            Some(before) => AuthoritativeMutationV3::replace(
                N::RetentionWatermark,
                key.as_bytes(),
                before.value(),
                encoded.as_bytes(),
            ),
            None => AuthoritativeMutationV3::put(
                N::RetentionWatermark,
                key.as_bytes(),
                None,
                encoded.as_bytes(),
            ),
        }
        .map_err(value_error)?
    };
    let receipt = AuthoritativeTransactionV3::new_for_catalog(
        AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence: history
                .expected_allocator()
                .allocate_one()
                .map_err(value_error)?
                .0,
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: history.tail().frontier(),
            prior_history_hash: history.tail().history_hash(),
        },
        ChangelogAttributionV3::RetentionPrune,
        vec![mutation],
        history.lineage().catalog_digest(),
    )
    .map_err(value_error)?;
    PreparedHistoryAdvance::prepare(transaction, &receipt).map(Some)
}

pub(super) fn edge(_name: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_WP772_RESTORE_CRASH").as_deref() == Ok(_name) {
        std::process::exit(93);
    }
}
