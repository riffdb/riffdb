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

use redb::{
    Database, Durability, Key, ReadableTable, TableDefinition, TableHandle, WriteTransaction,
};
use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3,
    AuthoritativeTransactionV3, ChangelogAttributionV3, ChangelogHistoryStateV3, ChangelogV3Error,
    MAX_CHANGELOG_FRAME_BYTES, StorageError, StorageErrorKind,
    proto_codec::{encode_changelog_history_state_v3, encode_changelog_transaction_allocator_v3},
};

use crate::{
    changelog_v3_activation::HISTORY,
    changelog_v3_capture::{CapturedTable, MutationCapture},
    changelog_v3_roots::{decode_physical_frontier, read_checkpoint_roots_for_write},
    error::{codec_error, precommit_storage_error, storage_error, table_error, transaction_error},
    layout::{META, META_ADMINISTRATION_SEQUENCE, META_APPLICATION_SEQUENCE},
    store::{RedbCommitProfile, SharedRedb},
};

/// Owns the transaction after its exact receipt and all mutations have been
/// staged. No raw transaction or writable table is returned to a caller, so
/// subsequent unreceipted writes cannot be added before commit. Dropping aborts.
pub(crate) struct PreparedImmediateReceipt {
    transaction: WriteTransaction,
    footprint: crate::source_continuation::SourceContinuationFootprint,
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
        require_direct_attribution(receipt.attribution())?;
        let mut transaction = database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(profile.uses_two_phase());
        let history = read_checkpoint_roots_for_write(&transaction)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let footprint = crate::source_continuation::SourceContinuationFootprint::captured(
            history,
            receipt.mutations(),
        );
        let advance = PreparedHistoryAdvance::prepare(&transaction, receipt)?;
        let tables = table_inventory(&transaction)?;
        for mutation in receipt.mutations() {
            if !tables.contains(mutation.namespace().table()) {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            check_predecessor(&transaction, mutation)?;
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
        advance.stage(&transaction)?;
        Ok(Self {
            transaction,
            footprint,
        })
    }

    /// Commits through the existing durable-root publication/fault owner.
    pub(crate) fn commit(self, shared: &SharedRedb) -> Result<(), StorageError> {
        shared.commit_with_source_footprint(self.transaction, self.footprint)?;
        #[cfg(test)]
        crash_edge("committed");
        Ok(())
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

pub(crate) fn require_direct_attribution(
    source: ChangelogAttributionV3,
) -> Result<(), StorageError> {
    use ChangelogAttributionV3 as A;
    if matches!(
        source,
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
            | A::PrimaryFence
    ) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    Ok(())
}

/// Owns one existing-profile engine transaction from its fresh root through
/// capture and sealing. No raw write transaction/table escapes, and finishing
/// never opens a replacement transaction. Operation semantics and the existing
/// exclusive/drained gate remain the caller's responsibility.
pub(crate) struct CapturedImmediateWrite {
    transaction: WriteTransaction,
    history: Option<ChangelogHistoryStateV3>,
    capture: MutationCapture,
    tables: BTreeSet<&'static str>,
    source: ChangelogAttributionV3,
    footprint: crate::source_continuation::SourceContinuationFootprint,
}

impl CapturedImmediateWrite {
    pub(crate) fn begin(
        database: &Database,
        profile: RedbCommitProfile,
        source: ChangelogAttributionV3,
    ) -> Result<Self, StorageError> {
        require_direct_attribution(source)?;
        let mut transaction = database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(profile.uses_two_phase());
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let captured = Self::from_checked_drain(
            transaction,
            source,
            crate::source_continuation::SourceContinuationFootprint::fresh(),
        )?;
        if captured.history.is_none() {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        Ok(captured)
    }

    /// Takes the caller's existing Immediate transaction AFTER its published
    /// journal suffix has been materialized in that same transaction. Capture
    /// begins at that exact predecessor; draining cannot become a second direct
    /// receipt. No transaction, profile change or commit is introduced here.
    pub(crate) fn from_drained(
        transaction: WriteTransaction,
        source: ChangelogAttributionV3,
    ) -> Result<Self, StorageError> {
        Self::from_checked_drain(
            transaction,
            source,
            crate::source_continuation::SourceContinuationFootprint::unknown(),
        )
    }

    pub(crate) fn from_checked_drain(
        transaction: WriteTransaction,
        source: ChangelogAttributionV3,
        footprint: crate::source_continuation::SourceContinuationFootprint,
    ) -> Result<Self, StorageError> {
        require_direct_attribution(source)?;
        // Transitional routing matches the existing journal owner. Any V3
        // control requires strict roots; final startup activation must make
        // current-registry root erasure unconditionally fatal before this lane.
        let history = if crate::changelog_v3_journal::has_write_recovery_roots(&transaction)? {
            Some(
                read_checkpoint_roots_for_write(&transaction)?
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?,
            )
        } else {
            None
        };
        if let Some(history) = history {
            history
                .expected_allocator()
                .allocate_one()
                .map_err(value_error)?;
        }
        let tables = table_inventory(&transaction)?;
        Ok(Self {
            transaction,
            history,
            capture: if history.is_some() {
                MutationCapture::default()
            } else {
                MutationCapture::for_inactive_legacy()
            },
            tables,
            source,
            footprint,
        })
    }

    pub(crate) fn open_table<K: Key + 'static>(
        &self,
        definition: TableDefinition<K, &'static [u8]>,
    ) -> Result<CapturedTable<'_, '_, K>, StorageError> {
        self.capture.ensure_healthy()?;
        if !self.tables.contains(definition.name()) {
            return Err(self.capture.refuse(StorageErrorKind::CorruptData));
        }
        let table = self
            .transaction
            .open_table(definition)
            .map_err(|error| self.capture.refuse(table_error(error).kind()))?;
        Ok(self.capture.table(table))
    }

    pub(crate) fn finish(self) -> Result<PreparedImmediateReceipt, StorageError> {
        let mutations = self.capture.finish()?;
        let mut footprint = self.footprint;
        footprint.combine(self.history.map_or(
            crate::source_continuation::SourceContinuationFootprint::unknown(),
            |history| {
                crate::source_continuation::SourceContinuationFootprint::captured(
                    history, &mutations,
                )
            },
        ));
        if !mutations.is_empty() {
            let history = self
                .history
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let frontier = {
                let meta = self.transaction.open_table(META).map_err(table_error)?;
                let application = meta
                    .get(META_APPLICATION_SEQUENCE)
                    .map_err(precommit_storage_error)?
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
                let administration = meta
                    .get(META_ADMINISTRATION_SEQUENCE)
                    .map_err(precommit_storage_error)?
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
                decode_physical_frontier(application.value(), administration.value())?
            };
            let binding = AuthoritativeTransactionBindingV3 {
                database_id: history.lineage().database_id(),
                history_incarnation: history.lineage().history_incarnation(),
                predecessor: Some(history.tail().sequence()),
                sequence: history
                    .expected_allocator()
                    .allocate_one()
                    .map_err(value_error)?
                    .0,
                predecessor_frontier: history.tail().frontier(),
                covered_frontier: frontier,
                prior_history_hash: history.tail().history_hash(),
            };
            let receipt = AuthoritativeTransactionV3::new_for_catalog(
                binding,
                self.source,
                mutations,
                history.lineage().catalog_digest(),
            )
            .map_err(value_error)?;
            #[cfg(test)]
            crash_edge("mutations");
            PreparedHistoryAdvance::from_captured_predecessor(
                &self.transaction,
                history,
                &receipt,
            )?
            .stage(&self.transaction)?;
        }
        Ok(PreparedImmediateReceipt {
            transaction: self.transaction,
            footprint,
        })
    }

    pub(crate) fn abort(self) -> Result<(), redb::StorageError> {
        self.transaction.abort()
    }

    pub(crate) fn read_command_value(
        &self,
        table: crate::journal::JournalTable,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, crate::journal::JournalIoError> {
        crate::journal::read_write_value(&self.transaction, table, key)
    }

    /// Reuses the existing journal mutation validator for direct command
    /// operations, recording its exact transaction-current before image first.
    /// A failure poisons capture so an ignored error cannot commit a prefix.
    pub(crate) fn apply_journal_mutation(
        &self,
        mutation: &crate::journal::JournalMutation,
    ) -> Result<(), StorageError> {
        // Borrow before-images from the engine; capture checks bounds before
        // retaining any bytes. Do not allocate a copy of an untrusted row.
        let capture = || -> Result<(), StorageError> {
            if mutation.table() == crate::journal::JournalTable::Meta {
                let key = std::str::from_utf8(mutation.key())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                let table = self.transaction.open_table(META).map_err(table_error)?;
                let before = table.get(key).map_err(precommit_storage_error)?;
                self.capture
                    .record(
                        mutation.table().label(),
                        mutation.key(),
                        before.as_ref().map(|row| row.value()),
                        mutation.value(),
                    )
                    .map_err(precommit_storage_error)
            } else {
                let definition = crate::journal::byte_table_definition(mutation.table())
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                if !self.tables.contains(definition.name()) {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                let table = self
                    .transaction
                    .open_table(definition)
                    .map_err(table_error)?;
                let before = table.get(mutation.key()).map_err(precommit_storage_error)?;
                self.capture
                    .record(
                        mutation.table().label(),
                        mutation.key(),
                        before.as_ref().map(|row| row.value()),
                        mutation.value(),
                    )
                    .map_err(precommit_storage_error)
            }
        };
        capture().map_err(|error| self.capture.refuse(error.kind()))?;
        crate::journal::apply_mutation(&self.transaction, mutation)
            .map_err(|_| self.capture.refuse(StorageErrorKind::CorruptData))
    }
}

/// Shared non-recursive history writes within an existing Immediate transaction.
/// This is not an operation permit and never commits: its caller must validate
/// and apply the exact source first, and abort the transaction on any failure.
pub(crate) struct PreparedHistoryAdvance {
    successor: ChangelogHistoryStateV3,
    encoded: Option<Vec<u8>>,
    encoded_history: Vec<u8>,
    encoded_allocator: Vec<u8>,
}

impl PreparedHistoryAdvance {
    pub(crate) fn prepare(
        transaction: &WriteTransaction,
        receipt: &AuthoritativeTransactionV3,
    ) -> Result<Self, StorageError> {
        let history = read_checkpoint_roots_for_write(transaction)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        Self::from_captured_predecessor(transaction, history, receipt)
    }

    pub(crate) fn prepare_stub(
        transaction: &WriteTransaction,
        receipt: &AuthoritativeTransactionV3,
    ) -> Result<Self, StorageError> {
        let history = read_checkpoint_roots_for_write(transaction)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        Self::from_predecessor(transaction, history, receipt, false)
    }

    /// [`Self::prepare`] for the next frame of one multi-frame checkpoint
    /// transaction, whose predecessor state is the successor the previous
    /// frame staged in this same transaction rather than a fresh root read.
    ///
    /// The caller must have fully validated the roots before the batch's first
    /// frame and must fully validate them again after its last
    /// ([`read_checkpoint_roots_for_write`]); no intermediate state is durable
    /// or visible. Per frame this still refuses a stored history or allocator
    /// row that differs from `history`, a receipt the admission roots refuse,
    /// a mismatched allocation, an occupied history slot, and a physical
    /// application/administration frontier that differs from the chain tail.
    pub(crate) fn prepare_chained(
        transaction: &WriteTransaction,
        history: ChangelogHistoryStateV3,
        receipt: &AuthoritativeTransactionV3,
    ) -> Result<Self, StorageError> {
        Self::prepare_chained_with_encoding(transaction, history, receipt, true)
    }

    /// Chained preparation for a retained frame that persists a stub rather
    /// than canonical receipt bytes. Keep the same per-frame root checks.
    pub(crate) fn prepare_stub_chained(
        transaction: &WriteTransaction,
        history: ChangelogHistoryStateV3,
        receipt: &AuthoritativeTransactionV3,
    ) -> Result<Self, StorageError> {
        Self::prepare_chained_with_encoding(transaction, history, receipt, false)
    }

    fn prepare_chained_with_encoding(
        transaction: &WriteTransaction,
        history: ChangelogHistoryStateV3,
        receipt: &AuthoritativeTransactionV3,
        encode_receipt: bool,
    ) -> Result<Self, StorageError> {
        {
            let meta = transaction.open_table(META).map_err(table_error)?;
            let read = |key: &str| -> Result<Vec<u8>, StorageError> {
                let value = meta
                    .get(key)
                    .map_err(precommit_storage_error)?
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
                // The same fixed-root envelope ceiling as the full root read.
                if value.value().len() > 512 {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                Ok(value.value().to_vec())
            };
            let frontier = decode_physical_frontier(
                &read(META_APPLICATION_SEQUENCE)?,
                &read(META_ADMINISTRATION_SEQUENCE)?,
            )?;
            if history.tail().frontier() != frontier {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
        Self::from_predecessor(transaction, history, receipt, encode_receipt)
    }

    /// The history state this advance stages.
    pub(crate) fn successor(&self) -> ChangelogHistoryStateV3 {
        self.successor
    }

    // Private to this owner. CapturedImmediateWrite validated these roots before
    // its first tracked mutation and cannot change any control row. Application
    // and administration metadata may now contain their tracked post-images;
    // full physical roots are checked again after the exact receipt is staged.
    fn from_captured_predecessor(
        transaction: &WriteTransaction,
        history: ChangelogHistoryStateV3,
        receipt: &AuthoritativeTransactionV3,
    ) -> Result<Self, StorageError> {
        Self::from_predecessor(transaction, history, receipt, true)
    }

    fn from_predecessor(
        transaction: &WriteTransaction,
        history: ChangelogHistoryStateV3,
        receipt: &AuthoritativeTransactionV3,
        encode_receipt: bool,
    ) -> Result<Self, StorageError> {
        {
            let meta = transaction.open_table(META).map_err(table_error)?;
            crate::primary_admission_roots::validate_write_receipt(&meta, history, receipt)?;
            for (namespace, expected) in [
                (
                    N::ChangelogHistoryState,
                    encode_changelog_history_state_v3(history).map_err(codec_error)?,
                ),
                (
                    N::NextChangelogTransaction,
                    encode_changelog_transaction_allocator_v3(history.expected_allocator())
                        .map_err(codec_error)?,
                ),
            ] {
                let observed = meta
                    .get(metadata_key(namespace)?)
                    .map_err(precommit_storage_error)?
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
                if observed.value() != expected.as_bytes() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
            }
        }
        let (assigned, allocator) = history
            .expected_allocator()
            .allocate_one()
            .map_err(value_error)?;
        if assigned != receipt.binding().sequence {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let (successor, encoded) = if encode_receipt {
            let (successor, encoded) = history.advance_and_encode(receipt).map_err(value_error)?;
            (successor, Some(encoded))
        } else {
            (history.advance(receipt).map_err(value_error)?, None)
        };
        let encoded_history = encode_changelog_history_state_v3(successor)
            .map_err(codec_error)?
            .into_bytes();
        let encoded_allocator = encode_changelog_transaction_allocator_v3(allocator)
            .map_err(codec_error)?
            .into_bytes();
        if transaction
            .open_table(HISTORY)
            .map_err(table_error)?
            .get(assigned.get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(Self {
            successor,
            encoded,
            encoded_history,
            encoded_allocator,
        })
    }

    pub(crate) fn stage(self, transaction: &WriteTransaction) -> Result<(), StorageError> {
        self.stage_as(transaction, None, true)
    }

    /// Stages the advance with a history stub (ADR-0263 Amendment 3) in place
    /// of the receipt bytes, for a receipt a retained journal frame re-derives.
    pub(crate) fn stage_stub(
        self,
        transaction: &WriteTransaction,
        stub: &[u8],
    ) -> Result<(), StorageError> {
        self.stage_as(transaction, Some(stub), true)
    }

    /// Stages one frame of a multi-frame checkpoint transaction without the
    /// per-frame physical root re-read. Only for an advance produced by
    /// [`Self::prepare_chained`], [`Self::prepare_stub_chained`] or
    /// [`Self::prepare`] inside a batch whose
    /// owner fully re-reads the roots after its final frame and aborts the
    /// transaction on any mismatch; `stub` is as for [`Self::stage_stub`].
    pub(crate) fn stage_within_batch(
        self,
        transaction: &WriteTransaction,
        stub: Option<&[u8]>,
    ) -> Result<(), StorageError> {
        self.stage_as(transaction, stub, false)
    }

    fn stage_as(
        self,
        transaction: &WriteTransaction,
        stub: Option<&[u8]>,
        verify_roots: bool,
    ) -> Result<(), StorageError> {
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
                stub.or(self.encoded.as_deref())
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
            )
            .map_err(precommit_storage_error)?;
        #[cfg(test)]
        crash_edge("receipt");
        {
            let mut meta = transaction.open_table(META).map_err(table_error)?;
            meta.insert(
                metadata_key(N::NextChangelogTransaction)?,
                self.encoded_allocator.as_slice(),
            )
            .map_err(precommit_storage_error)?;
            meta.insert(
                metadata_key(N::ChangelogHistoryState)?,
                self.encoded_history.as_slice(),
            )
            .map_err(precommit_storage_error)?;
        }
        #[cfg(test)]
        crash_edge("roots");
        // Bind the actual physical post-images, not just the declared frontier.
        // A batch owner performs this once after its final frame instead.
        if verify_roots && read_checkpoint_roots_for_write(transaction)? != Some(self.successor) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
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

pub(crate) fn value_error(error: ChangelogV3Error) -> StorageError {
    storage_error(match error {
        ChangelogV3Error::LimitExceeded => StorageErrorKind::LimitExceeded,
        ChangelogV3Error::SequenceExhausted => StorageErrorKind::SequenceExhausted,
        _ => StorageErrorKind::CorruptData,
    })
}

pub(crate) fn table_inventory(
    transaction: &WriteTransaction,
) -> Result<BTreeSet<&'static str>, StorageError> {
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

pub(crate) fn check_predecessor(
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
        match before.as_ref().map(|row| row.value()) {
            // A predecessor binds a history stub's resolved segment (ADR-0263 C3).
            Some(stored) if mutation.namespace() == N::Commits => {
                check(Some(&crate::history_stub::resolve_commits_value(stored)?))
            }
            before => check(before),
        }
    }
}

pub(crate) fn apply_mutation(
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
