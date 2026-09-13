//! V3 journal checkpoint/recovery materialization within the existing transaction.
//! No allocator mutation is admitted through the ordinary metadata apply path.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "WP-772 recovery integration in progress")
)]

use redb::{ReadTransaction, WriteTransaction};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV1, AuthoritativeTransactionBindingV3,
    AuthoritativeTransactionV3, ChangelogHistoryPointV3, ChangelogTransactionSequence,
    MAX_CHANGELOG_FRAME_BYTES, StorageError, StorageErrorKind,
};
use riffdb_types::DualFrontier;

use crate::{
    changelog_v3::{journal_allocator_assignment, receipt_from_journal},
    changelog_v3_activation::HISTORY,
    changelog_v3_roots::{read_checkpoint_roots, read_checkpoint_roots_for_write},
    changelog_v3_write::{PreparedHistoryAdvance, table_inventory, value_error},
    error::{precommit_storage_error, storage_error, table_error},
    journal::{JournalFrame, apply_mutation},
};

/// Called only with an independently decoded/validated recovery frame. The
/// live checkpoint has a separate retained-proof path; it does not re-decode.
/// On error the owner must abort, as for existing ordered journal application.
pub(crate) fn materialize_recovered_frame(
    transaction: &WriteTransaction,
    frame: &JournalFrame,
) -> Result<AuthoritativeTransactionV3, StorageError> {
    let history = read_checkpoint_roots_for_write(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
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
        covered_frontier: DualFrontier::new(
            frame.covered_sequence(),
            frame.covered_administration_sequence(),
        ),
        prior_history_hash: history.tail().history_hash(),
    };
    let receipt = receipt_from_journal(frame, binding).map_err(value_error)?;
    let advance = PreparedHistoryAdvance::prepare(transaction, &receipt)?;
    let tables = table_inventory(transaction)?;
    // Refuse missing target tables before any write; redb's open_table would
    // otherwise silently create them. Check cancelled net mutations as well.
    if frame
        .mutations()
        .iter()
        .any(|mutation| !tables.contains(mutation.table().label()))
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    for mutation in frame.mutations() {
        if AuthoritativeStateCatalogV1.lookup(mutation.table().label(), mutation.key())
            == Some(N::NextChangelogTransaction)
        {
            // Source conversion proved exactly one checked allocation against
            // these transaction-current roots. Stage it only with the receipt.
            continue;
        }
        // Apply the original ordered source, not just net receipt mutations:
        // cancelled keys still have before-images that must be checked.
        apply_mutation(transaction, mutation)
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    }
    advance.stage(transaction)?;
    Ok(receipt)
}

/// Checks one already materialized overlap against its original journal source
/// using one pinned checkpoint. Later authoritative overwrites are irrelevant.
/// Like bounded root validation, this checks local receipt/predecessor agreement;
/// complete retained-history validation remains the startup/recovery owner's duty.
pub(crate) fn verify_materialized_frame(
    transaction: &ReadTransaction,
    frame: &JournalFrame,
) -> Result<AuthoritativeTransactionV3, StorageError> {
    let history = read_checkpoint_roots(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
    let assignment = frame
        .mutations()
        .iter()
        .find(|mutation| {
            AuthoritativeStateCatalogV1.lookup(mutation.table().label(), mutation.key())
                == Some(N::NextChangelogTransaction)
        })
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let sequence = journal_allocator_assignment(assignment).map_err(value_error)?;
    if sequence < history.minimum_resume().sequence() || sequence > history.tail().sequence() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let table = transaction.open_table(HISTORY).map_err(table_error)?;
    let read = |sequence: ChangelogTransactionSequence| {
        let row = table
            .get(sequence.get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if row.value().len() > MAX_CHANGELOG_FRAME_BYTES {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let receipt = AuthoritativeTransactionV3::decode(row.value()).map_err(value_error)?;
        if receipt.binding().sequence != sequence
            || receipt.binding().database_id != history.lineage().database_id()
            || receipt.binding().history_incarnation != history.lineage().history_incarnation()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(receipt)
    };
    let materialized = read(sequence)?;
    let binding = materialized.binding();
    let point = ChangelogHistoryPointV3::from_receipt(&materialized).map_err(value_error)?;
    if sequence == history.minimum_resume().sequence() {
        if point != history.minimum_resume() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    } else {
        let predecessor = read(
            binding
                .predecessor
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?,
        )?;
        if predecessor.history_hash().map_err(value_error)? != binding.prior_history_hash
            || predecessor.binding().covered_frontier != binding.predecessor_frontier
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    let recovered = receipt_from_journal(frame, binding).map_err(value_error)?;
    let original = table
        .get(sequence.get().to_be_bytes().as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if recovered.encode().map_err(value_error)?.as_slice() != original.value() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(recovered)
}
