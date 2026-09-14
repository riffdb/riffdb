//! V3 journal checkpoint/recovery materialization within the existing transaction.
//! No allocator mutation is admitted through the ordinary metadata apply path.

use redb::{ReadTransaction, ReadableTable, TableHandle, WriteTransaction};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV1, AuthoritativeTransactionBindingV3,
    AuthoritativeTransactionV3, ChangelogHistoryPointV3, ChangelogTransactionSequence,
    MAX_CHANGELOG_FRAME_BYTES, StorageError, StorageErrorKind,
};
use riffdb_types::DualFrontier;

use crate::{
    changelog_v3::{journal_allocator_assignment, receipt_from_journal},
    changelog_v3_activation::HISTORY,
    changelog_v3_roots::{
        read_checkpoint_roots, read_checkpoint_roots_for_write, validate_retained_history,
    },
    changelog_v3_write::{PreparedHistoryAdvance, table_inventory, value_error},
    error::{precommit_storage_error, storage_error, table_error},
    journal::{JournalFrame, apply_mutation},
};

/// Routing only, never an inactivity/activation proof. The V3 registry claim or
/// any surviving V3 control domain requires strict V3 recovery; erased or partial
/// roots cannot select legacy replay.
/// Complete startup still owns the exact registry/inactivity decision.
pub(crate) fn has_recovery_roots(transaction: &ReadTransaction) -> Result<bool, StorageError> {
    let meta = transaction
        .open_table(crate::layout::META)
        .map_err(table_error)?;
    if has_metadata_roots(&meta)? {
        return Ok(true);
    }
    for (count, table) in transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .enumerate()
    {
        if count >= N::ALL.len() {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        if N::ALL.into_iter().any(|n| {
            n.requires_v3_activation() && n.metadata_key().is_none() && n.table() == table.name()
        }) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn has_write_recovery_roots(
    transaction: &WriteTransaction,
) -> Result<bool, StorageError> {
    let tables = table_inventory(transaction)?;
    if !tables.contains(crate::layout::META.name()) {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let meta = transaction
        .open_table(crate::layout::META)
        .map_err(table_error)?;
    Ok(has_metadata_roots(&meta)?
        || N::ALL.into_iter().any(|n| {
            n.requires_v3_activation() && n.metadata_key().is_none() && tables.contains(n.table())
        }))
}

fn has_metadata_roots(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<bool, StorageError> {
    if let Some(registry) = meta
        .get(crate::layout::META_RECORD_REGISTRY)
        .map_err(precommit_storage_error)?
    {
        if registry.value().len() > 512 {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        if *riffdb_storage_api::proto_codec::decode_record_registry_v2(registry.value())
            .map_err(crate::error::codec_error)?
            .value()
            == riffdb_storage_api::proto_codec::current_record_registry_digest()
        {
            return Ok(true);
        }
    }
    for namespace in N::ALL.into_iter().filter(|n| n.requires_v3_activation()) {
        if let Some(key) = namespace.metadata_key()
            && meta.get(key).map_err(precommit_storage_error)?.is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn has_receipt_source(frame: &JournalFrame) -> bool {
    frame.mutations().iter().any(|mutation| {
        AuthoritativeStateCatalogV1.lookup(mutation.table().label(), mutation.key())
            == Some(N::NextChangelogTransaction)
    })
}

pub(crate) struct RecoveryPlan {
    pub(crate) replay_from: usize,
    pub(crate) covered: DualFrontier,
}

#[cfg(test)]
pub(crate) fn recovery_crash_edge(edge: &str) {
    if std::env::var("RIFFDB_V3_RECOVERY_EDGE").as_deref() == Ok(edge) {
        std::process::exit(93);
    }
}

/// Runs before any recovery write or extent reset, under the existing exclusive
/// startup owner. The complete retained chain and every original overlap are
/// checked from one pin. Later direct writes may advance the root past the suffix;
/// neither their rows nor their physical counter replace the original evidence.
pub(crate) fn plan_recovery(
    transaction: &ReadTransaction,
    frames: &[JournalFrame],
    physical_frontier: DualFrontier,
) -> Result<RecoveryPlan, StorageError> {
    let corrupt = || storage_error(StorageErrorKind::CorruptData);
    // An already-empty extent has no source to reclaim. Keep the clean-open
    // path bounded; complete startup validation owns its full history scan.
    let mut history = if frames.is_empty() {
        read_checkpoint_roots(transaction)?
    } else {
        validate_retained_history(transaction)?
    }
    .ok_or_else(corrupt)?;
    if history.tail().frontier() != physical_frontier {
        return Err(corrupt());
    }
    let original_tail = history.tail().sequence();
    let mut replay_from = frames.len();
    let mut previous: Option<ChangelogTransactionSequence> = None;
    for (index, frame) in frames.iter().enumerate() {
        let mut assignments = frame.mutations().iter().filter(|mutation| {
            AuthoritativeStateCatalogV1.lookup(mutation.table().label(), mutation.key())
                == Some(N::NextChangelogTransaction)
        });
        let sequence = journal_allocator_assignment(assignments.next().ok_or_else(corrupt)?)
            .map_err(value_error)?;
        if assignments.next().is_some()
            || previous.is_some_and(|prior| prior.get().checked_add(1) != Some(sequence.get()))
        {
            return Err(corrupt());
        }
        previous = Some(sequence);
        if sequence <= original_tail {
            verify_materialized_frame(transaction, frame)?;
        } else {
            replay_from = replay_from.min(index);
            let receipt = receipt_from_journal(
                frame,
                AuthoritativeTransactionBindingV3 {
                    database_id: history.lineage().database_id(),
                    history_incarnation: history.lineage().history_incarnation(),
                    predecessor: Some(history.tail().sequence()),
                    sequence,
                    predecessor_frontier: history.tail().frontier(),
                    covered_frontier: DualFrontier::new(
                        frame.covered_sequence(),
                        frame.covered_administration_sequence(),
                    ),
                    prior_history_hash: history.tail().history_hash(),
                },
            )
            .map_err(value_error)?;
            history = history.advance(&receipt).map_err(value_error)?;
        }
    }
    Ok(RecoveryPlan {
        replay_from,
        covered: history.tail().frontier(),
    })
}

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
