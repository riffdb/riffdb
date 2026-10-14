//! V3 journal checkpoint/recovery materialization within the existing transaction.
//! No allocator mutation is admitted through the ordinary metadata apply path.

use redb::{ReadTransaction, ReadableTable, TableHandle, WriteTransaction};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV1, AuthoritativeTransactionBindingV3,
    AuthoritativeTransactionV3, ChangelogHistoryPointV3, ChangelogHistoryStateV3,
    ChangelogTransactionSequence, MAX_CHANGELOG_FRAME_BYTES, StorageError, StorageErrorKind,
};
use riffdb_types::DualFrontier;

use crate::{
    changelog_v3::{
        journal_allocator_assignment, receipt_from_journal_for_catalog as receipt_from_journal,
    },
    changelog_v3_activation::HISTORY,
    changelog_v3_roots::{
        read_checkpoint_roots, read_checkpoint_roots_for_write, validate_retained_history,
    },
    changelog_v3_write::{PreparedHistoryAdvance, table_inventory, value_error},
    error::{precommit_storage_error, storage_error, table_error},
    journal::JournalFrame,
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
    if meta
        .get(crate::primary_admission_roots::key()?)
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Ok(true);
    }
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
    let mut verified_point: Option<ChangelogHistoryPointV3> = None;
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
            let (_, point) =
                verify_materialized_frame_with_roots(transaction, frame, history, verified_point)?;
            verified_point = Some(point);
        } else {
            replay_from = replay_from.min(index);
            let summary = crate::changelog_v3::summary_from_journal_for_catalog(
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
                history.lineage().catalog_digest(),
            )
            .map_err(value_error)?;
            crate::primary_admission_roots::validate_write_receipt_summary(
                &transaction
                    .open_table(crate::layout::META)
                    .map_err(table_error)?,
                history,
                &summary,
            )?;
            history = summary.advance_history(history).map_err(value_error)?;
        }
    }
    Ok(RecoveryPlan {
        replay_from,
        covered: history.tail().frontier(),
    })
}

/// One standalone frame without a locator, for unit tests: roots validated in
/// full before and after it.
#[cfg(test)]
pub(crate) fn materialize_recovered_frame(
    transaction: &WriteTransaction,
    frame: &JournalFrame,
) -> Result<AuthoritativeTransactionV3, StorageError> {
    materialize_native_frame(transaction, frame, None, false, None).map(|(receipt, _, _)| receipt)
}

/// Materializes successive frames of one redb write transaction. Called only
/// with independently decoded/validated journal frames (recovery replay and
/// the published-suffix drain); the live checkpoint has a separate
/// retained-proof path and does not re-decode. On error the owner must abort,
/// as for existing ordered journal application. With a frame's `locator`, its
/// segments and its receipt are written as history stubs (ADR-0263
/// Amendment 3).
///
/// The transaction is the batch: no state between two of its frames is
/// durable or visible. The physical changelog roots, including the terminal
/// receipt the history index resolves, are validated in full before the first
/// receipt-bearing frame and again by [`Self::finish`] after the last. Between
/// them each frame starts from the successor the previous frame staged, and
/// `PreparedHistoryAdvance::prepare_chained` re-checks the stored root rows
/// and the physical frontier against it. Every frame is still decoded and
/// validated independently, its receipt derived from its own bytes and its
/// recorded binding checked, and every ordered mutation's before-image is
/// still checked. A frame without a receipt source breaks the chain, so the
/// next receipt-bearing frame validates the roots in full again.
#[derive(Default)]
pub(crate) struct ChainedFrameMaterialization {
    chain: Option<ChangelogHistoryStateV3>,
}

impl ChainedFrameMaterialization {
    /// The standalone single-frame materialization, for the next frame of
    /// this batch.
    pub(crate) fn materialize(
        &mut self,
        transaction: &WriteTransaction,
        frame: &JournalFrame,
        locator: Option<crate::history_stub::StubLocator>,
    ) -> Result<AuthoritativeTransactionV3, StorageError> {
        self.materialize_inner(transaction, frame, locator, false)
            .map(|(receipt, _)| receipt)
    }

    /// The live drain's form of [`Self::materialize`]: it additionally checks
    /// its actual native preimages before any mutation, preserving only the
    /// closed source-independent audit conversion.
    pub(crate) fn materialize_for_continuation(
        &mut self,
        transaction: &WriteTransaction,
        frame: &JournalFrame,
        locator: Option<crate::history_stub::StubLocator>,
    ) -> Result<
        (
            AuthoritativeTransactionV3,
            crate::source_continuation::SourceContinuationFootprint,
        ),
        StorageError,
    > {
        self.materialize_inner(transaction, frame, locator, true)
    }

    /// Records that a frame without a receipt source is about to be applied
    /// directly. The deferred post-image check for the frames chained so far
    /// runs first, exactly where the unchained path ran it.
    pub(crate) fn break_chain(
        &mut self,
        transaction: &WriteTransaction,
    ) -> Result<(), StorageError> {
        self.verify_chain(transaction)
    }

    fn materialize_inner(
        &mut self,
        transaction: &WriteTransaction,
        frame: &JournalFrame,
        locator: Option<crate::history_stub::StubLocator>,
        qualify: bool,
    ) -> Result<
        (
            AuthoritativeTransactionV3,
            crate::source_continuation::SourceContinuationFootprint,
        ),
        StorageError,
    > {
        // A failure leaves the chain unset; the owner must abort.
        let chain = self.chain.take();
        let (receipt, footprint, successor) =
            materialize_native_frame(transaction, frame, locator, qualify, Some(chain))?;
        self.chain = successor;
        Ok((receipt, footprint))
    }

    /// Binds the physical post-images after the batch's final frame. Must be
    /// called, and must succeed, before the owner commits.
    pub(crate) fn finish(mut self, transaction: &WriteTransaction) -> Result<(), StorageError> {
        self.verify_chain(transaction)
    }

    fn verify_chain(&mut self, transaction: &WriteTransaction) -> Result<(), StorageError> {
        match self.chain.take() {
            None => Ok(()),
            Some(expected) if read_checkpoint_roots_for_write(transaction)? == Some(expected) => {
                Ok(())
            }
            Some(_) => Err(storage_error(StorageErrorKind::CorruptData)),
        }
    }
}

/// `chained` is `None` for a standalone frame, which validates the roots in
/// full before and after itself. Inside a [`ChainedFrameMaterialization`] it
/// is `Some(predecessor)`: the successor the previous frame staged in this
/// transaction, or `None` to validate the roots in full first. A chained
/// frame defers its post-image check to its owner and returns its successor.
fn materialize_native_frame(
    transaction: &WriteTransaction,
    frame: &JournalFrame,
    locator: Option<crate::history_stub::StubLocator>,
    qualify: bool,
    chained: Option<Option<ChangelogHistoryStateV3>>,
) -> Result<
    (
        AuthoritativeTransactionV3,
        crate::source_continuation::SourceContinuationFootprint,
        Option<ChangelogHistoryStateV3>,
    ),
    StorageError,
> {
    let history = match chained {
        Some(Some(predecessor)) => predecessor,
        None | Some(None) => read_checkpoint_roots_for_write(transaction)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?,
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
        covered_frontier: DualFrontier::new(
            frame.covered_sequence(),
            frame.covered_administration_sequence(),
        ),
        prior_history_hash: history.tail().history_hash(),
    };
    // A version 3 frame recorded the binding it was written with (ADR-0263
    // Amendment 2 B4); it must agree with the one these roots derive.
    if let Some(recorded) = frame.changelog()
        && (recorded.binding(frame) != binding
            || recorded.catalog_digest() != history.lineage().catalog_digest())
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let receipt = receipt_from_journal(frame, binding, history.lineage().catalog_digest())
        .map_err(value_error)?;
    let advance = match chained {
        None => PreparedHistoryAdvance::prepare(transaction, &receipt)?,
        Some(_) => PreparedHistoryAdvance::prepare_chained(transaction, history, &receipt)?,
    };
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
    let footprint = if qualify {
        crate::source_continuation::SourceContinuationFootprint::captured_checked(
            history,
            receipt.mutations(),
            |mutation| {
                let audit = transaction
                    .open_table(crate::layout::AUDIT)
                    .map_err(table_error)?;
                Ok(audit
                    .get(mutation.key())
                    .map_err(precommit_storage_error)?
                    .map(|row| row.value().to_vec()))
            },
            |sequence| {
                let commits = transaction
                    .open_table(crate::layout::COMMITS)
                    .map_err(table_error)?;
                let key = crate::keys::encode_application_sequence_key(sequence);
                let mut rows = commits
                    .range::<&[u8]>((
                        std::ops::Bound::Unbounded,
                        std::ops::Bound::Included(key.as_slice()),
                    ))
                    .map_err(precommit_storage_error)?;
                rows.next_back()
                    .transpose()
                    .map_err(precommit_storage_error)
                    .map(|row| {
                        row.map(|(key, value)| (key.value().to_vec(), value.value().to_vec()))
                    })
            },
        )?
    } else {
        crate::source_continuation::SourceContinuationFootprint::unknown()
    };
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
        crate::journal::apply_located_mutation(transaction, mutation, locator)
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    }
    let successor = advance.successor();
    let stub = match locator {
        // Only a frame that records its binding can re-derive its receipt.
        Some(locator) if frame.changelog().is_some() => Some(locator.receipt_stub()?),
        _ => None,
    };
    match (chained, stub) {
        (None, Some(stub)) => advance.stage_stub(transaction, &stub)?,
        (None, None) => advance.stage(transaction)?,
        (Some(_), stub) => advance.stage_within_batch(transaction, stub.as_deref())?,
    }
    Ok((receipt, footprint, chained.map(|_| successor)))
}

/// Checks one already materialized overlap against its original journal source
/// using one pinned checkpoint. Later authoritative overwrites are irrelevant.
/// Like bounded root validation, this checks local receipt/predecessor agreement;
/// complete retained-history validation remains the startup/recovery owner's duty.
#[cfg(test)]
pub(crate) fn verify_materialized_frame(
    transaction: &ReadTransaction,
    frame: &JournalFrame,
) -> Result<AuthoritativeTransactionV3, StorageError> {
    let history = read_checkpoint_roots(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
    verify_materialized_frame_with_roots(transaction, frame, history, None)
        .map(|(receipt, _)| receipt)
}

fn verify_materialized_frame_with_roots(
    transaction: &ReadTransaction,
    frame: &JournalFrame,
    history: ChangelogHistoryStateV3,
    verified_predecessor: Option<ChangelogHistoryPointV3>,
) -> Result<(AuthoritativeTransactionV3, ChangelogHistoryPointV3), StorageError> {
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
    let row = table
        .get(sequence.get().to_be_bytes().as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if row.value().len() > MAX_CHANGELOG_FRAME_BYTES {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let resolved = crate::history_stub::resolve_history_value(row.value())?;
    let materialized = AuthoritativeTransactionV3::decode(&resolved).map_err(value_error)?;
    let binding = materialized.binding();
    if binding.sequence != sequence
        || binding.database_id != history.lineage().database_id()
        || binding.history_incarnation != history.lineage().history_incarnation()
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let point = crate::changelog_v3::ValidatedReceiptSummaryV3::from_decoded_receipt_and_bytes(
        &materialized,
        &resolved,
    )
    .map_err(value_error)?
    .point();
    if sequence == history.minimum_resume().sequence() {
        if point != history.minimum_resume() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    } else {
        let pred_sequence = binding
            .predecessor
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if let Some(prior) = verified_predecessor.filter(|prior| prior.sequence() == pred_sequence)
        {
            if prior.history_hash() != binding.prior_history_hash
                || prior.frontier() != binding.predecessor_frontier
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        } else {
            let pred_row = table
                .get(pred_sequence.get().to_be_bytes().as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            if pred_row.value().len() > MAX_CHANGELOG_FRAME_BYTES {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let pred_summary =
                crate::history_stub::resolve_history_summary_for_journal(pred_row.value())?;
            if pred_summary.binding().sequence != pred_sequence
                || pred_summary.binding().database_id != history.lineage().database_id()
                || pred_summary.binding().history_incarnation
                    != history.lineage().history_incarnation()
                || pred_summary.history_hash() != binding.prior_history_hash
                || pred_summary.binding().covered_frontier != binding.predecessor_frontier
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
    }
    if let Some(recorded) = frame.changelog()
        && (recorded.binding(frame) != binding
            || recorded.catalog_digest() != history.lineage().catalog_digest())
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let recovered = receipt_from_journal(frame, binding, history.lineage().catalog_digest())
        .map_err(value_error)?;
    // A history stub is compared by the receipt it resolves to (ADR-0263 C3).
    if recovered.encode().map_err(value_error)?.as_slice() != &*resolved {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok((recovered, point))
}
