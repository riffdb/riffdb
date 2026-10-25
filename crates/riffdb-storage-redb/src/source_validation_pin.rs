//! One immutable validation pin; composite identity includes its exact frame hash.

use crate::{
    checkpoint_root::CheckpointRoot,
    composite_view::RedbCompositeReadView,
    error::{precommit_storage_error, storage_error, table_error},
    source_validation_read::{ReceiptRead, SourceReceipt},
};
use redb::ReadableTableMetadata;
use riffdb_storage_api::{
    ChangelogHistoryStateV3 as History, ChangelogTransactionSequence, StorageError,
    StorageErrorKind,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceIdentity {
    Checkpoint(u64),
    Composite {
        checkpoint: u64,
        frame_hash: [u8; 32],
    },
}

#[derive(Clone)]
pub(crate) enum ValidationSource {
    Checkpoint(crate::checkpoint_root::CheckpointRootOwner),
    Composite(riffdb_types::OwnedCapacitySharedV1<RedbCompositeReadView>),
}

impl ValidationSource {
    pub(crate) fn is_service_audit_v3_compact_header(encoded: &[u8]) -> bool {
        const SERVICE_AUDIT_V3_HEADER_PREFIX: [u8; 8] = [b'R', b'D', b'B', b'2', 2, 22, 0, 3];
        encoded.len() >= riffdb_proto::envelope::COMPACT_RECORD_HEADER_V2_BYTES
            && encoded.starts_with(&SERVICE_AUDIT_V3_HEADER_PREFIX)
    }

    pub(crate) fn checkpoint_pin(&self) -> &crate::checkpoint_root::CheckpointRootOwner {
        match self {
            Self::Checkpoint(root) => root,
            Self::Composite(view) => view.checkpoint_root_pin(),
        }
    }

    pub(crate) fn receipt_summary(
        &self,
        table: &impl redb::ReadableTable<&'static [u8], &'static [u8]>,
        sequence: ChangelogTransactionSequence,
    ) -> Result<Option<crate::changelog_v3::ValidatedReceiptSummaryV3>, StorageError> {
        match self {
            Self::Checkpoint(_) => {
                let Some(row) = table
                    .get(sequence.get().to_be_bytes().as_slice())
                    .map_err(precommit_storage_error)?
                else {
                    return Ok(None);
                };
                crate::history_stub::resolve_history_summary(row.value()).map(Some)
            }
            Self::Composite(view) => view
                .changelog_suffix()
                .read_receipt_summary(table, sequence),
        }
    }

    pub(crate) fn visit_receipt_summary_page_with_cache(
        &self,
        table: &impl redb::ReadableTable<&'static [u8], &'static [u8]>,
        page_cache: &mut crate::history_stub::ReceiptPageExtentCache,
        start_sequence: u64,
        ceiling_inclusive: u64,
        limit: usize,
        mut visit: impl FnMut(
            ChangelogTransactionSequence,
            crate::changelog_v3::ValidatedReceiptSummaryV3,
        ) -> Result<bool, StorageError>,
    ) -> Result<(), StorageError> {
        if limit == 0 || start_sequence > ceiling_inclusive {
            return Ok(());
        }
        let corrupt = || storage_error(StorageErrorKind::CorruptData);
        let (composite_history, checkpoint_tail) = match self {
            Self::Checkpoint(_) => (None, ceiling_inclusive),
            Self::Composite(view) => {
                let (history, checkpoint_tail) =
                    view.changelog_suffix().checkpoint_summary_range()?;
                if start_sequence < history.minimum_resume().sequence().get()
                    || ceiling_inclusive > history.tail().sequence().get()
                {
                    return Err(corrupt());
                }
                (Some(history), checkpoint_tail)
            }
        };
        let mut next_sequence = start_sequence;
        let mut visited = 0_usize;
        if next_sequence <= checkpoint_tail {
            let max_span = u64::try_from(limit - 1).unwrap_or(u64::MAX);
            let max_checkpoint_seq = ceiling_inclusive
                .min(checkpoint_tail)
                .min(next_sequence.saturating_add(max_span));
            let start_key = next_sequence.to_be_bytes();
            let end_key = max_checkpoint_seq.to_be_bytes();
            for entry in table
                .range::<&[u8]>((
                    std::ops::Bound::Included(start_key.as_slice()),
                    std::ops::Bound::Included(end_key.as_slice()),
                ))
                .map_err(precommit_storage_error)?
            {
                let (key, value) = entry.map_err(precommit_storage_error)?;
                if key.value() != next_sequence.to_be_bytes() {
                    return Err(corrupt());
                }
                let sequence =
                    ChangelogTransactionSequence::new(next_sequence).ok_or_else(corrupt)?;
                let summary = page_cache.resolve_history_summary(value.value())?;
                if let Some(history) = composite_history
                    && (summary.binding().sequence != sequence
                        || summary.binding().database_id != history.lineage().database_id()
                        || summary.binding().history_incarnation
                            != history.lineage().history_incarnation())
                {
                    return Err(corrupt());
                }
                if !visit(sequence, summary)? {
                    return Ok(());
                }
                visited += 1;
                if next_sequence == max_checkpoint_seq {
                    next_sequence = next_sequence.checked_add(1).ok_or_else(corrupt)?;
                    break;
                }
                next_sequence = next_sequence.checked_add(1).ok_or_else(corrupt)?;
            }
            if next_sequence <= max_checkpoint_seq {
                return Err(corrupt());
            }
        }
        while visited < limit && next_sequence <= ceiling_inclusive {
            let sequence = ChangelogTransactionSequence::new(next_sequence).ok_or_else(corrupt)?;
            let summary = self.receipt_summary(table, sequence)?.ok_or_else(corrupt)?;
            if !visit(sequence, summary)? {
                return Ok(());
            }
            visited += 1;
            if next_sequence == ceiling_inclusive {
                break;
            }
            next_sequence = next_sequence.checked_add(1).ok_or_else(corrupt)?;
        }
        Ok(())
    }

    pub(crate) fn publication(&self) -> SourceIdentity {
        match self {
            Self::Checkpoint(root) => SourceIdentity::Checkpoint(root.identity()),
            Self::Composite(view) => SourceIdentity::Composite {
                checkpoint: view.checkpoint_root().identity(),
                frame_hash: view.overlay().terminal_frame_hash(),
            },
        }
    }

    pub(crate) fn history(&self) -> Result<Option<History>, StorageError> {
        let checkpoint = crate::changelog_v3_roots::read_checkpoint_roots(self)?;
        match self {
            Self::Checkpoint(_) => Ok(checkpoint),
            Self::Composite(view) => {
                let history = view.changelog_suffix().history_state();
                if history.is_some() != checkpoint.is_some()
                    || history.is_some_and(|history| {
                        history.tail().frontier()
                            != riffdb_types::DualFrontier::new(
                                view.overlay().published_application(),
                                view.overlay().published_administration(),
                            )
                    })
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                Ok(history)
            }
        }
    }

    pub(crate) fn empty(&self) -> Result<bool, StorageError> {
        let empty = self
            .open_table(crate::changelog_v3_activation::HISTORY)
            .map_err(table_error)?
            .is_empty()
            .map_err(precommit_storage_error)?;
        Ok(empty
            && match self {
                Self::Checkpoint(_) => true,
                Self::Composite(view) => !view.changelog_suffix().has_sources(),
            })
    }

    pub(crate) fn visit_audit_page(
        &self,
        after: Option<&[u8]>,
        limit: usize,
        mut visit: impl FnMut(&[u8], &[u8]) -> Result<bool, StorageError>,
    ) -> Result<bool, StorageError> {
        let Self::Composite(view) = self else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        if limit == 0 {
            return Ok(false);
        }
        if view
            .overlay()
            .provider_ownership_family()
            .is_some_and(|family| !family.observation().is_ok_and(|obs| obs.enabled))
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let max_key = riffdb_types::MAX_KEY_BYTES + 1;
        let end = [u8::MAX; riffdb_types::MAX_KEY_BYTES + 1];
        let next_overlay = view
            .overlay()
            .borrowed_provider_next_v1(
                riffdb_storage_api::CompositeTableV1::Audit,
                b"",
                after,
                &end[..max_key],
            )
            .map(|(key, _)| key);
        let table = view
            .checkpoint_root()
            .journal_byte_table(crate::journal::JournalTable::Audit)
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
            .map_err(table_error)?;
        let low = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
        let mut visited_rows = 0_usize;
        let mut last_key = None;
        let mut merge_needed = false;
        for entry in table
            .range::<&[u8]>((low, std::ops::Bound::Unbounded))
            .map_err(precommit_storage_error)?
        {
            if visited_rows == limit {
                return Ok(false);
            }
            let (key, value) = entry.map_err(precommit_storage_error)?;
            let key_bytes = key.value();
            let value_bytes = value.value();
            if key_bytes.is_empty()
                || value_bytes.is_empty()
                || key_bytes.len() > riffdb_types::MAX_KEY_BYTES
                || value_bytes.len() > riffdb_proto::envelope::MAX_STORED_ENVELOPE_BYTES
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            if next_overlay.is_some_and(|overlay_key| key_bytes >= overlay_key) {
                merge_needed = true;
                break;
            }
            if !visit(key_bytes, value_bytes)? {
                return Ok(false);
            }
            visited_rows += 1;
            if next_overlay.is_some() {
                last_key = Some(key);
            }
        }
        if !merge_needed && next_overlay.is_none() {
            return Ok(true);
        }
        if visited_rows == limit {
            return Ok(false);
        }
        let resume_after = last_key.as_ref().map(|key| key.value()).or(after);
        let mut start = resume_after.unwrap_or(&[]).to_vec();
        if resume_after.is_some() {
            start.push(0);
        }
        let remaining = limit - visited_rows;
        let base = table
            .range::<&[u8]>((
                std::ops::Bound::Included(start.as_slice()),
                std::ops::Bound::Unbounded,
            ))
            .map_err(precommit_storage_error)?
            .map(|row| {
                let (key, value) =
                    row.map_err(|_| riffdb_storage_api::StorageValueError::InvalidShape)?;
                if key.value().len() > riffdb_types::MAX_KEY_BYTES
                    || value.value().len() > riffdb_proto::envelope::MAX_STORED_ENVELOPE_BYTES
                {
                    return Err(riffdb_storage_api::StorageValueError::InvalidShape);
                }
                Ok((
                    key.value().to_vec().into_boxed_slice(),
                    value.value().to_vec().into_boxed_slice(),
                ))
            });
        let page = view
            .overlay()
            .merge_bounded(
                riffdb_storage_api::CompositeTableV1::Audit,
                base,
                &start,
                None,
                remaining,
                remaining.saturating_add(riffdb_storage_api::MAX_COMPOSITE_OVERLAY_TRANSITIONS + 1),
            )
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let exhausted = page.rows().len() < remaining;
        for (key, value) in page.rows() {
            if !visit(key, value)? {
                return Ok(false);
            }
        }
        Ok(exhausted)
    }
}

impl std::ops::Deref for ValidationSource {
    type Target = CheckpointRoot;
    fn deref(&self) -> &Self::Target {
        self.checkpoint_pin()
    }
}

impl ReceiptRead for ValidationSource {
    fn receipt(
        &self,
        sequence: ChangelogTransactionSequence,
        budget: Option<&riffdb_types::exact_provider_budget_v1::ProviderBudget>,
    ) -> Result<Option<SourceReceipt>, StorageError> {
        match self {
            Self::Checkpoint(root) => root
                .open_table(crate::changelog_v3_activation::HISTORY)
                .map_err(table_error)?
                .receipt(sequence, budget),
            Self::Composite(view) => {
                crate::source_validation_read::CompositeReceipts(view).receipt(sequence, budget)
            }
        }
    }
}
