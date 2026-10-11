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
use std::sync::Arc;

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
    Checkpoint(Arc<CheckpointRoot>),
    Composite(Arc<RedbCompositeReadView>),
}

impl ValidationSource {
    pub(crate) fn checkpoint_pin(&self) -> &Arc<CheckpointRoot> {
        match self {
            Self::Checkpoint(root) => root,
            Self::Composite(view) => view.checkpoint_root_pin(),
        }
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

    pub(crate) fn next_audit(
        &self,
        after: Option<&[u8]>,
    ) -> Result<Option<riffdb_storage_api::CompositeRow>, StorageError> {
        let Self::Composite(view) = self else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        let mut start = after.unwrap_or(&[]).to_vec();
        if after.is_some() {
            start.push(0);
        }
        let table = view
            .checkpoint_root()
            .open_table(crate::layout::AUDIT)
            .map_err(table_error)?;
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
                1,
                riffdb_storage_api::MAX_COMPOSITE_OVERLAY_TRANSITIONS + 2,
            )
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        Ok(page.rows().first().cloned())
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
