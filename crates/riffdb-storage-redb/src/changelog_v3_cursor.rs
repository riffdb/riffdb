//! Immutable original receipt sources paired with the exact published redb pin.
//! No database handle, mutation lease, journal I/O or durability decision lives here.

use riffdb_storage_api::{
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogCursorErrorV3, ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ChangelogReceiptCursorV3, ChangelogTransactionSequence, CompositeMutationV1, StorageError,
    StorageErrorKind,
};
use std::sync::Arc;

use crate::{
    changelog_v3_activation::HISTORY,
    changelog_v3_roots::read_checkpoint_roots,
    changelog_v3_write::value_error,
    checkpoint_root::CheckpointRoot,
    error::{precommit_storage_error, storage_error, table_error},
    journal::{MAX_JOURNAL_FRAME_BYTES, MAX_JOURNAL_SUFFIX_BYTES, MAX_JOURNAL_SUFFIX_TRANSITIONS},
    store::RedbReadAccess,
};

struct Source {
    binding: AuthoritativeTransactionBindingV3,
    attribution: ChangelogAttributionV3,
    history: ChangelogHistoryStateV3,
    mutations: Arc<[CompositeMutationV1]>,
    encoded_bytes: usize,
}

struct SourceNode {
    predecessor: Option<Arc<SourceNode>>,
    source: Arc<Source>,
}

// A full bounded suffix can contain thousands of nodes. Release uniquely owned
// ancestry iteratively, not recursively on a production thread's small stack.
impl Drop for SourceNode {
    fn drop(&mut self) {
        let mut predecessor = self.predecessor.take();
        while let Some(node) = predecessor {
            match Arc::try_unwrap(node) {
                Ok(mut node) => predecessor = node.predecessor.take(),
                Err(_) => break,
            }
        }
    }
}

/// Constant-time private successor construction; payloads are shared with the
/// existing checkpoint batch, never copied from latest overlay values.
#[derive(Clone)]
pub(crate) struct ChangelogSuffix {
    checkpoint: Option<ChangelogHistoryStateV3>,
    history: Option<ChangelogHistoryStateV3>,
    head: Option<Arc<SourceNode>>,
    frames: usize,
    bytes: usize,
}

impl ChangelogSuffix {
    pub(crate) fn from_checkpoint(root: &CheckpointRoot) -> Result<Self, StorageError> {
        // Transitional routing only. A V3 cursor never serves an inactive view;
        // full startup owns receipted activation and the exact registry decision.
        let history = if crate::changelog_v3_journal::has_recovery_roots(root)? {
            Some(read_checkpoint_roots(root)?.ok_or_else(corrupt)?)
        } else {
            None
        };
        Ok(Self {
            checkpoint: history,
            history,
            head: None,
            frames: 0,
            bytes: 0,
        })
    }

    pub(crate) fn append(
        &mut self,
        successor: Option<(AuthoritativeTransactionBindingV3, ChangelogHistoryStateV3)>,
        attribution: ChangelogAttributionV3,
        mutations: Arc<[CompositeMutationV1]>,
        encoded_bytes: usize,
    ) -> Result<(), StorageError> {
        let Some((binding, history)) = successor else {
            if self.history.is_some() {
                return Err(corrupt());
            }
            return Ok(());
        };
        self.append_source(Arc::new(Source {
            binding,
            attribution,
            history,
            mutations,
            encoded_bytes,
        }))
    }

    fn append_source(&mut self, source: Arc<Source>) -> Result<(), StorageError> {
        let previous = self.history.ok_or_else(corrupt)?;
        let binding = source.binding;
        if source.history.lineage() != previous.lineage()
            || source.history.anchor() != previous.anchor()
            || source.history.minimum_resume() != previous.minimum_resume()
            || binding.database_id != previous.lineage().database_id()
            || binding.history_incarnation != previous.lineage().history_incarnation()
            || binding.predecessor != Some(previous.tail().sequence())
            || previous.tail().sequence().checked_next() != Some(binding.sequence)
            || binding.predecessor_frontier != previous.tail().frontier()
            || binding.prior_history_hash != previous.tail().history_hash()
            || source.history.tail().sequence() != binding.sequence
            || source.history.tail().frontier() != binding.covered_frontier
        {
            return Err(corrupt());
        }
        let frames = self.frames.checked_add(1).ok_or_else(limit)?;
        let bytes = self
            .bytes
            .checked_add(source.encoded_bytes)
            .ok_or_else(limit)?;
        if frames > MAX_JOURNAL_SUFFIX_TRANSITIONS
            || bytes > MAX_JOURNAL_SUFFIX_BYTES
            || source.encoded_bytes == 0
            || source.encoded_bytes > MAX_JOURNAL_FRAME_BYTES
        {
            return Err(limit());
        }
        self.history = Some(source.history);
        self.head = Some(Arc::new(SourceNode {
            predecessor: self.head.clone(),
            source,
        }));
        self.frames = frames;
        self.bytes = bytes;
        Ok(())
    }

    fn sources(&self) -> Result<Vec<Arc<Source>>, StorageError> {
        if self.frames > MAX_JOURNAL_SUFFIX_TRANSITIONS {
            return Err(limit());
        }
        let mut sources = Vec::with_capacity(self.frames);
        let mut head = self.head.as_ref();
        while let Some(node) = head {
            if sources.len() >= self.frames {
                return Err(corrupt());
            }
            sources.push(Arc::clone(&node.source));
            head = node.predecessor.as_ref();
        }
        if sources.len() != self.frames {
            return Err(corrupt());
        }
        sources.reverse();
        Ok(sources)
    }

    pub(crate) fn rebase_after(
        &self,
        covered: &Self,
        root: &CheckpointRoot,
    ) -> Result<Self, StorageError> {
        let mut successor = Self::from_checkpoint(root)?;
        if successor.history != covered.history || self.checkpoint != covered.checkpoint {
            return Err(corrupt());
        }
        let mut newer = Vec::new();
        let mut head = self.head.as_ref();
        loop {
            if match (head, covered.head.as_ref()) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            } {
                break;
            }
            let node = head.ok_or_else(corrupt)?;
            if newer.len() >= self.frames || newer.len() >= MAX_JOURNAL_SUFFIX_TRANSITIONS {
                return Err(corrupt());
            }
            newer.push(Arc::clone(&node.source));
            head = node.predecessor.as_ref();
        }
        for source in newer.into_iter().rev() {
            successor.append_source(source)?;
        }
        if successor.history != self.history {
            return Err(corrupt());
        }
        Ok(successor)
    }
}

pub(crate) fn open(
    access: &RedbReadAccess,
    lineage: ChangelogLineageV3,
    after: ChangelogHistoryPointV3,
) -> Result<Box<dyn ChangelogReceiptCursorV3>, ChangelogCursorErrorV3> {
    let (root, history, sources) = match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => (
            Arc::clone(root),
            read_checkpoint_roots(root)?.ok_or_else(corrupt)?,
            Vec::new(),
        ),
        RedbReadAccess::Composite(view) => {
            let suffix = view.changelog_suffix();
            let history = suffix
                .history
                .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
            if history.tail().frontier()
                != riffdb_types::DualFrontier::new(
                    view.overlay().published_application(),
                    view.overlay().published_administration(),
                )
            {
                return Err(corrupt().into());
            }
            (view.checkpoint_root_shared(), history, suffix.sources()?)
        }
    };
    if lineage.database_id() != history.lineage().database_id()
        || lineage.history_incarnation() != history.lineage().history_incarnation()
    {
        return Err(ChangelogCursorErrorV3::ForeignLineage);
    }
    if lineage.leadership_epoch() != history.lineage().leadership_epoch() {
        return Err(ChangelogCursorErrorV3::StaleEpoch);
    }
    if after.sequence() < history.minimum_resume().sequence() {
        return Err(storage_error(StorageErrorKind::HistoryPruned).into());
    }
    if after.sequence() > history.tail().sequence() {
        return Err(ChangelogCursorErrorV3::InvalidPosition);
    }
    let cursor = ReceiptCursor {
        root,
        history,
        sources,
        current: after,
        failure: None,
    };
    let receipt = cursor.read_receipt(after.sequence())?;
    if ChangelogHistoryPointV3::from_receipt(&receipt).map_err(value_error)? != after {
        return Err(ChangelogCursorErrorV3::InvalidPosition);
    }
    Ok(Box::new(cursor))
}

struct ReceiptCursor {
    root: Arc<CheckpointRoot>,
    history: ChangelogHistoryStateV3,
    sources: Vec<Arc<Source>>,
    current: ChangelogHistoryPointV3,
    failure: Option<ChangelogCursorErrorV3>,
}

impl ReceiptCursor {
    fn read_receipt(
        &self,
        sequence: ChangelogTransactionSequence,
    ) -> Result<AuthoritativeTransactionV3, StorageError> {
        if let Ok(index) = self
            .sources
            .binary_search_by_key(&sequence, |source| source.binding.sequence)
        {
            let source = &self.sources[index];
            let receipt = crate::changelog_v3::receipt_from_validated_mutations(
                source.binding,
                source.attribution,
                &source.mutations,
            )
            .map_err(value_error)?;
            source
                .history
                .validate_terminal_receipt(&receipt)
                .map_err(value_error)?;
            return Ok(receipt);
        }
        let table = self.root.open_table(HISTORY).map_err(table_error)?;
        let value = table
            .get(sequence.get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let receipt = AuthoritativeTransactionV3::decode(value.value()).map_err(value_error)?;
        if receipt.binding().sequence != sequence
            || receipt.binding().database_id != self.history.lineage().database_id()
            || receipt.binding().history_incarnation != self.history.lineage().history_incarnation()
        {
            return Err(corrupt());
        }
        Ok(receipt)
    }

    fn advance(&mut self) -> Result<Option<AuthoritativeTransactionV3>, ChangelogCursorErrorV3> {
        if self.current.sequence() == self.history.tail().sequence() {
            if self.current != self.history.tail() {
                return Err(corrupt().into());
            }
            return Ok(None);
        }
        let sequence = self.current.sequence().checked_next().ok_or_else(corrupt)?;
        let receipt = self.read_receipt(sequence)?;
        let before = ChangelogHistoryStateV3::new(
            self.history.lineage(),
            self.history.anchor(),
            self.current,
            self.history.minimum_resume(),
        )
        .map_err(value_error)?;
        let after = before.advance(&receipt).map_err(value_error)?.tail();
        if after.sequence() != sequence
            || (sequence == self.history.tail().sequence() && after != self.history.tail())
        {
            return Err(corrupt().into());
        }
        self.current = after;
        Ok(Some(receipt))
    }
}

impl ChangelogReceiptCursorV3 for ReceiptCursor {
    fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }

    fn next_receipt(
        &mut self,
    ) -> Result<Option<AuthoritativeTransactionV3>, ChangelogCursorErrorV3> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let result = self.advance();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn limit() -> StorageError {
    storage_error(StorageErrorKind::LimitExceeded)
}

#[cfg(test)]
#[path = "changelog_v3_cursor_tests.rs"]
mod tests;
