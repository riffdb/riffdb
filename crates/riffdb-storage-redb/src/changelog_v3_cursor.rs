//! Immutable original receipt sources paired with the exact published redb pin.
//! No database handle, mutation lease, journal I/O or durability decision lives here.

#[path = "changelog_v3_progress.rs"]
pub(crate) mod progress;
#[path = "changelog_v3_state.rs"]
pub(crate) mod state;

use riffdb_storage_api::{
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogCursorErrorV3, ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ChangelogReceiptCursorV3, ChangelogTransactionSequence, CompositeMutationV1, StorageError,
    StorageErrorKind,
};
use std::sync::{Arc, Weak};

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
    reclaimed_resume: Option<ChangelogHistoryPointV3>,
}

struct SourceNode {
    predecessor: Option<Arc<SourceNode>>,
    source: Arc<Source>,
    // Weak binary ancestors index the immutable source chain without retaining
    // retired ancestry or copying a population-sized cursor vector per pin.
    ancestors: Box<[Weak<SourceNode>]>,
}

// A full bounded suffix can contain thousands of nodes. Release uniquely owned
// ancestry iteratively, not recursively on a production thread's small stack.
impl Drop for SourceNode {
    fn drop(&mut self) {
        #[cfg(test)]
        let _depth = tests::enter_source_node_drop();
        let mut predecessor = self.predecessor.take();
        while let Some(node) = predecessor {
            match take_source_predecessor(node) {
                Some(mut node) => predecessor = node.predecessor.take(),
                None => break,
            }
        }
    }
}

fn take_source_predecessor(node: Arc<SourceNode>) -> Option<SourceNode> {
    // into_inner releases this reference atomically with the ownership decision.
    // A failed try_unwrap instead retains an Arc that can become the last owner
    // before it is dropped, recursively destroying the remaining ancestry.
    let result = Arc::into_inner(node);
    #[cfg(test)]
    tests::after_predecessor_ownership_decision();
    result
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
    pub(crate) fn history_state(&self) -> Option<ChangelogHistoryStateV3> {
        self.history
    }
    pub(crate) fn has_sources(&self) -> bool {
        self.head.is_some()
    }
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
            reclaimed_resume: None,
        }))
    }

    fn append_source(&mut self, source: Arc<Source>) -> Result<(), StorageError> {
        let previous = self.history.ok_or_else(corrupt)?;
        let binding = source.binding;
        if source.history.lineage() != previous.lineage()
            || source.history.anchor() != previous.anchor()
            || (source.history.minimum_resume() != previous.minimum_resume()
                && !(source.attribution == ChangelogAttributionV3::HistoryReclamation
                    && source.reclaimed_resume == Some(source.history.minimum_resume())
                    && source.history.minimum_resume().sequence()
                        > previous.minimum_resume().sequence()
                    && source.history.minimum_resume().sequence().get()
                        - previous.minimum_resume().sequence().get()
                        <= 256))
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
        let mut ancestors = Vec::with_capacity(17);
        if let Some(predecessor) = self.head.as_ref() {
            ancestors.push(Arc::downgrade(predecessor));
            for index in 1..17 {
                let previous = ancestors[index - 1].upgrade().ok_or_else(corrupt)?;
                let Some(ancestor) = previous.ancestors.get(index - 1) else {
                    break;
                };
                ancestors.push(ancestor.clone());
            }
        }
        self.head = Some(Arc::new(SourceNode {
            predecessor: self.head.clone(),
            source,
            ancestors: ancestors.into_boxed_slice(),
        }));
        self.frames = frames;
        self.bytes = bytes;
        Ok(())
    }

    pub(crate) fn apply_source_cut(
        &mut self,
        proof: &crate::journal::SourceReclamationProof,
    ) -> Result<(), StorageError> {
        let history = self.history.ok_or_else(corrupt)?;
        let predecessor = proof.predecessor();
        if history.lineage() != predecessor.lineage()
            || history.anchor() != predecessor.anchor()
            || history.minimum_resume() != predecessor.minimum_resume()
            || predecessor.tail().sequence().checked_next() != Some(history.tail().sequence())
            || history.tail().frontier() != predecessor.tail().frontier()
        {
            return Err(corrupt());
        }
        let successor = ChangelogHistoryStateV3::new(
            history.lineage(),
            history.anchor(),
            history.tail(),
            proof.retained().minimum_resume(),
        )
        .map_err(value_error)?;
        let head = self
            .head
            .as_mut()
            .and_then(Arc::get_mut)
            .ok_or_else(corrupt)?;
        let source = Arc::get_mut(&mut head.source).ok_or_else(corrupt)?;
        if source.attribution != ChangelogAttributionV3::HistoryReclamation {
            return Err(corrupt());
        }
        source.history = successor;
        source.reclaimed_resume = Some(successor.minimum_resume());
        self.history = Some(successor);
        Ok(())
    }

    pub(crate) fn read_receipt(
        &self,
        root: &CheckpointRoot,
        sequence: ChangelogTransactionSequence,
    ) -> Result<Option<AuthoritativeTransactionV3>, StorageError> {
        let history = self.history.ok_or_else(corrupt)?;
        if sequence < history.minimum_resume().sequence() || sequence > history.tail().sequence() {
            return Ok(None);
        }
        let mut node = self.head.clone();
        while let Some(current) = node {
            if current.source.binding.sequence == sequence {
                let source = &current.source;
                let receipt = crate::changelog_v3::receipt_from_validated_mutations_for_catalog(
                    source.binding,
                    source.attribution,
                    &source.mutations,
                    source.history.lineage().catalog_digest(),
                )
                .map_err(value_error)?;
                source
                    .history
                    .validate_terminal_receipt(&receipt)
                    .map_err(value_error)?;
                return Ok(Some(receipt));
            }
            if current.source.binding.sequence < sequence {
                return Err(corrupt());
            }
            let mut next = current.predecessor.clone();
            for ancestor in current.ancestors.iter().rev() {
                let ancestor = ancestor.upgrade().ok_or_else(corrupt)?;
                if ancestor.source.binding.sequence >= sequence {
                    next = Some(ancestor);
                    break;
                }
            }
            node = next;
        }
        let table = root.open_table(HISTORY).map_err(table_error)?;
        let Some(row) = table
            .get(sequence.get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let receipt = crate::history_stub::decode_history_value(row.value())?;
        if receipt.binding().sequence != sequence
            || receipt.binding().database_id != history.lineage().database_id()
            || receipt.binding().history_incarnation != history.lineage().history_incarnation()
        {
            return Err(corrupt());
        }
        Ok(Some(receipt))
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

/// The source history of this read root, without consulting primary-only
/// consumer statistics. Follower snapshots have the same receipt proof but no
/// source-hold inventory. `open` additionally proves the exact resume receipt.
pub(crate) fn history(
    access: &RedbReadAccess,
) -> Result<ChangelogHistoryStateV3, ChangelogCursorErrorV3> {
    match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            Ok(read_checkpoint_roots(root)?.ok_or_else(corrupt)?)
        }
        RedbReadAccess::Composite(view) => {
            let history = view
                .changelog_suffix()
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
            Ok(history)
        }
    }
}

pub(crate) fn history_admitted_v1(
    access: &RedbReadAccess,
    budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
) -> Result<ChangelogHistoryStateV3, ChangelogCursorErrorV3> {
    match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => Ok(
            crate::changelog_v3_roots::read_checkpoint_roots_admitted_v1(root, budget)?
                .ok_or_else(corrupt)?,
        ),
        RedbReadAccess::Composite(_) => history(access),
    }
}

pub(crate) fn open(
    access: &RedbReadAccess,
    lineage: ChangelogLineageV3,
    after: ChangelogHistoryPointV3,
) -> Result<Box<dyn ChangelogReceiptCursorV3>, ChangelogCursorErrorV3> {
    let history = history(access)?;
    let (root, sources) = match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            (Arc::clone(root), Vec::new())
        }
        RedbReadAccess::Composite(view) => (
            view.checkpoint_root_shared(),
            view.changelog_suffix().sources()?,
        ),
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
        _provider_metadata: None,
    };
    let receipt = cursor.read_receipt(after.sequence())?;
    if ChangelogHistoryPointV3::from_receipt(&receipt).map_err(value_error)? != after {
        return Err(ChangelogCursorErrorV3::InvalidPosition);
    }
    Ok(Box::new(cursor))
}

pub(crate) fn open_admitted_v1(
    access: &RedbReadAccess,
    lineage: ChangelogLineageV3,
    after: ChangelogHistoryPointV3,
    budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
) -> Result<Box<dyn ChangelogReceiptCursorV3>, ChangelogCursorErrorV3> {
    // The existing 65,536-source suffix ceiling bounds the new Arc-vector.
    let metadata_bytes = MAX_JOURNAL_SUFFIX_TRANSITIONS
        .checked_add(4)
        .and_then(|count| count.checked_mul(std::mem::size_of::<Arc<Source>>() * 2))
        .and_then(|bytes| bytes.checked_add(32 * 1024))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
    let metadata = budget
        .reserve(metadata_bytes)
        .and_then(|work| work.commit())
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    let history = history_admitted_v1(access, budget)?;
    let (root, sources) = match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            (Arc::clone(root), Vec::new())
        }
        RedbReadAccess::Composite(view) => (
            view.checkpoint_root_shared(),
            view.changelog_suffix().sources()?,
        ),
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
        _provider_metadata: Some(metadata),
    };
    let receipt = cursor.read_receipt_candidate_admitted_v1(after.sequence(), budget)?;
    if ChangelogHistoryPointV3::from_receipt(&receipt.receipt).map_err(value_error)? != after {
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
    // Last: cursor/source-vector data is destroyed before provider metadata admission.
    _provider_metadata: Option<Arc<riffdb_types::exact_provider_budget_v1::OwnershipCharge>>,
}

use crate::history_stub::ProviderReceiptCandidate;

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
            let receipt = crate::changelog_v3::receipt_from_validated_mutations_for_catalog(
                source.binding,
                source.attribution,
                &source.mutations,
                source.history.lineage().catalog_digest(),
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
        let receipt = AuthoritativeTransactionV3::decode(
            &crate::history_stub::resolve_history_value(value.value())?,
        )
        .map_err(value_error)?;
        if receipt.binding().sequence != sequence
            || receipt.binding().database_id != self.history.lineage().database_id()
            || receipt.binding().history_incarnation != self.history.lineage().history_incarnation()
        {
            return Err(corrupt());
        }
        Ok(receipt)
    }

    fn read_receipt_candidate_admitted_v1(
        &self,
        sequence: ChangelogTransactionSequence,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<ProviderReceiptCandidate, ChangelogCursorErrorV3> {
        let unavailable = || storage_error(StorageErrorKind::Unavailable);
        let (receipt, work) = if let Ok(index) = self
            .sources
            .binary_search_by_key(&sequence, |source| source.binding.sequence)
        {
            let source = &self.sources[index];
            let bytes = riffdb_storage_api::provider_receipt_work_bound_v1(
                source.mutations.len(),
                source.encoded_bytes,
            )
            .map_err(|_| unavailable())?;
            let work = budget.reserve(bytes).map_err(|_| unavailable())?;
            let receipt = self.read_receipt(sequence)?;
            (receipt, work)
        } else {
            let table = self.root.open_table(HISTORY).map_err(table_error)?;
            let value = table
                .get(sequence.get().to_be_bytes().as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            let candidate =
                crate::history_stub::provider_receipt_from_value_v1(value.value(), budget)?;
            (candidate.receipt, candidate.work)
        };
        let candidate = ProviderReceiptCandidate { receipt, work };
        if candidate.receipt.binding().sequence != sequence
            || candidate.receipt.binding().database_id != self.history.lineage().database_id()
            || candidate.receipt.binding().history_incarnation
                != self.history.lineage().history_incarnation()
        {
            return Err(corrupt().into());
        }
        Ok(candidate)
    }

    fn advance_admitted_v1(
        &mut self,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<
        Option<
            Arc<
                riffdb_types::exact_provider_budget_v1::ProviderAllocation<
                    AuthoritativeTransactionV3,
                >,
            >,
        >,
        ChangelogCursorErrorV3,
    > {
        if self.current.sequence() == self.history.tail().sequence() {
            if self.current != self.history.tail() {
                return Err(corrupt().into());
            }
            return Ok(None);
        }
        let sequence = self.current.sequence().checked_next().ok_or_else(corrupt)?;
        let candidate = self.read_receipt_candidate_admitted_v1(sequence, budget)?;
        let before = ChangelogHistoryStateV3::new(
            self.history.lineage(),
            self.history.anchor(),
            self.current,
            self.history.minimum_resume(),
        )
        .map_err(value_error)?;
        let after = before
            .advance(&candidate.receipt)
            .map_err(value_error)?
            .tail();
        if after.sequence() != sequence
            || (sequence == self.history.tail().sequence() && after != self.history.tail())
        {
            return Err(corrupt().into());
        }
        let receipt = candidate
            .work
            .settle_owned_value(candidate.receipt)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        self.current = after;
        Ok(Some(receipt))
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
        if self._provider_metadata.is_some() && self.failure.is_none() {
            self.failure = Some(storage_error(StorageErrorKind::Unavailable).into());
        }
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let result = self.advance();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
    fn next_receipt_admitted_v1(
        &mut self,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<
        Option<
            Arc<
                riffdb_types::exact_provider_budget_v1::ProviderAllocation<
                    AuthoritativeTransactionV3,
                >,
            >,
        >,
        ChangelogCursorErrorV3,
    > {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self
            ._provider_metadata
            .as_ref()
            .is_some_and(|owner| !owner.belongs_to(budget))
        {
            let error: ChangelogCursorErrorV3 = storage_error(StorageErrorKind::Unavailable).into();
            self.failure = Some(error.clone());
            return Err(error);
        }
        let result = self.advance_admitted_v1(budget);
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
