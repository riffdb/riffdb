//! Immutable original receipt sources paired with the exact published redb pin.
//! No database handle, mutation lease, journal I/O or durability decision lives here.

#[path = "changelog_v3_progress.rs"]
pub(crate) mod progress;
#[path = "changelog_v3_state.rs"]
pub(crate) mod state;

use crate::locator_memory::{
    ChargedLocatorAllocation, LocatorAllocationClass, LocatorRetireContents, NativeLocatorPool,
    ProviderOnlyFamilyMember,
};
use riffdb_storage_api::{
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogCursorErrorV3, ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
    ChangelogReceiptCursorV3, ChangelogTransactionSequence, CompositeMutationV1, StorageError,
    StorageErrorKind,
};
use std::sync::{Arc, OnceLock, Weak};

use crate::{
    changelog_v3_activation::HISTORY,
    changelog_v3_roots::read_checkpoint_roots,
    changelog_v3_write::value_error,
    checkpoint_root::CheckpointRoot,
    error::{precommit_storage_error, storage_error, table_error},
    journal::{MAX_JOURNAL_FRAME_BYTES, MAX_JOURNAL_SUFFIX_BYTES, MAX_JOURNAL_SUFFIX_TRANSITIONS},
    store::RedbReadAccess,
};

/// One original mutation allocation shared by receipt and checkpoint owners.
/// Borrowing exposes no raw owning alias that can escape its physical token.
#[derive(Clone)]
pub(crate) struct SharedSourceMutations(Arc<SourceMutationsHandle>);
struct SourceMutationsHandle {
    rows: ChargedLocatorAllocation<SourceMutations>,
    // A weak resolver reference keeps only this charge, never the row payload.
    control: SourceControl,
}
pub(crate) struct WeakSourceMutations {
    weak: Weak<SourceMutationsHandle>,
    control: SourceControl,
}
impl std::fmt::Debug for WeakSourceMutations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WeakSourceMutations")
            .finish_non_exhaustive()
    }
}
impl Clone for WeakSourceMutations {
    fn clone(&self) -> Self {
        Self {
            weak: self.weak.clone(),
            control: self.control.clone(),
        }
    }
}
impl WeakSourceMutations {
    pub(crate) fn strong_count(&self) -> usize {
        self.weak.strong_count()
    }
    pub(crate) fn upgrade(&self) -> Option<SharedSourceMutations> {
        self.weak.upgrade().map(SharedSourceMutations)
    }
}
impl std::ops::Deref for SharedSourceMutations {
    type Target = SourceMutations;
    fn deref(&self) -> &SourceMutations {
        &self.0.rows
    }
}
impl AsRef<[CompositeMutationV1]> for SharedSourceMutations {
    fn as_ref(&self) -> &[CompositeMutationV1] {
        &self.rows
    }
}
impl SharedSourceMutations {
    pub(crate) fn downgrade(&self) -> WeakSourceMutations {
        WeakSourceMutations {
            weak: Arc::downgrade(&self.0),
            control: self.0.control.clone(),
        }
    }
    #[cfg(test)]
    pub(crate) fn ptr_eq(first: &Self, second: &Self) -> bool {
        Arc::ptr_eq(&first.0, &second.0)
    }
    fn retained_charge_bound(&self) -> usize {
        self.0.rows.retained_charge_bound()
    }
}
pub(crate) struct SourceMutations {
    rows: Vec<CompositeMutationV1>,
    payload_bytes: u64,
    construction_pool: Arc<NativeLocatorPool>,
    class: LocatorAllocationClass,
    // After rows: queued row retirement retains the full original byte owner.
    provider: OnceLock<ProviderOnlyFamilyMember>,
}
impl AsRef<[CompositeMutationV1]> for SourceMutations {
    fn as_ref(&self) -> &[CompositeMutationV1] {
        &self.rows
    }
}
impl std::ops::Deref for SourceMutations {
    type Target = [CompositeMutationV1];
    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}
impl LocatorRetireContents for SourceMutations {
    fn retire_one(&mut self) -> bool {
        self.rows.pop();
        self.rows.is_empty()
    }
    fn take_for_retirement(&mut self) -> Self {
        Self {
            rows: std::mem::take(&mut self.rows),
            payload_bytes: self.payload_bytes,
            construction_pool: Arc::clone(&self.construction_pool),
            class: self.class,
            provider: std::mem::take(&mut self.provider),
        }
    }
}
impl SourceMutations {
    pub(crate) fn try_new(
        rows: Vec<CompositeMutationV1>,
        class: LocatorAllocationClass,
    ) -> Result<SharedSourceMutations, StorageError> {
        Self::try_new_in_pool(rows, NativeLocatorPool::global(), class)
    }
    pub(crate) fn try_new_in_pool(
        rows: Vec<CompositeMutationV1>,
        pool: Arc<NativeLocatorPool>,
        class: LocatorAllocationClass,
    ) -> Result<SharedSourceMutations, StorageError> {
        if pool.provider_family_identity() != 0 {
            return Err(corrupt());
        }
        let payload = rows.iter().try_fold(
            rows.capacity()
                .checked_mul(std::mem::size_of::<CompositeMutationV1>())
                .and_then(|n| n.checked_add(64))
                .ok_or_else(limit)?,
            |bytes, row| {
                bytes
                    .checked_add(row.key().len())
                    .and_then(|n| n.checked_add(64))
                    .and_then(|n| {
                        n.checked_add(
                            row.value()
                                .map_or(Some(0), |value| value.len().checked_add(64))?,
                        )
                    })
                    .ok_or_else(limit)
            },
        )?;
        let payload_bytes = u64::try_from(payload).map_err(|_| limit())?;
        // Only wrapper/retirement metadata is native. Original row/key/value
        // capacities remain under their unchanged journal/provider ceilings.
        let control = unbound_source_control(
            &pool,
            class,
            std::mem::size_of::<SourceMutationsHandle>() + 64,
        )?;
        let rows = ChargedLocatorAllocation::try_new(&pool, class, 128, || {
            Ok(Self {
                rows,
                payload_bytes,
                construction_pool: Arc::clone(&pool),
                class,
                provider: OnceLock::new(),
            })
        })?;
        let handle = Arc::new(SourceMutationsHandle { rows, control });
        handle
            .control
            .allocation
            .set(SourceAllocationWeak::Mutations(Arc::downgrade(&handle)))
            .map_err(|_| corrupt())?;
        Ok(SharedSourceMutations(handle))
    }
}
fn bind_mutations_family(
    mutations: &SharedSourceMutations,
    pool: &NativeLocatorPool,
) -> Result<(), StorageError> {
    if !mutations.construction_pool.shares_native_budget(pool) {
        return Err(corrupt());
    }
    bind_control_family(&mutations.0.control, pool)?;
    if let Some(member) = mutations.provider.get() {
        return if member.same_provider_family(pool) {
            Ok(())
        } else {
            Err(corrupt())
        };
    }
    let bytes = mutations
        .payload_bytes
        .checked_add(mutations.retained_charge_bound() as u64)
        .ok_or_else(limit)?;
    let member = pool.register_external_provider_member(bytes)?;
    if let Err(member) = mutations.provider.set(member) {
        let same = member.same_provider_family(pool)
            && mutations
                .provider
                .get()
                .is_some_and(|current| current.same_provider_family(pool));
        if !same {
            return Err(corrupt());
        }
    }
    Ok(())
}

// The self-weak reference keeps the represented Arc allocation physically live
// until the queued control drops it, before releasing its numerical charge.
// It owns no data and cannot keep any strong Source/Node/buffer alive.
enum SourceAllocationWeak {
    Source(Weak<Source>),
    Node(Weak<SourceNode>),
    Mutations(Weak<SourceMutationsHandle>),
}
impl SourceAllocationWeak {
    fn release(self) {
        match self {
            Self::Source(weak) => drop(weak),
            Self::Node(weak) => drop(weak),
            Self::Mutations(weak) => drop(weak),
        }
    }
}
struct SourceAllocationControl {
    allocation: OnceLock<SourceAllocationWeak>,
    provider: OnceLock<ProviderOnlyFamilyMember>,
}
impl LocatorRetireContents for SourceAllocationControl {
    fn retire_one(&mut self) -> bool {
        if let Some(allocation) = self.allocation.take() {
            allocation.release();
        }
        true
    }
    fn take_for_retirement(&mut self) -> Self {
        Self {
            allocation: std::mem::take(&mut self.allocation),
            provider: std::mem::take(&mut self.provider),
        }
    }
}
type SourceControl = ChargedLocatorAllocation<SourceAllocationControl>;
fn unbound_source_control(
    pool: &Arc<NativeLocatorPool>,
    class: LocatorAllocationClass,
    represented: usize,
) -> Result<SourceControl, StorageError> {
    ChargedLocatorAllocation::try_new(pool, class, represented, || {
        Ok(SourceAllocationControl {
            allocation: OnceLock::new(),
            provider: OnceLock::new(),
        })
    })
}
fn bind_control_family(
    control: &SourceControl,
    family: &NativeLocatorPool,
) -> Result<(), StorageError> {
    if let Some(member) = control.provider.get() {
        return if member.same_provider_family(family) {
            Ok(())
        } else {
            Err(corrupt())
        };
    }
    let bytes = u64::try_from(control.retained_charge_bound()).map_err(|_| limit())?;
    let member = family.register_external_provider_member(bytes)?;
    if let Err(member) = control.provider.set(member)
        && (!member.same_provider_family(family)
            || !control
                .provider
                .get()
                .is_some_and(|current| current.same_provider_family(family)))
    {
        return Err(corrupt());
    }
    Ok(())
}
fn source_control(
    pool: &Arc<NativeLocatorPool>,
    family: &NativeLocatorPool,
    class: LocatorAllocationClass,
    represented: usize,
) -> Result<SourceControl, StorageError> {
    let control = unbound_source_control(pool, class, represented)?;
    bind_control_family(&control, family)?;
    Ok(control)
}

fn source_control_for_node(
    pool: &Arc<NativeLocatorPool>,
    family: &NativeLocatorPool,
    class: LocatorAllocationClass,
) -> Result<SourceControl, StorageError> {
    let represented = std::mem::size_of::<SourceNode>()
        + 17 * std::mem::size_of::<SourceWeak<SourceNode>>()
        + 128;
    source_control(pool, family, class, represented)
}

// The weak reference drops before its charge-only control. No control owns a
// Node/Source, so binary ancestors cannot become a strong ancestry cycle.
struct SourceWeak<T> {
    weak: Weak<T>,
    control: SourceControl,
}
impl<T> Clone for SourceWeak<T> {
    fn clone(&self) -> Self {
        Self {
            weak: self.weak.clone(),
            control: self.control.clone(),
        }
    }
}
impl<T> SourceWeak<T> {
    fn new(value: &Arc<T>, control: &SourceControl) -> Self {
        Self {
            weak: Arc::downgrade(value),
            control: control.clone(),
        }
    }
    fn upgrade(&self) -> Option<Arc<T>> {
        self.weak.upgrade()
    }
}

struct Source {
    binding: AuthoritativeTransactionBindingV3,
    attribution: ChangelogAttributionV3,
    history: ChangelogHistoryStateV3,
    mutations: SharedSourceMutations,
    encoded_bytes: usize,
    reclaimed_resume: Option<ChangelogHistoryPointV3>,
    // Last: source values and payload handles drop before allocation control.
    control: SourceControl,
}

struct SourceNode {
    predecessor: Option<Arc<SourceNode>>,
    source: Arc<Source>,
    // Weak binary ancestors index the immutable source chain without retaining
    // retired ancestry or copying a population-sized cursor vector per pin.
    ancestors: Box<[SourceWeak<SourceNode>]>,
    control: SourceControl,
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
    // Cached retained source ownership; point/provider capture never walks it.
    ownership_bytes: usize,
    provider_pool: Option<Arc<NativeLocatorPool>>,
    construction_pool: Arc<NativeLocatorPool>,
}

impl ChangelogSuffix {
    pub(crate) fn history_state(&self) -> Option<ChangelogHistoryStateV3> {
        self.history
    }
    pub(crate) fn has_sources(&self) -> bool {
        self.head.is_some()
    }
    #[cfg(test)]
    pub(crate) fn retained_ownership_bound(&self) -> usize {
        self.ownership_bytes
    }
    pub(crate) fn provider_ownership_family(
        &self,
    ) -> Result<
        Option<&Arc<riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamily>>,
        StorageError,
    > {
        self.provider_pool
            .as_ref()
            .map(|pool| pool.provider_accounting_family())
            .transpose()
    }

    pub(crate) fn from_checkpoint(root: &CheckpointRoot) -> Result<Self, StorageError> {
        Self::from_checkpoint_in_pool(root, NativeLocatorPool::global())
    }

    fn from_checkpoint_in_pool(
        root: &CheckpointRoot,
        construction_pool: Arc<NativeLocatorPool>,
    ) -> Result<Self, StorageError> {
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
            ownership_bytes: 0,
            provider_pool: None,
            construction_pool,
        })
    }

    pub(crate) fn append(
        &mut self,
        successor: Option<(AuthoritativeTransactionBindingV3, ChangelogHistoryStateV3)>,
        attribution: ChangelogAttributionV3,
        mutations: SharedSourceMutations,
        encoded_bytes: usize,
    ) -> Result<(), StorageError> {
        let Some((binding, history)) = successor else {
            if self.history.is_some() {
                return Err(corrupt());
            }
            return Ok(());
        };
        let family = match &self.provider_pool {
            Some(pool) => Arc::clone(pool),
            None => self
                .construction_pool
                .fresh_source_family(mutations.class)?,
        };
        bind_mutations_family(&mutations, &family)?;
        let control = source_control(
            &self.construction_pool,
            &family,
            mutations.class,
            std::mem::size_of::<Source>() + 128,
        )?;
        let node_class = mutations.class;
        let source = Arc::new(Source {
            binding,
            attribution,
            history,
            mutations,
            encoded_bytes,
            reclaimed_resume: None,
            control,
        });
        source
            .control
            .allocation
            .set(SourceAllocationWeak::Source(Arc::downgrade(&source)))
            .map_err(|_| corrupt())?;
        self.append_source_in_family(source, family, node_class)
    }

    fn append_source(
        &mut self,
        source: Arc<Source>,
        node_class: LocatorAllocationClass,
    ) -> Result<(), StorageError> {
        let family = self.provider_pool.as_ref().cloned().ok_or_else(corrupt)?;
        self.append_source_in_family(source, family, node_class)
    }

    fn append_source_in_family(
        &mut self,
        source: Arc<Source>,
        family: Arc<NativeLocatorPool>,
        node_class: LocatorAllocationClass,
    ) -> Result<(), StorageError> {
        bind_mutations_family(&source.mutations, &family)?;
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
        let source_bytes = source.mutations.iter().try_fold(
            std::mem::size_of::<SourceNode>()
                + std::mem::size_of::<Source>()
                + 17 * std::mem::size_of::<SourceWeak<SourceNode>>()
                + 256,
            |bytes, mutation| {
                bytes
                    .checked_add(std::mem::size_of::<CompositeMutationV1>())
                    .and_then(|bytes| bytes.checked_add(mutation.key().len()))
                    .and_then(|bytes| bytes.checked_add(64))
                    .and_then(|bytes| {
                        bytes.checked_add(mutation.value().map_or(0, |value| value.len() + 64))
                    })
                    .ok_or_else(limit)
            },
        )?;
        let ownership_bytes = self
            .ownership_bytes
            .checked_add(source_bytes)
            .ok_or_else(limit)?;
        let control = source_control_for_node(&self.construction_pool, &family, node_class)?;
        let mut ancestors = Vec::with_capacity(17);
        if let Some(predecessor) = self.head.as_ref() {
            ancestors.push(SourceWeak::new(predecessor, &predecessor.control));
            for index in 1..17 {
                let previous = ancestors[index - 1].upgrade().ok_or_else(corrupt)?;
                let Some(ancestor) = previous.ancestors.get(index - 1) else {
                    break;
                };
                ancestors.push(ancestor.clone());
            }
        }
        let successor_history = source.history;
        let node = Arc::new(SourceNode {
            predecessor: self.head.clone(),
            source,
            ancestors: ancestors.into_boxed_slice(),
            control,
        });
        node.control
            .allocation
            .set(SourceAllocationWeak::Node(Arc::downgrade(&node)))
            .map_err(|_| corrupt())?;
        self.history = Some(successor_history);
        self.head = Some(node);
        self.provider_pool = Some(family);
        self.frames = frames;
        self.bytes = bytes;
        self.ownership_bytes = ownership_bytes;
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
        let head = self.head.as_ref().ok_or_else(corrupt)?;
        if head.source.attribution != ChangelogAttributionV3::HistoryReclamation {
            return Err(corrupt());
        }
        let family = self.provider_pool.as_ref().ok_or_else(corrupt)?;
        let class = head.source.mutations.class;
        // Header custody includes self-weak owners, so replacement constructs
        // new private wrappers rather than mutating a possibly captured Arc.
        // Both controls are admitted before either allocation or publication.
        let source_control = source_control(
            &self.construction_pool,
            family,
            class,
            std::mem::size_of::<Source>() + 128,
        )?;
        let node_control = source_control_for_node(&self.construction_pool, family, class)?;
        let source = Arc::new(Source {
            binding: head.source.binding,
            attribution: head.source.attribution,
            history: successor,
            mutations: head.source.mutations.clone(),
            encoded_bytes: head.source.encoded_bytes,
            reclaimed_resume: Some(successor.minimum_resume()),
            control: source_control,
        });
        source
            .control
            .allocation
            .set(SourceAllocationWeak::Source(Arc::downgrade(&source)))
            .map_err(|_| corrupt())?;
        let node = Arc::new(SourceNode {
            predecessor: head.predecessor.clone(),
            source,
            ancestors: head.ancestors.clone(),
            control: node_control,
        });
        node.control
            .allocation
            .set(SourceAllocationWeak::Node(Arc::downgrade(&node)))
            .map_err(|_| corrupt())?;
        self.head = Some(node);
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

    #[cfg(test)]
    fn sources(&self) -> Result<Vec<Arc<Source>>, StorageError> {
        self.sources_from(ChangelogTransactionSequence::new(1).expect("sequence one"))
    }

    fn sources_from(
        &self,
        min_sequence: ChangelogTransactionSequence,
    ) -> Result<Vec<Arc<Source>>, StorageError> {
        if self.frames > MAX_JOURNAL_SUFFIX_TRANSITIONS {
            return Err(limit());
        }
        let Some(head) = self.head.as_ref() else {
            if self.frames != 0 {
                return Err(corrupt());
            }
            return Ok(Vec::new());
        };
        if self.frames == 0 {
            return Err(corrupt());
        }
        let tail_seq = head.source.binding.sequence.get();
        let min_seq = min_sequence.get();
        if tail_seq < min_seq {
            return Ok(Vec::new());
        }
        let capacity = usize::try_from(tail_seq - min_seq)
            .ok()
            .and_then(|diff| diff.checked_add(1))
            .unwrap_or(self.frames)
            .min(self.frames);
        let mut sources = Vec::with_capacity(capacity);
        let mut current = Some(head);
        let mut reached_cutoff = false;
        while let Some(node) = current {
            if sources.len() >= self.frames {
                return Err(corrupt());
            }
            if node.source.binding.sequence < min_sequence {
                reached_cutoff = true;
                break;
            }
            sources.push(Arc::clone(&node.source));
            current = node.predecessor.as_ref();
        }
        if !reached_cutoff && sources.len() != self.frames {
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
        let mut successor =
            Self::from_checkpoint_in_pool(root, Arc::clone(&self.construction_pool))?;
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
        if !newer.is_empty() {
            successor.provider_pool = self.provider_pool.clone();
        }
        for source in newer.into_iter().rev() {
            // Only native checkpoint installation calls this rebase. Original
            // buffers/source wrappers keep their existing class; its new node
            // metadata uses the already reserved maintenance progress domain.
            successor.append_source(source, LocatorAllocationClass::Maintenance)?;
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
    let (root, sources) = match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            (Arc::clone(root), Vec::new())
        }
        RedbReadAccess::Composite(view) => (
            view.checkpoint_root_shared(),
            view.changelog_suffix().sources_from(after.sequence())?,
        ),
    };
    let cursor = ReceiptCursor {
        root,
        history,
        sources,
        current: after,
        failure: None,
        _provider_metadata: None,
    };
    let summary = cursor.read_summary(after.sequence())?;
    if summary.point() != after {
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
    let (root, sources) = match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            (Arc::clone(root), Vec::new())
        }
        RedbReadAccess::Composite(view) => (
            view.checkpoint_root_shared(),
            view.changelog_suffix().sources_from(after.sequence())?,
        ),
    };
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
    fn read_summary(
        &self,
        sequence: ChangelogTransactionSequence,
    ) -> Result<crate::changelog_v3::ValidatedReceiptSummaryV3, StorageError> {
        if let Ok(index) = self
            .sources
            .binary_search_by_key(&sequence, |source| source.binding.sequence)
        {
            let source = &self.sources[index];
            let summary = crate::changelog_v3::summary_from_validated_mutations_for_catalog(
                source.binding,
                source.attribution,
                &source.mutations,
                source.history.lineage().catalog_digest(),
            )
            .map_err(value_error)?;
            summary
                .validate_terminal_receipt(source.history)
                .map_err(value_error)?;
            return Ok(summary);
        }
        let table = self.root.open_table(HISTORY).map_err(table_error)?;
        let value = table
            .get(sequence.get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let summary = crate::history_stub::resolve_history_summary_for_journal(value.value())?;
        if summary.binding().sequence != sequence
            || summary.binding().database_id != self.history.lineage().database_id()
            || summary.binding().history_incarnation != self.history.lineage().history_incarnation()
        {
            return Err(corrupt());
        }
        Ok(summary)
    }

    fn read_receipt(
        &self,
        sequence: ChangelogTransactionSequence,
    ) -> Result<AuthoritativeTransactionV3, StorageError> {
        Ok(self.read_receipt_with_point(sequence)?.0)
    }

    fn read_receipt_with_point(
        &self,
        sequence: ChangelogTransactionSequence,
    ) -> Result<(AuthoritativeTransactionV3, Option<ChangelogHistoryPointV3>), StorageError> {
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
            return Ok((receipt, Some(source.history.tail())));
        }
        let table = self.root.open_table(HISTORY).map_err(table_error)?;
        let value = table
            .get(sequence.get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let receipt = crate::history_stub::resolve_history_receipt_for_journal(value.value())?;
        if receipt.binding().sequence != sequence
            || receipt.binding().database_id != self.history.lineage().database_id()
            || receipt.binding().history_incarnation != self.history.lineage().history_incarnation()
        {
            return Err(corrupt());
        }
        Ok((receipt, None))
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
        let (receipt, verified_point) = self.read_receipt_with_point(sequence)?;
        let before = ChangelogHistoryStateV3::new(
            self.history.lineage(),
            self.history.anchor(),
            self.current,
            self.history.minimum_resume(),
        )
        .map_err(value_error)?;
        let after = if let Some(point) = verified_point {
            let row = receipt.binding();
            if row.database_id != before.lineage().database_id()
                || row.history_incarnation != before.lineage().history_incarnation()
                || row.predecessor != Some(before.tail().sequence())
                || row.predecessor_frontier != before.tail().frontier()
                || row.prior_history_hash != before.tail().history_hash()
            {
                return Err(
                    value_error(riffdb_storage_api::ChangelogV3Error::PredecessorMismatch).into(),
                );
            }
            ChangelogHistoryStateV3::new(
                before.lineage(),
                before.anchor(),
                point,
                before.minimum_resume(),
            )
            .map_err(value_error)?
            .tail()
        } else {
            before.advance(&receipt).map_err(value_error)?.tail()
        };
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
