//! Redb checkpoint roots bound to ADR-0104's immutable state overlay.
//!
//! WP-487 deliberately does not install these values as the production read
//! frontier. WP-488 owns publication after a known journal fence.

#![allow(
    dead_code,
    reason = "WP-487 builds the closed view; WP-488 installs its production publisher"
)]

use std::sync::{Arc, OnceLock, RwLock};

use redb::TableDefinition;
use riffdb_storage_api::{
    BoundedCompositePage, CompositeCheckpointV1, CompositeFrameV1, CompositeMutationStage,
    CompositeOverlayBuilder, CompositeTableV1, CompositeViewBase, FrozenCompositeOverlay,
    StorageError, StorageErrorKind, StorageValueError,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, SchemaHash};

use crate::checkpoint_root::CheckpointRoot;
use crate::codec::{
    IdempotencyRecordV1, decode_administration_audit_record_v1,
    decode_administration_sequence_allocator_v1, decode_application_sequence_allocator_v1,
    decode_database_identity_v1, decode_durable_event_v1, decode_entity_record_v1,
    decode_event_route_v1, decode_history_incarnation_v1, decode_idempotency_record_v1,
    decode_index_entry_v2, decode_index_epoch_v1, decode_pending_admission_v1,
    decode_provenance_record_v1, decode_record_registry_v2, decode_service_audit_request_index_v1,
    decode_vector_evidence_index_v1, decode_vector_evidence_v1,
    decode_vector_health_observation_v1, decode_vector_observation_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::journal::JournalFrame;
use crate::keys::{
    decode_application_sequence_key, decode_audit_by_request_key, decode_audit_key,
    decode_entity_key, decode_event_key, decode_event_route_key, decode_idempotency_key,
    decode_index_entry_key, decode_partition_index_key, decode_provenance_key,
    decode_vector_evidence_index_key, decode_vector_evidence_key,
    decode_vector_health_observation_key, decode_vector_observation_key,
};
use crate::layout::{META_DATABASE_ID, META_HISTORY_INCARNATION, META_RECORD_REGISTRY};
use crate::store::{RedbOperationalPorts, read_administration_tail, read_commit_tail};

/// Unpublished builder that proves one overlay against one frozen redb root.
pub(crate) struct RedbCompositeViewBuilder {
    root: Arc<CheckpointRoot>,
    overlay: CompositeOverlayBuilder,
}

impl RedbCompositeViewBuilder {
    /// Captures one operational root and proves its checkpoint identity.
    pub(crate) fn capture(
        ports: &RedbOperationalPorts,
        checkpoint_frame_hash: [u8; 32],
    ) -> Result<Self, StorageError> {
        let root = ports.begin_read()?.into_shared()?;
        Self::from_root(root, checkpoint_frame_hash)
    }

    pub(crate) fn from_root(
        root: Arc<CheckpointRoot>,
        checkpoint_frame_hash: [u8; 32],
    ) -> Result<Self, StorageError> {
        let checkpoint = checkpoint_identity(&root, checkpoint_frame_hash)?;
        Ok(Self {
            root,
            overlay: CompositeOverlayBuilder::new(checkpoint),
        })
    }

    /// Applies one already-decoded journal frame after proving exact canonical
    /// table bytes and predecessor state against this root plus prior frames.
    pub(crate) fn apply_frame(&mut self, frame: &JournalFrame) -> Result<(), StorageError> {
        let frame = frame.composite().map_err(corrupt_value)?;
        self.apply_composite_frame(&frame)
    }

    /// Forks a captured published view into sole-writer private state without
    /// copying its complete suffix map.
    pub(crate) fn from_published(published: &Arc<RedbCompositeReadView>) -> Self {
        Self {
            root: Arc::clone(&published.root),
            overlay: CompositeOverlayBuilder::from_published(&published.overlay),
        }
    }

    fn apply_composite_frame(&mut self, frame: &CompositeFrameV1) -> Result<(), StorageError> {
        self.overlay
            .apply_frame(frame, &RedbCheckpointBase { root: &self.root })
            .map_err(corrupt_value)
    }

    /// Freezes one checkpoint-plus-suffix identity without publishing it.
    pub(crate) fn freeze(self) -> RedbCompositeReadView {
        RedbCompositeReadView {
            root: self.root,
            overlay: self.overlay.freeze(),
            snapshot_head: OnceLock::new(),
        }
    }
}

/// One immutable redb checkpoint and exact validated overlay.
pub(crate) struct RedbCompositeReadView {
    root: Arc<CheckpointRoot>,
    overlay: FrozenCompositeOverlay,
    /// The snapshot-visible application frontier of THIS view, derived once.
    ///
    /// Empty in every constructor below, so a successor can never inherit its
    /// predecessor's frontier: each one is a distinct published state.
    snapshot_head: OnceLock<Option<CommitSequence>>,
}

/// Redb-rooted private mutation stage for one command subgroup.
pub(crate) struct RedbCompositeMutationStage {
    root: Arc<CheckpointRoot>,
    stage: CompositeMutationStage,
}

impl RedbCompositeMutationStage {
    pub(crate) fn from_published(published: &Arc<RedbCompositeReadView>) -> Self {
        Self {
            root: Arc::clone(&published.root),
            stage: CompositeMutationStage::new(&published.overlay),
        }
    }

    pub(crate) fn apply(
        &mut self,
        mutation: &crate::journal::JournalMutation,
    ) -> Result<(), StorageError> {
        let mutation = mutation.composite().map_err(corrupt_value)?;
        self.stage
            .apply(mutation, &RedbCheckpointBase { root: &self.root })
            .map_err(corrupt_value)
    }

    /// Applies a mutation against bytes read from this exact private stage by
    /// the sole writer immediately before mutation construction.
    pub(crate) fn apply_with_observed_current(
        &mut self,
        mutation: &crate::journal::JournalMutation,
        observed_current: Option<&[u8]>,
    ) -> Result<(), StorageError> {
        let mutation = mutation.composite().map_err(corrupt_value)?;
        self.stage
            .apply_with_observed_current(
                mutation,
                observed_current,
                &RedbCheckpointBase { root: &self.root },
            )
            .map_err(corrupt_value)
    }

    /// Consumes canonical record bytes and exact current bytes already proven
    /// by the storage-owned typed staging path.
    pub(crate) fn apply_with_proven_current(
        &mut self,
        mutation: &crate::journal::JournalMutation,
        proven_current: Option<&[u8]>,
    ) -> Result<(), StorageError> {
        let mutation = mutation.composite().map_err(corrupt_value)?;
        self.stage
            .apply_with_proven_current(mutation, proven_current)
            .map_err(corrupt_value)
    }

    pub(crate) fn resolve_point(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.stage
            .resolve_point(&RedbCheckpointBase { root: &self.root }, table, key)
            .map_err(corrupt_value)
    }

    /// Reads one capability from the immutable root paired with this private
    /// application overlay. Capability rows are not journal tables, and the
    /// global mutation gate prevents administration from changing them while
    /// the owning command epoch is open.
    pub(crate) fn read_capability_bytes(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let table = self
            .root
            .capability_table()
            .map_err(crate::error::table_error)?;
        table
            .get(key)
            .map_err(crate::error::precommit_storage_error)
            .map(|value| value.map(|value| value.value().to_vec()))
    }

    pub(crate) fn mutation_count(&self) -> usize {
        self.stage.mutation_count()
    }

    pub(crate) fn seal_frame(
        self,
        frame: &JournalFrame,
    ) -> Result<RedbCompositeReadView, StorageError> {
        let frame = frame.composite().map_err(corrupt_value)?;
        let overlay = self.stage.seal_frame(&frame).map_err(corrupt_value)?;
        Ok(RedbCompositeReadView {
            root: self.root,
            overlay,
            snapshot_head: OnceLock::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn seal_encoded_frame(
        self,
        kind: riffdb_storage_api::CompositeFrameKindV1,
        database_id: DatabaseId,
        predecessor_application: Option<CommitSequence>,
        covered_application: Option<CommitSequence>,
        predecessor_administration: Option<AdministrationSequence>,
        covered_administration: Option<AdministrationSequence>,
        transition_count: u16,
        encoded_bytes: usize,
        previous_hash: [u8; 32],
        frame_hash: [u8; 32],
    ) -> Result<RedbCompositeReadView, StorageError> {
        let overlay = self
            .stage
            .seal_encoded_frame(
                kind,
                database_id,
                predecessor_application,
                covered_application,
                predecessor_administration,
                covered_administration,
                transition_count,
                encoded_bytes,
                previous_hash,
                frame_hash,
            )
            .map_err(corrupt_value)?;
        Ok(RedbCompositeReadView {
            root: self.root,
            overlay,
            snapshot_head: OnceLock::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn seal_encoded_frame_with_mutations(
        self,
        kind: riffdb_storage_api::CompositeFrameKindV1,
        database_id: DatabaseId,
        predecessor_application: Option<CommitSequence>,
        covered_application: Option<CommitSequence>,
        predecessor_administration: Option<AdministrationSequence>,
        covered_administration: Option<AdministrationSequence>,
        transition_count: u16,
        encoded_bytes: usize,
        previous_hash: [u8; 32],
        frame_hash: [u8; 32],
    ) -> Result<
        (
            RedbCompositeReadView,
            Vec<riffdb_storage_api::CompositeMutationV1>,
        ),
        StorageError,
    > {
        let (overlay, mutations) = self
            .stage
            .seal_encoded_frame_with_mutations(
                kind,
                database_id,
                predecessor_application,
                covered_application,
                predecessor_administration,
                covered_administration,
                transition_count,
                encoded_bytes,
                previous_hash,
                frame_hash,
            )
            .map_err(corrupt_value)?;
        Ok((
            RedbCompositeReadView {
                root: self.root,
                overlay,
                snapshot_head: OnceLock::new(),
            },
            mutations,
        ))
    }

    pub(crate) fn read_checkpoint_bytes(
        &self,
        definition: TableDefinition<'static, &'static [u8], &'static [u8]>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.root
            .open_table(definition)
            .map_err(table_error)?
            .get(key)
            .map_err(precommit_storage_error)
            .map(|value| value.map(|value| value.value().to_vec()))
    }

    pub(crate) fn merge_bounded(
        &self,
        table: CompositeTableV1,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        max_rows: usize,
        max_inspected: usize,
    ) -> Result<BoundedCompositePage, StorageError> {
        let base = self
            .root
            .journal_byte_table(journal_table(table))
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .map_err(table_error)?;
        let end = end_exclusive
            .map(std::ops::Bound::Excluded)
            .unwrap_or(std::ops::Bound::Unbounded);
        let rows = base
            .range::<&[u8]>((std::ops::Bound::Included(start_inclusive), end))
            .map_err(precommit_storage_error)?
            .map(|row| {
                row.map(|(key, value)| {
                    (
                        key.value().to_vec().into_boxed_slice(),
                        value.value().to_vec().into_boxed_slice(),
                    )
                })
                .map_err(invalid_shape)
            });
        self.stage
            .merge_bounded(
                table,
                rows,
                start_inclusive,
                end_exclusive,
                max_rows,
                max_inspected,
            )
            .map_err(corrupt_value)
    }

    pub(crate) fn merge_bounded_reverse(
        &self,
        table: CompositeTableV1,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        max_rows: usize,
        max_inspected: usize,
    ) -> Result<BoundedCompositePage, StorageError> {
        let base = self
            .root
            .journal_byte_table(journal_table(table))
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .map_err(table_error)?;
        let end = end_exclusive
            .map(std::ops::Bound::Excluded)
            .unwrap_or(std::ops::Bound::Unbounded);
        let rows = base
            .range::<&[u8]>((std::ops::Bound::Included(start_inclusive), end))
            .map_err(precommit_storage_error)?
            .rev()
            .map(|row| {
                row.map(|(key, value)| {
                    (
                        key.value().to_vec().into_boxed_slice(),
                        value.value().to_vec().into_boxed_slice(),
                    )
                })
                .map_err(invalid_shape)
            });
        self.stage
            .merge_bounded_reverse(
                table,
                rows,
                start_inclusive,
                end_exclusive,
                max_rows,
                max_inspected,
            )
            .map_err(corrupt_value)
    }
}

/// One atomic publication cell for a checkpoint-plus-overlay read view.
///
/// Publication compares the exact captured predecessor object, not merely its
/// numeric frontier. A late fence can therefore never replace a newer view.
pub(crate) struct RedbCompositePublication {
    current: RwLock<Arc<RedbCompositeReadView>>,
}

impl RedbCompositePublication {
    pub(crate) fn new(initial: RedbCompositeReadView) -> Self {
        Self {
            current: RwLock::new(Arc::new(initial)),
        }
    }

    pub(crate) fn capture(&self) -> Result<Arc<RedbCompositeReadView>, StorageError> {
        self.current
            .read()
            .map(|current| Arc::clone(&current))
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
    }

    pub(crate) fn publish_successor(
        &self,
        expected: &Arc<RedbCompositeReadView>,
        successor: Arc<RedbCompositeReadView>,
    ) -> Result<Arc<RedbCompositeReadView>, StorageError> {
        let mut current = self
            .current
            .write()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        if !Arc::ptr_eq(&current, expected)
            || !Arc::ptr_eq(&successor.root, &expected.root)
            || successor.overlay.checkpoint() != expected.overlay.checkpoint()
            || successor.overlay.transition_count() <= expected.overlay.transition_count()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *current = Arc::clone(&successor);
        Ok(successor)
    }

    /// Replaces only the physical checkpoint beneath an identical logical
    /// published frontier after asynchronous materialization succeeds.
    pub(crate) fn publish_rebased(
        &self,
        expected: &Arc<RedbCompositeReadView>,
        successor: Arc<RedbCompositeReadView>,
    ) -> Result<Arc<RedbCompositeReadView>, StorageError> {
        let mut current = self
            .current
            .write()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        if !Arc::ptr_eq(&current, expected)
            || Arc::ptr_eq(&successor.root, &expected.root)
            || successor.overlay.published_application() != expected.overlay.published_application()
            || successor.overlay.published_administration()
                != expected.overlay.published_administration()
            || successor.overlay.terminal_frame_hash() != expected.overlay.terminal_frame_hash()
            || successor.overlay.checkpoint().application_frontier()
                < expected.overlay.checkpoint().application_frontier()
            || successor.overlay.checkpoint().administration_frontier()
                < expected.overlay.checkpoint().administration_frontier()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *current = Arc::clone(&successor);
        Ok(successor)
    }
}

impl RedbCompositeReadView {
    pub(crate) fn checkpoint_root(&self) -> &CheckpointRoot {
        &self.root
    }

    pub(crate) fn checkpoint_root_shared(&self) -> Arc<CheckpointRoot> {
        Arc::clone(&self.root)
    }

    pub(crate) fn overlay(&self) -> &FrozenCompositeOverlay {
        &self.overlay
    }

    /// Resolves the snapshot-visible application frontier once per view.
    ///
    /// `derive` is the complete uncached derivation, including the cross-check
    /// that the overlay's published application frontier equals the allocator's
    /// predecessor. Both inputs are fixed for this view's whole life — the
    /// overlay is frozen and the checkpoint root is one immutable snapshot — so
    /// deriving once and reusing the answer is exactly equivalent to deriving
    /// it per access, and the cross-check still runs against the same state it
    /// guards. A published successor is a different view with its own empty
    /// cell, so an advancing frontier is never masked.
    ///
    /// A failure is not cached, so a corrupt allocator, or a checkpoint that
    /// disagrees with its overlay, can never be converted into a frontier.
    pub(crate) fn snapshot_head<E>(
        &self,
        derive: impl FnOnce() -> Result<Option<CommitSequence>, E>,
    ) -> Result<Option<CommitSequence>, E> {
        crate::checkpoint_root::derive_once(&self.snapshot_head, derive)
    }

    /// Re-roots the exact published successor after `covered` has become the
    /// durable redb checkpoint, retaining only concurrent newer final states.
    pub(crate) fn rebase_after(
        &self,
        covered: &Self,
        root: Arc<CheckpointRoot>,
        checkpoint_frame_hash: [u8; 32],
    ) -> Result<Self, StorageError> {
        let checkpoint = checkpoint_identity(&root, checkpoint_frame_hash)?;
        let overlay = self
            .overlay
            .rebase_after(&covered.overlay, checkpoint)
            .map_err(corrupt_value)?;
        Ok(Self {
            root,
            overlay,
            snapshot_head: OnceLock::new(),
        })
    }

    pub(crate) fn resolve_point(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.overlay
            .resolve_point(&RedbCheckpointBase { root: &self.root }, table, key)
            .map_err(corrupt_value)
    }

    /// Merges one bounded byte-table range through this exact captured view.
    pub(crate) fn merge_bounded(
        &self,
        table: CompositeTableV1,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        max_rows: usize,
        max_inspected: usize,
    ) -> Result<BoundedCompositePage, StorageError> {
        let base = self
            .root
            .journal_byte_table(journal_table(table))
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .map_err(table_error)?;
        let end = end_exclusive
            .map(std::ops::Bound::Excluded)
            .unwrap_or(std::ops::Bound::Unbounded);
        let rows = base
            .range::<&[u8]>((std::ops::Bound::Included(start_inclusive), end))
            .map_err(precommit_storage_error)?
            .map(|row| {
                row.map(|(key, value)| {
                    (
                        key.value().to_vec().into_boxed_slice(),
                        value.value().to_vec().into_boxed_slice(),
                    )
                })
                .map_err(invalid_shape)
            });
        self.overlay
            .merge_bounded(
                table,
                rows,
                start_inclusive,
                end_exclusive,
                max_rows,
                max_inspected,
            )
            .map_err(corrupt_value)
    }

    /// Merges one bounded byte-table range in descending key order through
    /// this exact captured view.
    pub(crate) fn merge_bounded_reverse(
        &self,
        table: CompositeTableV1,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        max_rows: usize,
        max_inspected: usize,
    ) -> Result<BoundedCompositePage, StorageError> {
        let base = self
            .root
            .journal_byte_table(journal_table(table))
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .map_err(table_error)?;
        let end = end_exclusive
            .map(std::ops::Bound::Excluded)
            .unwrap_or(std::ops::Bound::Unbounded);
        let rows = base
            .range::<&[u8]>((std::ops::Bound::Included(start_inclusive), end))
            .map_err(precommit_storage_error)?
            .rev()
            .map(|row| {
                row.map(|(key, value)| {
                    (
                        key.value().to_vec().into_boxed_slice(),
                        value.value().to_vec().into_boxed_slice(),
                    )
                })
                .map_err(invalid_shape)
            });
        self.overlay
            .merge_bounded_reverse(
                table,
                rows,
                start_inclusive,
                end_exclusive,
                max_rows,
                max_inspected,
            )
            .map_err(corrupt_value)
    }
}

struct RedbCheckpointBase<'root> {
    root: &'root CheckpointRoot,
}

impl CompositeViewBase for RedbCheckpointBase<'_> {
    fn read_base(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageValueError> {
        self.root
            .read_value(journal_table(table), key)
            .map_err(|_| StorageValueError::InvalidShape)
    }

    fn validate_entry(
        &self,
        table: CompositeTableV1,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(), StorageValueError> {
        validate_canonical_entry(table, key, value)
    }
}

fn checkpoint_identity(
    root: &CheckpointRoot,
    checkpoint_frame_hash: [u8; 32],
) -> Result<CompositeCheckpointV1, StorageError> {
    let meta = root.meta_table().map_err(table_error)?;
    let database_bytes = required_meta(meta, META_DATABASE_ID)?;
    let history_bytes = required_meta(meta, META_HISTORY_INCARNATION)?;
    let registry_bytes = required_meta(meta, META_RECORD_REGISTRY)?;
    let database_id = decode_database_identity_v1(&database_bytes)?.into_parts().0;
    let history_incarnation = decode_history_incarnation_v1(&history_bytes)?
        .into_parts()
        .0;
    let registry_digest: SchemaHash = decode_record_registry_v2(&registry_bytes)?.into_parts().0;
    let application_frontier = read_commit_tail(root)?;
    let administration_frontier = read_administration_tail(root)?;
    CompositeCheckpointV1::new(
        database_id,
        history_incarnation,
        registry_digest,
        application_frontier,
        administration_frontier,
        checkpoint_frame_hash,
    )
    .map_err(corrupt_value)
}

fn required_meta(
    meta: &redb::ReadOnlyTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<Vec<u8>, StorageError> {
    meta.get(key)
        .map_err(precommit_storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))
}

fn validate_canonical_entry(
    table: CompositeTableV1,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<(), StorageValueError> {
    match table {
        CompositeTableV1::Meta => {
            let key = std::str::from_utf8(key).map_err(invalid_shape)?;
            let value = value.ok_or(StorageValueError::InvalidShape)?;
            match key {
                crate::layout::META_APPLICATION_SEQUENCE => {
                    decode_application_sequence_allocator_v1(value).map_err(invalid_shape)?;
                }
                crate::layout::META_ADMINISTRATION_SEQUENCE => {
                    decode_administration_sequence_allocator_v1(value).map_err(invalid_shape)?;
                }
                _ => return Err(StorageValueError::InvalidShape),
            }
        }
        CompositeTableV1::Entities => {
            let physical = decode_entity_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = decode_entity_record_v1(value).map_err(invalid_shape)?;
                if decoded.value().target().key() != &physical {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::SecondaryIndexes => {
            let physical = decode_index_entry_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = decode_index_entry_v2(value).map_err(invalid_shape)?;
                if decoded.value().key() != &physical {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::IndexEpochs => {
            let physical = decode_partition_index_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = decode_index_epoch_v1(value).map_err(invalid_shape)?;
                if decoded.value().target() != &physical {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        // ADR-0165 locator rows: the key must decode as its own kind and the
        // value must decode as a command locator. Anything else is closed here
        // rather than reaching a reader that could report absence.
        CompositeTableV1::IdempotencyLocators => {
            crate::keys::decode_idempotency_key(key).map_err(invalid_shape)?;
            let value = value.ok_or(StorageValueError::InvalidShape)?;
            crate::codec::decode_command_locator_v1(value).map_err(invalid_shape)?;
        }
        CompositeTableV1::ProvenanceLocators => {
            crate::keys::decode_provenance_key(key).map_err(invalid_shape)?;
            let value = value.ok_or(StorageValueError::InvalidShape)?;
            crate::codec::decode_command_locator_v1(value).map_err(invalid_shape)?;
        }
        CompositeTableV1::AuditByRequestLocators => {
            crate::keys::decode_audit_by_request_key(key).map_err(invalid_shape)?;
            let value = value.ok_or(StorageValueError::InvalidShape)?;
            crate::codec::decode_command_locator_v1(value).map_err(invalid_shape)?;
        }
        CompositeTableV1::Idempotency => {
            let physical = decode_idempotency_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = decode_idempotency_record_v1(value).map_err(invalid_shape)?;
                let matches = match decoded.value() {
                    IdempotencyRecordV1::StoredOutcome(outcome) => {
                        outcome.identity().storage_key().ok().as_ref() == Some(&physical)
                    }
                    IdempotencyRecordV1::ExecutionFailed(failure) => {
                        failure.pending().identity().storage_key().ok().as_ref() == Some(&physical)
                    }
                    IdempotencyRecordV1::CommandLocator(_) => true,
                };
                if !matches {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::IdempotencyPending => {
            let physical = decode_idempotency_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = decode_pending_admission_v1(value).map_err(invalid_shape)?;
                if decoded.value().identity().storage_key().ok().as_ref() != Some(&physical) {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::Events => {
            let physical = decode_event_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = decode_durable_event_v1(value).map_err(invalid_shape)?;
                if decoded.value().event_id() != physical {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::EventRoutes => {
            let (_, event_id) = decode_event_route_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = decode_event_route_v1(value).map_err(invalid_shape)?;
                if decoded.value().event_id() != event_id {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::Outbox => {
            let physical = decode_event_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = riffdb_storage_api::decode_outbox_event_reference(value)
                    .map_err(invalid_shape)?;
                if decoded.value().event_id() != physical {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::Provenance => {
            let physical = decode_provenance_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                match riffdb_storage_api::decode_command_locator_v1(value) {
                    Ok(_) => {}
                    Err(error)
                        if error.kind()
                            == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                    {
                        let decoded = decode_provenance_record_v1(value).map_err(invalid_shape)?;
                        if decoded.value().provenance_id() != physical {
                            return Err(StorageValueError::IdentityMismatch);
                        }
                    }
                    Err(_) => return Err(StorageValueError::InvalidShape),
                }
            }
        }
        CompositeTableV1::Commits => {
            let physical = decode_application_sequence_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded =
                    riffdb_storage_api::decode_command_segment_v1(value).map_err(invalid_shape)?;
                if decoded.value().first_commit_sequence() != physical {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::Audit => {
            decode_audit_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                match riffdb_storage_api::decode_command_audit_locator_v1(value) {
                    Ok(_) => {}
                    Err(error)
                        if error.kind()
                            == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                    {
                        decode_administration_audit_record_v1(value).map_err(invalid_shape)?;
                    }
                    Err(_) => return Err(StorageValueError::InvalidShape),
                }
            }
        }
        CompositeTableV1::AuditByRequest => {
            let physical = decode_audit_by_request_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded =
                    decode_service_audit_request_index_v1(value).map_err(invalid_shape)?;
                if (
                    decoded.value().request_id(),
                    decoded.value().administration_sequence(),
                ) != physical
                {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::EntityChainHeads => {
            let physical = decode_entity_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = riffdb_storage_api::decode_entity_chain_head_v1(value)
                    .map_err(invalid_shape)?;
                if decoded.value().target().key() != &physical {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::VectorEvidence => {
            let (entity_key, field) = decode_vector_evidence_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = decode_vector_evidence_v1(value).map_err(invalid_shape)?;
                if decoded.value().target().key() != &entity_key
                    || decoded.value().vector_field() != field
                {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
        CompositeTableV1::VectorObservations => {
            if let Ok(lineage) = decode_vector_health_observation_key(key) {
                if let Some(value) = value {
                    let decoded =
                        decode_vector_health_observation_v1(value).map_err(invalid_shape)?;
                    if decoded.value().lineage() != &lineage {
                        return Err(StorageValueError::IdentityMismatch);
                    }
                }
            } else {
                let physical = decode_vector_observation_key(key).map_err(invalid_shape)?;
                if let Some(value) = value {
                    let decoded = decode_vector_observation_v1(value).map_err(invalid_shape)?;
                    if decoded.value().target() != &physical {
                        return Err(StorageValueError::IdentityMismatch);
                    }
                }
            }
        }
        CompositeTableV1::VectorEvidenceIndex => {
            let (target, entity_key) =
                decode_vector_evidence_index_key(key).map_err(invalid_shape)?;
            if let Some(value) = value {
                let decoded = decode_vector_evidence_index_v1(value).map_err(invalid_shape)?;
                if decoded.value().target() != &target
                    || decoded.value().entity_key() != &entity_key
                {
                    return Err(StorageValueError::IdentityMismatch);
                }
            }
        }
    }
    Ok(())
}

const fn journal_table(table: CompositeTableV1) -> crate::journal::JournalTable {
    match table {
        CompositeTableV1::Meta => crate::journal::JournalTable::Meta,
        CompositeTableV1::Entities => crate::journal::JournalTable::Entities,
        CompositeTableV1::SecondaryIndexes => crate::journal::JournalTable::SecondaryIndexes,
        CompositeTableV1::IndexEpochs => crate::journal::JournalTable::IndexEpochs,
        CompositeTableV1::Idempotency => crate::journal::JournalTable::Idempotency,
        CompositeTableV1::IdempotencyLocators => crate::journal::JournalTable::IdempotencyLocators,
        CompositeTableV1::ProvenanceLocators => crate::journal::JournalTable::ProvenanceLocators,
        CompositeTableV1::AuditByRequestLocators => {
            crate::journal::JournalTable::AuditByRequestLocators
        }
        CompositeTableV1::IdempotencyPending => crate::journal::JournalTable::IdempotencyPending,
        CompositeTableV1::Events => crate::journal::JournalTable::Events,
        CompositeTableV1::EventRoutes => crate::journal::JournalTable::EventRoutes,
        CompositeTableV1::Outbox => crate::journal::JournalTable::Outbox,
        CompositeTableV1::Provenance => crate::journal::JournalTable::Provenance,
        CompositeTableV1::Commits => crate::journal::JournalTable::Commits,
        CompositeTableV1::Audit => crate::journal::JournalTable::Audit,
        CompositeTableV1::AuditByRequest => crate::journal::JournalTable::AuditByRequest,
        CompositeTableV1::EntityChainHeads => crate::journal::JournalTable::EntityChainHeads,
        CompositeTableV1::VectorEvidence => crate::journal::JournalTable::VectorEvidence,
        CompositeTableV1::VectorObservations => crate::journal::JournalTable::VectorObservations,
        CompositeTableV1::VectorEvidenceIndex => crate::journal::JournalTable::VectorEvidenceIndex,
    }
}

fn corrupt_value(_: StorageValueError) -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn invalid_shape<E>(_: E) -> StorageValueError {
    StorageValueError::InvalidShape
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use redb::{Durability, ReadableDatabase};
    use riffdb_storage_api::{
        AdministrationSequenceAllocator, CompositeFrameKindV1, CompositeMutationV1,
        DatabaseInitializationPort, StoredServiceAuditRequestIndexV1,
    };
    use riffdb_types::{AdministrationSequence, DatabaseId, RequestId};

    use super::*;
    use crate::codec::{
        encode_administration_sequence_allocator_v1, encode_service_audit_request_index_v1,
    };
    use crate::keys::encode_audit_by_request_key;
    use crate::layout::{AUDIT_BY_REQUEST, META, META_ADMINISTRATION_SEQUENCE};
    use crate::store::{RedbOperationalPorts, RedbStore};

    /// Whole-directory scope: the database and every side file it grows
    /// (journal, checkpoint, spare, durable-format marker, …) live in one
    /// [`crate::test_path::ScopedDirectory`] removed on drop — pass, fail, or
    /// panic — so cleanup never depends on a hand-maintained file list. This
    /// retires the stale `target/wp487-composite-view` marker-leak class.
    struct TestDatabasePath(
        PathBuf,
        // Held only so `Drop` removes the whole scope.
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestDatabasePath {
        fn new(label: &str) -> Self {
            let scope = crate::test_path::ScopedDirectory::new(label);
            Self(scope.join("db.redb"), scope)
        }
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [8; 10]).expect("database")
    }

    fn operational(label: &str) -> (TestDatabasePath, RedbStore, RedbOperationalPorts) {
        let path = TestDatabasePath::new(label);
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(database_id())
            .expect("initialize");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        (path, store, ports)
    }

    fn allocator_bytes(value: AdministrationSequenceAllocator) -> Vec<u8> {
        encode_administration_sequence_allocator_v1(value)
            .expect("encode allocator")
            .into_bytes()
    }

    fn request_id(millis: u64) -> RequestId {
        RequestId::from_unix_milliseconds_and_random(millis, [5; 10]).expect("request")
    }

    /// Point reads and range reads must land on the same physical redb table.
    ///
    /// Both now index one per-snapshot handle cache through
    /// `journal_table(..)`, where the range path previously carried its own
    /// `CompositeTableV1 -> TableDefinition` map. Two maps that disagreed for
    /// one variant would have made a range read scan a different table than
    /// the point read for the same logical table — and report absence for rows
    /// that are durably present. The physical names are pinned as literals so
    /// this fails on divergence rather than restating whatever the code says.
    #[test]
    fn every_composite_table_resolves_to_its_pinned_physical_table() {
        use redb::TableHandle;

        for (table, expected) in [
            (CompositeTableV1::Meta, None),
            (CompositeTableV1::Entities, Some("entities")),
            (CompositeTableV1::SecondaryIndexes, Some("secondary_indexes")),
            (CompositeTableV1::IndexEpochs, Some("index_epochs")),
            (CompositeTableV1::Idempotency, Some("idempotency")),
            (
                CompositeTableV1::IdempotencyLocators,
                Some("idempotency_locators"),
            ),
            (
                CompositeTableV1::ProvenanceLocators,
                Some("provenance_locators"),
            ),
            (
                CompositeTableV1::AuditByRequestLocators,
                Some("audit_by_request_locators"),
            ),
            (
                CompositeTableV1::IdempotencyPending,
                Some("idempotency_pending"),
            ),
            (CompositeTableV1::Events, Some("events")),
            (CompositeTableV1::EventRoutes, Some("event_routes")),
            (CompositeTableV1::Outbox, Some("outbox")),
            (CompositeTableV1::Provenance, Some("provenance")),
            (CompositeTableV1::Commits, Some("commits")),
            (CompositeTableV1::Audit, Some("audit")),
            (CompositeTableV1::AuditByRequest, Some("audit_by_request")),
            (
                CompositeTableV1::EntityChainHeads,
                Some("entity_chain_heads"),
            ),
            (CompositeTableV1::VectorEvidence, Some("vector_evidence")),
            (
                CompositeTableV1::VectorObservations,
                Some("vector_observations"),
            ),
            (
                CompositeTableV1::VectorEvidenceIndex,
                Some("vector_evidence_index"),
            ),
        ] {
            let resolved = crate::journal::byte_table_definition(journal_table(table))
                .map(|definition| definition.name().to_owned());
            assert_eq!(
                resolved.as_deref(),
                expected,
                "{table:?} resolves to the wrong physical table"
            );
        }

        // Every variant is pinned above, so no table can silently acquire a
        // mapping without also acquiring a pin.
        assert_eq!(CompositeTableV1::ALL.len(), 20);
    }

    /// The cached frontier belongs to one published view, so a successor that
    /// advances the frontier reports its own value rather than inheriting the
    /// predecessor's. A view that inherited one would report a durably
    /// committed frontier as absent.
    #[test]
    fn a_composite_successor_derives_its_own_frontier() {
        use riffdb_storage_api::ApplicationSequenceAllocator;
        use riffdb_types::CommitSequence;

        use crate::codec::encode_application_sequence_allocator_v1;
        use crate::layout::META_APPLICATION_SEQUENCE;
        use crate::store::RedbReadAccess;

        let (_path, _store, ports) = operational("composite-frontier");
        let initial =
            encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::initial())
                .expect("initial allocator")
                .as_bytes()
                .to_vec();
        let advanced = encode_application_sequence_allocator_v1(
            ApplicationSequenceAllocator::Next(
                CommitSequence::first().checked_next().expect("second"),
            ),
        )
        .expect("advanced allocator")
        .as_bytes()
        .to_vec();

        let predecessor = Arc::new(
            RedbCompositeViewBuilder::capture(&ports, [0; 32])
                .expect("capture")
                .freeze(),
        );
        let access = RedbReadAccess::Composite(Arc::clone(&predecessor));
        assert_eq!(
            crate::reads::read_snapshot_head(&access).expect("predecessor frontier"),
            None
        );
        // Second read takes the cached path and must agree with the first.
        assert_eq!(
            crate::reads::read_snapshot_head(&access).expect("cached frontier"),
            None
        );

        let mut builder = RedbCompositeViewBuilder::capture(&ports, [0; 32]).expect("capture");
        let frame = CompositeFrameV1::new(
            CompositeFrameKindV1::Command,
            database_id(),
            None,
            Some(CommitSequence::first()),
            None,
            None,
            1,
            256,
            [0; 32],
            [1; 32],
            vec![
                CompositeMutationV1::replace(
                    CompositeTableV1::Meta,
                    META_APPLICATION_SEQUENCE.as_bytes(),
                    &initial,
                    advanced.as_slice(),
                )
                .expect("mutation"),
            ],
        )
        .expect("frame");
        builder.apply_composite_frame(&frame).expect("apply");
        let successor = Arc::new(builder.freeze());

        assert_eq!(
            crate::reads::read_snapshot_head(&RedbReadAccess::Composite(successor))
                .expect("successor frontier"),
            Some(CommitSequence::first()),
            "a successor must derive the frontier it published, not its predecessor's"
        );
    }

    #[test]
    fn redb_root_plus_overlay_resolves_without_mutating_checkpoint() {
        let (_path, store, ports) = operational("point");
        let initial = allocator_bytes(AdministrationSequenceAllocator::initial());
        let next = allocator_bytes(AdministrationSequenceAllocator::next(
            AdministrationSequence::new(2).expect("sequence"),
        ));
        let mut builder = RedbCompositeViewBuilder::capture(&ports, [0; 32]).expect("capture");
        let frame = CompositeFrameV1::new(
            CompositeFrameKindV1::ServiceAudit,
            database_id(),
            None,
            None,
            None,
            Some(AdministrationSequence::first()),
            1,
            256,
            [0; 32],
            [1; 32],
            vec![
                CompositeMutationV1::replace(
                    CompositeTableV1::Meta,
                    META_ADMINISTRATION_SEQUENCE.as_bytes(),
                    &initial,
                    next.as_slice(),
                )
                .expect("mutation"),
            ],
        )
        .expect("frame");
        builder.apply_composite_frame(&frame).expect("apply");
        let view = builder.freeze();

        assert_eq!(
            view.resolve_point(
                CompositeTableV1::Meta,
                META_ADMINISTRATION_SEQUENCE.as_bytes()
            )
            .expect("resolve"),
            Some(next)
        );
        let read = store.shared.database.begin_read().expect("read root");
        let meta = read.open_table(META).expect("meta");
        assert_eq!(
            meta.get(META_ADMINISTRATION_SEQUENCE)
                .expect("get")
                .expect("allocator")
                .value(),
            initial
        );
    }

    #[test]
    fn captured_root_does_not_drift_when_redb_advances() {
        let (_path, store, ports) = operational("snapshot");
        let initial = allocator_bytes(AdministrationSequenceAllocator::initial());
        let view = RedbCompositeViewBuilder::capture(&ports, [0; 32])
            .expect("capture")
            .freeze();

        let mut write = store.shared.database.begin_write().expect("write");
        write.set_two_phase_commit(false);
        write
            .set_durability(Durability::Immediate)
            .expect("durability");
        let changed = allocator_bytes(AdministrationSequenceAllocator::next(
            AdministrationSequence::new(2).expect("sequence"),
        ));
        {
            let mut meta = write.open_table(META).expect("meta");
            meta.insert(META_ADMINISTRATION_SEQUENCE, changed.as_slice())
                .expect("insert");
        }
        write.commit().expect("commit");

        assert_eq!(
            view.resolve_point(
                CompositeTableV1::Meta,
                META_ADMINISTRATION_SEQUENCE.as_bytes()
            )
            .expect("resolve"),
            Some(initial)
        );
    }

    #[test]
    fn canonical_validation_fails_before_overlay_publication() {
        let (_path, _store, ports) = operational("canonical");
        let initial = allocator_bytes(AdministrationSequenceAllocator::initial());
        let mut builder = RedbCompositeViewBuilder::capture(&ports, [0; 32]).expect("capture");
        let frame = CompositeFrameV1::new(
            CompositeFrameKindV1::ServiceAudit,
            database_id(),
            None,
            None,
            None,
            Some(AdministrationSequence::first()),
            1,
            128,
            [0; 32],
            [1; 32],
            vec![
                CompositeMutationV1::replace(
                    CompositeTableV1::Meta,
                    META_ADMINISTRATION_SEQUENCE.as_bytes(),
                    &initial,
                    b"not-a-canonical-envelope".as_slice(),
                )
                .expect("mutation"),
            ],
        )
        .expect("frame");
        assert_eq!(
            builder
                .apply_composite_frame(&frame)
                .expect_err("corruption")
                .kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(builder.overlay.freeze().transition_count(), 0);
    }

    #[test]
    fn bounded_redb_range_merges_overlay_rows_in_physical_order() {
        let (_path, store, ports) = operational("range");
        let first = request_id(1);
        let middle = request_id(2);
        let last = request_id(3);
        let first_sequence = AdministrationSequence::new(2).expect("sequence");
        let middle_sequence = AdministrationSequence::first();
        let last_sequence = AdministrationSequence::new(3).expect("sequence");
        let mut write = store.shared.database.begin_write().expect("write");
        write.set_two_phase_commit(false);
        write
            .set_durability(Durability::Immediate)
            .expect("durability");
        {
            let mut rows = write.open_table(AUDIT_BY_REQUEST).expect("table");
            for (request, sequence) in [(first, first_sequence), (last, last_sequence)] {
                let key = encode_audit_by_request_key(request, sequence);
                let value = encode_service_audit_request_index_v1(
                    StoredServiceAuditRequestIndexV1::new(request, sequence),
                )
                .expect("encode");
                rows.insert(key.as_slice(), value.as_bytes())
                    .expect("insert");
            }
        }
        write.commit().expect("commit");

        let mut builder = RedbCompositeViewBuilder::capture(&ports, [0; 32]).expect("capture");
        let middle_key = encode_audit_by_request_key(middle, middle_sequence);
        let middle_value = encode_service_audit_request_index_v1(
            StoredServiceAuditRequestIndexV1::new(middle, middle_sequence),
        )
        .expect("encode");
        let frame = CompositeFrameV1::new(
            CompositeFrameKindV1::ServiceAudit,
            database_id(),
            None,
            None,
            None,
            Some(AdministrationSequence::first()),
            1,
            256,
            [0; 32],
            [1; 32],
            vec![
                CompositeMutationV1::put(
                    CompositeTableV1::AuditByRequest,
                    middle_key.as_slice(),
                    middle_value.as_bytes(),
                )
                .expect("mutation"),
            ],
        )
        .expect("frame");
        builder.apply_composite_frame(&frame).expect("apply");
        let view = builder.freeze();
        let page = view
            .merge_bounded(CompositeTableV1::AuditByRequest, &[0], Some(&[0xff]), 3, 3)
            .expect("merge");
        let keys: Vec<&[u8]> = page.rows().iter().map(|(key, _)| key.as_ref()).collect();
        let first_key = encode_audit_by_request_key(first, first_sequence);
        let last_key = encode_audit_by_request_key(last, last_sequence);
        assert_eq!(
            keys,
            vec![
                first_key.as_slice(),
                middle_key.as_slice(),
                last_key.as_slice(),
            ]
        );
    }

    #[test]
    fn publication_withholds_private_successor_and_rejects_stale_fence() {
        let (_path, _store, ports) = operational("publication");
        let initial = allocator_bytes(AdministrationSequenceAllocator::initial());
        let next = allocator_bytes(AdministrationSequenceAllocator::next(
            AdministrationSequence::new(2).expect("sequence"),
        ));
        let publication = RedbCompositePublication::new(
            RedbCompositeViewBuilder::capture(&ports, [0; 32])
                .expect("capture")
                .freeze(),
        );
        let captured = publication.capture().expect("published predecessor");
        let mut private = RedbCompositeViewBuilder::from_published(&captured);
        let frame = CompositeFrameV1::new(
            CompositeFrameKindV1::ServiceAudit,
            database_id(),
            None,
            None,
            None,
            Some(AdministrationSequence::first()),
            1,
            256,
            [0; 32],
            [1; 32],
            vec![
                CompositeMutationV1::replace(
                    CompositeTableV1::Meta,
                    META_ADMINISTRATION_SEQUENCE.as_bytes(),
                    &initial,
                    next.as_slice(),
                )
                .expect("mutation"),
            ],
        )
        .expect("frame");
        private.apply_composite_frame(&frame).expect("apply");

        assert_eq!(
            publication
                .capture()
                .expect("still predecessor")
                .resolve_point(
                    CompositeTableV1::Meta,
                    META_ADMINISTRATION_SEQUENCE.as_bytes(),
                )
                .expect("resolve"),
            Some(initial)
        );
        let published = publication
            .publish_successor(&captured, Arc::new(private.freeze()))
            .expect("publish after fence");
        assert_eq!(
            published
                .resolve_point(
                    CompositeTableV1::Meta,
                    META_ADMINISTRATION_SEQUENCE.as_bytes(),
                )
                .expect("resolve"),
            Some(next)
        );

        let stale_successor =
            Arc::new(RedbCompositeViewBuilder::from_published(&captured).freeze());
        let stale = publication.publish_successor(&captured, stale_successor);
        assert!(matches!(
            stale,
            Err(error) if error.kind() == StorageErrorKind::InvariantViolation
        ));
    }
}
