//! Separate follower construction and the sole frame-transaction owner.
//! No application, administration, journal, or maintenance writer is released.

#[path = "store_follower_projection.rs"]
mod projection;
#[path = "store_follower_recovery.rs"]
mod recovery;
pub use recovery::{RedbFollowerProjectionRecovery, RedbFollowerRecoveryCatalogSession};

use super::*;
use crate::changelog_v3_activation::{HISTORY, SOURCE_HOLDS};
use crate::changelog_v3_write::{apply_mutation, check_predecessor, table_inventory, value_error};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, ChangelogFrameV3, ChangelogHistoryPointV3,
    ChangelogHistoryStateV3, ReplicationFollowerStateV3, StartupValidationInputs,
    StructuralEvidenceOpen,
    proto_codec::{
        decode_replication_follower_state_v3, encode_changelog_history_state_v3,
        encode_changelog_transaction_allocator_v3, encode_replication_follower_state_v3,
    },
};

/// Dormant follower open: only the unchanged structural validation session can
/// be started. There is no conversion into a source store or write-port bundle.
pub struct RedbFollowerStore(pub(super) RedbStore);

impl RedbFollowerStore {
    /// Uses the unchanged follower evidence driver with cooperative cancellation
    /// between evidence reads. Cancellation cannot produce a startup proof.
    pub fn begin_structural_evidence_cancellable(
        self,
        inputs: StartupValidationInputs,
        cancellation: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<crate::RedbStructuralEvidenceSession, StorageError> {
        if cancellation.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let mut session = self.0.begin_structural_evidence(inputs)?;
        session.set_cancellation(cancellation);
        Ok(session)
    }

    pub(crate) fn begin_offline_bootstrap_scrub(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<crate::RedbStructuralEvidenceSession, StorageError> {
        crate::startup::begin_follower_bootstrap_scrub(self.0, inputs)
    }

    /// Opens an already attached current-format follower. Creation and offline
    /// staged bootstrap have separate owners; this never initializes or repairs
    /// missing replication metadata, replays a source journal, or promotes.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        if preflight_durable_format_path(path).map_err(format_preflight_storage_error)?
            != RedbDurableFormatPreflight::OpenCurrent
        {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        let namespace = crate::maintenance::FollowerNamespace::open(path)?;
        Self::open_with_namespace(path, namespace)
    }

    #[cfg(test)]
    pub(crate) fn open_with_failed_namespace_sync(path: &Path) -> Result<Self, StorageError> {
        let mut namespace = crate::maintenance::FollowerNamespace::open(path)?;
        namespace.fail_directory_sync();
        Self::open_with_namespace(path, namespace)
    }

    fn open_with_namespace(
        path: &Path,
        mut namespace: crate::maintenance::FollowerNamespace,
    ) -> Result<Self, StorageError> {
        let backend = namespace.backend()?;
        let store = RedbStore::open_after_format_preflight(
            OpenMode::Follower,
            path,
            RedbCommitProfile::Hardened,
            None,
            default_changelog_port(),
            None,
            Some(Box::new(backend)),
            Arc::new(RealJournalMedia),
        )?;
        // The actual engine lock remains held during both syncs. A publisher's
        // lost/failed final sync can never be mistaken for a durable pathname.
        namespace.synchronize()?;
        *store
            .shared
            .follower_namespace
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))? = Some(namespace);
        Ok(Self(store))
    }
}

impl StructuralEvidenceOpen for RedbFollowerStore {
    type Session = crate::RedbStructuralEvidenceSession;

    fn begin_structural_evidence(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<Self::Session, StorageError> {
        self.0.begin_structural_evidence(inputs)
    }
}

pub(crate) fn validate_open(
    database: &impl ReadableDatabase,
    path: &Path,
    media: &dyn JournalMedia,
) -> Result<(), StorageError> {
    let read = database.begin_read().map_err(transaction_error)?;
    // The physical schema is shared; source-only populations are not.
    if !read
        .open_table(HISTORY)
        .map_err(table_error)?
        .is_empty()
        .map_err(precommit_storage_error)?
        || !read
            .open_table(SOURCE_HOLDS)
            .map_err(table_error)?
            .is_empty()
            .map_err(precommit_storage_error)?
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let history = crate::changelog_v3_roots::read_checkpoint_roots(&read)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if !crate::follower_lifecycle::preserve_attached_lifecycle(
        &read,
        history.lineage().database_id(),
        history.lineage().history_incarnation(),
    )? {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    // Followers commit frames directly. A source suffix cannot be replayed or
    // discarded as part of this open: bootstrap must publish a complete engine.
    for journal in [
        crate::journal::journal_path(path),
        crate::journal::checkpoint_journal_path(path),
        crate::journal::spare_journal_path(path),
    ] {
        if media
            .try_exists(&journal)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    Ok(())
}

impl RedbDormantPorts {
    /// Server composition must first match its separate catalog-owned proof.
    /// Consumes the dormant bundle and releases exactly one non-cloneable
    /// follower applier, never source command or maintenance ports.
    pub fn into_follower_after_catalog_validation(
        self,
    ) -> Result<RedbFollowerApplier, StorageError> {
        self.into_follower_after_catalog_validation_cancellable(
            &std::sync::atomic::AtomicBool::new(false),
        )
    }

    /// Rebuilds the same transient indexes with cancellation between records.
    /// The separate catalog proof remains a prerequisite owned by composition.
    pub fn into_follower_after_catalog_validation_cancellable(
        self,
        cancellation: &std::sync::atomic::AtomicBool,
    ) -> Result<RedbFollowerApplier, StorageError> {
        if cancellation.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        if self.shared.open_mode != OpenMode::Follower
            || self.pending_v3_activation.is_some()
            || !self.shared.startup_validation_clean()
            || self.shared.bounded_clean_startup.load(Ordering::Acquire)
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let read = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let (indexes, rows) = TransientIndexes::rebuild_counted_cancellable(&read, cancellation)?;
        drop(read);
        let mut state = self
            .shared
            .transient_indexes
            .write()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if !matches!(*state, TransientIndexState::Dormant) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if cancellation.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        *state = TransientIndexState::Ready(indexes);
        drop(state);
        self.shared.note_transient_index_rebuild(rows);
        RedbFollowerApplier::from_validated_shared(self.shared)
    }
}

/// Sole, move-only follower writer. One complete frame is one hardened Immediate
/// transaction; cancellation or an error cannot publish a partial prefix.
/// The in-memory frame chain is connection-local; durable receipt hashes bind
/// the shared history across reconnects. Any apply failure fences this handle.
pub struct RedbFollowerApplier {
    pub(super) shared: Arc<SharedRedb>,
    prior_frame_hash: [u8; 32],
    last_frame_hash: Option<[u8; 32]>,
    failed: bool,
}

impl RedbFollowerApplier {
    #[cfg(test)]
    pub(crate) fn from_isolated_test_store(store: RedbFollowerStore) -> Result<Self, StorageError> {
        Self::from_validated_shared(store.0.shared)
    }

    #[cfg(test)]
    pub(crate) fn local_commit_epoch_for_test(&self) -> u64 {
        self.shared.durable_commit_epoch()
    }

    /// Reclaims only the configured receiver's completed bootstrap scratch.
    /// The receiver calls this after authenticated attachment succeeds. Live
    /// database and marker descriptors remain pinned under the engine owner;
    /// cleanup unlinks scratch names without opening or mutating that engine.
    pub fn retire_bootstrap_scratch(
        &mut self,
        repository: &crate::RedbBootstrapReceiverRepository,
    ) -> Result<(), StorageError> {
        let history = self.durable_history()?;
        let mut namespace = self
            .shared
            .follower_namespace
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        repository.retire_attached(
            history,
            namespace
                .as_mut()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        )
    }

    pub(super) fn from_validated_shared(shared: Arc<SharedRedb>) -> Result<Self, StorageError> {
        if shared.open_mode != OpenMode::Follower {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let read = shared.database.begin_read().map_err(transaction_error)?;
        let history = crate::changelog_v3_roots::read_checkpoint_roots(&read)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        crate::follower_lifecycle::preserve_attached_lifecycle(
            &read,
            history.lineage().database_id(),
            history.lineage().history_incarnation(),
        )?;
        drop(read);
        Ok(Self {
            shared,
            prior_frame_hash: history.tail().history_hash(),
            last_frame_hash: None,
            failed: false,
        })
    }

    /// Locally durable applied position, without claiming an upstream
    /// acknowledgement or serving readiness. Fenced handles release no progress.
    pub fn durable_position(&self) -> Result<ChangelogHistoryPointV3, StorageError> {
        Ok(self.durable_history()?.tail())
    }

    /// Exact durable lineage and applied history roots for startup/handshake
    /// binding. This does not grant source allocation or retention authority.
    pub fn durable_history(&self) -> Result<ChangelogHistoryStateV3, StorageError> {
        self.ensure_live()?;
        let read = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let history = crate::changelog_v3_roots::read_checkpoint_roots(&read)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        crate::follower_lifecycle::preserve_attached_lifecycle(
            &read,
            history.lineage().database_id(),
            history.lineage().history_incarnation(),
        )?;
        Ok(history)
    }

    /// Starts a new frame chain at the exact durable receipt position used in
    /// the next authenticated source handshake. No local receipt is allocated.
    pub fn resume_stream(&mut self) -> Result<ChangelogHistoryPointV3, StorageError> {
        let position = self.durable_position()?;
        self.prior_frame_hash = position.history_hash();
        self.last_frame_hash = None;
        Ok(position)
    }

    /// Checks bounded bytes, complete receipt chaining, exact pre-images and
    /// physical frontiers before returning only the committed applied position.
    /// Exact retries bound by the durable terminal receipt hash perform no write.
    pub fn apply_frame(&mut self, bytes: &[u8]) -> Result<ChangelogHistoryPointV3, StorageError> {
        self.ensure_live()?;
        self.failed = true;
        let result = self.apply_inner(bytes);
        if result.is_ok() {
            self.failed = false;
        }
        result
    }

    fn apply_inner(&mut self, bytes: &[u8]) -> Result<ChangelogHistoryPointV3, StorageError> {
        let corrupt = || storage_error(StorageErrorKind::CorruptData);
        let frame = ChangelogFrameV3::decode(bytes).map_err(value_error)?;
        let checksum = *bytes.last_chunk::<32>().ok_or_else(corrupt)?;
        let _lease = self.shared.mutation_gate.acquire()?;
        let mut write = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        write.set_two_phase_commit(true);
        write
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let history = crate::changelog_v3_roots::read_checkpoint_roots_for_write(&write)?
            .ok_or_else(corrupt)?;
        let meta = write.open_table(META).map_err(table_error)?;
        let retained = meta
            .get(key(N::ReplicationFollowerState)?)
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let state = *decode_replication_follower_state_v3(retained.value())
            .map_err(crate::error::codec_error)?
            .value();
        let (lineage, applied, acknowledged) = state.attached_state().ok_or_else(corrupt)?;
        if history.lineage() != lineage
            || history.tail() != applied
            || meta
                .get(META_CLEAN_CLOSE_LIFECYCLE)
                .map_err(precommit_storage_error)?
                .is_some()
            || frame.binding().database_id() != lineage.database_id()
            || frame.binding().history_incarnation() != lineage.history_incarnation()
            || frame.binding().leadership_epoch() != lineage.leadership_epoch().get()
            || frame.binding().catalog_digest() != lineage.catalog_digest()
        {
            return Err(corrupt());
        }
        drop(retained);
        drop(meta);
        let first = frame.receipts().first().ok_or_else(corrupt)?;
        let last = frame.receipts().last().ok_or_else(corrupt)?;
        let covered = ChangelogHistoryPointV3::from_receipt(last).map_err(value_error)?;
        if covered == applied {
            // On reconnect, only a frame seeded at its exact prior receipt can
            // be retried without a process-local previous-frame witness.
            if self.last_frame_hash.map_or(
                frame.binding().prior_frame_hash() != first.binding().prior_history_hash,
                |previous| previous != checksum,
            ) {
                return Err(corrupt());
            }
            // ChangelogHistoryPointV3::from_receipt hashes the complete last
            // canonical receipt, including its predecessor's receipt hash.
            // Frame decoding already checked every prior receipt in this frame.
            // Equality with the durable applied point therefore binds every
            // repeated receipt byte, position and frontier without copying the
            // source-only history population into a follower database.
            write.abort().map_err(precommit_storage_error)?;
        } else {
            if frame.binding().prior_frame_hash() != self.prior_frame_hash {
                return Err(corrupt());
            }
            let tables = table_inventory(&write)?;
            let mut indexes = self.take_indexes_for_frame(&frame)?;
            let mut successor = history;
            for receipt in frame.receipts() {
                successor = successor.advance(receipt).map_err(value_error)?;
                for mutation in receipt.mutations() {
                    if !tables.contains(mutation.namespace().table()) {
                        return Err(corrupt());
                    }
                    check_predecessor(&write, mutation)?;
                }
                for mutation in receipt.mutations() {
                    apply_mutation(&write, mutation)?;
                }
                if let Some(indexes) = &mut indexes {
                    indexes.apply_follower_receipt(&write, receipt.mutations())?;
                }
                // A later receipt cannot hide a false intermediate frontier.
                let meta = write.open_table(META).map_err(table_error)?;
                let app = meta
                    .get(META_APPLICATION_SEQUENCE)
                    .map_err(precommit_storage_error)?
                    .ok_or_else(corrupt)?;
                let admin = meta
                    .get(META_ADMINISTRATION_SEQUENCE)
                    .map_err(precommit_storage_error)?
                    .ok_or_else(corrupt)?;
                if crate::changelog_v3_roots::decode_physical_frontier(app.value(), admin.value())?
                    != successor.tail().frontier()
                {
                    return Err(corrupt());
                }
                drop(app);
                drop(admin);
                drop(meta);
                crash_edge("receipt");
            }
            stage_roots(
                &write,
                successor,
                ReplicationFollowerStateV3::attached(lineage, covered, acknowledged)
                    .map_err(value_error)?,
            )?;
            if crate::changelog_v3_roots::read_checkpoint_roots_for_write(&write)?
                != Some(successor)
            {
                return Err(corrupt());
            }
            crash_edge("roots");
            self.shared.commit_durable(write)?;
            crash_edge("committed");
            if let Some(indexes) = indexes {
                let mut state = self
                    .shared
                    .transient_indexes
                    .write()
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
                if !matches!(*state, TransientIndexState::Invalid) {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                *state = TransientIndexState::Ready(indexes);
                drop(state);
                crash_edge("indexes-published");
            }
        }
        self.prior_frame_hash = checksum;
        self.last_frame_hash = Some(checksum);
        Ok(covered)
    }

    fn take_indexes_for_frame(
        &self,
        frame: &ChangelogFrameV3,
    ) -> Result<Option<TransientIndexes>, StorageError> {
        if !frame
            .receipts()
            .iter()
            .flat_map(|receipt| receipt.mutations())
            .any(|mutation| crate::transient::follower::affects_indexes(mutation.namespace()))
        {
            return Ok(None);
        }
        let mut state = self
            .shared
            .transient_indexes
            .write()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        // Private opaque-row transaction tests intentionally do not run startup.
        // No production applier is released before its full cache rebuild.
        #[cfg(test)]
        if matches!(*state, TransientIndexState::Dormant) && !self.shared.startup_validation_clean()
        {
            return Ok(None);
        }
        match std::mem::replace(&mut *state, TransientIndexState::Invalid) {
            TransientIndexState::Ready(indexes) => Ok(Some(indexes)),
            TransientIndexState::Dormant | TransientIndexState::Invalid => {
                Err(storage_error(StorageErrorKind::Unavailable))
            }
        }
    }

    /// Durably records the exact applied position before it may be offered as
    /// an acknowledgement. This is follower-local metadata only; it neither
    /// allocates a source receipt nor claims the source received the message.
    /// Repeating an already recorded acknowledgement is read-only.
    pub fn acknowledge_durable_position(
        &mut self,
    ) -> Result<ChangelogHistoryPointV3, StorageError> {
        self.ensure_live()?;
        self.failed = true;
        let result = self.acknowledge_inner();
        if result.is_ok() {
            self.failed = false;
        }
        result
    }

    fn acknowledge_inner(&self) -> Result<ChangelogHistoryPointV3, StorageError> {
        let corrupt = || storage_error(StorageErrorKind::CorruptData);
        let _lease = self.shared.mutation_gate.acquire()?;
        let mut write = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        write.set_two_phase_commit(true);
        write
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let history = crate::changelog_v3_roots::read_checkpoint_roots_for_write(&write)?
            .ok_or_else(corrupt)?;
        let mut meta = write.open_table(META).map_err(table_error)?;
        let row = meta
            .get(key(N::ReplicationFollowerState)?)
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let state = *decode_replication_follower_state_v3(row.value())
            .map_err(crate::error::codec_error)?
            .value();
        let (lineage, applied, acknowledged) = state.attached_state().ok_or_else(corrupt)?;
        if lineage != history.lineage()
            || applied != history.tail()
            || meta
                .get(META_CLEAN_CLOSE_LIFECYCLE)
                .map_err(precommit_storage_error)?
                .is_some()
        {
            return Err(corrupt());
        }
        drop(row);
        if acknowledged == Some(applied) {
            drop(meta);
            write.abort().map_err(precommit_storage_error)?;
            return Ok(applied);
        }
        let next = ReplicationFollowerStateV3::attached(lineage, applied, Some(applied))
            .map_err(value_error)?;
        meta.insert(
            key(N::ReplicationFollowerState)?,
            encode_replication_follower_state_v3(next)
                .map_err(crate::error::codec_error)?
                .as_bytes(),
        )
        .map_err(precommit_storage_error)?;
        drop(meta);
        crash_edge("acknowledgement");
        self.shared.commit_durable(write)?;
        crash_edge("acknowledgement-committed");
        Ok(applied)
    }

    fn ensure_live(&self) -> Result<(), StorageError> {
        if self.failed || self.shared.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(())
    }

    /// Consumes the applier after its synchronous durability boundary has
    /// drained. No source journal, checkpoint, CLEAN row or sequence is written.
    pub fn close(self) -> Result<(), StorageError> {
        self.ensure_live()
    }
}

impl riffdb_storage_api::ChangelogFollowerApplyPortV3 for RedbFollowerApplier {
    fn apply_frame(&mut self, bytes: &[u8]) -> Result<ChangelogHistoryPointV3, StorageError> {
        RedbFollowerApplier::apply_frame(self, bytes)
    }
    fn resume_stream(&mut self) -> Result<ChangelogHistoryPointV3, StorageError> {
        RedbFollowerApplier::resume_stream(self)
    }
    fn acknowledge_durable_position(&mut self) -> Result<ChangelogHistoryPointV3, StorageError> {
        RedbFollowerApplier::acknowledge_durable_position(self)
    }
    fn close(self: Box<Self>) -> Result<(), StorageError> {
        RedbFollowerApplier::close(*self)
    }
}

fn key(namespace: N) -> Result<&'static str, StorageError> {
    namespace
        .metadata_key()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
}

fn stage_roots(
    write: &WriteTransaction,
    history: ChangelogHistoryStateV3,
    follower: ReplicationFollowerStateV3,
) -> Result<(), StorageError> {
    let mut meta = write.open_table(META).map_err(table_error)?;
    for (namespace, value) in [
        (
            N::ChangelogHistoryState,
            encode_changelog_history_state_v3(history),
        ),
        (
            N::NextChangelogTransaction,
            encode_changelog_transaction_allocator_v3(history.expected_allocator()),
        ),
        (
            N::ReplicationFollowerState,
            encode_replication_follower_state_v3(follower),
        ),
    ] {
        meta.insert(
            key(namespace)?,
            value.map_err(crate::error::codec_error)?.as_bytes(),
        )
        .map_err(precommit_storage_error)?;
    }
    Ok(())
}

fn crash_edge(_edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_FOLLOWER_APPLY_EDGE").as_deref() == Ok(_edge) {
        std::process::exit(93);
    }
}

impl std::fmt::Debug for RedbFollowerStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedbFollowerStore([DORMANT])")
    }
}
impl std::fmt::Debug for RedbFollowerApplier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedbFollowerApplier([redacted])")
    }
}
