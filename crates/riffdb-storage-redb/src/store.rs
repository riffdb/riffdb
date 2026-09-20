//! Dormant redb handle, identity probe, and atomic initialization.
#[path = "store/transient_access.rs"]
mod transient_access;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound::{Excluded, Unbounded};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

#[path = "store_export_ledger.rs"]
mod export_ledger;

#[path = "store_follower.rs"]
mod follower;
pub(crate) use follower::validate_open as validate_follower_open;
pub use follower::{
    RedbFollowerApplier, RedbFollowerProjectionRecovery, RedbFollowerRecoveryCatalogSession,
    RedbFollowerStore,
};

#[derive(Clone, Eq, PartialEq)]
enum OpenMode {
    Source,
    Follower,
    PrivateRestore(Arc<crate::maintenance::PrivateArchiveValidationBinding>),
}

#[path = "store_private_restore.rs"]
mod private_restore;

#[path = "store_graceful_close.rs"]
mod graceful_close;

#[path = "store_changelog_lifecycle.rs"]
mod changelog_lifecycle;

#[path = "store_changelog_source_control.rs"]
mod changelog_source_control;

#[path = "store_v3_layout.rs"]
pub(crate) mod v3_layout;

#[cfg(test)]
#[path = "store_changelog_lifecycle_tests.rs"]
mod changelog_lifecycle_tests;

#[cfg(test)]
#[path = "store_v3_layout_tests.rs"]
mod v3_layout_tests;

#[path = "store_journal_checkpoint.rs"]
mod journal_checkpoint;

#[path = "store_journal_runtime.rs"]
mod journal_runtime;

#[path = "store_journal_source.rs"]
mod journal_source;

use redb::{
    Builder, Database, Durability, MultimapTableHandle, ReadTransaction, ReadableDatabase,
    ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle, WriteTransaction,
};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, CommandSegmentDigestV1, CommittedEntityTransitionV1,
    DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
    DatabaseInitializationResult, DeferredCommandFence, EntityChainHeadV1, EntityChainStateV1,
    HISTORY_INCARNATION_INITIAL, MAX_VECTOR_MODELS_PER_OBSERVATION, StorageError, StorageErrorKind,
    StorageFormatVersion, StoredIndexEpochV1,
    proto_codec::{
        decode_administration_sequence_allocator_v1, decode_application_sequence_allocator_v1,
        decode_database_identity_v1, decode_history_incarnation_v1, decode_record_registry_v2,
        decode_storage_format_version_v1, encode_administration_sequence_allocator_v1,
        encode_application_sequence_allocator_v1, encode_database_identity_v1,
        encode_history_incarnation_v1, encode_record_registry_v2, encode_storage_format_version_v1,
        transcode_durable_record_to_v2,
    },
};
use riffdb_types::{
    AdministrationSequence, CommitSequence, DatabaseId, DualFrontier, EmbeddingMetadata,
    IndexEpoch, IndexId, SchemaHash,
};

use crate::codec::{
    decode_administration_audit_with_command_tables, decode_commit_with_event_table,
    decode_entity_record_v1, decode_event_route_v1, decode_index_entry_v2, decode_index_epoch_v1,
    decode_legacy_index_epoch_v1, decode_outbox_with_event_table,
    decode_service_audit_request_index_v1, encode_commit_record_v1, encode_event_route_v1,
    encode_index_epoch_v1, encode_outbox_intent_v1, encode_service_audit_request_index_v1,
};

use crate::checkpoint_root::CheckpointRoot;
use crate::error::{
    commit_error, database_error, precommit_storage_error, storage_error, table_error,
    transaction_error,
};
use crate::format_preflight::{
    RedbDurableFormatPreflight, RedbDurableFormatPreflightError, preflight_durable_format_path,
    preflight_durable_format_path_with_media, publish_initialized_current_marker_with_media,
};
use crate::gate::{ExclusiveGate, ExclusiveLease};
use crate::hooks::{RedbTestController, RedbTestOperation};
use crate::journal::JournalTable;
use crate::keys::{
    decode_application_sequence_key, decode_audit_by_request_key, decode_audit_key,
    decode_index_range_prefix_key, decode_partition_index_key, decode_vector_evidence_index_key,
    encode_audit_by_request_key, encode_audit_by_request_prefix, encode_event_route_key,
    encode_partition_index_key, encode_vector_health_observation_key,
    encode_vector_observation_key,
};
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, BYTE_TABLES, COLUMNAR_PROJECTION_CONTROLS, COMMITS, ENTITIES,
    ENTITY_CHAIN_HEADS, EVENT_ROUTES, EVENTS, INDEX_EPOCHS, META, META_ADMINISTRATION_SEQUENCE,
    META_APPLICATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP, META_CHANGELOG_V2_ROTATION_RECEIPT,
    META_CLEAN_CLOSE_LIFECYCLE, META_DATABASE_ID, META_FORMAT_VERSION, META_HISTORY_INCARNATION,
    META_INDEX_EPOCH_ROWS_REPAIRED, META_KEYS, META_RECORD_REGISTRY, META_RETENTION_HOLDS,
    META_RETENTION_WATERMARK, META_VALIDATED_PREFIX_CHECKPOINT, OUTBOX, OUTBOX_STATUS,
    PROJECTION_APPLIED, PROJECTION_FRONTIER, PROJECTION_STATE, SECONDARY_INDEXES, TABLE_NAMES,
    VALIDATED_PREFIX_ENTITY_HEADS, VECTOR_EVIDENCE, VECTOR_EVIDENCE_INDEX, VECTOR_OBSERVATIONS,
    VECTOR_PROJECTION_CONTROLS, create_all_tables,
};
use crate::media::{DynStorageBackend, JournalMedia, RealJournalMedia, RedbStorageMedia};
use crate::transient::{
    TransientIndexDelta, TransientIndexState, TransientIndexes, UnpublishedCommandIndexes,
};

/// Fixed redb page-cache budget for the production adapter.
///
/// redb's ambient default is 1 GiB and grows with pages touched. Keeping the
/// adapter's cache explicit prevents a clean reopen from acquiring
/// population-sized process-owned memory while retaining one bounded cache for
/// reads and writes. This is an internal performance bound; it does not alter
/// the durable format or transaction semantics.
const REDB_CACHE_SIZE_BYTES: usize = 4 * 1024 * 1024;

/// Sibling file that holds rebuildable derived state for a primary redb path.
pub(crate) fn derived_store_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("db"))
        .to_os_string();
    name.push(".derived");
    path.with_file_name(name)
}

/// Every file this database owns, including rebuildable derived state.
///
/// Restore, retirement and publication must account for this set rather than a
/// fixed three-file list (ADR-0246). The sidecar is rebuildable: removing it is
/// always correct.
pub(crate) fn owned_store_files(database: &Path) -> Vec<PathBuf> {
    vec![
        database.to_path_buf(),
        crate::journal::journal_path(database),
        crate::durable_format_marker_path(database),
        derived_store_path(database),
    ]
}

/// Drops derived state that belonged to a replaced timeline.
pub(crate) fn discard_replaced_derived_sidecar(database: &Path) -> Result<(), StorageError> {
    let sidecar = derived_store_path(database);
    if !owned_store_files(database)
        .iter()
        .any(|file| file == &sidecar)
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    match std::fs::remove_file(sidecar) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(storage_error(StorageErrorKind::Unavailable)),
    }
}

fn empty_derived_read() -> Result<redb::ReadTransaction, StorageError> {
    static EMPTY: std::sync::OnceLock<Database> = std::sync::OnceLock::new();
    if EMPTY.get().is_none() {
        let opened = RedbStore::open_derived_database(Path::new("empty-derived"), true)?;
        let _ = EMPTY.set(opened);
    }
    EMPTY
        .get()
        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?
        .begin_read()
        .map_err(transaction_error)
}

#[cfg(feature = "test-fixtures")]
struct ExternalKillBarrierFileBackend {
    inner: redb::backends::FileBackend,
    controller: RedbTestController,
}

#[cfg(feature = "test-fixtures")]
impl std::fmt::Debug for ExternalKillBarrierFileBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ExternalKillBarrierFileBackend")
    }
}

#[cfg(feature = "test-fixtures")]
impl redb::StorageBackend for ExternalKillBarrierFileBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        self.controller.wait_before_engine_sync_if_armed()?;
        self.inner.sync_data()?;
        self.controller.note_engine_sync_completed_if_armed();
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.inner.write(offset, data)
    }

    fn close(&self) -> Result<(), std::io::Error> {
        self.inner.close()
    }
}

/// Bound on `OUTBOX_STATUS` rows decoded by the pre-readiness `Delivering`
/// probe.
///
/// The probe exists to let a bounded start assert an invariant, so it must
/// itself be bounded: above this many retained status rows it reports
/// `Inconclusive` and the caller normalizes instead of claiming proof. Chosen at
/// the same order as the crate's other pre-readiness observation bounds
/// (`MAX_ROOT_VALIDATION_OBSERVATIONS`), which is far above any plausible
/// in-flight delivery set and far below a population-sized table.
///
/// A deployment that retains more than this many status rows keeps the
/// normalization it has today; it does not lose bounded startup.
pub(crate) const MAX_DELIVERING_PROBE_ROWS: u64 = 4_096;

/// Result of the bounded pre-readiness `Delivering` probe.
///
/// Three outcomes, not two, because "I could not tell" must not be recorded as
/// either proof. Only `Observed` is a contradiction of a clean certificate;
/// `Inconclusive` is ignorance and is resolved by doing the work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboxDeliveringProbe {
    /// Proven: no `Delivering` entry exists in this snapshot.
    NoneObserved,
    /// A `Delivering` entry was positively observed.
    Observed,
    /// Too many retained status rows to decide within the probe's bound.
    Inconclusive,
}

static COMMAND_PUBLICATION_COUNT: AtomicU64 = AtomicU64::new(0);
static COMMAND_PUBLICATION_RESIDENCE_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_PUBLICATION_RESIDENCE_MAX_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_RECEIPT_BLOCK_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_RECEIPT_BLOCK_MAX_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_DURABLE_TO_PUBLISH_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_DURABLE_TO_PUBLISH_MAX_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_PUBLICATION_WORK_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMAND_PUBLICATION_WORK_MAX_MICROS: AtomicU64 = AtomicU64::new(0);

fn record_publication_duration(sum: &AtomicU64, maximum: &AtomicU64, elapsed: std::time::Duration) {
    let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    let _ = sum.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(micros))
    });
    let _ = maximum.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.max(micros))
    });
}

fn saturating_elapsed_microseconds(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

pub(crate) fn command_publication_stage_census() -> [u64; 9] {
    [
        COMMAND_PUBLICATION_COUNT.load(Ordering::Relaxed),
        COMMAND_PUBLICATION_RESIDENCE_MICROS.load(Ordering::Relaxed),
        COMMAND_PUBLICATION_RESIDENCE_MAX_MICROS.load(Ordering::Relaxed),
        COMMAND_RECEIPT_BLOCK_MICROS.load(Ordering::Relaxed),
        COMMAND_RECEIPT_BLOCK_MAX_MICROS.load(Ordering::Relaxed),
        COMMAND_DURABLE_TO_PUBLISH_MICROS.load(Ordering::Relaxed),
        COMMAND_DURABLE_TO_PUBLISH_MAX_MICROS.load(Ordering::Relaxed),
        COMMAND_PUBLICATION_WORK_MICROS.load(Ordering::Relaxed),
        COMMAND_PUBLICATION_WORK_MAX_MICROS.load(Ordering::Relaxed),
    ]
}

pub(crate) struct SharedRedb {
    pub(crate) derived_source_pins: Arc<crate::derived_source_pin::DerivedSourcePins>,
    open_mode: OpenMode,
    follower_namespace: Mutex<Option<crate::maintenance::FollowerNamespace>>,
    pub(crate) database: Database,
    /// Sidecar redb for rebuildable derived state (ADR-0240). Writes here never
    /// acquire `mutation_gate` or the primary store's write transaction.
    /// Opened on first derived read or write so archive inventories that never
    /// touch derived state keep their closed three-file sets.
    derived: std::sync::OnceLock<Database>,
    /// Serialises the first open of `derived`. redb takes an exclusive file
    /// lock, so two threads racing to open the sidecar leave one holding an
    /// `Unavailable` that says nothing about the store's health. Only the
    /// holder of this opens; everyone else waits and then reads the `OnceLock`.
    derived_open: std::sync::Mutex<()>,
    derived_in_memory: bool,
    #[allow(dead_code, reason = "WP-070 offline backup consumes the source path")]
    path: PathBuf,
    /// The journal media port serving every side-file operation of this
    /// handle (ADR-0113). Production opens install [`RealJournalMedia`];
    /// only the hidden simulation constructor installs anything else.
    journal_media: Arc<dyn JournalMedia>,
    application_commit_profile: RedbCommitProfile,
    mutation_gate: ExclusiveGate,
    write_fenced: AtomicBool,
    /// True when redb invoked repair while opening this handle. Engine repair
    /// always disqualifies carried clean-close evidence for this generation.
    engine_repaired_at_open: bool,
    /// True when THIS open created the durable unit (the format preflight
    /// selected `InitializeCurrent`). A brand-new redb file has no persisted
    /// allocator state, so `engine_repaired_at_open` is always true here and
    /// says nothing about a previous close.
    engine_initialized_at_open: bool,
    /// Set only by the verified ADR-0157 startup handoff. Operational
    /// composition uses it to keep rebuildable population accelerators cold.
    pub(crate) bounded_clean_startup: AtomicBool,
    durable_commit_epoch: AtomicU64,
    /// Even values name stable redb roots; odd values mean an engine commit is
    /// in flight or visible but has not yet published its exact root identity.
    /// Readers use the adjacent condition variable only while an odd value is
    /// observed. The writer never holds that mutex across engine I/O.
    durable_root_publication: AtomicU64,
    durable_root_publication_wait: Mutex<()>,
    durable_root_publication_changed: Condvar,
    /// Predecessor read root installed before the first unpublished subgroup.
    ///
    /// `None` means ordinary readers may open redb's newest root. While an
    /// epoch is active every operational reader clones this immutable root;
    /// the epoch writer alone may observe redb's newer deferred roots.
    durable_read_frontier: RwLock<Option<Arc<CheckpointRoot>>>,
    /// The newest frontier-free read root, with the durable commit epoch that
    /// was already installed when it was captured.
    ///
    /// Only `begin_operational_read` reads it, and only on the branch where no
    /// durable frontier governs reads. See `current_read_root` for why an
    /// unchanged epoch entitles a later access to the same snapshot.
    current_read_root: RwLock<Option<(u64, Arc<CheckpointRoot>)>>,
    /// True while `current_read_root` may hold a root, so the frontier install
    /// can skip the lock on every later call once the root has been retired.
    current_read_root_live: AtomicBool,
    /// Shadow publication root used while WP-488 replaces the standard writer.
    /// It is not selected by operational reads until the complete frame-first
    /// path and recovery barriers are installed.
    composite_publication: RwLock<Option<crate::composite_view::RedbCompositePublication>>,
    /// Newest complete writer-private composite successor, including sealed
    /// epochs whose journal fence has not published yet.
    private_composite_frontier: Mutex<Option<Arc<crate::composite_view::RedbCompositeReadView>>>,
    /// Journal frames awaiting public composite-view advancement, in the exact
    /// order in which the sole writer submitted them.
    publication_queue: Mutex<PublicationQueue>,
    next_publication_ticket: AtomicU64,
    journal_runtime: Mutex<Option<JournalRuntime>>,
    journal_checkpoint: Mutex<Option<AsyncJournalCheckpoint>>,
    command_segment_preparation: crate::command_segment_preparation::CommandSegmentPreparationPool,
    test_controller: Option<RedbTestController>,
    transient_indexes: RwLock<TransientIndexState>,
    /// Process-local affine proof for ADR-0197's fresh-history locator prefix.
    /// Exact identities in sealed command epochs that are not public yet.
    /// Writers consult this overlay; operational readers never do.
    unpublished_command_indexes: Mutex<UnpublishedCommandIndexes>,
    /// True only after this handle's startup validation session finished with
    /// ZERO structural findings of any scope. Gates every validated-prefix
    /// checkpoint write (ADR-0019 A1: a finding of any severity vetoes the
    /// write so the fast path can never silence it).
    startup_validation_clean: AtomicBool,
    /// Failed checkpoint writes after clean validation (non-fatal; fast path lost).
    checkpoint_write_failures: AtomicU64,
    /// Per-reason counts of ignored validated-prefix checkpoints
    /// (index = `CheckpointIgnoreReason::index`).
    checkpoint_ignored: [AtomicU64; 10],
    /// Failed clean-close certificate writes (non-fatal; next open's bounded
    /// path lost). The checkpoint path's peer counter for the same class of
    /// deliberately-ignored graceful-shutdown write failure.
    clean_close_write_failures: AtomicU64,
    /// Per-reason counts of declined ADR-0157 bounded startups
    /// (index = `CleanCloseDeclineReason::index`).
    clean_close_declined: [AtomicU64; 11],
    /// True only when a bounded start PROVED no in-flight `Delivering` outbox
    /// entry exists. Never set optimistically: an inconclusive probe leaves it
    /// false, so a caller that skips normalization on the strength of this flag
    /// can only do so on proof.
    outbox_delivering_proven_absent: AtomicBool,
    /// Transient population-index rebuilds performed on this handle, and the
    /// `COMMITS` rows each one walked.
    ///
    /// The rebuild decodes every command segment, re-derives every manifest
    /// key, and retains every segment, so it is the most expensive thing this
    /// adapter does at open. Bounded clean-close startup deliberately skips it
    /// and leaves the caches cold, but `ensure_transient_indexes_ready` will
    /// still build them on the first derived read — which can be before
    /// readiness. Counted so a "bounded" start that paid for it anyway is
    /// visible rather than inferred from wall clock.
    transient_index_rebuilds: AtomicU64,
    transient_index_commit_rows: AtomicU64,
    fresh_locator_history_fallback_scans: AtomicU64,
    fresh_locator_history_fallback_rows: AtomicU64,
    /// Retention watermark sequence, loaded and self-hash-verified once at
    /// open. The watermark advances only under exclusive OFFLINE maintenance,
    /// which cannot run while this handle holds the database open, so reads
    /// never re-hash the meta record per call (ADR-0085 A2 hot-path rule).
    retention_watermark: AtomicU64,
    /// Terminal `ExecutionFailed` rows in `IDEMPOTENCY`.
    ///
    /// The checkpoint's `idempotency_count` admits only materialized
    /// `StoredOutcome` rows, which redb's `IDEMPOTENCY` row count cannot
    /// distinguish; this census is
    /// the one quantity the O(1) checkpoint build cannot read from table
    /// metadata. It is seeded from the startup walk that also opens the
    /// checkpoint write gate — the same statement pair, so a checkpoint can
    /// never be written from an unseeded census — and advanced by the single
    /// lane that writes such a row, inside that lane's mutation lease.
    ///
    /// Process memory only. It never survives a crash and never needs to:
    /// checkpoints are written only at clean points, and every open re-seeds
    /// from its own evidence. A drifted census can never be silently trusted —
    /// the next open compares every recorded count against redb's own row counts
    /// and refuses on divergence (`verify_checkpoint_prefix_counts`).
    terminal_execution_failure_rows: AtomicU64,
    /// History rows iterated to compute checkpoint counts on this handle. Stays
    /// zero for the whole life of a handle whose checkpoints are all built from
    /// table metadata (test observability; pins the O(1) claim).
    checkpoint_count_rows_walked: AtomicU64,
    /// The ADR-0100 changelog publication observer.
    ///
    /// Set once, at the single open site, and never replaced: the publication
    /// edge therefore needs no lock to reach it and cannot be re-pointed at
    /// runtime. The default observes nothing.
    changelog_port: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
}

/// Closed redb durability profiles for authoritative application writes.
///
/// Both profiles retain `Durability::Immediate` and the same RiffDB
/// acknowledgement contract. `Standard` uses redb's checksummed one-phase
/// commit slots and assumes a non-Byzantine host and storage stack. `Hardened`
/// adds redb's optional two-phase commit defense for a stronger local threat
/// model. This is a process composition choice, never a command input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedbCommitProfile {
    /// Immediate one-phase commits with checksummed commit slots.
    Standard,
    /// Immediate two-phase commits for the stronger local recovery oracle.
    Hardened,
}

impl RedbCommitProfile {
    pub(crate) const fn uses_two_phase(self) -> bool {
        matches!(self, Self::Hardened)
    }
}

impl SharedRedb {
    pub(crate) fn is_follower_mode(&self) -> bool {
        self.open_mode == OpenMode::Follower
    }

    pub(crate) fn cannot_serve_command_commit(&self) -> bool {
        self.open_mode != OpenMode::Source
    }

    pub(crate) fn derived_database(&self) -> Result<&Database, StorageError> {
        if let Some(database) = self.derived.get() {
            return Ok(database);
        }
        // Serialise the open. Retrying on `Unavailable` while the winner has
        // not yet published is not sufficient: between a loser's failed open
        // and the winner's `set`, `get` is still empty, and the loser returns
        // an `Unavailable` that describes a race rather than the store. Holding
        // this guard means exactly one thread ever calls `open_derived_database`
        // for a handle, so `Unavailable` from it is always real.
        let _open = self
            .derived_open
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Some(database) = self.derived.get() {
            return Ok(database);
        }
        let opened = RedbStore::open_derived_database(&self.path, self.derived_in_memory)?;
        let _ = self.derived.set(opened);
        self.derived
            .get()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    pub(crate) fn before_test_commit(
        &self,
        operation: RedbTestOperation,
    ) -> Result<(), StorageError> {
        if let Some(controller) = &self.test_controller {
            controller.before_commit(operation)?;
        }
        Ok(())
    }

    pub(crate) fn after_test_commit(
        &self,
        operation: RedbTestOperation,
    ) -> Result<(), StorageError> {
        if let Some(controller) = &self.test_controller
            && let Err(error) = controller.after_commit(operation)
        {
            self.fence_writes();
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn observe_index_migration_page(&self) {
        if let Some(controller) = &self.test_controller {
            controller.observe_index_migration_page();
        }
    }

    pub(crate) fn observe_index_migration_batch(&self, v1_rewrites: usize, v2_confirms: usize) {
        if let Some(controller) = &self.test_controller {
            controller.observe_index_migration_batch(v1_rewrites, v2_confirms);
        }
    }

    pub(crate) fn fence_writes(&self) {
        self.write_fenced.store(true, Ordering::Release);
    }

    /// Hands one published durable-frontier advancement to the ADR-0100
    /// changelog publication observer.
    ///
    /// Called strictly AFTER the frontier swap, at the same release rank as
    /// every other covered effect (ADR-0101 §4). It returns `()` and cannot
    /// fail: the frontier is already published, so there is nothing left for
    /// an observer to refuse. The observer receives the exact published
    /// snapshot, pinned — it can never reach a writer-private applied root.
    fn observe_changelog_publication(
        &self,
        predecessor: riffdb_types::DualFrontier,
        covered: riffdb_types::DualFrontier,
        frame_hash: [u8; 32],
        transition_count: usize,
        journaled: bool,
        published: RedbReadAccess,
    ) {
        self.changelog_port.observe_published_advancement(
            riffdb_storage_api::PublishedFrontierAdvancement::new(
                predecessor,
                covered,
                frame_hash,
                u32::try_from(transition_count).unwrap_or(u32::MAX),
                journaled,
                Arc::new(crate::changelog::RedbPublishedSnapshot::new(published)),
            ),
        );
    }

    /// Enqueues an already published pin; receipt reads and framing belong to
    /// the consumer, after this callback returns and the writer gate releases.
    fn observe_changelog_snapshot_v3(&self, published: RedbReadAccess) {
        self.changelog_port.observe_published_snapshot_v3(Arc::new(
            crate::changelog::RedbPublishedSnapshot::new(published),
        ));
    }

    /// A drained direct/lifecycle owner can pin the committed checkpoint. A
    /// failed pin closes consumers without changing the durable write's result.
    fn observe_changelog_checkpoint_v3(&self) {
        match self.capture_checkpoint_root() {
            Ok(root) => self.observe_changelog_snapshot_v3(RedbReadAccess::Durable(root)),
            Err(_) => self.changelog_port.observe_source_unavailable_v3(),
        }
    }

    /// Retention watermark sequence verified once at open (0 = unpruned).
    pub(crate) fn retention_watermark(&self) -> u64 {
        self.retention_watermark.load(Ordering::Acquire)
    }

    pub(crate) fn durable_commit_epoch(&self) -> u64 {
        self.durable_commit_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn set_startup_validation_clean(&self, clean: bool) {
        self.startup_validation_clean
            .store(clean, Ordering::Release);
    }

    pub(crate) fn startup_validation_clean(&self) -> bool {
        self.startup_validation_clean.load(Ordering::Acquire)
    }

    pub(crate) fn note_checkpoint_write_failure(&self) {
        let _ = self
            .checkpoint_write_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn checkpoint_write_failures(&self) -> u64 {
        self.checkpoint_write_failures.load(Ordering::Relaxed)
    }

    /// Counts one failed clean-close certificate write.
    ///
    /// The graceful-shutdown call site discards the error per ADR-0019 A1
    /// write-failure semantics, which is defensible — acknowledged work is
    /// already durable — but it left a failed certificate write with no
    /// observable at all. The next open then reports `record_absent` or
    /// `state_not_clean` with nothing to distinguish "never tried" from
    /// "tried and failed". This is the checkpoint path's
    /// `note_checkpoint_write_failure` equivalent; it changes no control flow.
    pub(crate) fn note_clean_close_write_failure(&self) {
        let _ = self
            .clean_close_write_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn clean_close_write_failures(&self) -> u64 {
        self.clean_close_write_failures.load(Ordering::Relaxed)
    }

    pub(crate) fn note_clean_close_declined(
        &self,
        reason: crate::clean_close::CleanCloseDeclineReason,
    ) {
        let _ = self.clean_close_declined[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn outbox_delivering_proven_absent(&self) -> bool {
        self.outbox_delivering_proven_absent.load(Ordering::Acquire)
    }

    pub(crate) fn note_transient_index_rebuild(&self, commit_rows: u64) {
        let _ = self
            .transient_index_rebuilds
            .fetch_add(1, Ordering::Relaxed);
        let _ = self
            .transient_index_commit_rows
            .fetch_add(commit_rows, Ordering::Relaxed);
    }

    pub(crate) fn transient_index_rebuilds(&self) -> u64 {
        self.transient_index_rebuilds.load(Ordering::Relaxed)
    }

    pub(crate) fn transient_index_commit_rows(&self) -> u64 {
        self.transient_index_commit_rows.load(Ordering::Relaxed)
    }

    /// ADR-0236 obligation three: the bounded history fallback is the only
    /// absence backstop, so both how often it runs and how much it reads are
    /// evidence. `rows` is the span it covered, bounded by the distance from
    /// the checkpoint application frontier to the captured frontier.
    fn note_fresh_locator_history_fallback_scan(&self, rows: u64) {
        if let Some(controller) = &self.test_controller {
            controller.observe_fresh_locator_history_fallback_scan(rows);
        }
        let _ = self.fresh_locator_history_fallback_scans.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(1)),
        );
        let _ = self.fresh_locator_history_fallback_rows.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(rows)),
        );
    }

    #[cfg(test)]
    pub(crate) fn fresh_locator_history_fallback_scans(&self) -> u64 {
        self.fresh_locator_history_fallback_scans
            .load(Ordering::Relaxed)
    }

    pub(crate) fn clean_close_decline_counts(&self) -> [(&'static str, u64); 11] {
        crate::clean_close::CleanCloseDeclineReason::ALL.map(|reason| {
            (
                reason.as_str(),
                self.clean_close_declined[reason.index()].load(Ordering::Relaxed),
            )
        })
    }

    /// Seeds the terminal execution-failure census from one completed startup
    /// walk. Called only where the checkpoint write gate opens, so "the census
    /// is exact for this durable head" and "checkpoints may be written" become
    /// true together.
    pub(crate) fn seed_terminal_execution_failure_rows(&self, rows: u64) {
        self.terminal_execution_failure_rows
            .store(rows, Ordering::Release);
    }

    pub(crate) fn terminal_execution_failure_rows(&self) -> u64 {
        self.terminal_execution_failure_rows.load(Ordering::Acquire)
    }

    /// Counts one newly durable terminal `ExecutionFailed` row. Called from the
    /// commit path while the mutation lease is still held, so a checkpoint write
    /// that acquires the lease afterwards can never observe the row without its
    /// census increment.
    pub(crate) fn note_terminal_execution_failure_row(&self) {
        let _ = self
            .terminal_execution_failure_rows
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn note_checkpoint_count_rows_walked(&self, rows: u64) {
        let _ = self
            .checkpoint_count_rows_walked
            .fetch_add(rows, Ordering::Relaxed);
    }

    pub(crate) fn checkpoint_count_rows_walked(&self) -> u64 {
        self.checkpoint_count_rows_walked.load(Ordering::Relaxed)
    }

    pub(crate) fn note_checkpoint_ignored(
        &self,
        reason: crate::validated_prefix::CheckpointIgnoreReason,
    ) {
        let _ = self.checkpoint_ignored[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn checkpoint_ignore_counts(&self) -> [(&'static str, u64); 10] {
        crate::validated_prefix::CheckpointIgnoreReason::ALL.map(|reason| {
            (
                reason.as_str(),
                self.checkpoint_ignored[reason.index()].load(Ordering::Relaxed),
            )
        })
    }

    /// Verifies the ADR-0157 clean-close certificate, naming the precondition
    /// that declined bounded startup.
    ///
    /// Fail-closed is unchanged: every path that used to return `None` still
    /// returns `Declined`, in the same order, on exactly the same conditions.
    /// The only new behaviour is that the reason is now a closed discriminant,
    /// counted on the store and readable from the startup session, instead of
    /// being erased into an anonymous `None`.
    pub(crate) fn verified_clean_close_lifecycle(
        &self,
        transaction: &ReadTransaction,
        database_id: DatabaseId,
        history_incarnation: u64,
    ) -> Result<crate::clean_close::CleanCloseVerdict, StorageError> {
        let verdict =
            self.verify_clean_close_lifecycle_inner(transaction, database_id, history_incarnation)?;
        if let Some(reason) = verdict.declined() {
            self.note_clean_close_declined(reason);
        }
        Ok(verdict)
    }

    fn verify_clean_close_lifecycle_inner(
        &self,
        transaction: &ReadTransaction,
        database_id: DatabaseId,
        history_incarnation: u64,
    ) -> Result<crate::clean_close::CleanCloseVerdict, StorageError> {
        use crate::clean_close::{CleanCloseDeclineReason, CleanCloseVerdict};

        if self.engine_repaired_at_open {
            // Both decline, exactly as before; the split only says whether the
            // rebuild was the unavoidable cost of creating the durable unit or
            // the consequence of a previous close that left no allocator state.
            return Ok(CleanCloseVerdict::Declined(
                if self.engine_initialized_at_open {
                    CleanCloseDeclineReason::EngineInitializedAtOpen
                } else {
                    CleanCloseDeclineReason::EngineRepairedAtOpen
                },
            ));
        }
        let meta = transaction.open_table(META).map_err(table_error)?;
        let Some(encoded) = meta
            .get(META_CLEAN_CLOSE_LIFECYCLE)
            .map_err(precommit_storage_error)?
        else {
            return Ok(CleanCloseVerdict::Declined(
                CleanCloseDeclineReason::RecordAbsent,
            ));
        };
        let Ok(lifecycle) = crate::clean_close::CleanCloseLifecycle::decode(encoded.value()) else {
            return Ok(CleanCloseVerdict::Declined(
                CleanCloseDeclineReason::RecordDecodeFailed,
            ));
        };
        if lifecycle.database_id() != database_id {
            return Ok(CleanCloseVerdict::Declined(
                CleanCloseDeclineReason::DatabaseIdMismatch,
            ));
        }
        if lifecycle.history_incarnation() != history_incarnation {
            return Ok(CleanCloseVerdict::Declined(
                CleanCloseDeclineReason::IncarnationMismatch,
            ));
        }
        if !matches!(
            lifecycle.state(),
            crate::clean_close::CleanCloseState::Clean(_)
        ) {
            return Ok(CleanCloseVerdict::Declined(
                CleanCloseDeclineReason::StateNotClean,
            ));
        }
        drop(encoded);
        drop(meta);
        if !changelog_lifecycle::clean_roots_available(transaction)? {
            return Ok(CleanCloseVerdict::Declined(
                CleanCloseDeclineReason::BoundedRootsUnavailable,
            ));
        }
        let application_frontier = read_commit_tail(transaction)?;
        let administration_frontier = read_administration_tail(transaction)?;
        let header_digest = match crate::journal::verify_clean_close_header_digest_with_media(
            self.journal_media.as_ref(),
            &self.path,
            database_id,
            application_frontier,
            administration_frontier,
        ) {
            Ok(digest) => digest,
            Err(crate::journal::JournalIoError::Corrupt) => {
                return Ok(CleanCloseVerdict::Declined(
                    CleanCloseDeclineReason::JournalBoundaryUnverified,
                ));
            }
            Err(error) => return Err(recovery_journal_error(error)),
        };
        let binding =
            match crate::clean_close::bounded_state_binding_hash(transaction, header_digest) {
                Ok(binding) => binding,
                Err(error)
                    if matches!(
                        error.kind(),
                        StorageErrorKind::CorruptData | StorageErrorKind::LimitExceeded
                    ) =>
                {
                    return Ok(CleanCloseVerdict::Declined(
                        CleanCloseDeclineReason::BoundedRootsUnavailable,
                    ));
                }
                Err(error) => return Err(error),
            };
        if lifecycle.state() != crate::clean_close::CleanCloseState::Clean(binding) {
            return Ok(CleanCloseVerdict::Declined(
                CleanCloseDeclineReason::BindingMismatch,
            ));
        }
        // The certificate covers bounded roots, not delivery lane state. A
        // clean close stopped every writer and delivery lane, so no attempt can
        // still hold a lease; assert that rather than assume it, with a probe
        // that cannot reach the population indexes. Only a positive observation
        // declines — ignorance is recorded and resolved by normalizing.
        match self.probe_outbox_delivering(transaction)? {
            OutboxDeliveringProbe::Observed => {
                self.outbox_delivering_proven_absent
                    .store(false, Ordering::Release);
                return Ok(CleanCloseVerdict::Declined(
                    CleanCloseDeclineReason::OutboxDeliveringObserved,
                ));
            }
            OutboxDeliveringProbe::NoneObserved => {
                self.outbox_delivering_proven_absent
                    .store(true, Ordering::Release);
            }
            OutboxDeliveringProbe::Inconclusive => {
                self.outbox_delivering_proven_absent
                    .store(false, Ordering::Release);
            }
        }
        Ok(CleanCloseVerdict::Verified(lifecycle))
    }

    /// Bounded probe for any in-flight `Delivering` outbox entry.
    ///
    /// `Delivering` is written only by a delivery worker holding a lease, and an
    /// event with no `OUTBOX_STATUS` row canonically means never-attempted
    /// `Pending` (SPEC 8.6). So `Delivering` is a subset of `OUTBOX_STATUS`
    /// rows, and this probe never consults `COMMITS`, the undelivered set, or
    /// the transient population indexes — it cannot reach
    /// `ensure_transient_indexes_ready` and therefore cannot trigger a rebuild.
    ///
    /// Bounded three ways, in cost order: redb reports the row count from table
    /// metadata (no walk); an empty table proves absence outright; and a
    /// non-empty table is decoded only up to
    /// [`MAX_DELIVERING_PROBE_ROWS`], above which the probe reports
    /// [`OutboxDeliveringProbe::Inconclusive`] rather than walking a
    /// population-sized table before readiness (ADR-0156 §5).
    ///
    /// Inconclusive is ignorance, not contradiction: the caller must still
    /// normalize, and must not treat it as proof of either state.
    pub(crate) fn probe_outbox_delivering(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<OutboxDeliveringProbe, StorageError> {
        let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        let rows = statuses.len().map_err(precommit_storage_error)?;
        if rows == 0 {
            return Ok(OutboxDeliveringProbe::NoneObserved);
        }
        if rows > MAX_DELIVERING_PROBE_ROWS {
            return Ok(OutboxDeliveringProbe::Inconclusive);
        }
        let mut inspected = 0_u64;
        for entry in statuses.iter().map_err(precommit_storage_error)? {
            let (_, value) = entry.map_err(precommit_storage_error)?;
            inspected = inspected.saturating_add(1);
            if inspected > MAX_DELIVERING_PROBE_ROWS {
                // The count and the iteration disagreed. Claim nothing.
                return Ok(OutboxDeliveringProbe::Inconclusive);
            }
            let status = crate::codec::decode_outbox_status_v1(value.value())?;
            if matches!(
                status.value().state(),
                riffdb_storage_api::OutboxDeliveryStateV1::Delivering { .. }
            ) {
                return Ok(OutboxDeliveringProbe::Observed);
            }
        }
        Ok(OutboxDeliveringProbe::NoneObserved)
    }

    pub(crate) fn advance_dirty_lifecycle_before_activation(
        &self,
        database_id: DatabaseId,
        history_incarnation: u64,
        verified_clean: Option<crate::clean_close::CleanCloseLifecycle>,
    ) -> Result<(), StorageError> {
        let read = self.database.begin_read().map_err(transaction_error)?;
        if crate::follower_lifecycle::preserve_attached_lifecycle(
            &read,
            database_id,
            history_incarnation,
        )? {
            if verified_clean.is_some() {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            return Ok(());
        }
        drop(read);
        let mut transaction = self.database.begin_write().map_err(transaction_error)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let receipt = changelog_lifecycle::prepare(
            &transaction,
            changelog_lifecycle::LifecycleSource::Dirty,
            database_id,
            history_incarnation,
        )?;
        let mut meta = transaction.open_table(META).map_err(table_error)?;
        let current = meta
            .get(META_CLEAN_CLOSE_LIFECYCLE)
            .map_err(precommit_storage_error)?
            .map(|encoded| crate::clean_close::CleanCloseLifecycle::decode(encoded.value()));
        let next = match (verified_clean, current) {
            (Some(expected), Some(Ok(observed))) if expected == observed => {
                expected.successor_dirty()
            }
            (Some(_), _) => return Err(storage_error(StorageErrorKind::CorruptData)),
            (None, Some(Ok(observed)))
                if observed.database_id() == database_id
                    && observed.history_incarnation() == history_incarnation =>
            {
                observed.successor_dirty()
            }
            (None, None | Some(Err(_))) => {
                crate::clean_close::CleanCloseLifecycle::dirty(database_id, history_incarnation, 1)
            }
            (None, Some(Ok(_))) => {
                crate::clean_close::CleanCloseLifecycle::dirty(database_id, history_incarnation, 1)
            }
        }
        .map_err(|error| match error {
            crate::clean_close::CleanCloseCodecError::GenerationExhausted => {
                storage_error(StorageErrorKind::SequenceExhausted)
            }
            crate::clean_close::CleanCloseCodecError::Invalid => {
                storage_error(StorageErrorKind::CorruptData)
            }
        })?;
        let encoded = next
            .encode()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        meta.insert(META_CLEAN_CLOSE_LIFECYCLE, encoded.as_slice())
            .map_err(precommit_storage_error)?;
        drop(meta);
        if let Some(receipt) = receipt {
            receipt.stage(&transaction)?;
        }
        #[cfg(test)]
        changelog_lifecycle::crash_edge("dirty-staged");
        self.commit_durable(transaction)?;
        #[cfg(test)]
        changelog_lifecycle::crash_edge("dirty-committed");
        self.observe_changelog_checkpoint_v3();
        Ok(())
    }

    pub(crate) fn commit_durable(&self, transaction: WriteTransaction) -> Result<(), StorageError> {
        if self.durable_commit_epoch.load(Ordering::Acquire) == u64::MAX {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::SequenceExhausted));
        }
        let publication = self.durable_root_publication.load(Ordering::Acquire);
        if !publication.is_multiple_of(2) {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let Some(stable_successor) = publication.checked_add(2) else {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::SequenceExhausted));
        };
        let in_progress = stable_successor - 1;
        if self
            .durable_root_publication
            .compare_exchange(
                publication,
                in_progress,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let committed = transaction.commit();
        #[cfg(test)]
        if committed.is_ok()
            && let Some(controller) = &self.test_controller
        {
            controller.wait_after_engine_commit_before_root_publication();
        }
        let publication_wait = self.durable_root_publication_wait.lock();
        self.durable_root_publication
            .store(stable_successor, Ordering::Release);
        self.durable_root_publication_changed.notify_all();
        let publication_wait_poisoned = match publication_wait {
            Ok(guard) => {
                drop(guard);
                false
            }
            Err(poisoned) => {
                drop(poisoned.into_inner());
                self.fence_writes();
                true
            }
        };
        if let Err(error) = committed {
            self.fence_writes();
            return Err(commit_error(error));
        }
        if publication_wait_poisoned {
            return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
        }
        if self
            .durable_commit_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .is_err()
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::SequenceExhausted));
        }
        Ok(())
    }

    fn begin_operational_read(&self) -> Result<RedbReadAccess, StorageError> {
        // Keep the shared guard through the selection below: an epoch cannot
        // install its predecessor frontier between observing `None` and redb
        // selecting the newest (possibly deferred) root.
        let frontier = self
            .durable_read_frontier
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Some(transaction) = frontier.as_ref() {
            return Ok(RedbReadAccess::Durable(Arc::clone(transaction)));
        }
        Ok(RedbReadAccess::Current(self.current_read_root()?))
    }

    /// The newest snapshot a frontier-free reader is entitled to observe.
    ///
    /// A `ReadTransaction` is one fixed committed root, so the only question a
    /// reused one raises is freshness: may THIS access be served the snapshot
    /// an earlier access opened? `durable_root_publication` marks an engine
    /// commit in progress before I/O, remains odd across the interval where
    /// redb may expose the successor, and publishes one distinct even generation
    /// after success or failure. A capture is accepted only when the same even
    /// generation brackets `begin_read`; otherwise it waits or retries within
    /// fixed bounds. The cached root therefore contains exactly the stable
    /// generation in its stamp and can be reused only at that generation.
    ///
    /// The caller holds the durable-read-frontier guard, so a frontier install
    /// cannot interleave with the capture; once one does install, it retires
    /// this root rather than leaving it to pin pages no reader can select.
    fn current_read_root(&self) -> Result<Arc<CheckpointRoot>, StorageError> {
        let epoch = self.stable_durable_root_version()?;
        {
            let cached = self
                .current_read_root
                .read()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            if let Some((captured, root)) = cached.as_ref()
                && *captured == epoch
            {
                return Ok(Arc::clone(root));
            }
        }
        let mut cached = self
            .current_read_root
            .write()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        // Re-read under the exclusive guard. A commit between the two loads
        // must not be stamped with the older epoch, and a racing capture that
        // already installed the newest snapshot must be reused rather than
        // replaced by an equally new one.
        let epoch = self.stable_durable_root_version()?;
        if let Some((captured, root)) = cached.as_ref()
            && *captured == epoch
        {
            return Ok(Arc::clone(root));
        }
        let root = self.capture_checkpoint_root_at(epoch)?;
        *cached = Some((epoch, Arc::clone(&root)));
        self.current_read_root_live.store(true, Ordering::Release);
        Ok(root)
    }

    fn stable_durable_root_version(&self) -> Result<u64, StorageError> {
        const MAX_WAIT: Duration = Duration::from_secs(30);
        let started = Instant::now();
        loop {
            let version = self.durable_root_publication.load(Ordering::Acquire);
            if version.is_multiple_of(2) {
                return Ok(version);
            }
            #[cfg(test)]
            if let Some(controller) = &self.test_controller {
                controller.observe_root_capture_retry();
            }
            let guard = match self.durable_root_publication_wait.lock() {
                Ok(guard) => guard,
                Err(_) => {
                    self.fence_writes();
                    return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
                }
            };
            if self
                .durable_root_publication
                .load(Ordering::Acquire)
                .is_multiple_of(2)
            {
                drop(guard);
                continue;
            }
            let remaining = MAX_WAIT
                .checked_sub(started.elapsed())
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
            let (_guard, wait) = match self
                .durable_root_publication_changed
                .wait_timeout(guard, remaining)
            {
                Ok(wait) => wait,
                Err(_) => {
                    self.fence_writes();
                    return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
                }
            };
            if wait.timed_out()
                && !self
                    .durable_root_publication
                    .load(Ordering::Acquire)
                    .is_multiple_of(2)
            {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
        }
    }

    fn capture_checkpoint_root_at(
        &self,
        expected: u64,
    ) -> Result<Arc<CheckpointRoot>, StorageError> {
        const MAX_CAPTURE_ATTEMPTS: usize = 64;
        let mut version = expected;
        for _ in 0..MAX_CAPTURE_ATTEMPTS {
            let transaction = self.database.begin_read().map_err(transaction_error)?;
            let after = self.durable_root_publication.load(Ordering::Acquire);
            if version == after && after.is_multiple_of(2) {
                return Ok(Arc::new(CheckpointRoot::new(transaction, after)));
            }
            drop(transaction);
            version = self.stable_durable_root_version()?;
        }
        Err(storage_error(StorageErrorKind::Unavailable))
    }

    fn capture_checkpoint_root(&self) -> Result<Arc<CheckpointRoot>, StorageError> {
        let version = self.stable_durable_root_version()?;
        self.capture_checkpoint_root_at(version)
    }

    /// Whether a reusable frontier-free root is held right now.
    ///
    /// Test observability for the retirement contract: the pin it would leave
    /// behind is invisible from outside redb, so it is asserted directly.
    #[cfg(test)]
    pub(crate) fn holds_reusable_read_root(&self) -> bool {
        self.current_read_root
            .read()
            .expect("reusable read root")
            .is_some()
    }

    /// Releases the reusable frontier-free root once a durable frontier governs
    /// every operational read.
    ///
    /// Called under the durable-read-frontier write guard at each install, so
    /// no capture can be in flight and none can start afterwards. Holding the
    /// root past that point would keep a redb read transaction alive that no
    /// reader can ever be handed again, pinning every page freed after it.
    pub(crate) fn retire_current_read_root(&self) {
        if !self.current_read_root_live.load(Ordering::Acquire) {
            return;
        }
        self.current_read_root_live.store(false, Ordering::Release);
        if let Ok(mut cached) = self.current_read_root.write() {
            *cached = None;
        }
    }

    fn begin_composite_operational_read(&self) -> Result<RedbReadAccess, StorageError> {
        // A published composite view is the complete standard-profile state:
        // one immutable redb checkpoint plus the exact fenced journal suffix.
        // Capture it before consulting the legacy deferred-redb frontier so a
        // reader can never select the checkpoint alone after publication.
        let composite = self
            .composite_publication
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Some(publication) = composite.as_ref() {
            return Ok(RedbReadAccess::Composite(publication.capture()?));
        }
        drop(composite);
        self.begin_operational_read()
    }

    #[cfg(test)]
    pub(crate) fn begin_composite_operational_read_profiled(
        &self,
    ) -> Result<(RedbReadAccess, CompositeReadAcquireProfileV1), StorageError> {
        let outer_started = Instant::now();
        let composite = self
            .composite_publication
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let outer_lock_ns = elapsed_nanos(outer_started);
        let view_started = Instant::now();
        let access = if let Some(publication) = composite.as_ref() {
            RedbReadAccess::Composite(publication.capture()?)
        } else {
            drop(composite);
            self.begin_operational_read()?
        };
        Ok((
            access,
            CompositeReadAcquireProfileV1 {
                outer_lock_ns,
                view_capture_ns: elapsed_nanos(view_started),
            },
        ))
    }
}

/// A dormant handle to one redb-backed RiffDB database.
///
/// Opening this handle performs only redb recovery. It does not claim RiffDB
/// structural, catalog, or operational readiness.
pub struct RedbStore {
    pub(crate) shared: Arc<SharedRedb>,
}

/// Unforgeable crate-internal proof that an offline retention operation owns
/// a recovered redb checkpoint and an empty rebased journal.
pub(crate) struct RedbOfflineRetentionPreparation<'a> {
    database: &'a Database,
    _journal: crate::journal::RetentionJournalRebaseWitness,
}

impl RedbOfflineRetentionPreparation<'_> {
    pub(crate) const fn database(&self) -> &Database {
        self.database
    }
}

/// Redb ports released by a complete structural evidence session.
///
/// This value is still dormant: it exposes no storage trait implementation and
/// is not an operational-readiness proof.
pub struct RedbDormantPorts {
    pub(crate) shared: Arc<SharedRedb>,
    pub(crate) pending_v3_activation: Option<crate::startup::PendingV3Activation>,
}

/// Activated redb-backed semantic storage ports.
///
/// Only server composition should construct this value, after it has matched
/// the structural handoff with the catalog-owned validation proof.
pub struct RedbOperationalPorts {
    pub(crate) shared: Arc<SharedRedb>,
}

// All operational helpers receive the sealed captured owner, never a raw
// writable transaction. Journal epochs retain their separate mutation source.
pub(crate) type OperationalWriteTransaction = crate::changelog_v3_write::CapturedImmediateWrite;

pub(crate) struct RedbWriteAccess {
    shared: Arc<SharedRedb>,
    transaction: Option<OperationalWriteTransaction>,
    direct_changelog_attribution: Option<riffdb_storage_api::ChangelogAttributionV3>,
    ownership: Option<RedbWriteOwnership>,
    journal_mutations: Option<RefCell<crate::journal::JournalMutationBuffer>>,
    journal_mutation_start: u32,
    journal_checkpoint: Option<JournalRuntime>,
    composite_predecessor: Option<Arc<crate::composite_view::RedbCompositeReadView>>,
    composite_stage: Option<RefCell<crate::composite_view::RedbCompositeMutationStage>>,
    command_prefix_capture: RefCell<crate::command_prefix::CommandMutationCapture>,
}

/// Write transaction against the derived-state sidecar (ADR-0240).
///
/// Holds no primary mutation lease and does not open a primary write
/// transaction. Failpoints still observe `RedbTestOperation` so crash tests
/// retain their existing before/after-commit edges.
pub(crate) struct DerivedWriteAccess {
    shared: Arc<SharedRedb>,
    transaction: Option<WriteTransaction>,
}

impl DerivedWriteAccess {
    pub(crate) fn transaction(&self) -> Result<&WriteTransaction, StorageError> {
        self.transaction
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))
    }

    pub(crate) fn commit_for(mut self, operation: RedbTestOperation) -> Result<(), StorageError> {
        self.shared.before_test_commit(operation)?;
        let sidecar = self
            .transaction
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        match sidecar.commit() {
            Ok(()) => self.shared.after_test_commit(operation),
            Err(error) => Err(commit_error(error)),
        }
    }

    pub(crate) fn abort(mut self) -> Result<(), StorageError> {
        if let Some(transaction) = self.transaction.take() {
            transaction
                .abort()
                .map_err(crate::error::precommit_storage_error)?;
        }
        Ok(())
    }
}

impl Drop for DerivedWriteAccess {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            let _ = transaction.abort();
        }
    }
}

enum RedbWriteOwnership {
    Direct { _lease: ExclusiveLease },
    Epoch(Box<RedbDurabilityEpoch>),
    ServiceAudit { _lease: ExclusiveLease },
}

/// Closed standard-profile durability epoch.
///
/// The value owns the database mutation lease and every unpublished command
/// result. Dropping an epoch that owns unpublished state before a successful
/// tail fence permanently fences this process handle and deliberately leaves
/// the predecessor read frontier installed. A pristine epoch may be dropped to
/// cancel command evaluation after a proven rollback because it has no private
/// state to publish.
pub struct RedbDurabilityEpoch {
    shared: Arc<SharedRedb>,
    lease: Option<ExclusiveLease>,
    applied: Vec<riffdb_storage_api::UnpublishedAuditedBatchV1>,
    transient_deltas: Vec<TransientIndexDelta>,
    command_count: usize,
    last_sequence: Option<riffdb_types::CommitSequence>,
    semantic_bytes: usize,
    reserved_encoded_bytes: usize,
    journal_mutations: crate::journal::JournalMutationBuffer,
    journal_mutation_groups: usize,
    composite_predecessor: Arc<crate::composite_view::RedbCompositeReadView>,
    composite_stage: Option<crate::composite_view::RedbCompositeMutationStage>,
    completed: bool,
}

struct JournalRuntime {
    changelog_history: Option<riffdb_storage_api::ChangelogHistoryStateV3>,
    lane: Arc<crate::journal::JournalLane>,
    database_id: DatabaseId,
    last_sequence: Option<CommitSequence>,
    last_administration_sequence: Option<AdministrationSequence>,
    last_hash: [u8; 32],
    published_sequence: Option<CommitSequence>,
    published_administration_sequence: Option<AdministrationSequence>,
    published_hash: [u8; 32],
    suffix_transitions: usize,
    suffix_commands: usize,
    suffix_audits: usize,
    suffix_bytes: usize,
    suffix_physical_bytes: usize,
    suffix_frames: Vec<ValidatedCheckpointFrame>,
    unpublished_transitions: usize,
    unpublished_commands: usize,
    unpublished_audits: usize,
    unpublished_bytes: usize,
    reanchor_required: bool,
}

#[derive(Clone)]
struct ValidatedCheckpointFrame {
    changelog_binding: Option<riffdb_storage_api::AuthoritativeTransactionBindingV3>,
    database_id: DatabaseId,
    predecessor_sequence: Option<CommitSequence>,
    covered_sequence: Option<CommitSequence>,
    predecessor_administration_sequence: Option<AdministrationSequence>,
    covered_administration_sequence: Option<AdministrationSequence>,
    previous_hash: [u8; 32],
    frame_hash: [u8; 32],
    transition_count: u16,
    command_count: u16,
    audit_count: u16,
    encoded: crate::journal::EncodedJournalFrame,
    mutations: Arc<[riffdb_storage_api::CompositeMutationV1]>,
}

#[derive(Default)]
struct PublicationQueue {
    pending: VecDeque<PendingPublication>,
}

#[derive(Clone)]
struct PublicationTicket {
    id: u64,
    result: Arc<Mutex<Option<Result<(), StorageError>>>>,
}

impl PublicationTicket {
    fn result(&self) -> Result<Option<()>, StorageError> {
        self.result
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?
            .clone()
            .transpose()
    }

    fn complete(&self, result: Result<(), StorageError>) {
        if let Ok(mut slot) = self.result.lock()
            && slot.is_none()
        {
            *slot = Some(result);
        }
    }
}

struct PendingPublication {
    ticket: PublicationTicket,
    receipt: crate::journal::JournalFenceReceipt,
    payload: PendingPublicationPayload,
    registered_at: Instant,
}

enum PendingPublicationPayload {
    Command(CommandPublication),
    ServiceAudit(ServiceAuditPublication),
}

struct CommandPublication {
    successor: Arc<CheckpointRoot>,
    transient_deltas: Vec<TransientIndexDelta>,
    command_count: usize,
    encoded_bytes: usize,
    first_sequence: CommitSequence,
    last_sequence: CommitSequence,
    predecessor_administration_sequence: Option<AdministrationSequence>,
    last_administration_sequence: Option<AdministrationSequence>,
    audit_count: usize,
    composite_predecessor: Arc<crate::composite_view::RedbCompositeReadView>,
    composite_successor: Arc<crate::composite_view::RedbCompositeReadView>,
}

struct ServiceAuditPublication {
    successor: Arc<CheckpointRoot>,
    transition_count: usize,
    encoded_bytes: usize,
    predecessor_sequence: Option<CommitSequence>,
    covered_sequence: Option<CommitSequence>,
    predecessor_administration_sequence: Option<AdministrationSequence>,
    covered_administration_sequence: Option<AdministrationSequence>,
    composite_predecessor: Arc<crate::composite_view::RedbCompositeReadView>,
    composite_successor: Arc<crate::composite_view::RedbCompositeReadView>,
}

#[derive(Clone, Copy)]
struct JournalCapacityCharge {
    checkpoint_transitions: usize,
    checkpoint_bytes: usize,
    suffix_transitions: usize,
    suffix_bytes: usize,
    suffix_physical_bytes: usize,
    unpublished_transitions: usize,
    unpublished_bytes: usize,
}

impl JournalCapacityCharge {
    fn admits(self, transitions: usize, bytes: usize, physical_bytes: usize) -> bool {
        transitions <= crate::journal::MAX_JOURNAL_TRANSITIONS
            && bytes <= crate::journal::MAX_JOURNAL_FRAME_BYTES
            && self
                .checkpoint_transitions
                .saturating_add(self.suffix_transitions)
                .saturating_add(transitions)
                <= crate::journal::MAX_JOURNAL_SUFFIX_TRANSITIONS
            && self
                .checkpoint_bytes
                .saturating_add(self.suffix_bytes)
                .saturating_add(bytes)
                <= crate::journal::MAX_JOURNAL_SUFFIX_BYTES
            && self.suffix_physical_bytes.saturating_add(physical_bytes)
                <= crate::journal::EXTENT_DATA_BYTES
            && self.unpublished_transitions.saturating_add(transitions)
                <= crate::journal::MAX_JOURNAL_TRANSITIONS
            && self.unpublished_bytes.saturating_add(bytes)
                <= crate::journal::MAX_JOURNAL_FRAME_BYTES
    }
}

const JOURNAL_CHECKPOINT_START_TRANSITIONS: usize =
    crate::journal::MAX_JOURNAL_SUFFIX_TRANSITIONS / 2;
const JOURNAL_CHECKPOINT_START_BYTES: usize = crate::journal::MAX_JOURNAL_SUFFIX_BYTES / 2;

fn journal_checkpoint_start_physical_bytes() -> Option<usize> {
    crate::journal::EXTENT_DATA_BYTES.checked_sub(crate::journal::extent_frame_bytes(
        crate::journal::MAX_JOURNAL_FRAME_BYTES,
    )?)
}

#[derive(Clone)]
struct JournalCheckpointBatch {
    database_id: DatabaseId,
    checkpoint_sequence: Option<CommitSequence>,
    checkpoint_administration_sequence: Option<AdministrationSequence>,
    checkpoint_hash: [u8; 32],
    last_sequence: Option<CommitSequence>,
    last_administration_sequence: Option<AdministrationSequence>,
    last_hash: [u8; 32],
    transition_count: usize,
    command_count: usize,
    audit_count: usize,
    encoded_bytes: usize,
    frames: Vec<ValidatedCheckpointFrame>,
}

struct AsyncJournalCheckpoint {
    batch: JournalCheckpointBatch,
    covered_view: Arc<crate::composite_view::RedbCompositeReadView>,
    completion: Option<std::sync::mpsc::Receiver<Result<(), StorageError>>>,
    result: Option<Result<(), StorageError>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg(test)]
pub(crate) struct CompositeReadAcquireProfileV1 {
    pub(crate) outer_lock_ns: u64,
    pub(crate) view_capture_ns: u64,
}

#[cfg(test)]
fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

pub struct RedbSubmittedCommandFence {
    shared: Arc<SharedRedb>,
    receipt: Option<crate::journal::JournalFenceReceipt>,
    successor: Arc<CheckpointRoot>,
    applied: Vec<riffdb_storage_api::UnpublishedAuditedBatchV1>,
    transient_deltas: Vec<TransientIndexDelta>,
    command_count: usize,
    last_sequence: CommitSequence,
    predecessor_administration_sequence: Option<AdministrationSequence>,
    last_administration_sequence: Option<AdministrationSequence>,
    journaled: bool,
    // A checkpoint may finish while this frame remains unpublished. Draining
    // this fence makes the runtime quiescent so the next writer turn can
    // install that checkpoint before consuming its reserved journal headroom.
    pipeline_drain_required: bool,
    publication_ticket: Option<PublicationTicket>,
    completed: bool,
}

pub(crate) struct RedbSubmittedServiceAuditFence {
    shared: Arc<SharedRedb>,
    receipt: Option<crate::journal::JournalFenceReceipt>,
    results: Option<Vec<riffdb_storage_api::ServiceAuditAppendResult>>,
    covered_sequence: Option<CommitSequence>,
    covered_administration_sequence: Option<AdministrationSequence>,
    publication_ticket: PublicationTicket,
    completed: bool,
}

#[derive(Clone)]
pub(crate) enum RedbReadAccess {
    Current(Arc<CheckpointRoot>),
    Durable(Arc<CheckpointRoot>),
    Composite(Arc<crate::composite_view::RedbCompositeReadView>),
}

impl RedbReadAccess {
    #[allow(
        dead_code,
        reason = "WP-487 introduces the composite root; WP-488 publishes it"
    )]
    pub(crate) fn into_shared(self) -> Result<Arc<CheckpointRoot>, StorageError> {
        match self {
            Self::Current(root) => Ok(root),
            Self::Durable(transaction) => Ok(transaction),
            Self::Composite(_) => Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
    }

    pub(crate) fn read_value(
        &self,
        table: crate::journal::JournalTable,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        match self {
            Self::Current(root) | Self::Durable(root) => {
                root.read_value(table, key).map_err(journal_io_error)
            }
            Self::Composite(view) => view.resolve_point(table.composite(), key),
        }
    }

    /// Cached byte-table handle for the two non-overlay read variants.
    ///
    /// `Composite` returns before reaching this: every overlay-bearing table
    /// must merge the checkpoint with the published suffix, which the callers
    /// above do first. A logical table with no byte definition is the same
    /// invariant violation it has always been, not an empty scan.
    fn cached_byte_table(
        &self,
        table: crate::journal::JournalTable,
    ) -> Result<&redb::ReadOnlyTable<&'static [u8], &'static [u8]>, StorageError> {
        match self {
            Self::Current(root) | Self::Durable(root) => root
                .journal_byte_table(table)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .map_err(table_error),
            Self::Composite(_) => Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
    }

    pub(crate) fn read_range(
        &self,
        table: crate::journal::JournalTable,
        start_inclusive: &[u8],
        end_exclusive: &[u8],
        max_rows: usize,
    ) -> Result<Vec<riffdb_storage_api::CompositeRow>, StorageError> {
        self.read_range_to(table, start_inclusive, Some(end_exclusive), max_rows)
    }

    /// Overlay-aware forward scan whose upper bound may be open.
    ///
    /// ADR-0104 section 2 requires every operational scan of a table named by
    /// [`crate::journal::JournalTable`] to merge the captured checkpoint with
    /// the published overlay. `Deref` exposes the checkpoint root alone, so a
    /// caller that needs an open-ended page must use this method rather than
    /// iterating the dereferenced table.
    pub(crate) fn read_range_to(
        &self,
        table: crate::journal::JournalTable,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        max_rows: usize,
    ) -> Result<Vec<riffdb_storage_api::CompositeRow>, StorageError> {
        if max_rows == 0 {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Self::Composite(view) = self {
            return view
                .merge_bounded(
                    table.composite(),
                    start_inclusive,
                    end_exclusive,
                    max_rows,
                    max_rows.saturating_add(riffdb_storage_api::MAX_COMPOSITE_OVERLAY_TRANSITIONS),
                )
                .map(|page| page.rows().to_vec());
        }
        self.cached_byte_table(table)?
            .range::<&[u8]>((
                std::ops::Bound::Included(start_inclusive),
                end_exclusive.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
            ))
            .map_err(precommit_storage_error)?
            .take(max_rows)
            .map(|row| {
                row.map(|(key, value)| {
                    (
                        key.value().to_vec().into_boxed_slice(),
                        value.value().to_vec().into_boxed_slice(),
                    )
                })
                .map_err(precommit_storage_error)
            })
            .collect()
    }

    pub(crate) fn read_range_reverse(
        &self,
        table: crate::journal::JournalTable,
        start_inclusive: &[u8],
        end_exclusive: &[u8],
        max_rows: usize,
    ) -> Result<Vec<riffdb_storage_api::CompositeRow>, StorageError> {
        if max_rows == 0 {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Self::Composite(view) = self {
            return view
                .merge_bounded_reverse(
                    table.composite(),
                    start_inclusive,
                    Some(end_exclusive),
                    max_rows,
                    max_rows.saturating_add(riffdb_storage_api::MAX_COMPOSITE_OVERLAY_TRANSITIONS),
                )
                .map(|page| page.rows().to_vec());
        }
        self.cached_byte_table(table)?
            .range::<&[u8]>((
                std::ops::Bound::Included(start_inclusive),
                std::ops::Bound::Excluded(end_exclusive),
            ))
            .map_err(precommit_storage_error)?
            .rev()
            .take(max_rows)
            .map(|row| {
                row.map(|(key, value)| {
                    (
                        key.value().to_vec().into_boxed_slice(),
                        value.value().to_vec().into_boxed_slice(),
                    )
                })
                .map_err(precommit_storage_error)
            })
            .collect()
    }

    pub(crate) fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
        crate::reads::read_snapshot_head(self)
    }

    pub(crate) fn administration_frontier(
        &self,
    ) -> Result<Option<AdministrationSequence>, StorageError> {
        match self {
            Self::Current(transaction) => read_administration_tail(transaction),
            Self::Durable(transaction) => read_administration_tail(transaction),
            Self::Composite(view) => Ok(view.overlay().published_administration()),
        }
    }

    pub(crate) fn checkpoint_application_frontier(&self) -> Option<CommitSequence> {
        match self {
            Self::Composite(view) => view.overlay().checkpoint().application_frontier(),
            Self::Current(_) | Self::Durable(_) => self.application_frontier().ok().flatten(),
        }
    }
}

pub(crate) enum RedbIndexedReadLease {
    Direct { _lease: ExclusiveLease },
    DurableEpoch,
}

impl Deref for RedbReadAccess {
    type Target = ReadTransaction;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Current(root) | Self::Durable(root) => root.transaction(),
            // Only non-overlay tables may use `Deref`. Every table named by
            // `JournalTable` must go through the explicit point/range methods.
            Self::Composite(view) => view.checkpoint_root().transaction(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LayoutState {
    Empty,
    Initialized,
    InitializedWithoutValidatedPrefixEntityHeads,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistryMigration {
    Current,
    V3Activation,
    EventRoute,
    EventReferencesThenGenerations,
    Generations,
    HistoryIncarnation,
    AuditRequestIndex,
    EntityReference,
    ContractMigration,
    ValidatedPrefixCheckpoint,
    RetentionWatermark,
    ReactiveConsumers,
    ApplicationInstallationCampaign,
    EntityTransitions,
    ApplicationExportOperation,
    VectorEvidence,
    VectorObservations,
    VectorHealthObservations,
    VectorProjectionControls,
    CleanCloseLifecycle,
    ColumnarProjectionControls,
}

const FORMAT_MIGRATION_MAX_ROWS: usize = 500;
const FORMAT_MIGRATION_MAX_BYTES: usize = 4 * 1024 * 1024;
const PRE_EVENT_REFERENCE_REGISTRY_DIGEST: [u8; 32] = [
    0x79, 0xc2, 0xc8, 0x65, 0x27, 0xe0, 0xb8, 0x3f, 0x67, 0xed, 0xe5, 0xa7, 0x4a, 0xaf, 0x75, 0x0a,
    0xc1, 0x94, 0x38, 0xd9, 0xc9, 0x0e, 0x45, 0xe0, 0xcb, 0x7b, 0x4f, 0x18, 0xe2, 0xdd, 0xc9, 0x9e,
];
pub(crate) const PRE_INDEX_GENERATION_REGISTRY_DIGEST: [u8; 32] = [
    0x0f, 0xab, 0x09, 0x09, 0x1b, 0x8f, 0x56, 0xc3, 0xbe, 0xb9, 0x3a, 0x05, 0x75, 0x56, 0xfb, 0x3d,
    0x12, 0x83, 0xa0, 0x77, 0x55, 0xa9, 0x20, 0xd3, 0xe4, 0xa9, 0x73, 0x04, 0xd8, 0x21, 0x40, 0xb3,
];
/// Registry digest at the history-incarnation branch base (pre-fence current).
pub(crate) const PRE_HISTORY_INCARNATION_REGISTRY_DIGEST: [u8; 32] = [
    0x25, 0xbd, 0x75, 0xfe, 0x14, 0xf1, 0xd7, 0x58, 0x60, 0x16, 0xe5, 0xa6, 0x12, 0x32, 0xd8, 0xc5,
    0x8f, 0x15, 0xcf, 0xe8, 0x38, 0xc9, 0x38, 0xce, 0x2a, 0x57, 0xda, 0xa4, 0x15, 0x73, 0x62, 0xa9,
];
/// Registry digest at the audit-request-index branch base (post-F current).
pub(crate) const PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST: [u8; 32] = [
    0xfe, 0xfb, 0x86, 0xac, 0xe8, 0x2e, 0x36, 0xc6, 0x74, 0x9f, 0x22, 0xc7, 0xb7, 0xec, 0x4c, 0x05,
    0x15, 0xa5, 0x1a, 0x14, 0xaa, 0x7d, 0xe2, 0xc2, 0x1a, 0x84, 0xee, 0x4f, 0xe0, 0x8a, 0xc2, 0xa2,
];
/// Registry digest immediately before partition event routes became durable.
pub(crate) const PRE_EVENT_ROUTE_REGISTRY_DIGEST: [u8; 32] = [
    0xe9, 0x53, 0xd2, 0xc4, 0x9f, 0x74, 0xe0, 0x28, 0xc1, 0xae, 0xc7, 0x1e, 0xed, 0xf6, 0x23, 0x14,
    0x62, 0x3a, 0xb9, 0x5c, 0xf5, 0x33, 0xa2, 0xd6, 0x36, 0xcc, 0x6d, 0x70, 0x08, 0x42, 0x32, 0x4f,
];
/// Registry digest immediately before authoritative entity references became
/// durable (the registry with event routes, before `StoredCommitRecordV3`).
pub(crate) const PRE_ENTITY_REFERENCE_REGISTRY_DIGEST: [u8; 32] = [
    0xfb, 0x52, 0x21, 0xd7, 0x9c, 0xd8, 0x29, 0xd3, 0x0a, 0x44, 0x78, 0x88, 0xca, 0xf8, 0x1d, 0xb8,
    0x51, 0xcd, 0x4e, 0x77, 0xb7, 0xdc, 0xb3, 0x94, 0xe9, 0x86, 0x39, 0x7d, 0x1a, 0x71, 0x2b, 0x4d,
];
/// Registry digest immediately before durable contract-migration records became writable.
pub(crate) const PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST: [u8; 32] = [
    0x5d, 0x79, 0x8d, 0x58, 0xec, 0x21, 0x95, 0x11, 0x3e, 0x51, 0x73, 0x88, 0x97, 0xbb, 0xc5, 0x3b,
    0x95, 0x96, 0xc4, 0xa1, 0x2c, 0x4e, 0x05, 0x4f, 0x32, 0xca, 0xa3, 0x67, 0x9e, 0x2d, 0x46, 0xe5,
];
/// Registry digest of the 44-schema registry immediately before the validated-prefix
/// checkpoint record became durable (frozen PRE for this additive schema).
pub(crate) const PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST: [u8; 32] = [
    0xe1, 0x59, 0x2f, 0xba, 0x8c, 0x33, 0x8a, 0xee, 0x4e, 0xd7, 0x17, 0x8b, 0x6a, 0x09, 0xbb, 0xcf,
    0x88, 0x26, 0x7e, 0x5c, 0xd4, 0x33, 0xfa, 0x23, 0x31, 0x19, 0x9c, 0xaa, 0x3c, 0xd2, 0xe8, 0xcd,
];
/// Registry digest of the 45-schema registry immediately before retention watermark
/// records and the checkpoint watermark binding (frozen PRE for RT-B).
pub(crate) const PRE_RETENTION_WATERMARK_REGISTRY_DIGEST: [u8; 32] = [
    0x99, 0x13, 0x0d, 0x68, 0x02, 0x71, 0x1b, 0x38, 0xe8, 0xda, 0x85, 0xc2, 0x04, 0x39, 0x60, 0xf3,
    0x62, 0x04, 0x7f, 0x41, 0xc2, 0xd8, 0x0a, 0x4d, 0x88, 0xff, 0xf8, 0x2c, 0x9c, 0xfc, 0x46, 0x69,
];
/// Registry digest immediately before reactive modules, durable consumers, and
/// service-audit V2 became current in WP-417.
pub(crate) const PRE_WP417_REACTIVE_CONSUMER_REGISTRY_DIGEST: [u8; 32] = [
    0x39, 0x5a, 0x7f, 0x77, 0xcf, 0x3a, 0x95, 0x52, 0x12, 0x95, 0x76, 0x8d, 0x57, 0xbd, 0x82, 0x27,
    0xa1, 0x5a, 0x9d, 0xd8, 0x99, 0x28, 0xcc, 0xf9, 0x81, 0x9e, 0x79, 0x41, 0x12, 0x1e, 0x6e, 0x21,
];
/// Registry digest immediately before durable application-installation
/// campaigns became current in WP-568.
pub(crate) const PRE_APPLICATION_INSTALLATION_REGISTRY_DIGEST: [u8; 32] = [
    0xe6, 0x50, 0x78, 0x74, 0xb8, 0x90, 0x77, 0x1f, 0xe7, 0x1c, 0xd6, 0xe2, 0x73, 0x08, 0x2d, 0xc3,
    0xdd, 0x22, 0xd5, 0xd3, 0x4d, 0xa4, 0x0a, 0x4e, 0x47, 0x11, 0x1f, 0xfa, 0x39, 0xaf, 0xec, 0x24,
];
/// Registry digest immediately before delete-aware entity transitions became current.
pub(crate) const PRE_ENTITY_TRANSITIONS_REGISTRY_DIGEST: [u8; 32] = [
    0x2f, 0x0c, 0x23, 0xfd, 0x25, 0x65, 0x64, 0x42, 0xcf, 0x6a, 0x43, 0x8b, 0x39, 0x09, 0x5b, 0x92,
    0xa2, 0xf9, 0x56, 0xcd, 0x89, 0x8b, 0x95, 0x27, 0xde, 0x8f, 0x56, 0xa9, 0xbb, 0x08, 0x72, 0xa7,
];
/// Registry digest immediately before durable application-export operations
/// became current in WP-575.
pub(crate) const PRE_APPLICATION_EXPORT_REGISTRY_DIGEST: [u8; 32] = [
    0x6e, 0xb5, 0x25, 0xfe, 0xdb, 0x4d, 0x6a, 0x17, 0xd0, 0x31, 0x87, 0x16, 0x4f, 0x03, 0xa5, 0x2c,
    0x33, 0x95, 0x4a, 0x8c, 0x14, 0x5a, 0x00, 0xd3, 0x37, 0xa0, 0xe7, 0x9c, 0x49, 0xa5, 0x9d, 0xd3,
];
/// Registry digest immediately before authoritative vector evidence became writable.
pub(crate) const PRE_VECTOR_EVIDENCE_REGISTRY_DIGEST: [u8; 32] = [
    0xdb, 0x0b, 0x95, 0x3f, 0xb7, 0x11, 0xa8, 0xc9, 0x13, 0x51, 0x9d, 0x91, 0xa0, 0xc0, 0x59, 0xde,
    0x73, 0x66, 0xb8, 0xd1, 0x28, 0xd5, 0xee, 0x7f, 0x88, 0xd3, 0x38, 0x3a, 0x28, 0x80, 0xaf, 0x81,
];
/// Registry digest immediately before authoritative vector observations became writable.
pub(crate) const PRE_VECTOR_OBSERVATION_REGISTRY_DIGEST: [u8; 32] = [
    0x4f, 0xa3, 0xad, 0xb5, 0x26, 0x57, 0x49, 0x1f, 0xde, 0x9f, 0x97, 0xb0, 0x90, 0xa7, 0x40, 0xe3,
    0x20, 0xe5, 0x19, 0x12, 0x89, 0xf5, 0xe6, 0x98, 0x0a, 0xc6, 0x61, 0x09, 0xb3, 0x9a, 0x9d, 0x5b,
];
/// Registry digest immediately before the lineage-wide vector-health
/// observation became writable.
pub(crate) const PRE_VECTOR_HEALTH_OBSERVATION_REGISTRY_DIGEST: [u8; 32] = [
    0x6b, 0x17, 0x4a, 0x49, 0xe8, 0x30, 0xcd, 0x9c, 0x05, 0x7a, 0xef, 0xd7, 0x17, 0x94, 0x92, 0x7c,
    0xc9, 0x99, 0xa3, 0xa7, 0x13, 0xdb, 0x62, 0x75, 0x71, 0x21, 0x3c, 0x56, 0x96, 0xac, 0xc5, 0x72,
];
/// Registry digest immediately before vector projection lifecycle controls
/// became authoritative and retention-visible.
pub(crate) const PRE_VECTOR_PROJECTION_CONTROL_REGISTRY_DIGEST: [u8; 32] = [
    0x84, 0x54, 0x2f, 0x62, 0xcf, 0xa3, 0x45, 0x05, 0xd7, 0x88, 0xe8, 0xfb, 0x82, 0x5c, 0xd4, 0x24,
    0xea, 0xcb, 0x1b, 0x49, 0x5b, 0xda, 0x5b, 0x53, 0xf9, 0x53, 0xf6, 0x68, 0x96, 0x77, 0x33, 0x37,
];
/// Sole predecessor registry accepted by ADR-0157 before the additive
/// clean-close lifecycle record became readable and writable.
pub(crate) const PRE_CLEAN_CLOSE_LIFECYCLE_REGISTRY_DIGEST: [u8; 32] = [
    0x55, 0xb6, 0x04, 0x06, 0xf6, 0x56, 0x87, 0xad, 0x6d, 0x5b, 0x16, 0x7f, 0xd4, 0x28, 0x16, 0x00,
    0x39, 0x90, 0x10, 0x25, 0x8f, 0x75, 0x2a, 0xb1, 0x41, 0x28, 0xb4, 0x31, 0x56, 0xe6, 0x8e, 0xcc,
];
/// Registry digest immediately before common schema-bound columnar controls
/// became writable. This is the complete WP-688 clean-close registry.
pub(crate) const PRE_COLUMNAR_PROJECTION_CONTROL_REGISTRY_DIGEST: [u8; 32] = [
    0x71, 0x3f, 0x2a, 0xab, 0xc2, 0x42, 0xd6, 0xd2, 0x26, 0x3f, 0xd2, 0x76, 0x75, 0x58, 0x64, 0x53,
    0xab, 0x41, 0xc7, 0x54, 0xae, 0xf8, 0x4b, 0x48, 0xd4, 0x23, 0x30, 0x60, 0xfe, 0x03, 0xa1, 0x31,
];

/// Last observed redb repair progress in basis points (0..=10_000), for recovery telemetry.
static LAST_REPAIR_PROGRESS_BPS: AtomicU64 = AtomicU64::new(0);

/// Returns the last observed redb repair progress in basis points (0..=10_000).
#[must_use]
pub fn last_repair_progress_basis_points() -> u64 {
    LAST_REPAIR_PROGRESS_BPS.load(Ordering::Relaxed)
}

/// Sentinel for [`reset_last_repair_progress_for_tests`]: never produced by the
/// repair callback, so "changed from sentinel" proves the callback fired even
/// at 0% progress.
#[doc(hidden)]
pub const REPAIR_PROGRESS_SENTINEL: u64 = u64::MAX;

/// Resets the process-global repair observation to the sentinel so a test can
/// assert the callback fired for ITS open (the static is process-global and
/// otherwise indistinguishable from an earlier in-process repair).
#[doc(hidden)]
pub fn reset_last_repair_progress_for_tests() {
    LAST_REPAIR_PROGRESS_BPS.store(REPAIR_PROGRESS_SENTINEL, Ordering::Relaxed);
}

/// The default publication observer: a port that observes nothing.
fn default_changelog_port() -> Arc<dyn riffdb_storage_api::ChangelogPublicationPort> {
    Arc::new(riffdb_storage_api::NoChangelogPublicationPort)
}

fn format_preflight_storage_error(error: RedbDurableFormatPreflightError) -> StorageError {
    let kind = match error {
        RedbDurableFormatPreflightError::Marker(
            riffdb_storage_api::DurableFormatMarkerError::Malformed
            | riffdb_storage_api::DurableFormatMarkerError::ChecksumMismatch,
        ) => StorageErrorKind::CorruptData,
        RedbDurableFormatPreflightError::Marker(
            riffdb_storage_api::DurableFormatMarkerError::UnknownVersion
            | riffdb_storage_api::DurableFormatMarkerError::ManifestMismatch,
        )
        | RedbDurableFormatPreflightError::Unsupported(_) => StorageErrorKind::IncompatibleFormat,
        RedbDurableFormatPreflightError::Unavailable
        | RedbDurableFormatPreflightError::AmbiguousInventory => StorageErrorKind::Unavailable,
    };
    storage_error(kind)
}

impl RedbStore {
    /// Opens an existing redb file or creates an empty redb container.
    ///
    /// Authoritative application writes use [`RedbCommitProfile::Standard`].
    /// Initialization and migration retain their independently hardened
    /// durability boundary.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_inner(
            path.as_ref(),
            RedbCommitProfile::Standard,
            None,
            default_changelog_port(),
            None,
        )
    }

    /// Opens a database over simulated storage media: a redb engine backend
    /// plus the journal media instance that must serve the format preflight,
    /// marker publication, and every journal side-file operation of this
    /// handle — so a simulated open is fully simulated from the first
    /// filesystem touch (ADR-0113 Phase 1).
    ///
    /// Hidden test surface following the `open_with_test_controller`
    /// convention; production opens never construct media and remain
    /// byte-identical to the pre-seam behavior.
    #[doc(hidden)]
    pub fn open_with_storage_media(
        path: impl AsRef<Path>,
        media: RedbStorageMedia,
    ) -> Result<Self, StorageError> {
        Self::open_inner(
            path.as_ref(),
            RedbCommitProfile::Standard,
            None,
            default_changelog_port(),
            Some(media),
        )
    }

    /// Opens a database with an explicit process-wide application commit profile.
    pub fn open_with_commit_profile(
        path: impl AsRef<Path>,
        application_commit_profile: RedbCommitProfile,
    ) -> Result<Self, StorageError> {
        Self::open_inner(
            path.as_ref(),
            application_commit_profile,
            None,
            default_changelog_port(),
            None,
        )
    }

    /// Opens a database with a changelog publication observer installed.
    ///
    /// This is the single construction site for the ADR-0100 emitter binding:
    /// the port is fixed for the life of the handle, so no later caller can
    /// re-point the publication edge at a different observer.
    pub fn open_with_changelog_publication_port(
        path: impl AsRef<Path>,
        application_commit_profile: RedbCommitProfile,
        changelog_port: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<Self, StorageError> {
        Self::open_inner(
            path.as_ref(),
            application_commit_profile,
            None,
            changelog_port,
            None,
        )
    }

    /// Opens a database with one closed process-test failpoint controller.
    #[doc(hidden)]
    pub fn open_with_test_controller(
        path: impl AsRef<Path>,
        controller: RedbTestController,
    ) -> Result<Self, StorageError> {
        Self::open_inner(
            path.as_ref(),
            RedbCommitProfile::Standard,
            Some(controller),
            default_changelog_port(),
            None,
        )
    }

    /// Opens a database with one explicit profile and process-test controller.
    #[doc(hidden)]
    pub fn open_with_test_controller_and_commit_profile(
        path: impl AsRef<Path>,
        application_commit_profile: RedbCommitProfile,
        controller: RedbTestController,
    ) -> Result<Self, StorageError> {
        Self::open_inner(
            path.as_ref(),
            application_commit_profile,
            Some(controller),
            default_changelog_port(),
            None,
        )
    }

    /// Opens with the production V3 observer and the existing process-test hooks.
    #[doc(hidden)]
    pub fn open_with_test_controller_and_changelog_publication_port(
        path: impl AsRef<Path>,
        application_commit_profile: RedbCommitProfile,
        controller: RedbTestController,
        changelog_port: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
    ) -> Result<Self, StorageError> {
        Self::open_inner(
            path.as_ref(),
            application_commit_profile,
            Some(controller),
            changelog_port,
            None,
        )
    }

    /// Opens an existing database over the real redb file backend with the
    /// closed external-kill barrier installed.
    ///
    /// This fixture-only seam delegates every operation to redb's real
    /// `FileBackend`. With a hardened two-phase command commit, the controller
    /// arms immediately before `WriteTransaction::commit`. The backend lets
    /// the first following `sync_data` durably synchronize redb's secondary
    /// slot, then blocks the second sync after the primary-header write. This
    /// is a first-party process-kill oracle, not simulated storage.
    #[cfg(feature = "test-fixtures")]
    #[doc(hidden)]
    pub fn open_with_external_kill_barrier(
        path: impl AsRef<Path>,
        application_commit_profile: RedbCommitProfile,
        controller: RedbTestController,
    ) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let format_preflight =
            preflight_durable_format_path(path).map_err(format_preflight_storage_error)?;
        if format_preflight != RedbDurableFormatPreflight::OpenCurrent {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let inner = redb::backends::FileBackend::new(file).map_err(database_error)?;
        let backend = ExternalKillBarrierFileBackend {
            inner,
            controller: controller.clone(),
        };
        Self::open_after_format_preflight(
            OpenMode::Source,
            path,
            application_commit_profile,
            Some(controller),
            default_changelog_port(),
            None,
            Some(Box::new(backend)),
            Arc::new(RealJournalMedia),
        )
    }

    fn open_inner(
        path: &Path,
        application_commit_profile: RedbCommitProfile,
        test_controller: Option<RedbTestController>,
        changelog_port: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
        media: Option<RedbStorageMedia>,
    ) -> Result<Self, StorageError> {
        let path = path.to_path_buf();
        let (engine_backend, journal_media) = match media {
            Some(media) => (Some(media.engine), media.journal),
            None => (None, Arc::new(RealJournalMedia) as Arc<dyn JournalMedia>),
        };
        let format_preflight =
            preflight_durable_format_path_with_media(journal_media.as_ref(), &path)
                .map_err(format_preflight_storage_error)?;
        if matches!(
            format_preflight,
            RedbDurableFormatPreflight::OfflineUpgradeRequired { .. }
        ) {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        Self::open_after_format_preflight(
            OpenMode::Source,
            &path,
            application_commit_profile,
            test_controller,
            changelog_port,
            (format_preflight == RedbDurableFormatPreflight::InitializeCurrent)
                .then_some(format_preflight),
            engine_backend,
            journal_media,
        )
    }

    pub(crate) fn open_for_durable_format_upgrade(
        path: &Path,
        format_preflight: RedbDurableFormatPreflight,
    ) -> Result<Self, StorageError> {
        if !matches!(
            format_preflight,
            RedbDurableFormatPreflight::OfflineUpgradeRequired { .. }
        ) || preflight_durable_format_path(path).map_err(format_preflight_storage_error)?
            != format_preflight
        {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        Self::open_after_format_preflight(
            OpenMode::Source,
            path,
            RedbCommitProfile::Hardened,
            None,
            default_changelog_port(),
            None,
            None,
            Arc::new(RealJournalMedia),
        )
    }

    fn open_derived_database(path: &Path, memory: bool) -> Result<Database, StorageError> {
        let mut sidecar_builder = Builder::new();
        let sidecar_cache = REDB_CACHE_SIZE_BYTES;
        sidecar_builder.set_cache_size(sidecar_cache);
        let database = if memory {
            sidecar_builder.create_with_backend(redb::backends::InMemoryBackend::new())
        } else {
            sidecar_builder.create(derived_store_path(path))
        }
        .map_err(database_error)?;
        let sidecar = database.begin_write().map_err(transaction_error)?;
        drop(sidecar.open_table(PROJECTION_STATE).map_err(table_error)?);
        drop(
            sidecar
                .open_table(PROJECTION_FRONTIER)
                .map_err(table_error)?,
        );
        drop(
            sidecar
                .open_table(PROJECTION_APPLIED)
                .map_err(table_error)?,
        );
        drop(
            sidecar
                .open_table(COLUMNAR_PROJECTION_CONTROLS)
                .map_err(table_error)?,
        );
        sidecar.commit().map_err(commit_error)?;
        Ok(database)
    }

    #[allow(clippy::too_many_arguments)]
    fn open_after_format_preflight(
        open_mode: OpenMode,
        path: &Path,
        application_commit_profile: RedbCommitProfile,
        test_controller: Option<RedbTestController>,
        changelog_port: Arc<dyn riffdb_storage_api::ChangelogPublicationPort>,
        initialize_marker_witness: Option<RedbDurableFormatPreflight>,
        engine_backend: Option<Box<dyn redb::StorageBackend>>,
        journal_media: Arc<dyn JournalMedia>,
    ) -> Result<Self, StorageError> {
        if let Some(witness) = initialize_marker_witness {
            publish_initialized_current_marker_with_media(journal_media.as_ref(), path, witness)
                .map_err(format_preflight_storage_error)?;
        }
        let repair_observed = Arc::new(AtomicBool::new(false));
        let repair_observed_callback = Arc::clone(&repair_observed);
        let mut builder = Builder::new();
        builder.set_cache_size(REDB_CACHE_SIZE_BYTES);
        builder.set_repair_callback(move |session| {
            repair_observed_callback.store(true, Ordering::Release);
            // Bounded progress telemetry only; do not enable quick_repair
            // (quick_repair forces two-phase commit, conflicting with Standard).
            let progress = session.progress();
            let basis_points = ((progress * 10_000.0) as u64).min(10_000);
            LAST_REPAIR_PROGRESS_BPS.store(basis_points, Ordering::Relaxed);
        });
        // The builder configuration above is shared by both arms; only the
        // storage medium differs (ADR-0113 backend-parameterized open).
        // Sidecar durability follows the primary path, not the backend
        // wrapper: a FileBackend kill-barrier still has to observe the
        // sidecar after process death (ADR-0240 two-store crash).
        let derived_in_memory = false;
        let database = match engine_backend {
            Some(backend) => builder.create_with_backend(DynStorageBackend(backend)),
            None => builder.create(path),
        }
        .map_err(database_error)?;
        // Source-mode construction cannot reinterpret attached follower state
        // as a writable primary. Refuse before locator installation, journal
        // recovery, or preparation workers. Promotion owns detachment.
        if let OpenMode::PrivateRestore(binding) = &open_mode {
            binding.validate(&database.begin_read().map_err(transaction_error)?)?;
        }
        let attached = crate::follower_lifecycle::is_attached(
            &database.begin_read().map_err(transaction_error)?,
        )?;
        if attached != (open_mode == OpenMode::Follower) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if open_mode == OpenMode::Follower {
            follower::validate_open(&database, path, journal_media.as_ref())?;
        }
        let command_segment_preparation =
            crate::command_segment_preparation::CommandSegmentPreparationPool::new(
                crate::command_segment_preparation::CommandSegmentPreparationPool::production_worker_count(),
            )
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let store = Self {
            shared: Arc::new(SharedRedb {
                derived_source_pins: Arc::default(),
                open_mode: open_mode.clone(),
                follower_namespace: Mutex::new(None),
                database,
                derived: std::sync::OnceLock::new(),
                derived_open: std::sync::Mutex::new(()),
                derived_in_memory,
                path: path.to_path_buf(),
                journal_media,
                application_commit_profile,
                mutation_gate: ExclusiveGate::default(),
                write_fenced: AtomicBool::new(matches!(open_mode, OpenMode::PrivateRestore(_))),
                engine_repaired_at_open: repair_observed.load(Ordering::Acquire),
                engine_initialized_at_open: initialize_marker_witness.is_some(),
                bounded_clean_startup: AtomicBool::new(false),
                durable_commit_epoch: AtomicU64::new(0),
                durable_root_publication: AtomicU64::new(0),
                durable_root_publication_wait: Mutex::new(()),
                durable_root_publication_changed: Condvar::new(),
                durable_read_frontier: RwLock::new(None),
                current_read_root: RwLock::new(None),
                current_read_root_live: AtomicBool::new(false),
                composite_publication: RwLock::new(None),
                private_composite_frontier: Mutex::new(None),
                publication_queue: Mutex::new(PublicationQueue::default()),
                next_publication_ticket: AtomicU64::new(1),
                journal_runtime: Mutex::new(None),
                journal_checkpoint: Mutex::new(None),
                command_segment_preparation,
                test_controller,
                transient_indexes: RwLock::new(TransientIndexState::Dormant),
                unpublished_command_indexes: Mutex::new(UnpublishedCommandIndexes::default()),
                startup_validation_clean: AtomicBool::new(false),
                checkpoint_write_failures: AtomicU64::new(0),
                checkpoint_ignored: [(); 10].map(|()| AtomicU64::new(0)),
                clean_close_write_failures: AtomicU64::new(0),
                clean_close_declined: [(); 11].map(|()| AtomicU64::new(0)),
                outbox_delivering_proven_absent: AtomicBool::new(false),
                transient_index_rebuilds: AtomicU64::new(0),
                transient_index_commit_rows: AtomicU64::new(0),
                fresh_locator_history_fallback_scans: AtomicU64::new(0),
                fresh_locator_history_fallback_rows: AtomicU64::new(0),
                retention_watermark: AtomicU64::new(0),
                terminal_execution_failure_rows: AtomicU64::new(0),
                checkpoint_count_rows_walked: AtomicU64::new(0),
                changelog_port,
            }),
        };
        if open_mode == OpenMode::Source {
            store.ensure_current_storage_format()?;
            store.install_command_locator_tables()?;
            store.recover_durability_journal()?;
        }
        store.cache_verified_retention_watermark()?;
        Ok(store)
    }

    /// Reconciles the redb checkpoint with a complete bounded durability
    /// journal before any startup validation or operational handle can exist.
    fn recover_durability_journal(&self) -> Result<(), StorageError> {
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        if classify_read_layout(&transaction)? == LayoutState::Empty {
            for journal in [
                crate::journal::journal_path(&self.shared.path),
                crate::journal::checkpoint_journal_path(&self.shared.path),
                crate::journal::spare_journal_path(&self.shared.path),
            ] {
                match self.shared.journal_media.try_exists(&journal) {
                    Ok(false) => {}
                    Ok(true) => return Err(storage_error(StorageErrorKind::CorruptData)),
                    Err(_) => return Err(storage_error(StorageErrorKind::Unavailable)),
                }
            }
            return Ok(());
        }
        let database_id = read_identity_from_read_transaction(&transaction)?;
        drop(transaction);
        let media = self.shared.journal_media.as_ref();
        let checkpoint = crate::journal::checkpoint_journal_path(&self.shared.path);
        if media
            .try_exists(&checkpoint)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
        {
            crate::journal::recover_journal_path_with_media(
                media,
                &self.shared.database,
                &checkpoint,
                database_id,
            )
            .map_err(recovery_journal_error)?;
            media
                .remove_file(&checkpoint)
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
            crate::journal::sync_parent_directory_with_media(media, &checkpoint)
                .map_err(journal_io_error)?;
        }
        crate::journal::recover_journal_with_media(
            media,
            &self.shared.database,
            &self.shared.path,
            database_id,
        )
        .map_err(recovery_journal_error)?;

        // The spare extent is never authoritative: it is only a preallocated
        // destination for the next active generation. Recovery first proves
        // the redb checkpoint plus checkpoint/active suffix chain, then may
        // discard any scratch name left by a crash between rotation renames.
        let spare = crate::journal::spare_journal_path(&self.shared.path);
        match media.remove_file(&spare) {
            Ok(()) => crate::journal::sync_parent_directory_with_media(media, &spare)
                .map_err(journal_io_error),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(storage_error(StorageErrorKind::Unavailable)),
        }
    }

    /// Loads and semantically verifies the retention watermark once per open
    /// handle; an undecodable watermark record refuses the open (fail closed
    /// — no read or validation path accepts one anyway).
    fn cache_verified_retention_watermark(&self) -> Result<(), StorageError> {
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        // A pre-initialization database has no META table yet: watermark 0.
        let sequence = match transaction.open_table(crate::layout::META) {
            Err(redb::TableError::TableDoesNotExist(_)) => 0,
            Err(error) => return Err(crate::error::table_error(error)),
            Ok(_) => crate::retention::load_watermark(&transaction)?
                .map(|watermark| watermark.watermark_sequence())
                .unwrap_or(0),
        };
        self.shared
            .retention_watermark
            .store(sequence, Ordering::Release);
        Ok(())
    }

    /// Installs the ADR-0165 command-derived locator tables. Idempotent.
    ///
    /// These tables add no durable message type — `StoredCommandLocatorV1` is
    /// already in the readable record registry — so the record-registry digest
    /// is unchanged and they ride no registry transition. That is also why they
    /// cannot be installed by the migration chain, which only advances when a
    /// digest changes: they are installed here instead.
    ///
    /// Takes a write transaction only when a table is genuinely absent, so a
    /// database that already has them pays one read transaction per open rather
    /// than a commit.
    fn install_command_locator_tables(&self) -> Result<(), StorageError> {
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        // A never-initialized database gets every table from the initialization
        // transaction. Creating them here first would make an empty layout look
        // initialized while META is still absent.
        if classify_read_layout(&transaction)? == LayoutState::Empty {
            return Ok(());
        }
        let present = transaction
            .open_table(crate::layout::IDEMPOTENCY_LOCATORS)
            .is_ok()
            && transaction
                .open_table(crate::layout::PROVENANCE_LOCATORS)
                .is_ok()
            && transaction
                .open_table(crate::layout::AUDIT_BY_REQUEST_LOCATORS)
                .is_ok();
        drop(transaction);
        if present {
            return Ok(());
        }
        let mut transaction = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        create_all_tables(&transaction).map_err(table_error)?;
        self.shared.commit_durable(transaction)
    }

    fn ensure_current_storage_format(&self) -> Result<(), StorageError> {
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let layout = classify_read_layout(&transaction)?;
        if layout == LayoutState::Empty {
            return Ok(());
        }
        let metadata = transaction.open_table(META).map_err(table_error)?;
        let encoded_format = metadata
            .get(META_FORMAT_VERSION)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let format = *decode_storage_format_version_v1(encoded_format.value())
            .map_err(crate::error::codec_error)?
            .value();
        let registry = metadata
            .get(META_RECORD_REGISTRY)
            .map_err(precommit_storage_error)?;
        let registry_migration = match format {
            StorageFormatVersion::V1 if registry.is_some() => {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            StorageFormatVersion::V2 => {
                let registry = registry
                    .as_ref()
                    .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
                let observed = decode_record_registry_v2(registry.value())
                    .map_err(crate::error::codec_error)?;
                if observed.value()
                    == &riffdb_storage_api::proto_codec::current_record_registry_digest()
                {
                    RegistryMigration::Current
                } else if observed.value() == &crate::changelog_v3_activation::PRE_V3_REGISTRY {
                    RegistryMigration::V3Activation
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST)
                {
                    RegistryMigration::EventRoute
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_EVENT_REFERENCE_REGISTRY_DIGEST)
                {
                    RegistryMigration::EventReferencesThenGenerations
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST)
                {
                    RegistryMigration::Generations
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST)
                {
                    RegistryMigration::HistoryIncarnation
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST)
                {
                    RegistryMigration::AuditRequestIndex
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST)
                {
                    RegistryMigration::EntityReference
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST)
                {
                    RegistryMigration::ContractMigration
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST)
                {
                    RegistryMigration::ValidatedPrefixCheckpoint
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST)
                {
                    RegistryMigration::RetentionWatermark
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_WP417_REACTIVE_CONSUMER_REGISTRY_DIGEST)
                {
                    RegistryMigration::ReactiveConsumers
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_APPLICATION_INSTALLATION_REGISTRY_DIGEST)
                {
                    RegistryMigration::ApplicationInstallationCampaign
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_ENTITY_TRANSITIONS_REGISTRY_DIGEST)
                {
                    RegistryMigration::EntityTransitions
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_APPLICATION_EXPORT_REGISTRY_DIGEST)
                {
                    RegistryMigration::ApplicationExportOperation
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_VECTOR_EVIDENCE_REGISTRY_DIGEST)
                {
                    RegistryMigration::VectorEvidence
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_VECTOR_OBSERVATION_REGISTRY_DIGEST)
                {
                    RegistryMigration::VectorObservations
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_VECTOR_HEALTH_OBSERVATION_REGISTRY_DIGEST)
                {
                    RegistryMigration::VectorHealthObservations
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_VECTOR_PROJECTION_CONTROL_REGISTRY_DIGEST)
                {
                    RegistryMigration::VectorProjectionControls
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_CLEAN_CLOSE_LIFECYCLE_REGISTRY_DIGEST)
                {
                    RegistryMigration::CleanCloseLifecycle
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_COLUMNAR_PROJECTION_CONTROL_REGISTRY_DIGEST)
                {
                    RegistryMigration::ColumnarProjectionControls
                } else {
                    return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                }
            }
            StorageFormatVersion::V1 => RegistryMigration::Current,
            _ => return Err(storage_error(StorageErrorKind::IncompatibleFormat)),
        };
        let install_export_ledger = export_ledger::needs_installation(
            &transaction,
            format == StorageFormatVersion::V1 || registry_migration != RegistryMigration::Current,
        )?;
        drop(registry);
        drop(encoded_format);
        drop(metadata);
        drop(transaction);

        if install_export_ledger {
            export_ledger::install_empty_legacy_table(&self.shared)?;
        }

        if registry_migration == RegistryMigration::EventRoute {
            migrate_event_routes(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST),
            )?;
        }

        if registry_migration == RegistryMigration::EventReferencesThenGenerations {
            migrate_event_reference_records(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_EVENT_REFERENCE_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST),
            )?;
        }
        if matches!(
            registry_migration,
            RegistryMigration::EventReferencesThenGenerations | RegistryMigration::Generations
        ) {
            migrate_partition_index_generations(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST),
            )?;
        }
        if matches!(
            registry_migration,
            RegistryMigration::EventReferencesThenGenerations
                | RegistryMigration::Generations
                | RegistryMigration::HistoryIncarnation
        ) {
            migrate_history_incarnation(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST),
            )?;
        }
        if matches!(
            registry_migration,
            RegistryMigration::EventReferencesThenGenerations
                | RegistryMigration::Generations
                | RegistryMigration::HistoryIncarnation
                | RegistryMigration::AuditRequestIndex
        ) {
            migrate_audit_request_index(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST),
            )?;
            migrate_event_routes(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST),
            )?;
        }
        if matches!(
            registry_migration,
            RegistryMigration::EventReferencesThenGenerations
                | RegistryMigration::Generations
                | RegistryMigration::HistoryIncarnation
                | RegistryMigration::AuditRequestIndex
                | RegistryMigration::EventRoute
                | RegistryMigration::EntityReference
        ) {
            migrate_commit_entity_references(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST),
            )?;
        }
        if matches!(
            registry_migration,
            RegistryMigration::EventReferencesThenGenerations
                | RegistryMigration::Generations
                | RegistryMigration::HistoryIncarnation
                | RegistryMigration::AuditRequestIndex
                | RegistryMigration::EventRoute
                | RegistryMigration::EntityReference
                | RegistryMigration::ContractMigration
        ) {
            install_contract_migration_tables(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST),
            )?;
        }
        if matches!(
            registry_migration,
            RegistryMigration::EventReferencesThenGenerations
                | RegistryMigration::Generations
                | RegistryMigration::HistoryIncarnation
                | RegistryMigration::AuditRequestIndex
                | RegistryMigration::EventRoute
                | RegistryMigration::EntityReference
                | RegistryMigration::ContractMigration
                | RegistryMigration::ValidatedPrefixCheckpoint
        ) {
            // No table install: the checkpoint is a meta-row only. Publish to the
            // pre-retention digest (not current); retention installs next.
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST),
            )?;
        }
        if matches!(
            registry_migration,
            RegistryMigration::EventReferencesThenGenerations
                | RegistryMigration::Generations
                | RegistryMigration::HistoryIncarnation
                | RegistryMigration::AuditRequestIndex
                | RegistryMigration::EventRoute
                | RegistryMigration::EntityReference
                | RegistryMigration::ContractMigration
                | RegistryMigration::ValidatedPrefixCheckpoint
                | RegistryMigration::RetentionWatermark
        ) {
            install_history_tombstones_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_WP417_REACTIVE_CONSUMER_REGISTRY_DIGEST),
            )?;
        }
        if matches!(
            registry_migration,
            RegistryMigration::EventReferencesThenGenerations
                | RegistryMigration::Generations
                | RegistryMigration::HistoryIncarnation
                | RegistryMigration::AuditRequestIndex
                | RegistryMigration::EventRoute
                | RegistryMigration::EntityReference
                | RegistryMigration::ContractMigration
                | RegistryMigration::ValidatedPrefixCheckpoint
                | RegistryMigration::RetentionWatermark
                | RegistryMigration::ReactiveConsumers
        ) {
            install_reactive_consumer_tables(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_WP417_REACTIVE_CONSUMER_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_APPLICATION_INSTALLATION_REGISTRY_DIGEST),
            )?;
        }
        if matches!(
            registry_migration,
            RegistryMigration::EventReferencesThenGenerations
                | RegistryMigration::Generations
                | RegistryMigration::HistoryIncarnation
                | RegistryMigration::AuditRequestIndex
                | RegistryMigration::EventRoute
                | RegistryMigration::EntityReference
                | RegistryMigration::ContractMigration
                | RegistryMigration::ValidatedPrefixCheckpoint
                | RegistryMigration::RetentionWatermark
                | RegistryMigration::ReactiveConsumers
                | RegistryMigration::ApplicationInstallationCampaign
        ) {
            install_application_installation_campaign_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_APPLICATION_INSTALLATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_ENTITY_TRANSITIONS_REGISTRY_DIGEST),
            )?;
        }

        if format == StorageFormatVersion::V2
            && (matches!(registry_migration, RegistryMigration::EntityTransitions)
                || observed_registry_digest(&self.shared)?
                    == SchemaHash::from_bytes(PRE_ENTITY_TRANSITIONS_REGISTRY_DIGEST))
        {
            migrate_entity_transition_heads(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_ENTITY_TRANSITIONS_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_APPLICATION_EXPORT_REGISTRY_DIGEST),
            )?;
        }
        if format == StorageFormatVersion::V2
            && (matches!(
                registry_migration,
                RegistryMigration::ApplicationExportOperation
            ) || observed_registry_digest(&self.shared)?
                == SchemaHash::from_bytes(PRE_APPLICATION_EXPORT_REGISTRY_DIGEST))
        {
            install_application_export_operation_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_APPLICATION_EXPORT_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_VECTOR_EVIDENCE_REGISTRY_DIGEST),
            )?;
        }
        if format == StorageFormatVersion::V2
            && (matches!(registry_migration, RegistryMigration::VectorEvidence)
                || observed_registry_digest(&self.shared)?
                    == SchemaHash::from_bytes(PRE_VECTOR_EVIDENCE_REGISTRY_DIGEST))
        {
            install_vector_evidence_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_VECTOR_EVIDENCE_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_VECTOR_OBSERVATION_REGISTRY_DIGEST),
            )?;
        }
        if format == StorageFormatVersion::V2
            && (matches!(registry_migration, RegistryMigration::VectorObservations)
                || observed_registry_digest(&self.shared)?
                    == SchemaHash::from_bytes(PRE_VECTOR_OBSERVATION_REGISTRY_DIGEST))
        {
            install_vector_observations_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_VECTOR_OBSERVATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_VECTOR_HEALTH_OBSERVATION_REGISTRY_DIGEST),
            )?;
        }
        if format == StorageFormatVersion::V2
            && (matches!(
                registry_migration,
                RegistryMigration::VectorHealthObservations
            ) || observed_registry_digest(&self.shared)?
                == SchemaHash::from_bytes(PRE_VECTOR_HEALTH_OBSERVATION_REGISTRY_DIGEST))
        {
            backfill_vector_observations_and_health(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_VECTOR_HEALTH_OBSERVATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_VECTOR_PROJECTION_CONTROL_REGISTRY_DIGEST),
            )?;
        }
        if format == StorageFormatVersion::V2
            && (matches!(
                registry_migration,
                RegistryMigration::VectorProjectionControls
            ) || observed_registry_digest(&self.shared)?
                == SchemaHash::from_bytes(PRE_VECTOR_PROJECTION_CONTROL_REGISTRY_DIGEST))
        {
            install_vector_projection_controls_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_VECTOR_PROJECTION_CONTROL_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_CLEAN_CLOSE_LIFECYCLE_REGISTRY_DIGEST),
            )?;
        }
        if format == StorageFormatVersion::V2
            && (matches!(registry_migration, RegistryMigration::CleanCloseLifecycle)
                || observed_registry_digest(&self.shared)?
                    == SchemaHash::from_bytes(PRE_CLEAN_CLOSE_LIFECYCLE_REGISTRY_DIGEST))
        {
            // Additive record-only transition. Absence is the frozen
            // predecessor state and deliberately earns no fast startup.
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_CLEAN_CLOSE_LIFECYCLE_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_COLUMNAR_PROJECTION_CONTROL_REGISTRY_DIGEST),
            )?;
        }
        if format == StorageFormatVersion::V2
            && (matches!(
                registry_migration,
                RegistryMigration::ColumnarProjectionControls
            ) || observed_registry_digest(&self.shared)?
                == SchemaHash::from_bytes(PRE_COLUMNAR_PROJECTION_CONTROL_REGISTRY_DIGEST))
        {
            install_columnar_projection_controls_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_COLUMNAR_PROJECTION_CONTROL_REGISTRY_DIGEST),
                crate::changelog_v3_activation::PRE_V3_REGISTRY,
            )?;
        }

        // ADR-0192 keeps tag 65 only as a bounded structural predecessor.
        // Its decoded values are discarded and never participate in common
        // control, selection, retention, health, or path resolution.
        if format == StorageFormatVersion::V2 {
            validate_inert_legacy_vector_controls(&self.shared)?;
        }

        // This additive proof table reuses the already-frozen entity-chain-head
        // encoding, so installing it does not rotate the record registry.
        // Predecessor databases start with an empty snapshot and perform one
        // full validation before a subsequent checkpoint populates it.
        if format == StorageFormatVersion::V2
            && layout == LayoutState::InitializedWithoutValidatedPrefixEntityHeads
        {
            install_validated_prefix_entity_heads_table(&self.shared)?;
        }

        let require_compact = format == StorageFormatVersion::V2;
        migrate_string_table(&self.shared, META, require_compact)?;
        for table in BYTE_TABLES {
            migrate_byte_table(&self.shared, table, require_compact)?;
        }
        if require_compact {
            return Ok(());
        }
        migrate_event_reference_records(&self.shared)?;

        let mut transaction = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut metadata = transaction.open_table(META).map_err(table_error)?;
        if metadata
            .get(META_RECORD_REGISTRY)
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let current_format = metadata
            .get(META_FORMAT_VERSION)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if *decode_storage_format_version_v1(current_format.value())
            .map_err(crate::error::codec_error)?
            .value()
            != StorageFormatVersion::V1
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        drop(current_format);
        let format = encode_storage_format_version_v1(StorageFormatVersion::V2)
            .map_err(crate::error::codec_error)?;
        let registry =
            encode_record_registry_v2(SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST))
                .map_err(crate::error::codec_error)?;
        metadata
            .insert(META_FORMAT_VERSION, format.as_bytes())
            .map_err(precommit_storage_error)?;
        metadata
            .insert(META_RECORD_REGISTRY, registry.as_bytes())
            .map_err(precommit_storage_error)?;
        drop(metadata);
        self.shared
            .before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        self.shared.commit_durable(transaction)?;
        self.shared
            .after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;

        // An empty index table needs no catalog-owned V1 row rewrite, so finish
        // the generation cutover immediately. Populated V1 index tables remain
        // on the pre-generation registry until catalog validation supplies the
        // authoritative partition for every migrated row.
        let read = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let indexes = read.open_table(SECONDARY_INDEXES).map_err(table_error)?;
        let index_table_is_empty = indexes.is_empty().map_err(precommit_storage_error)?;
        drop(indexes);
        drop(read);
        if index_table_is_empty {
            migrate_partition_index_generations(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST),
            )?;
            migrate_history_incarnation(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST),
            )?;
            migrate_audit_request_index(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST),
            )?;
            migrate_event_routes(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST),
            )?;
            migrate_commit_entity_references(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST),
            )?;
            install_contract_migration_tables(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST),
            )?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST),
            )?;
            install_history_tombstones_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_WP417_REACTIVE_CONSUMER_REGISTRY_DIGEST),
            )?;
            install_reactive_consumer_tables(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_WP417_REACTIVE_CONSUMER_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_APPLICATION_INSTALLATION_REGISTRY_DIGEST),
            )?;
            install_application_installation_campaign_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_APPLICATION_INSTALLATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_ENTITY_TRANSITIONS_REGISTRY_DIGEST),
            )?;
            migrate_entity_transition_heads(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_ENTITY_TRANSITIONS_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_APPLICATION_EXPORT_REGISTRY_DIGEST),
            )?;
            install_application_export_operation_table(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_APPLICATION_EXPORT_REGISTRY_DIGEST),
                crate::changelog_v3_activation::PRE_V3_REGISTRY,
            )?;
        }
        Ok(())
    }

    /// Returns the database path for an offline adapter after exclusivity is proven.
    #[allow(dead_code, reason = "WP-070 offline backup consumes the source path")]
    pub(crate) fn path(&self) -> &Path {
        &self.shared.path
    }

    pub(crate) fn acquire_mutation_lease(&self) -> Result<ExclusiveLease, StorageError> {
        self.shared.mutation_gate.acquire()
    }

    pub(crate) fn ensure_writable(&self) -> Result<(), StorageError> {
        if self.shared.open_mode != OpenMode::Source {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if self.shared.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(())
    }

    pub(crate) fn complete_partition_index_generation_migration(&self) -> Result<(), StorageError> {
        let observed = {
            let transaction = self
                .shared
                .database
                .begin_read()
                .map_err(transaction_error)?;
            let metadata = transaction.open_table(META).map_err(table_error)?;
            let encoded = metadata
                .get(META_RECORD_REGISTRY)
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            *decode_record_registry_v2(encoded.value())
                .map_err(crate::error::codec_error)?
                .value()
        };
        let current = riffdb_storage_api::proto_codec::current_record_registry_digest();
        // When the digest is already current, still repair legacy INDEX_EPOCHS
        // rows if needed, but gate the (expensive) full scan: empty table or a
        // durable one-shot repair marker is O(1) and conservatively correct.
        // INDEX_EPOCHS remains written by current paths, so emptiness alone is
        // insufficient once any generation rows exist.
        if observed == current || observed == crate::changelog_v3_activation::PRE_V3_REGISTRY {
            if index_epoch_rows_may_need_legacy_repair(&self.shared)? {
                migrate_partition_index_generations(&self.shared)?;
            }
            return Ok(());
        }
        if observed == SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST) {
            migrate_partition_index_generations(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST),
            )?;
        } else if observed == SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST) {
            // PRE_HISTORY_INCARNATION digest still needs row repair if a prior
            // cutover left legacy epoch rows behind a later digest bump.
            migrate_partition_index_generations(&self.shared)?;
        } else if observed != SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST)
            && observed != SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST)
            && observed != SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST)
        {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        if observed == SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST)
            || observed == SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST)
        {
            migrate_history_incarnation(&self.shared)?;
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_HISTORY_INCARNATION_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST),
            )?;
        }
        migrate_audit_request_index(&self.shared)?;
        if observed != SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST)
            && observed != SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST)
        {
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST),
            )?;
        }
        migrate_event_routes(&self.shared)?;
        if observed != SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST) {
            publish_record_registry(
                &self.shared,
                SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST),
                SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST),
            )?;
        }
        migrate_commit_entity_references(&self.shared)?;
        publish_record_registry(
            &self.shared,
            SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST),
            SchemaHash::from_bytes(PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST),
        )?;
        install_contract_migration_tables(&self.shared)?;
        publish_record_registry(
            &self.shared,
            SchemaHash::from_bytes(PRE_CONTRACT_MIGRATION_REGISTRY_DIGEST),
            SchemaHash::from_bytes(PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST),
        )?;
        publish_record_registry(
            &self.shared,
            SchemaHash::from_bytes(PRE_VALIDATED_PREFIX_CHECKPOINT_REGISTRY_DIGEST),
            SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST),
        )?;
        install_history_tombstones_table(&self.shared)?;
        publish_record_registry(
            &self.shared,
            SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST),
            crate::changelog_v3_activation::PRE_V3_REGISTRY,
        )
    }

    pub(crate) fn fence_writes(&self) {
        self.shared.fence_writes();
    }

    /// Compatibility checkpoint writer retained for existing callers.
    ///
    /// Production shutdown has no caller for this API. It retains its existing
    /// semantics for compatibility and explicit falsifiability, but must not be
    /// composed into runtime close.
    #[doc(hidden)]
    pub fn write_validated_prefix_checkpoint(&self) -> Result<bool, StorageError> {
        let _lease = self.acquire_mutation_lease()?;
        self.ensure_writable()?;
        if !self.shared.startup_validation_clean() {
            return Ok(false);
        }
        if let Err(error) = self
            .shared
            .checkpoint_published_journal_suffix_for_barrier()
        {
            self.shared.note_checkpoint_write_failure();
            return Err(error);
        }
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let retained = crate::startup::read_retained_metadata_pub(&transaction)?;
        drop(transaction);
        match crate::validated_prefix::write_validated_prefix_checkpoint(
            &self.shared,
            &retained,
            crate::validated_prefix::CheckpointPurpose::TestFixture,
        ) {
            Ok(()) => Ok(true),
            Err(error) => {
                // ADR-0019 A1: count the lost fast path; callers decide fatality.
                self.shared.note_checkpoint_write_failure();
                Err(error)
            }
        }
    }

    /// Performs the one storage-owned graceful-close sequence from the journal
    /// barrier through immutable checkpoint classification and final CLEAN.
    #[doc(hidden)]
    #[must_use]
    pub fn complete_graceful_close(&self) -> crate::GracefulCheckpointCloseReceiptV1 {
        let started = Instant::now();
        let lease = self.acquire_mutation_lease();
        let Ok(_lease) = lease else {
            self.shared.note_clean_close_write_failure();
            return crate::GracefulCheckpointCloseReceiptV1::barrier_failed([
                saturating_elapsed_microseconds(started),
                0,
                0,
            ]);
        };
        if self.ensure_writable().is_err() {
            self.shared.note_clean_close_write_failure();
            return crate::GracefulCheckpointCloseReceiptV1::barrier_failed([
                saturating_elapsed_microseconds(started),
                0,
                0,
            ]);
        }
        let receipt = self.shared.complete_graceful_close();
        if receipt.lifecycle() != crate::GracefulLifecycleOutcomeV1::CleanCommitted {
            self.shared.note_clean_close_write_failure();
        }
        receipt
    }

    /// Compatibility entry point for storage-level lifecycle tests.
    ///
    /// Production shutdown uses [`Self::complete_graceful_close`] so it retains
    /// the closed receipt even when CLEAN is not known committed.
    pub fn write_clean_close_lifecycle(&self) -> Result<(), StorageError> {
        match self.complete_graceful_close().lifecycle() {
            crate::GracefulLifecycleOutcomeV1::CleanCommitted => Ok(()),
            crate::GracefulLifecycleOutcomeV1::CleanUnknown => Err(StorageError::new(
                StorageErrorKind::CommitStatusUnknown,
                None,
            )),
            crate::GracefulLifecycleOutcomeV1::CleanNotAttempted
            | crate::GracefulLifecycleOutcomeV1::CleanFailed => {
                Err(StorageError::new(StorageErrorKind::Unavailable, None))
            }
        }
    }

    /// Failed checkpoint writes after clean validation (non-fatal; counted).
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_write_failures(&self) -> u64 {
        self.shared.checkpoint_write_failures()
    }

    /// Failed clean-close certificate writes on this handle (non-fatal; counted).
    #[doc(hidden)]
    #[must_use]
    pub fn clean_close_write_failures(&self) -> u64 {
        self.shared.clean_close_write_failures()
    }

    /// Per-reason counts of declined ADR-0157 bounded startups on this handle.
    #[doc(hidden)]
    #[must_use]
    pub fn clean_close_decline_counts(&self) -> [(&'static str, u64); 11] {
        self.shared.clean_close_decline_counts()
    }

    /// Per-reason counts of ignored validated-prefix checkpoints on this handle.
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_ignore_counts(&self) -> [(&'static str, u64); 10] {
        self.shared.checkpoint_ignore_counts()
    }

    /// History rows iterated to compute checkpoint counts on this handle.
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_count_rows_walked(&self) -> u64 {
        self.shared.checkpoint_count_rows_walked()
    }

    #[cfg(test)]
    pub(crate) fn reopen_for_test(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

fn migrate_event_reference_records(shared: &SharedRedb) -> Result<(), StorageError> {
    migrate_commit_event_references(shared)?;
    migrate_outbox_event_references(shared)
}

fn migrate_commit_entity_references(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        // Self-contained: hashes come from embedded post-images in the commit
        // payload. No EVENTS join — damaged events surface as structural findings.
        let mut commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let mut replacements = Vec::new();
        let mut last = None;
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0usize;
        let mut reached_end = true;
        {
            let mut rows = match after.as_deref() {
                Some(after) => commits
                    .range::<&[u8]>((Excluded(after), Unbounded))
                    .map_err(precommit_storage_error)?,
                None => commits.iter().map_err(precommit_storage_error)?,
            };
            for row in &mut rows {
                let (key, value) = row.map_err(precommit_storage_error)?;
                let key = key.value().to_vec();
                let original = value.value();
                let current = riffdb_storage_api::transcode_commit_to_entity_reference_v3(original)
                    .map_err(crate::error::codec_error)?;
                scanned_rows = scanned_rows.saturating_add(1);
                scanned_bytes = scanned_bytes
                    .checked_add(original.len())
                    .and_then(|total| total.checked_add(current.as_bytes().len()))
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if current.as_bytes() != original {
                    replacements.push((key.clone(), current.into_bytes()));
                }
                last = Some(key);
                if scanned_rows >= FORMAT_MIGRATION_MAX_ROWS
                    || scanned_bytes >= FORMAT_MIGRATION_MAX_BYTES
                {
                    reached_end = false;
                    break;
                }
            }
        }
        let changed = !replacements.is_empty();
        for (key, value) in replacements {
            commits
                .insert(key.as_slice(), value.as_slice())
                .map_err(precommit_storage_error)?;
        }
        drop(commits);
        if changed {
            shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
            shared.commit_durable(transaction)?;
            shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        } else {
            transaction.abort().map_err(precommit_storage_error)?;
        }
        if reached_end {
            return Ok(());
        }
        after = last;
    }
}

fn migrate_commit_event_references(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let mut commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let mut replacements = Vec::new();
        let mut last = None;
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0usize;
        let mut reached_end = true;
        {
            let mut rows = match after.as_deref() {
                Some(after) => commits
                    .range::<&[u8]>((Excluded(after), Unbounded))
                    .map_err(precommit_storage_error)?,
                None => commits.iter().map_err(precommit_storage_error)?,
            };
            for row in &mut rows {
                let (key, value) = row.map_err(precommit_storage_error)?;
                let key = key.value().to_vec();
                let original = value.value();
                let commit = decode_commit_with_event_table(original, &events)?
                    .into_parts()
                    .0;
                let current = encode_commit_record_v1(&commit)?;
                scanned_rows = scanned_rows.saturating_add(1);
                scanned_bytes = scanned_bytes
                    .checked_add(original.len())
                    .and_then(|total| total.checked_add(current.as_bytes().len()))
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if current.as_bytes() != original {
                    replacements.push((key.clone(), current.into_bytes()));
                }
                last = Some(key);
                if scanned_rows >= FORMAT_MIGRATION_MAX_ROWS
                    || scanned_bytes >= FORMAT_MIGRATION_MAX_BYTES
                {
                    reached_end = false;
                    break;
                }
            }
        }
        let changed = !replacements.is_empty();
        for (key, value) in replacements {
            commits
                .insert(key.as_slice(), value.as_slice())
                .map_err(precommit_storage_error)?;
        }
        drop(commits);
        drop(events);
        if changed {
            shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
            shared.commit_durable(transaction)?;
            shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        } else {
            transaction.abort().map_err(precommit_storage_error)?;
        }
        if reached_end {
            return Ok(());
        }
        after = last;
    }
}

fn migrate_outbox_event_references(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let mut outbox = transaction.open_table(OUTBOX).map_err(table_error)?;
        let mut replacements = Vec::new();
        let mut last = None;
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0usize;
        let mut reached_end = true;
        {
            let mut rows = match after.as_deref() {
                Some(after) => outbox
                    .range::<&[u8]>((Excluded(after), Unbounded))
                    .map_err(precommit_storage_error)?,
                None => outbox.iter().map_err(precommit_storage_error)?,
            };
            for row in &mut rows {
                let (key, value) = row.map_err(precommit_storage_error)?;
                let key = key.value().to_vec();
                let original = value.value();
                let intent = decode_outbox_with_event_table(original, &events)?
                    .into_parts()
                    .0;
                let current = encode_outbox_intent_v1(&intent)?;
                scanned_rows = scanned_rows.saturating_add(1);
                scanned_bytes = scanned_bytes
                    .checked_add(original.len())
                    .and_then(|total| total.checked_add(current.as_bytes().len()))
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if current.as_bytes() != original {
                    replacements.push((key.clone(), current.into_bytes()));
                }
                last = Some(key);
                if scanned_rows >= FORMAT_MIGRATION_MAX_ROWS
                    || scanned_bytes >= FORMAT_MIGRATION_MAX_BYTES
                {
                    reached_end = false;
                    break;
                }
            }
        }
        let changed = !replacements.is_empty();
        for (key, value) in replacements {
            outbox
                .insert(key.as_slice(), value.as_slice())
                .map_err(precommit_storage_error)?;
        }
        drop(outbox);
        drop(events);
        if changed {
            shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
            shared.commit_durable(transaction)?;
            shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        } else {
            transaction.abort().map_err(precommit_storage_error)?;
        }
        if reached_end {
            return Ok(());
        }
        after = last;
    }
}

fn migrate_partition_index_generations(shared: &SharedRedb) -> Result<(), StorageError> {
    let legacy_maxima = read_legacy_index_epoch_maxima(shared)?;
    migrate_partition_index_generation_rows(shared, &legacy_maxima)?;
    remove_legacy_index_epoch_rows(shared)?;
    validate_partition_index_generation_rows(shared)?;
    // Proven clean of legacy rows; subsequent current-digest opens skip the scan.
    mark_index_epoch_rows_repaired(shared)
}

/// O(1) gate: run full legacy-row repair only when the table may still hold
/// pre-generation prefix-keyed rows.
///
/// Conservative: empty → no work; durable repair marker → already proven;
/// otherwise scan.
fn index_epoch_rows_may_need_legacy_repair(shared: &SharedRedb) -> Result<bool, StorageError> {
    let read = shared.database.begin_read().map_err(transaction_error)?;
    let meta = read.open_table(META).map_err(table_error)?;
    if meta
        .get(META_INDEX_EPOCH_ROWS_REPAIRED)
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Ok(false);
    }
    let epochs = read.open_table(INDEX_EPOCHS).map_err(table_error)?;
    let empty = epochs.is_empty().map_err(precommit_storage_error)?;
    if empty {
        // No rows of any generation; mark repaired so reopen stays O(1).
        drop(epochs);
        drop(meta);
        drop(read);
        mark_index_epoch_rows_repaired(shared)?;
        return Ok(false);
    }
    Ok(true)
}

fn mark_index_epoch_rows_repaired(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let transaction = OperationalWriteTransaction::from_drained(
        transaction,
        riffdb_storage_api::ChangelogAttributionV3::StorageFormatMigration,
    )?;
    {
        let mut meta = transaction.open_table(META).map_err(table_error)?;
        if meta
            .get(META_INDEX_EPOCH_ROWS_REPAIRED)
            .map_err(precommit_storage_error)?
            .is_some()
        {
            drop(meta);
            return transaction.abort().map_err(precommit_storage_error);
        }
        // Fixed one-byte marker; not a durable record envelope.
        meta.insert(META_INDEX_EPOCH_ROWS_REPAIRED, [1u8].as_slice())
            .map_err(precommit_storage_error)?;
    }
    transaction.finish()?.commit(shared)?;
    Ok(())
}

fn read_legacy_index_epoch_maxima(
    shared: &SharedRedb,
) -> Result<
    BTreeMap<IndexId, (IndexEpoch, riffdb_storage_api::DurableKeySchemaBindingV1)>,
    StorageError,
> {
    const MAX_DISTINCT_MIGRATION_INDEXES: usize = 65_536;

    let transaction = shared.database.begin_read().map_err(transaction_error)?;
    let epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
    let mut maxima = BTreeMap::new();
    for row in epochs.iter().map_err(precommit_storage_error)? {
        let (physical, encoded) = row.map_err(precommit_storage_error)?;
        if let Ok(decoded) = decode_legacy_index_epoch_v1(encoded.value()) {
            let legacy = decoded.value();
            let key = decode_index_range_prefix_key(physical.value())
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            if legacy.target() != &key {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let index_id = legacy.target().index_id();
            match maxima.get_mut(&index_id) {
                Some((epoch, binding)) if legacy.epoch() > *epoch => {
                    *epoch = legacy.epoch();
                    *binding = legacy.schema_binding().clone();
                }
                Some(_) => {}
                None => {
                    if maxima.len() >= MAX_DISTINCT_MIGRATION_INDEXES {
                        return Err(storage_error(StorageErrorKind::LimitExceeded));
                    }
                    maxima.insert(index_id, (legacy.epoch(), legacy.schema_binding().clone()));
                }
            }
            continue;
        }
        let current = decode_index_epoch_v1(encoded.value())?.into_parts().0;
        let key = decode_partition_index_key(physical.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if current.target() != &key {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let index_id = current.target().index_id();
        match maxima.get_mut(&index_id) {
            Some((epoch, binding)) if current.epoch() > *epoch => {
                *epoch = current.epoch();
                *binding = current.schema_binding().clone();
            }
            Some(_) => {}
            None => {
                if maxima.len() >= MAX_DISTINCT_MIGRATION_INDEXES {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                maxima.insert(
                    index_id,
                    (current.epoch(), current.schema_binding().clone()),
                );
            }
        }
    }
    Ok(maxima)
}

fn migrate_partition_index_generation_rows(
    shared: &SharedRedb,
    legacy_maxima: &BTreeMap<IndexId, (IndexEpoch, riffdb_storage_api::DurableKeySchemaBindingV1)>,
) -> Result<(), StorageError> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let transaction = OperationalWriteTransaction::from_drained(
            transaction,
            riffdb_storage_api::ChangelogAttributionV3::StorageFormatMigration,
        )?;
        let indexes = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        let mut epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
        let mut replacements = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut last = None;
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0usize;
        let mut reached_end = true;
        {
            let mut rows = match after.as_deref() {
                Some(after) => indexes
                    .range::<&[u8]>((Excluded(after), Unbounded))
                    .map_err(precommit_storage_error)?,
                None => indexes.iter().map_err(precommit_storage_error)?,
            };
            for row in &mut rows {
                let (physical, encoded) = row.map_err(precommit_storage_error)?;
                let physical_key = physical.value().to_vec();
                let entry = decode_index_entry_v2(encoded.value())?.into_parts().0;
                if entry.key().as_bytes() != physical.value() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                let target = riffdb_storage_api::PartitionIndexTarget::new(
                    entry.partition_key().clone(),
                    entry.key().index_id(),
                );
                let generation_key = encode_partition_index_key(&target);
                if let Some(existing) = epochs
                    .get(generation_key.as_slice())
                    .map_err(precommit_storage_error)?
                {
                    let generation = decode_index_epoch_v1(existing.value())?.into_parts().0;
                    if generation.target() != &target {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                } else {
                    let Some((legacy_epoch, _)) = legacy_maxima.get(&target.index_id()) else {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    };
                    let generation = legacy_epoch
                        .checked_next()
                        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                    let current =
                        StoredIndexEpochV1::new(target, entry.schema_binding().clone(), generation);
                    let encoded = encode_index_epoch_v1(&current)?;
                    replacements.insert(generation_key, encoded.into_bytes());
                }
                scanned_rows = scanned_rows.saturating_add(1);
                scanned_bytes = scanned_bytes
                    .checked_add(physical.value().len())
                    .and_then(|value| value.checked_add(encoded.value().len()))
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                last = Some(physical_key);
                if scanned_rows >= FORMAT_MIGRATION_MAX_ROWS
                    || scanned_bytes >= FORMAT_MIGRATION_MAX_BYTES
                {
                    reached_end = false;
                    break;
                }
            }
        }
        drop(indexes);
        let changed = !replacements.is_empty();
        for (key, value) in replacements {
            epochs
                .insert(key.as_slice(), value.as_slice())
                .map_err(precommit_storage_error)?;
        }
        drop(epochs);
        if changed {
            shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
            transaction.finish()?.commit(shared)?;
            shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        } else {
            transaction.abort().map_err(precommit_storage_error)?;
        }
        if reached_end {
            return Ok(());
        }
        after = last;
    }
}

fn remove_legacy_index_epoch_rows(shared: &SharedRedb) -> Result<(), StorageError> {
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let transaction = OperationalWriteTransaction::from_drained(
            transaction,
            riffdb_storage_api::ChangelogAttributionV3::StorageFormatMigration,
        )?;
        let mut epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
        let mut removals = Vec::new();
        let mut scanned_bytes = 0usize;
        {
            let mut rows = epochs.iter().map_err(precommit_storage_error)?;
            for row in &mut rows {
                let (physical, encoded) = row.map_err(precommit_storage_error)?;
                if decode_legacy_index_epoch_v1(encoded.value()).is_ok() {
                    decode_index_range_prefix_key(physical.value())
                        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                    removals.push(physical.value().to_vec());
                    scanned_bytes = scanned_bytes
                        .checked_add(physical.value().len())
                        .and_then(|value| value.checked_add(encoded.value().len()))
                        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                } else {
                    let current = decode_index_epoch_v1(encoded.value())?.into_parts().0;
                    let key = decode_partition_index_key(physical.value())
                        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                    if current.target() != &key {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                }
                if removals.len() >= FORMAT_MIGRATION_MAX_ROWS
                    || scanned_bytes >= FORMAT_MIGRATION_MAX_BYTES
                {
                    break;
                }
            }
        }
        if removals.is_empty() {
            drop(epochs);
            return transaction.abort().map_err(precommit_storage_error);
        }
        for key in removals {
            epochs
                .remove(key.as_slice())
                .map_err(precommit_storage_error)?;
        }
        drop(epochs);
        shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        transaction.finish()?.commit(shared)?;
        shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
    }
}

fn validate_partition_index_generation_rows(shared: &SharedRedb) -> Result<(), StorageError> {
    let transaction = shared.database.begin_read().map_err(transaction_error)?;
    let epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
    for row in epochs.iter().map_err(precommit_storage_error)? {
        let (physical, encoded) = row.map_err(precommit_storage_error)?;
        let key = decode_partition_index_key(physical.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let current = decode_index_epoch_v1(encoded.value())?.into_parts().0;
        if current.target() != &key {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    Ok(())
}

/// Creates the partition route table and rebuilds it from authoritative commits.
///
/// Each batch is idempotent and bounded. A pre-existing row must exactly match
/// the commit-linked event identity, type, and hash; otherwise startup fails
/// closed rather than publishing a partially trustworthy routing index.
fn migrate_event_routes(shared: &SharedRedb) -> Result<(), StorageError> {
    ensure_event_routes_table(shared)?;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut changed = false;
        let mut last_key = after.clone();
        {
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let mut routes = transaction.open_table(EVENT_ROUTES).map_err(table_error)?;
            let mut scan = match after.as_deref() {
                Some(after_key) => commits
                    .range::<&[u8]>((Excluded(after_key), Unbounded))
                    .map_err(precommit_storage_error)?,
                None => commits.iter().map_err(precommit_storage_error)?,
            };
            for entry in &mut scan {
                let (key, value) = entry.map_err(precommit_storage_error)?;
                let key_bytes = key.value().to_vec();
                let sequence = decode_application_sequence_key(&key_bytes)
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                let commit = decode_commit_with_event_table(value.value(), &events)?
                    .into_parts()
                    .0;
                if commit.commit_sequence() != sequence {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                for event in commit.events() {
                    let route = riffdb_storage_api::StoredEventRouteV1::new(
                        event.event_id(),
                        event.event_type_id(),
                        event.event_hash(),
                    );
                    let route_key =
                        encode_event_route_key(commit.partition_hash(), event.event_id());
                    let encoded = encode_event_route_v1(route)?;
                    if let Some(existing) = routes
                        .get(route_key.as_slice())
                        .map_err(precommit_storage_error)?
                    {
                        if *decode_event_route_v1(existing.value())?.value() != route {
                            return Err(storage_error(StorageErrorKind::CorruptData));
                        }
                    } else {
                        routes
                            .insert(route_key.as_slice(), encoded.as_bytes())
                            .map_err(precommit_storage_error)?;
                        changed = true;
                    }
                    bytes = bytes
                        .checked_add(encoded.as_bytes().len())
                        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                }
                bytes = bytes
                    .checked_add(value.value().len())
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                rows = rows
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                last_key = Some(key_bytes);
                if rows >= FORMAT_MIGRATION_MAX_ROWS || bytes >= FORMAT_MIGRATION_MAX_BYTES {
                    break;
                }
            }
        }
        if rows == 0 {
            return transaction.abort().map_err(precommit_storage_error);
        }
        if changed {
            shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
            shared.commit_durable(transaction)?;
            shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        } else {
            transaction.abort().map_err(precommit_storage_error)?;
        }
        after = last_key;
        if rows < FORMAT_MIGRATION_MAX_ROWS && bytes < FORMAT_MIGRATION_MAX_BYTES {
            break;
        }
    }
    Ok(())
}

fn ensure_event_routes_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let read = shared.database.begin_read().map_err(transaction_error)?;
    let tables = read
        .list_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    drop(read);
    if tables.iter().any(|name| name == "event_routes") {
        return Ok(());
    }
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(transaction.open_table(EVENT_ROUTES).map_err(table_error)?);
    shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
    shared.commit_durable(transaction)?;
    shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)
}

/// Creates `audit_by_request` when absent and backfills service-audit index rows
/// from the authoritative AUDIT table. Batched and crash-restartable: each batch
/// is idempotent on key insert (duplicate key is a no-op success).
fn migrate_audit_request_index(shared: &SharedRedb) -> Result<(), StorageError> {
    ensure_audit_by_request_table(shared)?;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut last_key = after.clone();
        {
            let audit = transaction.open_table(AUDIT).map_err(table_error)?;
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let mut index = transaction
                .open_table(AUDIT_BY_REQUEST)
                .map_err(table_error)?;
            let mut scan = match after.as_deref() {
                Some(after_key) => audit
                    .range::<&[u8]>((Excluded(after_key), Unbounded))
                    .map_err(precommit_storage_error)?,
                None => audit.iter().map_err(precommit_storage_error)?,
            };
            for entry in &mut scan {
                let (key, value) = entry.map_err(precommit_storage_error)?;
                let key_bytes = key.value().to_vec();
                let value_bytes = value.value();
                let sequence = decode_audit_key(&key_bytes)
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                let record = decode_administration_audit_with_command_tables(
                    value_bytes,
                    &commits,
                    &events,
                )?
                .into_parts()
                .0;
                if record.administration_sequence() != sequence {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                if let riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(service) =
                    &record
                {
                    let index_key = encode_audit_by_request_key(service.request_id(), sequence);
                    let encoded = encode_service_audit_request_index_v1(
                        riffdb_storage_api::StoredServiceAuditRequestIndexV1::new(
                            service.request_id(),
                            sequence,
                        ),
                    )?;
                    // Idempotent: resume mid-batch must not fail on already-written keys.
                    let _ = index
                        .insert(index_key.as_slice(), encoded.as_bytes())
                        .map_err(precommit_storage_error)?;
                    bytes = bytes
                        .checked_add(encoded.as_bytes().len())
                        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                }
                last_key = Some(key_bytes);
                rows = rows
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if rows >= FORMAT_MIGRATION_MAX_ROWS || bytes >= FORMAT_MIGRATION_MAX_BYTES {
                    break;
                }
            }
        }
        if rows == 0 {
            return transaction.abort().map_err(precommit_storage_error);
        }
        shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        shared.commit_durable(transaction)?;
        shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        after = last_key;
        if rows < FORMAT_MIGRATION_MAX_ROWS && bytes < FORMAT_MIGRATION_MAX_BYTES {
            break;
        }
    }
    Ok(())
}

fn ensure_audit_by_request_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let read = shared.database.begin_read().map_err(transaction_error)?;
    let tables = read
        .list_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    drop(read);
    if tables.iter().any(|name| name == "audit_by_request") {
        return Ok(());
    }
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(AUDIT_BY_REQUEST)
            .map_err(table_error)?,
    );
    shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
    shared.commit_durable(transaction)?;
    shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)
}

/// Inserts `history_incarnation/v1 = 1` when absent. Idempotent; does not change an
/// existing value (restore is the only path that advances the incarnation).
fn migrate_history_incarnation(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let mut metadata = transaction.open_table(META).map_err(table_error)?;
    if metadata
        .get(META_HISTORY_INCARNATION)
        .map_err(precommit_storage_error)?
        .is_some()
    {
        drop(metadata);
        return transaction.abort().map_err(precommit_storage_error);
    }
    let encoded = encode_history_incarnation_v1(HISTORY_INCARNATION_INITIAL)
        .map_err(crate::error::codec_error)?;
    metadata
        .insert(META_HISTORY_INCARNATION, encoded.as_bytes())
        .map_err(precommit_storage_error)?;
    drop(metadata);
    shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
    shared.commit_durable(transaction)?;
    shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)
}

fn publish_record_registry(
    shared: &SharedRedb,
    expected: SchemaHash,
    next: SchemaHash,
) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let mut metadata = transaction.open_table(META).map_err(table_error)?;
    let observed = {
        let encoded = metadata
            .get(META_RECORD_REGISTRY)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        *decode_record_registry_v2(encoded.value())
            .map_err(crate::error::codec_error)?
            .value()
    };
    if observed == next {
        drop(metadata);
        return transaction.abort().map_err(precommit_storage_error);
    }
    if observed != expected {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    let current = encode_record_registry_v2(next).map_err(crate::error::codec_error)?;
    metadata
        .insert(META_RECORD_REGISTRY, current.as_bytes())
        .map_err(precommit_storage_error)?;
    drop(metadata);
    shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
    shared.commit_durable(transaction)?;
    shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)
}

fn observed_registry_digest(shared: &SharedRedb) -> Result<SchemaHash, StorageError> {
    let transaction = shared.database.begin_read().map_err(transaction_error)?;
    let metadata = transaction.open_table(META).map_err(table_error)?;
    let encoded = metadata
        .get(META_RECORD_REGISTRY)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    Ok(*decode_record_registry_v2(encoded.value())
        .map_err(crate::error::codec_error)?
        .value())
}

/// Bootstraps the delete-aware entity-chain catalog at one exact predecessor
/// frontier. The heads, receipt, and retirement of the old checkpoint publish
/// atomically; the registry digest advances only in the following transaction.
fn migrate_entity_transition_heads(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    create_all_tables(&transaction).map_err(table_error)?;

    let (database_id, history_incarnation) = {
        let meta = transaction.open_table(META).map_err(table_error)?;
        let identity = meta
            .get(META_DATABASE_ID)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let history = meta
            .get(META_HISTORY_INCARNATION)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        (
            *decode_database_identity_v1(identity.value())
                .map_err(crate::error::codec_error)?
                .value(),
            *decode_history_incarnation_v1(history.value())
                .map_err(crate::error::codec_error)?
                .value(),
        )
    };
    let application_frontier = {
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        commits
            .last()
            .map_err(precommit_storage_error)?
            .map(|(key, _)| {
                crate::keys::decode_application_sequence_key(key.value())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))
            })
            .transpose()?
    };
    let administration_frontier = {
        let audit = transaction.open_table(AUDIT).map_err(table_error)?;
        audit
            .last()
            .map_err(precommit_storage_error)?
            .map(|(key, _)| {
                decode_audit_key(key.value())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))
            })
            .transpose()?
    };
    let predecessor = DualFrontier::new(application_frontier, administration_frontier);
    let receipt = riffdb_storage_api::ChangelogV2RotationReceipt::new(
        database_id,
        history_incarnation,
        predecessor,
        [0; 32],
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;

    {
        let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
        let mut heads = transaction
            .open_table(ENTITY_CHAIN_HEADS)
            .map_err(table_error)?;
        if !heads.is_empty().map_err(precommit_storage_error)? {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let mut ordinal = 0_u32;
        for row in entities.iter().map_err(precommit_storage_error)? {
            let (key, value) = row.map_err(precommit_storage_error)?;
            let entity = decode_entity_record_v1(value.value())?.into_parts().0;
            if entity.target().key().as_bytes() != key.value() {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let sequence =
                application_frontier.ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let transition = CommittedEntityTransitionV1::new(
                sequence,
                ordinal,
                entity.target().clone(),
                EntityChainStateV1::NeverExisted,
                0,
                None,
                EntityChainStateV1::Live {
                    version: entity.entity_version(),
                    value_hash: riffdb_storage_api::derive_entity_record_hash_v1(&entity)
                        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?,
                },
            )
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let head = EntityChainHeadV1::from_genesis(&transition)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let encoded = riffdb_storage_api::encode_entity_chain_head_v1(&head)
                .map_err(crate::error::codec_error)?;
            heads
                .insert(key.value(), encoded.as_bytes())
                .map_err(precommit_storage_error)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    {
        let mut meta = transaction.open_table(META).map_err(table_error)?;
        let receipt_status = meta
            .get(META_CHANGELOG_V2_ROTATION_RECEIPT)
            .map_err(precommit_storage_error)?
            .map(|existing| {
                riffdb_storage_api::decode_changelog_v2_rotation_receipt_v1(existing.value())
                    .map(|decoded| decoded.value() == &receipt)
                    .map_err(crate::error::codec_error)
            })
            .transpose()?;
        match receipt_status {
            Some(true) => {}
            Some(false) => return Err(storage_error(StorageErrorKind::CorruptData)),
            None => {
                let encoded = riffdb_storage_api::encode_changelog_v2_rotation_receipt_v1(receipt)
                    .map_err(crate::error::codec_error)?;
                meta.insert(META_CHANGELOG_V2_ROTATION_RECEIPT, encoded.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
        }
        meta.remove(META_VALIDATED_PREFIX_CHECKPOINT)
            .map_err(precommit_storage_error)?;
    }
    shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
    shared.commit_durable(transaction)?;
    shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)
}

fn install_contract_migration_tables(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    create_all_tables(&transaction).map_err(table_error)?;
    shared.commit_durable(transaction)
}

/// Ensures `history_tombstones` exists (and any other current tables). Idempotent.
fn install_history_tombstones_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    create_all_tables(&transaction).map_err(table_error)?;
    shared.commit_durable(transaction)
}

fn install_reactive_consumer_tables(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(crate::layout::REACTIVE_MODULES)
            .map_err(table_error)?,
    );
    drop(
        transaction
            .open_table(crate::layout::EVENT_CONSUMERS)
            .map_err(table_error)?,
    );
    drop(
        transaction
            .open_table(crate::layout::EVENT_CONSUMER_DELIVERIES)
            .map_err(table_error)?,
    );
    shared.commit_durable(transaction)
}

fn install_application_installation_campaign_table(
    shared: &SharedRedb,
) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(crate::layout::APPLICATION_INSTALLATION_CAMPAIGNS)
            .map_err(table_error)?,
    );
    shared.commit_durable(transaction)
}

fn install_application_export_operation_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(crate::layout::APPLICATION_EXPORT_OPERATIONS)
            .map_err(table_error)?,
    );
    shared.commit_durable(transaction)
}

fn install_vector_evidence_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(VECTOR_EVIDENCE)
            .map_err(table_error)?,
    );
    shared.commit_durable(transaction)
}

fn install_vector_observations_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(VECTOR_OBSERVATIONS)
            .map_err(table_error)?,
    );
    drop(
        transaction
            .open_table(VECTOR_EVIDENCE_INDEX)
            .map_err(table_error)?,
    );
    shared.commit_durable(transaction)?;
    backfill_vector_evidence_index(shared)
}

fn install_vector_projection_controls_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(VECTOR_PROJECTION_CONTROLS)
            .map_err(table_error)?,
    );
    shared.commit_durable(transaction)
}

fn install_columnar_projection_controls_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(crate::layout::COLUMNAR_PROJECTION_CONTROLS)
            .map_err(table_error)?,
    );
    shared.commit_durable(transaction)
}

pub(crate) fn validate_inert_legacy_vector_controls(
    shared: &SharedRedb,
) -> Result<(), StorageError> {
    const MAX_LEGACY_VECTOR_CONTROLS: u64 = 4_096;

    let transaction = shared.database.begin_read().map_err(transaction_error)?;
    let table = transaction
        .open_table(VECTOR_PROJECTION_CONTROLS)
        .map_err(table_error)?;
    if table.len().map_err(precommit_storage_error)? > MAX_LEGACY_VECTOR_CONTROLS {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    for row in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let source = crate::keys::decode_vector_projection_control_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let control = riffdb_storage_api::decode_vector_projection_control_v1(value.value())
            .map_err(crate::error::codec_error)?;
        if control.value().source() != &source {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    Ok(())
}

fn backfill_vector_evidence_index(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let evidence = transaction
            .open_table(VECTOR_EVIDENCE)
            .map_err(table_error)?;
        let mut index = transaction
            .open_table(VECTOR_EVIDENCE_INDEX)
            .map_err(table_error)?;
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut changed = false;
        let mut last_key = after.clone();
        let mut scan = match after.as_deref() {
            Some(after_key) => evidence
                .range::<&[u8]>((Excluded(after_key), Unbounded))
                .map_err(precommit_storage_error)?,
            None => evidence.iter().map_err(precommit_storage_error)?,
        };
        for row in &mut scan {
            let (key, value) = row.map_err(precommit_storage_error)?;
            let primary_key = key.value().to_vec();
            let decoded = riffdb_storage_api::decode_vector_evidence_v1(value.value())
                .map_err(crate::error::codec_error)?;
            let evidence_row = decoded.value();
            let target = riffdb_storage_api::VectorObservationTargetV1::new(
                evidence_row.schema_binding().lineage().clone(),
                evidence_row.partition_key().clone(),
                evidence_row.target().entity_type_id(),
                evidence_row.vector_field(),
            );
            let index_key =
                crate::keys::encode_vector_evidence_index_key(&target, evidence_row.target().key())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let index_row =
                riffdb_storage_api::VectorEvidenceIndexEntryV1::from_evidence(evidence_row)
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let encoded = riffdb_storage_api::encode_vector_evidence_index_v1(&index_row)
                .map_err(crate::error::codec_error)?;
            if let Some(existing) = index
                .get(index_key.as_slice())
                .map_err(precommit_storage_error)?
            {
                let existing =
                    riffdb_storage_api::decode_vector_evidence_index_v1(existing.value())
                        .map_err(crate::error::codec_error)?;
                if existing.value() != &index_row {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
            } else {
                index
                    .insert(index_key.as_slice(), encoded.as_bytes())
                    .map_err(precommit_storage_error)?;
                changed = true;
            }
            bytes = bytes
                .checked_add(value.value().len())
                .and_then(|total| total.checked_add(encoded.as_bytes().len()))
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            rows = rows
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            last_key = Some(primary_key);
            if rows >= FORMAT_MIGRATION_MAX_ROWS || bytes >= FORMAT_MIGRATION_MAX_BYTES {
                break;
            }
        }
        drop(scan);
        drop(index);
        drop(evidence);
        if rows == 0 {
            return transaction.abort().map_err(precommit_storage_error);
        }
        if changed {
            shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
            shared.commit_durable(transaction)?;
            shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        } else {
            transaction.abort().map_err(precommit_storage_error)?;
        }
        after = last_key;
        if rows < FORMAT_MIGRATION_MAX_ROWS && bytes < FORMAT_MIGRATION_MAX_BYTES {
            return Ok(());
        }
    }
}

fn active_vector_specs_for_migration(
    shared: &SharedRedb,
) -> Result<crate::startup::ActiveVectorSpecsForMigration, StorageError> {
    crate::startup::active_vector_specs_for_migration(shared)
}

/// Upgrades the pre-health vector registry by reconstructing every missing
/// partition observation from the authoritative evidence index and then one
/// lineage-wide health row. The reconstruction is deterministic and
/// idempotent: an interrupted committed migration can be replayed, while any
/// conflicting pre-existing row refuses before the registry marker advances.
fn backfill_vector_observations_and_health(shared: &SharedRedb) -> Result<(), StorageError> {
    let active = active_vector_specs_for_migration(shared)?;
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let index = transaction
        .open_table(VECTOR_EVIDENCE_INDEX)
        .map_err(table_error)?;
    let mut observations = transaction
        .open_table(VECTOR_OBSERVATIONS)
        .map_err(table_error)?;
    let mut current_target: Option<riffdb_storage_api::VectorObservationTargetV1> = None;
    let mut total = 0_u64;
    let mut stale = 0_u64;
    let mut models = BTreeMap::<EmbeddingMetadata, u64>::new();
    let mut revision: Option<CommitSequence> = None;

    {
        let mut flush = |target: Option<riffdb_storage_api::VectorObservationTargetV1>,
                         total: u64,
                         stale: u64,
                         models: &mut BTreeMap<EmbeddingMetadata, u64>,
                         revision: Option<CommitSequence>|
         -> Result<(), StorageError> {
            let Some(target) = target else {
                return Ok(());
            };
            let revision = revision.ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let rebuilt = riffdb_storage_api::VectorObservationCountsV1::from_parts(
                target.clone(),
                total,
                stale,
                std::mem::take(models).into_iter().collect(),
                revision,
            )
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let key = encode_vector_observation_key(&target)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            if let Some(existing) = observations
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
            {
                let existing = riffdb_storage_api::decode_vector_observation_v1(existing.value())
                    .map_err(crate::error::codec_error)?;
                if existing.value() != &rebuilt {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
            } else {
                let encoded = riffdb_storage_api::encode_vector_observation_v1(&rebuilt)
                    .map_err(crate::error::codec_error)?;
                observations
                    .insert(key.as_slice(), encoded.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
            Ok(())
        };

        for row in index.iter().map_err(precommit_storage_error)? {
            let (key, value) = row.map_err(precommit_storage_error)?;
            let entry = riffdb_storage_api::decode_vector_evidence_index_v1(value.value())
                .map_err(crate::error::codec_error)?
                .into_parts()
                .0;
            let (physical_target, physical_entity_key) =
                decode_vector_evidence_index_key(key.value())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            if physical_target != *entry.target() || physical_entity_key != *entry.entity_key() {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            if current_target
                .as_ref()
                .is_some_and(|target| target != entry.target())
            {
                flush(current_target.take(), total, stale, &mut models, revision)?;
                total = 0;
                stale = 0;
                revision = None;
            }
            if current_target.is_none() {
                current_target = Some(entry.target().clone());
            }
            total = total
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if entry.source_stale() {
                stale = stale
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            }
            if let Some(embedding) = entry.embedding_write() {
                if !models.contains_key(embedding.metadata())
                    && models.len() == MAX_VECTOR_MODELS_PER_OBSERVATION
                {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                let count = models.entry(embedding.metadata().clone()).or_default();
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            }
            revision = Some(revision.map_or(entry.evidence_sequence(), |current| {
                current.max(entry.evidence_sequence())
            }));
        }
        flush(current_target.take(), total, stale, &mut models, revision)?;
    }
    drop(index);

    let mut health: Option<riffdb_storage_api::VectorHealthObservationV1> = None;
    for row in observations.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        if crate::keys::decode_vector_health_observation_key(key.value()).is_ok() {
            continue;
        }
        let observation = riffdb_storage_api::decode_vector_observation_v1(value.value())
            .map_err(crate::error::codec_error)?
            .into_parts()
            .0;
        let Some((lineage, specs)) = active.as_ref() else {
            return Err(storage_error(StorageErrorKind::CorruptData));
        };
        if observation.target().lineage() != lineage {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let threshold = specs
            .get(&(
                observation.target().entity_type(),
                observation.target().vector_field(),
            ))
            .copied()
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let summary = health.get_or_insert_with(|| {
            riffdb_storage_api::VectorHealthObservationV1::empty(
                lineage.clone(),
                observation.revision(),
            )
        });
        let summary_revision = summary.revision().max(observation.revision());
        summary
            .apply_partition(
                observation.target().entity_type(),
                observation.target().vector_field(),
                threshold,
                None,
                Some(&observation),
                summary_revision,
            )
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    }
    if let Some(health) = health {
        let (lineage, _) = active
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let key = encode_vector_health_observation_key(lineage)
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        let encoded = riffdb_storage_api::encode_vector_health_observation_v1(&health)
            .map_err(crate::error::codec_error)?;
        if let Some(existing) = observations
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
        {
            let existing =
                riffdb_storage_api::decode_vector_health_observation_v1(existing.value())
                    .map_err(crate::error::codec_error)?;
            if existing.value() != &health {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        } else {
            observations
                .insert(key.as_slice(), encoded.as_bytes())
                .map_err(precommit_storage_error)?;
        }
    }
    drop(observations);
    shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
    shared.commit_durable(transaction)?;
    shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)
}

fn install_validated_prefix_entity_heads_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(VALIDATED_PREFIX_ENTITY_HEADS)
            .map_err(table_error)?,
    );
    shared.commit_durable(transaction)
}

fn migrate_string_table(
    shared: &SharedRedb,
    definition: TableDefinition<&str, &[u8]>,
    require_compact: bool,
) -> Result<(), StorageError> {
    let mut after: Option<String> = None;
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut table = transaction.open_table(definition).map_err(table_error)?;
        let mut replacements = Vec::new();
        let mut last = None;
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0usize;
        let mut reached_end = true;
        {
            let mut rows = match after.as_deref() {
                Some(after) => table
                    .range::<&str>((Excluded(after), Unbounded))
                    .map_err(precommit_storage_error)?,
                None => table.iter().map_err(precommit_storage_error)?,
            };
            for row in &mut rows {
                let (key, value) = row.map_err(precommit_storage_error)?;
                let key = key.value().to_owned();
                let original = value.value();
                scanned_rows = scanned_rows.saturating_add(1);
                // One-shot process markers in META are not durable envelopes.
                if key == META_INDEX_EPOCH_ROWS_REPAIRED {
                    last = Some(key);
                    continue;
                }
                // Optional meta rows: invalid payloads must never block open
                // (startup ignores them / re-validates and falls back safely).
                if key == META_VALIDATED_PREFIX_CHECKPOINT
                    || key == META_RETENTION_WATERMARK
                    || key == META_RETENTION_HOLDS
                {
                    if let Ok(compact) = transcode_durable_record_to_v2(original) {
                        if require_compact && compact.as_bytes() != original {
                            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                        }
                        if compact.as_bytes() != original {
                            replacements.push((key.clone(), compact.into_bytes()));
                        }
                    }
                    last = Some(key);
                    continue;
                }
                let compact =
                    transcode_durable_record_to_v2(original).map_err(crate::error::codec_error)?;
                if require_compact && compact.as_bytes() != original {
                    return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                }
                scanned_bytes = scanned_bytes
                    .checked_add(original.len())
                    .and_then(|total| total.checked_add(compact.as_bytes().len()))
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if compact.as_bytes() != original {
                    replacements.push((key.clone(), compact.into_bytes()));
                }
                last = Some(key);
                if scanned_rows >= FORMAT_MIGRATION_MAX_ROWS
                    || scanned_bytes >= FORMAT_MIGRATION_MAX_BYTES
                {
                    reached_end = false;
                    break;
                }
            }
        }
        let changed = !replacements.is_empty();
        for (key, value) in replacements {
            table
                .insert(key.as_str(), value.as_slice())
                .map_err(precommit_storage_error)?;
        }
        drop(table);
        if changed {
            shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
            shared.commit_durable(transaction)?;
            shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        } else {
            transaction.abort().map_err(precommit_storage_error)?;
        }
        if reached_end {
            return Ok(());
        }
        after = last;
    }
}

fn migrate_byte_table(
    shared: &SharedRedb,
    definition: TableDefinition<&[u8], &[u8]>,
    require_compact: bool,
) -> Result<(), StorageError> {
    let derived_and_rebuildable = matches!(
        definition.name(),
        "outbox_status" | "projection_state" | "projection_frontier" | "projection_applied"
    );
    let mut after: Option<Vec<u8>> = None;
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut table = transaction.open_table(definition).map_err(table_error)?;
        let mut replacements = Vec::new();
        let mut last = None;
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0usize;
        let mut reached_end = true;
        {
            let mut rows = match after.as_deref() {
                Some(after) => table
                    .range::<&[u8]>((Excluded(after), Unbounded))
                    .map_err(precommit_storage_error)?,
                None => table.iter().map_err(precommit_storage_error)?,
            };
            for row in &mut rows {
                let (key, value) = row.map_err(precommit_storage_error)?;
                let key = key.value().to_vec();
                let original = value.value();
                scanned_rows = scanned_rows.saturating_add(1);
                scanned_bytes = scanned_bytes
                    .checked_add(original.len())
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                let compact = match transcode_durable_record_to_v2(original) {
                    Ok(compact) => compact,
                    Err(_) if derived_and_rebuildable => {
                        last = Some(key);
                        if scanned_rows >= FORMAT_MIGRATION_MAX_ROWS
                            || scanned_bytes >= FORMAT_MIGRATION_MAX_BYTES
                        {
                            reached_end = false;
                            break;
                        }
                        continue;
                    }
                    Err(error) => return Err(crate::error::codec_error(error)),
                };
                if require_compact && !derived_and_rebuildable && compact.as_bytes() != original {
                    return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                }
                scanned_bytes = scanned_bytes
                    .checked_add(compact.as_bytes().len())
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
                if compact.as_bytes() != original {
                    replacements.push((key.clone(), compact.into_bytes()));
                }
                last = Some(key);
                if scanned_rows >= FORMAT_MIGRATION_MAX_ROWS
                    || scanned_bytes >= FORMAT_MIGRATION_MAX_BYTES
                {
                    reached_end = false;
                    break;
                }
            }
        }
        let changed = !replacements.is_empty();
        for (key, value) in replacements {
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(precommit_storage_error)?;
        }
        drop(table);
        if changed {
            shared.before_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
            shared.commit_durable(transaction)?;
            shared.after_test_commit(RedbTestOperation::StorageFormatMigrationBatch)?;
        } else {
            transaction.abort().map_err(precommit_storage_error)?;
        }
        if reached_end {
            return Ok(());
        }
        after = last;
    }
}

impl RedbDormantPorts {
    /// Consumes dormant ports after the caller has matched the separate
    /// catalog-owned startup proof.
    ///
    /// Server composition owns the catalog proof join. A pending V3 migration
    /// retains the structural session's lease across that join and through the
    /// atomic activation, DIRTY transition, and operational handoff.
    pub fn into_operational_after_catalog_validation(
        self,
    ) -> Result<RedbOperationalPorts, StorageError> {
        if self.shared.open_mode != OpenMode::Source {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let _lease = self
            .pending_v3_activation
            .map(|pending| pending.activate(&self.shared))
            .transpose()?;
        activate_operational_ports(self.shared)
    }
}

impl RedbStore {
    /// Proves the ADR-0085 Amendment 4 precondition before offline retention
    /// is allowed to open its first write transaction.
    pub(crate) fn prepare_offline_retention(
        &self,
    ) -> Result<RedbOfflineRetentionPreparation<'_>, StorageError> {
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let database_id = read_identity_from_read_transaction(&transaction)?;
        let application_frontier = read_commit_tail(&transaction)?;
        let administration_frontier = read_administration_tail(&transaction)?;
        // Offline maintenance has no preceding operational startup session.
        // Validate the retained V3 source chain before granting its first write.
        if crate::changelog_v3_journal::has_recovery_roots(&transaction)? {
            crate::changelog_v3_roots::validate_retained_history(&transaction)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        }
        drop(transaction);
        let journal = crate::journal::verify_retention_journal_rebase_with_media(
            self.shared.journal_media.as_ref(),
            &self.shared.path,
            database_id,
            application_frontier,
            administration_frontier,
        )
        .map_err(recovery_journal_error)?;
        Ok(RedbOfflineRetentionPreparation {
            database: &self.shared.database,
            _journal: journal,
        })
    }

    /// Releases ports only to the crate-private migration-stage recovery gate.
    pub(crate) fn into_contract_migration_ports(
        self,
    ) -> Result<RedbOperationalPorts, StorageError> {
        activate_operational_ports(self.shared)
    }
}

fn activate_operational_ports(
    shared: Arc<SharedRedb>,
) -> Result<RedbOperationalPorts, StorageError> {
    if shared.open_mode != OpenMode::Source {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    if shared.bounded_clean_startup.load(Ordering::Acquire) {
        let state = shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if !matches!(*state, TransientIndexState::Dormant) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        drop(state);
        return Ok(RedbOperationalPorts { shared });
    }
    let transaction = shared.database.begin_read().map_err(transaction_error)?;
    let (indexes, commit_rows) = TransientIndexes::rebuild_counted(&transaction)?;
    drop(transaction);
    shared.note_transient_index_rebuild(commit_rows);
    let mut state = shared
        .transient_indexes
        .write()
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    if !matches!(*state, TransientIndexState::Dormant) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    *state = TransientIndexState::Ready(indexes);
    drop(state);
    Ok(RedbOperationalPorts { shared })
}

impl RedbOperationalPorts {
    /// Captures the complete published durable view for authorized replication
    /// composition. This releases only a read capability, never the writer.
    pub fn published_changelog_snapshot_v3(
        &self,
    ) -> Result<Arc<dyn riffdb_storage_api::PublishedDurableSnapshot>, StorageError> {
        Ok(Arc::new(crate::changelog::RedbPublishedSnapshot::new(
            self.shared.begin_composite_operational_read()?,
        )))
    }

    /// True when this handle reached readiness through verified bounded
    /// clean-close startup and intentionally left population caches cold.
    #[doc(hidden)]
    pub fn clean_close_fast_startup(&self) -> bool {
        self.shared.bounded_clean_startup.load(Ordering::Acquire)
    }

    pub(crate) fn standard_writer_journal_enabled(&self) -> bool {
        self.shared.application_commit_profile == RedbCommitProfile::Standard
    }

    pub(crate) fn note_fresh_locator_history_fallback_scan(&self, rows: u64) {
        self.shared.note_fresh_locator_history_fallback_scan(rows);
    }

    pub(crate) fn command_derived_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
        let state = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match &*state {
            TransientIndexState::Ready(indexes) => indexes
                .command_segment_coverage()
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable)),
            TransientIndexState::Dormant => Ok(None),
            TransientIndexState::Invalid => Err(storage_error(StorageErrorKind::Unavailable)),
        }
    }

    /// Populates the command-derived index before an administration-audit read.
    ///
    /// A command-owned administration audit member is durable only inside its
    /// command segment: no locator row is written into `AUDIT` for it, so the
    /// command-derived transient index is its only lookup structure. Clean-close
    /// fast startup (ADR-0156 section 5) deliberately leaves that index cold,
    /// and a cold index answers "no record" — which the administration read path
    /// cannot distinguish from a durably absent record, because it pairs the
    /// index against the physical row and treats absence on both sides as
    /// corruption. Leaving the index cold therefore reports an intact database
    /// as corrupt.
    ///
    /// WP-704 requires every integrity fact skipped at clean startup to move
    /// onto the operational path that first uses it, so the first such read
    /// populates the index here — the same deferred population that
    /// `pending_outbox_page` and `partition_event_route_page` already perform.
    /// This narrows what the read path accepts rather than widening it: a
    /// sequence still absent from a populated index remains a corruption
    /// incident, and `Invalid` still fails closed.
    fn ensure_command_audit_index(&self) -> Result<(), StorageError> {
        self.shared.ensure_transient_indexes_ready()
    }

    /// Exclusive-gate tickets ever issued on this database.
    ///
    /// Lets a test prove an operation answered without taking the mutation
    /// gate: `begin_write` and `acquire_indexed_read_lease` each take one
    /// ticket, `begin_read` takes none.
    #[cfg(test)]
    pub(crate) fn mutation_gate_tickets(&self) -> u128 {
        self.shared.mutation_gate.tickets_issued()
    }

    /// Compatibility checkpoint writer retained for existing callers.
    ///
    /// Gated exactly like the startup write: returns `Ok(false)` without
    /// writing unless this database's startup validation completed with zero
    /// structural findings of any scope.
    #[doc(hidden)]
    pub fn write_validated_prefix_checkpoint(&self) -> Result<bool, StorageError> {
        RedbStore {
            shared: Arc::clone(&self.shared),
        }
        .write_validated_prefix_checkpoint()
    }

    /// Writes the final private clean-close lifecycle certificate.
    pub fn write_clean_close_lifecycle(&self) -> Result<(), StorageError> {
        RedbStore {
            shared: Arc::clone(&self.shared),
        }
        .write_clean_close_lifecycle()
    }

    /// Performs the bounded graceful-close sequence and returns only closed,
    /// redaction-safe process evidence.
    #[doc(hidden)]
    #[must_use]
    pub fn complete_graceful_close(&self) -> crate::GracefulCheckpointCloseReceiptV1 {
        RedbStore {
            shared: Arc::clone(&self.shared),
        }
        .complete_graceful_close()
    }

    /// Failed checkpoint writes after clean validation (non-fatal; counted).
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_write_failures(&self) -> u64 {
        self.shared.checkpoint_write_failures()
    }

    /// Failed clean-close certificate writes on this database (non-fatal; counted).
    #[doc(hidden)]
    #[must_use]
    pub fn clean_close_write_failures(&self) -> u64 {
        self.shared.clean_close_write_failures()
    }

    /// Per-reason counts of declined ADR-0157 bounded startups on this database.
    ///
    /// Nonzero at exactly one index after an open that took the complete
    /// validation pass, naming which precondition closed the bounded gate.
    #[doc(hidden)]
    #[must_use]
    pub fn clean_close_decline_counts(&self) -> [(&'static str, u64); 11] {
        self.shared.clean_close_decline_counts()
    }

    /// True only when bounded startup PROVED no in-flight `Delivering` outbox
    /// entry exists at the startup snapshot.
    ///
    /// Callers may skip outbox normalization on the readiness path only on this
    /// proof. It is false on the complete path and false whenever the bounded
    /// probe could not decide, so ignorance never reads as proof.
    #[doc(hidden)]
    #[must_use]
    pub fn outbox_delivering_proven_absent(&self) -> bool {
        self.shared.outbox_delivering_proven_absent()
    }

    /// Transient population-index rebuilds performed on this database.
    ///
    /// Expected to be zero for the whole life of a handle that reached
    /// readiness through bounded clean-close startup. A nonzero value means the
    /// bounded path's saving was given back by a later derived read.
    #[doc(hidden)]
    #[must_use]
    pub fn transient_index_rebuilds(&self) -> u64 {
        self.shared.transient_index_rebuilds()
    }

    /// `COMMITS` rows walked by transient population-index rebuilds.
    #[doc(hidden)]
    #[must_use]
    pub fn transient_index_commit_rows(&self) -> u64 {
        self.shared.transient_index_commit_rows()
    }

    /// Per-reason counts of ignored validated-prefix checkpoints on this database.
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_ignore_counts(&self) -> [(&'static str, u64); 10] {
        self.shared.checkpoint_ignore_counts()
    }

    /// History rows iterated to compute checkpoint counts on this database.
    ///
    /// Stays zero across a whole process, including the graceful-shutdown write:
    /// checkpoint counts come from table metadata, never from a history pass.
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_count_rows_walked(&self) -> u64 {
        self.shared.checkpoint_count_rows_walked()
    }

    /// Terminal `ExecutionFailed` rows counted on this database since open.
    #[doc(hidden)]
    #[must_use]
    pub fn terminal_execution_failure_rows(&self) -> u64 {
        self.shared.terminal_execution_failure_rows()
    }

    /// Returns a cloneable pure-read handle over the same activated database.
    ///
    /// Mutation exclusion is the exclusive mutation gate, not handle uniqueness.
    /// The shared handle implements only `&self` storage ports.
    #[must_use]
    pub fn shared_ports(&self) -> crate::shared_ports::RedbSharedPorts {
        crate::shared_ports::RedbSharedPorts::new(Arc::clone(&self.shared))
    }

    pub(crate) fn begin_read(&self) -> Result<RedbReadAccess, StorageError> {
        self.shared.begin_operational_read()
    }

    pub(crate) fn begin_composite_read(&self) -> Result<RedbReadAccess, StorageError> {
        self.shared.begin_composite_operational_read()
    }

    /// Read transaction over the derived-state sidecar. The primary writer does
    /// not wait for this transaction. A missing sidecar is an empty read, not a
    /// create, so a read-only mount does not grow a sibling file.
    pub(crate) fn begin_derived_read(&self) -> Result<redb::ReadTransaction, StorageError> {
        if let Some(database) = self.shared.derived.get() {
            return database.begin_read().map_err(transaction_error);
        }
        if !self.shared.derived_in_memory && !derived_store_path(&self.shared.path).exists() {
            return empty_derived_read();
        }
        self.shared
            .derived_database()?
            .begin_read()
            .map_err(transaction_error)
    }

    /// Write transaction over the derived-state sidecar. Does not acquire the
    /// primary mutation gate or the primary store's write transaction.
    pub(crate) fn begin_derived_write(&self) -> Result<DerivedWriteAccess, StorageError> {
        let mut transaction = self
            .shared
            .derived_database()?
            .begin_write()
            .map_err(transaction_error)?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        Ok(DerivedWriteAccess {
            shared: Arc::clone(&self.shared),
            transaction: Some(transaction),
        })
    }

    #[cfg(any(test, feature = "test-fixtures", feature = "benchmark-support"))]
    pub(crate) fn begin_write(&self) -> Result<RedbWriteAccess, StorageError> {
        self.begin_attributed_write(
            riffdb_storage_api::ChangelogAttributionV3::CatalogAdministration,
        )
    }

    pub(crate) fn begin_attributed_write(
        &self,
        source: riffdb_storage_api::ChangelogAttributionV3,
    ) -> Result<RedbWriteAccess, StorageError> {
        crate::changelog_v3_write::require_direct_attribution(source)?;
        let lease = self.shared.mutation_gate.acquire()?;
        if self.shared.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        self.shared.publish_pending_journal_prefix()?;
        self.shared.poll_async_checkpoint_locked(true)?;
        let journal_checkpoint = self.shared.take_published_journal_suffix_locked(true)?;
        let mut transaction = match self.shared.database.begin_write() {
            Ok(transaction) => transaction,
            Err(error) => {
                if let Some(runtime) = journal_checkpoint {
                    self.shared.restore_journal_runtime(runtime)?;
                }
                return Err(transaction_error(error));
            }
        };
        transaction.set_two_phase_commit(self.shared.application_commit_profile.uses_two_phase());
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Some(runtime) = journal_checkpoint.as_ref()
            && let Err(error) = self
                .shared
                .apply_published_journal_suffix(&transaction, runtime)
        {
            let _ = transaction.abort();
            if let Some(runtime) = journal_checkpoint {
                let _ = self.shared.restore_journal_runtime(runtime);
            }
            self.shared.fence_writes();
            return Err(error);
        }
        let transaction = match OperationalWriteTransaction::from_drained(transaction, source) {
            Ok(transaction) => transaction,
            Err(error) => {
                if let Some(runtime) = journal_checkpoint {
                    self.shared.restore_journal_runtime(runtime)?;
                }
                self.shared.fence_writes();
                return Err(error);
            }
        };
        Ok(RedbWriteAccess {
            shared: Arc::clone(&self.shared),
            transaction: Some(transaction),
            direct_changelog_attribution: Some(source),
            ownership: Some(RedbWriteOwnership::Direct { _lease: lease }),
            journal_mutations: None,
            journal_mutation_start: 0,
            journal_checkpoint,
            composite_predecessor: None,
            composite_stage: None,
            command_prefix_capture: RefCell::default(),
        })
    }

    pub(crate) fn begin_deferred_epoch(&self) -> Result<RedbDurabilityEpoch, StorageError> {
        let lease = self.shared.mutation_gate.acquire()?;
        if self.shared.write_fenced.load(Ordering::Acquire)
            || self.shared.application_commit_profile != RedbCommitProfile::Standard
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        self.shared.poll_async_checkpoint_locked(false)?;
        self.shared.maybe_start_async_checkpoint()?;
        let mut frontier = self
            .shared
            .durable_read_frontier
            .write()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if frontier.is_none() {
            let transaction = self.shared.capture_checkpoint_root()?;
            *frontier = Some(transaction);
            // Still under the write guard, so no frontier-free capture can be
            // in flight and none can begin: every later read takes the durable
            // branch instead.
            self.shared.retire_current_read_root();
        }
        drop(frontier);
        let composite_predecessor = self.shared.capture_or_initialize_composite_view()?;
        let composite_stage = crate::composite_view::RedbCompositeMutationStage::from_published(
            &composite_predecessor,
        );
        Ok(RedbDurabilityEpoch {
            shared: Arc::clone(&self.shared),
            lease: Some(lease),
            applied: Vec::new(),
            transient_deltas: Vec::new(),
            command_count: 0,
            last_sequence: None,
            semantic_bytes: 0,
            reserved_encoded_bytes: 0,
            journal_mutations: crate::journal::JournalMutationBuffer::default(),
            journal_mutation_groups: 0,
            composite_predecessor,
            composite_stage: Some(composite_stage),
            completed: false,
        })
    }

    pub(crate) fn begin_deferred_service_audit_write(
        &self,
    ) -> Result<RedbWriteAccess, StorageError> {
        let lease = self.shared.mutation_gate.acquire()?;
        if self.shared.write_fenced.load(Ordering::Acquire)
            || self.shared.application_commit_profile != RedbCommitProfile::Standard
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        self.shared.poll_async_checkpoint_locked(false)?;
        self.shared.maybe_start_async_checkpoint()?;
        let mut frontier = self
            .shared
            .durable_read_frontier
            .write()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if frontier.is_none() {
            *frontier = Some(self.shared.capture_checkpoint_root()?);
            self.shared.retire_current_read_root();
        }
        drop(frontier);
        let composite_predecessor = self.shared.capture_or_initialize_composite_view()?;
        let composite_stage = crate::composite_view::RedbCompositeMutationStage::from_published(
            &composite_predecessor,
        );
        Ok(RedbWriteAccess {
            shared: Arc::clone(&self.shared),
            transaction: None,
            ownership: Some(RedbWriteOwnership::ServiceAudit { _lease: lease }),
            direct_changelog_attribution: None,
            journal_mutations: Some(RefCell::new(
                crate::journal::JournalMutationBuffer::default(),
            )),
            journal_mutation_start: 0,
            journal_checkpoint: None,
            composite_predecessor: Some(composite_predecessor),
            composite_stage: Some(RefCell::new(composite_stage)),
            command_prefix_capture: RefCell::default(),
        })
    }

    pub(crate) fn acquire_indexed_read_lease(&self) -> Result<RedbIndexedReadLease, StorageError> {
        let frontier = self
            .shared
            .durable_read_frontier
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if frontier.is_some() {
            return Ok(RedbIndexedReadLease::DurableEpoch);
        }
        drop(frontier);
        self.shared
            .mutation_gate
            .acquire()
            .map(|lease| RedbIndexedReadLease::Direct { _lease: lease })
    }

    pub(crate) fn pending_outbox_page(
        &self,
        after: Option<riffdb_types::EventId>,
        limit: usize,
    ) -> Result<(Vec<riffdb_types::EventId>, bool), StorageError> {
        self.shared.pending_outbox_page(after, limit)
    }

    pub(crate) fn undelivered_outbox_page(
        &self,
        after: Option<riffdb_types::EventId>,
        limit: usize,
    ) -> Result<(Vec<riffdb_types::EventId>, bool), StorageError> {
        self.shared.undelivered_outbox_page(after, limit)
    }

    pub(crate) fn outbox_intent_last(&self) -> Result<Option<riffdb_types::EventId>, StorageError> {
        self.shared.ensure_transient_indexes_ready()?;
        let state = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match &*state {
            TransientIndexState::Ready(indexes) => indexes
                .outbox_intent_last()
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable)),
            TransientIndexState::Dormant | TransientIndexState::Invalid => {
                Err(storage_error(StorageErrorKind::Unavailable))
            }
        }
    }

    pub(crate) fn partition_event_route_page(
        &self,
        partition_hash: riffdb_types::PartitionKeyHash,
        after: Option<riffdb_types::EventId>,
        requested_upper: Option<riffdb_types::EventId>,
        limit: usize,
    ) -> Result<
        (
            riffdb_storage_api::EventRouteUpperFenceV1,
            Vec<riffdb_storage_api::StoredEventRouteV1>,
            bool,
        ),
        StorageError,
    > {
        let state = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let TransientIndexState::Ready(indexes) = &*state {
            return indexes
                .partition_event_route_page(partition_hash, after, requested_upper, limit)
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        }
        if matches!(*state, TransientIndexState::Invalid) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        drop(state);
        if !self.shared.bounded_clean_startup.load(Ordering::Acquire) {
            // Dormant low-level conformance handles retain the historical
            // private exact-view behavior; only a verified clean operational
            // handle installs a deferred shared accelerator.
            let transaction = self.begin_read()?;
            let indexes = TransientIndexes::rebuild(&transaction)?;
            return indexes
                .partition_event_route_page(partition_hash, after, requested_upper, limit)
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        }
        self.shared.ensure_transient_indexes_ready()?;
        let state = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let TransientIndexState::Ready(indexes) = &*state else {
            return Err(storage_error(StorageErrorKind::Unavailable));
        };
        indexes
            .partition_event_route_page(partition_hash, after, requested_upper, limit)
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?
    }
}

impl RedbWriteAccess {
    pub(crate) fn begin_command_prefix_capture(&self, budget: usize) -> Result<(), StorageError> {
        self.command_prefix_capture
            .try_borrow_mut()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .begin(budget)
    }

    pub(crate) fn finish_command_prefix_capture(
        &self,
    ) -> Result<Vec<riffdb_storage_api::AuthoritativeMutationV3>, StorageError> {
        self.command_prefix_capture
            .try_borrow_mut()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .finish()
    }

    pub(crate) fn capture_logical_command_mutation(
        &self,
        mutation: &crate::journal::JournalMutation,
    ) -> Result<(), StorageError> {
        self.command_prefix_capture
            .try_borrow_mut()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .record(mutation)
    }

    fn record_command_mutation(
        &self,
        mutation: &crate::journal::JournalMutation,
    ) -> Result<(), StorageError> {
        self.capture_logical_command_mutation(mutation)
    }

    pub(crate) fn prepare_command_segment_capsules(
        &self,
        capsules: Vec<riffdb_storage_api::StoredCommandCapsuleV2>,
        wire_version: riffdb_storage_api::CommandCapsuleWireVersionV1,
    ) -> Result<
        (
            Vec<riffdb_storage_api::StoredCommandCapsuleV2>,
            Option<Vec<riffdb_storage_api::PreparedCommandSegmentCapsuleV1>>,
        ),
        StorageError,
    > {
        self.shared
            .command_segment_preparation
            .prepare(capsules, wire_version)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
    }

    pub(crate) fn command_segment_tail(
        &self,
    ) -> Result<Option<Option<(CommitSequence, CommandSegmentDigestV1)>>, StorageError> {
        let transient = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let published = match &*transient {
            TransientIndexState::Ready(indexes) => indexes
                .command_segment_tail()
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?,
            TransientIndexState::Dormant => return Ok(None),
            TransientIndexState::Invalid => {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
        };
        let unpublished = self
            .shared
            .unpublished_command_indexes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .command_segment_tail();
        let epoch = self
            .ownership
            .as_ref()
            .and_then(|ownership| match ownership {
                RedbWriteOwnership::Epoch(epoch) => epoch
                    .transient_deltas
                    .last()
                    .and_then(TransientIndexDelta::command_segment)
                    .map(|segment| (segment.first_commit_sequence(), segment.segment_digest())),
                RedbWriteOwnership::Direct { .. } | RedbWriteOwnership::ServiceAudit { .. } => None,
            });
        drop(transient);
        let earlier = match (published, unpublished) {
            (Some(published), Some(unpublished)) => {
                if unpublished.0 <= published.0 {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                Some(unpublished)
            }
            (Some(published), None) => Some(published),
            (None, Some(unpublished)) => Some(unpublished),
            (None, None) => None,
        };
        if let (Some(earlier), Some(epoch)) = (earlier, epoch)
            && epoch.0 <= earlier.0
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(Some(epoch.or(earlier)))
    }

    pub(crate) fn command_derived_key_exists(
        &self,
        kind: riffdb_storage_api::CommandDerivedIndexKindV1,
        exact_key: &[u8],
    ) -> Result<bool, StorageError> {
        self.command_derived_member(kind, exact_key)
            .map(|value| value.is_some())
    }

    /// True when the transient population indexes are dormant, which is what a
    /// bounded clean-close start leaves them.
    ///
    /// A write-path caller cannot warm them: it already holds the mutation
    /// lease and `ExclusiveGate` is a non-reentrant ticket lock. So a caller
    /// that must not manufacture absence uses this to select a durable fallback
    /// for the dormant case ONLY, leaving the warm case -- where an index miss
    /// is a real, meaningful answer such as "not published yet" -- untouched.
    pub(crate) fn transient_indexes_dormant(&self) -> Result<bool, StorageError> {
        let state = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        Ok(matches!(*state, TransientIndexState::Dormant))
    }

    pub(crate) const fn retains_journal_mutations(&self) -> bool {
        self.journal_mutations.is_some()
    }

    pub(crate) fn record_journal_mutations(
        &self,
        mutations: Vec<crate::journal::JournalMutation>,
    ) -> Result<(), StorageError> {
        let Some(retained) = self.journal_mutations.as_ref() else {
            return Ok(());
        };
        if let Some(stage) = self.composite_stage.as_ref() {
            let mut stage = stage
                .try_borrow_mut()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            for mutation in &mutations {
                stage.apply(mutation)?;
            }
        }
        retained
            .try_borrow_mut()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .extend(mutations)
            .map_err(journal_storage_error)?;
        Ok(())
    }

    fn record_observed_journal_mutation(
        &self,
        mutation: crate::journal::JournalMutation,
        observed_current: Option<&[u8]>,
    ) -> Result<(), StorageError> {
        let Some(retained) = self.journal_mutations.as_ref() else {
            return Ok(());
        };
        if let Some(stage) = self.composite_stage.as_ref() {
            stage
                .try_borrow_mut()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .apply_with_observed_current(&mutation, observed_current)?;
        }
        retained
            .try_borrow_mut()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .extend([mutation])
            .map_err(journal_storage_error)
    }

    pub(crate) fn put_proven_command_value(
        &self,
        table: crate::journal::JournalTable,
        key: Vec<u8>,
        proven_current: Option<Vec<u8>>,
        value: riffdb_storage_api::CanonicalStoredEnvelopeV1,
    ) -> Result<(), StorageError> {
        let mutation = match proven_current.as_deref() {
            Some(prior) => {
                crate::journal::JournalMutation::replace(table, key, prior, value.into_bytes())
            }
            None => crate::journal::JournalMutation::put(table, key, value.into_bytes()),
        }
        .map_err(journal_storage_error)?;
        self.record_command_mutation(&mutation)?;
        if let Some(transaction) = self.transaction.as_ref() {
            transaction.apply_journal_mutation(&mutation)?;
        }
        if let Some(stage) = self.composite_stage.as_ref() {
            stage
                .try_borrow_mut()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .apply_with_proven_current(&mutation, proven_current.as_deref())?;
        }
        if let Some(retained) = self.journal_mutations.as_ref() {
            retained
                .try_borrow_mut()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .extend([mutation])
                .map_err(journal_storage_error)?;
        }
        Ok(())
    }

    pub(crate) fn delete_proven_command_value(
        &self,
        table: crate::journal::JournalTable,
        key: Vec<u8>,
        proven_current: Vec<u8>,
    ) -> Result<(), StorageError> {
        let mutation =
            crate::journal::JournalMutation::delete_matching(table, key, &proven_current)
                .map_err(journal_storage_error)?;
        self.record_command_mutation(&mutation)?;
        if let Some(transaction) = self.transaction.as_ref() {
            transaction.apply_journal_mutation(&mutation)?;
        }
        if let Some(stage) = self.composite_stage.as_ref() {
            stage
                .try_borrow_mut()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .apply_with_proven_current(&mutation, Some(&proven_current))?;
        }
        if let Some(retained) = self.journal_mutations.as_ref() {
            retained
                .try_borrow_mut()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .extend([mutation])
                .map_err(journal_storage_error)?;
        }
        Ok(())
    }

    pub(crate) fn transaction(&self) -> Result<&OperationalWriteTransaction, StorageError> {
        self.transaction
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    pub(crate) fn read_command_value(
        &self,
        table: crate::journal::JournalTable,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(stage) = self.composite_stage.as_ref() {
            return stage
                .try_borrow()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .resolve_point(table.composite(), key);
        }
        self.transaction()?
            .read_command_value(table, key)
            .map_err(journal_io_error)
    }

    pub(crate) fn read_command_capability_bytes(
        &self,
        capability_id: riffdb_types::CapabilityId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let key = crate::keys::encode_capability_key(capability_id);
        if let Some(stage) = self.composite_stage.as_ref() {
            return stage
                .try_borrow()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .read_capability_bytes(key.as_slice());
        }
        let table = self
            .transaction()?
            .open_table(crate::layout::CAPABILITIES)
            .map_err(table_error)?;
        table
            .get(key.as_slice())
            .map_err(precommit_storage_error)
            .map(|value| value.map(|value| value.value().to_vec()))
    }

    pub(crate) fn put_command_value(
        &self,
        table: crate::journal::JournalTable,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let prior = self.read_command_value(table, &key)?;
        let mutation = match prior.as_deref() {
            Some(prior) => crate::journal::JournalMutation::replace(table, key, prior, value),
            None => crate::journal::JournalMutation::put(table, key, value),
        }
        .map_err(journal_storage_error)?;
        self.record_command_mutation(&mutation)?;
        if let Some(transaction) = self.transaction.as_ref() {
            transaction.apply_journal_mutation(&mutation)?;
        }
        self.record_observed_journal_mutation(mutation, prior.as_deref())?;
        Ok(prior)
    }

    /// Inserts a durable row whose key is new by construction, without the
    /// `read_command_value` pre-read `put_command_value` performs.
    ///
    /// Used for the ADR-0165 locator rows. Each key is proven new before this is
    /// called: an idempotency identity key by the admission reservation, a
    /// provenance id by its uniqueness reservation, and an audit-by-request key
    /// by a freshly allocated administration sequence that cannot collide.
    /// Paying a read per row to re-establish that measured 12-16% of seed
    /// throughput on its own.
    ///
    /// The mutation is recorded on the journal, so the row joins the same frame
    /// and the same fsync as the command segment it describes.
    pub(crate) fn put_command_value_assuming_absent(
        &self,
        table: crate::journal::JournalTable,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), StorageError> {
        let mutation = crate::journal::JournalMutation::put(table, key, value)
            .map_err(journal_storage_error)?;
        self.record_command_mutation(&mutation)?;
        if let Some(transaction) = self.transaction.as_ref() {
            transaction.apply_journal_mutation(&mutation)?;
        }
        self.record_observed_journal_mutation(mutation, None)
    }

    pub(crate) fn delete_command_value(
        &self,
        table: crate::journal::JournalTable,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(prior) = self.read_command_value(table, &key)? else {
            return Ok(None);
        };
        let mutation = crate::journal::JournalMutation::delete_matching(table, key, &prior)
            .map_err(journal_storage_error)?;
        self.record_command_mutation(&mutation)?;
        if let Some(transaction) = self.transaction.as_ref() {
            transaction.apply_journal_mutation(&mutation)?;
        }
        self.record_observed_journal_mutation(mutation, Some(&prior))?;
        Ok(Some(prior))
    }

    pub(crate) fn put_command_segment_value(
        &self,
        key: Vec<u8>,
        value: riffdb_storage_api::CanonicalStoredEnvelopeV1,
        command_count: usize,
        raw_envelope_bytes: usize,
    ) -> Result<bool, StorageError> {
        if self
            .read_command_value(crate::journal::JournalTable::Commits, &key)?
            .is_some()
        {
            return Ok(false);
        }
        let value = value.into_bytes();
        let mutation = crate::journal::JournalMutation::put(
            crate::journal::JournalTable::Commits,
            key.clone(),
            value.clone(),
        )
        .map_err(journal_storage_error)?;
        self.record_command_mutation(&mutation)?;
        if let Some(transaction) = self.transaction.as_ref() {
            transaction.apply_journal_mutation(&mutation)?;
        }
        if let Some(stage) = self.composite_stage.as_ref() {
            stage
                .try_borrow_mut()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .apply_with_proven_current(&mutation, None)?;
        }
        if let Some(retained) = self.journal_mutations.as_ref() {
            retained
                .try_borrow_mut()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .push_command_segment(key, value, command_count, raw_envelope_bytes)
                .map_err(journal_storage_error)?;
        }
        Ok(true)
    }

    pub(crate) fn read_command_range(
        &self,
        table: crate::journal::JournalTable,
        start_inclusive: &[u8],
        end_exclusive: &[u8],
        max_rows: usize,
    ) -> Result<Vec<riffdb_storage_api::CompositeRow>, StorageError> {
        if max_rows == 0 {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Some(stage) = self.composite_stage.as_ref() {
            return stage
                .try_borrow()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .merge_bounded(
                    table.composite(),
                    start_inclusive,
                    Some(end_exclusive),
                    max_rows,
                    max_rows,
                )
                .map(|page| page.rows().to_vec());
        }
        let definition = crate::journal::byte_table_definition(table)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let table = self
            .transaction()?
            .open_table(definition)
            .map_err(table_error)?;
        table
            .range::<&[u8]>((
                std::ops::Bound::Included(start_inclusive),
                std::ops::Bound::Excluded(end_exclusive),
            ))
            .map_err(precommit_storage_error)?
            .take(max_rows)
            .map(|row| {
                row.map(|(key, value)| {
                    (
                        key.value().to_vec().into_boxed_slice(),
                        value.value().to_vec().into_boxed_slice(),
                    )
                })
                .map_err(precommit_storage_error)
            })
            .collect()
    }

    pub(crate) fn read_checkpoint_byte_value(
        &self,
        definition: TableDefinition<'static, &'static [u8], &'static [u8]>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(stage) = self.composite_stage.as_ref() {
            return stage
                .try_borrow()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .read_checkpoint_bytes(definition, key);
        }
        self.transaction()?
            .open_table(definition)
            .map_err(table_error)?
            .get(key)
            .map_err(precommit_storage_error)
            .map(|value| value.map(|value| value.value().to_vec()))
    }

    #[allow(
        dead_code,
        reason = "WP-070 administration and derived ports migrate to named commits"
    )]
    pub(crate) fn commit(self) -> Result<(), StorageError> {
        self.commit_for(RedbTestOperation::CommandBatch)
    }

    pub(crate) fn commit_for(self, operation: RedbTestOperation) -> Result<(), StorageError> {
        self.commit_with_observations(operation, None, 0)
    }

    /// Commits one execution-failure terminalization, counting the terminal
    /// `ExecutionFailed` row it adds to `IDEMPOTENCY`.
    ///
    /// The census must advance under the same mutation lease that made the row
    /// durable: a validated-prefix checkpoint write acquires that lease, and one
    /// acquired between the commit and the increment would read the new row from
    /// redb's row count without its census entry and record an
    /// `idempotency_count` one too high. Because this is the only lane that
    /// writes such a row, this is the only counting site.
    pub(crate) fn commit_execution_failure(self) -> Result<(), StorageError> {
        self.commit_with_observations(RedbTestOperation::ExecutionFailure, None, 1)
    }

    pub(crate) fn commit_for_with_delta(
        self,
        operation: RedbTestOperation,
        delta: Option<TransientIndexDelta>,
    ) -> Result<(), StorageError> {
        self.commit_with_observations(operation, delta, 0)
    }

    /// The redb-`Immediate` control-plane commit lane.
    ///
    /// With V3 roots this seals an exact captured receipt before the existing
    /// commit and durable-frontier refresh. It does not notify the legacy
    /// ADR-0100 frame observer; WP-746's V3-only publication integration must
    /// consume these durable receipts rather than treating V1/V2 as complete.
    fn commit_with_observations(
        mut self,
        operation: RedbTestOperation,
        delta: Option<TransientIndexDelta>,
        execution_failure_rows: u64,
    ) -> Result<(), StorageError> {
        if !matches!(
            self.ownership.as_ref(),
            Some(RedbWriteOwnership::Direct { .. })
        ) || self.direct_changelog_attribution.is_none()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Some(controller) = &self.shared.test_controller {
            controller.before_commit(operation)?;
        }
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let prepared = transaction.finish().inspect_err(|_| {
            // Receipt refusal drops/aborts this transaction before any commit.
            // No transient delta has been applied; keep the valid published
            // indexes and let Drop restore the original journal source.
        })?;
        if let Err(error) = prepared.commit(&self.shared) {
            self.invalidate_transient_indexes();
            // `commit_durable` fences writes on any commit error, and a fenced
            // handle can no longer pass `ensure_writable`, so no checkpoint is
            // ever built from the census after a commit whose rows may or may
            // not have landed.
            return Err(error);
        }
        if let Err(error) = self.shared.refresh_durable_read_frontier() {
            self.shared.fence_writes();
            self.invalidate_transient_indexes();
            return Err(error);
        }
        if let Some(runtime) = self.journal_checkpoint.take()
            && let Err(error) = self.shared.finish_journal_checkpoint(runtime)
        {
            self.shared.fence_writes();
            self.invalidate_transient_indexes();
            return Err(error);
        }
        for _ in 0..execution_failure_rows {
            self.shared.note_terminal_execution_failure_row();
        }
        if let Some(delta) = delta
            && let Ok(mut state) = self.shared.transient_indexes.write()
        {
            state.apply_delta(delta);
        }
        if let Some(controller) = &self.shared.test_controller
            && let Err(error) = controller.after_commit(operation)
        {
            self.shared.write_fenced.store(true, Ordering::Release);
            return Err(error);
        }
        self.shared.observe_changelog_checkpoint_v3();
        Ok(())
    }

    pub(crate) fn apply_unpublished(
        mut self,
        applied: riffdb_storage_api::UnpublishedAuditedBatchV1,
        metrics: riffdb_storage_api::StagedBatchMetrics,
        delta: Option<TransientIndexDelta>,
    ) -> Result<RedbDurabilityEpoch, StorageError> {
        let Some(RedbWriteOwnership::Epoch(epoch)) = self.ownership.take() else {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        };
        let mut epoch = *epoch;
        let journal_mutations = self
            .journal_mutations
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .into_inner();
        let composite_stage = self
            .composite_stage
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .into_inner();
        if journal_mutations.mutation_count() == self.journal_mutation_start {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if usize::try_from(journal_mutations.mutation_count()).ok()
            != Some(composite_stage.mutation_count())
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if epoch
            .last_sequence
            .is_some_and(|prior| prior.checked_next() != Some(applied.first_commit_sequence()))
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let (next_count, next_semantic, next_reserved) = checked_epoch_totals(
            epoch.command_count,
            epoch.semantic_bytes,
            epoch.reserved_encoded_bytes,
            applied.command_count(),
            metrics,
        )?;
        if let Some(controller) = &self.shared.test_controller {
            controller.before_commit(RedbTestOperation::DeferredCommandBatch)?;
        }
        if self.transaction.take().is_some() {
            self.shared.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Some(controller) = &self.shared.test_controller
            && let Err(error) = controller.after_commit(RedbTestOperation::DeferredCommandBatch)
        {
            self.shared.fence_writes();
            return Err(error);
        }
        epoch.command_count = next_count;
        epoch.last_sequence = Some(applied.last_commit_sequence());
        epoch.semantic_bytes = next_semantic;
        epoch.reserved_encoded_bytes = next_reserved;
        epoch.journal_mutations = journal_mutations;
        epoch.composite_stage = Some(composite_stage);
        epoch.journal_mutation_groups = epoch
            .journal_mutation_groups
            .checked_add(1)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        epoch.applied.push(applied);
        if let Some(delta) = delta {
            epoch.transient_deltas.push(delta);
        }
        Ok(epoch)
    }

    pub(crate) fn submit_service_audit(
        mut self,
        results: Vec<riffdb_storage_api::ServiceAuditAppendResult>,
    ) -> Result<RedbSubmittedServiceAuditFence, StorageError> {
        let ownership = self
            .ownership
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if !matches!(ownership, RedbWriteOwnership::ServiceAudit { .. }) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let transition_count = results
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    riffdb_storage_api::ServiceAuditAppendResult::Appended(_)
                )
            })
            .count();
        if transition_count == 0
            || transition_count > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let changelog_predecessor = self.shared.stage_changelog_allocator(
            &mut self
                .journal_mutations
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .borrow_mut(),
            &mut self
                .composite_stage
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .borrow_mut(),
        )?;
        let prospective_encoded_bytes = self
            .journal_mutations
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .borrow()
            .encoded_frame_len()
            .map_err(journal_storage_error)?;
        let prospective_physical_bytes =
            crate::journal::extent_frame_bytes(prospective_encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let checkpoint_rebased = self.shared.ensure_journal_frame_headroom(
            transition_count,
            prospective_encoded_bytes,
            prospective_physical_bytes,
        )?;
        if checkpoint_rebased {
            self.composite_predecessor = Some(self.shared.capture_or_initialize_composite_view()?);
        }
        let mutations = self
            .journal_mutations
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .into_inner();
        let mut composite_stage = self
            .composite_stage
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .into_inner();
        let composite_predecessor = self
            .composite_predecessor
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if mutations.mutation_count() == self.journal_mutation_start {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if usize::try_from(mutations.mutation_count()).ok()
            != Some(composite_stage.mutation_count())
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Some(controller) = &self.shared.test_controller {
            controller.before_commit(RedbTestOperation::ServiceAudit)?;
        }
        if self.transaction.take().is_some() {
            self.shared.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let covered_sequence = composite_predecessor.overlay().published_application();
        let covered_administration_sequence = results
            .iter()
            .filter_map(|result| match result {
                riffdb_storage_api::ServiceAuditAppendResult::Appended(record) => {
                    Some(record.administration_sequence())
                }
                riffdb_storage_api::ServiceAuditAppendResult::PhaseConflict => None,
            })
            .next_back();
        let (
            receipt,
            encoded_bytes,
            predecessor_sequence,
            predecessor_administration_sequence,
            composite_successor,
        ) = {
            let (checkpoint_transitions, checkpoint_bytes) =
                self.shared.async_checkpoint_charge()?;
            let mut runtime_guard = self.shared.journal_runtime()?;
            let runtime = runtime_guard
                .as_mut()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            if covered_sequence != runtime.last_sequence {
                self.shared.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let audit_count = u16::try_from(transition_count)
                .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
            let predecessor_sequence = runtime.last_sequence;
            let predecessor_administration_sequence = runtime.last_administration_sequence;
            let frame = crate::journal::JournalFrame::encode_buffered_service_audit(
                runtime.database_id,
                predecessor_sequence,
                predecessor_administration_sequence,
                covered_administration_sequence,
                audit_count,
                runtime.last_hash,
                mutations,
            )
            .map_err(journal_storage_error)?;
            let encoded_bytes = frame.as_bytes().len();
            if encoded_bytes != prospective_encoded_bytes {
                self.shared.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            if checkpoint_rebased {
                composite_stage = self
                    .shared
                    .rebuild_composite_stage_for_frame(&composite_predecessor, &frame)?;
            }
            let physical_bytes = crate::journal::extent_frame_bytes(encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_transitions = runtime
                .suffix_transitions
                .checked_add(transition_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_audits = runtime
                .suffix_audits
                .checked_add(transition_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_bytes = runtime
                .suffix_bytes
                .checked_add(encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_physical_bytes = runtime
                .suffix_physical_bytes
                .checked_add(physical_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_transitions = runtime
                .unpublished_transitions
                .checked_add(transition_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_audits = runtime
                .unpublished_audits
                .checked_add(transition_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_bytes = runtime
                .unpublished_bytes
                .checked_add(encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if checkpoint_transitions.saturating_add(next_transitions)
                > crate::journal::MAX_JOURNAL_SUFFIX_TRANSITIONS
                || checkpoint_bytes.saturating_add(next_bytes)
                    > crate::journal::MAX_JOURNAL_SUFFIX_BYTES
                || next_physical_bytes > crate::journal::EXTENT_DATA_BYTES
                || next_unpublished_transitions > crate::journal::MAX_JOURNAL_TRANSITIONS
                || next_unpublished_bytes > crate::journal::MAX_JOURNAL_FRAME_BYTES
            {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let frame_hash = frame.frame_hash();
            let previous_hash = runtime.last_hash;
            let (mut composite_successor, checkpoint_mutations) = composite_stage
                .seal_encoded_frame_with_mutations(
                    riffdb_storage_api::CompositeFrameKindV1::ServiceAudit,
                    runtime.database_id,
                    predecessor_sequence,
                    predecessor_sequence,
                    predecessor_administration_sequence,
                    covered_administration_sequence,
                    audit_count,
                    encoded_bytes,
                    previous_hash,
                    frame_hash,
                )?;
            let checkpoint_mutations: Arc<[riffdb_storage_api::CompositeMutationV1]> =
                Arc::from(checkpoint_mutations);
            let changelog_successor = runtime.prepare_changelog_successor(
                changelog_predecessor,
                riffdb_storage_api::ChangelogAttributionV3::JournaledServiceAudit,
                DualFrontier::new(covered_sequence, covered_administration_sequence),
                &checkpoint_mutations,
            )?;
            composite_successor.append_changelog_source(
                changelog_successor,
                riffdb_storage_api::ChangelogAttributionV3::JournaledServiceAudit,
                Arc::clone(&checkpoint_mutations),
                encoded_bytes,
            )?;
            let composite_successor = Arc::new(composite_successor);
            let retained_frame = frame.clone();
            let receipt = runtime.lane.submit(frame).map_err(journal_io_error)?;
            runtime.changelog_history = changelog_successor.map(|(_, history)| history);
            runtime.last_administration_sequence = covered_administration_sequence;
            runtime.last_hash = frame_hash;
            runtime.suffix_transitions = next_transitions;
            runtime.suffix_audits = next_audits;
            runtime.suffix_bytes = next_bytes;
            runtime.suffix_physical_bytes = next_physical_bytes;
            runtime.suffix_frames.push(ValidatedCheckpointFrame {
                database_id: runtime.database_id,
                predecessor_sequence,
                covered_sequence: predecessor_sequence,
                predecessor_administration_sequence,
                covered_administration_sequence,
                previous_hash,
                frame_hash,
                transition_count: audit_count,
                command_count: 0,
                audit_count,
                encoded: retained_frame,
                mutations: checkpoint_mutations,
                changelog_binding: changelog_successor.map(|(binding, _)| binding),
            });
            runtime.unpublished_transitions = next_unpublished_transitions;
            runtime.unpublished_audits = next_unpublished_audits;
            runtime.unpublished_bytes = next_unpublished_bytes;
            (
                receipt,
                encoded_bytes,
                predecessor_sequence,
                predecessor_administration_sequence,
                composite_successor,
            )
        };
        if let Err(error) = self
            .shared
            .install_private_composite_successor(&composite_predecessor, &composite_successor)
        {
            self.shared.fence_writes();
            return Err(error);
        }
        let successor = composite_predecessor.checkpoint_root_shared();
        let publication_payload =
            PendingPublicationPayload::ServiceAudit(ServiceAuditPublication {
                successor: Arc::clone(&successor),
                transition_count,
                encoded_bytes,
                predecessor_sequence,
                covered_sequence,
                predecessor_administration_sequence,
                covered_administration_sequence,
                composite_predecessor: Arc::clone(&composite_predecessor),
                composite_successor,
            });
        let publication_ticket = match self
            .shared
            .register_publication(receipt.clone(), publication_payload)
        {
            Ok(ticket) => ticket,
            Err(error) => {
                self.shared.fence_writes();
                return Err(error);
            }
        };
        let fence = RedbSubmittedServiceAuditFence {
            shared: Arc::clone(&self.shared),
            receipt: Some(receipt),
            results: Some(results),
            covered_sequence,
            covered_administration_sequence,
            publication_ticket,
            completed: false,
        };
        drop(ownership);
        Ok(fence)
    }

    fn invalidate_transient_indexes(&self) {
        if let Ok(mut state) = self.shared.transient_indexes.write() {
            *state = TransientIndexState::Invalid;
        }
    }

    pub(crate) fn abort(mut self) -> Result<(), StorageError> {
        if let Some(transaction) = self.transaction.take() {
            transaction.abort().map_err(precommit_storage_error)?;
        }
        if let Some(runtime) = self.journal_checkpoint.take() {
            self.shared.restore_journal_runtime(runtime)?;
        }
        Ok(())
    }
}

impl Drop for RedbWriteAccess {
    fn drop(&mut self) {
        if let Some(runtime) = self.journal_checkpoint.take()
            && self.shared.restore_journal_runtime(runtime).is_err()
        {
            self.shared.fence_writes();
        }
    }
}

fn checked_epoch_totals(
    command_count: usize,
    semantic_bytes: usize,
    reserved_encoded_bytes: usize,
    added_commands: usize,
    added: riffdb_storage_api::StagedBatchMetrics,
) -> Result<(usize, usize, usize), StorageError> {
    let next_count = command_count
        .checked_add(added_commands)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    let next_semantic = semantic_bytes
        .checked_add(added.semantic_bytes())
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    let next_reserved = reserved_encoded_bytes
        .checked_add(added.reserved_encoded_bytes())
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    if next_count > riffdb_storage_api::MAX_STAGED_COMMANDS
        || next_semantic > riffdb_storage_api::MAX_STAGED_WRITE_BYTES
        || next_reserved > riffdb_storage_api::MAX_STAGED_WRITE_BYTES
    {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    Ok((next_count, next_semantic, next_reserved))
}

impl RedbDurabilityEpoch {
    /// Proves that the private logical frontier accumulated by this epoch is
    /// exactly the bounded command sequence that will be encoded and applied.
    ///
    /// This is deliberately a whole-epoch proof: commands are joined in
    /// admission order, every adjacent subgroup must be contiguous, and the
    /// independently retained command/mutation counts must agree before the
    /// durability lane can observe the epoch.
    fn private_frontier_is_equivalent(&self) -> bool {
        let mut command_count = 0usize;
        let mut prior_last = None;
        for batch in &self.applied {
            if batch.command_count() == 0
                || prior_last.is_some_and(|prior: CommitSequence| {
                    prior.checked_next() != Some(batch.first_commit_sequence())
                })
            {
                return false;
            }
            let Some(next_count) = command_count.checked_add(batch.command_count()) else {
                return false;
            };
            command_count = next_count;
            prior_last = Some(batch.last_commit_sequence());
        }
        command_count == self.command_count
            && prior_last == self.last_sequence
            && self.journal_mutation_groups == self.applied.len()
            && self.journal_mutations.mutation_count() != 0
            && self.composite_stage.as_ref().is_some_and(|stage| {
                usize::try_from(self.journal_mutations.mutation_count()).ok()
                    == Some(stage.mutation_count())
            })
    }

    fn has_unpublished_state(&self) -> bool {
        !self.applied.is_empty()
            || !self.transient_deltas.is_empty()
            || self.command_count != 0
            || self.last_sequence.is_some()
            || self.semantic_bytes != 0
            || self.reserved_encoded_bytes != 0
            || self.journal_mutations.mutation_count() != 0
            || self.journal_mutation_groups != 0
            || self
                .composite_stage
                .as_ref()
                .is_some_and(|stage| stage.mutation_count() != 0)
    }

    pub(crate) fn begin_write(mut self) -> Result<RedbWriteAccess, StorageError> {
        if self.completed
            || self.lease.is_none()
            || self.shared.write_fenced.load(Ordering::Acquire)
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let shared = Arc::clone(&self.shared);
        let journal_mutations = std::mem::take(&mut self.journal_mutations);
        let journal_mutation_start = journal_mutations.mutation_count();
        let composite_stage = self.composite_stage.take();
        Ok(RedbWriteAccess {
            shared,
            transaction: None,
            ownership: Some(RedbWriteOwnership::Epoch(Box::new(self))),
            direct_changelog_attribution: None,
            journal_mutations: Some(RefCell::new(journal_mutations)),
            journal_mutation_start,
            journal_checkpoint: None,
            composite_predecessor: None,
            composite_stage: composite_stage.map(RefCell::new),
            command_prefix_capture: RefCell::default(),
        })
    }

    pub(crate) fn seal(mut self) -> Result<RedbSubmittedCommandFence, StorageError> {
        if self.applied.is_empty() || self.lease.is_none() || self.completed {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        self.shared
            .before_test_commit(RedbTestOperation::CommandEpochTail)?;
        if !self.private_frontier_is_equivalent() {
            self.shared.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let changelog_predecessor = self.shared.stage_changelog_allocator(
            &mut self.journal_mutations,
            self.composite_stage
                .as_mut()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        )?;
        let prospective_encoded_bytes = self
            .journal_mutations
            .encoded_frame_len()
            .map_err(journal_storage_error)?;
        let prospective_physical_bytes =
            crate::journal::extent_frame_bytes(prospective_encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let checkpoint_rebased = self.shared.ensure_journal_frame_headroom(
            self.command_count,
            prospective_encoded_bytes,
            prospective_physical_bytes,
        )?;
        if checkpoint_rebased {
            self.composite_predecessor = self.shared.capture_or_initialize_composite_view()?;
        }
        let first_sequence = self
            .applied
            .first()
            .map(riffdb_storage_api::UnpublishedAuditedBatchV1::first_commit_sequence)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let last_sequence = self
            .last_sequence
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let last_administration_sequence = self
            .applied
            .last()
            .map(riffdb_storage_api::UnpublishedAuditedBatchV1::last_administration_sequence);
        let mutations = std::mem::take(&mut self.journal_mutations);
        let mut composite_stage = self
            .composite_stage
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let (
            receipt,
            encoded_bytes,
            audit_count,
            predecessor_administration_sequence,
            journaled,
            mut composite_successor,
        ) = {
            let (checkpoint_transitions, checkpoint_bytes) =
                self.shared.async_checkpoint_charge()?;
            let mut runtime_guard = self.shared.journal_runtime()?;
            let runtime = runtime_guard
                .as_mut()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            if next_commit_sequence(runtime.last_sequence) != Some(first_sequence) {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let command_count = u16::try_from(self.command_count)
                .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
            let frame = crate::journal::JournalFrame::encode_buffered_command(
                runtime.database_id,
                runtime.last_sequence,
                Some(last_sequence),
                runtime.last_administration_sequence,
                last_administration_sequence,
                command_count,
                runtime.last_hash,
                mutations,
            )
            .map_err(journal_storage_error)?;
            let encoded_bytes = frame.as_bytes().len();
            if encoded_bytes != prospective_encoded_bytes {
                self.shared.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            if checkpoint_rebased {
                composite_stage = self
                    .shared
                    .rebuild_composite_stage_for_frame(&self.composite_predecessor, &frame)?;
            }
            let physical_bytes = crate::journal::extent_frame_bytes(encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let audit_count = usize::from(frame.audit_count());
            let next_transitions = runtime
                .suffix_transitions
                .checked_add(self.command_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_commands = runtime
                .suffix_commands
                .checked_add(self.command_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_audits = runtime
                .suffix_audits
                .checked_add(audit_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_bytes = runtime
                .suffix_bytes
                .checked_add(encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_physical_bytes = runtime
                .suffix_physical_bytes
                .checked_add(physical_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_commands = runtime
                .unpublished_commands
                .checked_add(self.command_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_transitions = runtime
                .unpublished_transitions
                .checked_add(self.command_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_audits = runtime
                .unpublished_audits
                .checked_add(audit_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_bytes = runtime
                .unpublished_bytes
                .checked_add(encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if checkpoint_transitions.saturating_add(next_transitions)
                > crate::journal::MAX_JOURNAL_SUFFIX_TRANSITIONS
                || checkpoint_bytes.saturating_add(next_bytes)
                    > crate::journal::MAX_JOURNAL_SUFFIX_BYTES
                || next_physical_bytes > crate::journal::EXTENT_DATA_BYTES
                || next_unpublished_transitions > crate::journal::MAX_JOURNAL_TRANSITIONS
                || next_unpublished_bytes > crate::journal::MAX_JOURNAL_FRAME_BYTES
            {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            } else {
                let frame_hash = frame.frame_hash();
                let predecessor_sequence = runtime.last_sequence;
                let predecessor_administration_sequence = runtime.last_administration_sequence;
                let previous_hash = runtime.last_hash;
                let (mut composite_successor, checkpoint_mutations) = composite_stage
                    .seal_encoded_frame_with_mutations(
                        riffdb_storage_api::CompositeFrameKindV1::Command,
                        runtime.database_id,
                        predecessor_sequence,
                        Some(last_sequence),
                        predecessor_administration_sequence,
                        last_administration_sequence,
                        command_count,
                        encoded_bytes,
                        previous_hash,
                        frame_hash,
                    )?;
                let checkpoint_mutations: Arc<[riffdb_storage_api::CompositeMutationV1]> =
                    Arc::from(checkpoint_mutations);
                let changelog_successor = runtime.prepare_changelog_successor(
                    changelog_predecessor,
                    riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
                    DualFrontier::new(Some(last_sequence), last_administration_sequence),
                    &checkpoint_mutations,
                )?;
                composite_successor.append_changelog_source(
                    changelog_successor,
                    riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
                    Arc::clone(&checkpoint_mutations),
                    encoded_bytes,
                )?;
                let composite_successor = Arc::new(composite_successor);
                let retained_frame = frame.clone();
                let receipt = runtime.lane.submit(frame).map_err(journal_io_error)?;
                runtime.changelog_history = changelog_successor.map(|(_, history)| history);
                runtime.last_sequence = Some(last_sequence);
                runtime.last_administration_sequence = last_administration_sequence;
                runtime.last_hash = frame_hash;
                runtime.suffix_transitions = next_transitions;
                runtime.suffix_commands = next_commands;
                runtime.suffix_audits = next_audits;
                runtime.suffix_bytes = next_bytes;
                runtime.suffix_physical_bytes = next_physical_bytes;
                runtime.suffix_frames.push(ValidatedCheckpointFrame {
                    database_id: runtime.database_id,
                    predecessor_sequence,
                    covered_sequence: Some(last_sequence),
                    predecessor_administration_sequence,
                    covered_administration_sequence: last_administration_sequence,
                    previous_hash,
                    frame_hash,
                    transition_count: command_count,
                    command_count,
                    audit_count: u16::try_from(audit_count)
                        .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
                    encoded: retained_frame,
                    mutations: checkpoint_mutations,
                    changelog_binding: changelog_successor.map(|(binding, _)| binding),
                });
                runtime.unpublished_transitions = next_unpublished_transitions;
                runtime.unpublished_commands = next_unpublished_commands;
                runtime.unpublished_audits = next_unpublished_audits;
                runtime.unpublished_bytes = next_unpublished_bytes;
                (
                    Some(receipt),
                    encoded_bytes,
                    audit_count,
                    predecessor_administration_sequence,
                    true,
                    Some(composite_successor),
                )
            }
        };
        if let Some(composite_successor) = composite_successor.as_ref()
            && let Err(error) = self.shared.install_private_composite_successor(
                &self.composite_predecessor,
                composite_successor,
            )
        {
            self.shared.fence_writes();
            return Err(error);
        }
        {
            let mut unpublished = self
                .shared
                .unpublished_command_indexes
                .lock()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            for delta in &self.transient_deltas {
                if let Some(segment) = delta.command_segment() {
                    unpublished.insert_segment(Arc::clone(segment))?;
                }
            }
        }
        let successor = self.composite_predecessor.checkpoint_root_shared();
        let publication_ticket = if journaled {
            let journal_receipt = receipt
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .clone();
            let published = composite_successor
                .take()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let payload = PendingPublicationPayload::Command(CommandPublication {
                successor: Arc::clone(&successor),
                transient_deltas: std::mem::take(&mut self.transient_deltas),
                command_count: self.command_count,
                encoded_bytes,
                first_sequence,
                last_sequence,
                predecessor_administration_sequence,
                last_administration_sequence,
                audit_count,
                composite_predecessor: Arc::clone(&self.composite_predecessor),
                composite_successor: published,
            });
            Some(
                match self.shared.register_publication(journal_receipt, payload) {
                    Ok(ticket) => ticket,
                    Err(error) => {
                        self.shared.fence_writes();
                        return Err(error);
                    }
                },
            )
        } else {
            None
        };
        // Registration is part of sealing: releasing the sole-writer lease
        // first would let a later service-audit frame enter the publication
        // queue ahead of this already-submitted command frame.
        let pipeline_drain_required = self.shared.async_checkpoint_in_flight()?;
        self.completed = true;
        drop(self.lease.take());
        Ok(RedbSubmittedCommandFence {
            shared: Arc::clone(&self.shared),
            receipt,
            successor,
            applied: std::mem::take(&mut self.applied),
            transient_deltas: std::mem::take(&mut self.transient_deltas),
            command_count: self.command_count,
            last_sequence,
            predecessor_administration_sequence,
            last_administration_sequence,
            journaled,
            pipeline_drain_required,
            publication_ticket,
            completed: false,
        })
    }
}

impl SharedRedb {
    fn checkpoint_published_journal_suffix_for_barrier(
        self: &Arc<Self>,
    ) -> Result<(), StorageError> {
        self.poll_async_checkpoint_locked(true)?;
        let Some(runtime) = self.take_published_journal_suffix_locked(true)? else {
            return Ok(());
        };
        let mut transaction = match self.database.begin_write() {
            Ok(transaction) => transaction,
            Err(error) => {
                self.restore_journal_runtime(runtime)?;
                return Err(transaction_error(error));
            }
        };
        transaction.set_two_phase_commit(false);
        if transaction.set_durability(Durability::Immediate).is_err() {
            let _ = transaction.abort();
            self.restore_journal_runtime(runtime)?;
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Err(error) = self.apply_published_journal_suffix(&transaction, &runtime) {
            let _ = transaction.abort();
            self.restore_journal_runtime(runtime)?;
            return Err(error);
        }
        self.commit_durable(transaction)?;
        if let Err(error) = self.refresh_durable_read_frontier() {
            self.fence_writes();
            return Err(error);
        }
        if let Err(error) = self.finish_journal_checkpoint(runtime) {
            self.fence_writes();
            return Err(error);
        }
        Ok(())
    }

    fn capture_or_initialize_composite_view(
        &self,
    ) -> Result<Arc<crate::composite_view::RedbCompositeReadView>, StorageError> {
        if let Some(private) = self
            .private_composite_frontier
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .as_ref()
            .map(Arc::clone)
        {
            return Ok(private);
        }
        let published_hash = {
            let mut runtime = self.journal_runtime()?;
            runtime
                .as_mut()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .published_hash
        };
        let mut publication = self
            .composite_publication
            .write()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if publication.is_none() {
            let root = self
                .durable_read_frontier
                .read()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .as_ref()
                .map(Arc::clone)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let initial =
                crate::composite_view::RedbCompositeViewBuilder::from_root(root, published_hash)?
                    .freeze();
            *publication = Some(crate::composite_view::RedbCompositePublication::new(
                initial,
            ));
        }
        let captured = publication
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .capture()?;
        *self
            .private_composite_frontier
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))? =
            Some(Arc::clone(&captured));
        Ok(captured)
    }

    fn install_private_composite_successor(
        &self,
        expected: &Arc<crate::composite_view::RedbCompositeReadView>,
        successor: &Arc<crate::composite_view::RedbCompositeReadView>,
    ) -> Result<(), StorageError> {
        let mut private = self
            .private_composite_frontier
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if private
            .as_ref()
            .is_none_or(|current| !Arc::ptr_eq(current, expected))
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *private = Some(Arc::clone(successor));
        Ok(())
    }

    fn publish_composite_successor(
        &self,
        expected: &Arc<crate::composite_view::RedbCompositeReadView>,
        successor: Arc<crate::composite_view::RedbCompositeReadView>,
    ) -> Result<(), StorageError> {
        let publication = self
            .composite_publication
            .read()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        publication
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .publish_successor(expected, successor)?;
        Ok(())
    }

    fn register_publication(
        &self,
        receipt: crate::journal::JournalFenceReceipt,
        payload: PendingPublicationPayload,
    ) -> Result<PublicationTicket, StorageError> {
        let id = self
            .next_publication_ticket
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| storage_error(StorageErrorKind::SequenceExhausted))?;
        let ticket = PublicationTicket {
            id,
            result: Arc::new(Mutex::new(None)),
        };
        let mut queue = self
            .publication_queue
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        if queue.pending.len() >= crate::journal::MAX_JOURNAL_TRANSITIONS {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        queue.pending.push_back(PendingPublication {
            ticket: ticket.clone(),
            receipt,
            payload,
            registered_at: Instant::now(),
        });
        Ok(ticket)
    }

    /// Publishes the complete already-submitted journal prefix before a direct
    /// redb transition checkpoints it.
    ///
    /// The caller owns the mutation gate, so no later journal frame can enter
    /// the queue between selecting the tail and publishing through it. Fence
    /// results remain retained on their tickets for the original callers.
    fn publish_pending_journal_prefix(&self) -> Result<(), StorageError> {
        let tail = self
            .publication_queue
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?
            .pending
            .back()
            .map(|pending| pending.ticket.clone());
        match tail {
            Some(tail) => self.publish_through(&tail),
            None => Ok(()),
        }
    }

    /// Publishes every durable predecessor through `target` while holding one
    /// process-local sequencing gate. Journal completion is shared between the
    /// original fence and this queue, so a later waiter can advance an earlier
    /// frame without consuming that earlier caller's typed result.
    fn publish_through(&self, target: &PublicationTicket) -> Result<(), StorageError> {
        if let Some(result) = target.result()? {
            return Ok(result);
        }
        let mut queue = self
            .publication_queue
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        if let Some(result) = target.result()? {
            return Ok(result);
        }
        loop {
            let Some(pending) = queue.pending.pop_front() else {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            };
            let id = pending.ticket.id;
            let command_publication =
                matches!(&pending.payload, PendingPublicationPayload::Command(_));
            let receipt_started = Instant::now();
            let fenced = pending.receipt.wait().map_err(journal_io_error);
            let receipt_elapsed = receipt_started.elapsed();
            let durable_at = fenced.as_ref().ok().map(|fence| fence.durable_at);
            let publication_started = Instant::now();
            let publication = fenced.and_then(|fence| self.publish_pending(pending.payload, fence));
            let publication_elapsed = publication_started.elapsed();
            if command_publication && publication.is_ok() {
                let _ = COMMAND_PUBLICATION_COUNT.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| Some(current.saturating_add(1)),
                );
                record_publication_duration(
                    &COMMAND_PUBLICATION_RESIDENCE_MICROS,
                    &COMMAND_PUBLICATION_RESIDENCE_MAX_MICROS,
                    pending.registered_at.elapsed(),
                );
                record_publication_duration(
                    &COMMAND_RECEIPT_BLOCK_MICROS,
                    &COMMAND_RECEIPT_BLOCK_MAX_MICROS,
                    receipt_elapsed,
                );
                if let Some(durable_at) = durable_at {
                    record_publication_duration(
                        &COMMAND_DURABLE_TO_PUBLISH_MICROS,
                        &COMMAND_DURABLE_TO_PUBLISH_MAX_MICROS,
                        durable_at.elapsed(),
                    );
                }
                record_publication_duration(
                    &COMMAND_PUBLICATION_WORK_MICROS,
                    &COMMAND_PUBLICATION_WORK_MAX_MICROS,
                    publication_elapsed,
                );
            }
            pending.ticket.complete(publication.clone());
            if let Err(error) = publication {
                self.fence_writes();
                for unpublished in queue.pending.drain(..) {
                    unpublished.ticket.complete(Err(error.clone()));
                }
                return Err(error);
            }
            if id == target.id {
                return Ok(());
            }
            if id > target.id {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
    }

    fn publish_pending(
        &self,
        payload: PendingPublicationPayload,
        fence: crate::journal::JournalFence,
    ) -> Result<(), StorageError> {
        match payload {
            PendingPublicationPayload::Command(command) => {
                self.publish_pending_command(command, fence)
            }
            PendingPublicationPayload::ServiceAudit(audit) => {
                self.publish_pending_service_audit(audit, fence)
            }
        }
    }

    fn publish_pending_command(
        &self,
        mut publication: CommandPublication,
        fence: crate::journal::JournalFence,
    ) -> Result<(), StorageError> {
        if fence.covered_sequence != Some(publication.last_sequence)
            || fence.covered_administration_sequence != publication.last_administration_sequence
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
        }
        self.after_test_commit(RedbTestOperation::CommandEpochTail)?;
        let published_snapshot = Arc::clone(&publication.composite_successor);
        let mut transient = self
            .transient_indexes
            .write()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        let mut unpublished = self
            .unpublished_command_indexes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        for delta in publication.transient_deltas.drain(..) {
            if let Some(segment) = delta.command_segment() {
                unpublished.remove_segment(segment)?;
            }
            transient.apply_delta(delta);
        }
        if let Err(error) = self.publish_composite_successor(
            &publication.composite_predecessor,
            publication.composite_successor,
        ) {
            *transient = TransientIndexState::Invalid;
            self.fence_writes();
            return Err(error);
        }
        let mut frontier = match self.durable_read_frontier.write() {
            Ok(frontier) => frontier,
            Err(poisoned) => {
                drop(poisoned.into_inner());
                *transient = TransientIndexState::Invalid;
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
            }
        };
        if frontier.is_none() {
            *transient = TransientIndexState::Invalid;
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *frontier = Some(Arc::clone(&publication.successor));
        drop(frontier);
        drop(unpublished);
        drop(transient);

        let predecessor_application;
        {
            let mut runtime_guard = self.journal_runtime()?;
            let runtime = runtime_guard
                .as_mut()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            if next_commit_sequence(runtime.published_sequence) != Some(publication.first_sequence)
                || runtime.published_administration_sequence
                    != publication.predecessor_administration_sequence
                || runtime.unpublished_transitions < publication.command_count
                || runtime.unpublished_commands < publication.command_count
                || runtime.unpublished_audits < publication.audit_count
                || runtime.unpublished_bytes < publication.encoded_bytes
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            predecessor_application = runtime.published_sequence;
            runtime.published_sequence = Some(publication.last_sequence);
            runtime.published_administration_sequence = publication.last_administration_sequence;
            runtime.published_hash = fence.frame_hash;
            runtime.unpublished_transitions -= publication.command_count;
            runtime.unpublished_commands -= publication.command_count;
            runtime.unpublished_audits -= publication.audit_count;
            runtime.unpublished_bytes -= publication.encoded_bytes;
            if runtime.unpublished_transitions == 0
                && runtime.unpublished_bytes == 0
                && (runtime.published_sequence != runtime.last_sequence
                    || runtime.published_administration_sequence
                        != runtime.last_administration_sequence
                    || runtime.published_hash != runtime.last_hash)
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        self.observe_changelog_publication(
            riffdb_types::DualFrontier::new(
                predecessor_application,
                publication.predecessor_administration_sequence,
            ),
            riffdb_types::DualFrontier::new(
                Some(publication.last_sequence),
                publication.last_administration_sequence,
            ),
            fence.frame_hash,
            publication.command_count,
            true,
            RedbReadAccess::Composite(published_snapshot),
        );
        Ok(())
    }

    fn publish_pending_service_audit(
        &self,
        publication: ServiceAuditPublication,
        fence: crate::journal::JournalFence,
    ) -> Result<(), StorageError> {
        if fence.covered_sequence != publication.covered_sequence
            || fence.covered_administration_sequence != publication.covered_administration_sequence
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
        }
        self.after_test_commit(RedbTestOperation::ServiceAudit)?;
        let published_snapshot = Arc::clone(&publication.composite_successor);
        if let Err(error) = self.publish_composite_successor(
            &publication.composite_predecessor,
            publication.composite_successor,
        ) {
            self.fence_writes();
            return Err(error);
        }
        let mut frontier = match self.durable_read_frontier.write() {
            Ok(frontier) => frontier,
            Err(poisoned) => {
                drop(poisoned.into_inner());
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
            }
        };
        if frontier.is_none() {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *frontier = Some(Arc::clone(&publication.successor));
        drop(frontier);
        {
            let mut runtime_guard = self.journal_runtime()?;
            let runtime = runtime_guard
                .as_mut()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            if runtime.published_sequence != publication.predecessor_sequence
                || runtime.published_administration_sequence
                    != publication.predecessor_administration_sequence
                || runtime.unpublished_transitions < publication.transition_count
                || runtime.unpublished_audits < publication.transition_count
                || runtime.unpublished_bytes < publication.encoded_bytes
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            runtime.published_sequence = publication.covered_sequence;
            runtime.published_administration_sequence = publication.covered_administration_sequence;
            runtime.published_hash = fence.frame_hash;
            runtime.unpublished_transitions -= publication.transition_count;
            runtime.unpublished_audits -= publication.transition_count;
            runtime.unpublished_bytes -= publication.encoded_bytes;
            if runtime.unpublished_transitions == 0
                && runtime.unpublished_bytes == 0
                && (runtime.published_sequence != runtime.last_sequence
                    || runtime.published_administration_sequence
                        != runtime.last_administration_sequence
                    || runtime.published_hash != runtime.last_hash)
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        self.observe_changelog_publication(
            riffdb_types::DualFrontier::new(
                publication.predecessor_sequence,
                publication.predecessor_administration_sequence,
            ),
            riffdb_types::DualFrontier::new(
                publication.covered_sequence,
                publication.covered_administration_sequence,
            ),
            fence.frame_hash,
            publication.transition_count,
            true,
            RedbReadAccess::Composite(published_snapshot),
        );
        Ok(())
    }

    fn clear_composite_publication(&self) -> Result<(), StorageError> {
        if !self
            .publication_queue
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?
            .pending
            .is_empty()
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *self
            .composite_publication
            .write()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))? = None;
        *self
            .private_composite_frontier
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))? = None;
        Ok(())
    }

    fn async_checkpoint_charge(&self) -> Result<(usize, usize), StorageError> {
        let checkpoint = self
            .journal_checkpoint
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        Ok(checkpoint.as_ref().map_or((0, 0), |checkpoint| {
            (
                checkpoint.batch.transition_count,
                checkpoint.batch.encoded_bytes,
            )
        }))
    }

    fn async_checkpoint_in_flight(&self) -> Result<bool, StorageError> {
        self.journal_checkpoint
            .lock()
            .map(|checkpoint| checkpoint.is_some())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
    }

    /// Reserves exact capacity for one already-staged frame.
    ///
    /// A checkpoint can be slower than the foreground writer. Reaching its
    /// reserved headroom is bounded backpressure, not a semantic limit error:
    /// finish the covered checkpoint, rotate the newer suffix when needed,
    /// and only then admit the frame. The caller holds the sole mutation lease,
    /// so no sibling can consume the proven capacity between this check and
    /// frame submission.
    fn ensure_journal_frame_headroom(
        self: &Arc<Self>,
        transitions: usize,
        bytes: usize,
        physical_bytes: usize,
    ) -> Result<bool, StorageError> {
        if transitions == 0
            || transitions > crate::journal::MAX_JOURNAL_TRANSITIONS
            || bytes == 0
            || bytes > crate::journal::MAX_JOURNAL_FRAME_BYTES
            || physical_bytes == 0
            || physical_bytes > crate::journal::EXTENT_DATA_BYTES
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let mut rebased = false;
        for _ in 0..4 {
            let (checkpoint_transitions, checkpoint_bytes) = self.async_checkpoint_charge()?;
            let admits = {
                let runtime = self.journal_runtime()?;
                let runtime = runtime
                    .as_ref()
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                JournalCapacityCharge {
                    checkpoint_transitions,
                    checkpoint_bytes,
                    suffix_transitions: runtime.suffix_transitions,
                    suffix_bytes: runtime.suffix_bytes,
                    suffix_physical_bytes: runtime.suffix_physical_bytes,
                    unpublished_transitions: runtime.unpublished_transitions,
                    unpublished_bytes: runtime.unpublished_bytes,
                }
                .admits(transitions, bytes, physical_bytes)
            };
            if admits {
                return Ok(rebased);
            }
            if self.async_checkpoint_in_flight()? {
                self.poll_async_checkpoint_locked(true)?;
                rebased = true;
                continue;
            }
            self.maybe_start_async_checkpoint()?;
            if !self.async_checkpoint_in_flight()? {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
        }
        self.fence_writes();
        Err(storage_error(StorageErrorKind::InvariantViolation))
    }

    fn rebuild_composite_stage_for_frame(
        &self,
        predecessor: &Arc<crate::composite_view::RedbCompositeReadView>,
        frame: &crate::journal::EncodedJournalFrame,
    ) -> Result<crate::composite_view::RedbCompositeMutationStage, StorageError> {
        let (decoded, decoded_hash) = crate::journal::JournalFrame::decode(frame.as_bytes())
            .map_err(journal_storage_error)?;
        if decoded_hash != frame.frame_hash() {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let mut stage =
            crate::composite_view::RedbCompositeMutationStage::from_published(predecessor);
        for mutation in decoded.mutations() {
            stage.apply(mutation)?;
        }
        Ok(stage)
    }

    fn poll_async_checkpoint_locked(self: &Arc<Self>, wait: bool) -> Result<(), StorageError> {
        {
            let mut checkpoint = match self.journal_checkpoint.lock() {
                Ok(checkpoint) => checkpoint,
                Err(poisoned) => {
                    drop(poisoned.into_inner());
                    self.fence_writes();
                    return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
                }
            };
            let Some(checkpoint) = checkpoint.as_mut() else {
                return Ok(());
            };
            if checkpoint.result.is_none() {
                let receiver = checkpoint
                    .completion
                    .as_ref()
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
                let result = if wait {
                    Some(
                        receiver
                            .recv()
                            .unwrap_or_else(|_| Err(storage_error(StorageErrorKind::Unavailable))),
                    )
                } else {
                    match receiver.try_recv() {
                        Ok(result) => Some(result),
                        Err(std::sync::mpsc::TryRecvError::Empty) => None,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            Some(Err(storage_error(StorageErrorKind::Unavailable)))
                        }
                    }
                };
                if let Some(result) = result {
                    checkpoint.completion = None;
                    checkpoint.result = Some(result);
                }
            }
        }
        self.finish_async_checkpoint_if_quiescent()
    }

    fn finish_async_checkpoint_if_quiescent(self: &Arc<Self>) -> Result<(), StorageError> {
        let mut checkpoint = match self.journal_checkpoint.lock() {
            Ok(checkpoint) => checkpoint,
            Err(poisoned) => {
                drop(poisoned.into_inner());
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
            }
        };
        let Some(state) = checkpoint.as_mut() else {
            return Ok(());
        };
        let runtime = match self.journal_runtime.lock() {
            Ok(runtime) => runtime,
            Err(poisoned) => {
                drop(poisoned.into_inner());
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
            }
        };
        let Some(runtime) = runtime.as_ref() else {
            return Ok(());
        };
        if runtime.unpublished_transitions != 0 || runtime.unpublished_bytes != 0 {
            return Ok(());
        }
        let Some(result) = state.result.take() else {
            return Ok(());
        };
        if let Err(error) = result {
            self.fence_writes();
            return Err(error);
        }
        let batch = state.batch.clone();
        let covered_view = Arc::clone(&state.covered_view);
        let mut publication_began = false;
        let rebase_result = (|| -> Result<(), StorageError> {
            let current = self
                .composite_publication
                .read()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .capture()?;
            let root = self.capture_checkpoint_root()?;
            if read_commit_tail(&root)? != batch.last_sequence
                || read_administration_tail(&root)? != batch.last_administration_sequence
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            if covered_view.overlay().published_application() != batch.last_sequence
                || covered_view.overlay().published_administration()
                    != batch.last_administration_sequence
                || covered_view.overlay().terminal_frame_hash() != batch.last_hash
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let successor = Arc::new(current.rebase_after(&covered_view, root, batch.last_hash)?);
            let mut private = self
                .private_composite_frontier
                .lock()
                .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
            if private
                .as_ref()
                .is_none_or(|private| !Arc::ptr_eq(private, &current))
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            self.composite_publication
                .read()
                .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .publish_rebased(&current, Arc::clone(&successor))?;
            publication_began = true;
            *private = Some(Arc::clone(&successor));
            *self
                .durable_read_frontier
                .write()
                .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))? =
                Some(successor.checkpoint_root_shared());
            let checkpoint_path = crate::journal::checkpoint_journal_path(&self.path);
            let spare_path = crate::journal::spare_journal_path(&self.path);
            let media = self.journal_media.as_ref();
            if media
                .try_exists(&spare_path)
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
            {
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            media
                .rename(&checkpoint_path, &spare_path)
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
            crate::journal::sync_parent_directory_with_media(media, &spare_path)
                .map_err(journal_io_error)?;
            let spare_header = crate::journal::JournalFileHeader::with_frontiers(
                runtime.database_id,
                runtime.last_sequence,
                runtime.last_administration_sequence,
                runtime.last_hash,
            );
            if let Err(error) = crate::journal::reset_journal_after_with_media(
                media,
                &spare_path,
                &spare_header,
                &crate::journal::journal_path(&self.path),
            ) {
                self.fence_writes();
                return Err(journal_io_error(error));
            }
            *checkpoint = None;
            Ok(())
        })();
        match rebase_result {
            Ok(()) => Ok(()),
            Err(error) => {
                if publication_began {
                    self.fence_writes();
                }
                Err(error)
            }
        }
    }

    fn apply_published_journal_suffix(
        &self,
        transaction: &WriteTransaction,
        runtime: &JournalRuntime,
    ) -> Result<(), StorageError> {
        let v3 = crate::changelog_v3_journal::has_write_recovery_roots(transaction)?;
        let scanned = crate::journal::scan_journal_with_media(
            self.journal_media.as_ref(),
            &crate::journal::journal_path(&self.path),
            runtime.database_id,
            |frame| {
                if v3 || crate::changelog_v3_journal::has_receipt_source(frame) {
                    crate::changelog_v3_journal::materialize_recovered_frame(transaction, frame)
                        .map_err(|_| crate::journal::JournalIoError::Corrupt)?;
                } else {
                    for mutation in frame.mutations() {
                        crate::journal::apply_mutation(transaction, mutation)?;
                    }
                }
                Ok(())
            },
        )
        .map_err(journal_io_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let (header, tail) = scanned;
        if header.database_id() != runtime.database_id
            || tail.incomplete_tail
            || tail.last_sequence != runtime.last_sequence
            || tail.last_administration_sequence != runtime.last_administration_sequence
            || tail.last_hash != runtime.last_hash
            || tail.transition_count != runtime.suffix_transitions
            || tail.command_count != runtime.suffix_commands
            || tail.audit_count != runtime.suffix_audits
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(())
    }

    fn take_published_journal_suffix_locked(
        &self,
        force: bool,
    ) -> Result<Option<JournalRuntime>, StorageError> {
        let mut runtime = self
            .journal_runtime
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let Some(current) = runtime.as_ref() else {
            return Ok(None);
        };
        if current.unpublished_transitions != 0 || current.unpublished_bytes != 0 {
            if force {
                // Another lane can acquire the mutation gate after private
                // staging releases it but before the journal fence publishes.
                // The predecessor remains authoritative, so this is bounded
                // transient backpressure rather than an integrity failure.
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
            return Ok(None);
        }
        if !force
            && !current.reanchor_required
            && current.suffix_transitions < JOURNAL_CHECKPOINT_START_TRANSITIONS
            && current.suffix_bytes < JOURNAL_CHECKPOINT_START_BYTES
        {
            return Ok(None);
        }
        Ok(runtime.take())
    }

    fn finish_journal_checkpoint(&self, runtime: JournalRuntime) -> Result<(), StorageError> {
        drop(runtime.lane);
        let checkpoint = self.capture_checkpoint_root()?;
        let checkpoint_database_id = read_identity_from_read_transaction(&checkpoint)?;
        let checkpoint_sequence = read_commit_tail(&checkpoint)?;
        let checkpoint_administration_sequence = read_administration_tail(&checkpoint)?;
        if checkpoint_database_id != runtime.database_id
            || checkpoint_sequence < runtime.last_sequence
            || checkpoint_administration_sequence < runtime.last_administration_sequence
        {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Err(error) = crate::journal::reset_journal_with_media(
            self.journal_media.as_ref(),
            &crate::journal::journal_path(&self.path),
            &crate::journal::JournalFileHeader::with_frontiers(
                runtime.database_id,
                checkpoint_sequence,
                checkpoint_administration_sequence,
                runtime.last_hash,
            ),
        ) {
            self.fence_writes();
            return Err(journal_io_error(error));
        }
        let mut frontier = match self.durable_read_frontier.write() {
            Ok(frontier) => frontier,
            Err(poisoned) => {
                drop(poisoned.into_inner());
                self.fence_writes();
                return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
            }
        };
        if frontier.is_none() {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *frontier = Some(checkpoint);
        self.clear_composite_publication()?;
        Ok(())
    }

    fn refresh_durable_read_frontier(&self) -> Result<(), StorageError> {
        let mut frontier = self
            .durable_read_frontier
            .write()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        if frontier.is_some() {
            *frontier = Some(self.capture_checkpoint_root()?);
        }
        Ok(())
    }

    fn restore_journal_runtime(&self, runtime: JournalRuntime) -> Result<(), StorageError> {
        let mut current = self
            .journal_runtime
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if current.is_some() {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *current = Some(runtime);
        Ok(())
    }
}

pub(crate) fn read_commit_tail(
    transaction: &ReadTransaction,
) -> Result<Option<CommitSequence>, StorageError> {
    read_commit_tail_profiled(transaction).map(|(head, _)| head)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CommitTailProfileV1 {
    pub(crate) physical_bytes: u64,
    pub(crate) logical_commands: u64,
}

fn read_commit_tail_profiled(
    transaction: &ReadTransaction,
) -> Result<(Option<CommitSequence>, CommitTailProfileV1), StorageError> {
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let retained = crate::command_authority::command_authority_head_profiled(&commits, &events)?;
    let profile = CommitTailProfileV1 {
        physical_bytes: retained.physical_bytes,
        logical_commands: retained.logical_commands,
    };
    let watermark = crate::retention::load_watermark(transaction)?
        .map(|watermark| watermark.watermark_sequence())
        .unwrap_or(0);
    if watermark == 0 {
        return Ok((retained.head, profile));
    }
    let pruned = CommitSequence::new(watermark)
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let head = match retained.head {
        Some(retained) if retained <= pruned => Err(storage_error(StorageErrorKind::CorruptData)),
        Some(retained) => Ok(Some(retained)),
        None => Ok(Some(pruned)),
    }?;
    Ok((head, profile))
}

pub(crate) fn read_administration_tail(
    transaction: &ReadTransaction,
) -> Result<Option<AdministrationSequence>, StorageError> {
    let physical = transaction
        .open_table(AUDIT)
        .map_err(table_error)?
        .last()
        .map_err(precommit_storage_error)?
        .map(|(key, _)| crate::keys::decode_audit_key(key.value()))
        .transpose()
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let command = match commits.last().map_err(precommit_storage_error)? {
        None => None,
        Some((_, encoded)) => {
            match riffdb_storage_api::decode_command_segment_v1(encoded.value()) {
                Ok(segment) => Some(segment.value().last_administration_sequence()),
                Err(error)
                    if error.kind()
                        == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    None
                }
                Err(error) => return Err(crate::error::codec_error(error)),
            }
        }
    };
    match (physical, command) {
        (Some(physical), Some(command)) if physical == command => {
            Err(storage_error(StorageErrorKind::CorruptData))
        }
        (Some(physical), Some(command)) => Ok(Some(physical.max(command))),
        (Some(sequence), None) | (None, Some(sequence)) => Ok(Some(sequence)),
        (None, None) => Ok(None),
    }
}

fn next_commit_sequence(sequence: Option<CommitSequence>) -> Option<CommitSequence> {
    sequence.map_or(Some(CommitSequence::first()), CommitSequence::checked_next)
}

fn journal_storage_error(error: crate::journal::JournalCodecError) -> StorageError {
    let kind = if error == crate::journal::JournalCodecError::LimitExceeded {
        StorageErrorKind::LimitExceeded
    } else {
        StorageErrorKind::InvariantViolation
    };
    storage_error(kind)
}

fn journal_io_error(error: crate::journal::JournalIoError) -> StorageError {
    match error {
        crate::journal::JournalIoError::Corrupt => storage_error(StorageErrorKind::CorruptData),
        crate::journal::JournalIoError::LegacyNonEmpty(path) => {
            eprintln!(
                "RDB-STORAGE-UPGRADE: legacy durability journal '{}' is nonempty; reopen with the prior binary and perform a clean checkpoint before upgrading",
                path.display()
            );
            storage_error(StorageErrorKind::IncompatibleFormat)
        }
        crate::journal::JournalIoError::Capacity => storage_error(StorageErrorKind::LimitExceeded),
        crate::journal::JournalIoError::Io | crate::journal::JournalIoError::Stopped => {
            storage_error(StorageErrorKind::CommitStatusUnknown)
        }
    }
}

fn recovery_journal_error(error: crate::journal::JournalIoError) -> StorageError {
    match error {
        crate::journal::JournalIoError::Corrupt => storage_error(StorageErrorKind::CorruptData),
        crate::journal::JournalIoError::LegacyNonEmpty(path) => {
            eprintln!(
                "RDB-STORAGE-UPGRADE: legacy durability journal '{}' is nonempty; reopen with the prior binary and perform a clean checkpoint before upgrading",
                path.display()
            );
            storage_error(StorageErrorKind::IncompatibleFormat)
        }
        crate::journal::JournalIoError::Capacity => storage_error(StorageErrorKind::LimitExceeded),
        crate::journal::JournalIoError::Io | crate::journal::JournalIoError::Stopped => {
            storage_error(StorageErrorKind::Unavailable)
        }
    }
}

impl DeferredCommandFence for RedbSubmittedCommandFence {
    fn requires_pipeline_drain(&self) -> bool {
        command_fence_requires_pipeline_drain(self.journaled, self.pipeline_drain_required)
    }

    fn try_wait(
        &mut self,
    ) -> Result<Option<Vec<riffdb_storage_api::AuditedCommittedBatchV1>>, StorageError> {
        (|| {
            if !self.journaled {
                return self.publish_direct().map(Some);
            }
            let fenced = self
                .receipt
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .try_wait()
                .map_err(journal_io_error)?;
            let Some(fenced) = fenced else {
                return Ok(None);
            };
            self.receipt.take();
            self.publish_fenced(fenced).map(Some)
        })()
    }

    fn wait(mut self) -> Result<Vec<riffdb_storage_api::AuditedCommittedBatchV1>, StorageError> {
        if !self.journaled {
            return self.publish_direct();
        }
        let fenced = self
            .receipt
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .wait()
            .map_err(journal_io_error)?;
        self.publish_fenced(fenced)
    }
}

const fn command_fence_requires_pipeline_drain(
    journaled: bool,
    checkpoint_in_flight: bool,
) -> bool {
    !journaled || checkpoint_in_flight
}

impl RedbSubmittedCommandFence {
    fn publish_direct(
        &mut self,
    ) -> Result<Vec<riffdb_storage_api::AuditedCommittedBatchV1>, StorageError> {
        self.publish_successor(true)?;
        self.shared.clear_composite_publication()?;
        let predecessor_application;
        {
            let mut runtime_guard = self.shared.journal_runtime()?;
            let runtime = runtime_guard
                .as_mut()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let first_sequence = self
                .applied
                .first()
                .map(riffdb_storage_api::UnpublishedAuditedBatchV1::first_commit_sequence);
            if runtime.unpublished_transitions != 0
                || runtime.unpublished_bytes != 0
                || runtime.published_sequence != runtime.last_sequence
                || runtime.published_administration_sequence != runtime.last_administration_sequence
                || runtime.published_hash != runtime.last_hash
                || next_commit_sequence(runtime.published_sequence) != first_sequence
                || runtime.published_administration_sequence
                    != self.predecessor_administration_sequence
            {
                self.shared.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            predecessor_application = runtime.published_sequence;
            runtime.last_sequence = Some(self.last_sequence);
            runtime.published_sequence = Some(self.last_sequence);
            runtime.last_administration_sequence = self.last_administration_sequence;
            runtime.published_administration_sequence = self.last_administration_sequence;
            runtime.reanchor_required = true;
        }
        // ADR-0100 §1: the direct `Immediate` singleton path publishes a
        // single-group frame with no covering journal flush.
        self.shared.observe_changelog_publication(
            riffdb_types::DualFrontier::new(
                predecessor_application,
                self.predecessor_administration_sequence,
            ),
            riffdb_types::DualFrontier::new(
                Some(self.last_sequence),
                self.last_administration_sequence,
            ),
            [0; 32],
            self.command_count,
            false,
            RedbReadAccess::Durable(Arc::clone(&self.successor)),
        );
        self.finish_applied()
    }

    fn publish_fenced(
        &mut self,
        fenced: crate::journal::JournalFence,
    ) -> Result<Vec<riffdb_storage_api::AuditedCommittedBatchV1>, StorageError> {
        if fenced.covered_sequence != Some(self.last_sequence)
            || fenced.covered_administration_sequence != self.last_administration_sequence
        {
            self.shared.fence_writes();
            return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
        }
        let ticket = self
            .publication_ticket
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        self.shared.publish_through(ticket)?;
        self.finish_applied()
    }

    fn publish_successor(&mut self, run_test_hook: bool) -> Result<(), StorageError> {
        if run_test_hook {
            self.shared
                .after_test_commit(RedbTestOperation::CommandEpochTail)?;
        }
        let mut transient = self
            .shared
            .transient_indexes
            .write()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        let mut unpublished = self
            .shared
            .unpublished_command_indexes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        let mut frontier = self
            .shared
            .durable_read_frontier
            .write()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        if frontier.is_none() {
            self.shared.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *frontier = Some(Arc::clone(&self.successor));
        drop(frontier);
        for delta in self.transient_deltas.drain(..) {
            if let Some(segment) = delta.command_segment() {
                unpublished.remove_segment(segment)?;
            }
            transient.apply_delta(delta);
        }
        Ok(())
    }

    fn finish_applied(
        &mut self,
    ) -> Result<Vec<riffdb_storage_api::AuditedCommittedBatchV1>, StorageError> {
        let mut committed = Vec::with_capacity(self.applied.len());
        for batch in self.applied.drain(..) {
            let (outcomes, terminals) = batch.into_parts();
            let durability = outcomes
                .first()
                .map(riffdb_storage_api::StoredOutcomeV1::durability_mode)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let batch = riffdb_storage_api::CommittedBatchV1::new(outcomes, durability)
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            committed.push(
                riffdb_storage_api::AuditedCommittedBatchV1::new(batch, terminals)
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?,
            );
        }
        self.completed = true;
        Ok(committed)
    }
}

impl Drop for RedbSubmittedCommandFence {
    fn drop(&mut self) {
        if !self.completed {
            self.shared.fence_writes();
            if let Ok(mut state) = self.shared.transient_indexes.write() {
                *state = TransientIndexState::Invalid;
            }
        }
    }
}

impl riffdb_storage_api::DeferredServiceAuditFence for RedbSubmittedServiceAuditFence {
    fn try_wait(
        &mut self,
    ) -> Result<Option<Vec<riffdb_storage_api::ServiceAuditAppendResult>>, StorageError> {
        let fenced = self
            .receipt
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .try_wait()
            .map_err(journal_io_error)?;
        let Some(fenced) = fenced else {
            return Ok(None);
        };
        self.receipt.take();
        self.publish_fenced(fenced).map(Some)
    }

    fn wait(
        mut self: Box<Self>,
    ) -> Result<Vec<riffdb_storage_api::ServiceAuditAppendResult>, StorageError> {
        let fenced = self
            .receipt
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .wait()
            .map_err(journal_io_error)?;
        self.publish_fenced(fenced)
    }
}

impl RedbSubmittedServiceAuditFence {
    fn publish_fenced(
        &mut self,
        fenced: crate::journal::JournalFence,
    ) -> Result<Vec<riffdb_storage_api::ServiceAuditAppendResult>, StorageError> {
        if fenced.covered_sequence != self.covered_sequence
            || fenced.covered_administration_sequence != self.covered_administration_sequence
        {
            self.shared.fence_writes();
            return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
        }
        self.shared.publish_through(&self.publication_ticket)?;
        self.completed = true;
        self.results
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }
}

impl Drop for RedbSubmittedServiceAuditFence {
    fn drop(&mut self) {
        if !self.completed {
            self.shared.fence_writes();
        }
    }
}

impl Drop for RedbDurabilityEpoch {
    fn drop(&mut self) {
        if !self.completed && self.has_unpublished_state() {
            self.shared.fence_writes();
            if let Ok(mut state) = self.shared.transient_indexes.write() {
                *state = TransientIndexState::Invalid;
            }
        }
    }
}

impl RedbWriteAccess {
    pub(crate) fn service_audit_sequences(
        &self,
        request_id: riffdb_types::RequestId,
    ) -> Result<Vec<riffdb_types::AdministrationSequence>, StorageError> {
        let mut ids = std::collections::BTreeSet::new();
        ids.insert(request_id);
        Ok(self
            .service_audit_sequences_for(&ids)?
            .remove(&request_id)
            .unwrap_or_default())
    }

    /// Loads durable service-audit sequences for many request ids with one snapshot.
    pub(crate) fn service_audit_sequences_for(
        &self,
        request_ids: &std::collections::BTreeSet<riffdb_types::RequestId>,
    ) -> Result<
        std::collections::BTreeMap<
            riffdb_types::RequestId,
            Vec<riffdb_types::AdministrationSequence>,
        >,
        StorageError,
    > {
        self.shared.note_audit_sequence_begin_read();
        let mut sequences = std::collections::BTreeMap::new();
        for &request_id in request_ids {
            let prefix = encode_audit_by_request_prefix(request_id);
            let upper = exclusive_byte_prefix_end(&prefix)
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let rows = self.read_command_range(
                crate::journal::JournalTable::AuditByRequest,
                &prefix,
                &upper,
                3,
            )?;
            let mut found = Vec::new();
            // ADR-0165 locator rows carry the same (request_id, sequence) key,
            // so a command's audit sequences are recoverable from the key alone
            // without resolving its segment. Merged first so the physical rows
            // below still validate their own record.
            let locator_rows = self.read_command_range(
                crate::journal::JournalTable::AuditByRequestLocators,
                &prefix,
                &upper,
                3,
            )?;
            for (key, _) in locator_rows {
                let (decoded_request, sequence) = decode_audit_by_request_key(&key)
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                if decoded_request != request_id {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                found.push(sequence);
            }
            for (key, value) in rows {
                let (decoded_request, sequence) = decode_audit_by_request_key(&key)
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                let index = decode_service_audit_request_index_v1(&value)?
                    .into_parts()
                    .0;
                if decoded_request != request_id
                    || index.request_id() != request_id
                    || index.administration_sequence() != sequence
                    || found.last().is_some_and(|prior| prior >= &sequence)
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                found.push(sequence);
            }
            if found.len() > 2 {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            sequences.insert(request_id, found);
        }
        // One continuous transient guard spans both probes so a concurrent
        // publication cannot move a segment between them and drop its sequences.
        let transient = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match &*transient {
            TransientIndexState::Ready(indexes) => {
                let derived = indexes
                    .command_audit_sequences_for(request_ids)
                    .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))??;
                merge_audit_sequences(&mut sequences, derived)?;
            }
            TransientIndexState::Dormant => {}
            TransientIndexState::Invalid => {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
        }
        let unpublished = self
            .shared
            .unpublished_command_indexes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .command_audit_sequences_for(request_ids)?;
        drop(transient);
        merge_audit_sequences(&mut sequences, unpublished)?;
        if let Some(RedbWriteOwnership::Epoch(epoch)) = self.ownership.as_ref() {
            let mut derived = std::collections::BTreeMap::new();
            for delta in &epoch.transient_deltas {
                delta.command_audit_sequences_for(request_ids, &mut derived)?;
            }
            merge_audit_sequences(&mut sequences, derived)?;
        }
        Ok(sequences)
    }

    pub(crate) fn ensure_outbox_indexes_available(&self) -> Result<(), StorageError> {
        self.shared.pending_outbox_page(None, 0)?;
        self.shared.undelivered_outbox_page(None, 0).map(|_| ())
    }
}

fn merge_audit_sequences(
    target: &mut std::collections::BTreeMap<
        riffdb_types::RequestId,
        Vec<riffdb_types::AdministrationSequence>,
    >,
    source: std::collections::BTreeMap<
        riffdb_types::RequestId,
        Vec<riffdb_types::AdministrationSequence>,
    >,
) -> Result<(), StorageError> {
    const MAX_SERVICE_AUDIT_RECORDS_PER_REQUEST: usize = 2;
    for (request_id, source_sequences) in source {
        let sequences = target.entry(request_id).or_default();
        for sequence in source_sequences {
            if !sequences.contains(&sequence) {
                sequences.push(sequence);
            }
        }
        sequences.sort_unstable();
        if sequences.len() > MAX_SERVICE_AUDIT_RECORDS_PER_REQUEST {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    Ok(())
}

impl SharedRedb {
    fn note_audit_sequence_begin_read(&self) {
        if let Some(controller) = &self.test_controller {
            controller.observe_audit_sequence_begin_read();
        }
    }

    pub(crate) fn ensure_transient_indexes_ready(self: &Arc<Self>) -> Result<(), StorageError> {
        {
            let state = self
                .transient_indexes
                .read()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            match &*state {
                TransientIndexState::Ready(_) => return Ok(()),
                TransientIndexState::Invalid => {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
                TransientIndexState::Dormant => {}
            }
        }
        let _lease = self.mutation_gate.acquire()?;
        {
            let state = self
                .transient_indexes
                .read()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            match &*state {
                TransientIndexState::Ready(_) => return Ok(()),
                TransientIndexState::Invalid => {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
                TransientIndexState::Dormant => {}
            }
        }
        self.checkpoint_published_journal_suffix_for_barrier()?;
        let transaction = self.database.begin_read().map_err(transaction_error)?;
        let (indexes, commit_rows) = TransientIndexes::rebuild_counted(&transaction)?;
        drop(transaction);
        self.note_transient_index_rebuild(commit_rows);
        let mut state = self
            .transient_indexes
            .write()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if !matches!(*state, TransientIndexState::Dormant) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *state = TransientIndexState::Ready(indexes);
        Ok(())
    }

    fn pending_outbox_page(
        self: &Arc<Self>,
        after: Option<riffdb_types::EventId>,
        limit: usize,
    ) -> Result<(Vec<riffdb_types::EventId>, bool), StorageError> {
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        self.ensure_transient_indexes_ready()?;
        let state = self
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match &*state {
            TransientIndexState::Ready(indexes) => indexes
                .pending_outbox_page(after, limit)
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable)),
            TransientIndexState::Dormant | TransientIndexState::Invalid => {
                Err(storage_error(StorageErrorKind::Unavailable))
            }
        }
    }

    fn undelivered_outbox_page(
        self: &Arc<Self>,
        after: Option<riffdb_types::EventId>,
        limit: usize,
    ) -> Result<(Vec<riffdb_types::EventId>, bool), StorageError> {
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        self.ensure_transient_indexes_ready()?;
        let state = self
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match &*state {
            TransientIndexState::Ready(indexes) => indexes
                .undelivered_outbox_page(after, limit)
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable)),
            TransientIndexState::Dormant | TransientIndexState::Invalid => {
                Err(storage_error(StorageErrorKind::Unavailable))
            }
        }
    }
}

fn exclusive_byte_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    let position = end.iter().rposition(|byte| *byte != u8::MAX)?;
    end[position] = end[position].checked_add(1)?;
    end.truncate(position + 1);
    Some(end)
}

impl TransientIndexState {
    fn apply_delta(&mut self, delta: TransientIndexDelta) {
        if let Self::Ready(indexes) = self {
            indexes.apply(delta);
        }
    }
}

impl std::fmt::Debug for RedbStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RedbStore([DORMANT])")
    }
}

impl std::fmt::Debug for RedbDormantPorts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RedbDormantPorts([STRUCTURALLY_OPENED])")
    }
}

impl std::fmt::Debug for RedbOperationalPorts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RedbOperationalPorts([OPERATIONAL])")
    }
}

impl DatabaseIdentityProbePort for RedbStore {
    fn probe_database_identity(&self) -> Result<DatabaseIdentityProbe, StorageError> {
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        match classify_read_layout(&transaction)? {
            LayoutState::Empty => Ok(DatabaseIdentityProbe::NeedsInitialization),
            LayoutState::Initialized
            | LayoutState::InitializedWithoutValidatedPrefixEntityHeads => {
                read_identity_from_read_transaction(&transaction)
                    .map(DatabaseIdentityProbe::Existing)
            }
        }
    }
}

impl DatabaseInitializationPort for RedbStore {
    fn initialize_database(
        &mut self,
        candidate: DatabaseId,
    ) -> Result<DatabaseInitializationResult, StorageError> {
        let _lease = self.acquire_mutation_lease()?;
        self.ensure_writable()?;

        let mut transaction = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;

        match classify_write_layout(&transaction)? {
            LayoutState::Initialized
            | LayoutState::InitializedWithoutValidatedPrefixEntityHeads => {
                let winner = read_identity_from_write_transaction(&transaction)?;
                transaction.abort().map_err(precommit_storage_error)?;
                Ok(DatabaseInitializationResult::ConcurrentWinner(winner))
            }
            LayoutState::Empty => {
                create_all_tables(&transaction).map_err(table_error)?;
                write_initial_metadata(&transaction, candidate)?;
                if let Some(controller) = &self.shared.test_controller {
                    controller.before_commit(RedbTestOperation::Initialization)?;
                }
                self.shared.commit_durable(transaction)?;
                if let Some(controller) = &self.shared.test_controller
                    && let Err(error) = controller.after_commit(RedbTestOperation::Initialization)
                {
                    self.fence_writes();
                    return Err(error);
                }
                Ok(DatabaseInitializationResult::Installed(candidate))
            }
        }
    }
}

fn classify_read_layout(transaction: &redb::ReadTransaction) -> Result<LayoutState, StorageError> {
    let tables = transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    let multimaps = transaction
        .list_multimap_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    if let Some(layout) = v3_layout::classify_read(transaction, &tables, &multimaps)? {
        return Ok(layout);
    }
    classify_table_names(tables, multimaps)
}

fn classify_write_layout(
    transaction: &redb::WriteTransaction,
) -> Result<LayoutState, StorageError> {
    let tables = transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    let multimaps = transaction
        .list_multimap_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    if let Some(layout) = v3_layout::classify_write(transaction, &tables, &multimaps)? {
        return Ok(layout);
    }
    classify_table_names(tables, multimaps)
}

fn classify_table_names(
    tables: BTreeSet<String>,
    multimaps: BTreeSet<String>,
) -> Result<LayoutState, StorageError> {
    if tables.is_empty() && multimaps.is_empty() {
        return Ok(LayoutState::Empty);
    }
    if !multimaps.is_empty() {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }

    let expected = TABLE_NAMES
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    // The newest additive tables are empty on install and carry no history, so
    // their absence is normalized while classifying the already-enumerated
    // predecessor layouts below. The vector and common columnar control tables
    // are installed by their registry migrations before the successor digest;
    // the
    // ADR-0165 locator tables add no message type, so no digest advances for
    // them and `install_command_locator_tables` installs them at open instead.
    // Only these exact names are normalized. No other missing or extra table is
    // hidden.
    let mut with_newest_additive = tables.clone();
    for name in [
        "vector_projection_controls",
        "columnar_projection_controls",
        "application_export_page_commitments",
        "idempotency_locators",
        "provenance_locators",
        "audit_by_request_locators",
    ] {
        with_newest_additive.insert(name.to_owned());
    }
    let matches =
        |candidate: &BTreeSet<String>| tables == *candidate || with_newest_additive == *candidate;
    if matches(&expected) {
        return Ok(LayoutState::Initialized);
    }
    // The immediate predecessor lacks only the authoritative reciprocal
    // evidence index. Its registry is advanced only after this empty table is
    // installed. The same migration transaction is idempotent for databases
    // that already installed vector observations before a crash.
    let mut pre_vector_index = expected.clone();
    pre_vector_index.remove("vector_evidence_index");
    if matches(&pre_vector_index) {
        return Ok(LayoutState::Initialized);
    }
    // The earlier predecessor lacks observations and their reciprocal index.
    let mut pre_vector_observations = pre_vector_index;
    pre_vector_observations.remove("vector_observations");
    if matches(&pre_vector_observations) {
        return Ok(LayoutState::Initialized);
    }
    // The earlier predecessor lacks authoritative vector evidence and
    // observations. Its registry advances only after each empty table is installed.
    let mut pre_vector_evidence = pre_vector_observations;
    pre_vector_evidence.remove("vector_evidence");
    if matches(&pre_vector_evidence) {
        return Ok(LayoutState::Initialized);
    }
    // The immediate predecessor lacks only the additive checkpoint-head proof
    // table. It is installed before startup validation without a registry
    // rotation because its values reuse the frozen EntityChainHeadV1 codec.
    let mut pre_checkpoint_entity_heads = pre_vector_evidence.clone();
    pre_checkpoint_entity_heads.remove("validated_prefix_entity_heads");
    if matches(&pre_checkpoint_entity_heads) {
        return Ok(LayoutState::InitializedWithoutValidatedPrefixEntityHeads);
    }
    // The WP-575 predecessor lacks only durable application-export operation
    // checkpoints. The table is installed before the registry advances.
    // Accept the same registry predecessor with the additive proof table
    // already installed as well: table installation and registry publication
    // are separate crash boundaries.
    let mut pre_application_export_with_checkpoint_heads = pre_vector_evidence;
    pre_application_export_with_checkpoint_heads.remove("application_export_operations");
    if matches(&pre_application_export_with_checkpoint_heads) {
        return Ok(LayoutState::Initialized);
    }
    let mut pre_application_export = pre_checkpoint_entity_heads;
    pre_application_export.remove("application_export_operations");
    if matches(&pre_application_export) {
        return Ok(LayoutState::InitializedWithoutValidatedPrefixEntityHeads);
    }
    // The immediate predecessor lacks only delete-aware entity-chain heads.
    let mut pre_entity_transitions = pre_application_export;
    pre_entity_transitions.remove("entity_chain_heads");
    if matches(&pre_entity_transitions) {
        return Ok(LayoutState::InitializedWithoutValidatedPrefixEntityHeads);
    }
    // Pre-retention layout: missing history_tombstones is migration-eligible.
    let mut pre_retention = pre_entity_transitions;
    pre_retention.remove("history_tombstones");
    if matches(&pre_retention) {
        return Ok(LayoutState::InitializedWithoutValidatedPrefixEntityHeads);
    }
    // Pre-audit-request-index layout: exactly the 21-table predecessor is
    // migration-eligible and treated as initialized for open/identity probe.
    // ensure_current_storage_format creates audit_by_request before any write path.
    let mut pre_event_route = pre_retention;
    pre_event_route.remove("event_routes");
    if matches(&pre_event_route) {
        return Ok(LayoutState::InitializedWithoutValidatedPrefixEntityHeads);
    }
    let mut pre_audit_request_index = pre_event_route;
    pre_audit_request_index.remove("audit_by_request");
    if matches(&pre_audit_request_index) {
        return Ok(LayoutState::InitializedWithoutValidatedPrefixEntityHeads);
    }
    if tables.iter().any(|name| !expected.contains(name)) {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    Err(storage_error(StorageErrorKind::CorruptData))
}

fn write_initial_metadata(
    transaction: &redb::WriteTransaction,
    database_id: DatabaseId,
) -> Result<(), StorageError> {
    let format = encode_storage_format_version_v1(StorageFormatVersion::V2)
        .map_err(crate::error::codec_error)?;
    // Fresh layout is inactive until complete structural AND catalog validation.
    // Only the atomic V3 activation transaction may publish the current digest.
    let registry = encode_record_registry_v2(crate::changelog_v3_activation::PRE_V3_REGISTRY)
        .map_err(crate::error::codec_error)?;
    let identity = encode_database_identity_v1(database_id).map_err(crate::error::codec_error)?;
    let application =
        encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::initial())
            .map_err(crate::error::codec_error)?;
    let administration = encode_administration_sequence_allocator_v1(
        riffdb_storage_api::AdministrationSequenceAllocator::initial(),
    )
    .map_err(crate::error::codec_error)?;
    let history = encode_history_incarnation_v1(HISTORY_INCARNATION_INITIAL)
        .map_err(crate::error::codec_error)?;
    let rotation = riffdb_storage_api::ChangelogV2RotationReceipt::new(
        database_id,
        HISTORY_INCARNATION_INITIAL,
        DualFrontier::INITIAL,
        [0; 32],
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let rotation = riffdb_storage_api::encode_changelog_v2_rotation_receipt_v1(rotation)
        .map_err(crate::error::codec_error)?;

    let mut table = transaction.open_table(META).map_err(table_error)?;
    table
        .insert(META_FORMAT_VERSION, format.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_DATABASE_ID, identity.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_APPLICATION_SEQUENCE, application.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_ADMINISTRATION_SEQUENCE, administration.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_RECORD_REGISTRY, registry.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_HISTORY_INCARNATION, history.as_bytes())
        .map_err(precommit_storage_error)?;
    table
        .insert(META_CHANGELOG_V2_ROTATION_RECEIPT, rotation.as_bytes())
        .map_err(precommit_storage_error)?;
    // Fresh databases never hold legacy INDEX_EPOCHS rows.
    table
        .insert(META_INDEX_EPOCH_ROWS_REPAIRED, [1u8].as_slice())
        .map_err(precommit_storage_error)?;
    Ok(())
}

fn read_identity_from_read_transaction(
    transaction: &redb::ReadTransaction,
) -> Result<DatabaseId, StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    validate_and_read_identity(&table)
}

fn read_identity_from_write_transaction(
    transaction: &redb::WriteTransaction,
) -> Result<DatabaseId, StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    validate_and_read_identity(&table)
}

fn validate_and_read_identity<T>(table: &T) -> Result<DatabaseId, StorageError>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let mut seen = BTreeSet::new();
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let key = key.value();
        if !META_KEYS.contains(&key) {
            if crate::changelog_v3_roots::validate_metadata_entry(key, value.value())? {
                continue;
            }
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        if !seen.insert(key.to_owned()) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        match key {
            META_FORMAT_VERSION => {
                decode_storage_format_version_v1(value.value())
                    .map_err(crate::error::codec_error)?;
            }
            META_DATABASE_ID => {
                decode_database_identity_v1(value.value()).map_err(crate::error::codec_error)?;
            }
            META_APPLICATION_SEQUENCE => {
                decode_application_sequence_allocator_v1(value.value())
                    .map_err(crate::error::codec_error)?;
            }
            META_ADMINISTRATION_SEQUENCE => {
                decode_administration_sequence_allocator_v1(value.value())
                    .map_err(crate::error::codec_error)?;
            }
            META_CAPABILITY_BOOTSTRAP => {
                riffdb_storage_api::proto_codec::decode_capability_bootstrap_marker_v1(
                    value.value(),
                )
                .map_err(crate::error::codec_error)?;
            }
            META_RECORD_REGISTRY => {
                let observed =
                    decode_record_registry_v2(value.value()).map_err(crate::error::codec_error)?;
                if observed.value()
                    != &riffdb_storage_api::proto_codec::current_record_registry_digest()
                    && observed.value() != &crate::changelog_v3_activation::PRE_V3_REGISTRY
                {
                    return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                }
            }
            META_HISTORY_INCARNATION => {
                decode_history_incarnation_v1(value.value()).map_err(crate::error::codec_error)?;
            }
            META_INDEX_EPOCH_ROWS_REPAIRED => {
                if value.value() != [1u8].as_slice() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
            }
            META_VALIDATED_PREFIX_CHECKPOINT => {
                // Optional proof-carrying checkpoint. Decode failures are NOT open
                // errors: startup ignores an invalid checkpoint and runs full
                // validation (fail-closed = full validation, never blocks open).
                let _ = riffdb_storage_api::proto_codec::decode_validated_prefix_checkpoint_v2(
                    value.value(),
                );
            }
            META_RETENTION_WATERMARK => {
                // Optional; invalid payloads re-validated fail-closed at startup.
                let _ =
                    riffdb_storage_api::proto_codec::decode_retention_watermark_v1(value.value());
            }
            META_RETENTION_HOLDS => {
                let _ = riffdb_storage_api::proto_codec::decode_retention_holds_v1(value.value());
            }
            META_CHANGELOG_V2_ROTATION_RECEIPT => {
                riffdb_storage_api::decode_changelog_v2_rotation_receipt_v1(value.value())
                    .map_err(crate::error::codec_error)?;
            }
            META_CLEAN_CLOSE_LIFECYCLE => {
                // Invalid lifecycle evidence falls back to complete startup;
                // identity reads must not turn it into an open refusal.
                let _ = crate::clean_close::CleanCloseLifecycle::decode(value.value());
            }
            _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
    }

    for required in [
        META_FORMAT_VERSION,
        META_DATABASE_ID,
        META_APPLICATION_SEQUENCE,
        META_ADMINISTRATION_SEQUENCE,
        META_RECORD_REGISTRY,
    ] {
        if !seen.contains(required) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    if seen.len() > META_KEYS.len() {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }

    let identity = table
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    Ok(*decode_database_identity_v1(identity.value())
        .map_err(crate::error::codec_error)?
        .value())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "store_follower_tests.rs"]
mod follower_tests;
