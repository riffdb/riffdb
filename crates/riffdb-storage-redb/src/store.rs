//! Dormant redb handle, identity probe, and atomic initialization.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use redb::{
    Builder, Database, Durability, MultimapTableHandle, ReadTransaction, ReadableDatabase,
    ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle, WriteTransaction,
};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, DatabaseIdentityProbe, DatabaseIdentityProbePort,
    DatabaseInitializationPort, DatabaseInitializationResult, HISTORY_INCARNATION_INITIAL,
    StorageError, StorageErrorKind, StorageFormatVersion, StoredIndexEpochV1,
    proto_codec::{
        decode_administration_sequence_allocator_v1, decode_application_sequence_allocator_v1,
        decode_database_identity_v1, decode_history_incarnation_v1, decode_record_registry_v2,
        decode_storage_format_version_v1, encode_administration_sequence_allocator_v1,
        encode_application_sequence_allocator_v1, encode_database_identity_v1,
        encode_history_incarnation_v1, encode_record_registry_v2, encode_storage_format_version_v1,
        transcode_durable_record_to_v2,
    },
};
use riffdb_types::{DatabaseId, IndexEpoch, IndexId, SchemaHash};

use crate::codec::{
    decode_administration_audit_record_v1, decode_commit_with_event_table, decode_event_route_v1,
    decode_index_entry_v2, decode_index_epoch_v1, decode_legacy_index_epoch_v1,
    decode_outbox_with_event_table, decode_service_audit_request_index_v1, encode_commit_record_v1,
    encode_event_route_v1, encode_index_epoch_v1, encode_outbox_intent_v1,
    encode_service_audit_request_index_v1,
};
use crate::error::{
    commit_error, database_error, precommit_storage_error, storage_error, table_error,
    transaction_error,
};
use crate::gate::{ExclusiveGate, ExclusiveLease};
use crate::hooks::{RedbTestController, RedbTestOperation};
use crate::keys::{
    decode_application_sequence_key, decode_audit_by_request_key, decode_audit_key,
    decode_index_range_prefix_key, decode_partition_index_key, encode_audit_by_request_key,
    encode_audit_by_request_prefix, encode_event_route_key, encode_partition_index_key,
};
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, BYTE_TABLES, COMMITS, EVENT_ROUTES, EVENTS, INDEX_EPOCHS, META,
    META_ADMINISTRATION_SEQUENCE, META_APPLICATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP,
    META_DATABASE_ID, META_FORMAT_VERSION, META_HISTORY_INCARNATION,
    META_INDEX_EPOCH_ROWS_REPAIRED, META_KEYS, META_RECORD_REGISTRY, META_RETENTION_HOLDS,
    META_RETENTION_WATERMARK, META_VALIDATED_PREFIX_CHECKPOINT, OUTBOX, SECONDARY_INDEXES,
    TABLE_NAMES, create_all_tables,
};
use crate::transient::{TransientIndexDelta, TransientIndexState, TransientIndexes};

pub(crate) struct SharedRedb {
    pub(crate) database: Database,
    #[allow(dead_code, reason = "WP-070 offline backup consumes the source path")]
    path: PathBuf,
    application_commit_profile: RedbCommitProfile,
    mutation_gate: ExclusiveGate,
    write_fenced: AtomicBool,
    durable_commit_epoch: AtomicU64,
    test_controller: Option<RedbTestController>,
    transient_indexes: Mutex<TransientIndexState>,
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
    /// Retention watermark sequence, loaded and self-hash-verified once at
    /// open. The watermark advances only under exclusive OFFLINE maintenance,
    /// which cannot run while this handle holds the database open, so reads
    /// never re-hash the meta record per call (ADR-0085 A2 hot-path rule).
    retention_watermark: AtomicU64,
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
    const fn uses_two_phase(self) -> bool {
        matches!(self, Self::Hardened)
    }
}

impl SharedRedb {
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

    pub(crate) fn commit_durable(&self, transaction: WriteTransaction) -> Result<(), StorageError> {
        if self.durable_commit_epoch.load(Ordering::Acquire) == u64::MAX {
            self.fence_writes();
            return Err(storage_error(StorageErrorKind::SequenceExhausted));
        }
        if let Err(error) = transaction.commit() {
            self.fence_writes();
            return Err(commit_error(error));
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
}

/// A dormant handle to one redb-backed RiffDB database.
///
/// Opening this handle performs only redb recovery. It does not claim RiffDB
/// structural, catalog, or operational readiness.
pub struct RedbStore {
    pub(crate) shared: Arc<SharedRedb>,
}

/// Redb ports released by a complete structural evidence session.
///
/// This value is still dormant: it exposes no storage trait implementation and
/// is not an operational-readiness proof.
pub struct RedbDormantPorts {
    pub(crate) shared: Arc<SharedRedb>,
}

/// Activated redb-backed semantic storage ports.
///
/// Only server composition should construct this value, after it has matched
/// the structural handoff with the catalog-owned validation proof.
pub struct RedbOperationalPorts {
    pub(crate) shared: Arc<SharedRedb>,
}

pub(crate) struct RedbWriteAccess {
    shared: Arc<SharedRedb>,
    transaction: Option<WriteTransaction>,
    _lease: ExclusiveLease,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LayoutState {
    Empty,
    Initialized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistryMigration {
    Current,
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

impl RedbStore {
    /// Opens an existing redb file or creates an empty redb container.
    ///
    /// Authoritative application writes use [`RedbCommitProfile::Standard`].
    /// Initialization and migration retain their independently hardened
    /// durability boundary.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_inner(path.as_ref(), RedbCommitProfile::Standard, None)
    }

    /// Opens a database with an explicit process-wide application commit profile.
    pub fn open_with_commit_profile(
        path: impl AsRef<Path>,
        application_commit_profile: RedbCommitProfile,
    ) -> Result<Self, StorageError> {
        Self::open_inner(path.as_ref(), application_commit_profile, None)
    }

    /// Opens a database with one closed process-test failpoint controller.
    #[doc(hidden)]
    pub fn open_with_test_controller(
        path: impl AsRef<Path>,
        controller: RedbTestController,
    ) -> Result<Self, StorageError> {
        Self::open_inner(path.as_ref(), RedbCommitProfile::Standard, Some(controller))
    }

    /// Opens a database with one explicit profile and process-test controller.
    #[doc(hidden)]
    pub fn open_with_test_controller_and_commit_profile(
        path: impl AsRef<Path>,
        application_commit_profile: RedbCommitProfile,
        controller: RedbTestController,
    ) -> Result<Self, StorageError> {
        Self::open_inner(path.as_ref(), application_commit_profile, Some(controller))
    }

    fn open_inner(
        path: &Path,
        application_commit_profile: RedbCommitProfile,
        test_controller: Option<RedbTestController>,
    ) -> Result<Self, StorageError> {
        let path = path.to_path_buf();
        let database = Builder::new()
            .set_repair_callback(|session| {
                // Bounded progress telemetry only; do not enable quick_repair
                // (quick_repair forces two-phase commit, conflicting with Standard).
                let progress = session.progress();
                let basis_points = ((progress * 10_000.0) as u64).min(10_000);
                LAST_REPAIR_PROGRESS_BPS.store(basis_points, Ordering::Relaxed);
            })
            .create(&path)
            .map_err(database_error)?;
        let store = Self {
            shared: Arc::new(SharedRedb {
                database,
                path,
                application_commit_profile,
                mutation_gate: ExclusiveGate::default(),
                write_fenced: AtomicBool::new(false),
                durable_commit_epoch: AtomicU64::new(0),
                test_controller,
                transient_indexes: Mutex::new(TransientIndexState::Dormant),
                startup_validation_clean: AtomicBool::new(false),
                checkpoint_write_failures: AtomicU64::new(0),
                checkpoint_ignored: [(); 10].map(|()| AtomicU64::new(0)),
                retention_watermark: AtomicU64::new(0),
            }),
        };
        store.ensure_current_storage_format()?;
        store.cache_verified_retention_watermark()?;
        Ok(store)
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

    fn ensure_current_storage_format(&self) -> Result<(), StorageError> {
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        if classify_read_layout(&transaction)? == LayoutState::Empty {
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
                } else {
                    return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                }
            }
            StorageFormatVersion::V1 => RegistryMigration::Current,
            _ => return Err(storage_error(StorageErrorKind::IncompatibleFormat)),
        };
        drop(registry);
        drop(encoded_format);
        drop(metadata);
        drop(transaction);

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
                riffdb_storage_api::proto_codec::current_record_registry_digest(),
            )?;
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
                riffdb_storage_api::proto_codec::current_record_registry_digest(),
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
        if observed == current {
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
            current,
        )
    }

    pub(crate) fn fence_writes(&self) {
        self.shared.fence_writes();
    }

    /// Writes one proof-carrying validated-prefix startup checkpoint (ADR-0085 A1).
    ///
    /// Requires exclusive writer access. Same body — and the SAME write gate —
    /// as the post-validation startup write: the checkpoint is written only
    /// when this handle's startup validation session completed with zero
    /// structural findings of any scope. Returns `Ok(false)` (vetoed, nothing
    /// written) otherwise, so a finding can never be silenced by a shutdown
    /// checkpoint. A failed write after a clean gate is counted and returned as
    /// `Err`; callers (including graceful shutdown) treat that as non-fatal.
    pub fn write_validated_prefix_checkpoint(&self) -> Result<bool, StorageError> {
        let _lease = self.acquire_mutation_lease()?;
        self.ensure_writable()?;
        if !self.shared.startup_validation_clean() {
            return Ok(false);
        }
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let retained = crate::startup::read_retained_metadata_pub(&transaction)?;
        drop(transaction);
        match crate::validated_prefix::write_validated_prefix_checkpoint(&self.shared, &retained) {
            Ok(()) => Ok(true),
            Err(error) => {
                // ADR-0019 A1: count the lost fast path; callers decide fatality.
                self.shared.note_checkpoint_write_failure();
                Err(error)
            }
        }
    }

    /// Failed checkpoint writes after clean validation (non-fatal; counted).
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_write_failures(&self) -> u64 {
        self.shared.checkpoint_write_failures()
    }

    /// Per-reason counts of ignored validated-prefix checkpoints on this handle.
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_ignore_counts(&self) -> [(&'static str, u64); 10] {
        self.shared.checkpoint_ignore_counts()
    }

    #[cfg(test)]
    fn reopen_for_test(&self) -> Self {
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
    shared.commit_durable(transaction)?;
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

fn remove_legacy_index_epoch_rows(shared: &SharedRedb) -> Result<(), StorageError> {
    loop {
        let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
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
        shared.commit_durable(transaction)?;
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
                let record = decode_administration_audit_record_v1(value_bytes)?
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
    /// The redb adapter cannot depend on contract IR or the catalog proof type;
    /// WP-130 is the sole production caller and owns that proof composition.
    pub fn into_operational_after_catalog_validation(
        self,
    ) -> Result<RedbOperationalPorts, StorageError> {
        activate_operational_ports(self.shared)
    }
}

impl RedbStore {
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
    let transaction = shared.database.begin_read().map_err(transaction_error)?;
    let indexes = TransientIndexes::rebuild(&transaction)?;
    drop(transaction);
    let mut state = shared
        .transient_indexes
        .lock()
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    if !matches!(*state, TransientIndexState::Dormant) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    *state = TransientIndexState::Ready(indexes);
    drop(state);
    Ok(RedbOperationalPorts { shared })
}

impl RedbOperationalPorts {
    /// Exclusive-gate tickets ever issued on this database.
    ///
    /// Lets a test prove an operation answered without taking the mutation
    /// gate: `begin_write` and `acquire_indexed_read_lease` each take one
    /// ticket, `begin_read` takes none.
    #[cfg(test)]
    pub(crate) fn mutation_gate_tickets(&self) -> u128 {
        self.shared.mutation_gate.tickets_issued()
    }

    /// Writes one proof-carrying validated-prefix startup checkpoint (ADR-0085 A1).
    ///
    /// Gated exactly like the startup write: returns `Ok(false)` without
    /// writing unless this database's startup validation completed with zero
    /// structural findings of any scope.
    pub fn write_validated_prefix_checkpoint(&self) -> Result<bool, StorageError> {
        RedbStore {
            shared: Arc::clone(&self.shared),
        }
        .write_validated_prefix_checkpoint()
    }

    /// Failed checkpoint writes after clean validation (non-fatal; counted).
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_write_failures(&self) -> u64 {
        self.shared.checkpoint_write_failures()
    }

    /// Per-reason counts of ignored validated-prefix checkpoints on this database.
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_ignore_counts(&self) -> [(&'static str, u64); 10] {
        self.shared.checkpoint_ignore_counts()
    }

    /// Returns a cloneable pure-read handle over the same activated database.
    ///
    /// Mutation exclusion is the exclusive mutation gate, not handle uniqueness.
    /// The shared handle implements only `&self` storage ports.
    #[must_use]
    pub fn shared_ports(&self) -> crate::shared_ports::RedbSharedPorts {
        crate::shared_ports::RedbSharedPorts::new(Arc::clone(&self.shared))
    }

    pub(crate) fn begin_read(&self) -> Result<ReadTransaction, StorageError> {
        self.shared.database.begin_read().map_err(transaction_error)
    }

    pub(crate) fn begin_write(&self) -> Result<RedbWriteAccess, StorageError> {
        let lease = self.shared.mutation_gate.acquire()?;
        if self.shared.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let mut transaction = self
            .shared
            .database
            .begin_write()
            .map_err(transaction_error)?;
        transaction.set_two_phase_commit(self.shared.application_commit_profile.uses_two_phase());
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        Ok(RedbWriteAccess {
            shared: Arc::clone(&self.shared),
            transaction: Some(transaction),
            _lease: lease,
        })
    }

    pub(crate) fn acquire_indexed_read_lease(&self) -> Result<ExclusiveLease, StorageError> {
        self.shared.mutation_gate.acquire()
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
}

impl RedbWriteAccess {
    pub(crate) fn transaction(&self) -> Result<&WriteTransaction, StorageError> {
        self.transaction
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    #[allow(
        dead_code,
        reason = "WP-070 administration and derived ports migrate to named commits"
    )]
    pub(crate) fn commit(self) -> Result<(), StorageError> {
        self.commit_for(RedbTestOperation::CommandBatch)
    }

    pub(crate) fn commit_for(self, operation: RedbTestOperation) -> Result<(), StorageError> {
        self.commit_for_with_delta(operation, None)
    }

    pub(crate) fn commit_for_with_delta(
        mut self,
        operation: RedbTestOperation,
        delta: Option<TransientIndexDelta>,
    ) -> Result<(), StorageError> {
        if let Some(controller) = &self.shared.test_controller {
            controller.before_commit(operation)?;
        }
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Err(error) = self.shared.commit_durable(transaction) {
            self.invalidate_transient_indexes();
            return Err(error);
        }
        if let Some(delta) = delta
            && let Ok(mut state) = self.shared.transient_indexes.lock()
        {
            state.apply_delta(delta);
        }
        if let Some(controller) = &self.shared.test_controller
            && let Err(error) = controller.after_commit(operation)
        {
            self.shared.write_fenced.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    fn invalidate_transient_indexes(&self) {
        if let Ok(mut state) = self.shared.transient_indexes.lock() {
            *state = TransientIndexState::Invalid;
        }
    }

    pub(crate) fn abort(mut self) -> Result<(), StorageError> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        transaction.abort().map_err(precommit_storage_error)
    }
}

impl RedbWriteAccess {
    pub(crate) fn service_audit_sequences(
        &self,
        request_id: riffdb_types::RequestId,
    ) -> Result<Vec<riffdb_types::AdministrationSequence>, StorageError> {
        self.shared.service_audit_sequences(request_id)
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
        self.shared.service_audit_sequences_for(request_ids)
    }

    pub(crate) fn ensure_outbox_indexes_available(&self) -> Result<(), StorageError> {
        self.shared.pending_outbox_page(None, 0)?;
        self.shared.undelivered_outbox_page(None, 0).map(|_| ())
    }
}

impl SharedRedb {
    fn service_audit_sequences(
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

    /// One begin_read + one AUDIT_BY_REQUEST open; per-request bounded prefix ranges.
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
        const MAX_SERVICE_AUDIT_RECORDS_PER_REQUEST: usize = 2;
        let transaction = self.database.begin_read().map_err(transaction_error)?;
        self.note_audit_sequence_begin_read();
        let table = transaction
            .open_table(AUDIT_BY_REQUEST)
            .map_err(table_error)?;
        let mut results = std::collections::BTreeMap::new();
        for &request_id in request_ids {
            let prefix = encode_audit_by_request_prefix(request_id);
            let mut sequences = Vec::new();
            let scan = table
                .range::<&[u8]>((std::ops::Bound::Included(prefix.as_slice()), Unbounded))
                .map_err(precommit_storage_error)?;
            for entry in scan {
                let (key, value) = entry.map_err(precommit_storage_error)?;
                if !key.value().starts_with(prefix.as_slice()) {
                    break;
                }
                let (decoded_request, sequence) = decode_audit_by_request_key(key.value())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                if decoded_request != request_id {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                let index = decode_service_audit_request_index_v1(value.value())?
                    .into_parts()
                    .0;
                if index.request_id() != request_id || index.administration_sequence() != sequence {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                if sequences.len() >= MAX_SERVICE_AUDIT_RECORDS_PER_REQUEST
                    || sequences.last().is_some_and(|prior| prior >= &sequence)
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                sequences.push(sequence);
            }
            results.insert(request_id, sequences);
        }
        Ok(results)
    }

    fn note_audit_sequence_begin_read(&self) {
        if let Some(controller) = &self.test_controller {
            controller.observe_audit_sequence_begin_read();
        }
    }

    fn pending_outbox_page(
        &self,
        after: Option<riffdb_types::EventId>,
        limit: usize,
    ) -> Result<(Vec<riffdb_types::EventId>, bool), StorageError> {
        let state = self
            .transient_indexes
            .lock()
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
        &self,
        after: Option<riffdb_types::EventId>,
        limit: usize,
    ) -> Result<(Vec<riffdb_types::EventId>, bool), StorageError> {
        let state = self
            .transient_indexes
            .lock()
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
            LayoutState::Initialized => read_identity_from_read_transaction(&transaction)
                .map(DatabaseIdentityProbe::Existing),
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
            LayoutState::Initialized => {
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
    if tables == expected {
        return Ok(LayoutState::Initialized);
    }
    // Pre-retention layout: missing history_tombstones is migration-eligible.
    let mut pre_retention = expected.clone();
    pre_retention.remove("history_tombstones");
    if tables == pre_retention {
        return Ok(LayoutState::Initialized);
    }
    // Pre-audit-request-index layout: exactly the 21-table predecessor is
    // migration-eligible and treated as initialized for open/identity probe.
    // ensure_current_storage_format creates audit_by_request before any write path.
    let mut pre_event_route = pre_retention;
    pre_event_route.remove("event_routes");
    if tables == pre_event_route {
        return Ok(LayoutState::Initialized);
    }
    let mut pre_audit_request_index = pre_event_route;
    pre_audit_request_index.remove("audit_by_request");
    if tables == pre_audit_request_index {
        return Ok(LayoutState::Initialized);
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
    let registry = encode_record_registry_v2(
        riffdb_storage_api::proto_codec::current_record_registry_digest(),
    )
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
                let _ = riffdb_storage_api::proto_codec::decode_validated_prefix_checkpoint_v1(
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
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use riffdb_storage_api::{
        DurableKeySchemaBindingV1, IndexRangePrefixBuilder, LegacyStoredIndexEpochV1,
        StoredIndexEntryV2, StructurallyDecodedIndexRangePrefixV1,
        proto_codec::encode_legacy_index_epoch_v1_fixture,
    };
    use riffdb_types::{
        AggregateTypeId, CanonicalRecord, ContractBundleHash, ContractLineage, ContractVersion,
        EntityKeyBuilder, EntityTypeId, IndexEntryKeyBuilder, PartitionKeyBuilder,
    };
    use riffdb_types::{CommitSequence, EventId};

    use crate::keys::{
        encode_application_sequence_key, encode_event_key, encode_index_range_prefix_key,
    };

    use super::*;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

    struct TestDatabasePath(PathBuf);

    impl TestDatabasePath {
        fn new(label: &str) -> Self {
            let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "riffdb-redb-{label}-{}-{ordinal}.redb",
                std::process::id()
            )))
        }
    }

    impl Drop for TestDatabasePath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10])
            .expect("valid deterministic UUIDv7")
    }

    fn fixture_envelope_from(file: &str, record_type: &str) -> Vec<u8> {
        let line = file
            .lines()
            .find(|line| line.starts_with(record_type))
            .expect("fixture record exists");
        let encoded = line
            .split('\t')
            .nth(2)
            .expect("fixture includes envelope hex");
        assert_eq!(encoded.len() % 2, 0);
        encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let text = std::str::from_utf8(pair).expect("fixture hex is UTF-8");
                u8::from_str_radix(text, 16).expect("fixture hex is valid")
            })
            .collect()
    }

    fn wp373_fixture_envelope(record_type: &str) -> Vec<u8> {
        fixture_envelope_from(
            include_str!("../../../fixtures/proto/durable-wire-vectors-v2.txt"),
            record_type,
        )
    }

    fn legacy_v1_fixture_envelope(record_type: &str) -> Vec<u8> {
        fixture_envelope_from(
            include_str!("../../../fixtures/proto/durable-wire-vectors.txt"),
            record_type,
        )
    }

    fn install_pre_generation_fixture(path: &Path) {
        let mut store = RedbStore::open(path).expect("open generation fixture");
        store
            .initialize_database(database_id(0x1d))
            .expect("initialize generation fixture");
        let index_id = IndexId::new(7).expect("index ID");
        let mut entity = EntityKeyBuilder::new(EntityTypeId::first());
        entity.push_u64(9).expect("entity key component");
        let mut index = IndexEntryKeyBuilder::new(index_id);
        index.push_u64(11).expect("index component");
        let index_key = index
            .finish(entity.finish().expect("entity key"))
            .expect("index key");
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(3).expect("partition component");
        let partition = partition.finish().expect("partition key");
        let binding = DurableKeySchemaBindingV1::new(
            ContractLineage::new("generation-migration").expect("lineage"),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([0x44; 32]),
        );
        let entry = StoredIndexEntryV2::new(
            index_key.clone(),
            binding.clone(),
            CanonicalRecord::new(Vec::new()).expect("covered values"),
            partition,
        )
        .expect("index entry");
        let encoded_entry = crate::codec::encode_index_entry_v2(&entry).expect("encode index");
        let mut live_prefix = IndexRangePrefixBuilder::new(index_id);
        live_prefix.push_u64(11).expect("legacy prefix component");
        let prefix = StructurallyDecodedIndexRangePrefixV1::from_live(&live_prefix.finish());
        let legacy = LegacyStoredIndexEpochV1::new(
            prefix.clone(),
            binding,
            IndexEpoch::new(4).expect("legacy generation"),
        );
        let encoded_legacy =
            encode_legacy_index_epoch_v1_fixture(&legacy).expect("encode legacy generation");
        let predecessor =
            encode_record_registry_v2(SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST))
                .expect("encode predecessor registry");
        let transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin generation fixture");
        transaction
            .open_table(SECONDARY_INDEXES)
            .expect("open indexes")
            .insert(index_key.as_bytes(), encoded_entry.as_bytes())
            .expect("insert index");
        transaction
            .open_table(INDEX_EPOCHS)
            .expect("open generations")
            .insert(
                encode_index_range_prefix_key(&prefix),
                encoded_legacy.as_bytes(),
            )
            .expect("insert legacy generation");
        transaction
            .open_table(META)
            .expect("open metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("install predecessor registry");
        transaction.commit().expect("commit generation fixture");
    }

    fn assert_generation_fixture_migrated(store: &RedbStore) {
        let read = store
            .shared
            .database
            .begin_read()
            .expect("read migrated generation fixture");
        let metadata = read.open_table(META).expect("open metadata");
        let registry = metadata
            .get(META_RECORD_REGISTRY)
            .expect("read registry")
            .expect("registry exists");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode registry")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
        drop(registry);
        drop(metadata);

        let epochs = read.open_table(INDEX_EPOCHS).expect("open generations");
        let rows = epochs
            .iter()
            .expect("iterate generations")
            .collect::<Result<Vec<_>, _>>()
            .expect("read generation rows");
        assert_eq!(rows.len(), 1);
        let (physical, encoded) = &rows[0];
        let target =
            decode_partition_index_key(physical.value()).expect("decode partition/index key");
        let generation = decode_index_epoch_v1(encoded.value())
            .expect("decode current generation")
            .into_parts()
            .0;
        assert_eq!(generation.target(), &target);
        assert_eq!(
            generation.epoch(),
            IndexEpoch::new(5).expect("generation five")
        );
    }

    #[test]
    fn application_commit_profile_defaults_to_standard_and_can_be_hardened() {
        let standard_path = TestDatabasePath::new("standard-commit-profile");
        let standard = RedbStore::open(&standard_path.0).expect("open standard store");
        assert_eq!(
            standard.shared.application_commit_profile,
            RedbCommitProfile::Standard
        );
        assert!(!standard.shared.application_commit_profile.uses_two_phase());

        let hardened_path = TestDatabasePath::new("hardened-commit-profile");
        let hardened =
            RedbStore::open_with_commit_profile(&hardened_path.0, RedbCommitProfile::Hardened)
                .expect("open hardened store");
        assert_eq!(
            hardened.shared.application_commit_profile,
            RedbCommitProfile::Hardened
        );
        assert!(hardened.shared.application_commit_profile.uses_two_phase());
    }

    #[test]
    fn empty_initialize_probe_and_reopen_preserve_one_database_identity() {
        let path = TestDatabasePath::new("identity");
        let expected = database_id(0x11);
        {
            let mut store = RedbStore::open(&path.0).expect("open empty store");
            assert_eq!(
                store.probe_database_identity().expect("probe empty store"),
                DatabaseIdentityProbe::NeedsInitialization
            );
            assert_eq!(
                store
                    .initialize_database(expected)
                    .expect("initialize database"),
                DatabaseInitializationResult::Installed(expected)
            );
            assert_eq!(
                store.probe_database_identity().expect("probe identity"),
                DatabaseIdentityProbe::Existing(expected)
            );
        }

        let reopened = RedbStore::open(&path.0).expect("reopen database");
        assert_eq!(
            reopened
                .probe_database_identity()
                .expect("probe reopened identity"),
            DatabaseIdentityProbe::Existing(expected)
        );
    }

    #[test]
    fn mixed_v1_v2_framing_resumes_and_publishes_registry_last() {
        let path = TestDatabasePath::new("format-v2-migration");
        let expected = database_id(0x19);
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        store
            .initialize_database(expected)
            .expect("initialize current database");

        let mut transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin legacy fixture");
        transaction
            .set_durability(Durability::Immediate)
            .expect("set fixture durability");
        let mut metadata = transaction.open_table(META).expect("open metadata");
        let legacy_identity = {
            let identity = metadata
                .get(META_DATABASE_ID)
                .expect("read identity")
                .expect("identity exists");
            riffdb_storage_api::proto_codec::transcode_durable_record_to_v1(identity.value())
                .expect("identity transcodes")
                .into_bytes()
        };
        let legacy_format = riffdb_storage_api::proto_codec::transcode_durable_record_to_v1(
            encode_storage_format_version_v1(StorageFormatVersion::V1)
                .expect("legacy semantic format encodes")
                .as_bytes(),
        )
        .expect("format transcodes")
        .into_bytes();
        metadata
            .insert(META_DATABASE_ID, legacy_identity.as_slice())
            .expect("install legacy identity");
        metadata
            .insert(META_FORMAT_VERSION, legacy_format.as_slice())
            .expect("install legacy format");
        metadata
            .remove(META_RECORD_REGISTRY)
            .expect("remove final registry marker");
        drop(metadata);
        transaction.commit().expect("commit mixed fixture");
        drop(store);

        let reopened = RedbStore::open(&path.0).expect("resume format migration");
        assert_eq!(
            reopened
                .probe_database_identity()
                .expect("probe migrated identity"),
            DatabaseIdentityProbe::Existing(expected)
        );
        let read = reopened
            .shared
            .database
            .begin_read()
            .expect("read migrated metadata");
        let metadata = read.open_table(META).expect("open migrated metadata");
        for row in metadata.iter().expect("iterate migrated metadata") {
            let (key, value) = row.expect("read migrated row");
            if key.value() == META_INDEX_EPOCH_ROWS_REPAIRED {
                // Process marker is not a durable envelope.
                assert_eq!(value.value(), [1u8].as_slice());
                continue;
            }
            assert!(value.value().starts_with(b"RDB2"));
        }
        let registry = metadata
            .get(META_RECORD_REGISTRY)
            .expect("read registry")
            .expect("registry published");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode registry")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
    }

    #[test]
    fn prefix_epochs_migrate_to_one_partition_index_generation_and_publish_last() {
        let path = TestDatabasePath::new("partition-index-generation-migration");
        install_pre_generation_fixture(&path.0);

        let reopened = RedbStore::open(&path.0).expect("migrate generation fixture");
        assert_generation_fixture_migrated(&reopened);
    }

    #[test]
    fn prefix_epoch_migration_restarts_after_a_proven_precommit_failure() {
        let path = TestDatabasePath::new("partition-index-generation-restart");
        install_pre_generation_fixture(&path.0);
        let controller = RedbTestController::return_before_commit(
            RedbTestOperation::StorageFormatMigrationBatch,
        );
        let error = match RedbStore::open_with_test_controller(&path.0, controller) {
            Ok(_) => panic!("armed migration must fail before commit"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);

        let reopened = RedbStore::open(&path.0).expect("restart generation migration");
        assert_generation_fixture_migrated(&reopened);
    }

    #[test]
    fn wp373_event_copies_migrate_to_exact_references_before_registry_publication() {
        let path = TestDatabasePath::new("event-reference-migration");
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        store
            .initialize_database(database_id(0x1b))
            .expect("initialize current database");

        let transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin WP-373 fixture");
        let event_id = EventId::new(CommitSequence::first(), 0);
        let commit_key = encode_application_sequence_key(CommitSequence::first());
        let event_key = encode_event_key(event_id);
        let event = wp373_fixture_envelope("riffdb.storage.v1.StoredDurableEventV1");
        let commit = wp373_fixture_envelope("riffdb.storage.v1.StoredCommitRecordV1");
        let outbox = wp373_fixture_envelope("riffdb.storage.v1.StoredOutboxIntentV1");
        transaction
            .open_table(EVENTS)
            .expect("open events")
            .insert(event_key.as_slice(), event.as_slice())
            .expect("insert authoritative event");
        transaction
            .open_table(COMMITS)
            .expect("open commits")
            .insert(commit_key.as_slice(), commit.as_slice())
            .expect("insert historical commit");
        transaction
            .open_table(OUTBOX)
            .expect("open outbox")
            .insert(event_key.as_slice(), outbox.as_slice())
            .expect("insert historical outbox intent");
        let predecessor =
            encode_record_registry_v2(SchemaHash::from_bytes(PRE_EVENT_REFERENCE_REGISTRY_DIGEST))
                .expect("encode predecessor registry");
        transaction
            .open_table(META)
            .expect("open metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("install predecessor registry");
        transaction.commit().expect("commit WP-373 fixture");
        drop(store);

        let interrupted = RedbTestController::return_unknown_after_commit(
            RedbTestOperation::StorageFormatMigrationBatch,
        );
        assert_eq!(
            RedbStore::open_with_test_controller(&path.0, interrupted)
                .expect_err("injected postcommit uncertainty interrupts migration")
                .kind(),
            StorageErrorKind::CommitStatusUnknown
        );
        let migrated = RedbStore::open(&path.0).expect("migrate predecessor database");
        let read = migrated
            .shared
            .database
            .begin_read()
            .expect("read migrated database");
        let events = read.open_table(EVENTS).expect("open events");
        let commits = read.open_table(COMMITS).expect("open commits");
        let commit = commits
            .get(commit_key.as_slice())
            .expect("read commit")
            .expect("commit remains");
        // After the full migration chain, commits are current V3 entity references
        // (compact tag 17, revision 3); outbox remains V2 (tag 15, revision 2).
        assert_eq!(&commit.value()[..8], b"RDB2\x02\x11\0\x03");
        decode_commit_with_event_table(commit.value(), &events)
            .expect("migrated commit proves authoritative event");
        let outbox_table = read.open_table(OUTBOX).expect("open outbox");
        let outbox = outbox_table
            .get(event_key.as_slice())
            .expect("read outbox")
            .expect("outbox remains");
        assert_eq!(&outbox.value()[..8], b"RDB2\x02\x0f\0\x02");
        decode_outbox_with_event_table(outbox.value(), &events)
            .expect("migrated outbox proves authoritative event");
        let metadata = read.open_table(META).expect("open metadata");
        let registry = metadata
            .get(META_RECORD_REGISTRY)
            .expect("read registry")
            .expect("registry remains");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode registry")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
    }

    fn v2_commit_fixture_from_legacy_v1() -> (Vec<u8>, Vec<u8>, EventId, CommitSequence) {
        let legacy_commit = legacy_v1_fixture_envelope("riffdb.storage.v1.StoredCommitRecordV1");
        let (v2, event) = riffdb_storage_api::rewrap_legacy_commit_v1_as_v2_fixture(&legacy_commit)
            .expect("rewrap legacy commit as v2");
        let decoded = riffdb_storage_api::decode_commit_record_v2(
            v2.as_bytes(),
            // Event is rehydrated after table load; for identity extract decode event alone.
            vec![
                riffdb_storage_api::decode_durable_event_v1(event.as_bytes())
                    .expect("decode event")
                    .into_parts()
                    .0,
            ],
        )
        .expect("v2 decodes with its event");
        let commit = decoded.into_parts().0;
        let event_id = commit.events()[0].event_id();
        (
            v2.into_bytes(),
            event.into_bytes(),
            event_id,
            commit.commit_sequence(),
        )
    }

    #[test]
    fn entity_reference_migration_transcodes_v2_commits_and_is_idempotent() {
        let path = TestDatabasePath::new("entity-reference-migration");
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        store
            .initialize_database(database_id(0x2e))
            .expect("initialize current database");

        let (v2, event, event_id, sequence) = v2_commit_fixture_from_legacy_v1();
        let commit_key = encode_application_sequence_key(sequence);
        let event_key = encode_event_key(event_id);

        let transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin entity-reference fixture");
        transaction
            .open_table(EVENTS)
            .expect("open events")
            .insert(event_key.as_slice(), event.as_slice())
            .expect("insert event");
        transaction
            .open_table(COMMITS)
            .expect("open commits")
            .insert(commit_key.as_slice(), v2.as_slice())
            .expect("insert v2 commit");
        let predecessor =
            encode_record_registry_v2(SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST))
                .expect("encode predecessor registry");
        transaction
            .open_table(META)
            .expect("open metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("install predecessor registry");
        transaction
            .commit()
            .expect("commit entity-reference fixture");
        drop(store);

        // Crash-restart at page boundary: first open is interrupted after a batch.
        let interrupted = RedbTestController::return_unknown_after_commit(
            RedbTestOperation::StorageFormatMigrationBatch,
        );
        assert_eq!(
            RedbStore::open_with_test_controller(&path.0, interrupted)
                .expect_err("injected postcommit uncertainty interrupts migration")
                .kind(),
            StorageErrorKind::CommitStatusUnknown
        );

        let migrated = RedbStore::open(&path.0).expect("migrate predecessor database");
        let read = migrated
            .shared
            .database
            .begin_read()
            .expect("read migrated database");
        let events = read.open_table(EVENTS).expect("open events");
        let commits = read.open_table(COMMITS).expect("open commits");
        let commit = commits
            .get(commit_key.as_slice())
            .expect("read commit")
            .expect("commit remains");
        // Compact V3: magic RDB2, format 2, compact tag 17, revision 3.
        assert_eq!(&commit.value()[..8], b"RDB2\x02\x11\0\x03");
        decode_commit_with_event_table(commit.value(), &events)
            .expect("migrated commit proves entity references");
        let metadata = read.open_table(META).expect("open metadata");
        let registry = metadata
            .get(META_RECORD_REGISTRY)
            .expect("read registry")
            .expect("registry remains");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode registry")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
        drop(metadata);
        drop(commits);
        drop(events);
        drop(read);
        drop(migrated);

        // Second open is a no-op: all pages abort without rewrite.
        let again = RedbStore::open(&path.0).expect("second open is no-op");
        let read = again
            .shared
            .database
            .begin_read()
            .expect("read after no-op");
        let registry = read
            .open_table(META)
            .expect("meta")
            .get(META_RECORD_REGISTRY)
            .expect("read")
            .expect("registry");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
    }

    #[test]
    fn entity_reference_migration_from_pre_audit_request_index_converges() {
        let path = TestDatabasePath::new("entity-reference-from-audit");
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        store
            .initialize_database(database_id(0x30))
            .expect("initialize");
        let (v2, event, event_id, sequence) = v2_commit_fixture_from_legacy_v1();
        let commit_key = encode_application_sequence_key(sequence);
        let event_key = encode_event_key(event_id);
        let transaction = store.shared.database.begin_write().expect("write");
        transaction
            .open_table(EVENTS)
            .expect("events")
            .insert(event_key.as_slice(), event.as_slice())
            .expect("insert event");
        transaction
            .open_table(COMMITS)
            .expect("commits")
            .insert(commit_key.as_slice(), v2.as_slice())
            .expect("insert v2 commit");
        // Enter the chain at the AuditRequestIndex step: terminal publish was
        // rewired from `current` to PRE_ENTITY_REFERENCE.
        let predecessor = encode_record_registry_v2(SchemaHash::from_bytes(
            PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST,
        ))
        .expect("encode pre-audit registry");
        transaction
            .open_table(META)
            .expect("meta")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("install pre-audit registry");
        transaction.commit().expect("commit fixture");
        drop(store);

        let migrated = RedbStore::open(&path.0).expect("migrate from PRE_AUDIT");
        let read = migrated.shared.database.begin_read().expect("read");
        let registry = read
            .open_table(META)
            .expect("meta")
            .get(META_RECORD_REGISTRY)
            .expect("get")
            .expect("registry");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
        let commits = read.open_table(COMMITS).expect("commits");
        let commit = commits
            .get(commit_key.as_slice())
            .expect("get")
            .expect("commit");
        assert_eq!(&commit.value()[..8], b"RDB2\x02\x11\0\x03");
    }

    #[test]
    fn entity_reference_migration_survives_damaged_events_row() {
        let path = TestDatabasePath::new("entity-reference-damaged-event");
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        store
            .initialize_database(database_id(0x31))
            .expect("initialize");
        let (v2, _event, _event_id, sequence) = v2_commit_fixture_from_legacy_v1();
        let commit_key = encode_application_sequence_key(sequence);
        let transaction = store.shared.database.begin_write().expect("write");
        // Missing event row (commit still references it). Migration must not
        // join EVENTS; damage surfaces later as a structural finding. A garbage
        // EVENTS payload would also fail the general compact-row pass, which is
        // outside this migration step.
        transaction
            .open_table(COMMITS)
            .expect("commits")
            .insert(commit_key.as_slice(), v2.as_slice())
            .expect("insert v2 commit");
        let predecessor =
            encode_record_registry_v2(SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST))
                .expect("encode predecessor");
        transaction
            .open_table(META)
            .expect("meta")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("install predecessor");
        transaction.commit().expect("commit fixture");
        drop(store);

        let migrated = RedbStore::open(&path.0).expect("migration must not join EVENTS");
        let read = migrated.shared.database.begin_read().expect("read");
        let registry = read
            .open_table(META)
            .expect("meta")
            .get(META_RECORD_REGISTRY)
            .expect("get")
            .expect("registry");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
        let commit = read
            .open_table(COMMITS)
            .expect("commits")
            .get(commit_key.as_slice())
            .expect("get")
            .expect("commit");
        assert_eq!(&commit.value()[..8], b"RDB2\x02\x11\0\x03");
    }

    #[test]
    fn entity_reference_migration_splits_501_rows_and_crash_restarts() {
        let path = TestDatabasePath::new("entity-reference-501");
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        store
            .initialize_database(database_id(0x32))
            .expect("initialize");
        let (v2_template, event, event_id, _) = v2_commit_fixture_from_legacy_v1();
        let event_key = encode_event_key(event_id);
        let transaction = store.shared.database.begin_write().expect("write");
        transaction
            .open_table(EVENTS)
            .expect("events")
            .insert(event_key.as_slice(), event.as_slice())
            .expect("insert event");
        {
            let mut commits = transaction.open_table(COMMITS).expect("commits");
            // 501 rows forces at least one page split at FORMAT_MIGRATION_MAX_ROWS=500.
            for seq in 1u64..=501 {
                let sequence = CommitSequence::new(seq).expect("sequence");
                let key = encode_application_sequence_key(sequence);
                // Reuse the same V2 payload; migration rewrites by content not key.
                commits
                    .insert(key.as_slice(), v2_template.as_slice())
                    .expect("insert commit row");
            }
        }
        let predecessor =
            encode_record_registry_v2(SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST))
                .expect("encode predecessor");
        transaction
            .open_table(META)
            .expect("meta")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("install predecessor");
        transaction.commit().expect("commit 501-row fixture");
        drop(store);

        let interrupted = RedbTestController::return_unknown_after_commit(
            RedbTestOperation::StorageFormatMigrationBatch,
        );
        assert_eq!(
            RedbStore::open_with_test_controller(&path.0, interrupted)
                .expect_err("page-boundary interrupt")
                .kind(),
            StorageErrorKind::CommitStatusUnknown
        );
        let migrated = RedbStore::open(&path.0).expect("resume migration converges");
        let read = migrated.shared.database.begin_read().expect("read");
        let registry = read
            .open_table(META)
            .expect("meta")
            .get(META_RECORD_REGISTRY)
            .expect("get")
            .expect("registry");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
        let commits = read.open_table(COMMITS).expect("commits");
        let mut v3_count = 0usize;
        for row in commits.iter().expect("iter") {
            let (_, value) = row.expect("row");
            if value.value().starts_with(b"RDB2\x02\x11\0\x03") {
                v3_count += 1;
            }
        }
        assert_eq!(v3_count, 501, "all rows transcoded to V3");
        drop(commits);
        drop(read);
        drop(migrated);
        // Second open no-ops (all pages abort).
        RedbStore::open(&path.0).expect("second open no-op");
    }

    #[test]
    fn entity_reference_migration_empty_database_path_converges() {
        let path = TestDatabasePath::new("entity-reference-empty");
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        store
            .initialize_database(database_id(0x2f))
            .expect("initialize");
        let predecessor =
            encode_record_registry_v2(SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST))
                .expect("encode predecessor");
        let transaction = store.shared.database.begin_write().expect("write");
        transaction
            .open_table(META)
            .expect("meta")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("install predecessor");
        transaction.commit().expect("commit predecessor");
        drop(store);
        let migrated = RedbStore::open(&path.0).expect("empty path migrates");
        let registry = migrated
            .shared
            .database
            .begin_read()
            .expect("read")
            .open_table(META)
            .expect("meta")
            .get(META_RECORD_REGISTRY)
            .expect("get")
            .expect("registry");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
    }

    #[test]
    fn event_routes_rebuild_idempotently_before_registry_publication() {
        let path = TestDatabasePath::new("event-route-migration");
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        store
            .initialize_database(database_id(0x1c))
            .expect("initialize current database");

        let event_id = EventId::new(CommitSequence::first(), 0);
        let commit_key = encode_application_sequence_key(CommitSequence::first());
        let event_key = encode_event_key(event_id);
        let event = wp373_fixture_envelope("riffdb.storage.v1.StoredDurableEventV1");
        let commit = wp373_fixture_envelope("riffdb.storage.v1.StoredCommitRecordV1");
        let predecessor =
            encode_record_registry_v2(SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST))
                .expect("encode predecessor registry");
        let transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin event-route fixture");
        transaction
            .open_table(EVENTS)
            .expect("open events")
            .insert(event_key.as_slice(), event.as_slice())
            .expect("insert authoritative event");
        transaction
            .open_table(COMMITS)
            .expect("open commits")
            .insert(commit_key.as_slice(), commit.as_slice())
            .expect("insert historical commit");
        transaction
            .open_table(META)
            .expect("open metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("install predecessor registry");
        transaction.commit().expect("commit predecessor fixture");
        drop(store);

        let interrupted = RedbTestController::return_unknown_after_commit(
            RedbTestOperation::StorageFormatMigrationBatch,
        );
        assert_eq!(
            RedbStore::open_with_test_controller(&path.0, interrupted)
                .expect_err("injected postcommit uncertainty interrupts route migration")
                .kind(),
            StorageErrorKind::CommitStatusUnknown
        );

        let migrated = RedbStore::open(&path.0).expect("resume event-route migration");
        let read = migrated
            .shared
            .database
            .begin_read()
            .expect("read migrated database");
        let events = read.open_table(EVENTS).expect("open events");
        let commits = read.open_table(COMMITS).expect("open commits");
        let encoded_commit = commits
            .get(commit_key.as_slice())
            .expect("read commit")
            .expect("commit remains");
        let decoded_commit = decode_commit_with_event_table(encoded_commit.value(), &events)
            .expect("decode authoritative commit")
            .into_parts()
            .0;
        let event = decoded_commit.events().first().expect("commit event");
        let route_key = encode_event_route_key(decoded_commit.partition_hash(), event_id);
        let routes = read.open_table(EVENT_ROUTES).expect("open event routes");
        let encoded_route = routes
            .get(route_key.as_slice())
            .expect("read route")
            .expect("rebuilt route exists");
        let route = decode_event_route_v1(encoded_route.value())
            .expect("decode route")
            .into_parts()
            .0;
        assert_eq!(route.event_id(), event.event_id());
        assert_eq!(route.event_type_id(), event.event_type_id());
        assert_eq!(route.event_hash(), event.event_hash());
        let metadata = read.open_table(META).expect("open metadata");
        let registry = metadata
            .get(META_RECORD_REGISTRY)
            .expect("read registry")
            .expect("registry remains");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode registry")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
    }

    #[test]
    fn current_format_rejects_unknown_compact_identity_before_probe() {
        let path = TestDatabasePath::new("unknown-compact-tag");
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        store
            .initialize_database(database_id(0x1a))
            .expect("initialize current database");
        let transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin corruption fixture");
        let mut metadata = transaction.open_table(META).expect("open metadata");
        let mut identity = metadata
            .get(META_DATABASE_ID)
            .expect("read identity")
            .expect("identity exists")
            .value()
            .to_vec();
        identity[5] = u8::MAX;
        metadata
            .insert(META_DATABASE_ID, identity.as_slice())
            .expect("install unknown tag");
        drop(metadata);
        transaction.commit().expect("commit corruption fixture");
        drop(store);

        assert_eq!(
            RedbStore::open(&path.0)
                .expect_err("unknown compact identity must fail")
                .kind(),
            StorageErrorKind::IncompatibleFormat
        );
    }

    #[test]
    fn initialization_recheck_returns_the_durable_winner() {
        let path = TestDatabasePath::new("winner");
        let mut first = RedbStore::open(&path.0).expect("open store");
        let mut second = first.reopen_for_test();
        let winner = database_id(0x22);
        let loser = database_id(0x33);

        assert_eq!(
            first.initialize_database(winner).expect("install winner"),
            DatabaseInitializationResult::Installed(winner)
        );
        assert_eq!(
            second.initialize_database(loser).expect("observe winner"),
            DatabaseInitializationResult::ConcurrentWinner(winner)
        );
    }

    #[test]
    fn precommit_failure_is_proven_absent_and_postcommit_unknown_fences_writes() {
        let before_path = TestDatabasePath::new("before-commit");
        let before = RedbTestController::return_before_commit(RedbTestOperation::Initialization);
        let mut store = RedbStore::open_with_test_controller(&before_path.0, before)
            .expect("open controlled store");
        assert_eq!(
            store
                .initialize_database(database_id(0x44))
                .expect_err("injected precommit failure")
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            store.probe_database_identity().expect("probe after abort"),
            DatabaseIdentityProbe::NeedsInitialization
        );

        let after_path = TestDatabasePath::new("after-commit");
        let after =
            RedbTestController::return_unknown_after_commit(RedbTestOperation::Initialization);
        let mut store = RedbStore::open_with_test_controller(&after_path.0, after)
            .expect("open controlled store");
        assert_eq!(
            store
                .initialize_database(database_id(0x55))
                .expect_err("injected uncertain response")
                .kind(),
            StorageErrorKind::CommitStatusUnknown
        );
        assert_eq!(
            store.probe_database_identity().expect("durable winner"),
            DatabaseIdentityProbe::Existing(database_id(0x55))
        );
        assert_eq!(
            store
                .initialize_database(database_id(0x66))
                .expect_err("fenced handle")
                .kind(),
            StorageErrorKind::Unavailable
        );

        drop(store);
        let reopened = RedbStore::open(&after_path.0).expect("reopen clears process-local fence");
        assert_eq!(
            reopened.probe_database_identity().expect("reopen identity"),
            DatabaseIdentityProbe::Existing(database_id(0x55))
        );
    }

    #[test]
    fn postcommit_accelerator_failure_preserves_known_success_and_does_not_fence_core_writes() {
        let path = TestDatabasePath::new("postcommit-accelerator");
        let mut store = RedbStore::open(&path.0).expect("open store");
        store
            .initialize_database(database_id(0x77))
            .expect("initialize store");
        let dormant = RedbDormantPorts {
            shared: store.shared,
        };
        let ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate ports");
        let event_id = EventId::new(CommitSequence::first(), 0);

        for _ in 0..2 {
            ports
                .begin_write()
                .expect("begin known-success transaction")
                .commit_for_with_delta(
                    RedbTestOperation::CommandBatch,
                    Some(TransientIndexDelta::PendingOutboxInserted(vec![event_id])),
                )
                .expect("confirmed engine commit remains known success");
        }
        assert_eq!(
            ports
                .pending_outbox_page(None, 1)
                .expect_err("duplicate delta degrades only the outbox accelerator")
                .kind(),
            StorageErrorKind::Unavailable
        );
        ports
            .begin_write()
            .expect("core writes remain unfenced")
            .abort()
            .expect("abort proof transaction");
    }

    #[test]
    fn current_digest_reopen_skips_index_epoch_scan_after_repair_marker() {
        let path = TestDatabasePath::new("epoch-repair-marker-skip");
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(database_id(0x73))
            .expect("initialize");
        // Fresh init installs the repair marker; reopen must not require a scan.
        assert!(
            !index_epoch_rows_may_need_legacy_repair(&store.shared).expect("gate"),
            "init marker must short-circuit the full secondary-index scan"
        );
        store
            .complete_partition_index_generation_migration()
            .expect("current digest no-op");
        assert!(
            !index_epoch_rows_may_need_legacy_repair(&store.shared).expect("gate again"),
            "reopen remains O(1)"
        );
    }

    #[test]
    fn history_incarnation_migration_inserts_initial_and_is_idempotent() {
        let path = TestDatabasePath::new("history-incarnation-migrate");
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(database_id(0x71))
            .expect("initialize");

        // Simulate a pre-fence database: drop the key and roll the registry
        // digest back to PRE_HISTORY_INCARNATION.
        {
            let transaction = store.shared.database.begin_write().expect("begin write");
            {
                let mut meta = transaction.open_table(META).expect("meta");
                meta.remove(META_HISTORY_INCARNATION).expect("remove key");
                let predecessor = encode_record_registry_v2(SchemaHash::from_bytes(
                    PRE_HISTORY_INCARNATION_REGISTRY_DIGEST,
                ))
                .expect("encode predecessor");
                meta.insert(META_RECORD_REGISTRY, predecessor.as_bytes())
                    .expect("install predecessor");
            }
            transaction.commit().expect("commit pre-fence fixture");
        }

        store
            .complete_partition_index_generation_migration()
            .expect("migrate history incarnation");

        let read = store.shared.database.begin_read().expect("read");
        let meta = read.open_table(META).expect("meta");
        let encoded = meta
            .get(META_HISTORY_INCARNATION)
            .expect("get")
            .expect("history key present after migration");
        let incarnation = *decode_history_incarnation_v1(encoded.value())
            .expect("decode")
            .value();
        assert_eq!(incarnation, HISTORY_INCARNATION_INITIAL);
        let registry = meta
            .get(META_RECORD_REGISTRY)
            .expect("registry get")
            .expect("registry present");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode registry")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
        drop(registry);
        drop(encoded);
        drop(meta);
        drop(read);

        // Second open / migration is a no-op for both digest and key value.
        store
            .complete_partition_index_generation_migration()
            .expect("idempotent migration");
        let read = store.shared.database.begin_read().expect("read again");
        let meta = read.open_table(META).expect("meta");
        let encoded = meta
            .get(META_HISTORY_INCARNATION)
            .expect("get")
            .expect("still present");
        assert_eq!(
            *decode_history_incarnation_v1(encoded.value())
                .expect("decode")
                .value(),
            HISTORY_INCARNATION_INITIAL
        );
    }

    #[test]
    fn current_digest_still_runs_index_generation_row_repair() {
        // I4: short-circuit must not skip migrate_partition_index_generations
        // even when the registry digest is already current.
        let path = TestDatabasePath::new("current-digest-epoch-repair");
        let mut store = RedbStore::open(&path.0).expect("open");
        store
            .initialize_database(database_id(0x72))
            .expect("initialize");

        // Simulate a pre-marker database that published the current digest while
        // still holding legacy INDEX_EPOCHS rows: clear the init repair marker.
        {
            let transaction = store.shared.database.begin_write().expect("write");
            {
                let mut meta = transaction.open_table(META).expect("meta");
                meta.remove(META_INDEX_EPOCH_ROWS_REPAIRED)
                    .expect("clear marker");
            }
            transaction.commit().expect("commit");
        }

        // Full downgrade_all_index_rows_to_v1_fixture shape: V2 index entry +
        // legacy epoch row, with the current registry digest left in place.
        let index_id = IndexId::new(9).expect("index");
        let mut index = IndexEntryKeyBuilder::new(index_id);
        index.push_u64(11).expect("index component");
        let mut entity = EntityKeyBuilder::new(EntityTypeId::first());
        entity.push_u64(1).expect("entity component");
        let index_key = index
            .finish(entity.finish().expect("entity key"))
            .expect("index key");
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(3).expect("partition component");
        let partition = partition.finish().expect("partition key");
        let binding = DurableKeySchemaBindingV1::new(
            ContractLineage::new("repair-i4").expect("lineage"),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([0x45; 32]),
        );
        let entry = StoredIndexEntryV2::new(
            index_key.clone(),
            binding.clone(),
            CanonicalRecord::new(Vec::new()).expect("covered values"),
            partition,
        )
        .expect("index entry");
        let encoded_entry = crate::codec::encode_index_entry_v2(&entry).expect("encode index");
        let mut live_prefix = IndexRangePrefixBuilder::new(index_id);
        live_prefix.push_u64(11).expect("prefix component");
        let prefix = StructurallyDecodedIndexRangePrefixV1::from_live(&live_prefix.finish());
        let legacy = LegacyStoredIndexEpochV1::new(
            prefix.clone(),
            binding,
            IndexEpoch::new(3).expect("legacy generation"),
        );
        let encoded_legacy =
            encode_legacy_index_epoch_v1_fixture(&legacy).expect("encode legacy generation");
        {
            let transaction = store.shared.database.begin_write().expect("write");
            transaction
                .open_table(SECONDARY_INDEXES)
                .expect("indexes")
                .insert(index_key.as_bytes(), encoded_entry.as_bytes())
                .expect("insert index");
            transaction
                .open_table(INDEX_EPOCHS)
                .expect("epochs")
                .insert(
                    encode_index_range_prefix_key(&prefix),
                    encoded_legacy.as_bytes(),
                )
                .expect("insert legacy");
            transaction.commit().expect("commit legacy fixture");
        }

        // Registry is already current — this is the I4 short-circuit path.
        {
            let read = store.shared.database.begin_read().expect("read");
            let meta = read.open_table(META).expect("meta");
            let registry = meta
                .get(META_RECORD_REGISTRY)
                .expect("get")
                .expect("registry");
            assert_eq!(
                *decode_record_registry_v2(registry.value())
                    .expect("decode")
                    .value(),
                riffdb_storage_api::proto_codec::current_record_registry_digest()
            );
        }

        store
            .complete_partition_index_generation_migration()
            .expect("row repair on current digest");

        let read = store.shared.database.begin_read().expect("read");
        let epochs = read.open_table(INDEX_EPOCHS).expect("epochs");
        let rows = epochs
            .iter()
            .expect("iter")
            .collect::<Result<Vec<_>, _>>()
            .expect("rows");
        assert_eq!(rows.len(), 1);
        let (_key, value) = &rows[0];
        let generation = decode_index_epoch_v1(value.value())
            .expect("legacy row repaired to current encoding")
            .into_parts()
            .0;
        // Migration rewrites prefix-keyed legacy rows to partition-keyed current
        // encoding; the retained epoch is at least the legacy maximum.
        assert!(generation.epoch().get() >= 3);
    }
}
