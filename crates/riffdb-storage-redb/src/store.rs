//! Dormant redb handle, identity probe, and atomic initialization.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use redb::{
    Database, Durability, MultimapTableHandle, ReadTransaction, ReadableDatabase, ReadableTable,
    ReadableTableMetadata, TableDefinition, TableHandle, WriteTransaction,
};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, DatabaseIdentityProbe, DatabaseIdentityProbePort,
    DatabaseInitializationPort, DatabaseInitializationResult, StorageError, StorageErrorKind,
    StorageFormatVersion, StoredIndexEpochV1,
    proto_codec::{
        decode_administration_sequence_allocator_v1, decode_application_sequence_allocator_v1,
        decode_database_identity_v1, decode_record_registry_v2, decode_storage_format_version_v1,
        encode_administration_sequence_allocator_v1, encode_application_sequence_allocator_v1,
        encode_database_identity_v1, encode_record_registry_v2, encode_storage_format_version_v1,
        transcode_durable_record_to_v2,
    },
};
use riffdb_types::{DatabaseId, IndexEpoch, IndexId, SchemaHash};

use crate::codec::{
    decode_commit_with_event_table, decode_index_entry_v2, decode_index_epoch_v1,
    decode_legacy_index_epoch_v1, decode_outbox_with_event_table, encode_commit_record_v1,
    encode_index_epoch_v1, encode_outbox_intent_v1,
};
use crate::error::{
    commit_error, database_error, precommit_storage_error, storage_error, table_error,
    transaction_error,
};
use crate::gate::{ExclusiveGate, ExclusiveLease};
use crate::hooks::{RedbTestController, RedbTestOperation};
use crate::keys::{
    decode_index_range_prefix_key, decode_partition_index_key, encode_partition_index_key,
};
use crate::layout::{
    BYTE_TABLES, COMMITS, EVENTS, INDEX_EPOCHS, META, META_ADMINISTRATION_SEQUENCE,
    META_APPLICATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP, META_DATABASE_ID, META_FORMAT_VERSION,
    META_KEYS, META_RECORD_REGISTRY, OUTBOX, SECONDARY_INDEXES, TABLE_NAMES, create_all_tables,
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

    pub(crate) fn durable_commit_epoch(&self) -> u64 {
        self.durable_commit_epoch.load(Ordering::Acquire)
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
    EventReferencesThenGenerations,
    Generations,
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
        let database = Database::create(&path).map_err(database_error)?;
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
            }),
        };
        store.ensure_current_storage_format()?;
        Ok(store)
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
                    == &SchemaHash::from_bytes(PRE_EVENT_REFERENCE_REGISTRY_DIGEST)
                {
                    RegistryMigration::EventReferencesThenGenerations
                } else if observed.value()
                    == &SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST)
                {
                    RegistryMigration::Generations
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
        migrate_partition_index_generations(&self.shared)?;
        publish_record_registry(
            &self.shared,
            SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST),
            riffdb_storage_api::proto_codec::current_record_registry_digest(),
        )
    }

    pub(crate) fn fence_writes(&self) {
        self.shared.fence_writes();
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
    validate_partition_index_generation_rows(shared)
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
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let indexes = TransientIndexes::rebuild(&transaction)?;
        drop(transaction);
        let mut state = self
            .shared
            .transient_indexes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if !matches!(*state, TransientIndexState::Dormant) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        *state = TransientIndexState::Ready(indexes);
        drop(state);
        Ok(RedbOperationalPorts {
            shared: self.shared,
        })
    }
}

impl RedbOperationalPorts {
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
        let state = self
            .transient_indexes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match &*state {
            TransientIndexState::Ready(indexes) => indexes
                .service_audit_sequences(request_id)
                .map(|sequences| sequences.to_vec())
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable)),
            TransientIndexState::Dormant | TransientIndexState::Invalid => {
                Err(storage_error(StorageErrorKind::Unavailable))
            }
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

    fn wp373_fixture_envelope(record_type: &str) -> Vec<u8> {
        let line = include_str!("../../../fixtures/proto/durable-wire-vectors-v2.txt")
            .lines()
            .find(|line| line.starts_with(record_type))
            .expect("WP-373 fixture record exists");
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
            let (_, value) = row.expect("read migrated row");
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
        assert_eq!(&commit.value()[..8], b"RDB2\x02\x11\0\x02");
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
}
