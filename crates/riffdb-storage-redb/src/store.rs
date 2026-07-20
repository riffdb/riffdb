//! Dormant redb handle, identity probe, and atomic initialization.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use redb::{
    Database, Durability, MultimapTableHandle, ReadTransaction, ReadableDatabase, ReadableTable,
    TableHandle, WriteTransaction,
};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, DatabaseIdentityProbe, DatabaseIdentityProbePort,
    DatabaseInitializationPort, DatabaseInitializationResult, StorageError, StorageErrorKind,
    StorageFormatVersion,
    proto_codec::{
        decode_administration_sequence_allocator_v1, decode_application_sequence_allocator_v1,
        decode_database_identity_v1, decode_storage_format_version_v1,
        encode_administration_sequence_allocator_v1, encode_application_sequence_allocator_v1,
        encode_database_identity_v1, encode_storage_format_version_v1,
    },
};
use riffdb_types::DatabaseId;

use crate::error::{
    commit_error, database_error, precommit_storage_error, storage_error, table_error,
    transaction_error,
};
use crate::gate::{ExclusiveGate, ExclusiveLease};
use crate::hooks::{RedbTestController, RedbTestOperation};
use crate::layout::{
    META, META_ADMINISTRATION_SEQUENCE, META_APPLICATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP,
    META_DATABASE_ID, META_FORMAT_VERSION, META_KEYS, TABLE_NAMES, create_all_tables,
};
use crate::transient::{TransientIndexDelta, TransientIndexState, TransientIndexes};

pub(crate) struct SharedRedb {
    pub(crate) database: Database,
    #[allow(dead_code, reason = "WP-070 offline backup consumes the source path")]
    path: PathBuf,
    mutation_gate: ExclusiveGate,
    write_fenced: AtomicBool,
    test_controller: Option<RedbTestController>,
    transient_indexes: Mutex<TransientIndexState>,
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

impl RedbStore {
    /// Opens an existing redb file or creates an empty redb container.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_inner(path.as_ref(), None)
    }

    /// Opens a database with one closed process-test failpoint controller.
    #[doc(hidden)]
    pub fn open_with_test_controller(
        path: impl AsRef<Path>,
        controller: RedbTestController,
    ) -> Result<Self, StorageError> {
        Self::open_inner(path.as_ref(), Some(controller))
    }

    fn open_inner(
        path: &Path,
        test_controller: Option<RedbTestController>,
    ) -> Result<Self, StorageError> {
        let path = path.to_path_buf();
        let database = Database::create(&path).map_err(database_error)?;
        Ok(Self {
            shared: Arc::new(SharedRedb {
                database,
                path,
                mutation_gate: ExclusiveGate::default(),
                write_fenced: AtomicBool::new(false),
                test_controller,
                transient_indexes: Mutex::new(TransientIndexState::Dormant),
            }),
        })
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

    pub(crate) fn fence_writes(&self) {
        self.shared.write_fenced.store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn reopen_for_test(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
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
        transaction.set_two_phase_commit(true);
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
        if let Err(error) = transaction.commit() {
            self.shared.write_fenced.store(true, Ordering::Release);
            self.invalidate_transient_indexes();
            return Err(commit_error(error));
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

    pub(crate) fn ensure_pending_outbox_available(&self) -> Result<(), StorageError> {
        self.shared.pending_outbox_page(None, 0).map(|_| ())
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
                if let Err(error) = transaction.commit() {
                    self.fence_writes();
                    return Err(commit_error(error));
                }
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
    let format = encode_storage_format_version_v1(StorageFormatVersion::V1)
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
            _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
    }

    for required in [
        META_FORMAT_VERSION,
        META_DATABASE_ID,
        META_APPLICATION_SEQUENCE,
        META_ADMINISTRATION_SEQUENCE,
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

    use riffdb_types::{CommitSequence, EventId};

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
