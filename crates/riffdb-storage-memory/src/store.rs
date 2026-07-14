//! Volatile database handle and short state-lock helpers.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use riffdb_storage_api::{
    DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
    DatabaseInitializationResult, OpenSessionId, RetainedMetadataV1, StorageError,
    StorageErrorKind,
};
use riffdb_types::DatabaseId;

use crate::gate::{ExclusiveGate, ExclusiveLease};
use crate::startup::MemoryDormantPorts;
use crate::state::{MemoryMetadataSlot, MemoryState, PreparedMemoryDelta};

static NEXT_OPEN_SESSION: AtomicU64 = AtomicU64::new(1);

pub(crate) struct SharedMemory {
    gate: ExclusiveGate,
    state: Mutex<MemoryState>,
}

/// A dormant handle to one volatile in-memory database.
///
/// The handle exposes initialization and structural-open behavior only. It is
/// deliberately not an operational readiness proof.
pub struct MemoryStore {
    pub(crate) shared: Arc<SharedMemory>,
}

/// Activated memory-backend port bundle.
///
/// This type has no public constructor or activation method. The storage-memory
/// crate can create it only by consuming the dormant ports released by a
/// completed structural evidence session. It is not itself a readiness proof;
/// production readiness composition remains owned by WP-130 and uses redb.
pub struct MemoryOperationalPorts {
    shared: Arc<SharedMemory>,
}

pub(crate) struct MemoryAccess {
    shared: Arc<SharedMemory>,
    _lease: ExclusiveLease,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    /// Creates one empty volatile database.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shared: Arc::new(SharedMemory {
                gate: ExclusiveGate::default(),
                state: Mutex::new(MemoryState::default()),
            }),
        }
    }

    /// Duplicates dormant ownership for internal composition and tests only.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn reopen(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn acquire(&self) -> Result<MemoryAccess, StorageError> {
        let lease = self.shared.gate.acquire()?;
        Ok(MemoryAccess {
            shared: Arc::clone(&self.shared),
            _lease: lease,
        })
    }

    #[cfg(test)]
    pub(crate) fn gate_is_held(&self) -> bool {
        self.shared.gate.is_held()
    }
}

impl MemoryDormantPorts {
    /// Internal activation boundary for memory conformance and trait adapters.
    ///
    /// No public caller can bypass the server-owned structural/catalog proof
    /// composition by turning a directly constructed `MemoryStore` operational.
    #[allow(dead_code)]
    pub(crate) fn into_operational(self) -> MemoryOperationalPorts {
        MemoryOperationalPorts {
            shared: self.store.shared,
        }
    }
}

impl MemoryOperationalPorts {
    /// Acquires the exclusive lease for a consuming typed transition.
    ///
    /// The access value may cross storage type-state values, but a state mutex
    /// guard never does. Dropping an unfinished transition releases the lease
    /// without changing committed state.
    pub(crate) fn acquire(&self) -> Result<MemoryAccess, StorageError> {
        let lease = self.shared.gate.acquire()?;
        Ok(MemoryAccess {
            shared: Arc::clone(&self.shared),
            _lease: lease,
        })
    }

    /// Runs one ordinary operational read without taking the mutation gate.
    #[allow(dead_code)]
    pub(crate) fn read<R>(
        &self,
        operation: impl FnOnce(&MemoryState) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        self.shared.read(operation)
    }

    /// Prepares and atomically applies one short operational mutation.
    ///
    /// The exclusive gate is acquired before the state mutex. Preparation sees
    /// immutable state and completes every semantic check; application returns
    /// no recoverable error.
    #[allow(dead_code)]
    pub(crate) fn apply_prepared<D>(
        &self,
        prepare: impl FnOnce(&MemoryState) -> Result<D, StorageError>,
    ) -> Result<D::Output, StorageError>
    where
        D: PreparedMemoryDelta,
    {
        self.shared.apply_prepared(prepare)
    }
}

impl SharedMemory {
    /// Ordinary reads serialize only on the state mutex. Operational port
    /// activation guarantees they cannot exist during the exclusive startup pass.
    pub(crate) fn read<R>(
        &self,
        operation: impl FnOnce(&MemoryState) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let state = self.state()?;
        operation(&state)
    }

    fn apply_prepared<D>(
        &self,
        prepare: impl FnOnce(&MemoryState) -> Result<D, StorageError>,
    ) -> Result<D::Output, StorageError>
    where
        D: PreparedMemoryDelta,
    {
        let _lease = self.gate.acquire()?;
        let mut state = self.state()?;
        let delta = prepare(&state)?;
        Ok(delta.apply(&mut state))
    }

    fn state(&self) -> Result<MutexGuard<'_, MemoryState>, StorageError> {
        self.state
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))
    }
}

impl MemoryAccess {
    /// Locks are always acquired in gate-then-state order. The state guard is
    /// scoped to this call and never survives in a consuming semantic state.
    pub(crate) fn read<R>(
        &self,
        operation: impl FnOnce(&MemoryState) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        self.shared.read(operation)
    }

    pub(crate) fn write<R>(
        &self,
        operation: impl FnOnce(&mut MemoryState) -> Result<R, StorageError>,
    ) -> Result<R, StorageError> {
        let mut state = self.state()?;
        operation(&mut state)
    }

    pub(crate) fn allocate_open_session(&self) -> Result<OpenSessionId, StorageError> {
        let value = NEXT_OPEN_SESSION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| storage_error(StorageErrorKind::SequenceExhausted))?;
        OpenSessionId::new(value).ok_or_else(|| storage_error(StorageErrorKind::SequenceExhausted))
    }

    pub(crate) fn into_store(self) -> MemoryStore {
        let shared = Arc::clone(&self.shared);
        drop(self);
        MemoryStore { shared }
    }

    fn state(&self) -> Result<MutexGuard<'_, MemoryState>, StorageError> {
        self.shared.state()
    }
}

impl DatabaseIdentityProbePort for MemoryStore {
    fn probe_database_identity(&self) -> Result<DatabaseIdentityProbe, StorageError> {
        self.shared.read(|state| match &state.metadata {
            MemoryMetadataSlot::Retained(metadata) => {
                Ok(DatabaseIdentityProbe::Existing(metadata.database_id()))
            }
            MemoryMetadataSlot::Absent if state.is_truly_empty() => {
                Ok(DatabaseIdentityProbe::NeedsInitialization)
            }
            MemoryMetadataSlot::Absent => Err(storage_error(StorageErrorKind::CorruptData)),
            #[cfg(test)]
            MemoryMetadataSlot::Corrupt => Err(storage_error(StorageErrorKind::CorruptData)),
        })
    }
}

impl DatabaseInitializationPort for MemoryStore {
    fn initialize_database(
        &mut self,
        candidate: DatabaseId,
    ) -> Result<DatabaseInitializationResult, StorageError> {
        let access = self.acquire()?;
        access.write(|state| match &state.metadata {
            MemoryMetadataSlot::Retained(metadata) => Ok(
                DatabaseInitializationResult::ConcurrentWinner(metadata.database_id()),
            ),
            MemoryMetadataSlot::Absent if state.is_truly_empty() => {
                state.metadata =
                    MemoryMetadataSlot::Retained(RetainedMetadataV1::initial(candidate));
                Ok(DatabaseInitializationResult::Installed(candidate))
            }
            MemoryMetadataSlot::Absent => Err(storage_error(StorageErrorKind::CorruptData)),
            #[cfg(test)]
            MemoryMetadataSlot::Corrupt => Err(storage_error(StorageErrorKind::CorruptData)),
        })
    }
}

impl fmt::Debug for MemoryStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MemoryStore([DORMANT])")
    }
}

impl fmt::Debug for MemoryOperationalPorts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MemoryOperationalPorts([OPERATIONAL])")
    }
}

pub(crate) fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{
        DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
        DatabaseInitializationResult, StorageErrorKind,
    };

    use super::*;

    fn database(byte: u8) -> DatabaseId {
        let mut bytes = [0; 16];
        bytes[..10].copy_from_slice(&[0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02]);
        bytes[15] = byte;
        DatabaseId::from_bytes(bytes).expect("UUIDv7 database ID")
    }

    #[test]
    fn initialization_is_source_free_and_reopens_with_the_durable_winner() {
        let mut store = MemoryStore::new();
        assert_eq!(
            store.probe_database_identity().expect("empty probe"),
            DatabaseIdentityProbe::NeedsInitialization
        );

        let installed = store
            .initialize_database(database(1))
            .expect("initialize database");
        assert_eq!(
            installed,
            DatabaseInitializationResult::Installed(database(1))
        );
        assert_eq!(
            store
                .reopen()
                .probe_database_identity()
                .expect("reopen probe"),
            DatabaseIdentityProbe::Existing(database(1))
        );
    }

    #[test]
    fn initialization_returns_a_concurrent_winner_without_candidate_comparison() {
        let mut losing_handle = MemoryStore::new();
        let mut winning_handle = losing_handle.reopen();
        assert_eq!(
            losing_handle
                .probe_database_identity()
                .expect("initial probe"),
            DatabaseIdentityProbe::NeedsInitialization
        );

        winning_handle
            .initialize_database(database(2))
            .expect("winning initialization");
        assert_eq!(
            losing_handle
                .initialize_database(database(3))
                .expect("loser observes winner"),
            DatabaseInitializationResult::ConcurrentWinner(database(2))
        );
    }

    #[test]
    fn ordinary_reads_do_not_take_the_exclusive_mutation_gate() {
        let mut store = MemoryStore::new();
        store
            .initialize_database(database(9))
            .expect("initialize database");
        let exclusive = store.acquire().expect("hold startup-style exclusion");
        assert!(store.gate_is_held());

        assert_eq!(
            store
                .probe_database_identity()
                .expect("read uses only the state mutex"),
            DatabaseIdentityProbe::Existing(database(9))
        );
        drop(exclusive);
    }

    #[test]
    fn malformed_or_partial_metadata_fails_closed() {
        let mut corrupt = MemoryStore::new();
        corrupt
            .acquire()
            .expect("access")
            .write(|state| {
                state.metadata = MemoryMetadataSlot::Corrupt;
                Ok(())
            })
            .expect("inject corrupt slot");
        assert_eq!(
            corrupt
                .probe_database_identity()
                .expect_err("corrupt metadata must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            corrupt
                .initialize_database(database(4))
                .expect_err("initialization must not overwrite corruption")
                .kind(),
            StorageErrorKind::CorruptData
        );

        let mut partial = MemoryStore::new();
        partial
            .acquire()
            .expect("access")
            .write(|state| {
                state.injected_structural_findings.push(
                    riffdb_storage_api::StructuralFinding::new(
                        riffdb_storage_api::StructuralFindingScope::Authoritative,
                        riffdb_storage_api::StructuralFindingCode::MalformedRecord,
                    ),
                );
                Ok(())
            })
            .expect("inject partial state");
        assert_eq!(
            partial
                .initialize_database(database(5))
                .expect_err("partial data must not be overwritten")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }
}
