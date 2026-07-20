//! Production composition boundary for durable database initialization.

use riffdb_storage_api::{
    DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
    DatabaseInitializationResult, StartupValidationInputs, StorageError, StructuralEvidenceOpen,
};
use riffdb_types::DatabaseId;

/// Commit-owned wrapper around the source-free probe and atomic initialization port.
///
/// The wrapped storage value remains private. Production composition can inspect
/// identity through [`Self::probe`], but it receives a candidate-accepting
/// continuation only when storage proves that initialization is needed.
///
/// Structural evidence cannot begin before the probe consumes this value:
///
/// ```compile_fail
/// use riffdb_commit::DatabaseInitializationExecutor;
/// use riffdb_storage_api::{
///     DatabaseIdentityProbePort, DatabaseInitializationPort, StartupValidationInputs,
///     StructuralEvidenceOpen,
/// };
///
/// fn open_too_early<S>(
///     executor: DatabaseInitializationExecutor<S>,
///     inputs: StartupValidationInputs,
/// ) where
///     S: DatabaseIdentityProbePort + DatabaseInitializationPort + StructuralEvidenceOpen,
/// {
///     let _ = executor.begin_structural_evidence(inputs);
/// }
/// ```
pub struct DatabaseInitializationExecutor<Storage> {
    storage: Storage,
}

impl<Storage> DatabaseInitializationExecutor<Storage>
where
    Storage: DatabaseIdentityProbePort + DatabaseInitializationPort,
{
    /// Wraps the complete storage-side initialization capability.
    #[must_use]
    pub const fn new(storage: Storage) -> Self {
        Self { storage }
    }

    /// Performs the source-free identity probe.
    ///
    /// An existing identity is returned directly and no candidate can be
    /// supplied on that branch. Only `NeedsInitialization` yields a one-shot
    /// permit through which a caller may pass an already checked candidate.
    pub fn probe(self) -> Result<DatabaseInitializationDecision<Storage>, StorageError> {
        match self.storage.probe_database_identity()? {
            DatabaseIdentityProbe::Existing(database_id) => Ok(
                DatabaseInitializationDecision::Existing(InitializedDatabase {
                    database_id,
                    storage: self.storage,
                }),
            ),
            DatabaseIdentityProbe::NeedsInitialization => Ok(
                DatabaseInitializationDecision::NeedsInitialization(DatabaseInitializationPermit {
                    storage: self.storage,
                }),
            ),
        }
    }
}

/// Result of the source-free identity probe through the commit-owned executor.
pub enum DatabaseInitializationDecision<Storage> {
    /// Storage already has a durable identity and may begin structural evidence.
    Existing(InitializedDatabase<Storage>),
    /// Storage is truly empty and exposes a one-shot initialization continuation.
    NeedsInitialization(DatabaseInitializationPermit<Storage>),
}

/// One-shot authority to submit a checked database identity candidate.
///
/// This value is constructed only by [`DatabaseInitializationExecutor::probe`]
/// after `NeedsInitialization`. Consuming it prevents a second transition call
/// through the same decision, and the underlying storage handle never escapes.
///
/// Structural evidence cannot begin until initialization succeeds:
///
/// ```compile_fail
/// use riffdb_commit::DatabaseInitializationPermit;
/// use riffdb_storage_api::{
///     DatabaseInitializationPort, StartupValidationInputs, StructuralEvidenceOpen,
/// };
///
/// fn open_without_initializing<S>(
///     permit: DatabaseInitializationPermit<S>,
///     inputs: StartupValidationInputs,
/// ) where
///     S: DatabaseInitializationPort + StructuralEvidenceOpen,
/// {
///     let _ = permit.begin_structural_evidence(inputs);
/// }
/// ```
#[must_use = "dropping the permit leaves the database uninitialized"]
pub struct DatabaseInitializationPermit<Storage> {
    storage: Storage,
}

impl<Storage> DatabaseInitializationPermit<Storage>
where
    Storage: DatabaseInitializationPort,
{
    /// Re-proves emptiness and atomically installs or observes the durable identity.
    ///
    /// `ConcurrentWinner` is returned unchanged; the losing candidate is never
    /// compared with or substituted for the durable winner.
    pub fn initialize(
        mut self,
        candidate: DatabaseId,
    ) -> Result<DatabaseInitializationCompletion<Storage>, StorageError> {
        let result = self.storage.initialize_database(candidate)?;
        Ok(DatabaseInitializationCompletion {
            initialized: InitializedDatabase {
                database_id: result.database_id(),
                storage: self.storage,
            },
            result,
        })
    }
}

/// Successful initialization transition plus its ready-for-evidence continuation.
///
/// The exact storage-owned result preserves whether this caller installed the
/// candidate or observed a concurrent winner. The storage value remains private
/// and can advance only through [`Self::into_initialized_database`].
#[must_use = "successful initialization must continue into structural evidence"]
pub struct DatabaseInitializationCompletion<Storage> {
    initialized: InitializedDatabase<Storage>,
    result: DatabaseInitializationResult,
}

impl<Storage> DatabaseInitializationCompletion<Storage> {
    /// Returns the exact installed-versus-concurrent-winner result.
    #[must_use]
    pub const fn result(&self) -> DatabaseInitializationResult {
        self.result
    }

    /// Advances into the only type that can begin structural evidence.
    pub fn into_initialized_database(self) -> InitializedDatabase<Storage> {
        self.initialized
    }
}

/// Initialized dormant storage that may begin its exclusive structural pass.
///
/// This continuation is constructed only from an `Existing` probe or a
/// successful atomic initialization transition. It exposes the durable identity
/// but never the wrapped storage or an authoritative mutation port.
#[must_use = "initialized storage must continue into structural evidence"]
pub struct InitializedDatabase<Storage> {
    database_id: DatabaseId,
    storage: Storage,
}

impl<Storage> InitializedDatabase<Storage> {
    /// Returns the exact durable database identity established by the prior state.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }
}

impl<Storage> InitializedDatabase<Storage>
where
    Storage: StructuralEvidenceOpen,
{
    /// Consumes initialized storage and begins its exclusive structural pass.
    ///
    /// This delegates only to the storage-owned type-state transition; neither
    /// the dormant storage nor any mutation handle is returned to composition.
    pub fn begin_structural_evidence(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<Storage::Session, StorageError> {
        self.storage.begin_structural_evidence(inputs)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use riffdb_storage_api::{
        DormantPortBundle, EvidencePageLimit, HistoricalBundleEvidence, HistoricalEvidenceCursor,
        HistoricalEvidenceEnd, HistoricalEvidencePage, OpenSessionId,
        ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
        StorageErrorKind, StructuralEvidenceCursor, StructuralEvidenceEnd, StructuralEvidencePage,
        StructuralEvidenceSession,
    };
    use riffdb_types::{
        ContractBundleHash, ContractLineage, ContractVersion, DigestKeyId, Timestamp,
    };

    use super::*;

    #[derive(Default)]
    struct Calls {
        probes: usize,
        initializations: usize,
        candidates: Vec<DatabaseId>,
        structural_opens: usize,
    }

    struct FakeStorage {
        probe: Result<DatabaseIdentityProbe, StorageError>,
        initialization: Result<DatabaseInitializationResult, StorageError>,
        structural_database_id: DatabaseId,
        calls: Arc<Mutex<Calls>>,
    }

    impl DatabaseIdentityProbePort for FakeStorage {
        fn probe_database_identity(&self) -> Result<DatabaseIdentityProbe, StorageError> {
            self.calls.lock().expect("call lock").probes += 1;
            self.probe.clone()
        }
    }

    impl DatabaseInitializationPort for FakeStorage {
        fn initialize_database(
            &mut self,
            candidate: DatabaseId,
        ) -> Result<DatabaseInitializationResult, StorageError> {
            let mut calls = self.calls.lock().expect("call lock");
            calls.initializations += 1;
            calls.candidates.push(candidate);
            self.initialization.clone()
        }
    }

    struct FakeDormantPorts;

    impl DormantPortBundle for FakeDormantPorts {
        type CompletionAuthority = ();
    }

    struct FakeStructuralEnd(StructuralEvidenceCursor);

    impl StructuralEvidenceEnd for FakeStructuralEnd {
        fn cursor(&self) -> StructuralEvidenceCursor {
            self.0
        }
    }

    struct FakeHistoricalEnd(HistoricalEvidenceCursor);

    impl HistoricalEvidenceEnd for FakeHistoricalEnd {
        fn cursor(&self) -> HistoricalEvidenceCursor {
            self.0
        }
    }

    struct FakeStructuralSession {
        database_id: DatabaseId,
        open_session_id: OpenSessionId,
    }

    impl StructuralEvidenceSession for FakeStructuralSession {
        type DormantPorts = FakeDormantPorts;
        type StructuralEnd = FakeStructuralEnd;
        type HistoricalEnd = FakeHistoricalEnd;

        fn database_id(&self) -> DatabaseId {
            self.database_id
        }

        fn open_session_id(&self) -> OpenSessionId {
            self.open_session_id
        }

        fn read_structural_evidence(
            &mut self,
            _cursor: StructuralEvidenceCursor,
            _limit: EvidencePageLimit,
        ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError> {
            Err(unavailable())
        }

        fn read_historical_evidence(
            &mut self,
            _cursor: HistoricalEvidenceCursor,
            _limit: EvidencePageLimit,
        ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
            Err(unavailable())
        }

        fn read_historical_bundle(
            &mut self,
            _lineage: &ContractLineage,
            _contract_version: ContractVersion,
            _bundle_hash: ContractBundleHash,
        ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
            Err(unavailable())
        }

        fn finish(
            self,
            _structural_end: Self::StructuralEnd,
            _historical_end: Self::HistoricalEnd,
        ) -> Result<riffdb_storage_api::StructurallyOpened<Self::DormantPorts>, StorageError>
        {
            Err(unavailable())
        }
    }

    impl StructuralEvidenceOpen for FakeStorage {
        type Session = FakeStructuralSession;

        fn begin_structural_evidence(
            self,
            _inputs: StartupValidationInputs,
        ) -> Result<Self::Session, StorageError> {
            self.calls.lock().expect("call lock").structural_opens += 1;
            Ok(FakeStructuralSession {
                database_id: self.structural_database_id,
                open_session_id: OpenSessionId::new(1).expect("nonzero open session"),
            })
        }
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn database_id(fill: u8) -> DatabaseId {
        DatabaseId::from_bytes(uuid_bytes(fill)).expect("valid UUIDv7")
    }

    fn fake(
        probe: Result<DatabaseIdentityProbe, StorageError>,
        initialization: Result<DatabaseInitializationResult, StorageError>,
        structural_database_id: DatabaseId,
    ) -> (FakeStorage, Arc<Mutex<Calls>>) {
        let calls = Arc::new(Mutex::new(Calls::default()));
        (
            FakeStorage {
                probe,
                initialization,
                structural_database_id,
                calls: Arc::clone(&calls),
            },
            calls,
        )
    }

    fn unavailable() -> StorageError {
        StorageError::new(StorageErrorKind::Unavailable, None)
    }

    fn startup_inputs() -> StartupValidationInputs {
        let readable = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
        StartupValidationInputs::new(
            Timestamp::new(17, 23).expect("canonical timestamp"),
            ReadableCapabilityDigestInventory::new(vec![readable])
                .expect("capability digest inventory"),
            ReadableIdempotencyDigestInventory::new(vec![readable])
                .expect("idempotency digest inventory"),
        )
    }

    #[test]
    fn existing_identity_never_calls_or_accepts_initialization() {
        let existing = database_id(0x11);
        let (storage, calls) = fake(
            Ok(DatabaseIdentityProbe::Existing(existing)),
            Err(unavailable()),
            existing,
        );
        let executor = DatabaseInitializationExecutor::new(storage);

        match executor.probe().expect("probe succeeds") {
            DatabaseInitializationDecision::Existing(initialized) => {
                assert_eq!(initialized.database_id(), existing);
                let session = initialized
                    .begin_structural_evidence(startup_inputs())
                    .expect("begin structural evidence");
                assert_eq!(session.database_id(), existing);
            }
            DatabaseInitializationDecision::NeedsInitialization(_) => {
                panic!("existing storage must not request a candidate")
            }
        }

        let calls = calls.lock().expect("call lock");
        assert_eq!(calls.probes, 1);
        assert_eq!(calls.initializations, 0);
        assert!(calls.candidates.is_empty());
        assert_eq!(calls.structural_opens, 1);
    }

    #[test]
    fn needs_initialization_accepts_one_candidate_and_preserves_installed() {
        let candidate = database_id(0x21);
        let (storage, calls) = fake(
            Ok(DatabaseIdentityProbe::NeedsInitialization),
            Ok(DatabaseInitializationResult::Installed(candidate)),
            candidate,
        );
        let executor = DatabaseInitializationExecutor::new(storage);

        let decision = executor.probe().expect("probe succeeds");
        let completion = match decision {
            DatabaseInitializationDecision::Existing(_) => {
                panic!("empty storage must request initialization")
            }
            DatabaseInitializationDecision::NeedsInitialization(permit) => permit
                .initialize(candidate)
                .expect("initialization succeeds"),
        };

        assert_eq!(
            completion.result(),
            DatabaseInitializationResult::Installed(candidate)
        );
        let initialized = completion.into_initialized_database();
        assert_eq!(initialized.database_id(), candidate);
        let session = initialized
            .begin_structural_evidence(startup_inputs())
            .expect("begin structural evidence");
        assert_eq!(session.database_id(), candidate);
        let calls = calls.lock().expect("call lock");
        assert_eq!(calls.probes, 1);
        assert_eq!(calls.initializations, 1);
        assert_eq!(calls.candidates, vec![candidate]);
        assert_eq!(calls.structural_opens, 1);
    }

    #[test]
    fn concurrent_winner_is_returned_without_candidate_substitution() {
        let candidate = database_id(0x31);
        let winner = database_id(0x32);
        let (storage, calls) = fake(
            Ok(DatabaseIdentityProbe::NeedsInitialization),
            Ok(DatabaseInitializationResult::ConcurrentWinner(winner)),
            winner,
        );
        let executor = DatabaseInitializationExecutor::new(storage);

        let decision = executor.probe().expect("probe succeeds");
        let completion = match decision {
            DatabaseInitializationDecision::Existing(_) => {
                panic!("empty storage must request initialization")
            }
            DatabaseInitializationDecision::NeedsInitialization(permit) => permit
                .initialize(candidate)
                .expect("race resolves successfully"),
        };

        assert_eq!(
            completion.result(),
            DatabaseInitializationResult::ConcurrentWinner(winner)
        );
        let initialized = completion.into_initialized_database();
        assert_eq!(initialized.database_id(), winner);
        let session = initialized
            .begin_structural_evidence(startup_inputs())
            .expect("begin structural evidence");
        assert_eq!(session.database_id(), winner);
        let calls = calls.lock().expect("call lock");
        assert_eq!(calls.probes, 1);
        assert_eq!(calls.initializations, 1);
        assert_eq!(calls.candidates, vec![candidate]);
        assert_eq!(calls.structural_opens, 1);
    }

    #[test]
    fn probe_failure_never_calls_initialization() {
        let error = StorageError::new(StorageErrorKind::CorruptData, None);
        let (storage, calls) = fake(Err(error.clone()), Err(unavailable()), database_id(0x7f));
        let executor = DatabaseInitializationExecutor::new(storage);

        let observed = match executor.probe() {
            Ok(_) => panic!("corrupt probe must fail"),
            Err(observed) => observed,
        };
        assert_eq!(observed, error);
        let calls = calls.lock().expect("call lock");
        assert_eq!(calls.probes, 1);
        assert_eq!(calls.initializations, 0);
        assert!(calls.candidates.is_empty());
        assert_eq!(calls.structural_opens, 0);
    }

    #[test]
    fn initialization_failure_is_not_retried() {
        let candidate = database_id(0x41);
        let error = unavailable();
        let (storage, calls) = fake(
            Ok(DatabaseIdentityProbe::NeedsInitialization),
            Err(error.clone()),
            candidate,
        );
        let executor = DatabaseInitializationExecutor::new(storage);

        let decision = executor.probe().expect("probe succeeds");
        let observed = match decision {
            DatabaseInitializationDecision::Existing(_) => {
                panic!("empty storage must request initialization")
            }
            DatabaseInitializationDecision::NeedsInitialization(permit) => {
                match permit.initialize(candidate) {
                    Ok(_) => panic!("transition must fail"),
                    Err(error) => error,
                }
            }
        };

        assert_eq!(observed, error);
        let calls = calls.lock().expect("call lock");
        assert_eq!(calls.probes, 1);
        assert_eq!(calls.initializations, 1);
        assert_eq!(calls.candidates, vec![candidate]);
        assert_eq!(calls.structural_opens, 0);
    }
}
