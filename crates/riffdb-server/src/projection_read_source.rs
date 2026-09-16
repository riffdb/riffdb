//! Immutable snapshot access shared by primary and follower derived providers.
//! The captured reader stays behind this boundary; workers receive no writer port.
use std::sync::Arc;

use riffdb_query_executor::StorageQueryExecutor;
use riffdb_storage_api::{
    AuthoritativeEntityPartitionScanPage, AuthoritativeEntityPartitionScanRequest,
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest, EntityTarget,
    IdempotencyIdentity, OwnedSnapshotReader, StorageError, StoredCommitRecordV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1,
};
use riffdb_storage_redb::RedbOwnedSnapshot;
use riffdb_types::{CommitSequence, EventId, ProvenanceId};

#[derive(Clone)]
pub(crate) struct ProjectionReadSource {
    open: Arc<dyn Fn() -> Result<RedbOwnedSnapshot, StorageError> + Send + Sync>,
}
impl ProjectionReadSource {
    pub(crate) fn new<R>(reader: R) -> Self
    where
        R: Send + Sync + 'static,
        for<'a> R: OwnedSnapshotReader<Snapshot<'a> = RedbOwnedSnapshot>,
    {
        Self {
            open: Arc::new(move || reader.open_owned_snapshot()),
        }
    }

    /// Private worker reads and policy evidence share one immutable source root.
    pub(crate) fn from_snapshot(snapshot: RedbOwnedSnapshot) -> Self {
        Self {
            open: Arc::new(move || Ok(snapshot.clone())),
        }
    }

    /// A follower rechecks live publication on every pin, including withdrawal.
    pub(crate) fn pin(&self) -> Result<RedbOwnedSnapshot, StorageError> {
        (self.open)()
    }

    pub(crate) fn query_executor(&self) -> StorageQueryExecutor<Self> {
        StorageQueryExecutor::new(self.clone())
    }
}
impl OwnedSnapshotReader for ProjectionReadSource {
    type Snapshot<'a> = RedbOwnedSnapshot;

    fn open_owned_snapshot(&self) -> Result<Self::Snapshot<'_>, StorageError> {
        self.pin()
    }
}

impl AuthoritativePointReader for ProjectionReadSource {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        AuthoritativePointReader::read_entity(&self.pin()?, target)
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        AuthoritativePointReader::read_stored_outcome(&self.pin()?, identity)
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        AuthoritativePointReader::read_commit(&self.pin()?, sequence)
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        AuthoritativePointReader::read_provenance(&self.pin()?, provenance_id)
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        AuthoritativePointReader::read_durable_event(&self.pin()?, event_id)
    }
}

impl AuthoritativeScanReader for ProjectionReadSource {
    fn scan_entity_partition(
        &self,
        request: AuthoritativeEntityPartitionScanRequest,
    ) -> Result<AuthoritativeEntityPartitionScanPage, StorageError> {
        AuthoritativeScanReader::scan_entity_partition(&self.pin()?, request)
    }

    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        AuthoritativeScanReader::scan_index(&self.pin()?, request)
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        AuthoritativeScanReader::scan_commits(&self.pin()?, request)
    }
}
