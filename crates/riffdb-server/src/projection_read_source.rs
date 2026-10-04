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

type CatalogProbe = dyn Fn(
        &RedbOwnedSnapshot,
        &crate::exact_provider_graph::ProviderOwnershipGraph,
    )
        -> Result<riffdb_catalog::ActiveCatalogSnapshot, crate::exact_text_adapter::RebuildFailure>
    + Send
    + Sync;

#[derive(Clone)]
pub(crate) struct ProjectionReadSource {
    open: Arc<dyn Fn() -> Result<RedbOwnedSnapshot, StorageError> + Send + Sync>,
    catalog: Option<Arc<CatalogProbe>>,
}
impl ProjectionReadSource {
    pub(crate) fn new<R>(reader: R) -> Self
    where
        R: Send + Sync + 'static,
        for<'a> R: OwnedSnapshotReader<Snapshot<'a> = RedbOwnedSnapshot>,
    {
        Self {
            open: Arc::new(move || reader.open_owned_snapshot()),
            catalog: None,
        }
    }

    /// Private worker reads and policy evidence share one immutable source root.
    pub(crate) fn from_snapshot(snapshot: RedbOwnedSnapshot) -> Self {
        Self {
            open: Arc::new(move || Ok(snapshot.clone())),
            catalog: None,
        }
    }

    pub(crate) fn with_catalog_probe(
        mut self,
        probe: impl Fn(
            &RedbOwnedSnapshot,
            &crate::exact_provider_graph::ProviderOwnershipGraph,
        ) -> Result<
            riffdb_catalog::ActiveCatalogSnapshot,
            crate::exact_text_adapter::RebuildFailure,
        > + Send
        + Sync
        + 'static,
    ) -> Self {
        self.catalog = Some(Arc::new(probe));
        self
    }
    pub(crate) fn catalog(
        &self,
        snapshot: &RedbOwnedSnapshot,
        graph: &crate::exact_provider_graph::ProviderOwnershipGraph,
    ) -> Result<riffdb_catalog::ActiveCatalogSnapshot, crate::exact_text_adapter::RebuildFailure>
    {
        let probe = self
            .catalog
            .as_ref()
            .ok_or(crate::exact_text_adapter::RebuildFailure::Transient)?;
        probe(snapshot, graph)
    }
    /// A follower rechecks live publication on every pin, including withdrawal.
    pub(crate) fn pin(&self) -> Result<RedbOwnedSnapshot, StorageError> {
        (self.open)()
    }

    /// Provider workers admit source copies and decoding on the same immutable
    /// snapshot. Ordinary readers retain their existing contracts.
    pub(crate) fn read_provider_entity_v1(
        &self,
        target: &EntityTarget,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<Option<riffdb_storage_api::AdmittedProviderEntityRecordV1>, StorageError> {
        self.pin()?.read_provider_entity_record_v1(target, budget)
    }

    /// The provider worker's index page shares its captured root and admission.
    pub(crate) fn read_provider_index_page_v1(
        &self,
        request: &AuthoritativeIndexScanRequest,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<riffdb_storage_api::AdmittedProviderIndexPageV1, StorageError> {
        self.pin()?.read_provider_index_page_v1(request, budget)
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
