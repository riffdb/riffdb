//! Same-session historical evidence for offline projection resolution. No
//! structural completion, source ports, or readiness can be obtained here.
use super::*;
use riffdb_storage_api::{
    EntityTarget, EvidencePageLimit, HistoricalBundleEvidence, HistoricalEvidenceCursor,
    HistoricalEvidencePage, OpenSessionId, StoredEntityRecordV1, StructuralEvidenceCursor,
    StructuralEvidencePage, StructuralEvidenceSession, StructuralOpenOutcome, UniqueIndexTarget,
    UniqueOccupancyKind,
};
use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion, DatabaseId};

/// Exclusive historical evidence owner for an unpublished bootstrap candidate.
/// The ordinary catalog driver can consume it, but `finish` can never release
/// startup ports. Complete structural/catalog validation still follows rebuild.
pub struct RedbBootstrapCatalogSession {
    session: crate::RedbStructuralEvidenceSession,
    owner: ConstructionOwner,
    transfer: RedbBootstrapMaterializationInput,
}

impl RedbBootstrapCandidate {
    /// Closes the construction engine and opens its unchanged historical
    /// validator under the retained construction lock. No serving handle exists.
    pub fn begin_catalog_preflight(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<RedbBootstrapCatalogSession, StorageError> {
        self.owner.verify()?;
        if self.failed {
            return Err(corrupt());
        }
        let Self {
            database,
            owner,
            transfer,
            ..
        } = self;
        drop(database);
        let session =
            crate::RedbFollowerStore::open(&owner.path)?.begin_offline_bootstrap_scrub(inputs)?;
        owner.verify()?;
        Ok(RedbBootstrapCatalogSession {
            session,
            owner,
            transfer,
        })
    }

    /// Process-local binding for the historical proof consumed by the worker.
    /// This identity alone is not evidence of catalog validity or readiness.
    #[must_use]
    pub const fn catalog_validation_session(&self) -> Option<OpenSessionId> {
        self.catalog_session
    }
}

impl RedbBootstrapCatalogSession {
    /// Checks job cancellation at every bounded evidence read.
    pub fn with_cancellation(mut self, flag: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.session.set_cancellation(flag);
        self
    }

    /// Consumes exact historical EOF and returns the still-private candidate.
    /// The caller must independently retain the matching catalog-owned proof.
    pub fn finish_preflight(
        self,
        end: crate::RedbHistoricalEvidenceEnd,
    ) -> Result<RedbBootstrapCandidate, StorageError> {
        let Self {
            session,
            owner,
            transfer,
        } = self;
        owner.verify()?;
        let id = session.open_session_id();
        session.finish_bootstrap_catalog_preflight(end)?;
        let database = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create_file(owner.file.try_clone().map_err(unavailable)?)
            .map_err(unavailable)?;
        owner.verify()?;
        Ok(RedbBootstrapCandidate {
            database,
            owner,
            transfer,
            failed: false,
            catalog_session: Some(id),
        })
    }
}

impl StructuralEvidenceSession for RedbBootstrapCatalogSession {
    type DormantPorts = crate::RedbDormantPorts;
    type StructuralEnd = crate::RedbStructuralEvidenceEnd;
    type HistoricalEnd = crate::RedbHistoricalEvidenceEnd;
    type MigrationPort = crate::RedbStartupIndexMigrationPort;
    fn database_id(&self) -> DatabaseId {
        self.session.database_id()
    }
    fn open_session_id(&self) -> OpenSessionId {
        self.session.open_session_id()
    }
    fn read_structural_evidence(
        &mut self,
        _cursor: StructuralEvidenceCursor,
        _limit: EvidencePageLimit,
    ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError> {
        Err(corrupt())
    }
    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        self.owner.verify()?;
        self.session.read_historical_evidence(cursor, limit)
    }
    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        self.owner.verify()?;
        self.session.read_historical_bundle(lineage, version, hash)
    }
    fn read_integrity_entity(
        &mut self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        self.owner.verify()?;
        self.session.read_integrity_entity(target)
    }
    fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        self.owner.verify()?;
        self.session.read_integrity_unique_occupancy(target)
    }
    fn finish(
        self,
        _structural: Self::StructuralEnd,
        _historical: Self::HistoricalEnd,
    ) -> Result<StructuralOpenOutcome<Self::DormantPorts, Self::MigrationPort>, StorageError> {
        Err(corrupt())
    }
}
