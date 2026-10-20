//! Read-only entry to the sole historical catalog validator.
//!
//! This port and its result cannot release startup, migration or operational
//! authority. Native offline composition must separately prove its actual
//! immutable owner, complete structural walk and exact artifact custody.
use riffdb_storage_api::{
    EvidencePageLimit, HistoricalBundleEvidence, HistoricalEvidenceCursor, HistoricalEvidenceEnd,
    HistoricalEvidencePage, OpenSessionId, StorageError, StoredEntityRecordV1,
    StructuralEvidenceSession, UniqueIndexTarget, UniqueOccupancyKind,
};
use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion, DatabaseId};

use super::{CatalogHistoryOutcome, CatalogHistoryValidation, ValidatedCatalogHistory};
use crate::{CatalogError, ValidatedContractBundle};

/// Same-immutable-view historical reads without startup or migration ports.
///
/// Implementations bind every page and point read to the returned database and
/// process-local session identity. This trait grants neither source closure nor
/// native offline custody; those remain fields-private composition proofs.
pub trait ReadOnlyCatalogHistoryEvidenceV1 {
    /// Backend-owned exact-end token for this immutable historical view.
    type HistoricalEnd: HistoricalEvidenceEnd;

    /// Database identity bound to this view.
    fn database_id(&self) -> DatabaseId;
    /// Process-local identity bound to this exact view.
    fn open_session_id(&self) -> OpenSessionId;
    /// One bounded ordered historical page from the exact continuation.
    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError>;
    /// One exact immutable historical bundle, within the existing byte ceiling.
    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError>;
    /// One cursor-neutral entity read for reciprocal declared-unique validation.
    fn read_integrity_entity(
        &mut self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError>;
    /// One exact unique prefix; multiple physical rows must return corruption.
    fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError>;
}

/// Catalog validation of a read-only view, paired with its own exact-end token.
/// This result cannot become the ordinary startup catalog handoff.
pub struct ReadOnlyCatalogHistoryValidationV1<E> {
    outcome: ReadOnlyCatalogHistoryOutcomeV1,
    historical_end: E,
}

/// Closed read-only result; neither branch carries a migration capability.
pub enum ReadOnlyCatalogHistoryOutcomeV1 {
    /// The same catalog semantic checks succeeded through actual exact end.
    Ready(ValidatedReadOnlyCatalogHistoryV1),
    /// A checked predecessor needs migration; only fixed observation is exposed.
    MigrationRequired(ReadOnlyCatalogMigrationRequiredV1),
}

/// Opaque read-only catalog proof with no conversion to startup authority.
///
/// This owns the validator's complete catalog graph. Offline composition must
/// destroy it before entering the separately admitted source working phase;
/// database/session equality alone never supplies a native source proof.
pub struct ValidatedReadOnlyCatalogHistoryV1 {
    inner: ValidatedCatalogHistory,
}

impl ValidatedReadOnlyCatalogHistoryV1 {
    /// Checks this proof against the actual offline view's identities.
    #[must_use]
    pub fn matches(&self, database_id: DatabaseId, open_session_id: OpenSessionId) -> bool {
        self.inner.matches(database_id, open_session_id)
    }

    /// Borrows the checked immutable active bundle; this is not readiness.
    #[must_use]
    pub fn active(&self) -> Option<&ValidatedContractBundle> {
        self.inner.active()
    }

    /// Number of evidence items consumed through exact end.
    #[must_use]
    pub fn evidence_count(&self) -> u64 {
        self.inner.evidence_count()
    }
}

/// Fixed observation that grants no migration request or mutation authority.
pub struct ReadOnlyCatalogMigrationRequiredV1 {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    evidence_count: u64,
}

impl ReadOnlyCatalogMigrationRequiredV1 {
    /// Checks the observation against its actual immutable offline view.
    #[must_use]
    pub fn matches(&self, database_id: DatabaseId, open_session_id: OpenSessionId) -> bool {
        self.database_id == database_id && self.open_session_id == open_session_id
    }

    /// Number of historical evidence items consumed.
    #[must_use]
    pub const fn evidence_count(&self) -> u64 {
        self.evidence_count
    }
}

impl<E> ReadOnlyCatalogHistoryValidationV1<E> {
    /// Borrows the closed read-only result.
    #[must_use]
    pub const fn outcome(&self) -> &ReadOnlyCatalogHistoryOutcomeV1 {
        &self.outcome
    }

    /// Consumes this result with its original backend-owned historical end.
    #[must_use]
    pub fn into_parts(self) -> (ReadOnlyCatalogHistoryOutcomeV1, E) {
        (self.outcome, self.historical_end)
    }
}

/// Runs the unchanged sole validator without exposing a startup catalog proof.
///
/// Complete native offline validation additionally needs its actual immutable
/// session brand and full structural exact end. A caller-authored implementation
/// of this read-only trait cannot manufacture either native proof.
pub fn validate_catalog_history_read_only_v1<S: ReadOnlyCatalogHistoryEvidenceV1>(
    session: &mut S,
) -> Result<ReadOnlyCatalogHistoryValidationV1<S::HistoricalEnd>, CatalogError> {
    let CatalogHistoryValidation {
        outcome,
        historical_end,
    } = super::validate_catalog_history_reader_v1(session)?;
    let outcome = match outcome {
        CatalogHistoryOutcome::Ready(inner) => {
            ReadOnlyCatalogHistoryOutcomeV1::Ready(ValidatedReadOnlyCatalogHistoryV1 { inner })
        }
        CatalogHistoryOutcome::MigrationRequired(context) => {
            ReadOnlyCatalogHistoryOutcomeV1::MigrationRequired(ReadOnlyCatalogMigrationRequiredV1 {
                database_id: context.database_id,
                open_session_id: context.open_session_id,
                evidence_count: context.evidence_count,
            })
        }
    };
    Ok(ReadOnlyCatalogHistoryValidationV1 {
        outcome,
        historical_end,
    })
}

pub(super) struct StructuralHistoryReaderV1<'a, S> {
    session: &'a mut S,
}

impl<'a, S> StructuralHistoryReaderV1<'a, S> {
    pub(super) fn new(session: &'a mut S) -> Self {
        Self { session }
    }
}

impl<S: StructuralEvidenceSession> ReadOnlyCatalogHistoryEvidenceV1
    for StructuralHistoryReaderV1<'_, S>
{
    type HistoricalEnd = S::HistoricalEnd;
    fn database_id(&self) -> DatabaseId {
        self.session.database_id()
    }
    fn open_session_id(&self) -> OpenSessionId {
        self.session.open_session_id()
    }
    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        self.session.read_historical_evidence(cursor, limit)
    }
    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        self.session.read_historical_bundle(lineage, version, hash)
    }
    fn read_integrity_entity(
        &mut self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        self.session.read_integrity_entity(target)
    }
    fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        self.session.read_integrity_unique_occupancy(target)
    }
}
