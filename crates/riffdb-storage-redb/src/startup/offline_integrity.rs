//! Selector-free physical and exact-end logical offline verification.
use super::*;

/// Move-only complete logical evidence. Private materialization and physical
/// verification remain separate requirements; this grants no serving, writer
/// or publication capability and cannot be reconstructed from a receipt.
pub(crate) struct CompleteLogicalValidation {
    snapshot: CompletedLogicalSnapshot,
    catalog: riffdb_catalog::ValidatedCatalogHistory,
}

impl CompleteLogicalValidation {
    fn verify(&self) -> Result<(), StorageError> {
        let snapshot = &self.snapshot;
        if snapshot
            .cancellation
            .as_ref()
            .is_some_and(|value| value.load(Ordering::Acquire))
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        if snapshot.shared.durable_commit_epoch() != snapshot.durable_commit_epoch
            || !self
                .catalog
                .matches(snapshot.database_id, snapshot.open_session_id)
            || snapshot.retained_metadata.database_id() != snapshot.database_id
            || read_retained_metadata(&snapshot.read)? != snapshot.retained_metadata
        {
            return Err(corrupt());
        }
        Ok(())
    }

    /// Logical handoff only. The eventual native materialization owner must
    /// separately prove physical integrity and artifact custody before exposing
    /// this reader to the server's unchanged startup proof join.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "ADR-0289 owned private factory integration is still pending"
        )
    )]
    pub(crate) fn into_authorization_parts(
        self,
    ) -> Result<
        (
            DatabaseId,
            OpenSessionId,
            RetainedMetadataV1,
            riffdb_catalog::ValidatedCatalogHistory,
            crate::RedbOwnedSnapshot,
        ),
        StorageError,
    > {
        self.verify()?;
        let Self { snapshot, catalog } = self;
        let CompletedLogicalSnapshot {
            read,
            _lease,
            shared,
            durable_commit_epoch,
            database_id,
            open_session_id,
            retained_metadata,
            cancellation,
        } = snapshot;
        let root = Arc::new(crate::checkpoint_root::CheckpointRoot::new(read, 0));
        let custody = Arc::new(ValidatedReadCustody {
            _lease,
            shared,
            durable_commit_epoch,
            cancellation,
            invalid: AtomicBool::new(false),
        });
        let reader =
            crate::RedbOwnedSnapshot::from_read_access(crate::store::RedbReadAccess::Durable(root))
                .with_validated_custody(custody);
        Ok((
            database_id,
            open_session_id,
            retained_metadata,
            catalog,
            reader,
        ))
    }

    fn into_receipt(self) -> Result<OfflineIntegrityScrubReceiptV1, StorageError> {
        self.verify()?;
        // A public offline receipt carries none of the native evidence. Its
        // original source/copy owner performs its final checks after this drop.
        drop(self);
        Ok(OfflineIntegrityScrubReceiptV1 { _private: () })
    }
}

/// Exclusive offline complete-integrity scrub over one database file.
///
/// Binding does no I/O. `run` requires the engine's exclusive file ownership,
/// ignores clean-certificate and validated-prefix shortcuts, consumes both
/// evidence streams through exact end, and publishes a receipt only on total
/// physical-engine, structural and catalog-semantic success on an owned copy.
pub struct RedbOfflineIntegrityScrub {
    cancellation: Option<Arc<AtomicBool>>,
    path: PathBuf,
    inputs: StartupValidationInputs,
}

impl RedbOfflineIntegrityScrub {
    /// Binds one closed database file and the startup validation inputs the
    /// production startup uses (ADR-0203 Decision 8's complete validator).
    #[must_use]
    pub fn from_inputs(path: &Path, inputs: StartupValidationInputs) -> Self {
        Self {
            cancellation: None,
            path: path.to_path_buf(),
            inputs,
        }
    }

    /// Binds an operator-selected closed database and the configured readable
    /// digest-key inventory used by ordinary production startup.
    #[allow(dead_code)] // WP-760 owns the explicit operator-facing caller.
    pub(crate) fn bind(
        path: &Path,
        authorization_time: Timestamp,
        capability_key_ids: Vec<DigestKeyId>,
        idempotency_key_ids: Vec<DigestKeyId>,
    ) -> Result<Self, StorageError> {
        let capability = ReadableCapabilityDigestInventory::new(
            capability_key_ids
                .into_iter()
                .map(ReadableDigestKey::v1)
                .collect(),
        )
        .map_err(value_error_as_storage)?;
        let idempotency = ReadableIdempotencyDigestInventory::new(
            idempotency_key_ids
                .into_iter()
                .map(ReadableDigestKey::v1)
                .collect(),
        )
        .map_err(value_error_as_storage)?;
        Ok(Self::from_inputs(
            path,
            StartupValidationInputs::new(authorization_time, capability, idempotency),
        ))
    }

    /// Verifies a private physical copy, then the complete logical evidence.
    pub fn run(self) -> Result<OfflineIntegrityScrubReceiptV1, StorageError> {
        self.run_owned_copy(false)
    }

    pub(crate) fn with_cancellation(mut self, cancellation: Arc<AtomicBool>) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    pub(crate) fn run_follower(self) -> Result<OfflineIntegrityScrubReceiptV1, StorageError> {
        self.run_owned_copy(true)
    }

    fn run_owned_copy(
        self,
        follower: bool,
    ) -> Result<OfflineIntegrityScrubReceiptV1, StorageError> {
        let cancellation = self
            .cancellation
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        let copy =
            crate::maintenance::PhysicalScrubCopy::capture(&self.path, Arc::clone(&cancellation))?;
        let (store, history) =
            RedbStore::open_offline_scrub(&self.path, &copy, follower)?.into_parts();
        let mut session = store.begin_structural_evidence_for_history(
            self.inputs,
            EvidenceOpenPurpose::OfflineIntegrityScrub,
            history,
        )?;
        session.set_cancellation(cancellation);
        let receipt = Self::validate_session(session)?;
        copy.finish()?;
        Ok(receipt)
    }

    pub(crate) fn validate_owned_restore(
        store: RedbStore,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<CompleteLogicalValidation, StorageError> {
        let history = store.verify_materialized_journals()?;
        let mut session = store.begin_structural_evidence_for_history(
            inputs,
            EvidenceOpenPurpose::OfflineIntegrityScrub,
            history,
        )?;
        session.set_cancellation(cancellation);
        Self::validate_complete_session(session)
    }

    pub(super) fn validate_session(
        session: RedbStructuralEvidenceSession,
    ) -> Result<OfflineIntegrityScrubReceiptV1, StorageError> {
        Self::validate_complete_session(session)?.into_receipt()
    }

    pub(super) fn validate_complete_session(
        mut session: RedbStructuralEvidenceSession,
    ) -> Result<CompleteLogicalValidation, StorageError> {
        let database_id = session.database_id();
        let open_session_id = session.open_session_id();
        let limit = EvidencePageLimit::new(64).ok_or_else(limit_exceeded)?;
        let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
        let structural_end = loop {
            match session.read_structural_evidence(cursor, limit)? {
                StructuralEvidencePage::Page { findings, next, .. } => {
                    if !findings.is_empty() {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    cursor = next;
                }
                StructuralEvidencePage::ExactEnd(end) => break end,
            }
        };
        let validation = validate_catalog_history(&mut session).map_err(|error| {
            storage_error(
                error
                    .storage_kind()
                    .unwrap_or(StorageErrorKind::CorruptData),
            )
        })?;
        let (catalog, historical_end) = validation.into_parts();
        let CatalogHistoryOutcome::Ready(catalog) = catalog else {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        };
        if !catalog.matches(database_id, open_session_id) {
            return Err(corrupt());
        }
        let snapshot = session.finish_offline_integrity_scrub(structural_end, historical_end)?;
        Ok(CompleteLogicalValidation { snapshot, catalog })
    }
}

#[cfg(test)]
#[path = "offline_logical_proof_tests.rs"]
mod logical_proof_tests;
