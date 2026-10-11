//! Selector-free physical and exact-end logical offline verification.
use super::*;

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

    pub(super) fn validate_session(
        mut session: RedbStructuralEvidenceSession,
    ) -> Result<OfflineIntegrityScrubReceiptV1, StorageError> {
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
        if !matches!(catalog, CatalogHistoryOutcome::Ready(_)) {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        session.finish_offline_integrity_scrub(structural_end, historical_end)?;
        Ok(OfflineIntegrityScrubReceiptV1 { _private: () })
    }
}
