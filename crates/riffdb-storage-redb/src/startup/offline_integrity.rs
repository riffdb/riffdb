//! Selector-free physical and exact-end logical offline verification.
use super::*;

/// Move-only complete logical evidence. Private materialization and physical
/// verification remain separate requirements; this grants no serving, writer
/// or publication capability and cannot be reconstructed from a receipt.
pub(crate) struct CompleteLogicalValidation {
    snapshot: CompletedLogicalSnapshot,
    catalog: riffdb_catalog::ValidatedCatalogHistory,
}

pub(crate) type CompleteAuthorizationParts = (
    DatabaseId,
    OpenSessionId,
    RetainedMetadataV1,
    riffdb_catalog::ValidatedCatalogHistory,
    crate::RedbOwnedSnapshot,
);

impl CompleteLogicalValidation {
    pub(crate) fn verify_native_archive_follower(
        &self,
        expected: riffdb_storage_api::ChangelogHistoryStateV3,
    ) -> Result<(), StorageError> {
        self.verify()?;
        if self.snapshot.shared.restore_integrity.is_none()
            || crate::changelog_v3_roots::validate_retained_history(&self.snapshot.read)?
                != Some(expected)
            || !crate::follower_lifecycle::is_attached(&self.snapshot.read)?
        {
            return Err(corrupt());
        }
        Ok(())
    }

    pub(crate) fn verify_native_archive_source(
        &self,
        database: DatabaseId,
        incarnation: u64,
        frontier: riffdb_types::DualFrontier,
    ) -> Result<(), StorageError> {
        self.verify()?;
        let history = crate::changelog_v3_roots::validate_retained_history(&self.snapshot.read)?
            .ok_or_else(corrupt)?;
        if self.snapshot.shared.restore_integrity.is_none()
            || history.lineage().database_id() != database
            || history.lineage().history_incarnation() != incarnation
            || history.tail().frontier() != frontier
            || crate::follower_lifecycle::is_attached(&self.snapshot.read)?
        {
            return Err(corrupt());
        }
        Ok(())
    }

    /// Closed native proof consumption grants no authorization reader or source
    /// readiness. Pending original registry completion remains the ordinary
    /// post-publication startup's responsibility, as on the legacy path.
    pub(crate) fn retain_native_close_owner(&self) -> Result<Arc<SharedRedb>, StorageError> {
        self.verify()?;
        if self.snapshot.shared.restore_integrity.is_none()
            || !self.snapshot.shared.is_native_restore_image_mode()
        {
            return Err(invariant());
        }
        Ok(Arc::clone(&self.snapshot.shared))
    }

    pub(crate) fn native_authorization_cancellation(
        &self,
    ) -> Result<Arc<AtomicBool>, StorageError> {
        self.verify()?;
        self.snapshot.cancellation.clone().ok_or_else(invariant)
    }

    pub(crate) fn native_authorization_inputs(
        &self,
    ) -> Result<StartupValidationInputs, StorageError> {
        self.verify()?;
        if self.snapshot.shared.restore_integrity.is_none() {
            return Err(invariant());
        }
        Ok(self.snapshot.inputs.clone())
    }

    pub(crate) fn retain_native_authorization_owner(
        &self,
    ) -> Result<Arc<SharedRedb>, StorageError> {
        self.verify()?;
        if self.snapshot.shared.restore_integrity.is_none()
            || self.native_compatibility_required()?
        {
            return Err(invariant());
        }
        Ok(Arc::clone(&self.snapshot.shared))
    }
    #[cfg(test)]
    pub(crate) fn native_owner_for_test(&self) -> Arc<SharedRedb> {
        Arc::clone(&self.snapshot.shared)
    }

    fn verify(&self) -> Result<(), StorageError> {
        let snapshot = &self.snapshot;
        snapshot.shared.verify_private_restore_integrity()?;
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
        if self.snapshot.shared.restore_integrity.is_some()
            && self.native_compatibility_required()?
        {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
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
            inputs: _,
            registry_completion: _,
            v3_activation_required: _,
            terminal_execution_failure_rows: _,
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

    fn native_compatibility_required(&self) -> Result<bool, StorageError> {
        self.verify()?;
        if self.snapshot.shared.restore_integrity.is_none() {
            return Err(invariant());
        }
        Ok(self.snapshot.v3_activation_required
            || match self.snapshot.registry_completion.as_ref() {
                Some(proof) => !proof.is_already_completed()?,
                None => false,
            })
    }

    fn complete_required_native_compatibility(
        self,
    ) -> Result<(RedbStore, StartupValidationInputs), StorageError> {
        if !self.native_compatibility_required()? {
            return Err(invariant());
        }
        let CompletedLogicalSnapshot {
            read,
            _lease,
            mut shared,
            durable_commit_epoch,
            retained_metadata,
            cancellation: _,
            inputs,
            registry_completion,
            v3_activation_required,
            terminal_execution_failure_rows,
            database_id: _,
            open_session_id: _,
        } = self.snapshot;
        // Consume the old immutable proof before admitting the known write.
        // The unique SharedRedb owner proves no reader or bearer clone survives.
        drop(read);
        Arc::get_mut(&mut shared)
            .ok_or_else(invariant)?
            .restore_integrity
            .as_mut()
            .ok_or_else(invariant)?
            .begin_controlled_preparation()?;
        if v3_activation_required {
            PendingV3Activation::install_compatible_history(
                &shared,
                durable_commit_epoch,
                &retained_metadata,
                registry_completion.as_ref(),
            )?;
        } else {
            let proof = registry_completion.ok_or_else(invariant)?;
            shared.seed_terminal_execution_failure_rows(terminal_execution_failure_rows);
            shared.set_startup_validation_clean(true);
            // Keep the existing hardened atomic checkpoint/registry/receipt
            // transaction together; no source readiness or workers are created.
            crate::validated_prefix::write_completed_registry_checkpoint(
                &shared,
                &retained_metadata,
                proof,
            )?;
            shared.set_startup_validation_clean(false);
        }
        // Preparation owns its own gate; release the consumed proof's gate
        // first while retaining continuous outer native exclusion.
        drop(_lease);
        let mut store = RedbStore { shared };
        store.prepare_owned_restore_before_validation()?;
        Ok((store, inputs))
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
    pub(crate) fn validate_owned_archive_follower(
        store: RedbStore,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<CompleteLogicalValidation, StorageError> {
        if !store.shared.is_offline_follower_mode() || store.shared.restore_integrity.is_none() {
            return Err(invariant());
        }
        store.shared.verify_private_restore_integrity()?;
        // The original follower admission has verified journal absence. The
        // complete follower walk uses no source-journal pin, exactly as the
        // ordinary full offline follower scrub.
        let mut session = store.begin_structural_evidence_for_history(
            inputs,
            EvidenceOpenPurpose::OfflineIntegrityScrub,
            None,
        )?;
        session.set_cancellation(cancellation);
        Self::validate_complete_session(session)
    }

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

    pub(crate) fn validate_owned_archive_private(
        store: RedbStore,
        binding: crate::maintenance::PrivateArchiveValidationBinding,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<CompleteLogicalValidation, StorageError> {
        if store.shared.private_restore_binding() != Some(binding)
            || store.shared.restore_integrity.is_none()
        {
            return Err(invariant());
        }
        {
            let read = store
                .shared
                .database
                .begin_read()
                .map_err(transaction_error)?;
            binding.validate(&read)?;
            super::archive_private_graph::validate(&read, &cancellation)?;
        }
        let mut session = store
            .begin_structural_evidence_for(inputs, EvidenceOpenPurpose::OfflineIntegrityScrub)?;
        session.set_cancellation(cancellation);
        Self::validate_complete_session(session)
    }

    pub(crate) fn validate_owned_archive_image(
        store: RedbStore,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<CompleteLogicalValidation, StorageError> {
        if !store.shared.is_native_restore_image_mode() || store.shared.restore_integrity.is_none()
        {
            return Err(invariant());
        }
        let history = store.verify_materialized_journals()?;
        let mut session = store.begin_structural_evidence_for_history(
            inputs,
            EvidenceOpenPurpose::OfflineIntegrityScrub,
            history,
        )?;
        session.set_cancellation(cancellation);
        Self::validate_complete_session(session)
    }

    pub(crate) fn validate_owned_archive_source(
        store: RedbStore,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<CompleteLogicalValidation, StorageError> {
        if !store.shared.is_offline_source_mode() || store.shared.restore_integrity.is_none() {
            return Err(invariant());
        }
        let history = store.verify_materialized_journals()?;
        let mut session = store.begin_structural_evidence_for_history(
            inputs,
            EvidenceOpenPurpose::OfflineIntegrityScrub,
            history,
        )?;
        session.set_cancellation(cancellation);
        Self::validate_complete_session(session)
    }

    pub(crate) fn validate_owned_restore(
        store: RedbStore,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<CompleteLogicalValidation, StorageError> {
        let mut store = store;
        let mut inputs = inputs;
        // At most one catalog-owned index migration and one existing registry
        // transition. Each consumes its old proof and is followed by a fresh
        // physical, journal, structural and semantic proof on the same engine.
        let mut migrated_indexes = false;
        let mut completed_registry = false;
        for _ in 0..3 {
            let history = store.verify_materialized_journals()?;
            let mut session = store.begin_structural_evidence_for_history(
                inputs,
                EvidenceOpenPurpose::OfflineIntegrityScrub,
                history,
            )?;
            session.set_cancellation(Arc::clone(&cancellation));
            let (outcome, structural_end, historical_end) =
                Self::walk_complete_session(&mut session)?;
            match outcome {
                CatalogHistoryOutcome::MigrationRequired(context) => {
                    if migrated_indexes || completed_registry {
                        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                    }
                    (store, inputs) = session.complete_owned_index_migration(
                        context,
                        structural_end,
                        historical_end,
                    )?;
                    migrated_indexes = true;
                }
                CatalogHistoryOutcome::Ready(catalog) => {
                    let proof = Self::finish_complete_session(
                        session,
                        catalog,
                        structural_end,
                        historical_end,
                    )?;
                    if !proof.native_compatibility_required()? {
                        return Ok(proof);
                    }
                    if completed_registry {
                        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
                    }
                    (store, inputs) = proof.complete_required_native_compatibility()?;
                    completed_registry = true;
                }
            }
        }
        Err(invariant())
    }

    pub(super) fn validate_session(
        session: RedbStructuralEvidenceSession,
    ) -> Result<OfflineIntegrityScrubReceiptV1, StorageError> {
        Self::validate_complete_session(session)?.into_receipt()
    }

    pub(super) fn validate_complete_session(
        mut session: RedbStructuralEvidenceSession,
    ) -> Result<CompleteLogicalValidation, StorageError> {
        let (catalog, structural_end, historical_end) = Self::walk_complete_session(&mut session)?;
        let CatalogHistoryOutcome::Ready(catalog) = catalog else {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        };
        Self::finish_complete_session(session, catalog, structural_end, historical_end)
    }

    fn walk_complete_session(
        session: &mut RedbStructuralEvidenceSession,
    ) -> Result<
        (
            CatalogHistoryOutcome,
            RedbStructuralEvidenceEnd,
            RedbHistoricalEvidenceEnd,
        ),
        StorageError,
    > {
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
        let validation = session
            .validate_startup_catalog_history()
            .map_err(|error| {
                storage_error(
                    error
                        .storage_kind()
                        .unwrap_or(StorageErrorKind::CorruptData),
                )
            })?;
        let (catalog, historical_end) = validation.into_parts();
        Ok((catalog, structural_end, historical_end))
    }

    fn finish_complete_session(
        session: RedbStructuralEvidenceSession,
        catalog: riffdb_catalog::ValidatedCatalogHistory,
        structural_end: RedbStructuralEvidenceEnd,
        historical_end: RedbHistoricalEvidenceEnd,
    ) -> Result<CompleteLogicalValidation, StorageError> {
        if !catalog.matches(session.database_id(), session.open_session_id()) {
            return Err(corrupt());
        }
        let snapshot = session.finish_offline_integrity_scrub(structural_end, historical_end)?;
        let proof = CompleteLogicalValidation { snapshot, catalog };
        proof.verify()?;
        Ok(proof)
    }
}

#[cfg(test)]
#[path = "offline_logical_proof_tests.rs"]
mod logical_proof_tests;
