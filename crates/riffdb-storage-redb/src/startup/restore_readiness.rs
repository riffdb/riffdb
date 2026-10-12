//! Ordinary restore readiness under ADR-0291. This never produces a complete
//! semantic-scrub proof, registry-completion witness or clean certificate.
use super::*;

#[cfg(test)]
mod tests;

pub(crate) fn private_restore_supports_bounded_readiness(
    store: &RedbStore,
) -> Result<bool, StorageError> {
    if !store.shared.permits_owned_source_compatibility() {
        return Ok(false);
    }
    store.shared.verify_private_restore_integrity()?;
    let read = store
        .shared
        .database
        .begin_read()
        .map_err(transaction_error)?;
    let retained = read_retained_metadata(&read)?;
    let meta = read.open_table(META).map_err(table_error)?;
    let encoded = meta
        .get(META_RECORD_REGISTRY)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let registry = riffdb_storage_api::decode_record_registry(encoded.value())
        .map_err(crate::error::codec_error)?;
    Ok(registry.value().completed_for(
        retained.database_id(),
        retained.history_incarnation(),
        riffdb_storage_api::current_record_registry_digest(),
    ) && crate::changelog_v3_roots::read_checkpoint_roots(&read)?.is_some())
}

pub(crate) fn validate_private_restore_readiness(
    store: RedbStore,
    inputs: StartupValidationInputs,
    cancellation: Arc<AtomicBool>,
) -> Result<
    (
        Arc<SharedRedb>,
        ValidatedAuthorizationParts,
        crate::restore_history::CompleteRestoreHistory,
    ),
    StorageError,
> {
    if !private_restore_supports_bounded_readiness(&store)? {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    let mut history = store.verify_owned_restore_materialized_journals()?;
    let complete_history = history
        .as_mut()
        .and_then(crate::journal::VerifiedMaterializedHistory::take_restore_history)
        .ok_or_else(invariant)?;
    let mut session = store.begin_structural_evidence_for_history(
        inputs,
        EvidenceOpenPurpose::PrivateRestoreReadiness,
        history,
    )?;
    session.set_cancellation(cancellation);
    let (catalog, structural_end, historical_end) =
        RedbOfflineIntegrityScrub::walk_session(&mut session)?;
    let CatalogHistoryOutcome::Ready(catalog) = catalog else {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    };
    session.check_cancellation()?;
    if session.purpose != EvidenceOpenPurpose::PrivateRestoreReadiness
        || !session.bounded_dirty_startup
        || session.clean_close_fast
        || session.checkpoint_verified
        || session.v3_activation_required
        || !session.structural_finished
        || !session.historical_finished
        || !session.catalog_validation_complete
        || structural_end.cursor != session.next_structural
        || historical_end.cursor != session.next_historical
        || session.any_finding_seen
        || session.authoritative_finding_seen
        || session.saw_v1_index
        || !catalog.matches(session.database_id, session.open_session_id)
        || !session.shared.permits_owned_source_compatibility()
        || session.shared.durable_commit_epoch() != session.durable_commit_epoch
    {
        return Err(corrupt());
    }
    session.shared.verify_private_restore_integrity()?;
    let read = session.validation_read.take().ok_or_else(invariant)?;
    session.verify_structural_continuity(&read)?;
    complete_history.verify_read(
        &read,
        session
            .shared
            .restore_integrity
            .as_ref()
            .ok_or_else(invariant)?,
    )?;
    let lease = session.lease.take().ok_or_else(invariant)?;
    let owner = Arc::clone(&session.shared);
    let custody = Arc::new(ValidatedReadCustody {
        _lease: lease,
        shared: Arc::clone(&owner),
        durable_commit_epoch: session.durable_commit_epoch,
        cancellation: session.cancellation.clone(),
        invalid: AtomicBool::new(false),
    });
    let root = Arc::new(crate::checkpoint_root::CheckpointRoot::new(read, 0));
    let reader =
        crate::RedbOwnedSnapshot::from_read_access(crate::store::RedbReadAccess::Durable(root))
            .with_validated_custody(custody);
    let parts = (
        session.database_id,
        session.open_session_id,
        session.retained_metadata.clone(),
        catalog,
        reader,
    );
    drop(session);
    Ok((owner, parts, complete_history))
}

impl RedbStore {
    pub(crate) fn begin_published_restore_readiness(
        self,
        inputs: StartupValidationInputs,
        backup_incarnation: u64,
    ) -> Result<RedbStructuralEvidenceSession, StorageError> {
        if !self.shared.is_source_mode() || self.shared.restore_integrity.is_some() {
            return Err(invariant());
        }
        let bounded = {
            let read = self
                .shared
                .database
                .begin_read()
                .map_err(transaction_error)?;
            let retained = read_retained_metadata(&read)?;
            let meta = read.open_table(META).map_err(table_error)?;
            let registry = meta
                .get(META_RECORD_REGISTRY)
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            let registry = riffdb_storage_api::decode_record_registry(registry.value())
                .map_err(crate::error::codec_error)?;
            registry.value().completed_for(
                retained.database_id(),
                backup_incarnation,
                riffdb_storage_api::current_record_registry_digest(),
            ) && crate::changelog_v3_roots::read_checkpoint_roots(&read)?.is_some()
        };
        if !bounded {
            // Supported legacy materializations still require their original
            // complete validation and migration; never invent completion.
            return self.begin_structural_evidence_for(inputs, EvidenceOpenPurpose::Startup);
        }
        self.begin_structural_evidence_for(
            inputs,
            EvidenceOpenPurpose::PublishedRestoreReadiness { backup_incarnation },
        )
    }
}

/// Revalidate the roots affected by the known stamp before final sealing.
/// No catalog population, operational reader or complete proof is constructed.
pub(crate) fn validate_restore_transition_roots(
    database: &redb::Database,
    incarnation: u64,
) -> Result<(), StorageError> {
    let read = database.begin_read().map_err(transaction_error)?;
    let retained = read_retained_metadata(&read)?;
    if retained.history_incarnation() != incarnation {
        return Err(corrupt());
    }
    validate_table_inventory(&read, None)?;
    crate::clean_close::bounded_state_binding_hash(&read, [0xdd; 32])?;
    crate::changelog_v3_roots::read_checkpoint_roots(&read)?.ok_or_else(corrupt)?;
    if inspect_header(&read, retained.database_id(), HeaderProof::BoundedDirty)?.is_some() {
        return Err(corrupt());
    }
    let watermark = crate::retention::load_watermark(&read)?;
    if let Some(watermark) = watermark {
        if watermark.history_incarnation() != incarnation
            || watermark.computed_hash().map_err(|_| corrupt())? != watermark.watermark_hash()
            || watermark.watermark_sequence() > crate::retention::durable_application_head(&read)?
        {
            return Err(corrupt());
        }
        crate::retention::verify_tombstone_chain(
            &read,
            watermark.watermark_sequence(),
            watermark.chain_root_registry_digest(),
        )?;
    } else {
        crate::retention::verify_tombstone_chain(&read, 0, None)?;
    }
    Ok(())
}
