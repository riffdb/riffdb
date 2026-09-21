//! A restore supersedes promotion authority without deleting its external audit.
use super::*;
use riffdb_storage_api::{
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogTransactionSequence,
    ReplicationPromotionReceiptV1,
};
use riffdb_types::DatabaseId;

#[derive(Clone)]
pub(super) enum RestoreReceipt {
    Ordinary(OfflineMaintenanceReceiptV1),
    Archived(OfflineMaintenanceReceiptV3),
}

impl RestoreReceipt {
    fn source(&self) -> Option<DatabaseId> {
        match self {
            Self::Ordinary(r) => r.source_database_id(),
            Self::Archived(r) => r.source_database_id(),
        }
    }
    fn target(&self) -> Option<DatabaseId> {
        match self {
            Self::Ordinary(r) => r.staged_database_id(),
            Self::Archived(r) => r.staged_database_id(),
        }
    }
    fn incarnation(&self) -> Option<u64> {
        match self {
            Self::Ordinary(r) => r.published_history_incarnation(),
            Self::Archived(r) => r.published_history_incarnation(),
        }
    }
    fn phase(&self) -> OfflineMaintenanceReceiptPhaseV1 {
        match self {
            Self::Ordinary(r) => r.current_phase(),
            Self::Archived(r) => r.current_phase(),
        }
    }
    fn matches_anchor(&self, history: ChangelogHistoryStateV3) -> bool {
        if self.target() != Some(history.lineage().database_id())
            || self.incarnation() != Some(history.lineage().history_incarnation())
        {
            return false;
        }
        match self {
            // A V1 manifest's last retained command can precede the frontier
            // after pruning. It may never exceed the restored application head.
            Self::Ordinary(r) => r.manifest_identity().is_some_and(|manifest| {
                manifest.database_id() == history.lineage().database_id()
                    && manifest.included_application_frontier()
                        <= history.anchor().frontier().application()
            }),
            Self::Archived(r) => {
                r.restored_frontier() == Some(history.anchor().frontier())
                    && r.selection().is_some_and(|selection| {
                        selection.lineage().leadership_epoch()
                            == history.lineage().leadership_epoch()
                    })
            }
        }
    }
}

pub(super) fn verify_receipts(
    owner: &RedbMaintenanceStorage,
    expected: &[RestoreReceipt],
) -> Result<(), StorageError> {
    if expected.is_empty() {
        return Ok(());
    }
    let (ordinary, _, archived, _) = owner.read_complete_inventory(false)?;
    for receipt in expected {
        let present = match receipt {
            RestoreReceipt::Ordinary(prior) => ordinary
                .receipts()
                .iter()
                .any(|current| current.monotonically_extends(prior)),
            RestoreReceipt::Archived(prior) => archived
                .receipts()
                .iter()
                .any(|current| current.monotonically_extends(prior)),
        };
        if !present {
            return Err(corrupt());
        }
    }
    Ok(())
}

impl RedbMaintenanceStorage {
    /// Reconciles a historical successful promotion only through the retained
    /// restore receipts and a fully validated current Restore anchor. This
    /// never advances a receipt or grants operational ports; server startup and
    /// any unfinished maintenance operation must still complete their gates.
    pub fn reconcile_restored_promotion(
        &mut self,
        attempt: &ReplicationPromotionReceiptV1,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
        profile: crate::RedbCommitProfile,
        publication: Arc<dyn ChangelogPublicationPort>,
    ) -> Result<crate::RedbStore, StorageError> {
        self.reconciled_promotion = None;
        cancelled(&cancellation)?;
        self.verify_path_custody()?;
        let inventory = self.promotion_receipts()?;
        if attempt.phase() != Phase::Succeeded
            || !inventory.receipts().contains(attempt)
            || inventory.receipts().iter().any(|other| {
                !other.is_terminal() || (other.phase() == Phase::Succeeded && other != attempt)
            })
        {
            return Err(corrupt());
        }
        let (file, marker) = self.open_promotion_files()?;
        let history = {
            let database = redb::Database::builder()
                .create_file(file.try_clone().map_err(io_unavailable)?)
                .map_err(database_error)?;
            restore_history(&database.begin_read().map_err(transaction_error)?)?
        };
        let restores = self.restore_chain(attempt, history)?;
        self.verify_promotion_files(&file, &marker)?;
        cancelled(&cancellation)?;
        let store = crate::RedbStore::open_restored_source(
            &self.database_file,
            &file,
            profile,
            publication,
        )?;
        let store = crate::startup::validate_reconciled_promotion(store, inputs, cancellation)?;
        let observed = restore_history(
            &store
                .shared
                .database
                .begin_read()
                .map_err(transaction_error)?,
        )?;
        if observed.lineage() != history.lineage() || observed.anchor() != history.anchor() {
            return Err(corrupt());
        }
        self.verify_promotion_files(&file, &marker)?;
        verify_receipts(self, &restores)?;
        self.reconciled_promotion = Some(ReconciledPromotionReceipt {
            receipt: attempt.clone(),
            restores,
        });
        if let Err(error) = self.require_no_unreconciled_promotion() {
            self.reconciled_promotion = None;
            return Err(error);
        }
        Ok(store)
    }

    fn restore_chain(
        &self,
        attempt: &ReplicationPromotionReceiptV1,
        history: ChangelogHistoryStateV3,
    ) -> Result<Vec<RestoreReceipt>, StorageError> {
        let lineage = attempt.selection().ok_or_else(corrupt)?.published_lineage();
        // This role join precedes ordinary startup reconciliation. Preserve its
        // existing crash cleanup: validate the complete inventory first, then
        // remove only recognized unpublished receipt temporaries under custody.
        let (ordinary, _, archived, _) = self.read_complete_inventory(true)?;
        let mut candidates: Vec<_> = ordinary
            .receipts()
            .iter()
            .filter(|r| r.operation_kind() == OfflineMaintenanceOperationKind::RestoreBackup)
            .cloned()
            .map(RestoreReceipt::Ordinary)
            .chain(
                archived
                    .receipts()
                    .iter()
                    .cloned()
                    .map(RestoreReceipt::Archived),
            )
            .filter(|r| {
                // An unpublished failed attempt may reserve the same next
                // incarnation as a later successful restore. It is retained
                // audit evidence, never an authority transition in this chain.
                r.phase() != OfflineMaintenanceReceiptPhaseV1::FailedClosed
                    && r.incarnation()
                        .is_some_and(|i| i > lineage.history_incarnation())
            })
            .collect();
        candidates.sort_by_key(RestoreReceipt::incarnation);
        let mut database = lineage.database_id();
        let mut incarnation = lineage.history_incarnation();
        let mut chain = Vec::new();
        for receipt in candidates {
            let next = receipt.incarnation().ok_or_else(corrupt)?;
            if next > history.lineage().history_incarnation() {
                if receipt.phase() == OfflineMaintenanceReceiptPhaseV1::Offline {
                    continue;
                }
                return Err(corrupt());
            }
            // A later admitted recovery restore may have no readable prior
            // database identity. Its fresh staged authorization and durable
            // incarnation still belong to this same exclusive target owner.
            if next <= incarnation
                || receipt.source().is_some_and(|source| source != database)
                || !matches!(
                    receipt.phase(),
                    OfflineMaintenanceReceiptPhaseV1::Offline
                        | OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                        | OfflineMaintenanceReceiptPhaseV1::Validating
                        | OfflineMaintenanceReceiptPhaseV1::Succeeded
                )
                || (next < history.lineage().history_incarnation()
                    && receipt.phase() != OfflineMaintenanceReceiptPhaseV1::Succeeded)
            {
                return Err(corrupt());
            }
            database = receipt.target().ok_or_else(corrupt)?;
            incarnation = next;
            chain.push(receipt);
        }
        if chain
            .last()
            .is_none_or(|receipt| !receipt.matches_anchor(history))
        {
            return Err(corrupt());
        }
        Ok(chain)
    }
}

fn restore_history(read: &redb::ReadTransaction) -> Result<ChangelogHistoryStateV3, StorageError> {
    let history =
        crate::changelog_v3_roots::validate_retained_history(read)?.ok_or_else(corrupt)?;
    if crate::follower_lifecycle::is_attached(read)? {
        return Err(corrupt());
    }
    // The original anchor row may have been pruned. Reconstruct its canonical
    // empty Restore transaction from the retained lineage and covered frontier,
    // and require its exact hash and point instead of trusting a phase label.
    let anchor = AuthoritativeTransactionV3::new_for_catalog(
        AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: None,
            sequence: ChangelogTransactionSequence::new(1).ok_or_else(corrupt)?,
            predecessor_frontier: history.anchor().frontier(),
            covered_frontier: history.anchor().frontier(),
            prior_history_hash: [0; 32],
        },
        ChangelogAttributionV3::RestoreAnchor,
        Vec::new(),
        history.lineage().catalog_digest(),
    )
    .map_err(|_| corrupt())?;
    if ChangelogHistoryPointV3::from_receipt(&anchor).map_err(|_| corrupt())? != history.anchor() {
        return Err(corrupt());
    }
    Ok(history)
}
