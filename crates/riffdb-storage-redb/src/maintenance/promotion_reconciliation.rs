//! Exact committed recovery. No path here can perform a new promotion cutover.
use super::*;
use crate::error::{
    codec_error, database_error, precommit_storage_error, table_error, transaction_error,
};
use crate::promotion_cutover::{corrupt, history, validate_record};
use redb::ReadableDatabase;
use riffdb_storage_api::{
    ChangelogPublicationPort, ReplicationPromotionPhaseV1 as Phase,
    ReplicationPromotionStepV1 as Step, StoredPromotionAdministrationV1 as Record, proto_codec::*,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[path = "promotion_restore_reconciliation.rs"]
mod restored;

/// One bounded exact terminal receipt, minted only after this owner's complete
/// committed-source reconciliation. It retains no engine or publication pin and
/// grants neither source readiness nor an operation's independent preconditions.
pub(super) struct ReconciledPromotionReceipt {
    receipt: riffdb_storage_api::ReplicationPromotionReceiptV1,
    restores: Vec<restored::RestoreReceipt>,
}

impl ReconciledPromotionReceipt {
    pub(super) fn matches(
        &self,
        receipt: &riffdb_storage_api::ReplicationPromotionReceiptV1,
    ) -> bool {
        &self.receipt == receipt
    }

    // Exact committed success resolves the operation without rewriting an older
    // invocation's audit. Before selection it could not have cut over; after
    // selection it must name the same immutable choice. A second invocation
    // claiming committed/validated authority is contradictory, never covered.
    pub(super) fn covers_pre_cutover_attempt(
        &self,
        receipt: &riffdb_storage_api::ReplicationPromotionReceiptV1,
    ) -> bool {
        matches!(
            receipt.phase(),
            Phase::Attempted
                | Phase::Draining
                | Phase::Offline
                | Phase::Selected
                | Phase::CutoverPending
        ) && receipt.request() == self.receipt.request()
            && receipt
                .selection()
                .is_none_or(|selection| self.receipt.selection() == Some(selection))
    }

    pub(super) fn verify_restores(
        &self,
        owner: &RedbMaintenanceStorage,
    ) -> Result<(), StorageError> {
        restored::verify_receipts(owner, &self.restores)
    }
}

/// Exact committed audit/control/anchor binding. Source recovery additionally
/// joins the external attempt under the exclusive ledger owner. A verified
/// private backup may use this binding for fenced validation only.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct PromotionValidationBinding {
    record: Record,
}

impl PromotionValidationBinding {
    pub(in crate::maintenance) fn for_verified_backup(
        read: &redb::ReadTransaction,
        manifest: &OfflineBackupManifestV1,
    ) -> Result<Option<Self>, StorageError> {
        let Some(record) = crate::promotion_cutover::current_record(read)? else {
            return Ok(None);
        };
        let binding = Self { record };
        binding.validate(read)?;
        let lineage = history(&binding.record)?.lineage();
        if manifest.database_id() != lineage.database_id()
            || manifest.history_incarnation() != Some(lineage.history_incarnation())
        {
            return Err(corrupt());
        }
        Ok(Some(binding))
    }

    pub(crate) fn validate(&self, read: &redb::ReadTransaction) -> Result<(), StorageError> {
        let retained =
            crate::changelog_v3_roots::read_checkpoint_roots(read)?.ok_or_else(corrupt)?;
        let expected = history(&self.record)?;
        if retained.lineage() != expected.lineage()
            || retained.anchor() != expected.anchor()
            || crate::follower_lifecycle::is_attached(read)?
        {
            return Err(corrupt());
        }
        let meta = read.open_table(crate::layout::META).map_err(table_error)?;
        let admission = crate::primary_admission_roots::read(&meta)?.ok_or_else(corrupt)?;
        if admission.lineage() != retained.lineage() {
            return Err(corrupt());
        }
        let audit = read.open_table(crate::layout::AUDIT).map_err(table_error)?;
        let encoded = encode_promotion_administration_v1(&self.record).map_err(codec_error)?;
        if audit
            .get(crate::keys::encode_audit_key(self.record.administration_sequence()).as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?
            .value()
            != encoded.as_bytes()
        {
            return Err(corrupt());
        }
        validate_record(
            &self.record,
            retained,
            &audit,
            &read
                .open_table(crate::layout::AUDIT_BY_REQUEST)
                .map_err(table_error)?,
            &read
                .open_table(crate::changelog_v3_activation::HISTORY)
                .map_err(table_error)?,
        )
    }

    fn require_initial_cutover(&self, read: &redb::ReadTransaction) -> Result<(), StorageError> {
        self.validate(read)?;
        if crate::changelog_v3_roots::read_checkpoint_roots(read)? != Some(history(&self.record)?) {
            return Err(corrupt());
        }
        let meta = read.open_table(crate::layout::META).map_err(table_error)?;
        if crate::primary_admission_roots::read(&meta)?
            .ok_or_else(corrupt)?
            .fence()
            .is_some()
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

impl RedbMaintenanceStorage {
    /// Discovers an already committed current-lineage promotion under exclusive
    /// pinned custody and joins its exact external attempt. An attached follower
    /// returns None; this method never promotes, advances a receipt or grants
    /// readiness. Full reconciliation and ordinary server validation must follow.
    /// The caller must supply an existing database with the current format marker.
    pub fn discover_committed_promotion(&self) -> Result<Option<Record>, StorageError> {
        let (file, marker) = self.open_promotion_files()?;
        let found = {
            let database = redb::Database::builder()
                .create_file(file.try_clone().map_err(io_unavailable)?)
                .map_err(database_error)?;
            let read = database.begin_read().map_err(transaction_error)?;
            if crate::follower_lifecycle::is_attached(&read)? {
                None
            } else if let Some(record) = crate::promotion_cutover::current_record(&read)? {
                PromotionValidationBinding {
                    record: record.clone(),
                }
                .validate(&read)?;
                self.matching_promotion_attempt(&record)?;
                Some(record)
            } else {
                None
            }
        };
        self.verify_promotion_files(&file, &marker)?;
        Ok(found)
    }

    fn matching_promotion_attempt(
        &self,
        record: &Record,
    ) -> Result<riffdb_storage_api::ReplicationPromotionReceiptV1, StorageError> {
        let inventory = self.promotion_receipts()?;
        let attempt = inventory
            .receipts()
            .iter()
            .find(|attempt| attempt.request_id() == record.attempt().request_id())
            .filter(|attempt| attempt.monotonically_extends(record.attempt()))
            .cloned()
            .ok_or_else(corrupt)?;
        if !matches!(
            attempt.phase(),
            Phase::CutoverPending | Phase::CutoverCommitted | Phase::Validated | Phase::Succeeded
        ) || inventory.receipts().iter().any(|other| {
            !other.is_terminal()
                && other.request().operation_id() != attempt.request().operation_id()
        }) {
            return Err(corrupt());
        }
        Ok(attempt)
    }

    fn open_promotion_files(&self) -> Result<(File, File), StorageError> {
        self.verify_path_custody()?;
        let name = self.database_file.file_name().ok_or_else(invariant)?;
        let file = self
            .database_parent_guard
            .open_file_read_write(name)?
            .into_std();
        let marker_path = crate::durable_format_marker_path(&self.database_file);
        let marker_name = marker_path.file_name().ok_or_else(invariant)?;
        let mut marker = self
            .database_parent_guard
            .open_file(marker_name)?
            .into_std();
        crate::maintenance::path_guard::check_current_marker(&mut marker)?;
        self.verify_promotion_files(&file, &marker)?;
        Ok((file, marker))
    }

    /// Reconciles only an already committed promotion. Before cutover, this
    /// refuses without advancing the receipt or granting any source handle.
    /// Pending recovery proves the whole ordinary source while privately fenced,
    /// then durably publishes the remaining external phases. A succeeded retry
    /// checks the original authority and preserves subsequent source history.
    ///
    /// The returned store remains dormant: server composition must complete its
    /// normal catalog/structural proof join before acquiring operational ports.
    pub fn reconcile_committed_promotion(
        &mut self,
        record: &Record,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
        profile: crate::RedbCommitProfile,
        publication: Arc<dyn ChangelogPublicationPort>,
    ) -> Result<crate::RedbStore, StorageError> {
        // A failed fresh validation must not leave an earlier maintenance join
        // available. The complete current ledger is still checked on every use.
        self.reconciled_promotion = None;
        cancelled(&cancellation)?;
        self.verify_path_custody()?;
        let mut attempt = self.matching_promotion_attempt(record)?;
        let (file, marker) = self.open_promotion_files()?;
        let binding = Arc::new(PromotionValidationBinding {
            record: record.clone(),
        });
        self.verify_promotion_files(&file, &marker)?;
        {
            let database = redb::Database::builder()
                .create_file(file.try_clone().map_err(io_unavailable)?)
                .map_err(database_error)?;
            let read = database.begin_read().map_err(transaction_error)?;
            binding.validate(&read)?;
            if attempt.phase() != Phase::Succeeded {
                binding.require_initial_cutover(&read)?;
                for path in [
                    crate::journal::journal_path(&self.database_file),
                    crate::journal::checkpoint_journal_path(&self.database_file),
                    crate::journal::spare_journal_path(&self.database_file),
                ] {
                    if path.try_exists().map_err(io_unavailable)? {
                        return Err(corrupt());
                    }
                }
            }
        }
        edge("authority-joined");
        if attempt.phase() == Phase::CutoverPending {
            attempt
                .advance(Step::Phase(Phase::CutoverCommitted))
                .map_err(value_error)?;
            self.persist_promotion_receipt(&attempt)?;
        }
        edge("committed-receipt");
        if attempt.phase() != Phase::Succeeded {
            // A decoded Validated phase is never substituted for a new complete
            // walk after interruption. No journal or local writer exists here.
            crate::startup::validate_committed_promotion(
                &self.database_file,
                &file,
                Arc::clone(&binding),
                inputs.clone(),
                Arc::clone(&cancellation),
            )?;
            self.verify_promotion_files(&file, &marker)?;
            edge("source-validated");
            cancelled(&cancellation)?;
            if attempt.phase() == Phase::CutoverCommitted {
                attempt
                    .advance(Step::Phase(Phase::Validated))
                    .map_err(value_error)?;
                self.persist_promotion_receipt(&attempt)?;
            }
            edge("validated-receipt");
            attempt
                .advance(Step::Phase(Phase::Succeeded))
                .map_err(value_error)?;
        }
        // Re-establish file and parent durability for an uncertain identical
        // publication too, before constructing a source-capable store.
        self.persist_promotion_receipt(&attempt)?;
        self.verify_promotion_files(&file, &marker)?;
        edge("succeeded-receipt");
        cancelled(&cancellation)?;
        let store = crate::RedbStore::open_reconciled_promotion(
            &self.database_file,
            &file,
            binding,
            profile,
            publication,
        )?;
        edge("source-open-begun");
        let store = crate::startup::validate_reconciled_promotion(store, inputs, cancellation)?;
        self.verify_promotion_files(&file, &marker)?;
        edge("source-opened");
        self.reconciled_promotion = Some(ReconciledPromotionReceipt {
            receipt: attempt,
            restores: Vec::new(),
        });
        Ok(store)
    }

    fn verify_promotion_files(&self, file: &File, marker: &File) -> Result<(), StorageError> {
        self.verify_path_custody()?;
        crate::maintenance::path_guard::check_current_marker(
            &mut marker.try_clone().map_err(io_unavailable)?,
        )?;
        let marker_path = crate::durable_format_marker_path(&self.database_file);
        if !self
            .database_parent_guard
            .regular_file_matches(self.database_file.file_name().ok_or_else(invariant)?, file)?
            || !self
                .database_parent_guard
                .regular_file_matches(marker_path.file_name().ok_or_else(invariant)?, marker)?
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

fn cancelled(cancellation: &AtomicBool) -> Result<(), StorageError> {
    if cancellation.load(Ordering::Acquire) {
        Err(storage_error(StorageErrorKind::Unavailable))
    } else {
        Ok(())
    }
}

fn edge(_name: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_PROMOTION_RECONCILIATION_CRASH_EDGE")
        .ok()
        .as_deref()
        == Some(_name)
    {
        std::process::abort();
    }
}
