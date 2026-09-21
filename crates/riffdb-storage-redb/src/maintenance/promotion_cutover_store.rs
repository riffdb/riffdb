//! Closed offline transaction under the existing exclusive maintenance owner.
use super::*;
use crate::{
    changelog_v3_activation::{HISTORY, SOURCE_HOLDS},
    changelog_v3_write::{apply_mutation, check_predecessor, table_inventory},
    error::{
        codec_error, commit_error, database_error, precommit_storage_error, table_error,
        transaction_error,
    },
    layout::META,
    promotion_cutover::{corrupt, history, receipt},
};
use redb::{Durability, ReadableTable};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV2, ReplicationAuthorityClassV1,
    ReplicationFollowerStateV3, ReplicationPrimaryAdmissionV1, ReplicationTransferV1,
    StoredPromotionAdministrationV1, proto_codec::*,
};

impl RedbMaintenanceStorage {
    /// Storage-internal cutover only: the server must already own fresh policy,
    /// authenticated source proof and a drained receiver with all readers closed.
    /// Decoded attempt metadata provides none of those capabilities. This owner
    /// changes no external phase and grants no application readiness. Ordinary
    /// source opens remain refused until committed receipt reconciliation exists.
    pub fn apply_promotion_cutover(
        &mut self,
        record: &StoredPromotionAdministrationV1,
    ) -> Result<(), StorageError> {
        self.verify_path_custody()?;
        let inventory = self.promotion_receipts()?;
        if inventory
            .receipts()
            .iter()
            .find(|r| r.request_id() == record.attempt().request_id())
            != Some(record.attempt())
        {
            return Err(corrupt());
        }
        for attempt in inventory.receipts() {
            use riffdb_storage_api::ReplicationPromotionPhaseV1 as Phase;
            if matches!(
                attempt.phase(),
                Phase::CutoverCommitted | Phase::Validated | Phase::Succeeded
            ) || (!attempt.is_terminal()
                && attempt.request().operation_id() != record.attempt().request().operation_id())
            {
                return Err(corrupt());
            }
        }
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
        super::super::path_guard::check_current_marker(&mut marker)?;
        // create_file consumes an already pinned existing file. It cannot create
        // or follow a substituted database pathname.
        if !self
            .database_parent_guard
            .regular_file_matches(name, &file)?
        {
            return Err(corrupt());
        }
        let database = redb::Database::builder()
            .create_file(file.try_clone().map_err(io_unavailable)?)
            .map_err(database_error)?;
        crate::store::validate_follower_open(
            &database,
            &self.database_file,
            &crate::media::RealJournalMedia,
        )?;
        let mut transaction = database.begin_write().map_err(transaction_error)?;
        transaction.set_two_phase_commit(true);
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| invariant())?;
        edge("opened");
        let before = crate::changelog_v3_roots::validate_retained_history_for_write(&transaction)?
            .ok_or_else(corrupt)?;
        edge("roots-read");
        let selected = record.attempt().selection().ok_or_else(corrupt)?;
        if before.lineage() != selected.evidence().source_history().lineage()
            || before.tail() != selected.applied()
            || before.lineage().catalog_digest() != AuthoritativeStateCatalogV2.digest()
        {
            return Err(corrupt());
        }
        {
            let meta = transaction.open_table(META).map_err(table_error)?;
            let follower = decode_replication_follower_state_v3(
                meta.get(key(N::ReplicationFollowerState)?)
                    .map_err(precommit_storage_error)?
                    .ok_or_else(corrupt)?
                    .value(),
            )
            .map_err(codec_error)?;
            if !matches!(follower.value().attached_state(), Some((lineage, applied, _))
                if lineage == before.lineage() && applied == before.tail())
                || crate::primary_admission_roots::read(&meta)?.is_some()
            {
                return Err(corrupt());
            }
        }
        edge("follower-checked");
        let tables = table_inventory(&transaction)?;
        // A request ID must never acquire another lifecycle, including command
        // audits represented by checked locators instead of physical audit rows.
        let prefix = crate::keys::encode_audit_by_request_prefix(record.attempt().request_id());
        for table in [
            crate::layout::AUDIT_BY_REQUEST,
            crate::layout::AUDIT_BY_REQUEST_LOCATORS,
        ] {
            if !tables.contains(redb::TableHandle::name(&table)) {
                return Err(corrupt());
            }
            let table = transaction.open_table(table).map_err(table_error)?;
            if let Some(row) = table
                .range(prefix.as_slice()..)
                .map_err(precommit_storage_error)?
                .next()
            {
                let (key, _) = row.map_err(precommit_storage_error)?;
                if key.value().starts_with(&prefix) {
                    return Err(corrupt());
                }
            }
        }
        let receipt = receipt(record)?;
        edge("receipt-built");
        let after = history(record)?;
        for mutation in receipt.mutations() {
            if !tables.contains(mutation.namespace().table()) {
                return Err(corrupt());
            }
            check_predecessor(&transaction, mutation)?;
        }
        // Freeze every encoded new root before touching authority.
        let encoded_receipt = receipt.encode().map_err(|_| corrupt())?;
        let encoded_history = encode_changelog_history_state_v3(after).map_err(codec_error)?;
        let allocator = encode_changelog_transaction_allocator_v3(after.expected_allocator())
            .map_err(codec_error)?;
        let detached = encode_replication_follower_state_v3(ReplicationFollowerStateV3::detached())
            .map_err(codec_error)?;
        let admission = encode_replication_primary_admission_v1(
            &ReplicationPrimaryAdmissionV1::active(after.lineage()).map_err(|_| corrupt())?,
        )
        .map_err(codec_error)?;
        let epoch =
            encode_leadership_epoch_v1(after.lineage().leadership_epoch()).map_err(codec_error)?;
        edge("preflight");
        crate::backup::stamp_incarnation_metadata(
            &transaction,
            after.lineage().history_incarnation(),
        )?;
        edge("incarnation");
        for mutation in receipt.mutations() {
            apply_mutation(&transaction, mutation)?;
        }
        edge("audit");
        if !transaction.delete_table(HISTORY).map_err(table_error)?
            || !transaction
                .delete_table(SOURCE_HOLDS)
                .map_err(table_error)?
        {
            return Err(corrupt());
        }
        drop(transaction.open_table(SOURCE_HOLDS).map_err(table_error)?);
        {
            let mut meta = transaction.open_table(META).map_err(table_error)?;
            for namespace in N::ALL {
                if namespace.class()
                    == ReplicationAuthorityClassV1::ReplicationControl(
                        ReplicationTransferV1::SourceOnly,
                    )
                    && let Some(key) = namespace.metadata_key()
                {
                    meta.remove(key).map_err(precommit_storage_error)?;
                }
            }
            for (key, value) in [
                (key(N::ReplicationFollowerState)?, detached.as_bytes()),
                (crate::primary_admission_roots::key()?, admission.as_bytes()),
                (key(N::LeadershipEpoch)?, epoch.as_bytes()),
                (key(N::ChangelogHistoryState)?, encoded_history.as_bytes()),
                (key(N::NextChangelogTransaction)?, allocator.as_bytes()),
            ] {
                meta.insert(key, value).map_err(precommit_storage_error)?;
            }
        }
        edge("lineage");
        transaction
            .open_table(HISTORY)
            .map_err(table_error)?
            .insert(1u64.to_be_bytes().as_slice(), encoded_receipt.as_slice())
            .map_err(precommit_storage_error)?;
        if crate::changelog_v3_roots::validate_retained_history_for_write(&transaction)?
            != Some(after)
        {
            return Err(corrupt());
        }
        edge("anchor");
        self.verify_path_custody()?;
        if !self
            .database_parent_guard
            .regular_file_matches(name, &file)?
        {
            return Err(corrupt());
        }
        super::super::path_guard::check_current_marker(&mut marker)?;
        if !self
            .database_parent_guard
            .regular_file_matches(marker_name, &marker)?
        {
            return Err(corrupt());
        }
        transaction.commit().map_err(commit_error)?;
        edge("committed");
        self.verify_path_custody()?;
        if !self
            .database_parent_guard
            .regular_file_matches(name, &file)?
        {
            return Err(corrupt());
        }
        super::super::path_guard::check_current_marker(&mut marker)?;
        if !self
            .database_parent_guard
            .regular_file_matches(marker_name, &marker)?
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

fn key(namespace: N) -> Result<&'static str, StorageError> {
    namespace.metadata_key().ok_or_else(corrupt)
}
fn edge(_name: &str) {
    #[cfg(test)]
    if std::env::var_os("RIFFDB_PROMOTION_CUTOVER_TRACE").is_some() {
        eprintln!("promotion edge: {_name}");
    }
    #[cfg(test)]
    if std::env::var("RIFFDB_PROMOTION_CUTOVER_CRASH_EDGE")
        .ok()
        .as_deref()
        == Some(_name)
    {
        std::process::abort();
    }
}
