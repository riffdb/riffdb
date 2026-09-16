//! Additive empty ledger installation for explicitly recognized legacy upgrades.

use super::*;

pub(super) fn needs_installation(
    transaction: &redb::ReadTransaction,
    recognized_pre_v3_registry: bool,
) -> Result<bool, StorageError> {
    // Missing current authority is corruption. Active V3 table inventories
    // were already checked exactly by classify_read_layout.
    match transaction.open_table(crate::layout::APPLICATION_EXPORT_PAGE_COMMITMENTS) {
        Ok(_) => Ok(false),
        Err(redb::TableError::TableDoesNotExist(_)) if recognized_pre_v3_registry => Ok(true),
        Err(redb::TableError::TableDoesNotExist(_)) => {
            Err(storage_error(StorageErrorKind::CorruptData))
        }
        Err(error) => Err(table_error(error)),
    }
}

pub(super) fn install_empty_legacy_table(shared: &SharedRedb) -> Result<(), StorageError> {
    let mut transaction = shared.database.begin_write().map_err(transaction_error)?;
    transaction.set_two_phase_commit(shared.application_commit_profile.uses_two_phase());
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    drop(
        transaction
            .open_table(crate::layout::APPLICATION_EXPORT_PAGE_COMMITMENTS)
            .map_err(table_error)?,
    );
    shared.commit_durable(transaction)
}
