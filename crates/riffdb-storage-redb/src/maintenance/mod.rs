//! Private external receipt and offline staged-publication mechanics.

mod codec;
mod failpoint;
mod path_guard;
mod staged;
mod store;

#[doc(hidden)]
pub use failpoint::{
    RedbMaintenanceFailpoint, RedbMaintenanceTestController, RedbMaintenanceTestEvent,
};
pub use staged::{RedbSealedStagedRestore, RedbStagedRestore};
pub use store::{
    RedbMaintenanceOperationEvidence, RedbMaintenanceReconciliation, RedbMaintenanceStorage,
    RedbMigrationDiskReservation,
};

#[cfg(feature = "test-fixtures")]
pub(crate) fn encode_migration_receipt_fixture(
    receipt: &riffdb_storage_api::ContractMigrationReceiptV1,
) -> Result<Vec<u8>, riffdb_storage_api::StorageError> {
    codec::encode_migration_receipt(receipt)
}

#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub fn validate_maintenance_receipt_fixture(
    encoded: &[u8],
) -> Result<(), riffdb_storage_api::StorageError> {
    codec::decode_receipt(encoded).map(drop)
}

#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub fn validate_migration_receipt_fixture(
    encoded: &[u8],
) -> Result<(), riffdb_storage_api::StorageError> {
    codec::decode_migration_receipt(encoded).map(drop)
}
