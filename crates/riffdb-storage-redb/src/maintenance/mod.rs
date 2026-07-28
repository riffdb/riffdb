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
};

#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub fn validate_maintenance_receipt_fixture(
    encoded: &[u8],
) -> Result<(), riffdb_storage_api::StorageError> {
    codec::decode_receipt(encoded).map(drop)
}
