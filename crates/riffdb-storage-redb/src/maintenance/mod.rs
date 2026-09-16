//! Private external receipt and offline staged-publication mechanics.

mod archive_backup;
mod archive_repository;
pub use archive_backup::RedbVerifiedArchiveBackup;
#[cfg(test)]
mod archive_backup_tests;
#[cfg(test)]
mod archive_repository_tests;
pub use archive_repository::{RedbArchiveFrames, RedbArchiveRepository};
mod bootstrap_materialize;
mod bootstrap_receiver_repository;
mod bootstrap_repository;
pub use bootstrap_receiver_repository::RedbBootstrapReceiverRepository;
mod bootstrap_source;
pub use bootstrap_materialize::{
    RedbBootstrapCandidate, RedbBootstrapCatalogSession, RedbBootstrapMaterializer,
    RedbPublishedBootstrapCandidate, RedbValidatedBootstrapCandidate,
};
pub use bootstrap_repository::RedbBootstrapRepository;
#[cfg(test)]
mod bootstrap_materialize_tests;
mod bootstrap_stage;
pub use bootstrap_source::{RedbBootstrapSourceBuild, RedbHeldBootstrapSource};
mod codec;
pub use bootstrap_stage::{
    RedbBootstrapMaterializationInput, RedbBootstrapStage, RedbBootstrapVerification,
    RedbVerifiedBootstrapTransfer,
};
#[cfg(test)]
mod bootstrap_stage_tests;
mod failpoint;
mod follower_columnar_scratch;
pub use follower_columnar_scratch::{RedbFollowerColumnarBuild, RedbFollowerColumnarScratch};
#[cfg(test)]
mod follower_columnar_scratch_tests;
mod follower_namespace;
mod path_guard;
pub(crate) use follower_namespace::FollowerNamespace;
mod staged;
mod store;

#[doc(hidden)]
pub use failpoint::{
    RedbMaintenanceFailpoint, RedbMaintenanceTestController, RedbMaintenanceTestEvent,
};
pub(crate) use staged::PrivateArchiveValidationBinding;
pub use staged::{
    RedbArchiveRestoreStage, RedbPreparedArchiveRestore, RedbPrivateArchiveRestoreCandidate,
    RedbReplayedArchiveRestore, RedbSealedArchiveRestore, RedbSealedStagedRestore,
    RedbStagedRestore, RedbValidatedPrivateArchiveRestore,
};
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
