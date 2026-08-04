#![forbid(unsafe_code)]

//! Durable redb implementation of RiffDB's semantic storage ports.

mod administration;
mod application;
mod backup;
#[cfg(feature = "benchmark-support")]
#[doc(hidden)]
pub mod benchmark_support;
mod codec;
mod consumer;
mod derived;
#[cfg(feature = "test-fixtures")]
mod durable_fixtures;
mod error;
#[cfg(feature = "test-fixtures")]
mod fixtures;
mod gate;
mod hooks;
mod keys;
mod layout;
mod maintenance;
mod migration_stage;
mod query;
mod reads;
mod retention;
mod shared_ports;
mod startup;
mod store;
mod transient;
mod validated_prefix;

pub use backup::{
    RedbOfflineBackup, RedbOfflineRestore, read_history_incarnation, stamp_history_incarnation,
};
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub use durable_fixtures::{MigrationDurableFixture, migration_durable_fixture_set};
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub use fixtures::{
    downgrade_all_index_rows_to_v1_fixture,
    read_validated_prefix_checkpoint_commit_sequence_fixture,
};
#[doc(hidden)]
pub use hooks::{RedbTestController, RedbTestEvent, RedbTestOperation, RedbTestPhase};
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub use maintenance::validate_maintenance_receipt_fixture;
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub use maintenance::validate_migration_receipt_fixture;
#[doc(hidden)]
pub use maintenance::{
    RedbMaintenanceFailpoint, RedbMaintenanceTestController, RedbMaintenanceTestEvent,
};
pub use maintenance::{
    RedbMaintenanceOperationEvidence, RedbMaintenanceReconciliation, RedbMaintenanceStorage,
    RedbMigrationDiskReservation, RedbSealedStagedRestore, RedbStagedRestore,
};
pub use migration_stage::{
    RedbContractMigrationContext, RedbContractMigrationImmutableWitness,
    RedbContractMigrationPreflight, RedbContractMigrationStage, RedbMigrationProjectionPorts,
};
pub use retention::{RedbOfflineRetention, RetentionStatusV1};
pub use shared_ports::RedbSharedPorts;
pub use startup::{
    RedbCompletionAuthority, RedbHistoricalEvidenceEnd, RedbStartupIndexMigrationPort,
    RedbStructuralEvidenceEnd, RedbStructuralEvidenceSession,
};
#[doc(hidden)]
pub use store::{REPAIR_PROGRESS_SENTINEL, reset_last_repair_progress_for_tests};
pub use store::{
    RedbCommitProfile, RedbDormantPorts, RedbOperationalPorts, RedbStore,
    last_repair_progress_basis_points,
};
