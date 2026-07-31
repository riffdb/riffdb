#![forbid(unsafe_code)]

//! Durable redb implementation of RiffDB's semantic storage ports.

mod administration;
mod application;
mod backup;
#[cfg(feature = "benchmark-support")]
#[doc(hidden)]
pub mod benchmark_support;
mod codec;
mod derived;
mod error;
#[cfg(feature = "test-fixtures")]
mod fixtures;
mod gate;
mod hooks;
mod keys;
mod layout;
mod maintenance;
mod query;
mod reads;
mod startup;
mod store;
mod transient;

pub use backup::{
    RedbOfflineBackup, RedbOfflineRestore, read_history_incarnation, stamp_history_incarnation,
};
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub use fixtures::downgrade_all_index_rows_to_v1_fixture;
#[doc(hidden)]
pub use hooks::{RedbTestController, RedbTestEvent, RedbTestOperation, RedbTestPhase};
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub use maintenance::validate_maintenance_receipt_fixture;
#[doc(hidden)]
pub use maintenance::{
    RedbMaintenanceFailpoint, RedbMaintenanceTestController, RedbMaintenanceTestEvent,
};
pub use maintenance::{
    RedbMaintenanceOperationEvidence, RedbMaintenanceReconciliation, RedbMaintenanceStorage,
    RedbSealedStagedRestore, RedbStagedRestore,
};
pub use startup::{
    RedbCompletionAuthority, RedbHistoricalEvidenceEnd, RedbStartupIndexMigrationPort,
    RedbStructuralEvidenceEnd, RedbStructuralEvidenceSession,
};
pub use store::{RedbCommitProfile, RedbDormantPorts, RedbOperationalPorts, RedbStore};
