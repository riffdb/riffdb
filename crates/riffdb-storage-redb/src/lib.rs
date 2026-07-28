#![forbid(unsafe_code)]

//! Durable redb implementation of RiffDB's semantic storage ports.

mod administration;
mod application;
mod backup;
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
mod reads;
mod startup;
mod store;
mod transient;

pub use backup::{RedbOfflineBackup, RedbOfflineRestore};
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
pub use store::{RedbDormantPorts, RedbOperationalPorts, RedbStore};
