#![forbid(unsafe_code)]

//! Durable redb implementation of RiffDB's semantic storage ports.

pub use riffdb_storage_api::{
    DurableFormatAction, DurableFormatIdentity, current_durable_format_manifest,
};

mod administration;
mod application;
mod application_export;
mod application_installation;
mod backup;
#[cfg(feature = "benchmark-support")]
#[doc(hidden)]
pub mod benchmark_support;
mod changelog;
mod codec;
mod command_authority;
mod composite_view;
mod consumer;
mod derived;
#[cfg(feature = "test-fixtures")]
mod durable_fixtures;
mod error;
#[cfg(feature = "test-fixtures")]
mod fixtures;
mod format_preflight;
mod format_upgrade;
mod gate;
mod hooks;
mod journal;
mod keys;
mod layout;
mod maintenance;
mod media;
mod migration_stage;
mod query;
mod reads;
mod retention;
mod shared_ports;
mod startup;
mod store;
#[cfg(test)]
mod test_path;

/// Fixed-cardinality, redaction-safe process-generation command-frame census.
///
/// This diagnostic surface is consumed by the release benchmark harness. It
/// carries only aggregate counts and byte totals in a closed order.
#[doc(hidden)]
#[must_use]
pub fn writer_command_frame_census_v1() -> [u64; 6] {
    journal::command_frame_census()
}

/// Fixed-cardinality command-bearing physical journal-flush census.
///
/// Values are physical flushes, command frames, logical commands, complete
/// frame bytes, maximum command frames covered by one flush, cumulative
/// write-plus-sync microseconds, and maximum write-plus-sync microseconds.
#[doc(hidden)]
#[must_use]
pub fn writer_command_flush_census_v1() -> [u64; 7] {
    journal::command_flush_census()
}
mod transient;
mod validated_prefix;

pub use backup::{
    RedbOfflineBackup, RedbOfflineRestore, read_history_incarnation, stamp_history_incarnation,
};
pub use changelog::{
    DEFAULT_CHANGELOG_BUFFER_ADVANCEMENTS, RedbChangelogEmitter, RedbChangelogEmitterHandle,
    start_changelog_emitter, start_changelog_emitter_v2,
};
#[doc(hidden)]
pub use consumer::{
    MAX_PROTECTED_EVENT_REPLAY_CANDIDATES, ProtectedEventConsumerLeaseV1,
    ProtectedEventConsumerLeaseValidationResultV1, ProtectedEventConsumerLeaseValidationV1,
    ProtectedEventConsumerResolutionV1, ProtectedEventReplayDispositionV1,
    ProtectedEventReplayPageV1, ProtectedEventReplayResultV1, ProtectedEventReplayV1,
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
pub use format_preflight::{
    RedbDurableFormatPreflight, RedbDurableFormatPreflightError, durable_format_marker_path,
    preflight_durable_format_path,
};
pub use format_upgrade::{
    RedbDurableFormatUpgrade, RedbDurableFormatUpgradeDisposition, RedbDurableFormatUpgradeError,
    RedbDurableFormatUpgradePhase, RedbDurableFormatUpgradeReceipt, RedbDurableFormatUpgradeResult,
    durable_format_upgrade_receipt_path,
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
#[doc(hidden)]
pub use media::{
    JournalMedia, JournalMediaFile, MediaFile, MediaFileMetadata, RealJournalMedia,
    RedbStorageMedia,
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
    RedbCommitProfile, RedbDormantPorts, RedbDurabilityEpoch, RedbOperationalPorts, RedbStore,
    last_repair_progress_basis_points,
};
