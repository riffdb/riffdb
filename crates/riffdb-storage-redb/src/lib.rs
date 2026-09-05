#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

//! Durable redb implementation of RiffDB's semantic storage ports.

/// Fixed stack reservation for dedicated production storage threads.
pub(crate) const PRODUCTION_THREAD_STACK_BYTES: usize = 384 * 1024;

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
mod checkpoint_root;
mod clean_close;
mod codec;
mod columnar_projection_control;
mod command_authority;
mod command_segment_preparation;
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
#[cfg(feature = "test-fixtures")]
#[doc(hidden)]
pub use migration_stage::{RedbMigrationStageFixture, RedbMigrationStageSnapshot};
mod owned_snapshot;
#[cfg(test)]
mod query;
mod query_diagnostics;
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

/// Fixed-cardinality journal queue/encode/write/sync stage census.
///
/// Values are command-frame queue observations, queue sum/max microseconds,
/// command-bearing flushes, and encode/write/sync sum/max microseconds.
#[doc(hidden)]
#[must_use]
pub fn writer_journal_stage_census_v1() -> [u64; 10] {
    journal::command_journal_stage_census()
}

/// Fixed-cardinality ordered command-publication stage census.
///
/// Values are command publications followed by residence, receipt-block,
/// durable-to-publication, and publication-work sum/max microseconds.
#[doc(hidden)]
#[must_use]
pub fn writer_publication_stage_census_v1() -> [u64; 9] {
    store::command_publication_stage_census()
}

/// Fixed execute-stage order for the bounded query-growth diagnostic.
///
/// Stages `point_lookup` through `scan_setup` are physically disjoint segments
/// nested inside the storage read-view callbacks the program drive makes.
/// `view_call_residual` is what remains inside those callbacks, and
/// `program_drive_exclusive` is what remains in the executor once every
/// callback is removed. The three `point_*` sub-stages decompose `point_lookup`
/// itself and are therefore already counted inside it: they are deliberately
/// excluded from the residual subtraction.
#[doc(hidden)]
pub const QUERY_EXECUTE_STAGE_LABELS_V1: [&str; 22] = [
    "publication_outer_lock",
    "publication_view_capture",
    "frontier_capture",
    "point_lookup",
    "envelope_identity_bounds",
    "payload_checksum",
    "wire_preflight",
    "prost_decode",
    "canonical_reencode",
    "semantic_reconstruct",
    "target_validate",
    "row_policy",
    "row_materialize",
    "index_epoch_read",
    "index_range_read",
    "index_entry_decode",
    "scan_setup",
    "point_open_table",
    "point_btree_get",
    "point_value_copy",
    "view_call_residual",
    "program_drive_exclusive",
];

/// Trailing fixed-cardinality census values emitted after the stage vector.
///
/// Order: overlay transition sum/max, overlay byte sum/max, authority-tail
/// byte sum/max, authority-tail command sum/max, entity point-read sum/max,
/// index-row sum/max, index-range-read sum/max, program-step sum/max.
#[doc(hidden)]
pub const QUERY_EXECUTE_TRAILING_VALUES_V1: usize = 16;

/// Number of query-execute samples merged into one ordinal window.
#[doc(hidden)]
pub const QUERY_EXECUTE_WINDOW_WIDTH_V1: usize = 256;
/// Maximum retained ordinal windows; the final window absorbs later samples.
#[doc(hidden)]
pub const QUERY_EXECUTE_WINDOW_COUNT_V1: usize = 64;

/// One bounded query-execute ordinal window.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryExecuteWindowV1 {
    pub count: u64,
    pub stage_ns: [u64; QUERY_EXECUTE_STAGE_LABELS_V1.len()],
    pub overlay_transitions_sum: u64,
    pub overlay_transitions_max: u64,
    pub overlay_bytes_sum: u64,
    pub overlay_bytes_max: u64,
    pub authority_tail_bytes_sum: u64,
    pub authority_tail_bytes_max: u64,
    pub authority_tail_commands_sum: u64,
    pub authority_tail_commands_max: u64,
    pub entity_point_reads_sum: u64,
    pub entity_point_reads_max: u64,
    pub index_rows_sum: u64,
    pub index_rows_max: u64,
    pub index_range_reads_sum: u64,
    pub index_range_reads_max: u64,
    pub program_steps_sum: u64,
    pub program_steps_max: u64,
}

/// Complete fixed-cardinality process-generation query-execute census.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryExecuteCensusV1 {
    pub total_count: u64,
    pub windows: [QueryExecuteWindowV1; QUERY_EXECUTE_WINDOW_COUNT_V1],
}

/// Returns the retired storage-owned V1 census, empty after WP-754.
///
/// Query execution now lives above storage; reporting zero-filled storage
/// stages as executor timing would be false evidence.
#[doc(hidden)]
#[must_use]
pub fn query_execute_census_v1() -> QueryExecuteCensusV1 {
    query_diagnostics::query_execute_census_v1()
}
mod transient;
mod validated_prefix;
mod vector_projection_control;

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
    downgrade_all_index_rows_to_v1_fixture, read_validated_prefix_checkpoint_bytes_fixture,
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
#[doc(hidden)]
pub use validated_prefix::{
    GracefulCheckpointCloseReceiptV1, GracefulCheckpointDispositionV1, GracefulLifecycleOutcomeV1,
};
