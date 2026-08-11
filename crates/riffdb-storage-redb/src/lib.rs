#![forbid(unsafe_code)]

//! Durable redb implementation of RiffDB's semantic storage ports.

pub use riffdb_storage_api::{
    DurableFormatAction, DurableFormatIdentity, current_durable_format_manifest,
};

mod administration;
mod application;
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
mod transient;
mod validated_prefix;

pub use backup::{
    RedbOfflineBackup, RedbOfflineRestore, read_history_incarnation, stamp_history_incarnation,
};
pub use changelog::{
    DEFAULT_CHANGELOG_BUFFER_ADVANCEMENTS, RedbChangelogEmitter, RedbChangelogEmitterHandle,
    start_changelog_emitter, start_changelog_emitter_v2,
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
