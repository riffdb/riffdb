//! Immutable offline-maintenance submissions for uncertainty-safe retry.

use std::fmt;

use riffdb_proto::v1;
use riffdb_types::{
    BackupNameV1, ContractMigrationOperationId, MigrationBundleHash, OfflineMaintenanceOperationId,
    OfflineMaintenanceReplacementConfirmation, RequestId,
};

const MAX_CANDIDATE_BUNDLE_BYTES: usize = 15 * 1_024 * 1_024;
const MAX_MIGRATION_BUNDLE_BYTES: usize = 16 * 1_024 * 1_024;

/// A safe local failure while constructing immutable migration input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractMigrationSubmissionError {
    /// The candidate bundle is empty or exceeds the public request limit.
    InvalidCandidateBundleSize,
    /// The migration bundle is empty or exceeds the public request limit.
    InvalidMigrationBundleSize,
}

impl fmt::Display for ContractMigrationSubmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCandidateBundleSize => "candidate bundle size is outside public limits",
            Self::InvalidMigrationBundleSize => "migration bundle size is outside public limits",
        })
    }
}

impl std::error::Error for ContractMigrationSubmissionError {}

#[derive(Clone)]
struct ContractMigrationArtifacts {
    candidate_bundle: Vec<u8>,
    migration_bundle: Vec<u8>,
}

impl ContractMigrationArtifacts {
    fn new(
        candidate_bundle: Vec<u8>,
        migration_bundle: Vec<u8>,
    ) -> Result<Self, ContractMigrationSubmissionError> {
        if candidate_bundle.is_empty() || candidate_bundle.len() > MAX_CANDIDATE_BUNDLE_BYTES {
            return Err(ContractMigrationSubmissionError::InvalidCandidateBundleSize);
        }
        if migration_bundle.is_empty() || migration_bundle.len() > MAX_MIGRATION_BUNDLE_BYTES {
            return Err(ContractMigrationSubmissionError::InvalidMigrationBundleSize);
        }
        Ok(Self {
            candidate_bundle,
            migration_bundle,
        })
    }
}

/// One immutable read-only migration check with caller-stable identity.
#[derive(Clone)]
pub struct CheckContractMigration {
    operation_id: ContractMigrationOperationId,
    artifacts: ContractMigrationArtifacts,
}

impl CheckContractMigration {
    /// Checks public artifact bounds and retains exact canonical bytes for retries.
    pub fn new(
        operation_id: ContractMigrationOperationId,
        candidate_bundle: Vec<u8>,
        migration_bundle: Vec<u8>,
    ) -> Result<Self, ContractMigrationSubmissionError> {
        Ok(Self {
            operation_id,
            artifacts: ContractMigrationArtifacts::new(candidate_bundle, migration_bundle)?,
        })
    }

    /// Returns the caller-stable migration operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ContractMigrationOperationId {
        self.operation_id
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::CheckContractMigrationRequest {
        v1::CheckContractMigrationRequest {
            request_id: request_id.into_bytes().to_vec(),
            operation_id: self.operation_id.into_bytes().to_vec(),
            candidate_bundle: self.artifacts.candidate_bundle.clone(),
            migration_bundle: self.artifacts.migration_bundle.clone(),
        }
    }
}

impl fmt::Debug for CheckContractMigration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CheckContractMigration([REDACTED_ARTIFACTS])")
    }
}

/// One immutable confirmed migration apply with caller-stable identity.
#[derive(Clone)]
pub struct ApplyContractMigration {
    operation_id: ContractMigrationOperationId,
    artifacts: ContractMigrationArtifacts,
    confirmed_migration_hash: MigrationBundleHash,
}

impl ApplyContractMigration {
    /// Checks public artifact bounds and retains exact confirmed input for retries.
    pub fn new(
        operation_id: ContractMigrationOperationId,
        candidate_bundle: Vec<u8>,
        migration_bundle: Vec<u8>,
        confirmed_migration_hash: MigrationBundleHash,
    ) -> Result<Self, ContractMigrationSubmissionError> {
        Ok(Self {
            operation_id,
            artifacts: ContractMigrationArtifacts::new(candidate_bundle, migration_bundle)?,
            confirmed_migration_hash,
        })
    }

    /// Returns the caller-stable migration operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ContractMigrationOperationId {
        self.operation_id
    }

    /// Returns the exact operator-confirmed migration bundle identity.
    #[must_use]
    pub const fn confirmed_migration_hash(&self) -> MigrationBundleHash {
        self.confirmed_migration_hash
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::ApplyContractMigrationRequest {
        v1::ApplyContractMigrationRequest {
            request_id: request_id.into_bytes().to_vec(),
            operation_id: self.operation_id.into_bytes().to_vec(),
            candidate_bundle: self.artifacts.candidate_bundle.clone(),
            migration_bundle: self.artifacts.migration_bundle.clone(),
            confirmation: v1::ContractMigrationApplyConfirmation::AllowApplyContractMigration
                as i32,
            confirmed_migration_hash: self.confirmed_migration_hash.into_bytes().to_vec(),
        }
    }
}

impl fmt::Debug for ApplyContractMigration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplyContractMigration([REDACTED_ARTIFACTS])")
    }
}

/// One checked immutable offline-backup creation.
pub struct CreateOfflineBackup {
    operation_id: OfflineMaintenanceOperationId,
    backup_name: BackupNameV1,
}

impl CreateOfflineBackup {
    /// Creates a backup submission with caller-stable semantic identity.
    #[must_use]
    pub const fn new(
        operation_id: OfflineMaintenanceOperationId,
        backup_name: BackupNameV1,
    ) -> Self {
        Self {
            operation_id,
            backup_name,
        }
    }

    /// Returns the caller-stable maintenance operation ID.
    #[must_use]
    pub const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        self.operation_id
    }

    /// Borrows the checked backup name.
    #[must_use]
    pub const fn backup_name(&self) -> &BackupNameV1 {
        &self.backup_name
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::CreateOfflineBackupRequest {
        v1::CreateOfflineBackupRequest {
            request_id: request_id.into_bytes().to_vec(),
            operation_id: self.operation_id.into_bytes().to_vec(),
            backup_name: self.backup_name.as_str().to_owned(),
        }
    }
}

impl fmt::Debug for CreateOfflineBackup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateOfflineBackup")
            .field("operation_id", &self.operation_id)
            .field("backup_name", &self.backup_name)
            .finish()
    }
}

/// One checked immutable offline-backup restore.
pub struct RestoreOfflineBackup {
    operation_id: OfflineMaintenanceOperationId,
    backup_name: BackupNameV1,
    confirmation: OfflineMaintenanceReplacementConfirmation,
}

impl RestoreOfflineBackup {
    /// Creates a restore submission with caller-stable semantic identity.
    #[must_use]
    pub const fn new(
        operation_id: OfflineMaintenanceOperationId,
        backup_name: BackupNameV1,
        confirmation: OfflineMaintenanceReplacementConfirmation,
    ) -> Self {
        Self {
            operation_id,
            backup_name,
            confirmation,
        }
    }

    /// Returns the caller-stable maintenance operation ID.
    #[must_use]
    pub const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        self.operation_id
    }

    /// Borrows the checked backup name.
    #[must_use]
    pub const fn backup_name(&self) -> &BackupNameV1 {
        &self.backup_name
    }

    /// Returns the exact destructive-replacement confirmation.
    #[must_use]
    pub const fn confirmation(&self) -> OfflineMaintenanceReplacementConfirmation {
        self.confirmation
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::RestoreOfflineBackupRequest {
        let replacement_confirmation = match self.confirmation {
            OfflineMaintenanceReplacementConfirmation::NotProvided => {
                v1::OfflineMaintenanceReplacementConfirmation::Unspecified
            }
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget => {
                v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget
            }
        };
        v1::RestoreOfflineBackupRequest {
            request_id: request_id.into_bytes().to_vec(),
            operation_id: self.operation_id.into_bytes().to_vec(),
            backup_name: self.backup_name.as_str().to_owned(),
            replacement_confirmation: replacement_confirmation as i32,
        }
    }
}

impl fmt::Debug for RestoreOfflineBackup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestoreOfflineBackup")
            .field("operation_id", &self.operation_id)
            .field("backup_name", &self.backup_name)
            .field("confirmation", &self.confirmation)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_id(timestamp: u64) -> RequestId {
        RequestId::from_unix_milliseconds_and_random(timestamp, [1; 10]).expect("request ID")
    }

    fn operation_id() -> OfflineMaintenanceOperationId {
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [2; 10])
            .expect("operation ID")
    }

    fn migration_operation_id() -> ContractMigrationOperationId {
        ContractMigrationOperationId::from_unix_milliseconds_and_random(1, [3; 10])
            .expect("operation ID")
    }

    #[test]
    fn retries_change_only_the_transport_request_id() {
        let create =
            CreateOfflineBackup::new(operation_id(), BackupNameV1::new("stable").expect("name"));
        let first = create.request(request_id(2));
        let second = create.request(request_id(3));
        assert_ne!(first.request_id, second.request_id);
        assert_eq!(first.operation_id, second.operation_id);
        assert_eq!(first.backup_name, second.backup_name);

        let restore = RestoreOfflineBackup::new(
            operation_id(),
            BackupNameV1::new("stable").expect("name"),
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
        );
        let first = restore.request(request_id(4));
        let second = restore.request(request_id(5));
        assert_ne!(first.request_id, second.request_id);
        assert_eq!(first.operation_id, second.operation_id);
        assert_eq!(first.backup_name, second.backup_name);
        assert_eq!(
            first.replacement_confirmation,
            second.replacement_confirmation
        );
    }

    #[test]
    fn migration_retries_change_only_transport_identity() {
        let check =
            CheckContractMigration::new(migration_operation_id(), vec![1], vec![2]).expect("check");
        let first = check.request(request_id(2));
        let second = check.request(request_id(3));
        assert_ne!(first.request_id, second.request_id);
        assert_eq!(first.operation_id, second.operation_id);
        assert_eq!(first.candidate_bundle, second.candidate_bundle);
        assert_eq!(first.migration_bundle, second.migration_bundle);

        let apply = ApplyContractMigration::new(
            migration_operation_id(),
            vec![1],
            vec![2],
            MigrationBundleHash::from_bytes([9; 32]),
        )
        .expect("apply");
        let first = apply.request(request_id(4));
        let second = apply.request(request_id(5));
        assert_ne!(first.request_id, second.request_id);
        assert_eq!(first.operation_id, second.operation_id);
        assert_eq!(first.candidate_bundle, second.candidate_bundle);
        assert_eq!(first.migration_bundle, second.migration_bundle);
        assert_eq!(
            first.confirmed_migration_hash,
            second.confirmed_migration_hash
        );
    }
}
