//! Immutable offline-maintenance submissions for uncertainty-safe retry.

use std::fmt;

use riffdb_proto::v1;
use riffdb_types::{
    BackupNameV1, OfflineMaintenanceOperationId, OfflineMaintenanceReplacementConfirmation,
    RequestId,
};

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
}
