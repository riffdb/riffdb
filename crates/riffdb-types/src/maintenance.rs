//! Checked identities and canonical input for offline maintenance.

use std::error::Error;
use std::fmt;

use crate::{OfflineMaintenanceInputHash, hash_offline_maintenance_input};

/// Maximum bytes in one public backup name.
pub const MAX_BACKUP_NAME_V1_BYTES: usize = 64;

/// A checked immutable backup name beneath the server-owned backup root.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackupNameV1(String);

impl BackupNameV1 {
    /// Validates the exact v1 backup-name grammar without normalization.
    pub fn new(value: impl Into<String>) -> Result<Self, BackupNameV1Error> {
        let value = value.into();
        validate_backup_name_v1(&value)?;
        Ok(Self(value))
    }

    /// Borrows the exact ASCII backup name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrows the exact ASCII identity bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for BackupNameV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A safe backup-name validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackupNameV1Error {
    /// The name is empty.
    Empty,
    /// The name exceeds the v1 byte limit.
    TooLong,
    /// The first byte is not lowercase ASCII or a digit.
    InvalidFirstByte,
    /// A later byte is outside lowercase ASCII, digits, `-`, and `_`.
    InvalidByte {
        /// The zero-based byte offset.
        index: usize,
    },
}

impl fmt::Display for BackupNameV1Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "backup name must not be empty",
            Self::TooLong => "backup name exceeds 64 bytes",
            Self::InvalidFirstByte => {
                "backup name must start with a lowercase ASCII letter or digit"
            }
            Self::InvalidByte { .. } => "backup name contains an invalid byte",
        })
    }
}

impl Error for BackupNameV1Error {}

fn validate_backup_name_v1(value: &str) -> Result<(), BackupNameV1Error> {
    let bytes = value.as_bytes();
    let Some(first) = bytes.first().copied() else {
        return Err(BackupNameV1Error::Empty);
    };
    if bytes.len() > MAX_BACKUP_NAME_V1_BYTES {
        return Err(BackupNameV1Error::TooLong);
    }
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(BackupNameV1Error::InvalidFirstByte);
    }
    if let Some(index) = bytes.iter().enumerate().skip(1).find_map(|(index, byte)| {
        (!byte.is_ascii_lowercase() && !byte.is_ascii_digit() && !matches!(byte, b'-' | b'_'))
            .then_some(index)
    }) {
        return Err(BackupNameV1Error::InvalidByte { index });
    }
    Ok(())
}

/// The closed semantic kind of one offline maintenance operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OfflineMaintenanceOperationKind {
    /// Publish one immutable offline backup.
    CreateBackup,
    /// Restore one immutable backup through private staging.
    RestoreBackup,
}

impl OfflineMaintenanceOperationKind {
    const fn tag(self) -> u8 {
        match self {
            Self::CreateBackup => 1,
            Self::RestoreBackup => 2,
        }
    }
}

/// The caller's exact destructive-replacement confirmation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OfflineMaintenanceReplacementConfirmation {
    /// No destructive-replacement confirmation was supplied.
    NotProvided,
    /// The caller supplied `ALLOW_REPLACE_NONEMPTY_TARGET`.
    AllowReplaceNonemptyTarget,
}

impl OfflineMaintenanceReplacementConfirmation {
    const fn tag(self) -> u8 {
        match self {
            Self::NotProvided => 0,
            Self::AllowReplaceNonemptyTarget => 1,
        }
    }
}

/// Computes the stable v1 semantic-input identity for one maintenance request.
#[must_use]
pub fn offline_maintenance_input_hash(
    kind: OfflineMaintenanceOperationKind,
    backup_name: &BackupNameV1,
    confirmation: OfflineMaintenanceReplacementConfirmation,
) -> OfflineMaintenanceInputHash {
    let name = backup_name.as_bytes();
    let mut canonical = Vec::with_capacity(4 + name.len());
    canonical.push(1);
    canonical.push(kind.tag());
    canonical.push(u8::try_from(name.len()).expect("backup-name length is bounded to 64"));
    canonical.extend_from_slice(name);
    canonical.push(confirmation.tag());
    hash_offline_maintenance_input(&canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_name_v1_accepts_only_the_exact_ascii_grammar() {
        for accepted in ["a", "0", "before-upgrade", "backup_2026"] {
            assert_eq!(
                BackupNameV1::new(accepted).expect("accepted").as_str(),
                accepted
            );
        }
        for rejected in [
            "",
            "-backup",
            "_backup",
            ".maintenance",
            "Upper",
            "a.b",
            "a/b",
            "a\\b",
            "c:backup",
            "backup space",
            "café",
        ] {
            assert!(BackupNameV1::new(rejected).is_err(), "{rejected:?}");
        }
        assert!(BackupNameV1::new("a".repeat(64)).is_ok());
        assert_eq!(
            BackupNameV1::new("a".repeat(65)),
            Err(BackupNameV1Error::TooLong)
        );
    }

    #[test]
    fn semantic_input_hash_binds_kind_name_and_confirmation() {
        let name = BackupNameV1::new("before-upgrade").expect("name");
        let baseline = offline_maintenance_input_hash(
            OfflineMaintenanceOperationKind::CreateBackup,
            &name,
            OfflineMaintenanceReplacementConfirmation::NotProvided,
        );
        assert_eq!(
            baseline.into_bytes(),
            [
                0xb1, 0x5c, 0x8c, 0x4a, 0x4f, 0x1e, 0xbb, 0x53, 0x9a, 0xeb, 0x86, 0xef, 0x0e, 0x60,
                0x30, 0x53, 0x03, 0xa1, 0xb8, 0xff, 0xd8, 0x6b, 0xf4, 0xef, 0xfc, 0xfa, 0xc7, 0xe5,
                0x9c, 0xf9, 0xfc, 0x31,
            ]
        );
        assert_eq!(
            baseline,
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::CreateBackup,
                &name,
                OfflineMaintenanceReplacementConfirmation::NotProvided,
            )
        );
        assert_ne!(
            baseline,
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::RestoreBackup,
                &name,
                OfflineMaintenanceReplacementConfirmation::NotProvided,
            )
        );
        assert_ne!(
            baseline,
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::CreateBackup,
                &name,
                OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
            )
        );
    }
}
