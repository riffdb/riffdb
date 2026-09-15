//! Checked identities and canonical input for offline maintenance.

use std::error::Error;
use std::fmt;

use crate::{
    CommitSequence, ContractBundleHash, ContractMigrationInputHash, MigrationBundleHash,
    OfflineMaintenanceInputHash, hash_contract_migration_input, hash_offline_maintenance_input,
};

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

/// Maximum bytes in one configured archive name.
pub const MAX_ARCHIVE_NAME_V1_BYTES: usize = MAX_BACKUP_NAME_V1_BYTES;

/// A checked operator-configured archive name, never a filesystem path or URI.
///
/// Its distinct type prevents a backup identity from silently selecting an
/// archive. The accepted grammar is exactly the bounded backup-name grammar.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArchiveNameV1(BackupNameV1);

impl ArchiveNameV1 {
    /// Checks the exact ASCII grammar without normalization or path resolution.
    pub fn new(value: impl Into<String>) -> Result<Self, ArchiveNameV1Error> {
        BackupNameV1::new(value)
            .map(Self)
            .map_err(ArchiveNameV1Error)
    }

    /// Borrows the exact configured archive name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Borrows the canonical ASCII name bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for ArchiveNameV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A bounded archive-name validation failure that retains no rejected input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveNameV1Error(BackupNameV1Error);

impl fmt::Display for ArchiveNameV1Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.0 {
            BackupNameV1Error::Empty => "archive name must not be empty",
            BackupNameV1Error::TooLong => "archive name exceeds 64 bytes",
            BackupNameV1Error::InvalidFirstByte => {
                "archive name must start with a lowercase ASCII letter or digit"
            }
            BackupNameV1Error::InvalidByte { .. } => "archive name contains an invalid byte",
        })
    }
}

impl Error for ArchiveNameV1Error {}

/// Exact application-sequence selection for an offline archive restore.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ArchiveRestoreStopV1 {
    /// Resolve once to a verified terminal archive selection before replay.
    LastArchived,
    /// Restore exactly this application sequence, without rounding to a frame.
    AtApplicationSequence(CommitSequence),
}

/// Binds every archive restore input to a distinct canonical input domain.
///
/// Artifact selection is resolved and frozen separately in receipt V3. In
/// particular, `LastArchived` never hashes a moving archive head into a request
/// identity. A fresh transport request ID does not change this retry identity.
#[must_use]
pub fn archive_restore_input_hash(
    backup_name: &BackupNameV1,
    archive_name: &ArchiveNameV1,
    stop: ArchiveRestoreStopV1,
    confirmation: OfflineMaintenanceReplacementConfirmation,
) -> OfflineMaintenanceInputHash {
    let mut canonical = Vec::with_capacity(176);
    canonical.extend_from_slice(b"riffdb.archive-restore-input/v1\0");
    for name in [backup_name.as_bytes(), archive_name.as_bytes()] {
        canonical.push(u8::try_from(name.len()).expect("checked names have at most 64 bytes"));
        canonical.extend_from_slice(name);
    }
    canonical.push(confirmation.tag());
    match stop {
        ArchiveRestoreStopV1::LastArchived => canonical.push(0),
        ArchiveRestoreStopV1::AtApplicationSequence(sequence) => {
            canonical.push(1);
            canonical.extend_from_slice(&sequence.get().to_be_bytes());
        }
    }
    hash_offline_maintenance_input(&canonical)
}

/// The closed semantic kind of one offline maintenance operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OfflineMaintenanceOperationKind {
    /// Publish one immutable offline backup.
    CreateBackup,
    /// Restore one immutable backup through private staging.
    RestoreBackup,
    /// Permanently retire one previously published immutable backup.
    RetireBackup,
}

impl OfflineMaintenanceOperationKind {
    const fn tag(self) -> u8 {
        match self {
            Self::CreateBackup => 1,
            Self::RestoreBackup => 2,
            Self::RetireBackup => 3,
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

/// The closed semantic kind of one public contract-migration operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ContractMigrationOperationKind {
    /// Run the complete read-only preflight without draining or staging.
    Check,
    /// Run the complete offline staged migration lifecycle.
    Apply,
}

impl ContractMigrationOperationKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Check => 1,
            Self::Apply => 2,
        }
    }
}

/// The caller's exact destructive migration confirmation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ContractMigrationApplyConfirmation {
    /// No apply confirmation was supplied.
    NotProvided,
    /// The caller supplied `ALLOW_APPLY_CONTRACT_MIGRATION`.
    AllowApplyContractMigration,
}

impl ContractMigrationApplyConfirmation {
    const fn tag(self) -> u8 {
        match self {
            Self::NotProvided => 0,
            Self::AllowApplyContractMigration => 1,
        }
    }
}

/// Computes the stable V1 semantic-input identity for one migration request.
#[must_use]
pub fn contract_migration_input_hash(
    kind: ContractMigrationOperationKind,
    parent: ContractBundleHash,
    candidate: ContractBundleHash,
    migration: MigrationBundleHash,
    confirmation: ContractMigrationApplyConfirmation,
) -> ContractMigrationInputHash {
    let mut canonical = Vec::with_capacity(99);
    canonical.push(1);
    canonical.push(kind.tag());
    canonical.extend_from_slice(parent.as_bytes());
    canonical.extend_from_slice(candidate.as_bytes());
    canonical.extend_from_slice(migration.as_bytes());
    canonical.push(confirmation.tag());
    hash_contract_migration_input(&canonical)
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

    // req: REP-007
    #[test]
    fn archive_name_uses_the_exact_bounded_backup_grammar_without_becoming_a_path() {
        for accepted in ["a", "0", "daily-archive", "archive_2026"] {
            let name = ArchiveNameV1::new(accepted).expect("checked archive name");
            assert_eq!(name.as_str(), accepted);
            assert_eq!(name.as_bytes(), accepted.as_bytes());
        }
        for rejected in [
            "",
            "-archive",
            "_archive",
            ".maintenance",
            "Upper",
            "a.b",
            "a/b",
            "a\\b",
            "c:archive",
            "archive space",
            "café",
            "https://sink",
            "a\0b",
        ] {
            assert!(ArchiveNameV1::new(rejected).is_err(), "{rejected:?}");
        }
        assert!(ArchiveNameV1::new("a".repeat(64)).is_ok());
        assert!(ArchiveNameV1::new("a".repeat(65)).is_err());
        let error = ArchiveNameV1::new("secret/sink").unwrap_err();
        assert!(!error.to_string().contains("secret"));
        assert!(!format!("{error:?}").contains("secret"));
    }

    // req: REP-007
    #[test]
    fn archive_restore_retry_identity_binds_every_field_and_is_distinct_from_plain_restore() {
        use crate::CommitSequence;
        use ArchiveRestoreStopV1::{AtApplicationSequence, LastArchived};
        use OfflineMaintenanceReplacementConfirmation::{AllowReplaceNonemptyTarget, NotProvided};
        let backup = BackupNameV1::new("before-upgrade").unwrap();
        let archive = ArchiveNameV1::new("daily").unwrap();
        let baseline = archive_restore_input_hash(&backup, &archive, LastArchived, NotProvided);
        // Independent SHA-256 fixture for the ADR-0011 frame and exact input domain.
        assert_eq!(
            baseline.into_bytes(),
            [
                0x32, 0xfd, 0x15, 0x67, 0xf6, 0xdd, 0x2b, 0x4d, 0xc6, 0x4c, 0x78, 0xaa, 0x67, 0x54,
                0xfe, 0x7d, 0x79, 0x46, 0xe8, 0xc7, 0xb2, 0xfb, 0xbe, 0x24, 0xad, 0x7d, 0xb5, 0xb3,
                0xc7, 0x1b, 0x1f, 0x50,
            ]
        );
        assert_eq!(
            baseline,
            archive_restore_input_hash(&backup, &archive, LastArchived, NotProvided)
        );
        let variants = [
            archive_restore_input_hash(
                &BackupNameV1::new("other").unwrap(),
                &archive,
                LastArchived,
                NotProvided,
            ),
            archive_restore_input_hash(
                &backup,
                &ArchiveNameV1::new("other").unwrap(),
                LastArchived,
                NotProvided,
            ),
            archive_restore_input_hash(&backup, &archive, LastArchived, AllowReplaceNonemptyTarget),
            archive_restore_input_hash(
                &backup,
                &archive,
                AtApplicationSequence(CommitSequence::first()),
                NotProvided,
            ),
            archive_restore_input_hash(
                &backup,
                &archive,
                AtApplicationSequence(CommitSequence::new(u64::MAX).unwrap()),
                NotProvided,
            ),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::RestoreBackup,
                &backup,
                NotProvided,
            ),
        ];
        for (index, value) in variants.iter().enumerate() {
            assert_ne!(*value, baseline);
            assert!(!variants[..index].contains(value));
        }
        // Field boundaries are canonical even for names with ambiguous concatenation.
        assert_ne!(
            archive_restore_input_hash(
                &BackupNameV1::new("a").unwrap(),
                &ArchiveNameV1::new("bc").unwrap(),
                LastArchived,
                NotProvided
            ),
            archive_restore_input_hash(
                &BackupNameV1::new("ab").unwrap(),
                &ArchiveNameV1::new("c").unwrap(),
                LastArchived,
                NotProvided
            ),
        );
    }

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
                OfflineMaintenanceOperationKind::RetireBackup,
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

    #[test]
    fn migration_input_hash_binds_every_semantic_identity() {
        let parent = ContractBundleHash::from_bytes([1; 32]);
        let candidate = ContractBundleHash::from_bytes([2; 32]);
        let migration = MigrationBundleHash::from_bytes([3; 32]);
        let baseline = contract_migration_input_hash(
            ContractMigrationOperationKind::Check,
            parent,
            candidate,
            migration,
            ContractMigrationApplyConfirmation::NotProvided,
        );
        assert_eq!(
            baseline,
            contract_migration_input_hash(
                ContractMigrationOperationKind::Check,
                parent,
                candidate,
                migration,
                ContractMigrationApplyConfirmation::NotProvided,
            )
        );
        assert_ne!(
            baseline,
            contract_migration_input_hash(
                ContractMigrationOperationKind::Apply,
                parent,
                candidate,
                migration,
                ContractMigrationApplyConfirmation::AllowApplyContractMigration,
            )
        );
    }
}
