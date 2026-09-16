//! Paired collector measurements reuse one stopped full-backup setup in both arms.
use std::{fs, io::Read, path::Path};

use riffdb_storage_api::{
    ArchiveEncryptionPostureV1, BackupBuildMetadataV1, OfflineBackupPersistencePort,
};
use riffdb_storage_redb::{RedbOfflineBackup, RedbVerifiedArchiveBackup};

use crate::RiffDbError;

/// Archive activity verified after the measured daemon has closed its collector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveCollectionEvidence {
    /// Whether the automatic collector was configured in this generation.
    pub enabled: bool,
    /// Application frontier of the identical full-backup preparation in both arms.
    pub backup_application_sequence: Option<u64>,
    /// Last confirmed archived application frontier; absent in the disabled arm.
    pub archived_application_sequence: Option<u64>,
    /// Number of confirmed history transactions after the full backup.
    pub archived_history_transactions: u64,
    /// Exact terminal manifest identity, present only in the enabled arm.
    pub manifest_digest: Option<[u8; 32]>,
}
fn error(detail: &str) -> RiffDbError {
    RiffDbError::Server {
        detail: detail.into(),
    }
}
fn parse_mode(value: &str) -> Result<bool, RiffDbError> {
    match value {
        "enabled" => Ok(true),
        "disabled" => Ok(false),
        _ => Err(error(
            "RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION must be enabled or disabled",
        )),
    }
}
pub(super) fn requested_mode() -> Result<Option<bool>, RiffDbError> {
    match std::env::var("RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION") {
        Ok(value) => parse_mode(&value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(error("invalid archive measurement mode")),
    }
}

/// Called only after clean daemon shutdown, outside the measured interval.
pub(super) fn prepare(root: &Path, enabled: bool) -> Result<(), RiffDbError> {
    let backup = root.join("backups/measurement");
    if !backup.exists() {
        let build = BackupBuildMetadataV1::new(
            env!("CARGO_PKG_VERSION"),
            "app-baseline-archive-measurement",
            "rust",
            1,
            vec![],
        )
        .map_err(|_| error("archive measurement build metadata"))?;
        RedbOfflineBackup::bind(root.join("riffdb.redb"), &backup)
            .create_offline_backup(&build)
            .map_err(|_| error("archive measurement full backup failed"))?;
        // The harness owns this bounded, generated projection document.
        let config = root.join("projections.toml");
        let mut original = String::new();
        fs::File::open(&config)
            .map_err(|_| RiffDbError::Io)?
            .take(64 * 1024 + 1)
            .read_to_string(&mut original)
            .map_err(|_| RiffDbError::Io)?;
        if original.len() > 64 * 1024 {
            return Err(error("archive measurement configuration too large"));
        }
        let collection = if enabled {
            "backup = 'measurement'\n"
        } else {
            ""
        };
        fs::write(config, format!("{original}\n[[maintenance.archives]]\nname = 'measurement'\npath = {:?}\nencryption = 'unencrypted'\n{collection}", root.join("archive")))
            .map_err(|_| RiffDbError::Io)?;
    }
    RedbVerifiedArchiveBackup::open(&backup)
        .map_err(|_| error("archive measurement backup validation failed"))?;
    Ok(())
}

pub(super) fn observe(
    root: &Path,
    enabled: bool,
) -> Result<ArchiveCollectionEvidence, RiffDbError> {
    let backup = RedbVerifiedArchiveBackup::open(&root.join("backups/measurement"))
        .map_err(|_| error("archive measurement backup missing or invalid"))?;
    let before = backup.history().tail();
    let mut evidence = ArchiveCollectionEvidence {
        enabled,
        backup_application_sequence: before.frontier().application().map(|value| value.get()),
        archived_application_sequence: None,
        archived_history_transactions: 0,
        manifest_digest: None,
    };
    if !enabled {
        if root.join("archive").exists() {
            return Err(error(
                "disabled archive measurement unexpectedly created a sink",
            ));
        }
        return Ok(evidence);
    }
    // Do not create a missing sink while trying to prove that collection happened.
    if !root.join("archive/CURRENT").is_file() {
        return Err(error(
            "enabled archive measurement has no confirmed selector",
        ));
    }
    let archive = backup
        .open_archive(
            &root.join("archive"),
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .map_err(|_| error("archive measurement inventory validation failed"))?;
    let after = archive.position();
    if after.frontier().application() <= before.frontier().application() {
        return Err(error(
            "enabled archive measurement collected no application writes",
        ));
    }
    evidence.archived_application_sequence =
        after.frontier().application().map(|value| value.get());
    evidence.archived_history_transactions = after
        .sequence()
        .get()
        .checked_sub(before.sequence().get())
        .ok_or_else(|| error("archive measurement frontier regressed"))?;
    evidence.manifest_digest = Some(
        archive
            .head()
            .ok_or_else(|| error("archive measurement terminal manifest missing"))?
            .digest(),
    );
    Ok(evidence)
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: REP-007
    #[test]
    fn archive_measurement_mode_refuses_implicit_or_unknown_enablement() {
        assert!(parse_mode("enabled").unwrap());
        assert!(!parse_mode("disabled").unwrap());
        for invalid in ["", "1", "0", "true", "false", "auto", "Enabled"] {
            assert!(parse_mode(invalid).is_err());
        }
    }
}
