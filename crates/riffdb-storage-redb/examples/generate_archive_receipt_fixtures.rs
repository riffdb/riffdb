#![forbid(unsafe_code)]
//! Generate V3 receipt compatibility bytes through the production ledger.
use riffdb_storage_api::*;
use riffdb_storage_redb::RedbMaintenanceStorage;
use riffdb_types::*;
use std::{env, fs, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let check = env::args().skip(1).any(|arg| arg == "--check");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let temporary = tempfile::TempDir::new()?;
    let backups = temporary.path().join("backups");
    let (mut ledger, _) = RedbMaintenanceStorage::open(temporary.path().join("db.redb"), &backups)?;
    let backup = BackupNameV1::new("before")?;
    let archive = ArchiveNameV1::new("daily")?;
    let stop = ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(2).unwrap());
    let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
    let id = OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [11; 10])?;
    let mut receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
        id,
        backup.clone(),
        archive.clone(),
        stop,
        archive_restore_input_hash(&backup, &archive, stop, confirmation),
        confirmation,
        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator")?,
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1000, [7; 10])?,
            None,
        ),
        None,
    )?;
    ledger.create_or_read_archive_receipt(&receipt)?;
    let path = backups
        .join(".maintenance/receipts")
        .join(format!("{id}.receipt-v3"));
    fixture(root, "accepted", &fs::read(&path)?, check)?;
    let hex = fs::read_to_string(
        root.join("fixtures/replication/archive-manifest-v1-unencrypted-3.hex"),
    )?;
    let bytes = hex
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    let manifest = ArchiveManifestV1::decode(&bytes)?;
    let selection = ArchiveRestoreSelectionV3::new(
        OfflineBackupManifestIdentityV1::new(
            BackupIntegrityChecksumV1::new(manifest.full_backup_manifest_digest().to_vec())?,
            manifest.lineage().database_id(),
            None,
        ),
        manifest.lineage(),
        manifest.backup_fence(),
        ArchiveRestoreSuffixV3::Terminal(Box::new(manifest)),
    )?;
    receipt.record_selection(selection)?;
    ledger.replace_archive_receipt(&receipt)?;
    fixture(root, "selected", &fs::read(path)?, check)?;
    Ok(())
}
fn fixture(
    root: &Path,
    phase: &str,
    bytes: &[u8],
    check: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = format!("fixtures/compatibility/offline-maintenance-archive-{phase}-receipt-v3.hex");
    let text = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>() + "\n";
    if check {
        if fs::read(root.join(&name))? != text.as_bytes() {
            return Err(format!("stale fixture: {name}").into());
        }
    } else {
        fs::write(root.join(name), text)?;
    }
    Ok(())
}
