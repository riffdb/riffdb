#![cfg(test)]

//! Frozen predecessor-engine source and backup compatibility.

use super::*;
use std::io::{Seek, SeekFrom};

// req: REC-001, REC-002, REC-005
#[test]
fn frozen_engine_42_native_fixture_generation_child() {
    let Some(destination) = std::env::var_os("RIFFDB_ENGINE42_FIXTURE_DESTINATION") else {
        return;
    };
    assert!(include_str!("../../Cargo.toml").contains("version = \"=4.2.0\""));
    let source = TestDatabasePath::new("engine42-frozen-source");
    certify_clean_close(deployed_migration_store(&source, database_id(0x69)));
    let destination = PathBuf::from(destination);
    std::fs::create_dir(&destination).unwrap();
    let build = riffdb_storage_api::BackupBuildMetadataV1::new(
        "0.1.0",
        "engine42-adr0269",
        "rustc-test",
        1,
        Vec::new(),
    )
    .unwrap();
    riffdb_storage_api::OfflineBackupPersistencePort::create_offline_backup(
        &mut crate::backup::RedbOfflineBackup::bind(&source.0, destination.join("backup")),
        &build,
    )
    .unwrap();
    for entry in std::fs::read_dir(source.0.parent().unwrap()).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_type().unwrap().is_file());
        std::fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
    }
}

fn restore_frozen_fixture(root: &Path) {
    const FIXTURE: &str =
        include_str!("../../../../fixtures/storage/redb-4.2.0-native-source-and-backup.sparsehex");
    const NAMES: [&str; 7] = [
        "backup/database.redb",
        "backup/format.riffdb",
        "backup/journal.riffextent",
        "backup/manifest.riffdb",
        "db.redb",
        "db.redb.riffdb-format-v1",
        "db.redb.riffjournal",
    ];
    restore_fixture_bytes(root, FIXTURE, &NAMES);
}

pub(super) fn restore_fixture_bytes(root: &Path, fixture: &str, names: &[&str]) {
    std::fs::create_dir(root.join("backup")).unwrap();
    let mut file: Option<(std::fs::File, PathBuf, u64, String)> = None;
    let mut seen = BTreeSet::new();
    for line in fixture.lines().filter(|line| !line.starts_with('#')) {
        assert!(line.len() <= 1024);
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        match fields.as_slice() {
            ["riffdb-test-sparse-v1"] => (),
            ["file", name, length, checksum] => {
                assert!(file.is_none() && names.contains(name) && seen.insert(*name));
                let length = length.parse::<u64>().unwrap();
                assert!(length <= 48 * 1024 * 1024);
                let path = root.join(name);
                let opened = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .unwrap();
                opened.set_len(length).unwrap();
                file = Some((opened, path, length, (*checksum).to_owned()));
            }
            ["span", offset, hex] => {
                let (opened, _, length, _) = file.as_mut().unwrap();
                let offset = offset.parse::<u64>().unwrap();
                assert!(hex.len() <= 256 && hex.len() % 2 == 0);
                let bytes: Vec<_> = hex
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                    .collect();
                assert!(offset.checked_add(bytes.len() as u64).unwrap() <= *length);
                opened.seek(SeekFrom::Start(offset)).unwrap();
                opened.write_all(&bytes).unwrap();
            }
            ["end"] => {
                let (opened, path, _, checksum) = file.take().unwrap();
                opened.sync_all().unwrap();
                drop(opened);
                let actual = crate::backup::sha256_file(&path).unwrap();
                let actual: String = actual
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                assert_eq!(actual, checksum, "predecessor fixture byte identity");
            }
            ["copy", name, from] => {
                assert!(
                    file.is_none()
                        && names.contains(name)
                        && seen.contains(from)
                        && seen.insert(*name)
                );
                std::fs::copy(root.join(from), root.join(name)).unwrap();
            }
            _ => panic!("unknown frozen fixture record"),
        }
    }
    assert!(file.is_none());
    assert_eq!(seen.len(), names.len());
}

// req: REC-001, REC-002, REC-005, REC-006
#[test]
fn genuine_engine_42_source_and_backup_reopen_with_the_complete_native_validator() {
    use riffdb_storage_api::{
        OfflineRestoreOverwritePolicyV1, OfflineRestorePersistencePort, OfflineRestoreResultV1,
    };
    let directory = crate::test_path::ScopedDirectory::new("engine42-native-compatibility");
    restore_frozen_fixture(&directory.join(""));
    let source = directory.join("db.redb");
    RedbOfflineIntegrityScrub::from_inputs(&source, inputs())
        .run()
        .unwrap();
    let validate = |path: &Path| {
        let store = RedbStore::open(path).unwrap();
        let mut session = store.begin_structural_evidence(inputs()).unwrap();
        assert_eq!(session.database_id(), database_id(0x69));
        let structural_end = finish_structural(&mut session);
        let (catalog, historical_end) =
            validate_catalog_history(&mut session).unwrap().into_parts();
        assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
        assert!(matches!(
            session.finish(structural_end, historical_end).unwrap(),
            StructuralOpenOutcome::Clean(_)
        ));
    };
    validate(&source);
    validate(&source);
    let target = directory.join("restored");
    let result = crate::backup::RedbOfflineRestore::bind(directory.join("backup"), &target)
        .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
        .unwrap();
    assert!(matches!(result, OfflineRestoreResultV1::Restored { .. }));
    validate(&target.join("database.redb"));
}

// req: REC-001, REC-002, REC-005, REC-006, REC-007, REC-008
#[test]
fn genuine_engine_42_commands_preserve_exact_outcomes_through_current_open_scrub_and_restore() {
    use riffdb_storage_api::{
        AuthoritativePointReader, OfflineRestoreOverwritePolicyV1, OfflineRestorePersistencePort,
    };
    let directory = crate::test_path::ScopedDirectory::new("engine42-command-compatibility");
    restore_fixture_bytes(
        &directory.join(""),
        include_str!(
            "../../../../fixtures/storage/redb-4.2.0-native-commands-and-backup.sparsehex"
        ),
        &[
            "backup/database.redb",
            "backup/format.riffdb",
            "backup/journal.riffextent",
            "backup/manifest.riffdb",
            "db.redb",
            "db.redb.riffdb-format-v1",
            "expected-outcomes.hex",
        ],
    );
    let source = directory.join("db.redb");
    assert!(
        !crate::journal::journal_path(&source).exists(),
        "genuine Hardened source has no journal suffix"
    );
    let expected = {
        let text = std::fs::read_to_string(directory.join("expected-outcomes.hex")).unwrap();
        assert!(text.len() <= 64 * 1024);
        let lines: Vec<_> = text.lines().take(3).collect();
        assert_eq!(lines.len(), 2, "two actual predecessor terminal outcomes");
        lines
            .into_iter()
            .map(|line| {
                assert!(line.len() <= 32 * 1024 && line.len() % 2 == 0);
                let bytes: Vec<_> = line
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                    .collect();
                (
                    codec::decode_stored_outcome_v1(&bytes)
                        .unwrap()
                        .into_parts()
                        .0,
                    bytes,
                )
            })
            .collect::<Vec<_>>()
    };
    let validate = |path: &Path| {
        RedbOfflineIntegrityScrub::from_inputs(path, inputs())
            .run()
            .unwrap();
        let store = RedbStore::open(path).unwrap();
        let mut session = store.begin_structural_evidence(inputs()).unwrap();
        assert_eq!(session.database_id(), database_id(0x71));
        let structural_end = finish_structural(&mut session);
        let (catalog, historical_end) =
            validate_catalog_history(&mut session).unwrap().into_parts();
        assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
        let StructuralOpenOutcome::Clean(opened) =
            session.finish(structural_end, historical_end).unwrap()
        else {
            panic!("predecessor command graph must validate completely");
        };
        let (_, _, _, dormant) = opened.into_parts();
        let ports = dormant.into_operational_after_catalog_validation().unwrap();
        for (outcome, bytes) in &expected {
            let actual = ports
                .read_stored_outcome(outcome.identity())
                .unwrap()
                .unwrap();
            assert_eq!(&actual, outcome);
            assert_eq!(
                codec::encode_stored_outcome_v1(&actual).unwrap().as_bytes(),
                bytes
            );
        }
    };
    validate(&source);
    validate(&source);
    let target = directory.join("restored");
    crate::backup::RedbOfflineRestore::bind(directory.join("backup"), &target)
        .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
        .unwrap();
    validate(&target.join("database.redb"));
}
