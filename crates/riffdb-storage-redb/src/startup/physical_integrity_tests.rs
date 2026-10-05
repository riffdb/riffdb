#![cfg(test)]
//! Physical engine damage independent of canonical record envelopes.

use super::*;

fn verification_directory(path: &Path) -> PathBuf {
    let mut name = std::ffi::OsString::from(".");
    name.push(path.file_name().unwrap());
    name.push(".riffdb-offline-verification-v1");
    path.with_file_name(name)
}

// req: REC-001, REC-002, REC-008
#[test]
fn offline_copy_custody_excludes_a_native_writer_in_another_process_until_released() {
    const CHILD_PATH: &str = "RIFFDB_OFFLINE_COPY_LOCK_PATH";
    const CHILD_EXPECTED: &str = "RIFFDB_OFFLINE_COPY_LOCK_EXPECTED";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let opened = RedbStore::open(Path::new(&path));
        match std::env::var(CHILD_EXPECTED).unwrap().as_str() {
            "held" => assert!(
                matches!(opened, Err(error) if error.kind() == StorageErrorKind::Unavailable)
            ),
            "released" => assert!(opened.is_ok()),
            _ => panic!("unknown ownership state"),
        }
        return;
    }
    let path = TestDatabasePath::new("scrub-copy-lock");
    certify_clean_close(deployed_migration_store(&path, database_id(0x72)));
    let primary = std::fs::read(&path.0).unwrap();
    let child = |expected| {
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "startup::tests::physical_integrity::offline_copy_custody_excludes_a_native_writer_in_another_process_until_released"])
            .env(CHILD_PATH, &path.0).env(CHILD_EXPECTED, expected).status().unwrap();
        assert!(status.success());
    };
    let copy =
        crate::maintenance::PhysicalScrubCopy::capture(&path.0, Arc::new(AtomicBool::new(false)))
            .unwrap();
    child("held");
    copy.finish().unwrap();
    assert_eq!(std::fs::read(&path.0).unwrap(), primary);
    child("released");
}

// req: REC-001, REC-002, REC-005, REC-008
#[test]
fn offline_scrub_refuses_damaged_existing_journal_without_rewriting_any_original() {
    let path = TestDatabasePath::new("scrub-damaged-journal");
    certify_clean_close(deployed_migration_store(&path, database_id(0x71)));
    let primary = std::fs::read(&path.0).unwrap();
    let journal = crate::journal::journal_path(&path.0);
    let mut damaged = std::fs::read(&journal).unwrap();
    damaged[0] ^= 1;
    std::fs::write(&journal, &damaged).unwrap();
    let result = RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs()).run();
    assert!(matches!(result, Err(error) if error.kind() == StorageErrorKind::CorruptData));
    assert_eq!(std::fs::read(&journal).unwrap(), damaged);
    assert!(!verification_directory(&path.0).exists());
    assert_eq!(std::fs::read(&path.0).unwrap(), primary);
}

// req: REC-001, REC-002, REC-006, REC-008
#[test]
fn offline_copy_refuses_capacity_growth_cancellation_and_replaced_original_binding() {
    use redb::StorageBackend;
    let path = TestDatabasePath::new("scrub-copy-binding");
    certify_clean_close(deployed_migration_store(&path, database_id(0x6f)));
    let primary = std::fs::read(&path.0).unwrap();
    let cancellation = Arc::new(AtomicBool::new(false));
    let copy =
        crate::maintenance::PhysicalScrubCopy::capture(&path.0, Arc::clone(&cancellation)).unwrap();
    let backend = copy.backend().unwrap();
    let length = backend.len().unwrap();
    assert_eq!(
        backend.set_len(length + 1).unwrap_err().kind(),
        std::io::ErrorKind::StorageFull
    );
    assert_eq!(
        backend.write(length, &[1]).unwrap_err().kind(),
        std::io::ErrorKind::StorageFull
    );
    assert_eq!(backend.len().unwrap(), length);
    cancellation.store(true, Ordering::Release);
    assert_eq!(
        backend.read(0, &mut [0; 9]).unwrap_err().kind(),
        std::io::ErrorKind::Interrupted
    );
    drop(backend);
    cancellation.store(false, Ordering::Release);
    let replaced = path.0.with_extension("replaced");
    std::fs::rename(&path.0, &replaced).unwrap();
    std::fs::write(&path.0, &primary).unwrap();
    assert!(matches!(copy.finish(), Err(error) if error.kind() == StorageErrorKind::CorruptData));
    assert_eq!(std::fs::read(&path.0).unwrap(), primary);
    assert_eq!(std::fs::read(&replaced).unwrap(), primary);
    assert!(!verification_directory(&path.0).exists());
}

// req: REC-001, REC-002, REC-008
#[cfg(unix)]
#[test]
fn offline_copy_preserves_unknown_scratch_and_refuses_original_hardlink_substitution() {
    use std::os::unix::fs::PermissionsExt;
    let path = TestDatabasePath::new("scrub-copy-unknown");
    certify_clean_close(deployed_migration_store(&path, database_id(0x70)));
    let primary = std::fs::read(&path.0).unwrap();
    let directory = verification_directory(&path.0);
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let unknown = directory.join("unknown");
    std::fs::write(&unknown, b"preserve").unwrap();
    assert!(
        RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs())
            .run()
            .is_err()
    );
    assert_eq!(std::fs::read(&unknown).unwrap(), b"preserve");
    std::fs::remove_file(&unknown).unwrap();
    let alias = directory.join("primary.redb");
    std::fs::hard_link(&path.0, &alias).unwrap();
    assert!(
        RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs())
            .run()
            .is_err()
    );
    assert_eq!(std::fs::read(&alias).unwrap(), primary);
    assert_eq!(std::fs::read(&path.0).unwrap(), primary);
    std::fs::remove_file(&alias).unwrap();
    std::fs::remove_dir(&directory).unwrap();
    RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs())
        .run()
        .unwrap();
}

// req: REC-001, REC-002, REC-006, REC-008
#[test]
fn offline_physical_and_logical_scrub_preserves_all_original_bytes_and_releases_copy() {
    let path = TestDatabasePath::new("scrub-physical-clean");
    certify_clean_close(deployed_migration_store(&path, database_id(0x6b)));
    let primary = std::fs::read(&path.0).unwrap();
    let authority = complete_authority_snapshot(&path.0);
    for _ in 0..2 {
        let receipt = RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs())
            .run()
            .unwrap();
        assert!(receipt.structural_exact_end() && receipt.catalog_exact_end());
        assert_eq!(receipt.authoritative_mutations(), 0);
        assert_eq!(std::fs::read(&path.0).unwrap(), primary);
        assert_authority_snapshot_unchanged(&authority, &complete_authority_snapshot(&path.0));
        assert!(!verification_directory(&path.0).exists());
    }
}

// req: REC-001, REC-002, REC-006, REC-008
#[test]
fn offline_scrub_refuses_engine_repair_and_preserves_original() {
    let path = TestDatabasePath::new("scrub-repair-refusal");
    certify_clean_close(deployed_migration_store(&path, database_id(0x6c)));
    let mut primary = std::fs::read(&path.0).unwrap();
    assert_eq!(&primary[..9], b"redb\x1a\x0a\xa9\x0d\x0a");
    assert_eq!(primary[9] & 2, 0, "clean engine recovery flag");
    // Require full allocator repair: the dirty flag alone may load a valid
    // persisted two-phase allocator without invoking the repair callback.
    primary[9] = (primary[9] | 2) & !4;
    std::fs::write(&path.0, &primary).unwrap();
    let result = RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs()).run();
    assert!(
        matches!(result, Err(ref error) if error.kind() == StorageErrorKind::CorruptData),
        "repair refusal: {result:?}"
    );
    assert_eq!(std::fs::read(&path.0).unwrap(), primary);
    assert!(!verification_directory(&path.0).exists());
}

// req: REC-001, REC-002, REC-008
#[test]
fn offline_scrub_cancellation_refuses_receipt_and_releases_private_capabilities() {
    let path = TestDatabasePath::new("scrub-physical-cancel");
    certify_clean_close(deployed_migration_store(&path, database_id(0x6d)));
    let primary = std::fs::read(&path.0).unwrap();
    let cancellation = Arc::new(AtomicBool::new(true));
    let result = RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs())
        .with_cancellation(Arc::clone(&cancellation))
        .run();
    assert!(matches!(result, Err(error) if error.kind() == StorageErrorKind::Unavailable));
    cancellation.store(false, Ordering::Release);
    let copy =
        crate::maintenance::PhysicalScrubCopy::capture(&path.0, Arc::clone(&cancellation)).unwrap();
    cancellation.store(true, Ordering::Release);
    assert!(matches!(copy.finish(), Err(error) if error.kind() == StorageErrorKind::Unavailable));
    assert_eq!(std::fs::read(&path.0).unwrap(), primary);
    assert!(!verification_directory(&path.0).exists());
    RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs())
        .run()
        .unwrap();
}

// req: REC-001, REC-002, REC-008
#[test]
fn offline_copy_process_exit_releases_custody_and_next_scrub_reclaims_owned_scratch() {
    const CHILD_PATH: &str = "RIFFDB_OFFLINE_COPY_EXIT_PATH";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let _copy = crate::maintenance::PhysicalScrubCopy::capture(
            Path::new(&path),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        std::process::exit(77);
    }
    let path = TestDatabasePath::new("scrub-copy-exit");
    certify_clean_close(deployed_migration_store(&path, database_id(0x6e)));
    let primary = std::fs::read(&path.0).unwrap();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "startup::tests::physical_integrity::offline_copy_process_exit_releases_custody_and_next_scrub_reclaims_owned_scratch"])
        .env(CHILD_PATH, &path.0).status().unwrap();
    assert_eq!(status.code(), Some(77));
    assert!(
        verification_directory(&path.0)
            .join("primary.redb")
            .exists()
    );
    assert_eq!(std::fs::read(&path.0).unwrap(), primary);
    RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs())
        .run()
        .unwrap();
    assert!(!verification_directory(&path.0).exists());
    assert_eq!(std::fs::read(&path.0).unwrap(), primary);
}

fn contract_bytes(path: &Path) -> Vec<Vec<u8>> {
    let database = redb::ReadOnlyDatabase::open(path).unwrap();
    let read = database.begin_read().unwrap();
    read.open_table(CONTRACT_BUNDLES)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| row.unwrap().1.value().to_vec())
        .collect()
}

// req: REC-001, REC-002, REC-006, REC-008
#[test]
fn offline_scrub_detects_engine_tree_damage_with_unchanged_valid_envelopes_and_preserves_original()
{
    let path = TestDatabasePath::new("scrub-physical-tree-damage");
    certify_clean_close(deployed_migration_store(&path, database_id(0x6a)));
    let original_records = contract_bytes(&path.0);
    assert_eq!(original_records.len(), 1);
    let mut bytes = std::fs::read(&path.0).unwrap();
    assert!(bytes.len() <= 8 * 1024 * 1024);
    let needle = &original_records[0];
    let occurrences: Vec<_> = bytes
        .windows(needle.len())
        .enumerate()
        .filter_map(|(index, value)| (value == needle).then_some(index))
        .collect();
    assert_eq!(occurrences.len(), 1, "one live canonical contract envelope");
    // The exact pinned 4.x leaf layout ignores byte 1 when decoding pairs,
    // but includes it in its page-tree checksum. Default pages are 4096 bytes.
    // Guard the fault location: no key, value, envelope or offset is changed.
    let leaf = occurrences[0] / 4096 * 4096;
    assert_eq!(bytes[leaf], 1, "pinned engine leaf discriminator");
    assert!(u16::from_le_bytes(bytes[leaf + 2..leaf + 4].try_into().unwrap()) > 0);
    bytes[leaf + 1] ^= 1;
    std::fs::write(&path.0, &bytes).unwrap();
    assert_eq!(
        contract_bytes(&path.0),
        original_records,
        "ordinary reads still expose the exact valid envelopes"
    );
    let result = RedbOfflineIntegrityScrub::from_inputs(&path.0, inputs()).run();
    assert!(
        matches!(result, Err(ref error) if error.kind() == StorageErrorKind::CorruptData),
        "physical damage must refuse the success receipt: {result:?}"
    );
    assert_eq!(
        std::fs::read(&path.0).unwrap(),
        bytes,
        "the diagnosed primary is byte-for-byte unchanged"
    );
    assert!(!verification_directory(&path.0).exists());
}
