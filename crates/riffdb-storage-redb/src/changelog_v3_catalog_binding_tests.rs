//! Catalog codecs cannot change production activation or accept mixed roots.
// req: REP-003, REP-005, REC-001, STO-012
use super::*;
use riffdb_storage_api::AuthoritativeStateCatalogV2;

fn successor_lineage() -> ChangelogLineageV3 {
    let original = lineage();
    ChangelogLineageV3::new_with_catalog(
        original.database_id(),
        original.history_incarnation(),
        original.leadership_epoch(),
        AuthoritativeStateCatalogV2.digest(),
    )
    .unwrap()
}

#[test]
fn existing_activation_refuses_successor_catalog_without_installing_any_roots() {
    let root = crate::test_path::ScopedDirectory::new("v3-foreign-activation-catalog");
    let database = fixture(&root.join("database.redb"), PRE_V3_REGISTRY);
    let before = metadata(&database);
    let error = activate_validated(
        database.begin_write().unwrap(),
        successor_lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::IncompatibleFormat);
    assert_eq!(metadata(&database), before);
    let read = database.begin_read().unwrap();
    assert!(read.open_table(HISTORY).is_err());
    assert!(read.open_table(SOURCE_HOLDS).is_err());
}

#[test]
fn root_validation_rejects_checksum_valid_history_with_different_catalog_without_writes() {
    let root = crate::test_path::ScopedDirectory::new("v3-substituted-root-catalog");
    let path = root.join("database.redb");
    let database = fixture(&path, PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let foreign = ChangelogHistoryStateV3::new(
        successor_lineage(),
        history.anchor(),
        history.tail(),
        history.minimum_resume(),
    )
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            key(N::ChangelogHistoryState).unwrap(),
            encode_changelog_history_state_v3(foreign)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    write.commit().unwrap();
    let before = metadata(&database);
    let check = |database: &Database| {
        let read = database.begin_read().unwrap();
        assert_eq!(
            crate::changelog_v3_roots::read_checkpoint_roots(&read)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData,
        );
        assert!(crate::changelog_v3_roots::validate_retained_history(&read).is_err());
        drop(read);
        let write = database.begin_write().unwrap();
        assert!(crate::changelog_v3_roots::read_checkpoint_roots_for_write(&write).is_err());
        assert!(crate::changelog_v3_roots::validate_retained_history_for_write(&write).is_err());
        write.abort().unwrap();
        assert_eq!(metadata(database), before);
    };
    check(&database);
    drop(database);
    check(&Database::open(&path).unwrap());
}

fn successor_admission_vectors() -> Vec<Vec<u8>> {
    let mut values = include_str!("../../../fixtures/replication/primary-fence-v1.hex")
        .lines()
        .filter_map(|line| {
            let (name, hex) = line.split_once(' ')?;
            matches!(name, "active" | "fenced").then(|| {
                hex.as_bytes()
                    .chunks_exact(2)
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                    .collect()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(values.len(), 2);
    values.push(vec![0xff]);
    values
}

#[test]
fn v1_activation_never_ignores_existing_successor_admission_or_fence_evidence() {
    let admission_key = riffdb_storage_api::AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
        .metadata_key()
        .unwrap();
    for bytes in successor_admission_vectors() {
        let root = crate::test_path::ScopedDirectory::new("v3-activation-stray-admission");
        let database = fixture(&root.join("database.redb"), PRE_V3_REGISTRY);
        let write = database.begin_write().unwrap();
        write
            .open_table(META)
            .unwrap()
            .insert(admission_key, bytes.as_slice())
            .unwrap();
        write.commit().unwrap();
        let before = metadata(&database);
        let result = activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        );
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::CorruptData);
        assert_eq!(metadata(&database), before);
        let read = database.begin_read().unwrap();
        assert!(read.open_table(HISTORY).is_err());
        assert!(read.open_table(SOURCE_HOLDS).is_err());
    }
}

#[test]
fn v1_root_permits_never_ignore_successor_admission_or_fence_evidence() {
    let admission_key = riffdb_storage_api::AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
        .metadata_key()
        .unwrap();
    for bytes in successor_admission_vectors() {
        let root = crate::test_path::ScopedDirectory::new("v3-root-stray-admission");
        let path = root.join("database.redb");
        let database = fixture(&path, PRE_V3_REGISTRY);
        activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        let write = database.begin_write().unwrap();
        write
            .open_table(META)
            .unwrap()
            .insert(admission_key, bytes.as_slice())
            .unwrap();
        write.commit().unwrap();
        let before = metadata(&database);
        let check = |database: &Database| {
            let read = database.begin_read().unwrap();
            assert_eq!(
                crate::changelog_v3_roots::read_checkpoint_roots(&read)
                    .unwrap_err()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            assert!(crate::changelog_v3_roots::validate_retained_history(&read).is_err());
            drop(read);
            let write = database.begin_write().unwrap();
            assert!(crate::changelog_v3_roots::read_checkpoint_roots_for_write(&write).is_err());
            assert!(
                crate::changelog_v3_roots::validate_retained_history_for_write(&write).is_err()
            );
            write.abort().unwrap();
            assert_eq!(metadata(database), before);
        };
        check(&database);
        drop(database);
        check(&Database::open(&path).unwrap());
    }
}
