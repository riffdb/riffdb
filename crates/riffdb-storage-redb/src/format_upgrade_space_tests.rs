// req: AFC-001, AFC-002, REC-001, STO-032, END-008
use super::*;

fn legacy() -> RedbDurableFormatPreflight {
    crate::format_preflight::RedbDurableFormatPreflight::OfflineUpgradeRequired {
        current: riffdb_storage_api::DurableFormatIdentity::new(
            riffdb_storage_api::AlphaFormatEpoch::new(1).unwrap(),
            riffdb_storage_api::DurableFormatWriter::new(0),
        ),
        binary: riffdb_storage_api::current_durable_format_manifest().identity(),
        action: riffdb_storage_api::preflight_durable_format(
            riffdb_storage_api::DurableFormatIdentity::new(
                riffdb_storage_api::AlphaFormatEpoch::new(1).unwrap(),
                riffdb_storage_api::DurableFormatWriter::new(0),
            ),
        )
        .unwrap(),
    }
}

fn entry(name: &str, len: u64) -> Result<(OsString, Option<(bool, u64)>), Error> {
    Ok((name.into(), Some((true, len))))
}

#[test]
fn space_floor_uses_checked_principal_available_bytes_and_exact_equality() {
    assert!(check_floor(100, 2, 200, 1).is_ok());
    assert!(matches!(
        check_floor(100, 2, 199, 1),
        Err(Error::InsufficientSpace)
    ));
    assert!(check_floor(100, 2, 50, 4).is_ok());
    assert!(matches!(
        check_floor(100, 2, 49, 4),
        Err(Error::InsufficientSpace)
    ));
    for (source, factor, fragments, fragment) in [
        (u64::MAX, 2, u64::MAX, 1),
        (1, 2, u64::MAX, 2),
        (1, 2, 100, 0),
        (1, 0, 100, 1),
        (0, 2, 100, 1),
    ] {
        assert!(matches!(
            check_floor(source, factor, fragments, fragment),
            Err(Error::Unavailable)
        ));
    }
}

#[test]
fn source_inventory_counts_complete_lifecycle_logical_lengths_and_excludes_siblings() {
    let database = Path::new("app.redb");
    let names = [
        "app.redb",
        "app.redb.riffjournal",
        "app.redb.riffjournal.checkpoint",
        "app.redb.riffjournal.sealing",
        "app.redb.riffjournal.next",
        "app.redb.riffjournal.next.prep",
        "app.redb.riffjournal.recycled",
        "app.redb.riffjournal.sealed.00000000000000000001",
        "app.redb.riffdb-format-v1",
        "app.redb.riffdb-format-v1.staging",
        "app.redb.riffdb-format-upgrade-v1",
        "app.redb.riffdb-format-upgrade-v1.staging",
    ];
    let mut entries = names
        .into_iter()
        .map(|name| entry(name, 13))
        .collect::<Vec<_>>();
    entries.push(Ok(("backup".into(), None)));
    entries.push(entry("other.redb", u64::MAX));
    assert_eq!(
        inventory_bytes(database, RedbDurableFormatPreflight::OpenCurrent, entries).unwrap(),
        12 * 13
    );
    // A legacy writer-zero source has no marker and may have no active journal.
    assert_eq!(
        inventory_bytes(database, legacy(), [entry("app.redb", 17)]).unwrap(),
        17
    );
}

#[test]
fn inventory_refuses_missing_nonregular_duplicate_ambiguous_or_overflowed_source() {
    let database = Path::new("app.redb");
    let invalid = [
        vec![],
        vec![entry("app.redb.riffjournal", 1)],
        vec![Ok(("app.redb".into(), Some((false, 1))))],
        vec![entry("app.redb", 1), entry("app.redb", 1)],
        vec![
            entry("app.redb", u64::MAX),
            entry("app.redb.riffjournal", 1),
        ],
        vec![
            entry("app.redb", 1),
            entry("app.redb.riffjournal.unknown", 1),
        ],
        vec![
            entry("app.redb", 1),
            entry("app.redb.riffjournal.sealed.1", 1),
        ],
        vec![
            entry("app.redb", 1),
            entry("app.redb.riffjournal.sealed.00000000000000000000", 1),
        ],
        vec![
            entry("app.redb", 1),
            entry("app.redb.riffjournal.sealed.99999999999999999999", 1),
        ],
        vec![entry("app.redb", 1), Err(Error::Unavailable)],
    ];
    for entries in invalid {
        assert!(matches!(
            inventory_bytes(database, legacy(), entries),
            Err(Error::Unavailable)
        ));
    }
    assert!(matches!(
        inventory_bytes(
            database,
            RedbDurableFormatPreflight::OpenCurrent,
            [entry("app.redb", 1)]
        ),
        Err(Error::Unavailable)
    ));
}

#[test]
fn directory_ceiling_is_exact_and_cannot_admit_a_partial_inventory() {
    let database = Path::new("app.redb");
    let entries = || {
        std::iter::once(entry("app.redb", 7))
            .chain((1..MAX_DIRECTORY_ENTRIES).map(|i| Ok((format!("unrelated-{i}").into(), None))))
    };
    assert_eq!(inventory_bytes(database, legacy(), entries()).unwrap(), 7);
    assert!(matches!(
        inventory_bytes(
            database,
            legacy(),
            entries().chain([Ok(("one-more".into(), None))])
        ),
        Err(Error::Unavailable)
    ));
}

#[test]
fn retained_extents_above_the_backup_ceiling_cannot_grant_partial_admission() {
    let database = Path::new("app.redb");
    let entries = std::iter::once(entry("app.redb", 7)).chain(
        (1..=MAX_SEALED_EXTENTS + 1)
            .map(|generation| entry(&format!("app.redb.riffjournal.sealed.{generation:020}"), 9)),
    );
    // The tighter directory ceiling refuses first. Neither bound can admit
    // a partial accounting result, even when all names are canonical.
    assert!(matches!(
        inventory_bytes(database, legacy(), entries),
        Err(Error::Unavailable)
    ));
}

#[cfg(unix)]
#[test]
fn native_measurement_counts_sparse_logical_lengths_and_refuses_nonregular_files() {
    let root = tempfile::tempdir_in(crate::test_path::root()).unwrap();
    let database = root.path().join("app.redb");
    File::create(&database).unwrap().set_len(8192).unwrap();
    File::create(crate::journal::journal_path(&database))
        .unwrap()
        .set_len(4096)
        .unwrap();
    assert_eq!(source_bytes(&database, legacy()).unwrap(), 12_288);
    let directory = File::open(root.path()).unwrap();
    let (fragments, size) = observe_available(&directory).unwrap();
    assert!(size > 0);
    assert!(fragments.checked_mul(size).unwrap() > 0);
    assert!(admit(&database, legacy(), 2, observe_available).is_ok());
    fs::create_dir(crate::journal::sealed_journal_path(&database, 1)).unwrap();
    assert!(matches!(
        source_bytes(&database, legacy()),
        Err(Error::Unavailable)
    ));
    fs::remove_dir(crate::journal::sealed_journal_path(&database, 1)).unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&database, crate::journal::sealed_journal_path(&database, 1))
            .unwrap();
        assert!(matches!(
            source_bytes(&database, legacy()),
            Err(Error::Unavailable)
        ));
        fs::remove_file(crate::journal::sealed_journal_path(&database, 1)).unwrap();
    }
    fs::remove_file(&database).unwrap();
    assert!(matches!(
        source_bytes(&database, legacy()),
        Err(Error::Unavailable)
    ));
}
