//! The private validation and authorization snapshot are bound to exact bytes.
// req: REP-007, REC-001, AFC-007
use super::*;

#[test]
fn private_archive_validation_refuses_rewritten_frontier() {
    check_private_archive_case(RedbCommitProfile::Hardened, None, Some("changed-frontier"));
}
#[test]
fn private_archive_validation_refuses_missing_reciprocal_locator() {
    check_private_archive_case(RedbCommitProfile::Hardened, None, Some("missing-locator"));
}
#[test]
fn private_archive_validation_cancellation_discards_without_publication() {
    check_private_archive_case(RedbCommitProfile::Hardened, None, Some("cancel-validation"));
}
#[test]
fn private_archive_authorization_refuses_changes_after_validation() {
    check_private_archive_case(
        RedbCommitProfile::Hardened,
        None,
        Some("changed-after-validation"),
    );
}

#[test]
fn private_archive_validation_refuses_reinstated_normal_marker() {
    check_private_archive_case(RedbCommitProfile::Hardened, None, Some("restore-marker"));
}

pub(super) fn tamper(path: &Path, fault: &str) {
    if fault == "restore-marker" {
        std::fs::copy(
            path.parent().unwrap().join("private-restore-format.riffdb"),
            riffdb_storage_redb::durable_format_marker_path(path),
        )
        .unwrap();
        assert!(RedbStore::open(path).is_err());
        assert!(riffdb_storage_redb::RedbFollowerStore::open(path).is_err());
        return;
    }
    let database = redb::Database::open(path).unwrap();
    let write = database.begin_write().unwrap();
    match fault {
        "changed-frontier" => {
            let encoded = encode_application_sequence_allocator_v1(
                ApplicationSequenceAllocator::Next(CommitSequence::new(4).unwrap()),
            )
            .unwrap();
            write
                .open_table(META)
                .unwrap()
                .insert(
                    N::NextApplicationSequence.metadata_key().unwrap(),
                    encoded.as_bytes(),
                )
                .unwrap();
        }
        "missing-locator" => {
            let mut table = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new(
                    N::IdempotencyLocators.table(),
                ))
                .unwrap();
            let key = table.first().unwrap().unwrap().0.value().to_vec();
            table.remove(key.as_slice()).unwrap();
        }
        _ => panic!("closed fixture fault"),
    }
    write.commit().unwrap();
}

pub(super) fn rows(path: &Path) -> BTreeMap<(String, Vec<u8>), Vec<u8>> {
    let database = redb::ReadOnlyDatabase::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let mut rows = BTreeMap::new();
    let meta = read.open_table(META).unwrap();
    for entry in meta.iter().unwrap() {
        let (key, value) = entry.unwrap();
        rows.insert(
            ("meta".to_owned(), key.value().as_bytes().to_vec()),
            value.value().to_vec(),
        );
    }
    for namespace in N::ALL.into_iter().filter(|n| n.metadata_key().is_none()) {
        let table = read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(namespace.table()))
            .unwrap();
        for entry in table.iter().unwrap() {
            let (key, value) = entry.unwrap();
            assert!(rows.len() < 1024, "bounded fixture authority");
            rows.insert(
                (namespace.table().to_owned(), key.value().to_vec()),
                value.value().to_vec(),
            );
        }
    }
    rows
}
