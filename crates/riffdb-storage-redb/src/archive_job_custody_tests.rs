// req: STO-032, REP-007, REC-005
use crate::native_file_custody::NativeFileCustody;
use redb::StorageBackend;
use riffdb_storage_api::{DatabaseInitializationPort, StorageErrorKind};
use std::fs::OpenOptions;
use std::ops::Bound;
use std::sync::Arc;

#[test]
fn archive_engine_custody_keeps_actual_native_exclusion_without_shared_or_read_pin() {
    let root = crate::test_path::ScopedDirectory::new("archive-engine-custody");
    let path = root.join("database.redb");
    let mut store = crate::RedbStore::open(&path).unwrap();
    let id =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    store.initialize_database(id).unwrap();
    let ports = crate::startup::open_cleanly_for_test(store);
    let shared = Arc::downgrade(&ports.shared);
    let engine = ports
        .shared
        .database
        .admit_maintenance_weak_backing(&ports.shared.database)
        .unwrap();
    let custody = ports.archive_job_engine_custody_v1();
    assert!(custody._engine.same_allocation(&ports.shared.database));
    drop(ports);
    assert!(
        shared.upgrade().is_none(),
        "custody must not retain SharedRedb"
    );
    assert!(
        engine.upgrade().is_some(),
        "the actual engine remains alive"
    );
    let open = || {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap()
    };
    let refusal = NativeFileCustody::acquire(open()).err().unwrap();
    assert_eq!(refusal.kind(), StorageErrorKind::Unavailable);
    let native_probe = redb::backends::FileBackend::new(open()).unwrap();
    assert!(
        !native_probe
            .try_lock_range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
    );
    drop(native_probe);
    let alias = custody.clone();
    drop(custody);
    assert_eq!(
        NativeFileCustody::acquire(open()).err().unwrap().kind(),
        StorageErrorKind::Unavailable
    );
    drop(alias);
    assert!(engine.upgrade().is_none());
    let closed = NativeFileCustody::acquire(open()).unwrap();
    closed.verify_path_binding(&path).unwrap();
    drop(closed);
    assert!(redb::ReadOnlyDatabase::open(&path).is_ok());
}
