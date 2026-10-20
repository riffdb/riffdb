//! Derived read transactions retain the actual engine they captured.
// req: TXN-042, STO-032, OQ-040
use super::*;
use riffdb_storage_api::DatabaseInitializationPort;

#[test]
fn missing_sidecar_snapshot_retains_its_empty_engine_without_creating_a_file() {
    let scope = crate::test_path::ScopedDirectory::new("snapshot-empty-engine-origin");
    let path = scope.join("source.redb");
    let sidecar = crate::store::derived_store_path(&path);
    let mut store = crate::RedbStore::open(&path).unwrap();
    store
        .initialize_database(
            riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [0x24; 10]).unwrap(),
        )
        .unwrap();
    let ports = crate::RedbOperationalPorts {
        shared: std::sync::Arc::clone(&store.shared),
    };
    let old = ports.open_owned_snapshot().unwrap();
    let same_empty = ports.open_owned_snapshot().unwrap();
    assert!(
        !sidecar.exists(),
        "capturing an empty derived read cannot create a sibling file"
    );
    assert!(
        old._derived_engine
            .as_ref()
            .unwrap()
            .same_allocation(same_empty._derived_engine.as_ref().unwrap())
    );
    drop(same_empty);
    let write = ports.begin_derived_write().unwrap();
    write
        .transaction()
        .unwrap()
        .open_table(crate::layout::PROJECTION_STATE)
        .unwrap()
        .insert(b"origin".as_slice(), b"real-sidecar".as_slice())
        .unwrap();
    write
        .commit_for(crate::hooks::RedbTestOperation::ProjectionMutation)
        .unwrap();
    let current = ports.open_owned_snapshot().unwrap();
    assert!(sidecar.exists());
    assert!(
        !old._derived_engine
            .as_ref()
            .unwrap()
            .same_allocation(current._derived_engine.as_ref().unwrap())
    );
    drop(ports);
    drop(store);
    assert!(
        old.derived_transaction()
            .unwrap()
            .open_table(crate::layout::PROJECTION_STATE)
            .unwrap()
            .get(b"origin".as_slice())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        current
            .derived_transaction()
            .unwrap()
            .open_table(crate::layout::PROJECTION_STATE)
            .unwrap()
            .get(b"origin".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"real-sidecar"
    );
    assert!(redb::Database::open(&path).is_err());
    assert!(redb::Database::open(&sidecar).is_err());
    drop(current);
    // Only the old empty in-memory engine remains; it cannot retain a lock on
    // the different physical sidecar or substitute its later data.
    let reopened_sidecar = redb::Database::open(&sidecar).unwrap();
    drop(reopened_sidecar);
    assert!(
        old.derived_transaction()
            .unwrap()
            .open_table(crate::layout::PROJECTION_STATE)
            .unwrap()
            .get(b"origin".as_slice())
            .unwrap()
            .is_none()
    );
    drop(old);
    drop(redb::Database::open(&path).unwrap());
}
