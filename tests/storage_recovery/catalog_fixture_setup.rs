//! Genuine initialized and activated catalog fixtures for recovery tests.

use super::*;

/// Selects predecessor bytes only for tests of V1 bounded-start compatibility.
pub(super) fn prepare_legacy_clean_start_fixture(path: &Path) {
    let mut store = RedbStore::open(path).expect("open V1 compatibility fixture");
    riffdb_storage_redb::initialize_legacy_database_fixture(&mut store, database_id())
        .expect("initialize V1 compatibility fixture");
}

pub(super) fn prepare_command_database(path: &Path) {
    let mut store = RedbStore::open(path).expect("open command recovery database");
    store
        .initialize_database(database_id())
        .expect("initialize command recovery database");
    activate_command_catalog(store);
}

/// The same activated command database on an AuthoritativeStateCatalogV1
/// source. New databases are always V2; proofs that must hold for both
/// catalogs (the ADR-0182 bounded dirty path serves both since ADR-0260) run
/// a V1 arm here.
pub(super) fn prepare_v1_command_database(path: &Path) {
    let mut store = RedbStore::open(path).expect("open V1 command recovery database");
    riffdb_storage_redb::initialize_legacy_database_fixture(&mut store, database_id())
        .expect("initialize V1 command recovery database");
    activate_command_catalog(store);
}

pub(super) fn activate_command_catalog(store: RedbStore) {
    let mut ports = open_operational_completed_fixture(store);
    let bundle = contract_bundle();
    let result = ports
        .activate_catalog(&CatalogActivationIntentV1::new(
            None,
            bundle.clone(),
            RequestId::from_bytes(uuid_bytes(0x62)).expect("catalog request"),
            catalog_principal(),
            Timestamp::new(1_700_000_000, 0).expect("catalog timestamp"),
            None,
        ))
        .expect("activate command recovery catalog");
    assert!(
        matches!(
            &result,
            CatalogActivationResult::Activated { active, .. }
                if *active == riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
        ),
        "unexpected catalog activation result: {result:?}"
    );
}
