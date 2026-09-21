//! Production structural/catalog handoff must establish checked source admission.
// req: REP-005, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1, AuthoritativeNamespaceV2, AuthoritativeStateCatalogV2,
    proto_codec::{decode_changelog_history_state_v3, decode_replication_primary_admission_v1},
};

#[test]
fn fresh_source_handoff_installs_active_v2_before_granting_operational_ports() {
    let path = TestDatabasePath::new("v2-source-startup-admission");
    let mut store = RedbStore::open(&path.0).unwrap();
    store.initialize_database(database_id(0xe1)).unwrap();
    let shared = Arc::clone(&store.shared);
    let ports = finish_source(store);
    let read = shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let admission = meta
        .get(
            AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
                .metadata_key()
                .unwrap(),
        )
        .unwrap();
    assert!(
        admission.is_some(),
        "source operational ports require explicit checked primary admission"
    );
    let admission = decode_replication_primary_admission_v1(admission.unwrap().value()).unwrap();
    let history = decode_changelog_history_state_v3(
        meta.get(
            AuthoritativeNamespaceV1::ChangelogHistoryState
                .metadata_key()
                .unwrap(),
        )
        .unwrap()
        .unwrap()
        .value(),
    )
    .unwrap();
    assert_eq!(
        history.value().lineage().catalog_digest(),
        AuthoritativeStateCatalogV2.digest()
    );
    assert_eq!(admission.value().lineage(), history.value().lineage());
    assert!(admission.value().fence().is_none());
    use riffdb_storage_api::ReplicationPrimaryAdmissionReadPort;
    assert_eq!(
        ports.read_replication_primary_admission().unwrap(),
        *admission.value()
    );
    assert_eq!(
        ports
            .shared_ports()
            .read_replication_primary_admission()
            .unwrap(),
        *admission.value()
    );
}

#[test]
fn coordinator_admission_read_refuses_legacy_or_lost_source_state_without_repair() {
    use riffdb_storage_api::ReplicationPrimaryAdmissionReadPort;
    for legacy in [false, true] {
        let path = TestDatabasePath::new("v2-coordinator-admission-refusal");
        let mut store = RedbStore::open(&path.0).unwrap();
        if legacy {
            store.initialize_legacy_fixture(database_id(0xe2)).unwrap();
        } else {
            store.initialize_database(database_id(0xe2)).unwrap();
        }
        let shared = Arc::clone(&store.shared);
        let ports = finish_source(store);
        if !legacy {
            let write = shared.database.begin_write().unwrap();
            write
                .open_table(META)
                .unwrap()
                .remove(crate::primary_admission_roots::key().unwrap())
                .unwrap();
            write.commit().unwrap();
        }
        assert!(ports.read_replication_primary_admission().is_err());
        assert!(
            ports
                .shared_ports()
                .read_replication_primary_admission()
                .is_err()
        );
        let read = shared.database.begin_read().unwrap();
        assert!(
            read.open_table(META)
                .unwrap()
                .get(crate::primary_admission_roots::key().unwrap())
                .unwrap()
                .is_none()
        );
    }
}

fn finish_source(store: RedbStore) -> crate::RedbOperationalPorts {
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural_end = finish_structural(&mut session);
    let (catalog, historical_end) = validate_catalog_history(&mut session).unwrap().into_parts();
    let CatalogHistoryOutcome::Ready(catalog) = catalog else {
        panic!("valid fresh catalog");
    };
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("fresh structural proof");
    };
    let (id, session_id, _, dormant) = opened.into_parts();
    assert!(catalog.matches(id, session_id));
    dormant.into_operational_after_catalog_validation().unwrap()
}

#[test]
fn fresh_v2_initialization_crashes_leave_empty_or_complete_source_and_reopen() {
    use riffdb_storage_api::{DatabaseIdentityProbe, DatabaseIdentityProbePort};
    for edge in ["preflight", "roots", "receipt", "committed"] {
        let path = TestDatabasePath::new("v2-source-initialization-crash");
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup::tests::v2_primary::fresh_v2_initialization_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_WP748_FRESH_V2_PATH", &path.0)
            .env("RIFFDB_WP772_ACTIVATION_CRASH", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(91), "edge {edge}");
        let mut store = RedbStore::open(&path.0).unwrap();
        assert_eq!(
            store.probe_database_identity().unwrap(),
            if edge == "committed" {
                DatabaseIdentityProbe::Existing(database_id(0xe1))
            } else {
                DatabaseIdentityProbe::NeedsInitialization
            }
        );
        store.initialize_database(database_id(0xe1)).unwrap();
        let ports = finish_source(store);
        let read = ports.shared.database.begin_read().unwrap();
        let history = crate::changelog_v3_roots::validate_retained_history(&read)
            .unwrap()
            .unwrap();
        assert_eq!(
            history.lineage().catalog_digest(),
            AuthoritativeStateCatalogV2.digest()
        );
        assert_eq!(history.lineage().history_incarnation(), 1);
        drop(read);
        drop(ports);
        drop(finish_source(RedbStore::open(&path.0).unwrap()));
    }
}

#[test]
fn fresh_v2_initialization_process_child() {
    let Ok(path) = std::env::var("RIFFDB_WP748_FRESH_V2_PATH") else {
        return;
    };
    let mut store = RedbStore::open(path).unwrap();
    store.initialize_database(database_id(0xe1)).unwrap();
    panic!("armed initialization crash did not fire");
}

#[test]
fn source_reopen_refuses_erased_v2_admission_instead_of_recreating_active() {
    let path = TestDatabasePath::new("v2-source-missing-admission");
    let mut store = RedbStore::open(&path.0).unwrap();
    store.initialize_database(database_id(0xe1)).unwrap();
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .remove(crate::primary_admission_roots::key().unwrap())
        .unwrap();
    write.commit().unwrap();
    drop(store);
    let error = RedbStore::open(&path.0).unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    let raw = redb::Database::open(&path.0).unwrap();
    let read = raw.begin_read().unwrap();
    assert!(
        read.open_table(META)
            .unwrap()
            .get(crate::primary_admission_roots::key().unwrap())
            .unwrap()
            .is_none()
    );
}

// req: PERF-019, REP-005, STO-012
#[test]
fn v2_clean_close_requires_complete_startup_and_keeps_lifecycle_bytes() {
    let path = TestDatabasePath::new("v2-clean-complete-startup");
    let mut store = RedbStore::open(&path.0).unwrap();
    store.initialize_database(database_id(0xe3)).unwrap();
    let ports = finish_source(store);
    ports.write_clean_close_lifecycle().unwrap();
    assert_eq!(ports.clean_close_write_failures(), 0);
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let encoded = meta
        .get(META_CLEAN_CLOSE_LIFECYCLE)
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let lifecycle = crate::clean_close::CleanCloseLifecycle::decode(&encoded).unwrap();
    assert!(matches!(
        lifecycle.state(),
        crate::clean_close::CleanCloseState::Clean(_)
    ));
    assert_eq!(lifecycle.encode().unwrap(), encoded);
    drop(meta);
    drop(read);
    drop(ports);

    let store = RedbStore::open(&path.0).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    assert!(
        !session.clean_close_fast_path(),
        "CLEAN is insufficient for V2 authority"
    );
    assert_eq!(
        session.clean_close_declined_reason(),
        Some("bounded_roots_unavailable")
    );
    let structural_end = finish_structural(&mut session);
    let (catalog, historical_end) = validate_catalog_history(&mut session).unwrap().into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("complete V2 validation must succeed");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant.into_operational_after_catalog_validation().unwrap();
    assert!(!ports.clean_close_fast_startup());
    use riffdb_storage_api::ReplicationPrimaryAdmissionReadPort;
    assert!(
        ports
            .read_replication_primary_admission()
            .unwrap()
            .fence()
            .is_none()
    );
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let encoded = meta.get(META_CLEAN_CLOSE_LIFECYCLE).unwrap().unwrap();
    let consumed = crate::clean_close::CleanCloseLifecycle::decode(encoded.value()).unwrap();
    assert_eq!(consumed, lifecycle.successor_dirty().unwrap());
}
