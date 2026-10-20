//! Genuine FreshV3 full-scrub corruption controls; no source publication fixture.
use super::*;

// req: PRJ-004, STO-020, REC-001, REC-005
#[test]
fn full_scrub_rejects_malformed_authoritative_source_row_on_real_runtime_database() {
    let root = crate::test_path::ScopedDirectory::new("source-full-scrub-corruption");
    let path = root.join("db.redb");
    let mut store = RedbStore::open(&path).unwrap();
    let database =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    store
        .initialize_projection_source_fixture(database)
        .unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("genuine FreshV3 runtime fixture must pass normal startup");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    native_source_command_fixture::activate_source_projection(&mut ports).unwrap();
    native_source_command_fixture::append_source_projection_journaled(&ports, 1).unwrap();
    assert!(
        ports
            .complete_graceful_close()
            .disposition()
            .is_successful()
    );
    drop(ports);

    // This is the real complete offline validator, not a Source-only reader or
    // a structural fixture asserted to represent successful publication.
    let before = RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
        .run()
        .unwrap();
    assert!(before.structural_exact_end() && before.catalog_exact_end());
    assert_eq!(before.authoritative_mutations(), 0);
    assert_eq!(before.lifecycle_mutations(), 0);
    eprintln!("source_full_scrub_real_runtime_positive_control_passed");

    // Deliberate test-only corruption outside the coordinator. Neither these
    // bytes nor this key claim canonical syntax, allocation or source custody.
    {
        let database = redb::Database::open(&path).unwrap();
        let transaction = database.begin_write().unwrap();
        let definition = redb::TableDefinition::<&[u8], &[u8]>::new(
            riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceRows.table(),
        );
        {
            let mut rows = transaction.open_table(definition).unwrap();
            rows.insert(
                b"malformed-source-key".as_slice(),
                b"malformed-source-value".as_slice(),
            )
            .unwrap();
        }
        transaction.commit().unwrap();
    }
    let after = RedbOfflineIntegrityScrub::from_inputs(&path, inputs()).run();
    assert!(
        matches!(after, Err(ref error) if error.kind() == StorageErrorKind::CorruptData),
        "full_scrub_must_reject_source_namespace_corruption"
    );
}
