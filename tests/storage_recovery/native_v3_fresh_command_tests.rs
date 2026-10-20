// req: REP-002, REP-003, REC-001, STO-012, PRJ-004
use super::*;

#[test]
fn genuine_v3_fresh_command_keeps_full_graph_and_audits_with_source_capacity_saturated() {
    let path = TestDatabasePath::new("genuine-native-v3-source-pressure");
    let mut store = RedbStore::open(&path.0).expect("open genuine empty source");
    assert!(matches!(
        riffdb_storage_redb::initialize_projection_source_database_fixture(&mut store,database_id()).expect("install actual Empty V3 source"),
        riffdb_storage_api::DatabaseInitializationResult::Installed(id) if id==database_id()
    ));
    // The normal complete startup owner validates catalog and structural history;
    // no fixture token pretends that completion has happened.
    let mut ports = open_operational_completed_fixture(store);
    let pressure = riffdb_storage_redb::occupy_source_working_capacity_fixture(&ports)
        .expect("occupy this actual engine's remaining Source capacity");
    let bundle = contract_bundle();
    assert!(matches!(
        ports.activate_catalog(&CatalogActivationIntentV1::new(
            None,bundle.clone(),RequestId::from_bytes(uuid_bytes(0x62)).unwrap(),
            catalog_principal(),Timestamp::new(1_700_000_000,0).unwrap(),None,
        )).expect("catalog activation must not decode native origin under Source quota"),
        CatalogActivationResult::Activated { active, .. } if active==riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
    ));
    let fixture = command_fixture();
    try_commit_command_fixture_with_audit(&ports, &fixture, command_audit_transition(&fixture))
        .expect("real compiled command and coordinator commit while Source capacity is full");
    // Operator reads deliberately consume the source ledger for the immutable
    // native Audit DTO. Release worker pressure before proving the complete
    // graph through unchanged operator/query readers, including every audit row.
    drop(pressure);
    assert_postcommit_command_state(&ports, &fixture);
    let scan = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).unwrap(),
        ))
        .expect("complete operator Audit stream includes origin");
    let AdministrationAuditScan::ExactEnd { records } = scan else {
        panic!("small complete stream")
    };
    let origin = records
        .first()
        .expect("actual origin at administration one");
    assert_eq!(
        origin.value().administration_sequence(),
        AdministrationSequence::first()
    );
    assert!(
        matches!(origin.value(),StoredAdministrationAuditRecordV1::NativeCatalogTransition(value) if matches!(value.value().parts().origin,riffdb_storage_api::NativeCatalogTransitionOriginV1::FreshV3))
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.value(),
                StoredAdministrationAuditRecordV1::NativeCatalogTransition(_)
            ))
            .count(),
        1
    );
    assert_eq!(records.iter().filter(|record| matches!(record.value(),StoredAdministrationAuditRecordV1::Service(value) if value.operation()==ServiceOperationV1::ExecuteCommand)).count(),2);
}

#[test]
fn genuine_new_engine_cold_origin_keeps_full_graph_and_audits_with_source_capacity_saturated() {
    let path = TestDatabasePath::new("genuine-native-v3-cold-origin-pressure");
    let mut store = RedbStore::open(&path.0).expect("open genuine empty source");
    assert!(matches!(
        riffdb_storage_redb::initialize_projection_source_database_fixture(&mut store,database_id()).expect("install actual Empty V3 source"),
        riffdb_storage_api::DatabaseInitializationResult::Installed(id) if id==database_id()
    ));
    // Complete the original genuine startup/registry owner first. Its current
    // registry may now be Completed; the immutable origin still binds its
    // original Pending envelope, which cold reconstruction must preserve.
    let initialized_ports = open_operational_completed_fixture(store);
    drop(initialized_ports);
    // No original EngineOwner/context remains. The new actual startup owner
    // reconstructs bounded cold evidence from this genuine persisted database.
    let store = RedbStore::open(&path.0).expect("open new actual engine");
    // The normal complete startup owner validates catalog and structural history;
    // no fixture token pretends that completion has happened.
    let mut ports = open_operational_completed_fixture(store);
    let pressure = riffdb_storage_redb::occupy_source_working_capacity_fixture(&ports)
        .expect("occupy this actual engine's remaining Source capacity");
    let bundle = contract_bundle();
    assert!(matches!(
        ports.activate_catalog(&CatalogActivationIntentV1::new(
            None,bundle.clone(),RequestId::from_bytes(uuid_bytes(0x62)).unwrap(),
            catalog_principal(),Timestamp::new(1_700_000_000,0).unwrap(),None,
        )).expect("catalog activation must not decode native origin under Source quota"),
        CatalogActivationResult::Activated { active, .. } if active==riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
    ));
    let fixture = command_fixture();
    try_commit_command_fixture_with_audit(&ports, &fixture, command_audit_transition(&fixture))
        .expect("real compiled command and coordinator commit while Source capacity is full");
    // Operator reads deliberately consume the source ledger for the immutable
    // native Audit DTO. Release worker pressure before proving the complete
    // graph through unchanged operator/query readers, including every audit row.
    drop(pressure);
    assert_postcommit_command_state(&ports, &fixture);
    let scan = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).unwrap(),
        ))
        .expect("complete operator Audit stream includes origin");
    let AdministrationAuditScan::ExactEnd { records } = scan else {
        panic!("small complete stream")
    };
    let origin = records
        .first()
        .expect("actual origin at administration one");
    assert_eq!(
        origin.value().administration_sequence(),
        AdministrationSequence::first()
    );
    assert!(
        matches!(origin.value(),StoredAdministrationAuditRecordV1::NativeCatalogTransition(value) if matches!(value.value().parts().origin,riffdb_storage_api::NativeCatalogTransitionOriginV1::FreshV3))
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record.value(),
                StoredAdministrationAuditRecordV1::NativeCatalogTransition(_)
            ))
            .count(),
        1
    );
    assert_eq!(records.iter().filter(|record| matches!(record.value(),StoredAdministrationAuditRecordV1::Service(value) if value.operation()==ServiceOperationV1::ExecuteCommand)).count(),2);
}
