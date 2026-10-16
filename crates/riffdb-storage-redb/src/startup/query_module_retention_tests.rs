//! Cold module metadata reads over genuine command history and online pruning.
use super::*;
use crate::RedbOperationalPorts;
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    AuthoritativePointReader, CatalogActivationResult, CatalogRepository, OwnedSnapshotReader,
    QueryModuleActivationIntentV1, QueryModuleActivationResult, QueryModuleActiveExpectationV1,
    QueryModuleAdministrationRepository, QueryModuleRepository, StorageScanLimit,
    StoredQueryModuleV1,
};

const SUCCESSOR: &str = r#"
contract StorageRecovery version 2 {
  entity Row {
    key (id: u64)
    field value: u64
    index ByValue(value)
  }
  event RowCreated {
    partition_by (id)
    policy_anchor current Row(id: id)
    id: u64
    value: u64
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
  row policy RowAccess on Row {
    allow read when true
  }
  command CreateRow {
    input idempotency_key: string<128>
    input id: u64
    input value: u64
    idempotency_key idempotency_key
    create Row(id) as row
      else RowAlreadyExists { id: id }
    set row.value = value
    emit RowCreated { id: id, value: value }
    return RowCreatedOutcome { row: row }
  }
}
"#;

fn successor_fixture() -> (
    crate::test_path::ScopedDirectory,
    RedbOperationalPorts,
    StoredContractBundleV1,
) {
    let (scope, mut ports) = super::native_source_admission::fixture();
    super::native_source_admission::complete_first_event(&mut ports);
    let current = ports.read_active_catalog().unwrap().unwrap();
    let contract = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(SUCCESSOR).expect("compile the additive version-two contract"),
    )
    .unwrap()
    .to_stored()
    .unwrap();
    assert_eq!(contract.lineage(), current.lineage());
    assert_eq!(current.contract_version().get(), 1);
    assert!(matches!(
        ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                Some(current.contract_version()),
                contract.clone(),
                request_id(0xb1),
                audit_principal(0xb2),
                Timestamp::new(1_700_000_001, 0).unwrap(),
                None,
            ))
            .unwrap(),
        CatalogActivationResult::Activated { .. }
    ));
    (scope, ports, contract)
}

fn all_audits(ports: &RedbOperationalPorts) -> Vec<StoredAdministrationAuditRecordV1> {
    let AdministrationAuditScan::ExactEnd { records } = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).unwrap(),
        ))
        .unwrap()
    else {
        panic!("the two-command fixture must have a bounded complete audit stream");
    };
    records
        .into_iter()
        .map(|record| record.value().clone())
        .collect()
}

struct PrunedSnapshotFixture {
    pinned: crate::RedbOwnedSnapshot,
    current: crate::RedbOwnedSnapshot,
    contract: StoredContractBundleV1,
    outcome: StoredOutcomeV1,
    provenance: StoredProvenanceRecordV1,
    // Source custody outlives every snapshot and its retained engine.
    _scope: crate::test_path::ScopedDirectory,
}

fn pruned_snapshot_fixture() -> PrunedSnapshotFixture {
    let (scope, ports, contract) = successor_fixture();
    // Materialize the actual rebuildable sidecar so lifetime tests exercise
    // both real engines, rather than the process-owned empty derived reader.
    ports.begin_derived_write().unwrap().abort().unwrap();
    let pinned = ports.open_owned_snapshot().unwrap();
    assert_eq!(
        pinned
            .read_active_query_module(
                contract.lineage(),
                contract.contract_version(),
                contract.bundle_hash(),
            )
            .unwrap(),
        None,
        "cold absence is valid before the concurrent maintenance publication"
    );
    let before = all_audits(&ports);
    let access = ports.begin_composite_read().unwrap();
    let command = crate::command_authority::command_member_at_access(
        &access,
        CommitSequence::new(1).unwrap(),
    )
    .unwrap()
    .expect("the genuine fixture contains command one");
    let expected_outcome = command.base().outcome().clone();
    let expected_provenance = command.base().provenance().clone();
    assert_eq!(
        pinned
            .read_stored_outcome(expected_outcome.identity())
            .unwrap(),
        Some(expected_outcome.clone())
    );
    assert_eq!(
        pinned
            .read_provenance(expected_provenance.provenance_id())
            .unwrap(),
        Some(expected_provenance.clone())
    );
    let started = command.base().started_audit().clone();
    let terminal = command.base().terminal_audit().clone();
    drop(command);
    drop(access);
    let audit_key = keys::encode_audit_key(started.administration_sequence());
    assert!(
        pinned
            .complete_logical_access_for_test()
            .read_value(crate::journal::JournalTable::Audit, &audit_key)
            .unwrap()
            .is_none(),
        "current command service audits are owned by their command segment"
    );
    // The accepted owned read holds its immutable pin before the real writer
    // publishes retention. This explicit order needs no sleeps or private
    // mutation/index lock, and does not manufacture a missing audit row.
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::MIN, NonZeroU64::MIN)
            .unwrap(),
        crate::OnlineRetentionStep::Advanced {
            through: 1,
            sequences: 1
        }
    ));
    let current = ports.begin_composite_read().unwrap();
    for audit in [&started, &terminal] {
        let key = keys::encode_audit_key(audit.administration_sequence());
        assert!(
            current
                .read_value(crate::journal::JournalTable::Audit, &key)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            crate::administration::read_administration_record_access(
                &ports,
                &current,
                audit.administration_sequence(),
            )
            .unwrap(),
            StoredAdministrationAuditRecordV1::Service(audit.clone()),
            "retention preserves the complete original audit, rather than creating a hole"
        );
        assert!(
            ports
                .indexed_command_audit(audit.administration_sequence())
                .unwrap()
                .is_none(),
            "the current command-derived locator retires with the pruned segment"
        );
    }
    for audit in before {
        assert_eq!(
            crate::administration::read_administration_record_access(
                &ports,
                &current,
                audit.administration_sequence(),
            )
            .unwrap(),
            audit,
            "every historical administration record survives the genuine prune"
        );
    }
    PrunedSnapshotFixture {
        _scope: scope,
        pinned,
        current: ports.open_owned_snapshot().unwrap(),
        contract,
        outcome: expected_outcome,
        provenance: expected_provenance,
    }
}

// req: TXN-042, STO-032, QRY-003
#[test]
fn cold_query_module_absence_survives_online_retention_after_read_pin() {
    let fixture = pruned_snapshot_fixture();
    for snapshot in [&fixture.pinned, &fixture.current] {
        assert_eq!(
            snapshot
                .read_active_query_module(
                    fixture.contract.lineage(),
                    fixture.contract.contract_version(),
                    fixture.contract.bundle_hash(),
                )
                .unwrap(),
            None,
            "both immutable views preserve legitimate version-two module absence"
        );
    }
}

// req: OUT-001, TXN-042, STO-032
#[test]
fn owned_snapshot_outcome_survives_retention_of_original_command() {
    let fixture = pruned_snapshot_fixture();
    for snapshot in [&fixture.pinned, &fixture.current] {
        assert_eq!(
            snapshot
                .read_stored_outcome(fixture.outcome.identity())
                .unwrap(),
            Some(fixture.outcome.clone()),
            "old and current pins retain the original persisted outcome"
        );
    }
}

// req: TXN-042, STO-032
#[test]
fn owned_snapshot_provenance_survives_retention_of_original_command() {
    let fixture = pruned_snapshot_fixture();
    for snapshot in [&fixture.pinned, &fixture.current] {
        assert_eq!(
            snapshot
                .read_provenance(fixture.provenance.provenance_id())
                .unwrap(),
            Some(fixture.provenance.clone()),
            "old and current pins retain the original provenance"
        );
    }
}

// req: OUT-001, OUT-002, TXN-042, REC-001, STO-032
#[test]
fn cold_query_module_read_still_refuses_substituted_activation_audit() {
    let (_scope, mut ports, contract) = successor_fixture();
    let module = StoredQueryModuleV1::new(
        riffdb_types::QueryModuleName::new("retention_probe").unwrap(),
        riffdb_types::QueryModuleVersion::new(1).unwrap(),
        riffdb_types::QueryModuleHash::from_bytes([0xb3; 32]),
        contract.lineage().clone(),
        contract.contract_version(),
        contract.bundle_hash(),
        vec![0xb3, 1, 2, 3],
    )
    .unwrap();
    let QueryModuleActivationResult::Activated {
        administration_sequence,
        ..
    } = ports
        .activate_query_module(&QueryModuleActivationIntentV1::new(
            QueryModuleActiveExpectationV1::Absent,
            module,
            request_id(0xb4),
            audit_principal(0xb2),
            Timestamp::new(1_700_000_002, 0).unwrap(),
            None,
        ))
        .unwrap()
    else {
        panic!("the new version has no active query module");
    };
    assert!(
        ports
            .open_owned_snapshot()
            .unwrap()
            .read_active_query_module(
                contract.lineage(),
                contract.contract_version(),
                contract.bundle_hash(),
            )
            .unwrap()
            .is_some()
    );
    let write = ports.shared.database.begin_write().unwrap();
    let mut audit = write.open_table(AUDIT).unwrap();
    let replacement = audit.first().unwrap().unwrap().1.value().to_vec();
    audit
        .insert(
            keys::encode_audit_key(administration_sequence).as_slice(),
            replacement.as_slice(),
        )
        .unwrap();
    drop(audit);
    ports.shared.commit_durable(write).unwrap();
    // Deliberate below-port corruption is outside composite publication. Pin
    // the newly corrupted engine root explicitly rather than asking the live
    // composite owner to replace its already captured valid historical view.
    let corrupted = crate::RedbOwnedSnapshot::from_read_access(ports.begin_read().unwrap());
    let error = corrupted
        .read_active_query_module(
            contract.lineage(),
            contract.contract_version(),
            contract.bundle_hash(),
        )
        .expect_err("a valid module body cannot authorize a substituted activation audit");
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
}

// req: TXN-042, STO-032, REC-004
#[test]
fn locator_generation_publication_never_splices_two_source_views() {
    // The helper captures an immutable source before genuine online retention,
    // then captures its successor after the single publication. The retained
    // old pin deliberately survives the intervening physical membership cut.
    let fixture = pruned_snapshot_fixture();
    let old = fixture.pinned.complete_logical_access_for_test();
    let new = fixture.current.complete_logical_access_for_test();
    let one = CommitSequence::first();
    let two = one.checked_next().unwrap();
    let old_generation = old.locator_capability().require_complete().unwrap();
    let new_generation = new.locator_capability().require_complete().unwrap();
    for (access, generation, command_one_present) in
        [(old, old_generation, true), (new, new_generation, false)]
    {
        assert_eq!(generation.has_command_at(one).unwrap(), command_one_present);
        assert_eq!(
            access
                .read_value(
                    crate::journal::JournalTable::Commits,
                    &keys::encode_application_sequence_key(one)
                )
                .unwrap()
                .is_some(),
            command_one_present,
            "a captured directory describes its own exact command source"
        );
        assert!(generation.has_command_at(two).unwrap());
        assert!(
            crate::command_authority::command_member_at_access(access, two)
                .unwrap()
                .is_some(),
            "the retained command resolves through the same captured segment source"
        );
        let identity_key = fixture.outcome.identity().storage_key().unwrap();
        let identity = keys::encode_idempotency_key(&identity_key);
        assert_eq!(
            generation
                .has_command_member(
                    riffdb_storage_api::CommandDerivedIndexKindV1::Idempotency,
                    identity,
                )
                .unwrap(),
            command_one_present
        );
    }
    assert_eq!(
        fixture
            .pinned
            .read_stored_outcome(fixture.outcome.identity())
            .unwrap(),
        Some(fixture.outcome.clone())
    );
    assert_eq!(
        fixture
            .current
            .read_stored_outcome(fixture.outcome.identity())
            .unwrap(),
        Some(fixture.outcome)
    );
}

// req: TXN-042, STO-032, REC-004
#[test]
fn native_snapshot_keeps_primary_and_derived_engines_locked_after_ports_close() {
    let PrunedSnapshotFixture {
        pinned,
        current,
        outcome,
        provenance,
        _scope: scope,
        ..
    } = pruned_snapshot_fixture();
    // The fixture's operational ports have already closed. Both captured views
    // must still own their actual primary engine, rather than only its pages.
    let derived_path = crate::store::derived_store_path(&scope.join("db.redb"));
    assert!(redb::Database::open(scope.join("db.redb")).is_err());
    assert!(redb::Database::open(&derived_path).is_err());
    assert_eq!(
        pinned.read_stored_outcome(outcome.identity()).unwrap(),
        Some(outcome.clone())
    );
    assert_eq!(
        pinned.read_provenance(provenance.provenance_id()).unwrap(),
        Some(provenance.clone())
    );
    assert!(pinned.derived_transaction().unwrap().list_tables().is_ok());
    let final_pin = pinned.clone();
    drop(pinned);
    drop(current);
    assert!(redb::Database::open(scope.join("db.redb")).is_err());
    assert!(redb::Database::open(&derived_path).is_err());
    assert_eq!(
        final_pin.read_stored_outcome(outcome.identity()).unwrap(),
        Some(outcome)
    );
    assert_eq!(
        final_pin
            .read_provenance(provenance.provenance_id())
            .unwrap(),
        Some(provenance)
    );
    assert!(
        final_pin
            .derived_transaction()
            .unwrap()
            .list_tables()
            .is_ok()
    );
    drop(final_pin);
    let reopened = redb::Database::open(scope.join("db.redb")).unwrap();
    let derived = redb::Database::open(&derived_path).unwrap();
    drop(derived);
    drop(reopened);
}
