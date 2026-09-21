//! Physical cutover over a normally materialized and fully validated V2 follower.
//! Fence values here are isolated storage evidence, not authenticated peer proof.
// req: REP-003, REP-005, REC-001
use super::*;
use redb::ReadableDatabase;
use riffdb_types::*;
use std::num::NonZeroU64;

#[path = "promotion_cutover_crash_tests.rs"]
mod crashes;

#[path = "promotion_reconciliation_tests.rs"]
mod reconciliation;

fn pending(history: ChangelogHistoryStateV3) -> ReplicationPromotionReceiptV1 {
    let lineage = history.lineage();
    let target = ReplicationFollowerAuditTargetV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        lineage.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([0x73; 16]).unwrap(),
    )
    .unwrap();
    let principal = AuditPrincipalV1::new(
        ActorId::new("promotion-operator").unwrap(),
        ActorKind::Human,
        CapabilityId::from_unix_milliseconds_and_random(1234, [8; 10]).unwrap(),
        NonZeroU64::new(1).unwrap(),
    );
    let observed = ChangelogHistoryPointV3::new(
        history.tail().sequence().checked_next().unwrap(),
        [9; 32],
        DualFrontier::new(
            history.tail().frontier().application(),
            Some(AdministrationSequence::first()),
        ),
    );
    let fence = StoredPrimaryFenceAdministrationV1::new(
        AdministrationSequence::new(2).unwrap(),
        Timestamp::new(1234, 0).unwrap(),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1234, [3; 10]).unwrap(),
        RequestId::from_unix_milliseconds_and_random(1234, [4; 10]).unwrap(),
        principal.clone(),
        None,
        target,
        history.anchor().sequence(),
        observed,
    )
    .unwrap();
    let fenced = ChangelogHistoryPointV3::new(
        observed.sequence().checked_next().unwrap(),
        [5; 32],
        DualFrontier::new(
            history.tail().frontier().application(),
            Some(fence.administration_sequence()),
        ),
    );
    let source =
        ChangelogHistoryStateV3::new(lineage, history.anchor(), fenced, history.minimum_resume())
            .unwrap();
    let request = ReplicationPromotionRequestV1::new(
        ReplicationPromotionOperationId::from_unix_milliseconds_and_random(1234, [6; 10]).unwrap(),
        fence.operation_id(),
        target,
        fence.generation(),
    );
    let selection = ReplicationPromotionSelectionV1::new(
        request,
        ReplicationFollowerStateV3::attached(lineage, history.tail(), Some(history.anchor()))
            .unwrap(),
        PrimaryFenceSourceEvidenceV1::new(fence, history.tail(), source).unwrap(),
    )
    .unwrap();
    let mut attempt = ReplicationPromotionReceiptV1::attempted(
        request,
        RequestId::from_unix_milliseconds_and_random(1234, [7; 10]).unwrap(),
        principal,
        None,
        Timestamp::new(1234, 1).unwrap(),
    );
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        attempt
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
    }
    attempt.record_selection(selection).unwrap();
    attempt
        .advance(ReplicationPromotionStepV1::Phase(
            ReplicationPromotionPhaseV1::CutoverPending,
        ))
        .unwrap();
    attempt
}

fn fixture(
    scope: &crate::test_path::ScopedDirectory,
) -> (
    std::path::PathBuf,
    RedbMaintenanceStorage,
    StoredPromotionAdministrationV1,
    ChangelogHistoryStateV3,
) {
    let mut source = crate::RedbStore::open(scope.join("source.redb")).unwrap();
    let id = DatabaseId::from_unix_milliseconds_and_random(1234, [1; 10]).unwrap();
    source.initialize_database(id).unwrap();
    let transfer = transfer_from_ports(
        scope,
        crate::RedbOperationalPorts {
            shared: source.shared,
        },
    );
    let history = transfer.manifest().fence().history();
    let path = scope.join("candidate");
    let mut materializer = RedbBootstrapMaterializer::create(&path, transfer).unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    drop(materializer.finish().unwrap().validate(inputs()).unwrap());
    let database_path = path.join("follower.redb");
    let mut owner =
        RedbMaintenanceStorage::open_for_promotion_recovery(&database_path, scope.join("backups"))
            .unwrap();
    let attempt = pending(history);
    // Persist the ordinary sequence of external publications before cutover.
    let mut published = ReplicationPromotionReceiptV1::attempted(
        attempt.request(),
        attempt.request_id(),
        attempt.principal().clone(),
        attempt.approval_id().cloned(),
        attempt.timestamp(),
    );
    owner.persist_promotion_receipt(&published).unwrap();
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        published
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
        owner.persist_promotion_receipt(&published).unwrap();
    }
    published
        .record_selection(attempt.selection().unwrap().clone())
        .unwrap();
    owner.persist_promotion_receipt(&published).unwrap();
    published
        .advance(ReplicationPromotionStepV1::Phase(
            ReplicationPromotionPhaseV1::CutoverPending,
        ))
        .unwrap();
    owner.persist_promotion_receipt(&published).unwrap();
    let record = StoredPromotionAdministrationV1::new(
        attempt,
        Timestamp::new(1234, 2).unwrap(),
        ServiceIngressKindV1::Grpc,
    )
    .unwrap();
    (database_path, owner, record, history)
}

#[test]
fn promotion_cutover_atomically_stamps_lineage_audit_and_complete_anchor() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-cutover-v2");
    let (database_path, mut owner, record, history) = fixture(&scope);
    owner.apply_promotion_cutover(&record).unwrap();
    let database = redb::Database::open(&database_path).unwrap();
    let read = database.begin_read().unwrap();
    let after = crate::changelog_v3_roots::validate_retained_history(&read)
        .unwrap()
        .unwrap();
    assert_eq!(
        after.lineage().history_incarnation(),
        history.lineage().history_incarnation() + 1
    );
    assert_eq!(
        after.lineage().leadership_epoch(),
        history.lineage().leadership_epoch().checked_next().unwrap()
    );
    assert_eq!(after.tail().frontier(), record.covered_frontier());
    assert_eq!(after.anchor(), after.tail());
    assert_eq!(after.tail().sequence().get(), 1);
    let receipt = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let receipt = receipt.get(1u64.to_be_bytes().as_slice()).unwrap().unwrap();
    let receipt = AuthoritativeTransactionV3::decode(receipt.value()).unwrap();
    assert_eq!(receipt.attribution(), ChangelogAttributionV3::Promotion);
    assert_eq!(
        receipt
            .mutations()
            .iter()
            .filter(|m| m.namespace() == AuthoritativeNamespaceV1::Audit)
            .count(),
        3
    );
    let audit = read.open_table(crate::layout::AUDIT).unwrap();
    let control = audit
        .get(crate::keys::encode_audit_key(record.administration_sequence()).as_slice())
        .unwrap()
        .unwrap();
    assert_eq!(
        crate::codec::decode_administration_audit_record_v1(control.value())
            .unwrap()
            .value(),
        &StoredAdministrationAuditRecordV1::Promotion(Box::new(record.clone()))
    );
    // A committed stamp alone cannot publish primary readiness or discard the ledger.
    assert!(owner.reconcile().is_err());
    drop(control);
    drop(audit);
    drop(read);
    drop(database);
    assert!(owner.apply_promotion_cutover(&record).is_err());
    assert!(crate::RedbStore::open(&database_path).is_err());
    assert!(crate::RedbFollowerStore::open(&database_path).is_err());
    let database = redb::Database::open(&database_path).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
            .unwrap(),
        Some(after)
    );
}

#[test]
fn promotion_cutover_refuses_missing_audit_indexes_or_substituted_anchor() {
    use redb::ReadableTable;
    for damage in 0..8 {
        let scope = crate::test_path::ScopedDirectory::new("promotion-cutover-corruption");
        let (path, mut owner, record, _) = fixture(&scope);
        owner.apply_promotion_cutover(&record).unwrap();
        let database = redb::Database::open(&path).unwrap();
        let write = database.begin_write().unwrap();
        match damage {
            0..=2 => {
                let sequence = [
                    record.started_sequence(),
                    record.administration_sequence(),
                    record.succeeded_sequence(),
                ][damage];
                write
                    .open_table(crate::layout::AUDIT)
                    .unwrap()
                    .remove(crate::keys::encode_audit_key(sequence).as_slice())
                    .unwrap();
            }
            3..=4 => {
                let sequence = [record.started_sequence(), record.succeeded_sequence()][damage - 3];
                write
                    .open_table(crate::layout::AUDIT_BY_REQUEST)
                    .unwrap()
                    .remove(
                        crate::keys::encode_audit_by_request_key(
                            record.attempt().request_id(),
                            sequence,
                        )
                        .as_slice(),
                    )
                    .unwrap();
            }
            5 => {
                write
                    .open_table(crate::changelog_v3_activation::HISTORY)
                    .unwrap()
                    .remove(1u64.to_be_bytes().as_slice())
                    .unwrap();
            }
            6 => {
                let mut meta = write.open_table(crate::layout::META).unwrap();
                let encoded =
                    proto_codec::encode_leadership_epoch_v1(LeadershipEpochV1::initial()).unwrap();
                meta.insert(
                    AuthoritativeNamespaceV1::LeadershipEpoch
                        .metadata_key()
                        .unwrap(),
                    encoded.as_bytes(),
                )
                .unwrap();
            }
            7 => {
                let mut table = write
                    .open_table(crate::changelog_v3_activation::HISTORY)
                    .unwrap();
                let original = {
                    let row = table.get(1u64.to_be_bytes().as_slice()).unwrap().unwrap();
                    AuthoritativeTransactionV3::decode(row.value()).unwrap()
                };
                let false_receipt = AuthoritativeTransactionV3::new_for_catalog(
                    original.binding(),
                    ChangelogAttributionV3::Promotion,
                    vec![],
                    AuthoritativeStateCatalogV2.digest(),
                )
                .unwrap();
                table
                    .insert(
                        1u64.to_be_bytes().as_slice(),
                        false_receipt.encode().unwrap().as_slice(),
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        write.commit().unwrap();
        assert!(
            crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
                .is_err(),
            "damage {damage}"
        );
    }
}

#[test]
fn promotion_cutover_refuses_live_readers_missing_ledger_and_incompatible_marker() {
    for damage in 0..4 {
        let scope = crate::test_path::ScopedDirectory::new("promotion-cutover-preflight");
        let (path, mut owner, record, before) = fixture(&scope);
        let live = if damage == 0 {
            Some(crate::RedbFollowerStore::open(&path).unwrap())
        } else {
            None
        };
        if damage == 1 {
            let receipt = scope
                .join("backups/.maintenance/replication_promotion")
                .join(format!("{}.receipt-v1", record.attempt().request_id()));
            std::fs::remove_file(receipt).unwrap();
        }
        if damage == 2 {
            std::fs::write(crate::durable_format_marker_path(&path), b"invalid marker").unwrap();
        }
        if damage == 3 {
            std::fs::write(
                crate::journal::journal_path(&path),
                b"unreconciled source journal",
            )
            .unwrap();
        }
        assert!(
            owner.apply_promotion_cutover(&record).is_err(),
            "damage {damage}"
        );
        drop(live);
        let database = redb::Database::open(&path).unwrap();
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
                .unwrap(),
            Some(before)
        );
    }
}

#[test]
fn promotion_audit_binding_survives_physical_anchor_reclamation() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-reclaimed-anchor");
    let (path, mut owner, record, _) = fixture(&scope);
    owner.apply_promotion_cutover(&record).unwrap();
    let database = redb::Database::open(&path).unwrap();
    let original = crate::promotion_cutover::history(&record).unwrap();
    // An isolated valid later no-op receipt and reclamation preserve the original
    // anchor hash. Production reclamation still has its independent hold checks.
    let receipt = AuthoritativeTransactionV3::new_for_catalog(
        AuthoritativeTransactionBindingV3 {
            database_id: original.lineage().database_id(),
            history_incarnation: original.lineage().history_incarnation(),
            predecessor: Some(original.tail().sequence()),
            sequence: original.tail().sequence().checked_next().unwrap(),
            predecessor_frontier: original.tail().frontier(),
            covered_frontier: original.tail().frontier(),
            prior_history_hash: original.tail().history_hash(),
        },
        ChangelogAttributionV3::ReplicationSourceHold,
        vec![],
        original.lineage().catalog_digest(),
    )
    .unwrap();
    let advanced = original.advance(&receipt).unwrap();
    let retained = ChangelogHistoryStateV3::new(
        advanced.lineage(),
        advanced.anchor(),
        advanced.tail(),
        advanced.tail(),
    )
    .unwrap();
    let write = database.begin_write().unwrap();
    {
        let mut table = write
            .open_table(crate::changelog_v3_activation::HISTORY)
            .unwrap();
        table.remove(1u64.to_be_bytes().as_slice()).unwrap();
        table
            .insert(
                2u64.to_be_bytes().as_slice(),
                receipt.encode().unwrap().as_slice(),
            )
            .unwrap();
        let mut meta = write.open_table(crate::layout::META).unwrap();
        meta.insert(
            AuthoritativeNamespaceV1::ChangelogHistoryState
                .metadata_key()
                .unwrap(),
            proto_codec::encode_changelog_history_state_v3(retained)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
        meta.insert(
            AuthoritativeNamespaceV1::NextChangelogTransaction
                .metadata_key()
                .unwrap(),
            proto_codec::encode_changelog_transaction_allocator_v3(retained.expected_allocator())
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    }
    write.commit().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
            .unwrap(),
        Some(retained)
    );
    drop(database);
    assert!(crate::RedbStore::open(&path).is_err());
    let database = redb::Database::open(&path).unwrap();
    // Even a self-consistent, freshly checksummed replacement of the whole audit
    // pair/control cannot claim the original immutable anchor's identity.
    let changed = StoredPromotionAdministrationV1::new(
        record.attempt().clone(),
        Timestamp::new(1235, 0).unwrap(),
        record.ingress(),
    )
    .unwrap();
    let write = database.begin_write().unwrap();
    {
        let mut audit = write.open_table(crate::layout::AUDIT).unwrap();
        audit
            .insert(
                crate::keys::encode_audit_key(changed.administration_sequence()).as_slice(),
                proto_codec::encode_promotion_administration_v1(&changed)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
        for row in changed.service_audits().unwrap() {
            audit
                .insert(
                    crate::keys::encode_audit_key(row.administration_sequence()).as_slice(),
                    proto_codec::encode_service_audit_record_v3(&row)
                        .unwrap()
                        .as_bytes(),
                )
                .unwrap();
        }
    }
    write.commit().unwrap();
    assert!(
        crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
            .is_err()
    );
}
