// req: REP-006, REC-001, STO-012

use super::*;
use riffdb_types::{
    LeadershipEpochV1, ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1,
};

fn follower_targets(seed: u8) -> ServiceAuditTargetsV1 {
    ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ReplicationFollower(
        ReplicationFollowerAuditTargetV1::new(
            database_id(),
            1,
            LeadershipEpochV1::initial(),
            ReplicationSourceHoldIdV1::new([seed; 16]).unwrap(),
        )
        .unwrap(),
    )])
    .unwrap()
}

fn intent(request: u8, phase: ServiceAuditPhaseV1, follower: bool) -> ServiceAuditAppendIntentV1 {
    // Exercise the already checked audit vocabulary without activating a public
    // follower operation. Operation-specific control links are a separate gate.
    ServiceAuditAppendIntentV1::new(
        request_id(request),
        Timestamp::new(i64::from(request), 0).unwrap(),
        ServiceOperationV1::GetHealth,
        phase,
        principal(capability_id(2)),
        ServiceIngressKindV1::Grpc,
        if follower {
            follower_targets(9)
        } else {
            ServiceAuditTargetsV1::empty()
        },
        None,
        ServiceAuditLinkV1::None,
    )
    .unwrap()
}

fn records(ports: &RedbOperationalPorts) -> Vec<StoredServiceAuditRecordV1> {
    let AdministrationAuditScan::ExactEnd { records } = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).unwrap(),
        ))
        .unwrap()
    else {
        panic!("bounded fixture must reach its exact end")
    };
    records
        .into_iter()
        .map(|item| match item.into_parts().0 {
            StoredAdministrationAuditRecordV1::Service(record) => record,
            _ => panic!("fixture contains only service audit"),
        })
        .collect()
}

#[test]
fn production_audit_writer_selects_v3_only_for_follower_targets_and_recovers_mixed_phases() {
    let (path, mut ports) = initialized_ports("follower-audit-generations");
    let old = intent(50, ServiceAuditPhaseV1::Denied, false);
    ports.append_service_audit_group(&[old]).unwrap();
    for (offset, phase) in [
        ServiceAuditPhaseV1::Denied,
        ServiceAuditPhaseV1::Succeeded,
        ServiceAuditPhaseV1::Failed,
        ServiceAuditPhaseV1::Cancelled,
        ServiceAuditPhaseV1::OutcomeUncertain,
    ]
    .into_iter()
    .enumerate()
    {
        let request = 60 + u8::try_from(offset).unwrap();
        if phase != ServiceAuditPhaseV1::Denied {
            ports
                .append_service_audit_group(&[intent(request, ServiceAuditPhaseV1::Started, true)])
                .unwrap();
        }
        let result = ports
            .append_service_audit_group(&[intent(request, phase, true)])
            .unwrap();
        assert!(matches!(
            result.as_slice(),
            [ServiceAuditAppendResult::Appended(_)]
        ));
    }
    let expected = records(&ports);
    assert_eq!(expected.len(), 10);
    for record in &expected {
        let encoded = encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Service(record.clone()),
        )
        .unwrap();
        let canonical = if record.targets().is_empty() {
            riffdb_storage_api::encode_service_audit_record_v2(record).unwrap()
        } else {
            assert_eq!(record.targets(), &follower_targets(9));
            assert!(riffdb_storage_api::encode_service_audit_record_v2(record).is_err());
            riffdb_storage_api::encode_service_audit_record_v3(record).unwrap()
        };
        assert_eq!(encoded.as_bytes(), canonical.as_bytes());
    }
    drop(ports);
    let store = RedbStore::open(&path.0).unwrap();
    let reopened = crate::store::RedbDormantPorts {
        pending_v3_activation: None,
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .unwrap();
    assert_eq!(records(&reopened), expected);
}

#[test]
fn follower_audit_terminal_refuses_substitution_of_each_target_component() {
    let (_path, mut ports) = initialized_ports("follower-audit-target-substitution");
    for (offset, phase) in [
        ServiceAuditPhaseV1::Succeeded,
        ServiceAuditPhaseV1::Failed,
        ServiceAuditPhaseV1::Cancelled,
        ServiceAuditPhaseV1::OutcomeUncertain,
    ]
    .into_iter()
    .enumerate()
    {
        let request = 70 + u8::try_from(offset).unwrap();
        ports
            .append_service_audit_group(&[intent(request, ServiceAuditPhaseV1::Started, true)])
            .unwrap();
        let terminal = intent(request, phase, true);
        let count = audit_count(&ports);
        for component in 0..4 {
            let target = ReplicationFollowerAuditTargetV1::new(
                if component == 0 {
                    DatabaseId::from_bytes(uuid_bytes(8)).unwrap()
                } else {
                    database_id()
                },
                if component == 1 { 2 } else { 1 },
                LeadershipEpochV1::new(if component == 2 { 2 } else { 1 }).unwrap(),
                ReplicationSourceHoldIdV1::new([if component == 3 { 10 } else { 9 }; 16]).unwrap(),
            )
            .unwrap();
            let substituted = ServiceAuditAppendIntentV1::new(
                terminal.request_id(),
                terminal.timestamp(),
                terminal.operation(),
                terminal.phase(),
                terminal.principal().unwrap().clone(),
                terminal.ingress(),
                ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ReplicationFollower(target)])
                    .unwrap(),
                None,
                terminal.link(),
            )
            .unwrap();
            assert!(matches!(
                ports
                    .append_service_audit_group(&[substituted])
                    .unwrap()
                    .as_slice(),
                [ServiceAuditAppendResult::PhaseConflict]
            ));
            assert_eq!(audit_count(&ports), count);
        }
        assert!(matches!(
            ports
                .append_service_audit_group(&[terminal])
                .unwrap()
                .as_slice(),
            [ServiceAuditAppendResult::Appended(_)]
        ));
    }
}

#[test]
fn follower_audit_generation_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_FOLLOWER_AUDIT_GENERATION_PATH") else {
        return;
    };
    let mut store =
        RedbStore::open_with_commit_profile(path, crate::store::RedbCommitProfile::Hardened)
            .unwrap();
    store.initialize_database(database_id()).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        riffdb_storage_api::ChangelogLineageV3::new(database_id(), 1, LeadershipEpochV1::initial())
            .unwrap(),
        riffdb_types::DualFrontier::INITIAL,
    )
    .unwrap();
    let mut ports = crate::store::RedbDormantPorts {
        pending_v3_activation: None,
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .unwrap();
    ports
        .append_service_audit_group(&[
            intent(80, ServiceAuditPhaseV1::Denied, false),
            intent(81, ServiceAuditPhaseV1::Denied, true),
        ])
        .unwrap();
    panic!("requested crash edge must exit the child");
}

#[test]
fn mixed_follower_audit_generations_crash_atomically_with_their_physical_receipt() {
    for edge in ["mutations", "receipt", "roots", "committed"] {
        let scope = crate::test_path::ScopedDirectory::new("mixed-follower-audit-crash");
        let path = scope.join("db.redb");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "administration::tests::audit_generations::follower_audit_generation_crash_child",
                "--nocapture",
            ])
            .env("RIFFDB_FOLLOWER_AUDIT_GENERATION_PATH", &path)
            .env("RIFFDB_V3_DIRECT_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        let mut prior = None;
        for _ in 0..2 {
            let database = redb::Database::open(&path).unwrap();
            let root = database.begin_read().unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&root)
                .unwrap()
                .unwrap();
            let committed = edge == "committed";
            assert_eq!(
                history.tail().sequence().get(),
                if committed { 2 } else { 1 }
            );
            let audit = root.open_table(AUDIT).unwrap();
            assert_eq!(audit.len().unwrap(), if committed { 2 } else { 0 });
            let mut actual = Vec::new();
            for row in audit.iter().unwrap() {
                let (_, bytes) = row.unwrap();
                let decoded =
                    riffdb_storage_api::decode_service_audit_record(bytes.value()).unwrap();
                let record = decoded.value();
                let expected = if record.targets().is_empty() {
                    riffdb_storage_api::encode_service_audit_record_v2(record).unwrap()
                } else {
                    assert_eq!(record.targets(), &follower_targets(9));
                    riffdb_storage_api::encode_service_audit_record_v3(record).unwrap()
                };
                assert_eq!(bytes.value(), expected.as_bytes());
                actual.push(bytes.value().to_vec());
            }
            assert_eq!(actual.len(), if committed { 2 } else { 0 });
            let receipts = root
                .open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap();
            let receipt = receipts
                .get(2u64.to_be_bytes().as_slice())
                .unwrap()
                .map(|row| row.value().to_vec());
            assert_eq!(receipt.is_some(), committed);
            if let Some(bytes) = &receipt {
                let checked =
                    riffdb_storage_api::AuthoritativeTransactionV3::decode(bytes).unwrap();
                assert_eq!(
                    checked
                        .binding()
                        .covered_frontier
                        .administration()
                        .unwrap()
                        .get(),
                    2
                );
                assert_eq!(checked.attribution(), riffdb_storage_api::ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup);
            }
            let observed = (actual, receipt);
            if let Some(prior) = &prior {
                assert_eq!(&observed, prior);
            }
            prior = Some(observed);
        }
    }
}
