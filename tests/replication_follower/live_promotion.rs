//! Public TLS promotion through the live daemon owner; no direct storage cutover.
// req: REP-005, REC-001
use super::{primary_fence, proxy, support::*, workload};
use riffdb_client_rust::{AttemptBudget, BearerCredential, CallMetadata, v1};
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    NoChangelogPublicationPort, StorageScanLimit, StoredAdministrationAuditRecordV1,
};
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1 as Phase, ServiceOperationV1};
use std::sync::{Arc, atomic::AtomicBool};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_promotion_drains_follower_and_serves_new_lineage_without_old_primary() {
    for profile in ["standard", "hardened"] {
        live_promotion(profile).await;
    }
}

async fn live_promotion(profile: &str) {
    let fixture = Fixture::new();
    set_profile(&fixture, "primary", profile);
    let mut primary = fixture.start("primary", None);
    stop(&mut primary);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let replication = create_replication_capability(&mut client, &admin).await;
    workload::deploy(&mut client, &admin).await;
    let fence_token = primary_fence::create_fence_capability(&mut client, &admin).await;
    let target = v1::ReplicationFollowerTarget {
        database_id: lineage.database_id().as_bytes().to_vec(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        hold_id: vec![1; 16],
    };
    let Some(v1::register_follower_response::Result::Receipt(registered)) = client
        .register_follower(
            v1::RegisterFollowerRequest {
                request_id: request_id(180),
                target: Some(target.clone()),
                hold_budget_sequences: 100,
                expires_at_application_sequence: None,
            },
            &admin,
        )
        .await
        .unwrap()
        .result
    else {
        panic!("registered follower")
    };
    let relay = proxy::Proxy::start(fixture.channel("primary").await).await;
    let attached = relay.observe_attached_frame();
    fixture.configure_follower_via(lineage, &replication, relay.endpoint());
    set_profile(&fixture, "follower", profile);
    let mut follower = fixture.start("follower", Some("follower"));
    tokio::time::timeout(std::time::Duration::from_secs(30), attached)
        .await
        .unwrap()
        .unwrap();
    let fence = client
        .fence_replication_primary(
            v1::FenceReplicationPrimaryRequest {
                request_id: request_id(181),
                operation_id: request_id(182),
                target: Some(target.clone()),
                registration_generation: registered.registration_generation,
            },
            &CallMetadata::authenticated(BearerCredential::new(&fence_token).unwrap()),
        )
        .await
        .unwrap();
    assert!(matches!(
        fence.result,
        Some(v1::fence_replication_primary_response::Result::Receipt(_))
    ));
    let mut promoted = fixture.client("follower").await;
    let mut request = v1::PromoteFollowerRequest {
        request_id: request_id(183),
        operation_id: request_id(184),
        fence_operation_id: request_id(182),
        target: Some(target.clone()),
        registration_generation: registered.registration_generation,
    };
    let result = promoted
        .promote_follower(request.clone(), &admin)
        .await
        .expect("authorized live promotion must complete its daemon handoff");
    let receipt = result.receipt.unwrap();
    assert_eq!(receipt.target, Some(target));
    assert_eq!(
        receipt.history_incarnation,
        lineage.history_incarnation() + 1
    );
    assert_eq!(
        receipt.leadership_epoch,
        lineage.leadership_epoch().get() + 1
    );
    assert_eq!(receipt.application_rpo, 0);
    assert!(!receipt.replayed);
    drop(client);
    stop(&mut primary);
    relay.shutdown().await;
    let root = fixture.database("follower").parent().unwrap().to_path_buf();
    std::fs::remove_file(root.join("replication.token")).unwrap();
    // Exact retries use current source authority and the original committed
    // result, even when the former primary and its credential are unavailable.
    request.request_id = request_id(185);
    let retried = promoted
        .promote_follower(request.clone(), &admin)
        .await
        .expect("authorized promotion retry must use committed source authority")
        .receipt
        .unwrap();
    let mut expected_replay = receipt.clone();
    expected_replay.replayed = true;
    assert_eq!(retried, expected_replay);
    let mut denied = request.clone();
    denied.request_id = request_id(187);
    let error = promoted
        .promote_follower(
            denied,
            &CallMetadata::authenticated(BearerCredential::new(&replication).unwrap()),
        )
        .await
        .expect_err("replication authority must not authorize a promotion retry");
    assert!(
        matches!(error, riffdb_client_rust::ClientError::Public(error)
        if error.kind() == riffdb_errors::PublicErrorKind::AuthorizationDenied)
    );
    let mut conflicting = request.clone();
    conflicting.request_id = request_id(188);
    conflicting.registration_generation += 1;
    let error = promoted
        .promote_follower(conflicting, &admin)
        .await
        .expect_err("a committed operation cannot acquire a different generation");
    assert!(
        matches!(error, riffdb_client_rust::ClientError::Public(error)
        if error.kind() == riffdb_errors::PublicErrorKind::IdempotencyKeyReuse)
    );
    let authority = workload::authority_with_seed(&mut promoted, &admin, 190).await;
    let mut app = riffdb_ticketdesk::TicketDeskClient::new(
        fixture.application_client_for("follower").await,
        authority,
        AttemptBudget::new(2).unwrap(),
    );
    let input = riffdb_ticketdesk::CreateOrganizationInput {
        organization_id: "018f0000-0000-7000-8000-000000000097".into(),
        name: "live promotion".into(),
        idempotency_key: "live-promotion-first-write".into(),
    };
    let committed = app.create_organization(input.clone()).await.unwrap();
    assert_eq!(committed.commit_sequence, Some(1));
    assert!(!committed.replayed);
    let replay = app.create_organization(input).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.outcome, committed.outcome);
    assert_eq!(replay.outcome_uri, committed.outcome_uri);
    drop(app);
    drop(promoted);
    stop(&mut follower);
    follower = fixture.start("follower", Some("follower"));
    let mut restarted = fixture.client("follower").await;
    request.request_id = request_id(186);
    let retried = restarted
        .promote_follower(request, &admin)
        .await
        .expect("promotion retry must survive a fresh source graph")
        .receipt
        .unwrap();
    assert_eq!(retried, expected_replay);
    drop(restarted);
    stop(&mut follower);
    verify_retry_audit(&fixture, &receipt);
}

fn verify_retry_audit(fixture: &Fixture, receipt: &v1::FollowerPromotionReceipt) {
    let path = fixture.database("follower");
    let mut owner = riffdb_storage_redb::RedbMaintenanceStorage::open_for_promotion_recovery(
        &path,
        path.parent().unwrap().join("follower-backups"),
    )
    .unwrap();
    let record = owner.discover_committed_promotion().unwrap().unwrap();
    let before = owner.promotion_receipts().unwrap();
    assert_eq!(
        before.receipts().len(),
        1,
        "source retries must use normal audit"
    );
    assert_eq!(
        record.administration_sequence().get(),
        receipt.administration_sequence
    );
    let store = owner
        .reconcile_committed_promotion(
            &record,
            startup_inputs(),
            Arc::new(AtomicBool::new(false)),
            riffdb_storage_redb::RedbCommitProfile::Hardened,
            Arc::new(NoChangelogPublicationPort),
        )
        .unwrap();
    let ports = validate_primary_store(store);
    let AdministrationAuditScan::ExactEnd { records } = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(256).unwrap(),
        ))
        .unwrap()
    else {
        panic!("bounded audit inventory must be complete");
    };
    let controls = records
        .iter()
        .filter(|row| matches!(row.value(), StoredAdministrationAuditRecordV1::Promotion(_)))
        .count();
    assert_eq!(controls, 1, "retries must not mint another promotion");
    let audits: Vec<_> = records
        .into_iter()
        .filter_map(|row| match row.into_parts().0 {
            StoredAdministrationAuditRecordV1::Service(audit)
                if audit.operation() == ServiceOperationV1::PromoteFollower =>
            {
                Some(audit)
            }
            _ => None,
        })
        .collect();
    for (seed, phases) in [
        (183, vec![Phase::Started, Phase::Succeeded]),
        (185, vec![Phase::Started, Phase::Succeeded]),
        (186, vec![Phase::Started, Phase::Succeeded]),
        (187, vec![Phase::Denied]),
        (188, vec![Phase::Started, Phase::Failed]),
    ] {
        let attempt: Vec<_> = audits
            .iter()
            .filter(|r| r.request_id().as_bytes().as_slice() == request_id(seed))
            .collect();
        assert_eq!(
            attempt.iter().map(|r| r.phase()).collect::<Vec<_>>(),
            phases
        );
        for audit in attempt {
            assert_eq!(
                audit.principal().unwrap().capability_id(),
                capability_id(if seed == 187 { 2 } else { 1 })
            );
            assert_eq!(
                audit.link(),
                if audit.phase() == Phase::Succeeded {
                    ServiceAuditLinkV1::ControlPlane {
                        administration_sequence: record.administration_sequence(),
                    }
                } else {
                    ServiceAuditLinkV1::None
                }
            );
        }
    }
    assert_eq!(owner.promotion_receipts().unwrap(), before);
}

pub(super) fn set_profile(fixture: &Fixture, name: &str, profile: &str) {
    let path = fixture.config(name);
    let document = std::fs::read_to_string(&path).unwrap().replacen(
        "[server]\n",
        &format!("[server]\nredb_commit_profile = {profile:?}\n"),
        1,
    );
    std::fs::write(path, document).unwrap();
}
