//! Restart before cutover exposes only freshly authenticated exact retry.
// req: REP-005, REC-001, STO-012
use super::{live_promotion::set_profile, oracle, primary_fence, proxy, support::*, workload};
use riffdb_client_rust::{AttemptBudget, BearerCredential, CallMetadata, v1};
use riffdb_errors::PublicErrorKind;
use riffdb_service::ReplicationSourcePort;
use riffdb_storage_api::*;
use riffdb_storage_redb::RedbMaintenanceStorage;
use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};
use riffdb_types::*;
use std::{num::NonZeroU64, time::Duration};

#[path = "promotion_retry_reads.rs"]
mod reads;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_promotion_restarts_restricted_then_exact_retry_serves_one_new_lineage() {
    for profile in ["standard", "hardened"] {
        scenario(profile, None).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_promotion_shutdown_releases_custody_before_a_fresh_exact_retry() {
    for profile in ["standard", "hardened"] {
        scenario(profile, Some("signal-offline")).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_promotion_recovers_each_durable_cutover_boundary() {
    for profile in ["standard", "hardened"] {
        for point in [
            "draining",
            "offline",
            "selected",
            "cutover-pending",
            "cutover-committed",
        ] {
            scenario(profile, Some(point)).await;
        }
    }
}

async fn scenario(profile: &str, abort: Option<&str>) {
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
    let target = ReplicationFollowerAuditTargetV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        lineage.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
    )
    .unwrap();
    let wire_target = v1::ReplicationFollowerTarget {
        database_id: lineage.database_id().as_bytes().to_vec(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        hold_id: vec![1; 16],
    };
    let Some(v1::register_follower_response::Result::Receipt(registered)) = client
        .register_follower(
            v1::RegisterFollowerRequest {
                request_id: request_id(200),
                target: Some(wire_target.clone()),
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
    tokio::time::timeout(Duration::from_secs(30), attached)
        .await
        .unwrap()
        .unwrap();
    stop(&mut follower);
    let snapshot = oracle::capture_follower(&fixture.database("follower"));
    let fenced = client
        .fence_replication_primary(
            v1::FenceReplicationPrimaryRequest {
                request_id: request_id(201),
                operation_id: request_id(202),
                target: Some(wire_target.clone()),
                registration_generation: registered.registration_generation,
            },
            &CallMetadata::authenticated(BearerCredential::new(&fence_token).unwrap()),
        )
        .await
        .unwrap();
    assert!(matches!(
        fenced.result,
        Some(v1::fence_replication_primary_response::Result::Receipt(_))
    ));
    let request = ReplicationPromotionRequestV1::new(
        ReplicationPromotionOperationId::from_bytes(request_id(203).try_into().unwrap()).unwrap(),
        ReplicationFenceOperationId::from_bytes(request_id(202).try_into().unwrap()).unwrap(),
        target,
        ChangelogTransactionSequence::new(registered.registration_generation).unwrap(),
    );
    let peer = riffdb_server::VerifiedReplicationPeer::connect(
        fixture.tls_config("primary"),
        riffdb_auth::RawCapabilityToken::parse_canonical(replication.as_bytes()).unwrap(),
        DatabaseAlias::default_alias(),
    )
    .await
    .unwrap();
    let proof = peer
        .primary_fence_source_evidence(
            PrimaryFenceRequestV1::new(
                RequestId::from_bytes(request_id(204).try_into().unwrap()).unwrap(),
                request.fence_operation_id(),
                target,
                request.generation(),
            ),
            snapshot.position(),
        )
        .await
        .unwrap();
    drop(peer);
    drop(client);
    stop(&mut primary);
    relay.shutdown().await;
    // The daemon must reach restricted admission with neither a live source nor
    // a readable peer credential. Reconfigure the later retry to the real source.
    let root = fixture.database("follower").parent().unwrap().to_path_buf();
    std::fs::remove_file(root.join("replication.token")).unwrap();
    fixture.configure_follower(lineage, &replication);
    set_profile(&fixture, "follower", profile);
    std::fs::rename(
        root.join("replication.token"),
        root.join("replication.saved"),
    )
    .unwrap();
    let selection = ReplicationPromotionSelectionV1::new(
        request,
        ReplicationFollowerStateV3::attached(snapshot.lineage(), snapshot.position(), None)
            .unwrap(),
        proof,
    )
    .unwrap();
    let mut owner = RedbMaintenanceStorage::open_for_promotion_recovery(
        fixture.database("follower"),
        root.join("follower-backups"),
    )
    .unwrap();
    let mut original = ReplicationPromotionReceiptV1::attempted(
        request,
        RequestId::from_bytes(request_id(205).try_into().unwrap()).unwrap(),
        AuditPrincipalV1::new(
            ActorId::new("interrupted-promotion-fixture").unwrap(),
            ActorKind::Human,
            capability_id(1),
            NonZeroU64::MIN,
        ),
        None,
        now(),
    );
    owner.persist_promotion_receipt(&original).unwrap();
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        original
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
        owner.persist_promotion_receipt(&original).unwrap();
    }
    original.record_selection(selection.clone()).unwrap();
    owner.persist_promotion_receipt(&original).unwrap();
    // This interrupted invocation remains nonterminal even after another one
    // succeeds. Historical audit must not be rewritten into a fabricated reply.
    drop(owner);
    follower = restricted(&fixture, abort);
    let mut client = fixture.client("follower").await;
    let health = client
        .health(
            v1::HealthRequest {
                request_id: Some(request_id(206)),
            },
            &admin,
        )
        .await;
    assert!(
        health.is_err(),
        "retry-only hosting exposed ordinary Health"
    );
    let invocation = |seed| v1::PromoteFollowerRequest {
        request_id: request_id(seed),
        operation_id: request_id(203),
        fence_operation_id: request_id(202),
        target: Some(wire_target.clone()),
        registration_generation: registered.registration_generation,
    };
    let mut foreign = invocation(207);
    foreign.operation_id = request_id(208);
    let denied = client.promote_follower(foreign, &admin).await.unwrap_err();
    assert_eq!(
        denied.public_error().map(|error| error.kind()),
        Some(PublicErrorKind::AuthorizationDenied)
    );
    let denied = client
        .promote_follower(
            invocation(209),
            &CallMetadata::authenticated(BearerCredential::new(&replication).unwrap()),
        )
        .await
        .unwrap_err();
    assert_eq!(
        denied.public_error().map(|error| error.kind()),
        Some(PublicErrorKind::AuthorizationDenied)
    );
    // No unauthorized invocation contacted the source or needed its credential.
    if abort.is_none() {
        let unavailable = client
            .promote_follower(invocation(214), &admin)
            .await
            .unwrap_err();
        assert_eq!(
            unavailable.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::StorageUnavailable)
        );
        follower
            .wait_for_readiness("riffdb-promotion-retry-fixture-v1", Duration::from_secs(60))
            .unwrap();
    }
    primary = fixture.start("primary", None);
    std::fs::rename(
        root.join("replication.saved"),
        root.join("replication.token"),
    )
    .unwrap();
    let submission = client.promote_follower(invocation(210), &admin).await;
    let committed_before_crash = abort == Some("cutover-committed");
    let result = if abort.is_some() {
        use std::os::unix::process::ExitStatusExt;
        assert!(
            submission.is_err(),
            "an interrupted cutover cannot claim a completed handoff"
        );
        let exit = follower.wait_for_exit(Duration::from_secs(30)).unwrap();
        if abort == Some("signal-offline") {
            assert!(
                exit.status.success(),
                "signal shutdown must release the old engine cleanly"
            );
        } else {
            assert_eq!(
                exit.status.signal(),
                Some(6),
                "fixture must reach the armed boundary"
            );
        }
        drop(client);
        follower = if committed_before_crash {
            fixture.start("follower", Some("follower"))
        } else {
            restricted(&fixture, None)
        };
        client = fixture.client("follower").await;
        let result = client
            .promote_follower(invocation(212), &admin)
            .await
            .unwrap()
            .receipt
            .unwrap();
        if !committed_before_crash {
            follower
                .wait_for_readiness("riffdbd-ready-v1\t", Duration::from_secs(60))
                .unwrap();
        }
        result
    } else {
        let result = submission.unwrap().receipt.unwrap();
        follower
            .wait_for_readiness("riffdbd-ready-v1\t", Duration::from_secs(60))
            .unwrap();
        result
    };
    assert_eq!(
        result.history_incarnation,
        lineage.history_incarnation() + 1
    );
    assert_eq!(
        result.leadership_epoch,
        lineage.leadership_epoch().get() + 1
    );
    assert_eq!(result.application_rpo, selection.application_rpo());
    assert_eq!(result.replayed, committed_before_crash);
    stop(&mut primary);
    std::fs::remove_file(root.join("replication.token")).unwrap();
    let authority = workload::authority_with_seed(&mut client, &admin, 211).await;
    let mut app = riffdb_ticketdesk::TicketDeskClient::new(
        fixture.application_client_for("follower").await,
        authority,
        AttemptBudget::new(2).unwrap(),
    );
    let input = riffdb_ticketdesk::CreateOrganizationInput {
        organization_id: "018f0000-0000-7000-8000-000000000096".into(),
        name: "recovered promotion".into(),
        idempotency_key: "promotion-retry-first-write".into(),
    };
    let created = app.create_organization(input.clone()).await.unwrap();
    assert_eq!(created.commit_sequence, Some(1));
    assert!(!created.replayed);
    let replay = app.create_organization(input).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.outcome, created.outcome);
    assert_eq!(replay.outcome_uri, created.outcome_uri);
    let (reader, read) = reads::authority(&mut client, &admin).await;
    let entity = reads::read(&mut client, &reader, read.clone(), 226).await;
    assert_eq!(entity.entity_version, 1);
    assert!(entity.fields.as_ref().unwrap().fields.iter().any(
        |field| matches!(field.value.as_ref().and_then(|v| v.kind.as_ref()),
            Some(v1::value::Kind::StringValue(value)) if value == "recovered promotion")
    ));
    drop(app);
    drop(client);
    stop(&mut follower);
    follower = fixture.start("follower", Some("follower"));
    let mut client = fixture.client("follower").await;
    assert_eq!(reads::read(&mut client, &reader, read, 227).await, entity);
    drop(client);
    stop(&mut follower);
    let owner = RedbMaintenanceStorage::open_for_promotion_recovery(
        fixture.database("follower"),
        root.join("follower-backups"),
    )
    .unwrap();
    let record = owner.discover_committed_promotion().unwrap().unwrap();
    assert_eq!(record.attempt().selection(), Some(&selection));
    assert_eq!(
        record.attempt().request_id().as_bytes().as_slice(),
        request_id(if abort.is_some() && !committed_before_crash {
            212
        } else {
            210
        })
    );
    let inventory = owner.promotion_receipts().unwrap();
    assert!(inventory.receipts().contains(&original));
    assert_eq!(
        inventory.receipts().len(),
        if !committed_before_crash { 5 } else { 4 }
    );
    if abort.is_none() {
        let failed = inventory
            .receipts()
            .iter()
            .find(|row| row.request_id().as_bytes().as_slice() == request_id(214))
            .unwrap();
        assert_eq!(
            failed.steps().last(),
            Some(&ReplicationPromotionStepV1::FailedClosed(
                ReplicationPromotionFailureV1::FenceUnavailable
            ))
        );
        assert_eq!(
            inventory.selection_for(request.operation_id()),
            Some(&selection)
        );
    }
    assert_eq!(
        inventory
            .receipts()
            .iter()
            .filter(|row| row.phase() == ReplicationPromotionPhaseV1::Succeeded)
            .count(),
        1
    );
    assert_eq!(
        inventory
            .receipts()
            .iter()
            .filter(|row| row.steps().last()
                == Some(&ReplicationPromotionStepV1::Denied(
                    ReplicationPromotionFailureV1::AuthorizationDenied
                )))
            .count(),
        2
    );
}

fn restricted(fixture: &Fixture, abort: Option<&str>) -> ChildProcessController {
    let mut spec = ChildProcessSpec::new(env!("CARGO_BIN_EXE_riffdbd-promotion-recovery-fixture"))
        .unwrap()
        .clear_environment()
        .arg("--config")
        .unwrap()
        .arg(fixture.config("follower"))
        .unwrap()
        .arg("--mode")
        .unwrap()
        .arg("follower")
        .unwrap();
    if let Some(point) = abort {
        spec = if point == "signal-offline" {
            spec.env("RIFFDB_PROMOTION_STOP", "offline").unwrap()
        } else {
            spec.env("RIFFDB_PROMOTION_ABORT", point).unwrap()
        };
    }
    let process = ChildProcessController::spawn(&spec).unwrap();
    // No line may precede this: ordinary application readiness would fail.
    process
        .wait_for_readiness("riffdb-promotion-retry-fixture-v1", Duration::from_secs(60))
        .unwrap();
    process
}
