//! Bounded concurrent commands, an actual source fence, and exact measured RPO.
// req: REP-005, REC-001
use super::{live_promotion::set_profile, primary_fence, proxy, support::*, workload};
use riffdb_client_rust::{AttemptBudget, BearerCredential, CallMetadata, v1};
use riffdb_errors::ApplicationErrorCode;
use riffdb_types::*;
use std::{collections::BTreeSet, time::Duration};

#[path = "promotion_load_fences.rs"]
mod fences;
#[path = "promotion_load_queries.rs"]
mod queries;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_mints_incarnation_and_refuses_old_lineage_tokens() {
    for profile in ["standard", "hardened"] {
        drill(profile, false).await;
        drill(profile, true).await;
    }
}

async fn drill(profile: &str, lagging: bool) {
    let fixture = Fixture::new();
    set_profile(&fixture, "primary", profile);
    let mut primary = fixture.start("primary", None);
    stop(&mut primary);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    primary = fixture.start("primary", None);
    let mut control = fixture.client("primary").await;
    let replication = create_replication_capability(&mut control, &admin).await;
    workload::deploy(&mut control, &admin).await;
    let writer = workload::authority_with_seed(&mut control, &admin, 30).await;
    let query_reader = queries::authority(&mut control, &admin).await;
    let fence_token = primary_fence::create_fence_capability(&mut control, &admin).await;
    let target = v1::ReplicationFollowerTarget {
        database_id: lineage.database_id().as_bytes().to_vec(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        hold_id: vec![1; 16],
    };
    let Some(v1::register_follower_response::Result::Receipt(registered)) = control
        .register_follower(
            v1::RegisterFollowerRequest {
                request_id: request_id(230),
                target: Some(target.clone()),
                hold_budget_sequences: 4096,
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
    let mut seed = riffdb_ticketdesk::TicketDeskClient::new(
        fixture.application_client().await,
        writer.clone(),
        AttemptBudget::new(1).unwrap(),
    );
    let first = seed
        .create_organization(input(0x80, "initial organization"))
        .await
        .unwrap();
    assert_eq!(first.commit_sequence, Some(1));
    drop(seed);
    drop(control);
    stop(&mut primary);
    let old_position = super::oracle::baseline(&fixture.database("primary")).position();
    queries::configure(&fixture, "primary");
    primary = fixture.start("primary", None);
    control = fixture.client("primary").await;
    let old_token = queries::token(&fixture, &query_reader, lineage).await;
    let relay = proxy::Proxy::start(fixture.channel("primary").await).await;
    let attached = relay.observe_attached_frame();
    fixture.configure_follower_via(lineage, &replication, relay.endpoint());
    set_profile(&fixture, "follower", profile);
    queries::configure(&fixture, "follower");
    let mut follower = fixture.start("follower", Some("follower"));
    tokio::time::timeout(Duration::from_secs(30), attached)
        .await
        .unwrap()
        .unwrap();
    super::wait_for_commit(&fixture, &admin, 1).await;
    let old_discovery = fences::observe(&fixture, &writer).await;
    let held = lagging.then(|| relay.hold_after_commit(1));

    let (started, mut running) = tokio::sync::mpsc::channel(8);
    let (fence_completed, fenced_receipt) = tokio::sync::watch::channel(false);
    let start = std::sync::Arc::new(tokio::sync::Barrier::new(9));
    let mut workers = Vec::with_capacity(8);
    for worker in 0..8u64 {
        let mut client = riffdb_ticketdesk::TicketDeskClient::new(
            fixture.application_client().await,
            writer.clone(),
            AttemptBudget::new(1).unwrap(),
        );
        let started = started.clone();
        let start = start.clone();
        let mut fenced_receipt = fenced_receipt.clone();
        workers.push(tokio::spawn(async move {
            start.wait().await;
            let mut committed = Vec::new();
            for sequence in 0..64u64 {
                let input = input(10_000 + worker * 100 + sequence, "concurrent source write");
                match client.create_organization(input.clone()).await {
                    Ok(result) => {
                        assert!(matches!(
                            result.outcome,
                            riffdb_ticketdesk::CreateOrganizationOutcome::Created { .. }
                        ));
                        assert!(!result.replayed);
                        committed.push(result.commit_sequence.unwrap());
                        if sequence == 0 {
                            started.send(()).await.unwrap();
                        }
                    }
                    Err(error) => {
                        if error.semantic_error().map(|e| e.code())
                            == Some(ApplicationErrorCode::StorageUnavailable)
                        {
                            // The admission gate reports Draining before its
                            // durable fence, mapped to StorageUnavailable by
                            // the service. Synchronize on the actual receipt:
                            // retrying this exact command must then be fenced,
                            // never committed or replayed.
                            tokio::time::timeout(
                                Duration::from_secs(30),
                                fenced_receipt.wait_for(|complete| *complete),
                            )
                            .await
                            .unwrap()
                            .unwrap();
                            let error = client.create_organization(input).await.unwrap_err();
                            assert_eq!(
                                error.semantic_error().map(|e| e.code()),
                                Some(ApplicationErrorCode::PrimaryFenced)
                            );
                        } else {
                            assert_eq!(
                                error.semantic_error().map(|e| e.code()),
                                Some(ApplicationErrorCode::PrimaryFenced)
                            );
                        }
                        return (committed, true);
                    }
                }
            }
            (committed, false)
        }));
    }
    drop(started);
    start.wait().await;
    for _ in 0..8 {
        tokio::time::timeout(Duration::from_secs(30), running.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let release = if let Some((entered, release)) = held {
        tokio::time::timeout(Duration::from_secs(30), entered)
            .await
            .unwrap()
            .unwrap();
        Some(release)
    } else {
        None
    };
    let fence = control
        .fence_replication_primary(
            v1::FenceReplicationPrimaryRequest {
                request_id: request_id(231),
                operation_id: request_id(232),
                target: Some(target.clone()),
                registration_generation: registered.registration_generation,
            },
            &CallMetadata::authenticated(BearerCredential::new(&fence_token).unwrap()),
        )
        .await
        .unwrap();
    let Some(v1::fence_replication_primary_response::Result::Receipt(fence)) = fence.result else {
        panic!("fence receipt")
    };
    fence_completed.send(true).unwrap();
    let fenced = position(fence.final_application_frontier.as_ref().unwrap());
    let mut committed = BTreeSet::from([1]);
    let mut refused = 0;
    for worker in workers {
        let (sequences, hit_fence) = tokio::time::timeout(Duration::from_secs(30), worker)
            .await
            .unwrap()
            .unwrap();
        refused += usize::from(hit_fence);
        for sequence in sequences {
            assert!(committed.insert(sequence));
        }
    }
    assert!(
        refused > 0,
        "the source fence must meet running command workers"
    );
    assert_eq!(committed.len() as u64, fenced);
    assert_eq!(committed.last().copied(), Some(fenced));
    if !lagging {
        super::wait_for_commit(&fixture, &admin, fenced).await;
    }
    let mut promoted = fixture.client("follower").await;
    let before = promoted
        .stats(
            v1::StatsRequest {
                request_id: request_id(233),
            },
            &admin,
        )
        .await
        .unwrap()
        .last_commit_sequence
        .unwrap();
    assert_eq!(before, if lagging { 1 } else { fenced });
    let result = promoted
        .promote_follower(
            v1::PromoteFollowerRequest {
                request_id: request_id(234),
                operation_id: request_id(235),
                fence_operation_id: request_id(232),
                target: Some(target),
                registration_generation: registered.registration_generation,
            },
            &admin,
        )
        .await
        .unwrap()
        .receipt
        .unwrap();
    let applied = position(result.applied_application_frontier.as_ref().unwrap());
    assert_eq!(applied, before);
    assert_eq!(result.application_rpo, fenced - applied);
    assert_eq!(result.application_rpo == 0, !lagging);
    assert_eq!(
        result.history_incarnation,
        lineage.history_incarnation() + 1
    );
    assert_eq!(
        result.leadership_epoch,
        lineage.leadership_epoch().get() + 1
    );
    drop(release);
    drop(control);
    stop(&mut primary);
    relay.shutdown().await;
    queries::assert_old_token_refused(&fixture, &query_reader, old_token).await;
    fences::assert_refused(&fixture, &writer, old_discovery).await;
    fences::assert_old_stream_refused(&fixture, &replication, lineage, old_position).await;
    let mut client = riffdb_ticketdesk::TicketDeskClient::new(
        fixture.application_client_for("follower").await,
        writer,
        AttemptBudget::new(1).unwrap(),
    );
    let input = input(0x96, "post-promotion write");
    let created = client.create_organization(input.clone()).await.unwrap();
    assert_eq!(created.commit_sequence, Some(applied + 1));
    assert!(!created.replayed);
    let replay = client.create_organization(input).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.outcome, created.outcome);
    queries::assert_new_entity(&mut promoted, &query_reader).await;
    drop(client);
    drop(promoted);
    stop(&mut follower);
}

fn input(id: u64, name: &str) -> riffdb_ticketdesk::CreateOrganizationInput {
    riffdb_ticketdesk::CreateOrganizationInput {
        organization_id: format!("018f0000-0000-7000-8000-{id:012x}"),
        name: name.into(),
        idempotency_key: format!("promotion-load-{id}"),
    }
}

fn position(frontier: &v1::FrontierPosition) -> u64 {
    match frontier.position.as_ref().unwrap() {
        v1::frontier_position::Position::BeforeFirst(_) => 0,
        v1::frontier_position::Position::AppliedThrough(value) => *value,
    }
}
