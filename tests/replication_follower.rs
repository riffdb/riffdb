#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]
//! Real daemon replication over verified TLS. Component crash proofs live beside
//! the receiver; this harness checks the independently launched service graphs.
// req: REP-002, REP-003, REC-001

#[path = "replication_follower/columnar_reads.rs"]
mod columnar_reads;
#[path = "replication_follower/exact_reads.rs"]
mod exact_reads;
#[path = "replication_follower/oracle.rs"]
mod oracle;
#[path = "replication_follower/proxy.rs"]
mod proxy;
#[path = "replication_follower/support.rs"]
mod support;
#[path = "replication_follower/vector_prefix.rs"]
mod vector_prefix;
#[path = "replication_follower/workload.rs"]
mod workload;

use riffdb_client_rust::{BearerCredential, CallMetadata, RiffDbClient, v1};
use riffdb_errors::PublicErrorKind;
use support::*;

// req: REP-004, PRJ-008, PRJ-009
#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_causal_read_waits_for_token_then_matches_primary() {
    columnar_reads::run_scenario(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_to_tail_fence_is_gap_free_across_crash() {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, metadata, _) = seed_primary(&fixture.database("primary"));
    let baseline = oracle::baseline(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let token = create_replication_capability(&mut client, &metadata).await;
    let proxy = proxy::Proxy::start(fixture.channel("primary").await).await;
    fixture.configure_follower_via(lineage, &token, proxy.endpoint());
    workload::deploy(&mut client, &metadata).await;
    let (progress, mut checkpoints) = tokio::sync::mpsc::channel(1);
    let (permit, mut permits) = tokio::sync::mpsc::channel(1);
    let workload =
        workload::run_observed(&fixture, &mut client, &metadata, async |count, commit| {
            if matches!(count, 140 | 280) {
                progress.send(commit).await.unwrap();
                permits.recv().await.unwrap();
            }
        });
    let crash = async {
        let fence_commit =
            tokio::time::timeout(std::time::Duration::from_secs(30), checkpoints.recv())
                .await
                .unwrap()
                .unwrap();
        let (held, release) = proxy.hold_attachment();
        let mut follower = fixture.spawn("follower", Some("follower"));
        tokio::time::timeout(std::time::Duration::from_secs(30), held)
            .await
            .unwrap()
            .unwrap();
        // The durable bootstrap is published, but no attachment has reached
        // the primary. Subsequent workload commits must come from its held tail.
        assert!(
            !follower
                .kill(std::time::Duration::from_secs(30))
                .unwrap()
                .status
                .success()
        );
        drop(release);
        let snapshot = oracle::capture_follower(&fixture.database("follower"));
        assert_eq!(
            snapshot
                .position()
                .frontier()
                .application()
                .map(|v| v.get()),
            Some(fence_commit)
        );
        assert!(
            !primary
                .kill(std::time::Duration::from_secs(30))
                .unwrap()
                .status
                .success()
        );
        primary = fixture.start("primary", None);
        permit.send(()).await.unwrap();
        let advanced = tokio::time::timeout(std::time::Duration::from_secs(30), checkpoints.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(advanced > fence_commit);
        assert!(
            !primary
                .kill(std::time::Duration::from_secs(30))
                .unwrap()
                .status
                .success()
        );
        primary = fixture.start("primary", None);
        // Repeat the receiver crash at the same attachment boundary after the
        // source has recovered twice and advanced beyond the bootstrap fence.
        let (held, release) = proxy.hold_attachment();
        let resumed = proxy.expect_attachment(snapshot.position());
        follower = fixture.spawn("follower", Some("follower"));
        tokio::time::timeout(std::time::Duration::from_secs(30), held)
            .await
            .unwrap()
            .unwrap();
        assert!(resumed.await.unwrap());
        assert!(
            !follower
                .kill(std::time::Duration::from_secs(30))
                .unwrap()
                .status
                .success()
        );
        drop(release);
        assert_eq!(
            oracle::capture_follower(&fixture.database("follower")),
            snapshot
        );
        proxy::assert_foreign_tail_refused(
            fixture.channel("primary").await,
            &token,
            lineage,
            snapshot.position(),
        )
        .await;
        let resumed = proxy.expect_attachment(snapshot.position());
        follower = fixture.start("follower", Some("follower"));
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(30), resumed)
                .await
                .unwrap()
                .unwrap(),
            "bootstrap recovery did not reattach at its exact original fence"
        );
        wait_for_commit(&fixture, &metadata, advanced).await;
        permit.send(()).await.unwrap();
        (follower, snapshot)
    };
    let (last_commit, (mut follower, first)) = tokio::join!(workload, crash);
    wait_for_commit(&fixture, &metadata, last_commit).await;
    stop(&mut follower);
    let final_state = oracle::capture_follower(&fixture.database("follower"));
    assert_eq!(
        final_state
            .position()
            .frontier()
            .application()
            .map(|v| v.get()),
        Some(last_commit)
    );
    drop(client);
    proxy.shutdown().await;
    stop(&mut primary);
    oracle::compare_prefixes(
        baseline,
        &fixture.database("primary"),
        &[first, final_state],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// req: REP-004
async fn replication_stream_resumes_gap_free_after_repeated_kills() {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, metadata, _) = seed_primary(&fixture.database("primary"));
    let baseline = oracle::baseline(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let token = create_replication_capability(&mut client, &metadata).await;
    let proxy = proxy::Proxy::start(fixture.channel("primary").await).await;
    fixture.configure_follower_via(lineage, &token, proxy.endpoint());
    workload::deploy(&mut client, &metadata).await;
    let mut follower = fixture.start("follower", Some("follower"));
    let (progress, mut checkpoints) = tokio::sync::mpsc::channel(1);
    let (permit, mut permits) = tokio::sync::mpsc::channel(1);
    let mut prefixes = Vec::new();
    let workload =
        workload::run_observed(&fixture, &mut client, &metadata, async |count, commit| {
            if matches!(count, 20 | 21 | 140 | 141 | 280 | 281) {
                progress.send(commit).await.unwrap();
                permits.recv().await.unwrap();
            }
        });
    let crashes = async {
        for attempt in 0..3 {
            let commit =
                tokio::time::timeout(std::time::Duration::from_secs(30), checkpoints.recv())
                    .await
                    .unwrap()
                    .unwrap();
            let (held, release) = proxy.hold_after_commit(commit);
            permit.send(()).await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(30), held)
                .await
                .unwrap()
                .unwrap();
            // Observe after the command which produced the held frame has
            // completed. The workload remains parked at this explicit barrier.
            let advanced =
                tokio::time::timeout(std::time::Duration::from_secs(30), checkpoints.recv())
                    .await
                    .unwrap()
                    .unwrap();
            assert!(advanced > commit);
            assert_replication_lag_while_delivery_is_held(&fixture, &metadata, commit, attempt)
                .await;
            assert!(
                !follower
                    .kill(std::time::Duration::from_secs(30))
                    .unwrap()
                    .status
                    .success()
            );
            let snapshot = oracle::capture_follower(&fixture.database("follower"));
            assert!(
                snapshot
                    .position()
                    .frontier()
                    .application()
                    .map_or(0, |v| v.get())
                    <= commit
            );
            let resumed = proxy.expect_resume(snapshot.position());
            prefixes.push(snapshot);
            let _ = release.send(());
            follower = fixture.start("follower", Some("follower"));
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(30), resumed)
                    .await
                    .unwrap()
                    .unwrap(),
                "reconnect did not name the exact recovered durable prefix"
            );
            wait_for_commit(&fixture, &metadata, advanced).await;
            permit.send(()).await.unwrap();
        }
    };
    let (last_commit, ()) = tokio::join!(workload, crashes);
    wait_for_commit(&fixture, &metadata, last_commit).await;
    stop(&mut follower);
    prefixes.push(oracle::capture_follower(&fixture.database("follower")));
    assert_eq!(
        prefixes
            .last()
            .unwrap()
            .position()
            .frontier()
            .application()
            .map(|v| v.get()),
        Some(last_commit)
    );
    drop(client);
    proxy.shutdown().await;
    stop(&mut primary);
    oracle::compare_prefixes(baseline, &fixture.database("primary"), &prefixes);
}

async fn assert_replication_lag_while_delivery_is_held(
    fixture: &Fixture,
    metadata: &CallMetadata,
    held_after: u64,
    attempt: u8,
) {
    let sequence =
        |position: Option<v1::FrontierPosition>| match position.unwrap().position.unwrap() {
            v1::frontier_position::Position::BeforeFirst(_) => 0,
            v1::frontier_position::Position::AppliedThrough(value) => value,
        };
    let mut primary = fixture.client("primary").await;
    let stats = primary
        .stats(
            v1::StatsRequest {
                request_id: request_id(96 + attempt * 3),
            },
            metadata,
        )
        .await;
    let stats = stats
        .unwrap_or_else(|error| {
            panic!("primary statistics at held frontier {held_after}: {error:?}")
        })
        .replication
        .unwrap();
    assert_eq!(stats.role, v1::ReplicationRole::Primary as i32);
    assert_eq!(stats.registered_followers, Some(1));
    let source = stats.source_frontier.unwrap();
    let ack = stats.acknowledged_frontier.unwrap();
    assert!(sequence(source.application) > held_after);
    assert!(sequence(ack.application) <= held_after);
    assert_eq!(
        stats.application_lag_sequences,
        Some(sequence(source.application) - sequence(ack.application))
    );
    assert!(stats.application_lag_sequences.unwrap() > 0);
    assert_eq!(
        stats.administration_lag_sequences,
        Some(sequence(source.administration) - sequence(ack.administration))
    );
    let health = primary
        .health(
            v1::HealthRequest {
                request_id: Some(request_id(97 + attempt * 3)),
            },
            metadata,
        )
        .await
        .unwrap();
    let Some(v1::health_response::Result::Authenticated(health)) = health.result else {
        panic!("authenticated health")
    };
    let replication = health
        .components
        .iter()
        .find(|component| component.component == v1::HealthComponentKind::Replication as i32)
        .unwrap();
    assert_eq!(
        replication.status,
        v1::HealthComponentStatus::Degraded as i32
    );
    assert!(
        replication
            .replication
            .unwrap()
            .application_lag_sequences
            .unwrap()
            > 0
    );

    let mut follower = fixture.client("follower").await;
    let stats = follower
        .stats(
            v1::StatsRequest {
                request_id: request_id(98 + attempt * 3),
            },
            metadata,
        )
        .await
        .unwrap();
    let progress = stats.replication.unwrap();
    let applied = progress.applied_frontier.unwrap();
    assert!(sequence(applied.application) <= held_after);
    assert_eq!(
        stats.last_commit_sequence.unwrap_or(0),
        sequence(applied.application)
    );
    assert!(progress.registered_followers.is_none());
    if let Some(head) = progress.source_frontier {
        assert_eq!(
            progress.application_lag_sequences,
            Some(sequence(head.application) - sequence(applied.application))
        );
        assert_eq!(
            progress.administration_lag_sequences,
            Some(sequence(head.administration) - sequence(applied.administration))
        );
    } else {
        assert_eq!(progress.application_lag_sequences, None);
        assert_eq!(progress.administration_lag_sequences, None);
    }
}

async fn wait_for_commit(fixture: &Fixture, metadata: &CallMetadata, expected: u64) {
    let mut reader = fixture.client("follower").await;
    let mut observed = None;
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        // The deadline bounds this wait. A fixed poll count can expire much
        // earlier when local Statistics calls overtake durable follower replay.
        loop {
            let stats = reader
                .stats(
                    v1::StatsRequest {
                        request_id: request_id(95),
                    },
                    metadata,
                )
                .await
                .unwrap();
            if stats
                .last_commit_sequence
                .is_some_and(|value| value >= expected)
            {
                return;
            }
            observed = stats.last_commit_sequence;
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("follower frontier {observed:?} did not reach workload frontier {expected} in 30s")
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_applies_exact_prefix_byte_faithfully() {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, metadata, _) = seed_primary(&fixture.database("primary"));
    let baseline = oracle::baseline(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let token = create_replication_capability(&mut client, &metadata).await;
    fixture.configure_follower(lineage, &token);
    workload::deploy(&mut client, &metadata).await;
    let mut follower = fixture.start("follower", Some("follower"));
    let last_commit = workload::run(&fixture, &mut client, &metadata).await;
    wait_for_commit(&fixture, &metadata, last_commit).await;
    let mut reader = fixture.client("follower").await;
    let stats = reader
        .stats(
            v1::StatsRequest {
                request_id: request_id(90),
            },
            &metadata,
        )
        .await
        .unwrap();
    assert_eq!(stats.last_commit_sequence, Some(last_commit));
    reader
        .health(
            v1::HealthRequest {
                request_id: Some(request_id(91)),
            },
            &metadata,
        )
        .await
        .unwrap();
    let replication_metadata = CallMetadata::authenticated(BearerCredential::new(&token).unwrap());
    let denied = reader
        .get_active_contract(
            v1::GetActiveContractRequest {
                request_id: request_id(92),
            },
            &replication_metadata,
        )
        .await
        .unwrap_err();
    assert_eq!(
        denied.public_error().map(|error| error.kind()),
        Some(PublicErrorKind::AuthorizationDenied)
    );
    let refused = reader
        .revoke_capability(
            v1::RevokeCapabilityRequest {
                request_id: request_id(93),
                capability_id: capability_id(2).as_bytes().to_vec(),
                reason: v1::RevocationReason::Requested as i32,
            },
            &metadata,
        )
        .await
        .unwrap_err();
    assert_eq!(
        refused.public_error().map(|error| error.kind()),
        Some(PublicErrorKind::FollowerMode)
    );
    drop(reader);
    stop(&mut follower);
    drop(client);
    stop(&mut primary);
    oracle::compare(
        baseline,
        &fixture.database("primary"),
        &fixture.database("follower"),
        last_commit,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// req: REP-004
async fn tls_follower_daemon_serves_replicated_catalog_refuses_writes_and_reopens() {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, metadata, admin_token) = seed_primary(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let token = create_replication_capability(&mut client, &metadata).await;
    fixture.configure_follower(lineage, &token);

    let deployment = client
        .deploy_contract(
            v1::DeployContractRequest {
                request_id: request_id(20),
                source: include_str!("../examples/app-baseline/contracts/ticketdesk.riff").into(),
                expected_active_version: None,
                expected_active_bundle_hash: vec![],
                expected_candidate_bundle_hash: vec![],
            },
            &metadata,
        )
        .await
        .unwrap();
    let Some(v1::deploy_contract_response::Result::Activated(expected)) = deployment.result else {
        panic!("TicketDesk contract did not activate: {deployment:?}");
    };
    let primary_stats = client
        .stats(
            v1::StatsRequest {
                request_id: request_id(21),
            },
            &metadata,
        )
        .await
        .unwrap();
    let primary_progress = primary_stats
        .replication
        .expect("primary replication statistics");
    assert_eq!(primary_progress.role, v1::ReplicationRole::Primary as i32);
    assert_eq!(primary_progress.registered_followers, Some(0));
    assert_eq!(primary_progress.acknowledged_frontier, None);
    assert_eq!(primary_progress.application_lag_sequences, None);
    assert_eq!(
        primary_progress.source_frontier,
        primary_progress.applied_frontier
    );
    let primary_health = client
        .health(
            v1::HealthRequest {
                request_id: Some(request_id(22)),
            },
            &metadata,
        )
        .await
        .unwrap();
    let Some(v1::health_response::Result::Authenticated(primary_health)) = primary_health.result
    else {
        panic!("authenticated primary health");
    };
    let primary_replication: Vec<_> = primary_health
        .components
        .iter()
        .filter(|component| component.component == v1::HealthComponentKind::Replication as i32)
        .collect();
    assert_eq!(primary_replication.len(), 1);
    assert_eq!(
        primary_replication[0].replication.unwrap().role,
        v1::ReplicationRole::Primary as i32
    );

    for attempt in 0..3 {
        let mut follower = fixture.start("follower", Some("follower"));
        let mut reader: RiffDbClient = fixture.client("follower").await;
        let actual = reader
            .get_active_contract(
                v1::GetActiveContractRequest {
                    request_id: request_id(30 + attempt),
                },
                &metadata,
            )
            .await
            .unwrap();
        assert_eq!(
            actual.result,
            Some(v1::get_active_contract_response::Result::Present(
                expected.clone()
            ))
        );
        if attempt == 0 {
            assert_discovery_history_fence_refused(&mut reader, &metadata).await;
        }
        let health = reader
            .health(
                v1::HealthRequest {
                    request_id: Some(request_id(40 + attempt)),
                },
                &metadata,
            )
            .await
            .unwrap();
        let Some(v1::health_response::Result::Authenticated(health)) = health.result else {
            panic!("authenticated follower health");
        };
        assert_ne!(health.status, v1::HealthStatus::NotReady as i32);
        assert!(
            health.components.iter().all(|component| component.component
                != v1::HealthComponentKind::CommitCoordinator as i32)
        );
        let replication: Vec<_> = health
            .components
            .iter()
            .filter(|component| component.component == v1::HealthComponentKind::Replication as i32)
            .collect();
        assert_eq!(replication.len(), 1);
        assert_eq!(
            replication[0].replication.unwrap().role,
            v1::ReplicationRole::Follower as i32
        );
        let stats = reader
            .stats(
                v1::StatsRequest {
                    request_id: request_id(45 + attempt),
                },
                &metadata,
            )
            .await
            .unwrap();
        let progress = stats.replication.expect("follower replication statistics");
        assert_eq!(progress.role, v1::ReplicationRole::Follower as i32);
        assert_eq!(progress.registered_followers, None);
        assert!(progress.applied_frontier.is_some());
        let refusal = reader
            .revoke_capability(
                v1::RevokeCapabilityRequest {
                    request_id: request_id(50 + attempt),
                    capability_id: capability_id(1).as_bytes().to_vec(),
                    reason: v1::RevocationReason::Requested as i32,
                },
                &metadata,
            )
            .await
            .unwrap_err();
        assert_eq!(
            refusal.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::FollowerMode),
            "unexpected follower refusal: {refusal:?}"
        );
        if attempt == 2 {
            // Keep the same TLS connection across a source revocation. A new
            // request must authenticate against the latest completed prefix.
            let mut raw = fixture.raw_contract_client().await;
            let authenticated = || {
                let mut request = tonic::Request::new(v1::GetActiveContractRequest {
                    request_id: request_id(60),
                });
                request.metadata_mut().insert(
                    "authorization",
                    format!("Bearer {admin_token}").parse().unwrap(),
                );
                request
            };
            raw.get_active_contract(authenticated()).await.unwrap();
            client
                .revoke_capability(
                    v1::RevokeCapabilityRequest {
                        request_id: request_id(61),
                        capability_id: capability_id(1).as_bytes().to_vec(),
                        reason: v1::RevocationReason::Requested as i32,
                    },
                    &metadata,
                )
                .await
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(30), async {
                for _ in 0..4096 {
                    match raw.get_active_contract(authenticated()).await {
                        Ok(_) => {}
                        Err(status) => {
                            assert_eq!(status.code(), tonic::Code::PermissionDenied);
                            return;
                        }
                    }
                }
                panic!("follower did not observe source revocation within bounded requests");
            })
            .await
            .unwrap();
        }
        drop(reader);
        if attempt == 1 {
            assert!(
                !follower
                    .kill(std::time::Duration::from_secs(30))
                    .unwrap()
                    .status
                    .success()
            );
        } else {
            stop(&mut follower);
        }
    }
    drop(client);
    stop(&mut primary);
}

// req: REP-004
async fn assert_discovery_history_fence_refused(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
) {
    let request = v1::DiscoverCommandToolsRequest {
        request_id: request_id(80),
        page: Some(v1::PageRequest {
            limit: Some(10),
            cursor: None,
        }),
        prior_fence: None,
        representation: v1::DiscoveryRepresentation::CompactObservation as i32,
    };
    let response = client
        .discover_command_tools(request.clone(), metadata)
        .await
        .unwrap();
    let Some(v1::discover_command_tools_response::Result::CompactPage(page)) = response.result
    else {
        panic!("first discovery must return a page")
    };
    let prior = page.observed_fence.expect("discovery history fence");
    assert!(prior.history_incarnation > 0);
    let unchanged = client
        .discover_command_tools(
            v1::DiscoverCommandToolsRequest {
                request_id: request_id(81),
                prior_fence: Some(prior.clone()),
                ..request.clone()
            },
            metadata,
        )
        .await
        .unwrap();
    assert_eq!(
        unchanged.result,
        Some(v1::discover_command_tools_response::Result::CatalogUnchanged(prior.clone()))
    );
    for different_process in [false, true] {
        let mut foreign = prior.clone();
        foreign.history_incarnation += 1;
        if different_process {
            foreign.server_generation[0] ^= 1;
        }
        let error = client
            .discover_command_tools(
                v1::DiscoverCommandToolsRequest {
                    request_id: request_id(82),
                    prior_fence: Some(foreign.clone()),
                    ..request.clone()
                },
                metadata,
            )
            .await
            .expect_err("foreign-incarnation fence must not satisfy discovery");
        assert_eq!(
            error.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::HistoryIncarnationMismatch)
        );
        let error = client
            .discover_resources(
                v1::DiscoverResourcesRequest {
                    request_id: request_id(83),
                    page: request.page.clone(),
                    prior_fence: Some(foreign),
                    representation: v1::DiscoveryRepresentation::CompactObservation as i32,
                    kind: v1::ResourceDiscoveryKind::All as i32,
                },
                metadata,
            )
            .await
            .expect_err("resource discovery must apply the same history refusal");
        assert_eq!(
            error.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::HistoryIncarnationMismatch)
        );
    }
    // Process generation remains a cache hint, not a history identity. At the
    // same incarnation, ADR-0040 requires fresh authorized discovery after it changes.
    let mut stale_process = prior;
    stale_process.server_generation[0] ^= 1;
    let refreshed = client
        .discover_command_tools(
            v1::DiscoverCommandToolsRequest {
                request_id: request_id(84),
                prior_fence: Some(stale_process),
                ..request
            },
            metadata,
        )
        .await
        .unwrap();
    assert!(matches!(
        refreshed.result,
        Some(v1::discover_command_tools_response::Result::CompactPage(_))
    ));
}
