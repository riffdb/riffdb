#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]
//! Real daemon replication over verified TLS. Component crash proofs live beside
//! the receiver; this harness checks the independently launched service graphs.
// req: REP-002, REP-003, REC-001

#[path = "replication_follower/oracle.rs"]
mod oracle;
#[path = "replication_follower/proxy.rs"]
mod proxy;
#[path = "replication_follower/support.rs"]
mod support;
#[path = "replication_follower/workload.rs"]
mod workload;

use riffdb_client_rust::{BearerCredential, CallMetadata, RiffDbClient, v1};
use riffdb_errors::PublicErrorKind;
use support::*;

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
            if matches!(count, 20 | 140 | 280) {
                progress.send(commit).await.unwrap();
                permits.recv().await.unwrap();
            }
        });
    let crashes = async {
        for _ in 0..3 {
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
            wait_for_commit(&fixture, &metadata, commit + 1).await;
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

async fn wait_for_commit(fixture: &Fixture, metadata: &CallMetadata, expected: u64) {
    let mut reader = fixture.client("follower").await;
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut observed = None;
        // Catch-up after an offline bootstrap has a real backlog. Keep the
        // 30-second deadline and bound requests without ending observation
        // merely because thousands of fast Statistics calls overtook replay.
        for _ in 0..16_384 {
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
        }
        panic!("follower frontier {observed:?} did not reach workload frontier {expected}");
    })
    .await
    .unwrap();
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
    let mut reader = fixture.client("follower").await;
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for _ in 0..4096 {
            let stats = reader
                .stats(
                    v1::StatsRequest {
                        request_id: request_id(90),
                    },
                    &metadata,
                )
                .await
                .unwrap();
            if stats.last_commit_sequence == Some(last_commit) {
                return;
            }
        }
        panic!("follower did not reach the complete workload commit frontier");
    })
    .await
    .unwrap();
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
        reader
            .health(
                v1::HealthRequest {
                    request_id: Some(request_id(40 + attempt)),
                },
                &metadata,
            )
            .await
            .unwrap();
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
