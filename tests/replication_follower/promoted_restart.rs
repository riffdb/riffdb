//! Real daemon restart after a committed offline cutover. The source fence is
//! obtained over TLS; the external attempt below is storage fixture setup, not
//! the still-pending public promotion authorization and live handoff operation.
// req: REP-005, REC-001, STO-012
use super::{oracle, primary_fence, proxy, support::*, workload};
use riffdb_client_rust::{AttemptBudget, BearerCredential, CallMetadata, v1};
use riffdb_service::ReplicationSourcePort;
use riffdb_storage_api::*;
use riffdb_storage_redb::RedbMaintenanceStorage;
use riffdb_types::*;
use std::num::NonZeroU64;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_promotion_restarts_as_source_with_former_peer_offline() {
    promoted_restart(false, None, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_source_backup_and_retirement_resume_writes_and_survive_a_fresh_owner() {
    promoted_restart(true, None, false).await;
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_backup_recovers_after_drain_close_and_validation_crashes() {
    for point in ["drain", "closed", "validated"] {
        promoted_restart(true, Some(point), false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_backup_restore_rewinds_data_and_recovers_source_role_after_restart() {
    promoted_restart(true, None, true).await;
}

async fn promoted_restart(
    exercise_backup: bool,
    abort_point: Option<&'static str>,
    exercise_restore: bool,
) {
    let fixture = Fixture::new();
    let mut primary = fixture.start("primary", None);
    stop(&mut primary);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let replication_token = create_replication_capability(&mut client, &admin).await;
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
                request_id: request_id(140),
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
        panic!("registered source hold");
    };
    let relay = proxy::Proxy::start(fixture.channel("primary").await).await;
    let attached = relay.observe_attached_frame();
    fixture.configure_follower_via(lineage, &replication_token, relay.endpoint());
    let mut follower = fixture.start("follower", Some("follower"));
    tokio::time::timeout(std::time::Duration::from_secs(30), attached)
        .await
        .unwrap()
        .unwrap();
    stop(&mut follower);
    let snapshot = oracle::capture_follower(&fixture.database("follower"));
    let fence_operation =
        ReplicationFenceOperationId::from_bytes(request_id(141).try_into().unwrap()).unwrap();
    let generation = ChangelogTransactionSequence::new(registered.registration_generation).unwrap();
    let fenced = client
        .fence_replication_primary(
            v1::FenceReplicationPrimaryRequest {
                request_id: request_id(142),
                operation_id: fence_operation.as_bytes().to_vec(),
                target: Some(wire_target),
                registration_generation: generation.get(),
            },
            &CallMetadata::authenticated(BearerCredential::new(&fence_token).unwrap()),
        )
        .await
        .unwrap();
    assert!(matches!(
        fenced.result,
        Some(v1::fence_replication_primary_response::Result::Receipt(_))
    ));
    let peer = riffdb_server::VerifiedReplicationPeer::connect(
        fixture.tls_config("primary"),
        riffdb_auth::RawCapabilityToken::parse_canonical(replication_token.as_bytes()).unwrap(),
        DatabaseAlias::default_alias(),
    )
    .await
    .unwrap();
    let observed = peer
        .primary_fence_source_evidence(
            PrimaryFenceRequestV1::new(
                RequestId::from_bytes(request_id(143).try_into().unwrap()).unwrap(),
                fence_operation,
                target,
                generation,
            ),
            snapshot.position(),
        )
        .await;
    if let Err(error) = &observed {
        drop(peer);
        drop(client);
        stop(&mut primary);
        let ports = open_primary(&fixture.database("primary"));
        let fresh = ports
            .published_changelog_snapshot_v3()
            .unwrap()
            .primary_fence_source_evidence_v1(
                PrimaryFenceRequestV1::new(
                    RequestId::from_bytes(request_id(143).try_into().unwrap()).unwrap(),
                    fence_operation,
                    target,
                    generation,
                ),
                snapshot.position(),
            );
        panic!("live fence evidence refused: {error:?}; fresh validated source proof: {fresh:?}");
    }
    let proof = observed.unwrap();
    drop(peer);
    drop(client);
    stop(&mut primary);
    relay.shutdown().await;
    // The old source cannot be contacted and its replication credential is gone.
    let root = fixture.database("follower").parent().unwrap().to_path_buf();
    std::fs::remove_file(root.join("replication.token")).unwrap();
    let request = ReplicationPromotionRequestV1::new(
        ReplicationPromotionOperationId::from_bytes(request_id(144).try_into().unwrap()).unwrap(),
        fence_operation,
        target,
        generation,
    );
    let selection = ReplicationPromotionSelectionV1::new(
        request,
        ReplicationFollowerStateV3::attached(snapshot.lineage(), snapshot.position(), None)
            .unwrap(),
        proof,
    )
    .unwrap();
    assert_eq!(selection.application_rpo(), 0);
    let expected = selection.published_lineage();
    let mut owner = RedbMaintenanceStorage::open_for_promotion_recovery(
        fixture.database("follower"),
        root.join("follower-backups"),
    )
    .unwrap();
    let mut attempt = ReplicationPromotionReceiptV1::attempted(
        request,
        RequestId::from_bytes(request_id(145).try_into().unwrap()).unwrap(),
        AuditPrincipalV1::new(
            ActorId::new("offline-promotion-fixture").unwrap(),
            ActorKind::Human,
            capability_id(1),
            NonZeroU64::MIN,
        ),
        None,
        now(),
    );
    owner.persist_promotion_receipt(&attempt).unwrap();
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        attempt
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
        owner.persist_promotion_receipt(&attempt).unwrap();
    }
    attempt.record_selection(selection).unwrap();
    owner.persist_promotion_receipt(&attempt).unwrap();
    attempt
        .advance(ReplicationPromotionStepV1::Phase(
            ReplicationPromotionPhaseV1::CutoverPending,
        ))
        .unwrap();
    owner.persist_promotion_receipt(&attempt).unwrap();
    let record =
        StoredPromotionAdministrationV1::new(attempt, now(), ServiceIngressKindV1::Grpc).unwrap();
    owner.apply_promotion_cutover(&record).unwrap();
    drop(owner);

    // Keep the original follower configuration: local committed authority owns
    // the role. Readiness must not require loading its vanished peer credential.
    follower = match abort_point {
        Some(point) => armed_follower(&fixture, point),
        None => fixture.start("follower", Some("follower")),
    };
    let mut client = fixture.client("follower").await;
    let metadata = workload::authority_with_seed(&mut client, &admin, 4).await;
    let mut app = riffdb_ticketdesk::TicketDeskClient::new(
        fixture.application_client_for("follower").await,
        metadata,
        AttemptBudget::new(2).unwrap(),
    );
    let input = riffdb_ticketdesk::CreateOrganizationInput {
        organization_id: "018f0000-0000-7000-8000-000000000099".into(),
        name: "promoted source".into(),
        idempotency_key: "promoted-restart-command".into(),
    };
    let created = app.create_organization(input.clone()).await.unwrap();
    assert!(matches!(
        created.outcome,
        riffdb_ticketdesk::CreateOrganizationOutcome::Created { .. }
    ));
    assert!(!created.replayed);
    assert_eq!(created.commit_sequence, Some(1));
    let replay = app.create_organization(input.clone()).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.commit_sequence, created.commit_sequence);
    assert_eq!(replay.outcome, created.outcome);
    assert_eq!(replay.outcome_uri, created.outcome_uri);
    if exercise_backup {
        backup_and_resume(
            &fixture,
            &mut follower,
            &mut client,
            &admin,
            abort_point.is_some(),
            !exercise_restore,
        )
        .await;
        let after_backup = app.create_organization(input).await.unwrap();
        assert!(after_backup.replayed);
        assert_eq!(after_backup.commit_sequence, created.commit_sequence);
        assert_eq!(after_backup.outcome, created.outcome);
        assert_eq!(after_backup.outcome_uri, created.outcome_uri);
        let fresh = riffdb_ticketdesk::CreateOrganizationInput {
            organization_id: "018f0000-0000-7000-8000-000000000098".into(),
            name: "after promoted backup".into(),
            idempotency_key: "after-promoted-backup-command".into(),
        };
        let committed = app.create_organization(fresh.clone()).await.unwrap();
        assert!(!committed.replayed);
        assert_eq!(committed.commit_sequence, Some(2));
        assert!(matches!(
            committed.outcome,
            riffdb_ticketdesk::CreateOrganizationOutcome::Created { .. }
        ));
        let replay = app.create_organization(fresh.clone()).await.unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_sequence, committed.commit_sequence);
        assert_eq!(replay.outcome, committed.outcome);
        assert_eq!(replay.outcome_uri, committed.outcome_uri);
        if exercise_restore {
            restore_and_resume(&mut client, &admin).await;
            let rewound = app.create_organization(fresh.clone()).await.unwrap();
            assert!(
                !rewound.replayed,
                "the suffix command must have been removed by restore"
            );
            assert_eq!(rewound.commit_sequence, Some(2));
            let (
                riffdb_ticketdesk::CreateOrganizationOutcome::Created {
                    organization: restored,
                },
                riffdb_ticketdesk::CreateOrganizationOutcome::Created {
                    organization: prior,
                },
            ) = (&rewound.outcome, &committed.outcome)
            else {
                panic!("restore must allow the removed organization to be created again");
            };
            assert_eq!(restored.organization_id, prior.organization_id);
            assert_eq!(restored.name, prior.name);
            // This is a new command after rewind, so its creation timestamp is
            // newly supplied. Only the subsequent idempotent retry must retain
            // the complete outcome bytes from this new execution.
            let replay = app.create_organization(fresh).await.unwrap();
            assert!(replay.replayed);
            assert_eq!(replay.commit_sequence, rewound.commit_sequence);
            assert_eq!(replay.outcome, rewound.outcome);
            assert_eq!(replay.outcome_uri, rewound.outcome_uri);
        }
    }
    drop(app);
    drop(client);
    stop(&mut follower);
    // Reconcile a second process generation after a real new-lineage command.
    follower = fixture.start("follower", Some("follower"));
    stop(&mut follower);
    let owner = RedbMaintenanceStorage::open_for_promotion_recovery(
        fixture.database("follower"),
        root.join("follower-backups"),
    )
    .unwrap();
    let recovered = owner.discover_committed_promotion().unwrap();
    if exercise_restore {
        assert!(
            recovered.is_none(),
            "restore must establish its own current anchor"
        );
        assert_eq!(
            riffdb_storage_redb::read_history_incarnation(fixture.database("follower")).unwrap(),
            Some(expected.history_incarnation() + 1)
        );
    } else {
        let recovered = recovered.unwrap();
        assert_eq!(recovered, record);
        assert_eq!(
            recovered.attempt().selection().unwrap().published_lineage(),
            expected
        );
    }
    assert_eq!(
        owner.promotion_receipts().unwrap().receipts()[0].phase(),
        ReplicationPromotionPhaseV1::Succeeded
    );
}

async fn backup_and_resume(
    fixture: &Fixture,
    follower: &mut riffdb_testkit_server::process::ChildProcessController,
    client: &mut riffdb_client_rust::RiffDbClient,
    metadata: &CallMetadata,
    expect_abort: bool,
    exercise_retirement: bool,
) {
    let operation_id = request_id(146);
    let backup_name = "promoted-source-backup";
    let submission = client
        .create_offline_backup(
            v1::CreateOfflineBackupRequest {
                request_id: request_id(147),
                operation_id: operation_id.clone(),
                backup_name: backup_name.into(),
            },
            metadata,
        )
        .await;
    let accepted = if expect_abort {
        use std::os::unix::process::ExitStatusExt;
        // A crash can precede the accepted response. The durable operation ID
        // must resolve to the same successful operation after the real restart.
        let accepted = submission.ok().map(|started| {
            assert_eq!(
                started.disposition,
                v1::OfflineMaintenanceStartDisposition::Accepted as i32
            );
            started.operation.unwrap()
        });
        let exit = follower
            .wait_for_exit(std::time::Duration::from_secs(30))
            .unwrap();
        assert_eq!(
            exit.status.signal(),
            Some(6),
            "armed maintenance must abort"
        );
        *follower = fixture.start("follower", Some("follower"));
        *client = fixture.client("follower").await;
        accepted
    } else {
        let started = submission.unwrap();
        assert_eq!(
            started.disposition,
            v1::OfflineMaintenanceStartDisposition::Accepted as i32
        );
        Some(started.operation.unwrap())
    };
    let terminal = maintenance_terminal(client, metadata, &operation_id, 10_000).await;
    assert_eq!(terminal.operation_id, operation_id);
    assert_eq!(terminal.backup_name, backup_name);
    if let Some(accepted) = accepted {
        assert_eq!(terminal.operation_id, accepted.operation_id);
        assert_eq!(terminal.input_hash, accepted.input_hash);
    }
    assert_eq!(
        terminal.kind,
        v1::OfflineMaintenanceOperationKind::CreateBackup as i32
    );
    assert_eq!(
        terminal.failure,
        v1::OfflineMaintenanceFailureClass::Unspecified as i32
    );
    let replay = client
        .create_offline_backup(
            v1::CreateOfflineBackupRequest {
                request_id: request_id(148),
                operation_id,
                backup_name: backup_name.into(),
            },
            metadata,
        )
        .await
        .unwrap();
    assert_eq!(
        replay.disposition,
        v1::OfflineMaintenanceStartDisposition::Terminal as i32
    );
    assert_eq!(replay.operation.unwrap(), terminal);

    if !exercise_retirement {
        return;
    }
    let retirement_id = request_id(149);
    let started = client
        .retire_offline_backup(
            v1::RetireOfflineBackupRequest {
                request_id: request_id(150),
                operation_id: retirement_id.clone(),
                backup_name: backup_name.into(),
            },
            metadata,
        )
        .await
        .unwrap();
    assert_eq!(
        started.disposition,
        v1::OfflineMaintenanceStartDisposition::Accepted as i32
    );
    let accepted = started.operation.unwrap();
    let terminal = maintenance_terminal(client, metadata, &retirement_id, 30_000).await;
    assert_eq!(terminal.operation_id, accepted.operation_id);
    assert_eq!(terminal.input_hash, accepted.input_hash);
    assert_eq!(terminal.backup_name, backup_name);
    assert_eq!(
        terminal.kind,
        v1::OfflineMaintenanceOperationKind::RetireBackup as i32
    );
    assert_eq!(
        terminal.failure,
        v1::OfflineMaintenanceFailureClass::Unspecified as i32
    );
    let replay = client
        .retire_offline_backup(
            v1::RetireOfflineBackupRequest {
                request_id: request_id(151),
                operation_id: retirement_id,
                backup_name: backup_name.into(),
            },
            metadata,
        )
        .await
        .unwrap();
    assert_eq!(
        replay.disposition,
        v1::OfflineMaintenanceStartDisposition::Terminal as i32
    );
    assert_eq!(replay.operation.unwrap(), terminal);
}

async fn maintenance_terminal(
    client: &mut riffdb_client_rust::RiffDbClient,
    metadata: &CallMetadata,
    operation_id: &[u8],
    request_base: u64,
) -> v1::OfflineMaintenanceOperation {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
            // Public receipt observations are the synchronization barrier. The
            // outer deadline bounds the complete operation, including fast
            // refused reads while the route is deliberately offline.
            for ordinal in 0..u64::MAX - request_base {
                let request_id =
                    RequestId::from_unix_milliseconds_and_random(request_base + ordinal, [0x79; 10])
                        .unwrap()
                        .as_bytes()
                        .to_vec();
                if let Ok(observed) = client
                    .get_offline_maintenance_operation(
                        v1::GetOfflineMaintenanceOperationRequest {
                            request_id,
                            operation_id: operation_id.to_vec(),
                        },
                        metadata,
                    )
                    .await
                    && let Some(v1::get_offline_maintenance_operation_response::Result::Found(
                        receipt,
                    )) = observed.result
                {
                    assert_ne!(
                        receipt.phase,
                        v1::OfflineMaintenancePhase::FailedClosed as i32,
                        "promoted maintenance failed closed with class {}",
                        receipt.failure
                    );
                    if receipt.phase == v1::OfflineMaintenancePhase::Succeeded as i32 {
                        return receipt;
                    }
                }
                tokio::task::yield_now().await;
            }
            panic!("promoted maintenance exhausted the request identifier range");
        })
        .await
        .expect("promoted maintenance must regain source readiness")
}

fn armed_follower(
    fixture: &Fixture,
    point: &'static str,
) -> riffdb_testkit_server::process::ChildProcessController {
    #[cfg(feature = "test-fixtures")]
    {
        use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};
        let spec = ChildProcessSpec::new(env!("CARGO_BIN_EXE_riffdbd-maintenance-fixture"))
            .unwrap()
            .clear_environment()
            .env("RIFFDB_TEST_MAINTENANCE_POINT", point)
            .unwrap()
            .arg("--config")
            .unwrap()
            .arg(fixture.config("follower"))
            .unwrap()
            .arg("--mode")
            .unwrap()
            .arg("follower")
            .unwrap();
        let child = ChildProcessController::spawn(&spec).unwrap();
        child
            .wait_for_readiness("riffdbd-ready-v1\t", std::time::Duration::from_secs(60))
            .unwrap();
        child
    }
    #[cfg(not(feature = "test-fixtures"))]
    {
        let _ = (fixture, point);
        panic!("maintenance abort fixture requires test-fixtures");
    }
}

async fn restore_and_resume(
    client: &mut riffdb_client_rust::RiffDbClient,
    metadata: &CallMetadata,
) {
    let operation_id = request_id(152);
    let request = |seed| v1::RestoreOfflineBackupRequest {
        request_id: request_id(seed),
        operation_id: operation_id.clone(),
        backup_name: "promoted-source-backup".into(),
        replacement_confirmation:
            v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
    };
    let started = client
        .restore_offline_backup(request(153), metadata)
        .await
        .unwrap();
    assert_eq!(
        started.disposition,
        v1::OfflineMaintenanceStartDisposition::Accepted as i32
    );
    let accepted = started.operation.unwrap();
    let terminal = maintenance_terminal(client, metadata, &operation_id, 50_000).await;
    assert_eq!(terminal.operation_id, accepted.operation_id);
    assert_eq!(terminal.input_hash, accepted.input_hash);
    assert_eq!(
        terminal.kind,
        v1::OfflineMaintenanceOperationKind::RestoreBackup as i32
    );
    assert_eq!(
        terminal.failure,
        v1::OfflineMaintenanceFailureClass::Unspecified as i32
    );
    let replay = client
        .restore_offline_backup(request(154), metadata)
        .await
        .unwrap();
    assert_eq!(
        replay.disposition,
        v1::OfflineMaintenanceStartDisposition::Terminal as i32
    );
    assert_eq!(replay.operation.unwrap(), terminal);
}
