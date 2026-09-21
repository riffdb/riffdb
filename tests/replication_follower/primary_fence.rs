//! Public TLS fence execution, restart and exact audit linkage; no promotion claim.
// req: REP-005, REC-001, STO-012
use super::support::*;
use riffdb_client_rust::{BearerCredential, CallMetadata, v1};
use riffdb_errors::PublicErrorKind;
use riffdb_service::ReplicationSourcePort;
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    StorageScanLimit, StoredAdministrationAuditRecordV1,
};
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1};

pub(super) async fn create_fence_capability(
    client: &mut riffdb_client_rust::RiffDbClient,
    metadata: &CallMetadata,
) -> String {
    let response = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: request_id(3),
                mode: v1::CapabilityCreateMode::Normal as i32,
                capability_id: capability_id(3).as_bytes().to_vec(),
                principal_id: "replication-test-fence-operator".into(),
                actor_kind: v1::ActorKind::Service as i32,
                requested_lifetime_seconds: 3600,
                audiences: vec!["replication-process-test".into()],
                grant: Some(v1::CapabilityGrant {
                    tenant_scope: Some(v1::TenantScope {
                        scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                    }),
                    partition_scope: Some(v1::PartitionScope {
                        scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                    }),
                    permissions: vec![v1::CapabilityPermission {
                        permission: Some(
                            v1::capability_permission::Permission::FenceReplicationPrimary(
                                v1::Unit {},
                            ),
                        ),
                    }],
                    max_scan_rows: 100,
                    ..Default::default()
                }),
            },
            metadata,
        )
        .await
        .unwrap();
    let Some(v1::create_capability_response::Result::Normal(normal)) = response.result else {
        panic!("normal creation expected")
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = normal.result else {
        panic!("fresh replication capability expected")
    };
    created.token
}
fn receipt(response: v1::FenceReplicationPrimaryResponse) -> v1::PrimaryFenceReceipt {
    let Some(v1::fence_replication_primary_response::Result::Receipt(receipt)) = response.result
    else {
        panic!("exact fence receipt");
    };
    receipt
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn primary_fence_public_tls_distinct_authority_replay_restart_and_audit_linkage() {
    let fixture = Fixture::new();
    let mut primary = fixture.start("primary", None);
    stop(&mut primary);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let replication_token = create_replication_capability(&mut client, &admin).await;
    super::workload::deploy(&mut client, &admin).await;
    let fence_token = create_fence_capability(&mut client, &admin).await;
    let fence_authority = CallMetadata::authenticated(BearerCredential::new(&fence_token).unwrap());
    let target = v1::ReplicationFollowerTarget {
        database_id: lineage.database_id().as_bytes().to_vec(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        hold_id: vec![0x71; 16],
    };
    let register = |seed| v1::RegisterFollowerRequest {
        request_id: request_id(seed),
        target: Some(target.clone()),
        hold_budget_sequences: 100,
        expires_at_application_sequence: None,
    };
    let Some(v1::register_follower_response::Result::Receipt(registered)) = client
        .register_follower(register(120), &admin)
        .await
        .unwrap()
        .result
    else {
        panic!("registered follower");
    };
    let request = |seed| v1::FenceReplicationPrimaryRequest {
        request_id: request_id(seed),
        operation_id: request_id(121),
        target: Some(target.clone()),
        registration_generation: registered.registration_generation,
    };
    assert_eq!(
        client
            .fence_replication_primary(request(122), &admin)
            .await
            .unwrap_err()
            .public_error()
            .unwrap()
            .kind(),
        PublicErrorKind::AuthorizationDenied
    );
    drop(client);
    stop(&mut primary);
    // Prepare exact source attachment through the production artifact owner.
    // Receiver publication and remote promotion proof are separate exit gates.
    let ports = open_primary(&fixture.database("primary"));
    let repo = ports
        .bootstrap_repository(
            &fixture
                .database("primary")
                .with_extension("fence-artifacts"),
        )
        .unwrap();
    let mut build = repo
        .begin(riffdb_types::ReplicationSourceHoldIdV1::new([0x71; 16]).unwrap())
        .unwrap();
    while !build.advance().unwrap() {}
    let held = build.finish().unwrap();
    let manifest = held.manifest();
    drop(held);
    repo.attach(manifest, manifest.fence().history().tail())
        .unwrap();
    drop(repo);
    drop(ports);
    primary = fixture.start("primary", None);
    client = fixture.client("primary").await;
    let first = receipt(
        client
            .fence_replication_primary(request(123), &fence_authority)
            .await
            .unwrap(),
    );
    assert!(!first.replayed);
    assert_eq!(first.operation_id, request_id(121));
    assert_eq!(first.target, Some(target.clone()));
    assert_eq!(
        first.registration_generation,
        registered.registration_generation
    );
    assert_eq!(
        first.final_application_frontier.as_ref().unwrap().position,
        Some(v1::frontier_position::Position::BeforeFirst(v1::Unit {}))
    );
    assert_fenced_health(&mut client, &admin, 127).await;
    let selected = riffdb_storage_api::PrimaryFenceRequestV1::new(
        riffdb_types::RequestId::from_bytes(request_id(132).try_into().unwrap()).unwrap(),
        riffdb_types::ReplicationFenceOperationId::from_bytes(request_id(121).try_into().unwrap())
            .unwrap(),
        riffdb_types::ReplicationFollowerAuditTargetV1::new(
            lineage.database_id(),
            lineage.history_incarnation(),
            lineage.leadership_epoch(),
            riffdb_types::ReplicationSourceHoldIdV1::new([0x71; 16]).unwrap(),
        )
        .unwrap(),
        riffdb_storage_api::ChangelogTransactionSequence::new(registered.registration_generation)
            .unwrap(),
    );
    let peer = riffdb_server::VerifiedReplicationPeer::connect(
        fixture.tls_config("primary"),
        riffdb_auth::RawCapabilityToken::parse_canonical(replication_token.as_bytes()).unwrap(),
        riffdb_types::DatabaseAlias::new("default").unwrap(),
    )
    .await
    .unwrap();
    let applied = manifest.fence().history().tail();
    let evidence = peer
        .primary_fence_source_evidence(selected, applied)
        .await
        .unwrap();
    assert_eq!(
        evidence.fence().administration_sequence().get(),
        first.administration_sequence
    );
    assert_eq!(evidence.fence().target(), selected.target());
    assert_eq!(evidence.fence().operation_id(), selected.operation_id());
    assert_eq!(evidence.fence().generation(), selected.generation());
    assert_eq!(evidence.applied(), applied);
    assert_eq!(evidence.application_rpo(), 0);
    for invalid in [
        riffdb_storage_api::PrimaryFenceRequestV1::new(
            selected.request_id(),
            riffdb_types::ReplicationFenceOperationId::from_bytes(
                request_id(133).try_into().unwrap(),
            )
            .unwrap(),
            selected.target(),
            selected.generation(),
        ),
        riffdb_storage_api::PrimaryFenceRequestV1::new(
            selected.request_id(),
            selected.operation_id(),
            selected.target(),
            selected.generation().checked_next().unwrap(),
        ),
    ] {
        assert!(
            peer.primary_fence_source_evidence(invalid, applied)
                .await
                .is_err()
        );
    }
    let substituted = riffdb_storage_api::ChangelogHistoryPointV3::new(
        applied.sequence(),
        [0x99; 32],
        applied.frontier(),
    );
    assert!(
        peer.primary_fence_source_evidence(selected, substituted)
            .await
            .is_err()
    );
    drop(peer);

    let second = receipt(
        client
            .fence_replication_primary(request(124), &fence_authority)
            .await
            .unwrap(),
    );
    let mut expected = first.clone();
    expected.replayed = true;
    assert_eq!(second, expected);
    assert_eq!(
        client
            .register_follower(register(125), &admin)
            .await
            .unwrap_err()
            .public_error()
            .unwrap()
            .kind(),
        PublicErrorKind::PrimaryFenced
    );
    drop(client);
    stop(&mut primary);
    primary = fixture.start("primary", None);
    client = fixture.client("primary").await;
    assert_fenced_health(&mut client, &admin, 131).await;
    let peer = riffdb_server::VerifiedReplicationPeer::connect(
        fixture.tls_config("primary"),
        riffdb_auth::RawCapabilityToken::parse_canonical(replication_token.as_bytes()).unwrap(),
        riffdb_types::DatabaseAlias::new("default").unwrap(),
    )
    .await
    .unwrap();
    let recovered = peer
        .primary_fence_source_evidence(selected, applied)
        .await
        .unwrap();
    assert_eq!(recovered.fence(), evidence.fence());
    assert_eq!(recovered.applied(), applied);
    drop(peer);

    assert_eq!(
        receipt(
            client
                .fence_replication_primary(request(126), &fence_authority)
                .await
                .unwrap()
        ),
        expected
    );
    drop(client);
    stop(&mut primary);
    let ports = open_primary(&fixture.database("primary"));
    let AdministrationAuditScan::ExactEnd { records } = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(128).unwrap(),
        ))
        .unwrap()
    else {
        panic!("bounded audits");
    };
    let records: Vec<_> = records.into_iter().map(|r| r.into_parts().0).collect();
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, StoredAdministrationAuditRecordV1::PrimaryFence(_)))
            .count(),
        1
    );
    let successes: Vec<_> = records
        .iter()
        .filter_map(|r| match r {
            StoredAdministrationAuditRecordV1::Service(r)
                if r.operation() == ServiceOperationV1::FenceReplicationPrimary
                    && r.phase() == ServiceAuditPhaseV1::Succeeded =>
            {
                Some(r)
            }
            _ => None,
        })
        .collect();
    assert_eq!(successes.len(), 3);
    for result in successes {
        assert_eq!(
            result.principal().unwrap().capability_id(),
            capability_id(3)
        );
        assert_eq!(
            result.link(),
            ServiceAuditLinkV1::ControlPlane {
                administration_sequence: riffdb_types::AdministrationSequence::new(
                    first.administration_sequence
                )
                .unwrap()
            }
        );
    }
}

#[test]
fn primary_fence_runtime_refuses_legacy_without_inventing_active_admission() {
    use riffdb_storage_api::ReplicationPrimaryAdmissionReadPort;
    let fixture = Fixture::new();
    let mut store = riffdb_storage_redb::RedbStore::open(fixture.database("primary")).unwrap();
    let id = riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).unwrap();
    riffdb_storage_redb::initialize_legacy_database_fixture(&mut store, id).unwrap();
    drop(store);
    let (lineage, _, _) = seed_primary(&fixture.database("primary"));
    assert_eq!(
        lineage.catalog_digest(),
        riffdb_storage_api::AuthoritativeStateCatalogV1.digest()
    );
    assert!(
        open_primary(&fixture.database("primary"))
            .read_replication_primary_admission()
            .is_err()
    );
    let mut primary = fixture.spawn("primary", None);
    assert!(
        !primary
            .wait_for_exit(std::time::Duration::from_secs(30))
            .unwrap()
            .status
            .success()
    );
    assert!(
        open_primary(&fixture.database("primary"))
            .read_replication_primary_admission()
            .is_err()
    );
}

async fn assert_fenced_health(
    client: &mut riffdb_client_rust::RiffDbClient,
    metadata: &CallMetadata,
    seed: u8,
) {
    let health = client
        .health(
            v1::HealthRequest {
                request_id: Some(request_id(seed)),
            },
            metadata,
        )
        .await
        .unwrap();
    let Some(v1::health_response::Result::Authenticated(health)) = health.result else {
        panic!("authenticated health");
    };
    assert_eq!(
        health
            .components
            .iter()
            .find(|c| c.component == v1::HealthComponentKind::CommitCoordinator as i32)
            .unwrap()
            .status,
        v1::HealthComponentStatus::Unavailable as i32,
        "a fenced primary cannot claim command readiness"
    );
}
