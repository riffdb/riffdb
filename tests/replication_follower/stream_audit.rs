//! Exact source audit survives daemon death; later denial is continuation only.
// req: REP-003, REP-005, REC-001, STO-012
use super::support::*;
use riffdb_api_grpc::generated::replication_service_client::ReplicationServiceClient;
use riffdb_proto::v1;
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    ChangelogFrameV3, ChangelogHistoryStateV3, StorageScanLimit, StoredAdministrationAuditRecordV1,
};
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1 as Phase, ServiceOperationV1};
use std::time::Duration;

fn frontier(value: Option<u64>) -> Option<v1::FrontierPosition> {
    Some(v1::FrontierPosition {
        position: Some(match value {
            Some(value) => v1::frontier_position::Position::AppliedThrough(value),
            None => v1::frontier_position::Position::BeforeFirst(v1::Unit {}),
        }),
    })
}
fn request(
    seed: u8,
    token: &str,
    history: ChangelogHistoryStateV3,
) -> tonic::Request<v1::StreamChangelogRequest> {
    let lineage = history.lineage();
    let after = history.tail();
    let mut request = tonic::Request::new(v1::StreamChangelogRequest {
        request_id: request_id(seed),
        database_id: lineage.database_id().as_bytes().to_vec(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        readable_format: ChangelogFrameV3::IDENTITY.into(),
        catalog_digest: lineage.catalog_digest().to_vec(),
        after: Some(v1::ReplicationPosition {
            transaction_sequence: after.sequence().get(),
            history_hash: after.history_hash().to_vec(),
            application_frontier: frontier(after.frontier().application().map(|v| v.get())),
            administration_frontier: frontier(after.frontier().administration().map(|v| v.get())),
        }),
        maximum_frame_bytes: riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES as u64,
        maximum_transitions: riffdb_storage_api::MAX_STAGED_COMMANDS as u64,
        ..Default::default()
    });
    let mut credential: tonic::metadata::MetadataValue<_> =
        format!("Bearer {token}").parse().unwrap();
    credential.set_sensitive(true);
    request.metadata_mut().insert("authorization", credential);
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_establishment_and_denial_audit_survive_source_kill_in_both_profiles() {
    for profile in ["standard", "hardened"] {
        let fixture = Fixture::new();
        let path = fixture.config("primary");
        let document = std::fs::read_to_string(&path).unwrap().replacen(
            "[server]\n",
            &format!("[server]\nredb_commit_profile = {profile:?}\n"),
            1,
        );
        std::fs::write(path, document).unwrap();
        let mut primary = fixture.start("primary", None);
        stop(&mut primary);
        let (_, admin, admin_token) = seed_primary(&fixture.database("primary"));
        let history = {
            let ports = open_primary(&fixture.database("primary"));
            ports
                .published_changelog_snapshot_v3()
                .unwrap()
                .authoritative_state_v3()
                .unwrap()
                .history()
        };
        primary = fixture.start("primary", None);
        let mut client = fixture.client("primary").await;
        let token = create_replication_capability(&mut client, &admin).await;
        super::workload::deploy(&mut client, &admin).await;
        let mut replication = ReplicationServiceClient::new(fixture.channel("primary").await);
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut denied = replication
                .stream_changelog(request(160, &admin_token, history))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                denied.message().await.unwrap().unwrap().item,
                Some(v1::stream_changelog_response::Item::Refusal(
                    v1::ReplicationRefusal::AuthorizationDenied as i32
                ))
            );
            assert!(denied.message().await.unwrap().is_none());
            let mut stream = replication
                .stream_changelog(request(161, &token, history))
                .await
                .unwrap()
                .into_inner();
            assert!(matches!(
                stream.message().await.unwrap().unwrap().item,
                Some(v1::stream_changelog_response::Item::Frame(_))
            ));
            client
                .revoke_capability(
                    v1::RevokeCapabilityRequest {
                        request_id: request_id(162),
                        capability_id: capability_id(2).as_bytes().to_vec(),
                        reason: v1::RevocationReason::Requested as i32,
                    },
                    &admin,
                )
                .await
                .unwrap();
            // A transport can already hold frames authorized before revocation.
            // Drain its bounded prefix; the next fresh check must close it.
            let mut refused = false;
            for _ in 0..128 {
                match stream.message().await.unwrap().unwrap().item {
                    Some(v1::stream_changelog_response::Item::Frame(_)) => {}
                    Some(v1::stream_changelog_response::Item::Refusal(value)) => {
                        assert_eq!(value, v1::ReplicationRefusal::AuthorizationDenied as i32);
                        refused = true;
                        break;
                    }
                    _ => panic!("tail emitted a different phase"),
                }
            }
            assert!(refused);
            assert!(stream.message().await.unwrap().is_none());
        })
        .await
        .unwrap();
        drop(replication);
        drop(client);
        assert!(
            !primary
                .kill(Duration::from_secs(30))
                .unwrap()
                .status
                .success()
        );
        primary = fixture.start("primary", None);
        stop(&mut primary);
        let ports = open_primary(&fixture.database("primary"));
        let AdministrationAuditScan::ExactEnd { records } = ports
            .scan_administration_audit(AdministrationAuditScanRequest::new(
                None,
                StorageScanLimit::new(128).unwrap(),
            ))
            .unwrap()
        else {
            panic!("bounded audit inventory");
        };
        let records: Vec<_> = records
            .into_iter()
            .filter_map(|record| match record.into_parts().0 {
                StoredAdministrationAuditRecordV1::Service(record)
                    if record.operation() == ServiceOperationV1::StreamChangelog =>
                {
                    Some(record)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            records.iter().map(|r| r.phase()).collect::<Vec<_>>(),
            [Phase::Denied, Phase::Started, Phase::Succeeded],
            "{profile}"
        );
        for record in records {
            let denied = record.phase() == Phase::Denied;
            assert_eq!(
                record.request_id().as_bytes().as_slice(),
                request_id(if denied { 160 } else { 161 })
            );
            assert_eq!(
                record.principal().unwrap().capability_id(),
                capability_id(if denied { 1 } else { 2 })
            );
            assert!(record.targets().as_slice().is_empty());
            assert_eq!(record.link(), ServiceAuditLinkV1::None);
        }
    }
}
