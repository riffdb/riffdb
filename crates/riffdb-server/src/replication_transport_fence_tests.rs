//! Exact TLS source observation, one terminal item, current authority and audit.
// req: REP-003, REP-005
use super::*;
use riffdb_storage_api::{
    AuditPrincipalV1, ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
    ChangelogTransactionSequence as Sequence, PrimaryFenceRequestV1 as Selection,
    PrimaryFenceSourceEvidenceV1 as Evidence, StoredPrimaryFenceAdministrationV1 as Fence,
};
use riffdb_types::{
    DualFrontier, LeadershipEpochV1, ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1,
    ReplicationSourceHoldIdV1,
};

fn evidence(seed: u8) -> Evidence {
    let point = |seq, admin| {
        Point::new(
            Sequence::new(seq).unwrap(),
            [seed; 32],
            DualFrontier::new(None, AdministrationSequence::new(admin)),
        )
    };
    let fence = Fence::new(
        AdministrationSequence::new(3).unwrap(),
        timestamp(150),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(1, [seed; 10]).unwrap(),
        request_id(),
        AuditPrincipalV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1, [seed; 10]).unwrap(),
            NonZeroU64::MIN,
        ),
        None,
        ReplicationFollowerAuditTargetV1::new(
            database_id(),
            1,
            LeadershipEpochV1::initial(),
            ReplicationSourceHoldIdV1::new([seed; 16]).unwrap(),
        )
        .unwrap(),
        Sequence::new(2).unwrap(),
        point(4, 2),
    )
    .unwrap();
    let history = History::new(fence.lineage(), point(1, 0), point(5, 3), point(1, 0)).unwrap();
    Evidence::new(fence, point(3, 1), history).unwrap()
}
async fn verified(
    scope: &tempfile::TempDir,
    transport: &HostedGrpc,
) -> crate::VerifiedReplicationPeer {
    let HostedGrpcEndpoint::Tcp(address) = transport.endpoint() else {
        panic!("TLS TCP");
    };
    let trust = scope.path().join("source-ca.pem");
    fs::write(&trust, include_bytes!("../tests/fixtures/test-ca.pem")).unwrap();
    fs::set_permissions(&trust, fs::Permissions::from_mode(0o444)).unwrap();
    let endpoint = CanonicalHttpsEndpoint::parse(&format!("https://{address}")).unwrap();
    let config = riffdb_config::TlsClientConfig::new(
        endpoint.clone(),
        ProtectedFilePath::new(trust).unwrap(),
        endpoint.identity().clone(),
        Duration::from_secs(5),
        Duration::from_secs(30),
        std::num::NonZeroU32::MIN,
        std::num::NonZeroU32::MIN,
    )
    .unwrap();
    bounded(crate::VerifiedReplicationPeer::connect(
        config,
        RawCapabilityToken::parse_canonical(TOKEN.as_bytes()).unwrap(),
        riffdb_types::DatabaseAlias::new("default").unwrap(),
    ))
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_tls_fence_proof_requires_one_bound_item_eof_and_fresh_authority() {
    for case in 0..6 {
        let mut harness = Harness::new();
        let (scope, mut transport, channel) = tls(&harness.application).await;
        let peer = verified(&scope, &transport).await;
        let value = evidence(1);
        let fence = value.fence();
        let selected = Selection::new(
            request_id(),
            fence.operation_id(),
            fence.target(),
            fence.generation(),
        );
        let fetch = peer.authenticated_primary_fence(selected, value.applied());
        let produce = async {
            bounded(harness.entered.recv()).await.unwrap();
            if case == 4 {
                harness.authority.revoke();
            }
            let sent = if case == 1 {
                evidence(2)
            } else {
                value.clone()
            };
            harness
                .frames
                .send(Ok(if case == 3 {
                    None
                } else {
                    Some(ReplicationItem::FenceEvidence(Box::new(sent)))
                }))
                .await
                .unwrap();
            if case == 0 || case == 2 || case == 5 {
                bounded(harness.entered.recv()).await.unwrap();
                if case == 5 {
                    harness.authority.revoke();
                }
                harness
                    .frames
                    .send(Ok(if case == 2 {
                        Some(ReplicationItem::FenceEvidence(Box::new(value.clone())))
                    } else {
                        None
                    }))
                    .await
                    .unwrap();
            }
        };
        let (result, ()) = bounded(async { tokio::join!(fetch, produce) }).await;
        if case == 0 {
            let proof = result.unwrap();
            assert_eq!(
                format!("{proof:?}"),
                "AuthenticatedPrimaryFenceProof([redacted])"
            );
            assert_eq!(proof.into_evidence(), value);
        } else {
            assert!(result.is_err(), "invalid terminal proof case {case}");
        }
        {
            let requests = harness.source.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(
                matches!(requests[0].phase, ReplicationPhase::FenceEvidence { request } if request.target() == selected.target() && request.operation_id() == selected.operation_id() && request.generation() == selected.generation())
            );
            assert_eq!(requests[0].after_sequence, value.applied().sequence().get());
            assert_eq!(requests[0].after_hash, value.applied().history_hash());
        }
        assert_eq!(harness._audit.audit_submission_count(), 2);
        bounded(harness.dropped).await.unwrap();
        drop(peer);
        drop(channel);
        bounded(transport.drain_after_signal()).await.unwrap();
    }
}
