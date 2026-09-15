//! Real source capability transitions through the completed-prefix publication.
// req: REP-002, REP-003, PERF-007
use super::*;
use crate::auth_adapters::{ServerCredentialAuthenticator, ServerCurrentPolicyPort};
use crate::clocks::ProductionWallClocks;
use riffdb_api_grpc::GrpcLifecycleRoute;
use riffdb_auth::*;
use riffdb_policy::{
    Decision, NoopAuthorizationTelemetry, OperationRequest, TrustedAudienceCatalog,
};
use riffdb_service::CurrentPolicyPort;
use riffdb_storage_api::*;
use riffdb_types::*;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

struct UnavailablePeer;
impl ReplicationSourcePort for UnavailablePeer {
    fn open(&self, _: ReplicationRequest) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async { Err(Failure::Unavailable) })
    }
}

async fn catch_up(
    receiver: &mut FollowerReceiver,
    peer: &FinitePeer,
    readers: &FollowerReadSnapshots,
) {
    let target = peer.history().tail().sequence();
    for _ in 0..16 {
        receiver.advance(peer).await.unwrap();
        if readers.latest().unwrap().history().tail().sequence() >= target {
            return;
        }
    }
    panic!("bounded receiver did not reach the source capability transition");
}

#[tokio::test]
async fn follower_authentication_rechecks_published_capabilities_and_withdraws_on_close() {
    let (mut fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let (evidence, readers, notifier) = receiver.prepare_service().await.unwrap();
    let initial = readers.latest().unwrap();
    assert_eq!(
        receiver.advance(&UnavailablePeer).await,
        Err(Failure::Unavailable)
    );
    assert!(Arc::ptr_eq(&initial, &readers.latest().unwrap()));
    drop(initial);

    let keys = Arc::new(CapabilityDigestKeyProvider::parse_document(
        b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
    ).unwrap());
    let issued = issue_capability_token(&SystemEntropy, &keys).unwrap();
    let token = issued.text().expose_secret().to_owned();
    let id = CapabilityId::from_unix_milliseconds_and_random(9, [0x9c; 10]).unwrap();
    let projection_reads =
        crate::projection_read_source::ProjectionReadSource::new(readers.clone());
    let pinned_before_grant = projection_reads.pin().unwrap();
    assert!(pinned_before_grant.read_capability(id).unwrap().is_none());
    let request = RequestId::from_unix_milliseconds_and_random(9, [0x9d; 10]).unwrap();
    let environment = Environment::new("follower-read-test").unwrap();
    let audience = Audience::new("riffdb-test").unwrap();
    let database = fixture.manifest.fence().history().lineage().database_id();
    let clocks = ProductionWallClocks::settable(Arc::new(1001.into()));
    let auth = ServerCredentialAuthenticator::new(
        readers.clone(),
        keys,
        clocks.authentication(),
        Arc::new(NoopAuthenticationTelemetry),
    );
    let policy = ServerCurrentPolicyPort::new(
        readers.clone(),
        clocks.authorization(),
        database,
        environment.clone(),
        TrustedAudienceCatalog::new(vec![audience.clone()]).unwrap(),
        Arc::new(NoopAuthorizationTelemetry),
    );
    let context = AuthenticationContext::new(database, environment.clone(), audience.clone());
    assert!(
        auth.authenticate(OpaqueCredential::new(&token), &context)
            .is_err()
    );

    let graph_keys = crate::process_graph::ProductionDigestKeys::new(DigestKeyProviders::parse_documents(
        b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
        b"riffdb-idempotency-digest-keys-v1\n1:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n",
    ).unwrap());
    let build = riffdb_service::BuildInfo::new(
        "0.1.0",
        "test",
        "rustc-1.97.0",
        vec![],
        evidence.retained_metadata.storage_format_version().get(),
        riffdb_contract_ir::EXECUTABLE_IR_VERSION_V1,
        riffdb_api_mcp::MCP_PROTOCOL_VERSION,
    )
    .unwrap();
    let (initializing, activator, issuer) = riffdb_service::RiffDbService::begin_initialization();
    let lifecycle = Arc::new(crate::lifecycle::ProductionLifecycleRoute::new(
        initializing,
        issuer,
        crate::runtime_support::RuntimeRoutingState::new(),
    ));
    let mcp_audience = Audience::new("riffdb-mcp-test").unwrap();
    let graph = crate::process_graph::RunningFollowerService::start(
        evidence,
        readers.clone(),
        notifier,
        &fixture.path.with_extension("projections"),
        activator,
        graph_keys,
        audience.clone(),
        Some(mcp_audience.clone()),
        environment.clone(),
        riffdb_service::ServiceProcessMetadata::new(Timestamp::new(1001, 0).unwrap(), build),
        &clocks,
        lifecycle.clone(),
    )
    .unwrap();
    assert!(!lifecycle.bootstrap_available());
    assert!(lifecycle.begin_bootstrap().is_none());

    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .unwrap(),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadContract)
                .unwrap(),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                .unwrap(),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadStatistics)
                .unwrap(),
        ])
        .unwrap(),
        vec![],
        NonZeroU16::new(10).unwrap(),
        vec![],
    )
    .unwrap();
    let intent = CapabilityBootstrapIntentV1::new(
        id,
        CapabilityRequestedRecordV1::new(
            database,
            environment,
            ActorId::new("replicated-reader").unwrap(),
            ActorKind::Human,
            NonZeroU32::new(3600).unwrap(),
            vec![audience, mcp_audience],
            grant,
        )
        .unwrap(),
        BootstrapDigestCandidatesV1::new(vec![issued.digest()], issued.digest()).unwrap(),
        Timestamp::new(1000, 0).unwrap(),
        Timestamp::new(4600, 0).unwrap(),
        BootstrapServiceAuditStartV1::new(
            request,
            Timestamp::new(1000, 0).unwrap(),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(id)]).unwrap(),
            None,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        Arc::get_mut(&mut fixture.peer.ports)
            .unwrap()
            .bootstrap_capability(&intent)
            .unwrap(),
        CapabilityBootstrapResult::BootstrapCreated { .. }
    ));
    assert!(
        auth.authenticate(OpaqueCredential::new(&token), &context)
            .is_err(),
        "source publication alone cannot expose follower authority"
    );
    catch_up(&mut receiver, &fixture.peer, &readers).await;
    assert!(
        projection_reads
            .pin()
            .unwrap()
            .read_capability(id)
            .unwrap()
            .is_some()
    );
    assert!(pinned_before_grant.read_capability(id).unwrap().is_none());
    drop(pinned_before_grant);
    let principal = auth
        .authenticate(OpaqueCredential::new(&token), &context)
        .unwrap();
    assert!(matches!(
        policy.authorize(&principal, OperationRequest::get_active_contract()),
        Ok(Decision::Allow(_))
    ));
    let mcp = graph.hosted_mcp_dependencies().unwrap();
    let mcp_principal = mcp
        .authenticator
        .authenticate(OpaqueCredential::new(&token), &mcp.authentication)
        .unwrap();
    let mcp_request = riffdb_service::RequestContext::from_authenticated_mcp_http(
        request,
        mcp_principal,
        riffdb_service::RequestControl::new(
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .0,
        None,
    );
    assert!(matches!(
        mcp.service
            .get_active_contract(mcp_request, riffdb_service::GetActiveContractRequest)
            .await
            .unwrap(),
        riffdb_service::GetActiveContractResult::Absent
    ));
    let grpc = riffdb_api_grpc::GrpcApplication::new_with_audience(
        lifecycle.clone(),
        riffdb_api_grpc::GrpcRequestLimits::new(std::time::Duration::from_secs(5)).unwrap(),
        context.audience().clone(),
    );
    let grpc_request = || {
        let mut message = tonic::Request::new(riffdb_proto::v1::GetActiveContractRequest {
            request_id: request.as_bytes().to_vec(),
        });
        message.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", std::str::from_utf8(&token).unwrap())
                .parse()
                .unwrap(),
        );
        message
    };
    use riffdb_api_grpc::generated::contract_service_server::ContractService;
    let reply = grpc
        .get_active_contract(grpc_request())
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        reply.result,
        Some(riffdb_proto::v1::get_active_contract_response::Result::Absent(_))
    ));
    let service = lifecycle
        .admit_authenticated(ServiceOperationV1::GetActiveContract)
        .unwrap();
    let request_context = || {
        riffdb_service::RequestContext::from_authenticated_grpc(
            request,
            principal.clone(),
            riffdb_service::RequestControl::new(
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .0,
            None,
        )
    };
    let before_reads = readers.latest().unwrap().history();
    assert!(matches!(
        service
            .get_active_contract(request_context(), riffdb_service::GetActiveContractRequest)
            .await
            .unwrap(),
        riffdb_service::GetActiveContractResult::Absent
    ));
    service
        .health(
            riffdb_service::HealthContext::authenticated(request_context()),
            riffdb_service::HealthRequest,
        )
        .await
        .unwrap();
    service
        .statistics(request_context(), riffdb_service::StatisticsRequest)
        .await
        .unwrap();
    let refusal = service
        .revoke_capability(
            request_context(),
            riffdb_service::RevokeCapabilityRequest::new(id, RevocationReasonCodeV1::Requested),
        )
        .await
        .unwrap_err();
    assert_eq!(
        refusal.public_error().unwrap().kind(),
        riffdb_errors::PublicErrorKind::FollowerMode
    );
    assert_eq!(readers.latest().unwrap().history(), before_reads);
    assert!(policy.capability_view_checkpoint().is_none());
    let historical = readers.open_owned_snapshot().unwrap();

    let actor = AuditPrincipalV1::new(
        ActorId::new("replicated-reader").unwrap(),
        ActorKind::Human,
        id,
        NonZeroU64::MIN,
    );
    let (awaiting, current) = Arc::get_mut(&mut fixture.peer.ports)
        .unwrap()
        .begin_capability_revoke(CapabilityRevokeCandidateV1::new(
            id,
            request,
            actor.clone(),
            None,
            RevocationReasonCodeV1::Requested,
        ))
        .unwrap()
        .read_transaction_current()
        .unwrap();
    assert!(matches!(
        awaiting
            .commit_revoke(CapabilityRevokeIntentV1::new(
                id,
                current.target().unwrap().revision(),
                request,
                Timestamp::new(1001, 0).unwrap(),
                actor,
                None,
                RevocationReasonCodeV1::Requested
            ))
            .unwrap(),
        CapabilityRevokeResult::Revoked { .. }
    ));
    catch_up(&mut receiver, &fixture.peer, &readers).await;
    assert!(
        auth.authenticate(OpaqueCredential::new(&token), &context)
            .is_err()
    );
    assert!(matches!(
        policy.authorize(&principal, OperationRequest::get_active_contract()),
        Ok(Decision::Deny(_))
    ));
    assert!(matches!(
        historical.read_capability(id).unwrap().unwrap().lifecycle(),
        CapabilityLifecycleV1::Active
    ));
    assert!(
        service
            .get_active_contract(request_context(), riffdb_service::GetActiveContractRequest)
            .await
            .is_err()
    );
    assert_eq!(
        grpc.get_active_contract(grpc_request())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert!(matches!(
        mcp.authenticator
            .authenticate(OpaqueCredential::new(&token), &mcp.authentication),
        Err(AuthenticationFailure::CapabilityRevoked)
    ));
    drop(historical);
    receiver.close().await.unwrap();
    assert!(projection_reads.pin().is_err());
    assert!(
        lifecycle
            .admit_authenticated(ServiceOperationV1::GetActiveContract)
            .is_none()
    );
    drop(service);
    graph.shutdown().await.unwrap();
    assert!(readers.read_capability(id).is_err());
    assert!(readers.open_owned_snapshot().is_err());
    assert!(
        auth.authenticate(OpaqueCredential::new(&token), &context)
            .is_err()
    );
    assert!(crate::startup::open_redb_follower_startup(&fixture.path, inputs()).is_ok());
}
