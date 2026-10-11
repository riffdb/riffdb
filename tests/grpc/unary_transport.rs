//! Complete authenticated unary payloads must precede their success trailers.
// req: API-001, MCP-010

use super::*;
use riffdb_types::{CanonicalRecord, CanonicalString, CanonicalValue};

pub(super) fn ready_projection_with_value(value: CanonicalValue) -> QueryProjectionResult {
    let frontier = FrontierPosition::AppliedThrough(CommitSequence::first());
    let identity = ProjectionIdentity::new(
        ContractLineage::new(LINEAGE).expect("projection lineage"),
        ProjectionId::first(),
        ProjectionPlanHash::from_bytes([7; 32]),
    );
    let row = ProjectionRow::new(
        vec![value],
        CanonicalRecord::new(Vec::new()).expect("empty result record"),
    )
    .expect("bounded projection row");
    let page = Page::new(
        PageLimit::new(1).expect("one row"),
        vec![row],
        None,
        ProjectionPageFence::new(identity, ProjectionGeneration::first(), frontier),
    )
    .expect("bounded complete page");
    QueryProjectionResult::Ready(
        QueryProjectionReady::new(page, frontier).expect("matching frontier"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_unary_responses_preserve_complete_payload_across_http2_frames() {
    // Include a chained payload, a response crossing one 16-KiB frame, the
    // observed discovery size, and a payload larger than the initial flow window.
    for (ordinal, size) in [256, 16_384, 56_263, 131_072].into_iter().enumerate() {
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("database ID");
        let environment = Environment::new("grpc-test").expect("environment");
        let audience = Audience::new("grpc-loopback").expect("audience");
        let principal = authenticated_principal(database_id, environment.clone(), audience.clone());
        let expected_capability = principal.capability_id();
        let expected_actor = principal.principal_id().clone();
        let value = CanonicalValue::String(
            CanonicalString::new("x".repeat(size)).expect("bounded payload"),
        );
        let service = Arc::new(ProjectionService {
            projection_payload: Some(value.clone()),
            ..ProjectionService::new()
        });
        let route: Arc<dyn GrpcLifecycleRoute> = Arc::new(ActiveRoute {
            service: service.clone(),
            security: CheckedGrpcSecurityContext::new(
                Arc::new(AcceptingAuthenticator { principal }),
                AuthenticationContext::new(database_id, environment, audience),
                Arc::new(
                    CapabilityDigestKeyProvider::parse_document(CAPABILITY_KEYS)
                        .expect("capability keys"),
                ),
            ),
            read_stage_telemetry: None,
        });
        let application = GrpcApplication::new(
            route,
            GrpcRequestLimits::new(Duration::from_secs(30)).expect("bounded request"),
        );
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().expect("loopback")).expect("listener");
        let address = incoming.local_addr().expect("bound address");
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(
            Server::builder()
                .add_service(application.query_server())
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = stopped.await;
                }),
        );
        let mut client = RiffDbClient::connect(
            Endpoint::from_shared(format!("http://{address}")).expect("loopback endpoint"),
        )
        .await
        .expect("client connection");
        let metadata = CallMetadata::authenticated(
            BearerCredential::new(CAPABILITY_TOKEN).expect("test credential"),
        );
        let id = request_id(u8::try_from(ordinal + 1).expect("bounded ordinal"));
        let response = client
            .query_projection(projection_request(id, 1, false), &metadata)
            .await;
        drop(client);
        stop.send(()).expect("running server");
        server.await.expect("server task").expect("clean shutdown");
        let response =
            response.unwrap_or_else(|error| panic!("complete payload size {size}: {error}"));
        assert!(response.encoded_len() > size);
        let Some(v1::query_projection_response::Result::Ready(ready)) = response.result else {
            panic!("expected complete ready result");
        };
        assert_applied_through(ready.frontier, 1);
        let data = ready.data.expect("complete data");
        assert_eq!(data.next_cursor, None);
        assert_eq!(data.items.len(), 1);
        assert_eq!(data.items[0].group.len(), 1);
        assert_eq!(
            riffdb_proto::canonical_value_from_proto(data.items[0].group[0].clone())
                .expect("canonical complete value"),
            value
        );
        assert_eq!(
            service.observed(),
            vec![ObservedInvocation {
                request_id: id,
                capability_id: expected_capability,
                principal_id: expected_actor,
                ingress: ServiceIngressKindV1::Grpc,
                operation: ServiceOperationV1::QueryProjection,
            }]
        );
    }
}
