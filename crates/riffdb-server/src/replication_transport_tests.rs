//! Production listener and RPC/service checks with controlled frame custody.
//! Injected custody tests plus real source integration; no follower readiness claim.
// req: REP-003

use super::*;
use riffdb_api_grpc::{
    CheckedGrpcRestoreRetrySecurityContext, CheckedGrpcSecurityContext, GrpcBootstrapCompletion,
    GrpcDeploymentCompletion, GrpcOfflineMaintenanceOperation,
};
use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationClock, AuthenticationClockError, AuthenticationContext,
    AuthenticationFailure, CapabilityAuthenticator, CapabilityDigestKeyProvider,
    CapabilityReaderCurrentResolver, CredentialAuthenticator, NoopAuthenticationTelemetry,
    OpaqueCredential, RawCapabilityToken,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, DirectTlsListenerConfig, ProtectedFilePath, ServerTlsFiles,
};
use riffdb_policy::{
    AuthorizationClock, AuthorizationClockError, AuthorizationError, CurrentAuthorizer, Decision,
    NoopAuthorizationTelemetry, OperationRequest, ReplicationDecision,
};
use riffdb_proto::{generated::replication_service_client::ReplicationServiceClient, v1};
use riffdb_service::{
    ApplicationService, CurrentPolicyPort, HealthRequest, HealthResult,
    RecoveryOfflineMaintenanceApplication, ReplicationApplication, ReplicationFailure,
    ReplicationFuture, ReplicationItem, ReplicationItemSource, ReplicationPhase,
    ReplicationRequest, ReplicationService, ReplicationSourcePort, ReplicationStreamErrorV3,
    RestoreRetryOfflineMaintenanceApplication, ServiceFuture,
};
use riffdb_storage_api::{
    CapabilityLifecycleV1, CapabilityLookupResult, CapabilityReader, StorageError,
    StoredCapabilityRecordV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, Audience, CapabilityGrantV1, CapabilityId,
    CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
    CapabilityTokenDigest, DatabaseId, Environment, OfflineMaintenanceInputHash,
    OfflineMaintenanceOperationId, PartitionScopeV1, RequestId, RevocationReasonCodeV1,
    ServiceOperationV1, TenantScope, Timestamp,
};
use std::{
    num::{NonZeroU16, NonZeroU64},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::mpsc;
use tonic::{
    Code, Request,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

const TOKEN: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
const KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [1; 10]).unwrap()
}
fn request_id() -> RequestId {
    RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [2; 10]).unwrap()
}
fn environment() -> Environment {
    Environment::new("replication-transport").unwrap()
}
fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).unwrap()
}

struct Authority {
    database_id: DatabaseId,
    keys: Arc<CapabilityDigestKeyProvider>,
    record: Mutex<StoredCapabilityRecordV1>,
    authentications: AtomicUsize,
}

impl Authority {
    fn new() -> Arc<Self> {
        Self::for_database(database_id())
    }
    fn for_database(database_id: DatabaseId) -> Arc<Self> {
        let keys = Arc::new(CapabilityDigestKeyProvider::parse_document(KEYS).unwrap());
        let token = RawCapabilityToken::parse_canonical(TOKEN.as_bytes()).unwrap();
        let record = StoredCapabilityRecordV1::from_stored_parts(
            CapabilityId::from_unix_milliseconds_and_random(1_700_000_000_000, [3; 10]).unwrap(),
            NonZeroU64::MIN,
            keys.current_digest(&token),
            database_id,
            environment(),
            ActorId::new("replication-peer").unwrap(),
            ActorKind::Service,
            vec![Audience::new("grpc").unwrap()],
            timestamp(100),
            timestamp(200),
            AdministrationSequence::first(),
            request_id(),
            CapabilityGrantV1::new(
                TenantScope::Global,
                PartitionScopeV1::All,
                CapabilityPermissionsV1::new(vec![
                    CapabilityPermissionV1::unparameterized(
                        CapabilityPermissionKindV1::ReplicateChangelog,
                    )
                    .unwrap(),
                ])
                .unwrap(),
                vec![],
                NonZeroU16::MIN,
                vec![],
            )
            .unwrap(),
            CapabilityLifecycleV1::Active,
        )
        .unwrap();
        Arc::new(Self {
            database_id,
            keys,
            record: Mutex::new(record),
            authentications: AtomicUsize::new(0),
        })
    }

    fn revoke(&self) {
        let mut record = self.record.lock().unwrap();
        *record = record
            .revoked(
                record.revision(),
                timestamp(150),
                AdministrationSequence::new(2).unwrap(),
                RevocationReasonCodeV1::Requested,
            )
            .unwrap();
    }
}

impl CapabilityReader for Authority {
    fn read_capability(
        &self,
        id: CapabilityId,
    ) -> Result<Option<StoredCapabilityRecordV1>, StorageError> {
        let record = self.record.lock().unwrap();
        Ok((record.capability_id() == id).then(|| record.clone()))
    }
    fn resolve_capability_digests(
        &self,
        candidates: &[CapabilityTokenDigest],
    ) -> Result<CapabilityLookupResult, StorageError> {
        let record = self.record.lock().unwrap();
        Ok(if candidates.contains(&record.token_digest()) {
            CapabilityLookupResult::Found(Box::new(record.clone()))
        } else {
            CapabilityLookupResult::NotFound
        })
    }
}
impl AuthenticationClock for Authority {
    fn now(&self) -> Result<Timestamp, AuthenticationClockError> {
        Ok(timestamp(150))
    }
}
impl AuthorizationClock for Authority {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(timestamp(150))
    }
}
impl CredentialAuthenticator for Authority {
    fn authenticate(
        &self,
        credential: OpaqueCredential<'_>,
        context: &AuthenticationContext,
    ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
        self.authentications.fetch_add(1, Ordering::AcqRel);
        CapabilityAuthenticator::new(self, self.keys.as_ref(), self, &NoopAuthenticationTelemetry)
            .authenticate(credential, context)
    }
}
impl CurrentPolicyPort for Authority {
    fn authorize(
        &self,
        _: &AuthenticatedPrincipal,
        _: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        panic!("replication requires its administrative authorization path")
    }
    fn authorize_replication(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<ReplicationDecision, AuthorizationError> {
        CurrentAuthorizer::new(
            &CapabilityReaderCurrentResolver::new(self),
            self,
            &NoopAuthorizationTelemetry,
            self.database_id,
            environment(),
        )
        .authorize_replication(principal)
    }
}

type Frame = Result<Option<ReplicationItem>, ReplicationFailure>;
struct Frames {
    frames: mpsc::Receiver<Frame>,
    entered: mpsc::Sender<()>,
    dropped: Option<oneshot::Sender<()>>,
}
impl ReplicationItemSource for Frames {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        Box::pin(async move {
            self.entered.try_send(()).unwrap();
            self.frames.recv().await.unwrap()
        })
    }
}
impl Drop for Frames {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}
struct Source {
    frames: Mutex<Option<Frames>>,
    requests: Mutex<Vec<ReplicationRequest>>,
    refusal: Mutex<Option<ReplicationFailure>>,
}
impl ReplicationSourcePort for Source {
    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        self.requests.lock().unwrap().push(request);
        let refusal = self.refusal.lock().unwrap().take();
        let frames = if refusal.is_none() {
            self.frames.lock().unwrap().take()
        } else {
            None
        };
        Box::pin(async move {
            if let Some(error) = refusal {
                return Err(error);
            }
            Ok(Box::new(frames.unwrap()) as Box<dyn ReplicationItemSource>)
        })
    }
}

struct Route {
    service: Arc<dyn ReplicationApplication>,
    security: CheckedGrpcSecurityContext,
    ready: AtomicBool,
}
impl GrpcLifecycleRoute for Route {
    fn admit_replication(&self) -> Option<Arc<dyn ReplicationApplication>> {
        self.ready
            .load(Ordering::Acquire)
            .then(|| self.service.clone())
    }
    fn admit_authenticated(&self, _: ServiceOperationV1) -> Option<Arc<dyn ApplicationService>> {
        None
    }
    fn admit_offline_maintenance(
        &self,
        _: GrpcOfflineMaintenanceOperation,
    ) -> Option<Arc<dyn ApplicationService>> {
        None
    }
    fn admit_restore_retry(
        &self,
        _: OfflineMaintenanceOperationId,
        _: OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RestoreRetryOfflineMaintenanceApplication>> {
        None
    }
    fn admit_recovery_restore(
        &self,
        _: OfflineMaintenanceOperationId,
        _: OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>> {
        None
    }
    fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
        Some(self.security.clone())
    }
    fn restore_retry_security_context(&self) -> Option<CheckedGrpcRestoreRetrySecurityContext> {
        None
    }
    fn server_generation(&self) -> Option<[u8; 16]> {
        None
    }
    fn history_incarnation(&self) -> Option<u64> {
        Some(1)
    }
    fn restricted_health(&self, _: HealthRequest) -> Option<ServiceFuture<'_, HealthResult>> {
        None
    }
    fn bootstrap_available(&self) -> bool {
        false
    }
    fn begin_bootstrap(&self) -> Option<Arc<dyn ApplicationService>> {
        None
    }
    fn finish_bootstrap(&self, _: GrpcBootstrapCompletion) {}
    fn finish_deployment(&self, _: GrpcDeploymentCompletion) {}
}

struct Harness {
    authority: Arc<Authority>,
    source: Arc<Source>,
    route: Arc<Route>,
    frames: mpsc::Sender<Frame>,
    entered: mpsc::Receiver<()>,
    dropped: oneshot::Receiver<()>,
    application: GrpcApplication,
}
impl Harness {
    fn new() -> Self {
        let authority = Authority::new();
        let (frames, receiver) = mpsc::channel(1);
        let (entered_sender, entered) = mpsc::channel(4);
        let (dropped_sender, dropped) = oneshot::channel();
        let source = Arc::new(Source {
            frames: Mutex::new(Some(Frames {
                frames: receiver,
                entered: entered_sender,
                dropped: Some(dropped_sender),
            })),
            requests: Mutex::new(vec![]),
            refusal: Mutex::new(None),
        });
        let route = Arc::new(Route {
            service: Arc::new(ReplicationService::new(authority.clone(), source.clone())),
            security: CheckedGrpcSecurityContext::new(
                authority.clone(),
                AuthenticationContext::new(
                    database_id(),
                    environment(),
                    Audience::new("grpc").unwrap(),
                ),
                authority.keys.clone(),
            ),
            ready: AtomicBool::new(true),
        });
        let application = GrpcApplication::new(
            route.clone(),
            GrpcRequestLimits::new(Duration::from_secs(10)).unwrap(),
        );
        Self {
            authority,
            source,
            route,
            frames,
            entered,
            dropped,
            application,
        }
    }
}

fn request() -> Request<v1::StreamChangelogRequest> {
    let frontier = |sequence| {
        Some(v1::FrontierPosition {
            position: Some(v1::frontier_position::Position::AppliedThrough(sequence)),
        })
    };
    let mut request = Request::new(v1::StreamChangelogRequest {
        request_id: request_id().into_bytes().to_vec(),
        database_id: database_id().into_bytes().to_vec(),
        history_incarnation: 7,
        leadership_epoch: 11,
        readable_format: "riffdb-changelog-v3".into(),
        catalog_digest: vec![0x51; 32],
        after: Some(v1::ReplicationPosition {
            transaction_sequence: 19,
            history_hash: vec![0x52; 32],
            application_frontier: frontier(3),
            administration_frontier: frontier(5),
        }),
        maximum_frame_bytes: 32 * 1024 * 1024,
        maximum_transitions: 256,
        bootstrap: None,
        attachment: None,
        follower_hold_id: vec![],
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    request
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("bounded test operation")
}

// The checked-in client rejects malformed outgoing messages locally. This
// test-only codec skips that validation to exercise the server's wire boundary.
struct UncheckedRequestCodec;
struct UncheckedRequestEncoder;
struct ResponseDecoder;
impl tonic::codec::Codec for UncheckedRequestCodec {
    type Encode = v1::StreamChangelogRequest;
    type Decode = v1::StreamChangelogResponse;
    type Encoder = UncheckedRequestEncoder;
    type Decoder = ResponseDecoder;
    fn encoder(&mut self) -> Self::Encoder {
        UncheckedRequestEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        ResponseDecoder
    }
}
impl tonic::codec::Encoder for UncheckedRequestEncoder {
    type Item = v1::StreamChangelogRequest;
    type Error = tonic::Status;
    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        fn unchecked_encode<M: riffdb_proto::PublicMessage>(
            item: M,
            destination: &mut tonic::codec::EncodeBuf<'_>,
        ) -> Result<(), tonic::Status> {
            item.encode(destination)
                .map_err(|_| tonic::Status::internal("test request encoding failed"))
        }
        unchecked_encode(item, destination)
    }
}
impl tonic::codec::Decoder for ResponseDecoder {
    type Item = v1::StreamChangelogResponse;
    type Error = tonic::Status;
    fn decode(
        &mut self,
        source: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        fn decode<M: riffdb_proto::PublicMessage>(
            source: &mut tonic::codec::DecodeBuf<'_>,
        ) -> Result<Option<M>, tonic::Status> {
            M::decode(source)
                .map(Some)
                .map_err(|_| tonic::Status::internal("test response decoding failed"))
        }
        decode(source)
    }
}

async fn raw_request(
    client: &mut tonic::client::Grpc<Channel>,
    request: Request<v1::StreamChangelogRequest>,
) -> Result<tonic::Response<tonic::Streaming<v1::StreamChangelogResponse>>, tonic::Status> {
    client.ready().await.unwrap();
    client
        .server_streaming(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/riffdb.v1.ReplicationService/StreamChangelog",
            ),
            UncheckedRequestCodec,
        )
        .await
}

async fn tls(application: &GrpcApplication) -> (tempfile::TempDir, HostedGrpc, Channel) {
    let scope = tempfile::TempDir::with_prefix("riffdb-replication-tls-").unwrap();
    fs::set_permissions(scope.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let certificate = scope.path().join("server.pem");
    let key = scope.path().join("server.key");
    fs::write(
        &certificate,
        include_bytes!("../tests/fixtures/localhost-cert.pem"),
    )
    .unwrap();
    fs::write(&key, include_bytes!("../tests/fixtures/localhost-key.pem")).unwrap();
    fs::set_permissions(&certificate, fs::Permissions::from_mode(0o444)).unwrap();
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);
    let endpoint = format!("https://{address}");
    let listener = DirectTlsListenerConfig::new(
        address,
        CanonicalHttpsEndpoint::parse(&endpoint).unwrap(),
        ServerTlsFiles::new(
            ProtectedFilePath::new(certificate).unwrap(),
            ProtectedFilePath::new(key).unwrap(),
        )
        .unwrap(),
        ListenerBounds::alpha_default(),
    )
    .unwrap();
    // HostedGrpc chooses the confidential application instance from the actual
    // listener configuration. The test never sets that flag itself.
    let transport =
        HostedGrpc::bind(ApplicationListenerConfig::DirectTls(listener), application).unwrap();
    let channel = bounded(
        Endpoint::from_shared(endpoint)
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(include_bytes!(
                        "../tests/fixtures/test-ca.pem"
                    )))
                    .domain_name("127.0.0.1"),
            )
            .unwrap()
            .connect(),
    )
    .await
    .unwrap();
    (scope, transport, channel)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_cleartext_listener_refuses_before_authentication_even_with_proxy_claim() {
    let harness = Harness::new();
    let listener = LoopbackCleartextListener::new("127.0.0.1:0".parse().unwrap()).unwrap();
    let mut transport = HostedGrpc::bind(
        ApplicationListenerConfig::LoopbackCleartext(listener),
        &harness.application,
    )
    .unwrap();
    let HostedGrpcEndpoint::Tcp(address) = transport.endpoint() else {
        panic!("TCP listener");
    };
    let channel = bounded(
        Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect(),
    )
    .await
    .unwrap();
    let mut client = ReplicationServiceClient::new(channel);
    let mut request = request();
    request
        .metadata_mut()
        .insert("x-forwarded-proto", "https".parse().unwrap());
    assert_eq!(
        bounded(client.stream_changelog(request))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(harness.authority.authentications.load(Ordering::Acquire), 0);
    assert!(harness.source.requests.lock().unwrap().is_empty());
    drop(client);
    bounded(transport.drain_after_signal()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_tls_binds_handshake_and_withholds_bytes_after_revocation_or_lifecycle_stop() {
    for revoke in [true, false] {
        let mut harness = Harness::new();
        let (_scope, mut transport, channel) = tls(&harness.application).await;
        let mut client = ReplicationServiceClient::new(channel);
        let mut stream = bounded(client.stream_changelog(request()))
            .await
            .unwrap()
            .into_inner();
        bounded(harness.entered.recv()).await.unwrap();
        let observed = harness.source.requests.lock().unwrap()[0].clone();
        assert_eq!(observed.database_id, database_id());
        assert_eq!(
            (
                observed.history_incarnation,
                observed.leadership_epoch,
                observed.after_sequence
            ),
            (7, 11, 19)
        );
        assert_eq!(observed.after_hash, [0x52; 32]);
        assert_eq!(observed.catalog_digest, [0x51; 32]);
        assert_eq!(observed.readable_format, "riffdb-changelog-v3");
        assert_eq!(
            observed.after_frontier,
            riffdb_types::DualFrontier::new(
                riffdb_types::CommitSequence::new(3),
                AdministrationSequence::new(5)
            )
        );
        assert_eq!(
            (observed.maximum_frame_bytes, observed.maximum_transitions),
            (32 * 1024 * 1024, 256)
        );
        harness
            .frames
            .try_send(Ok(Some(ReplicationItem::Frame(vec![1, 2, 3]))))
            .unwrap();
        assert_eq!(
            bounded(stream.message()).await.unwrap().unwrap().item,
            Some(v1::stream_changelog_response::Item::Frame(vec![1, 2, 3]))
        );
        bounded(harness.entered.recv()).await.unwrap();
        if revoke {
            harness.authority.revoke();
        } else {
            harness.route.ready.store(false, Ordering::Release);
        }
        harness
            .frames
            .try_send(Ok(Some(ReplicationItem::Frame(vec![4, 5, 6]))))
            .unwrap();
        let expected = if revoke {
            v1::ReplicationRefusal::AuthorizationDenied
        } else {
            v1::ReplicationRefusal::Unavailable
        };
        assert_eq!(
            bounded(stream.message()).await.unwrap().unwrap().item,
            Some(v1::stream_changelog_response::Item::Refusal(
                expected as i32
            ))
        );
        assert!(bounded(stream.message()).await.unwrap().is_none());
        bounded(harness.dropped).await.unwrap();
        drop(stream);
        drop(client);
        bounded(transport.drain_after_signal()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_tls_rejects_bad_credentials_and_preserves_typed_terminal_refusals() {
    let harness = Harness::new();
    let (_scope, mut transport, channel) = tls(&harness.application).await;
    let mut raw = tonic::client::Grpc::new(channel.clone());
    let mut client = ReplicationServiceClient::new(channel);
    let mut malformed = request();
    malformed
        .get_mut()
        .after
        .as_mut()
        .unwrap()
        .history_hash
        .pop();
    assert_eq!(
        bounded(raw_request(&mut raw, malformed))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut oversized = request();
    oversized.get_mut().readable_format = "x".repeat(1025);
    assert_eq!(
        bounded(raw_request(&mut raw, oversized))
            .await
            .unwrap_err()
            .code(),
        Code::OutOfRange
    );
    assert_eq!(
        harness.authority.authentications.load(Ordering::Acquire),
        0,
        "wire bounds and shape are checked before credentials or source access"
    );
    let mut missing = request();
    missing.metadata_mut().remove("authorization");
    assert_eq!(
        bounded(client.stream_changelog(missing))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    let mut bootstrap = request();
    bootstrap.metadata_mut().insert_bin(
        "riffdb-bootstrap-token-bin",
        tonic::metadata::MetadataValue::from_bytes(b"bootstrap"),
    );
    assert_eq!(
        bounded(client.stream_changelog(bootstrap))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    let mut invalid = request();
    invalid.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", "A".repeat(TOKEN.len()))
            .parse()
            .unwrap(),
    );
    assert_eq!(
        bounded(client.stream_changelog(invalid))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert!(harness.source.requests.lock().unwrap().is_empty());
    use ReplicationStreamErrorV3 as S;
    use v1::ReplicationRefusal as R;
    for (source, expected) in [
        (S::ForeignLineage, R::ForeignLineage),
        (S::StaleEpoch, R::StaleEpoch),
        (S::HistoryPruned, R::HistoryPruned),
        (S::UnsupportedFormat, R::UnsupportedFormat),
        (S::UnsupportedCatalog, R::UnsupportedCatalog),
        (S::UnsupportedBounds, R::UnsupportedBounds),
        (S::InvalidPosition, R::InvalidPosition),
        (S::CorruptHistory, R::CorruptHistory),
        (S::Unavailable, R::Unavailable),
    ] {
        *harness.source.refusal.lock().unwrap() = Some(ReplicationFailure::Source(source));
        let mut stream = bounded(client.stream_changelog(request()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            bounded(stream.message()).await.unwrap().unwrap().item,
            Some(v1::stream_changelog_response::Item::Refusal(
                expected as i32
            ))
        );
        assert!(bounded(stream.message()).await.unwrap().is_none());
    }
    drop(client);
    bounded(transport.drain_after_signal()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_tls_cancelled_client_releases_its_pending_source() {
    let mut harness = Harness::new();
    let (_scope, mut transport, channel) = tls(&harness.application).await;
    let mut client = ReplicationServiceClient::new(channel);
    let stream = bounded(client.stream_changelog(request()))
        .await
        .unwrap()
        .into_inner();
    bounded(harness.entered.recv()).await.unwrap();
    drop(stream);
    bounded(harness.dropped).await.unwrap();
    drop(client);
    bounded(transport.drain_after_signal()).await.unwrap();
}

fn bootstrap_wire_request(resume: bool) -> Request<v1::StreamChangelogRequest> {
    let mut request = request();
    let message = request.get_mut();
    message.after = None;
    message.bootstrap = Some(v1::ReplicationBootstrapRequest {
        hold_id: vec![0x18; 16],
        resume_manifest: if resume { vec![1, 2, 3] } else { vec![] },
        after_page: if resume { 3 } else { 0 },
    });
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_tls_bootstrap_maps_resume_and_withholds_manifest_or_page_after_revocation() {
    for withhold_page in [false, true] {
        let mut harness = Harness::new();
        let (_scope, mut transport, channel) = tls(&harness.application).await;
        let mut client = ReplicationServiceClient::new(channel);
        let mut stream = bounded(client.stream_changelog(bootstrap_wire_request(withhold_page)))
            .await
            .unwrap()
            .into_inner();
        bounded(harness.entered.recv()).await.unwrap();
        let observed = harness.source.requests.lock().unwrap()[0].clone();
        assert_eq!(observed.after_sequence, 0);
        assert_eq!(observed.after_hash, [0; 32]);
        assert_eq!(observed.after_frontier, riffdb_types::DualFrontier::INITIAL);
        assert!(
            matches!(observed.phase, ReplicationPhase::Bootstrap { hold_id, after_page, resume_manifest }
            if hold_id == [0x18; 16] && after_page == if withhold_page {3} else {0} && resume_manifest == if withhold_page {vec![1,2,3]} else {vec![]})
        );
        if withhold_page {
            harness
                .frames
                .try_send(Ok(Some(ReplicationItem::BootstrapManifest(vec![1, 2, 3]))))
                .unwrap();
            assert_eq!(
                bounded(stream.message()).await.unwrap().unwrap().item,
                Some(v1::stream_changelog_response::Item::BootstrapManifest(
                    vec![1, 2, 3]
                ))
            );
            bounded(harness.entered.recv()).await.unwrap();
        }
        harness.authority.revoke();
        let item = if withhold_page {
            ReplicationItem::BootstrapPage(vec![4, 5])
        } else {
            ReplicationItem::BootstrapManifest(vec![1, 2, 3])
        };
        harness.frames.try_send(Ok(Some(item))).unwrap();
        assert_eq!(
            bounded(stream.message()).await.unwrap().unwrap().item,
            Some(v1::stream_changelog_response::Item::Refusal(
                v1::ReplicationRefusal::AuthorizationDenied as i32
            ))
        );
        assert!(bounded(stream.message()).await.unwrap().is_none());
        bounded(harness.dropped).await.unwrap();
        drop(stream);
        drop(client);
        bounded(transport.drain_after_signal()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_tls_delivers_the_full_bootstrap_page_ceiling_then_ends_without_tail() {
    let mut harness = Harness::new();
    let (_scope, mut transport, channel) = tls(&harness.application).await;
    let mut client =
        ReplicationServiceClient::new(channel).max_decoding_message_size(32 * 1024 * 1024 + 1024);
    let mut stream = bounded(client.stream_changelog(bootstrap_wire_request(false)))
        .await
        .unwrap()
        .into_inner();
    bounded(harness.entered.recv()).await.unwrap();
    harness
        .frames
        .try_send(Ok(Some(ReplicationItem::BootstrapManifest(vec![1; 512]))))
        .unwrap();
    assert_eq!(
        bounded(stream.message()).await.unwrap().unwrap().item,
        Some(v1::stream_changelog_response::Item::BootstrapManifest(
            vec![1; 512]
        ))
    );
    bounded(harness.entered.recv()).await.unwrap();
    let page = vec![0x81; riffdb_storage_api::MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES];
    harness
        .frames
        .try_send(Ok(Some(ReplicationItem::BootstrapPage(page.clone()))))
        .unwrap();
    let delivered = bounded(stream.message()).await.unwrap().unwrap();
    assert!(
        matches!(delivered.item, Some(v1::stream_changelog_response::Item::BootstrapPage(bytes)) if bytes == page)
    );
    bounded(harness.entered.recv()).await.unwrap();
    harness.frames.try_send(Ok(None)).unwrap();
    assert!(bounded(stream.message()).await.unwrap().is_none());
    bounded(harness.dropped).await.unwrap();
    drop(stream);
    drop(client);
    bounded(transport.drain_after_signal()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_tls_attachment_keeps_exact_acknowledgement_and_manifest_in_one_request() {
    let mut harness = Harness::new();
    let (_scope, mut transport, channel) = tls(&harness.application).await;
    let mut client = ReplicationServiceClient::new(channel);
    let mut request = request();
    let message = request.get_mut();
    message.attachment = Some(v1::ReplicationBootstrapAttachment {
        manifest: vec![0x71; 512],
        acknowledged: message.after.take(),
    });
    let mut stream = bounded(client.stream_changelog(request))
        .await
        .unwrap()
        .into_inner();
    bounded(harness.entered.recv()).await.unwrap();
    let observed = harness.source.requests.lock().unwrap()[0].clone();
    assert!(
        matches!(observed.phase, ReplicationPhase::Attach { manifest } if manifest == vec![0x71; 512])
    );
    assert_eq!(observed.after_sequence, 19);
    assert_eq!(observed.after_hash, [0x52; 32]);
    assert_eq!(
        observed.after_frontier,
        riffdb_types::DualFrontier::new(
            riffdb_types::CommitSequence::new(3),
            AdministrationSequence::new(5)
        )
    );
    harness
        .frames
        .try_send(Ok(Some(ReplicationItem::Frame(vec![7, 8]))))
        .unwrap();
    assert_eq!(
        bounded(stream.message()).await.unwrap().unwrap().item,
        Some(v1::stream_changelog_response::Item::Frame(vec![7, 8]))
    );
    bounded(harness.entered.recv()).await.unwrap();
    harness.frames.try_send(Ok(None)).unwrap();
    assert!(bounded(stream.message()).await.unwrap().is_none());
    bounded(harness.dropped).await.unwrap();
    drop(stream);
    drop(client);
    bounded(transport.drain_after_signal()).await.unwrap();
}

#[path = "replication_transport_source_tests.rs"]
mod real_source;
