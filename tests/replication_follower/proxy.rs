//! Test-only confidential relay. Source authentication and bytes remain owned
//! by the real primary; barriers control delivery, never authority or contents.
use riffdb_api_grpc::generated::{
    replication_service_client::ReplicationServiceClient,
    replication_service_server::{ReplicationService, ReplicationServiceServer},
};
use riffdb_proto::v1;
use riffdb_storage_api::{ChangelogFrameV3, ChangelogHistoryPointV3};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Channel, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

const MAX_RESPONSE: usize = 32 * 1024 * 1024 + 1024;
const TIMEOUT: Duration = Duration::from_secs(30);
type Jobs = Arc<Mutex<Vec<JoinHandle<()>>>>;
type ResumeCheck = (ChangelogHistoryPointV3, bool, oneshot::Sender<bool>);

enum Target {
    FrameAfter(u64),
    Attachment,
}

struct Trap {
    target: Target,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}
#[derive(Default)]
struct Control {
    trap: Option<Trap>,
    resume: Option<ResumeCheck>,
    attached_frame: Option<oneshot::Sender<()>>,
}
#[derive(Clone)]
struct Relay {
    upstream: ReplicationServiceClient<Channel>,
    control: Arc<Mutex<Control>>,
    jobs: Jobs,
}

#[tonic::async_trait]
impl ReplicationService for Relay {
    type StreamChangelogStream = ReceiverStream<Result<v1::StreamChangelogResponse, Status>>;
    async fn stream_changelog(
        &self,
        request: Request<v1::StreamChangelogRequest>,
    ) -> Result<Response<Self::StreamChangelogStream>, Status> {
        let attachment = request.get_ref().attachment.is_some();
        if let Some((expected, require_attachment, observed)) =
            self.control.lock().unwrap().resume.take()
        {
            let actual = request.get_ref().after.as_ref().or_else(|| {
                request
                    .get_ref()
                    .attachment
                    .as_ref()
                    .and_then(|attachment| attachment.acknowledged.as_ref())
            });
            let _ = observed.send(
                actual.is_some_and(|position| matches_position(position, expected))
                    && (!require_attachment || request.get_ref().attachment.is_some()),
            );
        }
        let attachment_trap = {
            let mut control = self.control.lock().unwrap();
            if request.get_ref().attachment.is_some()
                && matches!(
                    control.trap.as_ref().map(|trap| &trap.target),
                    Some(Target::Attachment)
                )
            {
                control.trap.take()
            } else {
                None
            }
        };
        if let Some(trap) = attachment_trap {
            let attachment = request.get_ref().attachment.as_ref().unwrap();
            let manifest =
                riffdb_storage_api::ReplicationBootstrapManifestV1::decode(&attachment.manifest)
                    .unwrap();
            assert!(
                attachment
                    .acknowledged
                    .as_ref()
                    .is_some_and(|position| matches_position(
                        position,
                        manifest.fence().history().tail()
                    ))
            );
            let _ = trap.entered.send(());
            // A dropped permit cancels the old request without letting a dead
            // follower claim its source hold after the parent has killed it.
            trap.release
                .await
                .map_err(|_| Status::cancelled("test attachment interrupted"))?;
        }
        let mut upstream = self
            .upstream
            .clone()
            .stream_changelog(request)
            .await?
            .into_inner();
        let (send, receive) = mpsc::channel(1);
        let control = self.control.clone();
        let mut jobs = self.jobs.lock().unwrap();
        if jobs.len() == 512 {
            return Err(Status::resource_exhausted("test relay stream bound"));
        }
        jobs.push(tokio::spawn(async move {
            loop {
                let item = tokio::select! {
                    _ = send.closed() => break,
                    item = upstream.message() => item,
                };
                let item = match item {
                    Ok(Some(item)) => item,
                    Ok(None) => break,
                    Err(status) => {
                        let _ = send.send(Err(status)).await;
                        break;
                    }
                };
                if attachment
                    && matches!(
                        &item.item,
                        Some(v1::stream_changelog_response::Item::Frame(_))
                    )
                    && let Some(observed) = control.lock().unwrap().attached_frame.take()
                {
                    let _ = observed.send(());
                }
                let trap = {
                    let mut state = control.lock().unwrap();
                    let matches = match (&state.trap, &item.item) {
                        (
                            Some(Trap {
                                target: Target::FrameAfter(after_commit),
                                ..
                            }),
                            Some(v1::stream_changelog_response::Item::Frame(bytes)),
                        ) => {
                            let frame = ChangelogFrameV3::decode(bytes).unwrap();
                            frame
                                .receipts()
                                .last()
                                .unwrap()
                                .binding()
                                .covered_frontier
                                .application()
                                .is_some_and(|sequence| sequence.get() > *after_commit)
                        }
                        _ => false,
                    };
                    if matches { state.trap.take() } else { None }
                };
                if let Some(trap) = trap {
                    let _ = trap.entered.send(());
                    tokio::select! {
                        _ = send.closed() => break,
                        _ = trap.release => {},
                    }
                }
                if send.send(Ok(item)).await.is_err() {
                    break;
                }
            }
        }));
        Ok(Response::new(ReceiverStream::new(receive)))
    }
}

pub(super) struct Proxy {
    endpoint: String,
    control: Arc<Mutex<Control>>,
    jobs: Jobs,
    stop: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}
impl Proxy {
    pub(super) async fn start(upstream: Channel) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("https://{}", listener.local_addr().unwrap());
        let control = Arc::new(Mutex::new(Control::default()));
        let jobs = Arc::new(Mutex::new(Vec::new()));
        let relay = Relay {
            upstream: ReplicationServiceClient::new(upstream)
                .max_encoding_message_size(1024)
                .max_decoding_message_size(MAX_RESPONSE),
            control: control.clone(),
            jobs: jobs.clone(),
        };
        let tls = ServerTlsConfig::new().identity(Identity::from_pem(
            include_bytes!("../../crates/riffdb-server/tests/fixtures/localhost-cert.pem"),
            include_bytes!("../../crates/riffdb-server/tests/fixtures/localhost-key.pem"),
        ));
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(
                    ReplicationServiceServer::new(relay)
                        .max_decoding_message_size(1024)
                        .max_encoding_message_size(MAX_RESPONSE),
                )
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
        });
        Self {
            endpoint,
            control,
            jobs,
            stop: Some(stop),
            server: Some(server),
        }
    }
    pub(super) fn endpoint(&self) -> &str {
        &self.endpoint
    }
    pub(super) fn hold_after_commit(
        &self,
        after_commit: u64,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        self.hold(Target::FrameAfter(after_commit))
    }
    pub(super) fn hold_attachment(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        self.hold(Target::Attachment)
    }
    /// A frame on the attachment response proves the real source accepted the
    /// handoff. Process readiness alone does not establish that source action.
    pub(super) fn observe_attached_frame(&self) -> oneshot::Receiver<()> {
        let (send, receive) = oneshot::channel();
        let mut control = self.control.lock().unwrap();
        assert!(control.attached_frame.is_none());
        control.attached_frame = Some(send);
        receive
    }
    fn hold(&self, target: Target) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let mut control = self.control.lock().unwrap();
        assert!(control.trap.is_none());
        control.trap = Some(Trap {
            target,
            entered,
            release: released,
        });
        (observed, release)
    }
    pub(super) fn expect_resume(
        &self,
        expected: ChangelogHistoryPointV3,
    ) -> oneshot::Receiver<bool> {
        self.expect(expected, false)
    }
    pub(super) fn expect_attachment(
        &self,
        expected: ChangelogHistoryPointV3,
    ) -> oneshot::Receiver<bool> {
        self.expect(expected, true)
    }
    fn expect(
        &self,
        expected: ChangelogHistoryPointV3,
        require_attachment: bool,
    ) -> oneshot::Receiver<bool> {
        let (send, receive) = oneshot::channel();
        let mut control = self.control.lock().unwrap();
        assert!(control.resume.is_none());
        control.resume = Some((expected, require_attachment, send));
        receive
    }
    pub(super) async fn shutdown(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(TIMEOUT, self.server.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let jobs = std::mem::take(&mut *self.jobs.lock().unwrap());
        for job in jobs {
            tokio::time::timeout(TIMEOUT, job).await.unwrap().unwrap();
        }
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        for job in self.jobs.lock().unwrap().drain(..) {
            job.abort();
        }
    }
}

fn matches_position(actual: &v1::ReplicationPosition, expected: ChangelogHistoryPointV3) -> bool {
    fn frontier(actual: Option<&v1::FrontierPosition>, expected: Option<u64>) -> bool {
        match (
            actual.and_then(|position| position.position.as_ref()),
            expected,
        ) {
            (Some(v1::frontier_position::Position::BeforeFirst(_)), None) => true,
            (Some(v1::frontier_position::Position::AppliedThrough(actual)), Some(expected)) => {
                *actual == expected
            }
            _ => false,
        }
    }
    actual.transaction_sequence == expected.sequence().get()
        && actual.history_hash == expected.history_hash()
        && frontier(
            actual.application_frontier.as_ref(),
            expected.frontier().application().map(|v| v.get()),
        )
        && frontier(
            actual.administration_frontier.as_ref(),
            expected.frontier().administration().map(|v| v.get()),
        )
}

pub(super) async fn assert_foreign_tail_refused(
    channel: Channel,
    token: &str,
    lineage: riffdb_storage_api::ChangelogLineageV3,
    after: ChangelogHistoryPointV3,
) {
    fn frontier(value: Option<u64>) -> Option<v1::FrontierPosition> {
        Some(v1::FrontierPosition {
            position: Some(match value {
                Some(value) => v1::frontier_position::Position::AppliedThrough(value),
                None => v1::frontier_position::Position::BeforeFirst(v1::Unit {}),
            }),
        })
    }
    let mut request = Request::new(v1::StreamChangelogRequest {
        request_id: super::support::request_id(99),
        database_id: lineage.database_id().as_bytes().to_vec(),
        history_incarnation: lineage.history_incarnation() + 1,
        leadership_epoch: lineage.leadership_epoch().get(),
        readable_format: ChangelogFrameV3::IDENTITY.into(),
        catalog_digest: lineage.catalog_digest().to_vec(),
        after: Some(v1::ReplicationPosition {
            transaction_sequence: after.sequence().get(),
            history_hash: after.history_hash().to_vec(),
            application_frontier: frontier(after.frontier().application().map(|value| value.get())),
            administration_frontier: frontier(
                after.frontier().administration().map(|value| value.get()),
            ),
        }),
        maximum_frame_bytes: riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES as u64,
        maximum_transitions: riffdb_storage_api::MAX_STAGED_COMMANDS as u64,
        ..Default::default()
    });
    let mut credential: tonic::metadata::MetadataValue<_> =
        format!("Bearer {token}").parse().unwrap();
    credential.set_sensitive(true);
    request.metadata_mut().insert("authorization", credential);
    tokio::time::timeout(TIMEOUT, async {
        let mut response = ReplicationServiceClient::new(channel).stream_changelog(request).await.unwrap().into_inner();
        assert!(matches!(response.message().await.unwrap().unwrap().item,
            Some(v1::stream_changelog_response::Item::Refusal(value)) if value == v1::ReplicationRefusal::ForeignLineage as i32));
        assert!(response.message().await.unwrap().is_none());
    }).await.unwrap();
}
