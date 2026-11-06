//! Transport-only administrative replication framing and confidentiality gate.

use super::*;
use crate::generated::replication_service_server::{ReplicationService, ReplicationServiceServer};
use riffdb_service::{
    ReplicationFailure, ReplicationItem, ReplicationPhase, ReplicationRequest,
    ReplicationStreamErrorV3,
};
use riffdb_types::{
    AdministrationSequence, CommitSequence, DatabaseId, DualFrontier,
    ReplicationEpochVoteRequestV1, ReplicationEpochVoteV1,
};

/// Server-owned voter epoch-promise evaluation and durable recording (`ADR-0337` Decision 2).
pub trait ReplicationEpochVoteHandler: Send + Sync {
    /// Evaluates a candidate's epoch-vote request against the voter's durable
    /// promised epoch and applied frontier after authenticating `principal`.
    fn request_epoch_vote(
        &self,
        principal: AuthenticatedPrincipal,
        request: ReplicationEpochVoteRequestV1,
    ) -> Pin<Box<dyn Future<Output = Result<ReplicationEpochVoteV1, PublicError>> + Send + '_>>;
}

/// Tonic unary service adapter for `/riffdb.v1.ReplicationVoteService/RequestReplicationEpochVote`.
#[derive(Clone)]
pub struct ReplicationVoteServiceServer {
    application: GrpcApplication,
}

impl std::fmt::Debug for ReplicationVoteServiceServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationVoteServiceServer([redacted])")
    }
}

/// Bounded pull stream: one complete frame or one terminal typed refusal.
pub type ReplicationResponseStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<v1::StreamChangelogResponse, Status>> + Send>>;

impl GrpcApplication {
    /// Builds the one administrative stream with its accepted V3 byte ceiling.
    #[must_use]
    pub fn replication_server(&self) -> ReplicationServiceServer<Self> {
        ReplicationServiceServer::new(self.clone())
            .max_decoding_message_size(1024)
            .max_encoding_message_size(32 * 1024 * 1024 + 1024)
    }

    /// Builds the confidential voter epoch-vote service (`ADR-0337` Decision 2).
    #[must_use]
    pub fn replication_vote_server(&self) -> ReplicationVoteServiceServer {
        ReplicationVoteServiceServer {
            application: self.clone(),
        }
    }
}

#[derive(Default)]
struct RawEpochVoteCodec;

struct RawEpochVoteEncoder;

impl tonic::codec::Encoder for RawEpochVoteEncoder {
    type Item = ReplicationEpochVoteV1;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        use tonic_prost::prost::bytes::BufMut as _;
        let encoded = item.encode_v1();
        if dst.remaining_mut() < encoded.len() {
            return Err(Status::internal(crate::EMERGENCY_INTERNAL_MESSAGE));
        }
        dst.put_slice(&encoded);
        Ok(())
    }
}

struct RawEpochVoteDecoder;

impl tonic::codec::Decoder for RawEpochVoteDecoder {
    type Item = ReplicationEpochVoteRequestV1;
    type Error = Status;

    fn decode(
        &mut self,
        src: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        use tonic_prost::prost::bytes::Buf as _;
        let remaining = src.remaining();
        if remaining == 0 {
            return Ok(None);
        }
        if remaining > 512 {
            src.advance(remaining);
            return Err(Status::invalid_argument("invalid epoch vote request"));
        }
        let mut bytes = vec![0_u8; remaining];
        src.copy_to_slice(&mut bytes);
        let decoded = ReplicationEpochVoteRequestV1::decode_v1(&bytes)
            .map_err(|_| Status::invalid_argument("invalid epoch vote request"))?;
        Ok(Some(decoded))
    }
}

impl tonic::codec::Codec for RawEpochVoteCodec {
    type Encode = ReplicationEpochVoteV1;
    type Decode = ReplicationEpochVoteRequestV1;
    type Encoder = RawEpochVoteEncoder;
    type Decoder = RawEpochVoteDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEpochVoteEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawEpochVoteDecoder
    }
}

impl tonic::server::NamedService for ReplicationVoteServiceServer {
    const NAME: &'static str = "riffdb.v1.ReplicationVoteService";
}

impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for ReplicationVoteServiceServer
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: tonic::codegen::http::Request<B>) -> Self::Future {
        match req.uri().path() {
            "/riffdb.v1.ReplicationVoteService/RequestReplicationEpochVote" => {
                struct VoteSvc(GrpcApplication);
                impl tonic::server::UnaryService<ReplicationEpochVoteRequestV1> for VoteSvc {
                    type Response = ReplicationEpochVoteV1;
                    type Future =
                        tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

                    fn call(
                        &mut self,
                        request: tonic::Request<ReplicationEpochVoteRequestV1>,
                    ) -> Self::Future {
                        let app = self.0.clone();
                        Box::pin(async move { app.handle_epoch_vote(request).await })
                    }
                }
                let app = self.application.clone();
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(RawEpochVoteCodec)
                        .apply_max_message_size_config(Some(512), Some(512));
                    Ok(grpc.unary(VoteSvc(app), req).await)
                })
            }
            _ => Box::pin(async move {
                let mut response =
                    tonic::codegen::http::Response::new(tonic::body::Body::default());
                let headers = response.headers_mut();
                headers.insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                headers.insert(
                    tonic::codegen::http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            }),
        }
    }
}

impl GrpcApplication {
    async fn handle_epoch_vote(
        &self,
        request: Request<ReplicationEpochVoteRequestV1>,
    ) -> Result<Response<ReplicationEpochVoteV1>, Status> {
        if !self.replication_confidential {
            return Err(Status::permission_denied(
                "replication requires a confidential connection",
            ));
        }
        let (metadata, _, message) = split_request(request);
        let lifecycle = self.select_lifecycle(&metadata)?;
        let handler = lifecycle
            .admit_replication_epoch_vote()
            .ok_or_else(service_not_ready)?;
        let security = lifecycle.security_context().ok_or_else(service_not_ready)?;
        let (context, _cancellation) =
            self.normal_context(&metadata, message.request_id(), &security)?;
        let deadline = tokio::time::Instant::from_std(context.control().deadline());
        let vote = tokio::time::timeout_at(
            deadline,
            handler.request_epoch_vote(context.principal().clone(), message),
        )
        .await
        .map_err(|_| status_from_public_error(&PublicError::storage_unavailable()))?
        .map_err(|err| status_from_public_error(&err))?;
        Ok(Response::new(vote))
    }
}

#[tonic::async_trait]
impl ReplicationService for GrpcApplication {
    type StreamChangelogStream = ReplicationResponseStream;

    async fn stream_changelog(
        &self,
        request: Request<v1::StreamChangelogRequest>,
    ) -> Result<Response<Self::StreamChangelogStream>, Status> {
        if !self.replication_confidential {
            return Err(Status::permission_denied(
                "replication requires a confidential connection",
            ));
        }
        let (metadata, _, message) = split_request(request);
        riffdb_proto::validate_public_message(&message)
            .map_err(|_| Status::invalid_argument("invalid replication request"))?;
        let lifecycle = self.select_lifecycle(&metadata)?;
        let service = lifecycle
            .admit_replication()
            .ok_or_else(service_not_ready)?;
        let security = lifecycle.security_context().ok_or_else(service_not_ready)?;
        let request_id = riffdb_types::RequestId::from_bytes(
            message
                .request_id
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("invalid replication request"))?,
        )
        .map_err(|_| Status::invalid_argument("invalid replication request"))?;
        let (context, cancellation) = self.normal_context(&metadata, request_id, &security)?;
        let deadline = tokio::time::Instant::from_std(context.control().deadline());
        let handshake = match handshake(message) {
            Ok(handshake) => handshake,
            Err(error) => return Ok(Response::new(terminal(error))),
        };
        let subscription =
            match tokio::time::timeout_at(deadline, service.stream_changelog(context, handshake))
                .await
            {
                Ok(Ok(subscription)) => subscription,
                Ok(Err(error)) => return Ok(Response::new(terminal(error))),
                Err(_) => return Ok(Response::new(terminal(ReplicationFailure::Unavailable))),
            };
        let stream = futures_util::stream::unfold(
            Some((subscription, lifecycle, service, cancellation)),
            move |state| async move {
                let (mut subscription, lifecycle, service, cancellation) = state?;
                let admitted = || {
                    lifecycle
                        .admit_replication()
                        .is_some_and(|current| Arc::ptr_eq(&current, &service))
                };
                if !admitted() {
                    return Some((Ok(refusal(ReplicationFailure::Unavailable)), None));
                }
                let next = tokio::time::timeout_at(deadline, subscription.next_item()).await;
                if !admitted() {
                    return Some((Ok(refusal(ReplicationFailure::Unavailable)), None));
                }
                match next {
                    Ok(Ok(Some(frame))) => match encode_item(frame) {
                        Ok(item) => Some((
                            Ok(item),
                            Some((subscription, lifecycle, service, cancellation)),
                        )),
                        Err(error) => Some((Ok(refusal(error)), None)),
                    },
                    Ok(Ok(None)) => None,
                    Ok(Err(error)) => Some((Ok(refusal(error)), None)),
                    Err(_) => Some((Ok(refusal(ReplicationFailure::Unavailable)), None)),
                }
            },
        );
        Ok(Response::new(Box::pin(stream)))
    }
}

fn handshake(
    message: v1::StreamChangelogRequest,
) -> Result<ReplicationRequest, ReplicationFailure> {
    let invalid = || ReplicationFailure::Source(ReplicationStreamErrorV3::InvalidPosition);
    let database_id = DatabaseId::from_bytes(
        message
            .database_id
            .as_slice()
            .try_into()
            .map_err(|_| invalid())?,
    )
    .map_err(|_| invalid())?;
    let (phase, position) = match (
        message.after,
        message.bootstrap,
        message.attachment,
        message.fence_evidence,
    ) {
        (Some(position), None, None, None) => {
            let phase = if message.follower_hold_id.is_empty() {
                ReplicationPhase::Tail
            } else {
                ReplicationPhase::Follower {
                    hold_id: message
                        .follower_hold_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| invalid())?,
                }
            };
            (phase, Some(position))
        }
        (None, Some(bootstrap), None, None) => (
            ReplicationPhase::Bootstrap {
                hold_id: bootstrap
                    .hold_id
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid())?,
                resume_manifest: bootstrap.resume_manifest,
                after_page: bootstrap.after_page,
            },
            None,
        ),
        (None, None, Some(attachment), None) => (
            ReplicationPhase::Attach {
                manifest: attachment.manifest,
            },
            Some(attachment.acknowledged.ok_or_else(invalid)?),
        ),
        (None, None, None, Some(fence)) => {
            let selected = crate::replication_fence::selection(
                &message.request_id,
                database_id,
                message.history_incarnation,
                message.leadership_epoch,
                &fence,
            )?;
            (
                ReplicationPhase::FenceEvidence { request: selected },
                Some(fence.applied.ok_or_else(invalid)?),
            )
        }
        _ => return Err(invalid()),
    };
    let (after_sequence, after_hash, after_frontier) = position
        .map(position_fields)
        .transpose()?
        .unwrap_or((0, [0; 32], DualFrontier::INITIAL));
    Ok(ReplicationRequest {
        database_id,
        history_incarnation: message.history_incarnation,
        leadership_epoch: message.leadership_epoch,
        phase,
        after_sequence,
        after_hash,
        after_frontier,
        readable_format: message.readable_format,
        catalog_digest: message
            .catalog_digest
            .as_slice()
            .try_into()
            .map_err(|_| invalid())?,
        maximum_frame_bytes: message.maximum_frame_bytes,
        maximum_transitions: message.maximum_transitions,
    })
}

fn position_fields(
    position: v1::ReplicationPosition,
) -> Result<(u64, [u8; 32], DualFrontier), ReplicationFailure> {
    let invalid = || ReplicationFailure::Source(ReplicationStreamErrorV3::InvalidPosition);
    Ok((
        position.transaction_sequence,
        position
            .history_hash
            .as_slice()
            .try_into()
            .map_err(|_| invalid())?,
        DualFrontier::new(
            frontier(position.application_frontier)
                .map_err(|_| invalid())?
                .map(|sequence| CommitSequence::new(sequence).ok_or_else(invalid))
                .transpose()?,
            frontier(position.administration_frontier)
                .map_err(|_| invalid())?
                .map(|sequence| AdministrationSequence::new(sequence).ok_or_else(invalid))
                .transpose()?,
        ),
    ))
}

fn frontier(frontier: Option<v1::FrontierPosition>) -> Result<Option<u64>, ()> {
    match frontier.and_then(|frontier| frontier.position) {
        Some(v1::frontier_position::Position::BeforeFirst(_)) => Ok(None),
        Some(v1::frontier_position::Position::AppliedThrough(sequence)) if sequence > 0 => {
            Ok(Some(sequence))
        }
        _ => Err(()),
    }
}

fn terminal(error: ReplicationFailure) -> ReplicationResponseStream {
    Box::pin(futures_util::stream::once(
        async move { Ok(refusal(error)) },
    ))
}

fn encode_item(item: ReplicationItem) -> Result<v1::StreamChangelogResponse, ReplicationFailure> {
    use v1::stream_changelog_response::Item;
    let (item, source_head) = match item {
        ReplicationItem::FenceEvidence(evidence) => (
            Item::FenceEvidence(crate::replication_fence::encode(&evidence)?),
            None,
        ),
        ReplicationItem::Frame(frame) => {
            let (bytes, head) = frame.into_parts();
            (
                Item::Frame(bytes),
                head.map(crate::replication_progress::encode),
            )
        }
        ReplicationItem::BootstrapManifest(bytes) => (Item::BootstrapManifest(bytes), None),
        ReplicationItem::BootstrapPage(bytes) => (Item::BootstrapPage(bytes), None),
    };
    Ok(v1::StreamChangelogResponse {
        item: Some(item),
        source_head,
    })
}

fn refusal(error: ReplicationFailure) -> v1::StreamChangelogResponse {
    use ReplicationStreamErrorV3 as S;
    use v1::ReplicationRefusal as R;
    let code = match error {
        ReplicationFailure::AuthorizationDenied => R::AuthorizationDenied,
        ReplicationFailure::Unavailable => R::Unavailable,
        ReplicationFailure::Source(source) => match source {
            S::ForeignLineage => R::ForeignLineage,
            S::StaleEpoch => R::StaleEpoch,
            S::HistoryPruned => R::HistoryPruned,
            S::UnsupportedFormat => R::UnsupportedFormat,
            S::UnsupportedCatalog => R::UnsupportedCatalog,
            S::UnsupportedBounds => R::UnsupportedBounds,
            S::InvalidPosition => R::InvalidPosition,
            S::CorruptHistory => R::CorruptHistory,
            S::Unavailable => R::Unavailable,
        },
    };
    v1::StreamChangelogResponse {
        item: Some(v1::stream_changelog_response::Item::Refusal(code.into())),
        source_head: None,
    }
}
