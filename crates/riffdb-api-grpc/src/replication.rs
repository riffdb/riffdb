//! Transport-only administrative replication framing and confidentiality gate.

use super::*;
use crate::generated::replication_service_server::{ReplicationService, ReplicationServiceServer};
use riffdb_service::{
    ReplicationFailure, ReplicationItem, ReplicationPhase, ReplicationRequest,
    ReplicationStreamErrorV3,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

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
        let principal = authenticate_normal_request(
            &metadata,
            security.authenticator.as_ref(),
            &security.authentication,
        )?;
        let deadline = tokio::time::Instant::from_std(self.limits.deadline(&metadata)?);
        let handshake = match handshake(message) {
            Ok(handshake) => handshake,
            Err(error) => return Ok(Response::new(terminal(error))),
        };
        let subscription =
            match tokio::time::timeout_at(deadline, service.stream_changelog(principal, handshake))
                .await
            {
                Ok(Ok(subscription)) => subscription,
                Ok(Err(error)) => return Ok(Response::new(terminal(error))),
                Err(_) => return Ok(Response::new(terminal(ReplicationFailure::Unavailable))),
            };
        let stream = futures_util::stream::unfold(
            Some((subscription, lifecycle, service)),
            move |state| async move {
                let (mut subscription, lifecycle, service) = state?;
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
                    Ok(Ok(Some(frame))) => Some((
                        Ok(v1::StreamChangelogResponse {
                            item: Some(match frame {
                                ReplicationItem::Frame(bytes) => {
                                    v1::stream_changelog_response::Item::Frame(bytes)
                                }
                                ReplicationItem::BootstrapManifest(bytes) => {
                                    v1::stream_changelog_response::Item::BootstrapManifest(bytes)
                                }
                                ReplicationItem::BootstrapPage(bytes) => {
                                    v1::stream_changelog_response::Item::BootstrapPage(bytes)
                                }
                            }),
                        }),
                        Some((subscription, lifecycle, service)),
                    )),
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
    let (phase, position) = match (message.after, message.bootstrap, message.attachment) {
        (Some(position), None, None) => {
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
        (None, Some(bootstrap), None) => (
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
        (None, None, Some(attachment)) => (
            ReplicationPhase::Attach {
                manifest: attachment.manifest,
            },
            Some(attachment.acknowledged.ok_or_else(invalid)?),
        ),
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
    }
}
