//! Bounded outbound wire conversion; channel trust belongs to server composition.
use riffdb_proto::{generated::replication_service_client::ReplicationServiceClient, v1};
use riffdb_service::{
    ReplicationFailure as Failure, ReplicationFuture, ReplicationItem as Item,
    ReplicationItemSource, ReplicationPhase, ReplicationRequest,
};
use riffdb_types::{DatabaseAlias, DualFrontier, RequestId};
use std::time::Duration;
use tokio::time::Instant;
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport::Channel,
};
const LIFETIME: Duration = Duration::from_secs(15 * 60);
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024 + 1024;

/// Wire-only replication client. Construction does not certify transport trust
/// or grant data authority; server composition owns those decisions.
pub struct ReplicationWireClient {
    client: ReplicationServiceClient<Channel>,
}
impl ReplicationWireClient {
    /// Wraps a composition-owned channel with the exact public codec limits.
    #[must_use]
    pub fn new(channel: Channel) -> Self {
        Self {
            client: ReplicationServiceClient::new(channel)
                .max_encoding_message_size(1024)
                .max_decoding_message_size(MAX_RESPONSE_BYTES),
        }
    }
    /// Lowers one API-neutral request and maps only bounded, typed responses.
    /// The caller must establish confidential transport before handing off the
    /// already checked credential metadata. Raw remote status text is discarded.
    pub async fn open(
        &self,
        input: ReplicationRequest,
        request_id: RequestId,
        mut authorization: MetadataValue<Ascii>,
        database: &DatabaseAlias,
    ) -> Result<Box<dyn ReplicationItemSource>, Failure> {
        let deadline = Instant::now() + LIFETIME;
        let follower_hold_id = match &input.phase {
            ReplicationPhase::Follower { hold_id } => hold_id.to_vec(),
            _ => Vec::new(),
        };
        let position =
            || wire_position(input.after_sequence, input.after_hash, input.after_frontier);
        let (after, bootstrap, attachment) = match input.phase {
            ReplicationPhase::Tail | ReplicationPhase::Follower { .. } => {
                (Some(position()), None, None)
            }
            ReplicationPhase::Bootstrap {
                hold_id,
                resume_manifest,
                after_page,
            } => {
                if input.after_sequence != 0
                    || input.after_hash != [0; 32]
                    || input.after_frontier != DualFrontier::INITIAL
                {
                    return Err(Failure::Source(
                        riffdb_errors::ReplicationStreamErrorV3::InvalidPosition,
                    ));
                }
                (
                    None,
                    Some(v1::ReplicationBootstrapRequest {
                        hold_id: hold_id.to_vec(),
                        resume_manifest,
                        after_page,
                    }),
                    None,
                )
            }
            ReplicationPhase::Attach { manifest } => (
                None,
                None,
                Some(v1::ReplicationBootstrapAttachment {
                    manifest,
                    acknowledged: Some(position()),
                }),
            ),
        };
        let mut request = tonic::Request::new(v1::StreamChangelogRequest {
            request_id: request_id.into_bytes().to_vec(),
            database_id: input.database_id.into_bytes().to_vec(),
            history_incarnation: input.history_incarnation,
            leadership_epoch: input.leadership_epoch,
            readable_format: input.readable_format,
            catalog_digest: input.catalog_digest.to_vec(),
            maximum_frame_bytes: input.maximum_frame_bytes,
            maximum_transitions: input.maximum_transitions,
            after,
            bootstrap,
            attachment,
            follower_hold_id,
        });

        authorization.set_sensitive(true);
        request
            .metadata_mut()
            .insert("authorization", authorization);
        request.metadata_mut().insert(
            riffdb_proto::DATABASE_METADATA_KEY,
            database.as_str().parse().map_err(|_| unavailable())?,
        );
        request.set_timeout(LIFETIME);
        let response =
            tokio::time::timeout_at(deadline, self.client.clone().stream_changelog(request))
                .await
                .map_err(|_| unavailable())?
                .map_err(transport_failure)?;
        Ok(Box::new(WireItems {
            stream: Some(response.into_inner()),
            deadline,
        }))
    }
}
struct WireItems {
    stream: Option<tonic::Streaming<v1::StreamChangelogResponse>>,
    deadline: Instant,
}
impl ReplicationItemSource for WireItems {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<Item>> {
        Box::pin(async move {
            let Some(mut stream) = self.stream.take() else {
                return Ok(None);
            };
            if Instant::now() >= self.deadline {
                return Err(unavailable());
            }
            let message = tokio::time::timeout_at(self.deadline, stream.message())
                .await
                .map_err(|_| unavailable())?
                .map_err(transport_failure)?;
            if Instant::now() >= self.deadline {
                return Err(unavailable());
            }
            let Some(message) = message else {
                return Ok(None);
            };
            use v1::stream_changelog_response::Item as Wire;
            if message.source_head.is_some() && !matches!(message.item, Some(Wire::Frame(_))) {
                return Err(Failure::Source(
                    riffdb_errors::ReplicationStreamErrorV3::CorruptHistory,
                ));
            }
            let head = message
                .source_head
                .map(crate::replication_progress::decode)
                .transpose()?;
            let item = match message.item {
                Some(Wire::Frame(bytes)) => {
                    Item::Frame(riffdb_service::ReplicationFrame::new(bytes, head))
                }
                Some(Wire::BootstrapManifest(bytes)) => Item::BootstrapManifest(bytes),
                Some(Wire::BootstrapPage(bytes)) => Item::BootstrapPage(bytes),
                Some(Wire::Refusal(code)) => return Err(refusal(code)),
                None => {
                    return Err(Failure::Source(
                        riffdb_errors::ReplicationStreamErrorV3::CorruptHistory,
                    ));
                }
            };
            self.stream = Some(stream);
            Ok(Some(item))
        })
    }
}
fn wire_position(sequence: u64, hash: [u8; 32], frontier: DualFrontier) -> v1::ReplicationPosition {
    fn position(value: Option<u64>) -> Option<v1::FrontierPosition> {
        Some(v1::FrontierPosition {
            position: Some(match value {
                None => v1::frontier_position::Position::BeforeFirst(v1::Unit {}),
                Some(value) => v1::frontier_position::Position::AppliedThrough(value),
            }),
        })
    }
    v1::ReplicationPosition {
        transaction_sequence: sequence,
        history_hash: hash.to_vec(),
        application_frontier: position(frontier.application().map(|v| v.get())),
        administration_frontier: position(frontier.administration().map(|v| v.get())),
    }
}
fn refusal(code: i32) -> Failure {
    use riffdb_errors::ReplicationStreamErrorV3 as E;
    use v1::ReplicationRefusal as R;
    let error = match R::try_from(code) {
        Ok(R::AuthorizationDenied) => return Failure::AuthorizationDenied,
        Ok(R::ForeignLineage) => E::ForeignLineage,
        Ok(R::StaleEpoch) => E::StaleEpoch,
        Ok(R::HistoryPruned) => E::HistoryPruned,
        Ok(R::UnsupportedFormat) => E::UnsupportedFormat,
        Ok(R::UnsupportedCatalog) => E::UnsupportedCatalog,
        Ok(R::UnsupportedBounds) => E::UnsupportedBounds,
        Ok(R::InvalidPosition) => E::InvalidPosition,
        Ok(R::Unavailable) => E::Unavailable,
        _ => E::CorruptHistory,
    };
    Failure::Source(error)
}
fn unavailable() -> Failure {
    Failure::Unavailable
}

fn transport_failure(status: tonic::Status) -> Failure {
    match status.code() {
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
            Failure::AuthorizationDenied
        }
        _ => unavailable(),
    }
}
