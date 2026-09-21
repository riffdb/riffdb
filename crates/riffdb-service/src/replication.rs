//! API-neutral administrative replication. Storage sources cannot authorize
//! releases, and transport adapters cannot bypass the fresh current-policy check.

use crate::{CurrentPolicyPort, RequestContext, RiffDbService, RiffDbServiceInner};
#[path = "replication_audit.rs"]
mod audit;
pub(crate) use audit::from_service_failure;
use riffdb_auth::AuthenticatedPrincipal;
use riffdb_policy::ReplicationDecision;
use std::{future::Future, pin::Pin, sync::Arc};

pub use riffdb_errors::ReplicationStreamErrorV3;
use riffdb_types::{DatabaseId, DualFrontier};
#[path = "replication_progress.rs"]
mod progress;
#[path = "replication_request.rs"]
mod request;
pub use progress::{ReplicationFrame, ReplicationSourceHead};
#[path = "replication_statistics.rs"]
mod statistics;
pub use riffdb_auth::{
    ChangelogHistoryPointV3, PrimaryFenceRequestV1, PrimaryFenceSourceEvidenceV1,
};
pub use statistics::{ReplicationRole, ReplicationStatistics};

/// API-neutral negotiation input. These caller-supplied values are not a
/// validated storage handshake, source pin, or authorization proof.
#[derive(Clone)]
pub struct ReplicationRequest {
    /// Requested database, checked against current policy before source access.
    pub database_id: DatabaseId,
    /// Selected phase; all bytes remain untrusted until the storage adapter checks them.
    pub phase: ReplicationPhase,
    /// Requested nonzero history incarnation.
    pub history_incarnation: u64,
    /// Requested nonzero leadership epoch.
    pub leadership_epoch: u64,
    /// Exact acknowledged nonzero receipt sequence.
    pub after_sequence: u64,
    /// Exact receipt history hash, never diagnostic text.
    pub after_hash: [u8; 32],
    /// Both frontiers at the requested receipt position.
    pub after_frontier: DualFrontier,
    /// Peer-readable format, bounded to 128 bytes before source admission.
    pub readable_format: String,
    /// Peer catalog identity; the storage adapter owns exact negotiation.
    pub catalog_digest: [u8; 32],
    /// Peer maximum frame bytes; never overrides the production ceiling.
    pub maximum_frame_bytes: u64,
    /// Peer maximum transitions; never overrides the production ceiling.
    pub maximum_transitions: u64,
}

/// API-neutral phase selection. Manifest bytes use the accepted external V1
/// encoding; this DTO is not a second semantic manifest or a durability proof.
#[derive(Clone)]
pub enum ReplicationPhase {
    /// Read one exact retained source-fence observation, without changing custody.
    FenceEvidence {
        /// Selected fence and follower generation. The outer position is applied.
        request: PrimaryFenceRequestV1,
    },
    /// Read successors of the exact acknowledged position.
    Tail,
    /// Resume after a durable acknowledgement for an already registered follower.
    Follower {
        /// Existing follower hold ID; this cannot create or release a hold.
        hold_id: [u8; 16],
    },
    /// New held snapshot or exact immutable artifact resume.
    Bootstrap {
        /// Nonzero source-local hold ID, never a capability or filesystem path.
        hold_id: [u8; 16],
        /// Empty for a new artifact; otherwise the exact prior source manifest.
        resume_manifest: Vec<u8>,
        /// Last page the receiver durably staged; zero for a new artifact.
        after_page: u32,
    },
    /// Local receiver publication and acknowledgement completed at this manifest.
    Attach {
        /// Exact prior source manifest. Position fields carry the durable ack.
        manifest: Vec<u8>,
    },
}

/// One bounded item in the existing stream. Payloads are deliberately redacted
/// from Debug and must not reach diagnostics or ordinary application surfaces.
#[derive(Clone, Eq, PartialEq)]
pub enum ReplicationItem {
    /// One checked observation. This is not authenticated promotion proof.
    FenceEvidence(Box<PrimaryFenceSourceEvidenceV1>),
    /// Complete checksummed V3 tail frame.
    Frame(ReplicationFrame),
    /// Exact checksummed V1 bootstrap manifest.
    BootstrapManifest(Vec<u8>),
    /// Complete bounded bootstrap page.
    BootstrapPage(Vec<u8>),
}
impl std::fmt::Debug for ReplicationItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationItem([redacted])")
    }
}
impl ReplicationItem {
    fn bounded(&self) -> bool {
        if let Self::FenceEvidence(evidence) = self {
            // The existing fence envelope is bounded; four fixed-width positions
            // plus its wire framing fit the separately enforced 2048-byte item.
            return evidence
                .encode_fence_record()
                .is_ok_and(|bytes| bytes.len() <= 1536);
        }
        let (bytes, limit) = match self {
            Self::FenceEvidence(_) => return false,
            Self::Frame(bytes) => (bytes.as_ref(), 32 * 1024 * 1024),
            Self::BootstrapManifest(bytes) => (bytes.as_slice(), 512),
            Self::BootstrapPage(bytes) => (bytes.as_slice(), 32 * 1024 * 1024 + 512),
        };
        !bytes.is_empty() && bytes.len() <= limit
    }
}

impl std::fmt::Debug for ReplicationRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationRequest([redacted])")
    }
}

/// Safe terminal classification, with no credential, key, value, or digest text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationFailure {
    /// Current capability does not authorize this release.
    AuthorizationDenied,
    /// Current policy or the bounded source is unavailable.
    Unavailable,
    /// Exact source negotiation or history refusal.
    Source(ReplicationStreamErrorV3),
}

/// Bounded asynchronous administrative operation.
pub type ReplicationFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ReplicationFailure>> + Send + 'a>>;

/// Least-authority factory supplied by production composition, below the
/// application boundary. A source owns published snapshot and bounded source-retention
/// control capabilities; only this service may authorize release to a peer.
pub trait ReplicationSourcePort: Send + Sync {
    /// Checks an exact candidate against a retained source fence. Evidence is
    /// not authenticated peer proof; unsupported sources fail closed.
    fn primary_fence_source_evidence(
        &self,
        _request: PrimaryFenceRequestV1,
        _applied: ChangelogHistoryPointV3,
    ) -> ReplicationFuture<'_, PrimaryFenceSourceEvidenceV1> {
        Box::pin(async { Err(ReplicationFailure::Unavailable) })
    }

    /// Opens the selected phase under bounded source admission. Attachments and
    /// follower acknowledgements may update only source retention control; they
    /// never authorize application writes.
    fn open(
        &self,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>>;
}

/// Internal item custody. Implementations bound waits and retain at most one
/// frame or bootstrap page under backpressure. Drop cancels waits; in-flight
/// blocking work retains its capacity until completion.
pub trait ReplicationItemSource: Send {
    /// Returns one whole item, or the end of this bounded connection lifetime.
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>>;
}

/// Administrative application surface shared by transport adapters.
pub trait ReplicationApplication: Send + Sync {
    /// Releases source evidence under current administrative replication
    /// authority. The adapter must require the existing confidential peer path;
    /// this observation alone cannot authorize promotion.
    fn primary_fence_source_evidence(
        &self,
        _context: RequestContext,
        _request: PrimaryFenceRequestV1,
        _applied: ChangelogHistoryPointV3,
    ) -> ReplicationFuture<'_, PrimaryFenceSourceEvidenceV1> {
        Box::pin(async { Err(ReplicationFailure::Unavailable) })
    }

    /// Opens an authenticated stream. The transport must establish a confidential
    /// connection before calling; all data authority is checked in this service.
    fn stream_changelog(
        &self,
        context: RequestContext,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, ReplicationSubscription>;
}

/// Service-owned composition of current policy and the published source.
pub struct ReplicationService {
    owner: RiffDbService,
    source: Arc<dyn ReplicationSourcePort>,
}

impl ReplicationService {
    /// Retains the ordinary service owner, including its sole audit coordinator.
    #[must_use]
    pub fn new(owner: RiffDbService, source: Arc<dyn ReplicationSourcePort>) -> Self {
        Self { owner, source }
    }
}

impl ReplicationApplication for ReplicationService {
    fn primary_fence_source_evidence(
        &self,
        context: RequestContext,
        request: PrimaryFenceRequestV1,
        applied: ChangelogHistoryPointV3,
    ) -> ReplicationFuture<'_, PrimaryFenceSourceEvidenceV1> {
        let service = self.owner.inner.clone();
        let source = self.source.clone();
        audit::submit(&self.owner, context.ingress(), async move {
            audit::observe_fence(service, source, context, request, applied).await
        })
    }

    fn stream_changelog(
        &self,
        context: RequestContext,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, ReplicationSubscription> {
        let service = self.owner.inner.clone();
        let source = self.source.clone();
        audit::submit(&self.owner, context.ingress(), async move {
            audit::establish(service, source, context, request).await
        })
    }
}

/// Move-only subscriber. No item is released without rechecking current
/// capability state after framing and any wait. Failure permanently closes it.
pub struct ReplicationSubscription {
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    database_id: DatabaseId,
    source: Option<ItemCustody>,
    emission: Emission,
    finished: bool,
}

struct ItemCustody(Option<Box<dyn ReplicationItemSource>>);
impl ItemCustody {
    fn new(source: Box<dyn ReplicationItemSource>) -> Self {
        Self(Some(source))
    }
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        match self.0.as_mut() {
            Some(source) => source.next_item(),
            None => Box::pin(async { Err(ReplicationFailure::Unavailable) }),
        }
    }
}
impl Drop for ItemCustody {
    fn drop(&mut self) {
        let source = self.0.take();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(source)));
    }
}

enum Emission {
    FenceEvidence(PrimaryFenceRequestV1, ChangelogHistoryPointV3),
    FenceEnd,
    Tail,
    Manifest,
    Pages(u32),
}
impl Emission {
    fn observe(&mut self, item: Option<&ReplicationItem>) -> bool {
        match (&self, item) {
            (
                Self::FenceEvidence(request, applied),
                Some(ReplicationItem::FenceEvidence(evidence)),
            ) if evidence.fence().target() == request.target()
                && evidence.fence().operation_id() == request.operation_id()
                && evidence.fence().generation() == request.generation()
                && evidence.applied() == *applied =>
            {
                *self = Self::FenceEnd;
                true
            }
            (Self::FenceEnd, None) => true,
            (Self::Tail, None | Some(ReplicationItem::Frame(_))) => true,
            (Self::Manifest, Some(ReplicationItem::BootstrapManifest(_))) => {
                *self = Self::Pages(0);
                true
            }
            (Self::Pages(_), None) => true,
            (Self::Pages(count), Some(ReplicationItem::BootstrapPage(_))) if *count < 1_048_576 => {
                *self = Self::Pages(*count + 1);
                true
            }
            _ => false,
        }
    }
}

impl ReplicationSubscription {
    /// Pulls one item under fresh authority; emission never acknowledges remote
    /// durability or releases a source retention hold.
    pub async fn next_item(&mut self) -> Result<Option<ReplicationItem>, ReplicationFailure> {
        if self.finished {
            return Ok(None);
        }
        // Local custody releases the source even if this pull future is dropped.
        let Some(mut source) = self.source.take() else {
            return Ok(None);
        };
        self.finished = true;
        let result = crate::catch_continuation_panic(async {
            authorize_context(&self.service, &self.context, self.database_id)?;
            if self.context.control().is_cancelled()
                || self.context.control().is_deadline_exceeded()
            {
                return Err(ReplicationFailure::Unavailable);
            }
            let frame = crate::wait::wait_with_control(
                self.context.control(),
                self.service.providers.deadline_scheduler.as_ref(),
                source.next_item(),
            )
            .await
            .map_err(|_| ReplicationFailure::Unavailable)??;
            if frame.as_ref().is_some_and(|item| !item.bounded())
                || !self.emission.observe(frame.as_ref())
            {
                return Err(ReplicationFailure::Source(
                    ReplicationStreamErrorV3::CorruptHistory,
                ));
            }
            authorize_context(&self.service, &self.context, self.database_id)?;
            if self.context.control().is_cancelled()
                || self.context.control().is_deadline_exceeded()
            {
                return Err(ReplicationFailure::Unavailable);
            }
            Ok(frame)
        })
        .await
        .unwrap_or_else(|()| {
            self.service.providers.telemetry.record(
                crate::ServiceTelemetryEvent::InternalIntegrity {
                    operation: riffdb_types::ServiceOperationV1::StreamChangelog,
                },
            );
            Err(ReplicationFailure::Unavailable)
        });
        self.finished = !matches!(result, Ok(Some(_)));
        if !self.finished {
            self.source = Some(source);
        }
        if let Err(error) = &result {
            let event = match error {
                ReplicationFailure::AuthorizationDenied => {
                    crate::ServiceTelemetryEvent::StreamClosedByPolicy
                }
                ReplicationFailure::Source(ReplicationStreamErrorV3::CorruptHistory) => {
                    crate::ServiceTelemetryEvent::InternalIntegrity {
                        operation: riffdb_types::ServiceOperationV1::StreamChangelog,
                    }
                }
                _ => crate::ServiceTelemetryEvent::CursorUnavailable,
            };
            self.service.providers.telemetry.record(event);
        }
        result
    }
}

fn authorize(
    policy: &dyn CurrentPolicyPort,
    principal: &AuthenticatedPrincipal,
    database_id: DatabaseId,
) -> Result<(), ReplicationFailure> {
    match policy.authorize_replication(principal) {
        Ok(ReplicationDecision::Allow(proof)) if proof.database_id() == database_id => Ok(()),
        Ok(_) => Err(ReplicationFailure::AuthorizationDenied),
        Err(_) => Err(ReplicationFailure::Unavailable),
    }
}

pub(crate) fn authorize_context(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    database: DatabaseId,
) -> Result<(), ReplicationFailure> {
    if context.ingress() == riffdb_types::ServiceIngressKindV1::McpHttp
        || database != service.identity.database_id()
    {
        return Err(ReplicationFailure::AuthorizationDenied);
    }
    authorize(
        service.providers.policy.as_ref(),
        context.principal(),
        database,
    )
}
