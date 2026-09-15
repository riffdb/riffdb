//! API-neutral administrative replication. Storage sources cannot authorize
//! releases, and transport adapters cannot bypass the fresh current-policy check.

use crate::CurrentPolicyPort;
use riffdb_auth::AuthenticatedPrincipal;
use riffdb_policy::ReplicationDecision;
use std::{future::Future, pin::Pin, sync::Arc};

pub use riffdb_errors::ReplicationStreamErrorV3;
use riffdb_types::{DatabaseId, DualFrontier};

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
    /// Complete checksummed V3 tail frame.
    Frame(Vec<u8>),
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
        let (bytes, limit) = match self {
            Self::Frame(bytes) => (bytes, 32 * 1024 * 1024),
            Self::BootstrapManifest(bytes) => (bytes, 512),
            Self::BootstrapPage(bytes) => (bytes, 32 * 1024 * 1024 + 512),
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
    /// Opens an authenticated stream. The transport must establish a confidential
    /// connection before calling; all data authority is checked in this service.
    fn stream_changelog(
        &self,
        principal: AuthenticatedPrincipal,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, ReplicationSubscription>;
}

/// Service-owned composition of current policy and the published source.
pub struct ReplicationService {
    policy: Arc<dyn CurrentPolicyPort>,
    source: Arc<dyn ReplicationSourcePort>,
}

impl ReplicationService {
    /// Connects the same current-policy owner used by ordinary application work.
    #[must_use]
    pub fn new(policy: Arc<dyn CurrentPolicyPort>, source: Arc<dyn ReplicationSourcePort>) -> Self {
        Self { policy, source }
    }
}

impl ReplicationApplication for ReplicationService {
    fn stream_changelog(
        &self,
        principal: AuthenticatedPrincipal,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, ReplicationSubscription> {
        Box::pin(async move {
            if request.history_incarnation == 0
                || request.leadership_epoch == 0
                || request.readable_format.is_empty()
                || request.readable_format.len() > 128
            {
                return Err(ReplicationFailure::Source(
                    ReplicationStreamErrorV3::InvalidPosition,
                ));
            }
            let phase_valid = match &request.phase {
                ReplicationPhase::Tail => request.after_sequence != 0,
                ReplicationPhase::Follower { hold_id } => {
                    request.after_sequence != 0 && *hold_id != [0; 16]
                }
                ReplicationPhase::Attach { manifest } => {
                    request.after_sequence != 0 && !manifest.is_empty() && manifest.len() <= 512
                }
                ReplicationPhase::Bootstrap {
                    hold_id,
                    resume_manifest,
                    after_page,
                } => {
                    request.after_sequence == 0
                        && request.after_hash == [0; 32]
                        && request.after_frontier == DualFrontier::INITIAL
                        && *hold_id != [0; 16]
                        && resume_manifest.len() <= 512
                        && *after_page <= 1_048_576
                        && (!resume_manifest.is_empty() || *after_page == 0)
                }
            };
            if !phase_valid {
                return Err(ReplicationFailure::Source(
                    ReplicationStreamErrorV3::InvalidPosition,
                ));
            }
            let database_id = request.database_id;
            authorize(self.policy.as_ref(), &principal, database_id)?;
            let emission = match request.phase {
                ReplicationPhase::Bootstrap { .. } => Emission::Manifest,
                _ => Emission::Tail,
            };
            let source = self.source.open(request).await?;
            authorize(self.policy.as_ref(), &principal, database_id)?;
            Ok(ReplicationSubscription {
                policy: Arc::clone(&self.policy),
                principal,
                database_id,
                source,
                emission,
                finished: false,
            })
        })
    }
}

/// Move-only subscriber. No item is released without rechecking current
/// capability state after framing and any wait. Failure permanently closes it.
pub struct ReplicationSubscription {
    policy: Arc<dyn CurrentPolicyPort>,
    principal: AuthenticatedPrincipal,
    database_id: DatabaseId,
    source: Box<dyn ReplicationItemSource>,
    emission: Emission,
    finished: bool,
}

enum Emission {
    Tail,
    Manifest,
    Pages(u32),
}
impl Emission {
    fn observe(&mut self, item: Option<&ReplicationItem>) -> bool {
        match (&self, item) {
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
        // Cancellation cannot resume a partially consumed source on this session.
        self.finished = true;
        let result = async {
            authorize(self.policy.as_ref(), &self.principal, self.database_id)?;
            let frame = self.source.next_item().await?;
            if frame.as_ref().is_some_and(|item| !item.bounded())
                || !self.emission.observe(frame.as_ref())
            {
                return Err(ReplicationFailure::Source(
                    ReplicationStreamErrorV3::CorruptHistory,
                ));
            }
            authorize(self.policy.as_ref(), &self.principal, self.database_id)?;
            Ok(frame)
        }
        .await;
        self.finished = !matches!(result, Ok(Some(_)));
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
