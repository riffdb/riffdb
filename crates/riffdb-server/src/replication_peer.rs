//! Confidential outbound replication, using the existing checked public codec.
use crate::identifiers::{ProductionIdentifierSources, ServerRequestIdSource};
use riffdb_api_grpc::ReplicationWireClient;
use riffdb_auth::RawCapabilityToken;
use riffdb_config::TlsClientConfig;
use riffdb_service::{
    ReplicationFailure as Failure, ReplicationFuture, ReplicationItem as Item,
    ReplicationItemSource, ReplicationRequest, ReplicationSourcePort,
};
use riffdb_types::DatabaseAlias;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

const LIFETIME: Duration = Duration::from_secs(15 * 60);

/// Administrative peer with mandatory explicit-CA and exact-name TLS validation.
/// No constructor accepts an arbitrary channel, cleartext endpoint or trust
/// bypass. Debug never exposes the endpoint, credential, alias or peer errors.
pub struct VerifiedReplicationPeer {
    client: ReplicationWireClient,
    credential: RawCapabilityToken,
    database: DatabaseAlias,
    identifiers: ServerRequestIdSource,
    capacity: Arc<Semaphore>,
}
impl VerifiedReplicationPeer {
    /// Connects with the existing checked TLS configuration and protected trust
    /// file rules. Filesystem loading runs outside the async runtime. The fixed
    /// peer admits one active stream, including its opening request.
    pub async fn connect(
        config: TlsClientConfig,
        credential: RawCapabilityToken,
        database: DatabaseAlias,
    ) -> Result<Self, Failure> {
        let root = config.trust_root().as_path().to_path_buf();
        let roots = tokio::task::spawn_blocking(move || {
            use rustls_pki_types::{CertificateDer, pem::PemObject};
            let (bytes, _) =
                super::read_transport_file(&root, 256 * 1024, false).map_err(|_| unavailable())?;
            let mut count = 0;
            for certificate in CertificateDer::pem_slice_iter(&bytes).take(65) {
                certificate.map_err(|_| unavailable())?;
                count += 1;
            }
            if count == 0 || count > 64 {
                return Err(unavailable());
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| unavailable())??;
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(roots))
            .domain_name(config.expected_server_identity().as_str());
        let endpoint = Endpoint::from_shared(config.endpoint().as_str().to_owned())
            .map_err(|_| unavailable())?
            .connect_timeout(config.connect_timeout())
            .timeout(LIFETIME)
            .concurrency_limit(1)
            .http2_keep_alive_interval(config.keepalive_interval())
            .keep_alive_while_idle(true)
            .tls_config(tls)
            .map_err(|_| unavailable())?;
        let channel = tokio::time::timeout(config.connect_timeout(), endpoint.connect())
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
        Ok(Self {
            client: ReplicationWireClient::new(channel),
            credential,
            database,
            identifiers: ProductionIdentifierSources::new().request_ids(),
            capacity: Arc::new(Semaphore::new(1)),
        })
    }
}
/// Move-only evidence obtained from this configured source's exact TLS path.
/// Kept inside the server: promotion must use its own configured peer, never a
/// caller-supplied channel, receipt or proof value.
pub(crate) struct AuthenticatedPrimaryFenceProof(riffdb_service::PrimaryFenceSourceEvidenceV1);
impl AuthenticatedPrimaryFenceProof {
    pub(crate) fn into_evidence(self) -> riffdb_service::PrimaryFenceSourceEvidenceV1 {
        self.0
    }
}
impl std::fmt::Debug for AuthenticatedPrimaryFenceProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthenticatedPrimaryFenceProof([redacted])")
    }
}
impl VerifiedReplicationPeer {
    pub(crate) async fn authenticated_primary_fence(
        &self,
        request: riffdb_service::PrimaryFenceRequestV1,
        applied: riffdb_service::ChangelogHistoryPointV3,
    ) -> Result<AuthenticatedPrimaryFenceProof, Failure> {
        let target = request.target();
        let catalog = riffdb_storage_api::AuthoritativeStateCatalogV2.digest();
        let input = ReplicationRequest {
            database_id: target.database_id(),
            history_incarnation: target.history_incarnation(),
            leadership_epoch: target.leadership_epoch().get(),
            phase: riffdb_service::ReplicationPhase::FenceEvidence { request },
            after_sequence: applied.sequence().get(),
            after_hash: applied.history_hash(),
            after_frontier: applied.frontier(),
            readable_format: riffdb_storage_api::ChangelogFrameV3::IDENTITY.into(),
            catalog_digest: catalog,
            maximum_frame_bytes: riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES as u64,
            maximum_transitions: riffdb_storage_api::MAX_STAGED_COMMANDS as u64,
        };
        let corrupt = || Failure::Source(riffdb_errors::ReplicationStreamErrorV3::CorruptHistory);
        let mut stream = self.open(input).await?;
        let Some(Item::FenceEvidence(evidence)) = stream.next_item().await? else {
            return Err(corrupt());
        };
        let fence = evidence.fence();
        if fence.target() != target
            || fence.operation_id() != request.operation_id()
            || fence.generation() != request.generation()
            || evidence.applied() != applied
            || evidence.source_history().lineage().catalog_digest() != catalog
        {
            return Err(corrupt());
        }
        // Authentication alone cannot excuse a missing/extra item or a late
        // denial. Consume the terminal boundary before creating private proof.
        if stream.next_item().await?.is_some() {
            return Err(corrupt());
        }
        Ok(AuthenticatedPrimaryFenceProof(*evidence))
    }
}
impl std::fmt::Debug for VerifiedReplicationPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VerifiedReplicationPeer([redacted])")
    }
}
impl ReplicationSourcePort for VerifiedReplicationPeer {
    fn primary_fence_source_evidence(
        &self,
        request: riffdb_service::PrimaryFenceRequestV1,
        applied: riffdb_service::ChangelogHistoryPointV3,
    ) -> ReplicationFuture<'_, riffdb_service::PrimaryFenceSourceEvidenceV1> {
        Box::pin(async move {
            self.authenticated_primary_fence(request, applied)
                .await
                .map(AuthenticatedPrimaryFenceProof::into_evidence)
        })
    }

    fn open(
        &self,
        input: ReplicationRequest,
    ) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        Box::pin(async move {
            let permit = Arc::clone(&self.capacity)
                .try_acquire_owned()
                .map_err(|_| unavailable())?;
            let deadline = Instant::now() + LIFETIME;
            let token = self.credential.encode_text().map_err(|_| unavailable())?;
            let mut bearer = zeroize::Zeroizing::new(String::from("Bearer "));
            bearer.push_str(std::str::from_utf8(token.expose_secret()).map_err(|_| unavailable())?);
            let mut authorization: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
                bearer.parse().map_err(|_| unavailable())?;
            authorization.set_sensitive(true);
            let stream = tokio::time::timeout_at(
                deadline,
                self.client.open(
                    input,
                    self.identifiers
                        .next_request_id()
                        .map_err(|_| unavailable())?,
                    authorization,
                    &self.database,
                ),
            )
            .await
            .map_err(|_| unavailable())??;
            Ok(Box::new(PeerItems {
                owner: Some(PeerStream {
                    stream,
                    _permit: permit,
                }),
                deadline,
            }) as Box<dyn ReplicationItemSource>)
        })
    }
}
struct PeerStream {
    stream: Box<dyn ReplicationItemSource>,
    _permit: OwnedSemaphorePermit,
}
struct PeerItems {
    owner: Option<PeerStream>,
    deadline: Instant,
}
impl ReplicationItemSource for PeerItems {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<Item>> {
        Box::pin(async move {
            let Some(mut owner) = self.owner.take() else {
                return Ok(None);
            };
            if Instant::now() >= self.deadline {
                return Err(unavailable());
            }
            let item = tokio::time::timeout_at(self.deadline, owner.stream.next_item())
                .await
                .map_err(|_| unavailable())??;
            if Instant::now() >= self.deadline {
                return Err(unavailable());
            }
            let Some(item) = item else {
                return Ok(None);
            };
            self.owner = Some(owner);
            Ok(Some(item))
        })
    }
}
fn unavailable() -> Failure {
    Failure::Unavailable
}
