//! Non-evidentiary ADR-0139 direct ownership client.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prost::Message as _;
use riffdb_api_exclusive::{DIAGNOSTIC_OPENED, DiagnosticServerStages, read_response, write_open, write_request};
use riffdb_app_baseline_core::{TicketRow, UuidBytes};
use riffdb_client_rust::generated::GeneratedQuery as _;
use riffdb_client_rust::{ApplicationClientError, app_v1, raise_query_result, v1};
use riffdb_proto::{decode_public_message, validate_public_message};
use riffdb_ticketdesk::{
    GET_TICKET_QUERY_PLAN_HASH, GetTicketFound, GetTicketQuery, GetTicketResult,
};
use rustls_pki_types::{CertificateDer, ServerName, pem::PemObject as _};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::{RiffDbError, RiffDbPublicBackend, map_app, parse_status_name, parse_uuid_text};

/// Complete paired timing for one direct generated read.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DirectDiagnosticTiming {
    /// Complete synchronous caller duration.
    pub outer: Duration,
    /// Runtime-entry scheduling before the direct future begins.
    pub runtime_entry: Duration,
    /// Strict Protobuf request construction and encode.
    pub client_encode: Duration,
    /// Direct caller-owned stream write and flush.
    pub stream_write: Duration,
    /// Server strict decode and metadata adaptation.
    pub server_decode_adapt: Duration,
    /// Existing application handler execution.
    pub application_service: Duration,
    /// Server strict response validation and encode.
    pub server_encode: Duration,
    /// Previous same-size response write, aligned after warmup.
    pub server_write: Duration,
    /// Remaining caller stream-poll/read time after server stages.
    pub stream_poll_read: Duration,
    /// Strict response decode and generated result assembly.
    pub client_decode: Duration,
    /// Runtime exit/caller wake after the future completes.
    pub caller_wakeup: Duration,
}

impl DirectDiagnosticTiming {
    /// Sum of the closed mutually-exclusive stage ledger.
    #[must_use]
    pub fn ledger_total(self) -> Duration {
        self.runtime_entry
            .saturating_add(self.client_encode)
            .saturating_add(self.stream_write)
            .saturating_add(self.server_decode_adapt)
            .saturating_add(self.application_service)
            .saturating_add(self.server_encode)
            .saturating_add(self.server_write)
            .saturating_add(self.stream_poll_read)
            .saturating_add(self.client_decode)
            .saturating_add(self.caller_wakeup)
    }
}

/// One caller-owned, one-in-flight diagnostic connection.
pub struct DirectDiagnosticClient {
    stream: Box<dyn DirectDiagnosticIo>,
    contract_bundle_hash: [u8; 32],
    query_module_hash: [u8; 32],
    runtime: tokio::runtime::Handle,
}

trait DirectDiagnosticIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> DirectDiagnosticIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Immutable, authority-free transport generation for one exact diagnostic
/// endpoint and TLS identity.
///
/// Clones share only rustls's bounded process-local resumption store. They do
/// not contain a credential, database, application identity, or authorization
/// decision.
#[derive(Clone)]
pub struct DirectDiagnosticTransportGeneration {
    address: SocketAddr,
    tls: Option<DirectDiagnosticTlsGeneration>,
}

#[derive(Clone)]
struct DirectDiagnosticTlsGeneration {
    config: Arc<tokio_rustls::rustls::ClientConfig>,
    server_name: ServerName<'static>,
}

/// Customer-paid setup intervals for one accepted direct session.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DirectDiagnosticOpenTiming {
    /// Socket creation through transport readiness and verified TLS, if used.
    pub trust_establishment: Duration,
    /// Credential presentation through the accepted session result.
    pub session_establishment: Duration,
    /// Caller time spanning both setup intervals.
    pub total: Duration,
}

impl DirectDiagnosticOpenTiming {
    /// Sum of the two non-overlapping setup intervals.
    #[must_use]
    pub fn ledger_total(self) -> Duration {
        self.trust_establishment
            .saturating_add(self.session_establishment)
    }
}

impl RiffDbPublicBackend {
    /// Compiles one exact authority-free diagnostic transport generation.
    pub fn direct_diagnostic_transport_generation(
        &self,
        address: SocketAddr,
    ) -> Result<DirectDiagnosticTransportGeneration, RiffDbError> {
        let tls = self
            .direct_diagnostic_tls_trust_root
            .as_deref()
            .map(build_verified_tls_generation)
            .transpose()?;
        Ok(DirectDiagnosticTransportGeneration { address, tls })
    }

    /// Opens the feature-gated lane and reports trust/session setup separately.
    pub async fn open_direct_diagnostic_on(
        &self,
        generation: &DirectDiagnosticTransportGeneration,
    ) -> Result<(DirectDiagnosticClient, DirectDiagnosticOpenTiming), RiffDbError> {
        let total_started = Instant::now();
        let trust_started = Instant::now();
        let address = generation.address;
        let stream = TcpStream::connect(address)
            .await
            .map_err(|_| RiffDbError::Connection)?;
        stream.set_nodelay(true).map_err(|_| RiffDbError::Connection)?;
        let mut stream: Box<dyn DirectDiagnosticIo> = if let Some(tls) = &generation.tls {
            Box::new(
                TlsConnector::from(tls.config.clone())
                    .connect(tls.server_name.clone(), stream)
                    .await
                    .map_err(|_| RiffDbError::Connection)?,
            )
        } else {
            Box::new(stream)
        };
        let trust_establishment = trust_started.elapsed();

        let session_started = Instant::now();
        let session = v1::ApplicationSessionRequest {
            correlation_id: 1,
            request: Some(v1::application_session_request::Request::Open(
                diagnostic_application_session(
                    self.contract_bundle_hash,
                    self.query_module_hash,
                    riffdb_client_rust::generate_request_id()
                        .map_err(|_| RiffDbError::Connection)?
                        .into_bytes()
                        .to_vec(),
                ),
            )),
        }
        .encode_to_vec();
        write_open(&mut stream, self.bearer_token.as_bytes(), &[], &session)
            .await
            .map_err(|_| RiffDbError::Connection)?;
        let opened = stream.read_u8().await.map_err(|_| RiffDbError::Connection)?;
        if opened != DIAGNOSTIC_OPENED {
            return Err(RiffDbError::Connection);
        }
        let session_establishment = session_started.elapsed();
        Ok((
            DirectDiagnosticClient {
                stream,
                contract_bundle_hash: self.contract_bundle_hash,
                query_module_hash: self.query_module_hash,
                runtime: self.runtime.clone(),
            },
            DirectDiagnosticOpenTiming {
                trust_establishment,
                session_establishment,
                total: total_started.elapsed(),
            },
        ))
    }

    /// Opens the feature-gated direct diagnostic lane with the same credential.
    pub async fn open_direct_diagnostic(
        &self,
        address: SocketAddr,
    ) -> Result<DirectDiagnosticClient, RiffDbError> {
        let generation = self.direct_diagnostic_transport_generation(address)?;
        self.open_direct_diagnostic_on(&generation)
            .await
            .map(|(client, _)| client)
    }
}

fn build_verified_tls_generation(
    trust_root: &std::path::Path,
) -> Result<DirectDiagnosticTlsGeneration, RiffDbError> {
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    let certificates = CertificateDer::pem_file_iter(trust_root)
        .map_err(|_| RiffDbError::Connection)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| RiffDbError::Connection)?;
    if certificates.is_empty() {
        return Err(RiffDbError::Connection);
    }
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|_| RiffDbError::Connection)?;
    }
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut config = tokio_rustls::rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| RiffDbError::Connection)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"riffdb-direct-diagnostic/2".to_vec()];
    config.enable_early_data = false;
    config.resumption = tokio_rustls::rustls::client::Resumption::in_memory_sessions(8);
    let server_name = ServerName::try_from("127.0.0.1")
        .map_err(|_| RiffDbError::Connection)?
        .to_owned();
    Ok(DirectDiagnosticTlsGeneration {
        config: Arc::new(config),
        server_name,
    })
}

fn diagnostic_application_session(
    contract_bundle_hash: [u8; 32],
    query_module_hash: [u8; 32],
    request_id: Vec<u8>,
) -> v1::ApplicationSessionOpen {
    v1::ApplicationSessionOpen {
        protocol_version: riffdb_client_rust::APPLICATION_SESSION_PROTOCOL_V1,
        contract: Some(app_v1::ContractSelector {
            lineage: "TicketDesk".to_owned(),
            version: 1,
            bundle_hash: contract_bundle_hash.to_vec(),
        }),
        query_module_hashes: vec![query_module_hash.to_vec()],
        application_lock_hash: crate::TICKETDESK_APPLICATION_LOCK_HASH.to_vec(),
        requested_max_in_flight: 1,
        request_id,
    }
}

impl DirectDiagnosticClient {
    /// Executes one generated `GetTicket` through the direct future.
    pub async fn get_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<(Option<TicketRow>, DirectDiagnosticTiming), RiffDbError> {
        let encode_started = Instant::now();
        let request = get_ticket_request(
            organization_id,
            ticket_id,
            self.contract_bundle_hash,
            self.query_module_hash,
        );
        validate_public_message(&request).map_err(|_| RiffDbError::Rpc("invalid diagnostic request".into()))?;
        let payload = request.encode_to_vec();
        let client_encode = encode_started.elapsed();

        let write_started = Instant::now();
        write_request(&mut self.stream, &payload)
            .await
            .map_err(|_| RiffDbError::Connection)?;
        let stream_write = write_started.elapsed();

        let read_started = Instant::now();
        let response = read_response(&mut self.stream)
            .await
            .map_err(|_| RiffDbError::Connection)?;
        let complete_read = read_started.elapsed();
        if response.status_code != tonic::Code::Ok as u8 {
            return Err(RiffDbError::Rpc(format!(
                "direct diagnostic returned status code {}",
                response.status_code
            )));
        }
        let server = stages(response.stages);
        let known_server = server.0.saturating_add(server.1).saturating_add(server.2).saturating_add(server.3);
        let stream_poll_read = complete_read.saturating_sub(known_server);

        let decode_started = Instant::now();
        let response = decode_public_message::<app_v1::ExecuteQueryResponse>(&response.payload)
            .map_err(|_| RiffDbError::Rpc("invalid diagnostic response".into()))?;
        validate_identity(&response, self.contract_bundle_hash, self.query_module_hash)?;
        let result = decode_get_ticket(response).map_err(map_app)?;
        let observed = match result {
            GetTicketResult::Found(found) => Some(ticket_row(organization_id, *found)?),
            GetTicketResult::NotFound(_) => None,
        };
        let client_decode = decode_started.elapsed();
        Ok((
            observed,
            DirectDiagnosticTiming {
                client_encode,
                stream_write,
                server_decode_adapt: server.0,
                application_service: server.1,
                server_encode: server.2,
                server_write: server.3,
                stream_poll_read,
                client_decode,
                ..DirectDiagnosticTiming::default()
            },
        ))
    }

    /// Runs one direct call from the frozen synchronous benchmark thread.
    pub fn get_ticket_paired(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<(Option<TicketRow>, DirectDiagnosticTiming), RiffDbError> {
        let outer_started = Instant::now();
        let runtime = self.runtime.clone();
        let (runtime_entry, result) = runtime.block_on(async {
            let runtime_entry = outer_started.elapsed();
            (runtime_entry, self.get_ticket(organization_id, ticket_id).await)
        });
        let (value, mut timing) = result?;
        let outer = outer_started.elapsed();
        let known = timing.ledger_total().saturating_add(runtime_entry);
        timing.outer = outer;
        timing.runtime_entry = runtime_entry;
        timing.caller_wakeup = outer.saturating_sub(known);
        Ok((value, timing))
    }
}

fn get_ticket_request(
    organization_id: UuidBytes,
    ticket_id: UuidBytes,
    contract_bundle_hash: [u8; 32],
    query_module_hash: [u8; 32],
) -> app_v1::ExecuteQueryRequest {
    let uuid = |value: UuidBytes| v1::Value {
        kind: Some(v1::value::Kind::UuidValue(value.to_vec())),
    };
    app_v1::ExecuteQueryRequest {
        contract: Some(app_v1::ContractSelector {
            lineage: "TicketDesk".to_owned(),
            version: 1,
            bundle_hash: contract_bundle_hash.to_vec(),
        }),
        query: Some(app_v1::execute_query_request::Query::QueryName("GetTicket".to_owned())),
        module_hash: Some(query_module_hash.to_vec()),
        parameters: vec![
            app_v1::Parameter {
                name: "organization_id".to_owned(),
                value: Some(uuid(organization_id)),
            },
            app_v1::Parameter {
                name: "ticket_id".to_owned(),
                value: Some(uuid(ticket_id)),
            },
        ],
        cursor: None,
        minimum_application_head: None,
        accepted_result_encodings: vec![
            app_v1::NamedResultEncoding::LegacyRecords as i32,
            app_v1::NamedResultEncoding::CompactV1 as i32,
        ],
        request_id: ticket_id.to_vec(),
    }
}

fn validate_identity(
    response: &app_v1::ExecuteQueryResponse,
    contract_bundle_hash: [u8; 32],
    query_module_hash: [u8; 32],
) -> Result<(), RiffDbError> {
    let identity = response.identity.as_ref().ok_or_else(|| RiffDbError::Rpc("missing identity".into()))?;
    if identity.contract_lineage != "TicketDesk"
        || identity.contract_version != 1
        || identity.contract_bundle_hash != contract_bundle_hash
        || identity.module_hash.as_deref() != Some(query_module_hash.as_slice())
        || identity.query_name.as_deref() != Some("GetTicket")
        || identity.plan_hash.as_slice() != GET_TICKET_QUERY_PLAN_HASH
    {
        return Err(RiffDbError::Rpc("diagnostic response identity mismatch".into()));
    }
    Ok(())
}

fn decode_get_ticket(
    response: app_v1::ExecuteQueryResponse,
) -> Result<GetTicketResult, ApplicationClientError> {
    match app_v1::NamedResultEncoding::try_from(response.selected_result_encoding)
        .map_err(|_| ApplicationClientError::InvalidResponse)?
    {
        app_v1::NamedResultEncoding::CompactV1 => GetTicketQuery::decode_compact_result(
            response.outcome,
            response.compact_result.ok_or(ApplicationClientError::InvalidResponse)?,
        ),
        app_v1::NamedResultEncoding::LegacyRecords | app_v1::NamedResultEncoding::Unspecified => {
            GetTicketQuery::decode_result(raise_query_result(response)?)
        }
        app_v1::NamedResultEncoding::PackedV1 => Err(ApplicationClientError::InvalidResponse),
    }
}

fn ticket_row(organization_id: UuidBytes, found: GetTicketFound) -> Result<TicketRow, RiffDbError> {
    Ok(TicketRow {
        organization_id,
        ticket_id: parse_uuid_text(&found.ticket.ticket_id)?,
        project_id: parse_uuid_text(&found.ticket.project_id)?,
        reporter_id: parse_uuid_text(&found.ticket.reporter_id)?,
        assignee_id: parse_uuid_text(&found.ticket.assignee_id)?,
        status: parse_status_name(&found.ticket.status)?,
        title: found.ticket.title,
    })
}

fn stages(stages: DiagnosticServerStages) -> (Duration, Duration, Duration, Duration) {
    (
        Duration::from_nanos(stages.decode_adapt_ns),
        Duration::from_nanos(stages.application_service_ns),
        Duration::from_nanos(stages.encode_ns),
        Duration::from_nanos(stages.previous_write_ns),
    )
}

#[cfg(test)]
mod architecture_tests {
    #[test]
    fn diagnostic_tls_forbids_early_data_and_bounds_resumption() {
        let source = include_str!("direct_diagnostic.rs");
        assert!(source.contains("config.enable_early_data = false;"));
        assert!(source.contains("Resumption::in_memory_sessions(8)"));
        let forbidden = ["enable_early_data = ", "true"].concat();
        assert!(!source.contains(&forbidden));
    }
}
