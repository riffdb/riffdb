//! Private ADR-0141 direct-ownership diagnostic transport.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use riffdb_api_exclusive::{
    DiagnosticServerStages, read_opened, read_response, write_open, write_request,
};
use riffdb_proto::{decode_public_message, v1, validate_public_message};
use rustls_pki_types::{CertificateDer, ServerName, pem::PemObject as _};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio_rustls::TlsConnector;
use tonic::Code;
use tonic_prost::prost::Message as _;

use crate::session::{SessionExpected, session_failure};
use crate::{
    ApplicationSessionIdentity, CallMetadata, ClientError, DetailsFreeStatus, OutcomeUnknown,
    ProtocolFailure, ProtocolFailureKind, generate_request_id,
};

/// Private diagnostic endpoint. This is not an application configuration
/// surface and carries no compatibility promise.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ExclusiveDiagnosticConfiguration {
    /// Loopback endpoint published by a `test-fixtures` daemon.
    pub address: SocketAddr,
    /// Optional exact trust root for the verified-TLS diagnostic generation.
    pub trust_root: Option<PathBuf>,
    /// Exact TLS peer name. Ignored for cleartext loopback.
    pub server_name: String,
}

/// Closed aggregate of customer-paid private-lane stages since the previous
/// evidence reset.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExclusiveDiagnosticEvidence {
    /// Completed operations.
    pub operations: u64,
    /// Total encoded request bytes.
    pub request_bytes: u64,
    /// Total encoded response bytes.
    pub response_bytes: u64,
    /// Complete caller-owned stream operation time.
    pub caller_ns: u64,
    /// Client request validation and encoding.
    pub client_encode_ns: u64,
    /// Client write and flush.
    pub client_write_ns: u64,
    /// Server strict decode and adaptation.
    pub server_decode_adapt_ns: u64,
    /// Existing API-neutral application handler.
    pub application_service_ns: u64,
    /// Server response validation and encoding.
    pub server_encode_ns: u64,
    /// Aligned prior response write.
    pub server_write_ns: u64,
    /// Remaining caller stream poll/read time.
    pub client_read_residual_ns: u64,
    /// Client strict response decode.
    pub client_decode_ns: u64,
}

/// One bounded operation-symbol ledger from the private diagnostic lane.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExclusiveDiagnosticOperationEvidence {
    /// Compiler-declared command or named-query symbol.
    pub operation: String,
    /// Aggregate stages for that exact symbol.
    pub evidence: ExclusiveDiagnosticEvidence,
}

impl ExclusiveDiagnosticEvidence {
    fn record(&mut self, operation: OperationEvidence) {
        self.operations = self.operations.saturating_add(1);
        self.request_bytes = self.request_bytes.saturating_add(operation.request_bytes);
        self.response_bytes = self.response_bytes.saturating_add(operation.response_bytes);
        self.caller_ns = self.caller_ns.saturating_add(operation.caller_ns);
        self.client_encode_ns = self
            .client_encode_ns
            .saturating_add(operation.client_encode_ns);
        self.client_write_ns = self
            .client_write_ns
            .saturating_add(operation.client_write_ns);
        self.server_decode_adapt_ns = self
            .server_decode_adapt_ns
            .saturating_add(operation.server.decode_adapt_ns);
        self.application_service_ns = self
            .application_service_ns
            .saturating_add(operation.server.application_service_ns);
        self.server_encode_ns = self
            .server_encode_ns
            .saturating_add(operation.server.encode_ns);
        self.server_write_ns = self
            .server_write_ns
            .saturating_add(operation.server.previous_write_ns);
        self.client_read_residual_ns = self
            .client_read_residual_ns
            .saturating_add(operation.client_read_residual_ns);
        self.client_decode_ns = self
            .client_decode_ns
            .saturating_add(operation.client_decode_ns);
    }
}

#[derive(Clone)]
pub(crate) struct ExclusiveDiagnosticSession {
    connection: Arc<AsyncMutex<Option<ExclusiveConnection>>>,
    metadata: CallMetadata,
    evidence: Arc<Mutex<BTreeMap<String, ExclusiveDiagnosticEvidence>>>,
}

trait ExclusiveIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> ExclusiveIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

struct ExclusiveConnection {
    stream: Box<dyn ExclusiveIo>,
    next_correlation: u64,
}

struct OperationEvidence {
    operation: String,
    request_bytes: u64,
    response_bytes: u64,
    caller_ns: u64,
    client_encode_ns: u64,
    client_write_ns: u64,
    server: DiagnosticServerStages,
    client_read_residual_ns: u64,
    client_decode_ns: u64,
}

impl ExclusiveDiagnosticSession {
    pub(crate) async fn open(
        configuration: ExclusiveDiagnosticConfiguration,
        identity: ApplicationSessionIdentity,
        metadata: CallMetadata,
    ) -> Result<Self, ClientError> {
        let credential = metadata
            .exclusive_bearer()
            .ok_or(ClientError::ConnectionFailure)?;
        let stream = TcpStream::connect(configuration.address)
            .await
            .map_err(|_| ClientError::ConnectionFailure)?;
        stream
            .set_nodelay(true)
            .map_err(|_| ClientError::ConnectionFailure)?;
        let mut stream: Box<dyn ExclusiveIo> = match configuration.trust_root.as_deref() {
            Some(trust_root) => Box::new(
                TlsConnector::from(build_tls(trust_root)?)
                    .connect(
                        ServerName::try_from(configuration.server_name)
                            .map_err(|_| ClientError::ConnectionFailure)?,
                        stream,
                    )
                    .await
                    .map_err(|_| ClientError::ConnectionFailure)?,
            ),
            None => Box::new(stream),
        };
        let opening_correlation = 1_u64;
        let request = v1::ApplicationSessionRequest {
            correlation_id: opening_correlation,
            request: Some(v1::application_session_request::Request::Open(
                identity.open_request(
                    generate_request_id()
                        .map_err(ClientError::IdentifierGeneration)?
                        .into_bytes()
                        .to_vec(),
                ),
            )),
        };
        validate_public_message(&request).map_err(|_| invalid_inbound())?;
        write_open(
            &mut stream,
            credential.as_bytes(),
            metadata.exclusive_database().as_bytes(),
            &request.encode_to_vec(),
        )
        .await
        .map_err(|_| ClientError::ConnectionFailure)?;
        let opened = read_opened(&mut stream)
            .await
            .map_err(|_| ClientError::ConnectionFailure)?;
        let opened = decode_public_message::<v1::ApplicationSessionResponse>(&opened)
            .map_err(|_| invalid_inbound())?;
        let Some(v1::application_session_response::Response::Opened(opened_identity)) =
            opened.response.as_ref()
        else {
            return Err(invalid_inbound());
        };
        if opened.correlation_id != opening_correlation || !identity.matches_opened(opened_identity)
        {
            return Err(invalid_inbound());
        }
        Ok(Self {
            connection: Arc::new(AsyncMutex::new(Some(ExclusiveConnection {
                stream,
                next_correlation: opening_correlation + 1,
            }))),
            metadata,
            evidence: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub(crate) fn matches_metadata(&self, metadata: &CallMetadata) -> bool {
        self.metadata.has_same_session_scope(metadata)
    }

    pub(crate) fn take_evidence(&self) -> Vec<ExclusiveDiagnosticOperationEvidence> {
        self.evidence.lock().map_or_else(
            |_| Vec::new(),
            |mut evidence| {
                std::mem::take(&mut *evidence)
                    .into_iter()
                    .map(
                        |(operation, evidence)| ExclusiveDiagnosticOperationEvidence {
                            operation,
                            evidence,
                        },
                    )
                    .collect()
            },
        )
    }

    async fn call(
        &self,
        request: v1::application_session_request::Request,
    ) -> Result<v1::ApplicationSessionResponse, ClientError> {
        let mut connection = self
            .connection
            .lock()
            .await
            .take()
            .ok_or_else(local_capacity)?;
        match connection.call(request).await {
            Ok((response, operation)) => {
                if let Ok(mut evidence) = self.evidence.lock()
                    && (evidence.contains_key(&operation.operation) || evidence.len() < 64)
                {
                    evidence
                        .entry(operation.operation.clone())
                        .or_default()
                        .record(operation);
                }
                *self.connection.lock().await = Some(connection);
                Ok(response)
            }
            // Cancellation or any uncertain framing failure destroys the lane.
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn execute_command(
        &self,
        request: v1::ExecuteCommandRequest,
    ) -> Result<v1::ExecuteCommandResponse, ClientError> {
        let response = self
            .call(v1::application_session_request::Request::Command(request))
            .await?;
        match response.response {
            Some(v1::application_session_response::Response::Command(response)) => Ok(response),
            Some(v1::application_session_response::Response::Failure(failure)) => {
                Err(session_failure(failure, SessionExpected::Command))
            }
            _ => Err(invalid_inbound()),
        }
    }

    pub(crate) async fn execute_query(
        &self,
        request: riffdb_proto::app::v1::ExecuteQueryRequest,
    ) -> Result<riffdb_proto::app::v1::ExecuteQueryResponse, ClientError> {
        let response = self
            .call(v1::application_session_request::Request::Query(request))
            .await?;
        match response.response {
            Some(v1::application_session_response::Response::Query(response)) => Ok(response),
            Some(v1::application_session_response::Response::Failure(failure)) => {
                Err(session_failure(failure, SessionExpected::Query))
            }
            _ => Err(invalid_inbound()),
        }
    }
}

impl ExclusiveConnection {
    async fn call(
        &mut self,
        request: v1::application_session_request::Request,
    ) -> Result<(v1::ApplicationSessionResponse, OperationEvidence), ClientError> {
        let caller_started = Instant::now();
        let operation = operation_symbol(&request)?;
        let correlation_id = self.next_correlation;
        self.next_correlation = self
            .next_correlation
            .checked_add(1)
            .filter(|next| *next != 0)
            .ok_or_else(invalid_inbound)?;
        let encode_started = Instant::now();
        let request = v1::ApplicationSessionRequest {
            correlation_id,
            request: Some(request),
        };
        validate_public_message(&request).map_err(|_| invalid_inbound())?;
        let payload = request.encode_to_vec();
        let client_encode_ns = elapsed_ns(encode_started.elapsed());
        let write_started = Instant::now();
        write_request(&mut self.stream, &payload)
            .await
            .map_err(|_| ClientError::ConnectionFailure)?;
        let client_write_ns = elapsed_ns(write_started.elapsed());
        let read_started = Instant::now();
        let response = read_response(&mut self.stream)
            .await
            .map_err(|_| ClientError::ConnectionFailure)?;
        let complete_read_ns = elapsed_ns(read_started.elapsed());
        if response.status_code != Code::Ok as u8 {
            return Err(details_free_status(Code::from_i32(i32::from(
                response.status_code,
            ))));
        }
        let known_server_ns = response
            .stages
            .decode_adapt_ns
            .saturating_add(response.stages.application_service_ns)
            .saturating_add(response.stages.encode_ns)
            .saturating_add(response.stages.previous_write_ns);
        let decode_started = Instant::now();
        let decoded = decode_public_message::<v1::ApplicationSessionResponse>(&response.payload)
            .map_err(|_| invalid_inbound())?;
        if decoded.correlation_id != correlation_id {
            return Err(invalid_inbound());
        }
        let client_decode_ns = elapsed_ns(decode_started.elapsed());
        Ok((
            decoded,
            OperationEvidence {
                operation,
                request_bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
                response_bytes: u64::try_from(response.payload.len()).unwrap_or(u64::MAX),
                caller_ns: elapsed_ns(caller_started.elapsed()),
                client_encode_ns,
                client_write_ns,
                server: response.stages,
                client_read_residual_ns: complete_read_ns.saturating_sub(known_server_ns),
                client_decode_ns,
            },
        ))
    }
}

fn operation_symbol(
    request: &v1::application_session_request::Request,
) -> Result<String, ClientError> {
    let symbol = match request {
        v1::application_session_request::Request::Command(command) => command.command_name.as_str(),
        v1::application_session_request::Request::Query(query) => match query.query.as_ref() {
            Some(riffdb_proto::app::v1::execute_query_request::Query::QueryName(name)) => name,
            Some(riffdb_proto::app::v1::execute_query_request::Query::Source(_)) | None => {
                return Err(invalid_inbound());
            }
        },
        v1::application_session_request::Request::Open(_)
        | v1::application_session_request::Request::Cancel(_) => return Err(invalid_inbound()),
    };
    if symbol.is_empty() || symbol.len() > 256 {
        return Err(invalid_inbound());
    }
    Ok(symbol.to_owned())
}

fn build_tls(trust_root: &Path) -> Result<Arc<tokio_rustls::rustls::ClientConfig>, ClientError> {
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    let certificates = CertificateDer::pem_file_iter(trust_root)
        .map_err(|_| ClientError::ConnectionFailure)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ClientError::ConnectionFailure)?;
    if certificates.is_empty() {
        return Err(ClientError::ConnectionFailure);
    }
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|_| ClientError::ConnectionFailure)?;
    }
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut config = tokio_rustls::rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| ClientError::ConnectionFailure)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"riffdb-private-wp670/3".to_vec()];
    config.enable_early_data = false;
    config.resumption = tokio_rustls::rustls::client::Resumption::in_memory_sessions(8);
    Ok(Arc::new(config))
}

fn details_free_status(code: Code) -> ClientError {
    match code {
        Code::Unknown => ClientError::OutcomeUnknown(OutcomeUnknown),
        Code::Unauthenticated => ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated),
        Code::Cancelled => ClientError::DetailsFree(DetailsFreeStatus::Cancelled),
        Code::DeadlineExceeded => ClientError::DetailsFree(DetailsFreeStatus::DeadlineExceeded),
        Code::Unavailable => ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable),
        Code::Internal => ClientError::DetailsFree(DetailsFreeStatus::EmergencyInternal),
        _ => ClientError::ConnectionFailure,
    }
}

fn invalid_inbound() -> ClientError {
    ClientError::Protocol(ProtocolFailure::new(
        ProtocolFailureKind::InvalidInboundMessage,
    ))
}

fn local_capacity() -> ClientError {
    ClientError::Public(riffdb_errors::PublicError::overloaded())
}

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
