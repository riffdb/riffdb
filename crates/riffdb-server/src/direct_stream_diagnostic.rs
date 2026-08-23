//! Feature-gated private direct-ownership probe for ADR-0141.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prost::Message as _;
use riffdb_api_exclusive::{
    DiagnosticServerStages, encode_response, read_open, read_request, write_opened,
};
use riffdb_api_grpc::{DATABASE_METADATA_KEY, GrpcApplication};
use riffdb_proto::v1::ApplicationSessionRequest;
use riffdb_proto::{decode_public_message, validate_public_message};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tonic::Code;
use tonic::metadata::MetadataValue;
use zeroize::{Zeroize as _, Zeroizing};

const CONNECTION_LIMIT: usize = 8;
const CONNECTION_LIFETIME: Duration = Duration::from_secs(120);
const OPERATION_LIMIT: Duration = Duration::from_secs(30);
const TLS_HANDSHAKE_LIMIT: Duration = Duration::from_secs(5);
const MAX_CERTIFICATE_BYTES: u64 = 256 * 1024;
const MAX_PRIVATE_KEY_BYTES: u64 = 64 * 1024;

/// Private diagnostic listener owned only by a `test-fixtures` daemon build.
pub(crate) struct HostedDirectStreamDiagnostic {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl HostedDirectStreamDiagnostic {
    /// Binds only when the exact diagnostic opt-in is present.
    pub(crate) async fn bind_if_enabled(application: &GrpcApplication) -> io::Result<Option<Self>> {
        if std::env::var_os("RIFFDB_DIRECT_STREAM_DIAGNOSTIC").as_deref()
            != Some(std::ffi::OsStr::new("1"))
        {
            return Ok(None);
        }
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let application = Arc::new(application.clone());
        let tls = diagnostic_tls_from_environment()?;
        let permits = Arc::new(Semaphore::new(CONNECTION_LIMIT));
        let (shutdown, mut stopping) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stopping => break,
                    accepted = listener.accept() => {
                        let Ok((stream, peer)) = accepted else { break; };
                        if !peer.ip().is_loopback() { continue; }
                        if stream.set_nodelay(true).is_err() { continue; }
                        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else { continue; };
                        let application = Arc::clone(&application);
                        let tls = tls.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            let served = async move {
                                if let Some(tls) = tls {
                                    let stream = tokio::time::timeout(
                                        TLS_HANDSHAKE_LIMIT,
                                        tls.accept(stream),
                                    )
                                    .await
                                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "diagnostic TLS handshake timed out"))?
                                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "diagnostic TLS handshake failed"))?;
                                    serve_connection(stream, application).await
                                } else {
                                    serve_connection(stream, application).await
                                }
                            };
                            let _ = tokio::time::timeout(CONNECTION_LIFETIME, served).await;
                        });
                    }
                }
            }
        });
        Ok(Some(Self {
            address,
            shutdown: Some(shutdown),
            task,
        }))
    }

    pub(crate) const fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for HostedDirectStreamDiagnostic {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

async fn serve_connection(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    application: Arc<GrpcApplication>,
) -> io::Result<()> {
    let mut open = tokio::time::timeout(OPERATION_LIMIT, read_open(&mut stream))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "diagnostic open timed out"))??;
    let credential = Zeroizing::new(
        String::from_utf8(std::mem::take(&mut open.credential))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "credential is not text"))?,
    );
    open.credential.zeroize();
    let database = String::from_utf8(open.database)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "database is not text"))?;
    let authorization_text = Zeroizing::new(format!("Bearer {}", credential.as_str()));
    let authorization = MetadataValue::from_str(authorization_text.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "credential is invalid"))?;
    let database = if database.is_empty() {
        None
    } else {
        Some(
            MetadataValue::from_str(&database)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "database is invalid"))?,
        )
    };
    let application_session = decode_public_message::<ApplicationSessionRequest>(
        &open.application_session,
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "session identity is invalid"))?;
    let opening_correlation_id = application_session.correlation_id;
    let Some(riffdb_proto::v1::application_session_request::Request::Open(application_session)) =
        application_session.request
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session identity is invalid",
        ));
    };
    let mut session_metadata = tonic::metadata::MetadataMap::new();
    session_metadata.insert("authorization", authorization.clone());
    if let Some(database) = database.as_ref() {
        session_metadata.insert(DATABASE_METADATA_KEY, database.clone());
    }
    let opened_identity = application
        .validate_application_session_open(session_metadata, &application_session)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::PermissionDenied, "session refused"))?;
    let opened = riffdb_proto::v1::ApplicationSessionResponse {
        correlation_id: opening_correlation_id,
        response: Some(
            riffdb_proto::v1::application_session_response::Response::Opened(
                opened_identity.clone(),
            ),
        ),
    };
    write_opened(&mut stream, &opened.encode_to_vec()).await?;

    let mut previous_write_ns = 0_u64;
    let mut last_correlation_id = opening_correlation_id;
    loop {
        let read = tokio::time::timeout(OPERATION_LIMIT, read_request(&mut stream)).await;
        let payload = match read {
            Ok(Ok(payload)) => payload,
            Ok(Err(error)) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "request timed out")),
        };
        let decode_started = Instant::now();
        let message = decode_public_message::<ApplicationSessionRequest>(&payload)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request is invalid"))?;
        if message.correlation_id == 0 || message.correlation_id <= last_correlation_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request correlation is invalid",
            ));
        }
        last_correlation_id = message.correlation_id;
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("authorization", authorization.clone());
        if let Some(database) = database.as_ref() {
            metadata.insert(DATABASE_METADATA_KEY, database.clone());
        }
        let decode_adapt_ns = elapsed_ns(decode_started.elapsed());

        let service_started = Instant::now();
        let result = tokio::time::timeout(
            OPERATION_LIMIT,
            application.execute_exclusive_diagnostic_operation(metadata, &opened_identity, message),
        )
        .await;
        let application_service_ns = elapsed_ns(service_started.elapsed());

        let encode_started = Instant::now();
        let (status_code, payload) = match result {
            Ok(Ok(response)) => {
                validate_public_message(&response)
                    .map_err(|_| io::Error::other("response validation failed"))?;
                (Code::Ok as u8, response.encode_to_vec())
            }
            Ok(Err(status)) => (status.code() as u8, status.details().to_vec()),
            Err(_) => (Code::DeadlineExceeded as u8, Vec::new()),
        };
        let encode_ns = elapsed_ns(encode_started.elapsed());
        let frame = encode_response(
            status_code,
            DiagnosticServerStages {
                decode_adapt_ns,
                application_service_ns,
                encode_ns,
                previous_write_ns,
            },
            &payload,
        )?;
        let write_started = Instant::now();
        stream.write_all(&frame).await?;
        stream.flush().await?;
        previous_write_ns = elapsed_ns(write_started.elapsed());
    }
}

fn diagnostic_tls_from_environment() -> io::Result<Option<TlsAcceptor>> {
    let certificate = std::env::var_os("RIFFDB_DIRECT_STREAM_DIAGNOSTIC_TLS_CERT");
    let private_key = std::env::var_os("RIFFDB_DIRECT_STREAM_DIAGNOSTIC_TLS_KEY");
    let (certificate, private_key) = match (certificate, private_key) {
        (None, None) => return Ok(None),
        (Some(certificate), Some(private_key)) => (certificate, private_key),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "incomplete diagnostic TLS configuration",
            ));
        }
    };
    let certificate = read_bounded(&certificate, MAX_CERTIFICATE_BYTES)?;
    let private_key = Zeroizing::new(read_bounded(&private_key, MAX_PRIVATE_KEY_BYTES)?);
    let certificates = CertificateDer::pem_reader_iter(io::Cursor::new(certificate))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid diagnostic certificate")
        })?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty diagnostic certificate chain",
        ));
    }
    let private_key = PrivateKeyDer::from_pem_reader(io::Cursor::new(private_key.as_slice()))
        .map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid diagnostic private key")
        })?;
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut config = tokio_rustls::rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| io::Error::other("diagnostic TLS versions unavailable"))?
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "diagnostic TLS identity invalid",
            )
        })?;
    config.alpn_protocols = vec![b"riffdb-private-wp670/3".to_vec()];
    config.max_early_data_size = 0;
    config.send_half_rtt_data = false;
    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}

fn read_bounded(path: &std::ffi::OsStr, maximum: u64) -> io::Result<Vec<u8>> {
    let path = Path::new(path);
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "diagnostic TLS file is invalid",
        ));
    }
    std::fs::read(path)
}

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
