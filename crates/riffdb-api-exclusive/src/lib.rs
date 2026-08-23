#![forbid(unsafe_code)]

//! Private direct-ownership framing for ADR-0141's diagnostic-first gate.
//!
//! This crate is not a public application protocol. WP-670 removes it if the
//! complete small-operation gate fails. A later package may harden it only
//! after that gate passes.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Distinct diagnostic preface; it is never accepted by a production listener.
pub const DIAGNOSTIC_MAGIC: [u8; 16] = *b"RDBX-WP670-V3\0\0\0";
/// Exact v1 bearer presentation bytes excluding the `Bearer ` prefix.
pub const DIAGNOSTIC_CREDENTIAL_BYTES: usize = 43;
/// Maximum canonical database alias bytes accepted by the probe.
pub const MAX_DIAGNOSTIC_DATABASE_BYTES: usize = 128;
/// Maximum strict encoded application-session identity bytes.
pub const MAX_DIAGNOSTIC_SESSION_BYTES: usize = 2_048;
/// Maximum strict request payload bytes.
pub const MAX_DIAGNOSTIC_REQUEST_BYTES: usize = 1_048_576;
/// Maximum strict response payload bytes.
pub const MAX_DIAGNOSTIC_RESPONSE_BYTES: usize = 4_194_304;
/// Server acknowledgement after a checked open preface.
pub const DIAGNOSTIC_OPENED: u8 = 0xa1;

/// One checked diagnostic connection presentation.
#[derive(Debug)]
pub struct DiagnosticOpen {
    /// Exact bearer presentation without the public metadata scheme prefix.
    pub credential: Vec<u8>,
    /// Empty selects the server's only/default database.
    pub database: Vec<u8>,
    /// Strict encoded `ApplicationSessionOpen` identity.
    pub application_session: Vec<u8>,
}

/// Per-operation server stages returned only to the mechanics probe.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DiagnosticServerStages {
    /// Strict decode plus request/metadata adaptation.
    pub decode_adapt_ns: u64,
    /// Existing application-service handler execution.
    pub application_service_ns: u64,
    /// Strict response validation plus Protobuf encode.
    pub encode_ns: u64,
    /// Previous same-connection response write, aligned after warmup.
    pub previous_write_ns: u64,
}

/// One response frame decoded by the direct caller.
#[derive(Debug)]
pub struct DiagnosticResponse {
    /// Zero denotes success; other values are stable gRPC status codes.
    pub status_code: u8,
    /// Server-owned stage observations.
    pub stages: DiagnosticServerStages,
    /// Strict response bytes on success or bounded structured details on failure.
    pub payload: Vec<u8>,
}

/// Writes one bounded connection presentation.
pub async fn write_open(
    stream: &mut (impl AsyncWrite + Unpin),
    credential: &[u8],
    database: &[u8],
    application_session: &[u8],
) -> io::Result<()> {
    if credential.len() != DIAGNOSTIC_CREDENTIAL_BYTES
        || database.len() > MAX_DIAGNOSTIC_DATABASE_BYTES
        || application_session.is_empty()
        || application_session.len() > MAX_DIAGNOSTIC_SESSION_BYTES
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid diagnostic open",
        ));
    }
    let database_len = u16::try_from(database.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "database alias too large"))?;
    let session_len = u16::try_from(application_session.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "session identity too large"))?;
    stream.write_all(&DIAGNOSTIC_MAGIC).await?;
    stream
        .write_all(&(DIAGNOSTIC_CREDENTIAL_BYTES as u16).to_be_bytes())
        .await?;
    stream.write_all(&database_len.to_be_bytes()).await?;
    stream.write_all(&session_len.to_be_bytes()).await?;
    stream.write_all(credential).await?;
    stream.write_all(database).await?;
    stream.write_all(application_session).await?;
    stream.flush().await
}

/// Reads and structurally checks one bounded connection presentation.
pub async fn read_open(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<DiagnosticOpen> {
    let mut magic = [0_u8; DIAGNOSTIC_MAGIC.len()];
    stream.read_exact(&mut magic).await?;
    if magic != DIAGNOSTIC_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid diagnostic magic",
        ));
    }
    let credential_len = usize::from(stream.read_u16().await?);
    let database_len = usize::from(stream.read_u16().await?);
    let session_len = usize::from(stream.read_u16().await?);
    if credential_len != DIAGNOSTIC_CREDENTIAL_BYTES
        || database_len > MAX_DIAGNOSTIC_DATABASE_BYTES
        || session_len == 0
        || session_len > MAX_DIAGNOSTIC_SESSION_BYTES
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid diagnostic open",
        ));
    }
    let mut credential = vec![0_u8; credential_len];
    let mut database = vec![0_u8; database_len];
    let mut application_session = vec![0_u8; session_len];
    stream.read_exact(&mut credential).await?;
    stream.read_exact(&mut database).await?;
    stream.read_exact(&mut application_session).await?;
    Ok(DiagnosticOpen {
        credential,
        database,
        application_session,
    })
}

/// Writes one checked open result after server-side identity validation.
pub async fn write_opened(
    stream: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
) -> io::Result<()> {
    if payload.is_empty() || payload.len() > MAX_DIAGNOSTIC_SESSION_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid opened identity length",
        ));
    }
    let length = u16::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "opened identity too large"))?;
    stream.write_all(&[DIAGNOSTIC_OPENED]).await?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

/// Reads one checked open result.
pub async fn read_opened(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    if stream.read_u8().await? != DIAGNOSTIC_OPENED {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid opened marker",
        ));
    }
    let length = usize::from(stream.read_u16().await?);
    if length == 0 || length > MAX_DIAGNOSTIC_SESSION_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid opened identity length",
        ));
    }
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

/// Writes one already strict request payload.
pub async fn write_request(
    stream: &mut (impl AsyncWrite + Unpin),
    payload: &[u8],
) -> io::Result<()> {
    if payload.is_empty() || payload.len() > MAX_DIAGNOSTIC_REQUEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid request length",
        ));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "request too large"))?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

/// Reads one bounded request payload.
pub async fn read_request(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let length = usize::try_from(stream.read_u32().await?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request length invalid"))?;
    if length == 0 || length > MAX_DIAGNOSTIC_REQUEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid request length",
        ));
    }
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

/// Encodes one bounded response into a single write buffer.
pub fn encode_response(
    status_code: u8,
    stages: DiagnosticServerStages,
    payload: &[u8],
) -> io::Result<Vec<u8>> {
    if payload.len() > MAX_DIAGNOSTIC_RESPONSE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "response too large",
        ));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "response too large"))?;
    let mut output = Vec::with_capacity(1 + 32 + 4 + payload.len());
    output.push(status_code);
    output.extend_from_slice(&stages.decode_adapt_ns.to_be_bytes());
    output.extend_from_slice(&stages.application_service_ns.to_be_bytes());
    output.extend_from_slice(&stages.encode_ns.to_be_bytes());
    output.extend_from_slice(&stages.previous_write_ns.to_be_bytes());
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(payload);
    Ok(output)
}

/// Reads one bounded response frame.
pub async fn read_response(
    stream: &mut (impl AsyncRead + Unpin),
) -> io::Result<DiagnosticResponse> {
    let status_code = stream.read_u8().await?;
    let stages = DiagnosticServerStages {
        decode_adapt_ns: stream.read_u64().await?,
        application_service_ns: stream.read_u64().await?,
        encode_ns: stream.read_u64().await?,
        previous_write_ns: stream.read_u64().await?,
    };
    let length = usize::try_from(stream.read_u32().await?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "response length invalid"))?;
    if length > MAX_DIAGNOSTIC_RESPONSE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "response too large",
        ));
    }
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await?;
    Ok(DiagnosticResponse {
        status_code,
        stages,
        payload,
    })
}
