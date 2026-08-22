use std::fmt;
use std::fs;
use std::future::Future;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;

use crate::protocol::{DRIVER_PROTOCOL_VERSION_V1, DRIVER_PROTOCOL_VERSION_V2};
use crate::{DriverHost, DriverRequest, DriverResponse, FrameCodec};

const MAX_CONNECTIONS: usize = 256;
const RESPONSE_QUEUE: usize = 64;
const MAX_INFLIGHT_PER_CONNECTION: usize = 64;
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Owned protected Unix listener for one driver host.
pub struct DriverSocket {
    listener: UnixListener,
    path: PathBuf,
    owner_uid: u32,
}

impl DriverSocket {
    /// Binds a new or safely recognized stale private socket in an existing
    /// private directory. Live duplicate ownership fails closed.
    pub fn bind(path: impl AsRef<Path>) -> Result<Self, SocketError> {
        let path = path.as_ref();
        if !path.is_absolute() || path.as_os_str().as_encoded_bytes().len() > 4_096 {
            return Err(SocketError::InvalidPath);
        }
        let parent = path.parent().ok_or(SocketError::InvalidPath)?;
        let parent_metadata =
            fs::symlink_metadata(parent).map_err(|_| SocketError::UnsafeDirectory)?;
        if !parent_metadata.is_dir()
            || parent_metadata.file_type().is_symlink()
            || parent_metadata.permissions().mode() & 0o077 != 0
        {
            return Err(SocketError::UnsafeDirectory);
        }
        let owner_uid = parent_metadata.uid();
        if let Ok(existing) = fs::symlink_metadata(path) {
            if !existing.file_type().is_socket()
                || existing.uid() != owner_uid
                || existing.permissions().mode() & 0o077 != 0
            {
                return Err(SocketError::DuplicateOwner);
            }
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                return Err(SocketError::DuplicateOwner);
            }
            fs::remove_file(path).map_err(|_| SocketError::DuplicateOwner)?;
        }
        let listener = UnixListener::bind(path).map_err(|_| SocketError::BindFailed)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|_| SocketError::BindFailed)?;
        Ok(Self {
            listener,
            path: path.to_owned(),
            owner_uid,
        })
    }

    /// Serves checked same-owner peers until shutdown, then drains accepted connections.
    pub async fn serve_until<F>(self, host: DriverHost, shutdown: F) -> Result<(), SocketError>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let (drain, _) = watch::channel(false);
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                accepted = self.listener.accept(), if connections.len() < MAX_CONNECTIONS => {
                    let (stream, _) = accepted.map_err(|_|SocketError::AcceptFailed)?;
                    if same_owner(&stream, self.owner_uid) { let host=host.clone(); let drain=drain.subscribe(); connections.spawn(async move { serve_connection(stream,host,drain).await; }); }
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
        let _ = drain.send(true);
        if tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while connections.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
        let metadata = fs::symlink_metadata(&self.path).map_err(|_| SocketError::CleanupFailed)?;
        if metadata.file_type().is_socket() && metadata.uid() == self.owner_uid {
            fs::remove_file(&self.path).map_err(|_| SocketError::CleanupFailed)?;
        }
        Ok(())
    }
}

fn same_owner(stream: &UnixStream, owner_uid: u32) -> bool {
    stream.peer_cred().is_ok_and(|cred| cred.uid() == owner_uid)
}

async fn serve_connection(stream: UnixStream, host: DriverHost, mut drain: watch::Receiver<bool>) {
    let (mut reader, mut writer) = stream.into_split();
    let (responses, mut outbound) = mpsc::channel::<DriverResponse>(RESPONSE_QUEUE);
    let writer_task = tokio::spawn(async move {
        while let Some(response) = outbound.recv().await {
            if FrameCodec::write_response(&mut writer, &response)
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let first = match tokio::select! {
        changed=drain.changed()=>{let _=changed;None}
        request=tokio::time::timeout(HANDSHAKE_TIMEOUT, FrameCodec::read_request(&mut reader))=>request.ok().and_then(Result::ok),
    } {
        Some(request) => request,
        None => {
            drop(responses);
            let _ = writer_task.await;
            return;
        }
    };
    let protocol_version = match &first {
        DriverRequest::Handshake {
            protocol_version, ..
        } => *protocol_version,
        _ => 0,
    };
    let handshake = host.handshake(&first);
    let accepted = matches!(handshake, DriverResponse::Handshake { .. });
    if responses.send(handshake).await.is_err() || !accepted {
        drop(responses);
        let _ = writer_task.await;
        return;
    }
    let mut operations = JoinSet::new();
    let operation_admission = std::sync::Arc::new(Semaphore::new(MAX_INFLIGHT_PER_CONNECTION));
    loop {
        while operations.try_join_next().is_some() {}
        let request = tokio::select! {
            changed=drain.changed()=>{let _=changed;break;}
            request=FrameCodec::read_request(&mut reader)=>match request{Ok(request)=>request,Err(_)=>break},
        };
        if matches!(
            (protocol_version, &request),
            (
                DRIVER_PROTOCOL_VERSION_V1,
                DriverRequest::Invoke { options, .. }
            ) if options.accept_compact_result || options.accept_packed_result
        ) || matches!(
            (protocol_version, &request),
            (
                DRIVER_PROTOCOL_VERSION_V2,
                DriverRequest::Invoke { options, .. }
            ) if options.accept_packed_result
        ) {
            let response = local_protocol_error(
                &request,
                "this driver protocol cannot negotiate generated result encodings",
            );
            if responses.send(response).await.is_err() {
                break;
            }
            continue;
        }
        match request {
            DriverRequest::Invoke { .. } => {
                let Ok(permit) = operation_admission.clone().try_acquire_owned() else {
                    let response = local_capacity_error(&request);
                    if responses.send(response).await.is_err() {
                        break;
                    }
                    continue;
                };
                let host = host.clone();
                let responses = responses.clone();
                operations.spawn(async move {
                    let _permit = permit;
                    let response = host.invoke(request).await;
                    let _ = responses.send(response).await;
                });
            }
            DriverRequest::Batch { .. } => {
                let Ok(permit) = operation_admission.clone().try_acquire_owned() else {
                    let response = local_capacity_error(&request);
                    if responses.send(response).await.is_err() {
                        break;
                    }
                    continue;
                };
                let host = host.clone();
                let responses = responses.clone();
                operations.spawn(async move {
                    let _permit = permit;
                    let response = host.batch(request).await;
                    let _ = responses.send(response).await;
                });
            }
            DriverRequest::Cancel {
                request_id,
                target_request_id,
            } => {
                let response = host.cancel(request_id, target_request_id).await;
                if responses.send(response).await.is_err() {
                    break;
                }
            }
            DriverRequest::Handshake { request_id, .. } => {
                let response = DriverResponse::Error {
                    request_id: Some(request_id),
                    code: "RDB-DRIVER-0001".to_owned(),
                    category: "protocol".to_owned(),
                    operation: None,
                    symbol_path: Vec::new(),
                    contract_lineage: None,
                    contract_version: None,
                    trace_id: None,
                    incident_id: None,
                    message: "driver handshake was already completed".to_owned(),
                    retryability: "not_retryable".to_owned(),
                    recovery_action: "reconnect".to_owned(),
                    outcome_uncertain: false,
                };
                if responses.send(response).await.is_err() {
                    break;
                }
            }
        }
    }
    if tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while operations.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        operations.abort_all();
        while operations.join_next().await.is_some() {}
    }
    drop(responses);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), writer_task).await;
}

fn local_capacity_error(request: &DriverRequest) -> DriverResponse {
    let (request_id, operation) = match request {
        DriverRequest::Invoke {
            request_id,
            operation,
            ..
        }
        | DriverRequest::Batch {
            request_id,
            operation,
            ..
        } => (Some(request_id.clone()), Some(operation.clone())),
        DriverRequest::Handshake { .. } | DriverRequest::Cancel { .. } => (None, None),
    };
    DriverResponse::Error {
        request_id,
        code: "RDB-CAPACITY-0101".to_owned(),
        category: "capacity".to_owned(),
        operation,
        symbol_path: Vec::new(),
        contract_lineage: None,
        contract_version: None,
        trace_id: None,
        incident_id: None,
        message: "driver host is over capacity".to_owned(),
        retryability: "retryable".to_owned(),
        recovery_action: "retry_later".to_owned(),
        outcome_uncertain: false,
    }
}

fn local_protocol_error(request: &DriverRequest, message: &'static str) -> DriverResponse {
    let (request_id, operation) = match request {
        DriverRequest::Invoke {
            request_id,
            operation,
            ..
        }
        | DriverRequest::Batch {
            request_id,
            operation,
            ..
        } => (Some(request_id.clone()), Some(operation.clone())),
        DriverRequest::Handshake { .. } | DriverRequest::Cancel { .. } => (None, None),
    };
    DriverResponse::Error {
        request_id,
        code: "RDB-DRIVER-0001".to_owned(),
        category: "protocol".to_owned(),
        operation,
        symbol_path: Vec::new(),
        contract_lineage: None,
        contract_version: None,
        trace_id: None,
        incident_id: None,
        message: message.to_owned(),
        retryability: "not_retryable".to_owned(),
        recovery_action: "upgrade_driver".to_owned(),
        outcome_uncertain: false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use riffdb_client_rust::{CallMetadata, StableApplicationClient};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::UnixStream;
    use tokio::sync::watch;
    use tonic::transport::Endpoint;

    use super::serve_connection;
    use crate::{
        ApplicationCatalog, DRIVER_ERROR_REGISTRY_HASH, DRIVER_PROTOCOL_VERSION,
        DRIVER_VALUE_REGISTRY_HASH, DriverHost, DriverPool, DriverRequest, DriverResponse,
        FrameCodec,
    };

    fn host_and_handshake() -> (DriverHost, DriverRequest) {
        let catalog = ApplicationCatalog::from_exact_artifacts(
            include_bytes!("../../../examples/agent-alpha/riffdb.application.lock.json"),
            include_bytes!("../../../examples/agent-alpha/generated/riffdb.application.exact.json"),
            include_bytes!("../../../examples/agent-alpha/generated/mcp/tools.json"),
            "default",
            "AgentAlphaApplication",
        )
        .expect("catalog");
        let remote_identity_hash = "00".repeat(32);
        let handshake = DriverRequest::Handshake {
            request_id: "quiet-session-handshake".to_owned(),
            protocol_version: DRIVER_PROTOCOL_VERSION,
            application_manifest_hash: catalog.application_manifest_hash(),
            operation_catalog_hash: catalog.catalog_hash(),
            contract_lineage: catalog.contract_lineage().to_owned(),
            contract_version: catalog.contract_version(),
            contract_bundle_hash: catalog.contract_bundle_hash_hex(),
            value_registry_hash: DRIVER_VALUE_REGISTRY_HASH.to_owned(),
            error_registry_hash: DRIVER_ERROR_REGISTRY_HASH.to_owned(),
            database: catalog.database().to_owned(),
            role: catalog.role().to_owned(),
            role_definition_hash: catalog.role_definition_hash(),
            remote_identity_hash: remote_identity_hash.clone(),
        };
        let channel = Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
        let pool =
            DriverPool::from_clients(vec![StableApplicationClient::from_channel(channel)], 1)
                .expect("pool");
        let host = DriverHost::new(catalog, pool, CallMetadata::default(), remote_identity_hash)
            .expect("host");
        (host, handshake)
    }

    async fn exchange(stream: &mut UnixStream, request: &DriverRequest) -> DriverResponse {
        stream
            .write_all(&FrameCodec::encode_request(request).expect("request frame"))
            .await
            .expect("write request");
        let mut prefix = [0_u8; 4];
        stream
            .read_exact(&mut prefix)
            .await
            .expect("read response prefix");
        let length = usize::try_from(u32::from_be_bytes(prefix)).expect("frame length");
        let mut body = vec![0_u8; length];
        stream
            .read_exact(&mut body)
            .await
            .expect("read response body");
        let mut frame = prefix.to_vec();
        frame.extend_from_slice(&body);
        FrameCodec::decode_response(&frame).expect("response frame")
    }

    #[tokio::test(start_paused = true)]
    async fn established_session_survives_more_than_the_handshake_timeout_without_traffic() {
        let (host, handshake) = host_and_handshake();
        let (mut client, server) = UnixStream::pair().expect("local stream pair");
        let (drain, receiver) = watch::channel(false);
        let serving = tokio::spawn(serve_connection(server, host, receiver));

        assert!(matches!(
            exchange(&mut client, &handshake).await,
            DriverResponse::Handshake { .. }
        ));
        tokio::time::advance(Duration::from_secs(301)).await;

        let duplicate = match handshake {
            DriverRequest::Handshake {
                protocol_version,
                application_manifest_hash,
                operation_catalog_hash,
                contract_lineage,
                contract_version,
                contract_bundle_hash,
                value_registry_hash,
                error_registry_hash,
                database,
                role,
                role_definition_hash,
                remote_identity_hash,
                ..
            } => DriverRequest::Handshake {
                request_id: "quiet-session-still-attached".to_owned(),
                protocol_version,
                application_manifest_hash,
                operation_catalog_hash,
                contract_lineage,
                contract_version,
                contract_bundle_hash,
                value_registry_hash,
                error_registry_hash,
                database,
                role,
                role_definition_hash,
                remote_identity_hash,
            },
            _ => unreachable!("fixture is a handshake"),
        };
        assert!(matches!(
            exchange(&mut client, &duplicate).await,
            DriverResponse::Error { code, .. } if code == "RDB-DRIVER-0001"
        ));

        drain.send(true).expect("request drain");
        serving.await.expect("connection task");
    }
}

/// Closed safe local socket failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketError {
    /// Socket path is not bounded, absolute, and normalized.
    InvalidPath,
    /// Parent directory is not an existing private real directory.
    UnsafeDirectory,
    /// A live owner or unsafe filesystem object already occupies the path.
    DuplicateOwner,
    /// The private socket could not be created.
    BindFailed,
    /// The listener failed while accepting a local peer.
    AcceptFailed,
    /// The owned socket could not be safely removed during shutdown.
    CleanupFailed,
}
impl fmt::Display for SocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidPath => "driver socket path is invalid",
            Self::UnsafeDirectory => "driver socket directory is not private",
            Self::DuplicateOwner => "driver socket already has an owner",
            Self::BindFailed => "driver socket could not bind",
            Self::AcceptFailed => "driver socket could not accept",
            Self::CleanupFailed => "driver socket could not clean up",
        })
    }
}
impl std::error::Error for SocketError {}
