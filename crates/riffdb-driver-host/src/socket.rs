use std::fmt;
use std::fs;
use std::future::Future;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;

use crate::{DriverHost, DriverRequest, DriverResponse, FrameCodec};

const MAX_CONNECTIONS: usize = 256;
const RESPONSE_QUEUE: usize = 64;
const MAX_INFLIGHT_PER_CONNECTION: usize = 64;
const IDLE_CONNECTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

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
        request=tokio::time::timeout(IDLE_CONNECTION_TIMEOUT, FrameCodec::read_request(&mut reader))=>request.ok().and_then(Result::ok),
    } {
        Some(request) => request,
        None => {
            drop(responses);
            let _ = writer_task.await;
            return;
        }
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
            request=tokio::time::timeout(IDLE_CONNECTION_TIMEOUT, FrameCodec::read_request(&mut reader))=>match request{Ok(Ok(request))=>request,Ok(Err(_))|Err(_)=>break},
        };
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
