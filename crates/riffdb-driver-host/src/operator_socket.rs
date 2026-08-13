use std::fs;
use std::future::Future;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use tokio::net::{UnixListener, UnixStream};

use crate::{
    OperatorDriverHost, OperatorDriverRequest, OperatorDriverResponse, OperatorFrameCodec,
};

const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Private single-campaign operator socket.
pub struct OperatorDriverSocket {
    listener: UnixListener,
    path: PathBuf,
    owner_uid: u32,
}

impl OperatorDriverSocket {
    /// Binds one new or safely stale same-owner socket.
    pub fn bind(path: impl AsRef<Path>) -> Result<Self, OperatorSocketError> {
        let path = path.as_ref();
        if !path.is_absolute() || path.as_os_str().as_encoded_bytes().len() > 4_096 {
            return Err(OperatorSocketError);
        }
        let parent = path.parent().ok_or(OperatorSocketError)?;
        let parent_metadata = fs::symlink_metadata(parent).map_err(|_| OperatorSocketError)?;
        if !parent_metadata.is_dir()
            || parent_metadata.file_type().is_symlink()
            || parent_metadata.permissions().mode() & 0o077 != 0
        {
            return Err(OperatorSocketError);
        }
        let owner_uid = parent_metadata.uid();
        if let Ok(existing) = fs::symlink_metadata(path) {
            if !existing.file_type().is_socket()
                || existing.uid() != owner_uid
                || existing.permissions().mode() & 0o077 != 0
                || std::os::unix::net::UnixStream::connect(path).is_ok()
            {
                return Err(OperatorSocketError);
            }
            fs::remove_file(path).map_err(|_| OperatorSocketError)?;
        }
        let listener = UnixListener::bind(path).map_err(|_| OperatorSocketError)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|_| OperatorSocketError)?;
        Ok(Self {
            listener,
            path: path.to_owned(),
            owner_uid,
        })
    }

    /// Serves checked same-owner peers until shutdown.
    pub async fn serve_until<F>(
        self,
        host: OperatorDriverHost,
        shutdown: F,
    ) -> Result<(), OperatorSocketError>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted.map_err(|_| OperatorSocketError)?;
                    if same_owner(&stream, self.owner_uid) {
                        serve_connection(stream, &host).await;
                    }
                }
            }
        }
        let metadata = fs::symlink_metadata(&self.path).map_err(|_| OperatorSocketError)?;
        if metadata.file_type().is_socket() && metadata.uid() == self.owner_uid {
            fs::remove_file(&self.path).map_err(|_| OperatorSocketError)?;
        }
        Ok(())
    }
}

fn same_owner(stream: &UnixStream, owner_uid: u32) -> bool {
    stream.peer_cred().is_ok_and(|peer| peer.uid() == owner_uid)
}

async fn serve_connection(mut stream: UnixStream, host: &OperatorDriverHost) {
    let first =
        match tokio::time::timeout(IDLE_TIMEOUT, OperatorFrameCodec::read_request(&mut stream))
            .await
        {
            Ok(Ok(request)) => request,
            _ => return,
        };
    let handshake = host.handshake(&first);
    let accepted = matches!(handshake, OperatorDriverResponse::Handshake { .. });
    if OperatorFrameCodec::write_response(&mut stream, &handshake)
        .await
        .is_err()
        || !accepted
    {
        return;
    }
    loop {
        let request =
            match tokio::time::timeout(IDLE_TIMEOUT, OperatorFrameCodec::read_request(&mut stream))
                .await
            {
                Ok(Ok(request)) => request,
                _ => return,
            };
        let response = match request {
            OperatorDriverRequest::Handshake { request_id, .. } => OperatorDriverResponse::Error {
                request_id: Some(request_id),
                code: "RDB-OPERATOR-0001".to_owned(),
                category: "operator".to_owned(),
                message: "handshake_already_completed".to_owned(),
                retryability: "not_retryable".to_owned(),
                recovery_action: "reconnect".to_owned(),
                outcome_uncertain: false,
            },
            request => host.invoke(request).await,
        };
        if OperatorFrameCodec::write_response(&mut stream, &response)
            .await
            .is_err()
        {
            return;
        }
    }
}

/// Fixed safe local operator-socket failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperatorSocketError;

impl std::fmt::Display for OperatorSocketError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("operator driver socket failed closed")
    }
}
impl std::error::Error for OperatorSocketError {}
