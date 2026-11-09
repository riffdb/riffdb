use std::fs;
use std::future::Future;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::{
    OperatorDriverHost, OperatorDriverRequest, OperatorDriverResponse, OperatorFrameCodec,
};

const MAX_CONNECTIONS: usize = 64;
const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
        crate::socket::checked_socket_path(path).map_err(|_| OperatorSocketError)?;
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

    /// Serves checked same-owner peers in bounded tasks until shutdown, then drains connections.
    pub async fn serve_until<F>(
        self,
        host: OperatorDriverHost,
        shutdown: F,
    ) -> Result<(), OperatorSocketError>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let (drain, _) = watch::channel(false);
        let host = Arc::new(host);
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                accepted = self.listener.accept(), if connections.len() < MAX_CONNECTIONS => {
                    let (stream, _) = accepted.map_err(|_| OperatorSocketError)?;
                    if same_owner(&stream, self.owner_uid) {
                        let host = Arc::clone(&host);
                        let drain = drain.subscribe();
                        connections.spawn(async move {
                            serve_connection(stream, host, drain).await;
                        });
                    }
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
        let _ = drain.send(true);
        if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, async {
            while connections.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
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

async fn serve_connection(
    mut stream: UnixStream,
    host: Arc<OperatorDriverHost>,
    mut drain: watch::Receiver<bool>,
) {
    if *drain.borrow() {
        return;
    }
    let first = tokio::select! {
        changed = drain.changed() => {
            let _ = changed;
            return;
        }
        result = tokio::time::timeout(IDLE_TIMEOUT, OperatorFrameCodec::read_request(&mut stream)) => {
            match result {
                Ok(Ok(request)) => request,
                _ => return,
            }
        }
    };
    let handshake = host.handshake(&first);
    let accepted = matches!(handshake, OperatorDriverResponse::Handshake { .. });
    if !matches!(
        tokio::time::timeout(
            IDLE_TIMEOUT,
            OperatorFrameCodec::write_response(&mut stream, &handshake),
        )
        .await,
        Ok(Ok(()))
    ) {
        return;
    }
    if !accepted {
        return;
    }
    loop {
        if *drain.borrow() {
            return;
        }
        let request = tokio::select! {
            changed = drain.changed() => {
                let _ = changed;
                return;
            }
            result = tokio::time::timeout(IDLE_TIMEOUT, OperatorFrameCodec::read_request(&mut stream)) => {
                match result {
                    Ok(Ok(request)) => request,
                    _ => return,
                }
            }
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
        if !matches!(
            tokio::time::timeout(
                IDLE_TIMEOUT,
                OperatorFrameCodec::write_response(&mut stream, &response),
            )
            .await,
            Ok(Ok(()))
        ) {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use riffdb_application::ApplicationPortabilityManifest;
    use riffdb_client_rust::{
        CallMetadata, CapabilityApplicationReimportScopeV1, RiffDbClient, StartApplicationReimport,
    };
    use riffdb_types::ApplicationInstallationCampaignId;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::sync::oneshot;
    use tonic::transport::Endpoint;

    use super::*;
    use crate::OPERATOR_DRIVER_PROTOCOL_VERSION;

    fn unique_socket_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "riffdb-operator-sock-{}-{nanos}-{seq}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create socket dir");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
        dir
    }

    fn test_host() -> (OperatorDriverHost, OperatorDriverRequest) {
        let manifest = ApplicationPortabilityManifest::decode_canonical(include_bytes!(
            "../../../fixtures/export/openfga/portability-manifest-v2.json"
        ))
        .expect("manifest");
        let campaign =
            ApplicationInstallationCampaignId::from_unix_milliseconds_and_random(1, [7; 10])
                .expect("campaign");
        let manifest_hash: String = manifest
            .identity()
            .as_bytes()
            .iter()
            .flat_map(|byte| {
                [
                    char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'),
                    char::from_digit(u32::from(byte & 15), 16).unwrap_or('0'),
                ]
            })
            .collect();
        let start = StartApplicationReimport::new(
            campaign,
            manifest.input().contract_lineage.clone(),
            CapabilityApplicationReimportScopeV1::WholeApplication,
            manifest,
            b"{\"complete\":true}".to_vec(),
            b"{\"complete\":true}".to_vec(),
        )
        .expect("start");
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let host = OperatorDriverHost::new(
            "restored".to_owned(),
            start,
            RiffDbClient::from_channel(channel),
            CallMetadata::default(),
        );
        let handshake = OperatorDriverRequest::Handshake {
            request_id: "handshake-1".to_owned(),
            protocol_version: OPERATOR_DRIVER_PROTOCOL_VERSION,
            database: "restored".to_owned(),
            campaign_id: campaign.to_string(),
            portability_manifest_hash: manifest_hash,
        };
        (host, handshake)
    }

    async fn exchange(
        stream: &mut UnixStream,
        request: &OperatorDriverRequest,
    ) -> OperatorDriverResponse {
        let body = OperatorFrameCodec::encode_request(request).expect("encode request");
        let length = u32::try_from(body.len()).expect("u32 length");
        stream
            .write_u32(length)
            .await
            .expect("write request length");
        stream.write_all(&body).await.expect("write request body");
        stream.flush().await.expect("flush request");
        let resp_len =
            usize::try_from(stream.read_u32().await.expect("read length")).expect("usize");
        let mut frame = vec![0_u8; resp_len];
        stream.read_exact(&mut frame).await.expect("read frame");
        OperatorFrameCodec::decode_response(&frame).expect("decode response")
    }

    #[tokio::test]
    async fn concurrent_operator_connections_and_shutdown_do_not_stall_on_active_peer() {
        let dir = unique_socket_dir();
        let socket_path = dir.join("operator.sock");
        let socket = OperatorDriverSocket::bind(&socket_path).expect("bind operator socket");
        let (host, handshake) = test_host();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            socket
                .serve_until(host, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        // First client connects and completes handshake, then stays open and active.
        let mut client1 = UnixStream::connect(&socket_path)
            .await
            .expect("connect client 1");
        assert!(matches!(
            exchange(&mut client1, &handshake).await,
            OperatorDriverResponse::Handshake { .. }
        ));

        // Second client connects concurrently while client 1 holds its connection open.
        let mut client2 =
            tokio::time::timeout(Duration::from_secs(2), UnixStream::connect(&socket_path))
                .await
                .expect("connect client 2 did not time out")
                .expect("connect client 2");
        let response2 =
            tokio::time::timeout(Duration::from_secs(2), exchange(&mut client2, &handshake))
                .await
                .expect("client 2 handshake did not block behind client 1");
        assert!(matches!(
            response2,
            OperatorDriverResponse::Handshake { .. }
        ));

        // Trigger shutdown while both clients remain connected; serve_until must
        // propagate drain to connection tasks and exit promptly.
        shutdown_tx.send(()).expect("signal shutdown");
        let serve_result = tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("operator socket shutdown did not block on open connections")
            .expect("server task join");
        assert_eq!(serve_result, Ok(()));
        assert!(!socket_path.exists(), "socket file removed on shutdown");

        let mut eof = [0_u8; 1];
        assert_eq!(
            client1.read(&mut eof).await.expect("client 1 closed"),
            0,
            "connection task closed stream on shutdown"
        );
        let _ = fs::remove_dir_all(&dir);
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
