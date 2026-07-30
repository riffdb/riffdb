//! First-party bounded newline-delimited JSON transport for MCP stdio.

use std::fmt;
use std::io::{self, Write as _};
use std::pin::Pin;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use rmcp::model::{
    ClientJsonRpcMessage, ClientRequest, ErrorData, RequestId, ResourceUpdatedNotificationParam,
    ServerJsonRpcMessage,
};
use rmcp::service::{Peer, RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::transport::Transport;
use rmcp::{RoleServer, serve_server};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Number, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex as AsyncMutex, Notify, watch};
#[cfg(test)]
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use crate::McpObserverResourceUpdate;
use crate::{
    MCP_INBOUND_MESSAGE_MAX_BYTES, MCP_OUTBOUND_MESSAGE_MAX_BYTES, MCP_PROTOCOL_VERSION,
    McpBackend, McpCancellationRegistry, McpObserverBackend, McpObserverLoop, McpObserverLoopError,
    McpObserverNotification, McpObserverNotificationReceiver, McpObserverNotificationSink,
    McpObserverSemaphore, McpObserverState, McpObserverTickResult, RiffDbMcpServer,
    SystemMcpObserverScheduler, drive_mcp_observer, mcp_observer_notification_channel,
};

const UNSUPPORTED_PROTOCOL_SENTINEL: &str = "riffdb-unsupported";
const MCP_STDIO_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const MCP_STDIO_SESSION_LIFETIME: Duration = Duration::from_secs(900);

/// A closed, redaction-safe stdio transport failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpStdioTransportError;

impl fmt::Display for McpStdioTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP stdio transport failed")
    }
}

impl std::error::Error for McpStdioTransportError {}

/// A closed, redaction-safe stdio service failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpStdioServeError;

impl fmt::Display for McpStdioServeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP stdio service failed")
    }
}

impl std::error::Error for McpStdioServeError {}

/// Shared proof of authenticated client activity for one stdio session.
///
/// Raw input, protocol handling, observer work, and output cannot update this
/// value. The ordinary public-client backend updates it only after a gRPC call
/// has returned an authenticated public result.
#[derive(Clone)]
pub struct McpStdioClientActivity {
    clock: Arc<dyn McpStdioLifecycleClock>,
    timeline: watch::Sender<McpStdioLifecycleTimeline>,
}

impl McpStdioClientActivity {
    /// Creates an activity clock that has not yet reached initialization.
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemMcpStdioLifecycleClock::new()))
    }

    /// Records completion of one authenticated client-originated public call.
    ///
    /// Calls before successful MCP initialization are deliberately ignored.
    /// Returns `false` when the session already expired or the injected
    /// monotonic clock failed.
    #[must_use]
    pub fn authenticated_request_completed(&self) -> bool {
        self.authenticated_request_completed_at(self.clock.now())
    }

    fn initialized(&self) {
        self.initialized_at(self.clock.now());
    }

    fn with_clock(clock: Arc<dyn McpStdioLifecycleClock>) -> Self {
        let (timeline, _) = watch::channel(McpStdioLifecycleTimeline::default());
        Self { clock, timeline }
    }

    fn initialized_at(&self, now: Duration) {
        self.timeline.send_if_modified(|timeline| {
            if timeline.initialized_at.is_some() {
                false
            } else {
                timeline.initialized_at = Some(now);
                timeline.last_authenticated_at = Some(now);
                true
            }
        });
    }

    fn authenticated_request_completed_at(&self, now: Duration) -> bool {
        let mut accepted = true;
        self.timeline.send_if_modified(|timeline| {
            if timeline.expired || timeline.clock_fault {
                accepted = false;
                return false;
            }
            let Some(last_authenticated) = timeline.last_authenticated_at else {
                return false;
            };
            if now < last_authenticated {
                accepted = false;
                timeline.clock_fault = true;
                return true;
            }
            match timeline.expired_at(now) {
                Ok(true) => {
                    accepted = false;
                    timeline.expired = true;
                    true
                }
                Err(_) => {
                    accepted = false;
                    timeline.clock_fault = true;
                    true
                }
                Ok(false) if now > last_authenticated => {
                    timeline.last_authenticated_at = Some(now);
                    true
                }
                Ok(false) => false,
            }
        });
        accepted
    }

    fn expire_if_due(&self, now: Duration) -> Result<bool, McpStdioServeError> {
        let mut result = Ok(false);
        self.timeline.send_if_modified(|timeline| {
            if timeline.clock_fault {
                result = Err(McpStdioServeError);
                return false;
            }
            if timeline.expired {
                result = Ok(true);
                return false;
            }
            match timeline.expired_at(now) {
                Ok(true) => {
                    timeline.expired = true;
                    result = Ok(true);
                    true
                }
                Ok(false) => false,
                Err(error) => {
                    timeline.clock_fault = true;
                    result = Err(error);
                    true
                }
            }
        });
        result
    }

    fn fail_closed(&self) {
        self.timeline.send_if_modified(|timeline| {
            if timeline.clock_fault {
                false
            } else {
                timeline.clock_fault = true;
                true
            }
        });
    }

    fn is_open_at_current_time(&self) -> Result<bool, McpStdioServeError> {
        self.expire_if_due(self.clock.now()).map(|expired| !expired)
    }

    async fn wait_until_closed(&self) {
        let mut timeline = self.timeline.subscribe();
        loop {
            let current = *timeline.borrow_and_update();
            if current.expired || current.clock_fault {
                return;
            }
            if timeline.changed().await.is_err() {
                return;
            }
        }
    }

    fn lifecycle_scheduler(&self) -> SystemMcpStdioLifecycleScheduler {
        SystemMcpStdioLifecycleScheduler {
            clock: Arc::clone(&self.clock),
        }
    }
}

impl Default for McpStdioClientActivity {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for McpStdioClientActivity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpStdioClientActivity")
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct McpStdioLifecycleTimeline {
    initialized_at: Option<Duration>,
    last_authenticated_at: Option<Duration>,
    expired: bool,
    clock_fault: bool,
}

impl McpStdioLifecycleTimeline {
    fn next_deadline(self) -> Result<Option<Duration>, McpStdioServeError> {
        if self.clock_fault {
            return Err(McpStdioServeError);
        }
        let (initialized_at, last_authenticated_at) =
            match (self.initialized_at, self.last_authenticated_at) {
                (None, None) => return Ok(None),
                (Some(initialized_at), Some(last_authenticated_at)) => {
                    (initialized_at, last_authenticated_at)
                }
                (None, Some(_)) | (Some(_), None) => return Err(McpStdioServeError),
            };
        let idle_deadline = last_authenticated_at
            .checked_add(MCP_STDIO_IDLE_TIMEOUT)
            .ok_or(McpStdioServeError)?;
        let lifetime_deadline = initialized_at
            .checked_add(MCP_STDIO_SESSION_LIFETIME)
            .ok_or(McpStdioServeError)?;
        Ok(Some(idle_deadline.min(lifetime_deadline)))
    }

    fn expired_at(self, now: Duration) -> Result<bool, McpStdioServeError> {
        if self
            .initialized_at
            .is_some_and(|initialized| now < initialized)
            || self
                .last_authenticated_at
                .is_some_and(|last_authenticated| now < last_authenticated)
        {
            return Err(McpStdioServeError);
        }
        self.next_deadline()
            .map(|deadline| deadline.is_some_and(|deadline| now >= deadline))
    }
}

trait McpStdioLifecycleClock: Send + Sync + 'static {
    fn now(&self) -> Duration;
}

struct SystemMcpStdioLifecycleClock {
    origin: tokio::time::Instant,
}

impl SystemMcpStdioLifecycleClock {
    fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}

impl McpStdioLifecycleClock for SystemMcpStdioLifecycleClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

type McpStdioLifecycleSchedulerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Duration, McpStdioServeError>> + Send + 'a>>;

trait McpStdioLifecycleScheduler: Send + 'static {
    fn wait_until(&mut self, deadline: Duration) -> McpStdioLifecycleSchedulerFuture<'_>;
}

struct SystemMcpStdioLifecycleScheduler {
    clock: Arc<dyn McpStdioLifecycleClock>,
}

impl McpStdioLifecycleScheduler for SystemMcpStdioLifecycleScheduler {
    fn wait_until(&mut self, deadline: Duration) -> McpStdioLifecycleSchedulerFuture<'_> {
        let delay = deadline.saturating_sub(self.clock.now());
        let clock = Arc::clone(&self.clock);
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(clock.now())
        })
    }
}

async fn drive_stdio_lifecycle<Scheduler>(
    activity: &McpStdioClientActivity,
    scheduler: &mut Scheduler,
) -> Result<(), McpStdioServeError>
where
    Scheduler: McpStdioLifecycleScheduler,
{
    let mut timeline = activity.timeline.subscribe();
    let mut last_sample = None;
    loop {
        let current = *timeline.borrow_and_update();
        if current.expired {
            return Ok(());
        }
        let Some(deadline) = current.next_deadline()? else {
            timeline.changed().await.map_err(|_| McpStdioServeError)?;
            continue;
        };
        tokio::select! {
            changed = timeline.changed() => {
                changed.map_err(|_| McpStdioServeError)?;
            }
            sample = scheduler.wait_until(deadline) => {
                let sample = sample?;
                if last_sample.is_some_and(|last| sample < last) {
                    return Err(McpStdioServeError);
                }
                last_sample = Some(sample);
                if activity.expire_if_due(sample)? {
                    return Ok(());
                }
            }
        }
    }
}

trait McpStdioSessionCancellation: Send + 'static {
    fn cancel(self);
}

impl McpStdioSessionCancellation for rmcp::service::RunningServiceCancellationToken {
    fn cancel(self) {
        rmcp::service::RunningServiceCancellationToken::cancel(self);
    }
}

#[cfg(test)]
impl McpStdioSessionCancellation for CancellationToken {
    fn cancel(self) {
        CancellationToken::cancel(&self);
    }
}

async fn enforce_stdio_lifecycle<Scheduler, Cancellation>(
    activity: McpStdioClientActivity,
    mut scheduler: Scheduler,
    cancellation: Cancellation,
    request_cancellations: Arc<McpCancellationRegistry>,
    status: TransportStatus,
) where
    Scheduler: McpStdioLifecycleScheduler,
    Cancellation: McpStdioSessionCancellation,
{
    if drive_stdio_lifecycle(&activity, &mut scheduler)
        .await
        .is_err()
    {
        activity.fail_closed();
        status.fail();
    }
    request_cancellations.cancel_all();
    cancellation.cancel();
}

#[derive(Clone, Default)]
struct TransportStatus {
    failed: Arc<AtomicBool>,
}

impl TransportStatus {
    fn fail(&self) {
        self.failed.store(true, Ordering::Release);
    }

    fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
}

#[derive(Default)]
struct InitializationState {
    pending_request: Option<RequestId>,
    initialized: bool,
}

impl InitializationState {
    fn begin(&mut self, request_id: RequestId) -> Result<(), McpStdioTransportError> {
        if self.initialized || self.pending_request.is_some() {
            return Err(McpStdioTransportError);
        }
        self.pending_request = Some(request_id);
        Ok(())
    }

    fn finish(&mut self, request_id: &RequestId, succeeded: bool) -> bool {
        if self.pending_request.as_ref() == Some(request_id) {
            self.pending_request = None;
            self.initialized = succeeded;
            return succeeded;
        }
        false
    }
}

#[derive(Clone)]
struct InitializationGate {
    state: Arc<StdMutex<InitializationState>>,
    initialized: Arc<Notify>,
}

impl InitializationGate {
    async fn wait(&self) {
        loop {
            if self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .initialized
            {
                return;
            }
            self.initialized.notified().await;
        }
    }
}

/// Serves one MCP stdio session over the first-party bounded transport.
///
/// This function is the only rmcp-facing entry point needed by the production
/// stdio process. Both supplied backends remain responsible for ordinary
/// public gRPC and contain no server-side authority. The observer backend must
/// reauthenticate and create a fresh request identity for every call.
///
/// `client_activity` must be the exact handle used by the client-request
/// backend. The observer backend must not receive it: observer reads,
/// notifications, and output never refresh the idle deadline.
pub async fn serve_mcp_stdio<B, ObserverBackend>(
    backend: B,
    observer_backend: ObserverBackend,
    client_activity: McpStdioClientActivity,
) -> Result<(), McpStdioServeError>
where
    B: McpBackend,
    ObserverBackend: McpObserverBackend,
{
    let status = TransportStatus::default();
    let transport = BoundedStdioTransport::with_status_and_activity(
        tokio::io::stdin(),
        tokio::io::stdout(),
        status.clone(),
        client_activity.clone(),
    );
    let initialization = transport.initialization_gate();
    let server = RiffDbMcpServer::new_stdio(backend);
    let request_cancellations = Arc::clone(server.cancellations());
    let observer_state = Arc::clone(server.observer());
    let (notification_sender, notification_receiver) =
        mcp_observer_notification_channel(&observer_state).map_err(|_| McpStdioServeError)?;
    let running = serve_server(server, transport)
        .await
        .map_err(|_| McpStdioServeError)?;
    let notification_peer = running.peer().clone();
    let notification_cancellation = running.cancellation_token();
    let notification_worker = tokio::spawn(async move {
        if forward_observer_notifications(notification_peer, notification_receiver)
            .await
            .is_err()
        {
            notification_cancellation.cancel();
        }
    });
    let observer_cancellation = running.cancellation_token();
    let lifecycle_state = Arc::clone(&observer_state);
    let observer_lifecycle = tokio::spawn(async move {
        initialization.wait().await;
        let result = run_stdio_observer(
            Arc::new(observer_backend),
            Arc::clone(&lifecycle_state),
            Arc::new(notification_sender),
        )
        .await;
        lifecycle_state.cancel();
        if result.is_err() || matches!(result, Ok(McpObserverTickResult::Expired)) {
            observer_cancellation.cancel();
        }
    });
    let lifecycle_cancellation = running.cancellation_token();
    let lifecycle_status = status.clone();
    let lifecycle_scheduler = client_activity.lifecycle_scheduler();
    let lifecycle_request_cancellations = Arc::clone(&request_cancellations);
    let session_lifecycle = tokio::spawn(async move {
        enforce_stdio_lifecycle(
            client_activity,
            lifecycle_scheduler,
            lifecycle_cancellation,
            lifecycle_request_cancellations,
            lifecycle_status,
        )
        .await;
    });

    let quit = running.waiting().await;
    request_cancellations.cancel_all();
    observer_state.cancel();
    observer_lifecycle.abort();
    notification_worker.abort();
    session_lifecycle.abort();
    let _ = observer_lifecycle.await;
    let _ = notification_worker.await;
    let _ = session_lifecycle.await;
    let quit = quit.map_err(|_| McpStdioServeError)?;
    if status.failed() || matches!(quit, rmcp::service::QuitReason::JoinError(_)) {
        return Err(McpStdioServeError);
    }
    Ok(())
}

/// Serves one credential-less local builder handler over the same bounded
/// first-party stdio transport without constructing an observer or backend.
pub async fn serve_builder_mcp_stdio<Server>(server: Server) -> Result<(), McpStdioServeError>
where
    Server: rmcp::handler::server::ServerHandler,
{
    let status = TransportStatus::default();
    let activity = McpStdioClientActivity::new();
    let transport = BoundedStdioTransport::with_status_and_activity(
        tokio::io::stdin(),
        tokio::io::stdout(),
        status.clone(),
        activity,
    );
    let running = serve_server(server, transport)
        .await
        .map_err(|_| McpStdioServeError)?;
    let quit = running.waiting().await.map_err(|_| McpStdioServeError)?;
    if status.failed() || matches!(quit, rmcp::service::QuitReason::JoinError(_)) {
        return Err(McpStdioServeError);
    }
    Ok(())
}

async fn run_stdio_observer<Backend>(
    backend: Arc<Backend>,
    state: Arc<McpObserverState>,
    notification_sink: Arc<dyn McpObserverNotificationSink>,
) -> Result<McpObserverTickResult, McpObserverLoopError>
where
    Backend: McpObserverBackend + ?Sized,
{
    let mut observer = McpObserverLoop::new(
        backend,
        state,
        Arc::new(McpObserverSemaphore::new()),
        notification_sink,
        Duration::ZERO,
    )?;
    let mut scheduler = SystemMcpObserverScheduler::new()?;
    drive_mcp_observer(&mut observer, &mut scheduler).await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct McpNotificationPeerError;

trait McpNotificationPeer: Send + Sync + 'static {
    fn notify<'a>(
        &'a self,
        notification: McpObserverNotification,
    ) -> Pin<Box<dyn Future<Output = Result<(), McpNotificationPeerError>> + Send + 'a>>;
}

impl McpNotificationPeer for Peer<RoleServer> {
    fn notify<'a>(
        &'a self,
        notification: McpObserverNotification,
    ) -> Pin<Box<dyn Future<Output = Result<(), McpNotificationPeerError>> + Send + 'a>> {
        Box::pin(async move {
            match notification {
                McpObserverNotification::ToolsListChanged => self.notify_tool_list_changed().await,
                McpObserverNotification::ResourcesListChanged => {
                    self.notify_resource_list_changed().await
                }
                McpObserverNotification::ResourceUpdated(update) => {
                    self.notify_resource_updated(ResourceUpdatedNotificationParam::new(
                        update.into_uri(),
                    ))
                    .await
                }
            }
            .map_err(|_| McpNotificationPeerError)
        })
    }
}

async fn forward_observer_notifications<NotificationPeer>(
    peer: NotificationPeer,
    mut receiver: McpObserverNotificationReceiver,
) -> Result<(), McpNotificationPeerError>
where
    NotificationPeer: McpNotificationPeer,
{
    while let Some(notification) = receiver.recv().await {
        peer.notify(notification).await?;
    }
    Ok(())
}

struct BoundedStdioTransport<R, W> {
    read: BufReader<R>,
    inbound_frame: Vec<u8>,
    write: Arc<AsyncMutex<Option<W>>>,
    status: TransportStatus,
    initialization: Arc<StdMutex<InitializationState>>,
    initialized: Arc<Notify>,
    client_activity: McpStdioClientActivity,
}

impl<R, W> BoundedStdioTransport<R, W>
where
    R: AsyncRead + Send + Unpin,
    W: AsyncWrite + Send + Unpin + 'static,
{
    #[cfg(test)]
    fn new(read: R, write: W) -> Self {
        Self::with_status(read, write, TransportStatus::default())
    }

    #[cfg(test)]
    fn with_status(read: R, write: W, status: TransportStatus) -> Self {
        Self::with_status_and_activity(read, write, status, McpStdioClientActivity::new())
    }

    fn with_status_and_activity(
        read: R,
        write: W,
        status: TransportStatus,
        client_activity: McpStdioClientActivity,
    ) -> Self {
        Self {
            read: BufReader::new(read),
            inbound_frame: Vec::with_capacity(MCP_INBOUND_MESSAGE_MAX_BYTES + 1),
            write: Arc::new(AsyncMutex::new(Some(write))),
            status,
            initialization: Arc::new(StdMutex::new(InitializationState::default())),
            initialized: Arc::new(Notify::new()),
            client_activity,
        }
    }

    fn initialization_gate(&self) -> InitializationGate {
        InitializationGate {
            state: Arc::clone(&self.initialization),
            initialized: Arc::clone(&self.initialized),
        }
    }

    fn session_is_open(&self) -> bool {
        match self.client_activity.is_open_at_current_time() {
            Ok(open) => open,
            Err(_) => {
                self.status.fail();
                false
            }
        }
    }

    async fn read_frame(&mut self) -> Result<Option<Vec<u8>>, McpStdioTransportError> {
        loop {
            let remaining_probe = MCP_INBOUND_MESSAGE_MAX_BYTES
                .checked_add(1)
                .and_then(|probe| probe.checked_sub(self.inbound_frame.len()))
                .ok_or(McpStdioTransportError)?;
            if remaining_probe == 0 {
                self.status.fail();
                return Err(McpStdioTransportError);
            }

            let read = (&mut self.read)
                .take(remaining_probe as u64)
                .read_until(b'\n', &mut self.inbound_frame)
                .await
                .map_err(|_| {
                    self.status.fail();
                    McpStdioTransportError
                })?;
            if read == 0 {
                if self.inbound_frame.is_empty() {
                    return Ok(None);
                }
                self.status.fail();
                return Err(McpStdioTransportError);
            }

            if self.inbound_frame.len() > MCP_INBOUND_MESSAGE_MAX_BYTES {
                self.status.fail();
                return Err(McpStdioTransportError);
            }
            if !self.inbound_frame.ends_with(b"\n") {
                continue;
            }

            let mut frame = std::mem::take(&mut self.inbound_frame);
            frame.pop();
            if frame.ends_with(b"\r") {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }

    fn send_invalid_request(
        &self,
    ) -> impl Future<Output = Result<(), McpStdioTransportError>> + Send + 'static {
        let response =
            ServerJsonRpcMessage::error(ErrorData::invalid_request("Invalid request", None), None);
        send_checked_for_session(
            self.write.clone(),
            response,
            self.status.clone(),
            self.client_activity.clone(),
        )
    }

    fn send_parse_error(
        &self,
    ) -> impl Future<Output = Result<(), McpStdioTransportError>> + Send + 'static {
        let response =
            ServerJsonRpcMessage::error(ErrorData::parse_error("Parse error", None), None);
        send_checked_for_session(
            self.write.clone(),
            response,
            self.status.clone(),
            self.client_activity.clone(),
        )
    }

    fn parse_frame(
        frame: &[u8],
        pre_initialization: bool,
    ) -> Result<Option<ClientJsonRpcMessage>, IncomingMessageFailure> {
        let strict =
            serde_json::from_slice::<StrictJsonValue>(frame).map_err(|error| {
                match error.classify() {
                    serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
                        IncomingMessageFailure::Unparseable
                    }
                    serde_json::error::Category::Data | serde_json::error::Category::Io => {
                        IncomingMessageFailure::InvalidRequest
                    }
                }
            })?;
        let root = strict
            .0
            .as_object()
            .ok_or(IncomingMessageFailure::InvalidRequest)?;
        let mut message: ClientJsonRpcMessage = serde_json::from_value(strict.0.clone())
            .map_err(|_| IncomingMessageFailure::InvalidRequest)?;
        if pre_initialization {
            prepare_initialization_message(root, &mut message)?;
        }
        let encoded = serialize_bounded(&message, MCP_INBOUND_MESSAGE_MAX_BYTES, false)
            .map_err(|_| IncomingMessageFailure::InvalidRequest)?;
        if encoded.is_empty() {
            return Err(IncomingMessageFailure::InvalidRequest);
        }
        Ok(Some(message))
    }
}

impl<R, W> Transport<RoleServer> for BoundedStdioTransport<R, W>
where
    R: AsyncRead + Send + Unpin,
    W: AsyncWrite + Send + Unpin + 'static,
{
    type Error = McpStdioTransportError;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleServer>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let initialization_completion = match &item {
            ServerJsonRpcMessage::Response(response)
                if matches!(
                    &response.result,
                    rmcp::model::ServerResult::InitializeResult(_)
                ) =>
            {
                Some((response.id.clone(), true))
            }
            ServerJsonRpcMessage::Response(_) => None,
            ServerJsonRpcMessage::Error(error) => error
                .id
                .as_ref()
                .map(|request_id| (request_id.clone(), false)),
            ServerJsonRpcMessage::Request(_) | ServerJsonRpcMessage::Notification(_) => None,
        };
        let write = Arc::clone(&self.write);
        let status = self.status.clone();
        let initialization = Arc::clone(&self.initialization);
        let initialized = Arc::clone(&self.initialized);
        let client_activity = self.client_activity.clone();
        async move {
            send_checked_for_session(write, item, status, client_activity.clone()).await?;
            if let Some((request_id, succeeded)) = initialization_completion {
                let became_initialized = initialization
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .finish(&request_id, succeeded);
                if became_initialized {
                    client_activity.initialized();
                    initialized.notify_one();
                }
            }
            Ok(())
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleServer>> {
        loop {
            let mut frame = match self.read_frame().await {
                Ok(Some(frame)) => frame,
                Ok(None) | Err(_) => return None,
            };
            if !self.session_is_open() {
                return None;
            }
            let pre_initialization = !self
                .initialization
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .initialized;
            let parsed = Self::parse_frame(&frame, pre_initialization);
            frame.clear();
            self.inbound_frame = frame;
            match parsed {
                Ok(Some(message)) => {
                    if let ClientJsonRpcMessage::Request(request) = &message
                        && matches!(&request.request, ClientRequest::InitializeRequest(_))
                        && pre_initialization
                        && self
                            .initialization
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .begin(request.id.clone())
                            .is_err()
                    {
                        self.status.fail();
                        return None;
                    }
                    return Some(message);
                }
                Ok(None) => continue,
                Err(IncomingMessageFailure::Unparseable) => {
                    if self.send_parse_error().await.is_err() {
                        return None;
                    }
                }
                Err(IncomingMessageFailure::InvalidRequest) => {
                    if self.send_invalid_request().await.is_err() {
                        return None;
                    }
                }
                Err(IncomingMessageFailure::InvalidInitialization) => {
                    self.status.fail();
                    return None;
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        let mut slot = self.write.lock().await;
        if let Some(mut write) = slot.take() {
            write.shutdown().await.map_err(|_| {
                self.status.fail();
                McpStdioTransportError
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
async fn send_checked<W, T>(
    write: Arc<AsyncMutex<Option<W>>>,
    item: T,
    status: TransportStatus,
) -> Result<(), McpStdioTransportError>
where
    W: AsyncWrite + Send + Unpin + 'static,
    T: Serialize,
{
    send_checked_inner(write, item, status, None).await
}

async fn send_checked_for_session<W, T>(
    write: Arc<AsyncMutex<Option<W>>>,
    item: T,
    status: TransportStatus,
    activity: McpStdioClientActivity,
) -> Result<(), McpStdioTransportError>
where
    W: AsyncWrite + Send + Unpin + 'static,
    T: Serialize,
{
    send_checked_inner(write, item, status, Some(activity)).await
}

async fn send_checked_inner<W, T>(
    write: Arc<AsyncMutex<Option<W>>>,
    item: T,
    status: TransportStatus,
    activity: Option<McpStdioClientActivity>,
) -> Result<(), McpStdioTransportError>
where
    W: AsyncWrite + Send + Unpin + 'static,
    T: Serialize,
{
    let frame = serialize_bounded(&item, MCP_OUTBOUND_MESSAGE_MAX_BYTES, true).map_err(|_| {
        status.fail();
        McpStdioTransportError
    })?;
    ensure_session_output_open(activity.as_ref(), &status)?;
    let mut slot = write.lock().await;
    ensure_session_output_open(activity.as_ref(), &status)?;
    let output = slot.as_mut().ok_or_else(|| {
        status.fail();
        McpStdioTransportError
    })?;
    if let Some(activity) = activity.as_ref() {
        tokio::select! {
            biased;
            () = activity.wait_until_closed() => return Err(McpStdioTransportError),
            result = output.write_all(&frame) => result.map_err(|_| {
                status.fail();
                McpStdioTransportError
            })?,
        }
        tokio::select! {
            biased;
            () = activity.wait_until_closed() => Err(McpStdioTransportError),
            result = output.flush() => result.map_err(|_| {
                status.fail();
                McpStdioTransportError
            }),
        }
    } else {
        output.write_all(&frame).await.map_err(|_| {
            status.fail();
            McpStdioTransportError
        })?;
        output.flush().await.map_err(|_| {
            status.fail();
            McpStdioTransportError
        })
    }
}

fn ensure_session_output_open(
    activity: Option<&McpStdioClientActivity>,
    status: &TransportStatus,
) -> Result<(), McpStdioTransportError> {
    let Some(activity) = activity else {
        return Ok(());
    };
    match activity.is_open_at_current_time() {
        Ok(true) => Ok(()),
        Ok(false) => Err(McpStdioTransportError),
        Err(_) => {
            status.fail();
            Err(McpStdioTransportError)
        }
    }
}

fn serialize_bounded<T>(
    value: &T,
    maximum_bytes: usize,
    append_newline: bool,
) -> Result<Vec<u8>, McpStdioTransportError>
where
    T: Serialize,
{
    let mut output = BoundedVecWriter::new(maximum_bytes);
    serde_json::to_writer(&mut output, value).map_err(|_| McpStdioTransportError)?;
    if append_newline {
        output
            .write_all(b"\n")
            .map_err(|_| McpStdioTransportError)?;
    }
    Ok(output.into_inner())
}

struct BoundedVecWriter {
    bytes: Vec<u8>,
    maximum_bytes: usize,
}

impl BoundedVecWriter {
    fn new(maximum_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(8 * 1_024),
            maximum_bytes,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for BoundedVecWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.maximum_bytes.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("bounded MCP serialization exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncomingMessageFailure {
    Unparseable,
    InvalidRequest,
    InvalidInitialization,
}

fn prepare_initialization_message(
    root: &Map<String, Value>,
    message: &mut ClientJsonRpcMessage,
) -> Result<(), IncomingMessageFailure> {
    let ClientJsonRpcMessage::Request(request) = message else {
        return Ok(());
    };
    match &mut request.request {
        ClientRequest::PingRequest(_) => {
            if root.len() != 3
                || !root.contains_key("jsonrpc")
                || !root.contains_key("id")
                || root.get("method") != Some(&Value::String("ping".to_owned()))
            {
                return Err(IncomingMessageFailure::InvalidInitialization);
            }
        }
        ClientRequest::InitializeRequest(initialize) => {
            if root.len() != 4
                || !root.contains_key("jsonrpc")
                || !root.contains_key("id")
                || !root.contains_key("params")
                || root.get("method") != Some(&Value::String("initialize".to_owned()))
            {
                return Err(IncomingMessageFailure::InvalidInitialization);
            }
            let offered = initialize.params.protocol_version.as_str();
            if !valid_protocol_offer(offered) {
                return Err(IncomingMessageFailure::InvalidInitialization);
            }
            if offered != MCP_PROTOCOL_VERSION {
                initialize.params.protocol_version =
                    serde_json::from_value(Value::String(UNSUPPORTED_PROTOCOL_SENTINEL.to_owned()))
                        .map_err(|_| IncomingMessageFailure::InvalidInitialization)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn valid_protocol_offer(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

struct StrictJsonValue(Value);

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictJsonVisitor).map(Self)
    }
}

struct StrictJsonVisitor;

impl<'de> Visitor<'de> for StrictJsonVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON without duplicate object members")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<StrictJsonValue>()? {
            values.push(value.0);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(serde::de::Error::custom("duplicate JSON object member"));
            }
            let value = object.next_value::<StrictJsonValue>()?;
            values.insert(key, value.0);
        }
        Ok(Value::Object(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct DeterministicLifecycleClock {
        now: StdMutex<Duration>,
    }

    impl DeterministicLifecycleClock {
        fn set(&self, now: Duration) {
            *self
                .now
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = now;
        }
    }

    impl McpStdioLifecycleClock for DeterministicLifecycleClock {
        fn now(&self) -> Duration {
            *self
                .now
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    struct DeterministicLifecycleScheduler {
        requested: tokio::sync::mpsc::UnboundedSender<Duration>,
        samples: tokio::sync::mpsc::UnboundedReceiver<Duration>,
    }

    impl McpStdioLifecycleScheduler for DeterministicLifecycleScheduler {
        fn wait_until(&mut self, deadline: Duration) -> McpStdioLifecycleSchedulerFuture<'_> {
            let requested = self.requested.send(deadline);
            Box::pin(async move {
                requested.map_err(|_| McpStdioServeError)?;
                self.samples.recv().await.ok_or(McpStdioServeError)
            })
        }
    }

    fn lifecycle_harness() -> (
        DeterministicLifecycleScheduler,
        tokio::sync::mpsc::UnboundedReceiver<Duration>,
        tokio::sync::mpsc::UnboundedSender<Duration>,
    ) {
        let (requested_sender, requested_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (sample_sender, sample_receiver) = tokio::sync::mpsc::unbounded_channel();
        (
            DeterministicLifecycleScheduler {
                requested: requested_sender,
                samples: sample_receiver,
            },
            requested_receiver,
            sample_sender,
        )
    }

    fn initialize(protocol_version: &str) -> Vec<u8> {
        format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{{\"protocolVersion\":\"{protocol_version}\",\"capabilities\":{{}},\"clientInfo\":{{\"name\":\"test\",\"version\":\"1\"}}}}}}"
        )
        .into_bytes()
    }

    #[test]
    fn valid_nonbaseline_offer_is_rewritten_only_in_the_typed_message() {
        let message = BoundedStdioTransport::<tokio::io::Empty, tokio::io::Sink>::parse_frame(
            &initialize("2025-06-18"),
            true,
        )
        .expect("valid message")
        .expect("message");
        let ClientJsonRpcMessage::Request(request) = message else {
            panic!("request");
        };
        let ClientRequest::InitializeRequest(initialize) = request.request else {
            panic!("initialize");
        };
        assert_eq!(
            initialize.params.protocol_version.as_str(),
            UNSUPPORTED_PROTOCOL_SENTINEL
        );
    }

    #[test]
    fn malformed_or_duplicate_initialization_is_rejected() {
        let malformed = initialize("2025-6-18");
        assert!(matches!(
            BoundedStdioTransport::<tokio::io::Empty, tokio::io::Sink>::parse_frame(
                &malformed, true
            ),
            Err(IncomingMessageFailure::InvalidInitialization)
        ));

        let duplicate = br#"{"jsonrpc":"2.0","id":1,"id":2,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
        assert!(matches!(
            BoundedStdioTransport::<tokio::io::Empty, tokio::io::Sink>::parse_frame(
                duplicate, true
            ),
            Err(IncomingMessageFailure::InvalidRequest)
        ));
    }

    #[test]
    fn client_activity_starts_at_initialization_and_advances_monotonically() {
        let clock = Arc::new(DeterministicLifecycleClock::default());
        let activity = McpStdioClientActivity::with_clock(clock);
        let origin = Duration::from_secs(10);

        activity.authenticated_request_completed_at(origin);
        assert_eq!(
            activity
                .timeline
                .borrow()
                .next_deadline()
                .expect("deadline"),
            None
        );

        activity.initialized_at(origin);
        assert_eq!(
            activity
                .timeline
                .borrow()
                .next_deadline()
                .expect("deadline"),
            Some(origin + MCP_STDIO_IDLE_TIMEOUT)
        );

        let authenticated = origin + Duration::from_secs(42);
        activity.authenticated_request_completed_at(authenticated);
        assert_eq!(
            activity
                .timeline
                .borrow()
                .next_deadline()
                .expect("deadline"),
            Some(authenticated + MCP_STDIO_IDLE_TIMEOUT)
        );

        activity.authenticated_request_completed_at(origin + Duration::from_secs(7));
        assert_eq!(
            activity.timeline.borrow().next_deadline(),
            Err(McpStdioServeError)
        );
    }

    #[test]
    fn idle_and_absolute_expiry_win_boundary_races_without_resurrection() {
        let idle_clock = Arc::new(DeterministicLifecycleClock::default());
        let idle = McpStdioClientActivity::with_clock(idle_clock.clone());
        idle.initialized();
        idle_clock.set(Duration::from_secs(300));
        assert!(!idle.authenticated_request_completed());
        assert!(
            idle.expire_if_due(Duration::from_secs(300))
                .expect("idle expiry")
        );
        idle_clock.set(Duration::from_secs(301));
        assert!(!idle.authenticated_request_completed());

        let expiry_first_clock = Arc::new(DeterministicLifecycleClock::default());
        let expiry_first = McpStdioClientActivity::with_clock(expiry_first_clock.clone());
        expiry_first.initialized();
        assert!(
            expiry_first
                .expire_if_due(Duration::from_secs(300))
                .expect("expiry first")
        );
        expiry_first_clock.set(Duration::from_secs(299));
        assert!(!expiry_first.authenticated_request_completed());

        let lifetime_clock = Arc::new(DeterministicLifecycleClock::default());
        let lifetime = McpStdioClientActivity::with_clock(lifetime_clock.clone());
        lifetime.initialized();
        for sample in [250, 500, 750, 899] {
            lifetime_clock.set(Duration::from_secs(sample));
            assert!(lifetime.authenticated_request_completed());
        }
        lifetime_clock.set(Duration::from_secs(900));
        assert!(!lifetime.authenticated_request_completed());
        assert!(
            lifetime
                .expire_if_due(Duration::from_secs(900))
                .expect("absolute expiry")
        );
    }

    #[tokio::test]
    async fn lifecycle_expires_at_exact_idle_boundary_after_authenticated_refresh() {
        let clock = Arc::new(DeterministicLifecycleClock::default());
        let activity = McpStdioClientActivity::with_clock(clock.clone());
        activity.initialized();
        let (scheduler, mut requested, samples) = lifecycle_harness();
        let driven_activity = activity.clone();
        let cancellation = CancellationToken::new();
        let driven_cancellation = cancellation.clone();
        let status = TransportStatus::default();
        let driven_status = status.clone();
        let lifecycle = tokio::spawn(async move {
            enforce_stdio_lifecycle(
                driven_activity,
                scheduler,
                driven_cancellation,
                Arc::new(McpCancellationRegistry::new()),
                driven_status,
            )
            .await;
        });

        assert_eq!(
            requested.recv().await.expect("initial deadline"),
            Duration::from_secs(300)
        );
        clock.set(Duration::from_secs(250));
        assert!(activity.authenticated_request_completed());
        assert_eq!(
            requested.recv().await.expect("refreshed deadline"),
            Duration::from_secs(550)
        );

        samples
            .send(Duration::from_secs(549))
            .expect("pre-deadline sample");
        assert_eq!(
            requested.recv().await.expect("same deadline"),
            Duration::from_secs(550)
        );
        samples
            .send(Duration::from_secs(550))
            .expect("deadline sample");
        lifecycle.await.expect("lifecycle task");
        assert!(cancellation.is_cancelled());
        assert!(!status.failed());
    }

    #[tokio::test]
    async fn absolute_lifetime_expires_while_observer_work_remains_pending() {
        assert_eq!(MCP_STDIO_SESSION_LIFETIME, crate::MAX_MCP_OBSERVER_LIFETIME);
        let clock = Arc::new(DeterministicLifecycleClock::default());
        let activity = McpStdioClientActivity::with_clock(clock.clone());
        activity.initialized();
        for sample in [250, 500, 750, 850] {
            clock.set(Duration::from_secs(sample));
            assert!(activity.authenticated_request_completed());
        }
        let (scheduler, mut requested, samples) = lifecycle_harness();
        let cancellation = CancellationToken::new();
        let driven_cancellation = cancellation.clone();
        let status = TransportStatus::default();
        let driven_status = status.clone();
        let lifecycle = tokio::spawn(async move {
            enforce_stdio_lifecycle(
                activity,
                scheduler,
                driven_cancellation,
                Arc::new(McpCancellationRegistry::new()),
                driven_status,
            )
            .await;
        });
        let pending_observer = tokio::spawn(std::future::pending::<()>());

        assert_eq!(
            requested.recv().await.expect("absolute deadline"),
            Duration::from_secs(900)
        );
        samples
            .send(Duration::from_secs(900))
            .expect("absolute deadline sample");
        lifecycle.await.expect("lifecycle task");
        assert!(cancellation.is_cancelled());
        assert!(!status.failed());
        assert!(!pending_observer.is_finished());
        pending_observer.abort();
        let _ = pending_observer.await;
    }

    #[tokio::test]
    async fn lifecycle_clock_fault_cancels_as_a_closed_serving_failure() {
        let clock = Arc::new(DeterministicLifecycleClock::default());
        let activity = McpStdioClientActivity::with_clock(clock.clone());
        activity.initialized();
        clock.set(Duration::from_secs(20));
        assert!(activity.authenticated_request_completed());
        clock.set(Duration::from_secs(19));
        assert!(!activity.authenticated_request_completed());
        let (scheduler, _, _) = lifecycle_harness();
        let cancellation = CancellationToken::new();
        let status = TransportStatus::default();

        enforce_stdio_lifecycle(
            activity,
            scheduler,
            cancellation.clone(),
            Arc::new(McpCancellationRegistry::new()),
            status.clone(),
        )
        .await;

        assert!(cancellation.is_cancelled());
        assert!(status.failed());
    }

    #[tokio::test]
    async fn inbound_complete_line_enforces_exact_limit_and_one_byte_probe() {
        let mut exact = vec![b' '; MCP_INBOUND_MESSAGE_MAX_BYTES - 3];
        exact.extend_from_slice(b"{}\n");
        let mut transport = BoundedStdioTransport::new(exact.as_slice(), tokio::io::sink());
        let frame = transport
            .read_frame()
            .await
            .expect("read")
            .expect("complete frame");
        assert_eq!(frame.len(), MCP_INBOUND_MESSAGE_MAX_BYTES - 1);

        let mut over = vec![b' '; MCP_INBOUND_MESSAGE_MAX_BYTES - 2];
        over.extend_from_slice(b"{}\n");
        let status = TransportStatus::default();
        let mut transport =
            BoundedStdioTransport::with_status(over.as_slice(), tokio::io::sink(), status.clone());
        assert_eq!(transport.read_frame().await, Err(McpStdioTransportError));
        assert!(status.failed());
    }

    #[tokio::test]
    async fn syntactically_invalid_json_returns_parse_error_without_an_id() {
        let input = b"{\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
        let (server_output, client_output) = tokio::io::duplex(1_024);
        let mut transport = BoundedStdioTransport::new(input.as_slice(), server_output);

        let received = Transport::<RoleServer>::receive(&mut transport)
            .await
            .expect("valid request after parse error");
        assert!(matches!(
            received,
            ClientJsonRpcMessage::Request(request)
                if matches!(request.request, ClientRequest::PingRequest(_))
        ));

        let mut output = BufReader::new(client_output);
        let mut response = Vec::new();
        output
            .read_until(b'\n', &mut response)
            .await
            .expect("parse-error response");
        let response: Value = serde_json::from_slice(&response).expect("response JSON");
        assert_eq!(response.get("jsonrpc"), Some(&Value::String("2.0".into())));
        assert_eq!(response.pointer("/error/code"), Some(&Value::from(-32_700)));
        assert_eq!(response.get("id"), None);
    }

    #[tokio::test]
    async fn blank_frame_returns_parse_error_and_does_not_hide_the_next_request() {
        let input = b"\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
        let (server_output, client_output) = tokio::io::duplex(1_024);
        let mut transport = BoundedStdioTransport::new(input.as_slice(), server_output);

        let received = Transport::<RoleServer>::receive(&mut transport)
            .await
            .expect("valid request after blank frame");
        assert!(matches!(
            received,
            ClientJsonRpcMessage::Request(request)
                if matches!(request.request, ClientRequest::PingRequest(_))
        ));

        let mut output = BufReader::new(client_output);
        let mut response = Vec::new();
        output
            .read_until(b'\n', &mut response)
            .await
            .expect("blank-frame parse-error response");
        let response: Value = serde_json::from_slice(&response).expect("response JSON");
        assert_eq!(response.pointer("/error/code"), Some(&Value::from(-32_700)));
        assert_eq!(response.get("id"), None);
    }

    #[tokio::test]
    async fn initialization_becomes_active_only_after_the_success_response_is_written() {
        let mut input = initialize(MCP_PROTOCOL_VERSION);
        input.push(b'\n');
        let mut transport = BoundedStdioTransport::new(input.as_slice(), tokio::io::sink());
        let initialization = transport.initialization_gate();
        let initialized = tokio::spawn(async move {
            initialization.wait().await;
        });

        let received = Transport::<RoleServer>::receive(&mut transport)
            .await
            .expect("initialize request");
        let ClientJsonRpcMessage::Request(request) = received else {
            panic!("request");
        };
        assert!(matches!(
            request.request,
            ClientRequest::InitializeRequest(_)
        ));
        assert!(
            !transport
                .initialization
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .initialized
        );
        assert!(!initialized.is_finished());
        assert_eq!(
            transport
                .client_activity
                .timeline
                .borrow()
                .next_deadline()
                .expect("deadline"),
            None
        );

        let unrelated = ServerJsonRpcMessage::response(
            rmcp::model::ServerResult::empty(()),
            request.id.clone(),
        );
        Transport::<RoleServer>::send(&mut transport, unrelated)
            .await
            .expect("unrelated response");
        assert!(
            !transport
                .initialization
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .initialized
        );
        assert!(!initialized.is_finished());

        let response = ServerJsonRpcMessage::response(
            rmcp::model::ServerResult::InitializeResult(crate::initialization_result()),
            request.id,
        );
        Transport::<RoleServer>::send(&mut transport, response)
            .await
            .expect("initialize response");
        assert!(
            transport
                .initialization
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .initialized
        );
        assert!(
            transport
                .client_activity
                .timeline
                .borrow()
                .next_deadline()
                .expect("deadline")
                .is_some()
        );
        initialized.await.expect("initialization waiter");
    }

    #[tokio::test]
    async fn failed_initialization_response_leaves_the_transport_uninitialized() {
        let mut input = initialize(MCP_PROTOCOL_VERSION);
        input.push(b'\n');
        let mut transport = BoundedStdioTransport::new(input.as_slice(), tokio::io::sink());

        let received = Transport::<RoleServer>::receive(&mut transport)
            .await
            .expect("initialize request");
        let ClientJsonRpcMessage::Request(request) = received else {
            panic!("request");
        };
        let response = ServerJsonRpcMessage::error(
            ErrorData::invalid_params("unsupported MCP protocol version", None),
            Some(request.id),
        );
        Transport::<RoleServer>::send(&mut transport, response)
            .await
            .expect("initialize error");
        let state = transport
            .initialization
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!state.initialized);
        assert!(state.pending_request.is_none());
    }

    #[tokio::test]
    async fn expired_session_writes_no_post_deadline_response() {
        let clock = Arc::new(DeterministicLifecycleClock::default());
        let activity = McpStdioClientActivity::with_clock(clock.clone());
        activity.initialized();
        let status = TransportStatus::default();
        let (server_output, mut client_output) = tokio::io::duplex(1_024);
        let mut transport = BoundedStdioTransport::with_status_and_activity(
            tokio::io::empty(),
            server_output,
            status.clone(),
            activity,
        );

        clock.set(MCP_STDIO_IDLE_TIMEOUT);
        let response = ServerJsonRpcMessage::response(
            rmcp::model::ServerResult::empty(()),
            RequestId::Number(42),
        );
        assert_eq!(
            Transport::<RoleServer>::send(&mut transport, response).await,
            Err(McpStdioTransportError)
        );
        assert!(!status.failed());
        drop(transport);

        let mut observed = Vec::new();
        client_output
            .read_to_end(&mut observed)
            .await
            .expect("closed output");
        assert!(observed.is_empty());
    }

    #[test]
    fn bounded_serializer_counts_the_final_newline() {
        let payload = "x".repeat(MCP_OUTBOUND_MESSAGE_MAX_BYTES - 3);
        let encoded =
            serialize_bounded(&payload, MCP_OUTBOUND_MESSAGE_MAX_BYTES, true).expect("exact frame");
        assert_eq!(encoded.len(), MCP_OUTBOUND_MESSAGE_MAX_BYTES);
    }

    #[test]
    fn serializer_refuses_a_complete_frame_one_byte_over() {
        let payload = "x".repeat(MCP_OUTBOUND_MESSAGE_MAX_BYTES - 2);
        assert_eq!(
            serialize_bounded(&payload, MCP_OUTBOUND_MESSAGE_MAX_BYTES, true),
            Err(McpStdioTransportError)
        );
    }

    #[tokio::test]
    async fn one_byte_over_outbound_frame_writes_nothing() {
        let payload = "x".repeat(MCP_OUTBOUND_MESSAGE_MAX_BYTES - 2);
        let (server_output, mut client_output) = tokio::io::duplex(64);
        let status = TransportStatus::default();
        let result = send_checked(
            Arc::new(AsyncMutex::new(Some(server_output))),
            payload,
            status.clone(),
        )
        .await;

        assert_eq!(result, Err(McpStdioTransportError));
        assert!(status.failed());
        let mut observed = Vec::new();
        client_output
            .read_to_end(&mut observed)
            .await
            .expect("closed output");
        assert!(observed.is_empty());
    }

    #[derive(Clone, Default)]
    struct RecordingNotificationPeer {
        sent: Arc<StdMutex<Vec<McpObserverNotification>>>,
    }

    impl McpNotificationPeer for RecordingNotificationPeer {
        fn notify<'a>(
            &'a self,
            notification: McpObserverNotification,
        ) -> Pin<Box<dyn Future<Output = Result<(), McpNotificationPeerError>> + Send + 'a>>
        {
            Box::pin(async move {
                self.sent
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(notification);
                Ok(())
            })
        }
    }

    struct RejectingNotificationPeer;

    impl McpNotificationPeer for RejectingNotificationPeer {
        fn notify<'a>(
            &'a self,
            _: McpObserverNotification,
        ) -> Pin<Box<dyn Future<Output = Result<(), McpNotificationPeerError>> + Send + 'a>>
        {
            Box::pin(async { Err(McpNotificationPeerError) })
        }
    }

    #[tokio::test]
    async fn notification_worker_preserves_marker_order_and_stops_on_peer_failure() {
        let state = Arc::new(McpObserverState::new());
        state
            .subscribe("riffdb://server/health")
            .expect("subscription");
        let (sender, receiver) =
            mcp_observer_notification_channel(&state).expect("notification mailbox");
        let expected = vec![
            McpObserverNotification::ToolsListChanged,
            McpObserverNotification::ResourcesListChanged,
            McpObserverNotification::ResourceUpdated(McpObserverResourceUpdate::new(
                "riffdb://server/health".to_owned(),
                1,
            )),
        ];
        for notification in expected.clone() {
            sender.try_emit(notification).expect("bounded marker");
        }
        drop(sender);

        let peer = RecordingNotificationPeer::default();
        forward_observer_notifications(peer.clone(), receiver)
            .await
            .expect("notification worker");
        assert_eq!(
            *peer
                .sent
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            expected
        );

        let state = Arc::new(McpObserverState::new());
        let (sender, receiver) =
            mcp_observer_notification_channel(&state).expect("notification mailbox");
        sender
            .try_emit(McpObserverNotification::ToolsListChanged)
            .expect("bounded marker");
        assert_eq!(
            forward_observer_notifications(RejectingNotificationPeer, receiver).await,
            Err(McpNotificationPeerError)
        );
    }
}
