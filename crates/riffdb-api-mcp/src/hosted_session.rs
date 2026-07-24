//! Bounded stateful-session ownership for hosted Streamable HTTP.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt, io,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::Stream;
use riffdb_types::CapabilityId;
use rmcp::{
    model::{
        ClientJsonRpcMessage, ClientRequest, GetExtensions, ServerJsonRpcMessage, ServerResult,
    },
    transport::{
        WorkerTransport,
        streamable_http_server::{
            RestoreOutcome, SessionId, SessionManager,
            session::ServerSseMessage,
            session::local::{
                EventId, LocalSessionHandle, LocalSessionWorker, SessionConfig, SessionError,
                create_local_session,
            },
        },
    },
};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    MCP_OUTBOUND_MESSAGE_MAX_BYTES, hosted_http::HostedAuthenticatedPrincipal,
    initialization_result,
};

/// Maximum number of hosted MCP sessions, including pending initialization.
pub(crate) const MAX_HOSTED_MCP_SESSIONS: usize = 128;
/// Maximum accepted visible-ASCII session identifier length.
pub(crate) const MAX_HOSTED_MCP_SESSION_ID_BYTES: usize = 128;
/// Maximum inactivity period refreshed only by authenticated client traffic.
pub(crate) const HOSTED_MCP_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// Maximum absolute lifetime of a hosted MCP session.
pub(crate) const HOSTED_MCP_LIFETIME: Duration = Duration::from_secs(900);

/// Injected monotonic time used only for hosted transport lifecycle control.
pub trait McpMonotonicClock: Send + Sync + 'static {
    /// Returns time elapsed from a process-local origin.
    fn now(&self) -> Result<Duration, McpMonotonicClockError>;
}

/// Closed failure to sample transport monotonic time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpMonotonicClockError;

impl fmt::Display for McpMonotonicClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP monotonic clock is unavailable")
    }
}

impl Error for McpMonotonicClockError {}

/// Process-local monotonic clock for production hosted transport composition.
pub struct SystemMcpMonotonicClock {
    origin: Instant,
}

impl SystemMcpMonotonicClock {
    /// Starts one independent process-local monotonic timeline.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemMcpMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl McpMonotonicClock for SystemMcpMonotonicClock {
    fn now(&self) -> Result<Duration, McpMonotonicClockError> {
        Ok(self.origin.elapsed())
    }
}

impl fmt::Debug for SystemMcpMonotonicClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SystemMcpMonotonicClock")
    }
}

pub(crate) trait McpSessionIdSource: Send + Sync + 'static {
    fn next_session_id(&self) -> SessionId;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RmcpSessionIdSource;

impl McpSessionIdSource for RmcpSessionIdSource {
    fn next_session_id(&self) -> SessionId {
        rmcp::transport::common::server_side_http::session_id()
    }
}

/// Closed, redaction-safe hosted-session failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HostedSessionError {
    InvalidCandidate,
    AdmissionDenied,
    SessionUnavailable,
    InvalidTransition,
    InvalidInitialization,
    ClockUnavailable,
    TransportUnavailable,
    OutputTooLarge,
}

impl fmt::Display for HostedSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP session is unavailable")
    }
}

impl Error for HostedSessionError {}

enum SessionEntry {
    Pending {
        handle: Option<LocalSessionHandle>,
        initializing: bool,
        created_at: Duration,
    },
    Active {
        handle: LocalSessionHandle,
        capability_id: CapabilityId,
        created_at: Duration,
        last_activity_at: Duration,
    },
}

#[derive(Default)]
struct SessionRegistry {
    sessions: BTreeMap<SessionId, SessionEntry>,
    last_observed_time: Option<Duration>,
}

/// Private RiffDB-owned manager used by the hosted registration wrapper.
pub(crate) struct HostedSessionManager {
    clock: Arc<dyn McpMonotonicClock>,
    id_source: Arc<dyn McpSessionIdSource>,
    registry: Arc<Mutex<SessionRegistry>>,
}

impl HostedSessionManager {
    pub(crate) fn new(
        clock: Arc<dyn McpMonotonicClock>,
        id_source: Arc<dyn McpSessionIdSource>,
    ) -> Self {
        Self {
            clock,
            id_source,
            registry: Arc::new(Mutex::new(SessionRegistry::default())),
        }
    }

    /// Revalidates binding and expiry, then refreshes authenticated activity.
    ///
    /// A capability mismatch is existence-blind and does not destroy the
    /// session belonging to the other capability.
    pub(crate) async fn authorize_and_touch(
        &self,
        id: &SessionId,
        capability_id: CapabilityId,
    ) -> Result<(), HostedSessionError> {
        let (result, close) = {
            let mut registry = self.lock_registry()?;
            match self.sample_time(&mut registry) {
                Err(error) => {
                    let close = registry.sessions.remove(id).and_then(entry_handle_owned);
                    (Err(error), close)
                }
                Ok(now) => {
                    let Some(entry) = registry.sessions.get(id) else {
                        return Err(HostedSessionError::SessionUnavailable);
                    };
                    let SessionEntry::Active {
                        capability_id: bound,
                        created_at,
                        last_activity_at,
                        ..
                    } = entry
                    else {
                        return Err(HostedSessionError::SessionUnavailable);
                    };
                    if *bound != capability_id {
                        return Err(HostedSessionError::SessionUnavailable);
                    }

                    match session_expired(now, *created_at, *last_activity_at) {
                        Ok(true) => (
                            Err(HostedSessionError::SessionUnavailable),
                            registry.sessions.remove(id).and_then(entry_handle_owned),
                        ),
                        Ok(false) => {
                            let Some(SessionEntry::Active {
                                last_activity_at, ..
                            }) = registry.sessions.get_mut(id)
                            else {
                                return Err(HostedSessionError::SessionUnavailable);
                            };
                            *last_activity_at = now;
                            (Ok(()), None)
                        }
                        Err(error) => (
                            Err(error),
                            registry.sessions.remove(id).and_then(entry_handle_owned),
                        ),
                    }
                }
            }
        };
        close_handle(close).await;
        result
    }

    /// Checks one freshly authenticated SSE frame against active session state.
    ///
    /// Server output is not client activity, so this check enforces expiry
    /// without refreshing the session's idle deadline.
    pub(crate) fn validate_sse_binding(
        &self,
        id: &SessionId,
        capability_id: CapabilityId,
    ) -> Result<(), HostedSessionError> {
        let (result, close) = {
            let mut registry = self.lock_registry()?;
            match self.sample_time(&mut registry) {
                Err(error) => {
                    let close = registry.sessions.remove(id).and_then(entry_handle_owned);
                    (Err(error), close)
                }
                Ok(now) => {
                    let Some(entry) = registry.sessions.get(id) else {
                        return Err(HostedSessionError::SessionUnavailable);
                    };
                    let SessionEntry::Active {
                        capability_id: bound,
                        created_at,
                        last_activity_at,
                        ..
                    } = entry
                    else {
                        return Err(HostedSessionError::SessionUnavailable);
                    };
                    if *bound != capability_id {
                        return Err(HostedSessionError::SessionUnavailable);
                    }

                    match session_expired(now, *created_at, *last_activity_at) {
                        Ok(false) => (Ok(()), None),
                        Ok(true) => (
                            Err(HostedSessionError::SessionUnavailable),
                            registry.sessions.remove(id).and_then(entry_handle_owned),
                        ),
                        Err(error) => (
                            Err(error),
                            registry.sessions.remove(id).and_then(entry_handle_owned),
                        ),
                    }
                }
            }
        };
        spawn_close(close);
        result
    }

    pub(crate) fn close_in_background(&self, id: &SessionId) {
        let handle = self
            .lock_registry()
            .ok()
            .and_then(|mut registry| registry.sessions.remove(id))
            .and_then(entry_handle_owned);
        spawn_close(handle);
    }

    pub(crate) fn is_active(&self, id: &SessionId) -> bool {
        self.lock_registry().is_ok_and(|registry| {
            matches!(registry.sessions.get(id), Some(SessionEntry::Active { .. }))
        })
    }

    /// Removes every session due under the injected clock and then closes its worker.
    pub(crate) async fn expire_due(&self) -> Result<usize, HostedSessionError> {
        let (handles, result) = {
            let mut registry = self.lock_registry()?;
            match self.sample_time(&mut registry) {
                Err(error) => (take_all_handles(&mut registry), Err(error)),
                Ok(now) => {
                    let expired_ids = registry
                        .sessions
                        .iter()
                        .map(|(id, entry)| {
                            entry_expired(now, entry).map(|expired| (id.clone(), expired))
                        })
                        .collect::<Result<Vec<_>, _>>();
                    match expired_ids {
                        Ok(expired_ids) => {
                            let due_ids = expired_ids
                                .into_iter()
                                .filter_map(|(id, expired)| expired.then_some(id))
                                .collect::<Vec<_>>();
                            let expired = due_ids.len();
                            let handles = due_ids
                                .into_iter()
                                .filter_map(|id| {
                                    registry.sessions.remove(&id).and_then(entry_handle_owned)
                                })
                                .collect::<Vec<_>>();
                            (handles, Ok(expired))
                        }
                        Err(error) => (take_all_handles(&mut registry), Err(error)),
                    }
                }
            }
        };
        for handle in handles {
            close_handle(Some(handle)).await;
        }
        result
    }

    fn lock_registry(&self) -> Result<MutexGuard<'_, SessionRegistry>, HostedSessionError> {
        self.registry
            .lock()
            .map_err(|_| HostedSessionError::TransportUnavailable)
    }

    fn sample_time(&self, registry: &mut SessionRegistry) -> Result<Duration, HostedSessionError> {
        let now = self
            .clock
            .now()
            .map_err(|_| HostedSessionError::ClockUnavailable)?;
        if registry
            .last_observed_time
            .is_some_and(|previous| now < previous)
        {
            return Err(HostedSessionError::ClockUnavailable);
        }
        registry.last_observed_time = Some(now);
        Ok(now)
    }

    async fn active_handle(
        &self,
        id: &SessionId,
    ) -> Result<LocalSessionHandle, HostedSessionError> {
        let (result, close) = {
            let mut registry = self.lock_registry()?;
            match self.sample_time(&mut registry) {
                Err(error) => {
                    let close = registry.sessions.remove(id).and_then(entry_handle_owned);
                    (Err(error), close)
                }
                Ok(now) => {
                    let Some(entry) = registry.sessions.get(id) else {
                        return Err(HostedSessionError::SessionUnavailable);
                    };
                    let SessionEntry::Active {
                        handle,
                        created_at,
                        last_activity_at,
                        ..
                    } = entry
                    else {
                        return Err(HostedSessionError::SessionUnavailable);
                    };

                    match session_expired(now, *created_at, *last_activity_at) {
                        Ok(false) => (Ok(handle.clone()), None),
                        Ok(true) => (
                            Err(HostedSessionError::SessionUnavailable),
                            registry.sessions.remove(id).and_then(entry_handle_owned),
                        ),
                        Err(error) => (
                            Err(error),
                            registry.sessions.remove(id).and_then(entry_handle_owned),
                        ),
                    }
                }
            }
        };
        close_handle(close).await;
        result
    }

    async fn remove_and_close(&self, id: &SessionId) {
        let handle = self
            .lock_registry()
            .ok()
            .and_then(|mut registry| registry.sessions.remove(id))
            .and_then(entry_handle_owned);
        close_handle(handle).await;
    }
}

impl fmt::Debug for HostedSessionManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedSessionManager([REDACTED])")
    }
}

impl SessionManager for HostedSessionManager {
    type Error = HostedSessionError;
    type Transport = WorkerTransport<LocalSessionWorker>;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        let id = self.id_source.next_session_id();
        if !valid_session_id(&id) {
            return Err(HostedSessionError::InvalidCandidate);
        }

        let stale_handles = {
            let mut registry = self.lock_registry()?;
            let now = self.sample_time(&mut registry)?;
            let expired_ids = registry
                .sessions
                .iter()
                .map(|(existing_id, entry)| {
                    entry_expired(now, entry).map(|expired| (existing_id.clone(), expired))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let stale_handles = expired_ids
                .into_iter()
                .filter_map(|(existing_id, expired)| {
                    expired
                        .then(|| registry.sessions.remove(&existing_id))
                        .flatten()
                        .and_then(entry_handle_owned)
                })
                .collect::<Vec<_>>();
            if registry.sessions.len() >= MAX_HOSTED_MCP_SESSIONS
                || registry.sessions.contains_key(&id)
            {
                return Err(HostedSessionError::AdmissionDenied);
            }
            registry.sessions.insert(
                id.clone(),
                SessionEntry::Pending {
                    handle: None,
                    initializing: false,
                    created_at: now,
                },
            );
            stale_handles
        };
        for handle in stale_handles {
            spawn_close(Some(handle));
        }

        let mut config = SessionConfig::default();
        config.keep_alive = None;
        config.sse_retry = None;
        let (handle, worker) = create_local_session(id.clone(), config);
        let inserted = {
            let mut registry = self.lock_registry()?;
            match registry.sessions.get_mut(&id) {
                Some(SessionEntry::Pending {
                    handle: pending_handle,
                    initializing: false,
                    ..
                }) => {
                    *pending_handle = Some(handle.clone());
                    true
                }
                _ => false,
            }
        };
        if !inserted {
            close_handle(Some(handle)).await;
            return Err(HostedSessionError::InvalidTransition);
        }
        Ok((id, WorkerTransport::spawn(worker)))
    }

    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        let Some(capability_id) = initializing_capability(&message) else {
            self.remove_and_close(id).await;
            return Err(HostedSessionError::InvalidInitialization);
        };
        let handle = {
            let mut registry = self.lock_registry()?;
            let Some(SessionEntry::Pending {
                handle: Some(handle),
                initializing,
                ..
            }) = registry.sessions.get_mut(id)
            else {
                return Err(HostedSessionError::InvalidTransition);
            };
            if *initializing {
                return Err(HostedSessionError::InvalidTransition);
            }
            *initializing = true;
            handle.clone()
        };

        let mut cancellation_guard =
            InitializationGuard::new(Arc::clone(&self.registry), id.clone());
        let response = match handle.initialize(message).await {
            Ok(response) => response,
            Err(_) => {
                cancellation_guard.disarm();
                self.remove_and_close(id).await;
                return Err(HostedSessionError::TransportUnavailable);
            }
        };
        if !valid_initialization_response(&response)
            || preflight_sse_message(&ServerSseMessage::from_message(response.clone())).is_err()
        {
            cancellation_guard.disarm();
            self.remove_and_close(id).await;
            return Err(HostedSessionError::InvalidInitialization);
        }

        let (transition, close) = {
            let mut registry = self.lock_registry()?;
            match self.sample_time(&mut registry) {
                Err(error) => {
                    let close = registry.sessions.remove(id).and_then(entry_handle_owned);
                    (Err(error), close)
                }
                Ok(now) => {
                    if let Some(entry) = registry.sessions.remove(id) {
                        match entry {
                            SessionEntry::Pending {
                                handle: Some(active_handle),
                                initializing: true,
                                created_at,
                            } => match session_expired(now, created_at, now) {
                                Ok(false) => {
                                    registry.sessions.insert(
                                        id.clone(),
                                        SessionEntry::Active {
                                            handle: active_handle,
                                            capability_id,
                                            created_at,
                                            last_activity_at: now,
                                        },
                                    );
                                    (Ok(()), None)
                                }
                                Ok(true) => (
                                    Err(HostedSessionError::SessionUnavailable),
                                    Some(active_handle),
                                ),
                                Err(error) => (Err(error), Some(active_handle)),
                            },
                            other => (
                                Err(HostedSessionError::InvalidTransition),
                                entry_handle_owned(other),
                            ),
                        }
                    } else {
                        (Err(HostedSessionError::InvalidTransition), None)
                    }
                }
            }
        };
        cancellation_guard.disarm();
        close_handle(close).await;
        transition?;
        Ok(response)
    }

    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        match self.active_handle(id).await {
            Ok(_) => Ok(true),
            Err(HostedSessionError::SessionUnavailable) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        self.remove_and_close(id).await;
        Ok(())
    }

    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let handle = self.active_handle(id).await?;
        let receiver = match handle.establish_request_wise_channel().await {
            Ok(receiver) => receiver,
            Err(_) => {
                self.remove_and_close(id).await;
                return Err(HostedSessionError::TransportUnavailable);
            }
        };
        if handle
            .push_message(message, receiver.http_request_id)
            .await
            .is_err()
        {
            self.remove_and_close(id).await;
            return Err(HostedSessionError::TransportUnavailable);
        }
        Ok(BoundedSessionStream::new(
            ReceiverStream::new(receiver.inner),
            Arc::clone(&self.registry),
            id.clone(),
        ))
    }

    async fn accept_message(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        let handle = self.active_handle(id).await?;
        if handle.push_message(message, None).await.is_err() {
            self.remove_and_close(id).await;
            return Err(HostedSessionError::TransportUnavailable);
        }
        Ok(())
    }

    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let handle = self.active_handle(id).await?;
        let receiver = match handle.establish_common_channel().await {
            Ok(receiver) => receiver,
            Err(_) => {
                self.remove_and_close(id).await;
                return Err(HostedSessionError::TransportUnavailable);
            }
        };
        Ok(BoundedSessionStream::new(
            ReceiverStream::new(receiver.inner),
            Arc::clone(&self.registry),
            id.clone(),
        ))
    }

    async fn resume(
        &self,
        id: &SessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let event_id = last_event_id
            .parse::<EventId>()
            .map_err(|_| HostedSessionError::SessionUnavailable)?;
        let handle = self.active_handle(id).await?;
        let receiver = match handle.resume(event_id).await {
            Ok(receiver) => receiver,
            Err(_) => {
                self.remove_and_close(id).await;
                return Err(HostedSessionError::TransportUnavailable);
            }
        };
        Ok(BoundedSessionStream::new(
            ReceiverStream::new(receiver.inner),
            Arc::clone(&self.registry),
            id.clone(),
        ))
    }

    async fn restore_session(
        &self,
        _id: SessionId,
    ) -> Result<RestoreOutcome<Self::Transport>, Self::Error> {
        Ok(RestoreOutcome::NotSupported)
    }
}

struct InitializationGuard {
    registry: Arc<Mutex<SessionRegistry>>,
    id: SessionId,
    armed: bool,
}

impl InitializationGuard {
    fn new(registry: Arc<Mutex<SessionRegistry>>, id: SessionId) -> Self {
        Self {
            registry,
            id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InitializationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let handle = self
            .registry
            .lock()
            .ok()
            .and_then(|mut registry| registry.sessions.remove(&self.id))
            .and_then(entry_handle_owned);
        spawn_close(handle);
    }
}

struct BoundedSessionStream {
    inner: ReceiverStream<ServerSseMessage>,
    registry: Arc<Mutex<SessionRegistry>>,
    id: SessionId,
    terminated: bool,
}

impl BoundedSessionStream {
    fn new(
        inner: ReceiverStream<ServerSseMessage>,
        registry: Arc<Mutex<SessionRegistry>>,
        id: SessionId,
    ) -> Self {
        Self {
            inner,
            registry,
            id,
            terminated: false,
        }
    }

    fn terminate(&mut self) {
        self.terminated = true;
        let handle = self
            .registry
            .lock()
            .ok()
            .and_then(|mut registry| registry.sessions.remove(&self.id))
            .and_then(entry_handle_owned);
        spawn_close(handle);
    }
}

impl Stream for BoundedSessionStream {
    type Item = ServerSseMessage;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_next(context) {
            Poll::Ready(Some(message)) => {
                if preflight_sse_message(&message).is_ok() {
                    Poll::Ready(Some(message))
                } else {
                    this.terminate();
                    Poll::Ready(None)
                }
            }
            other => other,
        }
    }
}

fn initializing_capability(message: &ClientJsonRpcMessage) -> Option<CapabilityId> {
    let ClientJsonRpcMessage::Request(request) = message else {
        return None;
    };
    if !matches!(&request.request, ClientRequest::InitializeRequest(_)) {
        return None;
    }
    let parts = request.request.extensions().get::<http::request::Parts>()?;
    parts
        .extensions
        .get::<HostedAuthenticatedPrincipal>()
        .map(HostedAuthenticatedPrincipal::capability_id)
}

fn valid_initialization_response(response: &ServerJsonRpcMessage) -> bool {
    let ServerJsonRpcMessage::Response(response) = response else {
        return false;
    };
    let ServerResult::InitializeResult(result) = &response.result else {
        return false;
    };
    result == &initialization_result()
}

fn valid_session_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_HOSTED_MCP_SESSION_ID_BYTES
        && id.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

fn entry_handle_owned(entry: SessionEntry) -> Option<LocalSessionHandle> {
    match entry {
        SessionEntry::Pending { handle, .. } => handle,
        SessionEntry::Active { handle, .. } => Some(handle),
    }
}

fn take_all_handles(registry: &mut SessionRegistry) -> Vec<LocalSessionHandle> {
    std::mem::take(&mut registry.sessions)
        .into_values()
        .filter_map(entry_handle_owned)
        .collect()
}

fn session_expired(
    now: Duration,
    created_at: Duration,
    last_activity_at: Duration,
) -> Result<bool, HostedSessionError> {
    let lifetime_deadline = created_at
        .checked_add(HOSTED_MCP_LIFETIME)
        .ok_or(HostedSessionError::ClockUnavailable)?;
    let idle_deadline = last_activity_at
        .checked_add(HOSTED_MCP_IDLE_TIMEOUT)
        .ok_or(HostedSessionError::ClockUnavailable)?;
    Ok(now >= lifetime_deadline || now >= idle_deadline)
}

fn entry_expired(now: Duration, entry: &SessionEntry) -> Result<bool, HostedSessionError> {
    match entry {
        SessionEntry::Pending { created_at, .. } => session_expired(now, *created_at, *created_at),
        SessionEntry::Active {
            created_at,
            last_activity_at,
            ..
        } => session_expired(now, *created_at, *last_activity_at),
    }
}

fn preflight_sse_message(message: &ServerSseMessage) -> Result<usize, HostedSessionError> {
    if message.retry.is_some() {
        return Err(HostedSessionError::OutputTooLarge);
    }
    let rpc_message = message
        .message
        .as_deref()
        .ok_or(HostedSessionError::OutputTooLarge)?;
    let mut writer = BoundedCountingWriter::default();
    serde_json::to_writer(&mut writer, rpc_message)
        .map_err(|_| HostedSessionError::OutputTooLarge)?;

    let mut framed = 6_usize
        .checked_add(writer.written)
        .and_then(|value| value.checked_add(1))
        .ok_or(HostedSessionError::OutputTooLarge)?;
    if let Some(event_id) = &message.event_id {
        if !valid_session_id(event_id) {
            return Err(HostedSessionError::OutputTooLarge);
        }
        framed = framed
            .checked_add(4)
            .and_then(|value| value.checked_add(event_id.len()))
            .and_then(|value| value.checked_add(1))
            .ok_or(HostedSessionError::OutputTooLarge)?;
    }
    framed = framed
        .checked_add(1)
        .ok_or(HostedSessionError::OutputTooLarge)?;
    if framed > MCP_OUTBOUND_MESSAGE_MAX_BYTES {
        return Err(HostedSessionError::OutputTooLarge);
    }
    Ok(framed)
}

#[derive(Default)]
struct BoundedCountingWriter {
    written: usize,
}

impl io::Write for BoundedCountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .written
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("MCP output exceeds its bound"))?;
        if next > MCP_OUTBOUND_MESSAGE_MAX_BYTES {
            return Err(io::Error::other("MCP output exceeds its bound"));
        }
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn close_handle(handle: Option<LocalSessionHandle>) {
    if let Some(handle) = handle {
        match handle.close().await {
            Ok(()) | Err(SessionError::SessionServiceTerminated) => {}
            Err(_) => {}
        }
    }
}

fn spawn_close(handle: Option<LocalSessionHandle>) {
    let Some(handle) = handle else {
        return;
    };
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(async move {
            close_handle(Some(handle)).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[derive(Clone)]
    struct TestClock(Arc<AtomicU64>);

    impl TestClock {
        fn new(seconds: u64) -> Self {
            Self(Arc::new(AtomicU64::new(seconds)))
        }

        fn set(&self, seconds: u64) {
            self.0.store(seconds, Ordering::SeqCst);
        }
    }

    impl McpMonotonicClock for TestClock {
        fn now(&self) -> Result<Duration, McpMonotonicClockError> {
            Ok(Duration::from_secs(self.0.load(Ordering::SeqCst)))
        }
    }

    #[derive(Clone)]
    struct FixedIdSource(SessionId);

    impl McpSessionIdSource for FixedIdSource {
        fn next_session_id(&self) -> SessionId {
            self.0.clone()
        }
    }

    #[derive(Default)]
    struct SequenceIdSource(AtomicU64);

    impl McpSessionIdSource for SequenceIdSource {
        fn next_session_id(&self) -> SessionId {
            format!("session-{}", self.0.fetch_add(1, Ordering::SeqCst)).into()
        }
    }

    fn manager(candidate: &str) -> HostedSessionManager {
        HostedSessionManager::new(
            Arc::new(TestClock::new(1)),
            Arc::new(FixedIdSource(candidate.into())),
        )
    }

    fn capability_id(marker: u8) -> CapabilityId {
        let mut bytes = [marker; 16];
        bytes[0..6].copy_from_slice(&[0, 0, 0, 0, 0, marker]);
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        CapabilityId::from_bytes(bytes).expect("UUIDv7 capability")
    }

    fn activate(
        manager: &HostedSessionManager,
        id: &SessionId,
        capability_id: CapabilityId,
        created_at: Duration,
        last_activity_at: Duration,
    ) {
        let mut registry = manager.lock_registry().expect("registry");
        let handle = registry
            .sessions
            .remove(id)
            .and_then(entry_handle_owned)
            .expect("pending handle");
        registry.sessions.insert(
            id.clone(),
            SessionEntry::Active {
                handle,
                capability_id,
                created_at,
                last_activity_at,
            },
        );
    }

    #[tokio::test]
    async fn one_candidate_is_reserved_before_worker_creation_and_pending_is_hidden() {
        let manager = manager("candidate");
        let (id, transport) = manager.create_session().await.expect("first reservation");
        assert_eq!(&*id, "candidate");
        assert!(!manager.has_session(&id).await.expect("pending lookup"));
        drop(transport);

        assert!(matches!(
            manager.create_session().await,
            Err(HostedSessionError::AdmissionDenied)
        ));
        manager.close_session(&id).await.expect("pending cleanup");
    }

    #[tokio::test]
    async fn malformed_candidates_fail_before_a_worker_exists() {
        for candidate in ["", "contains space", "\n", &"x".repeat(129)] {
            let manager = manager(candidate);
            assert!(matches!(
                manager.create_session().await,
                Err(HostedSessionError::InvalidCandidate)
            ));
            assert!(
                manager
                    .lock_registry()
                    .expect("registry")
                    .sessions
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn exact_session_capacity_includes_pending_reservations() {
        let manager = HostedSessionManager::new(
            Arc::new(TestClock::new(1)),
            Arc::new(SequenceIdSource::default()),
        );
        let mut reservations = Vec::with_capacity(MAX_HOSTED_MCP_SESSIONS);

        for _ in 0..MAX_HOSTED_MCP_SESSIONS {
            reservations.push(
                manager
                    .create_session()
                    .await
                    .expect("reservation within exact capacity"),
            );
        }
        assert_eq!(
            manager.lock_registry().expect("registry").sessions.len(),
            MAX_HOSTED_MCP_SESSIONS
        );
        assert!(matches!(
            manager.create_session().await,
            Err(HostedSessionError::AdmissionDenied)
        ));

        for (id, transport) in reservations {
            drop(transport);
            manager
                .close_session(&id)
                .await
                .expect("reservation cleanup");
        }
    }

    #[tokio::test]
    async fn lifecycle_hook_expires_due_sessions_at_the_half_open_deadline() {
        let clock = TestClock::new(1);
        let manager = HostedSessionManager::new(
            Arc::new(clock.clone()),
            Arc::new(SequenceIdSource::default()),
        );
        let (_first_id, first_transport) =
            manager.create_session().await.expect("first reservation");
        let (_second_id, second_transport) =
            manager.create_session().await.expect("second reservation");

        clock.set(300);
        assert_eq!(manager.expire_due().await.expect("before deadline"), 0);
        assert_eq!(manager.lock_registry().expect("registry").sessions.len(), 2);

        clock.set(301);
        assert_eq!(manager.expire_due().await.expect("at deadline"), 2);
        assert!(
            manager
                .lock_registry()
                .expect("registry")
                .sessions
                .is_empty()
        );
        drop((first_transport, second_transport));
    }

    #[tokio::test]
    async fn lifecycle_clock_regression_fails_closed_and_releases_all_state() {
        let clock = TestClock::new(10);
        let manager = HostedSessionManager::new(
            Arc::new(clock.clone()),
            Arc::new(SequenceIdSource::default()),
        );
        let (_id, transport) = manager.create_session().await.expect("reservation");

        clock.set(9);
        assert_eq!(
            manager.expire_due().await,
            Err(HostedSessionError::ClockUnavailable)
        );
        assert!(
            manager
                .lock_registry()
                .expect("registry")
                .sessions
                .is_empty()
        );
        drop(transport);
    }

    #[tokio::test]
    async fn lifecycle_hook_enforces_absolute_lifetime_despite_idle_refreshes() {
        let clock = TestClock::new(1);
        let manager = HostedSessionManager::new(
            Arc::new(clock.clone()),
            Arc::new(FixedIdSource("lifetime-session".into())),
        );
        let (id, transport) = manager.create_session().await.expect("reservation");
        let capability = capability_id(1);
        activate(
            &manager,
            &id,
            capability,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        for second in [299, 598, 897] {
            clock.set(second);
            manager
                .authorize_and_touch(&id, capability)
                .await
                .expect("idle refresh within absolute lifetime");
        }

        clock.set(900);
        assert_eq!(manager.expire_due().await.expect("before lifetime"), 0);
        clock.set(901);
        assert_eq!(manager.expire_due().await.expect("at lifetime"), 1);
        assert!(
            manager
                .lock_registry()
                .expect("registry")
                .sessions
                .is_empty()
        );
        drop(transport);
    }

    #[tokio::test]
    async fn capability_binding_is_existence_blind_and_expiry_removes_state() {
        let clock = TestClock::new(1);
        let manager = HostedSessionManager::new(
            Arc::new(clock.clone()),
            Arc::new(FixedIdSource("bound-session".into())),
        );
        let (id, transport) = manager.create_session().await.expect("reservation");
        activate(
            &manager,
            &id,
            capability_id(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );

        assert_eq!(
            manager.authorize_and_touch(&id, capability_id(2)).await,
            Err(HostedSessionError::SessionUnavailable)
        );
        assert!(
            manager
                .lock_registry()
                .expect("registry")
                .sessions
                .contains_key(&id)
        );
        manager
            .authorize_and_touch(&id, capability_id(1))
            .await
            .expect("matching capability");

        clock.set(301);
        assert_eq!(
            manager.authorize_and_touch(&id, capability_id(1)).await,
            Err(HostedSessionError::SessionUnavailable)
        );
        assert!(
            !manager
                .lock_registry()
                .expect("registry")
                .sessions
                .contains_key(&id)
        );
        drop(transport);
    }

    #[tokio::test]
    async fn sse_binding_uses_fresh_capability_without_refreshing_idle_time() {
        let clock = TestClock::new(1);
        let manager = HostedSessionManager::new(
            Arc::new(clock.clone()),
            Arc::new(FixedIdSource("sse-session".into())),
        );
        let (id, transport) = manager.create_session().await.expect("reservation");
        activate(
            &manager,
            &id,
            capability_id(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );

        clock.set(300);
        manager
            .validate_sse_binding(&id, capability_id(1))
            .expect("matching frame before idle deadline");
        assert_eq!(
            manager.validate_sse_binding(&id, capability_id(2)),
            Err(HostedSessionError::SessionUnavailable)
        );
        assert!(
            manager
                .lock_registry()
                .expect("registry")
                .sessions
                .contains_key(&id)
        );

        clock.set(301);
        assert_eq!(
            manager.validate_sse_binding(&id, capability_id(1)),
            Err(HostedSessionError::SessionUnavailable)
        );
        assert!(
            !manager
                .lock_registry()
                .expect("registry")
                .sessions
                .contains_key(&id)
        );
        drop(transport);
    }

    #[test]
    fn typed_sse_preflight_matches_the_pinned_sdk_frame_layout() {
        let response = ServerJsonRpcMessage::response(
            ServerResult::EmptyResult(rmcp::model::EmptyResult {}),
            rmcp::model::RequestId::Number(7),
        );
        let json = serde_json::to_string(&response).expect("JSON");
        let message = ServerSseMessage::new("1/7", response);
        let actual = preflight_sse_message(&message).expect("bounded frame");
        let expected = format!("data: {json}\nid: 1/7\n\n").len();
        assert_eq!(actual, expected);
        assert!(
            preflight_sse_message(&ServerSseMessage::priming("0", Duration::from_secs(1))).is_err()
        );
    }

    #[test]
    fn expiry_is_half_open_and_overflow_fails_closed() {
        assert!(
            !session_expired(Duration::from_secs(299), Duration::ZERO, Duration::ZERO)
                .expect("bounded deadlines")
        );
        assert!(
            session_expired(Duration::from_secs(300), Duration::ZERO, Duration::ZERO)
                .expect("bounded deadlines")
        );
        assert_eq!(
            session_expired(Duration::MAX, Duration::MAX, Duration::MAX),
            Err(HostedSessionError::ClockUnavailable)
        );
    }

    #[test]
    fn session_and_manager_debug_are_redacted() {
        let error = HostedSessionError::SessionUnavailable;
        assert_eq!(error.to_string(), "hosted MCP session is unavailable");
        let manager = manager("secret-session");
        assert_eq!(format!("{manager:?}"), "HostedSessionManager([REDACTED])");
        assert!(!format!("{error:?}").contains("secret-session"));
    }

    #[test]
    fn system_clock_is_monotonic_and_debug_has_no_origin() {
        let clock = SystemMcpMonotonicClock::new();
        let first = clock.now().expect("first sample");
        let second = clock.now().expect("second sample");
        assert!(second >= first);
        assert_eq!(format!("{clock:?}"), "SystemMcpMonotonicClock");
    }
}
