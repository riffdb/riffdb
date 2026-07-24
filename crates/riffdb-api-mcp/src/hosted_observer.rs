//! Credential-free hosted observer lifecycle and service-call bridge.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use riffdb_service::DiscoveryCatalogFence;
use rmcp::{
    Peer, RoleServer,
    model::{Extensions, ResourceUpdatedNotificationParam},
    transport::streamable_http_server::SessionId,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    McpBackendError, McpBackendFuture, McpCompactObservationRequest, McpCompactObservationResult,
    McpObservedInventory, McpObserverBackend, McpObserverBackendFuture, McpObserverLoop,
    McpObserverNotification, McpObserverNotificationReceiver, McpObserverNotificationSink,
    McpObserverPhysicalCallBudget, McpObserverPhysicalCallHandle, McpObserverSemaphore,
    McpObserverState, McpSubscribedResourceObservation,
    hosted_session::{MAX_HOSTED_MCP_SESSIONS, McpMonotonicClock},
    mcp_observer_notification_channel,
    observer::{
        MCP_OBSERVER_TICK_INTERVAL, McpObserverScheduler, McpObserverSchedulerFuture,
        drive_mcp_observer,
    },
    service_backend::{
        HostedObserverServiceCaller, HostedObserverServiceRequest, HostedObserverServiceResponse,
        HostedServiceMcpBackend,
    },
};

#[derive(Clone)]
pub(crate) struct HostedInitializationRendezvous {
    inner: Arc<InitializationRendezvousInner>,
}

struct InitializationRendezvousInner {
    pending: Mutex<Option<HostedObserverInitialization>>,
}

pub(crate) struct HostedObserverInitialization {
    peer: Peer<RoleServer>,
    state: Arc<McpObserverState>,
}

impl HostedObserverInitialization {
    pub(crate) fn cancel(self) {
        self.state.cancel();
    }
}

impl HostedInitializationRendezvous {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(InitializationRendezvousInner {
                pending: Mutex::new(None),
            }),
        }
    }

    fn publish(
        &self,
        peer: Peer<RoleServer>,
        state: Arc<McpObserverState>,
    ) -> Result<(), HostedObserverLifecycleError> {
        let mut pending = self
            .inner
            .pending
            .lock()
            .map_err(|_| HostedObserverLifecycleError)?;
        if pending.is_some() {
            return Err(HostedObserverLifecycleError);
        }
        *pending = Some(HostedObserverInitialization { peer, state });
        Ok(())
    }

    pub(crate) fn take(
        &self,
    ) -> Result<HostedObserverInitialization, HostedObserverLifecycleError> {
        self.inner
            .pending
            .lock()
            .map_err(|_| HostedObserverLifecycleError)?
            .take()
            .ok_or(HostedObserverLifecycleError)
    }
}

impl fmt::Debug for HostedInitializationRendezvous {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedInitializationRendezvous")
    }
}

impl Drop for InitializationRendezvousInner {
    fn drop(&mut self) {
        if let Ok(pending) = self.pending.get_mut()
            && let Some(initialization) = pending.take()
        {
            initialization.state.cancel();
        }
    }
}

pub(crate) fn publish_initialization_observer(
    extensions: &Extensions,
    peer: Peer<RoleServer>,
    state: Arc<McpObserverState>,
) -> Result<(), HostedObserverLifecycleError> {
    let parts = extensions
        .get::<http::request::Parts>()
        .ok_or(HostedObserverLifecycleError)?;
    parts
        .extensions
        .get::<HostedInitializationRendezvous>()
        .ok_or(HostedObserverLifecycleError)?
        .publish(peer, state)
}

pub(crate) struct HostedObserverRegistry {
    entries: Mutex<BTreeMap<SessionId, HostedObserverInitialization>>,
    semaphore: Arc<McpObserverSemaphore>,
}

impl HostedObserverRegistry {
    pub(crate) fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            semaphore: Arc::new(McpObserverSemaphore::new()),
        }
    }

    pub(crate) fn register(
        &self,
        session_id: SessionId,
        initialization: HostedObserverInitialization,
    ) -> Result<(), HostedObserverLifecycleError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| HostedObserverLifecycleError)?;
        if entries.len() >= MAX_HOSTED_MCP_SESSIONS || entries.contains_key(&session_id) {
            initialization.state.cancel();
            return Err(HostedObserverLifecycleError);
        }
        entries.insert(session_id, initialization);
        Ok(())
    }

    pub(crate) fn claim(
        &self,
        session_id: &SessionId,
    ) -> Result<HostedObserverInitialization, HostedObserverLifecycleError> {
        self.entries
            .lock()
            .map_err(|_| HostedObserverLifecycleError)?
            .remove(session_id)
            .ok_or(HostedObserverLifecycleError)
    }

    pub(crate) fn cancel(&self, session_id: &SessionId) {
        if let Ok(mut entries) = self.entries.lock()
            && let Some(initialization) = entries.remove(session_id)
        {
            initialization.state.cancel();
        }
    }

    pub(crate) fn retain(
        &self,
        mut keep: impl FnMut(&SessionId) -> bool,
    ) -> Result<(), HostedObserverLifecycleError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| HostedObserverLifecycleError)?;
        let removed = entries
            .keys()
            .filter(|session_id| !keep(session_id))
            .cloned()
            .collect::<Vec<_>>();
        for session_id in removed {
            if let Some(initialization) = entries.remove(&session_id) {
                initialization.state.cancel();
            }
        }
        Ok(())
    }

    pub(crate) fn semaphore(&self) -> Arc<McpObserverSemaphore> {
        Arc::clone(&self.semaphore)
    }
}

impl Drop for HostedObserverRegistry {
    fn drop(&mut self) {
        if let Ok(entries) = self.entries.get_mut() {
            for initialization in std::mem::take(entries).into_values() {
                initialization.state.cancel();
            }
        }
    }
}

impl fmt::Debug for HostedObserverRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedObserverRegistry")
    }
}

pub(crate) type HostedGenericObserverBackend =
    dyn McpObserverBackend<Fence = DiscoveryCatalogFence>;

enum HostedObserverCall {
    Service {
        request: HostedObserverServiceRequest,
        completion: oneshot::Sender<Result<HostedObserverServiceResponse, McpBackendError>>,
    },
    GenericDiscovery {
        backend: Arc<HostedGenericObserverBackend>,
        inventory: McpObservedInventory,
        cursor: Option<[u8; 16]>,
        prior_fence: Option<DiscoveryCatalogFence>,
        completion: oneshot::Sender<
            Result<
                McpCompactObservationResult<DiscoveryCatalogFence>,
                crate::McpObserverBackendError,
            >,
        >,
    },
    GenericResource {
        backend: Arc<HostedGenericObserverBackend>,
        uri: String,
        completion: oneshot::Sender<
            Result<McpSubscribedResourceObservation, crate::McpObserverBackendError>,
        >,
    },
}

#[derive(Clone)]
struct HostedObserverServiceBridge {
    sender: mpsc::Sender<HostedObserverCall>,
    physical_calls: McpObserverPhysicalCallHandle,
}

impl HostedObserverServiceCaller for HostedObserverServiceBridge {
    fn call<'a>(
        &'a self,
        request: HostedObserverServiceRequest,
    ) -> McpBackendFuture<'a, HostedObserverServiceResponse> {
        Box::pin(async move {
            self.physical_calls
                .charge()
                .map_err(|_| McpBackendError::Cancelled)?;
            let (completion, response) = oneshot::channel();
            self.sender
                .send(HostedObserverCall::Service {
                    request,
                    completion,
                })
                .await
                .map_err(|_| McpBackendError::Cancelled)?;
            response.await.map_err(|_| McpBackendError::Cancelled)?
        })
    }
}

struct HostedGenericObserverBridge {
    backend: Arc<HostedGenericObserverBackend>,
    sender: mpsc::Sender<HostedObserverCall>,
    physical_call_budget: Arc<McpObserverPhysicalCallBudget>,
}

impl McpObserverBackend for HostedGenericObserverBridge {
    type Fence = DiscoveryCatalogFence;

    fn discover_compact<'a>(
        &'a self,
        inventory: McpObservedInventory,
        request: McpCompactObservationRequest<'a, Self::Fence>,
    ) -> McpObserverBackendFuture<'a, McpCompactObservationResult<Self::Fence>> {
        Box::pin(async move {
            self.physical_call_budget
                .compact_discovery()
                .charge()
                .map_err(|_| crate::McpObserverBackendError::Cancelled)?;
            let (completion, response) = oneshot::channel();
            self.sender
                .send(HostedObserverCall::GenericDiscovery {
                    backend: Arc::clone(&self.backend),
                    inventory,
                    cursor: request.cursor(),
                    prior_fence: request.prior_fence().cloned(),
                    completion,
                })
                .await
                .map_err(|_| crate::McpObserverBackendError::Cancelled)?;
            response
                .await
                .map_err(|_| crate::McpObserverBackendError::Cancelled)?
        })
    }

    fn observe_subscribed_resource<'a>(
        &'a self,
        uri: &'a str,
    ) -> McpObserverBackendFuture<'a, McpSubscribedResourceObservation> {
        Box::pin(async move {
            self.physical_call_budget
                .subscribed_resource()
                .charge()
                .map_err(|_| crate::McpObserverBackendError::Cancelled)?;
            let (completion, response) = oneshot::channel();
            self.sender
                .send(HostedObserverCall::GenericResource {
                    backend: Arc::clone(&self.backend),
                    uri: uri.to_owned(),
                    completion,
                })
                .await
                .map_err(|_| crate::McpObserverBackendError::Cancelled)?;
            response
                .await
                .map_err(|_| crate::McpObserverBackendError::Cancelled)?
        })
    }
}

struct HostedObserverBackend {
    service: Arc<HostedServiceMcpBackend>,
    sender: mpsc::Sender<HostedObserverCall>,
    physical_call_budget: Arc<McpObserverPhysicalCallBudget>,
}

impl McpObserverBackend for HostedObserverBackend {
    type Fence = DiscoveryCatalogFence;

    fn discover_compact<'a>(
        &'a self,
        inventory: McpObservedInventory,
        request: McpCompactObservationRequest<'a, Self::Fence>,
    ) -> McpObserverBackendFuture<'a, McpCompactObservationResult<Self::Fence>> {
        Box::pin(async move {
            let bridge = HostedObserverServiceBridge {
                sender: self.sender.clone(),
                physical_calls: self.physical_call_budget.compact_discovery(),
            };
            self.service
                .observe_compact_with(&bridge, inventory, request)
                .await
        })
    }

    fn observe_subscribed_resource<'a>(
        &'a self,
        uri: &'a str,
    ) -> McpObserverBackendFuture<'a, McpSubscribedResourceObservation> {
        Box::pin(async move {
            let bridge = HostedObserverServiceBridge {
                sender: self.sender.clone(),
                physical_calls: self.physical_call_budget.subscribed_resource(),
            };
            self.service
                .observe_subscribed_resource_with(&bridge, uri)
                .await
        })
    }
}

struct HostedObserverScheduler {
    clock: Arc<dyn McpMonotonicClock>,
    next_tick_at: Instant,
}

impl HostedObserverScheduler {
    fn new(clock: Arc<dyn McpMonotonicClock>) -> Result<Self, HostedObserverLifecycleError> {
        let next_tick_at = Instant::now()
            .checked_add(MCP_OBSERVER_TICK_INTERVAL)
            .ok_or(HostedObserverLifecycleError)?;
        Ok(Self {
            clock,
            next_tick_at,
        })
    }
}

impl McpObserverScheduler for HostedObserverScheduler {
    fn next_tick(&mut self) -> McpObserverSchedulerFuture<'_> {
        Box::pin(async move {
            tokio::time::sleep_until(self.next_tick_at).await;
            let now = self
                .clock
                .now()
                .map_err(|_| crate::McpObserverLoopError::ClockUnavailable)?;
            self.next_tick_at = Instant::now()
                .checked_add(MCP_OBSERVER_TICK_INTERVAL)
                .ok_or(crate::McpObserverLoopError::ClockUnavailable)?;
            Ok(now)
        })
    }
}

pub(crate) enum HostedObserverBodyAction {
    Service {
        request: HostedObserverServiceRequest,
        completion: oneshot::Sender<Result<HostedObserverServiceResponse, McpBackendError>>,
    },
    GenericDiscovery {
        backend: Arc<HostedGenericObserverBackend>,
        inventory: McpObservedInventory,
        cursor: Option<[u8; 16]>,
        prior_fence: Option<DiscoveryCatalogFence>,
        completion: oneshot::Sender<
            Result<
                McpCompactObservationResult<DiscoveryCatalogFence>,
                crate::McpObserverBackendError,
            >,
        >,
    },
    GenericResource {
        backend: Arc<HostedGenericObserverBackend>,
        uri: String,
        completion: oneshot::Sender<
            Result<McpSubscribedResourceObservation, crate::McpObserverBackendError>,
        >,
    },
    /// Authority-free marker emitted after a fresh admitted observer pass.
    ///
    /// The response body reauthenticates and checks the exact session binding
    /// before the delayed transport send. It does not make a notification-only
    /// service call, which would escape the accepted observer call budget.
    Notification {
        peer: Peer<RoleServer>,
        notification: McpObserverNotification,
    },
}

pub(crate) struct HostedObserverResponseState {
    state: Arc<McpObserverState>,
    peer: Peer<RoleServer>,
    calls: mpsc::Receiver<HostedObserverCall>,
    notifications: McpObserverNotificationReceiver,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
    _physical_call_budget: Arc<McpObserverPhysicalCallBudget>,
    terminal: bool,
}

impl HostedObserverResponseState {
    pub(crate) fn start_service(
        initialization: HostedObserverInitialization,
        service: Arc<HostedServiceMcpBackend>,
        clock: Arc<dyn McpMonotonicClock>,
        semaphore: Arc<McpObserverSemaphore>,
    ) -> Result<Self, HostedObserverLifecycleError> {
        let (service_sender, calls) = mpsc::channel(1);
        let physical_call_budget = Arc::new(McpObserverPhysicalCallBudget::new());
        let backend = Arc::new(HostedObserverBackend {
            service,
            sender: service_sender,
            physical_call_budget: Arc::clone(&physical_call_budget),
        });
        Self::start_with_backend(
            initialization,
            backend,
            calls,
            clock,
            semaphore,
            physical_call_budget,
        )
    }

    pub(crate) fn start_generic(
        initialization: HostedObserverInitialization,
        backend: Arc<HostedGenericObserverBackend>,
        clock: Arc<dyn McpMonotonicClock>,
        semaphore: Arc<McpObserverSemaphore>,
    ) -> Result<Self, HostedObserverLifecycleError> {
        let (sender, calls) = mpsc::channel(1);
        let physical_call_budget = Arc::new(McpObserverPhysicalCallBudget::new());
        let bridge = Arc::new(HostedGenericObserverBridge {
            backend,
            sender,
            physical_call_budget: Arc::clone(&physical_call_budget),
        });
        Self::start_with_backend(
            initialization,
            bridge,
            calls,
            clock,
            semaphore,
            physical_call_budget,
        )
    }

    fn start_with_backend<Backend>(
        initialization: HostedObserverInitialization,
        backend: Arc<Backend>,
        calls: mpsc::Receiver<HostedObserverCall>,
        clock: Arc<dyn McpMonotonicClock>,
        semaphore: Arc<McpObserverSemaphore>,
        physical_call_budget: Arc<McpObserverPhysicalCallBudget>,
    ) -> Result<Self, HostedObserverLifecycleError>
    where
        Backend: McpObserverBackend<Fence = DiscoveryCatalogFence> + ?Sized,
    {
        let started_at = match clock.now() {
            Ok(started_at) => started_at,
            Err(_) => {
                initialization.state.cancel();
                return Err(HostedObserverLifecycleError);
            }
        };
        let mut scheduler = match HostedObserverScheduler::new(clock) {
            Ok(scheduler) => scheduler,
            Err(error) => {
                initialization.state.cancel();
                return Err(error);
            }
        };
        let (notification_sender, notifications) =
            mcp_observer_notification_channel(&initialization.state)
                .map_err(|_| HostedObserverLifecycleError)?;
        let notification_sink: Arc<dyn McpObserverNotificationSink> = Arc::new(notification_sender);
        let mut observer = match McpObserverLoop::new(
            backend,
            Arc::clone(&initialization.state),
            semaphore,
            notification_sink,
            started_at,
        ) {
            Ok(observer) => observer,
            Err(_) => {
                initialization.state.cancel();
                return Err(HostedObserverLifecycleError);
            }
        };
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task_state = Arc::clone(&initialization.state);
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = task_cancellation.cancelled() => observer.cancel(),
                _ = drive_mcp_observer(&mut observer, &mut scheduler) => {}
            }
            task_state.cancel();
        });
        Ok(Self {
            state: initialization.state,
            peer: initialization.peer,
            calls,
            notifications,
            cancellation,
            task,
            _physical_call_budget: physical_call_budget,
            terminal: false,
        })
    }

    pub(crate) fn poll_action(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<HostedObserverBodyAction>> {
        if self.terminal {
            return Poll::Ready(None);
        }
        match self.calls.poll_recv(context) {
            Poll::Ready(Some(HostedObserverCall::Service {
                request,
                completion,
            })) => {
                return Poll::Ready(Some(HostedObserverBodyAction::Service {
                    request,
                    completion,
                }));
            }
            Poll::Ready(Some(HostedObserverCall::GenericDiscovery {
                backend,
                inventory,
                cursor,
                prior_fence,
                completion,
            })) => {
                return Poll::Ready(Some(HostedObserverBodyAction::GenericDiscovery {
                    backend,
                    inventory,
                    cursor,
                    prior_fence,
                    completion,
                }));
            }
            Poll::Ready(Some(HostedObserverCall::GenericResource {
                backend,
                uri,
                completion,
            })) => {
                return Poll::Ready(Some(HostedObserverBodyAction::GenericResource {
                    backend,
                    uri,
                    completion,
                }));
            }
            Poll::Ready(None) => {
                self.shutdown();
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }
        match self.notifications.poll_recv(context) {
            Poll::Ready(Some(notification)) => {
                Poll::Ready(Some(HostedObserverBodyAction::Notification {
                    peer: self.peer.clone(),
                    notification,
                }))
            }
            Poll::Ready(None) => {
                self.shutdown();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    pub(crate) fn shutdown(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.cancellation.cancel();
        self.state.cancel();
        self.calls.close();
        self.task.abort();
    }
}

impl Drop for HostedObserverResponseState {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl fmt::Debug for HostedObserverResponseState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedObserverResponseState([REDACTED])")
    }
}

pub(crate) async fn emit_hosted_notification(
    peer: Peer<RoleServer>,
    notification: McpObserverNotification,
) -> Result<(), HostedObserverLifecycleError> {
    match notification {
        McpObserverNotification::ToolsListChanged => peer.notify_tool_list_changed().await,
        McpObserverNotification::ResourcesListChanged => peer.notify_resource_list_changed().await,
        McpObserverNotification::ResourceUpdated(update) => {
            peer.notify_resource_updated(ResourceUpdatedNotificationParam::new(update.into_uri()))
                .await
        }
    }
    .map_err(|_| HostedObserverLifecycleError)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HostedObserverLifecycleError;

impl fmt::Display for HostedObserverLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP observer is unavailable")
    }
}

impl std::error::Error for HostedObserverLifecycleError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hosted_service_bridge_charges_before_dispatch_and_never_refunds() {
        let physical_call_budget = Arc::new(McpObserverPhysicalCallBudget::new());
        let (sender, mut calls) = mpsc::channel(1);
        let bridge = HostedObserverServiceBridge {
            sender,
            physical_calls: physical_call_budget.compact_discovery(),
        };

        let first_bridge = bridge.clone();
        let first = tokio::spawn(async move {
            first_bridge
                .call(HostedObserverServiceRequest::Health)
                .await
        });
        let call = calls.recv().await.expect("one dispatched call");
        let HostedObserverCall::Service { completion, .. } = call else {
            panic!("service call");
        };
        assert_eq!(physical_call_budget.calls(), 1);
        let _ = completion.send(Err(McpBackendError::Cancelled));
        assert!(first.await.expect("caller task").is_err());
        assert_eq!(physical_call_budget.calls(), 1);

        assert!(
            bridge
                .call(HostedObserverServiceRequest::Health)
                .await
                .is_err()
        );
        assert!(calls.try_recv().is_err());
        assert_eq!(physical_call_budget.calls(), 1);
    }
}
