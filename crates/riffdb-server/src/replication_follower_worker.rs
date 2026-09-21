//! Supervised continuous tail: one receiver, bounded retry, actual custody drain.
use super::*;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const INITIAL_RETRY: Duration = Duration::from_millis(250);
const MAX_RETRY: Duration = Duration::from_secs(30);

/// Owns a validated continuous receiver until its actual storage work ends.
/// This task is not application readiness and never constructs a primary writer.
/// Dropping the handle requests shutdown; the task retains all custody needed
/// to drain a cancelled blocking operation even when its caller disappears.
pub struct RunningFollowerReceiver {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), Failure>>>,
    result: Option<Result<(), Failure>>,
}
impl RunningFollowerReceiver {
    /// Starts on the current Tokio runtime after bootstrap publication or fully
    /// validated reopen. The peer must own verified transport and credentials.
    pub fn start(
        receiver: FollowerReceiver,
        peer: Arc<dyn ReplicationSourcePort>,
    ) -> Result<Self, Failure> {
        Self::start_inner(receiver, peer, None)
    }

    /// Joins receiver completion to the daemon's existing per-generation fence.
    pub(crate) fn start_with_routing(
        receiver: FollowerReceiver,
        peer: Arc<dyn ReplicationSourcePort>,
        routing: crate::runtime_support::RuntimeRoutingState,
    ) -> Result<Self, Failure> {
        Self::start_inner(receiver, peer, Some(routing))
    }

    fn start_inner(
        receiver: FollowerReceiver,
        peer: Arc<dyn ReplicationSourcePort>,
        routing: Option<crate::runtime_support::RuntimeRoutingState>,
    ) -> Result<Self, Failure> {
        let capacity = Arc::clone(receiver.owner.as_ref().ok_or_else(busy)?.permit.semaphore());
        let (stop, stopped) = oneshot::channel();
        let guard = ReceiverRoutingGuard(routing);
        let task = tokio::spawn(async move {
            let _guard = guard;
            run(receiver, peer, stopped, capacity).await
        });
        Ok(Self {
            stop: Some(stop),
            task: Some(task),
            result: None,
        })
    }

    /// Observes a terminal refusal or completed shutdown, including storage drain.
    /// Cancellation of this wait retains the task and may be retried.
    pub async fn finished(&mut self) -> Result<(), Failure> {
        if let Some(result) = self.result {
            return result;
        }
        let result = match self.task.as_mut().ok_or_else(busy)?.await {
            Ok(result) => result,
            Err(_) => Err(busy()),
        };
        self.task = None;
        self.result = Some(result);
        result
    }

    /// Interrupts network receive/backoff, drains any complete received frame,
    /// then waits for actual engine release.
    /// The normal follower close never writes a source CLEAN lifecycle record.
    pub async fn shutdown(mut self) -> Result<(), Failure> {
        self.request_stop();
        self.finished().await
    }

    fn request_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// Captured before spawning so even an unpolled or panicked task fences routing.
struct ReceiverRoutingGuard(Option<crate::runtime_support::RuntimeRoutingState>);
impl Drop for ReceiverRoutingGuard {
    fn drop(&mut self) {
        use riffdb_service::{AuthoritativeReadinessFailure, ServiceHealthHooks};
        if let Some(routing) = &self.0 {
            routing.fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
        }
    }
}
impl Drop for RunningFollowerReceiver {
    fn drop(&mut self) {
        self.request_stop();
    }
}
impl std::fmt::Debug for RunningFollowerReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RunningFollowerReceiver([redacted])")
    }
}

async fn run(
    mut receiver: FollowerReceiver,
    peer: Arc<dyn ReplicationSourcePort>,
    stopped: oneshot::Receiver<()>,
    capacity: Arc<Semaphore>,
) -> Result<(), Failure> {
    let mut stopped = StopSignal::observed(stopped);
    let mut retry = INITIAL_RETRY;
    let outcome = loop {
        if stopped.requested() {
            break Ok(());
        }
        // Stop is observed only at network waits. Once receive returns a complete
        // frame, keep the same owner through apply and local acknowledgement.
        let result = receiver
            .advance_until_stopped(peer.as_ref(), &mut stopped)
            .await;
        match result {
            Ok(Some(_)) => {
                retry = INITIAL_RETRY;
                continue;
            }
            Ok(None) => {
                // A healthy source can close at its idle/lifetime limit. Reset
                // failure backoff so prior silence does not delay new commits.
                retry = INITIAL_RETRY;
            }
            Err(error) if transient(error) && receiver.owner.is_some() => {}
            Err(error) => break Err(error),
        }
        // EOF is also paced: an idle or repeatedly closing source cannot cause
        // a reconnect spin or unbounded acknowledgement/attachment churn.
        tokio::select! {
            biased;
            _ = stopped.wait() => break Ok(()),
            () = tokio::time::sleep(retry) => {},
        }
        if result.is_err() {
            retry = retry.saturating_mul(2).min(MAX_RETRY);
        }
    };
    let close = if receiver.owner.is_some() {
        receiver.close().await.map_err(storage)
    } else {
        // A cancelled advance has moved custody into its blocking operation.
        // Dropping this shell cannot release that operation's capacity early.
        drop(receiver);
        Ok(())
    };
    // Custody drops storage before its permit. This acquisition proves that a
    // detached blocking apply/close has ended, rather than guessing from a timer.
    let drained = capacity.acquire_owned().await.map_err(|_| busy())?;
    drop(drained);
    outcome.and(close)
}

/// A sticky supervisor stop. An unobserved signal preserves direct callers'
/// existing cancellation behavior; only the owning worker drives graceful drain.
pub(super) struct StopSignal {
    receiver: Option<oneshot::Receiver<()>>,
    requested: bool,
}
impl StopSignal {
    pub(super) fn unobserved() -> Self {
        Self {
            receiver: None,
            requested: false,
        }
    }
    fn observed(receiver: oneshot::Receiver<()>) -> Self {
        Self {
            receiver: Some(receiver),
            requested: false,
        }
    }
    fn requested(&mut self) -> bool {
        if self.requested {
            return true;
        }
        self.requested = self.receiver.as_mut().is_some_and(|receiver| {
            !matches!(
                receiver.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            )
        });
        self.requested
    }
    async fn wait(&mut self) {
        if self.requested() {
            return;
        }
        match self.receiver.as_mut() {
            Some(receiver) => {
                let _ = receiver.await;
            }
            None => std::future::pending().await,
        }
        self.requested = true;
    }
    pub(super) async fn network<T>(
        &mut self,
        operation: impl std::future::Future<Output = T>,
    ) -> Option<T> {
        tokio::select! {
            biased;
            () = self.wait() => None,
            result = operation => Some(result),
        }
    }
}
