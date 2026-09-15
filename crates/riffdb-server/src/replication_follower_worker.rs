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
        let capacity = Arc::clone(receiver.owner.as_ref().ok_or_else(busy)?.permit.semaphore());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(run(receiver, peer, stopped, capacity));
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

    /// Interrupts network receive/backoff, then waits for actual engine release.
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
    mut stopped: oneshot::Receiver<()>,
    capacity: Arc<Semaphore>,
) -> Result<(), Failure> {
    let mut retry = INITIAL_RETRY;
    let outcome = loop {
        let result = tokio::select! {
            biased;
            _ = &mut stopped => break Ok(()),
            result = receiver.advance(peer.as_ref()) => result,
        };
        match result {
            Ok(Some(_)) => {
                retry = INITIAL_RETRY;
                continue;
            }
            Ok(None) => {}
            Err(error) if transient(error) && receiver.owner.is_some() => {}
            Err(error) => break Err(error),
        }
        // EOF is also paced: an idle or repeatedly closing source cannot cause
        // a reconnect spin or unbounded acknowledgement/attachment churn.
        tokio::select! {
            biased;
            _ = &mut stopped => break Ok(()),
            () = tokio::time::sleep(retry) => {},
        }
        retry = retry.saturating_mul(2).min(MAX_RETRY);
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
