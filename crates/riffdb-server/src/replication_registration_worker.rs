//! Bounded continuation of stored policy; never a second authoritative writer.
use crate::replication_publication::ReplicationPublishedSnapshots;
use riffdb_commit::{ControlPlaneExecutionErrorKind, ControlPlaneExecutor};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use tokio::sync::Notify;

const MAX_STEPS_PER_PASS: usize = 16;
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Eq, PartialEq)]
enum Step {
    Complete,
    RetryLater,
}

struct Cancellation {
    stopped: AtomicBool,
    wake: Notify,
}
impl Cancellation {
    fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            wake: Notify::new(),
        }
    }
    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        // One retained permit covers cancellation before the sole wait begins.
        self.wake.notify_one();
    }
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

/// One graph-owned observer thread; stopped and joined before coordinator drain.
pub(crate) struct RunningRegistrationMaintenance {
    cancellation: Arc<Cancellation>,
    thread: Option<JoinHandle<()>>,
}
impl RunningRegistrationMaintenance {
    pub(crate) fn start(
        publications: Option<ReplicationPublishedSnapshots>,
        executor: ControlPlaneExecutor,
    ) -> Self {
        let cancellation = Arc::new(Cancellation::new());
        let thread = publications.and_then(|mut publications| {
            let stop = Arc::clone(&cancellation);
            let runtime = tokio::runtime::Handle::current();
            let result = std::thread::Builder::new()
                .name("riffdb-registration".into())
                .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
                .spawn(move || {
                    let result = runtime.block_on(pump(
                        || observe(&mut publications),
                        || async {
                            let Some(capacity) =
                                unless_cancelled(executor.reserve_capacity(), &stop).await
                            else {
                                return Ok(Step::Complete);
                            };
                            let receipt = capacity
                                .map_err(|_| ())?
                                .submit_replication_maintenance()
                                .map_err(|_| ())?;
                            // Accepted work drains even during shutdown. This observer
                            // cannot select a registration, policy, time or sequence.
                            match receipt.completion().await {
                                Ok(_) => Ok(Step::Complete),
                                Err(error)
                                    if error.kind()
                                        == ControlPlaneExecutionErrorKind::StorageUnavailable =>
                                {
                                    Ok(Step::RetryLater)
                                }
                                Err(_) => Err(()),
                            }
                        },
                        &stop,
                    ));
                    if result.is_err() && !stop.is_stopped() {
                        // No identities or storage sources enter telemetry. Do not
                        // infer an uncertain outcome; restart validates persisted policy.
                        eprintln!("riffdb-registration-v1\tstate=worker unavailable");
                    }
                });
            match result {
                Ok(thread) => Some(thread),
                Err(_) => {
                    eprintln!("riffdb-registration-v1\tstate=worker unavailable");
                    None
                }
            }
        });
        Self {
            cancellation,
            thread,
        }
    }
    pub(crate) fn shutdown(&mut self) {
        self.cancellation.stop();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            eprintln!("riffdb-registration-v1\tstate=worker failed");
        }
    }
}
impl Drop for RunningRegistrationMaintenance {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn observe(publications: &mut ReplicationPublishedSnapshots) -> Result<bool, ()> {
    let Some(pin) = publications.latest().map_err(|_| ())? else {
        return Ok(false);
    };
    let progress = pin.replication_source_progress_v3().map_err(|_| ())?;
    // The pin and watch guard are gone before coordinator capacity is reserved.
    // This hint is not authority: the coordinator revalidates under its barrier.
    Ok(progress.registration_maintenance_pending())
}

async fn unless_cancelled<T>(future: impl Future<Output = T>, stop: &Cancellation) -> Option<T> {
    if stop.is_stopped() {
        return None;
    }
    tokio::select! {
        biased;
        _ = stop.wake.notified() => None,
        result = future => Some(result),
    }
}

async fn pump<F: Future<Output = Result<Step, ()>>>(
    mut observe: impl FnMut() -> Result<bool, ()>,
    submit: impl Fn() -> F,
    stop: &Cancellation,
) -> Result<(), ()> {
    loop {
        for _ in 0..MAX_STEPS_PER_PASS {
            if stop.is_stopped() {
                return Ok(());
            }
            if !observe()? {
                break;
            }
            if submit().await? == Step::RetryLater {
                break;
            }
        }
        if unless_cancelled(tokio::time::sleep(RETRY_DELAY), stop)
            .await
            .is_none()
        {
            return Ok(());
        }
    }
}

#[cfg(test)]
#[path = "replication_registration_worker_tests.rs"]
mod tests;
