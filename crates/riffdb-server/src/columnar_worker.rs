//! Bounded owner for columnar projection apply catch-up and checkpoints.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_columnar::{ColumnarError, frontier_lag_sequences};
use riffdb_observability::{MetricRegistry, RequiredGauge};
use riffdb_types::FrontierPosition;

use crate::columnar_adapter::{ColumnarRuntime, is_holdback_active};

const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(25);
const CHECKPOINT_COMMIT_CADENCE: u64 = 4096;
const CHECKPOINT_POLL_CADENCE: u32 = 30;

/// Columnar-owned process-local worker state used by aggregate health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnarWorkerReadiness {
    Starting,
    Ready,
    Degraded,
    Stopped,
}

/// Cloneable observation handle with no worker or storage authority.
#[derive(Clone)]
pub(crate) struct ColumnarWorkerStatus {
    state: Arc<AtomicU8>,
}

impl ColumnarWorkerStatus {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(0)),
        }
    }

    pub(crate) fn publish(&self, readiness: ColumnarWorkerReadiness) {
        self.state.store(
            match readiness {
                ColumnarWorkerReadiness::Starting => 0,
                ColumnarWorkerReadiness::Ready => 1,
                ColumnarWorkerReadiness::Degraded => 2,
                ColumnarWorkerReadiness::Stopped => 3,
            },
            Ordering::Release,
        );
    }

    /// Returns the most recent bounded worker classification.
    pub(crate) fn readiness(&self) -> ColumnarWorkerReadiness {
        match self.state.load(Ordering::Acquire) {
            0 => ColumnarWorkerReadiness::Starting,
            1 => ColumnarWorkerReadiness::Ready,
            2 => ColumnarWorkerReadiness::Degraded,
            _ => ColumnarWorkerReadiness::Stopped,
        }
    }
}

impl fmt::Debug for ColumnarWorkerStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ColumnarWorkerStatus")
            .field("readiness", &self.readiness())
            .finish()
    }
}

struct StopState {
    requested: Mutex<bool>,
    changed: Condvar,
}

/// Owning guard for the one columnar apply/checkpoint thread.
#[must_use = "the columnar worker must be explicitly stopped and joined"]
pub(crate) struct RunningColumnarWorker {
    stop: Arc<StopState>,
    task: Option<JoinHandle<()>>,
    status: ColumnarWorkerStatus,
}

impl RunningColumnarWorker {
    /// Starts one bounded worker over already registered columnar engines.
    pub(crate) fn start(
        runtime: Arc<ColumnarRuntime>,
        metrics: Option<MetricRegistry>,
    ) -> Result<Self, ColumnarWorkerStartError> {
        let stop = Arc::new(StopState {
            requested: Mutex::new(false),
            changed: Condvar::new(),
        });
        let status = ColumnarWorkerStatus::new();
        let worker_stop = Arc::clone(&stop);
        let worker_status = status.clone();
        let task = thread::Builder::new()
            .name("riffdb-columnar".to_owned())
            .spawn(move || {
                let mut state = ColumnarWorkerState::new(runtime.as_ref());
                loop {
                    if stop_requested(&worker_stop) {
                        break;
                    }
                    worker_status.publish(
                        match run_columnar_pass(&runtime, metrics.as_ref(), &mut state) {
                            Ok(()) => ColumnarWorkerReadiness::Ready,
                            Err(_) => ColumnarWorkerReadiness::Degraded,
                        },
                    );
                    if wait_for_stop(&worker_stop, WORKER_POLL_INTERVAL) {
                        break;
                    }
                }
                worker_status.publish(ColumnarWorkerReadiness::Stopped);
            })
            .map_err(|_| ColumnarWorkerStartError)?;
        Ok(Self {
            stop,
            task: Some(task),
            status,
        })
    }

    /// Returns a least-authority aggregate-health observation handle.
    pub(crate) fn status(&self) -> ColumnarWorkerStatus {
        self.status.clone()
    }

    /// Requests termination and joins the worker before storage can be dropped.
    pub(crate) fn shutdown(mut self) -> Result<(), ColumnarWorkerShutdownError> {
        request_stop(&self.stop)?;
        let task = self
            .task
            .take()
            .expect("a running columnar worker retains one task");
        task.join().map_err(|_| ColumnarWorkerShutdownError)?;
        if self.status.readiness() == ColumnarWorkerReadiness::Stopped {
            Ok(())
        } else {
            Err(ColumnarWorkerShutdownError)
        }
    }
}

impl fmt::Debug for RunningColumnarWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunningColumnarWorker([DERIVED_AUTHORITY])")
    }
}

struct EngineCatchupState {
    last_published: FrontierPosition,
    commits_since_checkpoint: u64,
}

struct ColumnarWorkerState {
    engines: BTreeMap<String, EngineCatchupState>,
    polls_since_checkpoint: u32,
}

impl ColumnarWorkerState {
    fn new(runtime: &ColumnarRuntime) -> Self {
        let mut engines = BTreeMap::new();
        for (name, slot) in runtime.engines().unwrap_or_default() {
            let published = slot
                .lock_engine()
                .map(|engine| engine.published_frontier_position())
                .unwrap_or(FrontierPosition::BeforeFirst);
            engines.insert(
                name.clone(),
                EngineCatchupState {
                    last_published: published,
                    commits_since_checkpoint: 0,
                },
            );
        }
        Self {
            engines,
            polls_since_checkpoint: 0,
        }
    }
}

fn run_columnar_pass(
    runtime: &ColumnarRuntime,
    metrics: Option<&MetricRegistry>,
    state: &mut ColumnarWorkerState,
) -> Result<(), ColumnarWorkerError> {
    runtime
        .synchronize_active_vector_projections()
        .map_err(|_| ColumnarWorkerError::Registration)?;
    state.polls_since_checkpoint = state.polls_since_checkpoint.saturating_add(1);
    let force_checkpoint = state.polls_since_checkpoint >= CHECKPOINT_POLL_CADENCE;
    if force_checkpoint {
        state.polls_since_checkpoint = 0;
    }

    let apply_source = runtime.apply_source();
    let head = read_head(runtime)?;
    let mut max_lag = 0u64;

    for (name, slot) in runtime
        .engines()
        .map_err(|_| ColumnarWorkerError::Unavailable)?
    {
        let catchup = state
            .engines
            .entry(name.clone())
            .or_insert(EngineCatchupState {
                last_published: FrontierPosition::BeforeFirst,
                commits_since_checkpoint: 0,
            });
        let published_after;
        {
            let mut engine = slot
                .lock_engine()
                .map_err(|_| ColumnarWorkerError::Integrity)?;
            let processed_before = engine.processed_frontier().position();
            let published_before = engine.published_frontier_position();
            let progress = engine
                .apply_available(apply_source)
                .map_err(ColumnarWorkerError::Apply)?;
            published_after = progress.published_frontier;
            let commits_applied = sequences_advanced(processed_before, progress.processed);
            catchup.commits_since_checkpoint = catchup
                .commits_since_checkpoint
                .saturating_add(commits_applied);

            if published_after != published_before {
                let _ = runtime.notifier().notify(&name);
            }
            catchup.last_published = published_after;

            let should_checkpoint =
                force_checkpoint || catchup.commits_since_checkpoint >= CHECKPOINT_COMMIT_CADENCE;
            if should_checkpoint {
                match engine.checkpoint() {
                    Ok(_) => {
                        catchup.commits_since_checkpoint = 0;
                    }
                    Err(error) if is_holdback_active(&error) => {
                        // HoldbackActive: retry after the next apply pull.
                    }
                    Err(error) => return Err(ColumnarWorkerError::Apply(error)),
                }
            }
        }

        if let Some(lag) = frontier_lag_sequences(published_after, head) {
            max_lag = max_lag.max(lag);
        }
    }

    if let Some(metrics) = metrics {
        metrics.set_required_gauge(RequiredGauge::ProjectionLagCommits, max_lag);
    }
    Ok(())
}

fn read_head(runtime: &ColumnarRuntime) -> Result<FrontierPosition, ColumnarWorkerError> {
    runtime
        .read_application_head()
        .map_err(|error| match error {
            riffdb_service::ColumnarPortError::Unavailable => ColumnarWorkerError::Unavailable,
            riffdb_service::ColumnarPortError::Integrity => ColumnarWorkerError::Integrity,
        })
}

const fn sequences_advanced(from: FrontierPosition, to: FrontierPosition) -> u64 {
    match (from, to) {
        (FrontierPosition::BeforeFirst, FrontierPosition::AppliedThrough(to_seq)) => to_seq.get(),
        (FrontierPosition::AppliedThrough(from_seq), FrontierPosition::AppliedThrough(to_seq)) => {
            to_seq.get().saturating_sub(from_seq.get())
        }
        _ => 0,
    }
}

fn stop_requested(stop: &StopState) -> bool {
    stop.requested.lock().map_or(true, |requested| *requested)
}

fn wait_for_stop(stop: &StopState, duration: Duration) -> bool {
    let Ok(requested) = stop.requested.lock() else {
        return true;
    };
    if *requested {
        return true;
    }
    match stop.changed.wait_timeout(requested, duration) {
        Ok((requested, _)) => *requested,
        Err(_) => true,
    }
}

fn request_stop(stop: &StopState) -> Result<(), ColumnarWorkerShutdownError> {
    let mut requested = stop
        .requested
        .lock()
        .map_err(|_| ColumnarWorkerShutdownError)?;
    *requested = true;
    stop.changed.notify_all();
    Ok(())
}

/// Closed worker construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColumnarWorkerStartError;

impl fmt::Display for ColumnarWorkerStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the columnar worker could not start")
    }
}

impl std::error::Error for ColumnarWorkerStartError {}

/// Closed worker shutdown failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColumnarWorkerShutdownError;

impl fmt::Display for ColumnarWorkerShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the columnar worker did not stop cleanly")
    }
}

impl std::error::Error for ColumnarWorkerShutdownError {}

enum ColumnarWorkerError {
    Apply(ColumnarError),
    Registration,
    Unavailable,
    Integrity,
}

impl fmt::Debug for ColumnarWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let class = match self {
            Self::Apply(error) => {
                let _ = error;
                "apply"
            }
            Self::Registration => "registration",
            Self::Unavailable => "unavailable",
            Self::Integrity => "integrity",
        };
        formatter
            .debug_struct("ColumnarWorkerError")
            .field("class", &class)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequences_advanced_counts_contiguous_catch_up() {
        assert_eq!(
            sequences_advanced(
                FrontierPosition::BeforeFirst,
                FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first())
            ),
            1
        );
        assert_eq!(
            sequences_advanced(
                FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
                FrontierPosition::AppliedThrough(
                    riffdb_types::CommitSequence::new(5).expect("sequence")
                )
            ),
            4
        );
    }

    #[test]
    fn worker_errors_never_render_sources() {
        assert_eq!(
            format!("{:?}", ColumnarWorkerError::Integrity),
            "ColumnarWorkerError { class: \"integrity\" }"
        );
    }
}
