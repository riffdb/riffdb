//! Process-owned latest cumulative metrics, isolated from service runtimes.
use crate::config::TelemetryPushConfig;
#[cfg(test)]
use crate::telemetry_http::ExportFailure;
use crate::telemetry_http::{BatchContext, OtlpHttpClient, SetupFailure};
use crate::telemetry_metric_batches::split_snapshot;
use riffdb_observability::{
    Observability, RequiredGauge, TelemetryDisposition as Disposition, TelemetrySignal as Signal,
    process_telemetry_delivery_metrics as metrics,
};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);
#[derive(Clone)]
struct Source {
    observability: Arc<Observability>,
    database_path: PathBuf,
}
struct State {
    source: Option<Source>,
    generation: u64,
    active: bool,
    changed: bool,
    closing: Option<Instant>,
    done: bool,
    pending_points: usize,
}
#[cfg(test)]
type ReceiptObserver =
    std::sync::mpsc::SyncSender<Result<crate::telemetry_http::DeliveryReceipt, ExportFailure>>;
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    batch: Arc<BatchContext>,
    config: TelemetryPushConfig,
    #[cfg(test)]
    receipts: Mutex<Option<ReceiptObserver>>,
}
static PROCESS_METRICS: OnceLock<Weak<Shared>> = OnceLock::new();
#[derive(Clone)]
pub(crate) struct MetricsWorker(Arc<Shared>);
/// Unique root ownership also stops an idle worker if process setup fails.
pub(crate) struct ProcessMetricsWorker(MetricsWorker);
impl ProcessMetricsWorker {
    pub(crate) fn begin_drain(&self, deadline: Instant) {
        self.0.begin_drain(deadline);
    }
    pub(crate) fn close(&self, deadline: Instant) -> bool {
        self.0.close(deadline)
    }
}
impl Drop for ProcessMetricsWorker {
    fn drop(&mut self) {
        self.0.begin_drain(Instant::now());
    }
}
pub(crate) struct MetricsSource {
    worker: MetricsWorker,
    generation: u64,
    standalone: bool,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum StartFailure {
    Credential,
    TrustRoot,
    Worker,
}
impl From<SetupFailure> for StartFailure {
    fn from(value: SetupFailure) -> Self {
        match value {
            SetupFailure::Credential => Self::Credential,
            SetupFailure::TrustRoot => Self::TrustRoot,
        }
    }
}
impl MetricsWorker {
    pub(crate) fn install(
        config: &TelemetryPushConfig,
    ) -> Result<ProcessMetricsWorker, StartFailure> {
        let worker = Self::start(config)?;
        if PROCESS_METRICS.set(Arc::downgrade(&worker.0)).is_err() {
            worker.begin_drain(Instant::now());
            return Err(StartFailure::Worker);
        }
        Ok(ProcessMetricsWorker(worker))
    }
    fn start(config: &TelemetryPushConfig) -> Result<Self, StartFailure> {
        let batch = Arc::new(BatchContext::new());
        let client = OtlpHttpClient::new(Signal::Metrics, config, EXPORT_TIMEOUT, batch.clone())?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                source: None,
                generation: 0,
                active: false,
                changed: false,
                closing: None,
                done: false,
                pending_points: 0,
            }),
            wake: Condvar::new(),
            batch,
            config: config.clone(),
            #[cfg(test)]
            receipts: Mutex::new(None),
        });
        let worker = Self(shared.clone());
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("riffdb-otlp-metrics".into())
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .max_blocking_threads(1)
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        let _ = ready.send(false);
                        return;
                    }
                };
                if ready.send(true).is_err() {
                    runtime.shutdown_background();
                    return;
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(&shared, &runtime, &client)
                }));
                runtime.shutdown_background();
                let mut state = shared
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                account_unflushed(&mut state);
                state.done = true;
                shared.wake.notify_all();
                drop(state);
                if result.is_err() {
                    metrics().record(Signal::Metrics, Disposition::Transport, 1);
                    report(Some(Disposition::Transport));
                }
            })
            .map_err(|_| StartFailure::Worker)?;
        if started.recv_timeout(Duration::from_secs(1)) != Ok(true) {
            worker.begin_drain(Instant::now());
            return Err(StartFailure::Worker);
        }
        metrics().set_enabled(Signal::Metrics, true);
        Ok(worker)
    }
    fn attach(&self, source: Source, standalone: bool) -> Result<MetricsSource, StartFailure> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closing.is_some() || state.done {
            return Err(StartFailure::Worker);
        }
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(StartFailure::Worker)?;
        state.source = Some(source);
        state.active = true;
        state.changed = true;
        let generation = state.generation;
        self.0.wake.notify_all();
        Ok(MetricsSource {
            worker: self.clone(),
            generation,
            standalone,
        })
    }
    pub(crate) fn begin_drain(&self, deadline: Instant) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let first = state.closing.is_none();
        let deadline = state.closing.map_or(deadline, |old| old.min(deadline));
        state.closing = Some(deadline);
        if first {
            state.changed = state.source.is_some();
        }
        self.0.batch.begin_drain(deadline);
        self.0.wake.notify_all();
    }
    pub(crate) fn close(&self, deadline: Instant) -> bool {
        self.begin_drain(deadline);
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.done {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            state = self
                .0
                .wake
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        if !state.done {
            account_unflushed(&mut state);
        }
        state.done
    }
}
impl MetricsSource {
    pub(crate) fn start(
        observability: Arc<Observability>,
        database_path: PathBuf,
        config: &TelemetryPushConfig,
    ) -> Result<Self, StartFailure> {
        let shared = PROCESS_METRICS.get().and_then(Weak::upgrade);
        let (worker, standalone) = match shared {
            Some(shared) if shared.config == *config => (MetricsWorker(shared), false),
            Some(_) => return Err(StartFailure::Worker),
            None => (MetricsWorker::start(config)?, true),
        };
        worker.attach(
            Source {
                observability,
                database_path,
            },
            standalone,
        )
    }
    #[cfg(test)]
    pub(crate) fn observe_receipts(&self, sender: ReceiptObserver) {
        *self.worker.0.receipts.lock().unwrap() = Some(sender);
    }
    /// Stop periodic capture after this graph drains. A process-owned worker
    /// retains the latest cumulative source for the root's final shared drain.
    pub(crate) fn stop(self) -> Option<MetricsWorker> {
        let mut state = self
            .worker
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.generation == self.generation {
            state.active = false;
        }
        drop(state);
        if self.standalone {
            self.worker.begin_drain(Instant::now() + EXPORT_TIMEOUT);
            Some(self.worker)
        } else {
            None
        }
    }
}
fn account_unflushed(state: &mut State) {
    metrics().record(
        Signal::Metrics,
        Disposition::Unflushed,
        state.pending_points as u64,
    );
    state.pending_points = 0;
    metrics().set_depth(Signal::Metrics, 0);
    metrics().set_inflight(Signal::Metrics, 0);
}
fn report(failure: Option<Disposition>) {
    crate::operational_logging::emit(crate::operational_logging::OperationalEvent::ExportState {
        signal: Signal::Metrics,
        failure,
    });
}
fn run(shared: &Shared, runtime: &tokio::runtime::Runtime, client: &OtlpHttpClient) {
    let mut next = Instant::now();
    let mut reporting = crate::telemetry_worker::DeliveryReporter::default();
    loop {
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if state
                .closing
                .is_some_and(|deadline| deadline <= Instant::now())
            {
                return;
            }
            if state.closing.is_some() && !state.changed {
                return;
            }
            if state.source.is_some() && (state.changed || (state.active && Instant::now() >= next))
            {
                break;
            }
            if state.source.is_none() && state.closing.is_some() {
                return;
            }
            let wait = if state.active {
                next.saturating_duration_since(Instant::now())
            } else {
                shared.config.interval()
            };
            state = shared
                .wake
                .wait_timeout(state, wait)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        let Some(source) = state.source.clone() else {
            return;
        };
        state.changed = false;
        drop(state);
        // No queued interval snapshots: observe the latest cumulative state only
        // when the independent worker is ready to encode its next bounded export.
        let (storage_bytes, _) =
            crate::telemetry_exporter::measure_storage_and_journal_bytes(&source.database_path);
        source
            .observability
            .metrics()
            .set_required_gauge(RequiredGauge::StorageBytes, storage_bytes);
        let snapshot = source
            .observability
            .encode_otel_json_v1(crate::telemetry_worker::now_nanos());
        let batches = match split_snapshot(&snapshot) {
            Ok(batches) => batches,
            Err(error) => {
                metrics().record(
                    Signal::Metrics,
                    crate::telemetry_worker::failure_disposition(error),
                    1,
                );
                reporting.observe(
                    Signal::Metrics,
                    Some(crate::telemetry_worker::failure_disposition(error)),
                    Instant::now(),
                );
                next = Instant::now() + shared.config.interval();
                continue;
            }
        };
        let total = batches.iter().map(|batch| batch.points).sum::<usize>();
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .closing
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            metrics().record(Signal::Metrics, Disposition::Unflushed, total as u64);
            return;
        }
        state.pending_points = total;
        metrics().record(Signal::Metrics, Disposition::Enqueued, total as u64);
        drop(state);
        let deadline = Instant::now() + EXPORT_TIMEOUT;
        for batch in batches {
            let count = batch.points;
            metrics().set_inflight(Signal::Metrics, count);
            let started = Instant::now();
            let receipt = runtime.block_on(client.send_payload(batch.body, count, deadline));
            #[cfg(test)]
            if let Some(sender) = shared
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
            {
                let _ = sender.try_send(receipt);
            }

            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.pending_points == 0 {
                return;
            } // the root already accounted deadline loss
            state.pending_points = state.pending_points.saturating_sub(count);
            metrics().set_inflight(Signal::Metrics, 0);
            metrics().export_completed(
                Signal::Metrics,
                u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            );
            let failure = match receipt {
                Ok(receipt) => {
                    metrics().record(Signal::Metrics, Disposition::Accepted, receipt.accepted);
                    metrics().record(Signal::Metrics, Disposition::Rejected, receipt.rejected);
                    if receipt.accepted > 0 {
                        metrics()
                            .accepted_at(Signal::Metrics, crate::telemetry_worker::now_nanos());
                    }
                    if receipt.rejected > 0 {
                        Some(Disposition::Rejected)
                    } else {
                        None
                    }
                }
                Err(error) => {
                    let failure = crate::telemetry_worker::failure_disposition(error);
                    metrics().record(Signal::Metrics, failure, count as u64);
                    Some(failure)
                }
            };
            let abandon = receipt.is_err();
            if abandon {
                let remaining = state.pending_points;
                state.pending_points = 0;
                metrics().record(Signal::Metrics, Disposition::Unflushed, remaining as u64);
            }
            drop(state);
            reporting.observe(Signal::Metrics, failure, Instant::now());
            if abandon {
                break;
            }
        }
        next = Instant::now() + shared.config.interval();
    }
}

#[cfg(test)]
pub(crate) fn process_source_for_test(
    observability: Arc<Observability>,
    database_path: PathBuf,
    config: &TelemetryPushConfig,
) -> (ProcessMetricsWorker, MetricsSource) {
    let worker = MetricsWorker::start(config).unwrap();
    let source = worker
        .attach(
            Source {
                observability,
                database_path,
            },
            false,
        )
        .unwrap();
    (ProcessMetricsWorker(worker), source)
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: ID-005, PERF-001, END-007, END-008
    #[test]
    fn metrics_coalesce_to_one_latest_source_and_old_graphs_cannot_stop_a_replacement() {
        let config = TelemetryPushConfig::new(
            crate::config::parse_telemetry_push_endpoint("http://127.0.0.1:4318/v1/metrics", None)
                .unwrap(),
            Duration::from_secs(1),
            None,
        );
        let worker = MetricsWorker(Arc::new(Shared {
            state: Mutex::new(State {
                source: None,
                generation: 0,
                active: false,
                changed: false,
                closing: None,
                done: false,
                pending_points: 0,
            }),
            wake: Condvar::new(),
            batch: Arc::new(BatchContext::new()),
            config,
            receipts: Mutex::new(None),
        }));
        let source = || {
            let identifiers = crate::identifiers::ProductionIdentifierSources::new();
            Source {
                observability: Arc::new(
                    Observability::new(
                        Arc::new(identifiers.incident_ids()),
                        riffdb_observability::MAX_TRACE_RECORDS,
                    )
                    .unwrap(),
                ),
                database_path: PathBuf::from("test.redb"),
            }
        };
        let first = source();
        let old = Arc::downgrade(&first.observability);
        let first = worker.attach(first, false).unwrap();
        assert!(old.upgrade().is_some());
        let latest = source();
        let latest_weak = Arc::downgrade(&latest.observability);
        let latest = worker.attach(latest, false).unwrap();
        assert!(
            old.upgrade().is_none(),
            "no prior snapshots or graph source history accumulate"
        );
        assert!(first.stop().is_none());
        assert!(
            worker.0.state.lock().unwrap().active,
            "an old graph cannot stop a newer source"
        );
        assert!(latest.stop().is_none());
        assert!(!worker.0.state.lock().unwrap().active);
        assert!(
            latest_weak.upgrade().is_some(),
            "the process retains exactly the last source for its final snapshot"
        );
        drop(worker);
        assert!(latest_weak.upgrade().is_none());
    }
}
