//! One bounded queue, batch and private I/O runtime per diagnostic signal.
use crate::config::telemetry::{Signal, SignalConfig};
use crate::telemetry_http::{BatchContext, ExportFailure};
use opentelemetry::InstrumentationScope;
use opentelemetry_sdk::logs::{LogExporter as _, SdkLogRecord};
use opentelemetry_sdk::trace::{SpanData, SpanExporter as _};
use riffdb_observability::{
    TelemetryDisposition as Disposition, process_telemetry_delivery_metrics as metrics,
};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::Level;

pub(crate) enum Record {
    Log {
        data: SdkLogRecord,
        scope: InstrumentationScope,
        level: Level,
        weight: usize,
    },
    Span {
        data: SpanData,
        weight: usize,
    },
}
impl Record {
    fn level(&self) -> Option<Level> {
        match self {
            Self::Log { level, .. } => Some(*level),
            _ => None,
        }
    }
    fn weight(&self) -> usize {
        match self {
            Self::Log { weight, .. } => *weight,
            Self::Span { weight, .. } => *weight,
        }
    }
}
pub(crate) enum Exporter {
    Logs(opentelemetry_otlp::LogExporter),
    Traces(opentelemetry_otlp::SpanExporter),
}
impl Exporter {
    fn export(
        &self,
        runtime: &tokio::runtime::Runtime,
        records: Vec<Record>,
    ) -> Result<(), ExportFailure> {
        match self {
            Self::Logs(exporter) => {
                let logs = records
                    .into_iter()
                    .map(|record| match record {
                        Record::Log { data, scope, .. } => Ok((data, scope)),
                        _ => Err(ExportFailure::Encoding),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let references = logs
                    .iter()
                    .map(|(data, scope)| (data, scope))
                    .collect::<Vec<_>>();
                runtime
                    .block_on(exporter.export(opentelemetry_sdk::logs::LogBatch::new(&references)))
                    .map_err(|_| ExportFailure::Encoding)
            }
            Self::Traces(exporter) => {
                let spans = records
                    .into_iter()
                    .map(|record| match record {
                        Record::Span { data, .. } => Ok(data),
                        _ => Err(ExportFailure::Encoding),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                runtime
                    .block_on(exporter.export(spans))
                    .map_err(|_| ExportFailure::Encoding)
            }
        }
    }
}
#[derive(Clone, Copy, Default)]
struct Summary {
    count: usize,
    levels: [u64; 5],
}
impl Summary {
    fn add(&mut self, record: &Record) {
        self.count += 1;
        if let Some(level) = record.level() {
            self.levels[level_index(level)] += 1;
        }
    }
    fn log(&self, observation: riffdb_observability::LogObservation) {
        for (index, level) in LEVELS.iter().enumerate() {
            riffdb_observability::process_logging_metrics().record(
                riffdb_observability::LogDestination::Otlp,
                *level,
                observation,
                self.levels[index],
            );
        }
    }
}
const LEVELS: [Level; 5] = [
    Level::ERROR,
    Level::WARN,
    Level::INFO,
    Level::DEBUG,
    Level::TRACE,
];
fn level_index(level: Level) -> usize {
    match level {
        Level::ERROR => 0,
        Level::WARN => 1,
        Level::INFO => 2,
        Level::DEBUG => 3,
        Level::TRACE => 4,
    }
}
struct Queued {
    record: Record,
    sequence: u64,
}
struct State {
    queue: VecDeque<Queued>,
    closing: Option<Instant>,
    done: bool,
    next_sequence: u64,
    completed_sequence: u64,
    flush_until: u64,
    inflight: Option<Summary>,
    inflight_accounted: bool,
}
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    config: SignalConfig,
    batch: Arc<BatchContext>,
}
#[derive(Clone)]
pub(crate) struct Worker {
    shared: Arc<Shared>,
}
impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryWorker")
            .field("signal", &self.shared.config.signal)
            .finish()
    }
}
impl Worker {
    pub(crate) fn start(
        config: &SignalConfig,
        batch: Arc<BatchContext>,
        exporter: Exporter,
    ) -> Result<Self, ()> {
        // Each runtime can own at most one outstanding DNS lookup, even if the
        // OS resolver outlives a canceled export. It never uses service workers.
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                queue: VecDeque::with_capacity(config.queue_capacity),
                closing: None,
                done: false,
                next_sequence: 0,
                completed_sequence: 0,
                flush_until: 0,
                inflight: None,
                inflight_accounted: false,
            }),
            wake: Condvar::new(),
            config: config.clone(),
            batch,
        });
        let worker = Self {
            shared: Arc::clone(&shared),
        };
        let name = match config.signal {
            Signal::Logs => "riffdb-otlp-logs",
            Signal::Traces => "riffdb-otlp-traces",
            Signal::Metrics => return Err(()),
        };
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(name.into())
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
                let completed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(&shared, &runtime, &exporter)
                }));
                runtime.shutdown_background();
                let mut state = shared
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if completed.is_err() {
                    metrics().record(shared.config.signal, Disposition::Transport, 1);
                }
                account_unknown(&shared, &mut state);
                state.done = true;
                shared.wake.notify_all();
            })
            .map_err(|_| ())?;
        if started.recv_timeout(Duration::from_secs(1)) != Ok(true) {
            worker.begin_drain(Instant::now());
            return Err(());
        }
        metrics().set_enabled(config.signal, true);
        if config.signal == Signal::Logs {
            riffdb_observability::process_logging_metrics()
                .set_enabled(riffdb_observability::LogDestination::Otlp, true);
        }
        Ok(worker)
    }
    pub(crate) fn enqueue(
        &self,
        level: Option<Level>,
        weight: usize,
        create: impl FnOnce() -> Record,
    ) {
        let signal = self.shared.config.signal;
        let observation = |disposition, logging| {
            metrics().record(signal, disposition, 1);
            if let Some(level) = level {
                riffdb_observability::process_logging_metrics().record(
                    riffdb_observability::LogDestination::Otlp,
                    level,
                    logging,
                    1,
                );
            }
        };
        if weight > 900 * 1024 {
            observation(
                Disposition::RequestLimit,
                riffdb_observability::LogObservation::RecordLimit,
            );
            return;
        }
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closing.is_some() || state.done {
            observation(
                Disposition::Closing,
                riffdb_observability::LogObservation::Closing,
            );
            return;
        }
        // ADR-0341 reserves 64 entries for warnings/errors. Capacity 64 is an
        // explicitly permitted high-severity-only OTLP log queue.
        let reserve =
            if signal == Signal::Logs && !matches!(level, Some(Level::WARN | Level::ERROR)) {
                64
            } else {
                0
            };
        let limit = self.shared.config.queue_capacity.saturating_sub(reserve);
        if state.queue.len() >= limit {
            observation(
                Disposition::QueueFull,
                riffdb_observability::LogObservation::QueueFull,
            );
            return;
        }
        let record = create();
        state.next_sequence = state.next_sequence.saturating_add(1);
        let sequence = state.next_sequence;
        state.queue.push_back(Queued { record, sequence });
        metrics().set_depth(signal, state.queue.len());
        if signal == Signal::Logs {
            riffdb_observability::process_logging_metrics().set_depth(
                riffdb_observability::LogDestination::Otlp,
                state.queue.len(),
            );
        }
        observation(
            Disposition::Enqueued,
            riffdb_observability::LogObservation::Enqueued,
        );
        self.shared.wake.notify_all();
    }
    pub(crate) fn flush(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let target = state.next_sequence;
        state.flush_until = state.flush_until.max(target);
        self.shared.wake.notify_all();
        while state.completed_sequence < target && !state.done {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            state = self
                .shared
                .wake
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        state.completed_sequence >= target
    }
    pub(crate) fn begin_drain(&self, deadline: Instant) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let deadline = state.closing.map_or(deadline, |old| old.min(deadline));
        state.closing = Some(deadline);
        self.shared.batch.begin_drain(deadline);
        self.shared.wake.notify_all();
    }
    pub(crate) fn close(&self, deadline: Instant) -> bool {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let deadline = state.closing.map_or(deadline, |old| old.min(deadline));
        state.closing = Some(deadline);
        self.shared.batch.begin_drain(deadline);
        self.shared.wake.notify_all();
        while !state.done {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            state = self
                .shared
                .wake
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        if !state.done {
            account_unknown(&self.shared, &mut state);
        }
        state.done
    }
}
fn account_unknown(shared: &Shared, state: &mut State) {
    let mut unknown = Summary::default();
    for queued in state.queue.drain(..) {
        unknown.add(&queued.record);
    }
    if !state.inflight_accounted {
        if let Some(inflight) = state.inflight {
            unknown.count += inflight.count;
            for (count, add) in unknown.levels.iter_mut().zip(inflight.levels) {
                *count += add;
            }
        }
        state.inflight_accounted = true;
    }
    metrics().record(
        shared.config.signal,
        Disposition::Unflushed,
        unknown.count as u64,
    );
    metrics().set_depth(shared.config.signal, 0);
    if shared.config.signal == Signal::Logs {
        riffdb_observability::process_logging_metrics()
            .set_depth(riffdb_observability::LogDestination::Otlp, 0);
    }
    unknown.log(riffdb_observability::LogObservation::Unflushed);
    if unknown.count > 0 {
        crate::operational_logging::emit(
            crate::operational_logging::OperationalEvent::ExportState {
                signal: shared.config.signal,
                failure: Some(Disposition::Unflushed),
            },
        );
    }
}
fn run(shared: &Shared, runtime: &tokio::runtime::Runtime, exporter: &Exporter) {
    let signal = shared.config.signal;
    let interval = shared.config.push.interval();
    let mut next = Instant::now() + interval;
    let mut reporting = DeliveryReporter::default();
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
            if state.closing.is_some() && state.queue.is_empty() {
                return;
            }
            let force = state.closing.is_some() || state.flush_until > state.completed_sequence;
            if !state.queue.is_empty()
                && (force
                    || state.queue.len() >= shared.config.batch_size
                    || Instant::now() >= next)
            {
                break;
            }
            let wait = if state.queue.is_empty() {
                interval
            } else {
                next.saturating_duration_since(Instant::now())
            };
            state = shared
                .wake
                .wait_timeout(state, wait)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        let mut records = Vec::with_capacity(shared.config.batch_size);
        let mut summary = Summary::default();
        let mut weight = 0usize;
        let mut through = state.completed_sequence;
        while records.len() < shared.config.batch_size {
            let Some(queued) = state.queue.front() else {
                break;
            };
            if weight.saturating_add(queued.record.weight()) > 900 * 1024 {
                break;
            }
            let Some(queued) = state.queue.pop_front() else {
                break;
            };
            weight += queued.record.weight();
            summary.add(&queued.record);
            through = queued.sequence;
            records.push(queued.record);
        }
        state.inflight = Some(summary);
        state.inflight_accounted = false;
        metrics().set_depth(signal, state.queue.len());
        metrics().set_inflight(signal, summary.count);
        if signal == Signal::Logs {
            riffdb_observability::process_logging_metrics().set_depth(
                riffdb_observability::LogDestination::Otlp,
                state.queue.len(),
            );
        }
        drop(state);
        shared.batch.begin(summary.count);
        let started = Instant::now();
        let _exported = exporter.export(runtime, records);
        let receipt = shared
            .batch
            .take_receipt()
            .unwrap_or(Err(ExportFailure::Encoding));
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.inflight_accounted {
            metrics().export_completed(
                signal,
                u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            );
            match receipt {
                Ok(receipt) => {
                    metrics().record(signal, Disposition::Accepted, receipt.accepted);
                    metrics().record(signal, Disposition::Rejected, receipt.rejected);
                    if receipt.accepted > 0 {
                        metrics().accepted_at(signal, now_nanos());
                    }
                    if shared
                        .batch
                        .written
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        summary.log(riffdb_observability::LogObservation::Written);
                    }
                }
                Err(error) => {
                    metrics().record(signal, failure_disposition(error), summary.count as u64);
                    summary.log(if error == ExportFailure::Shutdown {
                        riffdb_observability::LogObservation::Unflushed
                    } else {
                        riffdb_observability::LogObservation::WriteFailure
                    });
                }
            }
        }
        let report = if state.inflight_accounted {
            None
        } else {
            Some(match receipt {
                Ok(receipt) if receipt.rejected == 0 => None,
                Ok(_) => Some(Disposition::Rejected),
                Err(error) => Some(failure_disposition(error)),
            })
        };
        state.inflight = None;
        state.completed_sequence = through;
        metrics().set_inflight(signal, 0);
        shared.wake.notify_all();
        drop(state);
        if let Some(failure) = report {
            reporting.observe(signal, failure, Instant::now());
        }
        next = Instant::now() + interval;
    }
}
#[derive(Default)]
pub(crate) struct DeliveryReporter {
    failed: bool,
    last: Option<Instant>,
}
impl DeliveryReporter {
    pub(crate) fn observe(&mut self, signal: Signal, failure: Option<Disposition>, now: Instant) {
        if self.should_report(failure.is_some(), now) {
            crate::operational_logging::emit(
                crate::operational_logging::OperationalEvent::ExportState { signal, failure },
            );
        }
    }
    fn should_report(&mut self, failed: bool, now: Instant) -> bool {
        let report = if failed {
            self.last
                .is_none_or(|last| now.saturating_duration_since(last) >= Duration::from_secs(60))
        } else {
            self.failed
        };
        self.failed = failed;
        if report {
            self.last = if failed { Some(now) } else { None };
        }
        report
    }
}
pub(crate) fn failure_disposition(error: ExportFailure) -> Disposition {
    match error {
        ExportFailure::Encoding => Disposition::Encoding,
        ExportFailure::RequestLimit => Disposition::RequestLimit,
        ExportFailure::Dns => Disposition::Dns,
        ExportFailure::Connect => Disposition::Connect,
        ExportFailure::Tls => Disposition::Tls,
        ExportFailure::Transport => Disposition::Transport,
        ExportFailure::ResponseLimit => Disposition::ResponseLimit,
        ExportFailure::ResponseInvalid => Disposition::ResponseInvalid,
        ExportFailure::Http(_) => Disposition::Http,
        ExportFailure::Timeout => Disposition::Timeout,
        ExportFailure::Shutdown => Disposition::Unflushed,
        ExportFailure::Credential => Disposition::Credential,
    }
}
pub(crate) fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::logs::{Logger as _, LoggerProvider as _};
    // req: ID-005, END-007, END-008, PERF-001
    #[test]
    fn queue_reserves_high_severity_capacity_and_closes_without_constructing_rejected_records() {
        let config = SignalConfig {
            signal: Signal::Logs,
            push: crate::config::TelemetryPushConfig::new(
                crate::config::parse_telemetry_push_endpoint("http://127.0.0.1:4318/v1/logs", None)
                    .unwrap(),
                Duration::from_secs(1),
                None,
            ),
            queue_capacity: 128,
            batch_size: 128,
            timeout: Duration::from_secs(5),
            sample_per_million: 0,
        };
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                closing: None,
                done: false,
                next_sequence: 0,
                completed_sequence: 0,
                flush_until: 0,
                inflight: None,
                inflight_accounted: false,
            }),
            wake: Condvar::new(),
            config,
            batch: Arc::new(BatchContext::new()),
        });
        let worker = Worker {
            shared: shared.clone(),
        };
        let provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder().build();
        let logger = provider.logger("test");
        for level in [Level::INFO, Level::ERROR] {
            for _ in 0..64 {
                worker.enqueue(Some(level), 1, || Record::Log {
                    data: logger.create_log_record(),
                    scope: InstrumentationScope::builder("test").build(),
                    level,
                    weight: 1,
                });
            }
            worker.enqueue(Some(level), 1, || {
                panic!("a rejected record must not be allocated")
            });
        }
        assert_eq!(shared.state.lock().unwrap().queue.len(), 128);
        worker.begin_drain(Instant::now());
        worker.enqueue(Some(Level::ERROR), 1, || {
            panic!("closed admission must not construct records")
        });
        assert!(!worker.close(Instant::now()));
        let state = shared.state.lock().unwrap();
        assert!(state.queue.is_empty());
        assert!(state.inflight_accounted);
    }
    // req: ID-005, END-007, END-008
    #[test]
    fn export_failure_warnings_are_rate_limited_and_recovery_resets_the_window() {
        let start = Instant::now();
        let mut reporter = DeliveryReporter::default();
        assert!(!reporter.should_report(false, start));
        assert!(reporter.should_report(true, start));
        assert!(!reporter.should_report(true, start + Duration::from_secs(59)));
        assert!(reporter.should_report(true, start + Duration::from_secs(60)));
        assert!(reporter.should_report(false, start + Duration::from_secs(61)));
        assert!(!reporter.should_report(false, start + Duration::from_secs(62)));
        assert!(reporter.should_report(true, start + Duration::from_secs(63)));
    }
}
