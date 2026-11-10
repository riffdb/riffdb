//! Process-owned operator logging. Typed producers redact before every subscriber.

pub(crate) mod diagnostics;
mod events;
mod file;
mod sink;
#[cfg(test)]
mod tests;

pub(crate) use events::{Component, Lifecycle, OperationalEvent, emit};

use crate::config::logging::{LogFormat, LoggingConfig};
use serde_json::{Value, json};
use sink::{NonBlocking, NonBlockingBuilder, WorkerGuard};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Default)]
pub(crate) struct LogCounters {
    pub(crate) rejected: AtomicU64,
    pub(crate) written: AtomicU64,
    pub(crate) write_failed: AtomicU64,
}

fn increment(counter: &AtomicU64) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_add(1))
    });
}

#[derive(Debug)]
pub(crate) enum LoggingStartError {
    Entropy,
    Output,
    FileCustody,
    DirectoryInUse,
    FilePermissionDenied,
    FileStorageFull,
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    FileUnsupported,
    Subscriber,
    Sampler,
    Telemetry(crate::telemetry_pipeline::StartError),
}

impl std::fmt::Display for LoggingStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Telemetry(error) => return error.fmt(f),
            Self::Entropy => "log_instance_unavailable: process entropy is unavailable",
            Self::Output => "log_writer_unavailable: a configured output worker could not start",
            Self::FileCustody => "log_file_unavailable: require an owned 0700 logging directory and 0600 single-link files; check permissions and available space",
            Self::DirectoryInUse => "log_directory_in_use: another daemon owns the logging directory; select a dedicated private directory per process",
            Self::FilePermissionDenied => "log_file_permission_denied: restore owner access to the configured protected logging directory and files",
            Self::FileStorageFull => "log_file_storage_full: restore space for the configured bounded log retention",
            #[cfg(not(any(target_os="linux",target_os="macos")))]
            Self::FileUnsupported => "log_file_unsupported: protected file logging requires a supported Unix platform",
            Self::Subscriber => "log_subscriber_unavailable: the process subscriber is already installed",
            Self::Sampler => "log_sampler_unavailable: the process accounting worker could not start",
        })
    }
}
impl std::error::Error for LoggingStartError {}

pub(crate) struct RunningLogging {
    writers: Vec<WorkerGuard>,
    deadline: Option<std::time::Instant>,
    sampler: Option<crate::exact_provider_diagnostics::RunningProviderSamples>,
    counters: Arc<LogCounters>,
    signals: Option<crate::telemetry_pipeline::RunningSignals>,
    metrics: Option<crate::telemetry_metrics_worker::ProcessMetricsWorker>,
}

impl RunningLogging {
    pub(crate) fn install(
        config: &LoggingConfig,
        telemetry: &crate::config::telemetry::TelemetryConfig,
        metric_push: Option<&crate::config::TelemetryPushConfig>,
    ) -> Result<Self, LoggingStartError> {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).map_err(|_| LoggingStartError::Entropy)?;
        let mut instance = String::with_capacity(32);
        for byte in random {
            let _ = write!(instance, "{byte:02x}");
        }
        let mut outputs = Vec::with_capacity(2);
        let mut writers = Vec::with_capacity(2);
        if config.stderr {
            let (writer, guard) = NonBlockingBuilder::default()
                .buffered_lines_limit(4096)
                .thread_name("riffdb-stderr")
                .finish(std::io::stderr())
                .map_err(|_| LoggingStartError::Output)?;
            outputs.push(writer);
            writers.push(guard);
        }
        if let Some(file) = &config.file {
            let file = self::file::ProtectedLogFile::open(file).map_err(|error| match error {
                file::OpenError::Custody => LoggingStartError::FileCustody,
                file::OpenError::DirectoryInUse => LoggingStartError::DirectoryInUse,
                file::OpenError::PermissionDenied => LoggingStartError::FilePermissionDenied,
                file::OpenError::StorageFull => LoggingStartError::FileStorageFull,
                #[cfg(not(any(target_os = "linux", target_os = "macos")))]
                file::OpenError::Unsupported => LoggingStartError::FileUnsupported,
            })?;
            let (writer, guard) = NonBlockingBuilder::default()
                .buffered_lines_limit(4096)
                .thread_name("riffdb-log-file")
                .destination(riffdb_observability::LogDestination::File)
                .finish(file)
                .map_err(|_| LoggingStartError::Output)?;
            outputs.push(writer);
            writers.push(guard);
        }
        let signals = crate::telemetry_pipeline::RunningSignals::start(telemetry, &instance)
            .map_err(LoggingStartError::Telemetry)?;
        let metrics = metric_push
            .map(crate::telemetry_metrics_worker::MetricsWorker::install)
            .transpose()
            .map_err(|failure| {
                use crate::telemetry_metrics_worker::StartFailure;
                use crate::telemetry_pipeline::{StartCause, StartError};
                LoggingStartError::Telemetry(StartError {
                    signal: riffdb_observability::TelemetrySignal::Metrics,
                    cause: match failure {
                        StartFailure::Credential => StartCause::Credential,
                        StartFailure::TrustRoot => StartCause::TrustRoot,
                        StartFailure::Worker => StartCause::Worker,
                    },
                })
            })?;
        let counters = Arc::new(LogCounters::default());
        build_dispatch(
            config.clone(),
            outputs,
            instance,
            counters.clone(),
            &signals,
        )
        .try_init()
        .map_err(|_| LoggingStartError::Subscriber)?;
        emit(OperationalEvent::Lifecycle(Lifecycle::Starting));
        let sampler = crate::exact_provider_diagnostics::RunningProviderSamples::start()
            .map_err(|_| LoggingStartError::Sampler)?;
        Ok(Self {
            writers,
            deadline: None,
            sampler: Some(sampler),
            counters,
            signals: Some(signals),
            metrics,
        })
    }
}

fn build_dispatch(
    config: LoggingConfig,
    outputs: Vec<NonBlocking>,
    instance: String,
    counters: Arc<LogCounters>,
    signals: &crate::telemetry_pipeline::RunningSignals,
) -> tracing::Dispatch {
    let trace_layer = signals.tracer().map(|tracer| {
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_location(false)
            .with_threads(false)
            .with_level(false)
            .with_tracked_inactivity(false)
            .with_error_fields_to_exceptions(false)
            .with_error_events_to_status(false)
            .with_error_events_to_exceptions(false)
            .with_error_records_to_exceptions(false)
            .with_context_activation(true)
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.is_span()
                    && ((metadata.target() == riffdb_observability::SAFE_TRACE_TARGET
                        && metadata.name() == "riffdb.request")
                        || (metadata.target() == crate::telemetry_ingress::TARGET
                            && metadata.name() == "riffdb.ingress"))
            }))
    });
    let layer = OperatorLayer {
        config: config.clone(),
        writers: outputs,
        instance,
        counters: counters.clone(),
        bridge: signals.log_bridge(),
    };
    tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(SpanAdmissionLayer)
            .with(trace_layer)
            .with(layer),
    )
}

impl RunningLogging {
    pub(crate) fn begin_final_drain(&mut self, deadline: std::time::Instant) {
        self.deadline = Some(self.deadline.map_or(deadline, |old| old.min(deadline)));
    }
}

impl Drop for RunningLogging {
    fn drop(&mut self) {
        let deadline = self
            .deadline
            .unwrap_or_else(|| std::time::Instant::now() + std::time::Duration::from_secs(5));
        if let Some(sampler) = self.sampler.take()
            && !sampler.shutdown(
                deadline
                    .saturating_duration_since(std::time::Instant::now())
                    .min(std::time::Duration::from_secs(1)),
            )
        {
            increment(&self.counters.write_failed);
            emit(OperationalEvent::WorkerUnavailable(Component::Projection));
        }
        // Start every signal before waiting for any one worker. A slow signal
        // consumes only the same absolute process deadline, never another budget.
        if let Some(metrics) = &self.metrics {
            metrics.begin_drain(deadline);
        }
        if let Some(signals) = self.signals.as_mut() {
            signals.begin_drain(deadline);
        }
        drop(self.signals.take());
        if let Some(metrics) = self.metrics.take() {
            let _ = metrics.close(deadline);
        }
        for writer in &mut self.writers {
            if !writer.stop(deadline.saturating_duration_since(std::time::Instant::now())) {
                increment(&self.counters.write_failed);
            }
        }
    }
}

struct OperatorLayer {
    config: LoggingConfig,
    writers: Vec<NonBlocking>,
    instance: String,
    counters: Arc<LogCounters>,
    bridge: Option<crate::telemetry_pipeline::LogBridge>,
}

#[derive(Default)]
struct Fields {
    code: Option<String>,
    component: Option<Component>,
    message: Option<String>,
    attributes: Option<Value>,
    numeric: serde_json::Map<String, Value>,
    invalid: bool,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "event_code" if value.len() <= 32 => self.code = Some(value.to_owned()),
            "component" => self.component = Component::parse(value),
            "message" if value.len() <= 1024 => self.message = Some(value.to_owned()),
            "attributes_json" if value.len() <= events::MAX_RECORD_BYTES => {
                self.attributes = serde_json::from_str(value).ok();
                if self.attributes.as_ref().is_none_or(|v| !v.is_object()) {
                    self.invalid = true;
                }
            }
            _ => self.invalid = true,
        }
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        if self.numeric.len() < 32 {
            self.numeric.insert(field.name().to_owned(), value.into());
        } else {
            self.invalid = true;
        }
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        if self.numeric.len() < 32 {
            self.numeric.insert(field.name().to_owned(), value.into());
        } else {
            self.invalid = true;
        }
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        if self.numeric.len() < 32 {
            self.numeric.insert(field.name().to_owned(), value.into());
        } else {
            self.invalid = true;
        }
    }
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {
        self.invalid = true;
    }
}

fn allowed_target(target: &str) -> bool {
    target == events::TARGET || target == riffdb_observability::SAFE_TRACE_TARGET
}

struct RequestCorrelation(String);
struct CorrelationVisitor(Option<String>);
impl CorrelationVisitor {
    fn accept(&mut self, value: &str) {
        if value.len() == 36
            && value.bytes().enumerate().all(|(index, byte)| {
                if matches!(index, 8 | 13 | 18 | 23) {
                    byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            })
        {
            self.0 = Some(value.to_owned());
        }
    }
}
impl Visit for CorrelationVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "request_id" {
            self.accept(value);
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() != "request_id" {
            return;
        }
        struct Bounded(String);
        impl std::fmt::Write for Bounded {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                if self.0.len().saturating_add(value.len()) > 36 {
                    return Err(std::fmt::Error);
                }
                self.0.push_str(value);
                Ok(())
            }
        }
        let mut text = Bounded(String::with_capacity(36));
        if write!(&mut text, "{value:?}").is_ok() {
            self.accept(&text.0);
        }
    }
}

struct SpanAdmissionLayer;
impl<S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> Layer<S>
    for SpanAdmissionLayer
{
    fn on_new_span(
        &self,
        _attributes: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        context: Context<'_, S>,
    ) {
        if let Some(permit) = riffdb_observability::take_constructing_diagnostic_span_admission()
            && let Some(span) = context.span(id)
        {
            span.extensions_mut().insert(permit);
        }
    }
}

impl<S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> Layer<S>
    for OperatorLayer
{
    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        // Admission is per construction, never a cached callsite decision.
        if metadata.is_span() {
            tracing::subscriber::Interest::sometimes()
        } else if allowed_target(metadata.target()) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        if metadata.is_span() {
            riffdb_observability::has_constructing_diagnostic_span_admission()
                && ((metadata.target() == riffdb_observability::SAFE_TRACE_TARGET
                    && metadata.name() == "riffdb.request")
                    || (metadata.target() == crate::telemetry_ingress::TARGET
                        && metadata.name() == "riffdb.ingress"))
        } else {
            allowed_target(metadata.target())
        }
    }

    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        context: Context<'_, S>,
    ) {
        let mut visitor = CorrelationVisitor(None);
        attributes.record(&mut visitor);
        if let Some(request) = visitor.0
            && let Some(span) = context.span(id)
        {
            span.extensions_mut().insert(RequestCorrelation(request));
        }
    }
    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let semantic = if event.metadata().target() == riffdb_observability::SAFE_TRACE_TARGET {
            match riffdb_observability::TraceRecord::from_event(event) {
                Some(record) => Some(record),
                None => {
                    increment(&self.counters.rejected);
                    riffdb_observability::process_logging_metrics().reject_input();
                    return;
                }
            }
        } else {
            None
        };
        let level = semantic.map_or(*event.metadata().level(), |record| {
            use riffdb_observability::TraceKind;
            match record.kind() {
                TraceKind::ServiceAuditUnavailable
                | TraceKind::ServiceInternalIntegrity
                | TraceKind::AuthenticationDefect
                | TraceKind::AuthorizationDefect
                | TraceKind::IncidentSourceUnavailable
                | TraceKind::AuthoritativeReadinessFailed => tracing::Level::ERROR,
                TraceKind::Incident | TraceKind::ServiceCursorUnavailable => tracing::Level::WARN,
                _ => tracing::Level::DEBUG,
            }
        });
        if let Some(record) = semantic {
            fields
                .numeric
                .insert("kind".to_owned(), json!(format!("{:?}", record.kind())));
        }
        let component = fields.component.unwrap_or(Component::Telemetry);
        if !self.config.allows(component, level) {
            return;
        }
        if fields.invalid {
            increment(&self.counters.rejected);
            riffdb_observability::process_logging_metrics().reject_input();
            return;
        }
        let mut timestamp = String::with_capacity(32);
        let mut timestamp_writer = tracing_subscriber::fmt::format::Writer::new(&mut timestamp);
        if tracing_subscriber::fmt::time::SystemTime
            .format_time(&mut timestamp_writer)
            .is_err()
        {
            increment(&self.counters.rejected);
            riffdb_observability::process_logging_metrics().reject_input();
            return;
        }
        let mut value = json!({
            "schema": "riffdb.log/v1", "timestamp": timestamp,
            "severity": level.as_str(),
            "event_code": fields.code.as_deref().unwrap_or("RDB-LOG-0400"),
            "component": component.name(),
            "message": fields.message.as_deref().unwrap_or("semantic telemetry observation"),
            "version": env!("CARGO_PKG_VERSION"), "instance_id": self.instance,
            "attributes": fields.attributes.unwrap_or(Value::Object(fields.numeric)),
        });
        if let Some(incident) = semantic.and_then(|record| record.incident_id()) {
            value["incident_id"] = json!(incident.to_string());
        }
        if let Some(scope) = context.event_scope(event) {
            for span in scope.from_root() {
                if let Some(request) = span.extensions().get::<RequestCorrelation>() {
                    value["request_id"] = json!(request.0);
                }
            }
        }
        use opentelemetry::trace::TraceContextExt as _;
        let current = opentelemetry::Context::current();
        let current_span = current.span();
        let span_context = current_span.span_context();
        if span_context.is_valid() {
            value["trace_id"] = json!(span_context.trace_id().to_string());
            value["span_id"] = json!(span_context.span_id().to_string());
        }
        let canonical = value.to_string();
        if canonical.len().saturating_add(1) > events::MAX_RECORD_BYTES {
            increment(&self.counters.rejected);
            riffdb_observability::process_logging_metrics().reject_input();
            return;
        }
        let mut line = match self.config.format {
            LogFormat::Json => canonical.clone(),
            LogFormat::Text => {
                // Provider evidence keeps its established numeric spelling when
                // an operator explicitly enables projection debug diagnostics.
                if ["RDB-LOG-0900", "RDB-LOG-0901", "RDB-LOG-0902"]
                    .iter()
                    .any(|code| value["event_code"] == *code)
                {
                    value["attributes"]["evidence"]
                        .as_str()
                        .unwrap_or("")
                        .to_owned()
                } else if value["event_code"] == "RDB-LOG-0500" {
                    let a = &value["attributes"];
                    if a["bounded_dirty"] == true {
                        "[riffdbd-diag] trigger=startup clean_close_fast=false bounded_dirty=true"
                            .to_owned()
                    } else if a["clean_close_fast"] == true {
                        "[riffdbd-diag] trigger=startup clean_close_fast=true".to_owned()
                    } else {
                        format!(
                            "[riffdbd-diag] trigger=startup clean_close_fast=false decline={} (full validation pass; cost scales with retained history)",
                            a["decline"].as_str().unwrap_or("unavailable")
                        )
                    }
                } else if value["event_code"] == "RDB-LOG-0200" {
                    let mut line = String::from(
                        "[riffdbd-diag] trigger=exact_provider_lifecycle available=true",
                    );
                    for key in [
                        "limit_bytes",
                        "fixed_owner_bytes",
                        "owned_bytes",
                        "reserved_bytes",
                        "reserved_work",
                        "allocations",
                        "released_allocations",
                        "active_owners",
                        "retiring_owners",
                        "pinned_owners",
                        "pins",
                        "released_owners",
                    ] {
                        let _ = write!(line, " {key}={}", value["attributes"][key]);
                    }
                    line
                } else {
                    format!(
                        "{} {} {} [{}] {} {}",
                        timestamp,
                        level,
                        value["event_code"].as_str().unwrap_or("RDB-LOG-0400"),
                        component.name(),
                        value["message"]
                            .as_str()
                            .unwrap_or("semantic telemetry observation"),
                        value["attributes"]
                    )
                }
            }
        };
        if self.config.format == LogFormat::Text {
            for key in ["request_id", "incident_id", "trace_id", "span_id"] {
                if let Some(value) = value.get(key).and_then(Value::as_str) {
                    let _ = write!(line, " {key}={value}");
                }
            }
        }
        line.push('\n');
        if line.len() > events::MAX_RECORD_BYTES {
            increment(&self.counters.rejected);
            riffdb_observability::process_logging_metrics().reject_input();
            return;
        }
        // Exporter diagnostics are local-only, preventing recursive queue load.
        if !matches!(
            value["event_code"].as_str(),
            Some("RDB-LOG-0610" | "RDB-LOG-0611")
        ) && let Some(bridge) = &self.bridge
        {
            crate::telemetry_pipeline::emit_checked_log(bridge, &canonical, level, context);
        }
        for writer in &self.writers {
            if writer.enqueue(line.as_bytes(), level).is_err() {
                increment(&self.counters.write_failed);
            } else {
                increment(&self.counters.written);
            }
        }
    }
}
