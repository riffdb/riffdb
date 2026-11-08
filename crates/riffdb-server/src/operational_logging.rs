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
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_add(1))
    });
}

#[derive(Debug)]
pub(crate) enum LoggingStartError {
    Entropy,
    Output,
    FileCustody,
    Subscriber,
    Sampler,
}

impl std::fmt::Display for LoggingStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Entropy => "log_instance_unavailable: process entropy is unavailable",
            Self::Output => "log_writer_unavailable: a configured output worker could not start",
            Self::FileCustody => "log_file_unavailable: require an owned 0700 logging directory and 0600 single-link files; check permissions and available space",
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
}

impl RunningLogging {
    pub(crate) fn install(config: &LoggingConfig) -> Result<Self, LoggingStartError> {
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
            let file = self::file::ProtectedLogFile::open(file)
                .map_err(|_| LoggingStartError::FileCustody)?;
            let (writer, guard) = NonBlockingBuilder::default()
                .buffered_lines_limit(4096)
                .thread_name("riffdb-log-file")
                .destination(riffdb_observability::LogDestination::File)
                .finish(file)
                .map_err(|_| LoggingStartError::Output)?;
            outputs.push(writer);
            writers.push(guard);
        }
        let counters = Arc::new(LogCounters::default());
        let layer = OperatorLayer {
            config: config.clone(),
            writers: outputs,
            instance,
            counters: counters.clone(),
        };
        tracing_subscriber::registry()
            .with(layer)
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
        })
    }
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

impl<S: Subscriber> Layer<S> for OperatorLayer {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        // A hard allow-list is independent of level, RUST_LOG and SDK targets.
        allowed_target(metadata.target())
    }

    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
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
        let mut line = match self.config.format {
            LogFormat::Json => value.to_string(),
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
        line.push('\n');
        if line.len() > events::MAX_RECORD_BYTES {
            increment(&self.counters.rejected);
            riffdb_observability::process_logging_metrics().reject_input();
            return;
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
