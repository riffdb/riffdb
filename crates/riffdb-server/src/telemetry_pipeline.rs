//! Official log/trace models and codecs, with process-owned bounded delivery.
use crate::config::telemetry::{Signal, SignalConfig, TelemetryConfig};
use crate::telemetry_http::{BatchContext, MAX_REQUEST_BYTES, OtlpHttpClient, SetupFailure};
use crate::telemetry_worker::{Exporter, Record, Worker};
use opentelemetry::logs::{AnyValue, LogRecord as _};
use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
use opentelemetry::{InstrumentationScope, Key, KeyValue};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::logs::{LogProcessor, SdkLogRecord, SdkLoggerProvider};
use opentelemetry_sdk::trace::{
    SamplingDecision, SamplingResult, SdkTracerProvider, ShouldSample, SpanData, SpanProcessor,
};
use riffdb_observability::{
    TelemetryDisposition as Disposition, process_telemetry_delivery_metrics as metrics,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::Level;

#[derive(Debug)]
pub(crate) enum StartCause {
    Credential,
    TrustRoot,
    Codec,
    Worker,
}
#[derive(Debug)]
pub(crate) struct StartError {
    pub(crate) signal: Signal,
    pub(crate) cause: StartCause,
}
impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "telemetry_start_unavailable: signal={} cause={}; {}",
            self.signal.name(),
            match self.cause {
                StartCause::Credential => "credential_unavailable",
                StartCause::TrustRoot => "trust_root_unavailable",
                StartCause::Codec => "codec_configuration",
                StartCause::Worker => "worker_unavailable",
            },
            match self.cause {
                StartCause::Credential =>
                    "check the selected protected bearer token file and its owner-only permissions",
                StartCause::TrustRoot =>
                    "check the selected CA bundle and verified endpoint identity",
                StartCause::Codec =>
                    "check the explicit telemetry table and installed build identity",
                StartCause::Worker => "restore process thread/memory capacity and restart",
            }
        )
    }
}
fn error(config: &SignalConfig, cause: StartCause) -> StartError {
    StartError {
        signal: config.signal,
        cause,
    }
}
fn client(config: &SignalConfig, batch: Arc<BatchContext>) -> Result<OtlpHttpClient, StartError> {
    OtlpHttpClient::new(config.signal, &config.push, config.timeout, batch).map_err(|cause| {
        error(
            config,
            match cause {
                SetupFailure::Credential => StartCause::Credential,
                SetupFailure::TrustRoot => StartCause::TrustRoot,
            },
        )
    })
}
fn endpoint(config: &SignalConfig) -> String {
    use crate::config::TelemetryPushEndpoint;
    match config.push.endpoint() {
        TelemetryPushEndpoint::LoopbackHttp {
            authority, path, ..
        } => format!("http://{authority}{path}"),
        TelemetryPushEndpoint::Https {
            authority, path, ..
        } => format!("https://{authority}{path}"),
    }
}
/// Private scope adapter; the official bridge's empty default scope is replaced
/// with a fixed first-party scope, without any automatic metadata attributes.
pub(crate) struct OperationalLogProvider(SdkLoggerProvider);
impl opentelemetry::logs::LoggerProvider for OperationalLogProvider {
    type Logger = opentelemetry_sdk::logs::SdkLogger;
    fn logger_with_scope(&self, _scope: InstrumentationScope) -> Self::Logger {
        self.0.logger_with_scope(
            InstrumentationScope::builder("riffdb.operational")
                .with_version(env!("CARGO_PKG_VERSION"))
                .build(),
        )
    }
}
pub(crate) type LogBridge = opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge<
    OperationalLogProvider,
    opentelemetry_sdk::logs::SdkLogger,
>;

pub(crate) struct RunningSignals {
    logs: Option<SdkLoggerProvider>,
    traces: Option<SdkTracerProvider>,
    workers: Vec<Worker>,
    deadline: Option<Instant>,
}
impl RunningSignals {
    pub(crate) fn start(config: &TelemetryConfig, instance: &str) -> Result<Self, StartError> {
        if config.logs.is_none() && config.traces.is_none() {
            return Ok(Self {
                logs: None,
                traces: None,
                workers: Vec::new(),
                deadline: None,
            });
        }
        let resource = opentelemetry_sdk::Resource::builder_empty()
            .with_attributes([
                KeyValue::new("service.name", "riffdbd"),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                KeyValue::new("service.instance.id", instance.to_owned()),
            ])
            .build();
        let mut running = Self {
            logs: None,
            traces: None,
            workers: Vec::with_capacity(2),
            deadline: None,
        };
        if let Some(config) = &config.logs {
            let batch = Arc::new(BatchContext::new());
            let mut exporter = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .with_endpoint(endpoint(config))
                .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
                .with_timeout(config.timeout)
                .with_retry_policy(opentelemetry_otlp::RetryPolicy::disabled())
                .with_max_request_body_size(MAX_REQUEST_BYTES)
                .with_http_client(client(config, Arc::clone(&batch))?)
                .build()
                .map_err(|_| error(config, StartCause::Codec))?;
            opentelemetry_sdk::logs::LogExporter::set_resource(&mut exporter, &resource);
            let worker = Worker::start(config, batch, Exporter::Logs(exporter))
                .map_err(|_| error(config, StartCause::Worker))?;
            running.logs = Some(
                SdkLoggerProvider::builder()
                    .with_log_processor(Logs(worker.clone()))
                    .with_resource(resource.clone())
                    .build(),
            );
            running.workers.push(worker);
        }
        if let Some(config) = &config.traces {
            let batch = Arc::new(BatchContext::new());
            let mut exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(endpoint(config))
                .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
                .with_timeout(config.timeout)
                .with_retry_policy(opentelemetry_otlp::RetryPolicy::disabled())
                .with_max_request_body_size(MAX_REQUEST_BYTES)
                .with_http_client(client(config, Arc::clone(&batch))?)
                .build()
                .map_err(|_| error(config, StartCause::Codec))?;
            opentelemetry_sdk::trace::SpanExporter::set_resource(&mut exporter, &resource);
            let worker = Worker::start(config, batch, Exporter::Traces(exporter))
                .map_err(|_| error(config, StartCause::Worker))?;
            running.traces = Some(
                SdkTracerProvider::builder()
                    .with_span_processor(Traces(worker.clone()))
                    .with_sampler(LocalSampler {
                        per_million: config.sample_per_million,
                    })
                    .with_span_limits(opentelemetry_sdk::trace::SpanLimits {
                        max_attributes_per_span: 32,
                        max_events_per_span: 32,
                        max_links_per_span: 1,
                        max_attributes_per_event: 32,
                        max_attributes_per_link: 32,
                    })
                    .with_resource(resource)
                    .build(),
            );
            running.workers.push(worker);
        }
        Ok(running)
    }
    pub(crate) fn tracer(&self) -> Option<opentelemetry_sdk::trace::SdkTracer> {
        self.traces
            .as_ref()
            .map(|provider| provider.tracer("riffdb.request"))
    }
    pub(crate) fn log_bridge(&self) -> Option<LogBridge> {
        self.logs
            .as_ref()
            .map(|provider| LogBridge::new(&OperationalLogProvider(provider.clone())))
    }
    pub(crate) fn begin_drain(&mut self, deadline: Instant) {
        let deadline = self.deadline.map_or(deadline, |old| old.min(deadline));
        self.deadline = Some(deadline);
        for worker in &self.workers {
            worker.begin_drain(deadline);
        }
    }
}
impl Drop for RunningSignals {
    fn drop(&mut self) {
        let deadline = self
            .deadline
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(5));
        self.begin_drain(deadline);
        if let Some(provider) = &self.logs {
            let _ =
                provider.shutdown_with_timeout(deadline.saturating_duration_since(Instant::now()));
        }
        if let Some(provider) = &self.traces {
            let _ =
                provider.shutdown_with_timeout(deadline.saturating_duration_since(Instant::now()));
        }
        for worker in &self.workers {
            let _ = worker.close(deadline);
        }
    }
}
#[derive(Debug)]
struct Logs(Worker);
impl LogProcessor for Logs {
    fn emit(&self, data: &mut SdkLogRecord, scope: &InstrumentationScope) {
        let level = match data.severity_number() {
            Some(opentelemetry::logs::Severity::Error) => Level::ERROR,
            Some(opentelemetry::logs::Severity::Warn) => Level::WARN,
            Some(opentelemetry::logs::Severity::Info) => Level::INFO,
            Some(opentelemetry::logs::Severity::Debug) => Level::DEBUG,
            Some(opentelemetry::logs::Severity::Trace) => Level::TRACE,
            _ => return,
        };
        let Some(AnyValue::String(text)) = data.body() else {
            return;
        };
        let text = text.as_str();
        if text.len() > 16384 {
            metrics().record(Signal::Logs, Disposition::RequestLimit, 1);
            return;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
            metrics().record(Signal::Logs, Disposition::Encoding, 1);
            return;
        };
        if value["schema"] != "riffdb.log/v1" {
            metrics().record(Signal::Logs, Disposition::Encoding, 1);
            return;
        }
        let weight = text.len().saturating_mul(4).saturating_add(1024);
        let Some(body) = body_value(value, 0, &mut 128) else {
            metrics().record(Signal::Logs, Disposition::Encoding, 1);
            return;
        };
        data.set_body(body);
        self.0.enqueue(Some(level), weight, || Record::Log {
            data: data.clone(),
            scope: scope.clone(),
            level,
            weight,
        });
    }
    fn force_flush(&self) -> OTelSdkResult {
        flush(&self.0)
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        close(&self.0, timeout)
    }
}
#[derive(Debug)]
struct Traces(Worker);
impl SpanProcessor for Traces {
    fn on_start(
        &self,
        _span: &mut opentelemetry_sdk::trace::Span,
        _context: &opentelemetry::Context,
    ) {
    }
    fn on_end(&self, data: SpanData) {
        // All exported producers have static names and typed bounded attributes;
        // this second bound also protects the encoder from future mistakes.
        let Some(weight) = span_weight(&data) else {
            metrics().record(Signal::Traces, Disposition::RequestLimit, 1);
            return;
        };
        self.0
            .enqueue(None, weight, || Record::Span { data, weight });
    }
    fn force_flush(&self) -> OTelSdkResult {
        flush(&self.0)
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        close(&self.0, timeout)
    }
}
fn span_weight(data: &SpanData) -> Option<usize> {
    fn attributes(values: &[KeyValue]) -> Option<usize> {
        if values.len() > 32 {
            return None;
        }
        values.iter().try_fold(0usize, |sum, attribute| {
            if attribute.key.as_str().len() > 256 {
                return None;
            }
            let value = match &attribute.value {
                opentelemetry::Value::String(value) if value.as_str().len() <= 256 => {
                    value.as_str().len() * 6
                }
                opentelemetry::Value::Bool(_) | opentelemetry::Value::I64(_) => 32,
                opentelemetry::Value::F64(value) if value.is_finite() => 32,
                // The first-party span vocabulary contains scalar values only.
                _ => return None,
            };
            Some(sum + attribute.key.as_str().len() * 6 + value + 256)
        })
    }
    if data.name.len() > 256 || data.events.len() > 32 || data.links.len() > 1 {
        return None;
    }
    let mut weight = 2048 + data.name.len() * 6 + attributes(&data.attributes)?;
    for event in data.events.iter() {
        if event.name.len() > 256 {
            return None;
        }
        weight += 512 + event.name.len() * 6 + attributes(&event.attributes)?;
    }
    for link in data.links.iter() {
        if link.span_context.trace_state().header().len() > 256 {
            return None;
        }
        weight += 1024 + attributes(&link.attributes)?;
    }
    if let opentelemetry::trace::Status::Error { description } = &data.status {
        if description.len() > 256 {
            return None;
        }
        weight += description.len() * 6;
    }
    (weight <= 900 * 1024).then_some(weight)
}
fn flush(worker: &Worker) -> OTelSdkResult {
    if worker.flush(Duration::from_secs(5)) {
        Ok(())
    } else {
        Err(OTelSdkError::Timeout(Duration::from_secs(5)))
    }
}
fn close(worker: &Worker, timeout: Duration) -> OTelSdkResult {
    if worker.close(Instant::now() + timeout) {
        Ok(())
    } else {
        Err(OTelSdkError::Timeout(timeout))
    }
}
fn body_value(value: serde_json::Value, depth: usize, budget: &mut usize) -> Option<AnyValue> {
    if depth > 8 || *budget == 0 {
        return None;
    }
    *budget -= 1;
    Some(match value {
        serde_json::Value::Null => AnyValue::from("null"),
        serde_json::Value::Bool(value) => AnyValue::Boolean(value),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(AnyValue::Int)
            .unwrap_or_else(|| AnyValue::from(value.to_string())),
        serde_json::Value::String(value) => AnyValue::from(value),
        serde_json::Value::Array(values) => AnyValue::ListAny(Box::new(
            values
                .into_iter()
                .map(|value| body_value(value, depth + 1, budget))
                .collect::<Option<Vec<_>>>()?,
        )),
        serde_json::Value::Object(values) => AnyValue::Map(Box::new(
            values
                .into_iter()
                .map(|(key, value)| {
                    body_value(value, depth + 1, budget).map(|value| (Key::new(key), value))
                })
                .collect::<Option<std::collections::HashMap<_, _>>>()?,
        )),
    })
}
#[derive(Clone, Debug)]
struct LocalSampler {
    per_million: u32,
}
impl ShouldSample for LocalSampler {
    fn should_sample(
        &self,
        parent: Option<&opentelemetry::Context>,
        _trace: opentelemetry::trace::TraceId,
        _name: &str,
        _kind: &opentelemetry::trace::SpanKind,
        _attributes: &[KeyValue],
        _links: &[opentelemetry::trace::Link],
    ) -> SamplingResult {
        let local = parent
            .map(|parent| parent.span().span_context().clone())
            .filter(|parent| parent.is_valid() && !parent.is_remote());
        let sampled = match local {
            Some(parent) => parent.is_sampled(),
            None => {
                if self.per_million == 0 {
                    false
                } else if self.per_million == 1000000 {
                    true
                } else {
                    let mut random = [0; 8];
                    getrandom::fill(&mut random).is_ok()
                        && ((u128::from(u64::from_ne_bytes(random)) * 1000000) >> 64)
                            < u128::from(self.per_million)
                }
            }
        };
        if !sampled {
            metrics().record(Signal::Traces, Disposition::SampledOut, 1);
        }
        SamplingResult {
            decision: if sampled {
                SamplingDecision::RecordAndSample
            } else {
                SamplingDecision::Drop
            },
            attributes: Vec::new(),
            trace_state: opentelemetry::trace::TraceState::default(),
        }
    }
}

pub(crate) fn emit_checked_log<S>(
    bridge: &LogBridge,
    canonical: &str,
    level: Level,
    context: tracing_subscriber::layer::Context<'_, S>,
) where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use tracing::Callsite as _;
    use tracing_subscriber::Layer as _;
    let metadata=match level {
        Level::ERROR=>tracing::callsite!{name:"riffdb.log",kind:tracing::metadata::Kind::EVENT,target:"riffdb.operational",level:Level::ERROR,fields:message}.metadata(),
        Level::WARN=>tracing::callsite!{name:"riffdb.log",kind:tracing::metadata::Kind::EVENT,target:"riffdb.operational",level:Level::WARN,fields:message}.metadata(),
        Level::INFO=>tracing::callsite!{name:"riffdb.log",kind:tracing::metadata::Kind::EVENT,target:"riffdb.operational",level:Level::INFO,fields:message}.metadata(),
        Level::DEBUG=>tracing::callsite!{name:"riffdb.log",kind:tracing::metadata::Kind::EVENT,target:"riffdb.operational",level:Level::DEBUG,fields:message}.metadata(),
        Level::TRACE=>tracing::callsite!{name:"riffdb.log",kind:tracing::metadata::Kind::EVENT,target:"riffdb.operational",level:Level::TRACE,fields:message}.metadata(),
    };
    let Some(message) = metadata.fields().field("message") else {
        return;
    };
    let values = [(&message, Some(&canonical as &dyn tracing::field::Value))];
    let fields = metadata.fields().value_set(&values);
    bridge.on_event(&tracing::Event::new(metadata, &fields), context);
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{SpanContext, SpanId, SpanKind, TraceFlags, TraceId, TraceState};
    // req: ID-005, END-007, END-008, PERF-001
    #[test]
    fn untrusted_traceparent_cannot_override_sampling_or_authority() {
        for sampled in [false, true] {
            let parent = opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
                TraceId::from_bytes([1; 16]),
                SpanId::from_bytes([2; 8]),
                if sampled {
                    TraceFlags::SAMPLED
                } else {
                    TraceFlags::default()
                },
                true,
                TraceState::default(),
            ));
            for (rate, decision) in [
                (0, SamplingDecision::Drop),
                (1_000_000, SamplingDecision::RecordAndSample),
            ] {
                let result = LocalSampler { per_million: rate }.should_sample(
                    Some(&parent),
                    TraceId::from_bytes([3; 16]),
                    "riffdb.ingress",
                    &SpanKind::Internal,
                    &[],
                    &[],
                );
                assert_eq!(result.decision, decision);
                assert_eq!(result.trace_state, TraceState::default());
            }
        }
        let local = opentelemetry::Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from_bytes([1; 16]),
            SpanId::from_bytes([2; 8]),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        ));
        assert_eq!(
            LocalSampler { per_million: 0 }
                .should_sample(
                    Some(&local),
                    TraceId::from_bytes([3; 16]),
                    "riffdb.request",
                    &SpanKind::Internal,
                    &[],
                    &[]
                )
                .decision,
            SamplingDecision::RecordAndSample
        );
    }
}
