// req: ID-005, DX-003, END-007, END-008
use super::*;
use crate::config::logging::LogLevel;
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Clone)]
struct Buffer(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct Permissive(Arc<Mutex<Vec<String>>>);
impl<S: Subscriber> Layer<S> for Permissive {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        struct Capture(String);
        impl Visit for Capture {
            fn record_debug(&mut self, _: &Field, value: &dyn std::fmt::Debug) {
                let _ = write!(self.0, "{value:?}");
            }
            fn record_str(&mut self, _: &Field, value: &str) {
                self.0.push_str(value);
            }
        }
        let mut visitor = Capture(String::new());
        event.record(&mut visitor);
        self.0.lock().unwrap().push(visitor.0);
    }
}

fn config(format: LogFormat, level: LogLevel) -> LoggingConfig {
    LoggingConfig {
        format,
        level,
        stderr: true,
        file: None,
        components: BTreeMap::new(),
    }
}

#[test]
fn operational_logging_redacts_before_every_subscriber() {
    let output = Buffer(Arc::default());
    let captured = Arc::new(Mutex::new(Vec::new()));
    let (writer, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(8)
        .finish(output.clone())
        .unwrap();
    let layer = OperatorLayer {
        config: config(LogFormat::Json, LogLevel::Trace),
        writers: vec![writer],
        instance: "test-instance".to_owned(),
        counters: Arc::default(),
        bridge: None,
    };
    let subscriber = tracing_subscriber::registry()
        .with(layer)
        .with(Permissive(captured.clone()));
    tracing::subscriber::with_default(subscriber, || {
        emit(OperationalEvent::ReloadRejected {
            reason: crate::inventory_reload::InventoryReloadError::RetainedDatabaseChanged,
            database: None,
        });
        emit(OperationalEvent::Lifecycle(Lifecycle::Starting));
        let source = crate::daemon::DaemonError::Listener(std::io::Error::other(
            "secret-canary-never-export",
        ));
        emit(OperationalEvent::Failure(&source));
    });
    drop(guard);
    let raw = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<Value> = raw
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len(), 3);
    let mut canonical = lines[0].clone();
    canonical["timestamp"] = json!("2000-01-01T00:00:00Z");
    canonical["version"] = json!("build-version");
    canonical["instance_id"] = json!("process-instance");
    assert_eq!(
        canonical.to_string() + "\n",
        include_str!("../../../../fixtures/observability/operator-log-v1.json")
    );
    assert_eq!(lines[0]["schema"], "riffdb.log/v1");
    assert_eq!(lines[0]["severity"], "WARN");
    assert_eq!(lines[0]["attributes"]["cause"], "retained_database_changed");
    assert_eq!(
        lines[0]["attributes"]["fix"],
        "restart_server_to_apply_configuration"
    );
    assert_eq!(lines[0]["attributes"]["retained_previous"], true);
    assert!(
        captured
            .lock()
            .unwrap()
            .iter()
            .any(|v| v.contains("retained_database_changed"))
    );
    assert!(!raw.contains("/home/"));
    assert!(!raw.contains("secret-canary-never-export"));
    assert!(
        captured
            .lock()
            .unwrap()
            .iter()
            .all(|v| !v.contains("secret-canary-never-export"))
    );
    assert_eq!(lines[2]["attributes"]["cause"], "listener");
    assert!(
        raw.lines()
            .all(|line| line.len() < events::MAX_RECORD_BYTES)
    );
}

#[test]
fn hard_target_and_component_filters_preserve_warning_visibility() {
    let output = Buffer(Arc::default());
    let (writer, guard) = NonBlockingBuilder::default()
        .finish(output.clone())
        .unwrap();
    let layer = OperatorLayer {
        config: config(LogFormat::Json, LogLevel::Info),
        writers: vec![writer],
        instance: "test-instance".to_owned(),
        counters: Arc::default(),
        bridge: None,
    };
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
        tracing::warn!(target:"hyper", secret="secret-canary-never-export");
        emit(OperationalEvent::Provider(Some(
            riffdb_types::exact_provider_budget_v1::ProviderBudget::new()
                .observation()
                .unwrap(),
        )));
        emit(OperationalEvent::Provider(None));
        emit(OperationalEvent::ReloadUnavailable);
    });
    drop(guard);
    let raw = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
    assert!(!raw.contains("secret-canary-never-export"));
    let events: Vec<Value> = raw
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|e| e["severity"] == "WARN"));
    assert!(!events.iter().any(|e| e["event_code"] == "RDB-LOG-0200"));
}

#[test]
fn text_evidence_retains_the_strict_provider_sample_protocol() {
    let output = Buffer(Arc::default());
    let (writer, guard) = NonBlockingBuilder::default()
        .finish(output.clone())
        .unwrap();
    let layer = OperatorLayer {
        config: config(LogFormat::Text, LogLevel::Debug),
        writers: vec![writer],
        instance: "test-instance".to_owned(),
        counters: Arc::default(),
        bridge: None,
    };
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
        emit(OperationalEvent::Provider(Some(
            riffdb_types::exact_provider_budget_v1::ProviderBudget::new()
                .observation()
                .unwrap(),
        )));
    });
    drop(guard);
    let raw = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
    assert!(raw.starts_with(
        "[riffdbd-diag] trigger=exact_provider_lifecycle available=true limit_bytes=1073741824"
    ));
    assert_eq!(raw.lines().count(), 1);
    assert_eq!(raw.split_whitespace().count(), 15);
}

#[test]
fn routine_semantic_observations_do_not_bury_internal_defects() {
    let output = Buffer(Arc::default());
    let (writer, guard) = NonBlockingBuilder::default()
        .finish(output.clone())
        .unwrap();
    let layer = OperatorLayer {
        config: config(LogFormat::Json, LogLevel::Info),
        writers: vec![writer],
        instance: "test-instance".to_owned(),
        counters: Arc::default(),
        bridge: None,
    };
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
        tracing::event!(target: riffdb_observability::SAFE_TRACE_TARGET, tracing::Level::INFO,
            kind_tag=14_u64, operation_tag=0_u64, detail_tag=0_u64, value=1_u64,
            incident_present=false, incident_id_high=0_u64, incident_id_low=0_u64);
        tracing::event!(target: riffdb_observability::SAFE_TRACE_TARGET, tracing::Level::INFO,
            kind_tag=2_u64, operation_tag=0_u64, detail_tag=0_u64, value=1_u64,
            incident_present=false, incident_id_high=0_u64, incident_id_low=0_u64);
    });
    drop(guard);
    let raw = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
    let events: Vec<Value> = raw
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["severity"], "ERROR");
    assert_eq!(events[0]["attributes"]["kind"], "ServiceInternalIntegrity");
}

// req: ID-005, END-007, END-008, PERF-001
#[tokio::test]
async fn otlp_logs_and_request_spans_correlate_without_secret_attributes() {
    use crate::config::telemetry::{Signal, SignalConfig, TelemetryConfig};
    use axum::{Router, extract::State, routing::post};
    use std::time::{Duration, Instant};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Value>(8);
    async fn collect(
        State(sender): State<tokio::sync::mpsc::Sender<Value>>,
        headers: axum::http::HeaderMap,
        body: axum::body::Bytes,
    ) -> ([(&'static str, &'static str); 1], &'static str) {
        assert!(!headers.contains_key("authorization"));
        assert!(!headers.contains_key("x-secret"));
        let value = serde_json::from_slice(&body).unwrap();
        sender.send(value).await.unwrap();
        ([("content-type", "application/json")], "{}")
    }
    let router = Router::new()
        .route("/v1/logs", post(collect))
        .route("/v1/traces", post(collect))
        .with_state(sender);
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let selected = |signal, path| SignalConfig {
        signal,
        push: crate::config::TelemetryPushConfig::new(
            crate::config::parse_telemetry_push_endpoint(&format!("http://{address}/{path}"), None)
                .unwrap(),
            Duration::from_secs(1),
            None,
        ),
        queue_capacity: 128,
        batch_size: 1,
        timeout: Duration::from_secs(3),
        sample_per_million: 1_000_000,
    };
    let telemetry = TelemetryConfig {
        logs: Some(selected(Signal::Logs, "v1/logs")),
        traces: Some(selected(Signal::Traces, "v1/traces")),
    };
    let mut signals =
        crate::telemetry_pipeline::RunningSignals::start(&telemetry, "process-instance").unwrap();
    let dispatch = build_dispatch(
        config(LogFormat::Json, LogLevel::Info),
        Vec::new(),
        "process-instance".into(),
        Arc::default(),
        &signals,
    );
    let request =
        riffdb_types::RequestId::from_unix_milliseconds_and_random(1000, [7; 10]).unwrap();
    tracing::dispatcher::with_default(&dispatch, || {
        let span = riffdb_observability::request_span(
            request,
            riffdb_types::ServiceIngressKindV1::Grpc,
            riffdb_types::ServiceOperationV1::GetEntity,
        );
        span.in_scope(|| emit(OperationalEvent::Lifecycle(Lifecycle::Starting)));
        // A dependency target cannot introduce payloads, even inside a sampled span.
        span.in_scope(|| tracing::warn!(target:"untrusted-dependency",message="secret-canary"));
    });
    let stopped = tokio::task::spawn_blocking(move || {
        signals.begin_drain(Instant::now() + Duration::from_secs(3));
        drop(signals);
    });
    let mut logs = None;
    let mut traces = None;
    for _ in 0..2 {
        let value = tokio::time::timeout(Duration::from_secs(4), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        if value.get("resourceLogs").is_some() {
            logs = Some(value);
        } else if value.get("resourceSpans").is_some() {
            traces = Some(value);
        } else {
            panic!("collector received an unexpected signal");
        }
    }
    stopped.await.unwrap();
    let logs = logs.unwrap();
    let traces = traces.unwrap();
    let resource = &logs["resourceLogs"][0]["resource"]["attributes"];
    assert_eq!(resource.as_array().unwrap().len(), 3);
    assert!(
        resource
            .as_array()
            .unwrap()
            .iter()
            .any(|attribute| attribute["key"] == "service.instance.id"
                && attribute["value"]["stringValue"] == "process-instance")
    );
    assert_eq!(
        logs["resourceLogs"][0]["scopeLogs"][0]["scope"]["name"],
        "riffdb.operational"
    );
    let record = &logs["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
    let span = &traces["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
    assert_eq!(record["traceId"], span["traceId"]);
    assert_eq!(record["spanId"], span["spanId"]);
    let body = record["body"]["kvlistValue"]["values"].as_array().unwrap();
    assert!(body.iter().any(|field| field["key"] == "request_id"
        && field["value"]["stringValue"] == request.to_string()));
    assert!(body.iter().any(
        |field| field["key"] == "trace_id" && field["value"]["stringValue"] == span["traceId"]
    ));
    assert_eq!(span["name"], "riffdb.request");
    let attributes = span["attributes"].as_array().unwrap();
    let expected_transport = riffdb_types::ServiceIngressKindV1::Grpc.tag().to_string();
    let expected_operation = riffdb_types::ServiceOperationV1::GetEntity
        .tag()
        .to_string();
    for (name, expected) in [
        ("transport", expected_transport),
        ("service_operation", expected_operation),
    ] {
        assert!(attributes.iter().any(|attribute| attribute["key"] == name
            && attribute["value"]["intValue"].as_str() == Some(expected.as_str())));
    }

    for value in [logs, traces] {
        let encoded = value.to_string();
        for forbidden in [
            "secret-canary",
            "code.filepath",
            "host.name",
            "process.command",
            "/home/",
        ] {
            assert!(!encoded.contains(forbidden), "{forbidden}");
        }
    }
    server.abort();
}

// req: ID-005, END-007, END-008, PERF-001
#[test]
fn registry_retains_span_admission_until_the_last_executor_parent_is_dropped() {
    let signals =
        crate::telemetry_pipeline::RunningSignals::start(&Default::default(), "instance").unwrap();
    let dispatch = build_dispatch(
        config(LogFormat::Json, LogLevel::Info),
        Vec::new(),
        "instance".into(),
        Arc::default(),
        &signals,
    );
    tracing::dispatcher::with_default(&dispatch, || {
        let permit = Arc::new(riffdb_observability::try_admit_diagnostic_span().unwrap());
        let weak = Arc::downgrade(&permit);
        let span=riffdb_observability::with_diagnostic_span_admission(permit,|| {
            let span=tracing::info_span!(target:riffdb_observability::SAFE_TRACE_TARGET,"riffdb.request");
            assert!(!span.is_disabled());
            // The admission token is consumed exactly once, even at another callsite.
            assert!(tracing::info_span!(target:riffdb_observability::SAFE_TRACE_TARGET,"riffdb.request").is_disabled());
            span
        }).unwrap();
        let parent = span.in_scope(riffdb_observability::RequestSpanParent::capture);
        drop(span);
        assert!(
            weak.upgrade().is_some(),
            "an executor parent still retains the registered span"
        );
        drop(parent);
        assert!(
            weak.upgrade().is_none(),
            "the registry releases the permit on actual span closure"
        );
        assert!(
            tracing::info_span!(target:riffdb_observability::SAFE_TRACE_TARGET,"riffdb.request")
                .is_disabled()
        );
    });
}

// req: ID-005, END-007, END-008, PERF-001
#[tokio::test]
async fn stalled_log_export_cannot_block_trace_delivery_or_extend_the_shared_deadline() {
    use crate::config::telemetry::{Signal, SignalConfig, TelemetryConfig};
    use axum::{Router, extract::State, routing::post};
    use std::time::{Duration, Instant};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (seen, mut received) = tokio::sync::mpsc::channel(4);
    async fn blocked(
        State(sender): State<tokio::sync::mpsc::Sender<&'static str>>,
    ) -> &'static str {
        sender.send("logs").await.unwrap();
        std::future::pending::<()>().await;
        "{}"
    }
    async fn healthy(
        State(sender): State<tokio::sync::mpsc::Sender<&'static str>>,
    ) -> &'static str {
        sender.send("traces").await.unwrap();
        "{}"
    }
    let router = Router::new()
        .route("/v1/logs", post(blocked))
        .route("/v1/traces", post(healthy))
        .with_state(seen);
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let selected = |signal, path| SignalConfig {
        signal,
        push: crate::config::TelemetryPushConfig::new(
            crate::config::parse_telemetry_push_endpoint(&format!("http://{address}/{path}"), None)
                .unwrap(),
            Duration::from_secs(1),
            None,
        ),
        queue_capacity: 128,
        batch_size: 1,
        timeout: Duration::from_secs(10),
        sample_per_million: 1_000_000,
    };
    let telemetry = TelemetryConfig {
        logs: Some(selected(Signal::Logs, "v1/logs")),
        traces: Some(selected(Signal::Traces, "v1/traces")),
    };
    let mut signals =
        crate::telemetry_pipeline::RunningSignals::start(&telemetry, "process-instance").unwrap();
    let dispatch = build_dispatch(
        config(LogFormat::Json, LogLevel::Info),
        Vec::new(),
        "process-instance".into(),
        Arc::default(),
        &signals,
    );
    tracing::dispatcher::with_default(&dispatch, || {
        let request =
            riffdb_types::RequestId::from_unix_milliseconds_and_random(1000, [7; 10]).unwrap();
        let span = riffdb_observability::request_span(
            request,
            riffdb_types::ServiceIngressKindV1::Grpc,
            riffdb_types::ServiceOperationV1::GetEntity,
        );
        span.in_scope(|| emit(OperationalEvent::Lifecycle(Lifecycle::Starting)));
    });
    let mut observed = Vec::new();
    for _ in 0..2 {
        observed.push(
            tokio::time::timeout(Duration::from_secs(3), received.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    observed.sort();
    assert_eq!(observed, ["logs", "traces"]);
    // The stalled request has a ten-second export timeout. The shared shutdown
    // deadline must interrupt it, rather than waiting for that timeout.
    let elapsed = tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        signals.begin_drain(started + Duration::from_millis(100));
        drop(signals);
        started.elapsed()
    })
    .await
    .unwrap();
    assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
    server.abort();
}

// req: ID-005, NET-002, NET-003, END-007, END-008
#[test]
fn ambient_otel_configuration_cannot_redirect_or_enrich_the_configured_pipeline() {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command.args(["--exact","operational_logging::tests::otlp_logs_and_request_spans_correlate_without_secret_attributes","--nocapture"]);
    for (name, value) in [
        (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "http://192.0.2.1:1/secret-canary",
        ),
        (
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            "http://192.0.2.1:1/secret-canary",
        ),
        (
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://192.0.2.1:1/secret-canary",
        ),
        ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
        ("OTEL_EXPORTER_OTLP_TIMEOUT", "1"),
        (
            "OTEL_EXPORTER_OTLP_HEADERS",
            "authorization=secret-canary,x-secret=secret-canary",
        ),
        (
            "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
            "authorization=secret-canary",
        ),
        (
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
            "x-secret=secret-canary",
        ),
        (
            "OTEL_RESOURCE_ATTRIBUTES",
            "host.name=secret-canary,service.name=secret-canary",
        ),
        ("OTEL_SERVICE_NAME", "secret-canary"),
        ("OTEL_TRACES_SAMPLER", "always_off"),
        ("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip"),
        ("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "0"),
    ] {
        command.env(name, value);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "collector child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
}

// req: ID-005, PERF-001, NET-002, NET-003, END-007, END-008
#[tokio::test]
async fn all_three_signals_drain_concurrently_under_one_process_deadline() {
    use crate::config::telemetry::{Signal, SignalConfig, TelemetryConfig};
    use axum::{Router, extract::State, routing::post};
    use std::time::{Duration, Instant};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    async fn blocked(
        State(sender): State<tokio::sync::mpsc::Sender<&'static str>>,
        request: axum::extract::Request,
    ) -> &'static str {
        sender
            .send(if request.uri().path() == "/v1/logs" {
                "logs"
            } else {
                "metrics"
            })
            .await
            .unwrap();
        std::future::pending::<()>().await;
        "{}"
    }
    async fn healthy(
        State(sender): State<tokio::sync::mpsc::Sender<&'static str>>,
    ) -> &'static str {
        sender.send("traces").await.unwrap();
        "{}"
    }
    let router = Router::new()
        .route("/v1/logs", post(blocked))
        .route("/v1/metrics", post(blocked))
        .route("/v1/traces", post(healthy))
        .with_state(sender);
    let collector = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let push = |path| {
        crate::config::TelemetryPushConfig::new(
            crate::config::parse_telemetry_push_endpoint(&format!("http://{address}/{path}"), None)
                .unwrap(),
            Duration::from_secs(1),
            None,
        )
    };
    let selected = |signal, path| SignalConfig {
        signal,
        push: push(path),
        queue_capacity: 128,
        batch_size: 1,
        timeout: Duration::from_secs(10),
        sample_per_million: 1_000_000,
    };
    let signals = crate::telemetry_pipeline::RunningSignals::start(
        &TelemetryConfig {
            logs: Some(selected(Signal::Logs, "v1/logs")),
            traces: Some(selected(Signal::Traces, "v1/traces")),
        },
        "instance",
    )
    .unwrap();
    let dispatch = build_dispatch(
        config(LogFormat::Json, LogLevel::Info),
        Vec::new(),
        "instance".into(),
        Arc::default(),
        &signals,
    );
    let identifiers = crate::identifiers::ProductionIdentifierSources::new();
    let observability = Arc::new(
        riffdb_observability::Observability::new(
            Arc::new(identifiers.incident_ids()),
            riffdb_observability::MAX_TRACE_RECORDS,
        )
        .unwrap(),
    );
    let directory = tempfile::tempdir().unwrap();
    let (metrics, source) = crate::telemetry_metrics_worker::process_source_for_test(
        observability,
        directory.path().join("test.redb"),
        &push("v1/metrics"),
    );
    let mut logging = RunningLogging {
        writers: Vec::new(),
        deadline: None,
        sampler: None,
        counters: Arc::default(),
        signals: Some(signals),
        metrics: Some(metrics),
    };
    tracing::dispatcher::with_default(&dispatch, || {
        let span = riffdb_observability::request_span(
            riffdb_types::RequestId::from_unix_milliseconds_and_random(1000, [7; 10]).unwrap(),
            riffdb_types::ServiceIngressKindV1::Grpc,
            riffdb_types::ServiceOperationV1::GetEntity,
        );
        span.in_scope(|| emit(OperationalEvent::Lifecycle(Lifecycle::Starting)));
    });
    let mut observed = Vec::new();
    for _ in 0..3 {
        observed.push(
            tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    observed.sort();
    assert_eq!(observed, ["logs", "metrics", "traces"]);
    assert!(
        source.stop().is_none(),
        "the process root owns the final metrics drain"
    );
    let elapsed = tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        logging.begin_final_drain(started + Duration::from_millis(100));
        drop(logging);
        started.elapsed()
    })
    .await
    .unwrap();
    assert!(
        elapsed < Duration::from_secs(1),
        "all diagnostic sinks share the same deadline: {elapsed:?}"
    );
    collector.abort();
}
