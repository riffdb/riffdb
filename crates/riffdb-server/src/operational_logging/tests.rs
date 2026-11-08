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
        writer,
        instance: "test-instance".to_owned(),
        counters: Arc::default(),
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
        writer,
        instance: "test-instance".to_owned(),
        counters: Arc::default(),
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
        writer,
        instance: "test-instance".to_owned(),
        counters: Arc::default(),
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
        writer,
        instance: "test-instance".to_owned(),
        counters: Arc::default(),
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
