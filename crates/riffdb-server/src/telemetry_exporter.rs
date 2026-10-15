//! Optional loopback HTTP metrics exposition (`/metrics`, `/v1/metrics`) and
//! periodic NDJSON soak flight-recorder writer (`ADR-0306`).

use std::fs::{self, OpenOptions};
use std::future::IntoFuture;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::routing::get;
use riffdb_observability::{Observability, RequiredGauge};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
const OTEL_JSON_CONTENT_TYPE: &str = "application/json";
const MAX_JOURNAL_SIBLING_SCAN_ENTRIES: usize = 256;

#[derive(Clone)]
struct MetricsHttpState {
    observability: Arc<Observability>,
    database_path: PathBuf,
}

/// Owns the optional loopback HTTP metrics server and periodic NDJSON flight recorder.
pub(crate) struct RunningTelemetryExporter {
    observability: Arc<Observability>,
    database_path: PathBuf,
    local_address: Option<SocketAddr>,
    flight_recorder_path: Option<PathBuf>,
    shutdown_tx: Option<watch::Sender<bool>>,
    http_task: Option<JoinHandle<()>>,
    flight_task: Option<JoinHandle<()>>,
}

impl RunningTelemetryExporter {
    /// Starts the configured loopback metrics listener and/or periodic flight recorder.
    ///
    /// When neither is configured, returns an inert handle with zero I/O or tasks.
    pub(crate) fn start(
        observability: Arc<Observability>,
        database_path: PathBuf,
        metrics_listen_address: Option<SocketAddr>,
        flight_recorder_path: Option<PathBuf>,
        flight_interval: Duration,
    ) -> Self {
        if metrics_listen_address.is_none() && flight_recorder_path.is_none() {
            return Self {
                observability,
                database_path,
                local_address: None,
                flight_recorder_path: None,
                shutdown_tx: None,
                http_task: None,
                flight_task: None,
            };
        }

        if let Some(path) = &flight_recorder_path {
            append_flight_record_snapshot(&observability, &database_path, path);
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut local_address = None;
        let mut http_task = None;
        let mut flight_task = None;

        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            if let Some(listen_addr) = metrics_listen_address
                && listen_addr.ip().is_loopback()
                && let Ok(std_listener) = std::net::TcpListener::bind(listen_addr)
                && std_listener.set_nonblocking(true).is_ok()
            {
                local_address = std_listener.local_addr().ok();
                let state = MetricsHttpState {
                    observability: Arc::clone(&observability),
                    database_path: database_path.clone(),
                };
                let mut rx = shutdown_rx.clone();
                http_task = Some(runtime.spawn(async move {
                    let Ok(listener) = tokio::net::TcpListener::from_std(std_listener) else {
                        return;
                    };
                    let router = Router::new()
                        .route("/metrics", get(handle_prometheus_metrics))
                        .route("/v1/metrics", get(handle_otel_json_metrics))
                        .with_state(state);
                    let server = axum::serve(listener, router)
                        .with_graceful_shutdown(async move {
                            while !*rx.borrow_and_update() {
                                if rx.changed().await.is_err() {
                                    break;
                                }
                            }
                        })
                        .into_future();
                    let _ = server.await;
                }));
            }

            if let Some(path) = flight_recorder_path.clone() {
                let obs = Arc::clone(&observability);
                let db_path = database_path.clone();
                let mut rx = shutdown_rx;
                let interval_duration = flight_interval.max(Duration::from_millis(100));
                flight_task = Some(runtime.spawn(async move {
                    let mut interval = tokio::time::interval_at(
                        Instant::now() + interval_duration,
                        interval_duration,
                    );
                    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            biased;
                            changed = rx.changed() => {
                                if changed.is_err() || *rx.borrow_and_update() {
                                    break;
                                }
                            }
                            _ = interval.tick() => {
                                append_flight_record_snapshot(&obs, &db_path, &path);
                            }
                        }
                    }
                }));
            }
        }

        Self {
            observability,
            database_path,
            local_address,
            flight_recorder_path,
            shutdown_tx: Some(shutdown_tx),
            http_task,
            flight_task,
        }
    }

    #[allow(dead_code)]
    pub(crate) const fn local_address(&self) -> Option<SocketAddr> {
        self.local_address
    }

    /// Signals background tasks to stop and writes one final post-drain flight-recorder snapshot.
    pub(crate) fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
        if let Some(task) = self.flight_task.take() {
            task.abort();
        }
        if let Some(task) = self.http_task.take() {
            task.abort();
        }
        if let Some(path) = self.flight_recorder_path.take() {
            append_flight_record_snapshot(&self.observability, &self.database_path, &path);
        }
    }
}

impl Drop for RunningTelemetryExporter {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn handle_prometheus_metrics(
    State(state): State<MetricsHttpState>,
) -> (StatusCode, [(header::HeaderName, &'static str); 1], String) {
    let (storage_bytes, _) = measure_storage_and_journal_bytes(&state.database_path);
    state
        .observability
        .metrics()
        .set_required_gauge(RequiredGauge::StorageBytes, storage_bytes);
    let body = state.observability.encode_prometheus_v1();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
        body,
    )
}

async fn handle_otel_json_metrics(
    State(state): State<MetricsHttpState>,
) -> (StatusCode, [(header::HeaderName, &'static str); 1], String) {
    let (storage_bytes, _) = measure_storage_and_journal_bytes(&state.database_path);
    state
        .observability
        .metrics()
        .set_required_gauge(RequiredGauge::StorageBytes, storage_bytes);
    let timestamp_unix_nanos = current_unix_timestamp_nanos();
    let body = state
        .observability
        .encode_otel_json_v1(timestamp_unix_nanos);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, OTEL_JSON_CONTENT_TYPE)],
        body,
    )
}

fn append_flight_record_snapshot(
    observability: &Observability,
    database_path: &Path,
    flight_recorder_path: &Path,
) {
    let (storage_bytes, journal_bytes) = measure_storage_and_journal_bytes(database_path);
    observability
        .metrics()
        .set_required_gauge(RequiredGauge::StorageBytes, storage_bytes);
    let timestamp_unix_nanos = current_unix_timestamp_nanos();
    let line = observability.encode_flight_record_ndjson_v1(
        timestamp_unix_nanos,
        storage_bytes,
        journal_bytes,
    );
    if let Some(parent) = flight_recorder_path.parent()
        && !parent.as_os_str().is_empty()
    {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(flight_recorder_path)
    {
        let _ = file.write_all(line.as_bytes());
        let _ = file.flush();
    }
}

fn current_unix_timestamp_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        })
}

fn measure_storage_and_journal_bytes(database_path: &Path) -> (u64, u64) {
    let storage_bytes = fs::metadata(database_path).map_or(0, |meta| meta.len());
    let mut journal_bytes = 0_u64;
    if let Some(file_name) = database_path.file_name().and_then(|name| name.to_str()) {
        let journal_prefix = format!("{file_name}.riffjournal");
        let parent = database_path
            .parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.take(MAX_JOURNAL_SIBLING_SCAN_ENTRIES).flatten() {
                if let Some(entry_name) = entry.file_name().to_str()
                    && entry_name.starts_with(&journal_prefix)
                    && let Ok(meta) = entry.metadata()
                    && meta.is_file()
                {
                    journal_bytes = journal_bytes.saturating_add(meta.len());
                }
            }
        }
    }
    (storage_bytes, journal_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifiers::ProductionIdentifierSources;
    use riffdb_observability::{
        AuthoritativeComponent, AuthoritativeCondition, CommandPipelineStage, MAX_TRACE_RECORDS,
        ReadPipelineStage, RequiredCounter,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn http_request(addr: SocketAddr, request: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to loopback metrics listener");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write HTTP request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read HTTP response");
        String::from_utf8(response).expect("UTF-8 HTTP response")
    }

    #[tokio::test]
    // req: ID-005, PERF-001, END-007, END-008
    async fn loopback_metrics_listener_serves_prometheus_and_otel_json_and_flight_recorder_writes_ndjson()
     {
        let temp =
            tempfile::TempDir::with_prefix("riffdb-telemetry-exporter-").expect("create temp dir");
        let db_path = temp.path().join("test.redb");
        let journal_path = temp.path().join("test.redb.riffjournal");
        let flight_path = temp.path().join("telemetry").join("flight.ndjson");
        fs::write(&db_path, vec![b'd'; 4096]).expect("write mock database file");
        fs::write(&journal_path, vec![b'j'; 1024]).expect("write mock journal file");

        let identifiers = ProductionIdentifierSources::new();
        let obs = Arc::new(
            Observability::new(Arc::new(identifiers.incident_ids()), MAX_TRACE_RECORDS)
                .expect("build observability"),
        );
        for component in [
            AuthoritativeComponent::Storage,
            AuthoritativeComponent::Catalog,
            AuthoritativeComponent::CommitCoordinator,
        ] {
            obs.health()
                .set_authoritative(component, AuthoritativeCondition::Healthy);
        }
        for _ in 0..3 {
            obs.metrics()
                .increment_required_counter(RequiredCounter::Commits);
        }
        obs.metrics()
            .observe_command_stage_duration(CommandPipelineStage::Evaluation, 420);
        obs.metrics()
            .observe_read_stage_duration(ReadPipelineStage::Execute, 85);

        let mut exporter = RunningTelemetryExporter::start(
            Arc::clone(&obs),
            db_path.clone(),
            Some("127.0.0.1:0".parse().expect("loopback")),
            Some(flight_path.clone()),
            Duration::from_millis(100),
        );
        let listen_addr = exporter
            .local_address()
            .expect("bound loopback metrics address");
        assert!(listen_addr.ip().is_loopback());

        // Scrape GET /metrics (Prometheus text format).
        let prom_response = http_request(
            listen_addr,
            "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            prom_response.starts_with("HTTP/1.1 200 OK\r\n"),
            "unexpected /metrics status: {prom_response}"
        );
        assert!(
            prom_response.contains("content-type: text/plain; version=0.0.4; charset=utf-8"),
            "missing Prometheus content-type header: {prom_response}"
        );
        assert!(prom_response.contains("riffdb_commits_total 3"));
        assert!(prom_response.contains("riffdb_storage_bytes 4096"));
        assert!(
            prom_response.contains(
                "riffdb_command_stage_duration_microseconds_count{stage=\"evaluation\"} 1"
            )
        );

        // Scrape GET /v1/metrics (OTLP JSON format).
        let otel_response = http_request(
            listen_addr,
            "GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            otel_response.starts_with("HTTP/1.1 200 OK\r\n"),
            "unexpected /v1/metrics status: {otel_response}"
        );
        assert!(
            otel_response.contains("content-type: application/json"),
            "missing JSON content-type header: {otel_response}"
        );
        let (_, json_body) = otel_response
            .split_once("\r\n\r\n")
            .expect("HTTP header/body separator");
        let parsed_otel: serde_json::Value =
            serde_json::from_str(json_body).expect("valid OTLP JSON body");
        assert!(parsed_otel["resourceMetrics"].is_array());

        // Non-GET methods and unknown paths fail closed.
        let post_response = http_request(
            listen_addr,
            "POST /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            post_response.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
            "POST /metrics must fail closed with 405: {post_response}"
        );
        let unknown_response = http_request(
            listen_addr,
            "GET /admin HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(
            unknown_response.starts_with("HTTP/1.1 404 Not Found\r\n"),
            "GET /admin must fail closed with 404: {unknown_response}"
        );

        // Shut down and verify the flight-recorder file has valid NDJSON lines with storage & journal bytes.
        for _ in 0..2 {
            obs.metrics()
                .increment_required_counter(RequiredCounter::Commits);
        }
        exporter.shutdown();

        let ndjson = fs::read_to_string(&flight_path).expect("read flight recorder file");
        let lines: Vec<&str> = ndjson.lines().collect();
        assert!(
            lines.len() >= 2,
            "flight recorder must write at least startup and shutdown snapshots, got {}",
            lines.len()
        );
        let last_record: serde_json::Value =
            serde_json::from_str(lines.last().expect("last line")).expect("valid NDJSON line");
        assert_eq!(last_record["schema"], "riffdb.telemetry.flight-record.v1");
        assert_eq!(last_record["schema_version"], 1);
        assert_eq!(last_record["storage_bytes"], 4096);
        assert_eq!(last_record["journal_bytes"], 1024);
        assert_eq!(last_record["commits_total"], 5);
    }
}
