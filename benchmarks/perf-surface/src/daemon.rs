//! One live `riffdbd` process, started the way the shipped binary expects.
//!
//! The harness drives the real daemon over its public surface rather than
//! reaching into the crates behind it, so a per-mechanism cost measured here is
//! a cost a deployment would actually pay.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Digest keys the daemon requires. Fixed so a run is reproducible.
const CAPABILITY_KEY_DOCUMENT: &[u8] =
    b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] =
    b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "perf-surface";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const READY_TIMEOUT: Duration = Duration::from_secs(120);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(120);

/// Failures starting or stopping the daemon.
#[derive(Debug)]
pub enum DaemonError {
    /// The process could not be spawned or its pipes could not be taken.
    Spawn(String),
    /// The daemon did not print a ready line within the timeout.
    NotReady(String),
    /// The daemon did not exit cleanly.
    Shutdown(String),
    /// A filesystem operation for the run directory failed.
    Io(String),
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn(detail) => write!(formatter, "spawning riffdbd failed: {detail}"),
            Self::NotReady(detail) => write!(formatter, "riffdbd never became ready: {detail}"),
            Self::Shutdown(detail) => write!(formatter, "riffdbd shutdown failed: {detail}"),
            Self::Io(detail) => write!(formatter, "run directory: {detail}"),
        }
    }
}

impl std::error::Error for DaemonError {}

/// A running daemon plus the lines it has emitted.
pub struct Daemon {
    child: Child,
    address: String,
    stdout_lines: Arc<Mutex<Vec<String>>>,
    stderr_lines: Arc<Mutex<Vec<String>>>,
}

impl Daemon {
    /// Starts one daemon against a fresh database under `run_dir`.
    pub fn start(binary: &Path, run_dir: &Path) -> Result<Self, DaemonError> {
        let io = |error: std::io::Error| DaemonError::Io(error.to_string());
        std::fs::create_dir_all(run_dir).map_err(io)?;
        let database = run_dir.join("riffdb.redb");
        let backups = run_dir.join("backups");
        std::fs::create_dir_all(&backups).map_err(io)?;
        let capability_keys = run_dir.join("capability.keys");
        let idempotency_keys = run_dir.join("idempotency.keys");
        write_protected(&capability_keys, CAPABILITY_KEY_DOCUMENT).map_err(io)?;
        write_protected(&idempotency_keys, IDEMPOTENCY_KEY_DOCUMENT).map_err(io)?;

        let mut child = Command::new(binary)
            .arg("--database")
            .arg(&database)
            .arg("--backup-root")
            .arg(&backups)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--environment")
            .arg(ENVIRONMENT)
            .arg("--audience")
            .arg(AUDIENCE)
            .arg("--capability-keys")
            .arg(&capability_keys)
            .arg("--idempotency-keys")
            .arg(&idempotency_keys)
            .env("RUST_BACKTRACE", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| DaemonError::Spawn(error.to_string()))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| DaemonError::Spawn("no stdout".to_owned()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| DaemonError::Spawn("no stderr".to_owned()))?;

        let stdout_lines = Arc::new(Mutex::new(Vec::new()));
        let stderr_lines = Arc::new(Mutex::new(Vec::new()));
        let (ready_sender, ready) = mpsc::sync_channel::<String>(1);

        let collected = Arc::clone(&stdout_lines);
        thread::spawn(move || {
            let mut announced = false;
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                // Scan for the ready line rather than assuming it comes first:
                // the daemon may emit diagnostics ahead of it.
                if !announced && let Some(address) = line.strip_prefix(READY_PREFIX) {
                    announced = true;
                    let _ = ready_sender.send(address.trim().to_owned());
                }
                if let Ok(mut lines) = collected.lock() {
                    lines.push(line);
                }
            }
        });

        let collected_err = Arc::clone(&stderr_lines);
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if let Ok(mut lines) = collected_err.lock() {
                    lines.push(line);
                }
            }
        });

        let address = wait_for_ready(&ready, &stderr_lines)?;
        Ok(Self {
            child,
            address,
            stdout_lines,
            stderr_lines,
        })
    }

    /// The loopback endpoint the daemon is listening on.
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Every stdout line so far, including the shutdown census.
    #[must_use]
    pub fn stdout(&self) -> Vec<String> {
        self.stdout_lines
            .lock()
            .map(|lines| lines.clone())
            .unwrap_or_default()
    }

    /// The last stderr lines, for diagnosing a failure.
    #[must_use]
    pub fn stderr_tail(&self, lines: usize) -> Vec<String> {
        let all = self
            .stderr_lines
            .lock()
            .map(|lines| lines.clone())
            .unwrap_or_default();
        all.iter().rev().take(lines).rev().cloned().collect()
    }

    /// Asks the daemon to close cleanly and returns its stdout, which carries
    /// the writer census the measurement reads.
    pub fn shutdown(mut self) -> Result<Vec<String>, DaemonError> {
        if let Some(mut stdin) = self.child.stdin.take() {
            let _ = stdin.write_all(b"shutdown\n");
            let _ = stdin.flush();
        }
        let deadline = std::time::Instant::now() + SHUTDOWN_TIMEOUT;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    // Give the reader threads a moment to drain the final
                    // census lines the daemon writes as it closes.
                    thread::sleep(Duration::from_millis(250));
                    if status.success() {
                        return Ok(self.stdout());
                    }
                    return Err(DaemonError::Shutdown(format!(
                        "exit {status}: {}",
                        self.stderr_tail(20).join(" | ")
                    )));
                }
                Ok(None) if std::time::Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(50));
                }
                Ok(None) => {
                    let _ = self.child.kill();
                    return Err(DaemonError::Shutdown("timed out".to_owned()));
                }
                Err(error) => return Err(DaemonError::Shutdown(error.to_string())),
            }
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // A harness that fails mid-run must not leave a daemon holding the
        // database directory.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_for_ready(
    ready: &Receiver<String>,
    stderr_lines: &Arc<Mutex<Vec<String>>>,
) -> Result<String, DaemonError> {
    match ready.recv_timeout(READY_TIMEOUT) {
        Ok(address) if !address.is_empty() => Ok(address),
        Ok(_) => Err(DaemonError::NotReady("empty ready address".to_owned())),
        Err(RecvTimeoutError::Timeout) => Err(DaemonError::NotReady(format!(
            "no ready line within {}s: {}",
            READY_TIMEOUT.as_secs(),
            tail(stderr_lines)
        ))),
        Err(RecvTimeoutError::Disconnected) => Err(DaemonError::NotReady(format!(
            "exited before ready: {}",
            tail(stderr_lines)
        ))),
    }
}

fn tail(lines: &Arc<Mutex<Vec<String>>>) -> String {
    lines
        .lock()
        .map(|lines| {
            lines
                .iter()
                .rev()
                .take(20)
                .rev()
                .cloned()
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .unwrap_or_default()
}

pub(crate) fn write_protected(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Resolves the `riffdbd` binary the harness should drive.
#[must_use]
pub fn riffdbd_binary() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("RIFFDB_PERF_SURFACE_RIFFDBD_BIN") {
        let path = PathBuf::from(explicit);
        return path.is_file().then_some(path);
    }
    let candidate = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/release/riffdbd")
        .canonicalize()
        .ok()?;
    candidate.is_file().then_some(candidate)
}
