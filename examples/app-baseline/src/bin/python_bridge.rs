//! Holds a live TicketDesk `riffdbd` so the Python generated client can load.

#![forbid(unsafe_code)]

use std::env;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

use riffdb_app_baseline_riffdb::{
    RiffDbServerSession, ServerStartOptions, min_free_bytes_for_full,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("python-bridge failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let riffdbd = env::var_os("RIFFDB_APP_BASELINE_RIFFDBD_BIN")
        .map(PathBuf::from)
        .ok_or("RIFFDB_APP_BASELINE_RIFFDBD_BIN is required")?;
    if !riffdbd.is_file() {
        return Err(format!("riffdbd binary not found: {}", riffdbd.display()));
    }
    let credential_path = env::var_os("RIFFDB_PYTHON_BRIDGE_CREDENTIAL")
        .map(PathBuf::from)
        .ok_or("RIFFDB_PYTHON_BRIDGE_CREDENTIAL is required")?;
    let keepalive_path = env::var_os("RIFFDB_PYTHON_BRIDGE_KEEPALIVE")
        .map(PathBuf::from)
        .ok_or("RIFFDB_PYTHON_BRIDGE_KEEPALIVE is required")?;
    let full = env::var_os("RIFFDB_PYTHON_BRIDGE_FULL").is_some_and(|value| value == "1");
    let options = ServerStartOptions {
        min_free_bytes: min_free_bytes_for_full(full),
        ..ServerStartOptions::default()
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let session = runtime
        .block_on(RiffDbServerSession::start_with_options(&riffdbd, options))
        .map_err(|error| error.to_string())?;
    write_mode_600(
        &credential_path,
        session.backend.runner_bearer_token().as_bytes(),
    )?;
    let endpoint = session.backend.public_grpc_uri();
    println!(
        "riffdb-python-bridge-ready-v1\t{}\t{}",
        endpoint,
        credential_path.display()
    );
    io::stdout().flush().map_err(|error| error.to_string())?;
    // Bash scripts redirect background stdin from /dev/null, so waiting on EOF
    // would drop riffdbd immediately. The launcher removes this file on cleanup.
    while keepalive_path.is_file() {
        thread::sleep(Duration::from_millis(200));
    }
    drop(session);
    Ok(())
}

fn write_mode_600(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(bytes).map_err(|error| error.to_string())?;
    file.flush().map_err(|error| error.to_string())?;
    Ok(())
}
