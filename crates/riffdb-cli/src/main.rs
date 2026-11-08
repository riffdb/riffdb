#![forbid(unsafe_code)]
//! `riffdb` public command-line client.
use std::process::ExitCode;
fn main() -> ExitCode {
    if let Some(code) = riffdb_cli::early_version_from_env() {
        return code;
    }
    run_client()
}
#[tokio::main]
async fn run_client() -> ExitCode {
    if let Some(code) = riffdb_cli::saga::try_run_from_env().await {
        return code;
    }
    riffdb_cli::run().await
}
