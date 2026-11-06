#![forbid(unsafe_code)]

//! `riffdb` public command-line client.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    if let Some(code) = riffdb_cli::saga::try_run_from_env().await {
        return code;
    }
    riffdb_cli::run().await
}
