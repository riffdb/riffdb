#![forbid(unsafe_code)]

//! `riffdb` public command-line client.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    riffdb_cli::run().await
}
