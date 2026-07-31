#![forbid(unsafe_code)]

//! Process entry point for the public-client MCP stdio bridge.

use std::io::Write as _;
use std::process::ExitCode;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("doctor")) {
        return match riffdb_mcp_stdio::doctor().await {
            Ok(report) => {
                let mut stdout = std::io::stdout().lock();
                if stdout
                    .write_all(report.as_bytes())
                    .and_then(|()| stdout.write_all(b"\n"))
                    .is_ok()
                {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                }
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        };
    }
    match riffdb_mcp_stdio::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
