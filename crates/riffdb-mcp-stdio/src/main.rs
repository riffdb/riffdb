#![forbid(unsafe_code)]

//! Process entry point for the public-client MCP stdio bridge.

use std::io::Write as _;
use std::process::ExitCode;

const USAGE: &str = "\
Usage: riffdb-mcp [doctor] [OPTIONS]

Options:
  --config <PATH>            Path to mcp.toml configuration file
  --endpoint <URI>           RiffDB gRPC endpoint (http://loopback:port or https://host:port)
  --database <ALIAS>         Target database alias
  --tls-trust-root <PATH>    PEM CA bundle path for https:// endpoints
  --tls-server-name <NAME>   Expected TLS server name for https:// endpoints
  -h, --help                 Print help
  -V, --version              Print version
";

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    for arg in std::env::args_os().skip(1) {
        if arg == "--help" || arg == "-h" {
            eprint!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        if arg == "--version" || arg == "-V" {
            eprintln!("riffdb-mcp {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
    }
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
