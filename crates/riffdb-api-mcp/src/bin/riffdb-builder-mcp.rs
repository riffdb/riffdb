#![forbid(unsafe_code)]

//! Credential-less local application-authoring MCP process.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;

use riffdb_api_mcp::{BuilderMcpConfiguration, BuilderMcpServer, serve_builder_mcp_stdio};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(()) => {
            eprintln!("riffdb-builder-mcp failed");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), ()> {
    let mut arguments = std::env::args_os();
    let _program = arguments.next().ok_or(())?;
    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("--workspace")) {
        return Err(());
    }
    let workspace = bounded_path(arguments.next().ok_or(())?)?;
    if arguments.next().is_some() {
        return Err(());
    }

    let executable = std::env::current_exe().map_err(|_| ())?;
    let bin = executable.parent().ok_or(())?;
    let kit_root = bin.parent().ok_or(())?.to_path_buf();
    let riffdb = bin.join("riffdb");
    let configuration =
        BuilderMcpConfiguration::new(workspace, kit_root, riffdb).map_err(|_| ())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| ())?;
    runtime
        .block_on(serve_builder_mcp_stdio(BuilderMcpServer::new(
            configuration,
        )))
        .map_err(|_| ())
}

fn bounded_path(value: OsString) -> Result<PathBuf, ()> {
    if value.is_empty() || value.as_encoded_bytes().len() > 4_096 {
        return Err(());
    }
    Ok(PathBuf::from(value))
}
