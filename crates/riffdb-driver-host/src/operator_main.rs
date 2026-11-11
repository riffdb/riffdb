//! RiffDB operator-driver host process.

#![forbid(unsafe_code)]

use std::path::Path;

use riffdb_driver_host::OperatorDriverRuntime;

#[tokio::main]
async fn main() {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let Some(config) = arguments.next() else {
        eprintln!(
            "riffdb-operator-driverd: expected one protected configuration path (Usage: riffdb-operator-driverd [--version] [--help] <CONFIG_PATH>)"
        );
        std::process::exit(2);
    };
    if arguments.next().is_some() {
        eprintln!(
            "riffdb-operator-driverd: expected one protected configuration path (Usage: riffdb-operator-driverd [--version] [--help] <CONFIG_PATH>)"
        );
        std::process::exit(2);
    }
    if config == "--version" || config == "-V" {
        println!("riffdb-operator-driverd {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if config == "--help" || config == "-h" {
        println!("Usage: riffdb-operator-driverd [--version] [--help] <CONFIG_PATH>");
        return;
    }
    if config.as_encoded_bytes().starts_with(b"-") {
        eprintln!(
            "riffdb-operator-driverd: unknown option '{}' (expected --version, --help, or one protected configuration path)",
            config.to_string_lossy()
        );
        std::process::exit(2);
    }
    let runtime = match OperatorDriverRuntime::from_config_file(Path::new(&config)).await {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("riffdb-operator-driverd: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = runtime.serve_until(shutdown_signal()).await {
        eprintln!("riffdb-operator-driverd: {error}");
        std::process::exit(1);
    }
}

async fn shutdown_signal() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        else {
            let _ = interrupt.await;
            return;
        };
        tokio::select! { _=interrupt=>{}, _=terminate.recv()=>{} }
    }
    #[cfg(not(unix))]
    {
        let _ = interrupt.await;
    }
}
