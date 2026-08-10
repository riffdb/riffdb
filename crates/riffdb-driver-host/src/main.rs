//! RiffDB application-driver host process.

#![forbid(unsafe_code)]

use std::path::Path;

use riffdb_driver_host::DriverRuntime;

#[tokio::main]
async fn main() {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let Some(config) = arguments.next() else {
        eprintln!("riffdb-driverd: expected one protected configuration path");
        std::process::exit(2);
    };
    if arguments.next().is_some() {
        eprintln!("riffdb-driverd: expected one protected configuration path");
        std::process::exit(2);
    }
    let runtime = match DriverRuntime::from_config_file(Path::new(&config)).await {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("riffdb-driverd: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = runtime.serve_until(shutdown_signal()).await {
        eprintln!("riffdb-driverd: {error}");
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
