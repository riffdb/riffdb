// req: TEST-001, TEST-003, TXN-001, TXN-002, CMP-001, QRY-001, QRY-004, AAA-003, AAA-004, DRV-005, DX-003
#![cfg(any(target_os = "linux", target_os = "macos"))]
#![forbid(unsafe_code)]

//! Mandatory public-guarantee end-to-end integration suite (ADR-0350 Decision 3).
//!
//! Exercises real `riffdbd` listeners, `redb` (`Standard`/`Group` and
//! `Hardened`/`Sync` commit profiles), `riffdb-driverd` Unix sockets, hosted
//! MCP Streamable HTTP, `riffdb` CLI JSON, and `StableApplicationClient`
//! verified-TLS gRPC across five composed guarantee boundaries.

mod idempotency_group_epochs;
mod invariant_rejection_boundary;
mod query_role_auth_agreement;
mod support;
mod uncertainty_and_restart_recovery;
mod wide_cursor_pagination;

use std::process::ExitCode;
use std::time::Instant;

use support::TestResult;

fn main() -> ExitCode {
    if std::env::var_os(support::SERVER_CHILD_ENV).is_some() {
        return riffdb_server::riffdbd_main();
    }
    if std::env::var_os(support::CLI_CHILD_ENV).is_some() {
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(_) => return ExitCode::FAILURE,
        };
        return runtime.block_on(riffdb_cli::run());
    }

    let filter = std::env::var("RIFFDB_PG_FILTER").ok();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("public_guarantees runtime initialization failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run_all(filter.as_deref())) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("public_guarantees failed: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_all(filter: Option<&str>) -> TestResult<()> {
    let mut executed = 0_usize;

    if should_run(filter, "idempotency_group_epochs") {
        let started = Instant::now();
        idempotency_group_epochs::run().await?;
        eprintln!(
            "[public_guarantees] idempotency_group_epochs passed ({:.2?})",
            started.elapsed()
        );
        executed += 1;
    }

    if should_run(filter, "invariant_rejection_boundary") {
        let started = Instant::now();
        invariant_rejection_boundary::run().await?;
        eprintln!(
            "[public_guarantees] invariant_rejection_boundary passed ({:.2?})",
            started.elapsed()
        );
        executed += 1;
    }

    if should_run(filter, "query_role_auth_agreement") {
        let started = Instant::now();
        query_role_auth_agreement::run().await?;
        eprintln!(
            "[public_guarantees] query_role_auth_agreement passed ({:.2?})",
            started.elapsed()
        );
        executed += 1;
    }

    if should_run(filter, "wide_cursor_pagination") {
        let started = Instant::now();
        wide_cursor_pagination::run().await?;
        eprintln!(
            "[public_guarantees] wide_cursor_pagination passed ({:.2?})",
            started.elapsed()
        );
        executed += 1;
    }

    if should_run(filter, "uncertainty_and_restart_recovery") {
        let started = Instant::now();
        uncertainty_and_restart_recovery::run().await?;
        eprintln!(
            "[public_guarantees] uncertainty_and_restart_recovery passed ({:.2?})",
            started.elapsed()
        );
        executed += 1;
    }

    if executed == 0 {
        return Err(format!("RIFFDB_PG_FILTER={filter:?} matched zero modules").into());
    }
    Ok(())
}

fn should_run(filter: Option<&str>, name: &str) -> bool {
    filter.is_none_or(|pattern| name.contains(pattern))
}
