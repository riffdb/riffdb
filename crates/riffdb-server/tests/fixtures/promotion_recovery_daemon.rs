#![forbid(unsafe_code)]
//! Exact production composition with bounded observation/crash hooks for tests.

fn main() -> std::process::ExitCode {
    if !riffdb_server::test_fixtures::install_promotion_recovery_probe(|point| {
        if point == "retry-ready" {
            println!("riffdb-promotion-retry-fixture-v1");
        }
        if std::env::var("RIFFDB_PROMOTION_ABORT").ok().as_deref() == Some(point) {
            std::process::abort();
        }
        if std::env::var("RIFFDB_PROMOTION_STOP").ok().as_deref() == Some(point) {
            assert!(
                std::process::Command::new("kill")
                    .arg("-TERM")
                    .arg(std::process::id().to_string())
                    .status()
                    .expect("signal fixture process")
                    .success()
            );
        }
    }) {
        return std::process::ExitCode::FAILURE;
    }
    riffdb_server::riffdbd_main()
}
