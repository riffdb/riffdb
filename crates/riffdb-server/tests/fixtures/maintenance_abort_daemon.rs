#![forbid(unsafe_code)]
//! Closed process-abort points for real daemon maintenance recovery tests.
fn main() -> std::process::ExitCode {
    use riffdb_server::test_fixtures::MaintenanceRecoveryTestPoint as Point;
    let point = match std::env::var("RIFFDB_TEST_MAINTENANCE_POINT").as_deref() {
        Ok("drain") => Point::DrainComplete,
        Ok("closed") => Point::DatabaseClosed,
        Ok("validated") => Point::FreshValidationComplete,
        _ => return std::process::ExitCode::FAILURE,
    };
    riffdb_server::test_fixtures::riffdbd_main_with_maintenance_abort(point)
}
