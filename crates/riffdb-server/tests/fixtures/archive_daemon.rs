#![forbid(unsafe_code)]
//! Real daemon with a one-shot observation after archive durability confirmation.
fn main() -> std::process::ExitCode {
    if !riffdb_server::test_fixtures::install_archive_progress_probe("daily", 3, || {
        println!("riffdb-archive-collected-v1\tcommit=3");
    }) {
        return std::process::ExitCode::FAILURE;
    }
    riffdb_server::riffdbd_main()
}
