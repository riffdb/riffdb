#![forbid(unsafe_code)]
//! Real daemon with fixed one-shot observations after archive confirmation or sink failure.
fn main() -> std::process::ExitCode {
    if !riffdb_server::test_fixtures::install_archive_progress_probe(
        "daily",
        3,
        || println!("riffdb-archive-collected-v1\tcommit=3"),
        || println!("riffdb-archive-failed-v1\tclass=sink-unavailable"),
    ) {
        return std::process::ExitCode::FAILURE;
    }
    riffdb_server::riffdbd_main()
}
