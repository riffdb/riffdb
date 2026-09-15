#![forbid(unsafe_code)]
//! Production daemon composition plus one redacted, one-shot waiter observation.

fn main() -> std::process::ExitCode {
    if !riffdb_service::install_columnar_wait_probe("document_board", || {
        println!("riffdb-columnar-wait-registered-v1");
    }) {
        return std::process::ExitCode::FAILURE;
    }
    riffdb_server::riffdbd_main()
}
