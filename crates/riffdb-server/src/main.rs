#![forbid(unsafe_code)]

//! Hosted `riffdbd` server process.

fn main() -> std::process::ExitCode {
    riffdb_server::riffdbd_main()
}
