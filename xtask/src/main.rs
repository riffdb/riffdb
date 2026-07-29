#![forbid(unsafe_code)]

use std::env;
use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be directly beneath the repository root")
        .to_owned();
    let mut arguments = env::args_os().skip(1);
    let Some(command) = arguments.next() else {
        print_help();
        return ExitCode::from(2);
    };

    if command == "-h" || command == "--help" {
        print_help();
        return ExitCode::SUCCESS;
    }

    let (program, forwarded): (PathBuf, Vec<OsString>) = if command == "install" {
        (
            repository.join("scripts/release-source-install"),
            std::iter::once(command).chain(arguments).collect(),
        )
    } else if command == "bootstrap" {
        (
            repository.join("scripts/release-source-bootstrap"),
            arguments.collect(),
        )
    } else {
        eprintln!(
            "unknown cargo riffdb command: {}",
            command.to_string_lossy()
        );
        print_help();
        return ExitCode::from(2);
    };

    let error = Command::new(program)
        .args(forwarded)
        .current_dir(repository)
        .exec();
    eprintln!("failed to execute the RiffDB source command: {error}");
    ExitCode::FAILURE
}

fn print_help() {
    eprintln!(
        "\
RiffDB source installation

Usage:
  cargo riffdb install (--user|--system) [--from-binaries DIR] [--no-start]
  cargo riffdb bootstrap (--user|--system) [--register-codex]

Commands:
  install    Build and install the three binaries, configuration, and systemd unit
  bootstrap  Create local operator authority and a restricted MCP capability
"
    );
}
