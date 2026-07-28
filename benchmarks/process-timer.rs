#![forbid(unsafe_code)]

use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(2);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("benchmark process timer failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = env::args_os().skip(1);
    let result_path = arguments.next().map(PathBuf::from).ok_or_else(usage)?;
    let size_root = arguments.next().map(PathBuf::from).ok_or_else(usage)?;
    let size_file_name = arguments.next().ok_or_else(usage)?;
    let size_file_name = (size_file_name != "-").then_some(size_file_name);
    let program = arguments.next().ok_or_else(usage)?;
    let program_arguments = arguments.collect::<Vec<_>>();

    if !result_path.is_absolute() || !size_root.is_absolute() {
        return Err("result and size-root paths must be absolute".to_owned());
    }
    if result_path.exists() {
        return Err(format!(
            "result path already exists: {}",
            result_path.display()
        ));
    }
    let size_root_metadata = fs::symlink_metadata(&size_root)
        .map_err(|error| format!("inspect size root {}: {error}", size_root.display()))?;
    if !size_root_metadata.is_dir() || size_root_metadata.file_type().is_symlink() {
        return Err(format!(
            "size root must be a non-symlink directory: {}",
            size_root.display()
        ));
    }

    let started = Instant::now();
    let mut child = Command::new(program)
        .args(program_arguments)
        .spawn()
        .map_err(|error| format!("spawn measured process: {error}"))?;
    let mut peak_size_bytes = directory_size(&size_root, size_file_name.as_ref())?;
    let status = loop {
        peak_size_bytes = peak_size_bytes.max(directory_size(&size_root, size_file_name.as_ref())?);
        match child
            .try_wait()
            .map_err(|error| format!("wait for measured process: {error}"))?
        {
            Some(status) => break status,
            None => thread::sleep(POLL_INTERVAL),
        }
    };
    let elapsed_ns = u64::try_from(started.elapsed().as_nanos())
        .map_err(|_| "measured process duration exceeded u64 nanoseconds".to_owned())?;
    peak_size_bytes = peak_size_bytes.max(directory_size(&size_root, size_file_name.as_ref())?);
    if !status.success() {
        return Err(format!("measured process exited with {status}"));
    }
    if elapsed_ns == 0 {
        return Err("measured process duration was zero".to_owned());
    }

    fs::write(&result_path, format!("{elapsed_ns}\t{peak_size_bytes}\n"))
        .map_err(|error| format!("write result {}: {error}", result_path.display()))
}

fn directory_size(root: &Path, file_name: Option<&OsString>) -> Result<u64, String> {
    let mut pending = vec![root.to_path_buf()];
    let mut total = 0_u64;
    while let Some(path) = pending.pop() {
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && path != root => continue,
            Err(error) => {
                return Err(format!("inspect measured path {}: {error}", path.display()));
            }
        };
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "measured database root contains a symbolic link: {}",
                path.display()
            ));
        }
        if metadata.is_dir() {
            let entries = match fs::read_dir(&path) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound && path != root => {
                    continue;
                }
                Err(error) => {
                    return Err(format!(
                        "read measured directory {}: {error}",
                        path.display()
                    ));
                }
            };
            for entry in entries {
                match entry {
                    Ok(entry) => pending.push(entry.path()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(format!(
                            "read measured directory entry {}: {error}",
                            path.display()
                        ));
                    }
                }
            }
        } else if metadata.is_file()
            && file_name.is_none_or(|expected| path.file_name() == Some(expected.as_os_str()))
        {
            total = total
                .checked_add(metadata.len())
                .ok_or_else(|| "measured database size overflowed u64".to_owned())?;
        }
    }
    Ok(total)
}

fn usage() -> String {
    "usage: process-timer ABSOLUTE_RESULT_PATH ABSOLUTE_SIZE_ROOT SIZE_FILE_NAME_OR_DASH PROGRAM [ARG ...]"
        .to_owned()
}
