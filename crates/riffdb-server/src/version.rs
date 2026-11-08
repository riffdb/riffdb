//! Standalone offline build identity before configuration or runtime installation.
use std::ffi::OsStr;
use std::io::Write as _;
use std::process::ExitCode;
fn is_version(value: &OsStr) -> bool {
    value == "--version" || value == "-V"
}
/// Handles only standalone version invocations and rejects every mixed version invocation.
#[doc(hidden)]
pub(crate) fn early_version_from_env() -> Option<ExitCode> {
    let mut arguments = std::env::args_os().skip(1);
    let first = arguments.next()?;
    if is_version(&first) {
        if arguments.next().is_none() {
            const LINE: &str = concat!("riffdbd ", env!("CARGO_PKG_VERSION"), "\n");
            return Some(
                if LINE.len() <= 128 && std::io::stdout().lock().write_all(LINE.as_bytes()).is_ok()
                {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::from(2)
                },
            );
        }
    } else if !arguments.any(|value| is_version(&value)) {
        return None;
    }
    let _ = std::io::stderr()
        .lock()
        .write_all(b"riffdbd: invalid_version_arguments: --version/-V must be used alone\n");
    Some(ExitCode::from(2))
}
