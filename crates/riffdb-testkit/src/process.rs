//! Bounded child-process lifecycle used by recovery tests.

use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Maximum process arguments accepted by the test controller.
pub const MAX_CHILD_ARGUMENTS: usize = 64;
/// Maximum environment entries accepted by the test controller.
pub const MAX_CHILD_ENVIRONMENT_ENTRIES: usize = 32;
/// Maximum bytes in one argument, environment component, or readiness line.
pub const MAX_CHILD_COMPONENT_BYTES: usize = 4_096;
/// Maximum readiness lines buffered while the parent is inspecting state.
pub const MAX_BUFFERED_READINESS_LINES: usize = 16;

const REAPER_POLL: Duration = Duration::from_millis(10);
const DROP_KILL_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounded immutable process launch specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildProcessSpec {
    executable: PathBuf,
    arguments: Vec<OsString>,
    environment: Vec<(OsString, OsString)>,
}

impl ChildProcessSpec {
    /// Starts one checked process specification.
    pub fn new(executable: impl Into<PathBuf>) -> Result<Self, ChildProcessSpecError> {
        let executable = executable.into();
        validate_component(executable.as_os_str())?;
        Ok(Self {
            executable,
            arguments: Vec::new(),
            environment: Vec::new(),
        })
    }

    /// Appends one bounded argument.
    pub fn arg(mut self, argument: impl Into<OsString>) -> Result<Self, ChildProcessSpecError> {
        if self.arguments.len() == MAX_CHILD_ARGUMENTS {
            return Err(ChildProcessSpecError::TooManyArguments);
        }
        let argument = argument.into();
        validate_component(&argument)?;
        self.arguments.push(argument);
        Ok(self)
    }

    /// Appends one bounded environment entry with a unique key.
    pub fn env(
        mut self,
        key: impl Into<OsString>,
        value: impl Into<OsString>,
    ) -> Result<Self, ChildProcessSpecError> {
        if self.environment.len() == MAX_CHILD_ENVIRONMENT_ENTRIES {
            return Err(ChildProcessSpecError::TooManyEnvironmentEntries);
        }
        let key = key.into();
        let value = value.into();
        validate_component(&key)?;
        validate_component(&value)?;
        if key.as_encoded_bytes().contains(&b'=')
            || self
                .environment
                .iter()
                .any(|(existing, _)| existing == &key)
        {
            return Err(ChildProcessSpecError::InvalidEnvironment);
        }
        self.environment.push((key, value));
        Ok(self)
    }

    /// Returns the executable selected by the test.
    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    /// Borrows the exact bounded argument vector.
    #[must_use]
    pub fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    /// Borrows the exact bounded environment vector.
    #[must_use]
    pub fn environment(&self) -> &[(OsString, OsString)] {
        &self.environment
    }
}

fn validate_component(value: &OsStr) -> Result<(), ChildProcessSpecError> {
    if value.is_empty() || value.as_encoded_bytes().len() > MAX_CHILD_COMPONENT_BYTES {
        Err(ChildProcessSpecError::InvalidComponent)
    } else {
        Ok(())
    }
}

/// Closed launch-specification validation error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildProcessSpecError {
    /// One path, argument, key, or value was empty or oversized.
    InvalidComponent,
    /// The argument count exceeded its fixed test bound.
    TooManyArguments,
    /// The environment count exceeded its fixed test bound.
    TooManyEnvironmentEntries,
    /// An environment key was duplicated or contained `=`.
    InvalidEnvironment,
}

impl fmt::Display for ChildProcessSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidComponent => "child process component is invalid",
            Self::TooManyArguments => "child process argument bound exceeded",
            Self::TooManyEnvironmentEntries => "child process environment bound exceeded",
            Self::InvalidEnvironment => "child process environment is invalid",
        })
    }
}

impl Error for ChildProcessSpecError {}

/// Drained output counts recorded when a child exits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildOutputCounts {
    /// Total stdout bytes consumed by the bounded readiness reader.
    pub stdout_bytes: usize,
    /// Total stderr bytes drained without interpretation.
    pub stderr_bytes: usize,
}

/// One exited child and its drained-output evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildExit {
    /// Operating-system exit status.
    pub status: ExitStatus,
    /// Bounded readers' total consumed byte counts.
    pub output: ChildOutputCounts,
}

enum ReaperCommand {
    Kill,
}

/// Owning controller for one recovery child.
///
/// The controller never interprets stderr and never exposes a raw `Child`.
/// Drop performs a bounded best-effort kill so failed tests do not retain
/// database locks.
pub struct ChildProcessController {
    stdin: Option<ChildStdin>,
    readiness: Receiver<io::Result<String>>,
    reaper_commands: SyncSender<ReaperCommand>,
    exited: Receiver<io::Result<ExitStatus>>,
    reaper: Option<JoinHandle<()>>,
    stdout: Option<JoinHandle<usize>>,
    stderr: Option<JoinHandle<usize>>,
    exit_observed: bool,
}

impl ChildProcessController {
    /// Spawns one checked process with piped lifecycle channels.
    pub fn spawn(specification: &ChildProcessSpec) -> Result<Self, ChildProcessError> {
        let mut command = Command::new(specification.executable());
        command
            .args(specification.arguments())
            .envs(
                specification
                    .environment()
                    .iter()
                    .map(|(key, value)| (key, value)),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(ChildProcessError::Io)?;
        let stdin = child.stdin.take().ok_or(ChildProcessError::MissingPipe)?;
        let stdout = child.stdout.take().ok_or(ChildProcessError::MissingPipe)?;
        let stderr = child.stderr.take().ok_or(ChildProcessError::MissingPipe)?;

        let (readiness_sender, readiness) = mpsc::sync_channel(MAX_BUFFERED_READINESS_LINES);
        let stdout = thread::spawn(move || read_bounded_lines(stdout, readiness_sender));
        let stderr = thread::spawn(move || drain_stream(stderr));
        let (reaper_commands, commands) = mpsc::sync_channel(1);
        let (exit_sender, exited) = mpsc::sync_channel(1);
        let reaper = thread::spawn(move || reap_child(child, commands, exit_sender));

        Ok(Self {
            stdin: Some(stdin),
            readiness,
            reaper_commands,
            exited,
            reaper: Some(reaper),
            stdout: Some(stdout),
            stderr: Some(stderr),
            exit_observed: false,
        })
    }

    /// Waits for the next complete line and requires the supplied prefix.
    pub fn wait_for_readiness(
        &self,
        prefix: &str,
        deadline: Duration,
    ) -> Result<String, ChildProcessError> {
        if prefix.is_empty() || prefix.len() > MAX_CHILD_COMPONENT_BYTES {
            return Err(ChildProcessError::InvalidReadinessPrefix);
        }
        match self.readiness.recv_timeout(deadline) {
            Ok(Ok(line)) if line.starts_with(prefix) => Ok(line),
            Ok(Ok(_)) => Err(ChildProcessError::UnexpectedReadiness),
            Ok(Err(error)) => Err(ChildProcessError::Io(error)),
            Err(RecvTimeoutError::Timeout) => Err(ChildProcessError::ReadinessTimeout),
            Err(RecvTimeoutError::Disconnected) => Err(ChildProcessError::ReadinessDisconnected),
        }
    }

    /// Sends one exact shutdown command and requires a successful bounded exit.
    pub fn shutdown_cleanly(
        &mut self,
        shutdown_command: &[u8],
        deadline: Duration,
    ) -> Result<ChildExit, ChildProcessError> {
        if shutdown_command.is_empty() || shutdown_command.len() > MAX_CHILD_COMPONENT_BYTES {
            return Err(ChildProcessError::InvalidShutdownCommand);
        }
        let mut stdin = self.stdin.take().ok_or(ChildProcessError::StdinClosed)?;
        stdin
            .write_all(shutdown_command)
            .map_err(ChildProcessError::Io)?;
        stdin.flush().map_err(ChildProcessError::Io)?;
        drop(stdin);
        let exit = self.wait_for_exit(deadline)?;
        if !exit.status.success() {
            return Err(ChildProcessError::UnsuccessfulExit(exit));
        }
        Ok(exit)
    }

    /// Abruptly terminates the child and returns its observed exit.
    pub fn kill(&mut self, deadline: Duration) -> Result<ChildExit, ChildProcessError> {
        self.stdin.take();
        self.reaper_commands
            .send(ReaperCommand::Kill)
            .map_err(|_| ChildProcessError::ReaperDisconnected)?;
        self.wait_for_exit(deadline)
    }

    /// Waits for a natural exit without sending lifecycle input.
    pub fn wait_for_exit(&mut self, deadline: Duration) -> Result<ChildExit, ChildProcessError> {
        if self.exit_observed {
            return Err(ChildProcessError::ExitAlreadyObserved);
        }
        let status = match self.exited.recv_timeout(deadline) {
            Ok(result) => result.map_err(ChildProcessError::Io)?,
            Err(RecvTimeoutError::Timeout) => return Err(ChildProcessError::ExitTimeout),
            Err(RecvTimeoutError::Disconnected) => {
                return Err(ChildProcessError::ReaperDisconnected);
            }
        };
        self.exit_observed = true;
        let output = self.join_threads()?;
        Ok(ChildExit { status, output })
    }

    fn join_threads(&mut self) -> Result<ChildOutputCounts, ChildProcessError> {
        if let Some(reaper) = self.reaper.take() {
            reaper
                .join()
                .map_err(|_| ChildProcessError::ControllerPanicked)?;
        }
        let stdout_bytes = self
            .stdout
            .take()
            .ok_or(ChildProcessError::ControllerAlreadyJoined)?
            .join()
            .map_err(|_| ChildProcessError::ControllerPanicked)?;
        let stderr_bytes = self
            .stderr
            .take()
            .ok_or(ChildProcessError::ControllerAlreadyJoined)?
            .join()
            .map_err(|_| ChildProcessError::ControllerPanicked)?;
        Ok(ChildOutputCounts {
            stdout_bytes,
            stderr_bytes,
        })
    }
}

impl fmt::Debug for ChildProcessController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildProcessController")
            .field("stdin_open", &self.stdin.is_some())
            .field("exit_observed", &self.exit_observed)
            .finish_non_exhaustive()
    }
}

impl Drop for ChildProcessController {
    fn drop(&mut self) {
        self.stdin.take();
        if !self.exit_observed {
            let _ = self.reaper_commands.send(ReaperCommand::Kill);
            if self.exited.recv_timeout(DROP_KILL_TIMEOUT).is_ok() {
                self.exit_observed = true;
                let _ = self.join_threads();
            }
        }
    }
}

fn reap_child(
    mut child: Child,
    commands: Receiver<ReaperCommand>,
    exited: SyncSender<io::Result<ExitStatus>>,
) {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let _ = exited.send(Ok(status));
                return;
            }
            Ok(None) => {}
            Err(error) => {
                let _ = exited.send(Err(error));
                return;
            }
        }
        match commands.recv_timeout(REAPER_POLL) {
            Ok(ReaperCommand::Kill) | Err(RecvTimeoutError::Disconnected) => {
                let _ = child.kill();
                let _ = exited.send(child.wait());
                return;
            }
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

fn read_bounded_lines(stdout: ChildStdout, sender: SyncSender<io::Result<String>>) -> usize {
    let mut reader = BufReader::new(stdout);
    let mut total = 0usize;
    loop {
        match read_bounded_line(&mut reader, MAX_CHILD_COMPONENT_BYTES) {
            Ok(Some((line, bytes))) => {
                total = total.saturating_add(bytes);
                if sender.send(Ok(line)).is_err() {
                    return total.saturating_add(drain_reader(&mut reader));
                }
            }
            Ok(None) => return total,
            Err(error) => {
                let _ = sender.send(Err(error));
                return total.saturating_add(drain_reader(&mut reader));
            }
        }
    }
}

fn read_bounded_line(
    reader: &mut impl Read,
    maximum: usize,
) -> io::Result<Option<(String, usize)>> {
    let mut bytes = Vec::with_capacity(maximum.min(128));
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte)? {
            0 if bytes.is_empty() => return Ok(None),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "child readiness line ended without newline",
                ));
            }
            1 => {
                if byte[0] == b'\n' {
                    let consumed = bytes.len().saturating_add(1);
                    let line = String::from_utf8(bytes).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "child readiness line was not UTF-8",
                        )
                    })?;
                    return Ok(Some((line, consumed)));
                }
                if bytes.len() == maximum {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "child readiness line exceeded its bound",
                    ));
                }
                bytes.push(byte[0]);
            }
            _ => unreachable!("one-byte read returned more than one byte"),
        }
    }
}

fn drain_stream(stderr: ChildStderr) -> usize {
    drain_reader(&mut BufReader::new(stderr))
}

fn drain_reader(reader: &mut impl Read) -> usize {
    let mut total = 0usize;
    let mut buffer = [0_u8; 4_096];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => return total,
            Ok(read) => total = total.saturating_add(read),
        }
    }
}

/// Closed process-controller failure.
#[derive(Debug)]
pub enum ChildProcessError {
    /// Operating-system process or pipe failure.
    Io(io::Error),
    /// A configured process pipe was unexpectedly absent.
    MissingPipe,
    /// The readiness prefix was empty or oversized.
    InvalidReadinessPrefix,
    /// A complete readiness line did not carry the expected prefix.
    UnexpectedReadiness,
    /// No readiness line arrived before the explicit deadline.
    ReadinessTimeout,
    /// The readiness reader ended before yielding another line.
    ReadinessDisconnected,
    /// The shutdown command was empty or oversized.
    InvalidShutdownCommand,
    /// The process stdin had already been consumed.
    StdinClosed,
    /// The process did not exit before the explicit deadline.
    ExitTimeout,
    /// The process reaper ended unexpectedly.
    ReaperDisconnected,
    /// Exit was already consumed.
    ExitAlreadyObserved,
    /// A controller thread was joined twice.
    ControllerAlreadyJoined,
    /// A controller thread panicked.
    ControllerPanicked,
    /// A clean shutdown produced a failure status.
    UnsuccessfulExit(ChildExit),
}

impl fmt::Display for ChildProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Io(_) => "child process I/O failed",
            Self::MissingPipe => "child process pipe was missing",
            Self::InvalidReadinessPrefix => "child readiness prefix is invalid",
            Self::UnexpectedReadiness => "child emitted an unexpected readiness line",
            Self::ReadinessTimeout => "child readiness timed out",
            Self::ReadinessDisconnected => "child readiness reader disconnected",
            Self::InvalidShutdownCommand => "child shutdown command is invalid",
            Self::StdinClosed => "child stdin is closed",
            Self::ExitTimeout => "child exit timed out",
            Self::ReaperDisconnected => "child process reaper disconnected",
            Self::ExitAlreadyObserved => "child exit was already observed",
            Self::ControllerAlreadyJoined => "child controller was already joined",
            Self::ControllerPanicked => "child controller thread panicked",
            Self::UnsuccessfulExit(_) => "child clean shutdown returned failure",
        })
    }
}

impl Error for ChildProcessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_spec_is_bounded_and_environment_keys_are_unique() {
        let spec = ChildProcessSpec::new("/test/riffdbd")
            .expect("executable")
            .arg("--database")
            .expect("argument")
            .arg("/tmp/riffdb.redb")
            .expect("argument")
            .env("RIFFDB_MODE", "recovery")
            .expect("environment");
        assert_eq!(spec.arguments().len(), 2);
        assert_eq!(spec.environment().len(), 1);
        assert_eq!(
            spec.clone().env("RIFFDB_MODE", "other"),
            Err(ChildProcessSpecError::InvalidEnvironment)
        );
    }

    #[test]
    fn bounded_line_requires_utf8_newline_and_limit() {
        let mut complete = &b"riffdbd-ready-v1\t127.0.0.1:1\n"[..];
        assert_eq!(
            read_bounded_line(&mut complete, 64).expect("line"),
            Some(("riffdbd-ready-v1\t127.0.0.1:1".to_owned(), 29))
        );

        let mut incomplete = &b"no-newline"[..];
        assert_eq!(
            read_bounded_line(&mut incomplete, 64)
                .expect_err("newline is required")
                .kind(),
            io::ErrorKind::UnexpectedEof
        );

        let mut oversized = &b"abcd\n"[..];
        assert_eq!(
            read_bounded_line(&mut oversized, 3)
                .expect_err("bound is enforced")
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}
