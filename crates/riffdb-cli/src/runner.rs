use std::ffi::{OsStr, OsString};
use std::io::{self, Read};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use crate::cli::BudgetCase;
use crate::input::validate_path;

const RUNNER_PROTOCOL: &str = "riffdb.budget.public-run/v1";
const RUNNER_ADAPTER: &str = "riffdb-public-grpc-v1";
const MAX_RUNNER_STREAM_BYTES: usize = 4_096;
const RUNNER_DEADLINE: Duration = Duration::from_secs(180);
const RUNNER_REAP_DEADLINE: Duration = Duration::from_secs(5);
const CHECKED_FAILURE: &[u8] = b"riffdb budget public run failed\n";
const INVALID_INVOCATION: &[u8] = b"riffdb budget public invocation invalid\n";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunnerError {
    StartFailed,
    Timeout,
    OutputTooLarge(RunnerStream),
    InvocationInvalid,
    ProtocolInvalid,
    CheckedFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunnerStream {
    Stdout,
    Stderr,
}

pub(crate) fn run_budget(
    runner: &OsStr,
    case: BudgetCase,
    endpoint: &str,
    credential_file: &Path,
) -> Result<(), RunnerError> {
    let mut deadline = SystemDeadline::new(RUNNER_DEADLINE);
    run_budget_with(
        &SystemLauncher::default(),
        &mut deadline,
        runner,
        case,
        endpoint,
        credential_file,
    )
}

fn run_budget_with(
    launcher: &dyn ProcessLauncher,
    deadline: &mut dyn ProcessDeadline,
    runner: &OsStr,
    case: BudgetCase,
    endpoint: &str,
    credential_file: &Path,
) -> Result<(), RunnerError> {
    validate_direct_runner(runner)?;
    validate_path(credential_file.as_os_str()).map_err(|_| RunnerError::StartFailed)?;

    let arguments = runner_arguments(case, endpoint, credential_file);
    let process = launcher
        .launch(runner, &arguments)
        .map_err(|()| RunnerError::StartFailed)?;
    let stdout = bounded_reader(process.stdout);
    let stderr = bounded_reader(process.stderr);
    monitor_child(process.control, stdout, stderr, case, deadline)
}

fn validate_direct_runner(runner: &OsStr) -> Result<(), RunnerError> {
    validate_path(runner).map_err(|_| RunnerError::StartFailed)?;
    let path = Path::new(runner);
    if !path.is_absolute() && path.components().count() < 2 {
        return Err(RunnerError::StartFailed);
    }
    Ok(())
}

fn runner_arguments(case: BudgetCase, endpoint: &str, credential_file: &Path) -> [OsString; 8] {
    [
        OsString::from("--protocol"),
        OsString::from(RUNNER_PROTOCOL),
        OsString::from("--case"),
        OsString::from(case.as_str()),
        OsString::from("--endpoint"),
        OsString::from(endpoint),
        OsString::from("--credential-file"),
        credential_file.as_os_str().to_owned(),
    ]
}

trait ProcessLauncher {
    fn launch(&self, runner: &OsStr, arguments: &[OsString]) -> Result<SpawnedProcess, ()>;
}

struct SpawnedProcess {
    control: Box<dyn ProcessControl>,
    stdout: Box<dyn Read + Send>,
    stderr: Box<dyn Read + Send>,
}

trait ProcessControl {
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>>;
    fn kill_and_reap(&mut self) -> Result<(), ()>;
}

trait ProcessDeadline {
    fn expired(&self) -> bool;
    fn wait(&mut self);
}

#[derive(Default)]
struct SystemLauncher {
    #[cfg(test)]
    observation: Option<std::sync::Arc<SystemProcessObservation>>,
}

#[cfg(test)]
impl SystemLauncher {
    fn observed(observation: std::sync::Arc<SystemProcessObservation>) -> Self {
        Self {
            observation: Some(observation),
        }
    }
}

#[cfg(test)]
#[derive(Default)]
struct SystemProcessObservation {
    kill_called: std::sync::atomic::AtomicBool,
    kill_succeeded: std::sync::atomic::AtomicBool,
    reap_succeeded: std::sync::atomic::AtomicBool,
}

impl ProcessLauncher for SystemLauncher {
    fn launch(&self, runner: &OsStr, arguments: &[OsString]) -> Result<SpawnedProcess, ()> {
        let mut child = Command::new(runner)
            .args(arguments)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| ())?;
        let Some(stdout) = child.stdout.take() else {
            kill_and_reap_child(child)
                .confirmed()
                .then_some(())
                .ok_or(())?;
            return Err(());
        };
        let Some(stderr) = child.stderr.take() else {
            kill_and_reap_child(child)
                .confirmed()
                .then_some(())
                .ok_or(())?;
            return Err(());
        };
        Ok(SpawnedProcess {
            control: Box::new(SystemProcess {
                child: Some(child),
                #[cfg(test)]
                observation: self.observation.clone(),
            }),
            stdout: Box::new(stdout),
            stderr: Box::new(stderr),
        })
    }
}

struct SystemProcess {
    child: Option<Child>,
    #[cfg(test)]
    observation: Option<std::sync::Arc<SystemProcessObservation>>,
}

impl ProcessControl for SystemProcess {
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child
            .as_mut()
            .ok_or_else(|| io::Error::other("child already handed to reaper"))?
            .try_wait()
    }

    fn kill_and_reap(&mut self) -> Result<(), ()> {
        let child = self.child.take().ok_or(())?;
        let outcome = kill_and_reap_child(child);
        #[cfg(test)]
        if let Some(observation) = &self.observation {
            observation
                .kill_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            observation.kill_succeeded.store(
                outcome.kill_succeeded(),
                std::sync::atomic::Ordering::SeqCst,
            );
            observation
                .reap_succeeded
                .store(outcome.confirmed(), std::sync::atomic::Ordering::SeqCst);
        }
        outcome.confirmed().then_some(()).ok_or(())
    }
}

struct SystemDeadline {
    deadline: Instant,
}

impl SystemDeadline {
    fn new(duration: Duration) -> Self {
        Self {
            deadline: Instant::now() + duration,
        }
    }
}

impl ProcessDeadline for SystemDeadline {
    fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    fn wait(&mut self) {
        thread::sleep(Duration::from_millis(10));
    }
}

enum StreamResult {
    Complete(Vec<u8>),
    TooLarge,
    Failed,
}

fn bounded_reader<R: Read + Send + 'static>(mut reader: R) -> Receiver<StreamResult> {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut bytes = Vec::with_capacity(MAX_RUNNER_STREAM_BYTES);
        let result = match reader
            .by_ref()
            .take((MAX_RUNNER_STREAM_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
        {
            Ok(_) if bytes.len() > MAX_RUNNER_STREAM_BYTES => StreamResult::TooLarge,
            Ok(_) => StreamResult::Complete(bytes),
            Err(_) => StreamResult::Failed,
        };
        let _ = sender.send(result);
    });
    receiver
}

fn monitor_child(
    mut child: Box<dyn ProcessControl>,
    stdout: Receiver<StreamResult>,
    stderr: Receiver<StreamResult>,
    case: BudgetCase,
    deadline: &mut dyn ProcessDeadline,
) -> Result<(), RunnerError> {
    let mut stdout_result = None;
    let mut stderr_result = None;
    let mut exit_status = None;
    loop {
        receive_stream(
            &stdout,
            &mut stdout_result,
            RunnerStream::Stdout,
            child.as_mut(),
        )?;
        receive_stream(
            &stderr,
            &mut stderr_result,
            RunnerStream::Stderr,
            child.as_mut(),
        )?;
        if exit_status.is_none() {
            match child.try_wait() {
                Ok(status) => exit_status = status,
                Err(_) => return fail_child(child.as_mut(), RunnerError::ProtocolInvalid),
            }
        }
        if let (Some(status), Some(stdout), Some(stderr)) = (
            exit_status,
            stdout_result.as_deref(),
            stderr_result.as_deref(),
        ) {
            return match check_protocol(status, stdout, stderr, case) {
                Ok(()) => Ok(()),
                Err(error) => fail_child(child.as_mut(), error),
            };
        }
        if deadline.expired() {
            return fail_child(child.as_mut(), RunnerError::Timeout);
        }
        deadline.wait();
    }
}

fn receive_stream(
    receiver: &Receiver<StreamResult>,
    slot: &mut Option<Vec<u8>>,
    stream: RunnerStream,
    child: &mut dyn ProcessControl,
) -> Result<(), RunnerError> {
    if slot.is_some() {
        return Ok(());
    }
    match receiver.try_recv() {
        Ok(StreamResult::Complete(bytes)) => *slot = Some(bytes),
        Ok(StreamResult::TooLarge) => {
            return fail_child(child, RunnerError::OutputTooLarge(stream));
        }
        Ok(StreamResult::Failed) => return fail_child(child, RunnerError::ProtocolInvalid),
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            return fail_child(child, RunnerError::ProtocolInvalid);
        }
    }
    Ok(())
}

fn fail_child<T>(child: &mut dyn ProcessControl, error: RunnerError) -> Result<T, RunnerError> {
    match child.kill_and_reap() {
        Ok(()) => Err(error),
        Err(()) => Err(RunnerError::ProtocolInvalid),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminationOutcome {
    KilledAndReaped,
    ExitedAndReaped,
    Unconfirmed,
}

impl TerminationOutcome {
    const fn confirmed(self) -> bool {
        self.kill_succeeded() || matches!(self, Self::ExitedAndReaped)
    }

    const fn kill_succeeded(self) -> bool {
        matches!(self, Self::KilledAndReaped)
    }
}

fn kill_and_reap_child(mut child: Child) -> TerminationOutcome {
    let kill_succeeded = child.kill().is_ok();
    let (sender, receiver) = mpsc::sync_channel(1);
    // The wait owner remains alive to reap eventually if foreground confirmation times out.
    thread::spawn(move || {
        let result = child.wait().map(|_| ()).map_err(|_| ());
        let _ = sender.send(result);
    });
    match (kill_succeeded, receiver.recv_timeout(RUNNER_REAP_DEADLINE)) {
        (true, Ok(Ok(()))) => TerminationOutcome::KilledAndReaped,
        (false, Ok(Ok(()))) => TerminationOutcome::ExitedAndReaped,
        (_, Ok(Err(())) | Err(_)) => TerminationOutcome::Unconfirmed,
    }
}

fn check_protocol(
    status: ExitStatus,
    stdout: &[u8],
    stderr: &[u8],
    case: BudgetCase,
) -> Result<(), RunnerError> {
    match status.code() {
        Some(0) if stderr.is_empty() && stdout == expected_success(case).as_bytes() => Ok(()),
        Some(1) if stdout.is_empty() && stderr == CHECKED_FAILURE => {
            Err(RunnerError::CheckedFailure)
        }
        Some(2) if stdout.is_empty() && stderr == INVALID_INVOCATION => {
            Err(RunnerError::InvocationInvalid)
        }
        _ => Err(RunnerError::ProtocolInvalid),
    }
}

fn expected_success(case: BudgetCase) -> String {
    format!(
        "{{\"schema\":\"{RUNNER_PROTOCOL}\",\"adapter\":\"{RUNNER_ADAPTER}\",\"case\":\"{}\",\"workload_version\":1,\"status\":\"passed\"}}\n",
        case.as_str()
    )
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::fs;
    use std::io::Cursor;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::ExitStatusExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    static SYSTEM_CHILD_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn exact_success_and_failure_protocol_is_closed() {
        let success = expected_success(BudgetCase::Sequential);
        assert_eq!(
            check_protocol(
                ExitStatus::from_raw(0),
                success.as_bytes(),
                b"",
                BudgetCase::Sequential,
            ),
            Ok(())
        );
        assert_eq!(
            check_protocol(
                ExitStatus::from_raw(1 << 8),
                b"",
                CHECKED_FAILURE,
                BudgetCase::Sequential,
            ),
            Err(RunnerError::CheckedFailure)
        );
        assert_eq!(
            check_protocol(
                ExitStatus::from_raw(0),
                success.as_bytes(),
                b"untrusted text",
                BudgetCase::Sequential,
            ),
            Err(RunnerError::ProtocolInvalid)
        );
    }

    #[test]
    fn case_mismatch_is_not_accepted_as_success() {
        let wrong = expected_success(BudgetCase::Contention);
        assert_eq!(
            check_protocol(
                ExitStatus::from_raw(0),
                wrong.as_bytes(),
                b"",
                BudgetCase::Sequential,
            ),
            Err(RunnerError::ProtocolInvalid)
        );
    }

    #[test]
    fn invocation_is_exact_and_system_launch_is_direct_and_sanitized() {
        let _system_child = SYSTEM_CHILD_TEST_LOCK
            .lock()
            .expect("system child test lock");
        let directory = temporary_directory();
        let runner = directory.path().join("riffdb runner; false");
        let credential = directory.path().join("credential");
        let expected = expected_success(BudgetCase::Sequential);
        let script = format!(
            concat!(
                "#!/bin/sh\n",
                "[ \"$#\" -eq 8 ] || exit 2\n",
                "[ \"$1\" = \"--protocol\" ] || exit 2\n",
                "[ \"$2\" = \"riffdb.budget.public-run/v1\" ] || exit 2\n",
                "[ \"$3\" = \"--case\" ] || exit 2\n",
                "[ \"$4\" = \"sequential\" ] || exit 2\n",
                "[ \"$5\" = \"--endpoint\" ] || exit 2\n",
                "[ \"$6\" = \"http://127.0.0.1:7443\" ] || exit 2\n",
                "[ \"$7\" = \"--credential-file\" ] || exit 2\n",
                "[ \"$8\" = \"{}\" ] || exit 2\n",
                "[ -z \"${{HOME+x}}\" ] || exit 2\n",
                "if IFS= read -r ignored; then exit 2; fi\n",
                "printf '%s' '{}'\n"
            ),
            credential.display(),
            expected.replace('\'', "'\\''"),
        );
        fs::write(&runner, script).expect("script");
        let mut permissions = fs::metadata(&runner).expect("metadata").permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&runner, permissions).expect("mode");
        let mut deadline = YieldDeadline::never();
        let result = run_budget_with(
            &SystemLauncher::default(),
            &mut deadline,
            runner.as_os_str(),
            BudgetCase::Sequential,
            "http://127.0.0.1:7443",
            &credential,
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn real_children_cover_every_closed_failure_exit_and_stream_boundary() {
        let _system_child = SYSTEM_CHILD_TEST_LOCK
            .lock()
            .expect("system child test lock");
        let directory = temporary_directory();
        let credential = directory.path().join("credential");
        let cases = [
            (
                "checked-failure",
                "#!/bin/sh\nprintf '%s\\n' 'riffdb budget public run failed' >&2\nexit 1\n"
                    .to_owned(),
                RunnerError::CheckedFailure,
            ),
            (
                "invalid-invocation",
                "#!/bin/sh\nprintf '%s\\n' 'riffdb budget public invocation invalid' >&2\nexit 2\n"
                    .to_owned(),
                RunnerError::InvocationInvalid,
            ),
            (
                "invalid-protocol",
                "#!/bin/sh\nprintf '%s\\n' 'unregistered success'\nexit 0\n".to_owned(),
                RunnerError::ProtocolInvalid,
            ),
            (
                "stdout-too-large",
                format!(
                    "#!/bin/sh\nprintf '%s' '{}'\n",
                    "x".repeat(MAX_RUNNER_STREAM_BYTES + 1)
                ),
                RunnerError::OutputTooLarge(RunnerStream::Stdout),
            ),
            (
                "stderr-too-large",
                format!(
                    "#!/bin/sh\nprintf '%s' '{}' >&2\n",
                    "x".repeat(MAX_RUNNER_STREAM_BYTES + 1)
                ),
                RunnerError::OutputTooLarge(RunnerStream::Stderr),
            ),
        ];

        for (name, script, expected) in cases {
            let runner = write_executable_runner(directory.path(), name, &script);
            let result = run_budget_with(
                &SystemLauncher::default(),
                &mut YieldDeadline::after(1_000_000_000),
                runner.as_os_str(),
                BudgetCase::Sequential,
                "http://127.0.0.1:7443",
                &credential,
            );
            assert_eq!(result, Err(expected), "{name}");
        }

        let runner = write_executable_runner(
            directory.path(),
            "deadline",
            "#!/bin/sh\nwhile :; do :; done\n",
        );
        let observation = Arc::new(SystemProcessObservation::default());
        let result = run_budget_with(
            &SystemLauncher::observed(Arc::clone(&observation)),
            &mut YieldDeadline::after(0),
            runner.as_os_str(),
            BudgetCase::Sequential,
            "http://127.0.0.1:7443",
            &credential,
        );
        assert_eq!(result, Err(RunnerError::Timeout));
        assert!(observation.kill_called.load(Ordering::SeqCst));
        assert!(observation.kill_succeeded.load(Ordering::SeqCst));
        assert!(observation.reap_succeeded.load(Ordering::SeqCst));
    }

    #[test]
    fn injected_process_covers_both_output_caps_and_exact_failure_exits() {
        let checked = FakeLauncher::exited(1, Vec::new(), CHECKED_FAILURE.to_vec());
        assert_eq!(
            run_fake(&checked, &mut YieldDeadline::never()),
            Err(RunnerError::CheckedFailure)
        );

        let invalid = FakeLauncher::exited(2, Vec::new(), INVALID_INVOCATION.to_vec());
        assert_eq!(
            run_fake(&invalid, &mut YieldDeadline::never()),
            Err(RunnerError::InvocationInvalid)
        );

        for (stdout, stderr, expected) in [
            (
                vec![b'x'; MAX_RUNNER_STREAM_BYTES + 1],
                Vec::new(),
                RunnerError::OutputTooLarge(RunnerStream::Stdout),
            ),
            (
                Vec::new(),
                vec![b'x'; MAX_RUNNER_STREAM_BYTES + 1],
                RunnerError::OutputTooLarge(RunnerStream::Stderr),
            ),
        ] {
            let launcher = FakeLauncher::running(stdout, stderr);
            assert_eq!(
                run_fake(&launcher, &mut YieldDeadline::after(10_000)),
                Err(expected)
            );
            assert!(launcher.killed.load(Ordering::SeqCst));
            assert!(launcher.reaped.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn injected_deadline_proves_fixed_timeout_kills_and_reaps_without_sleeping() {
        assert_eq!(RUNNER_DEADLINE, Duration::from_secs(180));
        assert_eq!(RUNNER_REAP_DEADLINE, Duration::from_secs(5));
        let launcher = FakeLauncher::running(Vec::new(), Vec::new());
        assert_eq!(
            run_fake(&launcher, &mut YieldDeadline::after(0)),
            Err(RunnerError::Timeout)
        );
        assert!(launcher.killed.load(Ordering::SeqCst));
        assert!(launcher.reaped.load(Ordering::SeqCst));
    }

    #[test]
    fn exited_child_with_a_held_pipe_remains_under_the_deadline() {
        let (stdout_sender, stdout) = mpsc::sync_channel(1);
        let (stderr_sender, stderr) = mpsc::sync_channel(1);
        stderr_sender
            .send(StreamResult::Complete(Vec::new()))
            .expect("stderr result");
        let killed = Arc::new(AtomicBool::new(false));
        let reaped = Arc::new(AtomicBool::new(false));
        let child = FakeProcess {
            status: VecDeque::from([Some(ExitStatus::from_raw(0))]),
            wait_error: false,
            cleanup_fails: false,
            killed: Arc::clone(&killed),
            reaped: Arc::clone(&reaped),
        };
        let result = monitor_child(
            Box::new(child),
            stdout,
            stderr,
            BudgetCase::Sequential,
            &mut YieldDeadline::after(1),
        );
        drop(stdout_sender);
        drop(stderr_sender);
        assert_eq!(result, Err(RunnerError::Timeout));
        assert!(killed.load(Ordering::SeqCst));
        assert!(reaped.load(Ordering::SeqCst));
    }

    #[test]
    fn child_wait_failure_always_invokes_kill_and_reap() {
        let (stdout_sender, stdout) = mpsc::sync_channel(1);
        let (stderr_sender, stderr) = mpsc::sync_channel(1);
        stdout_sender
            .send(StreamResult::Complete(Vec::new()))
            .expect("stdout result");
        stderr_sender
            .send(StreamResult::Complete(Vec::new()))
            .expect("stderr result");
        let killed = Arc::new(AtomicBool::new(false));
        let reaped = Arc::new(AtomicBool::new(false));
        let child = FakeProcess {
            status: VecDeque::new(),
            wait_error: true,
            cleanup_fails: false,
            killed: Arc::clone(&killed),
            reaped: Arc::clone(&reaped),
        };
        let result = monitor_child(
            Box::new(child),
            stdout,
            stderr,
            BudgetCase::Sequential,
            &mut YieldDeadline::never(),
        );
        assert_eq!(result, Err(RunnerError::ProtocolInvalid));
        assert!(killed.load(Ordering::SeqCst));
        assert!(reaped.load(Ordering::SeqCst));
    }

    #[test]
    fn unconfirmed_cleanup_fails_closed_instead_of_discarding_the_result() {
        let (stdout_sender, stdout) = mpsc::sync_channel(1);
        let (stderr_sender, stderr) = mpsc::sync_channel(1);
        stdout_sender
            .send(StreamResult::Complete(Vec::new()))
            .expect("stdout result");
        stderr_sender
            .send(StreamResult::Complete(Vec::new()))
            .expect("stderr result");
        let killed = Arc::new(AtomicBool::new(false));
        let reaped = Arc::new(AtomicBool::new(false));
        let child = FakeProcess {
            status: VecDeque::new(),
            wait_error: false,
            cleanup_fails: true,
            killed: Arc::clone(&killed),
            reaped: Arc::clone(&reaped),
        };
        let result = monitor_child(
            Box::new(child),
            stdout,
            stderr,
            BudgetCase::Sequential,
            &mut YieldDeadline::after(0),
        );
        assert_eq!(result, Err(RunnerError::ProtocolInvalid));
        assert!(killed.load(Ordering::SeqCst));
        assert!(!reaped.load(Ordering::SeqCst));
    }

    #[test]
    fn runner_must_be_a_direct_path_not_a_lookup_name() {
        assert_eq!(
            validate_direct_runner(OsStr::new("riffdb-budget-public")),
            Err(RunnerError::StartFailed)
        );
        assert!(validate_direct_runner(OsStr::new("./riffdb-budget-public")).is_ok());
        assert!(validate_direct_runner(OsStr::new("/tmp/riffdb-budget-public")).is_ok());
    }

    fn run_fake(
        launcher: &FakeLauncher,
        deadline: &mut dyn ProcessDeadline,
    ) -> Result<(), RunnerError> {
        run_budget_with(
            launcher,
            deadline,
            OsStr::new("/not/executed"),
            BudgetCase::Sequential,
            "http://127.0.0.1:7443",
            Path::new("/credential"),
        )
    }

    struct FakeLauncher {
        status: RefCell<VecDeque<Option<ExitStatus>>>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        killed: Arc<AtomicBool>,
        reaped: Arc<AtomicBool>,
    }

    impl FakeLauncher {
        fn exited(code: i32, stdout: Vec<u8>, stderr: Vec<u8>) -> Self {
            Self {
                status: RefCell::new(VecDeque::from([Some(ExitStatus::from_raw(code << 8))])),
                stdout,
                stderr,
                killed: Arc::new(AtomicBool::new(false)),
                reaped: Arc::new(AtomicBool::new(false)),
            }
        }

        fn running(stdout: Vec<u8>, stderr: Vec<u8>) -> Self {
            Self {
                status: RefCell::new(VecDeque::new()),
                stdout,
                stderr,
                killed: Arc::new(AtomicBool::new(false)),
                reaped: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl ProcessLauncher for FakeLauncher {
        fn launch(&self, _runner: &OsStr, arguments: &[OsString]) -> Result<SpawnedProcess, ()> {
            assert_eq!(
                arguments,
                runner_arguments(
                    BudgetCase::Sequential,
                    "http://127.0.0.1:7443",
                    Path::new("/credential"),
                )
            );
            Ok(SpawnedProcess {
                control: Box::new(FakeProcess {
                    status: self.status.borrow().clone(),
                    wait_error: false,
                    cleanup_fails: false,
                    killed: Arc::clone(&self.killed),
                    reaped: Arc::clone(&self.reaped),
                }),
                stdout: Box::new(Cursor::new(self.stdout.clone())),
                stderr: Box::new(Cursor::new(self.stderr.clone())),
            })
        }
    }

    struct FakeProcess {
        status: VecDeque<Option<ExitStatus>>,
        wait_error: bool,
        cleanup_fails: bool,
        killed: Arc<AtomicBool>,
        reaped: Arc<AtomicBool>,
    }

    impl ProcessControl for FakeProcess {
        fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
            if self.wait_error {
                return Err(io::Error::other("injected wait failure"));
            }
            let status = self.status.pop_front().unwrap_or(None);
            if status.is_some() {
                self.reaped.store(true, Ordering::SeqCst);
            }
            Ok(status)
        }

        fn kill_and_reap(&mut self) -> Result<(), ()> {
            self.killed.store(true, Ordering::SeqCst);
            if self.cleanup_fails {
                return Err(());
            }
            self.reaped.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    struct YieldDeadline {
        waits: usize,
        expire_after: Option<usize>,
    }

    impl YieldDeadline {
        const fn never() -> Self {
            Self {
                waits: 0,
                expire_after: None,
            }
        }

        const fn after(waits: usize) -> Self {
            Self {
                waits: 0,
                expire_after: Some(waits),
            }
        }
    }

    impl ProcessDeadline for YieldDeadline {
        fn expired(&self) -> bool {
            self.expire_after
                .is_some_and(|expire_after| self.waits >= expire_after)
        }

        fn wait(&mut self) {
            self.waits += 1;
            thread::yield_now();
        }
    }

    fn temporary_directory() -> tempfile::TempDir {
        tempfile::TempDir::with_prefix("riffdb-cli-runner-").expect("scratch directory")
    }

    fn write_executable_runner(directory: &Path, name: &str, script: &str) -> PathBuf {
        let runner = directory.join(name);
        fs::write(&runner, script).expect("script");
        let mut permissions = fs::metadata(&runner).expect("metadata").permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&runner, permissions).expect("mode");
        runner
    }
}
