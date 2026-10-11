//! Finite campaign-only ownership barrier after the last acknowledged load operation.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::Result;
use super::probe::{Release, publish_quiescence, read_release};

const LIMIT: Duration = Duration::from_secs(60);

pub(super) fn configured(root: Option<&Path>, value: Option<&str>) -> Result<bool> {
    match value {
        None => Ok(false),
        Some("1") => {
            let root = root.ok_or("terminal_configuration")?;
            if !root.is_absolute() || root.is_symlink() || !root.is_dir() {
                return Err("terminal_configuration");
            }
            for name in [
                "load.terminal",
                ".load.terminal.pending",
                "load.terminal-release",
            ] {
                if std::fs::symlink_metadata(root.join(name)).is_ok() {
                    return Err("terminal_stale");
                }
            }
            read_release(root, &root.join("load.phase"))?;
            Ok(true)
        }
        _ => Err("terminal_configuration"),
    }
}

struct Gate {
    root: PathBuf,
    release: Release,
    last: Duration,
    finished: bool,
    failed: bool,
}

impl Gate {
    fn begin(root: &Path, release: Release, commands: u64, reads: u64) -> Result<Self> {
        configured(Some(root), Some("1"))?;
        if read_release(root, &root.join("load.phase"))? != release
            || root.join("load.quiesce").exists()
            || root.join("observer.quiesce").exists()
        {
            return Err("terminal_identity");
        }
        publish_quiescence(
            root,
            "load.terminal",
            &format!(
                concat!(
                    "{{\"schema\":\"riffdb.ticketdesk-lifecycle-terminal/v1\",",
                    "\"generation\":{},\"contract_version\":{},\"history_incarnation\":{},",
                    "\"completed_commands\":{commands},\"completed_reads\":{reads}}}\n"
                ),
                release.generation,
                release.version,
                release.incarnation,
                commands = commands,
                reads = reads
            ),
        )?;
        Ok(Self {
            root: root.to_path_buf(),
            release,
            last: Duration::ZERO,
            finished: false,
            failed: false,
        })
    }

    fn poll(&mut self, elapsed: Duration) -> Result<bool> {
        if self.failed || self.finished {
            return Err("terminal_finished");
        }
        let result = self.poll_inner(elapsed);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn poll_inner(&mut self, elapsed: Duration) -> Result<bool> {
        if elapsed < self.last || elapsed >= LIMIT {
            return Err("terminal_deadline");
        }
        self.last = elapsed;
        if read_release(&self.root, &self.root.join("load.phase"))? != self.release
            || self.root.join("load.quiesce").exists()
            || self.root.join("observer.quiesce").exists()
        {
            return Err("terminal_identity");
        }
        let path = self.root.join("load.terminal-release");
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(_) => return Err("terminal_release"),
            Ok(_) => {}
        }
        if read_release(&self.root, &path)? != self.release {
            return Err("terminal_identity");
        }
        self.finished = true;
        Ok(true)
    }
}

pub(super) async fn hold(root: &Path, release: Release, commands: u64, reads: u64) -> Result<()> {
    let mut gate = Gate::begin(root, release, commands, reads)?;
    let started = Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if gate.poll(started.elapsed())? {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Directory(PathBuf);

    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn fixture(name: &str) -> (Directory, Release) {
        let root = Directory(
            std::env::temp_dir().join(format!("riffdb-terminal-{name}-{}", std::process::id())),
        );
        std::fs::create_dir(&root.0).unwrap();
        std::fs::write(root.0.join("load.phase"), b"4 2 2 application.credential\n").unwrap();
        let release = read_release(&root.0, &root.0.join("load.phase")).unwrap();
        (root, release)
    }

    // req: END-007, END-008, END-010
    #[test]
    fn terminal_retains_completed_load_until_exact_controller_release() {
        let (root, release) = fixture("release");
        let mut gate = Gate::begin(&root.0, release, 2_880_000, 11_520_000).unwrap();
        let ready = std::fs::read_to_string(root.0.join("load.terminal")).unwrap();
        assert!(
            ready.contains("\"generation\":4,\"contract_version\":2,\"history_incarnation\":2")
        );
        assert!(ready.contains("\"completed_commands\":2880000,\"completed_reads\":11520000"));
        for second in [0, 1, 30, 59] {
            assert!(!gate.poll(Duration::from_secs(second)).unwrap());
        }
        // Publication, rather than elapsed time, releases the original owner.
        std::fs::write(
            root.0.join("load.terminal-release"),
            b"4 2 2 application.credential\n",
        )
        .unwrap();
        assert!(gate.poll(Duration::from_secs(59)).unwrap());
        assert_eq!(gate.poll(Duration::from_secs(59)), Err("terminal_finished"));
        assert_eq!(
            std::fs::read_to_string(root.0.join("load.terminal")).unwrap(),
            ready
        );
    }

    // req: END-007, END-008, END-010
    #[test]
    fn terminal_refuses_wrong_release_stale_files_phase_changes_and_expired_waits() {
        for (index, encoded) in [
            "3 2 2 application.credential\n",
            "4 1 2 application.credential\n",
            "4 2 1 application.credential\n",
            "4 2 2 other.credential\n",
            "04 2 2 application.credential\n",
        ]
        .iter()
        .enumerate()
        {
            let (root, release) = fixture(&format!("wrong-{index}"));
            let mut gate = Gate::begin(&root.0, release, 0, 0).unwrap();
            std::fs::write(root.0.join("load.terminal-release"), encoded).unwrap();
            assert!(gate.poll(Duration::ZERO).is_err());
            assert_eq!(gate.poll(Duration::ZERO), Err("terminal_finished"));
            assert_eq!(configured(Some(&root.0), Some("1")), Err("terminal_stale"));
        }
        for (name, elapsed, changed) in
            [("deadline", LIMIT, false), ("phase", Duration::ZERO, true)]
        {
            let (root, release) = fixture(name);
            let mut gate = Gate::begin(&root.0, release, 0, 0).unwrap();
            if changed {
                std::fs::write(root.0.join("load.phase"), b"5 2 2 application.credential\n")
                    .unwrap();
            }
            assert_eq!(
                gate.poll(elapsed),
                Err(if changed {
                    "terminal_identity"
                } else {
                    "terminal_deadline"
                })
            );
        }
        let (root, release) = fixture("clock");
        let mut gate = Gate::begin(&root.0, release, 0, 0).unwrap();
        assert!(!gate.poll(Duration::from_secs(2)).unwrap());
        assert_eq!(gate.poll(Duration::from_secs(1)), Err("terminal_deadline"));
        assert_eq!(configured(None, None), Ok(false));
        assert_eq!(configured(None, Some("1")), Err("terminal_configuration"));
        assert_eq!(
            configured(Some(&root.0), Some("0")),
            Err("terminal_configuration")
        );
    }

    // req: END-007, END-008, END-010
    #[test]
    fn terminal_process_release_preserves_original_owner() {
        use std::io::{BufRead, Read, Write};
        use std::process::{Child, Command, Stdio};
        if let Some(root) = std::env::var_os("RIFFDB_TEST_TERMINAL_CHILD") {
            let root = PathBuf::from(root);
            let release = read_release(&root, &root.join("load.phase")).unwrap();
            let mut gate = Gate::begin(&root, release, 71, 284).unwrap();
            println!("terminal-ready:{}", std::process::id());
            std::io::stdout().flush().unwrap();
            for second in [0, 1] {
                let mut command = [0];
                std::io::stdin().read_exact(&mut command).unwrap();
                assert_eq!(command, [b'p']);
                let released = gate.poll(Duration::from_secs(second)).unwrap();
                assert_eq!(released, second == 1);
                println!("terminal-{}", if released { "released" } else { "held" });
                std::io::stdout().flush().unwrap();
            }
            return;
        }
        struct OwnedChild(Child);
        impl Drop for OwnedChild {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let (root, _) = fixture("process");
        let mut child = OwnedChild(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "terminal::tests::terminal_process_release_preserves_original_owner",
                    "--nocapture",
                    "--format=terse",
                ])
                .env("RIFFDB_TEST_TERMINAL_CHILD", &root.0)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(3);
        let reader = std::thread::spawn(move || {
            let mut source = std::io::BufReader::new(stdout);
            // Test-harness preamble and three protocol messages are bounded.
            for _ in 0..32 {
                let mut line = Vec::new();
                let count = source
                    .by_ref()
                    .take(1025)
                    .read_until(b'\n', &mut line)
                    .unwrap();
                if count == 0 {
                    return;
                }
                assert!(count <= 1024 && line.ends_with(b"\n"));
                if line.starts_with(b"terminal-") {
                    sender.send(String::from_utf8(line).unwrap()).unwrap();
                }
            }
        });
        let receive = || receiver.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(receive(), format!("terminal-ready:{}\n", child.0.id()));
        let ready = std::fs::read_to_string(root.0.join("load.terminal")).unwrap();
        assert!(ready.contains("\"completed_commands\":71,\"completed_reads\":284"));
        child.0.stdin.as_mut().unwrap().write_all(b"p").unwrap();
        assert_eq!(receive(), "terminal-held\n");
        assert!(child.0.try_wait().unwrap().is_none());
        std::fs::write(
            root.0.join("load.terminal-release"),
            b"4 2 2 application.credential\n",
        )
        .unwrap();
        child.0.stdin.as_mut().unwrap().write_all(b"p").unwrap();
        assert_eq!(receive(), "terminal-released\n");
        assert!(child.0.wait().unwrap().success());
        reader.join().unwrap();
    }
}
