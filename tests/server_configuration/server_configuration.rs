#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Process-level precedence and strictness checks for `riffdbd` configuration.

use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const START_TIMEOUT: Duration = Duration::from_secs(20);
const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS: &[u8] = b"riffdb-idempotency-digest-keys-v1\n1:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
fn toml_environment_and_cli_precedence_start_real_riffdbd() -> TestResult<()> {
    let root = TestRoot::new("precedence")?;
    let paths = ProcessPaths::new(root.path())?;
    let config = paths.write_config("toml.redb", "127.0.0.1:1", "toml", "toml-audience")?;

    let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
    clear_server_environment(&mut command);
    command
        .arg("--config")
        .arg(&config)
        .arg("--listen")
        .arg("127.0.0.1:0")
        .arg("--environment")
        .arg("cli")
        .arg("--audience")
        .arg("cli-audience")
        .env("RIFFDB_DATABASE", &paths.environment_database)
        .env("RIFFDB_LISTEN", "0.0.0.0:2")
        .env("RIFFDB_ENVIRONMENT", "environment")
        .env("RIFFDB_AUDIENCE", "environment-audience");
    run_to_readiness_and_shutdown(command)?;

    if !paths.environment_database.is_file() || paths.toml_database.exists() {
        return Err("per-field precedence selected the wrong database path".into());
    }
    Ok(())
}

#[test]
fn explicitly_selected_toml_can_supply_every_server_field() -> TestResult<()> {
    let root = TestRoot::new("toml-only")?;
    let paths = ProcessPaths::new(root.path())?;
    let config = paths.write_config(
        "toml.redb",
        "127.0.0.1:0",
        "toml-only",
        "riffdb-grpc-loopback",
    )?;

    let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
    clear_server_environment(&mut command);
    command.arg("--config").arg(config);
    run_to_readiness_and_shutdown(command)?;
    if !paths.toml_database.is_file() {
        return Err("TOML-selected database was not opened".into());
    }
    Ok(())
}

#[test]
// req: REP-003, REC-001
fn two_named_databases_start_on_one_listener_and_create_isolated_files() -> TestResult<()> {
    let root = TestRoot::new("named-databases")?;
    let paths = ProcessPaths::new(root.path())?;
    let alpha = root.path().join("alpha.redb");
    let beta = root.path().join("beta.redb");
    let alpha_backups = root.path().join("alpha-backups");
    let beta_backups = root.path().join("beta-backups");
    fs::create_dir_all(&alpha_backups)?;
    fs::create_dir_all(&beta_backups)?;
    let document = format!(
        "[server]\n\
         grpc_listen = \"127.0.0.1:0\"\n\
         audience = \"riffdb-grpc-loopback\"\n\
         capability_keys = {capability_keys:?}\n\
         idempotency_keys = {idempotency_keys:?}\n\
         \n\
         [databases.alpha]\n\
         path = {alpha:?}\n\
         backup_root = {alpha_backups:?}\n\
         environment = \"development\"\n\
         \n\
         [databases.beta]\n\
         path = {beta:?}\n\
         backup_root = {beta_backups:?}\n\
         environment = \"development\"\n",
        capability_keys = paths
            .capability_keys
            .to_str()
            .ok_or("non-UTF-8 capability path")?,
        idempotency_keys = paths
            .idempotency_keys
            .to_str()
            .ok_or("non-UTF-8 idempotency path")?,
        alpha = alpha.to_str().ok_or("non-UTF-8 alpha path")?,
        beta = beta.to_str().ok_or("non-UTF-8 beta path")?,
        alpha_backups = alpha_backups
            .to_str()
            .ok_or("non-UTF-8 alpha backup path")?,
        beta_backups = beta_backups.to_str().ok_or("non-UTF-8 beta backup path")?,
    );
    let config = root.path().join("riffdb-multi.toml");
    fs::write(&config, document)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
    clear_server_environment(&mut command);
    command.arg("--config").arg(config);
    let stdout = run_to_readiness_and_capture_shutdown(command)?;
    if stdout
        .lines()
        .filter(|line| line.starts_with("riffdb-writer-evidence-v1\t"))
        .count()
        != 2
    {
        return Err("named databases did not emit one writer evidence line per graph".into());
    }
    for database in [&alpha, &beta] {
        let mut area = database.as_os_str().to_os_string();
        area.push(".riffreplication");
        let area = PathBuf::from(area);
        if !area.join("inventory.lock").is_file() || fs::read_dir(&area)?.count() != 1 {
            return Err(
                "named databases did not reserve independent bounded artifact roots".into(),
            );
        }
    }
    if !alpha.is_file() || !beta.is_file() {
        return Err("named databases did not create two isolated storage files".into());
    }
    Ok(())
}

#[test]
fn hosted_mcp_selects_a_named_database_before_authentication() -> TestResult<()> {
    let root = TestRoot::new("named-hosted-mcp")?;
    let paths = ProcessPaths::new(root.path())?;
    let mcp_address = reserve_loopback_address()?;
    let alpha = root.path().join("alpha.redb");
    let beta = root.path().join("beta.redb");
    let alpha_backups = root.path().join("alpha-backups");
    let beta_backups = root.path().join("beta-backups");
    fs::create_dir_all(&alpha_backups)?;
    fs::create_dir_all(&beta_backups)?;
    let document = format!(
        "[server]\n\
         grpc_listen = \"127.0.0.1:0\"\n\
         mcp_listen = {mcp_listen:?}\n\
         audience = \"riffdb-grpc-loopback\"\n\
         capability_keys = {capability_keys:?}\n\
         idempotency_keys = {idempotency_keys:?}\n\
         \n\
         [databases.alpha]\n\
         path = {alpha:?}\n\
         backup_root = {alpha_backups:?}\n\
         environment = \"development\"\n\
         \n\
         [databases.beta]\n\
         path = {beta:?}\n\
         backup_root = {beta_backups:?}\n\
         environment = \"development\"\n",
        mcp_listen = mcp_address.to_string(),
        capability_keys = paths
            .capability_keys
            .to_str()
            .ok_or("non-UTF-8 capability path")?,
        idempotency_keys = paths
            .idempotency_keys
            .to_str()
            .ok_or("non-UTF-8 idempotency path")?,
        alpha = alpha.to_str().ok_or("non-UTF-8 alpha path")?,
        beta = beta.to_str().ok_or("non-UTF-8 beta path")?,
        alpha_backups = alpha_backups
            .to_str()
            .ok_or("non-UTF-8 alpha backup path")?,
        beta_backups = beta_backups.to_str().ok_or("non-UTF-8 beta backup path")?,
    );
    let config = root.path().join("riffdb-hosted-mcp.toml");
    fs::write(&config, document)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
    clear_server_environment(&mut command);
    command.arg("--config").arg(config);
    run_to_readiness_with(command, || {
        assert_hosted_mcp_status(mcp_address, &[], 404)?;
        assert_hosted_mcp_status(
            mcp_address,
            &[("riffdb-database", "alpha"), ("riffdb-database", "beta")],
            404,
        )?;
        assert_hosted_mcp_status(mcp_address, &[("riffdb-database", "INVALID")], 404)?;
        assert_hosted_mcp_status(mcp_address, &[("riffdb-database", "unknown")], 404)?;
        assert_hosted_mcp_status(mcp_address, &[("riffdb-database", "alpha")], 401)?;
        Ok(())
    })
    .map(|_| ())
}

#[test]
fn safe_defaults_start_from_an_explicit_working_directory() -> TestResult<()> {
    let root = TestRoot::new("defaults")?;
    let config_root = root.path().join("config");
    fs::create_dir_all(root.path().join("data"))?;
    fs::create_dir_all(root.path().join("backups"))?;
    fs::create_dir_all(&config_root)?;
    write_protected(&config_root.join("capability.keys"), CAPABILITY_KEYS)?;
    write_protected(&config_root.join("idempotency.keys"), IDEMPOTENCY_KEYS)?;

    let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
    clear_server_environment(&mut command);
    command
        .current_dir(root.path())
        .arg("--listen")
        .arg("127.0.0.1:0");
    run_to_readiness_and_shutdown(command)?;
    if !root.path().join("data/riffdb.redb").is_file() {
        return Err("default database path was not opened".into());
    }
    Ok(())
}

#[test]
fn unknown_duplicate_wrong_type_oversize_and_invalid_higher_values_reject() -> TestResult<()> {
    let root = TestRoot::new("strict")?;
    let cases = [
        "[server]\nunknown = true\n".as_bytes().to_vec(),
        "[server]\ndatabase = \"a\"\ndatabase = \"b\"\n"
            .as_bytes()
            .to_vec(),
        "[server]\ngrpc_listen = 7443\n".as_bytes().to_vec(),
        "[unknown]\nvalue = true\n".as_bytes().to_vec(),
        vec![b'x'; 65_537],
    ];
    for (index, bytes) in cases.into_iter().enumerate() {
        let path = root.path().join(format!("invalid-{index}.toml"));
        fs::write(&path, bytes)?;
        assert_config_rejected(["--config", path.to_str().ok_or("non-UTF-8 test path")?])?;
    }

    let paths = ProcessPaths::new(root.path())?;
    let valid = paths.write_config("toml.redb", "127.0.0.1:0", "toml", "riffdb-grpc-loopback")?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
    clear_server_environment(&mut command);
    let output = command
        .arg("--config")
        .arg(valid)
        .env("RIFFDB_LISTEN", "")
        .output()?;
    if output.status.success()
        || !output.stdout.is_empty()
        || String::from_utf8_lossy(&output.stderr).contains("127.0.0.1")
    {
        return Err("invalid higher-precedence environment value did not fail closed".into());
    }
    Ok(())
}

fn assert_config_rejected<const N: usize>(arguments: [&str; N]) -> TestResult<()> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
    clear_server_environment(&mut command);
    let output = command.args(arguments).output()?;
    if output.status.success() || !output.stdout.is_empty() {
        return Err("invalid configuration reached process readiness".into());
    }
    let stderr = String::from_utf8(output.stderr)?;
    if !stderr.starts_with("RDB-CONFIG-0001: ") || !stderr.ends_with('\n') || stderr.len() > 256 {
        return Err("invalid configuration did not return one bounded safe diagnostic".into());
    }
    Ok(())
}

fn clear_server_environment(command: &mut Command) {
    for name in [
        "RIFFDB_CONFIG",
        "RIFFDB_DATABASE",
        "RIFFDB_LISTEN",
        "RIFFDB_ENVIRONMENT",
        "RIFFDB_AUDIENCE",
        "RIFFDB_MCP_LISTEN",
        "RIFFDB_MCP_ORIGINS",
        "RIFFDB_BACKUP_ROOT",
        "RIFFDB_CAPABILITY_KEYS",
        "RIFFDB_IDEMPOTENCY_KEYS",
    ] {
        command.env_remove(name);
    }
}

fn run_to_readiness_and_shutdown(command: Command) -> TestResult<()> {
    run_to_readiness_with(command, || Ok(())).map(|_| ())
}

fn run_to_readiness_and_capture_shutdown(command: Command) -> TestResult<String> {
    run_to_readiness_with(command, || Ok(()))
}

fn run_to_readiness_with(
    mut command: Command,
    after_readiness: impl FnOnce() -> TestResult<()>,
) -> TestResult<String> {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().ok_or("missing child stdout")?;
    let stderr = child.stderr.take().ok_or("missing child stderr")?;
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
    let stdout_thread = std::thread::spawn(move || -> std::io::Result<String> {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(_) => {
                let _ = ready_sender.send(Ok(line.clone()));
            }
            Err(error) => {
                let _ =
                    ready_sender.send(Err(std::io::Error::new(error.kind(), error.to_string())));
                return Err(error);
            }
        }
        let mut suffix = String::new();
        reader.read_to_string(&mut suffix)?;
        line.push_str(&suffix);
        Ok(line)
    });
    let stderr_thread = std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut bytes = Vec::new();
        let _ = std::io::Read::read_to_end(&mut reader, &mut bytes);
        bytes
    });

    let line = ready_receiver
        .recv_timeout(START_TIMEOUT)
        .map_err(|_| "riffdbd readiness timed out")??;
    if !line.starts_with(READY_PREFIX) || line.len() > 256 {
        let _ = child.kill();
        let _ = child.wait();
        let _ = stdout_thread.join();
        let stderr = stderr_thread.join().map_err(|_| "stderr reader panicked")?;
        return Err(format!(
            "riffdbd emitted an invalid readiness line: {}",
            String::from_utf8_lossy(&stderr)
        )
        .into());
    }
    let assertion = after_readiness();
    let mut stdin = child.stdin.take().ok_or("missing child stdin")?;
    stdin.write_all(b"shutdown\n")?;
    stdin.flush()?;
    drop(stdin);

    let status = child.wait()?;
    let stdout = stdout_thread
        .join()
        .map_err(|_| "stdout reader panicked")??;
    let stderr = stderr_thread.join().map_err(|_| "stderr reader panicked")?;
    if !status.success() {
        return Err(format!(
            "riffdbd did not shut down cleanly: {}",
            String::from_utf8_lossy(&stderr)
        )
        .into());
    }
    assertion?;
    Ok(stdout)
}

fn reserve_loopback_address() -> TestResult<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

fn assert_hosted_mcp_status(
    address: SocketAddr,
    headers: &[(&str, &str)],
    expected: u16,
) -> TestResult<()> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    write!(
        stream,
        "POST /mcp HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n"
    )?;
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"\r\n{}")?;
    stream.flush()?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or("hosted MCP returned an invalid HTTP status line")?;
    if status != expected {
        return Err(format!("hosted MCP returned status {status}, expected {expected}").into());
    }
    Ok(())
}

struct ProcessPaths {
    root: PathBuf,
    toml_database: PathBuf,
    environment_database: PathBuf,
    capability_keys: PathBuf,
    idempotency_keys: PathBuf,
    backup_root: PathBuf,
}

impl ProcessPaths {
    fn new(root: &Path) -> TestResult<Self> {
        let capability_keys = root.join("capability.keys");
        let idempotency_keys = root.join("idempotency.keys");
        let backup_root = root.join("backups");
        fs::create_dir_all(&backup_root)?;
        write_protected(&capability_keys, CAPABILITY_KEYS)?;
        write_protected(&idempotency_keys, IDEMPOTENCY_KEYS)?;
        Ok(Self {
            root: root.to_path_buf(),
            toml_database: root.join("toml.redb"),
            environment_database: root.join("environment.redb"),
            capability_keys,
            idempotency_keys,
            backup_root,
        })
    }

    fn write_config(
        &self,
        database_name: &str,
        listen: &str,
        environment: &str,
        audience: &str,
    ) -> TestResult<PathBuf> {
        let database = self.root.join(database_name);
        let document = format!(
            "[server]\n\
             database = {database:?}\n\
             grpc_listen = {listen:?}\n\
             environment = {environment:?}\n\
             audience = {audience:?}\n\
             capability_keys = {capability_keys:?}\n\
             idempotency_keys = {idempotency_keys:?}\n\
             \n\
             [maintenance]\n\
             backup_root = {backup_root:?}\n",
            database = database.to_str().ok_or("non-UTF-8 database path")?,
            capability_keys = self
                .capability_keys
                .to_str()
                .ok_or("non-UTF-8 capability path")?,
            idempotency_keys = self
                .idempotency_keys
                .to_str()
                .ok_or("non-UTF-8 idempotency path")?,
            backup_root = self.backup_root.to_str().ok_or("non-UTF-8 backup path")?,
        );
        let path = self.root.join("riffdb.toml");
        fs::write(&path, document)?;
        Ok(path)
    }
}

/// Whole-directory scope backed by the canonical testkit guard: removed on
/// `Drop` — pass, fail, or panic — with a dead-pid sweep for directories
/// orphaned by a killed harness.
struct TestRoot(riffdb_testkit::scratch::ScratchDir);

impl TestRoot {
    fn new(label: &str) -> TestResult<Self> {
        let scratch = riffdb_testkit::scratch::ScratchDir::new(&format!("server-config-{label}"))?;
        Ok(Self(scratch))
    }

    fn path(&self) -> &Path {
        self.0.path()
    }
}

fn write_protected(path: &Path, bytes: &[u8]) -> TestResult<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}
