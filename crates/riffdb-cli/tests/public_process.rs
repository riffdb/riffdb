#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Ignored external-process proof for every WP-150 public CLI operation.

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use riffdb_auth::bootstrap_secret::{
    BootstrapCredential, SystemEntropy, generate_bootstrap_credential,
    load_bootstrap_credential_file,
};
use serde_json::Value;
use zeroize::Zeroizing;

const SERVER_ENV: &str = "RIFFDB_TEST_RIFFDBD_BIN";
const RUNNER_ENV: &str = "RIFFDB_TEST_BUDGET_RUNNER_BIN";
const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "wp150-public-process";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const SHUTDOWN: &[u8] = b"shutdown\n";
const START_TIMEOUT: Duration = Duration::from_secs(15);
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CLI_OUTPUT: usize = 4_194_304;
const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS: &[u8] = b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const ORGANIZATION_ID: &str = "018f22a1-7b3c-7def-8123-456789abcdef";
const FISCAL_YEAR: i64 = 2026;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
#[ignore = "requires separately built riffdbd and riffdb-budget-public"]
fn public_process_all_acceptance_operations() -> TestResult<()> {
    let binaries = Binaries::from_environment()?;
    run_generated_bootstrap_and_retention_order(&binaries)?;
    run_stdin_bootstrap_flow(&binaries)?;
    run_core_cli_operations(&binaries)?;
    for case in ["sequential", "contention", "same_key_replay"] {
        run_demo_case(&binaries, case)?;
    }
    Ok(())
}

fn run_generated_bootstrap_and_retention_order(binaries: &Binaries) -> TestResult<()> {
    let mut harness = Harness::start(binaries)?;
    let request = harness.temporary.path().join("generate-request.json");
    let retained_before_failure = harness
        .temporary
        .path()
        .join("generated-before-failure.credential");
    let occupied_bearer = harness.temporary.path().join("occupied.bearer");
    write_regular(&request, bootstrap_request().as_bytes())?;
    write_protected(&occupied_bearer, b"occupied")?;

    let failure = harness.cli_output(
        &[
            "--endpoint",
            harness.endpoint.as_str(),
            "--output",
            "json",
            "capability",
            "bootstrap",
            "--request",
            path_text(&request)?,
            "--generate",
            path_text(&retained_before_failure)?,
            "--bearer-output",
            path_text(&occupied_bearer)?,
        ],
        None,
    )?;
    assert_local_failure(&failure, "credential_retention_failed")?;
    load_bootstrap_credential_file(&retained_before_failure)?;

    let health = harness.cli(&[
        "--endpoint",
        harness.endpoint.as_str(),
        "--output",
        "json",
        "server",
        "health",
    ])?;
    assert_status(&health, "pre_bootstrap")?;

    let generated_document = harness.temporary.path().join("generated.credential");
    let generated_bearer = harness.temporary.path().join("generated.bearer");
    let generated = harness.cli(&[
        "--endpoint",
        harness.endpoint.as_str(),
        "--output",
        "json",
        "capability",
        "bootstrap",
        "--request",
        path_text(&request)?,
        "--generate",
        path_text(&generated_document)?,
        "--bearer-output",
        path_text(&generated_bearer)?,
    ])?;
    assert_status(&generated, "created")?;
    assert_eq!(generated["result"]["bearer_retained"], true);
    let capability_id =
        assert_generated_bootstrap_identity(&generated_document, &generated_bearer)?;
    assert_eq!(
        json_string(
            &generated,
            &["result", "transition", "identity", "capability_id"]
        )?,
        capability_id
    );
    harness.stop()
}

fn run_stdin_bootstrap_flow(binaries: &Binaries) -> TestResult<()> {
    let mut harness = Harness::start(binaries)?;
    let request = harness.temporary.path().join("stdin-request.json");
    let bearer = harness.temporary.path().join("stdin.bearer");
    write_regular(&request, bootstrap_request().as_bytes())?;
    let source = generate_bootstrap_credential(1_700_000_000_001, &SystemEntropy)?;
    let document = source.render_document();

    let result = harness.cli_with_stdin(
        &[
            "--endpoint",
            harness.endpoint.as_str(),
            "--output",
            "json",
            "capability",
            "bootstrap",
            "--request",
            path_text(&request)?,
            "--bootstrap-stdin",
            "--bearer-output",
            path_text(&bearer)?,
        ],
        document.expose_secret(),
    )?;
    assert_status(&result, "created")?;
    assert_eq!(result["result"]["bearer_retained"], true);
    assert_eq!(
        json_string(
            &result,
            &["result", "transition", "identity", "capability_id"]
        )?,
        source.capability_id().to_string()
    );
    assert_bearer_identity(&source, &bearer)?;
    harness.stop()
}

fn run_core_cli_operations(binaries: &Binaries) -> TestResult<()> {
    let mut harness = Harness::start(binaries)?;
    let endpoint = harness.endpoint.clone();
    let pre_bootstrap = harness.cli(&[
        "--endpoint",
        endpoint.as_str(),
        "--output",
        "json",
        "server",
        "health",
    ])?;
    assert_status(&pre_bootstrap, "pre_bootstrap")?;

    harness.bootstrap_and_deploy()?;
    let health = harness.authenticated(&["server", "health"])?;
    assert_status(&health, "ready")?;

    let create_input = harness.temporary.path().join("create-budget.json");
    write_regular(
        &create_input,
        create_budget_input("wp150-cli-create").as_bytes(),
    )?;
    let created = harness.authenticated(&[
        "command",
        "execute",
        "CreateBudget",
        "--input",
        path_text(&create_input)?,
    ])?;
    assert_status(&created, "committed")?;
    let commit_sequence = json_string(&created, &["result", "commit_sequence"])?;
    let outcome_uri = json_string(&created, &["result", "outcome_uri"])?;

    let replayed = harness.authenticated(&[
        "command",
        "execute",
        "CreateBudget",
        "--input",
        path_text(&create_input)?,
    ])?;
    assert_status(&replayed, "replayed")?;
    assert_eq!(
        json_string(&replayed, &["result", "commit_sequence"])?,
        commit_sequence
    );

    let outcome =
        harness.authenticated(&["command", "outcome", "--outcome-uri", outcome_uri.as_str()])?;
    assert_status(&outcome, "found")?;

    let key = budget_entity_key()?;
    let entity = harness.authenticated(&["entity", "get", "1", "--entity-key", key.as_str()])?;
    assert_status(&entity, "found")?;

    let commit = harness.authenticated(&["commit", "show", commit_sequence.as_str()])?;
    assert_status(&commit, "found")?;

    let projection_input = harness.temporary.path().join("projection.json");
    write_regular(&projection_input, br#"{"leading_components":[]}"#)?;
    let projection = harness.authenticated(&[
        "projection",
        "query",
        "1",
        "--input",
        path_text(&projection_input)?,
        "--after",
        commit_sequence.as_str(),
        "--wait-nanos",
        "0",
        "--limit",
        "10",
    ])?;
    assert!(projection["result"]["status"].is_string());

    let normal = harness.create_normal_credential("wp150-core-normal")?;
    let capability_id = json_string(
        &normal,
        &["result", "transition", "identity", "capability_id"],
    )?;
    let revoked = harness.authenticated(&[
        "capability",
        "revoke",
        capability_id.as_str(),
        "--reason",
        "requested",
    ])?;
    assert_status(&revoked, "revoked")?;
    harness.stop()
}

fn run_demo_case(binaries: &Binaries, case: &str) -> TestResult<()> {
    let mut harness = Harness::start(binaries)?;
    harness.bootstrap_and_deploy()?;
    let created = harness.create_normal_credential(&format!("wp150-demo-{case}"))?;
    assert_status(&created, "created")?;
    let output = harness.cli(&[
        "--endpoint",
        harness.endpoint.as_str(),
        "--credential-file",
        path_text(&harness.normal_path)?,
        "--output",
        "json",
        "demo",
        "budget",
        "--runner",
        path_text(&binaries.runner)?,
        "--case",
        case,
    ])?;
    assert_status(&output, "passed")?;
    assert_eq!(output["result"]["case"], case);
    harness.stop()
}

struct Binaries {
    server: PathBuf,
    runner: PathBuf,
}

impl Binaries {
    fn from_environment() -> TestResult<Self> {
        let server = required_binary(SERVER_ENV)?;
        let runner = required_binary(RUNNER_ENV)?;
        Ok(Self { server, runner })
    }
}

struct Harness {
    temporary: TemporaryDirectory,
    process: ServerProcess,
    endpoint: String,
    root_path: PathBuf,
    normal_path: PathBuf,
}

impl Harness {
    fn start(binaries: &Binaries) -> TestResult<Self> {
        let temporary = TemporaryDirectory::new()?;
        let capability_keys = temporary.path().join("capability.keys");
        let idempotency_keys = temporary.path().join("idempotency.keys");
        write_protected(&capability_keys, CAPABILITY_KEYS)?;
        write_protected(&idempotency_keys, IDEMPOTENCY_KEYS)?;
        let process = ServerProcess::spawn(
            &binaries.server,
            &temporary.path().join("database"),
            &temporary.path().join("backups"),
            &capability_keys,
            &idempotency_keys,
        )?;
        let address = process.ready_address()?;
        Ok(Self {
            temporary,
            process,
            endpoint: format!("http://{address}"),
            root_path: PathBuf::new(),
            normal_path: PathBuf::new(),
        })
    }

    fn bootstrap_and_deploy(&mut self) -> TestResult<()> {
        let bootstrap = self.temporary.path().join("bootstrap.credential");
        let request = self.temporary.path().join("bootstrap-request.json");
        self.root_path = self.temporary.path().join("root.credential");
        let generated = generate_bootstrap_credential(1_700_000_000_000, &SystemEntropy)?;
        write_protected(&bootstrap, generated.render_document().expose_secret())?;
        write_regular(&request, bootstrap_request().as_bytes())?;

        let result = self.cli(&[
            "--endpoint",
            self.endpoint.as_str(),
            "--output",
            "json",
            "capability",
            "bootstrap",
            "--request",
            path_text(&request)?,
            "--bootstrap-file",
            path_text(&bootstrap)?,
            "--bearer-output",
            path_text(&self.root_path)?,
        ])?;
        assert_status(&result, "created")?;
        assert_eq!(result["result"]["bearer_retained"], true);
        assert_bearer_identity(&generated, &self.root_path)?;

        let contract = workspace_root().join("contracts/examples/budget.riff");
        let validated = self.authenticated(&["contract", "validate", path_text(&contract)?])?;
        assert_status(&validated, "valid")?;
        let deployed = self.authenticated(&[
            "contract",
            "deploy",
            path_text(&contract)?,
            "--expected-version",
            "0",
        ])?;
        assert_status(&deployed, "activated")
    }

    fn create_normal_credential(&mut self, principal: &str) -> TestResult<Value> {
        let request = self.temporary.path().join(format!("{principal}.json"));
        self.normal_path = self
            .temporary
            .path()
            .join(format!("{principal}.credential"));
        write_regular(&request, normal_request(principal).as_bytes())?;
        let root_path = self.root_path.clone();
        let normal_path = self.normal_path.clone();
        self.cli(&[
            "--endpoint",
            self.endpoint.as_str(),
            "--credential-file",
            path_text(&root_path)?,
            "--output",
            "json",
            "capability",
            "create",
            "--request",
            path_text(&request)?,
            "--credential-output",
            path_text(&normal_path)?,
        ])
    }

    fn authenticated(&self, arguments: &[&str]) -> TestResult<Value> {
        let mut complete = vec![
            "--endpoint",
            self.endpoint.as_str(),
            "--credential-file",
            path_text(&self.root_path)?,
            "--output",
            "json",
        ];
        complete.extend_from_slice(arguments);
        self.cli(&complete)
    }

    fn cli(&self, arguments: &[&str]) -> TestResult<Value> {
        success_value(self.cli_output(arguments, None)?)
    }

    fn cli_with_stdin(&self, arguments: &[&str], input: &[u8]) -> TestResult<Value> {
        success_value(self.cli_output(arguments, Some(input))?)
    }

    fn cli_output(&self, arguments: &[&str], input: Option<&[u8]>) -> TestResult<Output> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_riffdb"));
        command.env_clear().args(arguments);
        let output = if let Some(input) = input {
            let mut child = command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| test_failure("CLI stdin was not piped"))?;
            stdin.write_all(input)?;
            drop(stdin);
            child.wait_with_output()?
        } else {
            command.stdin(Stdio::null()).output()?
        };
        if output.stdout.len() > MAX_CLI_OUTPUT || output.stderr.len() > MAX_CLI_OUTPUT {
            return Err(test_failure("CLI output exceeded its accepted bound"));
        }
        Ok(output)
    }

    fn stop(&mut self) -> TestResult<()> {
        self.process.stop()
    }
}

fn success_value(output: Output) -> TestResult<Value> {
    if output.status.code() != Some(0) || !output.stderr.is_empty() {
        return Err(test_failure(format!(
            "CLI invocation failed with {:?}; stdout bytes {}; stderr bytes {}",
            output.status.code(),
            output.stdout.len(),
            output.stderr.len()
        )));
    }
    let value: Value = serde_json::from_slice(&output.stdout)?;
    if value["schema"] != "riffdb.cli.output/v1" || value["ok"] != true {
        return Err(test_failure("CLI returned a non-success terminal object"));
    }
    Ok(value)
}

fn assert_local_failure(output: &Output, expected_code: &str) -> TestResult<()> {
    if output.status.code() != Some(2) || !output.stderr.is_empty() {
        return Err(test_failure(
            "CLI local failure did not use the accepted JSON/exit shape",
        ));
    }
    let value: Value = serde_json::from_slice(&output.stdout)?;
    if value["schema"] != "riffdb.cli.output/v1"
        || value["command"] != "capability.bootstrap"
        || value["ok"] != false
        || value["error"]["type"] != "local"
        || value["error"]["code"] != expected_code
    {
        return Err(test_failure(
            "CLI local failure did not match the expected terminal object",
        ));
    }
    Ok(())
}

fn assert_generated_bootstrap_identity(
    document_path: &Path,
    bearer_path: &Path,
) -> TestResult<String> {
    let credential = load_bootstrap_credential_file(document_path)?;
    let retained = Zeroizing::new(fs::read(document_path)?);
    let rendered = credential.render_document();
    if retained.as_slice() != rendered.expose_secret() {
        return Err(test_failure(
            "generated bootstrap document was not byte-identical after protected reread",
        ));
    }
    assert_bearer_identity(&credential, bearer_path)?;
    Ok(credential.capability_id().to_string())
}

fn assert_bearer_identity(credential: &BootstrapCredential, bearer_path: &Path) -> TestResult<()> {
    let bearer = Zeroizing::new(fs::read(bearer_path)?);
    if bearer.as_slice() != credential.token().expose_secret() {
        return Err(test_failure(
            "retained bearer was not the bootstrap document's exact token presentation",
        ));
    }
    Ok(())
}

fn bootstrap_request() -> String {
    capability_request(
        "wp150-root",
        concat!(
            r#"{"type":"validate_contract"},{"type":"deploy_contract"},"#,
            r#"{"type":"invoke_command","contract_lineage":"LegalSpend","stable_id":1},"#,
            r#"{"type":"invoke_command","contract_lineage":"LegalSpend","stable_id":2},"#,
            r#"{"type":"read_entity","contract_lineage":"LegalSpend","stable_id":1},"#,
            r#"{"type":"query_projection","contract_lineage":"LegalSpend","stable_id":1},"#,
            r#"{"type":"read_commit"},{"type":"subscribe_commits"},{"type":"read_health"},"#,
            r#"{"type":"create_capability"},{"type":"revoke_capability"},{"type":"administer_capabilities"}"#
        ),
    )
}

fn normal_request(principal: &str) -> String {
    capability_request(
        principal,
        concat!(
            r#"{"type":"invoke_command","contract_lineage":"LegalSpend","stable_id":1},"#,
            r#"{"type":"invoke_command","contract_lineage":"LegalSpend","stable_id":2},"#,
            r#"{"type":"read_entity","contract_lineage":"LegalSpend","stable_id":1},"#,
            r#"{"type":"subscribe_commits"}"#
        ),
    )
}

fn capability_request(principal: &str, permissions: &str) -> String {
    format!(
        concat!(
            r#"{{"principal_id":"{}","actor_kind":"human","requested_lifetime_seconds":3600,"#,
            r#""audiences":["riffdb-grpc-loopback"],"grant":{{"tenant_scope":{{"type":"global"}},"#,
            r#""partition_scope":{{"type":"all"}},"permissions":[{}],"#,
            r#""field_visibility":[{{"contract_lineage":"LegalSpend","entity_type_id":1,"field_ids":[1,3,5]}}],"#,
            r#""max_scan_rows":500,"approval_required":[]}}}}"#
        ),
        principal, permissions,
    )
}

fn create_budget_input(idempotency_key: &str) -> String {
    format!(
        concat!(
            r#"{{"type":"record","fields":["#,
            r#"{{"name":"approved_amount","value":{{"type":"decimal","coefficient_twos_complement":"JxA=","scale":2}}}},"#,
            r#"{{"name":"fiscal_year","value":{{"type":"i64","value":"{}"}}}},"#,
            r#"{{"name":"idempotency_key","value":{{"type":"string","value":"{}"}}}},"#,
            r#"{{"name":"organization_id","value":{{"type":"uuid","value":"{}"}}}}"#,
            r#"]}}"#
        ),
        FISCAL_YEAR, idempotency_key, ORGANIZATION_ID,
    )
}

fn budget_entity_key() -> TestResult<String> {
    let mut bytes = vec![0x45, 0x01, 0, 0, 0, 1];
    bytes.extend_from_slice(&parse_uuid(ORGANIZATION_ID)?);
    let mut fiscal_year = FISCAL_YEAR.to_be_bytes();
    fiscal_year[0] ^= 0x80;
    bytes.extend_from_slice(&fiscal_year);
    Ok(STANDARD.encode(bytes))
}

fn parse_uuid(value: &str) -> TestResult<[u8; 16]> {
    let compact = value.replace('-', "");
    if compact.len() != 32 {
        return Err(test_failure("invalid fixture UUID"));
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&compact[index * 2..index * 2 + 2], 16)?;
    }
    Ok(bytes)
}

fn assert_status(value: &Value, expected: &str) -> TestResult<()> {
    if value["result"]["status"] != expected {
        return Err(test_failure(format!(
            "expected status {expected}, got {:?}",
            value["result"]["status"]
        )));
    }
    Ok(())
}

fn json_string(value: &Value, path: &[&str]) -> TestResult<String> {
    let mut selected = value;
    for segment in path {
        selected = &selected[*segment];
    }
    selected
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| test_failure("expected JSON string"))
}

fn required_binary(name: &str) -> TestResult<PathBuf> {
    let path = std::env::var_os(name).ok_or_else(|| test_failure(format!("{name} is absent")))?;
    let path = PathBuf::from(path);
    if !path.is_file() {
        return Err(test_failure(format!("{name} is not a file")));
    }
    Ok(path)
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn path_text(path: &Path) -> TestResult<&str> {
    path.to_str()
        .ok_or_else(|| test_failure("non-UTF-8 test path"))
}

fn write_regular(path: &Path, bytes: &[u8]) -> io::Result<()> {
    fs::write(path, bytes)
}

fn write_protected(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    File::open(path.parent().ok_or_else(|| io::Error::other("no parent"))?)?.sync_all()
}

struct ServerProcess {
    stdin: Option<ChildStdin>,
    child: Option<Child>,
    ready: Receiver<io::Result<String>>,
    stdout: Option<JoinHandle<io::Result<usize>>>,
    stderr: Option<JoinHandle<io::Result<usize>>>,
}

impl ServerProcess {
    fn spawn(
        binary: &Path,
        database: &Path,
        backup_root: &Path,
        capability_keys: &Path,
        idempotency_keys: &Path,
    ) -> io::Result<Self> {
        let mut child = Command::new(binary)
            .arg("--database")
            .arg(database)
            .arg("--backup-root")
            .arg(backup_root)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--environment")
            .arg(ENVIRONMENT)
            .arg("--audience")
            .arg(AUDIENCE)
            .arg("--capability-keys")
            .arg(capability_keys)
            .arg("--idempotency-keys")
            .arg(idempotency_keys)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("stderr"))?;
        let (sender, ready) = mpsc::sync_channel(1);
        let stdout = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let read = reader.read_line(&mut line);
            let _ = sender.send(read.map(|_| line.trim_end().to_owned()));
            drain(reader)
        });
        let stderr = thread::spawn(move || drain(stderr));
        Ok(Self {
            stdin: Some(stdin),
            child: Some(child),
            ready,
            stdout: Some(stdout),
            stderr: Some(stderr),
        })
    }

    fn ready_address(&self) -> TestResult<SocketAddr> {
        let line = self
            .ready
            .recv_timeout(START_TIMEOUT)
            .map_err(|_| test_failure("riffdbd readiness timed out"))??;
        let address: SocketAddr = line
            .strip_prefix(READY_PREFIX)
            .ok_or_else(|| test_failure("invalid readiness line"))?
            .parse()?;
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(test_failure("non-loopback readiness address"));
        }
        Ok(address)
    }

    fn stop(&mut self) -> TestResult<()> {
        let mut stdin = self
            .stdin
            .take()
            .ok_or_else(|| test_failure("missing stdin"))?;
        stdin.write_all(SHUTDOWN)?;
        stdin.flush()?;
        drop(stdin);
        let mut child = self
            .child
            .take()
            .ok_or_else(|| test_failure("missing child"))?;
        let (sender, receiver) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let _ = sender.send(child.wait());
        });
        let status = match receiver.recv_timeout(STOP_TIMEOUT) {
            Ok(status) => status?,
            Err(RecvTimeoutError::Timeout) => return Err(test_failure("riffdbd stop timed out")),
            Err(RecvTimeoutError::Disconnected) => {
                return Err(test_failure("riffdbd reaper disconnected"));
            }
        };
        self.join_output()?;
        if !status.success() {
            return Err(test_failure("riffdbd exited unsuccessfully"));
        }
        Ok(())
    }

    fn join_output(&mut self) -> TestResult<()> {
        for thread in [&mut self.stdout, &mut self.stderr] {
            let drained = thread
                .take()
                .ok_or_else(|| test_failure("missing output thread"))?
                .join()
                .map_err(|_| test_failure("output thread panicked"))??;
            if drained > MAX_CLI_OUTPUT {
                return Err(test_failure(
                    "riffdbd diagnostic output exceeded test bound",
                ));
            }
        }
        Ok(())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stdin.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.join_output();
    }
}

fn drain(mut reader: impl Read) -> io::Result<usize> {
    let mut total = 0_usize;
    let mut buffer = [0_u8; 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            return Ok(total);
        }
        total = total.saturating_add(count);
    }
}

struct TemporaryDirectory {
    directory: tempfile::TempDir,
}

impl TemporaryDirectory {
    fn new() -> io::Result<Self> {
        Ok(Self {
            directory: tempfile::TempDir::with_prefix("riffdb-wp150-")?,
        })
    }

    fn path(&self) -> &Path {
        self.directory.path()
    }
}

fn test_failure(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

#[test]
fn process_test_source_never_passes_a_database_path_to_the_cli() {
    let source = include_str!("public_process.rs");
    let cli_launcher = source
        .split("    fn cli(&self")
        .nth(1)
        .and_then(|tail| tail.split("    fn stop(&mut self").next())
        .expect("CLI launcher source");
    assert!(!cli_launcher.contains("--database"));
    assert!(!cli_launcher.contains("database"));
    assert!(cli_launcher.contains("CARGO_BIN_EXE_riffdb"));
}

#[test]
fn process_server_uses_an_explicit_backup_root() {
    let source = include_str!("public_process.rs");
    assert!(source.contains(".arg(\"--backup-root\")"));
}

#[test]
fn dev_run_forwards_a_go_package_only_when_the_public_option_is_present() -> TestResult<()> {
    let application = tempfile::TempDir::with_prefix("riffdb-dev-typescript-")?;
    fs::create_dir(application.path().join("scripts"))?;
    write_regular(&application.path().join("riffdb.application.json"), b"{}\n")?;
    write_regular(&application.path().join("package.json"), b"{}\n")?;
    let runner = application.path().join("scripts/riffdb-dev");
    write_regular(
        &runner,
        b"#!/bin/sh\nfor argument do\n  [ \"$argument\" != \"--go-runner-package\" ] || exit 97\ndone\nexit 0\n",
    )?;
    fs::set_permissions(&runner, fs::Permissions::from_mode(0o700))?;

    let default = Command::new(env!("CARGO_BIN_EXE_riffdb"))
        .current_dir(application.path())
        .args(["dev", "--run"])
        .output()?;
    assert!(
        default.status.success(),
        "default TypeScript run received the Go-only option: {}",
        String::from_utf8_lossy(&default.stderr)
    );

    let explicit = Command::new(env!("CARGO_BIN_EXE_riffdb"))
        .current_dir(application.path())
        .args(["dev", "--run", "--go-runner-package", "."])
        .output()?;
    assert_eq!(explicit.status.code(), Some(97));
    Ok(())
}

#[test]
fn fixture_key_encoder_is_stable() -> TestResult<()> {
    let encoded = budget_entity_key()?;
    assert_eq!(
        STANDARD.decode(encoded.as_bytes())?,
        [
            &[0x45, 0x01, 0, 0, 0, 1][..],
            &parse_uuid(ORGANIZATION_ID)?[..],
            &[0x80, 0, 0, 0, 0, 0, 0x07, 0xea][..],
        ]
        .concat()
    );
    Ok(())
}
