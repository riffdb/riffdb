#![forbid(unsafe_code)]
//! Process-level custody of local bootstrap material: no local refusal may destroy
//! existing outputs, and an uncertain post-send outcome must leave the exact
//! bootstrap document needed for a replay.

use std::fs;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use riffdb_auth::bootstrap_secret::load_bootstrap_credential_file;
use serde_json::Value;

const SENTINEL_BOOTSTRAP: &[u8] = b"sentinel bootstrap document\n";
const SENTINEL_BEARER: &[u8] = b"sentinel bearer\n";

fn riffdb(root: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_riffdb"))
        .env_clear()
        .env("TMPDIR", root)
        .current_dir(root)
        .args(arguments)
        .output()
        .expect("spawn riffdb")
}

fn write_protected(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).expect("write sentinel");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("protect sentinel");
}

fn text(path: &Path) -> &str {
    path.to_str().expect("UTF-8 temporary path")
}

fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        panic!(
            "stdout is not one JSON document: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// A closed loopback port: nothing listens once the probe listener is dropped.
fn refused_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = listener.local_addr().expect("probe address").port();
    drop(listener);
    format!("http://127.0.0.1:{port}")
}

fn assert_sentinels_intact(bootstrap: &Path, bearer: &Path) {
    assert_eq!(
        fs::read(bootstrap).ok().as_deref(),
        Some(SENTINEL_BOOTSTRAP),
        "existing bootstrap output was destroyed"
    );
    assert_eq!(
        fs::read(bearer).ok().as_deref(),
        Some(SENTINEL_BEARER),
        "existing bearer output was destroyed"
    );
}

#[test]
fn forced_bootstrap_with_invalid_request_preserves_existing_outputs() {
    let root = tempfile::TempDir::new().expect("temporary root");
    let bootstrap = root.path().join("bootstrap");
    let bearer = root.path().join("bearer");
    write_protected(&bootstrap, SENTINEL_BOOTSTRAP);
    write_protected(&bearer, SENTINEL_BEARER);
    let endpoint = refused_endpoint();
    let output = riffdb(
        root.path(),
        &[
            "--endpoint",
            &endpoint,
            "--database",
            "review",
            "--output",
            "json",
            "capability",
            "bootstrap",
            "--operator",
            "--principal",
            "",
            "--generate",
            text(&bootstrap),
            "--bearer-output",
            text(&bearer),
            "--force",
        ],
    );
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert_eq!(json(&output)["error"]["code"], "input_invalid");
    assert_sentinels_intact(&bootstrap, &bearer);
}

#[test]
fn forced_bootstrap_that_cannot_connect_preserves_existing_outputs() {
    let root = tempfile::TempDir::new().expect("temporary root");
    let bootstrap = root.path().join("bootstrap");
    let bearer = root.path().join("bearer");
    write_protected(&bootstrap, SENTINEL_BOOTSTRAP);
    write_protected(&bearer, SENTINEL_BEARER);
    let endpoint = refused_endpoint();
    let output = riffdb(
        root.path(),
        &[
            "--endpoint",
            &endpoint,
            "--database",
            "review",
            "--output",
            "json",
            "capability",
            "bootstrap",
            "--operator",
            "--generate",
            text(&bootstrap),
            "--bearer-output",
            text(&bearer),
            "--force",
        ],
    );
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_ne!(json(&output)["error"]["code"], "bootstrap_outcome_unknown");
    assert_sentinels_intact(&bootstrap, &bearer);
}

/// What the fake peer does once a request stream has been fully received.
#[derive(Clone, Copy)]
enum AfterRequest {
    /// Close the connection without any response.
    Drop,
    /// Answer with bytes that are not a valid HTTP/2 frame sequence, then close.
    Garble,
}

/// A minimal HTTP/2 peer that completes the connection handshake, reads one full
/// request stream, then drops or garbles the response: the request was sent and
/// its outcome is unknowable to the client.
struct DroppingEndpoint {
    endpoint: String,
    requests: Arc<AtomicUsize>,
}

impl DroppingEndpoint {
    fn start() -> Self {
        Self::start_with(AfterRequest::Drop)
    }

    fn start_with(after: AfterRequest) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake endpoint");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&requests);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let counter = Arc::clone(&counter);
                std::thread::spawn(move || {
                    let _ = serve_until_request(stream, &counter, after);
                });
            }
        });
        Self { endpoint, requests }
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

fn serve_until_request(
    mut stream: TcpStream,
    requests: &AtomicUsize,
    after: AfterRequest,
) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    // Server connection preface: an empty SETTINGS frame.
    stream.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0])?;
    let mut preface = [0_u8; 24];
    stream.read_exact(&mut preface)?;
    if &preface != b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" {
        return Ok(());
    }
    loop {
        let mut header = [0_u8; 9];
        stream.read_exact(&mut header)?;
        let length =
            usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
        let (kind, flags) = (header[3], header[4]);
        let stream_id = u32::from_be_bytes([header[5] & 0x7f, header[6], header[7], header[8]]);
        let mut payload = vec![0_u8; length];
        stream.read_exact(&mut payload)?;
        match kind {
            // SETTINGS without ACK: acknowledge it.
            4 if flags & 1 == 0 => stream.write_all(&[0, 0, 0, 4, 1, 0, 0, 0, 0])?,
            // PING without ACK: echo it.
            6 if flags & 1 == 0 => {
                stream.write_all(&[0, 0, 8, 6, 1, 0, 0, 0, 0])?;
                stream.write_all(&payload)?;
            }
            // DATA or HEADERS closing a request stream: the request is fully sent.
            0 | 1 if stream_id != 0 && flags & 1 == 1 => {
                requests.fetch_add(1, Ordering::SeqCst);
                if matches!(after, AfterRequest::Garble) {
                    // A DATA frame on stream 0 is a connection-level protocol error.
                    let _ = stream.write_all(&[0, 0, 4, 0, 0, 0, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef]);
                }
                let _ = stream.shutdown(Shutdown::Both);
                return Ok(());
            }
            _ => {}
        }
    }
}

fn retained_document(directory: &Path, capability_id: &str) -> PathBuf {
    directory.join(format!(".riffdb-bootstrap-{capability_id}.credential"))
}

fn assert_retained_recovery_document(path: &Path, capability_id: &str) {
    let metadata = fs::symlink_metadata(path).unwrap_or_else(|_| {
        panic!(
            "uncertain bootstrap did not retain its recovery document at {}",
            path.display()
        )
    });
    assert!(metadata.file_type().is_file());
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    let credential = load_bootstrap_credential_file(path).expect("retained document is loadable");
    assert_eq!(credential.capability_id().to_string(), capability_id);
}

fn uncertain_bootstrap(root: &Path, endpoint: &str, mode: &str, extra: &[&str]) -> Output {
    let mut arguments = vec![
        "--endpoint",
        endpoint,
        "--database",
        "review",
        "--max-attempts",
        "1",
        "--output",
        mode,
        "capability",
        "bootstrap",
        "--operator",
    ];
    arguments.extend_from_slice(extra);
    riffdb(root, &arguments)
}

#[test]
fn uncertain_default_operator_bootstrap_retains_its_recovery_document() {
    let root = tempfile::TempDir::new().expect("temporary root");
    let fake = DroppingEndpoint::start();
    let output = uncertain_bootstrap(root.path(), &fake.endpoint, "json", &[]);
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    assert!(
        fake.requests() >= 1,
        "the request never reached the endpoint"
    );
    let value = json(&output);
    assert_eq!(value["error"]["code"], "bootstrap_outcome_unknown");
    assert_eq!(
        value["error"]["recovery_action"],
        "retry_with_retained_bootstrap_credential"
    );
    let capability_id = value["error"]["capability_id"]
        .as_str()
        .expect("uncertain bootstrap names its capability")
        .to_owned();
    let document = retained_document(root.path(), &capability_id);
    assert_retained_recovery_document(&document, &capability_id);

    // The retained document is exactly what a replay consumes; a replay that is
    // itself uncertain keeps the caller's document and reports the same identity.
    let replay = uncertain_bootstrap(
        root.path(),
        &fake.endpoint,
        "json",
        &["--bootstrap-file", text(&document)],
    );
    assert_eq!(replay.status.code(), Some(3), "{replay:?}");
    assert_eq!(
        json(&replay)["error"]["capability_id"],
        capability_id.as_str()
    );
    assert_retained_recovery_document(&document, &capability_id);
}

#[test]
fn uncertain_operator_bootstrap_with_bearer_output_retains_both_files() {
    let root = tempfile::TempDir::new().expect("temporary root");
    let outputs = root.path().join("outputs");
    fs::create_dir(&outputs).expect("output directory");
    let bearer = outputs.join("operator.bearer");
    let fake = DroppingEndpoint::start();
    let output = uncertain_bootstrap(
        root.path(),
        &fake.endpoint,
        "human",
        &["--bearer-output", text(&bearer)],
    );
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    assert!(
        fake.requests() >= 1,
        "the request never reached the endpoint"
    );
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 human output");
    let document = fs::read_dir(&outputs)
        .expect("list outputs")
        .map(|entry| entry.expect("entry").path())
        .find(|path| path != &bearer)
        .expect("uncertain bootstrap retained a recovery document beside the bearer");
    let name = document
        .file_name()
        .and_then(|name| name.to_str())
        .expect("name");
    let capability_id = name
        .strip_prefix(".riffdb-bootstrap-")
        .and_then(|rest| rest.strip_suffix(".credential"))
        .expect("documented recovery document name");
    assert_retained_recovery_document(&document, capability_id);
    assert!(
        stderr.contains("bootstrap_outcome_unknown") && stderr.contains(text(&document)),
        "human recovery guidance does not name the retained document: {stderr}"
    );
    assert!(stderr.contains("--bootstrap-file"), "{stderr}");
    let credential = load_bootstrap_credential_file(&document).expect("document");
    assert_eq!(
        fs::read(&bearer).expect("bearer retained"),
        credential.token().expose_secret()
    );
}

#[test]
fn uncertain_bootstrap_json_v2_carries_a_path_free_recovery_diagnostic() {
    let root = tempfile::TempDir::new().expect("temporary root");
    let fake = DroppingEndpoint::start();
    let output = uncertain_bootstrap(root.path(), &fake.endpoint, "json-v2", &[]);
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    let value = json(&output);
    assert_eq!(value["schema"], "riffdb.cli.output/v2");
    assert_eq!(value["error"]["code"], "bootstrap_outcome_unknown");
    assert_eq!(value["diagnostics"][0]["component"], "bootstrap_recovery");
    assert_eq!(value["diagnostics"][0]["cause"], "outcome_unknown");
    let raw = String::from_utf8_lossy(&output.stdout);
    assert!(!raw.contains(text(root.path())), "v2 leaked a path: {raw}");
    let capability_id = value["error"]["capability_id"].as_str().expect("identity");
    assert_retained_recovery_document(
        &retained_document(root.path(), capability_id),
        capability_id,
    );
}

#[test]
fn bootstrap_retention_refusal_is_actionable_in_every_output_mode() {
    let root = tempfile::TempDir::new().expect("temporary root");
    let bootstrap = root.path().join("bootstrap");
    write_protected(&bootstrap, SENTINEL_BOOTSTRAP);
    let endpoint = refused_endpoint();
    let invoke = |mode: &str| {
        riffdb(
            root.path(),
            &[
                "--endpoint",
                &endpoint,
                "--database",
                "review",
                "--output",
                mode,
                "capability",
                "bootstrap",
                "--operator",
                "--generate",
                text(&bootstrap),
            ],
        )
    };

    let human = invoke("human");
    assert_eq!(human.status.code(), Some(2));
    assert!(human.stdout.is_empty());
    let stderr = String::from_utf8(human.stderr).expect("UTF-8");
    assert!(stderr.contains("credential_retention_failed"), "{stderr}");
    assert!(stderr.contains("--force"), "{stderr}");
    assert!(stderr.contains("component=credential_output"), "{stderr}");

    // V1 is closed: the frozen refusal bytes, nothing more.
    let v1 = invoke("json");
    assert_eq!(v1.status.code(), Some(2));
    assert!(v1.stderr.is_empty());
    assert_eq!(
        v1.stdout,
        include_bytes!("../fixtures/output-v1/capability.bootstrap.local_error.jsonl")
    );

    let v2 = invoke("json-v2");
    assert_eq!(v2.status.code(), Some(2));
    assert!(v2.stderr.is_empty());
    assert_eq!(
        v2.stdout,
        include_bytes!(
            "../../../fixtures/diagnostics/cli-output-v2/capability.bootstrap.credential_retention_failed.jsonl"
        )
    );
    assert_eq!(fs::read(&bootstrap).expect("sentinel"), SENTINEL_BOOTSTRAP);
}

#[test]
fn garbled_post_send_response_still_retains_the_recovery_document() {
    let root = tempfile::TempDir::new().expect("temporary root");
    let fake = DroppingEndpoint::start_with(AfterRequest::Garble);
    let output = uncertain_bootstrap(root.path(), &fake.endpoint, "json", &[]);
    assert!(
        fake.requests() >= 1,
        "the request never reached the endpoint"
    );
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    let value = json(&output);
    assert_eq!(value["error"]["code"], "bootstrap_outcome_unknown");
    let capability_id = value["error"]["capability_id"].as_str().expect("identity");
    assert_retained_recovery_document(
        &retained_document(root.path(), capability_id),
        capability_id,
    );
}
