#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Real-process direct-TLS and downgrade acceptance for WP-553.

use std::error::Error;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use riffdb_client_rust::{CallMetadata, DatabaseAlias, RiffDbClient, v1};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};
use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};

const CHILD_MODE: &str = "RIFFDB_REMOTE_INGRESS_CHILD";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const START_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(30);
const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS: &[u8] = b"riffdb-idempotency-digest-keys-v1\n1:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> ExitCode {
    if std::env::var_os(CHILD_MODE).is_some() {
        return riffdb_server::riffdbd_main();
    }
    match direct_tls_real_process_acceptance() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("remote ingress acceptance failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn direct_tls_real_process_acceptance() -> TestResult<()> {
    eprintln!("remote-ingress phase=fixture");
    let root = TestRoot::new()?;
    let port = reserve_loopback_port()?;
    let certificate = root.write_protected(
        "server.pem",
        include_bytes!("../crates/riffdb-server/tests/fixtures/localhost-cert.pem"),
        0o444,
    )?;
    let private_key = root.write_protected(
        "server.key",
        include_bytes!("../crates/riffdb-server/tests/fixtures/localhost-key.pem"),
        0o600,
    )?;
    let trust_root = root.write_protected(
        "ca.pem",
        include_bytes!("../crates/riffdb-server/tests/fixtures/test-ca.pem"),
        0o444,
    )?;
    let capability_keys = root.write_protected("capability.keys", CAPABILITY_KEYS, 0o600)?;
    let idempotency_keys = root.write_protected("idempotency.keys", IDEMPOTENCY_KEYS, 0o600)?;
    let backup_root = root.path().join("backups");
    fs::create_dir(&backup_root)?;
    fs::set_permissions(&backup_root, fs::Permissions::from_mode(0o700))?;
    let database = root.path().join("riffdb.redb");
    let mismatched = root.path().join("mismatched.toml");
    write_server_config(
        &mismatched,
        ServerConfigPaths {
            database: &database,
            capability_keys: &capability_keys,
            idempotency_keys: &idempotency_keys,
            certificate: &certificate,
            private_key: &private_key,
            backup_root: &backup_root,
        },
        port,
        "127.0.0.2",
    )?;
    let mut rejected = ChildProcessController::spawn(&child_specification(mismatched)?)?;
    eprintln!("remote-ingress phase=prebind-rejection");
    let rejected_exit = rejected.wait_for_exit(START_TIMEOUT)?;
    if rejected_exit.status.success() || rejected_exit.output.stdout_bytes != 0 {
        return Err("mismatched certificate identity did not fail before readiness".into());
    }
    let pre_bind_probe = TcpListener::bind(("127.0.0.1", port))?;
    drop(pre_bind_probe);

    let config = root.path().join("riffdbd.toml");
    write_server_config(
        &config,
        ServerConfigPaths {
            database: &database,
            capability_keys: &capability_keys,
            idempotency_keys: &idempotency_keys,
            certificate: &certificate,
            private_key: &private_key,
            backup_root: &backup_root,
        },
        port,
        "127.0.0.1",
    )?;

    let specification = child_specification(config)?;
    let mut process = ChildProcessController::spawn(&specification)?;
    eprintln!("remote-ingress phase=start-valid");
    let readiness = process.wait_for_readiness(READY_PREFIX, START_TIMEOUT)?;
    if readiness != format!("{READY_PREFIX}127.0.0.1:{port}") {
        return Err("direct-TLS daemon published an unexpected endpoint".into());
    }
    eprintln!("remote-ingress phase=downgrade");
    assert_cleartext_downgrade_rejected(port)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let tls = tls_client_config(port, trust_root.clone())?;
        let mut client = connect_after_permit_release(&tls).await?;
        eprintln!("remote-ingress phase=verified");
        let health = client
            .health(
                v1::HealthRequest { request_id: None },
                &CallMetadata::default(),
            )
            .await?;
        let Some(v1::health_response::Result::PreBootstrap(liveness)) = health.result else {
            return Err("verified TLS did not reach the payload-free liveness service".into());
        };
        if liveness.lifecycle != v1::PreBootstrapLifecycle::Unspecified as i32
            || !liveness.liveness
            || liveness.readiness
            || !health.database_alias.is_empty()
            || !health.authentication_audience.is_empty()
        {
            return Err("unauthenticated TLS liveness disclosed readiness identity".into());
        }
        let selected = CallMetadata::default().with_database(DatabaseAlias::default_alias());
        if client
            .health(v1::HealthRequest { request_id: None }, &selected)
            .await
            .is_ok()
        {
            return Err("unauthenticated liveness accepted a database selector".into());
        }

        if matches!(
            tokio::time::timeout(
                Duration::from_secs(4),
                RiffDbClient::connect_verified_tls(&tls)
            )
            .await,
            Ok(Ok(_))
        ) {
            return Err("connection ceiling admitted a second live connection".into());
        }
        eprintln!("remote-ingress phase=saturated");
        drop(client);

        let invalid = root.path().join("invalid-server.pem");
        fs::write(&invalid, b"not a certificate\n")?;
        fs::set_permissions(&invalid, fs::Permissions::from_mode(0o444))?;
        fs::rename(&invalid, &certificate)?;
        let retained = connect_after_permit_release(&tls).await?;
        eprintln!("remote-ingress phase=retained-last-good");
        drop(retained);

        let restored = root.path().join("restored-server.pem");
        fs::write(
            &restored,
            include_bytes!("../crates/riffdb-server/tests/fixtures/localhost-cert.pem"),
        )?;
        fs::set_permissions(&restored, fs::Permissions::from_mode(0o444))?;
        fs::rename(&restored, &certificate)?;
        let restored = connect_after_permit_release(&tls).await?;
        eprintln!("remote-ingress phase=restored");
        drop(restored);
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    })?;

    eprintln!("remote-ingress phase=shutdown");
    let exit = process.shutdown_cleanly(b"shutdown\n", STOP_TIMEOUT)?;
    if !exit.status.success() {
        return Err("direct-TLS daemon did not drain cleanly".into());
    }
    eprintln!("remote-ingress phase=complete");
    Ok(())
}

fn assert_cleartext_downgrade_rejected(port: u16) -> TestResult<()> {
    let address = format!("127.0.0.1:{port}").parse()?;
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(4)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")?;
    stream.shutdown(Shutdown::Write)?;
    let mut response = [0_u8; 32];
    match stream.read(&mut response) {
        Ok(0) => Ok(()),
        Ok(read) if response[..read].first() == Some(&21) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::UnexpectedEof
            ) =>
        {
            Ok(())
        }
        Ok(_) | Err(_) => Err("direct-TLS listener emitted a cleartext protocol response".into()),
    }
}

async fn connect_after_permit_release(
    config: &TlsClientConfig,
) -> Result<RiffDbClient, Box<dyn Error + Send + Sync>> {
    for _ in 0..4 {
        match tokio::time::timeout(
            Duration::from_secs(4),
            RiffDbClient::connect_verified_tls(config),
        )
        .await
        {
            Ok(Ok(client)) => return Ok(client),
            Ok(Err(_)) | Err(_) => tokio::task::yield_now().await,
        }
    }
    Err("connection permit was not released within the bounded retry budget".into())
}

struct ServerConfigPaths<'a> {
    database: &'a Path,
    capability_keys: &'a Path,
    idempotency_keys: &'a Path,
    certificate: &'a Path,
    private_key: &'a Path,
    backup_root: &'a Path,
}

fn write_server_config(
    destination: &Path,
    paths: ServerConfigPaths<'_>,
    port: u16,
    public_identity: &str,
) -> TestResult<()> {
    fs::write(
        destination,
        format!(
            "[server]\n\
             database = {database:?}\n\
             environment = \"remote-ingress-test\"\n\
             audience = \"riffdb-application-test\"\n\
             capability_keys = {capability_keys:?}\n\
             idempotency_keys = {idempotency_keys:?}\n\
             \n\
             [server.application_listener]\n\
             mode = \"direct_tls\"\n\
             listen = \"127.0.0.1:{port}\"\n\
             public_endpoint = \"https://{public_identity}:{port}\"\n\
             certificate_chain = {certificate:?}\n\
             private_key = {private_key:?}\n\
             \n\
             [server.application_listener.bounds]\n\
             max_connections = 1\n\
             handshake_timeout_seconds = 2\n\
             \n\
             [maintenance]\n\
             backup_root = {backup_root:?}\n",
            database = paths.database,
            capability_keys = paths.capability_keys,
            idempotency_keys = paths.idempotency_keys,
            certificate = paths.certificate,
            private_key = paths.private_key,
            backup_root = paths.backup_root,
        ),
    )?;
    Ok(())
}

fn child_specification(config: PathBuf) -> TestResult<ChildProcessSpec> {
    Ok(ChildProcessSpec::new(std::env::current_exe()?)?
        .clear_environment()
        .arg("--config")?
        .arg(config.into_os_string())?
        .env(CHILD_MODE, "1")?)
}

fn tls_client_config(port: u16, trust_root: PathBuf) -> TestResult<TlsClientConfig> {
    Ok(TlsClientConfig::new(
        CanonicalHttpsEndpoint::parse(&format!("https://127.0.0.1:{port}"))?,
        ProtectedFilePath::new(trust_root)?,
        TlsServerIdentity::parse("127.0.0.1")?,
        Duration::from_secs(1),
        Duration::from_secs(30),
        NonZeroU32::new(1).ok_or("nonzero pool bound")?,
        NonZeroU32::new(64).ok_or("nonzero stream bound")?,
    )?)
}

fn reserve_loopback_port() -> TestResult<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> TestResult<Self> {
        let unique = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::current_dir()?
            .join("target")
            .join("remote-ingress-tests")
            .join(format!("{}-{unique}", std::process::id()));
        fs::create_dir_all(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        Ok(Self(root))
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn write_protected(&self, name: &str, bytes: &[u8], mode: u32) -> TestResult<PathBuf> {
        let path = self.0.join(name);
        fs::write(&path, bytes)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
        Ok(path)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
