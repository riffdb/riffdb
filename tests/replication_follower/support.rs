//! Bounded process harness and legitimate source authority fixture setup.
use riffdb_auth::*;
use riffdb_catalog::{CatalogHistoryOutcome, validate_catalog_history};
use riffdb_client_rust::{BearerCredential, CallMetadata, RiffDbClient, v1};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};
use riffdb_storage_api::*;
use riffdb_storage_redb::{RedbOperationalPorts, RedbStore};
use riffdb_testkit::scratch::ScratchDir;
use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};
use riffdb_types::*;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::TcpListener;
use std::num::{NonZeroU16, NonZeroU32};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS: &[u8] = b"riffdb-idempotency-digest-keys-v1\n1:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const TIMEOUT: Duration = Duration::from_secs(30);
const AUDIENCE: &str = "replication-process-test";
const ENVIRONMENT: &str = "replication-test";

pub(super) struct Fixture {
    root: ScratchDir,
    primary: u16,
    follower: u16,
}

impl Fixture {
    pub(super) fn new() -> Self {
        let root = ScratchDir::new("replication-process").unwrap();
        // Reserve both while held so they cannot select the same ephemeral port.
        let primary = TcpListener::bind("127.0.0.1:0").unwrap();
        let follower = TcpListener::bind("127.0.0.1:0").unwrap();
        let fixture = Self {
            root,
            primary: primary.local_addr().unwrap().port(),
            follower: follower.local_addr().unwrap().port(),
        };
        for (name, bytes) in [
            ("capability.keys", CAPABILITY_KEYS),
            ("idempotency.keys", IDEMPOTENCY_KEYS),
            (
                "tls.key",
                include_bytes!("../../crates/riffdb-server/tests/fixtures/localhost-key.pem")
                    .as_slice(),
            ),
            (
                "tls.crt",
                include_bytes!("../../crates/riffdb-server/tests/fixtures/localhost-cert.pem")
                    .as_slice(),
            ),
            (
                "ca.crt",
                include_bytes!("../../crates/riffdb-server/tests/fixtures/test-ca.pem").as_slice(),
            ),
        ] {
            protected(&fixture.root.path().join(name), bytes);
        }
        for name in ["primary", "follower"] {
            fs::create_dir(fixture.root.path().join(format!("{name}-backups"))).unwrap();
            fs::write(fixture.config(name), fixture.document(name)).unwrap();
        }
        fixture
    }

    pub(super) fn database(&self, name: &str) -> PathBuf {
        self.root.path().join(format!("{name}.redb"))
    }

    pub(super) fn configure_document_projection(&self, name: &str) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(self.config(name))
            .unwrap();
        file.write_all(b"\n[[projections]]\nname = \"document_board\"\nentity = \"Document\"\nprojected_fields = [\"title\"]\norg_scope_field = \"organization_id\"\n").unwrap();
    }

    pub(super) fn projections(&self, name: &str) -> PathBuf {
        self.root.path().join(format!("{name}-projections"))
    }
    fn config(&self, name: &str) -> PathBuf {
        self.root.path().join(format!("{name}.toml"))
    }
    fn endpoint(&self, name: &str) -> String {
        format!(
            "https://127.0.0.1:{}",
            if name == "primary" {
                self.primary
            } else {
                self.follower
            }
        )
    }
    fn document(&self, name: &str) -> String {
        let root = self.root.path();
        let port = if name == "primary" {
            self.primary
        } else {
            self.follower
        };
        format!(
            r#"[server]
database = {database:?}
environment = {ENVIRONMENT:?}
audience = {AUDIENCE:?}
capability_keys = {capability:?}
idempotency_keys = {idempotency:?}
[server.application_listener]
mode = "direct_tls"
listen = "127.0.0.1:{port}"
public_endpoint = {endpoint:?}
certificate_chain = {certificate:?}
private_key = {key:?}
[maintenance]
backup_root = {backups:?}
projections_root = {projections:?}
"#,
            database = self.database(name),
            capability = root.join("capability.keys"),
            idempotency = root.join("idempotency.keys"),
            endpoint = self.endpoint(name),
            certificate = root.join("tls.crt"),
            key = root.join("tls.key"),
            backups = root.join(format!("{name}-backups")),
            projections = root.join(format!("{name}-projections"))
        )
    }

    pub(super) fn configure_follower(&self, lineage: ChangelogLineageV3, token: &str) {
        self.configure_follower_via(lineage, token, &self.endpoint("primary"));
    }

    pub(super) fn configure_follower_via(
        &self,
        lineage: ChangelogLineageV3,
        token: &str,
        endpoint: &str,
    ) {
        let root = self.root.path();
        protected(&root.join("replication.token"), token.as_bytes());
        let source = format!(
            r#"
[server.replication_source]
endpoint = {endpoint:?}
trust_root = {trust:?}
server_name = "127.0.0.1"
credential_file = {credential:?}
database = "default"
database_id = "{database}"
history_incarnation = {incarnation}
leadership_epoch = {epoch}
hold_id = "01010101010101010101010101010101"
"#,
            endpoint = endpoint,
            trust = root.join("ca.crt"),
            credential = root.join("replication.token"),
            database = lineage.database_id(),
            incarnation = lineage.history_incarnation(),
            epoch = lineage.leadership_epoch().get()
        );
        fs::write(self.config("follower"), self.document("follower") + &source).unwrap();
    }

    pub(super) fn start(&self, name: &str, mode: Option<&str>) -> ChildProcessController {
        let process = self.spawn(name, mode);
        process
            .wait_for_readiness("riffdbd-ready-v1\t", TIMEOUT)
            .unwrap();
        process
    }

    pub(super) fn start_wait_observed_follower(&self) -> ChildProcessController {
        #[cfg(feature = "test-fixtures")]
        {
            let spec = ChildProcessSpec::new(env!("CARGO_BIN_EXE_riffdbd-causal-wait-fixture"))
                .unwrap()
                .clear_environment()
                .arg("--config")
                .unwrap()
                .arg(self.config("follower"))
                .unwrap()
                .arg("--mode")
                .unwrap()
                .arg("follower")
                .unwrap();
            let process = ChildProcessController::spawn(&spec).unwrap();
            process
                .wait_for_readiness("riffdbd-ready-v1\t", TIMEOUT)
                .unwrap();
            process
        }
        #[cfg(not(feature = "test-fixtures"))]
        panic!("the causal-wait test requires test-fixtures");
    }

    pub(super) fn spawn(&self, name: &str, mode: Option<&str>) -> ChildProcessController {
        let mut spec = ChildProcessSpec::new(env!("CARGO_BIN_EXE_riffdbd"))
            .unwrap()
            .clear_environment()
            .arg("--config")
            .unwrap()
            .arg(self.config(name))
            .unwrap();
        if let Some(mode) = mode {
            spec = spec.arg("--mode").unwrap().arg(mode).unwrap();
        }
        ChildProcessController::spawn(&spec).unwrap()
    }

    pub(super) async fn client(&self, name: &str) -> RiffDbClient {
        RiffDbClient::connect_verified_tls(&self.tls_config(name))
            .await
            .unwrap()
    }

    pub(super) async fn application_client(&self) -> riffdb_client_rust::StableApplicationClient {
        self.application_client_for("primary").await
    }

    pub(super) async fn application_client_for(
        &self,
        name: &str,
    ) -> riffdb_client_rust::StableApplicationClient {
        riffdb_client_rust::StableApplicationClient::connect_verified_tls(&self.tls_config(name))
            .await
            .unwrap()
    }

    fn tls_config(&self, name: &str) -> TlsClientConfig {
        TlsClientConfig::new(
            CanonicalHttpsEndpoint::parse(&self.endpoint(name)).unwrap(),
            ProtectedFilePath::new(self.root.path().join("ca.crt")).unwrap(),
            TlsServerIdentity::parse("127.0.0.1").unwrap(),
            Duration::from_secs(5),
            TIMEOUT,
            NonZeroU32::MIN,
            NonZeroU32::MIN,
        )
        .unwrap()
    }

    pub(super) async fn raw_contract_client(
        &self,
    ) -> riffdb_api_grpc::generated::contract_service_client::ContractServiceClient<
        tonic::transport::Channel,
    > {
        riffdb_api_grpc::generated::contract_service_client::ContractServiceClient::new(
            self.channel("follower").await,
        )
    }

    pub(super) async fn channel(&self, name: &str) -> tonic::transport::Channel {
        use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
        Endpoint::from_shared(self.endpoint(name))
            .unwrap()
            .connect_timeout(Duration::from_secs(5))
            .timeout(TIMEOUT)
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("127.0.0.1")
                    .ca_certificate(Certificate::from_pem(
                        fs::read(self.root.path().join("ca.crt")).unwrap(),
                    )),
            )
            .unwrap()
            .connect()
            .await
            .unwrap()
    }
}

pub(super) fn stop(child: &mut ChildProcessController) {
    assert!(
        child
            .shutdown_cleanly(b"shutdown\n", TIMEOUT)
            .unwrap()
            .status
            .success()
    );
}

fn protected(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

pub(super) fn request_id(byte: u8) -> Vec<u8> {
    RequestId::from_unix_milliseconds_and_random(1000, [byte; 10])
        .unwrap()
        .as_bytes()
        .to_vec()
}
pub(super) fn capability_id(byte: u8) -> CapabilityId {
    CapabilityId::from_unix_milliseconds_and_random(1000, [byte; 10]).unwrap()
}

pub(super) fn now() -> Timestamp {
    Timestamp::new(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        0,
    )
    .unwrap()
}

pub(super) fn open_primary(path: &Path) -> RedbOperationalPorts {
    let key = DigestKeyId::new(1).unwrap();
    let inputs = StartupValidationInputs::new(
        now(),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(key)]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(key)]).unwrap(),
    );
    let mut session = RedbStore::open(path)
        .unwrap()
        .begin_structural_evidence(inputs)
        .unwrap();
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let end = loop {
        match session
            .read_structural_evidence(cursor, EvidencePageLimit::new(64).unwrap())
            .unwrap()
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty());
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (history, history_end) = validate_catalog_history(&mut session).unwrap().into_parts();
    assert!(matches!(history, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = session.finish(end, history_end).unwrap() else {
        panic!("unclean primary fixture")
    };
    opened
        .into_parts()
        .3
        .into_operational_after_catalog_validation()
        .unwrap()
}

pub(super) fn seed_primary(path: &Path) -> (ChangelogLineageV3, CallMetadata, String) {
    // Seed only the legitimate bootstrap transition on a stopped primary. All
    // replication capability creation and contract deployment use the public API.
    let mut ports = open_primary(path);
    let lineage = ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .authoritative_state_v3()
        .unwrap()
        .history()
        .lineage();
    let keys = CapabilityDigestKeyProvider::parse_document(CAPABILITY_KEYS).unwrap();
    let issued = issue_capability_token(&SystemEntropy, &keys).unwrap();
    let token = std::str::from_utf8(issued.text().expose_secret())
        .unwrap()
        .to_owned();
    let id = capability_id(1);
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(
            [
                CapabilityPermissionKindV1::AdministerCapabilities,
                CapabilityPermissionKindV1::DeployContract,
                CapabilityPermissionKindV1::ReadContract,
                CapabilityPermissionKindV1::ReadHealth,
                CapabilityPermissionKindV1::ReadStatistics,
            ]
            .into_iter()
            .map(|kind| CapabilityPermissionV1::unparameterized(kind).unwrap())
            .collect(),
        )
        .unwrap(),
        vec![],
        NonZeroU16::new(100).unwrap(),
        vec![],
    )
    .unwrap();
    let created = now();
    let intent = CapabilityBootstrapIntentV1::new(
        id,
        CapabilityRequestedRecordV1::new(
            lineage.database_id(),
            Environment::new(ENVIRONMENT).unwrap(),
            ActorId::new("replication-test-admin").unwrap(),
            ActorKind::Human,
            NonZeroU32::new(3600).unwrap(),
            vec![Audience::new(AUDIENCE).unwrap()],
            grant,
        )
        .unwrap(),
        BootstrapDigestCandidatesV1::new(vec![issued.digest()], issued.digest()).unwrap(),
        created,
        Timestamp::new(created.seconds() + 3600, 0).unwrap(),
        BootstrapServiceAuditStartV1::new(
            RequestId::from_bytes(request_id(1).try_into().unwrap()).unwrap(),
            created,
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(id)]).unwrap(),
            None,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        ports.bootstrap_capability(&intent).unwrap(),
        CapabilityBootstrapResult::BootstrapCreated { .. }
    ));
    (
        lineage,
        CallMetadata::authenticated(BearerCredential::new(&token).unwrap()),
        token,
    )
}

pub(super) async fn create_replication_capability(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
) -> String {
    let response = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: request_id(2),
                mode: v1::CapabilityCreateMode::Normal as i32,
                capability_id: capability_id(2).as_bytes().to_vec(),
                principal_id: "replication-test-follower".into(),
                actor_kind: v1::ActorKind::Service as i32,
                requested_lifetime_seconds: 3600,
                audiences: vec![AUDIENCE.into()],
                grant: Some(v1::CapabilityGrant {
                    tenant_scope: Some(v1::TenantScope {
                        scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                    }),
                    partition_scope: Some(v1::PartitionScope {
                        scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                    }),
                    permissions: vec![v1::CapabilityPermission {
                        permission: Some(
                            v1::capability_permission::Permission::ReplicateChangelog(v1::Unit {}),
                        ),
                    }],
                    max_scan_rows: 100,
                    ..Default::default()
                }),
            },
            metadata,
        )
        .await
        .unwrap();
    let Some(v1::create_capability_response::Result::Normal(normal)) = response.result else {
        panic!("normal creation expected")
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = normal.result else {
        panic!("fresh replication capability expected")
    };
    created.token
}
