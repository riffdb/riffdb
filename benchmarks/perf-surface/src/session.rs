//! Bootstrapping one daemon to the point where the measured command can run.
//!
//! Every step here is the same public call an application makes: create the
//! bootstrap capability, deploy the contract, mint a capability that may invoke
//! the two commands, and execute them by name. Nothing reaches behind the
//! server's public surface, so what the harness measures is what a deployment
//! would pay.

use std::collections::BTreeMap;
use std::path::Path;

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_client_rust::{
    ApplicationCommand, ApplicationUuid, ApplicationValue, AttemptBudget, BearerCredential,
    BootstrapCallMetadata, BootstrapCredential as TransportBootstrapCredential, CallMetadata,
    RiffDbClient, StableApplicationClient, generate_request_id, v1,
};

/// Connect and per-call ceiling. Long enough for a cold start, short enough
/// that a hung daemon fails the run instead of stalling it.
const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

use tonic::transport::Endpoint;

use crate::daemon::Daemon;

const AUDIENCE: &str = "riffdb-grpc-loopback";
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;

/// Failures while bootstrapping or driving a session.
#[derive(Debug)]
pub enum SessionError {
    /// Generating or reading the bootstrap credential failed.
    Bootstrap(String),
    /// A remote call failed or returned an unexpected shape.
    Rpc(String),
    /// Connecting to the daemon failed.
    Connect(String),
    /// A command input could not be built.
    Input(String),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bootstrap(detail) => write!(formatter, "bootstrap failed: {detail}"),
            Self::Rpc(detail) => write!(formatter, "rpc failed: {detail}"),
            Self::Connect(detail) => write!(formatter, "connect failed: {detail}"),
            Self::Input(detail) => write!(formatter, "command input invalid: {detail}"),
        }
    }
}

impl std::error::Error for SessionError {}

fn request_id_bytes() -> Result<Vec<u8>, SessionError> {
    generate_request_id()
        .map(|id| id.into_bytes().to_vec())
        .map_err(|_| SessionError::Bootstrap("request id".to_owned()))
}

/// Creates the bootstrap capability and deploys `source`, returning a bearer
/// token that may deploy and administer capabilities.
pub async fn bootstrap_and_deploy(
    endpoint: &str,
    credential_path: &Path,
    source: &str,
) -> Result<String, SessionError> {
    let generated = generate_bootstrap_credential(1_700_000_000_000, &SystemEntropy)
        .map_err(|_| SessionError::Bootstrap("generate".to_owned()))?;
    crate::daemon::write_protected(credential_path, generated.render_document().expose_secret())
        .map_err(|error| SessionError::Bootstrap(error.to_string()))?;
    drop(generated);
    let retained = load_bootstrap_credential_file(credential_path)
        .map_err(|_| SessionError::Bootstrap("load".to_owned()))?;

    let mut client = RiffDbClient::connect(transport_endpoint(endpoint)?)
        .await
        .map_err(|error| SessionError::Connect(format!("{error:?}")))?;

    let token = std::str::from_utf8(retained.token().expose_secret())
        .map_err(|_| SessionError::Bootstrap("token utf8".to_owned()))?
        .to_owned();
    let bootstrap_metadata = BootstrapCallMetadata::new(
        TransportBootstrapCredential::new(&token)
            .map_err(|_| SessionError::Bootstrap("transport credential".to_owned()))?,
    );
    let authenticated = CallMetadata::authenticated(
        BearerCredential::new(&token)
            .map_err(|_| SessionError::Bootstrap("bearer".to_owned()))?,
    );

    let created = client
        .create_bootstrap_capability(bootstrap_request(&retained)?, &bootstrap_metadata)
        .await
        .map_err(|error| SessionError::Rpc(format!("create_bootstrap_capability: {error:?}")))?;
    let Some(v1::create_capability_response::Result::Bootstrap(result)) = created.result else {
        return Err(SessionError::Rpc("bootstrap response shape".to_owned()));
    };
    let Some(v1::bootstrap_create_capability_result::Result::Created(_)) = result.result else {
        return Err(SessionError::Rpc("capability not newly created".to_owned()));
    };

    let deployed = client
        .deploy_contract(
            v1::DeployContractRequest {
                request_id: request_id_bytes()?,
                source: source.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            &authenticated,
        )
        .await
        .map_err(|error| SessionError::Rpc(format!("deploy_contract: {error:?}")))?;
    let Some(v1::deploy_contract_response::Result::Activated(_)) = deployed.result else {
        return Err(SessionError::Rpc(format!(
            "deploy_contract did not activate: {deployed:?}"
        )));
    };

    Ok(token)
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
) -> Result<v1::CreateCapabilityRequest, SessionError> {
    use v1::capability_permission::Permission;
    Ok(v1::CreateCapabilityRequest {
        request_id: request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "perf-surface-bootstrap".to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
        audiences: vec![AUDIENCE.to_owned()],
        grant: Some(v1::CapabilityGrant {
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            partition_scope: Some(v1::PartitionScope {
                scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
            }),
            permissions: vec![
                v1::CapabilityPermission {
                    permission: Some(Permission::DeployContract(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
                },
            ],
            field_visibility: Vec::new(),
            max_scan_rows: 1,
            approval_required: Vec::new(),
            row_policy: None,
            export: None,
            reimport: None,
            vector_inspection: None,
        }),
    })
}

/// One command invocation the harness measures.
#[must_use]
pub fn publish_document_input(
    workspace: [u8; 16],
    document: [u8; 16],
    idempotency_key: String,
    title: String,
    body: String,
    size_bytes: i64,
) -> BTreeMap<String, ApplicationValue> {
    let mut input = BTreeMap::new();
    input.insert(
        "idempotency_key".to_owned(),
        ApplicationValue::String(idempotency_key),
    );
    input.insert("workspace_id".to_owned(), ApplicationValue::Uuid(ApplicationUuid::from_bytes(workspace)));
    input.insert("document_id".to_owned(), ApplicationValue::Uuid(ApplicationUuid::from_bytes(document)));
    input.insert("title".to_owned(), ApplicationValue::String(title));
    input.insert("body".to_owned(), ApplicationValue::String(body));
    input.insert("size_bytes".to_owned(), ApplicationValue::I64(size_bytes));
    input
}

/// Builds the named command the measurement drives.
pub fn command(
    name: &str,
    input: BTreeMap<String, ApplicationValue>,
) -> Result<ApplicationCommand, SessionError> {
    ApplicationCommand::new(name, Some(1), input)
        .map_err(|error| SessionError::Input(format!("{error:?}")))
}

/// Connects an application client bound to `token`.
pub async fn application_client(
    endpoint: &str,
) -> Result<StableApplicationClient, SessionError> {
    StableApplicationClient::connect(transport_endpoint(endpoint)?)
        .await
        .map_err(|error| SessionError::Connect(format!("{error:?}")))
}

fn transport_endpoint(endpoint: &str) -> Result<Endpoint, SessionError> {
    Endpoint::from_shared(endpoint.to_owned())
        .map(|endpoint| endpoint.connect_timeout(RPC_TIMEOUT).timeout(RPC_TIMEOUT))
        .map_err(|error| SessionError::Connect(format!("{error:?}")))
}

/// The attempt budget for one command. One submission: a retry would fold a
/// recovery path into a measurement meant to time the ordinary commit.
#[must_use]
pub fn attempts() -> AttemptBudget {
    AttemptBudget::new(1).expect("one submission is a valid budget")
}

/// Convenience: the endpoint of a running daemon.
#[must_use]
pub fn endpoint_of(daemon: &Daemon) -> String {
    daemon.endpoint()
}
