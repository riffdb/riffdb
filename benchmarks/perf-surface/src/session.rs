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
    RiffDbClient, StableApplicationClient, app_v1, generate_request_id, v1,
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
        BearerCredential::new(&token).map_err(|_| SessionError::Bootstrap("bearer".to_owned()))?,
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
    embedding: Option<Vec<f32>>,
) -> BTreeMap<String, ApplicationValue> {
    let mut input = BTreeMap::new();
    input.insert(
        "idempotency_key".to_owned(),
        ApplicationValue::String(idempotency_key),
    );
    input.insert(
        "workspace_id".to_owned(),
        ApplicationValue::Uuid(ApplicationUuid::from_bytes(workspace)),
    );
    input.insert(
        "document_id".to_owned(),
        ApplicationValue::Uuid(ApplicationUuid::from_bytes(document)),
    );
    input.insert("title".to_owned(), ApplicationValue::String(title));
    input.insert("body".to_owned(), ApplicationValue::String(body));
    input.insert("size_bytes".to_owned(), ApplicationValue::I64(size_bytes));
    // Only the vector variant declares the field, and only its command accepts
    // these, so the other variants must send exactly what they sent before.
    if let Some(components) = embedding {
        let vector = riffdb_types::CanonicalVector::new(components)
            .expect("perf-surface embedding is a fixed valid dimension");
        input.insert("embedding".to_owned(), ApplicationValue::Vector(vector));
        input.insert(
            "submitted_model".to_owned(),
            ApplicationValue::String("perf-surface-v1".to_owned()),
        );
        input.insert(
            "submitted_version".to_owned(),
            ApplicationValue::String("2026-09-20".to_owned()),
        );
    }
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
pub async fn application_client(endpoint: &str) -> Result<StableApplicationClient, SessionError> {
    StableApplicationClient::connect(transport_endpoint(endpoint)?)
        .await
        .map_err(|error| SessionError::Connect(format!("{error:?}")))
}

fn transport_endpoint(endpoint: &str) -> Result<Endpoint, SessionError> {
    Endpoint::from_shared(endpoint.to_owned())
        .map(|endpoint| endpoint.connect_timeout(RPC_TIMEOUT).timeout(RPC_TIMEOUT))
        .map_err(|error| SessionError::Connect(format!("{error:?}")))
}

/// Deploys one RiffQL query module against the active contract.
///
/// `RiffDbClient` does not expose `deploy_query_module`: its generated surface
/// carries `deploy_contract` and its service clients are private fields, and
/// that file is generator-owned. The tonic client type is public, so the
/// harness constructs its own rather than editing generated code. This is the
/// only way to reach a `nearest` binding, which is how a projected vector query
/// demands the columnar source WP-777 keeps cold.
pub async fn deploy_query_module(
    endpoint: &str,
    token: &str,
    lineage: &str,
    module_name: &str,
    queries: Vec<(String, String)>,
) -> Result<Vec<u8>, SessionError> {
    use riffdb_proto::generated_app::application_query_service_client::ApplicationQueryServiceClient;

    let channel = transport_endpoint(endpoint)?
        .connect()
        .await
        .map_err(|error| SessionError::Connect(format!("{error:?}")))?;
    let mut client = ApplicationQueryServiceClient::new(channel);
    let request = app_v1::DeployQueryModuleRequest {
        // The selector is required: the server's wire validation refuses a
        // missing one with MissingRequiredField, which surfaces as an HTTP/2
        // stream reset rather than a typed error, so an absent selector looks
        // like a transport fault.
        contract: Some(app_v1::ContractSelector {
            lineage: lineage.to_owned(),
            version: 1,
            bundle_hash: Vec::new(),
        }),
        module_name: module_name.to_owned(),
        module_version: 1,
        queries: queries
            .into_iter()
            .map(|(name, source)| app_v1::NamedQuerySource { name, source })
            .collect(),
        expected_active: Some(app_v1::deploy_query_module_request::ExpectedActive::AnyActive(true)),
        request_id: request_id_bytes()?,
    };
    let mut call = tonic::Request::new(request);
    // `CallMetadata::apply` is crate-private, so the header is set directly.
    // It must match what `bearer` produces or the server refuses the call.
    let authorization: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
        format!("Bearer {token}")
            .parse()
            .map_err(|_| SessionError::Bootstrap("authorization header".to_owned()))?;
    call.metadata_mut().insert("authorization", authorization);
    let response = client
        .deploy_query_module(call)
        .await
        .map_err(|error| SessionError::Rpc(format!("deploy_query_module: {error:?}")))?
        .into_inner();
    // The module hash is what a named-query capability is scoped to, so it must
    // come back out of the deployment rather than be guessed.
    let module = response
        .module
        .ok_or_else(|| SessionError::Rpc("deploy_query_module returned no module".to_owned()))?;
    Ok(module.module_hash)
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

/// Mints a capability that may invoke exactly the two commands the harness
/// runs, scoped to this contract's lineage.
///
/// Scoped rather than broad on purpose: a capability wide enough to do anything
/// would let a mistake in the harness exercise a path the measurement does not
/// name, and the permission is per-command by design.
#[allow(clippy::too_many_arguments)]
pub async fn issue_command_capability(
    endpoint: &str,
    bootstrap_token: &str,
    source: &str,
    named_queries: &[(String, Vec<u8>)],
) -> Result<String, SessionError> {
    use riffdb_client_rust::generate_capability_id;
    use riffdb_contract_compiler::compile_contract_source;
    use v1::capability_permission::Permission;

    let bundle = compile_contract_source(source)
        .map_err(|error| SessionError::Input(format!("variant must compile: {error:?}")))?;
    let lineage = bundle.lineage().as_str().to_owned();

    let mut permissions = Vec::new();
    for command in bundle.commands() {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::InvokeCommand(v1::LineageScopedStableId {
                contract_lineage: lineage.clone(),
                stable_id: command.command_id().get(),
            })),
        });
    }
    // A query's authority is not just "may run this query": its compiled proof
    // carries one access requirement per entity and index it touches, and the
    // capability has to satisfy each. Granting read/scan per declared entity
    // and index is what lets a projected vector query reach its rows; without
    // it the query is refused with AuthorizationDenied, which reads like a cold
    // source rather than a missing grant.
    for entity in bundle.schema().entities() {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::ReadEntity(v1::LineageScopedStableId {
                contract_lineage: lineage.clone(),
                stable_id: entity.id().get(),
            })),
        });
        for index in entity.indexes() {
            permissions.push(v1::CapabilityPermission {
                permission: Some(Permission::ScanIndex(v1::LineageScopedStableId {
                    contract_lineage: lineage.clone(),
                    stable_id: index.id().get(),
                })),
            });
        }
    }

    // Reading projection status is its own permission. The measurement drains
    // through it, so without this the projection arm cannot prove it caught up
    // and the drain fails with AuthorizationDenied rather than a real lag.
    for plan in bundle.projections() {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::ReadProjectionStatus(
                v1::LineageScopedStableId {
                    contract_lineage: lineage.clone(),
                    stable_id: plan.projection_id().get(),
                },
            )),
        });
    }
    if permissions.is_empty() {
        return Err(SessionError::Input("no commands to invoke".to_owned()));
    }
    // The wire requires permissions in strictly ascending canonical order and
    // refuses the whole request as NonCanonical otherwise — which surfaces as
    // InvalidOutboundMessage and says nothing about which permission or why.
    // The key is (kind tag, lineage, stable id, bytes, name); every permission
    // here shares one lineage, so sorting by tag then stable id is sufficient.
    // Tags come from the oneof field numbers: invoke_command 5, read_entity 6,
    // scan_index 7, read_projection_status 9, execute_named_query 24.
    let mut visibility = Vec::new();
    for entity in bundle.schema().entities() {
        let mut field_ids: Vec<u32> = entity
            .record()
            .fields()
            .iter()
            .map(|field| field.id().get())
            .filter(|id| {
                !entity
                    .primary_key_fields()
                    .iter()
                    .any(|key| key.get() == *id)
            })
            .collect();
        field_ids.sort_unstable();
        field_ids.dedup();
        if field_ids.is_empty() {
            continue;
        }
        visibility.push(v1::EntityFieldVisibility {
            contract_lineage: lineage.clone(),
            entity_type_id: entity.id().get(),
            field_ids,
            secret_field_ids: Vec::new(),
        });
    }
    // Canonical order is framed: lineage length before lineage bytes, then the
    // entity id.
    visibility.sort_by_key(|entry| {
        (
            entry.contract_lineage.len(),
            entry.contract_lineage.clone(),
            entry.entity_type_id,
        )
    });

    permissions.sort_by_key(|permission| match permission.permission.as_ref() {
        Some(Permission::InvokeCommand(value)) => (5_u8, value.stable_id, String::new()),
        Some(Permission::ReadEntity(value)) => (6, value.stable_id, String::new()),
        Some(Permission::ScanIndex(value)) => (7, value.stable_id, String::new()),
        Some(Permission::ReadProjectionStatus(value)) => (9, value.stable_id, String::new()),
        Some(Permission::ExecuteNamedQuery(value)) => (24, 0, value.query_name.clone()),
        _ => (u8::MAX, 0, String::new()),
    });
    // A named query is a separate permission from a command, scoped to the
    // module hash the deployment returned. Without it the query is refused with
    // AuthorizationDenied, which is indistinguishable from a cold source in the
    // error text and easy to mistake for one.
    for (query_name, module_hash) in named_queries {
        permissions.push(v1::CapabilityPermission {
            permission: Some(Permission::ExecuteNamedQuery(v1::NamedQueryPermission {
                contract_lineage: lineage.clone(),
                query_module_hash: module_hash.clone(),
                query_name: query_name.clone(),
            })),
        });
    }

    let mut client = RiffDbClient::connect(transport_endpoint(endpoint)?)
        .await
        .map_err(|error| SessionError::Connect(format!("{error:?}")))?;
    let admin = CallMetadata::authenticated(
        BearerCredential::new(bootstrap_token)
            .map_err(|_| SessionError::Bootstrap("bearer".to_owned()))?,
    );
    let capability_id = generate_capability_id()
        .map_err(|_| SessionError::Bootstrap("capability id".to_owned()))?;

    let created = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: request_id_bytes()?,
                mode: v1::CapabilityCreateMode::Normal as i32,
                capability_id: capability_id.into_bytes().to_vec(),
                principal_id: "perf-surface-runner".to_owned(),
                actor_kind: v1::ActorKind::Service as i32,
                requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
                audiences: vec![AUDIENCE.to_owned()],
                grant: Some(v1::CapabilityGrant {
                    tenant_scope: Some(v1::TenantScope {
                        scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                    }),
                    partition_scope: Some(v1::PartitionScope {
                        scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                    }),
                    permissions,
                    // A query that returns a non-key field needs visibility
                    // for it: the compiled proof lists the field in its access
                    // requirement, and a capability without it is refused.
                    // Entries must be ordered by (lineage, entity) and each
                    // field list strictly increasing, or the request is
                    // NonCanonical.
                    field_visibility: visibility,
                    // A nearest binding is charged the provider's compiled
                    // partition-scan ceiling rather than k, so a ceiling of one
                    // refuses every projected query. The commands the harness
                    // runs still scan nothing.
                    // A nearest binding is charged the provider's compiled
                    // partition-scan ceiling rather than k, so a ceiling of one
                    // refuses every projected query.
                    max_scan_rows: 4_096,
                    approval_required: Vec::new(),
                    row_policy: None,
                    export: None,
                    reimport: None,
                    vector_inspection: None,
                }),
            },
            &admin,
        )
        .await
        .map_err(|error| SessionError::Rpc(format!("create_capability: {error:?}")))?;

    match created.result {
        Some(v1::create_capability_response::Result::Normal(normal)) => match normal.result {
            Some(v1::normal_create_capability_result::Result::Created(created)) => {
                Ok(created.token)
            }
            other => Err(SessionError::Rpc(format!(
                "capability was not newly created: {other:?}"
            ))),
        },
        other => Err(SessionError::Rpc(format!(
            "create_capability returned an unexpected shape: {other:?}"
        ))),
    }
}

/// Metadata for a bearer token.
pub fn bearer(token: &str) -> Result<CallMetadata, SessionError> {
    Ok(CallMetadata::authenticated(
        BearerCredential::new(token).map_err(|_| SessionError::Bootstrap("bearer".to_owned()))?,
    ))
}

/// Blocks until the projection has consumed everything the primary has, or the
/// budget expires.
///
/// A throughput figure taken without this measures how fast writes were
/// accepted while the projection fell behind, which is a faster number for
/// doing less work. OBL-0240-3 compares a contract that declares a projection
/// against one that does not, so both arms have to have finished the work
/// before either is timed.
pub async fn drain_projection(
    endpoint: &str,
    token: &str,
    projection_id: u32,
    budget: std::time::Duration,
) -> Result<std::time::Duration, SessionError> {
    let mut client = RiffDbClient::connect(transport_endpoint(endpoint)?)
        .await
        .map_err(|error| SessionError::Connect(format!("{error:?}")))?;
    let metadata = bearer(token)?;
    let started = std::time::Instant::now();

    loop {
        let response = client
            .get_projection_status(
                v1::GetProjectionStatusRequest {
                    request_id: request_id_bytes()?,
                    // The harness deploys exactly one contract and never
                    // rotates it, so the active selection is the one under test.
                    contract: Some(v1::ContractSelection {
                        selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
                    }),
                    projection_id,
                },
                &metadata,
            )
            .await
            .map_err(|error| SessionError::Rpc(format!("get_projection_status: {error:?}")))?;

        let Some(v1::get_projection_status_response::Result::Found(status)) = response.result
        else {
            return Err(SessionError::Rpc(
                "projection status not found; the drain would never complete".to_owned(),
            ));
        };

        if let Some(failure) = status.failure {
            return Err(SessionError::Rpc(format!(
                "projection failed while draining: {failure:?}"
            )));
        }

        let head = frontier_value(status.authoritative_head.as_ref());
        let published = status
            .published
            .as_ref()
            .and_then(|generation| frontier_value(generation.frontier.as_ref()));

        // Caught up when the published frontier has reached the authoritative
        // head. Both absent means nothing was written, which is also caught up.
        if head.is_none() || (published.is_some() && published >= head) {
            return Ok(started.elapsed());
        }

        if started.elapsed() > budget {
            return Err(SessionError::Rpc(format!(
                "projection did not drain within {budget:?}: published {published:?}, head {head:?}"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

fn frontier_value(position: Option<&v1::FrontierPosition>) -> Option<u64> {
    match position.and_then(|position| position.position.as_ref()) {
        Some(v1::frontier_position::Position::AppliedThrough(sequence)) => Some(*sequence),
        Some(v1::frontier_position::Position::BeforeFirst(_)) => Some(0),
        None => None,
    }
}
