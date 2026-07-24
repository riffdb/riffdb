use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use riffdb_api_mcp::{
    McpBackend, McpBackendError, McpBackendFuture, McpBackendRequest, McpCompactObservationRequest,
    McpCompactObservationResult, McpDiscoveryRequest, McpDynamicToolDefinition,
    McpInvocationTarget, McpMarkdownBuilder, McpObservedInventory, McpObserverBackend,
    McpObserverBackendError, McpObserverBackendFuture, McpPostAuthenticationAdmission,
    McpRateTarget, McpRequestId, McpResourceBody, McpResourceContent, McpResourceDescriptor,
    McpResourceDiscoveryRequest, McpResourceDiscoverySurface, McpResourceJson, McpResourceLocator,
    McpResourcePage, McpResourceReadRequest, McpSubscribedResourceObservation,
    McpSubscriptionRequest, McpToolDiscoveryItem, McpToolInvocation, McpToolPage, McpToolResult,
    McpTransportKind, RequestIdSourceError, SchemaDocument, format_command_documentation_locator,
    format_command_plan_locator, format_contract_version_locator, format_entity_schema_locator,
    format_projection_status_locator,
};
use riffdb_errors::PublicError;
use riffdb_types::{RequestId, ServiceOperationV1, hash_schema};
use serde_json::{Value, json};

pub(crate) const CAPABILITY_TOKEN: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
pub(crate) const STABLE_TOOL: &str = "riffdb.cmd.legalspend.allocatebudget";
pub(crate) const STALE_TOOL: &str = "riffdb.cmd.legalspend.retiredbudget";
pub(crate) const UNKNOWN_TOOL: &str = "riffdb.cmd.legalspend.unknown";
pub(crate) const ACTIVE_CONTRACT_URI: &str = "riffdb://contract/active";
pub(crate) const COMMAND_PLAN_URI: &str = "riffdb://command/LegalSpend/2/plan";
pub(crate) const PROJECTION_STATUS_URI: &str = "riffdb://projection/LegalSpend/1/status";
pub(crate) const INTERNAL_CANARY: &str = "riffdb-internal-canary-never-public";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackendCounts {
    pub begin_stdio: usize,
    pub begin_http: usize,
    pub tool_discovery: usize,
    pub dynamic_resolution: usize,
    pub invocation: usize,
    pub resource_discovery: usize,
    pub resource_read: usize,
}

#[derive(Default)]
struct BackendState {
    next_id: AtomicU64,
    begin_stdio: AtomicUsize,
    begin_http: AtomicUsize,
    tool_discovery: AtomicUsize,
    dynamic_resolution: AtomicUsize,
    invocation: AtomicUsize,
    resource_discovery: AtomicUsize,
    resource_read: AtomicUsize,
}

#[derive(Clone, Default)]
pub(crate) struct ConformanceBackend {
    state: Arc<BackendState>,
}

pub(crate) struct ConformanceInvocation {
    source: McpTransportKind,
    admission: Option<McpPostAuthenticationAdmission>,
}

impl ConformanceBackend {
    pub(crate) fn counts(&self) -> BackendCounts {
        BackendCounts {
            begin_stdio: self.state.begin_stdio.load(Ordering::Acquire),
            begin_http: self.state.begin_http.load(Ordering::Acquire),
            tool_discovery: self.state.tool_discovery.load(Ordering::Acquire),
            dynamic_resolution: self.state.dynamic_resolution.load(Ordering::Acquire),
            invocation: self.state.invocation.load(Ordering::Acquire),
            resource_discovery: self.state.resource_discovery.load(Ordering::Acquire),
            resource_read: self.state.resource_read.load(Ordering::Acquire),
        }
    }
}

impl ConformanceInvocation {
    fn admit(&self, target: McpRateTarget) -> Result<(), McpBackendError> {
        match &self.admission {
            Some(admission) => admission
                .admit(target)
                .map_err(|_| McpBackendError::RateLimited),
            None if self.source == McpTransportKind::Stdio => Ok(()),
            None => Err(McpBackendError::AuthenticationLost),
        }
    }
}

impl McpBackend for ConformanceBackend {
    type Invocation = ConformanceInvocation;

    fn next_request_id(&self) -> Result<McpRequestId, RequestIdSourceError> {
        let counter = self
            .state
            .next_id
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .ok_or(RequestIdSourceError)?;
        let mut random = [0_u8; 10];
        random[2..].copy_from_slice(&counter.to_be_bytes());
        let request_id = RequestId::from_unix_milliseconds_and_random(1, random)
            .map_err(|_| RequestIdSourceError)?;
        McpRequestId::from_public_bytes(request_id.as_bytes())
    }

    fn begin_invocation(
        &self,
        request: McpBackendRequest<'_>,
    ) -> Result<Self::Invocation, PublicError> {
        let source = request.source();
        let admission = request.post_authentication_admission().cloned();
        match source {
            McpTransportKind::Stdio => {
                if admission.is_some() {
                    return Err(PublicError::authorization_denied());
                }
                self.state.begin_stdio.fetch_add(1, Ordering::AcqRel);
            }
            McpTransportKind::StreamableHttp => {
                if admission.is_none() {
                    return Err(PublicError::authorization_denied());
                }
                #[cfg(feature = "streamable-http")]
                {
                    use std::time::{Duration, Instant};

                    use riffdb_api_mcp::hosted_mcp_request_context;
                    use riffdb_service::RequestControl;

                    let deadline = Instant::now()
                        .checked_add(Duration::from_secs(5))
                        .ok_or_else(PublicError::authorization_denied)?;
                    let (control, _cancellation) = RequestControl::new(deadline);
                    hosted_mcp_request_context(&request, control)
                        .map_err(|_| PublicError::authorization_denied())?;
                    self.state.begin_http.fetch_add(1, Ordering::AcqRel);
                }
                #[cfg(not(feature = "streamable-http"))]
                {
                    return Err(PublicError::authorization_denied());
                }
            }
        }
        Ok(ConformanceInvocation { source, admission })
    }

    fn discover_tools<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        _request: McpDiscoveryRequest,
    ) -> McpBackendFuture<'a, McpToolPage> {
        self.state.tool_discovery.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            invocation.admit(McpRateTarget::Service(
                ServiceOperationV1::DiscoverCommandTools,
            ))?;
            let stable = dynamic_tool(STABLE_TOOL)?;
            let stale = dynamic_tool(STALE_TOOL)?;
            McpToolPage::new(
                (1..=14)
                    .map(McpToolDiscoveryItem::Fixed)
                    .chain(
                        [stable, stale]
                            .into_iter()
                            .map(Box::new)
                            .map(McpToolDiscoveryItem::Dynamic),
                    )
                    .collect(),
                None,
            )
            .map_err(|_| McpBackendError::InvalidResponse)
        })
    }

    fn resolve_dynamic_tool<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        exact_name: String,
    ) -> McpBackendFuture<'a, McpDynamicToolDefinition> {
        self.state.dynamic_resolution.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            invocation.admit(
                McpRateTarget::command_tool(exact_name.clone())
                    .map_err(|_| McpBackendError::InvalidResponse)?,
            )?;
            if exact_name == STABLE_TOOL {
                dynamic_tool(STABLE_TOOL)
            } else {
                Err(McpBackendError::TargetUnavailable)
            }
        })
    }

    fn invoke_tool<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpToolInvocation,
    ) -> McpBackendFuture<'a, McpToolResult> {
        self.state.invocation.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            let McpInvocationTarget::Dynamic(name) = request.target() else {
                return Err(McpBackendError::TargetUnavailable);
            };
            invocation.admit(
                McpRateTarget::command_tool(name.clone())
                    .map_err(|_| McpBackendError::InvalidResponse)?,
            )?;
            if name != STABLE_TOOL {
                return Err(McpBackendError::TargetUnavailable);
            }
            let arguments: Value = request
                .arguments()
                .deserialize()
                .map_err(|_| McpBackendError::InvalidResponse)?;
            if arguments["allocation"]["priority"] != "High"
                || arguments["allocation"]["account_name"] != "Operations"
            {
                return Err(McpBackendError::InvalidResponse);
            }
            McpToolResult::from_serializable(&json!({
                "commit_sequence": "7",
                "contract_version": 2,
                "durability_mode": "sync",
                "outcome": {
                    "allocation": {
                        "account_name": "Operations",
                        "priority": "High"
                    },
                    "type": "Allocated"
                },
                "outcome_uri": concat!(
                    "riffdb://outcome/conformance-agent/LegalSpend/2/",
                    "riffdb.cmd.legalspend.allocatebudget/",
                    "AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                ),
                "plan_hash": "3333333333333333333333333333333333333333333333333333333333333333",
                "provenance_uri": "riffdb://provenance/00000000-0001-7000-8000-000000000000",
                "status": "committed"
            }))
            .map_err(|_| McpBackendError::InvalidResponse)
        })
    }

    fn discover_resources<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpResourceDiscoveryRequest,
    ) -> McpBackendFuture<'a, McpResourcePage> {
        self.state.resource_discovery.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            invocation.admit(McpRateTarget::Service(
                ServiceOperationV1::DiscoverResources,
            ))?;
            let items = match request.surface() {
                McpResourceDiscoverySurface::Concrete => concrete_resources(),
                McpResourceDiscoverySurface::Template => template_resources(),
            }?;
            McpResourcePage::new(request.surface(), items, None)
                .map_err(|_| McpBackendError::InvalidResponse)
        })
    }

    fn read_resource<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpResourceReadRequest,
    ) -> McpBackendFuture<'a, McpResourceContent> {
        self.state.resource_read.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            invocation.admit(resource_rate_target(request.locator()))?;
            resource_content(request.locator())
        })
    }

    fn authorize_subscription<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        _request: McpSubscriptionRequest,
    ) -> McpBackendFuture<'a, ()> {
        Box::pin(async move {
            invocation.admit(McpRateTarget::Service(
                ServiceOperationV1::DiscoverResources,
            ))
        })
    }
}

impl McpObserverBackend for ConformanceBackend {
    type Fence = u64;

    fn discover_compact<'a>(
        &'a self,
        _inventory: McpObservedInventory,
        _request: McpCompactObservationRequest<'a, Self::Fence>,
    ) -> McpObserverBackendFuture<'a, McpCompactObservationResult<Self::Fence>> {
        Box::pin(async { Err(McpObserverBackendError::RetryNextTick) })
    }

    fn observe_subscribed_resource<'a>(
        &'a self,
        _uri: &'a str,
    ) -> McpObserverBackendFuture<'a, McpSubscribedResourceObservation> {
        Box::pin(async { Err(McpObserverBackendError::RetryNextTick) })
    }
}

fn dynamic_tool(name: &str) -> Result<McpDynamicToolDefinition, McpBackendError> {
    McpDynamicToolDefinition::from_discovered_command(name, input_schema()?, outcome_schema()?)
        .map_err(|_| McpBackendError::InvalidResponse)
}

fn input_schema() -> Result<SchemaDocument, McpBackendError> {
    checked_schema(
        "riffdb.generated-schema/command-input/2/v1",
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "additionalProperties": false,
            "properties": {
                "allocation": {
                    "additionalProperties": false,
                    "properties": {
                        "account_name": {
                            "type": "string",
                            "x-riffdb-maxUtf8Bytes": 32
                        },
                        "priority": {
                            "enum": ["High", "Normal"],
                            "type": "string"
                        }
                    },
                    "required": ["account_name", "priority"],
                    "type": "object"
                },
                "idempotency_key": {
                    "type": "string",
                    "x-riffdb-maxUtf8Bytes": 128
                }
            },
            "required": ["allocation", "idempotency_key"],
            "type": "object"
        }),
    )
}

fn outcome_schema() -> Result<SchemaDocument, McpBackendError> {
    checked_schema(
        "riffdb.generated-schema/command-outcome-union/2/v1",
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "oneOf": [{
                "additionalProperties": false,
                "properties": {
                    "allocation": {
                        "additionalProperties": false,
                        "properties": {
                            "account_name": {
                                "type": "string",
                                "x-riffdb-maxUtf8Bytes": 32
                            },
                            "priority": {
                                "enum": ["High", "Normal"],
                                "type": "string"
                            }
                        },
                        "required": ["account_name", "priority"],
                        "type": "object"
                    },
                    "type": {"const": "Allocated"}
                },
                "required": ["allocation", "type"],
                "type": "object"
            }]
        }),
    )
}

fn checked_schema(id: &str, value: Value) -> Result<SchemaDocument, McpBackendError> {
    let source = serde_json::to_string(&value).map_err(|_| McpBackendError::InvalidResponse)?;
    let hash = hash_schema(source.as_bytes());
    SchemaDocument::from_public_parts(id, hash.as_bytes(), source)
        .map_err(|_| McpBackendError::InvalidResponse)
}

fn concrete_resources() -> Result<Vec<McpResourceDescriptor>, McpBackendError> {
    [
        ("active_contract", ACTIVE_CONTRACT_URI),
        ("contract_version", "riffdb://contract/LegalSpend/2"),
        ("entity_schema", "riffdb://entity/LegalSpend/1/schema"),
        ("command_plan", COMMAND_PLAN_URI),
        (
            "command_documentation",
            "riffdb://command/LegalSpend/2/docs",
        ),
        ("projection_status", PROJECTION_STATUS_URI),
        ("server_health", "riffdb://server/health"),
    ]
    .into_iter()
    .map(|(branch, uri)| {
        McpResourceDescriptor::new(branch, uri).map_err(|_| McpBackendError::InvalidResponse)
    })
    .collect()
}

fn template_resources() -> Result<Vec<McpResourceDescriptor>, McpBackendError> {
    [
        (
            "command_outcome",
            concat!(
                "riffdb://outcome/{principal}/LegalSpend/2/",
                "riffdb.cmd.legalspend.allocatebudget/{key_hash}"
            ),
        ),
        ("commit.class_template", "riffdb://commit/{sequence}"),
        (
            "provenance.class_template",
            "riffdb://provenance/{provenance_id}",
        ),
    ]
    .into_iter()
    .map(|(branch, uri)| {
        McpResourceDescriptor::new(branch, uri).map_err(|_| McpBackendError::InvalidResponse)
    })
    .collect()
}

fn resource_rate_target(locator: &McpResourceLocator) -> McpRateTarget {
    McpRateTarget::Service(match locator {
        McpResourceLocator::ActiveContract | McpResourceLocator::ContractVersion { .. } => {
            ServiceOperationV1::GetActiveContract
        }
        McpResourceLocator::EntitySchema { .. }
        | McpResourceLocator::CommandPlan { .. }
        | McpResourceLocator::CommandDocumentation { .. } => ServiceOperationV1::ExplainCommand,
        McpResourceLocator::ProjectionStatus { .. } => ServiceOperationV1::GetProjectionStatus,
        McpResourceLocator::ServerHealth => ServiceOperationV1::GetHealth,
        McpResourceLocator::Outcome { .. } => ServiceOperationV1::ResolveCommandOutcome,
        McpResourceLocator::Commit(_) => ServiceOperationV1::GetCommit,
        McpResourceLocator::Provenance(_) => ServiceOperationV1::TraceProvenance,
    })
}

fn resource_content(locator: &McpResourceLocator) -> Result<McpResourceContent, McpBackendError> {
    let (branch, uri, body) = match locator {
        McpResourceLocator::ActiveContract => (
            "active_contract",
            ACTIVE_CONTRACT_URI.to_owned(),
            json_body(json!({
                "bundle_hash": "0000000000000000000000000000000000000000000000000000000000000000",
                "compatibility": {
                    "code_counts": [],
                    "overall": "compatible",
                    "parent": null
                },
                "contract_lineage": "LegalSpend",
                "contract_version": "2",
                "plan_root_hash": "2222222222222222222222222222222222222222222222222222222222222222",
                "source_hash": "1111111111111111111111111111111111111111111111111111111111111111"
            }))?,
        ),
        McpResourceLocator::ContractVersion { lineage, version } => (
            "contract_version",
            format_contract_version_locator(lineage, *version),
            json_body(json!({
                "compatibility": {
                    "code_counts": [{"code": "RDB-K010", "count": 1}],
                    "overall": "requires_explicit_version",
                    "parent": {
                        "bundle_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "contract_version": "1"
                    }
                },
                "contract_lineage": lineage.as_str(),
                "contract_version": version.get().to_string()
            }))?,
        ),
        McpResourceLocator::EntitySchema { lineage, entity_id } => (
            "entity_schema",
            format_entity_schema_locator(lineage, *entity_id),
            json_body(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "additionalProperties": false,
                "properties": {
                    "account_name": {"type": "string"},
                    "priority": {"enum": ["High", "Normal"], "type": "string"}
                },
                "required": ["account_name", "priority"],
                "type": "object"
            }))?,
        ),
        McpResourceLocator::CommandPlan {
            lineage,
            command_id,
        } => (
            "command_plan",
            format_command_plan_locator(lineage, *command_id),
            json_body(json!({
                "command_id": command_id.get(),
                "contract_lineage": lineage.as_str(),
                "contract_version": "2",
                "explanation": {
                    "conflict_domains": ["budget"],
                    "effects": ["budget_allocated"],
                    "invariants": ["remaining_budget_nonnegative"],
                    "locality": "partition_local",
                    "outcomes": ["Allocated"],
                    "retry": "idempotent"
                },
                "input_schema": input_schema_json(),
                "outcome_schema": outcome_schema_json(),
                "plan_hash": "3333333333333333333333333333333333333333333333333333333333333333",
                "source_command": "AllocateBudget"
            }))?,
        ),
        McpResourceLocator::CommandDocumentation {
            lineage,
            command_id,
        } => {
            let mut builder = McpMarkdownBuilder::new();
            builder
                .push_heading(1, "AllocateBudget")
                .and_then(|builder| {
                    builder.push_paragraph(
                        "Idempotently allocates a named account under the active contract.",
                    )
                })
                .map_err(|_| McpBackendError::InvalidResponse)?;
            (
                "command_documentation",
                format_command_documentation_locator(lineage, *command_id),
                McpResourceBody::Markdown(
                    builder
                        .finish()
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                ),
            )
        }
        McpResourceLocator::ProjectionStatus {
            lineage,
            projection_id,
        } => (
            "projection_status",
            format_projection_status_locator(lineage, *projection_id),
            json_body(json!({
                "authoritative_head": {"applied_through": "12"},
                "identity": {
                    "contract_lineage": lineage.as_str(),
                    "projection_id": projection_id.get(),
                    "projection_plan_hash": "4444444444444444444444444444444444444444444444444444444444444444"
                },
                "lag": "5",
                "lifecycle": "ready",
                "published": {
                    "frontier": {"applied_through": "7"},
                    "generation": "3"
                },
                "published_apply_mode": "enabled"
            }))?,
        ),
        McpResourceLocator::ServerHealth => (
            "server_health",
            "riffdb://server/health".to_owned(),
            json_body(json!({"readiness": "ready", "status": "serving"}))?,
        ),
        McpResourceLocator::Outcome { .. }
        | McpResourceLocator::Commit(_)
        | McpResourceLocator::Provenance(_) => {
            return Err(McpBackendError::TargetUnavailable);
        }
    };
    McpResourceContent::new(branch, uri, body).map_err(|_| McpBackendError::InvalidResponse)
}

fn json_body(value: Value) -> Result<McpResourceBody, McpBackendError> {
    McpResourceJson::from_serializable(&value)
        .map(McpResourceBody::Json)
        .map_err(|_| McpBackendError::InvalidResponse)
}

fn input_schema_json() -> Value {
    serde_json::to_value(input_schema().expect("static conformance input schema"))
        .expect("schema serializes")
}

fn outcome_schema_json() -> Value {
    serde_json::to_value(outcome_schema().expect("static conformance outcome schema"))
        .expect("schema serializes")
}
