//! Ordinary public-gRPC backend for the common MCP handler.

use std::collections::BTreeSet;

use riffdb_api_mcp::{
    McpBackend, McpBackendError, McpBackendFuture, McpBackendRequest, McpCancellationSignal,
    McpDiscoveryRequest, McpDynamicToolDefinition, McpGeneratedSchemaKind, McpInvocationTarget,
    McpObserverPhysicalCallHandle, McpRequestId, McpResourceBody, McpResourceContent,
    McpResourceDescriptor, McpResourceDiscoveryRequest, McpResourceDiscoverySurface,
    McpResourceJson, McpResourceLocator, McpResourcePage, McpResourceReadRequest,
    McpStdioClientActivity, McpSubscriptionRequest, McpToolDiscoveryItem, McpToolInvocation,
    McpToolPage, McpToolResult, McpTransportKind, RequestIdSourceError, SchemaDocument,
    decode_dynamic_command_input, decode_fixed_tool_request, fixed_tool_registry,
    format_active_contract_locator, format_command_documentation_locator_from_public,
    format_command_plan_locator_from_public, format_commit_locator,
    format_commit_locator_from_public, format_commit_template_locator,
    format_contract_version_locator, format_contract_version_locator_from_public,
    format_entity_schema_locator, format_entity_schema_locator_from_public, format_outcome_locator,
    format_outcome_template_locator_from_public, format_projection_status_locator,
    format_projection_status_locator_from_public, format_provenance_locator,
    format_provenance_locator_from_public, format_provenance_template_locator,
    format_server_health_locator, parse_resource_locator,
};
use riffdb_client_rust::{
    CallMetadata, ClientError, DetailsFreeStatus, RiffDbClient, generate_request_id, v1,
};

use crate::response;
use crate::wire::{self, FixedGrpcRequest};

const MAX_INITIAL_DISCOVERY_PAGES: usize = 3;
const MAX_INITIAL_DISCOVERY_ITEMS: usize = 1_024;
const MAX_FULL_CONCRETE_RESOURCE_ITEMS: usize = 16_387;

/// Public-client MCP backend with no local service or authorization authority.
pub struct PublicGrpcMcpBackend {
    client: RiffDbClient,
    metadata: CallMetadata,
    client_activity: McpStdioClientActivity,
}

#[derive(Clone)]
pub struct PublicGrpcInvocation {
    pub(crate) request_id: [u8; 16],
    pub(crate) cancellation: Option<McpCancellationSignal>,
    pub(crate) observer_physical_calls: Option<McpObserverPhysicalCallHandle>,
}

impl PublicGrpcInvocation {
    pub(crate) fn charge_observer_physical_call(&self) -> Result<(), McpBackendError> {
        self.observer_physical_calls
            .as_ref()
            .map_or(Ok(()), |calls| {
                calls.charge().map_err(|_| McpBackendError::Cancelled)
            })
    }
}

#[derive(Clone, Copy)]
enum CommandResourceKind {
    Plan,
    Documentation,
}

#[derive(Clone)]
struct ResolvedDynamicTool {
    definition: McpDynamicToolDefinition,
    source_command: String,
    contract_lineage: String,
    contract_version: u64,
    command_id: u32,
}

impl PublicGrpcMcpBackend {
    /// Creates the bridge over one fixed public client and credential.
    #[must_use]
    pub fn new(client: RiffDbClient, metadata: CallMetadata) -> Self {
        Self::with_client_activity(client, metadata, McpStdioClientActivity::new())
    }

    /// Creates the production bridge with its shared stdio idle clock.
    #[must_use]
    pub fn with_client_activity(
        client: RiffDbClient,
        metadata: CallMetadata,
        client_activity: McpStdioClientActivity,
    ) -> Self {
        Self {
            client,
            metadata,
            client_activity,
        }
    }

    async fn discover_tool_page(
        &self,
        request_id: [u8; 16],
        request: McpDiscoveryRequest,
    ) -> Result<McpToolPage, McpBackendError> {
        let mut client = self.client.clone();
        let response = client
            .discover_command_tools(
                tool_discovery_request(request_id, request.limit(), request.cursor()),
                &self.metadata,
            )
            .await
            .authenticated_client_result(&self.client_activity)?;
        tool_page_from_public(response)
    }

    async fn discover_resource_page(
        &self,
        request_id: [u8; 16],
        request: McpResourceDiscoveryRequest,
    ) -> Result<McpResourcePage, McpBackendError> {
        let mut client = self.client.clone();
        let response = client
            .discover_resources(
                resource_discovery_request(
                    request_id,
                    request.page().limit(),
                    request.page().cursor(),
                    resource_kind(request.surface()),
                ),
                &self.metadata,
            )
            .await
            .authenticated_client_result(&self.client_activity)?;
        resource_page_from_public(response, request.surface())
    }

    pub(crate) async fn read_resource_locator(
        &self,
        invocation: &PublicGrpcInvocation,
        locator: McpResourceLocator,
    ) -> Result<McpResourceContent, McpBackendError> {
        reject_cancelled(invocation)?;

        match locator {
            McpResourceLocator::ActiveContract => {
                let mut client = self.client.clone();
                invocation.charge_observer_physical_call()?;
                let response = client
                    .get_active_contract(
                        v1::GetActiveContractRequest {
                            request_id: invocation.request_id.to_vec(),
                        },
                        &self.metadata,
                    )
                    .await
                    .authenticated_client_result(&self.client_activity)?;
                let contract = match response.result {
                    Some(v1::get_active_contract_response::Result::Present(contract)) => contract,
                    Some(v1::get_active_contract_response::Result::Absent(_)) | None => {
                        return Err(McpBackendError::TargetUnavailable);
                    }
                };
                json_resource(
                    "active_contract",
                    format_active_contract_locator().to_owned(),
                    response::active_contract_resource(contract)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                )
            }
            McpResourceLocator::ContractVersion { lineage, version } => {
                let uri = format_contract_version_locator(&lineage, version);
                let expected_lineage = lineage.as_str().to_owned();
                let expected_version = version.get();
                let mut client = self.client.clone();
                let response = client
                    .get_contract_version(
                        v1::GetContractVersionRequest {
                            request_id: invocation.request_id.to_vec(),
                            contract_lineage: expected_lineage.clone(),
                            contract_version: expected_version,
                        },
                        &self.metadata,
                    )
                    .await
                    .authenticated_client_result(&self.client_activity)?;
                let contract = match response.result {
                    Some(v1::get_contract_version_response::Result::Found(contract)) => contract,
                    Some(v1::get_contract_version_response::Result::NotFound(_)) | None => {
                        return Err(McpBackendError::TargetUnavailable);
                    }
                };
                if contract.contract_lineage != expected_lineage
                    || contract.contract_version != expected_version
                {
                    return Err(McpBackendError::InvalidResponse);
                }
                json_resource(
                    "contract_version",
                    uri,
                    response::contract_version_resource(contract)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                )
            }
            McpResourceLocator::EntitySchema { lineage, entity_id } => {
                let uri = format_entity_schema_locator(&lineage, entity_id);
                let schema = self
                    .read_entity_schema(invocation, lineage.as_str().to_owned(), entity_id.get())
                    .await?;
                json_resource("entity_schema", uri, schema)
            }
            McpResourceLocator::CommandPlan {
                lineage,
                command_id,
            } => {
                let uri =
                    format_command_plan_locator_from_public(lineage.as_str(), command_id.get())
                        .map_err(|_| McpBackendError::InvalidResponse)?;
                let (source_command, explained) = self
                    .authorize_command_resource(
                        invocation,
                        lineage.as_str(),
                        command_id.get(),
                        CommandResourceKind::Plan,
                    )
                    .await?;
                json_resource(
                    "command_plan",
                    uri,
                    response::command_plan_resource(&source_command, explained)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                )
            }
            McpResourceLocator::CommandDocumentation {
                lineage,
                command_id,
            } => {
                let uri = format_command_documentation_locator_from_public(
                    lineage.as_str(),
                    command_id.get(),
                )
                .map_err(|_| McpBackendError::InvalidResponse)?;
                let (source_command, explained) = self
                    .authorize_command_resource(
                        invocation,
                        lineage.as_str(),
                        command_id.get(),
                        CommandResourceKind::Documentation,
                    )
                    .await?;
                let document = response::command_documentation_resource(&source_command, explained)
                    .map_err(|_| McpBackendError::InvalidResponse)?;
                McpResourceContent::new(
                    "command_documentation",
                    uri,
                    McpResourceBody::Markdown(document),
                )
                .map_err(|_| McpBackendError::InvalidResponse)
            }
            McpResourceLocator::Outcome {
                principal,
                lineage,
                command_id,
                tool_name,
                key_hash,
            } => {
                let uri =
                    format_outcome_locator(&principal, &lineage, command_id, &tool_name, key_hash)
                        .map_err(|_| McpBackendError::InvalidResponse)?;
                let mut client = self.client.clone();
                let response = client
                    .get_outcome(
                        v1::GetOutcomeRequest {
                            request_id: invocation.request_id.to_vec(),
                            contract_lineage: String::new(),
                            command_name: String::new(),
                            idempotency_key: String::new(),
                            outcome_uri: Some(uri.clone()),
                        },
                        &self.metadata,
                    )
                    .await
                    .authenticated_client_result(&self.client_activity)?;
                let outcome = match response.result {
                    Some(v1::get_outcome_response::Result::Found(outcome)) => outcome,
                    Some(v1::get_outcome_response::Result::NotFound(_)) | None => {
                        return Err(McpBackendError::TargetUnavailable);
                    }
                };
                json_resource(
                    "command_outcome",
                    uri.clone(),
                    response::outcome_resource(outcome, &uri)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                )
            }
            McpResourceLocator::Commit(sequence) => {
                let expected_sequence = sequence.get();
                let uri = format_commit_locator(sequence);
                let mut client = self.client.clone();
                let response = client
                    .get_commit(
                        v1::GetCommitRequest {
                            request_id: invocation.request_id.to_vec(),
                            commit_sequence: expected_sequence,
                        },
                        &self.metadata,
                    )
                    .await
                    .authenticated_client_result(&self.client_activity)?;
                let commit = match response.result {
                    Some(v1::get_commit_response::Result::Found(commit)) => commit,
                    Some(v1::get_commit_response::Result::NotFound(_)) | None => {
                        return Err(McpBackendError::TargetUnavailable);
                    }
                };
                json_resource(
                    "commit.commit_sequence",
                    uri,
                    response::commit_resource(commit, expected_sequence)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                )
            }
            McpResourceLocator::Provenance(provenance_id) => {
                let uri = format_provenance_locator(provenance_id);
                let mut client = self.client.clone();
                let response = client
                    .trace_provenance(
                        v1::TraceProvenanceRequest {
                            request_id: invocation.request_id.to_vec(),
                            selector: Some(v1::ProvenanceSelection {
                                selection: Some(v1::provenance_selection::Selection::ProvenanceId(
                                    provenance_id.as_bytes().to_vec(),
                                )),
                            }),
                        },
                        &self.metadata,
                    )
                    .await
                    .authenticated_client_result(&self.client_activity)?;
                let provenance = match response.result {
                    Some(v1::trace_provenance_response::Result::Found(provenance)) => provenance,
                    Some(v1::trace_provenance_response::Result::NotFound(_)) | None => {
                        return Err(McpBackendError::TargetUnavailable);
                    }
                };
                json_resource(
                    "provenance.provenance_id",
                    uri,
                    response::provenance_resource(provenance, provenance_id.as_bytes())
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                )
            }
            McpResourceLocator::ProjectionStatus {
                lineage,
                projection_id,
            } => {
                let uri = format_projection_status_locator(&lineage, projection_id);
                let expected_lineage = lineage.as_str().to_owned();
                let expected_projection_id = projection_id.get();
                let mut client = self.client.clone();
                invocation.charge_observer_physical_call()?;
                let response = client
                    .get_projection_status(
                        v1::GetProjectionStatusRequest {
                            request_id: invocation.request_id.to_vec(),
                            contract: Some(v1::ContractSelection {
                                selection: Some(v1::contract_selection::Selection::Active(
                                    v1::Unit {},
                                )),
                            }),
                            projection_id: expected_projection_id,
                        },
                        &self.metadata,
                    )
                    .await
                    .authenticated_client_result(&self.client_activity)?;
                let status = match response.result {
                    Some(v1::get_projection_status_response::Result::Found(status)) => status,
                    Some(v1::get_projection_status_response::Result::NotFound(_)) | None => {
                        return Err(McpBackendError::TargetUnavailable);
                    }
                };
                json_resource(
                    "projection_status",
                    uri,
                    response::projection_status_resource(
                        status,
                        &expected_lineage,
                        expected_projection_id,
                    )
                    .map_err(|_| McpBackendError::InvalidResponse)?,
                )
            }
            McpResourceLocator::ServerHealth => {
                let mut client = self.client.clone();
                invocation.charge_observer_physical_call()?;
                let response = client
                    .health(
                        v1::HealthRequest {
                            request_id: Some(invocation.request_id.to_vec()),
                        },
                        &self.metadata,
                    )
                    .await
                    .authenticated_client_result(&self.client_activity)?;
                let health = match response.result {
                    Some(v1::health_response::Result::Authenticated(health)) => health,
                    Some(v1::health_response::Result::PreBootstrap(_)) | None => {
                        return Err(McpBackendError::TargetUnavailable);
                    }
                };
                json_resource(
                    "server_health",
                    format_server_health_locator().to_owned(),
                    response::authenticated_health_resource(health)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                )
            }
        }
    }

    pub(crate) async fn observe_command_plan(
        &self,
        invocation: &PublicGrpcInvocation,
        expected_lineage: &str,
        expected_command_id: u32,
        uri: &str,
    ) -> Result<riffdb_api_mcp::McpVisibleFingerprint, McpBackendError> {
        reject_cancelled(invocation)?;
        let (source_command, explained) = self
            .authorize_command_resource(
                invocation,
                expected_lineage,
                expected_command_id,
                CommandResourceKind::Plan,
            )
            .await?;
        response::command_plan_observation(uri, &source_command, explained)
            .map_err(|_| McpBackendError::InvalidResponse)
    }

    async fn read_entity_schema(
        &self,
        invocation: &PublicGrpcInvocation,
        expected_lineage: String,
        expected_entity_id: u32,
    ) -> Result<McpResourceJson, McpBackendError> {
        let mut client = self.client.clone();
        let mut request_id = invocation.request_id;
        let mut cursor = None;
        let mut observed_fence = None;
        let mut seen_items = 0_usize;
        let mut seen_uris = BTreeSet::new();
        let mut matched_schema = None;

        for _ in 0..=MAX_FULL_CONCRETE_RESOURCE_ITEMS {
            reject_cancelled(invocation)?;
            let response = client
                .discover_resources(
                    resource_discovery_request(
                        request_id,
                        500,
                        cursor,
                        v1::ResourceDiscoveryKind::Concrete,
                    ),
                    &self.metadata,
                )
                .await
                .authenticated_client_result(&self.client_activity)?;
            let page = match response.result {
                Some(v1::discover_resources_response::Result::Page(page)) => page,
                Some(
                    v1::discover_resources_response::Result::CatalogUnchanged(_)
                    | v1::discover_resources_response::Result::CompactPage(_),
                )
                | None => return Err(McpBackendError::InvalidResponse),
            };
            let fence = page
                .observed_fence
                .as_ref()
                .ok_or(McpBackendError::InvalidResponse)?;
            validate_public_fence(fence)?;
            if observed_fence
                .as_ref()
                .is_some_and(|observed| observed != fence)
            {
                return Err(McpBackendError::InvalidResponse);
            }
            observed_fence.get_or_insert_with(|| fence.clone());

            seen_items = seen_items
                .checked_add(page.items.len())
                .ok_or(McpBackendError::InvalidResponse)?;
            if seen_items > MAX_FULL_CONCRETE_RESOURCE_ITEMS {
                return Err(McpBackendError::InvalidResponse);
            }
            for descriptor in page.items {
                if let Some(v1::resource_descriptor::Resource::EntitySchema(entity)) =
                    descriptor.resource.as_ref()
                    && entity.contract_lineage == expected_lineage
                    && entity.entity_type_id == expected_entity_id
                {
                    if matched_schema.is_some() {
                        return Err(McpBackendError::InvalidResponse);
                    }
                    matched_schema = Some(
                        entity
                            .schema
                            .clone()
                            .ok_or(McpBackendError::InvalidResponse)?,
                    );
                }
                let descriptor = resource_descriptor_from_public(descriptor)?;
                if !seen_uris.insert(descriptor.uri().to_owned()) {
                    return Err(McpBackendError::InvalidResponse);
                }
            }

            cursor = optional_public_cursor(page.next_cursor)?;
            if cursor.is_none() {
                let schema = matched_schema.ok_or(McpBackendError::TargetUnavailable)?;
                return response::entity_schema_resource(schema, expected_entity_id)
                    .map_err(|_| McpBackendError::InvalidResponse);
            }
            if seen_items == MAX_FULL_CONCRETE_RESOURCE_ITEMS {
                return Err(McpBackendError::InvalidResponse);
            }
            request_id = fresh_public_request_id().map_err(|_| McpBackendError::InvalidResponse)?;
        }
        Err(McpBackendError::InvalidResponse)
    }

    async fn authorize_command_resource(
        &self,
        invocation: &PublicGrpcInvocation,
        expected_lineage: &str,
        expected_command_id: u32,
        kind: CommandResourceKind,
    ) -> Result<(String, v1::ExplainedCommand), McpBackendError> {
        let mut client = self.client.clone();
        let mut request_id = invocation.request_id;
        let mut cursor = None;
        let mut observed_fence = None;
        let mut total = 0_usize;
        let mut seen_uris = BTreeSet::new();
        let mut matched_descriptor = None;

        for page_index in 0..MAX_INITIAL_DISCOVERY_PAGES {
            reject_cancelled(invocation)?;
            invocation.charge_observer_physical_call()?;
            let response = client
                .discover_resources(
                    compact_resource_discovery_request(
                        request_id,
                        cursor,
                        None,
                        v1::ResourceDiscoveryKind::Concrete,
                    ),
                    &self.metadata,
                )
                .await
                .authenticated_client_result(&self.client_activity)?;
            let page = match response.result {
                Some(v1::discover_resources_response::Result::CompactPage(page)) => page,
                Some(
                    v1::discover_resources_response::Result::CatalogUnchanged(_)
                    | v1::discover_resources_response::Result::Page(_),
                )
                | None => return Err(McpBackendError::InvalidResponse),
            };
            let fence = page
                .observed_fence
                .as_ref()
                .ok_or(McpBackendError::InvalidResponse)?;
            validate_public_fence(fence)?;
            if observed_fence
                .as_ref()
                .is_some_and(|observed| observed != fence)
            {
                return Err(McpBackendError::InvalidResponse);
            }
            observed_fence.get_or_insert_with(|| fence.clone());

            total = total
                .checked_add(page.items.len())
                .ok_or(McpBackendError::InvalidResponse)?;
            if total > MAX_INITIAL_DISCOVERY_ITEMS {
                return Err(McpBackendError::InvalidResponse);
            }
            for descriptor in page.items {
                let candidate = command_resource_candidate(
                    &descriptor,
                    kind,
                    expected_lineage,
                    expected_command_id,
                )?;
                let descriptor = compact_resource_descriptor_from_public(descriptor)?;
                if !seen_uris.insert(descriptor.uri().to_owned()) {
                    return Err(McpBackendError::InvalidResponse);
                }
                if let Some(candidate) = candidate
                    && matched_descriptor.replace(candidate).is_some()
                {
                    return Err(McpBackendError::InvalidResponse);
                }
            }

            cursor = optional_public_cursor(page.next_cursor)?;
            if cursor.is_none() {
                break;
            }
            if total == MAX_INITIAL_DISCOVERY_ITEMS || page_index + 1 == MAX_INITIAL_DISCOVERY_PAGES
            {
                return Err(McpBackendError::InvalidResponse);
            }
            request_id = fresh_public_request_id().map_err(|_| McpBackendError::InvalidResponse)?;
        }

        let descriptor = matched_descriptor.ok_or(McpBackendError::TargetUnavailable)?;
        let fence = observed_fence.ok_or(McpBackendError::InvalidResponse)?;
        reject_cancelled(invocation)?;
        request_id = fresh_public_request_id().map_err(|_| McpBackendError::InvalidResponse)?;
        invocation.charge_observer_physical_call()?;
        let explain_response = client
            .explain_command(
                v1::ExplainCommandRequest {
                    request_id: request_id.to_vec(),
                    contract: Some(v1::ContractSelection {
                        selection: Some(v1::contract_selection::Selection::Exact(
                            v1::ExactContractSelection {
                                contract_lineage: descriptor.contract_lineage.clone(),
                                contract_version: descriptor.contract_version,
                            },
                        )),
                    }),
                    command_name: descriptor.source_command.clone(),
                },
                &self.metadata,
            )
            .await
            .authenticated_client_result(&self.client_activity)?;
        let explained = match explain_response.result {
            Some(v1::explain_command_response::Result::Found(explained)) => explained,
            Some(v1::explain_command_response::Result::NotFound(_)) | None => {
                return Err(McpBackendError::TargetUnavailable);
            }
        };
        let contract = explained
            .contract
            .as_ref()
            .ok_or(McpBackendError::InvalidResponse)?;
        if contract.contract_lineage != expected_lineage
            || contract.contract_version != descriptor.contract_version
            || explained.command_id != expected_command_id
        {
            return Err(McpBackendError::InvalidResponse);
        }
        response::explain_command(v1::ExplainCommandResponse {
            result: Some(v1::explain_command_response::Result::Found(
                explained.clone(),
            )),
        })
        .map_err(|_| McpBackendError::InvalidResponse)?;

        reject_cancelled(invocation)?;
        request_id = fresh_public_request_id().map_err(|_| McpBackendError::InvalidResponse)?;
        invocation.charge_observer_physical_call()?;
        let final_response = client
            .discover_resources(
                compact_resource_discovery_request(
                    request_id,
                    None,
                    Some(fence.clone()),
                    v1::ResourceDiscoveryKind::Concrete,
                ),
                &self.metadata,
            )
            .await
            .authenticated_client_result(&self.client_activity)?;
        match final_response.result {
            Some(v1::discover_resources_response::Result::CatalogUnchanged(returned))
                if returned == fence =>
            {
                validate_public_fence(&returned)?;
                Ok((descriptor.source_command, explained))
            }
            Some(
                v1::discover_resources_response::Result::CatalogUnchanged(_)
                | v1::discover_resources_response::Result::Page(_)
                | v1::discover_resources_response::Result::CompactPage(_),
            )
            | None => Err(McpBackendError::InvalidResponse),
        }
    }

    async fn resolve_dynamic(
        &self,
        first_request_id: [u8; 16],
        exact_name: String,
    ) -> Result<ResolvedDynamicTool, McpBackendError> {
        let mut client = self.client.clone();
        let mut cursor = None;
        let mut request_id = first_request_id;
        let mut observed_fence = None;
        let mut total = 0_usize;
        let mut matched = None;
        for page_index in 0..MAX_INITIAL_DISCOVERY_PAGES {
            let response = client
                .discover_command_tools(
                    tool_discovery_request(request_id, 500, cursor),
                    &self.metadata,
                )
                .await
                .authenticated_client_result(&self.client_activity)
                .map_err(hide_dynamic_resolution_error)?;
            let fence = match response.result.as_ref() {
                Some(v1::discover_command_tools_response::Result::Page(page)) => page
                    .observed_fence
                    .as_ref()
                    .ok_or(McpBackendError::TargetUnavailable)?,
                Some(
                    v1::discover_command_tools_response::Result::CatalogUnchanged(_)
                    | v1::discover_command_tools_response::Result::CompactPage(_),
                )
                | None => return Err(McpBackendError::TargetUnavailable),
            };
            validate_public_fence(fence).map_err(|_| McpBackendError::TargetUnavailable)?;
            if observed_fence
                .as_ref()
                .is_some_and(|observed| observed != fence)
            {
                return Err(McpBackendError::TargetUnavailable);
            }
            observed_fence.get_or_insert_with(|| fence.clone());
            if let Some(v1::discover_command_tools_response::Result::Page(page)) =
                response.result.as_ref()
            {
                for item in &page.items {
                    if let Some(v1::command_tool_discovery_item::Item::CommandTool(tool)) =
                        item.item.as_ref()
                        && tool.tool_name == exact_name
                        && matched
                            .replace(resolved_dynamic_tool_from_public(tool.clone())?)
                            .is_some()
                    {
                        return Err(McpBackendError::TargetUnavailable);
                    }
                }
            }
            let page =
                tool_page_from_public(response).map_err(|_| McpBackendError::TargetUnavailable)?;
            total = total
                .checked_add(page.items().len())
                .ok_or(McpBackendError::TargetUnavailable)?;
            if total > MAX_INITIAL_DISCOVERY_ITEMS {
                return Err(McpBackendError::TargetUnavailable);
            }
            cursor = page.next_cursor();
            let Some(_) = cursor else {
                return matched.ok_or(McpBackendError::TargetUnavailable);
            };
            if total == MAX_INITIAL_DISCOVERY_ITEMS || page_index + 1 == MAX_INITIAL_DISCOVERY_PAGES
            {
                return Err(McpBackendError::TargetUnavailable);
            }
            request_id =
                fresh_public_request_id().map_err(|_| McpBackendError::TargetUnavailable)?;
        }
        Err(McpBackendError::TargetUnavailable)
    }

    async fn invoke_fixed(
        &self,
        request_id: [u8; 16],
        tag: u8,
        request: &McpToolInvocation,
    ) -> Result<McpToolResult, McpBackendError> {
        let request = decode_fixed_tool_request(tag, request.arguments())
            .map_err(|_| McpBackendError::InvalidResponse)?;
        let request = wire::fixed_request_to_proto(request_id, request)
            .map_err(|_| McpBackendError::InvalidResponse)?;
        let mut client = self.client.clone();
        match request {
            FixedGrpcRequest::ValidateContract(request) => response::validate_contract(
                client
                    .validate_contract(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::GetActiveContract(request) => response::get_active_contract(
                client
                    .get_active_contract(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::ExplainCommand(request) => response::explain_command(
                client
                    .explain_command(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::DeployContract(request) => response::deploy_contract(
                client
                    .deploy_contract(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::GetOutcome(request) => response::get_outcome(
                client
                    .get_outcome(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::GetEntity(request) => response::get_entity(
                client
                    .get_entity(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::ScanIndex(request) => response::scan_index(
                client
                    .scan_index(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::GetCommit(request) => {
                let expected_sequence = request.commit_sequence;
                response::get_commit(
                    client
                        .get_commit(request, &self.metadata)
                        .await
                        .authenticated_client_result(&self.client_activity)?,
                    expected_sequence,
                )
            }
            FixedGrpcRequest::ScanCommits(request) => response::scan_commits(
                client
                    .scan_commits(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::TraceProvenance(request) => response::trace_provenance(
                client
                    .trace_provenance(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::QueryProjection(request) => response::query_projection(
                client
                    .query_projection(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::GetProjectionStatus(request) => response::get_projection_status(
                client
                    .get_projection_status(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::ListPendingOutboxDeliveries(request) => {
                response::list_pending_outbox_deliveries(
                    client
                        .list_pending_outbox_deliveries(request, &self.metadata)
                        .await
                        .authenticated_client_result(&self.client_activity)?,
                )
            }
            FixedGrpcRequest::Health(request) => response::health(
                client
                    .health(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::DescribeContract(request) => response::describe_contract(
                client
                    .describe_contract(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::CheckQuery(request) => response::check_query(
                client
                    .check_query(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::ExplainQuery(request) => response::explain_query(
                client
                    .explain_query(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::ExecuteQuery(request) => response::execute_query(
                client
                    .execute_query(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
            FixedGrpcRequest::RunCommand(request) => response::run_command(
                client
                    .execute(request, &self.metadata)
                    .await
                    .authenticated_client_result(&self.client_activity)?,
            ),
        }
        .map_err(|_| McpBackendError::InvalidResponse)
    }

    async fn invoke_dynamic(
        &self,
        invocation: &PublicGrpcInvocation,
        exact_name: String,
        request: &McpToolInvocation,
    ) -> Result<McpToolResult, McpBackendError> {
        let resolution_request_id =
            fresh_public_request_id().map_err(|_| McpBackendError::InvalidResponse)?;
        let resolved = self
            .resolve_dynamic(resolution_request_id, exact_name)
            .await?;
        let fields =
            decode_dynamic_command_input(request.arguments(), resolved.definition.input_schema())
                .map_err(|_| McpBackendError::InvalidResponse)?;
        let input = wire::submitted_record_to_proto(fields)
            .map_err(|_| McpBackendError::InvalidResponse)?;
        reject_cancelled(invocation)?;
        let request_id = fresh_public_request_id().map_err(|_| McpBackendError::InvalidResponse)?;
        let mut client = self.client.clone();
        let response = client
            .execute(
                v1::ExecuteCommandRequest {
                    request_id: request_id.to_vec(),
                    command_name: resolved.source_command,
                    expected_contract_version: Some(resolved.contract_version),
                    input: Some(input),
                },
                &self.metadata,
            )
            .await
            .authenticated_client_result(&self.client_activity)?;
        if let Some(uri) = response.outcome_uri.as_deref() {
            match parse_resource_locator(uri) {
                Ok(McpResourceLocator::Outcome {
                    lineage,
                    command_id,
                    tool_name,
                    ..
                }) if lineage.as_str() == resolved.contract_lineage
                    && command_id.get() == resolved.command_id
                    && tool_name == resolved.definition.name() => {}
                _ => return Err(McpBackendError::InvalidResponse),
            }
        }
        response::dynamic_command_result(
            response,
            resolved.contract_version,
            resolved.definition.outcome_schema(),
            resolved.definition.result_schema(),
        )
        .map_err(|_| McpBackendError::InvalidResponse)
    }
}

impl McpBackend for PublicGrpcMcpBackend {
    type Invocation = PublicGrpcInvocation;

    fn next_request_id(&self) -> Result<McpRequestId, RequestIdSourceError> {
        McpRequestId::from_public_bytes(&fresh_public_request_id()?)
    }

    fn begin_invocation(
        &self,
        request: McpBackendRequest<'_>,
    ) -> Result<Self::Invocation, riffdb_client_rust::PublicError> {
        if request.source() != McpTransportKind::Stdio {
            return Err(riffdb_client_rust::PublicError::authorization_denied());
        }
        Ok(PublicGrpcInvocation {
            request_id: request.request_id().into_public_bytes(),
            cancellation: request.cancellation().cloned(),
            observer_physical_calls: None,
        })
    }

    fn discover_tools<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpDiscoveryRequest,
    ) -> McpBackendFuture<'a, McpToolPage> {
        Box::pin(async move {
            reject_cancelled(invocation)?;
            self.discover_tool_page(invocation.request_id, request)
                .await
        })
    }

    fn resolve_dynamic_tool<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        exact_name: String,
    ) -> McpBackendFuture<'a, McpDynamicToolDefinition> {
        Box::pin(async move {
            reject_cancelled(invocation)?;
            self.resolve_dynamic(invocation.request_id, exact_name)
                .await
                .map(|resolved| resolved.definition)
        })
    }

    fn invoke_tool<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpToolInvocation,
    ) -> McpBackendFuture<'a, McpToolResult> {
        Box::pin(async move {
            reject_cancelled(invocation)?;
            match request.target().clone() {
                McpInvocationTarget::Fixed { tag, name: _ } => {
                    self.invoke_fixed(invocation.request_id, tag, &request)
                        .await
                }
                McpInvocationTarget::Dynamic(exact_name) => {
                    self.invoke_dynamic(invocation, exact_name, &request).await
                }
            }
        })
    }

    fn discover_resources<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpResourceDiscoveryRequest,
    ) -> McpBackendFuture<'a, McpResourcePage> {
        Box::pin(async move {
            reject_cancelled(invocation)?;
            self.discover_resource_page(invocation.request_id, request)
                .await
        })
    }

    fn read_resource<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpResourceReadRequest,
    ) -> McpBackendFuture<'a, riffdb_api_mcp::McpResourceContent> {
        Box::pin(async move {
            reject_cancelled(invocation)?;
            self.read_resource_locator(invocation, request.locator().clone())
                .await
        })
    }

    fn authorize_subscription<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpSubscriptionRequest,
    ) -> McpBackendFuture<'a, ()> {
        Box::pin(async move {
            reject_cancelled(invocation)?;
            match request.locator() {
                McpResourceLocator::ActiveContract
                | McpResourceLocator::CommandPlan { .. }
                | McpResourceLocator::ProjectionStatus { .. }
                | McpResourceLocator::ServerHealth => {
                    self.read_resource_locator(invocation, request.locator().clone())
                        .await?;
                    Ok(())
                }
                McpResourceLocator::ContractVersion { .. }
                | McpResourceLocator::EntitySchema { .. }
                | McpResourceLocator::CommandDocumentation { .. }
                | McpResourceLocator::Outcome { .. }
                | McpResourceLocator::Commit(_)
                | McpResourceLocator::Provenance(_) => Err(McpBackendError::TargetUnavailable),
            }
        })
    }
}

fn reject_cancelled(invocation: &PublicGrpcInvocation) -> Result<(), McpBackendError> {
    if invocation
        .cancellation
        .as_ref()
        .is_some_and(McpCancellationSignal::is_cancelled)
    {
        Err(McpBackendError::Cancelled)
    } else {
        Ok(())
    }
}

fn fresh_public_request_id() -> Result<[u8; 16], RequestIdSourceError> {
    generate_request_id()
        .map(|request_id| request_id.into_bytes())
        .map_err(|_| RequestIdSourceError)
}

pub(crate) fn map_client_error(error: ClientError) -> McpBackendError {
    match error {
        ClientError::Public(error) => McpBackendError::Public(error),
        ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated) => {
            McpBackendError::AuthenticationLost
        }
        ClientError::DetailsFree(DetailsFreeStatus::Cancelled) => McpBackendError::Cancelled,
        ClientError::DetailsFree(_)
        | ClientError::Protocol(_)
        | ClientError::IdentifierGeneration(_)
        | ClientError::ConnectionFailure
        | ClientError::OutcomeUnknown(_) => McpBackendError::InvalidResponse,
    }
}

trait AuthenticatedClientResult<T> {
    fn authenticated_client_result(
        self,
        activity: &McpStdioClientActivity,
    ) -> Result<T, McpBackendError>;
}

impl<T> AuthenticatedClientResult<T> for Result<T, ClientError> {
    fn authenticated_client_result(
        self,
        activity: &McpStdioClientActivity,
    ) -> Result<T, McpBackendError> {
        if client_result_proves_authentication(&self) && !activity.authenticated_request_completed()
        {
            return Err(McpBackendError::InvalidResponse);
        }
        self.map_err(map_client_error)
    }
}

fn client_result_proves_authentication<T>(result: &Result<T, ClientError>) -> bool {
    matches!(result, Ok(_) | Err(ClientError::Public(_)))
}

fn hide_dynamic_resolution_error(error: McpBackendError) -> McpBackendError {
    match error {
        McpBackendError::AuthenticationLost | McpBackendError::Cancelled => error,
        McpBackendError::Public(_)
        | McpBackendError::InvalidResponse
        | McpBackendError::RateLimited
        | McpBackendError::TargetUnavailable => McpBackendError::TargetUnavailable,
    }
}

fn json_resource(
    branch: &'static str,
    uri: String,
    json: McpResourceJson,
) -> Result<McpResourceContent, McpBackendError> {
    McpResourceContent::new(branch, uri, McpResourceBody::Json(json))
        .map_err(|_| McpBackendError::InvalidResponse)
}

pub(crate) fn validate_public_fence(
    fence: &v1::DiscoveryCatalogFence,
) -> Result<(), McpBackendError> {
    if fence.server_generation.len() != 16 {
        return Err(McpBackendError::InvalidResponse);
    }
    match fence
        .state
        .as_ref()
        .ok_or(McpBackendError::InvalidResponse)?
    {
        v1::discovery_catalog_fence::State::NoActiveContract(_) => {}
        v1::discovery_catalog_fence::State::ActiveContract(active) => {
            format_contract_version_locator_from_public(
                &active.contract_lineage,
                active.contract_version,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?;
            if active.bundle_hash.len() != 32 {
                return Err(McpBackendError::InvalidResponse);
            }
        }
    }

    let identities = fence
        .operation_schemas
        .as_ref()
        .ok_or(McpBackendError::InvalidResponse)?;
    let registry = fixed_tool_registry().map_err(|_| McpBackendError::InvalidResponse)?;
    let expected = registry.operation_schemas();
    if expected.len() != 2 {
        return Err(McpBackendError::InvalidResponse);
    }
    for (identity, expected) in [
        identities.command_operation_envelope.as_ref(),
        identities.command_get_outcome_result.as_ref(),
    ]
    .into_iter()
    .zip(expected)
    {
        let identity = identity.ok_or(McpBackendError::InvalidResponse)?;
        if identity.schema_id != expected.schema_id()
            || identity.schema_hash.as_slice() != expected.schema_hash_bytes()
        {
            return Err(McpBackendError::InvalidResponse);
        }
    }
    Ok(())
}

fn validate_operation_catalog(catalog: &v1::OperationSchemaCatalog) -> Result<(), McpBackendError> {
    let registry = fixed_tool_registry().map_err(|_| McpBackendError::InvalidResponse)?;
    let expected = registry.operation_schemas();
    if expected.len() != 2 {
        return Err(McpBackendError::InvalidResponse);
    }
    for (artifact, expected) in [
        catalog.command_operation_envelope.as_ref(),
        catalog.command_get_outcome_result.as_ref(),
    ]
    .into_iter()
    .zip(expected)
    {
        let artifact = artifact.ok_or(McpBackendError::InvalidResponse)?;
        if artifact.schema_id != expected.schema_id()
            || artifact.dialect != "https://json-schema.org/draft/2020-12/schema"
            || artifact.schema_hash.as_slice() != expected.schema_hash_bytes()
            || artifact.canonical_json != expected.canonical_json()
        {
            return Err(McpBackendError::InvalidResponse);
        }
        SchemaDocument::from_public_parts(
            artifact.schema_id.clone(),
            &artifact.schema_hash,
            artifact.canonical_json.clone(),
        )
        .map_err(|_| McpBackendError::InvalidResponse)?;
    }
    Ok(())
}

fn command_resource_candidate(
    descriptor: &v1::CompactResourceDescriptor,
    kind: CommandResourceKind,
    expected_lineage: &str,
    expected_command_id: u32,
) -> Result<Option<v1::CommandResource>, McpBackendError> {
    let resource = descriptor
        .resource
        .as_ref()
        .ok_or(McpBackendError::InvalidResponse)?;
    let resource = match (kind, resource) {
        (
            CommandResourceKind::Plan,
            v1::compact_resource_descriptor::Resource::CommandPlan(resource),
        )
        | (
            CommandResourceKind::Documentation,
            v1::compact_resource_descriptor::Resource::CommandDocumentation(resource),
        ) => resource,
        _ => return Ok(None),
    };
    if resource.contract_lineage == expected_lineage && resource.command_id == expected_command_id {
        Ok(Some(resource.clone()))
    } else {
        Ok(None)
    }
}

pub(crate) fn compact_resource_descriptor_from_public(
    descriptor: v1::CompactResourceDescriptor,
) -> Result<McpResourceDescriptor, McpBackendError> {
    use v1::compact_resource_descriptor::Resource;

    let (branch, uri) = match descriptor
        .resource
        .ok_or(McpBackendError::InvalidResponse)?
    {
        Resource::ActiveContract(_) => (
            "active_contract",
            format_active_contract_locator().to_owned(),
        ),
        Resource::ContractVersion(resource) => (
            "contract_version",
            format_contract_version_locator_from_public(
                &resource.contract_lineage,
                resource.contract_version,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?,
        ),
        Resource::EntitySchema(resource) => {
            let identity = resource.schema.ok_or(McpBackendError::InvalidResponse)?;
            let key = identity
                .key
                .and_then(|key| key.artifact)
                .ok_or(McpBackendError::InvalidResponse)?;
            if key != v1::schema_artifact_key::Artifact::EntityId(resource.entity_type_id)
                || identity.schema_hash.len() != 32
            {
                return Err(McpBackendError::InvalidResponse);
            }
            (
                "entity_schema",
                format_entity_schema_locator_from_public(
                    &resource.contract_lineage,
                    resource.entity_type_id,
                )
                .map_err(|_| McpBackendError::InvalidResponse)?,
            )
        }
        Resource::CommandPlan(resource) => {
            validate_command_resource_identity(&resource)?;
            (
                "command_plan",
                format_command_plan_locator_from_public(
                    &resource.contract_lineage,
                    resource.command_id,
                )
                .map_err(|_| McpBackendError::InvalidResponse)?,
            )
        }
        Resource::CommandDocumentation(resource) => {
            validate_command_resource_identity(&resource)?;
            (
                "command_documentation",
                format_command_documentation_locator_from_public(
                    &resource.contract_lineage,
                    resource.command_id,
                )
                .map_err(|_| McpBackendError::InvalidResponse)?,
            )
        }
        Resource::CommandOutcome(resource) => (
            "command_outcome",
            format_outcome_template_locator_from_public(
                &resource.contract_lineage,
                resource.command_id,
                &resource.tool_name,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?,
        ),
        Resource::Commit(resource) => {
            match resource.target.ok_or(McpBackendError::InvalidResponse)? {
                v1::commit_resource::Target::ClassTemplate(_) => (
                    "commit.class_template",
                    format_commit_template_locator().to_owned(),
                ),
                v1::commit_resource::Target::CommitSequence(sequence) => (
                    "commit.commit_sequence",
                    format_commit_locator_from_public(sequence)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                ),
            }
        }
        Resource::Provenance(resource) => {
            match resource.target.ok_or(McpBackendError::InvalidResponse)? {
                v1::provenance_resource::Target::ClassTemplate(_) => (
                    "provenance.class_template",
                    format_provenance_template_locator().to_owned(),
                ),
                v1::provenance_resource::Target::ProvenanceId(id) => (
                    "provenance.provenance_id",
                    format_provenance_locator_from_public(&id)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                ),
            }
        }
        Resource::ProjectionStatus(resource) => (
            "projection_status",
            format_projection_status_locator_from_public(
                &resource.contract_lineage,
                resource.projection_id,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?,
        ),
        Resource::ServerHealth(_) => ("server_health", format_server_health_locator().to_owned()),
    };
    McpResourceDescriptor::new(branch, uri).map_err(|_| McpBackendError::InvalidResponse)
}

fn validate_command_resource_identity(
    resource: &v1::CommandResource,
) -> Result<(), McpBackendError> {
    if resource.contract_version == 0 || resource.source_command.is_empty() {
        Err(McpBackendError::InvalidResponse)
    } else {
        Ok(())
    }
}

fn tool_discovery_request(
    request_id: [u8; 16],
    limit: u16,
    cursor: Option<[u8; 16]>,
) -> v1::DiscoverCommandToolsRequest {
    v1::DiscoverCommandToolsRequest {
        request_id: request_id.to_vec(),
        page: Some(v1::PageRequest {
            limit: Some(u32::from(limit)),
            cursor: cursor.map(|cursor| cursor.to_vec()),
        }),
        prior_fence: None,
        representation: v1::DiscoveryRepresentation::Full as i32,
    }
}

fn resource_discovery_request(
    request_id: [u8; 16],
    limit: u16,
    cursor: Option<[u8; 16]>,
    kind: v1::ResourceDiscoveryKind,
) -> v1::DiscoverResourcesRequest {
    v1::DiscoverResourcesRequest {
        request_id: request_id.to_vec(),
        page: Some(v1::PageRequest {
            limit: Some(u32::from(limit)),
            cursor: cursor.map(|cursor| cursor.to_vec()),
        }),
        prior_fence: None,
        representation: v1::DiscoveryRepresentation::Full as i32,
        kind: kind as i32,
    }
}

fn compact_resource_discovery_request(
    request_id: [u8; 16],
    cursor: Option<[u8; 16]>,
    prior_fence: Option<v1::DiscoveryCatalogFence>,
    kind: v1::ResourceDiscoveryKind,
) -> v1::DiscoverResourcesRequest {
    v1::DiscoverResourcesRequest {
        request_id: request_id.to_vec(),
        page: Some(v1::PageRequest {
            limit: Some(500),
            cursor: cursor.map(|cursor| cursor.to_vec()),
        }),
        prior_fence,
        representation: v1::DiscoveryRepresentation::CompactObservation as i32,
        kind: kind as i32,
    }
}

fn resource_kind(surface: McpResourceDiscoverySurface) -> v1::ResourceDiscoveryKind {
    match surface {
        McpResourceDiscoverySurface::Concrete => v1::ResourceDiscoveryKind::Concrete,
        McpResourceDiscoverySurface::Template => v1::ResourceDiscoveryKind::Template,
    }
}

fn tool_page_from_public(
    response: v1::DiscoverCommandToolsResponse,
) -> Result<McpToolPage, McpBackendError> {
    let page = match response.result {
        Some(v1::discover_command_tools_response::Result::Page(page)) => page,
        Some(
            v1::discover_command_tools_response::Result::CatalogUnchanged(_)
            | v1::discover_command_tools_response::Result::CompactPage(_),
        )
        | None => return Err(McpBackendError::InvalidResponse),
    };
    validate_public_fence(
        page.observed_fence
            .as_ref()
            .ok_or(McpBackendError::InvalidResponse)?,
    )?;
    validate_operation_catalog(
        page.operation_schemas
            .as_ref()
            .ok_or(McpBackendError::InvalidResponse)?,
    )?;
    let items = page
        .items
        .into_iter()
        .map(tool_item_from_public)
        .collect::<Result<Vec<_>, _>>()?;
    let cursor = optional_public_cursor(page.next_cursor)?;
    McpToolPage::new(items, cursor).map_err(|_| McpBackendError::InvalidResponse)
}

fn tool_item_from_public(
    item: v1::CommandToolDiscoveryItem,
) -> Result<McpToolDiscoveryItem, McpBackendError> {
    match item.item {
        Some(v1::command_tool_discovery_item::Item::FixedTool(tag)) => {
            let tag =
                v1::FixedToolKind::try_from(tag).map_err(|_| McpBackendError::InvalidResponse)?;
            let tag = u8::try_from(tag as i32).map_err(|_| McpBackendError::InvalidResponse)?;
            if tag == 0 {
                return Err(McpBackendError::InvalidResponse);
            }
            Ok(McpToolDiscoveryItem::Fixed(tag))
        }
        Some(v1::command_tool_discovery_item::Item::CommandTool(tool)) => {
            dynamic_tool_from_public(tool)
                .map(Box::new)
                .map(McpToolDiscoveryItem::Dynamic)
        }
        None => Err(McpBackendError::InvalidResponse),
    }
}

fn dynamic_tool_from_public(
    tool: v1::CommandToolDescriptor,
) -> Result<McpDynamicToolDefinition, McpBackendError> {
    if tool.contract_version == 0 || tool.source_command.is_empty() {
        return Err(McpBackendError::InvalidResponse);
    }
    format_command_plan_locator_from_public(&tool.contract_lineage, tool.command_id)
        .map_err(|_| McpBackendError::InvalidResponse)?;
    let input = generated_command_schema(
        tool.input_schema,
        tool.command_id,
        GeneratedCommandSchemaKind::Input,
    )?;
    let outcome = generated_command_schema(
        tool.outcome_schema,
        tool.command_id,
        GeneratedCommandSchemaKind::Outcome,
    )?;
    McpDynamicToolDefinition::from_discovered_command(tool.tool_name, input, outcome)
        .map_err(|_| McpBackendError::InvalidResponse)
}

fn resolved_dynamic_tool_from_public(
    tool: v1::CommandToolDescriptor,
) -> Result<ResolvedDynamicTool, McpBackendError> {
    let source_command = tool.source_command.clone();
    let contract_lineage = tool.contract_lineage.clone();
    let contract_version = tool.contract_version;
    let command_id = tool.command_id;
    let definition = dynamic_tool_from_public(tool)?;
    Ok(ResolvedDynamicTool {
        definition,
        source_command,
        contract_lineage,
        contract_version,
        command_id,
    })
}

#[derive(Clone, Copy)]
enum GeneratedCommandSchemaKind {
    Input,
    Outcome,
}

fn generated_command_schema(
    schema: Option<v1::GeneratedSchemaArtifact>,
    command_id: u32,
    kind: GeneratedCommandSchemaKind,
) -> Result<SchemaDocument, McpBackendError> {
    let schema = schema.ok_or(McpBackendError::InvalidResponse)?;
    if schema.dialect != "https://json-schema.org/draft/2020-12/schema" {
        return Err(McpBackendError::InvalidResponse);
    }
    let key = schema
        .key
        .and_then(|key| key.artifact)
        .ok_or(McpBackendError::InvalidResponse)?;
    let (generated_kind, matches) = match (kind, key) {
        (
            GeneratedCommandSchemaKind::Input,
            v1::schema_artifact_key::Artifact::CommandInputId(id),
        ) => (McpGeneratedSchemaKind::CommandInput, id == command_id),
        (
            GeneratedCommandSchemaKind::Outcome,
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(id),
        ) => (
            McpGeneratedSchemaKind::CommandOutcomeUnion,
            id == command_id,
        ),
        _ => return Err(McpBackendError::InvalidResponse),
    };
    if !matches {
        return Err(McpBackendError::InvalidResponse);
    }
    SchemaDocument::from_public_generated(
        generated_kind,
        command_id,
        &schema.schema_hash,
        schema.canonical_json,
    )
    .map_err(|_| McpBackendError::InvalidResponse)
}

fn resource_page_from_public(
    response: v1::DiscoverResourcesResponse,
    surface: McpResourceDiscoverySurface,
) -> Result<McpResourcePage, McpBackendError> {
    let page = match response.result {
        Some(v1::discover_resources_response::Result::Page(page)) => page,
        Some(
            v1::discover_resources_response::Result::CatalogUnchanged(_)
            | v1::discover_resources_response::Result::CompactPage(_),
        )
        | None => return Err(McpBackendError::InvalidResponse),
    };
    validate_public_fence(
        page.observed_fence
            .as_ref()
            .ok_or(McpBackendError::InvalidResponse)?,
    )?;
    let items = page
        .items
        .into_iter()
        .map(resource_descriptor_from_public)
        .collect::<Result<Vec<_>, _>>()?;
    let cursor = optional_public_cursor(page.next_cursor)?;
    McpResourcePage::new(surface, items, cursor).map_err(|_| McpBackendError::InvalidResponse)
}

fn resource_descriptor_from_public(
    descriptor: v1::ResourceDescriptor,
) -> Result<McpResourceDescriptor, McpBackendError> {
    use v1::resource_descriptor::Resource;

    let (branch, uri) = match descriptor
        .resource
        .ok_or(McpBackendError::InvalidResponse)?
    {
        Resource::ActiveContract(_) => (
            "active_contract",
            format_active_contract_locator().to_owned(),
        ),
        Resource::ContractVersion(resource) => (
            "contract_version",
            format_contract_version_locator_from_public(
                &resource.contract_lineage,
                resource.contract_version,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?,
        ),
        Resource::EntitySchema(resource) => (
            "entity_schema",
            format_entity_schema_locator_from_public(
                &resource.contract_lineage,
                resource.entity_type_id,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?,
        ),
        Resource::CommandPlan(resource) => (
            {
                validate_command_resource_identity(&resource)?;
                "command_plan"
            },
            format_command_plan_locator_from_public(
                &resource.contract_lineage,
                resource.command_id,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?,
        ),
        Resource::CommandDocumentation(resource) => (
            {
                validate_command_resource_identity(&resource)?;
                "command_documentation"
            },
            format_command_documentation_locator_from_public(
                &resource.contract_lineage,
                resource.command_id,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?,
        ),
        Resource::CommandOutcome(resource) => (
            "command_outcome",
            format_outcome_template_locator_from_public(
                &resource.contract_lineage,
                resource.command_id,
                &resource.tool_name,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?,
        ),
        Resource::Commit(resource) => {
            match resource.target.ok_or(McpBackendError::InvalidResponse)? {
                v1::commit_resource::Target::ClassTemplate(_) => (
                    "commit.class_template",
                    format_commit_template_locator().to_owned(),
                ),
                v1::commit_resource::Target::CommitSequence(sequence) => (
                    "commit.commit_sequence",
                    format_commit_locator_from_public(sequence)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                ),
            }
        }
        Resource::Provenance(resource) => {
            match resource.target.ok_or(McpBackendError::InvalidResponse)? {
                v1::provenance_resource::Target::ClassTemplate(_) => (
                    "provenance.class_template",
                    format_provenance_template_locator().to_owned(),
                ),
                v1::provenance_resource::Target::ProvenanceId(id) => (
                    "provenance.provenance_id",
                    format_provenance_locator_from_public(&id)
                        .map_err(|_| McpBackendError::InvalidResponse)?,
                ),
            }
        }
        Resource::ProjectionStatus(resource) => (
            "projection_status",
            format_projection_status_locator_from_public(
                &resource.contract_lineage,
                resource.projection_id,
            )
            .map_err(|_| McpBackendError::InvalidResponse)?,
        ),
        Resource::ServerHealth(_) => ("server_health", format_server_health_locator().to_owned()),
    };
    McpResourceDescriptor::new(branch, uri).map_err(|_| McpBackendError::InvalidResponse)
}

pub(crate) fn optional_public_cursor(
    cursor: Option<Vec<u8>>,
) -> Result<Option<[u8; 16]>, McpBackendError> {
    cursor
        .map(|cursor| {
            cursor
                .try_into()
                .map_err(|_| McpBackendError::InvalidResponse)
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use riffdb_api_mcp::McpObserverPhysicalCallBudget;

    use super::*;

    fn operation_identity(schema: &SchemaDocument) -> v1::OperationSchemaIdentity {
        v1::OperationSchemaIdentity {
            schema_id: schema.schema_id().to_owned(),
            schema_hash: schema.schema_hash_bytes().to_vec(),
        }
    }

    fn operation_artifact(schema: &SchemaDocument) -> v1::OperationSchemaArtifact {
        v1::OperationSchemaArtifact {
            schema_id: schema.schema_id().to_owned(),
            dialect: "https://json-schema.org/draft/2020-12/schema".to_owned(),
            schema_hash: schema.schema_hash_bytes().to_vec(),
            canonical_json: schema.canonical_json().to_owned(),
        }
    }

    fn accepted_fence() -> v1::DiscoveryCatalogFence {
        let registry = fixed_tool_registry().expect("accepted registry");
        let schemas = registry.operation_schemas();
        v1::DiscoveryCatalogFence {
            server_generation: vec![7; 16],
            operation_schemas: Some(v1::OperationSchemaCatalogIdentity {
                command_operation_envelope: Some(operation_identity(&schemas[0])),
                command_get_outcome_result: Some(operation_identity(&schemas[1])),
            }),
            state: Some(v1::discovery_catalog_fence::State::NoActiveContract(
                v1::Unit {},
            )),
        }
    }

    #[test]
    fn public_operation_catalog_must_equal_the_common_sources() {
        let registry = fixed_tool_registry().expect("accepted registry");
        let schemas = registry.operation_schemas();
        let mut catalog = v1::OperationSchemaCatalog {
            command_operation_envelope: Some(operation_artifact(&schemas[0])),
            command_get_outcome_result: Some(operation_artifact(&schemas[1])),
        };
        assert!(validate_operation_catalog(&catalog).is_ok());

        catalog
            .command_operation_envelope
            .as_mut()
            .expect("artifact")
            .schema_id
            .push_str("-different");
        assert!(validate_operation_catalog(&catalog).is_err());
    }

    #[test]
    fn public_fence_requires_generation_and_exact_operation_identities() {
        let mut fence = accepted_fence();
        assert!(validate_public_fence(&fence).is_ok());

        fence.server_generation.pop();
        assert!(validate_public_fence(&fence).is_err());

        let mut fence = accepted_fence();
        fence
            .operation_schemas
            .as_mut()
            .and_then(|catalog| catalog.command_get_outcome_result.as_mut())
            .expect("identity")
            .schema_hash[0] ^= 0xff;
        assert!(validate_public_fence(&fence).is_err());
    }

    #[test]
    fn command_resource_descriptors_require_self_contained_resolution_facts() {
        let descriptor = v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::CommandPlan(
                v1::CommandResource {
                    contract_lineage: "LegalSpend".to_owned(),
                    command_id: 2,
                    contract_version: 0,
                    source_command: "AllocateBudget".to_owned(),
                },
            )),
        };
        assert!(resource_descriptor_from_public(descriptor).is_err());
    }

    #[test]
    fn lost_public_transport_authentication_remains_distinct() {
        assert!(matches!(
            map_client_error(ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated)),
            McpBackendError::AuthenticationLost
        ));
        assert!(matches!(
            map_client_error(ClientError::DetailsFree(DetailsFreeStatus::Cancelled)),
            McpBackendError::Cancelled
        ));
        assert!(client_result_proves_authentication(&Ok::<_, ClientError>(
            ()
        )));
        assert!(client_result_proves_authentication(&Err::<(), _>(
            ClientError::Public(riffdb_client_rust::PublicError::authorization_denied())
        )));
        assert!(!client_result_proves_authentication(&Err::<(), _>(
            ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated)
        )));
        assert!(matches!(
            hide_dynamic_resolution_error(McpBackendError::AuthenticationLost),
            McpBackendError::AuthenticationLost
        ));
        assert!(matches!(
            hide_dynamic_resolution_error(McpBackendError::Public(
                riffdb_client_rust::PublicError::authorization_denied()
            )),
            McpBackendError::TargetUnavailable
        ));
    }

    #[test]
    fn observer_invocation_enforces_five_calls_while_client_work_is_unmetered() {
        let budget = Arc::new(McpObserverPhysicalCallBudget::new());
        let observer = PublicGrpcInvocation {
            request_id: [1; 16],
            cancellation: None,
            observer_physical_calls: Some(budget.subscribed_resource()),
        };
        for _ in 0..5 {
            observer
                .charge_observer_physical_call()
                .expect("subscribed call within bound");
        }
        assert!(matches!(
            observer.charge_observer_physical_call(),
            Err(McpBackendError::Cancelled)
        ));
        assert_eq!(budget.calls(), 5);

        let client = PublicGrpcInvocation {
            request_id: [2; 16],
            cancellation: None,
            observer_physical_calls: None,
        };
        for _ in 0..6 {
            client
                .charge_observer_physical_call()
                .expect("client work is outside watcher meter");
        }
        assert_eq!(budget.calls(), 5);
    }
}
