//! Fresh public-gRPC observation backend for one MCP stdio session.

use std::collections::BTreeSet;
use std::{fmt, sync::Arc};

use riffdb_api_mcp::{
    McpCompactObservationPage, McpCompactObservationRequest, McpCompactObservationResult,
    McpObservedInventory, McpObserverBackend, McpObserverBackendError, McpObserverBackendFuture,
    McpObserverError, McpObserverPhysicalCallBudget, McpResourceDescriptor,
    McpSubscribedResourceObservation, McpVisibleFingerprint, fixed_tool_registry,
    format_command_plan_locator_from_public, parse_resource_locator,
};
use riffdb_client_rust::{
    CallMetadata, ClientError, DetailsFreeStatus, RiffDbClient, generate_request_id, v1,
};

use crate::backend::{
    PublicGrpcInvocation, PublicGrpcMcpBackend, compact_resource_descriptor_from_public,
    map_client_error, optional_public_cursor, validate_public_fence,
};

/// Observer path over the same ordinary public client and credential as stdio.
pub struct PublicGrpcMcpObserverBackend {
    client: RiffDbClient,
    metadata: CallMetadata,
    resource_reader: PublicGrpcMcpBackend,
    physical_call_budget: Arc<McpObserverPhysicalCallBudget>,
}

impl PublicGrpcMcpObserverBackend {
    /// Creates one fresh-call observer with no server-side authority.
    #[must_use]
    pub fn new(client: RiffDbClient, metadata: CallMetadata) -> Self {
        let physical_call_budget = Arc::new(McpObserverPhysicalCallBudget::new());
        Self {
            resource_reader: PublicGrpcMcpBackend::new(client.clone(), metadata.clone()),
            client,
            metadata,
            physical_call_budget,
        }
    }
}

impl fmt::Debug for PublicGrpcMcpObserverBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PublicGrpcMcpObserverBackend([REDACTED])")
    }
}

impl McpObserverBackend for PublicGrpcMcpObserverBackend {
    type Fence = v1::DiscoveryCatalogFence;

    fn discover_compact<'a>(
        &'a self,
        inventory: McpObservedInventory,
        request: McpCompactObservationRequest<'a, Self::Fence>,
    ) -> McpObserverBackendFuture<'a, McpCompactObservationResult<Self::Fence>> {
        Box::pin(async move {
            let request_id = fresh_request_id()?;
            self.physical_call_budget
                .compact_discovery()
                .charge()
                .map_err(|_| McpObserverBackendError::Cancelled)?;
            let mut client = self.client.clone();
            match inventory {
                McpObservedInventory::Tools => {
                    let response = client
                        .discover_command_tools(
                            compact_tool_request(request_id, request),
                            &self.metadata,
                        )
                        .await
                        .map_err(map_observer_client_error)?;
                    compact_tool_result(response)
                }
                McpObservedInventory::Resources => {
                    let response = client
                        .discover_resources(
                            compact_resource_request(request_id, request),
                            &self.metadata,
                        )
                        .await
                        .map_err(map_observer_client_error)?;
                    compact_resource_result(response)
                }
            }
        })
    }

    fn observe_subscribed_resource<'a>(
        &'a self,
        uri: &'a str,
    ) -> McpObserverBackendFuture<'a, McpSubscribedResourceObservation> {
        Box::pin(async move {
            let locator =
                parse_resource_locator(uri).map_err(|_| McpObserverBackendError::RetryNextTick)?;
            let invocation = PublicGrpcInvocation {
                request_id: fresh_request_id()?,
                cancellation: None,
                observer_physical_calls: Some(self.physical_call_budget.subscribed_resource()),
            };
            let observation = match locator {
                riffdb_api_mcp::McpResourceLocator::CommandPlan {
                    lineage,
                    command_id,
                } => {
                    self.resource_reader
                        .observe_command_plan(&invocation, lineage.as_str(), command_id.get(), uri)
                        .await
                }
                locator @ (riffdb_api_mcp::McpResourceLocator::ActiveContract
                | riffdb_api_mcp::McpResourceLocator::ProjectionStatus { .. }
                | riffdb_api_mcp::McpResourceLocator::ServerHealth
                | riffdb_api_mcp::McpResourceLocator::ReactiveWakeup) => self
                    .resource_reader
                    .read_resource_locator(&invocation, locator)
                    .await
                    .and_then(|content| {
                        content
                            .visible_fingerprint()
                            .map_err(|_| riffdb_api_mcp::McpBackendError::InvalidResponse)
                    }),
                riffdb_api_mcp::McpResourceLocator::ContractVersion { .. }
                | riffdb_api_mcp::McpResourceLocator::ApplicationGuidance
                | riffdb_api_mcp::McpResourceLocator::EntitySchema { .. }
                | riffdb_api_mcp::McpResourceLocator::CommandDocumentation { .. }
                | riffdb_api_mcp::McpResourceLocator::Outcome { .. }
                | riffdb_api_mcp::McpResourceLocator::Commit(_)
                | riffdb_api_mcp::McpResourceLocator::Provenance(_) => {
                    Err(riffdb_api_mcp::McpBackendError::InvalidResponse)
                }
            };
            match observation {
                Ok(fingerprint) => Ok(McpSubscribedResourceObservation::Visible(fingerprint)),
                Err(
                    riffdb_api_mcp::McpBackendError::Public(_)
                    | riffdb_api_mcp::McpBackendError::Application(_),
                )
                | Err(riffdb_api_mcp::McpBackendError::TargetUnavailable) => {
                    Ok(McpSubscribedResourceObservation::Hidden)
                }
                Err(riffdb_api_mcp::McpBackendError::Cancelled) => {
                    Err(McpObserverBackendError::Cancelled)
                }
                Err(riffdb_api_mcp::McpBackendError::AuthenticationLost) => {
                    Err(McpObserverBackendError::AuthenticationLost)
                }
                Err(
                    riffdb_api_mcp::McpBackendError::InvalidResponse
                    | riffdb_api_mcp::McpBackendError::RateLimited,
                ) => Err(McpObserverBackendError::RetryNextTick),
            }
        })
    }
}

fn compact_tool_request(
    request_id: [u8; 16],
    request: McpCompactObservationRequest<'_, v1::DiscoveryCatalogFence>,
) -> v1::DiscoverCommandToolsRequest {
    v1::DiscoverCommandToolsRequest {
        request_id: request_id.to_vec(),
        page: Some(v1::PageRequest {
            limit: Some(u32::from(request.limit())),
            cursor: request.cursor().map(|cursor| cursor.to_vec()),
        }),
        prior_fence: request.prior_fence().cloned(),
        representation: v1::DiscoveryRepresentation::CompactObservation as i32,
    }
}

fn compact_resource_request(
    request_id: [u8; 16],
    request: McpCompactObservationRequest<'_, v1::DiscoveryCatalogFence>,
) -> v1::DiscoverResourcesRequest {
    v1::DiscoverResourcesRequest {
        request_id: request_id.to_vec(),
        page: Some(v1::PageRequest {
            limit: Some(u32::from(request.limit())),
            cursor: request.cursor().map(|cursor| cursor.to_vec()),
        }),
        prior_fence: request.prior_fence().cloned(),
        representation: v1::DiscoveryRepresentation::CompactObservation as i32,
        kind: v1::ResourceDiscoveryKind::All as i32,
    }
}

fn compact_tool_result(
    response: v1::DiscoverCommandToolsResponse,
) -> Result<McpCompactObservationResult<v1::DiscoveryCatalogFence>, McpObserverBackendError> {
    match response.result {
        Some(v1::discover_command_tools_response::Result::CatalogUnchanged(fence)) => {
            validate_public_fence(&fence).map_err(retry)?;
            Ok(McpCompactObservationResult::CatalogUnchanged(fence))
        }
        Some(v1::discover_command_tools_response::Result::CompactPage(page)) => {
            let fence = page
                .observed_fence
                .ok_or(McpObserverBackendError::RetryNextTick)?;
            validate_public_fence(&fence).map_err(retry)?;
            let fingerprints = compact_tool_fingerprints(page.items)?;
            let cursor = optional_public_cursor(page.next_cursor).map_err(retry)?;
            McpCompactObservationPage::new(fingerprints, cursor, fence)
                .map(McpCompactObservationResult::Page)
                .map_err(observer_retry)
        }
        Some(v1::discover_command_tools_response::Result::Page(_)) | None => {
            Err(McpObserverBackendError::RetryNextTick)
        }
    }
}

fn compact_resource_result(
    response: v1::DiscoverResourcesResponse,
) -> Result<McpCompactObservationResult<v1::DiscoveryCatalogFence>, McpObserverBackendError> {
    match response.result {
        Some(v1::discover_resources_response::Result::CatalogUnchanged(fence)) => {
            validate_public_fence(&fence).map_err(retry)?;
            Ok(McpCompactObservationResult::CatalogUnchanged(fence))
        }
        Some(v1::discover_resources_response::Result::CompactPage(page)) => {
            let fence = page
                .observed_fence
                .ok_or(McpObserverBackendError::RetryNextTick)?;
            validate_public_fence(&fence).map_err(retry)?;
            let fingerprints = compact_resource_fingerprints(page.items)?;
            let cursor = optional_public_cursor(page.next_cursor).map_err(retry)?;
            McpCompactObservationPage::new(fingerprints, cursor, fence)
                .map(McpCompactObservationResult::Page)
                .map_err(observer_retry)
        }
        Some(v1::discover_resources_response::Result::Page(_)) | None => {
            Err(McpObserverBackendError::RetryNextTick)
        }
    }
}

fn compact_tool_fingerprints(
    items: Vec<v1::CompactCommandToolDiscoveryItem>,
) -> Result<Vec<McpVisibleFingerprint>, McpObserverBackendError> {
    let registry = fixed_tool_registry().map_err(|_| McpObserverBackendError::RetryNextTick)?;
    let mut visible_names = BTreeSet::new();
    let mut fingerprints = Vec::with_capacity(items.len());
    for item in items {
        let fingerprint = match item.item.ok_or(McpObserverBackendError::RetryNextTick)? {
            v1::compact_command_tool_discovery_item::Item::FixedTool(kind) => {
                let kind = v1::FixedToolKind::try_from(kind)
                    .map_err(|_| McpObserverBackendError::RetryNextTick)?;
                let kind = u8::try_from(kind as i32)
                    .map_err(|_| McpObserverBackendError::RetryNextTick)?;
                let definition = registry
                    .tools()
                    .iter()
                    .find(|tool| tool.kind() == kind)
                    .ok_or(McpObserverBackendError::RetryNextTick)?;
                if !visible_names.insert(definition.name().to_owned()) {
                    return Err(McpObserverBackendError::RetryNextTick);
                }
                McpVisibleFingerprint::fixed_tool(kind).map_err(observer_retry)?
            }
            v1::compact_command_tool_discovery_item::Item::CommandTool(tool) => {
                if tool.contract_version == 0 || tool.source_command.is_empty() {
                    return Err(McpObserverBackendError::RetryNextTick);
                }
                format_command_plan_locator_from_public(&tool.contract_lineage, tool.command_id)
                    .map_err(|_| McpObserverBackendError::RetryNextTick)?;
                if !visible_names.insert(tool.tool_name.clone()) {
                    return Err(McpObserverBackendError::RetryNextTick);
                }
                let input = generated_schema_hash(
                    tool.input_schema,
                    tool.command_id,
                    GeneratedSchemaKind::Input,
                )?;
                let outcome = generated_schema_hash(
                    tool.outcome_schema,
                    tool.command_id,
                    GeneratedSchemaKind::Outcome,
                )?;
                McpVisibleFingerprint::command_tool(
                    &tool.tool_name,
                    &tool.source_command,
                    &tool.contract_lineage,
                    tool.contract_version,
                    tool.command_id,
                    &input,
                    &outcome,
                )
                .map_err(observer_retry)?
            }
            v1::compact_command_tool_discovery_item::Item::NamedQueryTool(tool) => {
                if !visible_names.insert(tool.tool_name.clone()) {
                    return Err(McpObserverBackendError::RetryNextTick);
                }
                McpVisibleFingerprint::named_query_tool(
                    &tool.tool_name,
                    &tool.source_query,
                    &tool.contract_lineage,
                    tool.contract_version,
                    &tool.query_module_name,
                    tool.query_module_version,
                    &tool.query_module_hash,
                    &tool.input_schema_hash,
                    &tool.result_schema_hash,
                )
                .map_err(observer_retry)?
            }
        };
        fingerprints.push(fingerprint);
    }
    Ok(fingerprints)
}

fn compact_resource_fingerprints(
    items: Vec<v1::CompactResourceDescriptor>,
) -> Result<Vec<McpVisibleFingerprint>, McpObserverBackendError> {
    let mut visible_uris = BTreeSet::new();
    let mut fingerprints = Vec::with_capacity(items.len());
    for item in items {
        let descriptor: McpResourceDescriptor =
            compact_resource_descriptor_from_public(item).map_err(retry)?;
        if !visible_uris.insert(descriptor.uri().to_owned()) {
            return Err(McpObserverBackendError::RetryNextTick);
        }
        fingerprints
            .push(McpVisibleFingerprint::resource_descriptor(&descriptor).map_err(observer_retry)?);
    }
    Ok(fingerprints)
}

#[derive(Clone, Copy)]
enum GeneratedSchemaKind {
    Input,
    Outcome,
}

fn generated_schema_hash(
    identity: Option<v1::GeneratedSchemaIdentity>,
    command_id: u32,
    kind: GeneratedSchemaKind,
) -> Result<[u8; 32], McpObserverBackendError> {
    let identity = identity.ok_or(McpObserverBackendError::RetryNextTick)?;
    let artifact = identity
        .key
        .and_then(|key| key.artifact)
        .ok_or(McpObserverBackendError::RetryNextTick)?;
    let matches = match (kind, artifact) {
        (GeneratedSchemaKind::Input, v1::schema_artifact_key::Artifact::CommandInputId(id))
        | (
            GeneratedSchemaKind::Outcome,
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(id),
        ) => id == command_id,
        _ => false,
    };
    if !matches {
        return Err(McpObserverBackendError::RetryNextTick);
    }
    identity
        .schema_hash
        .try_into()
        .map_err(|_| McpObserverBackendError::RetryNextTick)
}

fn fresh_request_id() -> Result<[u8; 16], McpObserverBackendError> {
    generate_request_id()
        .map(|request_id| request_id.into_bytes())
        .map_err(|_| McpObserverBackendError::RetryNextTick)
}

fn map_observer_client_error(error: ClientError) -> McpObserverBackendError {
    match error {
        ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated) => {
            McpObserverBackendError::AuthenticationLost
        }
        ClientError::DetailsFree(DetailsFreeStatus::Cancelled) => {
            McpObserverBackendError::Cancelled
        }
        error => {
            let _ = map_client_error(error);
            McpObserverBackendError::RetryNextTick
        }
    }
}

fn retry(_: riffdb_api_mcp::McpBackendError) -> McpObserverBackendError {
    McpObserverBackendError::RetryNextTick
}

const fn observer_retry(_: McpObserverError) -> McpObserverBackendError {
    McpObserverBackendError::RetryNextTick
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence(marker: u8) -> v1::DiscoveryCatalogFence {
        let registry = fixed_tool_registry().expect("registry");
        let schemas = registry.operation_schemas();
        v1::DiscoveryCatalogFence {
            server_generation: vec![marker; 16],
            operation_schemas: Some(v1::OperationSchemaCatalogIdentity {
                command_operation_envelope: Some(v1::OperationSchemaIdentity {
                    schema_id: schemas[0].schema_id().to_owned(),
                    schema_hash: schemas[0].schema_hash_bytes().to_vec(),
                }),
                command_get_outcome_result: Some(v1::OperationSchemaIdentity {
                    schema_id: schemas[1].schema_id().to_owned(),
                    schema_hash: schemas[1].schema_hash_bytes().to_vec(),
                }),
            }),
            state: Some(v1::discovery_catalog_fence::State::NoActiveContract(
                v1::Unit {},
            )),
            history_incarnation: 1,
        }
    }

    #[test]
    fn compact_tool_conversion_uses_common_fingerprints_and_rejects_duplicates() {
        let items = vec![
            v1::CompactCommandToolDiscoveryItem {
                item: Some(v1::compact_command_tool_discovery_item::Item::FixedTool(
                    v1::FixedToolKind::ValidateContract as i32,
                )),
            },
            v1::CompactCommandToolDiscoveryItem {
                item: Some(v1::compact_command_tool_discovery_item::Item::CommandTool(
                    v1::CompactCommandToolDescriptor {
                        tool_name: "riffdb_cmd_orders_place".to_owned(),
                        source_command: "PlaceOrder".to_owned(),
                        contract_lineage: "orders".to_owned(),
                        contract_version: 1,
                        command_id: 7,
                        input_schema: Some(v1::GeneratedSchemaIdentity {
                            key: Some(v1::SchemaArtifactKey {
                                artifact: Some(v1::schema_artifact_key::Artifact::CommandInputId(
                                    7,
                                )),
                            }),
                            schema_hash: vec![1; 32],
                        }),
                        outcome_schema: Some(v1::GeneratedSchemaIdentity {
                            key: Some(v1::SchemaArtifactKey {
                                artifact: Some(
                                    v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(7),
                                ),
                            }),
                            schema_hash: vec![2; 32],
                        }),
                    },
                )),
            },
        ];
        let fingerprints = compact_tool_fingerprints(items.clone()).expect("compact tools");
        assert_eq!(fingerprints.len(), 2);
        assert_eq!(
            fingerprints[1],
            McpVisibleFingerprint::command_tool(
                "riffdb_cmd_orders_place",
                "PlaceOrder",
                "orders",
                1,
                7,
                &[1; 32],
                &[2; 32],
            )
            .expect("common command identity")
        );

        let mut duplicate = items;
        duplicate.push(duplicate[0].clone());
        assert!(compact_tool_fingerprints(duplicate).is_err());
    }

    #[test]
    fn compact_results_require_the_exact_variant_fence_and_cursor() {
        let response = v1::DiscoverResourcesResponse {
            result: Some(v1::discover_resources_response::Result::CompactPage(
                v1::CompactResourceDiscoveryPage {
                    items: vec![v1::CompactResourceDescriptor {
                        resource: Some(v1::compact_resource_descriptor::Resource::ActiveContract(
                            v1::Unit {},
                        )),
                    }],
                    next_cursor: Some(vec![9; 16]),
                    observed_fence: Some(fence(1)),
                },
            )),
        };
        let McpCompactObservationResult::Page(page) =
            compact_resource_result(response).expect("compact page")
        else {
            panic!("page");
        };
        assert_eq!(page.next_cursor(), Some([9; 16]));
        assert_eq!(page.fingerprints().len(), 1);

        assert!(
            compact_resource_result(v1::DiscoverResourcesResponse {
                result: Some(v1::discover_resources_response::Result::Page(
                    v1::ResourceDiscoveryPage {
                        items: Vec::new(),
                        next_cursor: None,
                        observed_fence: Some(fence(1)),
                    },
                )),
            })
            .is_err()
        );
    }

    #[test]
    fn observer_closes_only_on_lost_transport_authentication() {
        assert_eq!(
            map_observer_client_error(ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated)),
            McpObserverBackendError::AuthenticationLost
        );
        assert_eq!(
            map_observer_client_error(ClientError::DetailsFree(
                DetailsFreeStatus::TransportUnavailable
            )),
            McpObserverBackendError::RetryNextTick
        );
    }
}
