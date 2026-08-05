use std::borrow::Cow;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::{Either, select};
use riffdb_errors::{ApplicationError, ApplicationErrorCode, PublicError, PublicErrorKind};
use rmcp::ErrorData as McpError;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestMethod, CallToolRequestParams, CallToolResult, CancelTaskMethod,
    CancelTaskParams, CancelTaskResult, CompleteRequestMethod, CompleteRequestParams,
    CompleteResult, CreateTaskResult, ErrorCode, Extensions, GetPromptRequestMethod,
    GetPromptRequestParams, GetPromptResult, GetTaskMethod, GetTaskParams, GetTaskPayloadMethod,
    GetTaskPayloadParams, GetTaskPayloadResult, GetTaskResult, InitializeRequestParams,
    InitializeResult, ListPromptsRequestMethod, ListPromptsResult, ListResourceTemplatesResult,
    ListResourcesResult, ListTasksMethod, ListTasksResult, ListToolsResult, PaginatedRequestParams,
    ReadResourceRequestParams, ReadResourceResult, RequestId, ResourceContents,
    ServerJsonRpcMessage, ServerResult, SubscribeRequestParams, Tool, ToolAnnotations,
    UnsubscribeRequestParams,
};
use rmcp::service::{NotificationContext, RequestContext, RoleServer};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::bounded_json;
use crate::presentation::{
    BoundedStructuredContent, escape_markdown_block_text, render_application_error,
    render_business_result, render_public_error,
};
use crate::schema::{RiffDbSchemaValidator, compose_command_result_schema};
use crate::{
    McpAdmissionSessionKey, McpCancellationRegistry, McpCancellationSignal, McpInflightLimiter,
    McpInflightPermit, McpObserverError, McpObserverState, McpPostAuthenticationAdmission,
    McpProgressTracker, McpRequestId, McpResourceLocator, McpRiskClass, McpSchemaFailurePhase,
    McpTelemetry, McpTelemetryEvent, McpTransportKind, McpVisibleFingerprint, NoopMcpTelemetry,
    RegistryError, ResourceDefinition, ResourceSurface, SchemaDocument, decode_mcp_cursor,
    encode_mcp_cursor, fixed_tool_registry, initialization_result, parse_resource_locator,
    resource_registry, validate_command_tool_name,
};

/// Exact service discovery page size used by both public MCP list methods.
pub const MCP_DISCOVERY_PAGE_LIMIT: u16 = 500;
/// Maximum policy-visible items returned in one service page.
pub const MAX_MCP_DISCOVERY_PAGE_ITEMS: usize = 500;
/// Maximum bytes retained for one optional dynamic tool title.
pub const MAX_MCP_DYNAMIC_TOOL_TITLE_BYTES: usize = 256;
/// Maximum bytes retained for one optional dynamic tool description.
pub const MAX_MCP_DYNAMIC_TOOL_DESCRIPTION_BYTES: usize = 4_096;
/// Exact safety notice in generated documentation for mutating commands.
pub const MCP_IDEMPOTENT_MUTATION_CANCELLATION_NOTICE: &str = "Cancellation after command submission may not prevent commit. Resolve an uncertain result with the same idempotency key or the returned outcome URI.";

const MCP_SERVICE_DISCOVERY_COMPONENT_MAX_BYTES: usize = 2_621_440;
const MCP_FIXED_SCHEMA_COMPONENT_MAX_BYTES: usize = 1_048_576;
const MCP_ADAPTER_COMPONENT_MAX_BYTES: usize = 524_288;
// Worst-case hosted framing: `data: `, JSON, newline, `id: `, a
// 128-byte accepted event ID, and the terminating blank line.
const MAX_MCP_OUTBOUND_FRAME_OVERHEAD_BYTES: usize = 140;

/// Object-safe future returned by an MCP backend port.
pub type McpBackendFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, McpBackendError>> + Send + 'a>>;

/// Fresh transport-neutral context from which a backend begins one operation.
///
/// Extensions remain typed process-local carriers. They are never serialized,
/// logged, or interpreted by the common handler.
pub struct McpBackendRequest<'a> {
    request_id: McpRequestId,
    source: McpTransportKind,
    #[cfg(feature = "streamable-http")]
    extensions: &'a Extensions,
    #[cfg(not(feature = "streamable-http"))]
    _extensions: std::marker::PhantomData<&'a Extensions>,
    post_authentication_admission: Option<McpPostAuthenticationAdmission>,
    cancellation: Option<McpCancellationSignal>,
}

impl McpBackendRequest<'_> {
    /// Returns the fresh RiffDB request identity.
    #[must_use]
    pub const fn request_id(&self) -> McpRequestId {
        self.request_id
    }

    /// Returns the exact MCP transport source.
    #[must_use]
    pub const fn source(&self) -> McpTransportKind {
        self.source
    }

    /// Returns hosted HTTP admission for charging each protected service call.
    ///
    /// Stdio uses the ordinary authenticated gRPC/service admission path and
    /// therefore never receives this process-local capability.
    #[must_use]
    pub const fn post_authentication_admission(&self) -> Option<&McpPostAuthenticationAdmission> {
        self.post_authentication_admission.as_ref()
    }

    #[cfg(feature = "streamable-http")]
    pub(crate) fn extension<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.extensions.get::<T>()
    }

    /// Returns the optional live cancellation signal for this operation.
    #[must_use]
    pub const fn cancellation(&self) -> Option<&McpCancellationSignal> {
        self.cancellation.as_ref()
    }
}

impl fmt::Debug for McpBackendRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpBackendRequest([REDACTED])")
    }
}

/// One fresh policy-filtered discovery page request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpDiscoveryRequest {
    cursor: Option<[u8; 16]>,
    limit: u16,
}

impl McpDiscoveryRequest {
    fn new(cursor: Option<[u8; 16]>) -> Self {
        Self {
            cursor,
            limit: MCP_DISCOVERY_PAGE_LIMIT,
        }
    }

    /// Returns the opaque API-neutral cursor without interpreting it.
    #[must_use]
    pub const fn cursor(&self) -> Option<[u8; 16]> {
        self.cursor
    }

    /// Returns the exact service page limit.
    #[must_use]
    pub const fn limit(&self) -> u16 {
        self.limit
    }
}

/// Exact MCP list surface requested from resource discovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpResourceDiscoverySurface {
    /// Concrete `resources/list` inventory.
    Concrete,
    /// RFC 6570 `resources/templates/list` inventory.
    Template,
}

/// One complete resource discovery request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpResourceDiscoveryRequest {
    page: McpDiscoveryRequest,
    surface: McpResourceDiscoverySurface,
}

impl McpResourceDiscoveryRequest {
    fn new(cursor: Option<[u8; 16]>, surface: McpResourceDiscoverySurface) -> Self {
        Self {
            page: McpDiscoveryRequest::new(cursor),
            surface,
        }
    }

    /// Returns the exact bounded page request.
    #[must_use]
    pub const fn page(&self) -> McpDiscoveryRequest {
        self.page
    }

    /// Returns the sole requested list surface.
    #[must_use]
    pub const fn surface(&self) -> McpResourceDiscoverySurface {
        self.surface
    }
}

/// Closed dynamic-tool annotations supplied by the policy-filtered backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpDynamicToolAnnotations {
    read_only: bool,
    destructive: bool,
    idempotent: bool,
    open_world: bool,
}

impl McpDynamicToolAnnotations {
    /// Creates the four exact MCP hint values without defaults.
    #[must_use]
    pub const fn new(
        read_only: bool,
        destructive: bool,
        idempotent: bool,
        open_world: bool,
    ) -> Self {
        Self {
            read_only,
            destructive,
            idempotent,
            open_world,
        }
    }

    fn to_mcp(self) -> ToolAnnotations {
        ToolAnnotations::from_raw(
            None,
            Some(self.read_only),
            Some(self.destructive),
            Some(self.idempotent),
            Some(self.open_world),
        )
    }
}

/// One checked compiler-owned dynamic command tool.
#[derive(Clone, Debug)]
pub struct McpDynamicToolDefinition {
    name: String,
    title: Option<String>,
    description: Option<String>,
    annotations: McpDynamicToolAnnotations,
    input_schema: SchemaDocument,
    outcome_schema: SchemaDocument,
    result_schema: SchemaDocument,
}

impl McpDynamicToolDefinition {
    /// Builds the exact presentation available from policy-filtered discovery.
    ///
    /// Public and API-neutral descriptors intentionally carry no free-form
    /// title, description, or execution-risk text. Both transports use this
    /// constructor so those absent fields and conservative annotations cannot
    /// drift.
    pub fn from_discovered_command(
        name: impl Into<String>,
        input_schema: SchemaDocument,
        outcome_schema: SchemaDocument,
    ) -> Result<Self, McpHandlerContractError> {
        Self::new(
            name,
            None,
            None,
            McpDynamicToolAnnotations::new(false, false, true, false),
            input_schema,
            outcome_schema,
        )
    }

    /// Builds one compiler-owned read-only named-query presentation.
    pub fn from_discovered_query(
        name: impl Into<String>,
        input_schema: SchemaDocument,
        result_schema: SchemaDocument,
    ) -> Result<Self, McpHandlerContractError> {
        let name = name.into();
        if !valid_named_query_tool_name(&name) {
            return Err(McpHandlerContractError);
        }
        Ok(Self {
            name,
            title: None,
            description: None,
            annotations: McpDynamicToolAnnotations::new(true, false, true, false),
            input_schema,
            outcome_schema: result_schema.clone(),
            result_schema,
        })
    }

    /// Checks exact name, presentation bounds, and mechanical result composition.
    pub fn new(
        name: impl Into<String>,
        title: Option<String>,
        description: Option<String>,
        annotations: McpDynamicToolAnnotations,
        input_schema: SchemaDocument,
        outcome_schema: SchemaDocument,
    ) -> Result<Self, McpHandlerContractError> {
        let name = name.into();
        validate_command_tool_name(&name).map_err(|_| McpHandlerContractError)?;
        if title
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_MCP_DYNAMIC_TOOL_TITLE_BYTES)
            || description.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > MAX_MCP_DYNAMIC_TOOL_DESCRIPTION_BYTES
            })
        {
            return Err(McpHandlerContractError);
        }
        let operation_envelope = fixed_tool_registry()
            .map_err(|_| McpHandlerContractError)?
            .operation_schemas()
            .first()
            .ok_or(McpHandlerContractError)?;
        let result_schema = compose_command_result_schema(&outcome_schema, operation_envelope)
            .map_err(|_| McpHandlerContractError)?;
        Ok(Self {
            name,
            title,
            description,
            annotations,
            input_schema,
            outcome_schema,
            result_schema,
        })
    }

    /// Returns the exact compiler-owned name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the exact compiler input schema.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaDocument {
        &self.input_schema
    }

    /// Returns the exact compiler declared-outcome union.
    #[must_use]
    pub const fn outcome_schema(&self) -> &SchemaDocument {
        &self.outcome_schema
    }

    /// Returns the mechanically composed advertised result schema.
    #[must_use]
    pub const fn result_schema(&self) -> &SchemaDocument {
        &self.result_schema
    }

    fn to_mcp_tool(&self) -> Tool {
        let mut tool = Tool::new_with_raw(
            self.name.clone(),
            self.description.clone().map(Cow::Owned),
            Arc::new(self.input_schema.json_object()),
        )
        .with_raw_output_schema(Arc::new(self.result_schema.json_object()))
        .with_annotations(self.annotations.to_mcp());
        tool.title.clone_from(&self.title);
        tool
    }
}

/// One policy-visible tool item in canonical fixed-then-dynamic order.
#[derive(Clone, Debug)]
pub enum McpToolDiscoveryItem {
    /// One exact accepted fixed-tool tag in `1..=30`.
    Fixed(u8),
    /// One compiler-owned dynamic command tool.
    Dynamic(Box<McpDynamicToolDefinition>),
}

/// One bounded policy-filtered tool page.
#[derive(Clone, Debug)]
pub struct McpToolPage {
    items: Vec<McpToolDiscoveryItem>,
    next_cursor: Option<[u8; 16]>,
}

impl McpToolPage {
    /// Checks bounds, ordering, uniqueness, and terminal cursor shape.
    pub fn new(
        items: Vec<McpToolDiscoveryItem>,
        next_cursor: Option<[u8; 16]>,
    ) -> Result<Self, McpHandlerContractError> {
        if items.len() > MAX_MCP_DISCOVERY_PAGE_ITEMS
            || items.is_empty() && next_cursor.is_some()
            || !tool_items_are_canonical(&items)
            || !tool_page_fits_component_budgets(&items, next_cursor)
        {
            return Err(McpHandlerContractError);
        }
        Ok(Self { items, next_cursor })
    }

    /// Returns the complete checked page.
    #[must_use]
    pub fn items(&self) -> &[McpToolDiscoveryItem] {
        &self.items
    }

    /// Returns the opaque continuation cursor.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<[u8; 16]> {
        self.next_cursor
    }
}

fn tool_items_are_canonical(items: &[McpToolDiscoveryItem]) -> bool {
    let mut fixed = None;
    let mut dynamic: Option<&str> = None;
    for item in items {
        match item {
            McpToolDiscoveryItem::Fixed(tag) if dynamic.is_none() => {
                if !(1..=30).contains(tag) || fixed.is_some_and(|prior| prior >= *tag) {
                    return false;
                }
                fixed = Some(*tag);
            }
            McpToolDiscoveryItem::Fixed(_) => return false,
            McpToolDiscoveryItem::Dynamic(tool) => {
                if dynamic.is_some_and(|prior| prior >= tool.name()) {
                    return false;
                }
                dynamic = Some(tool.name());
            }
        }
    }
    true
}

fn tool_page_fits_component_budgets(
    items: &[McpToolDiscoveryItem],
    next_cursor: Option<[u8; 16]>,
) -> bool {
    let Ok(registry) = fixed_tool_registry() else {
        return false;
    };
    if registry.fixed_schema_bytes() > MCP_FIXED_SCHEMA_COMPONENT_MAX_BYTES {
        return false;
    }
    let Ok(schema_components) = tool_page_schema_components(items, registry) else {
        return false;
    };
    if schema_components.fixed > MCP_FIXED_SCHEMA_COMPONENT_MAX_BYTES
        || schema_components.service > MCP_SERVICE_DISCOVERY_COMPONENT_MAX_BYTES
    {
        return false;
    }

    let Ok(tools) = materialize_tool_items(items) else {
        return false;
    };
    let result = ListToolsResult {
        meta: None,
        next_cursor: next_cursor.map(encode_mcp_cursor),
        tools,
    };
    let request_id = RequestId::String(
        "r".repeat(crate::MAX_MCP_REQUEST_ID_BYTES.saturating_sub(2))
            .into(),
    );
    let response =
        ServerJsonRpcMessage::response(ServerResult::ListToolsResult(result), request_id);
    let Ok(encoded_bytes) =
        bounded_json::encoded_len(&response, crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES)
    else {
        return false;
    };
    let Some(framed_bytes) = encoded_bytes.checked_add(MAX_MCP_OUTBOUND_FRAME_OVERHEAD_BYTES)
    else {
        return false;
    };
    // Credit only schema bytes covered by the service or fixed-schema
    // components. Composed schemas retain their service charge when their
    // container changes; descriptor metadata, JSON-RPC structure, and
    // worst-case transport framing remain charged to the adapter component.
    let Some(adapter_bytes) = framed_bytes.checked_sub(schema_components.emitted_credit) else {
        return false;
    };
    framed_bytes <= crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES
        && adapter_bytes <= MCP_ADAPTER_COMPONENT_MAX_BYTES
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ToolPageSchemaComponents {
    fixed: usize,
    service: usize,
    emitted_credit: usize,
}

fn tool_page_schema_components(
    items: &[McpToolDiscoveryItem],
    registry: &crate::FixedToolRegistry,
) -> Result<ToolPageSchemaComponents, McpHandlerContractError> {
    // Every full service discovery page carries the complete operation catalog,
    // including an empty or fixed-only page. The backend remains responsible
    // for the complete checked service-response charge; this independently
    // checks the retained source bodies needed by MCP presentation.
    let mut components = ToolPageSchemaComponents {
        fixed: 0,
        service: 0,
        emitted_credit: 0,
    };
    for schema in registry.operation_schemas() {
        checked_add_component(&mut components.service, schema.canonical_bytes())?;
    }

    for item in items {
        match item {
            McpToolDiscoveryItem::Fixed(tag) => {
                let definition = registry
                    .tools()
                    .get(usize::from(*tag).saturating_sub(1))
                    .filter(|definition| definition.kind() == *tag)
                    .ok_or(McpHandlerContractError)?;
                for schema in [definition.input_schema(), definition.result_schema()] {
                    checked_add_component(
                        &mut components.emitted_credit,
                        schema.canonical_bytes(),
                    )?;
                    if !registry.operation_schemas().iter().any(|operation| {
                        operation.schema_id() == schema.schema_id()
                            && operation.schema_hash_bytes() == schema.schema_hash_bytes()
                    }) {
                        checked_add_component(&mut components.fixed, schema.canonical_bytes())?;
                    }
                }
            }
            McpToolDiscoveryItem::Dynamic(tool) => {
                checked_add_component(
                    &mut components.service,
                    tool.input_schema.canonical_bytes(),
                )?;
                checked_add_component(
                    &mut components.service,
                    tool.outcome_schema.canonical_bytes(),
                )?;
                checked_add_component(
                    &mut components.emitted_credit,
                    tool.input_schema.canonical_bytes(),
                )?;
                checked_add_component(
                    &mut components.emitted_credit,
                    tool.result_schema.canonical_bytes(),
                )?;
            }
        }
    }
    Ok(components)
}

fn checked_add_component(total: &mut usize, value: usize) -> Result<(), McpHandlerContractError> {
    *total = total.checked_add(value).ok_or(McpHandlerContractError)?;
    Ok(())
}

fn materialize_tool_items(
    items: &[McpToolDiscoveryItem],
) -> Result<Vec<Tool>, McpHandlerContractError> {
    let registry = fixed_tool_registry().map_err(|_| McpHandlerContractError)?;
    let mut tools = Vec::with_capacity(items.len());
    for item in items {
        match item {
            McpToolDiscoveryItem::Fixed(tag) => {
                let definition = registry
                    .tools()
                    .get(usize::from(*tag).saturating_sub(1))
                    .filter(|definition| definition.kind() == *tag)
                    .ok_or(McpHandlerContractError)?;
                tools.push(definition.to_mcp_tool());
            }
            McpToolDiscoveryItem::Dynamic(tool) => tools.push(tool.to_mcp_tool()),
        }
    }
    Ok(tools)
}

/// One checked policy-visible resource descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpResourceDescriptor {
    descriptor_branch: String,
    uri: String,
}

impl McpResourceDescriptor {
    /// Checks the exact accepted branch and its URI or template shape.
    pub fn new(
        descriptor_branch: impl Into<String>,
        uri: impl Into<String>,
    ) -> Result<Self, McpHandlerContractError> {
        let descriptor_branch = descriptor_branch.into();
        let uri = uri.into();
        let definition = resource_registry()
            .map_err(|_| McpHandlerContractError)?
            .by_descriptor_branch(&descriptor_branch)
            .ok_or(McpHandlerContractError)?;
        validate_descriptor_uri(definition, &uri)?;
        Ok(Self {
            descriptor_branch,
            uri,
        })
    }

    /// Returns the exact accepted structural branch.
    #[must_use]
    pub fn descriptor_branch(&self) -> &str {
        &self.descriptor_branch
    }

    /// Returns the exact canonical URI or template.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

/// One bounded resource page on exactly one MCP list surface.
#[derive(Clone, Debug)]
pub struct McpResourcePage {
    surface: McpResourceDiscoverySurface,
    items: Vec<McpResourceDescriptor>,
    next_cursor: Option<[u8; 16]>,
}

impl McpResourcePage {
    /// Checks item count, surface ownership, uniqueness, and terminal cursor shape.
    ///
    /// Item order is the service-owned canonical descriptor order and is
    /// preserved verbatim; URI lexical order is not a protocol invariant.
    pub fn new(
        surface: McpResourceDiscoverySurface,
        items: Vec<McpResourceDescriptor>,
        next_cursor: Option<[u8; 16]>,
    ) -> Result<Self, McpHandlerContractError> {
        let mut seen_uris = BTreeSet::new();
        if items.len() > MAX_MCP_DISCOVERY_PAGE_ITEMS
            || items.is_empty() && next_cursor.is_some()
            || items
                .iter()
                .any(|item| !seen_uris.insert(item.uri.as_str()))
        {
            return Err(McpHandlerContractError);
        }
        for item in &items {
            let definition = resource_registry()
                .map_err(|_| McpHandlerContractError)?
                .by_descriptor_branch(&item.descriptor_branch)
                .ok_or(McpHandlerContractError)?;
            if resource_surface(definition.surface()) != surface {
                return Err(McpHandlerContractError);
            }
        }
        Ok(Self {
            surface,
            items,
            next_cursor,
        })
    }

    /// Returns the sole page surface.
    #[must_use]
    pub const fn surface(&self) -> McpResourceDiscoverySurface {
        self.surface
    }

    /// Returns the complete checked page.
    #[must_use]
    pub fn items(&self) -> &[McpResourceDescriptor] {
        &self.items
    }

    /// Returns the opaque continuation cursor.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<[u8; 16]> {
        self.next_cursor
    }
}

/// One invocation target already selected by exact name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpInvocationTarget {
    /// Exact fixed registry tag and name.
    Fixed {
        /// Stable fixed tool tag.
        tag: u8,
        /// Exact accepted name.
        name: String,
    },
    /// Exact compiler-owned dynamic name.
    Dynamic(String),
}

/// One fully bounded invocation after structural validation.
#[derive(Clone)]
pub struct McpToolInvocation {
    target: McpInvocationTarget,
    arguments: McpToolArguments,
}

impl McpToolInvocation {
    /// Returns the exact invocation target.
    #[must_use]
    pub const fn target(&self) -> &McpInvocationTarget {
        &self.target
    }

    /// Returns the schema-validated argument object.
    #[must_use]
    pub const fn arguments(&self) -> &McpToolArguments {
        &self.arguments
    }
}

impl fmt::Debug for McpToolInvocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpToolInvocation([REDACTED])")
    }
}

/// Opaque schema-validated MCP argument object.
#[derive(Clone, Eq, PartialEq)]
pub struct McpToolArguments(Value);

impl McpToolArguments {
    /// Deserializes into one adapter-owned checked request shape.
    ///
    /// External backends can use local Serde DTOs without depending on
    /// `serde_json` or gaining access to an unvalidated mutable value.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, McpHandlerContractError> {
        serde_json::from_value(self.0.clone()).map_err(|_| McpHandlerContractError)
    }

    pub(crate) fn from_validated(value: Value) -> Self {
        Self(value)
    }
}

impl fmt::Debug for McpToolArguments {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpToolArguments([REDACTED])")
    }
}

/// Opaque backend result object awaiting advertised-schema validation.
#[derive(Clone, Eq, PartialEq)]
pub struct McpToolResult(Value);

impl McpToolResult {
    /// Serializes and bounds one adapter-owned result DTO.
    pub fn from_serializable<T: Serialize>(value: &T) -> Result<Self, McpHandlerContractError> {
        checked_json_object(value).map(Self)
    }

    /// Parses and bounds one complete UTF-8 JSON result object.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, McpHandlerContractError> {
        checked_json_object_bytes(bytes).map(Self)
    }

    fn into_value(self) -> Value {
        self.0
    }
}

impl fmt::Debug for McpToolResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpToolResult([REDACTED])")
    }
}

/// Opaque already-obligated JSON resource document.
#[derive(Clone, Eq, PartialEq)]
pub struct McpResourceJson(Value);

impl McpResourceJson {
    /// Serializes and bounds one adapter-owned resource DTO.
    pub fn from_serializable<T: Serialize>(value: &T) -> Result<Self, McpHandlerContractError> {
        checked_json_object(value).map(Self)
    }

    /// Parses and bounds one complete UTF-8 JSON resource object.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, McpHandlerContractError> {
        checked_json_object_bytes(bytes).map(Self)
    }

    fn as_value(&self) -> &Value {
        &self.0
    }

    fn into_value(self) -> Value {
        self.0
    }
}

impl fmt::Debug for McpResourceJson {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpResourceJson([REDACTED])")
    }
}

/// Subscription authorization action requested from the API-neutral backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpSubscriptionAction {
    /// Authorize and retain an exact URI.
    Subscribe,
    /// Authorize and remove an exact URI.
    Unsubscribe,
}

/// One fresh resource subscription authorization request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpSubscriptionRequest {
    locator: McpResourceLocator,
    action: McpSubscriptionAction,
}

impl McpSubscriptionRequest {
    /// Returns the completely parsed canonical locator.
    #[must_use]
    pub const fn locator(&self) -> &McpResourceLocator {
        &self.locator
    }

    /// Returns the requested state transition.
    #[must_use]
    pub const fn action(&self) -> McpSubscriptionAction {
        self.action
    }
}

/// One fresh policy-filtered resource read request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpResourceReadRequest {
    locator: McpResourceLocator,
}

impl McpResourceReadRequest {
    /// Returns the completely parsed canonical locator.
    #[must_use]
    pub const fn locator(&self) -> &McpResourceLocator {
        &self.locator
    }
}

/// One bounded generated Markdown document with static caller-owned structure.
#[derive(Clone, Eq, PartialEq)]
pub struct McpMarkdownDocument(String);

impl McpMarkdownDocument {
    /// Borrows the complete checked Markdown text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Debug for McpMarkdownDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpMarkdownDocument([REDACTED])")
    }
}

/// Builder that keeps Markdown structure separate from escaped dynamic text.
#[derive(Default)]
pub struct McpMarkdownBuilder {
    document: String,
}

impl McpMarkdownBuilder {
    /// Creates an empty generated document.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            document: String::new(),
        }
    }

    /// Adds a level-one through level-six heading with escaped dynamic text.
    pub fn push_heading(
        &mut self,
        level: u8,
        text: &str,
    ) -> Result<&mut Self, McpHandlerContractError> {
        if !(1..=6).contains(&level) {
            return Err(McpHandlerContractError);
        }
        self.begin_block()?;
        for _ in 0..level {
            self.push_fragment("#")?;
        }
        self.push_fragment(" ")?;
        self.push_dynamic_text(text)?;
        self.push_fragment("\n")?;
        Ok(self)
    }

    /// Adds one paragraph whose dynamic text cannot create Markdown structure.
    pub fn push_paragraph(&mut self, text: &str) -> Result<&mut Self, McpHandlerContractError> {
        self.begin_block()?;
        self.push_dynamic_text(text)?;
        self.push_fragment("\n")?;
        Ok(self)
    }

    /// Adds escaped dynamic text as an indented preformatted data block.
    ///
    /// Indentation avoids a caller-controlled closing fence. Newlines retain
    /// their data-line boundaries, while controls and directional formatting
    /// characters are still removed or replaced.
    pub fn push_preformatted(&mut self, text: &str) -> Result<&mut Self, McpHandlerContractError> {
        if text.is_empty() || text.len() > crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES {
            return Err(McpHandlerContractError);
        }
        self.begin_block()?;
        for line in text.lines() {
            self.push_fragment("    ")?;
            if !line.is_empty() {
                self.push_preformatted_line(line)?;
            }
            self.push_fragment("\n")?;
        }
        Ok(self)
    }

    /// Adds the accepted static notice for an idempotent mutating command.
    pub fn push_idempotent_mutation_cancellation_notice(
        &mut self,
    ) -> Result<&mut Self, McpHandlerContractError> {
        self.begin_block()?;
        self.push_fragment(MCP_IDEMPOTENT_MUTATION_CANCELLATION_NOTICE)?;
        self.push_fragment("\n")?;
        Ok(self)
    }

    /// Finishes a nonempty bounded document.
    pub fn finish(self) -> Result<McpMarkdownDocument, McpHandlerContractError> {
        if self.document.is_empty() || self.document.len() > crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES {
            return Err(McpHandlerContractError);
        }
        Ok(McpMarkdownDocument(self.document))
    }

    fn begin_block(&mut self) -> Result<(), McpHandlerContractError> {
        if !self.document.is_empty() {
            self.push_fragment("\n")?;
        }
        Ok(())
    }

    fn push_dynamic_text(&mut self, text: &str) -> Result<(), McpHandlerContractError> {
        let escaped = escape_markdown_block_text(text).map_err(|_| McpHandlerContractError)?;
        self.push_fragment(&escaped)
    }

    fn push_preformatted_line(&mut self, text: &str) -> Result<(), McpHandlerContractError> {
        for character in text.chars() {
            if character.is_control() {
                self.push_fragment(" ")?;
            } else if crate::presentation::is_directional_control(character) {
                self.push_fragment("\u{fffd}")?;
            } else {
                let mut encoded = [0_u8; 4];
                self.push_fragment(character.encode_utf8(&mut encoded))?;
            }
        }
        Ok(())
    }

    fn push_fragment(&mut self, fragment: &str) -> Result<(), McpHandlerContractError> {
        let next = self
            .document
            .len()
            .checked_add(fragment.len())
            .ok_or(McpHandlerContractError)?;
        if next > crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES {
            return Err(McpHandlerContractError);
        }
        self.document.push_str(fragment);
        Ok(())
    }
}

impl fmt::Debug for McpMarkdownBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpMarkdownBuilder([REDACTED])")
    }
}

/// Redacted already-obligated resource content returned by a backend.
#[derive(Clone, Eq, PartialEq)]
pub enum McpResourceBody {
    /// One JSON or JSON Schema document.
    Json(McpResourceJson),
    /// One structurally generated, injection-safe Markdown document.
    Markdown(McpMarkdownDocument),
}

impl fmt::Debug for McpResourceBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpResourceBody([REDACTED])")
    }
}

/// One checked policy-filtered resource read result.
#[derive(Clone, Eq, PartialEq)]
pub struct McpResourceContent {
    descriptor_branch: String,
    uri: String,
    body: McpResourceBody,
}

impl McpResourceContent {
    /// Checks branch, canonical URI, MIME-compatible body, and response bound.
    pub fn new(
        descriptor_branch: impl Into<String>,
        uri: impl Into<String>,
        body: McpResourceBody,
    ) -> Result<Self, McpHandlerContractError> {
        let descriptor_branch = descriptor_branch.into();
        let uri = uri.into();
        let locator = parse_resource_locator(&uri).map_err(|_| McpHandlerContractError)?;
        if locator_descriptor_branch(&locator) != descriptor_branch {
            return Err(McpHandlerContractError);
        }
        let definition = resource_registry()
            .map_err(|_| McpHandlerContractError)?
            .by_descriptor_branch(&descriptor_branch)
            .ok_or(McpHandlerContractError)?;
        let content_bytes = match (&body, definition.mime_type()) {
            (McpResourceBody::Json(value), "application/json" | "application/schema+json") => {
                bounded_json::encoded_len(value.as_value(), crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES)
                    .map_err(|_| McpHandlerContractError)?
            }
            (McpResourceBody::Markdown(document), "text/markdown") => document.as_str().len(),
            _ => return Err(McpHandlerContractError),
        };
        if content_bytes > crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES {
            return Err(McpHandlerContractError);
        }
        Ok(Self {
            descriptor_branch,
            uri,
            body,
        })
    }

    /// Reduces already-obligated subscribed content to the common compact identity.
    pub fn visible_fingerprint(
        &self,
    ) -> Result<crate::McpVisibleFingerprint, McpHandlerContractError> {
        let canonical_content = match &self.body {
            McpResourceBody::Json(value) => {
                bounded_json::to_vec(value.as_value(), crate::MAX_MCP_COMPACT_FINGERPRINT_BYTES)
                    .map_err(|_| McpHandlerContractError)?
            }
            McpResourceBody::Markdown(document) => {
                if document.as_str().len() > crate::MAX_MCP_COMPACT_FINGERPRINT_BYTES {
                    return Err(McpHandlerContractError);
                }
                document.as_str().as_bytes().to_vec()
            }
        };
        crate::McpVisibleFingerprint::subscribed_resource_content(&self.uri, &canonical_content)
            .map_err(|_| McpHandlerContractError)
    }

    fn into_mcp(self) -> Result<ResourceContents, McpHandlerContractError> {
        let definition = resource_registry()
            .map_err(|_| McpHandlerContractError)?
            .by_descriptor_branch(&self.descriptor_branch)
            .ok_or(McpHandlerContractError)?;
        let text = match self.body {
            McpResourceBody::Json(value) => {
                bounded_json::to_string(&value.into_value(), crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES)
                    .map_err(|_| McpHandlerContractError)?
            }
            McpResourceBody::Markdown(document) => document.into_string(),
        };
        Ok(ResourceContents::text(text, self.uri).with_mime_type(definition.mime_type()))
    }
}

impl fmt::Debug for McpResourceContent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpResourceContent([REDACTED])")
    }
}

/// API-neutral, policy-filtered backend used by both MCP transports.
///
/// Implementations own fresh UUIDv7 generation and every service/auth boundary.
/// A discovery result grants no authority: invocation, read, and subscription
/// methods must repeat current authorization and obligations. Dynamic
/// resolution must collapse unknown, stale, and unauthorized names to
/// [`McpBackendError::TargetUnavailable`].
pub trait McpBackend: Send + Sync + 'static {
    /// Transport-specific operation context with no MCP wire lifetime.
    type Invocation: Send + Sync;

    /// Produces one fresh checked RiffDB request identity.
    fn next_request_id(&self) -> Result<McpRequestId, crate::RequestIdSourceError>;

    /// Begins one operation from fresh transport context and current identity.
    ///
    /// Implementations fail closed when an authenticated hosted carrier,
    /// retained stdio metadata, post-authentication limiter, or other required
    /// operation context is unavailable.
    fn begin_invocation(
        &self,
        request: McpBackendRequest<'_>,
    ) -> Result<Self::Invocation, PublicError>;

    /// Returns one full policy-filtered tool page.
    fn discover_tools<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpDiscoveryRequest,
    ) -> McpBackendFuture<'a, McpToolPage>;

    /// Resolves one dynamic name under fresh authorization for pre-validation.
    fn resolve_dynamic_tool<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        exact_name: String,
    ) -> McpBackendFuture<'a, McpDynamicToolDefinition>;

    /// Invokes one already schema-validated target under fresh authorization.
    fn invoke_tool<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpToolInvocation,
    ) -> McpBackendFuture<'a, McpToolResult>;

    /// Returns one full policy-filtered resource page on exactly one surface.
    fn discover_resources<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpResourceDiscoveryRequest,
    ) -> McpBackendFuture<'a, McpResourcePage>;

    /// Reads one canonical locator through its named API-neutral algorithm.
    fn read_resource<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpResourceReadRequest,
    ) -> McpBackendFuture<'a, McpResourceContent>;

    /// Repeats current authorization and returns the exact resource baseline
    /// before a local subscription transition.
    fn authorize_subscription<'a>(
        &'a self,
        invocation: &'a Self::Invocation,
        request: McpSubscriptionRequest,
    ) -> McpBackendFuture<'a, McpVisibleFingerprint>;
}

/// Closed backend failure classification.
pub enum McpBackendError {
    /// A public-safe application failure after a known operation was selected.
    Public(PublicError),
    /// A bounded symbolic application failure.
    Application(Box<ApplicationError>),
    /// Fresh transport authentication no longer proves the current session.
    AuthenticationLost,
    /// Unknown, stale, hidden, unauthorized, or otherwise unavailable target.
    TargetUnavailable,
    /// Backend response violated the checked adapter contract.
    InvalidResponse,
    /// Hosted post-authentication admission denied the protected operation.
    RateLimited,
    /// Current request cancellation stopped nondurable backend work.
    Cancelled,
}

impl fmt::Debug for McpBackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Public(error) => formatter
                .debug_tuple("Public")
                .field(&error.code())
                .finish(),
            Self::Application(error) => formatter
                .debug_tuple("Application")
                .field(&error.code())
                .finish(),
            Self::AuthenticationLost => formatter.write_str("AuthenticationLost"),
            Self::TargetUnavailable => formatter.write_str("TargetUnavailable"),
            Self::InvalidResponse => formatter.write_str("InvalidResponse"),
            Self::RateLimited => formatter.write_str("RateLimited"),
            Self::Cancelled => formatter.write_str("Cancelled"),
        }
    }
}

impl fmt::Display for McpBackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP backend operation failed")
    }
}

impl Error for McpBackendError {}

/// A backend DTO, schema identity, ordering, or presentation bound was invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpHandlerContractError;

impl fmt::Display for McpHandlerContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP handler contract is invalid")
    }
}

impl Error for McpHandlerContractError {}

struct McpSessionTelemetryLease {
    telemetry: Arc<dyn McpTelemetry>,
    transport: McpTransportKind,
}

impl McpSessionTelemetryLease {
    fn new(telemetry: Arc<dyn McpTelemetry>, transport: McpTransportKind) -> Self {
        telemetry.record(McpTelemetryEvent::SessionOpened { transport });
        Self {
            telemetry,
            transport,
        }
    }
}

impl Drop for McpSessionTelemetryLease {
    fn drop(&mut self) {
        self.telemetry.record(McpTelemetryEvent::SessionClosed {
            transport: self.transport,
        });
    }
}

/// One transport-neutral MCP server over an API-neutral policy-filtered backend.
pub struct RiffDbMcpServer<B> {
    backend: B,
    source: McpTransportKind,
    telemetry: Arc<dyn McpTelemetry>,
    _session_telemetry: McpSessionTelemetryLease,
    observer: Arc<McpObserverState>,
    cancellations: Arc<McpCancellationRegistry>,
    inflight: Arc<McpInflightLimiter>,
    admission_session: McpAdmissionSessionKey,
}

impl<B> RiffDbMcpServer<B>
where
    B: McpBackend,
{
    /// Creates the sole stdio session with private bounded process state.
    #[must_use]
    pub fn new_stdio(backend: B) -> Self {
        Self::new_stdio_with_telemetry(backend, Arc::new(NoopMcpTelemetry))
    }

    /// Creates the sole stdio session with a closed semantic telemetry sink.
    #[must_use]
    pub fn new_stdio_with_telemetry(backend: B, telemetry: Arc<dyn McpTelemetry>) -> Self {
        Self::with_shared_admission_and_telemetry(
            backend,
            McpTransportKind::Stdio,
            Arc::new(McpInflightLimiter::new()),
            McpAdmissionSessionKey::stdio(),
            telemetry,
        )
    }

    /// Creates one session over a caller-owned server-wide admission boundary.
    #[must_use]
    pub fn with_shared_admission(
        backend: B,
        source: McpTransportKind,
        inflight: Arc<McpInflightLimiter>,
        admission_session: McpAdmissionSessionKey,
    ) -> Self {
        Self::with_shared_admission_and_telemetry(
            backend,
            source,
            inflight,
            admission_session,
            Arc::new(NoopMcpTelemetry),
        )
    }

    /// Creates one session with server-wide admission and semantic telemetry.
    #[must_use]
    pub fn with_shared_admission_and_telemetry(
        backend: B,
        source: McpTransportKind,
        inflight: Arc<McpInflightLimiter>,
        admission_session: McpAdmissionSessionKey,
        telemetry: Arc<dyn McpTelemetry>,
    ) -> Self {
        let observer = Arc::new(McpObserverState::with_telemetry(Arc::clone(&telemetry)));
        Self::with_session_state_and_telemetry(
            backend,
            source,
            observer,
            Arc::new(McpCancellationRegistry::new()),
            inflight,
            admission_session,
            telemetry,
        )
    }

    /// Creates one server over transport-owned bounded session state.
    #[must_use]
    pub fn with_session_state(
        backend: B,
        source: McpTransportKind,
        observer: Arc<McpObserverState>,
        cancellations: Arc<McpCancellationRegistry>,
        inflight: Arc<McpInflightLimiter>,
        admission_session: McpAdmissionSessionKey,
    ) -> Self {
        Self::with_session_state_and_telemetry(
            backend,
            source,
            observer,
            cancellations,
            inflight,
            admission_session,
            Arc::new(NoopMcpTelemetry),
        )
    }

    /// Creates one server over transport-owned state and semantic telemetry.
    #[must_use]
    pub fn with_session_state_and_telemetry(
        backend: B,
        source: McpTransportKind,
        observer: Arc<McpObserverState>,
        cancellations: Arc<McpCancellationRegistry>,
        inflight: Arc<McpInflightLimiter>,
        admission_session: McpAdmissionSessionKey,
        telemetry: Arc<dyn McpTelemetry>,
    ) -> Self {
        Self {
            backend,
            source,
            telemetry: Arc::clone(&telemetry),
            _session_telemetry: McpSessionTelemetryLease::new(telemetry, source),
            observer,
            cancellations,
            inflight,
            admission_session,
        }
    }

    /// Borrows the API-neutral backend.
    #[must_use]
    pub const fn backend(&self) -> &B {
        &self.backend
    }

    /// Borrows this session's bounded observer state.
    #[must_use]
    pub const fn observer(&self) -> &Arc<McpObserverState> {
        &self.observer
    }

    #[cfg(feature = "stdio")]
    pub(crate) const fn cancellations(&self) -> &Arc<McpCancellationRegistry> {
        &self.cancellations
    }

    /// Executes exact policy-filtered `tools/list` pagination.
    async fn handle_list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        extensions: &Extensions,
        cancellation: Option<McpCancellationSignal>,
    ) -> Result<ListToolsResult, McpError> {
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let cursor = decode_optional_cursor(request)?;
        let _permit = self.admit()?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let invocation = self
            .begin_invocation(extensions, cancellation.clone())
            .map_err(discovery_begin_error)?;
        let page = await_backend_with_cancellation(
            cancellation.as_ref(),
            self.backend
                .discover_tools(&invocation, McpDiscoveryRequest::new(cursor)),
        )
        .await
        .map_err(discovery_error)?;
        let tools = materialize_tool_items(&page.items).map_err(|_| internal_error())?;
        let result = ListToolsResult {
            meta: None,
            next_cursor: page.next_cursor.map(encode_mcp_cursor),
            tools,
        };
        enforce_outbound_bound(&result)?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        Ok(result)
    }

    /// Executes one fixed or dynamic tool call.
    async fn handle_call_tool_with_cancellation(
        &self,
        request: CallToolRequestParams,
        extensions: &Extensions,
        cancellation: Option<McpCancellationSignal>,
    ) -> Result<CallToolResult, McpError> {
        if request.task.is_some() || request.name.len() > 128 {
            self.telemetry.record(McpTelemetryEvent::SchemaFailure {
                phase: McpSchemaFailurePhase::Input,
            });
            return Err(invalid_tool_arguments());
        }
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let _permit = self.admit()?;
        let exact_name = request.name.into_owned();
        let arguments = Value::Object(request.arguments.unwrap_or_default());
        let registry = fixed_tool_registry().map_err(|_| internal_error())?;

        let (target, input_schema, result_schema, risk) =
            if let Some(definition) = registry.by_name(&exact_name) {
                (
                    McpInvocationTarget::Fixed {
                        tag: definition.kind(),
                        name: exact_name,
                    },
                    definition.input_schema().clone(),
                    definition.result_schema().clone(),
                    McpRiskClass::from_fixed(definition.risk_class()).ok_or_else(internal_error)?,
                )
            } else {
                if validate_command_tool_name(&exact_name).is_err()
                    && !valid_named_query_tool_name(&exact_name)
                {
                    return Err(unavailable_tool());
                }
                ensure_request_not_cancelled(cancellation.as_ref())?;
                let invocation = self
                    .begin_invocation(extensions, cancellation.clone())
                    .map_err(|_| unavailable_tool())?;
                let resolved = match await_backend_with_cancellation(
                    cancellation.as_ref(),
                    self.backend
                        .resolve_dynamic_tool(&invocation, exact_name.clone()),
                )
                .await
                {
                    Ok(resolved) => resolved,
                    Err(McpBackendError::Cancelled) => return Err(cancelled_request()),
                    Err(McpBackendError::Public(error)) => {
                        self.record_authorization_denial(&error);
                        return Err(unavailable_tool());
                    }
                    Err(McpBackendError::Application(_)) => return Err(unavailable_tool()),
                    Err(
                        McpBackendError::AuthenticationLost
                        | McpBackendError::TargetUnavailable
                        | McpBackendError::InvalidResponse,
                    ) => return Err(unavailable_tool()),
                    Err(McpBackendError::RateLimited) => return Err(rate_limited_request()),
                };
                if resolved.name() != exact_name {
                    return Err(internal_error());
                }
                let risk = if resolved.annotations.read_only {
                    McpRiskClass::ReadOnlyData
                } else {
                    McpRiskClass::DynamicCommand
                };
                (
                    McpInvocationTarget::Dynamic(exact_name),
                    resolved.input_schema().clone(),
                    resolved.result_schema().clone(),
                    risk,
                )
            };

        self.telemetry.record(McpTelemetryEvent::ToolCall { risk });
        if RiffDbSchemaValidator
            .validate(&input_schema, &arguments)
            .is_err()
        {
            self.telemetry.record(McpTelemetryEvent::SchemaFailure {
                phase: McpSchemaFailurePhase::Input,
            });
            return Err(invalid_tool_arguments_with_detail(
                RiffDbSchemaValidator.input_violation(&input_schema, &arguments),
            ));
        }

        let request = McpToolInvocation {
            target,
            arguments: McpToolArguments::from_validated(arguments),
        };
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let invocation = match self.begin_invocation(extensions, cancellation.clone()) {
            Ok(invocation) => invocation,
            Err(error) => {
                return render_public_error(&error).map_err(|_| internal_error());
            }
        };
        let output = match await_backend_with_cancellation(
            cancellation.as_ref(),
            self.backend.invoke_tool(&invocation, request),
        )
        .await
        {
            Ok(output) => output,
            Err(McpBackendError::Public(error)) => {
                self.record_authorization_denial(&error);
                return render_public_error(&error).map_err(|_| internal_error());
            }
            Err(McpBackendError::Application(error)) => {
                self.record_application_authorization_denial(&error);
                return render_application_error(&error).map_err(|_| internal_error());
            }
            Err(McpBackendError::AuthenticationLost) => return Err(unavailable_tool()),
            Err(McpBackendError::Cancelled) => return Err(cancelled_request()),
            Err(McpBackendError::TargetUnavailable) => return Err(unavailable_tool()),
            Err(McpBackendError::InvalidResponse) => return Err(internal_error()),
            Err(McpBackendError::RateLimited) => return Err(rate_limited_request()),
        };
        let bounded = BoundedStructuredContent::validate(
            &result_schema,
            output.into_value(),
            &RiffDbSchemaValidator,
        )
        .map_err(|_| {
            self.telemetry.record(McpTelemetryEvent::SchemaFailure {
                phase: McpSchemaFailurePhase::Output,
            });
            internal_error()
        })?;
        let result = render_business_result(bounded);
        enforce_outbound_bound(&result)?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        Ok(result)
    }

    /// Executes exact policy-filtered `resources/list` pagination.
    async fn handle_list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        extensions: &Extensions,
        cancellation: Option<McpCancellationSignal>,
    ) -> Result<ListResourcesResult, McpError> {
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let cursor = decode_optional_cursor(request)?;
        let _permit = self.admit()?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let invocation = self
            .begin_invocation(extensions, cancellation.clone())
            .map_err(discovery_begin_error)?;
        let page = await_backend_with_cancellation(
            cancellation.as_ref(),
            self.backend.discover_resources(
                &invocation,
                McpResourceDiscoveryRequest::new(cursor, McpResourceDiscoverySurface::Concrete),
            ),
        )
        .await
        .map_err(discovery_error)?;
        if page.surface != McpResourceDiscoverySurface::Concrete {
            return Err(internal_error());
        }
        let registry = resource_registry().map_err(|_| internal_error())?;
        let mut resources = Vec::with_capacity(page.items.len());
        for item in page.items {
            let definition = registry
                .by_descriptor_branch(&item.descriptor_branch)
                .ok_or_else(internal_error)?;
            resources.push(
                definition
                    .to_mcp_resource(&item.uri)
                    .map_err(|_| internal_error())?,
            );
        }
        let result = ListResourcesResult {
            meta: None,
            next_cursor: page.next_cursor.map(encode_mcp_cursor),
            resources,
        };
        enforce_outbound_bound(&result)?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        Ok(result)
    }

    /// Executes exact policy-filtered `resources/templates/list` pagination.
    async fn handle_list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        extensions: &Extensions,
        cancellation: Option<McpCancellationSignal>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let cursor = decode_optional_cursor(request)?;
        let _permit = self.admit()?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let invocation = self
            .begin_invocation(extensions, cancellation.clone())
            .map_err(discovery_begin_error)?;
        let page = await_backend_with_cancellation(
            cancellation.as_ref(),
            self.backend.discover_resources(
                &invocation,
                McpResourceDiscoveryRequest::new(cursor, McpResourceDiscoverySurface::Template),
            ),
        )
        .await
        .map_err(discovery_error)?;
        if page.surface != McpResourceDiscoverySurface::Template {
            return Err(internal_error());
        }
        let registry = resource_registry().map_err(|_| internal_error())?;
        let mut templates = Vec::with_capacity(page.items.len());
        for item in page.items {
            let definition = registry
                .by_descriptor_branch(&item.descriptor_branch)
                .ok_or_else(internal_error)?;
            templates.push(
                definition
                    .to_mcp_template_uri(&item.uri)
                    .map_err(|_| internal_error())?,
            );
        }
        let result = ListResourceTemplatesResult {
            meta: None,
            next_cursor: page.next_cursor.map(encode_mcp_cursor),
            resource_templates: templates,
        };
        enforce_outbound_bound(&result)?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        Ok(result)
    }

    /// Executes one fresh authorized resource read.
    async fn handle_read_resource(
        &self,
        request: ReadResourceRequestParams,
        extensions: &Extensions,
        cancellation: Option<McpCancellationSignal>,
    ) -> Result<ReadResourceResult, McpError> {
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let locator = parse_resource_locator(&request.uri).map_err(|_| unavailable_resource())?;
        let _permit = self.admit()?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let invocation = self
            .begin_invocation(extensions, cancellation.clone())
            .map_err(resource_begin_error)?;
        let content = await_backend_with_cancellation(
            cancellation.as_ref(),
            self.backend
                .read_resource(&invocation, McpResourceReadRequest { locator }),
        )
        .await
        .map_err(resource_error)?;
        if content.uri != request.uri {
            return Err(internal_error());
        }
        let result =
            ReadResourceResult::new(vec![content.into_mcp().map_err(|_| internal_error())?]);
        enforce_outbound_bound(&result)?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        Ok(result)
    }

    /// Authorizes and retains one exact supported subscription.
    async fn handle_subscribe(
        &self,
        request: SubscribeRequestParams,
        extensions: &Extensions,
        cancellation: Option<McpCancellationSignal>,
    ) -> Result<(), McpError> {
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let locator = parse_resource_locator(&request.uri).map_err(|_| unavailable_resource())?;
        let definition =
            resource_definition_for_locator(&locator).map_err(|_| unavailable_resource())?;
        if !definition.subscribable() {
            return Err(unavailable_resource());
        }
        let _permit = self.admit()?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let invocation = self
            .begin_invocation(extensions, cancellation.clone())
            .map_err(resource_begin_error)?;
        let baseline = await_backend_with_cancellation(
            cancellation.as_ref(),
            self.backend.authorize_subscription(
                &invocation,
                McpSubscriptionRequest {
                    locator,
                    action: McpSubscriptionAction::Subscribe,
                },
            ),
        )
        .await
        .map_err(resource_error)?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        self.observer
            .subscribe_with_baseline(&request.uri, baseline)
            .map(|_| ())
            .map_err(observer_error)
    }

    /// Authorizes and removes one exact subscription; absence is idempotent.
    async fn handle_unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        extensions: &Extensions,
        cancellation: Option<McpCancellationSignal>,
    ) -> Result<(), McpError> {
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let locator = parse_resource_locator(&request.uri).map_err(|_| unavailable_resource())?;
        let _permit = self.admit()?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        let invocation = self
            .begin_invocation(extensions, cancellation.clone())
            .map_err(resource_begin_error)?;
        await_backend_with_cancellation(
            cancellation.as_ref(),
            self.backend.authorize_subscription(
                &invocation,
                McpSubscriptionRequest {
                    locator,
                    action: McpSubscriptionAction::Unsubscribe,
                },
            ),
        )
        .await
        .map_err(resource_error)?;
        ensure_request_not_cancelled(cancellation.as_ref())?;
        self.observer
            .unsubscribe(&request.uri)
            .map(|_| ())
            .map_err(observer_error)
    }

    fn begin_invocation(
        &self,
        extensions: &Extensions,
        cancellation: Option<McpCancellationSignal>,
    ) -> Result<B::Invocation, PublicError> {
        let request_id = self
            .backend
            .next_request_id()
            .map_err(|_| PublicError::storage_unavailable())?;
        #[cfg(feature = "streamable-http")]
        let post_authentication_admission = match self.source {
            McpTransportKind::Stdio => None,
            McpTransportKind::StreamableHttp => {
                match crate::hosted_http::hosted_mcp_post_authentication_admission(extensions) {
                    Some(admission) => Some(admission),
                    None => {
                        self.telemetry
                            .record(McpTelemetryEvent::AuthorizationDenied);
                        return Err(PublicError::authorization_denied());
                    }
                }
            }
        };
        #[cfg(not(feature = "streamable-http"))]
        let post_authentication_admission = None;
        #[cfg(not(feature = "streamable-http"))]
        let _ = extensions;
        let result = self.backend.begin_invocation(McpBackendRequest {
            request_id,
            source: self.source,
            #[cfg(feature = "streamable-http")]
            extensions,
            #[cfg(not(feature = "streamable-http"))]
            _extensions: std::marker::PhantomData,
            post_authentication_admission,
            cancellation,
        });
        if let Err(error) = &result {
            self.record_authorization_denial(error);
        }
        result
    }

    fn admit(&self) -> Result<McpInflightPermit, McpError> {
        self.inflight
            .try_acquire(self.admission_session.clone())
            .map_err(|_| internal_error())
    }

    fn record_authorization_denial(&self, error: &PublicError) {
        if error.kind() == PublicErrorKind::AuthorizationDenied {
            self.telemetry
                .record(McpTelemetryEvent::AuthorizationDenied);
        }
    }

    fn record_application_authorization_denial(&self, error: &ApplicationError) {
        if matches!(
            error.code(),
            ApplicationErrorCode::AuthorizationDenied | ApplicationErrorCode::CapabilityRevoked
        ) {
            self.telemetry
                .record(McpTelemetryEvent::AuthorizationDenied);
        }
    }
}

impl<B> fmt::Debug for RiffDbMcpServer<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RiffDbMcpServer([REDACTED])")
    }
}

impl<B> ServerHandler for RiffDbMcpServer<B>
where
    B: McpBackend,
{
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        let _permit = self.admit()?;
        if !accepts_initialization_offer(request.protocol_version.as_str()) {
            return Err(McpError::invalid_params(
                "unsupported MCP protocol version",
                None,
            ));
        }
        context.peer.set_peer_info(request);
        #[cfg(feature = "streamable-http")]
        if self.source == McpTransportKind::StreamableHttp {
            crate::hosted_observer::publish_initialization_observer(
                &context.extensions,
                context.peer.clone(),
                Arc::clone(&self.observer),
            )
            .map_err(|_| internal_error())?;
        }
        Ok(initialization_result())
    }

    async fn ping(&self, _: RequestContext<RoleServer>) -> Result<(), McpError> {
        let _permit = self.admit()?;
        Ok(())
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let (signal, _guard) = self
            .cancellations
            .register_with_token(context.id, context.ct.clone())
            .map_err(|_| internal_error())?;
        self.handle_list_tools(request, &context.extensions, Some(signal))
            .await
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        fixed_tool_registry()
            .ok()?
            .by_name(name)
            .map(|tool| tool.to_mcp_tool())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let protocol_version = context.protocol_version();
        let mut progress = McpProgressTracker::new(
            crate::protocol::progress_is_negotiated(protocol_version.as_ref()),
            context.meta.get_progress_token(),
        )
        .map_err(|_| invalid_progress_request())?;
        let (signal, _guard) = self
            .cancellations
            .register_with_token(context.id, context.ct.clone())
            .map_err(|_| internal_error())?;
        let result = self
            .handle_call_tool_with_cancellation(request, &context.extensions, Some(signal.clone()))
            .await;
        let progress_result = match &result {
            Ok(result) if result.is_error == Some(false) => {
                ensure_request_not_cancelled(Some(&signal))
                    .and_then(|()| progress.advance(1, Some(1)).map_err(|_| internal_error()))
            }
            Ok(_) | Err(_) => Ok(None),
        };
        let progress_result = match progress_result {
            Ok(Some(notification)) => context
                .peer
                .notify_progress(notification)
                .await
                .map_err(|_| internal_error())
                .and_then(|()| ensure_request_not_cancelled(Some(&signal))),
            Ok(None) => Ok(()),
            Err(error) => Err(error),
        };
        progress.finish();
        progress_result?;
        result
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let (signal, _guard) = self
            .cancellations
            .register_with_token(context.id, context.ct.clone())
            .map_err(|_| internal_error())?;
        self.handle_list_resources(request, &context.extensions, Some(signal))
            .await
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        let (signal, _guard) = self
            .cancellations
            .register_with_token(context.id, context.ct.clone())
            .map_err(|_| internal_error())?;
        self.handle_list_resource_templates(request, &context.extensions, Some(signal))
            .await
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let (signal, _guard) = self
            .cancellations
            .register_with_token(context.id, context.ct.clone())
            .map_err(|_| internal_error())?;
        self.handle_read_resource(request, &context.extensions, Some(signal))
            .await
    }

    async fn subscribe(
        &self,
        request: SubscribeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        let (signal, _guard) = self
            .cancellations
            .register_with_token(context.id, context.ct.clone())
            .map_err(|_| internal_error())?;
        self.handle_subscribe(request, &context.extensions, Some(signal))
            .await
    }

    async fn unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<(), McpError> {
        let (signal, _guard) = self
            .cancellations
            .register_with_token(context.id, context.ct.clone())
            .map_err(|_| internal_error())?;
        self.handle_unsubscribe(request, &context.extensions, Some(signal))
            .await
    }

    fn get_info(&self) -> rmcp::model::ServerInfo {
        initialization_result()
    }

    async fn complete(
        &self,
        _: CompleteRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, McpError> {
        Err(McpError::method_not_found::<CompleteRequestMethod>())
    }

    async fn get_prompt(
        &self,
        _: GetPromptRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        Err(McpError::method_not_found::<GetPromptRequestMethod>())
    }

    async fn list_prompts(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        Err(McpError::method_not_found::<ListPromptsRequestMethod>())
    }

    async fn enqueue_task(
        &self,
        _: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CreateTaskResult, McpError> {
        Err(McpError::method_not_found::<CallToolRequestMethod>())
    }

    async fn list_tasks(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListTasksResult, McpError> {
        Err(McpError::method_not_found::<ListTasksMethod>())
    }

    async fn get_task_info(
        &self,
        _: GetTaskParams,
        _: RequestContext<RoleServer>,
    ) -> Result<GetTaskResult, McpError> {
        Err(McpError::method_not_found::<GetTaskMethod>())
    }

    async fn get_task_result(
        &self,
        _: GetTaskPayloadParams,
        _: RequestContext<RoleServer>,
    ) -> Result<GetTaskPayloadResult, McpError> {
        Err(McpError::method_not_found::<GetTaskPayloadMethod>())
    }

    async fn cancel_task(
        &self,
        _: CancelTaskParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CancelTaskResult, McpError> {
        Err(McpError::method_not_found::<CancelTaskMethod>())
    }

    async fn on_cancelled(
        &self,
        notification: rmcp::model::CancelledNotificationParam,
        _: NotificationContext<RoleServer>,
    ) {
        if let Some(request_id) = notification.request_id {
            let _ = self.cancellations.cancel(&request_id);
        }
    }
}

fn decode_optional_cursor(
    request: Option<PaginatedRequestParams>,
) -> Result<Option<[u8; 16]>, McpError> {
    request
        .and_then(|request| request.cursor)
        .map(|cursor| decode_mcp_cursor(&cursor).map_err(|_| invalid_cursor()))
        .transpose()
}

fn validate_descriptor_uri(
    definition: &ResourceDefinition,
    uri: &str,
) -> Result<(), McpHandlerContractError> {
    match definition.surface() {
        ResourceSurface::Concrete => {
            let locator = parse_resource_locator(uri).map_err(|_| McpHandlerContractError)?;
            if locator_descriptor_branch(&locator) != definition.descriptor_branch() {
                return Err(McpHandlerContractError);
            }
        }
        ResourceSurface::Template => match definition.descriptor_branch() {
            "commit.class_template" if uri == "riffdb://commit/{sequence}" => {}
            "provenance.class_template" if uri == "riffdb://provenance/{provenance_id}" => {}
            "command_outcome"
                if uri.matches("{principal}").count() == 1
                    && uri.matches("{key_hash}").count() == 1 =>
            {
                let expanded = uri.replace("{principal}", "operator").replace(
                    "{key_hash}",
                    "AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                );
                if !matches!(
                    parse_resource_locator(&expanded),
                    Ok(McpResourceLocator::Outcome { .. })
                ) {
                    return Err(McpHandlerContractError);
                }
            }
            _ => return Err(McpHandlerContractError),
        },
    }
    Ok(())
}

fn resource_surface(surface: ResourceSurface) -> McpResourceDiscoverySurface {
    match surface {
        ResourceSurface::Concrete => McpResourceDiscoverySurface::Concrete,
        ResourceSurface::Template => McpResourceDiscoverySurface::Template,
    }
}

fn locator_descriptor_branch(locator: &McpResourceLocator) -> &'static str {
    match locator {
        McpResourceLocator::ActiveContract => "active_contract",
        McpResourceLocator::ContractVersion { .. } => "contract_version",
        McpResourceLocator::EntitySchema { .. } => "entity_schema",
        McpResourceLocator::CommandPlan { .. } => "command_plan",
        McpResourceLocator::CommandDocumentation { .. } => "command_documentation",
        McpResourceLocator::Outcome { .. } => "command_outcome",
        McpResourceLocator::Commit(_) => "commit.commit_sequence",
        McpResourceLocator::Provenance(_) => "provenance.provenance_id",
        McpResourceLocator::ProjectionStatus { .. } => "projection_status",
        McpResourceLocator::ServerHealth => "server_health",
        McpResourceLocator::ReactiveWakeup => "reactive_wakeup",
    }
}

fn resource_definition_for_locator(
    locator: &McpResourceLocator,
) -> Result<&'static ResourceDefinition, RegistryError> {
    resource_registry()?
        .by_descriptor_branch(locator_descriptor_branch(locator))
        .ok_or(RegistryError)
}

fn accepts_initialization_offer(protocol_version: &str) -> bool {
    // ADR-0008 permits transport wrappers to replace any other syntactically
    // valid offer with this private sentinel before rmcp invokes the handler.
    matches!(
        protocol_version,
        crate::MCP_PROTOCOL_VERSION | "riffdb-unsupported"
    )
}

fn checked_json_object<T: Serialize + ?Sized>(value: &T) -> Result<Value, McpHandlerContractError> {
    let bytes = bounded_json::to_vec(value, crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES)
        .map_err(|_| McpHandlerContractError)?;
    checked_json_object_bytes(&bytes)
}

fn checked_json_object_bytes(bytes: &[u8]) -> Result<Value, McpHandlerContractError> {
    let value = bounded_json::parse_unique(bytes, crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES)
        .map_err(|_| McpHandlerContractError)?;
    if !value.is_object() {
        return Err(McpHandlerContractError);
    }
    Ok(value)
}

fn enforce_outbound_bound(value: &impl serde::Serialize) -> Result<(), McpError> {
    bounded_json::encoded_len(value, crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES)
        .map(|_| ())
        .map_err(|_| internal_error())
}

fn ensure_request_not_cancelled(
    cancellation: Option<&McpCancellationSignal>,
) -> Result<(), McpError> {
    if cancellation.is_some_and(McpCancellationSignal::is_cancelled) {
        return Err(cancelled_request());
    }
    Ok(())
}

async fn await_backend_with_cancellation<'a, T>(
    cancellation: Option<&'a McpCancellationSignal>,
    future: McpBackendFuture<'a, T>,
) -> Result<T, McpBackendError> {
    let Some(cancellation) = cancellation else {
        return future.await;
    };
    if cancellation.is_cancelled() {
        return Err(McpBackendError::Cancelled);
    }

    let cancelled = Box::pin(cancellation.cancelled());
    match select(cancelled, future).await {
        Either::Left(((), _pending_backend)) => Err(McpBackendError::Cancelled),
        Either::Right((result, _pending_cancellation)) => {
            if cancellation.is_cancelled() {
                Err(McpBackendError::Cancelled)
            } else {
                result
            }
        }
    }
}

fn invalid_cursor() -> McpError {
    McpError::invalid_params("invalid pagination cursor", None)
}

fn invalid_tool_arguments() -> McpError {
    McpError::invalid_params("invalid tool arguments", None)
}

fn invalid_tool_arguments_with_detail(violation: crate::schema::InputSchemaViolation) -> McpError {
    McpError::invalid_params("invalid tool arguments", Some(violation.as_json()))
}

fn invalid_progress_request() -> McpError {
    McpError::invalid_params("invalid progress request", None)
}

fn unavailable_tool() -> McpError {
    McpError::new(ErrorCode::METHOD_NOT_FOUND, "tool unavailable", None)
}

fn valid_named_query_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn unavailable_resource() -> McpError {
    McpError::resource_not_found("resource unavailable", None)
}

fn internal_error() -> McpError {
    McpError::internal_error("MCP request failed", None)
}

fn cancelled_request() -> McpError {
    McpError::new(ErrorCode(-32_000), "request cancelled", None)
}

fn rate_limited_request() -> McpError {
    McpError::new(ErrorCode(-32_000), "request rate limited", None)
}

fn discovery_error(error: McpBackendError) -> McpError {
    match error {
        McpBackendError::Cancelled => cancelled_request(),
        McpBackendError::RateLimited => rate_limited_request(),
        McpBackendError::Public(_)
        | McpBackendError::Application(_)
        | McpBackendError::AuthenticationLost
        | McpBackendError::TargetUnavailable
        | McpBackendError::InvalidResponse => {
            McpError::invalid_params("discovery unavailable", None)
        }
    }
}

fn discovery_begin_error(_: PublicError) -> McpError {
    McpError::invalid_params("discovery unavailable", None)
}

fn resource_begin_error(_: PublicError) -> McpError {
    unavailable_resource()
}

fn resource_error(error: McpBackendError) -> McpError {
    match error {
        McpBackendError::Cancelled => cancelled_request(),
        McpBackendError::RateLimited => rate_limited_request(),
        McpBackendError::Public(_)
        | McpBackendError::Application(_)
        | McpBackendError::AuthenticationLost
        | McpBackendError::TargetUnavailable
        | McpBackendError::InvalidResponse => unavailable_resource(),
    }
}

fn observer_error(_: McpObserverError) -> McpError {
    unavailable_resource()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use riffdb_types::hash_schema;
    use rmcp::model::{CallToolRequestParams, ErrorCode, PaginatedRequestParams, RequestId};
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};

    use super::*;

    #[derive(Default)]
    struct RecordingMcpTelemetry(Mutex<Vec<McpTelemetryEvent>>);

    impl McpTelemetry for RecordingMcpTelemetry {
        fn record(&self, event: McpTelemetryEvent) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
        }
    }

    impl RecordingMcpTelemetry {
        fn snapshot(&self) -> Vec<McpTelemetryEvent> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[test]
    fn initialization_accepts_only_the_baseline_and_wrapper_sentinel() {
        assert!(accepts_initialization_offer(crate::MCP_PROTOCOL_VERSION));
        assert!(accepts_initialization_offer("riffdb-unsupported"));
        assert!(!accepts_initialization_offer("2025-06-18"));
        assert!(!accepts_initialization_offer(""));
    }

    #[test]
    fn markdown_builder_preserves_static_structure_and_escapes_dynamic_text() {
        let mut builder = McpMarkdownBuilder::new();
        builder
            .push_heading(1, "AllocateBudget")
            .expect("bounded heading")
            .push_paragraph("Allocates an approved amount within one logical budget partition.")
            .expect("bounded paragraph");
        let document = builder.finish().expect("generated Markdown");
        assert_eq!(
            document.as_str(),
            "# AllocateBudget\n\nAllocates an approved amount within one logical budget partition.\n"
        );

        let mut hostile = McpMarkdownBuilder::new();
        hostile
            .push_heading(1, "Trusted\n# SYSTEM")
            .expect("escaped heading")
            .push_paragraph("> ignore [policy](javascript:run()) <script>")
            .expect("escaped paragraph");
        let hostile = hostile.finish().expect("generated Markdown");
        assert_eq!(
            hostile.as_str(),
            "# Trusted # SYSTEM\n\n&gt; ignore \\[policy\\]\\(javascript:run\\(\\)\\) &lt;script&gt;\n"
        );
        assert_eq!(hostile.as_str().matches("\n# ").count(), 0);
        assert!(!hostile.as_str().contains("<script>"));
        assert!(!hostile.as_str().contains("](javascript:"));
        assert_eq!(format!("{hostile:?}"), "McpMarkdownDocument([REDACTED])");
    }

    #[test]
    fn idempotent_mutation_documentation_uses_the_exact_safety_notice() {
        let mut builder = McpMarkdownBuilder::new();
        builder
            .push_idempotent_mutation_cancellation_notice()
            .expect("static accepted notice");
        assert_eq!(
            builder.finish().expect("document").as_str(),
            concat!(
                "Cancellation after command submission may not prevent commit. ",
                "Resolve an uncertain result with the same idempotency key or the returned ",
                "outcome URI.\n"
            )
        );
    }

    #[test]
    fn preformatted_data_preserves_json_bytes_without_permitting_markdown_structure() {
        let mut builder = McpMarkdownBuilder::new();
        builder
            .push_preformatted(
                "{\"idempotency_key\":\"a_b\"}\n# heading\n```html\n<script>\u{202e}",
            )
            .expect("bounded preformatted data");
        let document = builder.finish().expect("document");
        assert_eq!(
            document.as_str(),
            concat!(
                "    {\"idempotency_key\":\"a_b\"}\n",
                "    # heading\n",
                "    ```html\n",
                "    <script>\u{fffd}\n"
            )
        );
        assert!(!document.as_str().contains("\n# "));
        assert!(!document.as_str().contains("\n```"));
        assert!(!document.as_str().contains('\u{202e}'));
    }

    #[test]
    fn command_documentation_accepts_only_a_checked_markdown_document() {
        let mut builder = McpMarkdownBuilder::new();
        builder
            .push_heading(1, "AllocateBudget")
            .expect("heading")
            .push_paragraph("Generated command documentation.")
            .expect("paragraph");
        let content = McpResourceContent::new(
            "command_documentation",
            "riffdb://command/LegalSpend/2/docs",
            McpResourceBody::Markdown(builder.finish().expect("document")),
        )
        .expect("checked command documentation")
        .into_mcp()
        .expect("MCP resource");

        assert_eq!(
            serde_json::to_value(content).expect("resource content"),
            json!({
                "uri": "riffdb://command/LegalSpend/2/docs",
                "mimeType": "text/markdown",
                "text": "# AllocateBudget\n\nGenerated command documentation.\n"
            })
        );
        assert!(
            McpMarkdownBuilder::new()
                .push_heading(0, "invalid")
                .is_err()
        );
        assert!(
            McpMarkdownBuilder::new()
                .push_paragraph(&"x".repeat(crate::MCP_OUTBOUND_MESSAGE_MAX_BYTES))
                .is_err()
        );
    }

    #[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct NaturalPresentation {
        outcome: String,
        account: NaturalAccount,
    }

    #[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct NaturalAccount {
        display_name: String,
    }

    #[test]
    fn opaque_backend_json_preserves_schema_names_without_public_value_types() {
        let expected = NaturalPresentation {
            outcome: "Approved".to_owned(),
            account: NaturalAccount {
                display_name: "North".to_owned(),
            },
        };
        let arguments = McpToolArguments::from_validated(
            checked_json_object(&expected).expect("checked arguments"),
        );
        assert_eq!(
            arguments
                .deserialize::<NaturalPresentation>()
                .expect("typed arguments"),
            expected
        );

        let result = McpToolResult::from_serializable(&expected).expect("checked result");
        assert_eq!(
            result,
            McpToolResult::from_json_bytes(
                br#"{"outcome":"Approved","account":{"display_name":"North"}}"#
            )
            .expect("checked JSON bytes")
        );
        assert_eq!(
            McpToolResult::from_json_bytes(b"[]"),
            Err(McpHandlerContractError)
        );
        assert_eq!(format!("{arguments:?}"), "McpToolArguments([REDACTED])");
        assert_eq!(format!("{result:?}"), "McpToolResult([REDACTED])");
    }

    #[cfg(feature = "streamable-http")]
    #[derive(Clone)]
    struct AuthenticatedMarker;

    #[derive(Clone)]
    enum InvokeMode {
        Success(Value),
        PublicAuthorization,
        TargetUnavailable,
        InvalidResponse,
        Cancelled,
    }

    #[derive(Clone)]
    struct CancellationTrigger {
        registry: Arc<McpCancellationRegistry>,
        request_id: RequestId,
    }

    impl CancellationTrigger {
        fn fire(&self) {
            assert!(
                self.registry
                    .cancel(&self.request_id)
                    .expect("cancel live request")
            );
        }
    }

    struct FakeState {
        #[cfg(feature = "streamable-http")]
        require_marker: bool,
        begin_ids: Vec<McpRequestId>,
        begin_sources: Vec<McpTransportKind>,
        begin_cancellation_present: Vec<bool>,
        begin_cancelled: Vec<bool>,
        cancel_during_tool_discovery: Option<CancellationTrigger>,
        cancel_during_invoke: Option<CancellationTrigger>,
        discover_tool_calls: usize,
        resolve_calls: usize,
        invoke_calls: usize,
        discover_resource_calls: usize,
        read_calls: usize,
        subscription_calls: usize,
        last_tool_cursor: Option<[u8; 16]>,
        tool_page: McpToolPage,
        dynamic_tool: McpDynamicToolDefinition,
        dynamic_available: bool,
        invoke_mode: InvokeMode,
        concrete_page: McpResourcePage,
        template_page: McpResourcePage,
        resource_content: McpResourceContent,
    }

    #[derive(Clone)]
    struct FakeBackend {
        next_id: Arc<AtomicU64>,
        state: Arc<Mutex<FakeState>>,
    }

    #[derive(Clone, Copy)]
    struct FakeInvocation {
        request_id: McpRequestId,
    }

    impl FakeBackend {
        fn new() -> Self {
            let dynamic_tool = dynamic_tool();
            let tool_page = McpToolPage::new(
                (1..=14)
                    .map(McpToolDiscoveryItem::Fixed)
                    .chain(std::iter::once(McpToolDiscoveryItem::Dynamic(Box::new(
                        dynamic_tool.clone(),
                    ))))
                    .collect(),
                Some([0xabu8; 16]),
            )
            .expect("tool page");
            let concrete_page = McpResourcePage::new(
                McpResourceDiscoverySurface::Concrete,
                vec![
                    McpResourceDescriptor::new("active_contract", "riffdb://contract/active")
                        .expect("resource"),
                    McpResourceDescriptor::new("server_health", "riffdb://server/health")
                        .expect("resource"),
                ],
                None,
            )
            .expect("resource page");
            let template_page = McpResourcePage::new(
                McpResourceDiscoverySurface::Template,
                vec![
                    McpResourceDescriptor::new(
                        "commit.class_template",
                        "riffdb://commit/{sequence}",
                    )
                    .expect("template"),
                    McpResourceDescriptor::new(
                        "provenance.class_template",
                        "riffdb://provenance/{provenance_id}",
                    )
                    .expect("template"),
                ],
                None,
            )
            .expect("template page");
            let resource_content = McpResourceContent::new(
                "active_contract",
                "riffdb://contract/active",
                McpResourceBody::Json(
                    McpResourceJson::from_serializable(&json!({"status": "active"}))
                        .expect("resource JSON"),
                ),
            )
            .expect("resource content");
            Self {
                next_id: Arc::new(AtomicU64::new(1)),
                state: Arc::new(Mutex::new(FakeState {
                    #[cfg(feature = "streamable-http")]
                    require_marker: true,
                    begin_ids: Vec::new(),
                    begin_sources: Vec::new(),
                    begin_cancellation_present: Vec::new(),
                    begin_cancelled: Vec::new(),
                    cancel_during_tool_discovery: None,
                    cancel_during_invoke: None,
                    discover_tool_calls: 0,
                    resolve_calls: 0,
                    invoke_calls: 0,
                    discover_resource_calls: 0,
                    read_calls: 0,
                    subscription_calls: 0,
                    last_tool_cursor: None,
                    tool_page,
                    dynamic_tool,
                    dynamic_available: true,
                    invoke_mode: InvokeMode::Success(dynamic_result()),
                    concrete_page,
                    template_page,
                    resource_content,
                })),
            }
        }

        fn state(&self) -> std::sync::MutexGuard<'_, FakeState> {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    impl McpBackend for FakeBackend {
        type Invocation = FakeInvocation;

        fn next_request_id(&self) -> Result<McpRequestId, crate::RequestIdSourceError> {
            let counter = self.next_id.fetch_add(1, Ordering::Relaxed);
            let mut random = [0_u8; 10];
            random[2..].copy_from_slice(&counter.to_be_bytes());
            let request_id = riffdb_types::RequestId::from_unix_milliseconds_and_random(1, random)
                .map_err(|_| crate::RequestIdSourceError)?;
            McpRequestId::from_public_bytes(request_id.as_bytes())
        }

        fn begin_invocation(
            &self,
            request: McpBackendRequest<'_>,
        ) -> Result<Self::Invocation, PublicError> {
            let mut state = self.state();
            #[cfg(feature = "streamable-http")]
            if state.require_marker && request.extension::<AuthenticatedMarker>().is_none() {
                return Err(PublicError::authorization_denied());
            }
            state.begin_ids.push(request.request_id());
            state.begin_sources.push(request.source());
            state
                .begin_cancellation_present
                .push(request.cancellation().is_some());
            state.begin_cancelled.push(
                request
                    .cancellation()
                    .is_some_and(McpCancellationSignal::is_cancelled),
            );
            Ok(FakeInvocation {
                request_id: request.request_id(),
            })
        }

        fn discover_tools<'a>(
            &'a self,
            invocation: &'a Self::Invocation,
            request: McpDiscoveryRequest,
        ) -> McpBackendFuture<'a, McpToolPage> {
            let _ = invocation.request_id;
            let (page, cancellation) = {
                let mut state = self.state();
                state.discover_tool_calls += 1;
                state.last_tool_cursor = request.cursor();
                (
                    state.tool_page.clone(),
                    state.cancel_during_tool_discovery.take(),
                )
            };
            if let Some(cancellation) = cancellation {
                return Box::pin(async move {
                    cancellation.fire();
                    futures::future::pending::<Result<McpToolPage, McpBackendError>>().await
                });
            }
            Box::pin(async move { Ok(page) })
        }

        fn resolve_dynamic_tool<'a>(
            &'a self,
            invocation: &'a Self::Invocation,
            exact_name: String,
        ) -> McpBackendFuture<'a, McpDynamicToolDefinition> {
            let _ = invocation.request_id;
            let result = {
                let mut state = self.state();
                state.resolve_calls += 1;
                if state.dynamic_available && state.dynamic_tool.name() == exact_name {
                    Ok(state.dynamic_tool.clone())
                } else {
                    Err(McpBackendError::TargetUnavailable)
                }
            };
            Box::pin(async move { result })
        }

        fn invoke_tool<'a>(
            &'a self,
            invocation: &'a Self::Invocation,
            _: McpToolInvocation,
        ) -> McpBackendFuture<'a, McpToolResult> {
            let _ = invocation.request_id;
            let (mode, cancellation) = {
                let mut state = self.state();
                state.invoke_calls += 1;
                (state.invoke_mode.clone(), state.cancel_during_invoke.take())
            };
            if let Some(cancellation) = cancellation {
                return Box::pin(async move {
                    cancellation.fire();
                    futures::future::pending::<Result<McpToolResult, McpBackendError>>().await
                });
            }
            Box::pin(async move {
                match mode {
                    InvokeMode::Success(value) => McpToolResult::from_serializable(&value)
                        .map_err(|_| McpBackendError::InvalidResponse),
                    InvokeMode::PublicAuthorization => {
                        Err(McpBackendError::Public(PublicError::authorization_denied()))
                    }
                    InvokeMode::TargetUnavailable => Err(McpBackendError::TargetUnavailable),
                    InvokeMode::InvalidResponse => Err(McpBackendError::InvalidResponse),
                    InvokeMode::Cancelled => Err(McpBackendError::Cancelled),
                }
            })
        }

        fn discover_resources<'a>(
            &'a self,
            invocation: &'a Self::Invocation,
            request: McpResourceDiscoveryRequest,
        ) -> McpBackendFuture<'a, McpResourcePage> {
            let _ = invocation.request_id;
            let result = {
                let mut state = self.state();
                state.discover_resource_calls += 1;
                match request.surface() {
                    McpResourceDiscoverySurface::Concrete => state.concrete_page.clone(),
                    McpResourceDiscoverySurface::Template => state.template_page.clone(),
                }
            };
            Box::pin(async move { Ok(result) })
        }

        fn read_resource<'a>(
            &'a self,
            invocation: &'a Self::Invocation,
            _: McpResourceReadRequest,
        ) -> McpBackendFuture<'a, McpResourceContent> {
            let _ = invocation.request_id;
            let content = {
                let mut state = self.state();
                state.read_calls += 1;
                state.resource_content.clone()
            };
            Box::pin(async move { Ok(content) })
        }

        fn authorize_subscription<'a>(
            &'a self,
            invocation: &'a Self::Invocation,
            _: McpSubscriptionRequest,
        ) -> McpBackendFuture<'a, McpVisibleFingerprint> {
            let _ = invocation.request_id;
            let baseline = {
                let mut state = self.state();
                state.subscription_calls += 1;
                state
                    .resource_content
                    .visible_fingerprint()
                    .expect("fixture resource fingerprint")
            };
            Box::pin(async move { Ok(baseline) })
        }
    }

    fn dynamic_tool() -> McpDynamicToolDefinition {
        dynamic_tool_named("riffdb_cmd_orders_place")
    }

    fn dynamic_tool_named(name: impl Into<String>) -> McpDynamicToolDefinition {
        dynamic_tool_named_with_metadata(
            name,
            Some("Place order".to_owned()),
            Some("Places one order.".to_owned()),
        )
    }

    fn dynamic_tool_named_with_metadata(
        name: impl Into<String>,
        title: Option<String>,
        description: Option<String>,
    ) -> McpDynamicToolDefinition {
        let input_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"additionalProperties\":false,\"properties\":{\"value\":{\"type\":\"string\",",
            "\"x-riffdb-maxUtf8Bytes\":16}},\"required\":[\"value\"],\"type\":\"object\"}"
        );
        let outcome_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"oneOf\":[{\"additionalProperties\":false,\"properties\":{",
            "\"type\":{\"const\":\"Done\"}},\"required\":[\"type\"],\"type\":\"object\"}]}"
        );
        McpDynamicToolDefinition::new(
            name,
            title,
            description,
            McpDynamicToolAnnotations::new(false, false, true, false),
            SchemaDocument::from_canonical(
                "compiler.command-input/1",
                hash_schema(input_source.as_bytes()),
                input_source,
            )
            .expect("input schema"),
            SchemaDocument::from_canonical(
                "compiler.command-outcome/1",
                hash_schema(outcome_source.as_bytes()),
                outcome_source,
            )
            .expect("outcome schema"),
        )
        .expect("dynamic tool")
    }

    fn named_query_tool() -> McpDynamicToolDefinition {
        let input_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"additionalProperties\":false,\"properties\":{},\"required\":[],\"type\":\"object\"}"
        );
        let result_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"oneOf\":[{\"additionalProperties\":false,\"properties\":{",
            "\"outcome\":{\"const\":\"Found\",\"type\":\"string\"}},",
            "\"required\":[\"outcome\"],\"type\":\"object\"}]}"
        );
        McpDynamicToolDefinition::from_discovered_query(
            "ticket_desk_ticket_page",
            SchemaDocument::from_canonical(
                "compiler.named-query-input/1",
                hash_schema(input_source.as_bytes()),
                input_source,
            )
            .expect("input schema"),
            SchemaDocument::from_canonical(
                "compiler.named-query-result/1",
                hash_schema(result_source.as_bytes()),
                result_source,
            )
            .expect("result schema"),
        )
        .expect("named query tool")
    }

    fn tool_page_items(
        fixed_count: u8,
        dynamic: &[McpDynamicToolDefinition],
        dynamic_count: usize,
    ) -> Vec<McpToolDiscoveryItem> {
        (1..=fixed_count)
            .map(McpToolDiscoveryItem::Fixed)
            .chain(
                dynamic
                    .iter()
                    .take(dynamic_count)
                    .cloned()
                    .map(Box::new)
                    .map(McpToolDiscoveryItem::Dynamic),
            )
            .collect()
    }

    fn maximum_dynamic_page_count(fixed_count: u8, dynamic: &[McpDynamicToolDefinition]) -> usize {
        let maximum_by_item_count =
            MAX_MCP_DISCOVERY_PAGE_ITEMS.saturating_sub(usize::from(fixed_count));
        let mut accepted = 0_usize;
        let mut rejected = maximum_by_item_count.min(dynamic.len()).saturating_add(1);
        while accepted + 1 < rejected {
            let candidate = accepted + (rejected - accepted) / 2;
            let items = tool_page_items(fixed_count, dynamic, candidate);
            if McpToolPage::new(items, Some([0xff; 16])).is_ok() {
                accepted = candidate;
            } else {
                rejected = candidate;
            }
        }
        accepted
    }

    fn dynamic_result() -> Value {
        json!({
            "status": "committed",
            "commit_sequence": "1",
            "contract_version": 1,
            "plan_hash": "0".repeat(64),
            "outcome": {"type": "Done"},
            "provenance_uri": "riffdb://provenance/00000000-0001-7000-8000-000000000000",
            "durability_mode": "sync",
            "outcome_uri": concat!(
                "riffdb://outcome/actor/orders/1/riffdb_cmd_orders_place/",
                "AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            )
        })
    }

    fn extensions() -> Extensions {
        #[cfg(feature = "streamable-http")]
        {
            let mut extensions = Extensions::new();
            extensions.insert(AuthenticatedMarker);
            extensions
        }
        #[cfg(not(feature = "streamable-http"))]
        {
            Extensions::new()
        }
    }

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(future)
    }

    #[test]
    fn named_query_tools_are_read_only_and_preserve_compiler_schemas() {
        let input_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"additionalProperties\":false,\"properties\":{},\"required\":[],\"type\":\"object\"}"
        );
        let result_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"oneOf\":[{\"additionalProperties\":false,\"properties\":{",
            "\"outcome\":{\"const\":\"Found\",\"type\":\"string\"}},",
            "\"required\":[\"outcome\"],\"type\":\"object\"}]}"
        );
        let input = SchemaDocument::from_canonical(
            "compiler.named-query-input/1",
            hash_schema(input_source.as_bytes()),
            input_source,
        )
        .expect("input schema");
        let result = SchemaDocument::from_canonical(
            "compiler.named-query-result/1",
            hash_schema(result_source.as_bytes()),
            result_source,
        )
        .expect("result schema");

        let definition = McpDynamicToolDefinition::from_discovered_query(
            "ticket_desk_ticket_page",
            input.clone(),
            result.clone(),
        )
        .expect("named-query tool");

        assert_eq!(
            definition.annotations,
            McpDynamicToolAnnotations::new(true, false, true, false)
        );
        assert_eq!(
            definition.input_schema().canonical_bytes(),
            input.canonical_bytes()
        );
        assert_eq!(
            definition.outcome_schema().canonical_bytes(),
            result.canonical_bytes()
        );
        assert_eq!(
            definition.result_schema().canonical_bytes(),
            result.canonical_bytes()
        );
    }

    #[test]
    fn advertised_named_query_tool_is_invocable_and_accounted_as_read_only() {
        let backend = FakeBackend::new();
        {
            let mut state = backend.state();
            state.dynamic_tool = named_query_tool();
            state.invoke_mode = InvokeMode::Success(json!({"outcome": "Found"}));
        }
        let telemetry = Arc::new(RecordingMcpTelemetry::default());
        let server = RiffDbMcpServer::new_stdio_with_telemetry(backend.clone(), telemetry.clone());
        let request = CallToolRequestParams::new("ticket_desk_ticket_page")
            .with_arguments(serde_json::Map::new());
        let result =
            block_on(server.handle_call_tool_with_cancellation(request, &extensions(), None))
                .expect("named query result");

        assert_eq!(result.structured_content, Some(json!({"outcome": "Found"})));
        assert_eq!(backend.state().resolve_calls, 1);
        assert_eq!(backend.state().invoke_calls, 1);
        assert!(telemetry.snapshot().contains(&McpTelemetryEvent::ToolCall {
            risk: McpRiskClass::ReadOnlyData,
        }));
    }

    #[test]
    fn resource_pages_preserve_service_order_and_reject_only_duplicate_uris() {
        let health =
            McpResourceDescriptor::new("server_health", "riffdb://server/health").expect("health");
        let active = McpResourceDescriptor::new("active_contract", "riffdb://contract/active")
            .expect("active");
        let page = McpResourcePage::new(
            McpResourceDiscoverySurface::Concrete,
            vec![health.clone(), active.clone()],
            None,
        )
        .expect("service order need not be URI lexical order");
        assert_eq!(page.items(), &[health.clone(), active.clone()]);
        assert_eq!(
            McpResourcePage::new(
                McpResourceDiscoverySurface::Concrete,
                vec![health.clone(), health],
                None,
            )
            .expect_err("duplicate URI"),
            McpHandlerContractError
        );
    }

    #[test]
    fn tools_list_schema_ownership_is_registry_driven_and_catalog_is_unconditional() {
        let registry = fixed_tool_registry().expect("accepted fixed registry");
        let operation_bytes = registry
            .operation_schemas()
            .iter()
            .map(crate::SchemaDocument::canonical_bytes)
            .sum();

        let empty = tool_page_schema_components(&[], registry).expect("empty page ledger");
        assert_eq!(
            empty,
            ToolPageSchemaComponents {
                fixed: 0,
                service: operation_bytes,
                emitted_credit: 0,
            }
        );

        let fixed_items: Vec<_> = (1..=30).map(McpToolDiscoveryItem::Fixed).collect();
        let fixed = tool_page_schema_components(&fixed_items, registry).expect("fixed page ledger");
        let emitted_fixed_bytes = registry
            .tools()
            .iter()
            .flat_map(|tool| [tool.input_schema(), tool.result_schema()])
            .map(crate::SchemaDocument::canonical_bytes)
            .sum::<usize>();
        assert_eq!(fixed.fixed, registry.fixed_schema_bytes());
        assert_eq!(fixed.service, operation_bytes);
        assert_eq!(fixed.emitted_credit, emitted_fixed_bytes);
        let service_owned_fixed_schemas: Vec<_> = registry
            .tools()
            .iter()
            .flat_map(|tool| [tool.input_schema(), tool.result_schema()])
            .filter(|schema| {
                registry.operation_schemas().iter().any(|operation| {
                    operation.schema_id() == schema.schema_id()
                        && operation.schema_hash_bytes() == schema.schema_hash_bytes()
                })
            })
            .collect();
        assert_eq!(service_owned_fixed_schemas.len(), 1);
        assert_eq!(
            service_owned_fixed_schemas[0].schema_id(),
            "riffdb.command-get-outcome-result/v1"
        );
        assert_eq!(
            fixed.emitted_credit - fixed.fixed,
            service_owned_fixed_schemas[0].canonical_bytes(),
            "the fixed outcome tool reuses the service-owned GetOutcome result schema"
        );
    }

    #[test]
    fn tools_list_component_ledgers_cover_maximum_fixed_dynamic_and_mixed_pages() {
        let fixed = McpToolPage::new(
            (1..=14).map(McpToolDiscoveryItem::Fixed).collect(),
            Some([0xff; 16]),
        )
        .expect("complete fixed page");
        assert_eq!(fixed.items().len(), 14);

        let dynamic: Vec<_> = (0..MAX_MCP_DISCOVERY_PAGE_ITEMS)
            .map(|index| dynamic_tool_named(format!("riffdb_cmd_budget_command{index:03}")))
            .collect();
        for (fixed_count, expected_maximum) in [(0, 500), (14, 486)] {
            let maximum = maximum_dynamic_page_count(fixed_count, &dynamic);
            assert_eq!(maximum, expected_maximum);
            McpToolPage::new(
                tool_page_items(fixed_count, &dynamic, maximum),
                Some([0xff; 16]),
            )
            .expect("maximum component-bounded page");

            let maximum_by_item_count = MAX_MCP_DISCOVERY_PAGE_ITEMS - usize::from(fixed_count);
            if maximum < maximum_by_item_count {
                assert_eq!(
                    McpToolPage::new(
                        tool_page_items(fixed_count, &dynamic, maximum + 1),
                        Some([0xff; 16]),
                    )
                    .expect_err("first component-over-budget page"),
                    McpHandlerContractError
                );
            } else {
                let mut over = tool_page_items(fixed_count, &dynamic, maximum);
                over.push(McpToolDiscoveryItem::Dynamic(Box::new(dynamic_tool_named(
                    "riffdb_cmd_budget_overflow",
                ))));
                assert_eq!(
                    McpToolPage::new(over, Some([0xff; 16])).expect_err("item count over maximum"),
                    McpHandlerContractError
                );
            }
        }

        let maximum_metadata: Vec<_> = (0..MAX_MCP_DISCOVERY_PAGE_ITEMS)
            .map(|index| {
                dynamic_tool_named_with_metadata(
                    format!("riffdb_cmd_maximum_command{index:03}"),
                    Some("t".repeat(MAX_MCP_DYNAMIC_TOOL_TITLE_BYTES)),
                    Some("d".repeat(MAX_MCP_DYNAMIC_TOOL_DESCRIPTION_BYTES)),
                )
            })
            .collect();
        assert_eq!(
            McpToolPage::new(
                tool_page_items(0, &maximum_metadata, MAX_MCP_DISCOVERY_PAGE_ITEMS),
                Some([0xff; 16]),
            )
            .expect_err("maximum metadata exceeds the residual adapter component"),
            McpHandlerContractError
        );
    }

    #[test]
    fn discovery_materializes_exact_registry_and_preserves_cursor_bytes() {
        let backend = FakeBackend::new();
        let server = RiffDbMcpServer::new_stdio(backend.clone());
        let extensions = extensions();
        let result =
            block_on(server.handle_list_tools(None, &extensions, None)).expect("tool page");

        assert_eq!(result.tools.len(), 15);
        assert_eq!(result.tools[0].name, "riffdb_contract_validate");
        assert_eq!(
            result.tools.last().expect("dynamic").name,
            "riffdb_cmd_orders_place"
        );
        assert_eq!(
            result.next_cursor.as_deref(),
            Some("abababababababababababababababab")
        );
        assert_eq!(backend.state().discover_tool_calls, 1);
        assert_eq!(backend.state().begin_ids.len(), 1);

        let continuation = PaginatedRequestParams::default()
            .with_cursor(Some("000102030405060708090a0b0c0d0e0f".to_owned()));
        block_on(server.handle_list_tools(Some(continuation), &extensions, None))
            .expect("continuation");
        assert_eq!(
            backend.state().last_tool_cursor,
            Some([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
        );
        let state = backend.state();
        assert_eq!(state.begin_ids.len(), 2);
        assert_ne!(state.begin_ids[0], state.begin_ids[1]);
        assert!(
            state
                .begin_sources
                .iter()
                .all(|source| *source == McpTransportKind::Stdio)
        );
    }

    #[cfg(feature = "streamable-http")]
    #[test]
    fn missing_per_request_carrier_denies_before_discovery() {
        let backend = FakeBackend::new();
        let telemetry = Arc::new(RecordingMcpTelemetry::default());
        let server = RiffDbMcpServer::with_shared_admission_and_telemetry(
            backend.clone(),
            McpTransportKind::StreamableHttp,
            Arc::new(McpInflightLimiter::new()),
            McpAdmissionSessionKey::new("hosted-test").expect("session"),
            telemetry.clone(),
        );
        let error = block_on(server.handle_list_tools(None, &Extensions::new(), None))
            .expect_err("missing carrier");

        assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
        assert_eq!(backend.state().discover_tool_calls, 0);
        assert!(backend.state().begin_ids.is_empty());
        assert!(
            telemetry
                .snapshot()
                .contains(&McpTelemetryEvent::AuthorizationDenied)
        );
    }

    #[test]
    fn exact_session_and_server_admission_reject_before_backend_work() {
        let session_limiter = Arc::new(McpInflightLimiter::new());
        let session = McpAdmissionSessionKey::new("saturated-session").expect("session");
        let session_permits: Vec<_> = (0..crate::MAX_MCP_SESSION_IN_FLIGHT)
            .map(|_| {
                session_limiter
                    .try_acquire(session.clone())
                    .expect("within session bound")
            })
            .collect();
        let session_backend = FakeBackend::new();
        let session_server = RiffDbMcpServer::with_shared_admission(
            session_backend.clone(),
            McpTransportKind::Stdio,
            Arc::clone(&session_limiter),
            session,
        );
        let error = block_on(session_server.handle_list_tools(None, &extensions(), None))
            .expect_err("session capacity");
        assert_eq!(error.code, ErrorCode::INTERNAL_ERROR);
        assert_eq!(session_backend.state().discover_tool_calls, 0);
        assert!(session_backend.state().begin_ids.is_empty());
        drop(session_permits);

        let server_limiter = Arc::new(McpInflightLimiter::new());
        let server_permits: Vec<_> = (0..crate::MAX_MCP_SERVER_IN_FLIGHT)
            .map(|index| {
                server_limiter
                    .try_acquire(
                        McpAdmissionSessionKey::new(format!("server-{index}")).expect("session"),
                    )
                    .expect("within server bound")
            })
            .collect();
        let server_backend = FakeBackend::new();
        let server = RiffDbMcpServer::with_shared_admission(
            server_backend.clone(),
            McpTransportKind::StreamableHttp,
            Arc::clone(&server_limiter),
            McpAdmissionSessionKey::new("overflow-session").expect("session"),
        );
        let error = block_on(server.handle_list_tools(None, &extensions(), None))
            .expect_err("server capacity");
        assert_eq!(error.code, ErrorCode::INTERNAL_ERROR);
        assert_eq!(server_backend.state().discover_tool_calls, 0);
        assert!(server_backend.state().begin_ids.is_empty());
        drop(server_permits);
    }

    #[test]
    fn fixed_input_validation_happens_before_begin_or_invoke() {
        let backend = FakeBackend::new();
        let telemetry = Arc::new(RecordingMcpTelemetry::default());
        let server = RiffDbMcpServer::new_stdio_with_telemetry(backend.clone(), telemetry.clone());
        let request = CallToolRequestParams::new("riffdb_server_health")
            .with_arguments(json!({"unexpected": true}).as_object().unwrap().clone());
        let error =
            block_on(server.handle_call_tool_with_cancellation(request, &extensions(), None))
                .expect_err("invalid fixed input");

        assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
        assert_eq!(
            error.data,
            Some(json!({
                "schema": "riffdb.mcp.input-error/v1",
                "code": "unexpected_property",
                "path": "/unexpected",
                "expected": "declared property",
            }))
        );
        assert!(
            !serde_json::to_string(&error)
                .expect("error JSON")
                .contains("true")
        );
        assert_eq!(backend.state().invoke_calls, 0);
        assert!(backend.state().begin_ids.is_empty());
        assert_eq!(
            telemetry.snapshot(),
            vec![
                McpTelemetryEvent::SessionOpened {
                    transport: McpTransportKind::Stdio,
                },
                McpTelemetryEvent::ToolCall {
                    risk: McpRiskClass::ReadOnly,
                },
                McpTelemetryEvent::SchemaFailure {
                    phase: McpSchemaFailurePhase::Input,
                },
            ]
        );
        drop(server);
        assert_eq!(
            telemetry.snapshot().last(),
            Some(&McpTelemetryEvent::SessionClosed {
                transport: McpTransportKind::Stdio,
            })
        );
    }

    #[test]
    fn dynamic_call_uses_fresh_resolve_and_execute_contexts() {
        let backend = FakeBackend::new();
        let server = RiffDbMcpServer::new_stdio(backend.clone());
        let request = CallToolRequestParams::new("riffdb_cmd_orders_place")
            .with_arguments(json!({"value": "one"}).as_object().unwrap().clone());
        let result =
            block_on(server.handle_call_tool_with_cancellation(request, &extensions(), None))
                .expect("dynamic result");

        assert_eq!(result.is_error, Some(false));
        assert_eq!(result.structured_content, Some(dynamic_result()));
        let state = backend.state();
        assert_eq!(state.resolve_calls, 1);
        assert_eq!(state.invoke_calls, 1);
        assert_eq!(state.begin_ids.len(), 2);
        assert_ne!(state.begin_ids[0], state.begin_ids[1]);
    }

    #[test]
    fn unknown_stale_and_unavailable_dynamic_targets_are_indistinguishable() {
        let backend = FakeBackend::new();
        backend.state().dynamic_available = false;
        let server = RiffDbMcpServer::new_stdio(backend.clone());
        let request = CallToolRequestParams::new("riffdb_cmd_orders_place")
            .with_arguments(json!({"value": "one"}).as_object().unwrap().clone());
        let error =
            block_on(server.handle_call_tool_with_cancellation(request, &extensions(), None))
                .expect_err("unavailable");

        assert_eq!(error.code, ErrorCode::METHOD_NOT_FOUND);
        assert_eq!(error.message, "tool unavailable");
        assert_eq!(backend.state().invoke_calls, 0);
    }

    #[test]
    fn public_errors_use_only_the_common_redacted_tool_error_view() {
        let backend = FakeBackend::new();
        backend.state().invoke_mode = InvokeMode::PublicAuthorization;
        let telemetry = Arc::new(RecordingMcpTelemetry::default());
        let server = RiffDbMcpServer::new_stdio_with_telemetry(backend, telemetry.clone());
        let request = CallToolRequestParams::new("riffdb_cmd_orders_place")
            .with_arguments(json!({"value": "one"}).as_object().unwrap().clone());
        let result =
            block_on(server.handle_call_tool_with_cancellation(request, &extensions(), None))
                .expect("public tool error");
        let encoded = serde_json::to_string(&result).expect("result");

        assert_eq!(result.is_error, Some(true));
        assert_eq!(result.structured_content, None);
        assert_eq!(result.content.len(), 1);
        assert!(encoded.contains("authorization_denied"));
        assert!(!encoded.contains("AuthenticatedMarker"));
        assert!(!encoded.contains("riffdb_cmd_orders_place"));
        assert!(telemetry.snapshot().contains(&McpTelemetryEvent::ToolCall {
            risk: McpRiskClass::DynamicCommand,
        }));
        assert!(
            telemetry
                .snapshot()
                .contains(&McpTelemetryEvent::AuthorizationDenied)
        );
    }

    #[test]
    fn resource_lists_reads_and_subscriptions_are_fresh_and_bounded() {
        let backend = FakeBackend::new();
        let server = RiffDbMcpServer::new_stdio(backend.clone());
        let extensions = extensions();

        let resources =
            block_on(server.handle_list_resources(None, &extensions, None)).expect("resources");
        assert_eq!(resources.resources.len(), 2);
        assert_eq!(resources.resources[0].uri, "riffdb://contract/active");
        let templates = block_on(server.handle_list_resource_templates(None, &extensions, None))
            .expect("templates");
        assert_eq!(templates.resource_templates.len(), 2);
        assert_eq!(
            templates.resource_templates[0].uri_template,
            "riffdb://commit/{sequence}"
        );

        let read = block_on(server.handle_read_resource(
            ReadResourceRequestParams::new("riffdb://contract/active"),
            &extensions,
            None,
        ))
        .expect("read");
        assert_eq!(read.contents.len(), 1);
        assert_eq!(
            serde_json::to_value(&read).expect("read result"),
            json!({
                "contents": [{
                    "uri": "riffdb://contract/active",
                    "mimeType": "application/json",
                    "text": "{\"status\":\"active\"}"
                }]
            })
        );

        for _ in 0..2 {
            block_on(server.handle_subscribe(
                SubscribeRequestParams::new("riffdb://contract/active"),
                &extensions,
                None,
            ))
            .expect("idempotent subscribe");
        }
        block_on(server.handle_unsubscribe(
            UnsubscribeRequestParams::new("riffdb://contract/active"),
            &extensions,
            None,
        ))
        .expect("unsubscribe");
        block_on(server.handle_subscribe(
            SubscribeRequestParams::new("riffdb://reactive/wakeup"),
            &extensions,
            None,
        ))
        .expect("subscribe to reactive wakeup");
        block_on(server.handle_unsubscribe(
            UnsubscribeRequestParams::new("riffdb://reactive/wakeup"),
            &extensions,
            None,
        ))
        .expect("unsubscribe from reactive wakeup");

        let state = backend.state();
        assert_eq!(state.discover_resource_calls, 2);
        assert_eq!(state.read_calls, 1);
        assert_eq!(state.subscription_calls, 5);
        assert_eq!(state.begin_ids.len(), 8);
    }

    #[test]
    fn pre_cancelled_operations_stop_before_invocation_or_backend_work() {
        let cancellations = Arc::new(McpCancellationRegistry::new());
        let request_id = RequestId::Number(77);
        let (signal, _guard) = cancellations
            .register(request_id.clone())
            .expect("cancellation registration");
        assert!(
            cancellations
                .cancel(&request_id)
                .expect("cancel live request")
        );

        let backend = FakeBackend::new();
        let server = RiffDbMcpServer::new_stdio(backend.clone());
        let extensions = extensions();
        assert_eq!(
            block_on(server.handle_list_tools(None, &extensions, Some(signal.clone())))
                .expect_err("cancelled tool discovery")
                .code,
            ErrorCode(-32_000)
        );
        assert_eq!(
            block_on(server.handle_list_resources(None, &extensions, Some(signal.clone())))
                .expect_err("cancelled resource discovery")
                .code,
            ErrorCode(-32_000)
        );
        assert_eq!(
            block_on(server.handle_list_resource_templates(
                None,
                &extensions,
                Some(signal.clone())
            ))
            .expect_err("cancelled template discovery")
            .code,
            ErrorCode(-32_000)
        );
        assert_eq!(
            block_on(server.handle_read_resource(
                ReadResourceRequestParams::new("riffdb://contract/active"),
                &extensions,
                Some(signal.clone()),
            ))
            .expect_err("cancelled resource read")
            .code,
            ErrorCode(-32_000)
        );
        assert_eq!(
            block_on(server.handle_subscribe(
                SubscribeRequestParams::new("riffdb://contract/active"),
                &extensions,
                Some(signal.clone()),
            ))
            .expect_err("cancelled subscribe")
            .code,
            ErrorCode(-32_000)
        );
        assert_eq!(
            block_on(server.handle_unsubscribe(
                UnsubscribeRequestParams::new("riffdb://contract/active"),
                &extensions,
                Some(signal.clone()),
            ))
            .expect_err("cancelled unsubscribe")
            .code,
            ErrorCode(-32_000)
        );
        let request = CallToolRequestParams::new("riffdb_cmd_orders_place")
            .with_arguments(json!({"value": "one"}).as_object().unwrap().clone());
        assert_eq!(
            block_on(server.handle_call_tool_with_cancellation(request, &extensions, Some(signal)))
                .expect_err("cancelled tool call")
                .code,
            ErrorCode(-32_000)
        );

        let state = backend.state();
        assert!(state.begin_ids.is_empty());
        assert_eq!(state.discover_tool_calls, 0);
        assert_eq!(state.discover_resource_calls, 0);
        assert_eq!(state.read_calls, 0);
        assert_eq!(state.subscription_calls, 0);
        assert_eq!(state.resolve_calls, 0);
        assert_eq!(state.invoke_calls, 0);
    }

    #[test]
    fn cancellation_races_pending_discovery_and_command_submission_without_emission() {
        let discovery_registry = Arc::new(McpCancellationRegistry::new());
        let discovery_id = RequestId::Number(78);
        let (discovery_signal, _discovery_guard) = discovery_registry
            .register(discovery_id.clone())
            .expect("discovery cancellation registration");
        let discovery_backend = FakeBackend::new();
        discovery_backend.state().cancel_during_tool_discovery = Some(CancellationTrigger {
            registry: Arc::clone(&discovery_registry),
            request_id: discovery_id,
        });
        let discovery_server = RiffDbMcpServer::new_stdio(discovery_backend.clone());
        let error = block_on(discovery_server.handle_list_tools(
            None,
            &extensions(),
            Some(discovery_signal),
        ))
        .expect_err("pending discovery cancelled");
        assert_eq!(error.code, ErrorCode(-32_000));
        let state = discovery_backend.state();
        assert_eq!(state.begin_ids.len(), 1);
        assert_eq!(state.discover_tool_calls, 1);
        assert_eq!(state.begin_cancellation_present, vec![true]);
        assert_eq!(state.begin_cancelled, vec![false]);
        drop(state);

        let invoke_registry = Arc::new(McpCancellationRegistry::new());
        let invoke_id = RequestId::Number(79);
        let (invoke_signal, _invoke_guard) = invoke_registry
            .register(invoke_id.clone())
            .expect("invoke cancellation registration");
        let invoke_backend = FakeBackend::new();
        invoke_backend.state().cancel_during_invoke = Some(CancellationTrigger {
            registry: Arc::clone(&invoke_registry),
            request_id: invoke_id,
        });
        let invoke_server = RiffDbMcpServer::new_stdio(invoke_backend.clone());
        let request = CallToolRequestParams::new("riffdb_cmd_orders_place")
            .with_arguments(json!({"value": "one"}).as_object().unwrap().clone());
        let error = block_on(invoke_server.handle_call_tool_with_cancellation(
            request,
            &extensions(),
            Some(invoke_signal),
        ))
        .expect_err("pending command submission cancelled");
        assert_eq!(error.code, ErrorCode(-32_000));
        assert_eq!(error.message, "request cancelled");
        assert!(error.data.is_none());
        let state = invoke_backend.state();
        assert_eq!(state.begin_ids.len(), 2);
        assert_eq!(state.resolve_calls, 1);
        assert_eq!(state.invoke_calls, 1);
        assert!(
            state
                .begin_cancellation_present
                .iter()
                .all(|present| *present)
        );
        assert!(state.begin_cancelled.iter().all(|cancelled| !cancelled));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn session_cancellation_drops_pending_backend_work_and_releases_admission() {
        struct DropMarker(Arc<AtomicBool>);

        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let cancellations = Arc::new(McpCancellationRegistry::new());
        let (signal, _guard) = cancellations
            .register(RequestId::Number(80))
            .expect("live cancellation");
        let limiter = Arc::new(McpInflightLimiter::new());
        let session = McpAdmissionSessionKey::new("lifecycle-test").expect("session");
        let permit = limiter
            .try_acquire(session.clone())
            .expect("initial permit");
        let dropped = Arc::new(AtomicBool::new(false));
        let (polled, observed_poll) = tokio::sync::oneshot::channel();
        let backend: McpBackendFuture<'static, ()> = Box::pin({
            let dropped = Arc::clone(&dropped);
            async move {
                let _drop_marker = DropMarker(dropped);
                let _ = polled.send(());
                std::future::pending().await
            }
        });

        let operation = tokio::spawn(async move {
            let _permit = permit;
            await_backend_with_cancellation(Some(&signal), backend).await
        });
        observed_poll.await.expect("backend was polled");
        cancellations.cancel_all();
        assert!(matches!(
            operation.await.expect("operation task"),
            Err(McpBackendError::Cancelled)
        ));
        assert!(dropped.load(Ordering::Acquire));

        let recovered: Vec<_> = (0..crate::MAX_MCP_SESSION_IN_FLIGHT)
            .map(|_| {
                limiter
                    .try_acquire(session.clone())
                    .expect("released session permit")
            })
            .collect();
        assert!(matches!(
            limiter.try_acquire(session),
            Err(crate::McpAdmissionError::CapacityExhausted)
        ));
        drop(recovered);
    }

    #[test]
    fn malformed_cursor_resource_and_unsupported_subscription_never_reach_backend() {
        let backend = FakeBackend::new();
        let server = RiffDbMcpServer::new_stdio(backend.clone());
        let invalid_cursor =
            PaginatedRequestParams::default().with_cursor(Some("UPPERCASE".to_owned()));
        assert_eq!(
            block_on(server.handle_list_resources(Some(invalid_cursor), &extensions(), None))
                .expect_err("cursor")
                .code,
            ErrorCode::INVALID_PARAMS
        );
        assert_eq!(
            block_on(server.handle_subscribe(
                SubscribeRequestParams::new("riffdb://commit/1"),
                &extensions(),
                None,
            ))
            .expect_err("unsupported")
            .code,
            ErrorCode::RESOURCE_NOT_FOUND
        );
        let state = backend.state();
        assert_eq!(state.discover_resource_calls, 0);
        assert_eq!(state.subscription_calls, 0);
        assert!(state.begin_ids.is_empty());
    }

    #[test]
    fn invalid_backend_outputs_never_cross_the_mcp_boundary() {
        let backend = FakeBackend::new();
        backend.state().invoke_mode = InvokeMode::Success(json!({"status": "wrong"}));
        let server = RiffDbMcpServer::new_stdio(backend.clone());
        let request = CallToolRequestParams::new("riffdb_cmd_orders_place")
            .with_arguments(json!({"value": "one"}).as_object().unwrap().clone());
        let error =
            block_on(server.handle_call_tool_with_cancellation(request, &extensions(), None))
                .expect_err("invalid output");
        assert_eq!(error.code, ErrorCode::INTERNAL_ERROR);

        backend.state().invoke_mode = InvokeMode::TargetUnavailable;
        let request = CallToolRequestParams::new("riffdb_cmd_orders_place")
            .with_arguments(json!({"value": "one"}).as_object().unwrap().clone());
        assert_eq!(
            block_on(server.handle_call_tool_with_cancellation(request, &extensions(), None,))
                .expect_err("unavailable")
                .code,
            ErrorCode::METHOD_NOT_FOUND
        );

        backend.state().invoke_mode = InvokeMode::InvalidResponse;
        let request = CallToolRequestParams::new("riffdb_cmd_orders_place")
            .with_arguments(json!({"value": "one"}).as_object().unwrap().clone());
        assert_eq!(
            block_on(server.handle_call_tool_with_cancellation(request, &extensions(), None,))
                .expect_err("invalid response")
                .code,
            ErrorCode::INTERNAL_ERROR
        );

        backend.state().invoke_mode = InvokeMode::Cancelled;
        let request = CallToolRequestParams::new("riffdb_cmd_orders_place")
            .with_arguments(json!({"value": "one"}).as_object().unwrap().clone());
        let cancellation =
            block_on(server.handle_call_tool_with_cancellation(request, &extensions(), None))
                .expect_err("cancelled");
        assert_eq!(cancellation.code, ErrorCode(-32_000));
        assert_eq!(cancellation.message, "request cancelled");
        assert!(cancellation.data.is_none());
    }
}
