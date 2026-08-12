use std::collections::{BTreeSet, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, OnceLock};

use riffdb_types::{SchemaHash, hash_schema};
use rmcp::model::{Resource, ResourceTemplate, Tool, ToolAnnotations};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::parse_resource_locator;

const FIXED_TOOL_REGISTRY_SOURCE: &str = include_str!("../fixtures/fixed-tool-registry-v1.json");
const RESOURCE_REGISTRY_SOURCE: &str = include_str!("../fixtures/resource-registry-v1.json");

const FIXED_TOOL_REGISTRY_ID: &str = "riffdb.mcp.fixed-tool-registry/v1";
const RESOURCE_REGISTRY_ID: &str = "riffdb.mcp.resource-registry/v1";
const ACCEPTED_CHECKPOINT: &str = "accepted-by-human-maintainer-2026-07-30";
const RESOURCE_ACCEPTED_CHECKPOINT: &str = "accepted-by-human-maintainer-2026-08-04";
const SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
const MAX_SCHEMA_BYTES: usize = 65_536;
const MAX_DYNAMIC_SCHEMA_BYTES: usize = 1_048_576;
const MAX_FIXED_SCHEMA_BYTES: usize = 1_048_576;
const EXPECTED_FIXED_SCHEMA_BYTES: usize = 70_194;

static FIXED_TOOL_REGISTRY: OnceLock<Result<FixedToolRegistry, RegistryError>> = OnceLock::new();
static RESOURCE_REGISTRY: OnceLock<Result<ResourceRegistry, RegistryError>> = OnceLock::new();

/// One checked canonical Draft 2020-12 schema source.
#[derive(Clone, Debug)]
pub struct SchemaDocument {
    schema_id: String,
    schema_hash: SchemaHash,
    canonical_json: Arc<str>,
    object: Map<String, Value>,
    source_path: Option<String>,
}

impl Serialize for SchemaDocument {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.object.serialize(serializer)
    }
}

/// Closed generated-schema artifact key kind used by public discovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpGeneratedSchemaKind {
    /// Entity record schema.
    Entity,
    /// Durable event payload schema.
    Event,
    /// Command input schema.
    CommandInput,
    /// Command declared-outcome union.
    CommandOutcomeUnion,
    /// Projection result-row schema.
    ProjectionResult,
}

impl McpGeneratedSchemaKind {
    const fn identity_segment(self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::Event => "event",
            Self::CommandInput => "command-input",
            Self::CommandOutcomeUnion => "command-outcome-union",
            Self::ProjectionResult => "projection-result",
        }
    }
}

impl SchemaDocument {
    /// Checks and owns one canonical compiler or service schema artifact.
    ///
    /// The caller supplies the upstream identity and claimed domain-separated
    /// hash. No identity is derived from presentation metadata.
    pub(crate) fn from_canonical(
        schema_id: impl Into<String>,
        schema_hash: SchemaHash,
        canonical_json: impl Into<String>,
    ) -> Result<Self, RegistryError> {
        let schema_id = schema_id.into();
        let canonical_json = canonical_json.into();
        if schema_id.is_empty()
            || schema_id.len() > 256
            || canonical_json.len() > MAX_DYNAMIC_SCHEMA_BYTES
            || hash_schema(canonical_json.as_bytes()) != schema_hash
        {
            return Err(RegistryError);
        }
        require_canonical_json(&canonical_json)?;
        let value: Value = serde_json::from_str(&canonical_json).map_err(|_| RegistryError)?;
        let object = value.as_object().ok_or(RegistryError)?.clone();
        if object.get("$schema").and_then(Value::as_str) != Some(SCHEMA_DIALECT)
            || !root_is_closed(&object)
            || crate::schema::validate_schema_source(&value).is_err()
        {
            return Err(RegistryError);
        }
        Ok(Self {
            schema_id,
            schema_hash,
            canonical_json: Arc::from(canonical_json),
            object,
            source_path: None,
        })
    }

    /// Checks one schema received from the public gRPC representation.
    ///
    /// The hash must be exactly 32 bytes. This constructor neither derives nor
    /// normalizes an upstream identity and keeps `SchemaHash` owned by the
    /// common adapter crate.
    pub fn from_public_parts(
        schema_id: impl Into<String>,
        schema_hash: &[u8],
        canonical_json: impl Into<String>,
    ) -> Result<Self, RegistryError> {
        let schema_hash: [u8; 32] = schema_hash.try_into().map_err(|_| RegistryError)?;
        Self::from_canonical(
            schema_id,
            SchemaHash::from_bytes(schema_hash),
            canonical_json,
        )
    }

    /// Checks one generated schema received with its typed public artifact key.
    ///
    /// The derived identity is adapter-internal bookkeeping. Generated schema
    /// JSON remains owned by the compiled bundle and no derived identifier is
    /// emitted into the advertised schema document.
    pub fn from_public_generated(
        kind: McpGeneratedSchemaKind,
        stable_id: u32,
        schema_hash: &[u8],
        canonical_json: impl Into<String>,
    ) -> Result<Self, RegistryError> {
        if stable_id == 0 {
            return Err(RegistryError);
        }
        let schema_id = format!(
            "riffdb.generated-schema/{}/{stable_id}/v1",
            kind.identity_segment()
        );
        Self::from_public_parts(schema_id, schema_hash, canonical_json)
    }

    /// Returns the exact versioned schema identity.
    #[must_use]
    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }

    /// Returns the exact 32 public-wire hash bytes.
    #[must_use]
    pub const fn schema_hash_bytes(&self) -> [u8; 32] {
        *self.schema_hash.as_bytes()
    }

    /// Returns the accepted canonical JSON bytes as UTF-8 text.
    #[must_use]
    pub fn canonical_json(&self) -> &str {
        &self.canonical_json
    }

    /// Returns the canonical byte length.
    #[must_use]
    pub fn canonical_bytes(&self) -> usize {
        self.canonical_json.len()
    }

    /// Clones the checked schema object as an already bounded MCP resource body.
    pub fn to_resource_json(&self) -> Result<crate::McpResourceJson, RegistryError> {
        crate::McpResourceJson::from_serializable(&self.object).map_err(|_| RegistryError)
    }

    /// Returns the accepted repository-relative source path when embedded.
    #[must_use]
    pub fn source_path(&self) -> Option<&str> {
        self.source_path.as_deref()
    }

    /// Clones the parsed JSON object for an MCP SDK descriptor.
    #[must_use]
    pub(crate) fn json_object(&self) -> Map<String, Value> {
        self.object.clone()
    }
}

/// One fixed tool and its exact accepted presentation metadata.
#[derive(Clone, Debug)]
pub struct FixedToolDefinition {
    kind: u8,
    name: String,
    title: String,
    description: String,
    risk_class: String,
    grpc_service: String,
    grpc_method: String,
    service_operation: String,
    request_converter_id: String,
    result_converter_id: String,
    result_branches: Vec<String>,
    annotations: FixedToolAnnotations,
    input_schema: SchemaDocument,
    result_schema: SchemaDocument,
}

impl FixedToolDefinition {
    /// Returns the exact one-based `FixedToolKind` compatibility tag.
    #[must_use]
    pub const fn kind(&self) -> u8 {
        self.kind
    }

    /// Returns the exact fixed tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the accepted bounded title.
    #[must_use]
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the accepted bounded description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the accepted risk-class identifier.
    #[must_use]
    pub fn risk_class(&self) -> &str {
        &self.risk_class
    }

    /// Returns the matching public gRPC service.
    #[must_use]
    pub fn grpc_service(&self) -> &str {
        &self.grpc_service
    }

    /// Returns the matching public gRPC method.
    #[must_use]
    pub fn grpc_method(&self) -> &str {
        &self.grpc_method
    }

    /// Returns the API-neutral service operation.
    #[must_use]
    pub fn service_operation(&self) -> &str {
        &self.service_operation
    }

    /// Returns the accepted request converter identity.
    #[must_use]
    pub fn request_converter_id(&self) -> &str {
        &self.request_converter_id
    }

    /// Returns the accepted result converter identity.
    #[must_use]
    pub fn result_converter_id(&self) -> &str {
        &self.result_converter_id
    }

    /// Returns the ordered top-level business-result branches.
    #[must_use]
    pub fn result_branches(&self) -> &[String] {
        &self.result_branches
    }

    /// Returns the fixed input schema.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaDocument {
        &self.input_schema
    }

    /// Returns the fixed result schema.
    #[must_use]
    pub const fn result_schema(&self) -> &SchemaDocument {
        &self.result_schema
    }

    /// Materializes the exact SDK tool descriptor from accepted registry data.
    #[must_use]
    pub fn to_mcp_tool(&self) -> Tool {
        Tool::new_with_raw(
            self.name.clone(),
            Some(self.description.clone().into()),
            Arc::new(self.input_schema.json_object()),
        )
        .with_title(self.title.clone())
        .with_raw_output_schema(Arc::new(self.result_schema.json_object()))
        .with_annotations(ToolAnnotations::from_raw(
            None,
            Some(self.annotations.read_only_hint),
            Some(self.annotations.destructive_hint),
            Some(self.annotations.idempotent_hint),
            Some(self.annotations.open_world_hint),
        ))
    }
}

/// The exact ordered fixed-tool registry.
#[derive(Clone, Debug)]
pub struct FixedToolRegistry {
    tools: Vec<FixedToolDefinition>,
    artifact_manifest: Vec<String>,
    operation_schemas: Vec<SchemaDocument>,
    fixed_schema_bytes: usize,
}

impl FixedToolRegistry {
    /// Returns all fixed tools in exact `FixedToolKind` order.
    #[must_use]
    pub fn tools(&self) -> &[FixedToolDefinition] {
        &self.tools
    }

    /// Finds one exact fixed name without normalization.
    #[must_use]
    pub fn by_name(&self, name: &str) -> Option<&FixedToolDefinition> {
        self.tools.iter().find(|tool| tool.name == name)
    }

    /// Returns the exact ordered fixed-artifact manifest.
    #[must_use]
    pub fn artifact_manifest(&self) -> &[String] {
        &self.artifact_manifest
    }

    /// Returns the two service-owned operation schemas in exact order.
    #[must_use]
    pub fn operation_schemas(&self) -> &[SchemaDocument] {
        &self.operation_schemas
    }

    /// Returns the aggregate bytes of the 27 local fixed schema sources.
    #[must_use]
    pub const fn fixed_schema_bytes(&self) -> usize {
        self.fixed_schema_bytes
    }
}

/// The MCP list surface on which a resource descriptor appears.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceSurface {
    /// `resources/list`.
    Concrete,
    /// `resources/templates/list`.
    Template,
}

/// One exact accepted resource or resource-template descriptor mapping.
#[derive(Clone, Debug)]
pub struct ResourceDefinition {
    descriptor_branch: String,
    surface: ResourceSurface,
    uri: String,
    mime_type: String,
    golden_name: String,
    golden_uri: String,
    title: String,
    description: String,
    subscribable: bool,
    content_converter_id: String,
    content_golden: ResourceContentGolden,
}

impl ResourceDefinition {
    /// Returns the exact service descriptor branch.
    #[must_use]
    pub fn descriptor_branch(&self) -> &str {
        &self.descriptor_branch
    }

    /// Returns the sole MCP inventory surface for this branch.
    #[must_use]
    pub const fn surface(&self) -> ResourceSurface {
        self.surface
    }

    /// Returns the accepted concrete pattern or RFC 6570 template.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the accepted MIME type.
    #[must_use]
    pub fn mime_type(&self) -> &str {
        &self.mime_type
    }

    /// Returns the accepted concrete golden URI.
    #[must_use]
    pub fn golden_uri(&self) -> &str {
        &self.golden_uri
    }

    /// Returns whether this exact resource kind may be subscribed.
    #[must_use]
    pub const fn subscribable(&self) -> bool {
        self.subscribable
    }

    /// Returns the accepted content converter identity.
    #[must_use]
    pub fn content_converter_id(&self) -> &str {
        &self.content_converter_id
    }

    /// Returns the accepted bounded result-content golden.
    #[must_use]
    pub fn content_golden(&self) -> (&str, &str, &str) {
        (
            &self.content_golden.uri,
            &self.content_golden.mime_type,
            &self.content_golden.text,
        )
    }

    /// Builds a concrete MCP resource descriptor from an already checked URI.
    pub fn to_mcp_resource(&self, canonical_uri: &str) -> Result<Resource, RegistryError> {
        if self.surface != ResourceSurface::Concrete
            || parse_resource_locator(canonical_uri).is_err()
        {
            return Err(RegistryError);
        }
        Ok(Resource::new(canonical_uri, canonical_uri)
            .with_title(self.title.clone())
            .with_description(self.description.clone())
            .with_mime_type(self.mime_type.clone()))
    }

    /// Builds the exact MCP template descriptor recorded by the registry.
    pub fn to_mcp_template(&self) -> Result<ResourceTemplate, RegistryError> {
        if self.golden_name != self.uri {
            return Err(RegistryError);
        }
        self.to_mcp_template_uri(&self.uri)
    }

    pub(crate) fn to_mcp_template_uri(
        &self,
        checked_uri_template: &str,
    ) -> Result<ResourceTemplate, RegistryError> {
        if self.surface != ResourceSurface::Template {
            return Err(RegistryError);
        }
        Ok(
            ResourceTemplate::new(checked_uri_template, checked_uri_template)
                .with_title(self.title.clone())
                .with_description(self.description.clone())
                .with_mime_type(self.mime_type.clone()),
        )
    }
}

/// The exact ordered resource and resource-template registry.
#[derive(Clone, Debug)]
pub struct ResourceRegistry {
    entries: Vec<ResourceDefinition>,
}

impl ResourceRegistry {
    /// Returns all 13 mappings in structural registry order.
    #[must_use]
    pub fn entries(&self) -> &[ResourceDefinition] {
        &self.entries
    }

    /// Finds one exact descriptor branch without fallback or normalization.
    #[must_use]
    pub fn by_descriptor_branch(&self, branch: &str) -> Option<&ResourceDefinition> {
        self.entries
            .iter()
            .find(|entry| entry.descriptor_branch == branch)
    }
}

/// Loads and validates the embedded fixed-tool registry exactly once.
pub fn fixed_tool_registry() -> Result<&'static FixedToolRegistry, RegistryError> {
    match FIXED_TOOL_REGISTRY.get_or_init(load_fixed_tool_registry) {
        Ok(registry) => Ok(registry),
        Err(error) => Err(*error),
    }
}

/// Loads and validates the embedded resource registry exactly once.
pub fn resource_registry() -> Result<&'static ResourceRegistry, RegistryError> {
    match RESOURCE_REGISTRY.get_or_init(load_resource_registry) {
        Ok(registry) => Ok(registry),
        Err(error) => Err(*error),
    }
}

fn load_fixed_tool_registry() -> Result<FixedToolRegistry, RegistryError> {
    require_canonical_json(FIXED_TOOL_REGISTRY_SOURCE)?;
    let wire: FixedRegistryWire =
        serde_json::from_str(FIXED_TOOL_REGISTRY_SOURCE).map_err(|_| RegistryError)?;

    if wire.schema != FIXED_TOOL_REGISTRY_ID
        || wire.compatibility_checkpoint != ACCEPTED_CHECKPOINT
        || wire.conversion_golden_path
            != "crates/riffdb-api-mcp/fixtures/fixed-tool-conversion-goldens-v1.json"
        || wire.counts
            != (FixedCountsWire {
                fixed_tools: 14,
                new_fixed_schema_sources: 27,
                top_level_result_branches: 31,
                unique_operation_and_fixed_artifacts: 29,
            })
        || wire.schema_hashing.scheme != 1
        || wire.schema_hashing.domain != "riffdb.schema/v1"
        || wire.schema_hashing.framing != "RIFFDB-HASH-v1"
        || wire.tools.len() != 14
        || wire.operation_schemas.len() != 2
        || wire.artifact_manifest.len() != 29
        || wire.fixed_schema_ledger.canonical_source_bytes != EXPECTED_FIXED_SCHEMA_BYTES
        || wire.fixed_schema_ledger.component_ceiling_bytes != MAX_FIXED_SCHEMA_BYTES
        || wire.fixed_schema_ledger.service_discovery_ceiling_bytes != 2_621_440
        || wire.fixed_schema_ledger.remaining_adapter_ceiling_bytes != 524_288
        || wire.fixed_schema_ledger.aggregate_response_ceiling_bytes != 4_194_304
        || wire
            .fixed_schema_ledger
            .boundary_witnesses
            .as_array()
            .is_none_or(|witnesses| witnesses.len() != 3)
        || wire.fixed_schema_ledger.full_page_rule.is_empty()
        || wire.validation_boundary.as_object().is_none_or(|boundary| {
            boundary
                .get("draft_2020_12")
                .and_then(Value::as_str)
                .is_none()
                || boundary
                    .get("drift_control")
                    .and_then(Value::as_str)
                    .is_none()
        })
    {
        return Err(RegistryError);
    }
    if wire
        .operation_schemas
        .iter()
        .any(|schema| schema.owner.as_deref() != Some("riffdb-service"))
    {
        return Err(RegistryError);
    }
    let operation_schemas = wire
        .operation_schemas
        .iter()
        .map(schema_document)
        .collect::<Result<Vec<_>, _>>()?;
    let mut tools = Vec::with_capacity(wire.tools.len());
    let mut names = HashSet::with_capacity(wire.tools.len());
    let mut local_sources = BTreeSet::new();
    let mut result_branches = 0_usize;

    for (index, tool) in wire.tools.into_iter().enumerate() {
        let expected_kind = u8::try_from(index + 1).map_err(|_| RegistryError)?;
        if tool.fixed_tool_kind != expected_kind
            || !names.insert(tool.name.clone())
            || !valid_public_tool_name(&tool.name)
            || tool.annotations.open_world_hint
            || (tool.name == "riffdb_contract_deploy")
                != (!tool.annotations.read_only_hint && tool.annotations.destructive_hint)
        {
            return Err(RegistryError);
        }
        result_branches = result_branches
            .checked_add(tool.result_branches.len())
            .ok_or(RegistryError)?;
        if tool
            .input_schema
            .source_path
            .starts_with("crates/riffdb-api-mcp/")
        {
            local_sources.insert(tool.input_schema.source_path.clone());
        }
        if tool
            .result_schema
            .source_path
            .starts_with("crates/riffdb-api-mcp/")
        {
            local_sources.insert(tool.result_schema.source_path.clone());
        }

        tools.push(FixedToolDefinition {
            kind: tool.fixed_tool_kind,
            name: tool.name,
            title: tool.title,
            description: tool.description,
            risk_class: tool.risk_class,
            grpc_service: tool.grpc_service,
            grpc_method: tool.grpc_method,
            service_operation: tool.service_operation,
            request_converter_id: tool.request_converter_id,
            result_converter_id: tool.result_converter_id,
            result_branches: tool.result_branches,
            annotations: tool.annotations,
            input_schema: schema_document(&tool.input_schema)?,
            result_schema: schema_document(&tool.result_schema)?,
        });
    }

    if local_sources.len() != 27 || result_branches != 31 {
        return Err(RegistryError);
    }
    let fixed_schema_bytes = local_sources.iter().try_fold(0_usize, |total, path| {
        total
            .checked_add(schema_source(path).ok_or(RegistryError)?.len())
            .ok_or(RegistryError)
    })?;
    if fixed_schema_bytes != EXPECTED_FIXED_SCHEMA_BYTES
        || fixed_schema_bytes > MAX_FIXED_SCHEMA_BYTES
        || wire.artifact_manifest.iter().collect::<HashSet<_>>().len() != 29
    {
        return Err(RegistryError);
    }

    let mut artifact_manifest = wire.artifact_manifest;
    append_symbolic_tools(&mut tools, &mut artifact_manifest)?;
    if tools.iter().any(|tool| !valid_public_tool_name(&tool.name)) {
        return Err(RegistryError);
    }
    let symbolic_schema_bytes = tools.iter().skip(14).try_fold(0_usize, |total, tool| {
        total
            .checked_add(tool.input_schema.canonical_bytes())
            .and_then(|total| total.checked_add(tool.result_schema.canonical_bytes()))
            .ok_or(RegistryError)
    })?;

    Ok(FixedToolRegistry {
        tools,
        artifact_manifest,
        operation_schemas,
        fixed_schema_bytes: fixed_schema_bytes
            .checked_add(symbolic_schema_bytes)
            .ok_or(RegistryError)?,
    })
}

fn append_symbolic_tools(
    tools: &mut Vec<FixedToolDefinition>,
    manifest: &mut Vec<String>,
) -> Result<(), RegistryError> {
    let specifications = [
        SymbolicToolSpec {
            kind: 15,
            name: "riffdb_contract_describe",
            title: "Describe symbolic contract",
            description: "Return the selected contract as a bounded, name-only symbolic catalog.",
            method: "DescribeContract",
            operation: "DescribeContract",
            branches: &["described"],
            input: serde_json::json!({
                "$schema": SCHEMA_DIALECT,
                "type": "object",
                "additionalProperties": false,
                "properties": {"contract": {"type": "object"}}
            }),
            result: wrapped_result_schema("described"),
        },
        SymbolicToolSpec {
            kind: 16,
            name: "riffdb_query_check",
            title: "Check RiffQL",
            description: "Parse, resolve, type check, and plan bounded RiffQL without executing it.",
            method: "CheckQuery",
            operation: "CheckQuery",
            branches: &["valid", "invalid"],
            input: symbolic_source_schema(false),
            result: alternative_result_schema("valid", "invalid"),
        },
        SymbolicToolSpec {
            kind: 17,
            name: "riffdb_query_explain",
            title: "Explain RiffQL",
            description: "Return a deterministic, name-only execution plan for bounded RiffQL.",
            method: "ExplainQuery",
            operation: "ExplainQuery",
            branches: &["valid", "invalid"],
            input: symbolic_source_schema(false),
            result: alternative_result_schema("valid", "invalid"),
        },
        SymbolicToolSpec {
            kind: 18,
            name: "riffdb_query",
            title: "Execute RiffQL",
            description: "Execute one authorized bounded RiffQL read against one database snapshot.",
            method: "ExecuteQuery",
            operation: "ExecuteQuery",
            branches: &["completed"],
            input: symbolic_source_schema(true),
            result: wrapped_result_schema("completed"),
        },
    ];

    for specification in specifications {
        if !valid_public_tool_name(specification.name) {
            return Err(RegistryError);
        }
        let input_id = format!("riffdb.fixed-tool/{}/input/v1", specification.name);
        let result_id = format!("riffdb.fixed-tool/{}/result/v1", specification.name);
        let input_schema = generated_schema(input_id.clone(), specification.input)?;
        let result_schema = generated_schema(result_id.clone(), specification.result)?;
        manifest.extend([input_id, result_id]);
        tools.push(FixedToolDefinition {
            kind: specification.kind,
            name: specification.name.to_owned(),
            title: specification.title.to_owned(),
            description: specification.description.to_owned(),
            risk_class: "symbolic_read".to_owned(),
            grpc_service: "ApplicationQueryService".to_owned(),
            grpc_method: specification.method.to_owned(),
            service_operation: specification.operation.to_owned(),
            request_converter_id: format!("riffdb.mcp.symbolic.{}.request/v1", specification.kind),
            result_converter_id: format!("riffdb.mcp.symbolic.{}.result/v1", specification.kind),
            result_branches: specification
                .branches
                .iter()
                .map(|branch| (*branch).to_owned())
                .collect(),
            annotations: FixedToolAnnotations {
                read_only_hint: true,
                destructive_hint: false,
                idempotent_hint: true,
                open_world_hint: false,
            },
            input_schema,
            result_schema,
        });
    }
    let input_id = "riffdb.fixed-tool/riffdb_command_run/input/v1".to_owned();
    let result_id = "riffdb.fixed-tool/riffdb_command_run/result/v1".to_owned();
    let input_schema = generated_schema(
        input_id.clone(),
        serde_json::json!({
            "$schema": SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "command_name": {"type": "string", "minLength": 1, "maxLength": 128},
                "input": {"type": "object"},
                "expected_contract_version": {"type": "string", "minLength": 1, "maxLength": 20}
            },
            "required": ["command_name", "input"]
        }),
    )?;
    let result_schema = generated_schema(result_id.clone(), wrapped_result_schema("completed"))?;
    manifest.extend([input_id, result_id]);
    tools.push(FixedToolDefinition {
        kind: 19,
        name: "riffdb_command_run".to_owned(),
        title: "Run symbolic command".to_owned(),
        description:
            "Invoke one compiled command with name-addressed natural JSON and declared outcomes."
                .to_owned(),
        risk_class: "application_mutation".to_owned(),
        grpc_service: "CommandService".to_owned(),
        grpc_method: "Execute".to_owned(),
        service_operation: "ExecuteCommand".to_owned(),
        request_converter_id: "riffdb.mcp.symbolic.command.request/v1".to_owned(),
        result_converter_id: "riffdb.mcp.symbolic.command.result/v1".to_owned(),
        result_branches: vec!["completed".to_owned()],
        annotations: FixedToolAnnotations {
            read_only_hint: false,
            destructive_hint: true,
            idempotent_hint: true,
            open_world_hint: false,
        },
        input_schema,
        result_schema,
    });
    append_reactive_tools(tools, manifest)?;
    append_contextual_tools(tools, manifest)?;
    append_application_catalog_tool(tools, manifest)?;
    Ok(())
}

fn append_application_catalog_tool(
    tools: &mut Vec<FixedToolDefinition>,
    manifest: &mut Vec<String>,
) -> Result<(), RegistryError> {
    let name = "riffdb_application_catalog";
    let input_id = format!("riffdb.fixed-tool/{name}/input/v1");
    let result_id = format!("riffdb.fixed-tool/{name}/result/v1");
    let input_schema = generated_schema(
        input_id.clone(),
        serde_json::json!({
            "$schema": SCHEMA_DIALECT,
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "contract": {"type": "object"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 100},
                "cursor": {"type": "string", "pattern": "^[0-9a-f]{32}$"}
            }
        }),
    )?;
    let result_schema = generated_schema(result_id.clone(), wrapped_result_schema("page"))?;
    manifest.extend([input_id, result_id]);
    tools.push(FixedToolDefinition {
        kind: 31,
        name: name.to_owned(),
        title: "Inspect authorized application symbols".to_owned(),
        description: "Return one bounded policy-filtered page of symbolic application operations and features without numeric or storage identities.".to_owned(),
        risk_class: "symbolic_read".to_owned(),
        grpc_service: "ApplicationQueryService".to_owned(),
        grpc_method: "GetApplicationCatalog".to_owned(),
        service_operation: "DescribeContract".to_owned(),
        request_converter_id: "riffdb.mcp.symbolic.31.request/v1".to_owned(),
        result_converter_id: "riffdb.mcp.symbolic.31.result/v1".to_owned(),
        result_branches: vec!["page".to_owned()],
        annotations: FixedToolAnnotations {
            read_only_hint: true,
            destructive_hint: false,
            idempotent_hint: true,
            open_world_hint: false,
        },
        input_schema,
        result_schema,
    });
    Ok(())
}

fn append_contextual_tools(
    tools: &mut Vec<FixedToolDefinition>,
    manifest: &mut Vec<String>,
) -> Result<(), RegistryError> {
    let identity = serde_json::json!({
        "module_hash": {"type":"string", "pattern":"^[0-9a-f]{64}$"},
        "operation_name": {"type":"string", "pattern":"^[A-Za-z_][A-Za-z0-9_]{0,255}$"},
        "parameters": {"type":"object"},
        "consumer_name": {"type":"string", "minLength":1, "maxLength":64}
    });
    let specs = [
        (
            26,
            "riffdb_contextual_next",
            "Lease contextual work",
            "ConsumeContextualSubscription",
            "ConsumeContextualSubscription",
            true,
            false,
        ),
        (
            27,
            "riffdb_contextual_ack",
            "Acknowledge contextual work",
            "AcknowledgeContextualSubscription",
            "AcknowledgeContextualSubscription",
            true,
            false,
        ),
        (
            28,
            "riffdb_contextual_nack",
            "Negative acknowledge contextual work",
            "NegativeAcknowledgeContextualSubscription",
            "NegativeAcknowledgeContextualSubscription",
            true,
            false,
        ),
        (
            29,
            "riffdb_contextual_status",
            "Read contextual consumer status",
            "GetContextualSubscriptionStatus",
            "GetContextualSubscriptionStatus",
            true,
            true,
        ),
        (
            30,
            "riffdb_contextual_react",
            "Execute contextual reaction",
            "ExecuteContextualReaction",
            "ExecuteContextualReaction",
            false,
            false,
        ),
    ];
    for (kind, name, title, method, operation, idempotent, read_only) in specs {
        let mut properties = identity.as_object().cloned().ok_or(RegistryError)?;
        let mut required = vec![
            "module_hash",
            "operation_name",
            "parameters",
            "consumer_name",
        ];
        if kind == 26 {
            properties.insert("maximum_wait_nanos".to_owned(), serde_json::json!({"type":"integer", "minimum":0, "maximum":30_000_000_000_u64, "default":0}));
        }
        if matches!(kind, 27 | 28) {
            properties.insert(
                "event_id".to_owned(),
                serde_json::json!({"type":"string", "minLength":3, "maxLength":32}),
            );
            properties.insert(
                "lease_token".to_owned(),
                serde_json::json!({"type":"string", "pattern":"^[0-9a-f]{64}$"}),
            );
            properties.insert(
                "history_incarnation".to_owned(),
                serde_json::json!({"type":"string", "pattern":"^[1-9][0-9]*$"}),
            );
            required.extend(["event_id", "lease_token", "history_incarnation"]);
            if kind == 28 {
                properties.insert("retry_delay_nanos".to_owned(), serde_json::json!({"type":"integer", "minimum":0, "maximum":3_600_000_000_000_u64, "default":0}));
            }
        }
        if kind == 30 {
            properties.insert(
                "reaction_name".to_owned(),
                serde_json::json!({"type":"string", "minLength":1, "maxLength":256}),
            );
            properties.insert("causation_token".to_owned(), serde_json::json!({"type":"string", "contentEncoding":"base64", "minLength":44, "maxLength":1368}));
            properties.insert(
                "command_name".to_owned(),
                serde_json::json!({"type":"string", "minLength":1, "maxLength":128}),
            );
            properties.insert("input".to_owned(), serde_json::json!({"type":"object"}));
            properties.insert(
                "expected_contract_version".to_owned(),
                serde_json::json!({"type":"string", "pattern":"^[1-9][0-9]*$"}),
            );
            required.extend(["reaction_name", "causation_token", "command_name", "input"]);
        }
        let input_id = format!("riffdb.fixed-tool/{name}/input/v1");
        let result_id = format!("riffdb.fixed-tool/{name}/result/v1");
        let input_schema = generated_schema(
            input_id.clone(),
            serde_json::json!({
                "$schema": SCHEMA_DIALECT, "type":"object", "additionalProperties":false,
                "properties":properties, "required":required,
            }),
        )?;
        let result_schema =
            generated_schema(result_id.clone(), wrapped_result_schema("completed"))?;
        manifest.extend([input_id, result_id]);
        tools.push(FixedToolDefinition {
            kind,
            name: name.to_owned(),
            title: title.to_owned(),
            description: format!("Run the authorized {title} operation."),
            risk_class: if kind == 30 {
                "application_mutation"
            } else {
                "reactive_application"
            }
            .to_owned(),
            grpc_service: "EventService".to_owned(),
            grpc_method: method.to_owned(),
            service_operation: operation.to_owned(),
            request_converter_id: format!("riffdb.mcp.contextual.{kind}.request/v1"),
            result_converter_id: format!("riffdb.mcp.contextual.{kind}.result/v1"),
            result_branches: vec!["completed".to_owned()],
            annotations: FixedToolAnnotations {
                read_only_hint: read_only,
                destructive_hint: kind == 30,
                idempotent_hint: idempotent,
                open_world_hint: false,
            },
            input_schema,
            result_schema,
        });
    }
    Ok(())
}

fn append_reactive_tools(
    tools: &mut Vec<FixedToolDefinition>,
    manifest: &mut Vec<String>,
) -> Result<(), RegistryError> {
    let identity = serde_json::json!({
        "module_hash": {"type":"string", "pattern":"^[0-9a-f]{64}$"},
        "operation_name": {"type":"string", "pattern":"^[A-Za-z_][A-Za-z0-9_]{0,255}$"},
        "parameters": {"type":"object"},
        "consumer_name": {"type":"string", "minLength":1, "maxLength":64}
    });
    let specs = [
        (
            20,
            "riffdb_event_next",
            "Lease next events",
            "Lease one bounded authorized event batch.",
            "ConsumeEventStream",
            "ConsumeEventStream",
            false,
            false,
            false,
        ),
        (
            21,
            "riffdb_event_ack",
            "Acknowledge event",
            "Acknowledge one exact authorized event lease.",
            "AcknowledgeEventStream",
            "AcknowledgeEventStream",
            false,
            false,
            true,
        ),
        (
            22,
            "riffdb_event_nack",
            "Negative acknowledge event",
            "Release or delay one exact authorized event lease.",
            "NegativeAcknowledgeEventStream",
            "NegativeAcknowledgeEventStream",
            false,
            false,
            true,
        ),
        (
            23,
            "riffdb_event_seek",
            "Seek event consumer",
            "Move one exact consumer checkpoint under explicit seek authority.",
            "SeekEventStreamConsumer",
            "SeekEventStreamConsumer",
            false,
            true,
            true,
        ),
        (
            24,
            "riffdb_event_status",
            "Read event consumer status",
            "Read bounded status for one exact authorized event consumer.",
            "GetEventStreamConsumerStatus",
            "GetEventStreamConsumerStatus",
            true,
            false,
            true,
        ),
        (
            25,
            "riffdb_query_watch",
            "Watch named query",
            "Retrieve one authorized closed live-query update; reconnect with its opaque cursor.",
            "WatchNamedQuery",
            "WatchNamedQuery",
            true,
            false,
            true,
        ),
    ];
    for (kind, name, title, description, method, operation, read_only, destructive, idempotent) in
        specs
    {
        let mut properties = identity.as_object().cloned().ok_or(RegistryError)?;
        let required = if kind == 25 {
            properties.remove("consumer_name");
            properties.insert(
                "cursor".to_owned(),
                serde_json::json!({"type":"string", "minLength":2, "maxLength":5464}),
            );
            vec!["module_hash", "operation_name", "parameters"]
        } else if kind == 20 {
            properties.insert(
                "batch_limit".to_owned(),
                serde_json::json!({"type":"integer", "minimum":1, "maximum":64, "default":1}),
            );
            properties.insert(
                "in_flight_limit".to_owned(),
                serde_json::json!({"type":"integer", "minimum":1, "maximum":64, "default":16}),
            );
            properties.insert(
                "lease_seconds".to_owned(),
                serde_json::json!({"type":"integer", "minimum":5, "maximum":900, "default":60}),
            );
            properties.insert("maximum_wait_nanos".to_owned(), serde_json::json!({"type":"integer", "minimum":0, "maximum":30_000_000_000_u64, "default":0}));
            vec![
                "module_hash",
                "operation_name",
                "parameters",
                "consumer_name",
            ]
        } else {
            if matches!(kind, 21 | 22) {
                properties.insert(
                    "event_id".to_owned(),
                    serde_json::json!({"type":"string", "minLength":3, "maxLength":32}),
                );
                properties.insert(
                    "lease_token".to_owned(),
                    serde_json::json!({"type":"string", "pattern":"^[0-9a-f]{64}$"}),
                );
                properties.insert(
                    "history_incarnation".to_owned(),
                    serde_json::json!({"type":"string", "pattern":"^[1-9][0-9]*$"}),
                );
                if kind == 22 {
                    properties.insert(
                        "retry_delay_nanos".to_owned(),
                        serde_json::json!({"type":"integer", "minimum":0, "maximum":3_600_000_000_000_u64}),
                    );
                }
            }
            if kind == 23 {
                properties.insert(
                    "checkpoint".to_owned(),
                    serde_json::json!({"type":"string", "minLength":3, "maxLength":32}),
                );
                properties.insert(
                    "progress_cursor".to_owned(),
                    serde_json::json!({"type":"string", "pattern":"^[0-9a-f]{32}$"}),
                );
            }
            let mut values = vec![
                "module_hash",
                "operation_name",
                "parameters",
                "consumer_name",
            ];
            if matches!(kind, 21 | 22) {
                values.extend(["event_id", "lease_token", "history_incarnation"]);
            }
            values
        };
        let input_id = format!("riffdb.fixed-tool/{name}/input/v1");
        let result_id = format!("riffdb.fixed-tool/{name}/result/v1");
        let mut input_schema_value = serde_json::json!({
            "$schema": SCHEMA_DIALECT,
            "type":"object", "additionalProperties":false,
            "properties": properties,
            "required": required,
        });
        if kind == 23 {
            input_schema_value["anyOf"] = serde_json::json!([
                {
                    "type":"object",
                    "properties":{
                        "checkpoint":{"type":"string", "minLength":3, "maxLength":32}
                    },
                    "required":["checkpoint"]
                },
                {
                    "type":"object",
                    "properties":{
                        "progress_cursor":{"type":"string", "pattern":"^[0-9a-f]{32}$"}
                    },
                    "required":["progress_cursor"]
                }
            ]);
            input_schema_value["not"] = serde_json::json!({
                "type":"object",
                "properties":{
                    "checkpoint":{"type":"string", "minLength":3, "maxLength":32},
                    "progress_cursor":{"type":"string", "pattern":"^[0-9a-f]{32}$"}
                },
                "required":["checkpoint", "progress_cursor"]
            });
        }
        let input_schema = generated_schema(input_id.clone(), input_schema_value)?;
        let result_schema =
            generated_schema(result_id.clone(), wrapped_result_schema("completed"))?;
        manifest.extend([input_id, result_id]);
        tools.push(FixedToolDefinition {
            kind,
            name: name.to_owned(),
            title: title.to_owned(),
            description: description.to_owned(),
            risk_class: if destructive {
                "consumer_control"
            } else {
                "reactive_application"
            }
            .to_owned(),
            grpc_service: if kind == 25 {
                "ApplicationQueryService"
            } else {
                "EventService"
            }
            .to_owned(),
            grpc_method: method.to_owned(),
            service_operation: operation.to_owned(),
            request_converter_id: format!("riffdb.mcp.reactive.{kind}.request/v1"),
            result_converter_id: format!("riffdb.mcp.reactive.{kind}.result/v1"),
            result_branches: vec!["completed".to_owned()],
            annotations: FixedToolAnnotations {
                read_only_hint: read_only,
                destructive_hint: destructive,
                idempotent_hint: idempotent,
                open_world_hint: false,
            },
            input_schema,
            result_schema,
        });
    }
    Ok(())
}

fn valid_public_tool_name(name: &str) -> bool {
    name.len() <= 128
        && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

struct SymbolicToolSpec {
    kind: u8,
    name: &'static str,
    title: &'static str,
    description: &'static str,
    method: &'static str,
    operation: &'static str,
    branches: &'static [&'static str],
    input: Value,
    result: Value,
}

fn symbolic_source_schema(execute: bool) -> Value {
    let mut properties = serde_json::Map::from_iter([
        ("contract".to_owned(), serde_json::json!({"type": "object"})),
        (
            "source".to_owned(),
            serde_json::json!({"type": "string", "minLength": 1, "maxLength": 262144}),
        ),
    ]);
    let mut required = vec![Value::String("source".to_owned())];
    if execute {
        properties.insert(
            "parameters".to_owned(),
            serde_json::json!({"type": "object"}),
        );
        properties.insert(
            "cursor".to_owned(),
            serde_json::json!({"type": "string", "minLength": 1, "maxLength": 64}),
        );
        required.push(Value::String("parameters".to_owned()));
    }
    serde_json::json!({
        "$schema": SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": required
    })
}

fn wrapped_result_schema(branch: &str) -> Value {
    serde_json::json!({
        "$schema": SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": false,
        "properties": {(branch): {"type": "object"}},
        "required": [branch]
    })
}

fn alternative_result_schema(first: &str, second: &str) -> Value {
    serde_json::json!({
        "$schema": SCHEMA_DIALECT,
        "type": "object",
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "properties": {(first): {"type": "object"}},
                "required": [first]
            },
            {
                "type": "object",
                "additionalProperties": false,
                "properties": {(second): {"type": "object"}},
                "required": [second]
            }
        ]
    })
}

fn generated_schema(schema_id: String, value: Value) -> Result<SchemaDocument, RegistryError> {
    let canonical_json = serde_json::to_string(&value).map_err(|_| RegistryError)?;
    let schema_hash = hash_schema(canonical_json.as_bytes());
    SchemaDocument::from_canonical(schema_id, schema_hash, canonical_json)
}

fn schema_document(wire: &SchemaWire) -> Result<SchemaDocument, RegistryError> {
    let canonical_json = schema_source(&wire.source_path).ok_or(RegistryError)?;
    if canonical_json.len() != wire.canonical_bytes || canonical_json.len() > MAX_SCHEMA_BYTES {
        return Err(RegistryError);
    }
    require_canonical_json(canonical_json)?;
    let value: Value = serde_json::from_str(canonical_json).map_err(|_| RegistryError)?;
    let object = value.as_object().ok_or(RegistryError)?.clone();
    if object.get("$schema").and_then(Value::as_str) != Some(SCHEMA_DIALECT)
        || !root_is_closed(&object)
        || crate::schema::validate_schema_source(&value).is_err()
    {
        return Err(RegistryError);
    }
    let expected_hash = decode_schema_hash(&wire.schema_hash)?;
    let actual_hash = hash_schema(canonical_json.as_bytes());
    if actual_hash != expected_hash {
        return Err(RegistryError);
    }
    Ok(SchemaDocument {
        schema_id: wire.schema_id.clone(),
        schema_hash: actual_hash,
        canonical_json: Arc::from(canonical_json),
        object,
        source_path: Some(wire.source_path.clone()),
    })
}

fn root_is_closed(object: &Map<String, Value>) -> bool {
    if let Some(branches) = object.get("oneOf").and_then(Value::as_array) {
        !branches.is_empty()
            && branches.iter().all(|branch| {
                branch.get("type").and_then(Value::as_str) == Some("object")
                    && branch.get("additionalProperties").and_then(Value::as_bool) == Some(false)
            })
    } else {
        object.get("type").and_then(Value::as_str) == Some("object")
            && object.get("additionalProperties").and_then(Value::as_bool) == Some(false)
    }
}

fn load_resource_registry() -> Result<ResourceRegistry, RegistryError> {
    require_canonical_json(RESOURCE_REGISTRY_SOURCE)?;
    let wire: ResourceRegistryWire =
        serde_json::from_str(RESOURCE_REGISTRY_SOURCE).map_err(|_| RegistryError)?;
    if wire.schema != RESOURCE_REGISTRY_ID
        || wire.compatibility_checkpoint != RESOURCE_ACCEPTED_CHECKPOINT
        || wire.counts
            != (ResourceCountsWire {
                concrete: 10,
                content_goldens: 13,
                entries: 13,
                subscribable: 5,
                templates: 3,
            })
        || wire.entries.len() != 13
    {
        return Err(RegistryError);
    }

    let expected_branches = [
        "active_contract",
        "contract_version",
        "entity_schema",
        "command_plan",
        "command_documentation",
        "command_outcome",
        "commit.class_template",
        "commit.commit_sequence",
        "provenance.class_template",
        "provenance.provenance_id",
        "projection_status",
        "server_health",
        "reactive_wakeup",
    ];
    let expected_subscribable = [
        "active_contract",
        "command_plan",
        "projection_status",
        "server_health",
        "reactive_wakeup",
    ];
    let mut entries = Vec::with_capacity(wire.entries.len());
    for (entry, expected_branch) in wire.entries.into_iter().zip(expected_branches) {
        if entry.descriptor_branch != expected_branch
            || entry.annotations_present
            || entry.name_rule != "exact-canonical-uri-or-template"
            || entry.content_golden.uri != entry.golden_uri
            || entry.content_golden.mime_type != entry.mime_type
            || parse_resource_locator(&entry.golden_uri).is_err()
        {
            return Err(RegistryError);
        }
        let surface = match (entry.surface.as_str(), entry.uri_kind.as_str()) {
            ("resources/list", "concrete") => ResourceSurface::Concrete,
            ("resources/templates/list", "template") => ResourceSurface::Template,
            _ => return Err(RegistryError),
        };
        entries.push(ResourceDefinition {
            descriptor_branch: entry.descriptor_branch,
            surface,
            uri: entry.uri,
            mime_type: entry.mime_type,
            golden_name: entry.golden_name,
            golden_uri: entry.golden_uri,
            title: entry.title,
            description: entry.description,
            subscribable: entry.subscribable,
            content_converter_id: entry.content_converter_id,
            content_golden: entry.content_golden,
        });
    }

    let subscribable: Vec<_> = entries
        .iter()
        .filter(|entry| entry.subscribable)
        .map(|entry| entry.descriptor_branch.as_str())
        .collect();
    if subscribable != expected_subscribable {
        return Err(RegistryError);
    }
    Ok(ResourceRegistry { entries })
}

fn require_canonical_json(source: &str) -> Result<(), RegistryError> {
    let value: Value = serde_json::from_str(source).map_err(|_| RegistryError)?;
    let encoded = serde_json::to_string(&value).map_err(|_| RegistryError)?;
    if encoded != source {
        return Err(RegistryError);
    }
    Ok(())
}

fn decode_schema_hash(text: &str) -> Result<SchemaHash, RegistryError> {
    if text.len() != 64 {
        return Err(RegistryError);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let high = lowercase_hex(pair[0]).ok_or(RegistryError)?;
        let low = lowercase_hex(pair[1]).ok_or(RegistryError)?;
        bytes[index] = (high << 4) | low;
    }
    Ok(SchemaHash::from_bytes(bytes))
}

const fn lowercase_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn schema_source(path: &str) -> Option<&'static str> {
    match path {
        "crates/riffdb-service/schema/riffdb.command-operation-envelope-v1.schema.json" => {
            Some(include_str!(
                "../../riffdb-service/schema/riffdb.command-operation-envelope-v1.schema.json"
            ))
        }
        "crates/riffdb-service/schema/riffdb.command-get-outcome-result-v1.schema.json" => {
            Some(include_str!(
                "../../riffdb-service/schema/riffdb.command-get-outcome-result-v1.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_command_get_outcome.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_command_get_outcome.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_commit_get.input.schema.json" => Some(
            include_str!("../schema/fixed-tool/v1/riffdb_commit_get.input.schema.json"),
        ),
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_commit_get.result.schema.json" => Some(
            include_str!("../schema/fixed-tool/v1/riffdb_commit_get.result.schema.json"),
        ),
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_commit_scan.input.schema.json" => Some(
            include_str!("../schema/fixed-tool/v1/riffdb_commit_scan.input.schema.json"),
        ),
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_commit_scan.result.schema.json" => Some(
            include_str!("../schema/fixed-tool/v1/riffdb_commit_scan.result.schema.json"),
        ),
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_contract_deploy.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_contract_deploy.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_contract_deploy.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_contract_deploy.result.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_contract_explain_command.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_contract_explain_command.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_contract_explain_command.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_contract_explain_command.result.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_contract_get_active.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_contract_get_active.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_contract_get_active.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_contract_get_active.result.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_contract_validate.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_contract_validate.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_contract_validate.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_contract_validate.result.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_entity_get.input.schema.json" => Some(
            include_str!("../schema/fixed-tool/v1/riffdb_entity_get.input.schema.json"),
        ),
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_entity_get.result.schema.json" => Some(
            include_str!("../schema/fixed-tool/v1/riffdb_entity_get.result.schema.json"),
        ),
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_entity_scan_index.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_entity_scan_index.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_entity_scan_index.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_entity_scan_index.result.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_outbox_list_pending.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_outbox_list_pending.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_outbox_list_pending.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_outbox_list_pending.result.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_projection_query.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_projection_query.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_projection_query.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_projection_query.result.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_projection_status.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_projection_status.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_projection_status.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_projection_status.result.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_provenance_trace.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_provenance_trace.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_provenance_trace.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_provenance_trace.result.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_server_health.input.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_server_health.input.schema.json"
            ))
        }
        "crates/riffdb-api-mcp/schema/fixed-tool/v1/riffdb_server_health.result.schema.json" => {
            Some(include_str!(
                "../schema/fixed-tool/v1/riffdb_server_health.result.schema.json"
            ))
        }
        _ => None,
    }
}

/// An embedded registry or schema source failed a compatibility check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistryError;

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("embedded MCP registry is invalid")
    }
}

impl Error for RegistryError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixedRegistryWire {
    schema: String,
    compatibility_checkpoint: String,
    conversion_golden_path: String,
    counts: FixedCountsWire,
    schema_hashing: SchemaHashingWire,
    fixed_schema_ledger: FixedSchemaLedgerWire,
    validation_boundary: Value,
    artifact_manifest: Vec<String>,
    operation_schemas: Vec<SchemaWire>,
    tools: Vec<FixedToolWire>,
}

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct FixedCountsWire {
    fixed_tools: usize,
    new_fixed_schema_sources: usize,
    top_level_result_branches: usize,
    unique_operation_and_fixed_artifacts: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaHashingWire {
    domain: String,
    framing: String,
    scheme: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixedSchemaLedgerWire {
    aggregate_response_ceiling_bytes: usize,
    boundary_witnesses: Value,
    canonical_source_bytes: usize,
    component_ceiling_bytes: usize,
    full_page_rule: String,
    remaining_adapter_ceiling_bytes: usize,
    service_discovery_ceiling_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaWire {
    canonical_bytes: usize,
    owner: Option<String>,
    schema_hash: String,
    schema_id: String,
    source_path: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixedToolWire {
    annotations: FixedToolAnnotations,
    description: String,
    fixed_tool_kind: u8,
    grpc_method: String,
    grpc_service: String,
    input_schema: SchemaWire,
    name: String,
    request_converter_id: String,
    result_branches: Vec<String>,
    result_converter_id: String,
    result_schema: SchemaWire,
    risk_class: String,
    service_operation: String,
    title: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixedToolAnnotations {
    #[serde(rename = "destructiveHint")]
    destructive_hint: bool,
    #[serde(rename = "idempotentHint")]
    idempotent_hint: bool,
    #[serde(rename = "openWorldHint")]
    open_world_hint: bool,
    #[serde(rename = "readOnlyHint")]
    read_only_hint: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceRegistryWire {
    schema: String,
    compatibility_checkpoint: String,
    counts: ResourceCountsWire,
    entries: Vec<ResourceEntryWire>,
}

#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ResourceCountsWire {
    concrete: usize,
    content_goldens: usize,
    entries: usize,
    subscribable: usize,
    templates: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceEntryWire {
    annotations_present: bool,
    content_converter_id: String,
    content_golden: ResourceContentGolden,
    description: String,
    descriptor_branch: String,
    golden_name: String,
    golden_uri: String,
    mime_type: String,
    name_rule: String,
    subscribable: bool,
    surface: String,
    title: String,
    uri: String,
    uri_kind: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ResourceContentGolden {
    mime_type: String,
    text: String,
    uri: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_registry_reproduces_every_accepted_schema_identity() {
        let registry = fixed_tool_registry().expect("accepted fixed registry loads");
        assert_eq!(registry.tools().len(), 31);
        assert_eq!(registry.artifact_manifest().len(), 63);
        assert_eq!(registry.operation_schemas().len(), 2);
        assert!(registry.fixed_schema_bytes() > EXPECTED_FIXED_SCHEMA_BYTES);
        assert!(registry.fixed_schema_bytes() <= MAX_FIXED_SCHEMA_BYTES);

        for (index, tool) in registry.tools().iter().enumerate() {
            assert_eq!(usize::from(tool.kind()), index + 1);
            assert!(tool.input_schema().canonical_bytes() <= MAX_SCHEMA_BYTES);
            assert!(tool.result_schema().canonical_bytes() <= MAX_SCHEMA_BYTES);
            let serialized =
                serde_json::to_value(tool.to_mcp_tool()).expect("tool serializes through rmcp");
            assert_eq!(serialized["name"], tool.name());
            assert_eq!(serialized["title"], tool.title());
            assert_eq!(
                serialized["inputSchema"]["$schema"],
                Value::String(SCHEMA_DIALECT.to_owned())
            );
            assert_eq!(
                serialized["inputSchema"]["type"],
                Value::String("object".to_owned())
            );
            assert_eq!(
                serialized["outputSchema"]["type"],
                Value::String("object".to_owned())
            );
            assert!(serialized.get("execution").is_none());
            assert!(serialized.get("icons").is_none());
            assert!(serialized.get("_meta").is_none());
        }

        for schema in registry.operation_schemas() {
            assert_eq!(
                schema.json_object().get("type").and_then(Value::as_str),
                Some("object")
            );
        }
    }

    #[test]
    fn public_schema_parts_require_exact_hash_bytes_and_content_identity() {
        let source = fixed_tool_registry()
            .expect("registry")
            .tools()
            .first()
            .expect("fixed tool")
            .input_schema();
        let decoded = SchemaDocument::from_public_parts(
            source.schema_id(),
            &source.schema_hash_bytes(),
            source.canonical_json(),
        )
        .expect("public parts");
        assert_eq!(decoded.schema_id(), source.schema_id());
        assert_eq!(decoded.schema_hash_bytes(), source.schema_hash_bytes());
        assert!(
            SchemaDocument::from_public_parts(
                source.schema_id(),
                &[0_u8; 31],
                source.canonical_json(),
            )
            .is_err()
        );
    }

    #[test]
    fn fixed_lookup_consumes_exact_names_only() {
        let registry = fixed_tool_registry().expect("accepted fixed registry loads");
        assert!(registry.by_name("riffdb_contract_deploy").is_some());
        assert!(registry.by_name("RIFFDB.CONTRACT.DEPLOY").is_none());
        assert!(registry.by_name("riffdb_contract_deploy ").is_none());
        assert!(registry.by_name("riffdb.contract.unknown").is_none());
    }

    #[test]
    fn reactive_fixed_tools_are_underscore_named_and_risk_classified() {
        let registry = fixed_tool_registry().expect("accepted fixed registry loads");
        let expected = [
            ("riffdb_event_next", false, false),
            ("riffdb_event_ack", false, false),
            ("riffdb_event_nack", false, false),
            ("riffdb_event_seek", false, true),
            ("riffdb_event_status", true, false),
            ("riffdb_query_watch", true, false),
            ("riffdb_contextual_next", false, false),
            ("riffdb_contextual_ack", false, false),
            ("riffdb_contextual_nack", false, false),
            ("riffdb_contextual_status", true, false),
            ("riffdb_contextual_react", false, true),
        ];
        for (name, read_only, destructive) in expected {
            let tool = registry.by_name(name).expect("reactive fixed tool");
            assert_eq!(tool.annotations.read_only_hint, read_only);
            assert_eq!(tool.annotations.destructive_hint, destructive);
            assert!(!tool.name().contains('.'));
            assert!(tool.input_schema().canonical_bytes() <= MAX_SCHEMA_BYTES);
            assert!(tool.result_schema().canonical_bytes() <= MAX_SCHEMA_BYTES);
            if matches!(name, "riffdb_event_next" | "riffdb_contextual_next") {
                assert_eq!(
                    tool.input_schema().json_object()["properties"]["maximum_wait_nanos"]["default"],
                    0
                );
            }
        }
    }

    #[test]
    fn resource_registry_has_one_surface_and_exact_subscription_set() {
        let registry = resource_registry().expect("accepted resource registry loads");
        assert_eq!(registry.entries().len(), 13);
        assert_eq!(
            registry
                .entries()
                .iter()
                .filter(|entry| entry.surface() == ResourceSurface::Concrete)
                .count(),
            10
        );
        assert_eq!(
            registry
                .entries()
                .iter()
                .filter(|entry| entry.surface() == ResourceSurface::Template)
                .count(),
            3
        );
        assert_eq!(
            registry
                .entries()
                .iter()
                .filter(|entry| entry.subscribable())
                .map(ResourceDefinition::descriptor_branch)
                .collect::<Vec<_>>(),
            [
                "active_contract",
                "command_plan",
                "projection_status",
                "server_health",
                "reactive_wakeup"
            ]
        );
    }

    #[test]
    fn resource_sdk_objects_have_no_unaccepted_optional_metadata() {
        let registry = resource_registry().expect("accepted resource registry loads");
        for entry in registry.entries() {
            let value = match entry.surface() {
                ResourceSurface::Concrete => serde_json::to_value(
                    entry
                        .to_mcp_resource(entry.golden_uri())
                        .expect("golden concrete locator is valid"),
                ),
                ResourceSurface::Template => serde_json::to_value(
                    entry
                        .to_mcp_template()
                        .expect("template entry materializes"),
                ),
            }
            .expect("resource descriptor serializes");
            assert!(value.get("annotations").is_none());
            assert!(value.get("icons").is_none());
            assert!(value.get("_meta").is_none());
        }
    }
}
