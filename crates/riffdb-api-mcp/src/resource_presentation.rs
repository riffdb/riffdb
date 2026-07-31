//! Shared ADR-0046 resource and generated-documentation presentation.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::{Map, Number, Value};

use crate::bounded_json;
use crate::schema::RiffDbSchemaValidator;
use crate::{
    MCP_OUTBOUND_MESSAGE_MAX_BYTES, McpMarkdownBuilder, McpMarkdownDocument, McpPresentationError,
    McpPresentedHash, McpPresentedU64, McpResourceJson, SchemaDocument,
    format_command_plan_locator_from_public, format_contract_version_locator_from_public,
    format_projection_status_locator_from_public, validate_command_tool_name,
};

const MAX_COMPATIBILITY_CODES: usize = 20;
const MAX_COMPATIBILITY_FINDINGS: u32 = 4_096;
const COMPATIBILITY_CODES: [&str; MAX_COMPATIBILITY_CODES] = [
    "RDB-K001", "RDB-K010", "RDB-K011", "RDB-K012", "RDB-K013", "RDB-K020", "RDB-K021", "RDB-K100",
    "RDB-K101", "RDB-K102", "RDB-K103", "RDB-K104", "RDB-K105", "RDB-K106", "RDB-K107", "RDB-K108",
    "RDB-K109", "RDB-K110", "RDB-K111", "RDB-K112",
];
const MAX_EXPLANATION_ITEMS: usize = 4_096;
const MAX_EXPLANATION_TEXT_BYTES: usize = 1_048_576;
const MAX_EXAMPLE_DEPTH: usize = 32;
const MAX_EXAMPLE_NODES: usize = 262_144;
const MAX_EXAMPLE_ARRAY_ITEMS: usize = 4_096;
const UUID_PATTERN: &str = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";

/// Closed compatibility class presented by contract resources.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpContractCompatibilityClass {
    /// Safe under the active additive compatibility policy.
    Compatible,
    /// Callers must select the new version explicitly.
    RequiresExplicitVersion,
    /// The version is not activatable under the POC policy.
    Incompatible,
}

/// One nonzero compiler compatibility-code count.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct McpCompatibilityCodeCount {
    code: String,
    count: NonZeroU32,
}

impl McpCompatibilityCodeCount {
    /// Checks one bounded exact compiler code and nonzero count.
    pub fn new(code: impl Into<String>, count: u32) -> Result<Self, McpPresentationError> {
        let code = code.into();
        let count = NonZeroU32::new(count).ok_or(McpPresentationError)?;
        if !COMPATIBILITY_CODES.contains(&code.as_str()) || count.get() > MAX_COMPATIBILITY_FINDINGS
        {
            return Err(McpPresentationError);
        }
        Ok(Self { code, count })
    }

    /// Borrows the exact stable compatibility code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the nonzero finding count.
    #[must_use]
    pub const fn count(&self) -> NonZeroU32 {
        self.count
    }
}

/// Exact parent bundle identity in a compatibility summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct McpContractParentPresentation {
    bundle_hash: McpPresentedHash,
    contract_version: McpPresentedU64,
}

/// Bounded compatibility summary for one immutable contract bundle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct McpContractCompatibilityPresentation {
    code_counts: Vec<McpCompatibilityCodeCount>,
    overall: McpContractCompatibilityClass,
    parent: Option<McpContractParentPresentation>,
}

impl McpContractCompatibilityPresentation {
    /// Creates the exact genesis summary.
    #[must_use]
    pub const fn genesis() -> Self {
        Self {
            code_counts: Vec::new(),
            overall: McpContractCompatibilityClass::Compatible,
            parent: None,
        }
    }

    /// Checks one successor summary and its canonical ordered code counts.
    pub fn successor(
        parent_contract_version: u64,
        parent_bundle_hash: [u8; 32],
        overall: McpContractCompatibilityClass,
        code_counts: Vec<McpCompatibilityCodeCount>,
    ) -> Result<Self, McpPresentationError> {
        if parent_contract_version == 0 {
            return Err(McpPresentationError);
        }
        validate_code_counts(&code_counts, true)?;
        let expected_overall = code_counts
            .iter()
            .filter_map(|count| {
                COMPATIBILITY_CODES
                    .iter()
                    .position(|code| *code == count.code())
                    .map(compatibility_class)
            })
            .max()
            .ok_or(McpPresentationError)?;
        if overall != expected_overall {
            return Err(McpPresentationError);
        }
        Ok(Self {
            code_counts,
            overall,
            parent: Some(McpContractParentPresentation {
                bundle_hash: presented_hash(parent_bundle_hash)?,
                contract_version: McpPresentedU64::new(parent_contract_version),
            }),
        })
    }

    /// Returns whether this is a genesis summary.
    #[must_use]
    pub const fn is_genesis(&self) -> bool {
        self.parent.is_none()
    }
}

/// Complete common presentation of one immutable contract descriptor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct McpContractDescriptorPresentation {
    bundle_hash: McpPresentedHash,
    compatibility: McpContractCompatibilityPresentation,
    contract_lineage: String,
    contract_version: McpPresentedU64,
    plan_root_hash: McpPresentedHash,
    source_hash: McpPresentedHash,
}

impl McpContractDescriptorPresentation {
    /// Checks a complete descriptor and its compatibility-parent relationship.
    pub fn new(
        contract_lineage: impl Into<String>,
        contract_version: u64,
        bundle_hash: [u8; 32],
        source_hash: [u8; 32],
        plan_root_hash: [u8; 32],
        compatibility: McpContractCompatibilityPresentation,
    ) -> Result<Self, McpPresentationError> {
        let contract_lineage = contract_lineage.into();
        format_contract_version_locator_from_public(&contract_lineage, contract_version)
            .map_err(|_| McpPresentationError)?;
        if compatibility
            .parent
            .as_ref()
            .is_some_and(|parent| parent.contract_version.get() >= contract_version)
        {
            return Err(McpPresentationError);
        }
        Ok(Self {
            bundle_hash: presented_hash(bundle_hash)?,
            compatibility,
            contract_lineage,
            contract_version: McpPresentedU64::new(contract_version),
            plan_root_hash: presented_hash(plan_root_hash)?,
            source_hash: presented_hash(source_hash)?,
        })
    }

    /// Borrows the exact checked lineage.
    #[must_use]
    pub fn contract_lineage(&self) -> &str {
        &self.contract_lineage
    }

    /// Returns the exact nonzero version.
    #[must_use]
    pub const fn contract_version(&self) -> u64 {
        self.contract_version.get()
    }

    /// Borrows the bounded compatibility summary.
    #[must_use]
    pub const fn compatibility(&self) -> &McpContractCompatibilityPresentation {
        &self.compatibility
    }
}

/// Renders active contract metadata with its exact immutable-version link.
pub fn render_active_contract_resource(
    descriptor: &McpContractDescriptorPresentation,
) -> Result<McpResourceJson, McpPresentationError> {
    let mut object = descriptor_value(descriptor)?;
    let version_uri = format_contract_version_locator_from_public(
        descriptor.contract_lineage(),
        descriptor.contract_version(),
    )
    .map_err(|_| McpPresentationError)?;
    object.insert(
        "links".to_owned(),
        serde_json::json!({"contract_version": version_uri}),
    );
    McpResourceJson::from_serializable(&object).map_err(|_| McpPresentationError)
}

/// Renders one immutable contract descriptor including compatibility metadata.
pub fn render_contract_version_resource(
    descriptor: &McpContractDescriptorPresentation,
) -> Result<McpResourceJson, McpPresentationError> {
    McpResourceJson::from_serializable(descriptor).map_err(|_| McpPresentationError)
}

/// Closed command execution class used by common plan and documentation output.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpCommandExecutionClass {
    /// Unjournaled read-only execution.
    ReadOnly,
    /// Admitted idempotent mutation.
    IdempotentMutation,
}

/// One stable binding/field reference in a public command explanation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct McpBindingFieldReferencePresentation {
    binding_id: u32,
    field_id: u32,
}

impl McpBindingFieldReferencePresentation {
    /// Checks the zero-based binding ID and nonzero stable field ID.
    pub fn new(binding_id: u32, field_id: u32) -> Result<Self, McpPresentationError> {
        if field_id == 0 {
            return Err(McpPresentationError);
        }
        Ok(Self {
            binding_id,
            field_id,
        })
    }
}

/// Adapter inputs for one public command explanation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpCommandExplanationFields {
    /// Stable command ID.
    pub command_id: u32,
    /// Closed execution classification.
    pub execution_class: McpCommandExecutionClass,
    /// Partition tuple arity.
    pub partition_component_count: u32,
    /// Upfront conflict-key count.
    pub conflict_key_count: u32,
    /// Dense stable binding IDs.
    pub binding_ids: Vec<u32>,
    /// Canonically ordered influential fields.
    pub read_fields: Vec<McpBindingFieldReferencePresentation>,
    /// Canonically ordered mutated fields.
    pub write_fields: Vec<McpBindingFieldReferencePresentation>,
    /// Stable invariant IDs.
    pub invariant_ids: Vec<u32>,
    /// Emitted event IDs in occurrence order.
    pub event_type_ids: Vec<u32>,
    /// Stable declared outcome IDs.
    pub outcome_ids: Vec<u32>,
    /// Compiler-rendered deterministic structural explanation.
    pub rendered_text: String,
}

/// Checked public command explanation shared by both MCP transports.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct McpCommandExplanationPresentation {
    binding_ids: Vec<u32>,
    command_id: u32,
    conflict_key_count: u32,
    event_type_ids: Vec<u32>,
    execution_class: McpCommandExecutionClass,
    invariant_ids: Vec<u32>,
    outcome_ids: Vec<u32>,
    partition_component_count: u32,
    read_fields: Vec<McpBindingFieldReferencePresentation>,
    rendered_text: String,
    write_fields: Vec<McpBindingFieldReferencePresentation>,
}

impl McpCommandExplanationPresentation {
    /// Checks bounded identities, canonical lists, and rendered explanation.
    pub fn from_fields(fields: McpCommandExplanationFields) -> Result<Self, McpPresentationError> {
        if fields.command_id == 0
            || fields.rendered_text.is_empty()
            || fields.rendered_text.len() > MAX_EXPLANATION_TEXT_BYTES
            || usize::try_from(fields.partition_component_count)
                .map_or(true, |count| count > MAX_EXPLANATION_ITEMS)
            || usize::try_from(fields.conflict_key_count)
                .map_or(true, |count| count > MAX_EXPLANATION_ITEMS)
            || !bounded_binding_ids(&fields.binding_ids)
            || !bounded_nonzero_ids(&fields.invariant_ids, false)
            || !bounded_nonzero_ids(&fields.event_type_ids, false)
            || !bounded_nonzero_ids(&fields.outcome_ids, true)
            || !bounded_sorted_refs(&fields.read_fields)
            || !bounded_sorted_refs(&fields.write_fields)
        {
            return Err(McpPresentationError);
        }
        Ok(Self {
            binding_ids: fields.binding_ids,
            command_id: fields.command_id,
            conflict_key_count: fields.conflict_key_count,
            event_type_ids: fields.event_type_ids,
            execution_class: fields.execution_class,
            invariant_ids: fields.invariant_ids,
            outcome_ids: fields.outcome_ids,
            partition_component_count: fields.partition_component_count,
            read_fields: fields.read_fields,
            rendered_text: fields.rendered_text,
            write_fields: fields.write_fields,
        })
    }

    /// Returns the stable explained command ID.
    #[must_use]
    pub const fn command_id(&self) -> u32 {
        self.command_id
    }

    /// Returns the exact execution class.
    #[must_use]
    pub const fn execution_class(&self) -> McpCommandExecutionClass {
        self.execution_class
    }

    /// Borrows the compiler-rendered explanation.
    #[must_use]
    pub fn rendered_text(&self) -> &str {
        &self.rendered_text
    }
}

/// Descriptor-fenced explained command and its exact compiler schemas.
#[derive(Clone, Debug)]
pub struct McpExplainedCommandPresentation {
    contract: McpContractDescriptorPresentation,
    source_command: String,
    tool_name: String,
    plan_hash: McpPresentedHash,
    explanation: McpCommandExplanationPresentation,
    input_schema: SchemaDocument,
    outcome_schema: SchemaDocument,
}

impl McpExplainedCommandPresentation {
    /// Checks exact command identity and generated-schema ownership.
    pub fn new(
        contract: McpContractDescriptorPresentation,
        source_command: impl Into<String>,
        tool_name: impl Into<String>,
        plan_hash: [u8; 32],
        explanation: McpCommandExplanationPresentation,
        input_schema: SchemaDocument,
        outcome_schema: SchemaDocument,
    ) -> Result<Self, McpPresentationError> {
        let source_command = source_command.into();
        let tool_name = tool_name.into();
        if !is_source_name(&source_command) || validate_command_tool_name(&tool_name).is_err() {
            return Err(McpPresentationError);
        }
        let command_id = explanation.command_id();
        if input_schema.schema_id()
            != format!("riffdb.generated-schema/command-input/{command_id}/v1")
            || outcome_schema.schema_id()
                != format!("riffdb.generated-schema/command-outcome-union/{command_id}/v1")
        {
            return Err(McpPresentationError);
        }
        Ok(Self {
            contract,
            source_command,
            tool_name,
            plan_hash: presented_hash(plan_hash)?,
            explanation,
            input_schema,
            outcome_schema,
        })
    }

    /// Borrows the exact immutable contract descriptor.
    #[must_use]
    pub const fn contract(&self) -> &McpContractDescriptorPresentation {
        &self.contract
    }

    /// Borrows the exact command source name.
    #[must_use]
    pub fn source_command(&self) -> &str {
        &self.source_command
    }

    /// Borrows the exact compiler-owned MCP tool name.
    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    /// Borrows the checked public explanation.
    #[must_use]
    pub const fn explanation(&self) -> &McpCommandExplanationPresentation {
        &self.explanation
    }

    /// Returns the exact checked command plan hash.
    #[must_use]
    pub const fn plan_hash(&self) -> [u8; 32] {
        self.plan_hash.as_bytes()
    }

    /// Borrows the exact compiler-owned input schema.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaDocument {
        &self.input_schema
    }

    /// Borrows the exact compiler-owned outcome union.
    #[must_use]
    pub const fn outcome_schema(&self) -> &SchemaDocument {
        &self.outcome_schema
    }
}

/// Renders exact command identity, explanation, and compiler schema objects.
pub fn render_command_plan_resource(
    command: &McpExplainedCommandPresentation,
) -> Result<McpResourceJson, McpPresentationError> {
    #[derive(Serialize)]
    struct Plan<'a> {
        command_id: u32,
        contract_lineage: &'a str,
        contract_version: McpPresentedU64,
        explanation: &'a McpCommandExplanationPresentation,
        input_schema: &'a SchemaDocument,
        outcome_schema: &'a SchemaDocument,
        plan_hash: McpPresentedHash,
        source_command: &'a str,
    }
    McpResourceJson::from_serializable(&Plan {
        command_id: command.explanation.command_id,
        contract_lineage: command.contract.contract_lineage(),
        contract_version: McpPresentedU64::new(command.contract.contract_version()),
        explanation: &command.explanation,
        input_schema: &command.input_schema,
        outcome_schema: &command.outcome_schema,
        plan_hash: command.plan_hash,
        source_command: &command.source_command,
    })
    .map_err(|_| McpPresentationError)
}

/// Generates deterministic factual command documentation and validated examples.
pub fn render_command_documentation(
    command: &McpExplainedCommandPresentation,
) -> Result<McpMarkdownDocument, McpPresentationError> {
    let input_example = minimal_example(&command.input_schema, None)?;
    RiffDbSchemaValidator
        .validate(&command.input_schema, &input_example)
        .map_err(|_| McpPresentationError)?;

    let outcome_root = command.outcome_schema.json_object();
    let branches = outcome_root
        .get("oneOf")
        .and_then(Value::as_array)
        .ok_or(McpPresentationError)?;
    if branches.is_empty() || branches.len() > MAX_EXPLANATION_ITEMS {
        return Err(McpPresentationError);
    }
    let mut outcome_examples = Vec::with_capacity(branches.len());
    for branch_index in 0..branches.len() {
        let example = minimal_example(&command.outcome_schema, Some(branch_index))?;
        RiffDbSchemaValidator
            .validate(&command.outcome_schema, &example)
            .map_err(|_| McpPresentationError)?;
        outcome_examples.push(example);
    }

    let input_table = input_field_guide(&command.input_schema)?;
    let call = bounded_json::to_string(
        &serde_json::json!({
            "name": command.tool_name,
            "arguments": input_example,
        }),
        MCP_OUTBOUND_MESSAGE_MAX_BYTES,
    )
    .map_err(presentation)?;
    let plan_uri = format_command_plan_locator_from_public(
        command.contract.contract_lineage(),
        command.explanation.command_id,
    )
    .map_err(|_| McpPresentationError)?;

    let mut builder = McpMarkdownBuilder::new();
    builder
        .push_heading(1, &command.source_command)
        .map_err(presentation)?
        .push_paragraph(&format!(
            "Execute the compiled {} command from contract {} version {}.",
            command.source_command,
            command.contract.contract_lineage(),
            command.contract.contract_version()
        ))
        .map_err(presentation)?
        .push_heading(2, "MCP Tool")
        .map_err(presentation)?
        .push_preformatted(&command.tool_name)
        .map_err(presentation)?
        .push_heading(2, "Inputs")
        .map_err(presentation)?
        .push_preformatted(&input_table)
        .map_err(presentation)?
        .push_heading(2, "Call Example")
        .map_err(presentation)?
        .push_preformatted(&call)
        .map_err(presentation)?;
    if command.explanation.execution_class() == McpCommandExecutionClass::IdempotentMutation {
        builder
            .push_heading(2, "Retry and Uncertainty")
            .map_err(presentation)?
            .push_idempotent_mutation_cancellation_notice()
            .map_err(presentation)?;
    }
    builder
        .push_heading(2, "Declared Outcomes")
        .map_err(presentation)?;
    for (index, example) in outcome_examples.iter().enumerate() {
        let outcome_name = declared_outcome_name(example).unwrap_or_else(|| {
            index
                .checked_add(1)
                .map_or_else(|| "Outcome".to_owned(), |value| format!("Outcome {value}"))
        });
        builder
            .push_heading(3, &outcome_name)
            .map_err(presentation)?;
        let example = bounded_json::to_string(example, MCP_OUTBOUND_MESSAGE_MAX_BYTES)
            .map_err(presentation)?;
        builder.push_preformatted(&example).map_err(presentation)?;
    }
    builder
        .push_heading(2, "Plan Resource")
        .map_err(presentation)?
        .push_preformatted(&plan_uri)
        .map_err(presentation)?;
    builder.finish().map_err(presentation)
}

/// Exact projection identity in status presentation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct McpProjectionIdentityPresentation {
    contract_lineage: String,
    projection_id: u32,
    projection_plan_hash: McpPresentedHash,
}

impl McpProjectionIdentityPresentation {
    /// Checks lineage, nonzero projection ID, plan hash, and canonical locator.
    pub fn new(
        contract_lineage: impl Into<String>,
        projection_id: u32,
        projection_plan_hash: [u8; 32],
    ) -> Result<Self, McpPresentationError> {
        let contract_lineage = contract_lineage.into();
        format_projection_status_locator_from_public(&contract_lineage, projection_id)
            .map_err(|_| McpPresentationError)?;
        Ok(Self {
            contract_lineage,
            projection_id,
            projection_plan_hash: presented_hash(projection_plan_hash)?,
        })
    }
}

/// Authoritative or derived projection log frontier.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum McpFrontierPresentation {
    /// No commit has been applied.
    BeforeFirst,
    /// The named nonzero commit is included.
    AppliedThrough(u64),
}

impl McpFrontierPresentation {
    fn ordinal(self) -> Result<u64, McpPresentationError> {
        match self {
            Self::BeforeFirst => Ok(0),
            Self::AppliedThrough(sequence) if sequence != 0 => Ok(sequence),
            Self::AppliedThrough(_) => Err(McpPresentationError),
        }
    }
}

impl Serialize for McpFrontierPresentation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Self::BeforeFirst => {
                map.serialize_entry("before_first", &Map::<String, Value>::new())?
            }
            Self::AppliedThrough(sequence) => {
                map.serialize_entry("applied_through", &sequence.to_string())?
            }
        }
        map.end()
    }
}

/// One retained projection generation and its frontier.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct McpProjectionGenerationFrontierPresentation {
    frontier: McpFrontierPresentation,
    generation: McpPresentedU64,
}

impl McpProjectionGenerationFrontierPresentation {
    /// Checks a nonzero generation and valid frontier.
    pub fn new(
        generation: u64,
        frontier: McpFrontierPresentation,
    ) -> Result<Self, McpPresentationError> {
        if generation == 0 {
            return Err(McpPresentationError);
        }
        frontier.ordinal()?;
        Ok(Self {
            frontier,
            generation: McpPresentedU64::new(generation),
        })
    }
}

/// Closed durable projection lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpProjectionLifecycle {
    /// Initial generation allocation or uninitialized state.
    Building,
    /// Initial candidate is catching up.
    CatchingUp,
    /// One published generation is queryable.
    Ready,
    /// A replacement candidate is building.
    Rebuilding,
    /// A retained generation failed but may be recoverable.
    Degraded,
    /// Recovery is impossible under the v1 policy.
    Invalid,
}

/// Whether a published projection generation may keep applying commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpPublishedApplyMode {
    /// Application remains enabled.
    Enabled,
    /// Application is suspended.
    Suspended,
}

/// Closed public projection failure code.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpProjectionFailureCode {
    /// Checked projection arithmetic overflowed.
    ArithmeticOverflow,
    /// A durable event was malformed for its checked plan.
    MalformedDurableEvent,
    /// An authoritative commit was missing.
    MissingCommit,
    /// A required historical plan or schema is unavailable.
    PlanOrSchemaUnavailable,
    /// Derived state failed integrity checks.
    ProjectionStateIntegrity,
    /// A hard bound was exceeded.
    HardLimitExceeded,
}

/// One safe retained projection-generation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct McpProjectionFailurePresentation {
    #[serde(skip_serializing_if = "Option::is_none")]
    at_sequence: Option<McpPresentedU64>,
    code: McpProjectionFailureCode,
    generation: McpPresentedU64,
}

impl McpProjectionFailurePresentation {
    /// Checks nonzero generation and optional nonzero failing sequence.
    pub fn new(
        generation: u64,
        code: McpProjectionFailureCode,
        at_sequence: Option<u64>,
    ) -> Result<Self, McpPresentationError> {
        if generation == 0 || at_sequence == Some(0) {
            return Err(McpPresentationError);
        }
        Ok(Self {
            at_sequence: at_sequence.map(McpPresentedU64::new),
            code,
            generation: McpPresentedU64::new(generation),
        })
    }
}

/// Adapter inputs for one complete public projection status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpProjectionStatusParts {
    /// Exact projection identity.
    pub identity: McpProjectionIdentityPresentation,
    /// Closed lifecycle.
    pub lifecycle: McpProjectionLifecycle,
    /// Optional published generation.
    pub published: Option<McpProjectionGenerationFrontierPresentation>,
    /// Optional candidate generation.
    pub candidate: Option<McpProjectionGenerationFrontierPresentation>,
    /// Apply mode present exactly when a published generation exists.
    pub published_apply_mode: Option<McpPublishedApplyMode>,
    /// Optional safe failure.
    pub failure: Option<McpProjectionFailurePresentation>,
    /// Exact authoritative commit-log head.
    pub authoritative_head: McpFrontierPresentation,
}

/// Checked projection status with presentation-only lag.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct McpProjectionStatusPresentation {
    authoritative_head: McpFrontierPresentation,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate: Option<McpProjectionGenerationFrontierPresentation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<McpProjectionFailurePresentation>,
    identity: McpProjectionIdentityPresentation,
    lag: Option<McpPresentedU64>,
    lifecycle: McpProjectionLifecycle,
    #[serde(skip_serializing_if = "Option::is_none")]
    published: Option<McpProjectionGenerationFrontierPresentation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    published_apply_mode: Option<McpPublishedApplyMode>,
}

impl McpProjectionStatusPresentation {
    /// Checks cross-field shape and computes lag from the published frontier.
    pub fn from_parts(parts: McpProjectionStatusParts) -> Result<Self, McpPresentationError> {
        let head = parts.authoritative_head.ordinal()?;
        for position in [parts.published, parts.candidate].into_iter().flatten() {
            if position.frontier.ordinal()? > head {
                return Err(McpPresentationError);
            }
        }
        let generations_in_order =
            parts
                .published
                .zip(parts.candidate)
                .is_none_or(|(published, candidate)| {
                    published.generation.get() < candidate.generation.get()
                });
        let failure_matches_position = parts.failure.is_none_or(|failure| {
            [parts.published, parts.candidate]
                .into_iter()
                .flatten()
                .find(|position| position.generation == failure.generation)
                .is_some_and(|position| {
                    failure.at_sequence.is_none_or(|sequence| {
                        position
                            .frontier
                            .ordinal()
                            .ok()
                            .and_then(|frontier| frontier.checked_add(1))
                            == Some(sequence.get())
                    })
                })
        });
        let failed_published_is_suspended = parts.failure.is_none_or(|failure| {
            !parts
                .published
                .is_some_and(|published| published.generation == failure.generation)
                || parts.published_apply_mode == Some(McpPublishedApplyMode::Suspended)
        });
        let lifecycle_shape = match parts.lifecycle {
            McpProjectionLifecycle::Building => {
                parts.published.is_none()
                    && parts.published_apply_mode.is_none()
                    && parts.failure.is_none()
                    && parts.candidate.is_none_or(|candidate| {
                        candidate.frontier == McpFrontierPresentation::BeforeFirst
                    })
            }
            McpProjectionLifecycle::CatchingUp => {
                parts.published.is_none()
                    && parts.candidate.is_some()
                    && parts.published_apply_mode.is_none()
                    && parts.failure.is_none()
            }
            McpProjectionLifecycle::Ready => {
                parts.published.is_some()
                    && parts.candidate.is_none()
                    && parts.published_apply_mode == Some(McpPublishedApplyMode::Enabled)
                    && parts.failure.is_none()
            }
            McpProjectionLifecycle::Rebuilding => {
                parts.published.is_some()
                    && parts.candidate.is_some()
                    && parts.failure.is_none()
                    && generations_in_order
            }
            McpProjectionLifecycle::Degraded | McpProjectionLifecycle::Invalid => {
                parts.failure.is_some()
                    && (parts.published.is_some() || parts.candidate.is_some())
                    && generations_in_order
            }
        };
        if parts.published.is_some() != parts.published_apply_mode.is_some()
            || parts
                .published
                .zip(parts.candidate)
                .is_some_and(|(published, candidate)| published.generation == candidate.generation)
            || !failure_matches_position
            || !failed_published_is_suspended
            || !lifecycle_shape
        {
            return Err(McpPresentationError);
        }
        let lag = parts
            .published
            .map(|published| {
                head.checked_sub(published.frontier.ordinal()?)
                    .map(McpPresentedU64::new)
                    .ok_or(McpPresentationError)
            })
            .transpose()?;
        Ok(Self {
            authoritative_head: parts.authoritative_head,
            candidate: parts.candidate,
            failure: parts.failure,
            identity: parts.identity,
            lag,
            lifecycle: parts.lifecycle,
            published: parts.published,
            published_apply_mode: parts.published_apply_mode,
        })
    }
}

/// Renders complete public projection status plus computed lag.
pub fn render_projection_status_resource(
    status: &McpProjectionStatusPresentation,
) -> Result<McpResourceJson, McpPresentationError> {
    McpResourceJson::from_serializable(status).map_err(|_| McpPresentationError)
}

fn descriptor_value(
    descriptor: &McpContractDescriptorPresentation,
) -> Result<Map<String, Value>, McpPresentationError> {
    serde_json::to_value(descriptor)
        .map_err(|_| McpPresentationError)?
        .as_object()
        .cloned()
        .ok_or(McpPresentationError)
}

fn presented_hash(bytes: [u8; 32]) -> Result<McpPresentedHash, McpPresentationError> {
    McpPresentedHash::from_slice(&bytes).map_err(|_| McpPresentationError)
}

fn validate_code_counts(
    counts: &[McpCompatibilityCodeCount],
    require_nonempty: bool,
) -> Result<(), McpPresentationError> {
    if counts.len() > MAX_COMPATIBILITY_CODES || require_nonempty && counts.is_empty() {
        return Err(McpPresentationError);
    }
    let mut previous = None;
    let mut total = 0u32;
    for count in counts {
        let position = COMPATIBILITY_CODES
            .iter()
            .position(|code| *code == count.code())
            .ok_or(McpPresentationError)?;
        if previous.is_some_and(|previous| previous >= position) {
            return Err(McpPresentationError);
        }
        previous = Some(position);
        total = total
            .checked_add(count.count().get())
            .ok_or(McpPresentationError)?;
    }
    if total > MAX_COMPATIBILITY_FINDINGS {
        return Err(McpPresentationError);
    }
    Ok(())
}

const fn compatibility_class(position: usize) -> McpContractCompatibilityClass {
    match position {
        0..=4 => McpContractCompatibilityClass::Compatible,
        5..=6 => McpContractCompatibilityClass::RequiresExplicitVersion,
        _ => McpContractCompatibilityClass::Incompatible,
    }
}

fn bounded_nonzero_ids(ids: &[u32], require_strict_order: bool) -> bool {
    ids.len() <= MAX_EXPLANATION_ITEMS
        && ids.iter().all(|id| *id != 0)
        && (!require_strict_order || ids.windows(2).all(|pair| pair[0] < pair[1]))
}

fn bounded_binding_ids(ids: &[u32]) -> bool {
    ids.len() <= MAX_EXPLANATION_ITEMS
        && ids
            .iter()
            .enumerate()
            .all(|(position, id)| u32::try_from(position) == Ok(*id))
}

fn bounded_sorted_refs(values: &[McpBindingFieldReferencePresentation]) -> bool {
    values.len() <= MAX_EXPLANATION_ITEMS && values.windows(2).all(|pair| pair[0] < pair[1])
}

fn input_field_guide(schema: &SchemaDocument) -> Result<String, McpPresentationError> {
    let root = schema.json_object();
    let properties = root
        .get("properties")
        .and_then(Value::as_object)
        .ok_or(McpPresentationError)?;
    let required = root
        .get("required")
        .and_then(Value::as_array)
        .ok_or(McpPresentationError)?
        .iter()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    let mut output = String::from("field\trequired\ttype\tconstraints");
    for (name, field) in properties {
        let object = field.as_object().ok_or(McpPresentationError)?;
        let kind = object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("declared union");
        let mut constraints = Vec::new();
        if object.get("pattern").and_then(Value::as_str) == Some(UUID_PATTERN) {
            constraints.push("UUID");
        } else if object.contains_key("pattern") {
            constraints.push("pattern");
        }
        if let (Some(precision), Some(scale)) = (
            object
                .get("x-riffdb-decimalPrecision")
                .and_then(Value::as_u64),
            object.get("x-riffdb-decimalScale").and_then(Value::as_u64),
        ) {
            constraints.push(if precision == scale {
                "fixed-scale decimal"
            } else {
                "decimal"
            });
        }
        if object.contains_key("minimum") {
            constraints.push("minimum");
        }
        if object.contains_key("maximum") {
            constraints.push("maximum");
        }
        if object.contains_key("minLength") || object.contains_key("x-riffdb-minUtf8Bytes") {
            constraints.push("minimum length");
        }
        if object.contains_key("maxLength") || object.contains_key("x-riffdb-maxUtf8Bytes") {
            constraints.push("maximum length");
        }
        output.push('\n');
        output.push_str(name);
        output.push('\t');
        output.push_str(if required.contains(name.as_str()) {
            "yes"
        } else {
            "no"
        });
        output.push('\t');
        output.push_str(kind);
        output.push('\t');
        let constraint_text = if constraints.is_empty() {
            "none".to_owned()
        } else {
            constraints.join(", ")
        };
        output.push_str(&constraint_text);
        if output.len() > MCP_OUTBOUND_MESSAGE_MAX_BYTES {
            return Err(McpPresentationError);
        }
    }
    Ok(output)
}

fn declared_outcome_name(example: &Value) -> Option<String> {
    let object = example.as_object()?;
    object
        .get("type")
        .or_else(|| object.get("outcome"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn minimal_example(
    schema: &SchemaDocument,
    root_branch: Option<usize>,
) -> Result<Value, McpPresentationError> {
    let root = Value::Object(schema.json_object());
    let node = match root_branch {
        Some(index) => root
            .as_object()
            .and_then(|root| root.get("oneOf"))
            .and_then(Value::as_array)
            .and_then(|branches| branches.get(index))
            .ok_or(McpPresentationError)?,
        None => &root,
    };
    let mut state = ExampleState { nodes: 0 };
    let value = generate_example_node(node, 0, &mut state)?;
    bounded_json::encoded_len(&value, MCP_OUTBOUND_MESSAGE_MAX_BYTES)
        .map_err(|_| McpPresentationError)?;
    Ok(value)
}

struct ExampleState {
    nodes: usize,
}

impl ExampleState {
    fn visit(&mut self, depth: usize) -> Result<(), McpPresentationError> {
        if depth > MAX_EXAMPLE_DEPTH {
            return Err(McpPresentationError);
        }
        self.nodes = self.nodes.checked_add(1).ok_or(McpPresentationError)?;
        if self.nodes > MAX_EXAMPLE_NODES {
            return Err(McpPresentationError);
        }
        Ok(())
    }
}

fn generate_example_node(
    schema: &Value,
    depth: usize,
    state: &mut ExampleState,
) -> Result<Value, McpPresentationError> {
    state.visit(depth)?;
    let schema = schema.as_object().ok_or(McpPresentationError)?;
    if let Some(value) = schema.get("const") {
        return Ok(value.clone());
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values.first().cloned().ok_or(McpPresentationError);
    }
    if let Some(branches) = schema.get("oneOf").and_then(Value::as_array) {
        let branch = branches.first().ok_or(McpPresentationError)?;
        return generate_example_node(branch, depth + 1, state);
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("null") => Ok(Value::Null),
        Some("boolean") => Ok(Value::Bool(false)),
        Some("integer") => minimal_integer(schema).map(Value::Number),
        Some("string") => minimal_string(schema).map(Value::String),
        Some("object") => generate_object_example(schema, depth, state),
        Some("array") => generate_array_example(schema, depth, state),
        _ => Err(McpPresentationError),
    }
}

fn generate_object_example(
    schema: &Map<String, Value>,
    depth: usize,
    state: &mut ExampleState,
) -> Result<Value, McpPresentationError> {
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or(McpPresentationError)?;
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .ok_or(McpPresentationError)?;
    let mut required_names = BTreeSet::new();
    for name in required {
        let name = name.as_str().ok_or(McpPresentationError)?;
        if !required_names.insert(name) || !properties.contains_key(name) {
            return Err(McpPresentationError);
        }
    }
    let mut object = Map::new();
    for (name, property_schema) in properties {
        if required_names.contains(name.as_str()) {
            object.insert(
                name.clone(),
                generate_example_node(property_schema, depth + 1, state)?,
            );
        }
    }
    Ok(Value::Object(object))
}

fn generate_array_example(
    schema: &Map<String, Value>,
    depth: usize,
    state: &mut ExampleState,
) -> Result<Value, McpPresentationError> {
    let minimum = schema.get("minItems").and_then(Value::as_u64).unwrap_or(0);
    let minimum = usize::try_from(minimum).map_err(|_| McpPresentationError)?;
    if minimum > MAX_EXAMPLE_ARRAY_ITEMS {
        return Err(McpPresentationError);
    }
    let prefix = schema
        .get("prefixItems")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut values = Vec::with_capacity(minimum);
    for item_schema in prefix.iter().take(minimum) {
        values.push(generate_example_node(item_schema, depth + 1, state)?);
    }
    if values.len() < minimum {
        let item_schema = schema
            .get("items")
            .filter(|items| items.is_object())
            .ok_or(McpPresentationError)?;
        while values.len() < minimum {
            values.push(generate_example_node(item_schema, depth + 1, state)?);
        }
    }
    Ok(Value::Array(values))
}

fn minimal_integer(schema: &Map<String, Value>) -> Result<Number, McpPresentationError> {
    let minimum = schema
        .get("minimum")
        .and_then(number_i128)
        .unwrap_or(i128::from(i64::MIN));
    let maximum = schema
        .get("maximum")
        .and_then(number_i128)
        .unwrap_or(i128::from(i64::MAX));
    let value = if minimum <= 0 && maximum >= 0 {
        0
    } else {
        minimum
    };
    if let Ok(value) = i64::try_from(value) {
        Ok(Number::from(value))
    } else {
        u64::try_from(value)
            .map(Number::from)
            .map_err(|_| McpPresentationError)
    }
}

fn minimal_string(schema: &Map<String, Value>) -> Result<String, McpPresentationError> {
    if let Some(scale) = schema.get("x-riffdb-decimalScale").and_then(Value::as_u64) {
        let scale = usize::try_from(scale).map_err(|_| McpPresentationError)?;
        if scale == 0 {
            return Ok("0".to_owned());
        }
        return Ok(format!("0.{}", "0".repeat(scale)));
    }
    if schema.get("x-riffdb-integerType").is_some() {
        let nonzero = schema
            .get("pattern")
            .and_then(Value::as_str)
            .is_some_and(|pattern| pattern.starts_with("^[1-9]"));
        return Ok(if nonzero { "1" } else { "0" }.to_owned());
    }
    if schema.get("contentEncoding").and_then(Value::as_str) == Some("base64") {
        return Ok(String::new());
    }
    match schema.get("pattern").and_then(Value::as_str) {
        Some(UUID_PATTERN) => return Ok("00000000-0000-0000-0000-000000000000".to_owned()),
        Some("^[0-9a-f]{32}$") => return Ok("0".repeat(32)),
        Some("^[0-9a-f]{64}$") => return Ok("0".repeat(64)),
        Some("^[A-Z]{3}$") => return Ok("AAA".to_owned()),
        Some("^[A-Za-z_][A-Za-z0-9_]{0,255}$") => return Ok("a".to_owned()),
        Some("^[ -~]+$") => return Ok("a".to_owned()),
        Some(pattern) if pattern.contains("[0-9]") && pattern.contains("\\.") => {
            return Err(McpPresentationError);
        }
        Some(_) => return Err(McpPresentationError),
        None => {}
    }
    let minimum = schema
        .get("minLength")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .max(
            schema
                .get("x-riffdb-minUtf8Bytes")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        );
    let minimum = usize::try_from(minimum).map_err(|_| McpPresentationError)?;
    if minimum > MCP_OUTBOUND_MESSAGE_MAX_BYTES {
        return Err(McpPresentationError);
    }
    Ok("a".repeat(minimum))
}

fn number_i128(value: &Value) -> Option<i128> {
    let number = value.as_number()?;
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

fn is_source_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && value.len() <= 256
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn presentation<T>(_: T) -> McpPresentationError {
    McpPresentationError
}

impl fmt::Display for McpContractCompatibilityClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Compatible => "compatible",
            Self::RequiresExplicitVersion => "requires_explicit_version",
            Self::Incompatible => "incompatible",
        })
    }
}

#[cfg(test)]
mod tests {
    use riffdb_types::hash_schema;
    use serde_json::json;

    use super::*;

    fn schema(id: &str, source: &str) -> SchemaDocument {
        let value: Value = serde_json::from_str(source).expect("schema JSON");
        let canonical = serde_json::to_string(&value).expect("canonical schema");
        assert_eq!(
            serde_json::to_string(
                &serde_json::from_str::<Value>(&canonical).expect("canonical schema parses")
            )
            .expect("canonical schema serializes"),
            canonical
        );
        if crate::schema::validate_schema_source(&value).is_err() {
            diagnose_schema_nodes(&value, "$");
            panic!("root schema is outside accepted subset");
        }
        SchemaDocument::from_public_parts(
            id,
            hash_schema(canonical.as_bytes()).as_bytes(),
            canonical,
        )
        .expect("checked schema")
    }

    fn diagnose_schema_nodes(node: &Value, path: &str) {
        let Some(object) = node.as_object() else {
            return;
        };
        for (name, child) in object
            .get("properties")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            let wrapped = json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "additionalProperties": false,
                "properties": {"value": child},
                "required": ["value"],
                "type": "object"
            });
            assert!(
                crate::schema::validate_schema_source(&wrapped).is_ok(),
                "unsupported schema node at {path}.properties.{name}: {child}"
            );
            diagnose_schema_nodes(child, &format!("{path}.properties.{name}"));
        }
        for (index, child) in object
            .get("oneOf")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            diagnose_schema_nodes(child, &format!("{path}.oneOf[{index}]"));
        }
    }

    fn descriptor() -> McpContractDescriptorPresentation {
        McpContractDescriptorPresentation::new(
            "LegalSpend",
            2,
            [0x00; 32],
            [0x11; 32],
            [0x22; 32],
            McpContractCompatibilityPresentation::successor(
                1,
                [0xaa; 32],
                McpContractCompatibilityClass::RequiresExplicitVersion,
                vec![
                    McpCompatibilityCodeCount::new("RDB-K010", 1).expect("count"),
                    McpCompatibilityCodeCount::new("RDB-K020", 2).expect("count"),
                ],
            )
            .expect("compatibility"),
        )
        .expect("descriptor")
    }

    fn explanation() -> McpCommandExplanationPresentation {
        McpCommandExplanationPresentation::from_fields(McpCommandExplanationFields {
            command_id: 2,
            execution_class: McpCommandExecutionClass::IdempotentMutation,
            partition_component_count: 1,
            conflict_key_count: 1,
            binding_ids: vec![0],
            read_fields: vec![McpBindingFieldReferencePresentation::new(0, 1).expect("reference")],
            write_fields: vec![McpBindingFieldReferencePresentation::new(0, 2).expect("reference")],
            invariant_ids: vec![1],
            event_type_ids: vec![1],
            outcome_ids: vec![1, 2, 3, 4],
            rendered_text: "command:2\nexecution:IdempotentMutation\n".to_owned(),
        })
        .expect("explanation")
    }

    fn explained() -> McpExplainedCommandPresentation {
        let input = schema(
            "riffdb.generated-schema/command-input/2/v1",
            include_str!("../../../fixtures/compiler/schemas/03-00000001.json"),
        );
        let outcome = schema(
            "riffdb.generated-schema/command-outcome-union/2/v1",
            include_str!("../../../fixtures/compiler/schemas/04-00000002.json"),
        );
        McpExplainedCommandPresentation::new(
            descriptor(),
            "AllocateBudget",
            "riffdb_cmd_legalspend_allocatebudget",
            [0x33; 32],
            explanation(),
            input,
            outcome,
        )
        .expect("explained command")
    }

    #[test]
    fn contract_resources_have_one_corrected_descriptor_shape() {
        let descriptor = descriptor();
        let expected_version = json!({
            "bundle_hash": "0000000000000000000000000000000000000000000000000000000000000000",
            "compatibility": {
                "code_counts": [
                    {"code": "RDB-K010", "count": 1},
                    {"code": "RDB-K020", "count": 2}
                ],
                "overall": "requires_explicit_version",
                "parent": {
                    "bundle_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "contract_version": "1"
                }
            },
            "contract_lineage": "LegalSpend",
            "contract_version": "2",
            "plan_root_hash": "2222222222222222222222222222222222222222222222222222222222222222",
            "source_hash": "1111111111111111111111111111111111111111111111111111111111111111"
        });
        assert_eq!(
            render_contract_version_resource(&descriptor).expect("version"),
            McpResourceJson::from_serializable(&expected_version).expect("expected")
        );
        let mut expected_active = expected_version.as_object().expect("object").clone();
        expected_active.insert(
            "links".to_owned(),
            json!({"contract_version": "riffdb://contract/LegalSpend/2"}),
        );
        assert_eq!(
            render_active_contract_resource(&descriptor).expect("active"),
            McpResourceJson::from_serializable(&expected_active).expect("expected")
        );
    }

    #[test]
    fn command_explanation_requires_dense_zero_based_binding_ids() {
        assert!(McpBindingFieldReferencePresentation::new(0, 1).is_ok());
        assert!(McpBindingFieldReferencePresentation::new(0, 0).is_err());

        let mut fields = McpCommandExplanationFields {
            command_id: 2,
            execution_class: McpCommandExecutionClass::ReadOnly,
            partition_component_count: 1,
            conflict_key_count: 1,
            binding_ids: vec![0, 2],
            read_fields: vec![],
            write_fields: vec![],
            invariant_ids: vec![],
            event_type_ids: vec![],
            outcome_ids: vec![1],
            rendered_text: "command:2\nexecution:ReadOnly\n".to_owned(),
        };
        assert!(McpCommandExplanationPresentation::from_fields(fields.clone()).is_err());

        fields.binding_ids = vec![0, 1];
        assert!(McpCommandExplanationPresentation::from_fields(fields).is_ok());
    }

    #[test]
    fn compatibility_summary_rejects_noncanonical_or_unbounded_counts() {
        let reversed = vec![
            McpCompatibilityCodeCount::new("RDB-K013", 1).expect("count"),
            McpCompatibilityCodeCount::new("RDB-K010", 1).expect("count"),
        ];
        assert_eq!(
            McpContractCompatibilityPresentation::successor(
                1,
                [0; 32],
                McpContractCompatibilityClass::Compatible,
                reversed,
            ),
            Err(McpPresentationError)
        );
        assert!(
            McpContractCompatibilityPresentation::successor(
                1,
                [0; 32],
                McpContractCompatibilityClass::Compatible,
                Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn command_plan_contains_exact_schemas_and_complete_explanation() {
        let command = explained();
        let expected = serde_json::to_value(serde_json::json!({
            "command_id": 2,
            "contract_lineage": "LegalSpend",
            "contract_version": "2",
            "explanation": command.explanation(),
            "input_schema": command.input_schema(),
            "outcome_schema": command.outcome_schema(),
            "plan_hash": "3333333333333333333333333333333333333333333333333333333333333333",
            "source_command": "AllocateBudget"
        }))
        .expect("expected value");
        assert_eq!(
            render_command_plan_resource(&command).expect("plan"),
            McpResourceJson::from_serializable(&expected).expect("expected")
        );
    }

    #[test]
    fn generated_documentation_is_deterministic_factual_and_injection_safe() {
        let mut command = explained();
        command.source_command = "AllocateBudget".to_owned();
        let first = render_command_documentation(&command).expect("documentation");
        let second = render_command_documentation(&command).expect("documentation");
        assert_eq!(first, second);
        let text = first.as_str();
        assert!(text.starts_with("# AllocateBudget\n"));
        assert!(text.contains("## MCP Tool\n"));
        assert!(text.contains("riffdb_cmd_legalspend_allocatebudget"));
        assert!(text.contains("## Inputs\n"));
        assert!(text.contains("field required type constraints"));
        assert!(text.contains("## Call Example\n"));
        assert!(text.contains("## Retry and Uncertainty\n"));
        assert!(text.contains("## Declared Outcomes\n"));
        assert!(text.contains("## Plan Resource\n"));
        assert!(text.contains("riffdb://command/LegalSpend/2/plan"));
        assert!(!text.contains("## Execution Plan\n"));
        assert_eq!(text.matches("\n### ").count(), 4);
        assert!(text.contains(crate::MCP_IDEMPOTENT_MUTATION_CANCELLATION_NOTICE));
        assert!(text.contains("\"idempotency_key\":\"a\""));
        assert!(text.contains("\"type\":\"Allocated\""));
        assert!(!text.contains("Allocates an approved amount"));
        assert_eq!(
            text,
            include_str!("../fixtures/command-documentation-v2.md")
        );
    }

    #[test]
    fn every_generated_example_revalidates_against_its_exact_schema() {
        let command = explained();
        let input = minimal_example(command.input_schema(), None).expect("input example");
        RiffDbSchemaValidator
            .validate(command.input_schema(), &input)
            .expect("valid input");
        let branches = command.outcome_schema().json_object()["oneOf"]
            .as_array()
            .expect("branches")
            .len();
        for index in 0..branches {
            let outcome =
                minimal_example(command.outcome_schema(), Some(index)).expect("outcome example");
            assert!(
                RiffDbSchemaValidator
                    .validate(command.outcome_schema(), &outcome)
                    .is_ok(),
                "invalid outcome branch {index}: {outcome}"
            );
        }
    }

    #[test]
    fn projection_status_lag_is_derived_or_null() {
        let identity =
            McpProjectionIdentityPresentation::new("LegalSpend", 1, [0x44; 32]).expect("identity");
        let caught_up = McpProjectionStatusPresentation::from_parts(McpProjectionStatusParts {
            identity: identity.clone(),
            lifecycle: McpProjectionLifecycle::Ready,
            published: Some(
                McpProjectionGenerationFrontierPresentation::new(
                    3,
                    McpFrontierPresentation::AppliedThrough(7),
                )
                .expect("published"),
            ),
            candidate: None,
            published_apply_mode: Some(McpPublishedApplyMode::Enabled),
            failure: None,
            authoritative_head: McpFrontierPresentation::AppliedThrough(12),
        })
        .expect("status");
        assert_eq!(
            render_projection_status_resource(&caught_up).expect("resource"),
            McpResourceJson::from_serializable(&json!({
                "authoritative_head": {"applied_through": "12"},
                "identity": {
                    "contract_lineage": "LegalSpend",
                    "projection_id": 1,
                    "projection_plan_hash": "4444444444444444444444444444444444444444444444444444444444444444"
                },
                "lag": "5",
                "lifecycle": "ready",
                "published": {
                    "frontier": {"applied_through": "7"},
                    "generation": "3"
                },
                "published_apply_mode": "enabled"
            }))
            .expect("expected")
        );

        let uninitialized = McpProjectionStatusPresentation::from_parts(McpProjectionStatusParts {
            identity,
            lifecycle: McpProjectionLifecycle::Building,
            published: None,
            candidate: None,
            published_apply_mode: None,
            failure: None,
            authoritative_head: McpFrontierPresentation::BeforeFirst,
        })
        .expect("status");
        assert_eq!(
            serde_json::to_value(&uninitialized).expect("serialize")["lag"],
            Value::Null
        );
    }

    #[test]
    fn projection_status_rejects_frontiers_ahead_of_authority() {
        let status = McpProjectionStatusPresentation::from_parts(McpProjectionStatusParts {
            identity: McpProjectionIdentityPresentation::new("LegalSpend", 1, [0; 32])
                .expect("identity"),
            lifecycle: McpProjectionLifecycle::Ready,
            published: Some(
                McpProjectionGenerationFrontierPresentation::new(
                    1,
                    McpFrontierPresentation::AppliedThrough(2),
                )
                .expect("published"),
            ),
            candidate: None,
            published_apply_mode: Some(McpPublishedApplyMode::Enabled),
            failure: None,
            authoritative_head: McpFrontierPresentation::AppliedThrough(1),
        });
        assert_eq!(status, Err(McpPresentationError));
    }
}
