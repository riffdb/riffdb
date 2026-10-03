//! Closed provider-owned schema generation with pre-admitted JSON workspace.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget,
};
use std::{
    mem::{align_of, size_of},
    sync::Arc,
};

/// Borrowed first-party inputs; no runtime schema or application budget selector.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum ProviderSchemaArtifactSourceV1<'a> {
    Entity {
        id: EntityTypeId,
        record: &'a RecordSchema,
    },
    Event {
        id: EventTypeId,
        record: &'a RecordSchema,
    },
    CommandInput {
        id: CommandId,
        record: &'a RecordSchema,
        idempotency_input: Option<FieldId>,
    },
    CommandOutcomes {
        id: CommandId,
        outcomes: &'a [OutcomeSchema],
    },
    ProjectionResultFromPlan {
        plan: &'a crate::ProjectionPlan,
    },
    ProjectionResult {
        id: ProjectionId,
        group_types: &'a [ValueType],
        measures: &'a RecordSchema,
    },
}

/// Safe internal distinction between admission and unchanged artifact validation.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSchemaArtifactErrorV1 {
    Unavailable,
    Invalid(IrValidationError),
}
impl From<BudgetUnavailable> for ProviderSchemaArtifactErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::Unavailable
    }
}
impl From<IrValidationError> for ProviderSchemaArtifactErrorV1 {
    fn from(error: IrValidationError) -> Self {
        Self::Invalid(error)
    }
}
impl std::fmt::Display for ProviderSchemaArtifactErrorV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("provider schema artifact unavailable"),
            Self::Invalid(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for ProviderSchemaArtifactErrorV1 {}

// A full std B-tree node per entry plus one root is the same conservative
// eleven-slot/twelve-edge layout used by the ownership inspector. There are
// at most N entries and N objects in N rendered bytes, so doubling allocation
// capacities costs at most four nodes per rendered byte. Array growth retains
// at most eight element slots per initialized element across old/new buffers;
// doubling those capacities costs sixteen Json slots per byte. The remaining
// allowance covers array headers, growing string buffers, string headers and
// output encoding. Empty objects also fit the separate object/root term.
const TREE_NODE_BYTES: usize = 11 * (size_of::<String>() + size_of::<Json>())
    + 256
    + align_of::<String>()
    + align_of::<Json>();
const PER_RENDERED_BYTE: usize =
    4 * TREE_NODE_BYTES + 16 * size_of::<Json>() + 4 * align_of::<Json>() + 1024;
const SIZING_WORK: u64 = 32 * 1024;

pub(crate) fn json_workspace_bound(bytes: usize) -> Result<u64, BudgetUnavailable> {
    bytes
        .checked_mul(PER_RENDERED_BYTE)
        .and_then(|value| value.checked_add(64 * 1024))
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(BudgetUnavailable)
}

impl ProviderSchemaArtifactSourceV1<'_> {
    /// Generate the same ordinary artifact after admitting its complete JSON work.
    pub fn generate_admitted(
        self,
        schema: &SchemaIr,
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderAllocation<GeneratedSchemaArtifact>>, ProviderSchemaArtifactErrorV1>
    {
        // Sizing walks borrowed declarations. Its only buffers are bounded
        // numeric/pattern formatting temporaries, released before JSON building.
        let sizing = budget.reserve(SIZING_WORK)?;
        let expected = self.expected_size(schema)?;
        checked_len(
            "generated JSON Schema",
            expected,
            MAX_JSON_SCHEMA_ARTIFACT_BYTES,
        )?;
        let maximum = json_workspace_bound(expected)?
            .checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .ok_or(BudgetUnavailable)?;
        let work = budget.reserve(maximum)?;
        drop(sizing);
        let artifact = self.generate_ordinary(schema)?;
        work.settle_owned_value(artifact).map_err(Into::into)
    }

    pub(crate) fn expected_size(self, schema: &SchemaIr) -> Result<usize, IrValidationError> {
        match self {
            Self::Entity { id, record } => {
                require_shape(JsonSchemaShapeConstruction::OutputRecord)?;
                if record.owner() != &RecordTypeRef::Entity(id) {
                    return Err(IrValidationError::InvalidReference {
                        kind: "record schema artifact owner",
                    });
                }
                record_node_size(record, schema, RecordShape::Output, true)
            }
            Self::Event { id, record } => {
                require_shape(JsonSchemaShapeConstruction::OutputRecord)?;
                if record.owner() != &RecordTypeRef::Event(id) {
                    return Err(IrValidationError::InvalidReference {
                        kind: "record schema artifact owner",
                    });
                }
                record_node_size(record, schema, RecordShape::Output, true)
            }
            Self::CommandInput {
                id,
                record,
                idempotency_input,
            } => {
                validate_command_input_artifact(id, record, idempotency_input)?;
                record_node_size(
                    record,
                    schema,
                    RecordShape::CommandInput { idempotency_input },
                    true,
                )
            }
            Self::CommandOutcomes { id, outcomes } => {
                require_shape(JsonSchemaShapeConstruction::CommandOutcomeUnion)?;
                command_outcomes_size(id, outcomes, schema)
            }
            Self::ProjectionResultFromPlan { plan } => {
                let group = plan.group_schema();
                validate_projection_result_artifact(plan.projection_id(), group.measures())?;
                projection_result_size(
                    group
                        .group_components()
                        .iter()
                        .map(crate::ProjectionGroupComponentSchema::value_type),
                    group.measures(),
                    schema,
                )
            }
            Self::ProjectionResult {
                id,
                group_types,
                measures,
            } => {
                validate_projection_result_artifact(id, measures)?;
                projection_result_size(group_types.iter(), measures, schema)
            }
        }
    }

    pub(crate) fn generate_ordinary(
        self,
        schema: &SchemaIr,
    ) -> Result<GeneratedSchemaArtifact, IrValidationError> {
        let (key, node, expected) = self.parts(schema)?;
        GeneratedSchemaArtifact::from_node(key, node, expected)
    }

    pub(super) fn parts(
        self,
        schema: &SchemaIr,
    ) -> Result<(SchemaArtifactKey, Json, usize), IrValidationError> {
        match self {
            Self::Entity { id, record } => {
                GeneratedSchemaArtifact::entity_parts(id, record, schema)
            }
            Self::Event { id, record } => GeneratedSchemaArtifact::event_parts(id, record, schema),
            Self::CommandInput {
                id,
                record,
                idempotency_input,
            } => {
                GeneratedSchemaArtifact::command_input_parts(id, record, schema, idempotency_input)
            }
            Self::CommandOutcomes { id, outcomes } => {
                GeneratedSchemaArtifact::command_outcomes_parts(id, outcomes, schema)
            }
            Self::ProjectionResultFromPlan { plan } => {
                let group = plan.group_schema();
                GeneratedSchemaArtifact::projection_result_parts(
                    plan.projection_id(),
                    group
                        .group_components()
                        .iter()
                        .map(crate::ProjectionGroupComponentSchema::value_type),
                    group.measures(),
                    schema,
                )
            }
            Self::ProjectionResult {
                id,
                group_types,
                measures,
            } => GeneratedSchemaArtifact::projection_result_parts(
                id,
                group_types.iter(),
                measures,
                schema,
            ),
        }
    }
}
