//! Checked aggregate-local workflow declarations.

use std::collections::BTreeSet;

use riffdb_types::{EntityTypeId, EnumTypeId, EnumVariantId, FieldId};

use crate::{
    IrValidationError, MAX_DECLARATIONS_PER_KIND, SchemaIr, ValueTypeTag, checked_len,
    validate_source_name,
};

/// Inclusive maximum compiler-declared lease duration.
pub const MAX_WORKFLOW_LEASE_DURATION_SECONDS: u64 = 86_400;

/// One legal directed state transition in a workflow declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTransitionSchema {
    name: String,
    source_states: Vec<EnumVariantId>,
    destination: EnumVariantId,
}

impl WorkflowTransitionSchema {
    /// Creates a transition with a nonempty canonical source-state set.
    pub fn new(
        name: impl Into<String>,
        mut source_states: Vec<EnumVariantId>,
        destination: EnumVariantId,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "workflow transition")?;
        checked_len(
            "workflow transition source states",
            source_states.len(),
            MAX_DECLARATIONS_PER_KIND,
        )?;
        source_states.sort_unstable();
        if source_states.is_empty() || source_states.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "workflow transition source states",
            });
        }
        Ok(Self {
            name,
            source_states,
            destination,
        })
    }

    /// Exact source symbol.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Canonical legal source states.
    #[must_use]
    pub fn source_states(&self) -> &[EnumVariantId] {
        &self.source_states
    }
    /// Exact destination state.
    #[must_use]
    pub const fn destination(&self) -> EnumVariantId {
        self.destination
    }
}

/// Stored fields and duration bounds for one aggregate-local fenced lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowLeaseSchema {
    name: String,
    owner_field: FieldId,
    expiry_field: FieldId,
    fencing_token_field: FieldId,
    attempt_field: Option<FieldId>,
    minimum_duration_seconds: u64,
    maximum_duration_seconds: u64,
}

impl WorkflowLeaseSchema {
    /// Creates one bounded lease declaration.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        owner_field: FieldId,
        expiry_field: FieldId,
        fencing_token_field: FieldId,
        attempt_field: Option<FieldId>,
        minimum_duration_seconds: u64,
        maximum_duration_seconds: u64,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "workflow lease")?;
        let mut fields = vec![owner_field, expiry_field, fencing_token_field];
        if let Some(field) = attempt_field {
            fields.push(field);
        }
        fields.sort_unstable();
        if fields.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(IrValidationError::InvalidReference {
                kind: "workflow lease fields",
            });
        }
        if minimum_duration_seconds == 0
            || minimum_duration_seconds > maximum_duration_seconds
            || maximum_duration_seconds > MAX_WORKFLOW_LEASE_DURATION_SECONDS
        {
            return Err(IrValidationError::LimitExceeded {
                kind: "workflow lease duration seconds",
                maximum: MAX_WORKFLOW_LEASE_DURATION_SECONDS as usize,
                actual: maximum_duration_seconds as usize,
            });
        }
        Ok(Self {
            name,
            owner_field,
            expiry_field,
            fencing_token_field,
            attempt_field,
            minimum_duration_seconds,
            maximum_duration_seconds,
        })
    }

    /// Exact source symbol.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Optional UUID owner field.
    #[must_use]
    pub const fn owner_field(&self) -> FieldId {
        self.owner_field
    }
    /// Optional timestamp expiration field.
    #[must_use]
    pub const fn expiry_field(&self) -> FieldId {
        self.expiry_field
    }
    /// Monotonic nonzero `u64` fencing-token field.
    #[must_use]
    pub const fn fencing_token_field(&self) -> FieldId {
        self.fencing_token_field
    }
    /// Optional bounded-attempt counter field.
    #[must_use]
    pub const fn attempt_field(&self) -> Option<FieldId> {
        self.attempt_field
    }
    /// Inclusive minimum duration.
    #[must_use]
    pub const fn minimum_duration_seconds(&self) -> u64 {
        self.minimum_duration_seconds
    }
    /// Inclusive maximum duration.
    #[must_use]
    pub const fn maximum_duration_seconds(&self) -> u64 {
        self.maximum_duration_seconds
    }
}

/// One compiler-checked workflow over an aggregate-owned entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowSchema {
    name: String,
    entity: EntityTypeId,
    state_field: FieldId,
    state_enum: EnumTypeId,
    transitions: Vec<WorkflowTransitionSchema>,
    lease: Option<WorkflowLeaseSchema>,
}

impl WorkflowSchema {
    /// Creates one canonically ordered workflow declaration.
    pub fn new(
        name: impl Into<String>,
        entity: EntityTypeId,
        state_field: FieldId,
        state_enum: EnumTypeId,
        mut transitions: Vec<WorkflowTransitionSchema>,
        lease: Option<WorkflowLeaseSchema>,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "workflow")?;
        checked_len(
            "workflow transitions",
            transitions.len(),
            MAX_DECLARATIONS_PER_KIND,
        )?;
        transitions.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if transitions.is_empty()
            || transitions
                .windows(2)
                .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(IrValidationError::InvalidName {
                kind: "workflow transition",
            });
        }
        let mut edges = BTreeSet::new();
        if transitions.iter().any(|transition| {
            transition
                .source_states
                .iter()
                .any(|source| !edges.insert((*source, transition.destination)))
        }) {
            return Err(IrValidationError::InvalidReference {
                kind: "duplicate workflow transition edge",
            });
        }
        Ok(Self {
            name,
            entity,
            state_field,
            state_enum,
            transitions,
            lease,
        })
    }

    /// Exact source symbol.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Aggregate-owned entity carrying workflow state.
    #[must_use]
    pub const fn entity(&self) -> EntityTypeId {
        self.entity
    }
    /// Stored enum state field.
    #[must_use]
    pub const fn state_field(&self) -> FieldId {
        self.state_field
    }
    /// Declared enum containing every transition state.
    #[must_use]
    pub const fn state_enum(&self) -> EnumTypeId {
        self.state_enum
    }
    /// Legal transitions in source-symbol order.
    #[must_use]
    pub fn transitions(&self) -> &[WorkflowTransitionSchema] {
        &self.transitions
    }
    /// Optional fenced lease declaration.
    #[must_use]
    pub const fn lease(&self) -> Option<&WorkflowLeaseSchema> {
        self.lease.as_ref()
    }
}

/// Complete canonical workflow catalog for one contract bundle.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkflowCatalog {
    workflows: Vec<WorkflowSchema>,
}

impl WorkflowCatalog {
    /// Validates workflow references against one structural schema.
    pub fn new(
        mut workflows: Vec<WorkflowSchema>,
        schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        checked_len("workflows", workflows.len(), MAX_DECLARATIONS_PER_KIND)?;
        workflows.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if workflows
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(IrValidationError::InvalidName {
                kind: "duplicate workflow",
            });
        }
        let mut entities = BTreeSet::new();
        for workflow in &workflows {
            if !entities.insert(workflow.entity) {
                return Err(IrValidationError::InvalidReference {
                    kind: "entity has multiple workflows",
                });
            }
            validate_workflow(workflow, schema)?;
        }
        Ok(Self { workflows })
    }

    /// Workflows in canonical source-symbol order.
    #[must_use]
    pub fn workflows(&self) -> &[WorkflowSchema] {
        &self.workflows
    }

    /// Resolves the workflow for one entity.
    #[must_use]
    pub fn for_entity(&self, entity: EntityTypeId) -> Option<&WorkflowSchema> {
        self.workflows
            .iter()
            .find(|workflow| workflow.entity == entity)
    }

    /// True when no workflow semantics are declared.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.workflows.is_empty()
    }
}

fn validate_workflow(
    workflow: &WorkflowSchema,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    let entity = schema
        .entity(workflow.entity)
        .ok_or(IrValidationError::InvalidReference {
            kind: "workflow entity",
        })?;
    if schema.aggregate_for_entity(workflow.entity).is_none()
        || entity.primary_key_fields().contains(&workflow.state_field)
    {
        return Err(IrValidationError::InvalidReference {
            kind: "workflow aggregate ownership or state field",
        });
    }
    let state =
        entity
            .record()
            .field(workflow.state_field)
            .ok_or(IrValidationError::InvalidReference {
                kind: "workflow state field",
            })?;
    if state.value_type().enum_type_id() != Some(workflow.state_enum) {
        return Err(IrValidationError::TypeMismatch {
            context: "workflow state field",
        });
    }
    let enumeration =
        schema
            .enumeration(workflow.state_enum)
            .ok_or(IrValidationError::InvalidReference {
                kind: "workflow state enum",
            })?;
    let variants = enumeration
        .variants()
        .iter()
        .map(crate::EnumVariantSchema::id)
        .collect::<BTreeSet<_>>();
    if workflow.transitions.iter().any(|transition| {
        !variants.contains(&transition.destination)
            || transition
                .source_states
                .iter()
                .any(|source| !variants.contains(source))
    }) {
        return Err(IrValidationError::InvalidReference {
            kind: "workflow transition state",
        });
    }
    if let Some(lease) = &workflow.lease {
        validate_lease_field(entity, lease.owner_field, ValueTypeTag::Uuid, true)?;
        validate_lease_field(entity, lease.expiry_field, ValueTypeTag::Timestamp, true)?;
        validate_lease_field(entity, lease.fencing_token_field, ValueTypeTag::U64, false)?;
        if let Some(field) = lease.attempt_field {
            validate_lease_field(entity, field, ValueTypeTag::U64, false)?;
        }
    }
    Ok(())
}

fn validate_lease_field(
    entity: &crate::EntitySchema,
    field: FieldId,
    expected: ValueTypeTag,
    optional: bool,
) -> Result<(), IrValidationError> {
    if entity.primary_key_fields().contains(&field) {
        return Err(IrValidationError::InvalidReference {
            kind: "workflow lease key field",
        });
    }
    let value_type = entity
        .record()
        .field(field)
        .ok_or(IrValidationError::InvalidReference {
            kind: "workflow lease field",
        })?
        .value_type();
    let actual = if optional {
        value_type.optional_inner().map(crate::ValueType::tag)
    } else {
        Some(value_type.tag())
    };
    if actual != Some(expected) {
        return Err(IrValidationError::TypeMismatch {
            context: "workflow lease field",
        });
    }
    Ok(())
}
