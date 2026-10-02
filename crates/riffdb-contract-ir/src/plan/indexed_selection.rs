//! Closed, single-candidate indexed selection and its structural proof.

mod capacity;
use super::*;

/// One ascending, exact equality-prefix selection with a one-entry bound.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedSelectionPlan {
    index: IndexId,
    prefix_expressions: Vec<ExprId>,
}

impl IndexedSelectionPlan {
    /// Builds the bounded descriptor; command construction verifies its schema and ownership.
    pub fn new(index: IndexId, prefix_expressions: Vec<ExprId>) -> Result<Self, IrValidationError> {
        if prefix_expressions.is_empty() || prefix_expressions.len() > MAX_OBJECT_FIELDS {
            return Err(invalid_selection());
        }
        Ok(Self {
            index,
            prefix_expressions,
        })
    }
    /// Authoritative local secondary index.
    #[must_use]
    pub const fn index(&self) -> IndexId {
        self.index
    }
    /// Input/constant equality values in declared index order.
    #[must_use]
    pub fn prefix_expressions(&self) -> &[ExprId] {
        &self.prefix_expressions
    }
}

impl BindingPlan {
    /// Attaches a selector; the owning command rederives the full proof.
    pub fn with_indexed_selection(
        mut self,
        selection: IndexedSelectionPlan,
    ) -> Result<Self, IrValidationError> {
        if self.mode != BindingMode::Mutate || self.indexed_selection.is_some() {
            return Err(invalid_selection());
        }
        self.indexed_selection = Some(selection);
        Ok(self)
    }
    /// Selector for this binding, absent on exact-key bindings.
    #[must_use]
    pub const fn indexed_selection(&self) -> Option<&IndexedSelectionPlan> {
        self.indexed_selection.as_ref()
    }
}

impl CommandPlan {
    /// Selected binding and its bounded descriptor, if this command selects a row.
    #[must_use]
    pub fn indexed_selection(&self) -> Option<(&BindingPlan, &IndexedSelectionPlan)> {
        self.bindings.iter().find_map(|binding| {
            binding
                .indexed_selection()
                .map(|selection| (binding, selection))
        })
    }
    /// Selector semantics require executable IR V26.
    #[must_use]
    pub fn requires_ir_v26(&self) -> bool {
        self.indexed_selection().is_some()
    }
}

fn invalid_selection() -> IrValidationError {
    IrValidationError::InvalidDependency {
        reason: "invalid indexed selection proof",
    }
}

pub(super) fn is_selected_key(
    binding: &BindingPlan,
    kind: &ExpressionKind,
    field: FieldId,
) -> bool {
    binding.indexed_selection.is_some()
        && matches!(kind, ExpressionKind::BoundField { binding: id, field: actual } if *id == binding.id && *actual == field)
}

pub(super) fn validate_indexed_selection(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    collection: Option<&CollectionExpansionPlanV1>,
    decisions: &[CommandDecisionPlanV1],
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    let selected: Vec<_> = bindings
        .iter()
        .filter(|binding| binding.indexed_selection.is_some())
        .collect();
    if selected.is_empty() {
        return Ok(());
    }
    if selected.len() != 1 || collection.is_some() {
        return Err(invalid_selection());
    }
    let binding = selected[0];
    let selection = binding.indexed_selection().ok_or_else(invalid_selection)?;
    let entity = schema
        .entity(binding.entity_type())
        .ok_or_else(invalid_selection)?;
    let index = entity
        .indexes()
        .iter()
        .find(|index| index.id() == selection.index())
        .ok_or_else(invalid_selection)?;
    if binding.mode != BindingMode::Mutate
        || index
            .fields()
            .iter()
            .any(|field| schema.is_secret_field(entity.id(), *field))
        || selection.prefix_expressions.len() > index.fields().len()
        || index
            .encodings()
            .iter()
            .take(selection.prefix_expressions.len())
            .any(|encoding| *encoding != crate::IndexFieldEncodingV1::Canonical)
        || bindings.iter().any(|other| {
            (other.id != binding.id && other.entity_type == binding.entity_type)
                || other.restriction_failure.is_some()
                || other.cascade_failure.is_some()
        })
        || decisions
            .iter()
            .flat_map(|decision| {
                decision
                    .when_arms()
                    .iter()
                    .map(CommandDecisionArmV1::action)
                    .chain(std::iter::once(decision.else_action()))
            })
            .any(|action| action.bindings().contains(&binding.id))
    {
        return Err(invalid_selection());
    }
    for (expression, field) in selection.prefix_expressions.iter().zip(index.fields()) {
        let node = arena.get(*expression).ok_or_else(invalid_selection)?;
        let field = entity
            .record()
            .field(*field)
            .ok_or_else(invalid_selection)?;
        if node.result_type() != field.value_type()
            || !arena.dependencies(*expression)?.is_input_computable()
        {
            return Err(invalid_selection());
        }
    }
    // Every input-derived key component must be fixed by the equality prefix. All
    // remaining components are exact self-key references resolved by snapshot proof.
    // Existing locality validation then proves partition and complete conflict ownership.
    for (field, expression) in entity
        .primary_key_fields()
        .iter()
        .zip(binding.key_expressions())
    {
        if let Some(position) = index
            .fields()
            .iter()
            .take(selection.prefix_expressions.len())
            .position(|id| id == field)
        {
            if !expression_tuples_equal(
                arena,
                &[*expression],
                arena,
                &[selection.prefix_expressions[position]],
            )? {
                return Err(invalid_selection());
            }
        } else if !is_selected_key(
            binding,
            arena.get(*expression).ok_or_else(invalid_selection)?.kind(),
            *field,
        ) {
            return Err(invalid_selection());
        }
    }
    Ok(())
}

pub(super) fn validate_binding_plans(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    locality: &LocalityPlan,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    for binding in bindings {
        let entity =
            schema
                .entity(binding.entity_type)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "binding entity",
                })?;
        let owner = schema.aggregate_for_entity(binding.entity_type).ok_or(
            IrValidationError::InvalidReference {
                kind: "binding aggregate owner",
            },
        )?;
        // ADR-0170: a mutable binding may belong to any aggregate the command
        // takes conflict ownership of, not only the partition owner. The
        // locality's conflict keys name that set, so membership in it is the
        // invariant; a binding whose aggregate is absent has no lease and is
        // still rejected.
        let owned = locality.conflict_keys.iter().any(
            |key| matches!(key.schema().purpose(), KeyPurpose::Conflict(id) if id == owner.id()),
        );
        if entity.primary_key() != &binding.key_schema
            || (matches!(
                binding.mode,
                BindingMode::Mutate
                    | BindingMode::Create
                    | BindingMode::InitOrMutate
                    | BindingMode::ObserveOrInitialize
                    | BindingMode::Delete
            ) && !owned)
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "binding key mismatch or mutable binding has no conflict ownership",
            });
        }
        for (position, (expression, component)) in binding
            .key_expressions
            .iter()
            .zip(binding.key_schema.components())
            .enumerate()
        {
            let node = arena
                .get(*expression)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "binding key expression",
                })?;
            if node.result_type() != component.value_type()
                || (!arena.dependencies(*expression)?.is_input_computable()
                    && !is_selected_key(
                        binding,
                        node.kind(),
                        entity.primary_key_fields()[position],
                    ))
            {
                return Err(IrValidationError::InvalidDependency {
                    reason: "binding key is not input-computable with the declared type",
                });
            }
        }
        if binding
            .accessed_fields
            .iter()
            .any(|field| entity.record().field(*field).is_none())
        {
            return Err(IrValidationError::InvalidReference {
                kind: "binding accessed field",
            });
        }
        let key_fields = entity
            .primary_key_fields()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        for initialized in binding.initializer() {
            let field = entity.record().field(initialized.field_id()).ok_or(
                IrValidationError::InvalidReference {
                    kind: "binding initializer field",
                },
            )?;
            let expression =
                arena
                    .get(initialized.expression())
                    .ok_or(IrValidationError::InvalidReference {
                        kind: "binding initializer expression",
                    })?;
            let dependencies = arena.dependencies(initialized.expression())?;
            if key_fields.contains(&initialized.field_id())
                || expression.result_type() != field.value_type()
                || !dependencies.bindings().is_empty()
                || !dependencies.schema_fields().is_empty()
                || !dependencies.root_validation_reads().is_empty()
                || !dependencies.source_event_fields().is_empty()
                || dependencies.uses_transaction_date()
            {
                return Err(IrValidationError::InvalidDependency {
                    reason: "binding initializer is not state-independent with the declared type",
                });
            }
        }
        if let Some(failure) = &binding.failure {
            for field in &failure.payload.fields {
                let dependencies = arena.dependencies(field.expression)?;
                if !dependencies.is_input_computable() || dependencies.uses_transaction_time() {
                    return Err(IrValidationError::InvalidDependency {
                        reason: "binding failure outcome is not input/constant-only",
                    });
                }
            }
        }
    }
    Ok(())
}
