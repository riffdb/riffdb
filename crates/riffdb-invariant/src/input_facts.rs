//! Proof-bearing derivation of command facts available before admission.

use std::fmt;

use riffdb_contract_ir::{CommandPlan, ExprId, KeySchema};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, ConflictKey, EntityKey, PartitionKey, PlanHash,
};

use crate::{EvaluationError, ExpressionEvaluator, ExpressionValueSource};

/// Move-only evidence derived from one exact checked plan and normalized input.
///
/// This value binds the complete normalized input to its plan hash and to every
/// input-computable locality and snapshot-target value produced by the shared
/// expression evaluator. Conflict keys remain in declared `LocalityPlan` order;
/// entity keys remain in their separate dense binding and root-validation-read
/// orders. Acquisition sorting and deduplication belong to the commit
/// coordinator.
///
/// Fields are private so callers cannot assemble a proof from independently
/// derived values:
///
/// ```compile_fail
/// use riffdb_invariant::InputDerivedCommandFacts;
///
/// let _ = InputDerivedCommandFacts {};
/// ```
///
/// The proof cannot be duplicated:
///
/// ```compile_fail
/// use riffdb_invariant::InputDerivedCommandFacts;
///
/// fn duplicate(facts: &InputDerivedCommandFacts) -> InputDerivedCommandFacts {
///     facts.clone()
/// }
/// ```
#[must_use = "input-derived facts must remain bound to command preparation"]
pub struct InputDerivedCommandFacts {
    plan_hash: PlanHash,
    normalized_input: CanonicalRecord,
    partition_key: PartitionKey,
    declared_conflict_keys: Vec<ConflictKey>,
    binding_entity_keys: Vec<EntityKey>,
    root_validation_entity_keys: Vec<EntityKey>,
}

impl InputDerivedCommandFacts {
    /// Checks the exact plan hash and full normalized input bound to this proof.
    ///
    /// This comparison performs no expression evaluation and exposes neither
    /// the retained input nor reconstructible proof fields.
    #[must_use]
    pub fn matches_command(&self, plan: &CommandPlan, normalized_input: &CanonicalRecord) -> bool {
        self.plan_hash == plan.plan_hash() && &self.normalized_input == normalized_input
    }

    /// Borrows the one derived aggregate partition key.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Borrows derived mutation conflict keys in exact declaration order.
    #[must_use]
    pub fn declared_conflict_keys(&self) -> &[ConflictKey] {
        &self.declared_conflict_keys
    }

    /// Borrows canonical entity keys in exact dense binding order.
    ///
    /// The exact matched plan supplies each corresponding entity type when the
    /// commit coordinator lowers these keys to storage-owned entity targets.
    #[must_use]
    pub fn binding_entity_keys(&self) -> &[EntityKey] {
        &self.binding_entity_keys
    }

    /// Borrows canonical entity keys in exact dense root-validation-read order.
    ///
    /// Grammar/IR v1 has no command range-read plan, so these two ordered key
    /// sets are the complete snapshot target derivation owned by this proof.
    #[must_use]
    pub fn root_validation_entity_keys(&self) -> &[EntityKey] {
        &self.root_validation_entity_keys
    }
}

impl fmt::Debug for InputDerivedCommandFacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InputDerivedCommandFacts([REDACTED])")
    }
}

/// Derives all pre-admission locality facts from one checked plan and input.
///
/// The normalized input is consumed and retained unchanged. The function first
/// checks its exact field-ID shape and declared value types, then evaluates the
/// partition, every conflict-key component, and every binding/root entity key
/// through the shared pure evaluator. Checked arithmetic is returned as
/// [`EvaluationError::Arithmetic`]; a plan, input, expression, or key-schema
/// inconsistency fails closed as [`EvaluationError::Integrity`]. No key is
/// sorted or deduplicated here.
pub fn derive_input_command_facts(
    plan: &CommandPlan,
    normalized_input: CanonicalRecord,
) -> Result<InputDerivedCommandFacts, EvaluationError> {
    validate_normalized_input(plan, &normalized_input)?;

    let (partition_key, declared_conflict_keys, binding_entity_keys, root_validation_entity_keys) = {
        let values = NormalizedInputValues {
            input: &normalized_input,
        };
        let mut evaluator = ExpressionEvaluator::new(plan.expressions());
        let mut evaluation = evaluator.batch(&values);

        let partition_component = evaluation.evaluate(plan.locality().partition_expression())?;
        let partition_key = plan
            .locality()
            .partition_schema()
            .encode_partition(&[partition_component])
            .map_err(|_| EvaluationError::Integrity)?;

        let mut declared_conflict_keys = Vec::with_capacity(plan.locality().conflict_keys().len());
        for derivation in plan.locality().conflict_keys() {
            let components = derivation
                .expressions()
                .iter()
                .map(|expression| evaluation.evaluate(*expression))
                .collect::<Result<Vec<_>, _>>()?;
            let key = derivation
                .schema()
                .encode_conflict(&components)
                .map_err(|_| EvaluationError::Integrity)?;
            declared_conflict_keys.push(key);
        }

        let binding_entity_keys = plan
            .bindings()
            .iter()
            .map(|binding| {
                derive_entity_key(
                    binding.key_schema(),
                    binding.key_expressions(),
                    &mut evaluation,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let root_validation_entity_keys = plan
            .root_validation_reads()
            .iter()
            .map(|read| {
                derive_entity_key(read.key_schema(), read.key_expressions(), &mut evaluation)
            })
            .collect::<Result<Vec<_>, _>>()?;

        (
            partition_key,
            declared_conflict_keys,
            binding_entity_keys,
            root_validation_entity_keys,
        )
    };

    Ok(InputDerivedCommandFacts {
        plan_hash: plan.plan_hash(),
        normalized_input,
        partition_key,
        declared_conflict_keys,
        binding_entity_keys,
        root_validation_entity_keys,
    })
}

fn derive_entity_key<Values: ExpressionValueSource + ?Sized>(
    key_schema: &KeySchema,
    key_expressions: &[ExprId],
    evaluation: &mut crate::EvaluationBatch<'_, '_, '_, Values>,
) -> Result<EntityKey, EvaluationError> {
    let components = key_expressions
        .iter()
        .map(|expression| evaluation.evaluate(*expression))
        .collect::<Result<Vec<_>, _>>()?;
    key_schema
        .encode_entity(&components)
        .map_err(|_| EvaluationError::Integrity)
}

fn validate_normalized_input(
    plan: &CommandPlan,
    normalized_input: &CanonicalRecord,
) -> Result<(), EvaluationError> {
    let declared = plan.input().record().fields();
    if declared.len() != normalized_input.fields().len() {
        return Err(EvaluationError::Integrity);
    }
    for (field, (actual_id, value)) in declared.iter().zip(normalized_input.fields()) {
        if field.id() != *actual_id || field.value_type().validate_value(value).is_err() {
            return Err(EvaluationError::Integrity);
        }
    }
    Ok(())
}

struct NormalizedInputValues<'input> {
    input: &'input CanonicalRecord,
}

impl ExpressionValueSource for NormalizedInputValues<'_> {
    fn input_field(&self, field: riffdb_types::FieldId) -> Option<CanonicalValue> {
        self.input
            .fields()
            .binary_search_by_key(&field, |(candidate, _)| *candidate)
            .ok()
            .map(|index| self.input.fields()[index].1.clone())
    }
}
