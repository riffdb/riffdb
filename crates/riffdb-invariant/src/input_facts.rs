//! Proof-bearing derivation of command facts available before admission.

use std::collections::BTreeSet;
use std::fmt;

use riffdb_contract_ir::{CommandPlan, ExprId, KeySchema};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, ConflictKey, EntityKey, PartitionKey, PlanHash,
    encode_canonical_value,
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
    binding_plan_indices: Vec<u32>,
    binding_element_ordinals: Vec<Option<u16>>,
    root_validation_entity_keys: Vec<EntityKey>,
    root_validation_plan_indices: Vec<u32>,
    root_validation_element_ordinals: Vec<Option<u16>>,
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

    /// Borrows the checked plan-binding index corresponding to each concrete key.
    ///
    /// Ordinary commands contain the dense identity sequence. A collection
    /// command repeats only the compiler-declared template indices in submitted
    /// element order.
    #[must_use]
    pub fn binding_plan_indices(&self) -> &[u32] {
        &self.binding_plan_indices
    }

    /// Borrows the submitted element ordinal for each concrete binding key.
    ///
    /// `None` identifies a non-repeated binding. Ordinals are zero-based and
    /// bounded by the executable IR's 1,024-element ceiling.
    #[must_use]
    pub fn binding_element_ordinals(&self) -> &[Option<u16>] {
        &self.binding_element_ordinals
    }

    /// Borrows canonical entity keys in exact dense root-validation-read order.
    ///
    /// Grammar/IR v1 has no command range-read plan, so these two ordered key
    /// sets are the complete snapshot target derivation owned by this proof.
    #[must_use]
    pub fn root_validation_entity_keys(&self) -> &[EntityKey] {
        &self.root_validation_entity_keys
    }

    /// Borrows the checked root-validation plan index for each concrete key.
    #[must_use]
    pub fn root_validation_plan_indices(&self) -> &[u32] {
        &self.root_validation_plan_indices
    }

    /// Borrows the submitted element ordinal for each concrete root read.
    #[must_use]
    pub fn root_validation_element_ordinals(&self) -> &[Option<u16>] {
        &self.root_validation_element_ordinals
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

    let collection_elements = collection_elements(plan, &normalized_input)?;
    let (
        partition_key,
        declared_conflict_keys,
        binding_entity_keys,
        binding_plan_indices,
        binding_element_ordinals,
        root_validation_entity_keys,
        root_validation_plan_indices,
        root_validation_element_ordinals,
    ) = {
        let mut evaluator = ExpressionEvaluator::new(plan.expressions());
        let evaluate_partition = |evaluator: &mut ExpressionEvaluator<'_>, element| {
            let values = NormalizedInputValues {
                input: &normalized_input,
                element,
            };
            let partition_component = evaluator
                .batch(&values)
                .evaluate(plan.locality().partition_expression())?;
            plan.locality()
                .partition_schema()
                .encode_partition(&[partition_component])
                .map_err(|_| EvaluationError::Integrity)
        };
        let partition_key = match collection_elements {
            Some(elements) => {
                let first = elements.first().ok_or(EvaluationError::Integrity)?;
                let partition = evaluate_partition(&mut evaluator, Some(first))?;
                for element in &elements[1..] {
                    if evaluate_partition(&mut evaluator, Some(element))? != partition {
                        return Err(EvaluationError::Integrity);
                    }
                }
                partition
            }
            None => evaluate_partition(&mut evaluator, None)?,
        };

        let mut declared_conflict_keys = Vec::with_capacity(plan.locality().conflict_keys().len());
        let conflict_elements = collection_elements.map_or_else(
            || vec![None],
            |elements| elements.iter().map(Some).collect(),
        );
        for element in conflict_elements {
            let values = NormalizedInputValues {
                input: &normalized_input,
                element,
            };
            let mut evaluation = evaluator.batch(&values);
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
            for derivation in plan.unique_conflicts() {
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
        }

        let mut binding_entity_keys = Vec::new();
        let mut binding_plan_indices = Vec::new();
        let mut binding_element_ordinals = Vec::new();
        for (index, binding) in plan.bindings().iter().enumerate() {
            let repeated = plan.collection_expansion().is_some_and(|expansion| {
                let first = expansion.first_binding().get() as usize;
                (first..first + expansion.binding_count()).contains(&index)
            });
            let elements = if repeated {
                collection_elements
                    .ok_or(EvaluationError::Integrity)?
                    .iter()
                    .enumerate()
                    .map(|(ordinal, element)| (Some(ordinal as u16), Some(element)))
                    .collect::<Vec<_>>()
            } else {
                vec![(None, None)]
            };
            for (ordinal, element) in elements {
                let values = NormalizedInputValues {
                    input: &normalized_input,
                    element,
                };
                let mut evaluation = evaluator.batch(&values);
                binding_entity_keys.push(derive_entity_key(
                    binding.key_schema(),
                    binding.key_expressions(),
                    &mut evaluation,
                )?);
                binding_plan_indices.push(index as u32);
                binding_element_ordinals.push(ordinal);
            }
        }

        let mut root_validation_entity_keys = Vec::new();
        let mut root_validation_plan_indices = Vec::new();
        let mut root_validation_element_ordinals = Vec::new();
        for (index, read) in plan.root_validation_reads().iter().enumerate() {
            let repeated = read
                .key_expressions()
                .iter()
                .try_fold(false, |uses, expression| {
                    let dependencies = plan
                        .expressions()
                        .dependencies(*expression)
                        .map_err(|_| EvaluationError::Integrity)?;
                    Ok::<_, EvaluationError>(
                        uses || dependencies.uses_collection_element()
                            || !dependencies.collection_element_fields().is_empty(),
                    )
                })?;
            let elements = if repeated {
                collection_elements
                    .ok_or(EvaluationError::Integrity)?
                    .iter()
                    .enumerate()
                    .map(|(ordinal, element)| (Some(ordinal as u16), Some(element)))
                    .collect::<Vec<_>>()
            } else {
                vec![(None, None)]
            };
            for (ordinal, element) in elements {
                let values = NormalizedInputValues {
                    input: &normalized_input,
                    element,
                };
                let mut evaluation = evaluator.batch(&values);
                root_validation_entity_keys.push(derive_entity_key(
                    read.key_schema(),
                    read.key_expressions(),
                    &mut evaluation,
                )?);
                root_validation_plan_indices.push(index as u32);
                root_validation_element_ordinals.push(ordinal);
            }
        }

        (
            partition_key,
            declared_conflict_keys,
            binding_entity_keys,
            binding_plan_indices,
            binding_element_ordinals,
            root_validation_entity_keys,
            root_validation_plan_indices,
            root_validation_element_ordinals,
        )
    };

    Ok(InputDerivedCommandFacts {
        plan_hash: plan.plan_hash(),
        normalized_input,
        partition_key,
        declared_conflict_keys,
        binding_entity_keys,
        binding_plan_indices,
        binding_element_ordinals,
        root_validation_entity_keys,
        root_validation_plan_indices,
        root_validation_element_ordinals,
    })
}

fn collection_elements<'a>(
    plan: &CommandPlan,
    input: &'a CanonicalRecord,
) -> Result<Option<&'a [CanonicalValue]>, EvaluationError> {
    let Some(expansion) = plan.collection_expansion() else {
        return Ok(None);
    };
    let CanonicalValue::List(list) = input
        .fields()
        .binary_search_by_key(&expansion.input_field(), |(field, _)| *field)
        .ok()
        .map(|index| &input.fields()[index].1)
        .ok_or(EvaluationError::Integrity)?
    else {
        return Err(EvaluationError::Integrity);
    };
    if list.len() < expansion.minimum_elements() || list.len() > expansion.maximum_elements() {
        return Err(EvaluationError::Integrity);
    }
    let mut canonical = BTreeSet::new();
    for element in list.values() {
        expansion
            .element_type()
            .validate_value(element)
            .map_err(|_| EvaluationError::Integrity)?;
        let encoded = encode_canonical_value(element).map_err(|_| EvaluationError::Integrity)?;
        if !canonical.insert(encoded) {
            return Err(EvaluationError::Integrity);
        }
    }
    Ok(Some(list.values()))
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
    element: Option<&'input CanonicalValue>,
}

impl ExpressionValueSource for NormalizedInputValues<'_> {
    fn input_field(&self, field: riffdb_types::FieldId) -> Option<CanonicalValue> {
        self.input
            .fields()
            .binary_search_by_key(&field, |(candidate, _)| *candidate)
            .ok()
            .map(|index| self.input.fields()[index].1.clone())
    }

    fn collection_element(&self) -> Option<CanonicalValue> {
        self.element.cloned()
    }

    fn collection_element_field(&self, field: riffdb_types::FieldId) -> Option<CanonicalValue> {
        let CanonicalValue::Record(record) = self.element? else {
            return None;
        };
        record
            .fields()
            .binary_search_by_key(&field, |(candidate, _)| *candidate)
            .ok()
            .map(|index| record.fields()[index].1.clone())
    }
}
