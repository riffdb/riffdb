//! The single owning CommandPlan construction body.
// req: OQ-040, PRJ-004
use super::*;

impl CommandPlan {
    // Sole existing constructor. The closed Source caller must preadmit all
    // argument/clone/derived-validator work before entering; no Source entry
    // is exposed by this factoring. Ordinary invocation is the fixed no-op sink.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_internal_with_admission<
        A: crate::bundle::source_decode_sink::BundleDecodeAdmissionV1,
    >(
        command_id: CommandId,
        contract_lineage: ContractLineage,
        name: impl Into<String>,
        contract_version: ContractVersion,
        input: CommandInputSchema,
        mut service_values: Vec<ServiceValueSchema>,
        mut outcomes: Vec<OutcomeSchema>,
        success_outcome: OutcomeId,
        idempotency_input: Option<FieldId>,
        expressions: ExpressionArena,
        mut bindings: Vec<BindingPlan>,
        mut root_validation_reads: Vec<RootValidationReadPlan>,
        locality: LocalityPlan,
        mut commit_checks: Vec<CommitCheckPlan>,
        instructions: Vec<Instruction>,
        mut collection_expansion: Option<CollectionExpansionPlanV1>,
        decisions: Vec<CommandDecisionPlanV1>,
        mut secret_reveals: Vec<SecretRevealSpecV1>,
        secret_reveal_validation: SecretRevealValidation,
        invocation_class: CommandInvocationClass,
        execution_class: ExecutionClass,
        contract_schema: &SchemaIr,
        admission: &mut A,
    ) -> Result<Self, A::Error> {
        // The closed ordinary sink is a no-op. Source admission occurs before
        // the first union/deep clone or derived validation container. Declared
        // first, this guard outlives every subsequently constructed temporary
        // and the final hash on success and all partial-error paths.
        let _constructor_work = admission.command_constructor_work(
            &expressions,
            &bindings,
            &outcomes,
            &root_validation_reads,
            &locality,
            &commit_checks,
            &instructions,
            &decisions,
            contract_schema,
        )?;
        let name = name.into();
        validate_source_name(&name, "command")?;
        if input.command_id() != command_id {
            return Err((IrValidationError::InvalidReference {
                kind: "command input",
            })
            .into());
        }
        for field in input.record().fields() {
            crate::schema::validate_command_input_field_type(
                field.value_type(),
                contract_schema,
                collection_expansion
                    .as_ref()
                    .is_some_and(|expansion| expansion.input_field() == field.id()),
            )?;
        }
        checked_len(
            "command service values",
            service_values.len(),
            MAX_COMMAND_ITEMS,
        )?;
        service_values.sort_unstable_by_key(|value| value.field.id());
        if service_values.windows(2).any(|pair| {
            pair[0].field.id() == pair[1].field.id() || pair[0].field.name() == pair[1].field.name()
        }) {
            return Err((IrValidationError::NonCanonicalOrder {
                kind: "command service values",
            })
            .into());
        }
        checked_len("command outcomes", outcomes.len(), MAX_COMMAND_ITEMS)?;
        checked_len("command bindings", bindings.len(), MAX_COMMAND_ITEMS)?;
        checked_len(
            "command commit checks",
            commit_checks.len(),
            MAX_COMMAND_ITEMS,
        )?;
        checked_len(
            "command instructions",
            instructions.len(),
            MAX_COMMAND_ITEMS,
        )?;
        checked_len("command decisions", decisions.len(), MAX_COMMAND_ITEMS)?;
        let mut union_instructions = instructions.clone();
        union_instructions.extend(decisions.iter().flat_map(|decision| {
            decision
                .when_arms()
                .iter()
                .map(CommandDecisionArmV1::action)
                .chain(std::iter::once(decision.else_action()))
                .flat_map(CommandDecisionActionV1::instructions)
                .cloned()
        }));
        checked_len(
            "command union instructions",
            union_instructions.len(),
            MAX_COMMAND_ITEMS,
        )?;
        if outcomes.is_empty() || bindings.is_empty() || instructions.is_empty() {
            return Err((IrValidationError::Empty {
                kind: "command plan",
            })
            .into());
        }
        let (maximum_request_bytes, maximum_mutation_instances) =
            if let Some(expansion) = &collection_expansion {
                let maximum_mutation_instances = validate_collection_expansion(
                    expansion,
                    &input,
                    &bindings,
                    &instructions,
                    contract_schema,
                )?;
                let proof = validate_collection_graph_bytes(
                    expansion,
                    &name,
                    &input,
                    &expressions,
                    &bindings,
                    &instructions,
                    &decisions,
                    contract_schema,
                )?;
                if let Some(coefficient) = proof.maximum_copy_coefficient {
                    collection_expansion
                        .as_mut()
                        .expect("validated collection expansion")
                        .set_maximum_copy_coefficient(coefficient)?;
                }
                (proof.maximum_request_bytes, maximum_mutation_instances)
            } else {
                let deletes = bindings
                    .iter()
                    .filter(|binding| binding.mode() == BindingMode::Delete)
                    .collect::<Vec<_>>();
                if deletes.len() > 1 {
                    return Err((IrValidationError::LimitExceeded {
                        kind: "ordinary delete bindings",
                        actual: deletes.len(),
                        maximum: 1,
                    })
                    .into());
                }
                if let Some(binding) = deletes.first() {
                    let policy = contract_schema.delete_policy(binding.entity_type()).ok_or(
                        IrValidationError::InvalidDependency {
                            reason: "ordinary delete lacks one checked structural deletion policy",
                        },
                    )?;
                    if !matches!(policy.mode(), crate::DeletePolicyModeV1::NoInbound)
                        || binding.restriction_failure().is_some()
                        || binding.cascade_failure().is_some()
                    {
                        return Err((IrValidationError::InvalidDependency {
                            reason: "ordinary delete requires the no_inbound policy",
                        })
                        .into());
                    }
                }
                (
                    maximum_command_request_bytes(&name, &input, contract_schema)?,
                    0,
                )
            };
        checked_len(
            "command public request bytes",
            maximum_request_bytes,
            MAX_ATOMIC_COMMAND_REQUEST_BYTES_V2,
        )?;
        for outcome in &outcomes {
            for field in outcome.payload().fields() {
                crate::schema::validate_payload_field_type(field.value_type(), contract_schema)?;
            }
        }
        outcomes.sort_unstable_by_key(OutcomeSchema::id);
        if outcomes.windows(2).any(|pair| pair[0].id == pair[1].id)
            || outcomes.iter().all(|outcome| outcome.id != success_outcome)
        {
            return Err((IrValidationError::NonCanonicalOrder {
                kind: "command outcomes",
            })
            .into());
        }
        let mut outcome_names = BTreeSet::new();
        if outcomes
            .iter()
            .any(|outcome| !outcome_names.insert(outcome.name.as_str()))
        {
            return Err((IrValidationError::InvalidName {
                kind: "duplicate outcome",
            })
            .into());
        }
        bindings.sort_unstable_by_key(BindingPlan::id);
        for (index, binding) in bindings.iter().enumerate() {
            if binding.id.get() as usize != index {
                return Err((IrValidationError::NonCanonicalOrder {
                    kind: "dense bindings",
                })
                .into());
            }
        }
        validate_command_decisions(
            &expressions,
            &decisions,
            &bindings,
            &outcomes,
            success_outcome,
            &instructions,
            collection_expansion.as_ref(),
            contract_schema,
        )?;
        checked_len(
            "command root-validation reads",
            root_validation_reads.len(),
            MAX_COMMAND_ITEMS,
        )?;
        root_validation_reads.sort_unstable_by_key(RootValidationReadPlan::id);
        for (index, read) in root_validation_reads.iter().enumerate() {
            if read.id.get() as usize != index {
                return Err((IrValidationError::NonCanonicalOrder {
                    kind: "dense root-validation reads",
                })
                .into());
            }
        }
        commit_checks.sort_unstable_by(|left, right| {
            left.invariant_id
                .cmp(&right.invariant_id)
                .then_with(|| left.source_bindings.cmp(&right.source_bindings))
                .then_with(|| left.root_validation_reads.cmp(&right.root_validation_reads))
        });
        if commit_checks.windows(2).any(|pair| {
            pair[0].invariant_id == pair[1].invariant_id
                && pair[0].source_bindings == pair[1].source_bindings
                && pair[0].root_validation_reads == pair[1].root_validation_reads
        }) {
            return Err((IrValidationError::NonCanonicalOrder {
                kind: "commit checks",
            })
            .into());
        }

        validate_declared_constructions(
            &expressions,
            &bindings,
            &union_instructions,
            &outcomes,
            success_outcome,
            contract_schema,
        )?;

        validate_expression_contexts(
            &expressions,
            &input,
            &service_values,
            &bindings,
            &root_validation_reads,
            collection_expansion.as_ref(),
            contract_schema,
        )?;
        validate_collection_expression_uses(
            &expressions,
            collection_expansion.as_ref(),
            &bindings,
            &root_validation_reads,
            &locality,
            &commit_checks,
            &instructions,
        )?;
        contract_schema.validate_expression_enum_constants(&expressions)?;
        validate_indexed_selection(
            &expressions,
            &bindings,
            collection_expansion.as_ref(),
            &decisions,
            contract_schema,
        )?;
        validate_binding_plans(&expressions, &bindings, &locality, contract_schema)?;
        if collection_expansion.is_none() {
            validate_ordinary_delete_aliases(&expressions, &bindings)?;
        }
        validate_root_validation_reads(
            &expressions,
            &bindings,
            &root_validation_reads,
            contract_schema,
        )?;
        validate_locality(&expressions, &bindings, &locality, contract_schema)?;
        validate_commit_checks(
            &expressions,
            &commit_checks,
            &bindings,
            &root_validation_reads,
            contract_schema,
        )?;
        let no_common_completion = BTreeSet::new();
        validate_instruction_stream(
            &expressions,
            &instructions,
            &bindings,
            &outcomes,
            success_outcome,
            contract_schema,
            (!decisions.is_empty()).then_some(&no_common_completion),
        )?;
        validate_root_validation_expression_uses(
            &expressions,
            &bindings,
            &locality,
            &instructions,
            &decisions,
        )?;
        validate_read_dependencies(
            &expressions,
            &bindings,
            &root_validation_reads,
            &commit_checks,
            &instructions,
            &decisions,
        )?;
        validate_command_expression_reachability(
            &expressions,
            &bindings,
            &root_validation_reads,
            &locality,
            &commit_checks,
            &instructions,
            &decisions,
        )?;
        validate_idempotency(
            invocation_class,
            execution_class,
            idempotency_input,
            &input,
            &expressions,
            &bindings,
            &root_validation_reads,
            &locality,
            &commit_checks,
            &instructions,
        )?;
        if invocation_class == CommandInvocationClass::Reimport {
            validate_reimport_shape(
                &service_values,
                &bindings,
                &instructions,
                collection_expansion.as_ref(),
            )?;
        }
        if execution_class == ExecutionClass::IdempotentMutation
            && !bindings.iter().any(|binding| {
                matches!(
                    binding.mode,
                    BindingMode::Mutate
                        | BindingMode::Create
                        | BindingMode::InitOrMutate
                        | BindingMode::ObserveOrInitialize
                        | BindingMode::Delete
                )
            })
        {
            return Err((IrValidationError::InvalidDependency {
                reason: "mutating command has no mutable binding",
            })
            .into());
        }
        if execution_class == ExecutionClass::ReadOnly
            && (bindings
                .iter()
                .any(|binding| binding.mode != BindingMode::Read)
                || instructions.iter().any(|instruction| {
                    matches!(
                        instruction,
                        Instruction::SetField { .. }
                            | Instruction::SetEmbedding { .. }
                            | Instruction::WorkflowTransition { .. }
                            | Instruction::EmitEvent(_)
                    )
                }))
        {
            return Err((IrValidationError::InvalidInstructionStream {
                reason: "read-only command contains a mutation or event",
            })
            .into());
        }

        let delete_checks = derive_delete_checks(&bindings, contract_schema)?;
        validate_worst_case_index_derivation(
            &expressions,
            &bindings,
            &root_validation_reads,
            &delete_checks,
            &instructions,
            &decisions,
            collection_expansion.as_ref(),
            locality.partition_schema().maximum_encoded_bytes(),
            contract_schema,
        )?;
        checked_len(
            "command secret reveals",
            secret_reveals.len(),
            MAX_COMMAND_ITEMS,
        )?;
        secret_reveals.sort_unstable();
        if secret_reveals.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err((IrValidationError::NonCanonicalOrder {
                kind: "duplicate command secret reveal",
            })
            .into());
        }
        if secret_reveal_validation == SecretRevealValidation::Exact {
            validate_secret_reveals(
                &expressions,
                &bindings,
                &instructions,
                &decisions,
                &secret_reveals,
                contract_schema,
            )?;
        }
        let relationship_checks = derive_relationship_checks(
            &expressions,
            &bindings,
            &union_instructions,
            contract_schema,
        )?;
        let unique_conflicts = derive_unique_conflicts(
            &expressions,
            &bindings,
            &union_instructions,
            contract_schema,
        )?;
        if let Some(expansion) = collection_expansion.as_ref() {
            validate_collection_conflict_instances(
                expansion,
                &expressions,
                &locality,
                &unique_conflicts,
            )?;
        }
        checked_len(
            "all command conflict derivations",
            locality
                .conflict_keys()
                .len()
                .checked_add(unique_conflicts.len())
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "all command conflict derivations",
                })?,
            MAX_COMMAND_CONFLICT_KEYS_V1,
        )?;

        let mut plan = Self {
            command_id,
            name,
            contract_version,
            input,
            maximum_request_bytes,
            maximum_mutation_instances,
            service_values,
            outcomes,
            success_outcome,
            idempotency_input,
            expressions,
            bindings,
            relationship_checks,
            delete_checks,
            unique_conflicts,
            root_validation_reads,
            locality,
            commit_checks,
            instructions,
            decisions,
            collection_expansion,
            secret_reveals,
            invocation_class,
            execution_class,
            retry_policy: RetryPolicy::BoundedFullReevaluation,
            required_capability: CapabilityRequirement::InvokeCommand {
                lineage: contract_lineage,
                command_id,
            },
            plan_hash: PlanHash::from_bytes([0; 32]),
            write_proofs: None,
            lifecycle: riffdb_types::OwnedCapacityLifecycleV1::default(),
        };
        plan.plan_hash = admission.command_hash(&plan, contract_schema)?;
        Ok(plan)
    }
}

// Numerical construction work over the existing typed instructions. These
// helpers do not decode or validate another command representation and do not
// invoke the allocating WorkflowLeaseOperation expressions()/outcomes() getters.
fn source_outcome_clone_heap_bound(
    value: &OutcomeConstruction,
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    crate::bundle::source_decode_sink::source_fixed_vector_bound::<FieldExpression>(
        value.payload.fields.len(),
    )
}

fn source_instruction_clone_heap_bound(
    value: &Instruction,
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::source_fixed_vector_bound;
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let outcomes = |values: &[&OutcomeConstruction]| {
        values.iter().try_fold(0u64, |total, value| {
            add(total, source_outcome_clone_heap_bound(value)?)
        })
    };
    match value {
        Instruction::Require { reject, .. } | Instruction::Return(reject) => {
            source_outcome_clone_heap_bound(reject)
        }
        Instruction::SetField { .. } | Instruction::SetEmbedding { .. } => Ok(0),
        Instruction::WorkflowTransition {
            source_states,
            stale,
            illegal,
            ..
        } => add(
            source_fixed_vector_bound::<EnumVariantId>(source_states.len())?,
            outcomes(&[stale, illegal])?,
        ),
        Instruction::WorkflowLease { operation, .. } => match operation {
            WorkflowLeaseOperation::Claim {
                stale,
                unavailable,
                invalid,
                exhausted,
                ..
            } => outcomes(&[stale, unavailable, invalid, exhausted]),
            WorkflowLeaseOperation::Renew {
                stale,
                invalid,
                expired,
                exhausted,
                ..
            } => outcomes(&[stale, invalid, expired, exhausted]),
            WorkflowLeaseOperation::Release { stale, invalid, .. } => outcomes(&[stale, invalid]),
            WorkflowLeaseOperation::Expire { stale, active, .. } => outcomes(&[stale, active]),
            WorkflowLeaseOperation::Fence {
                stale,
                invalid,
                expired,
                ..
            } => outcomes(&[stale, invalid, expired]),
        },
        Instruction::EmitEvent(event) => {
            source_fixed_vector_bound::<FieldExpression>(event.payload.fields.len())
        }
    }
}

// The complete union Vec and every nested cloned field/state Vec survive from
// the original clone/extend through ALL validators and the final plan hash.
// Growing-vector accounting includes the actual old/new reallocation overlap;
// the owned child buffers move across reallocation and are counted once.
fn source_union_instruction_clone_bound(
    common: &[Instruction],
    decisions: &[CommandDecisionPlanV1],
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::source_growing_vector_bound;
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let mut count = common.len();
    let mut children = common.iter().try_fold(0u64, |total, value| {
        total
            .checked_add(source_instruction_clone_heap_bound(value)?)
            .ok_or(BudgetUnavailable)
    })?;
    for decision in decisions {
        for action in decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
        {
            let values = action.instructions(); // Borrowed slice; no allocation.
            count = count.checked_add(values.len()).ok_or(BudgetUnavailable)?;
            for value in values {
                children = children
                    .checked_add(source_instruction_clone_heap_bound(value)?)
                    .ok_or(BudgetUnavailable)?;
            }
        }
    }
    children
        .checked_add(source_growing_vector_bound::<Instruction>(count)?)
        .ok_or(BudgetUnavailable)
}

// validate_command_decisions keeps a separate success clone while one Apply
// stream clone plus appended success is alive. Only one arm stream is alive;
// its workspace is sequential across arms but overlaps the full union above.
fn source_decision_stream_clone_bound(
    common: &[Instruction],
    decisions: &[CommandDecisionPlanV1],
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::source_growing_vector_bound;
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let success = match common.last() {
        Some(Instruction::Return(value)) => source_outcome_clone_heap_bound(value)?,
        _ => 0, // The sole original validator refuses before cloning success.
    };
    let mut maximum = 0u64;
    for decision in decisions {
        for action in decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
        {
            let CommandDecisionActionV1::Apply { instructions, .. } = action else {
                continue;
            };
            let count = instructions.len().checked_add(1).ok_or(BudgetUnavailable)?;
            let mut arm = source_growing_vector_bound::<Instruction>(count)?;
            for instruction in instructions {
                arm = arm
                    .checked_add(source_instruction_clone_heap_bound(instruction)?)
                    .ok_or(BudgetUnavailable)?;
            }
            // The success payload appears in both the outer success clone and
            // the appended terminal instruction. Price both exact clones.
            arm = arm
                .checked_add(success)
                .and_then(|v| v.checked_add(success))
                .ok_or(BudgetUnavailable)?;
            maximum = maximum.max(arm);
        }
    }
    // Even a graph with no Apply can have the outer success clone alive.
    Ok(maximum.max(success))
}

// Retained compiler-derived graphs are additional to all decoded command
// inputs. Count every schema family that could produce an output, including
// malformed commands that fail only after accumulating earlier outputs. This
// borrows canonical semantic values; it does not reimplement the validators.
fn source_derived_command_outputs_bound(
    bindings: &[BindingPlan],
    schema: &SchemaIr,
) -> Result<(u64, u64), riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{
        source_fixed_vector_bound, source_growing_vector_bound,
    };
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let mut retained = 0u64;
    let mut deletes = 0usize;
    let mut relationships = 0usize;
    let mut uniques = 0usize;
    let mut maximum_mode_clone = 0u64;
    for binding in bindings {
        if binding.mode != BindingMode::Read
            && let Some(policy) = schema.delete_policy(binding.entity_type)
        {
            let cloned_mode = policy.source_mode_clone_heap_bound()?;
            // validate_collection_expansion::binding_weight clones a mode
            // for EVERY non-Read binding, even when no deletion is declared.
            // Later DeleteCheckPlanV1::mode clones the same spec/name graph.
            maximum_mode_clone = maximum_mode_clone.max(cloned_mode);
            if binding.mode == BindingMode::Delete {
                retained = add(retained, cloned_mode)?;
            }
        }
        if binding.mode == BindingMode::Delete {
            deletes = deletes.checked_add(1).ok_or(BudgetUnavailable)?;
        }
        if !matches!(
            binding.mode,
            BindingMode::Create
                | BindingMode::Mutate
                | BindingMode::InitOrMutate
                | BindingMode::ObserveOrInitialize
        ) {
            continue;
        }
        for relationship in schema
            .relationships()
            .iter()
            .filter(|value| value.source_entity() == binding.entity_type)
        {
            relationships = relationships.checked_add(1).ok_or(BudgetUnavailable)?;
            retained = add(
                retained,
                source_fixed_vector_bound::<u8>(relationship.name().len())?,
            )?;
        }
        let Some(entity) = schema.entity(binding.entity_type) else {
            continue;
        };
        for unique in schema
            .unique_keys()
            .iter()
            .filter(|value| value.source_entity() == binding.entity_type)
        {
            // Missing indexes are refused by the sole original derivation
            // before component/name ownership can be constructed.
            let Some(index) = entity
                .indexes()
                .iter()
                .find(|value| value.id() == unique.index_id())
            else {
                continue;
            };
            uniques = uniques.checked_add(1).ok_or(BudgetUnavailable)?;
            retained = add(
                retained,
                source_fixed_vector_bound::<u8>(unique.name().len())?,
            )?;
            // collect::<Option<Vec<_>>>() can grow geometrically; unlike an
            // exact-capacity clone, pricing only fields.len() is insufficient.
            retained = add(
                retained,
                source_growing_vector_bound::<ExprId>(unique.fields().len())?,
            )?;
            let components = index.key_schema().components();
            retained = add(
                retained,
                source_fixed_vector_bound::<crate::KeyComponentSchema>(components.len())?,
            )?;
            for component in components {
                // KeyComponentSchema's owning constructor admits only closed
                // authoritative scalars, so ValueType here owns no nested Box.
                // KeySchema::new receives components only, not the index's
                // separately boxed entity-key suffix.
                retained = add(
                    retained,
                    source_fixed_vector_bound::<EnumVariantId>(component.enum_variants().len())?,
                )?;
            }
        }
    }
    retained = add(
        retained,
        source_growing_vector_bound::<DeleteCheckPlanV1>(deletes)?,
    )?;
    retained = add(
        retained,
        source_growing_vector_bound::<RelationshipCheckPlan>(relationships)?,
    )?;
    retained = add(
        retained,
        source_growing_vector_bound::<UniqueConflictPlan>(uniques)?,
    )?;
    Ok((retained, maximum_mode_clone))
}

// Exact number of expression roots contributed by one existing instruction.
// Static borrowed arrays replace the allocating operation.outcomes()/expressions()
// getters ONLY in numerical measurement; the original validators are unchanged.
fn source_instruction_reference_count(
    instruction: &Instruction,
) -> Result<usize, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let fields = |values: &[&OutcomeConstruction]| {
        values.iter().try_fold(0usize, |total, value| {
            total
                .checked_add(value.payload.fields.len())
                .ok_or(BudgetUnavailable)
        })
    };
    match instruction {
        Instruction::Require { reject, .. } => reject
            .payload
            .fields
            .len()
            .checked_add(1)
            .ok_or(BudgetUnavailable),
        Instruction::SetField { .. } => Ok(1),
        Instruction::SetEmbedding { .. } => Ok(3),
        Instruction::WorkflowTransition { stale, illegal, .. } => fields(&[stale, illegal])?
            .checked_add(1)
            .ok_or(BudgetUnavailable),
        Instruction::WorkflowLease { operation, .. } => {
            let (expressions, payloads) = match operation {
                WorkflowLeaseOperation::Claim {
                    stale,
                    unavailable,
                    invalid,
                    exhausted,
                    ..
                } => (3usize, fields(&[stale, unavailable, invalid, exhausted])?),
                WorkflowLeaseOperation::Renew {
                    stale,
                    invalid,
                    expired,
                    exhausted,
                    ..
                } => (4, fields(&[stale, invalid, expired, exhausted])?),
                WorkflowLeaseOperation::Release { stale, invalid, .. } => {
                    (3, fields(&[stale, invalid])?)
                }
                WorkflowLeaseOperation::Expire { stale, active, .. } => {
                    (1, fields(&[stale, active])?)
                }
                WorkflowLeaseOperation::Fence {
                    stale,
                    invalid,
                    expired,
                    ..
                } => (3, fields(&[stale, invalid, expired])?),
            };
            expressions.checked_add(payloads).ok_or(BudgetUnavailable)
        }
        Instruction::EmitEvent(event) => Ok(event.payload.fields.len()),
        Instruction::Return(outcome) => Ok(outcome.payload.fields.len()),
    }
}

fn source_command_reachability_parent_work_bound(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    reads: &[RootValidationReadPlan],
    locality: &LocalityPlan,
    checks: &[CommitCheckPlan],
    instructions: &[Instruction],
    decisions: &[CommandDecisionPlanV1],
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{
        source_fixed_vector_bound, source_growing_vector_bound,
    };
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let mut roots = 1usize; // The actual locality partition expression.
    for binding in bindings {
        roots = roots
            .checked_add(binding.key_expressions.len())
            .and_then(|n| n.checked_add(binding.initializer.len()))
            .ok_or(BudgetUnavailable)?;
        if let Some(selection) = binding.indexed_selection() {
            roots = roots
                .checked_add(selection.prefix_expressions().len())
                .ok_or(BudgetUnavailable)?;
        }
        if let Some(failure) = &binding.failure {
            roots = roots
                .checked_add(failure.payload.fields.len())
                .ok_or(BudgetUnavailable)?;
        }
    }
    for read in reads {
        roots = roots
            .checked_add(read.key_expressions.len())
            .ok_or(BudgetUnavailable)?;
    }
    for conflict in &locality.conflict_keys {
        roots = roots
            .checked_add(conflict.expressions.len())
            .ok_or(BudgetUnavailable)?;
    }
    roots = roots.checked_add(checks.len()).ok_or(BudgetUnavailable)?;
    for decision in decisions {
        roots = roots
            .checked_add(decision.when_arms.len())
            .ok_or(BudgetUnavailable)?;
        for action in decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
        {
            if let Some(reject) = action.rejection() {
                roots = roots
                    .checked_add(reject.payload.fields.len())
                    .ok_or(BudgetUnavailable)?;
            }
            for instruction in action.instructions() {
                roots = roots
                    .checked_add(source_instruction_reference_count(instruction)?)
                    .ok_or(BudgetUnavailable)?;
            }
        }
    }
    for instruction in instructions {
        roots = roots
            .checked_add(source_instruction_reference_count(instruction)?)
            .ok_or(BudgetUnavailable)?;
    }
    // validate_reachable_from retains the roots vector, a separate roots copy
    // used as pending, and its visited bool vector. Each actual distinct node
    // can push at most two children before its visited flag prevents expansion.
    let pending = arena
        .len()
        .checked_mul(2)
        .and_then(|n| n.checked_add(roots))
        .ok_or(BudgetUnavailable)?;
    source_growing_vector_bound::<ExprId>(roots)?
        .checked_add(source_growing_vector_bound::<ExprId>(pending)?)
        .and_then(|bytes| bytes.checked_add(source_fixed_vector_bound::<bool>(arena.len()).ok()?))
        .ok_or(BudgetUnavailable)
}

fn source_command_read_dependency_parent_work_bound(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    reads: &[RootValidationReadPlan],
    instructions: &[Instruction],
    decisions: &[CommandDecisionPlanV1],
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{source_fixed_vector_bound, source_tree_layout_bound};
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let mut bytes = add(
        source_fixed_vector_bound::<BTreeSet<FieldId>>(bindings.len())?,
        source_fixed_vector_bound::<BTreeSet<FieldId>>(reads.len())?,
    )?;
    bytes = add(bytes, source_fixed_vector_bound::<bool>(bindings.len())?)?;
    let mut maximum_equality_set = 0usize;
    for binding in bindings {
        let mut fields = arena
            .nodes()
            .iter()
            .filter(|node| {
                matches!(node.kind(),
            ExpressionKind::BoundField { binding: source, .. } if *source == binding.id)
            })
            .count();
        // These are the only additional non-expression field insertions in the
        // sole validator. Duplicate entries merely make this bound conservative.
        for instruction in instructions
            .iter()
            .chain(decisions.iter().flat_map(|decision| {
                decision
                    .when_arms
                    .iter()
                    .map(|arm| &arm.action)
                    .chain(std::iter::once(&decision.else_action))
                    .flat_map(CommandDecisionActionV1::instructions)
            }))
        {
            let added = match instruction {
                Instruction::WorkflowTransition {
                    binding: source, ..
                } if *source == binding.id => 1,
                Instruction::WorkflowLease {
                    binding: source,
                    fields,
                    ..
                } if *source == binding.id => 3 + usize::from(fields.attempt_field.is_some()),
                _ => 0,
            };
            fields = fields.checked_add(added).ok_or(BudgetUnavailable)?;
        }
        bytes = add(bytes, source_tree_layout_bound::<FieldId, ()>(fields)?)?;
        maximum_equality_set = maximum_equality_set.max(binding.accessed_fields.len());
    }
    for read in reads {
        let fields = arena
            .nodes()
            .iter()
            .filter(|node| {
                matches!(node.kind(),
            ExpressionKind::RootValidationField { read: source, .. } if *source == read.id)
            })
            .count();
        bytes = add(bytes, source_tree_layout_bound::<FieldId, ()>(fields)?)?;
        maximum_equality_set = maximum_equality_set.max(read.accessed_fields.len());
    }
    // Final equality reconstruction is sequential per binding/read but all
    // accumulated sets above remain alive. Dependency result work is the shared
    // leaf allowance, added separately by the complete constructor planner.
    add(
        bytes,
        source_tree_layout_bound::<FieldId, ()>(maximum_equality_set)?,
    )
}

// Parent buffers of instruction validation, including one decision arm's
// appended success. All common/arm effects are counted as a conservative
// superset; no synthetic Instruction or temporary union is materialized here.
fn source_instruction_stream_parent_work_bound(
    bindings: &[BindingPlan],
    common: &[Instruction],
    decisions: &[CommandDecisionPlanV1],
    schema: &SchemaIr,
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{
        source_fixed_vector_bound, source_growing_vector_bound, source_tree_layout_bound,
    };
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let mut bytes = source_fixed_vector_bound::<BTreeSet<FieldId>>(bindings.len())?;
    let mut maximum_states = 0usize;
    let mut all_assigned = 0usize;
    for binding in bindings {
        let mut fields = schema
            .entity(binding.entity_type)
            .map_or(0, |entity| entity.record().fields().len())
            .checked_add(binding.initializer.len())
            .ok_or(BudgetUnavailable)?;
        for instruction in common.iter().chain(decisions.iter().flat_map(|decision| {
            decision
                .when_arms
                .iter()
                .map(|arm| &arm.action)
                .chain(std::iter::once(&decision.else_action))
                .flat_map(CommandDecisionActionV1::instructions)
        })) {
            let writes = match instruction {
                Instruction::SetField {
                    binding: target, ..
                }
                | Instruction::SetEmbedding {
                    binding: target, ..
                } if *target == binding.id => 1,
                Instruction::WorkflowTransition {
                    binding: target, ..
                } if *target == binding.id => 1,
                Instruction::WorkflowLease {
                    binding: target,
                    fields,
                    ..
                } if *target == binding.id => 3 + usize::from(fields.attempt_field.is_some()),
                _ => 0,
            };
            fields = fields.checked_add(writes).ok_or(BudgetUnavailable)?;
        }
        bytes = add(bytes, source_tree_layout_bound::<FieldId, ()>(fields)?)?;
    }
    for instruction in common.iter().chain(decisions.iter().flat_map(|decision| {
        decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
            .flat_map(CommandDecisionActionV1::instructions)
    })) {
        let writes = match instruction {
            Instruction::SetField { .. } | Instruction::SetEmbedding { .. } => 1,
            Instruction::WorkflowTransition { source_states, .. } => {
                maximum_states = maximum_states.max(source_states.len());
                1
            }
            Instruction::WorkflowLease { fields, .. } => {
                3 + usize::from(fields.attempt_field.is_some())
            }
            _ => 0,
        };
        all_assigned = all_assigned.checked_add(writes).ok_or(BudgetUnavailable)?;
    }
    bytes = add(
        bytes,
        source_tree_layout_bound::<(BindingId, FieldId), ()>(all_assigned)?,
    )?;
    bytes = add(
        bytes,
        source_tree_layout_bound::<EnumVariantId, ()>(maximum_states)?,
    )?;
    // A lease has at most four named fields/outcomes/expressions. The mutable
    // lease_fields Vec can grow from three to four; written_fields is a clone
    // or fixed vec. The outcome-reference Vec survives its outcome-ID set and
    // nested initialized-read checks. Price these simultaneously, not by max.
    bytes = add(bytes, source_growing_vector_bound::<FieldId>(4)?)?;
    bytes = add(bytes, source_fixed_vector_bound::<FieldId>(4)?)?;
    bytes = add(bytes, source_fixed_vector_bound::<&OutcomeConstruction>(4)?)?;
    bytes = add(bytes, source_fixed_vector_bound::<ExprId>(4)?)?;
    add(bytes, source_tree_layout_bound::<OutcomeId, ()>(4)?)
}

fn source_decision_validation_parent_work_bound(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    common: &[Instruction],
    decisions: &[CommandDecisionPlanV1],
    schema: &SchemaIr,
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{
        source_growing_vector_bound, source_tree_layout_bound,
    };
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let mut bytes = source_tree_layout_bound::<BindingId, ()>(bindings.len())?; // deferred
    bytes = add(
        bytes,
        source_tree_layout_bound::<BindingId, ()>(decisions.len())?,
    )?; // consumed
    bytes = add(
        bytes,
        source_tree_layout_bound::<BindingId, BindingId>(bindings.len())?,
    )?; // branch_owner
    bytes = add(
        bytes,
        source_tree_layout_bound::<BindingId, ()>(bindings.len())?,
    )?; // all_branch_bindings
    bytes = add(
        bytes,
        source_tree_layout_bound::<BindingId, ()>(
            bindings.len().checked_add(1).ok_or(BudgetUnavailable)?,
        )?,
    )?;
    // allowed_locals moves into completion; it is ONE tree, with the current
    // decision binding appended. Do not invent a second clone of that set.
    bytes = add(
        bytes,
        source_decision_stream_clone_bound(common, decisions)?,
    )?;
    bytes = add(
        bytes,
        source_instruction_stream_parent_work_bound(bindings, common, decisions, schema)?,
    )?;
    let mut maximum_roots = 0usize;
    for instruction in common.iter().chain(decisions.iter().flat_map(|decision| {
        decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
            .flat_map(CommandDecisionActionV1::instructions)
    })) {
        maximum_roots = maximum_roots.max(source_instruction_reference_count(instruction)?);
    }
    // instruction_binding_dependencies retains its root vector/IntoIter while
    // one dependency result is built, and grows a result binding set. Its
    // explicit target can add one member beyond the actual arena-node count.
    bytes = add(bytes, source_growing_vector_bound::<ExprId>(maximum_roots)?)?;
    add(
        bytes,
        source_tree_layout_bound::<BindingId, ()>(
            arena.len().checked_add(1).ok_or(BudgetUnavailable)?,
        )?,
    )
}

fn source_coverage_validation_parent_work_bound(
    bindings: &[BindingPlan],
    reads: &[RootValidationReadPlan],
    schema: &SchemaIr,
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{
        source_fixed_vector_bound, source_growing_vector_bound, source_tree_layout_bound,
    };
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let mut root = source_tree_layout_bound::<AggregateTypeId, ()>(bindings.len())?;
    root = add(
        root,
        source_growing_vector_bound::<(BindingId, Vec<ExprId>, EntityTypeId)>(bindings.len())?,
    )?;
    let mut maximum_prefix = 0usize;
    let mut expected = 0usize;
    for binding in bindings {
        maximum_prefix = maximum_prefix.max(binding.key_expressions.len());
        // Each validated entity has one aggregate owner. The root-read pass
        // creates one prefix per binding at most; grouped prefixes die instead
        // of being retained. Summing all actual key lengths covers retained
        // prior prefixes and the current ungrouped/grouped prefix together.
        root = add(
            root,
            source_fixed_vector_bound::<ExprId>(binding.key_expressions.len())?,
        )?;
        if !matches!(binding.mode, BindingMode::Read | BindingMode::Delete)
            && let Some(entity) = schema.entity(binding.entity_type)
        {
            expected = expected
                .checked_add(entity.invariants().len())
                .ok_or(BudgetUnavailable)?;
        }
        if binding.mode != BindingMode::Read
            && let Some(aggregate) = schema.aggregate_for_entity(binding.entity_type)
        {
            // At most one root subject per actual mutable binding; duplicate
            // subjects can only reduce the original set/output cardinality.
            expected = expected
                .checked_add(aggregate.invariants().len())
                .ok_or(BudgetUnavailable)?;
        }
    }
    // validate_commit_checks keeps postimages and all expected tuples while
    // visiting written aggregates/root subjects and doing template comparisons.
    type Expected<'a> = (
        InvariantId,
        Vec<BindingId>,
        Vec<RootValidationReadId>,
        &'a crate::InvariantPlan,
        EntityTypeId,
    );
    let mut commit = source_growing_vector_bound::<&BindingPlan>(bindings.len())?;
    commit = add(
        commit,
        source_growing_vector_bound::<Expected<'_>>(expected)?,
    )?;
    // An expected tuple contains exactly one binding OR one read vector. Its
    // two Vec headers are already in Expected; price the larger concrete heap.
    let one_subject = source_fixed_vector_bound::<BindingId>(1)?
        .max(source_fixed_vector_bound::<RootValidationReadId>(1)?);
    commit = add(
        commit,
        u64::try_from(expected)
            .map_err(|_| BudgetUnavailable)?
            .checked_mul(one_subject)
            .ok_or(BudgetUnavailable)?,
    )?;
    commit = add(
        commit,
        source_tree_layout_bound::<AggregateTypeId, ()>(bindings.len())?,
    )?;
    commit = add(
        commit,
        source_tree_layout_bound::<(Option<BindingId>, Option<RootValidationReadId>), ()>(
            bindings.len(),
        )?,
    )?;
    commit = add(commit, source_fixed_vector_bound::<ExprId>(maximum_prefix)?)?;
    commit = add(
        commit,
        source_growing_vector_bound::<BindingId>(bindings.len())?,
    )?;
    commit = add(
        commit,
        source_growing_vector_bound::<RootValidationReadId>(reads.len())?,
    )?;
    // Root-read and commit-coverage calls return only unit. Their parent
    // containers do not escape; one shared dependency/comparison leaf charge
    // is added by the whole constructor while either parent's graph is alive.
    Ok(root.max(commit))
}

fn source_secret_flow_validation_parent_work_bound(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    common: &[Instruction],
    decisions: &[CommandDecisionPlanV1],
    schema: &SchemaIr,
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{
        source_fixed_vector_bound, source_growing_vector_bound, source_tree_layout_bound,
    };
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let mut flows = 0usize;
    let mut complete_secret_sources = 0usize;
    let mut maximum_entity_secret_fields = 0usize;
    for binding in bindings {
        for outcome in [
            binding.failure.as_ref(),
            binding.restriction_failure.as_ref(),
            binding.cascade_failure.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            flows = flows
                .checked_add(outcome.payload.fields.len())
                .ok_or(BudgetUnavailable)?;
        }
        if let Some(entity) = schema.entity(binding.entity_type) {
            let secrets = entity
                .record()
                .fields()
                .iter()
                .filter(|field| schema.is_secret_field(entity.id(), field.id()))
                .count();
            complete_secret_sources = complete_secret_sources
                .checked_add(secrets)
                .ok_or(BudgetUnavailable)?;
            maximum_entity_secret_fields = maximum_entity_secret_fields.max(secrets);
        }
    }
    for decision in decisions {
        for action in decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
        {
            if let Some(outcome) = action.rejection() {
                flows = flows
                    .checked_add(outcome.payload.fields.len())
                    .ok_or(BudgetUnavailable)?;
            }
        }
    }
    for instruction in common.iter().chain(decisions.iter().flat_map(|decision| {
        decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
            .flat_map(CommandDecisionActionV1::instructions)
    })) {
        // Root references for Require predicates/revisions/lease inputs are not
        // destinations. Every other instruction reference counted by the shared
        // borrowed helper contributes one actual flow in validate_secret_reveals.
        let non_destinations = match instruction {
            Instruction::Require { .. } | Instruction::WorkflowTransition { .. } => 1,
            Instruction::WorkflowLease { operation, .. } => match operation {
                WorkflowLeaseOperation::Claim { .. }
                | WorkflowLeaseOperation::Release { .. }
                | WorkflowLeaseOperation::Fence { .. } => 3,
                WorkflowLeaseOperation::Renew { .. } => 4,
                WorkflowLeaseOperation::Expire { .. } => 1,
            },
            Instruction::SetField { .. }
            | Instruction::SetEmbedding { .. }
            | Instruction::EmitEvent(_)
            | Instruction::Return(_) => 0,
        };
        let destinations = source_instruction_reference_count(instruction)?
            .checked_sub(non_destinations)
            .ok_or(BudgetUnavailable)?;
        flows = flows.checked_add(destinations).ok_or(BudgetUnavailable)?;
    }
    // A dependency can own at most one bound-field pair per actual arena node;
    // complete bindings can add every actual declared secret field once. The
    // B-tree deduplicates overlap, so their sum is a conservative cardinality.
    let sources = arena
        .len()
        .checked_add(complete_secret_sources)
        .ok_or(BudgetUnavailable)?;
    let required = flows.checked_mul(sources).ok_or(BudgetUnavailable)?;
    let mut bytes =
        source_growing_vector_bound::<(SecretRevealDestinationV1, ExprId, bool)>(flows)?;
    bytes = add(
        bytes,
        source_growing_vector_bound::<SecretRevealSpecV1>(required)?,
    )?;
    // The flows IntoIter and growing required Vec overlap the separately cloned
    // source-pair tree, its dependency result, and one secret_fields_for_entity
    // temporary Vec. Shared dependency work is added by the complete planner.
    bytes = add(
        bytes,
        source_tree_layout_bound::<(BindingId, FieldId), ()>(sources)?,
    )?;
    add(
        bytes,
        source_fixed_vector_bound::<FieldId>(maximum_entity_secret_fields)?,
    )
}

fn source_collection_index_parent_work_bound(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    common: &[Instruction],
    decisions: &[CommandDecisionPlanV1],
    schema: &SchemaIr,
) -> Result<u64, riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{
        source_fixed_vector_bound, source_growing_vector_bound, source_tree_layout_bound,
    };
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let mut branch_references = 0usize;
    let mut maximum_selected = 1usize;
    for decision in decisions {
        for action in decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
        {
            branch_references = branch_references
                .checked_add(action.bindings().len())
                .ok_or(BudgetUnavailable)?;
            maximum_selected = maximum_selected.max(
                action
                    .bindings()
                    .len()
                    .checked_add(1)
                    .ok_or(BudgetUnavailable)?,
            );
        }
    }
    let mut assigned = source_fixed_vector_bound::<BTreeSet<FieldId>>(bindings.len())?;
    let mut maximum_map = 0usize;
    let mut index_count = 0usize;
    let mut prefix_sources = 0usize;
    let mut maximum_components = 0usize;
    let mut maximum_record_fields = 0usize;
    for entity in schema.entities() {
        maximum_record_fields = maximum_record_fields.max(entity.record().fields().len());
    }
    for event in schema.events() {
        maximum_record_fields = maximum_record_fields.max(event.payload().fields().len());
    }
    for binding in bindings {
        let writes = common.iter().chain(decisions.iter().flat_map(|decision| {
            decision.when_arms.iter().map(|arm| &arm.action).chain(std::iter::once(&decision.else_action))
                .flat_map(CommandDecisionActionV1::instructions)
        })).filter(|instruction| matches!(instruction,
            Instruction::SetField { binding: target, .. } | Instruction::SetEmbedding { binding: target, .. }
                if *target == binding.id)).count();
        assigned = add(assigned, source_tree_layout_bound::<FieldId, ()>(writes)?)?;
        if let Some(entity) = schema.entity(binding.entity_type) {
            maximum_map = maximum_map.max(
                entity
                    .primary_key_fields()
                    .len()
                    .checked_add(binding.initializer.len())
                    .and_then(|n| n.checked_add(writes))
                    .ok_or(BudgetUnavailable)?,
            );
            index_count = index_count
                .checked_add(entity.indexes().len())
                .ok_or(BudgetUnavailable)?;
            for index in entity.indexes() {
                let k = index.key_schema().components().len();
                maximum_components = maximum_components.max(k);
                // One direct source per component, copied into each successive
                // prefix: 1+...+k. Collection multiplicity changes scalar byte
                // arithmetic, not this vector's physical source-entry count.
                let triangular = k
                    .checked_add(1)
                    .and_then(|next| k.checked_mul(next))
                    .ok_or(BudgetUnavailable)?
                    / 2;
                prefix_sources = prefix_sources
                    .checked_add(triangular)
                    .ok_or(BudgetUnavailable)?;
            }
        }
    }
    // Event mapping maps can be built before a later shape failure. Bound the
    // actual owned mappings as well as schema fields, without materializing any
    // new semantic graph or invoking allocating operation helper getters.
    for instruction in common.iter().chain(decisions.iter().flat_map(|decision| {
        decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
            .flat_map(CommandDecisionActionV1::instructions)
    })) {
        if let Instruction::EmitEvent(event) = instruction {
            maximum_map = maximum_map.max(event.payload.fields.len());
        }
    }
    let mut partition_nodes = 0usize;
    for aggregate in schema.aggregates() {
        partition_nodes = partition_nodes.max(aggregate.keys().expressions().len());
    }
    // aggregate_partition_route_fields owns visited/pending/dependency sets,
    // then a collected route Vec. Its actual template arena is already owned
    // and validated by SchemaIr; at most two children are pushed per visit.
    let mut route = source_tree_layout_bound::<ExprId, ()>(partition_nodes)?;
    route = add(
        route,
        source_tree_layout_bound::<FieldId, ()>(partition_nodes)?,
    )?;
    route = add(
        route,
        source_growing_vector_bound::<ExprId>(
            partition_nodes
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .ok_or(BudgetUnavailable)?,
        )?,
    )?;
    route = add(
        route,
        source_growing_vector_bound::<FieldId>(partition_nodes)?,
    )?;
    let distinct_sources = arena.len().checked_add(1).ok_or(BudgetUnavailable)?;
    // Outer decision-index sets overlap the complete inner index derivation.
    let mut index = source_tree_layout_bound::<BindingId, ()>(
        branch_references
            .checked_add(decisions.len())
            .ok_or(BudgetUnavailable)?,
    )?;
    index = add(
        index,
        source_tree_layout_bound::<BindingId, ()>(decisions.len())?,
    )?;
    index = add(
        index,
        source_tree_layout_bound::<BindingId, ()>(
            bindings.len().checked_add(1).ok_or(BudgetUnavailable)?,
        )?,
    )?;
    index = add(index, source_tree_layout_bound::<IndexId, ()>(index_count)?)?; // outer affected_indexes
    index = add(index, assigned)?;
    index = add(
        index,
        source_tree_layout_bound::<IndexId, ()>(schema.unique_keys().len())?,
    )?;
    index = add(index, source_tree_layout_bound::<IndexId, ()>(index_count)?)?; // inner affected_indexes
    index = add(index, source_tree_layout_bound::<IndexId, ()>(index_count)?)?; // affected_index_count helper
    index = add(
        index,
        source_tree_layout_bound::<FieldId, ExprId>(maximum_map)?,
    )?;
    index = add(index, route)?;
    index = add(
        index,
        source_growing_vector_bound::<AggregateElementSource>(maximum_components)?,
    )?;
    index = add(
        index,
        source_growing_vector_bound::<AggregateElementSource>(prefix_sources)?,
    )?;
    index = add(
        index,
        source_tree_layout_bound::<AggregateElementSource, usize>(
            prefix_sources.min(distinct_sources),
        )?,
    )?;
    // Early collection-graph validation precedes decision validation: count
    // every actual branch ID reference, NOT only the validated binding count.
    let mut collection = source_tree_layout_bound::<BindingId, ()>(branch_references)?;
    collection = add(
        collection,
        source_tree_layout_bound::<BindingId, ()>(decisions.len())?,
    )?;
    collection = add(
        collection,
        source_growing_vector_bound::<BindingId>(maximum_selected)?,
    )?;
    collection = add(
        collection,
        source_tree_layout_bound::<FieldId, ExprId>(maximum_map)?,
    )?;
    collection = add(
        collection,
        source_growing_vector_bound::<AggregateElementSource>(maximum_record_fields)?,
    )?;
    collection = add(
        collection,
        source_tree_layout_bound::<AggregateElementSource, usize>(
            maximum_record_fields.min(distinct_sources),
        )?,
    )?;
    // Both closed phases return only copied scalar charges. Their owned graphs
    // do not escape, so the peak is max; original input/union/output owners and
    // a possible delete-mode clone are charged separately by the full planner.
    Ok(index.max(collection))
}

// Heap component of the complete owning constructor plan. Fixed frame/control
// admission and the sealed Source entry are intentionally not enabled by this
// function. Decoded input families remain charged by the caller's Source sink.
#[expect(
    clippy::too_many_arguments,
    reason = "One owning bound prices every borrowed constructor graph before the sole constructor runs."
)]
fn source_command_constructor_heap_bound(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    outcomes: &[OutcomeSchema],
    reads: &[RootValidationReadPlan],
    locality: &LocalityPlan,
    checks: &[CommitCheckPlan],
    common: &[Instruction],
    decisions: &[CommandDecisionPlanV1],
    schema: &SchemaIr,
) -> Result<(u64, u64), riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
    use crate::bundle::source_decode_sink::{
        source_expression_comparison_work_bound, source_expression_dependency_work_bound,
        source_fixed_vector_bound, source_growing_vector_bound, source_tree_layout_bound,
    };
    use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
    let add = |left: u64, right: u64| left.checked_add(right).ok_or(BudgetUnavailable);
    let (mut retained, maximum_mode_clone) =
        source_derived_command_outputs_bound(bindings, schema)?;
    retained = add(
        retained,
        riffdb_types::OwnedCapacityLifecycleV1::control_allocation_upper_bound()
            .map_err(|_| BudgetUnavailable)?,
    )?;
    // These two owners survive all validation calls and the final hash. They
    // are independent of retained derived outputs and sequential parent work.
    let always = add(
        source_union_instruction_clone_bound(common, decisions)?,
        source_tree_layout_bound::<&str, ()>(outcomes.len())?,
    )?;
    let mut template_nodes = arena.len();
    let mut maximum_key_fields = 0usize;
    for entity in schema.entities() {
        maximum_key_fields = maximum_key_fields.max(entity.primary_key_fields().len());
        for invariant in entity.invariants() {
            template_nodes = template_nodes.max(invariant.expressions().len());
        }
    }
    for aggregate in schema.aggregates() {
        template_nodes = template_nodes.max(aggregate.keys().expressions().len());
        for invariant in aggregate.invariants() {
            template_nodes = template_nodes.max(invariant.expressions().len());
        }
    }
    let dependency = source_expression_dependency_work_bound(arena.len())?;
    let comparison = source_expression_comparison_work_bound(arena.len(), template_nodes)?;
    // Leaf computations return Copy booleans/charges or a scoped dependency
    // result. Only one such leaf is alive inside the existing parent algorithms;
    // cloned secret-source sets are counted in their parent, not hidden here.
    let leaf = dependency.max(comparison);
    let mut parents =
        source_instruction_stream_parent_work_bound(bindings, common, decisions, schema)?;
    parents = parents.max(source_decision_validation_parent_work_bound(
        arena, bindings, common, decisions, schema,
    )?);
    parents = parents.max(source_collection_index_parent_work_bound(
        arena, bindings, common, decisions, schema,
    )?);
    parents = parents.max(source_coverage_validation_parent_work_bound(
        bindings, reads, schema,
    )?);
    parents = parents.max(source_secret_flow_validation_parent_work_bound(
        arena, bindings, common, decisions, schema,
    )?);
    parents = parents.max(source_command_read_dependency_parent_work_bound(
        arena, bindings, reads, common, decisions,
    )?);
    parents = parents.max(source_command_reachability_parent_work_bound(
        arena, bindings, reads, locality, checks, common, decisions,
    )?);
    let mut union_count = common.len();
    let mut maximum_object_clone = 0u64;
    for binding in bindings {
        for outcome in [
            binding.failure.as_ref(),
            binding.restriction_failure.as_ref(),
            binding.cascade_failure.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            maximum_object_clone =
                maximum_object_clone.max(source_outcome_clone_heap_bound(outcome)?);
        }
    }
    for decision in decisions {
        for action in decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
        {
            union_count = union_count
                .checked_add(action.instructions().len())
                .ok_or(BudgetUnavailable)?;
        }
    }
    for instruction in common.iter().chain(decisions.iter().flat_map(|decision| {
        decision
            .when_arms
            .iter()
            .map(|arm| &arm.action)
            .chain(std::iter::once(&decision.else_action))
            .flat_map(CommandDecisionActionV1::instructions)
    })) {
        // An instruction's complete clone contains each possible single
        // ObjectConstruction field clone used by declaration validation.
        maximum_object_clone =
            maximum_object_clone.max(source_instruction_clone_heap_bound(instruction)?);
    }
    let operations = add(
        source_fixed_vector_bound::<ExprId>(4)?,
        source_fixed_vector_bound::<&OutcomeConstruction>(4)?,
    )?;
    // The remaining simple parent phases: ordinary-delete/mutable/selected
    // borrowed vectors, key-field set, idempotency referenced-input set and one
    // declaration object clone. Each phase retires its parents before the next.
    let simple = source_growing_vector_bound::<&BindingPlan>(bindings.len())?
        .max(source_tree_layout_bound::<FieldId, ()>(maximum_key_fields)?)
        .max(source_tree_layout_bound::<FieldId, ()>(arena.len())?)
        .max(maximum_object_clone);
    parents = parents.max(add(simple, operations)?);
    let mut maximum_relationship_fields = 0usize;
    for relationship in schema.relationships() {
        maximum_relationship_fields =
            maximum_relationship_fields.max(relationship.source_fields().len());
    }
    // Both retained-output derivations own a writes map over union instructions.
    // Relationship matching additionally retains a shunted resulting-expression
    // Vec and collected bool Vec while comparing targets. Unique result/key
    // buffers are already retained above; only their dependency leaf is extra.
    let mut derived = source_tree_layout_bound::<(BindingId, FieldId), ExprId>(union_count)?;
    derived = add(
        derived,
        source_growing_vector_bound::<ExprId>(maximum_relationship_fields)?,
    )?;
    derived = add(
        derived,
        source_growing_vector_bound::<bool>(maximum_relationship_fields)?,
    )?;
    parents = parents.max(derived);
    let workspace = add(add(add(always, parents)?, leaf)?, maximum_mode_clone)?;
    Ok((retained, workspace))
}

impl CommandPlan {
    // Numerical-only borrowed owning calculation. This constructs no plan and
    // grants no decode/Source authority; the closed admission sink is sole user.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn source_constructor_heap_bound(
        arena: &ExpressionArena,
        bindings: &[BindingPlan],
        outcomes: &[OutcomeSchema],
        reads: &[RootValidationReadPlan],
        locality: &LocalityPlan,
        checks: &[CommitCheckPlan],
        common: &[Instruction],
        decisions: &[CommandDecisionPlanV1],
        schema: &SchemaIr,
    ) -> Result<(u64, u64), riffdb_types::exact_provider_budget_v1::BudgetUnavailable> {
        source_command_constructor_heap_bound(
            arena, bindings, outcomes, reads, locality, checks, common, decisions, schema,
        )
    }
}
