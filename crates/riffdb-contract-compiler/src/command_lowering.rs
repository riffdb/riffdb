//! Checked executable command-plan lowering from compiler-private typed HIR.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{
    BindingId, BindingMode, BindingPlan, CollectionDuplicatePolicyV1, CollectionExpansionPlanV1,
    CommandInputSchema, CommandInvocationClass, CommandPlan, CommitCheckPlan,
    ConflictDerivationPlan, EventConstruction, EventSchema, ExecutionClass, ExprId, ExpressionKind,
    FieldExpression, FieldSchema, Instruction, IrValidationError, KeySchema, LocalityPlan,
    OutcomeConstruction, OutcomeSchema, RecordSchema, RecordTypeRef, RootValidationReadId,
    RootValidationReadPlan, SchemaIr, SecretRevealDestinationV1, SecretRevealSpecV1,
    ServiceValueKind, ServiceValueSchema, WorkflowLeaseFields, WorkflowLeaseOperation,
};
use riffdb_contract_syntax::Span;
use riffdb_types::{CanonicalValue, ContractLineage, EntityTypeId, FieldId, InvariantId};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::hir::{
    HirBinding, HirCommand, HirEffect, HirExpressionArena, HirExpressionNode, HirOutcome,
    HirWorkflowLeaseOperation, TypedContractHir,
};
use crate::locality::command_expression_fingerprint;

enum RawInstruction {
    Require {
        requirement_index: u32,
        predicate: ExprId,
        rejection_occurrence: usize,
    },
    SetField {
        binding: BindingId,
        field: FieldId,
        value: ExprId,
    },
    SetEmbedding {
        binding: BindingId,
        field: FieldId,
        value: ExprId,
        model_identity: ExprId,
        model_version: ExprId,
    },
    WorkflowTransition {
        binding: BindingId,
        state_field: FieldId,
        source_states: Vec<riffdb_types::EnumVariantId>,
        destination: riffdb_types::EnumVariantId,
        expected_revision: ExprId,
        stale_occurrence: usize,
        illegal_occurrence: usize,
    },
    WorkflowLease {
        binding: BindingId,
        fields: WorkflowLeaseFields,
        operation: RawWorkflowLeaseOperation,
    },
    EmitEvent {
        event_id: riffdb_types::EventTypeId,
        fields: Vec<FieldExpression>,
        span: Span,
    },
}

enum RawWorkflowLeaseOperation {
    Claim {
        owner: ExprId,
        duration_seconds: ExprId,
        expected_revision: ExprId,
        outcomes: [usize; 4],
    },
    Renew {
        owner: ExprId,
        fencing_token: ExprId,
        duration_seconds: ExprId,
        expected_revision: ExprId,
        outcomes: [usize; 4],
    },
    Release {
        owner: ExprId,
        fencing_token: ExprId,
        expected_revision: ExprId,
        outcomes: [usize; 2],
    },
    Expire {
        expected_revision: ExprId,
        outcomes: [usize; 2],
    },
    Fence {
        owner: ExprId,
        fencing_token: ExprId,
        expected_revision: ExprId,
        outcomes: [usize; 3],
    },
}

fn lease_hir_outcomes(operation: &HirWorkflowLeaseOperation) -> Vec<&HirOutcome> {
    match operation {
        HirWorkflowLeaseOperation::Claim {
            stale,
            unavailable,
            invalid,
            exhausted,
            ..
        } => vec![stale, unavailable, invalid, exhausted],
        HirWorkflowLeaseOperation::Renew {
            stale,
            invalid,
            expired,
            exhausted,
            ..
        } => vec![stale, invalid, expired, exhausted],
        HirWorkflowLeaseOperation::Release { stale, invalid, .. } => vec![stale, invalid],
        HirWorkflowLeaseOperation::Expire { stale, active, .. } => vec![stale, active],
        HirWorkflowLeaseOperation::Fence {
            stale,
            invalid,
            expired,
            ..
        } => vec![stale, invalid, expired],
    }
}

struct NormalizedOutcome<'a> {
    source: &'a HirOutcome,
    fields: BTreeMap<FieldId, ExprId>,
}

struct RawCommitCheck {
    invariant_id: InvariantId,
    predicate: ExprId,
    source_bindings: Vec<BindingId>,
    root_validation_reads: Vec<RootValidationReadId>,
    span: Span,
}

struct RawRootValidationRead {
    id: RootValidationReadId,
    source_binding: BindingId,
    entity_type: EntityTypeId,
    key_schema: KeySchema,
    key_expressions: Vec<ExprId>,
    accessed_fields: Vec<FieldId>,
    span: Span,
}

struct RootValidationTarget {
    source_binding: BindingId,
    key_expressions: Vec<ExprId>,
    fingerprint: Vec<Vec<u8>>,
    span: Span,
}

#[derive(Clone, Copy)]
enum SchemaReplacement<'a> {
    Existing(&'a BTreeMap<FieldId, ExprId>),
    Binding(BindingId),
    RootValidation(RootValidationReadId),
}

/// Lowers every typed HIR command into stable-ID-ordered checked executable plans.
pub(crate) fn lower_commands(
    hir: &TypedContractHir,
    schema: &SchemaIr,
) -> Result<Vec<CommandPlan>, CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    let mut plans = Vec::new();
    for command in &hir.commands {
        match lower_command(hir, schema, command) {
            Ok(plan) => plans.push(plan),
            Err(mut command_diagnostics) => diagnostics.append(&mut command_diagnostics),
        }
    }
    if diagnostics.is_empty() {
        plans.sort_by_key(CommandPlan::command_id);
        Ok(plans)
    } else {
        Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"))
    }
}

fn lower_command(
    hir: &TypedContractHir,
    schema: &SchemaIr,
    command: &HirCommand,
) -> Result<CommandPlan, Vec<CompilerDiagnostic>> {
    let contract_lineage =
        ContractLineage::new(hir.name.clone()).map_err(|_| vec![ir_diagnostic(hir.name_span)])?;
    let mut diagnostics = Vec::new();
    let input_fields = command
        .inputs
        .iter()
        .filter_map(|input| {
            FieldSchema::new(
                input.field.id,
                input.field.name.clone(),
                input.field.value_type.clone(),
            )
            .map_err(|_| ir_diagnostic(input.field.name_span))
            .map_err(|diagnostic| diagnostics.push(diagnostic))
            .ok()
        })
        .collect();
    let input_record = RecordSchema::new(RecordTypeRef::CommandInput(command.id), input_fields)
        .map_err(|_| vec![ir_diagnostic(command.span)])?;
    let input_schema = CommandInputSchema::new(command.id, input_record)
        .map_err(|_| vec![ir_diagnostic(command.span)])?;

    let mut hir_expressions = command.expressions.clone();
    let mut failure_occurrence = BTreeMap::new();
    let mut restriction_failure_occurrence = BTreeMap::new();
    let mut cascade_failure_occurrence = BTreeMap::new();
    let mut occurrences = Vec::new();
    for binding in &command.bindings {
        failure_occurrence.insert(binding.id, occurrences.len());
        occurrences.push(&binding.failure);
        if let Some(failure) = &binding.restriction_failure {
            restriction_failure_occurrence.insert(binding.id, occurrences.len());
            occurrences.push(failure);
        }
        if let Some(failure) = &binding.cascade_failure {
            cascade_failure_occurrence.insert(binding.id, occurrences.len());
            occurrences.push(failure);
        }
    }
    let rejection_base = occurrences.len();
    occurrences.extend(
        command
            .requirements
            .iter()
            .map(|requirement| &requirement.rejection),
    );
    occurrences.extend(command.effects.iter().flat_map(|effect| match effect {
        HirEffect::WorkflowTransition { stale, illegal, .. } => vec![stale, illegal],
        HirEffect::WorkflowLease { operation, .. } => lease_hir_outcomes(operation),
        HirEffect::Set { .. } | HirEffect::Embed { .. } | HirEffect::Emit { .. } => Vec::new(),
    }));
    occurrences.push(&command.success);
    let secret_reveals = lower_secret_reveals(command, &occurrences);
    let (outcome_schemas, normalized_outcomes) = normalize_outcomes(
        command,
        &occurrences,
        &mut hir_expressions,
        &mut diagnostics,
    );
    let effect_outcome_count = command
        .effects
        .iter()
        .try_fold(0usize, |count, effect| {
            let additional = match effect {
                HirEffect::WorkflowTransition { .. } => 2,
                HirEffect::WorkflowLease { operation, .. } => lease_hir_outcomes(operation).len(),
                HirEffect::Set { .. } | HirEffect::Embed { .. } | HirEffect::Emit { .. } => 0,
            };
            count.checked_add(additional)
        })
        .ok_or_else(|| vec![ir_diagnostic(command.span)])?;
    let success_occurrence = rejection_base + command.requirements.len() + effect_outcome_count;

    let (locality, aggregate_id) =
        lower_locality(hir, schema, command, &mut hir_expressions, &mut diagnostics);
    let (raw_checks, raw_root_validation_reads) = lower_commit_checks(
        hir,
        schema,
        command,
        aggregate_id,
        &mut hir_expressions,
        &mut diagnostics,
    );
    let (raw_instructions, repeated_instruction_range) =
        command_instructions(command, &mut diagnostics);
    validate_event_partition_proofs(
        schema,
        &hir_expressions,
        locality.as_ref(),
        &raw_instructions,
        &mut diagnostics,
    );

    if !diagnostics.is_empty() {
        return Err(diagnostics);
    }
    let expressions = hir_expressions
        .to_ir(command.span)
        .map_err(|diagnostic| vec![diagnostic])?;
    let outcomes_by_id = outcome_schemas
        .iter()
        .map(|outcome| (outcome.id(), outcome))
        .collect::<BTreeMap<_, _>>();
    let constructions = normalized_outcomes
        .iter()
        .map(|occurrence| {
            let outcome = outcomes_by_id
                .get(&occurrence.source.id)
                .copied()
                .ok_or_else(|| ir_diagnostic(occurrence.source.span))?;
            let fields = outcome
                .payload()
                .fields()
                .iter()
                .map(|field| {
                    occurrence
                        .fields
                        .get(&field.id())
                        .copied()
                        .map(|expression| FieldExpression::new(field.id(), expression))
                        .ok_or_else(|| ir_diagnostic(occurrence.source.span))
                })
                .collect::<Result<Vec<_>, _>>()?;
            OutcomeConstruction::new(outcome, fields, &expressions)
                .map_err(|_| ir_diagnostic(occurrence.source.span))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|diagnostic| vec![diagnostic])?;

    let all_roots = collect_influential_roots(&raw_checks, &raw_instructions, &constructions);
    let (mut accessed_fields, complete_access) = read_dependencies(
        &expressions,
        command.bindings.len(),
        &all_roots,
        command.span,
    )
    .map_err(|diagnostic| vec![diagnostic])?;
    for instruction in &raw_instructions {
        match instruction {
            RawInstruction::WorkflowTransition {
                binding,
                state_field,
                ..
            } => {
                accessed_fields[binding.get() as usize].insert(*state_field);
            }
            RawInstruction::WorkflowLease {
                binding, fields, ..
            } => {
                accessed_fields[binding.get() as usize].extend([
                    fields.owner_field,
                    fields.expiry_field,
                    fields.fencing_token_field,
                ]);
                if let Some(field) = fields.attempt_field {
                    accessed_fields[binding.get() as usize].insert(field);
                }
            }
            _ => {}
        }
    }
    let binding_plans = command
        .bindings
        .iter()
        .enumerate()
        .map(|(index, binding)| {
            let entity = schema
                .entity(binding.entity_id)
                .ok_or_else(|| ir_diagnostic(binding.entity_span))?;
            BindingPlan::new_with_delete_failures(
                binding.id,
                binding.name.clone(),
                binding.mode,
                binding.entity_id,
                entity.primary_key().clone(),
                binding
                    .arguments
                    .iter()
                    .map(|argument| argument.id)
                    .collect(),
                accessed_fields[index].iter().copied().collect(),
                complete_access[index],
                constructions[*failure_occurrence
                    .get(&binding.id)
                    .expect("binding occurrence")]
                .clone(),
                restriction_failure_occurrence
                    .get(&binding.id)
                    .map(|occurrence| constructions[*occurrence].clone()),
                cascade_failure_occurrence
                    .get(&binding.id)
                    .map(|occurrence| constructions[*occurrence].clone()),
            )
            .map_err(|_| ir_diagnostic(binding.name_span))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|diagnostic| vec![diagnostic])?;
    let root_validation_reads = raw_root_validation_reads
        .into_iter()
        .map(|read| {
            RootValidationReadPlan::new(
                read.id,
                read.source_binding,
                read.entity_type,
                read.key_schema,
                read.key_expressions,
                read.accessed_fields,
            )
            .map_err(|_| ir_diagnostic(read.span))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|diagnostic| vec![diagnostic])?;
    let commit_checks = raw_checks
        .into_iter()
        .map(|check| {
            CommitCheckPlan::new(
                check.invariant_id,
                check.predicate,
                check.source_bindings,
                check.root_validation_reads,
            )
            .map_err(|_| ir_diagnostic(check.span))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|diagnostic| vec![diagnostic])?;
    let mut instructions = Vec::new();
    for raw in raw_instructions {
        let instruction = match raw {
            RawInstruction::Require {
                requirement_index,
                predicate,
                rejection_occurrence,
            } => Instruction::Require {
                requirement_index,
                predicate,
                reject: constructions[rejection_occurrence].clone(),
            },
            RawInstruction::SetField {
                binding,
                field,
                value,
            } => Instruction::SetField {
                binding,
                field,
                value,
            },
            RawInstruction::SetEmbedding {
                binding,
                field,
                value,
                model_identity,
                model_version,
            } => Instruction::SetEmbedding {
                binding,
                field,
                value,
                model_identity,
                model_version,
            },
            RawInstruction::WorkflowTransition {
                binding,
                state_field,
                source_states,
                destination,
                expected_revision,
                stale_occurrence,
                illegal_occurrence,
            } => Instruction::WorkflowTransition {
                binding,
                state_field,
                source_states,
                destination,
                expected_revision,
                stale: constructions[stale_occurrence].clone(),
                illegal: constructions[illegal_occurrence].clone(),
            },
            RawInstruction::WorkflowLease {
                binding,
                fields,
                operation,
            } => {
                let operation = match operation {
                    RawWorkflowLeaseOperation::Claim {
                        owner,
                        duration_seconds,
                        expected_revision,
                        outcomes,
                    } => WorkflowLeaseOperation::Claim {
                        owner,
                        duration_seconds,
                        expected_revision,
                        stale: constructions[outcomes[0]].clone(),
                        unavailable: constructions[outcomes[1]].clone(),
                        invalid: constructions[outcomes[2]].clone(),
                        exhausted: constructions[outcomes[3]].clone(),
                    },
                    RawWorkflowLeaseOperation::Renew {
                        owner,
                        fencing_token,
                        duration_seconds,
                        expected_revision,
                        outcomes,
                    } => WorkflowLeaseOperation::Renew {
                        owner,
                        fencing_token,
                        duration_seconds,
                        expected_revision,
                        stale: constructions[outcomes[0]].clone(),
                        invalid: constructions[outcomes[1]].clone(),
                        expired: constructions[outcomes[2]].clone(),
                        exhausted: constructions[outcomes[3]].clone(),
                    },
                    RawWorkflowLeaseOperation::Release {
                        owner,
                        fencing_token,
                        expected_revision,
                        outcomes,
                    } => WorkflowLeaseOperation::Release {
                        owner,
                        fencing_token,
                        expected_revision,
                        stale: constructions[outcomes[0]].clone(),
                        invalid: constructions[outcomes[1]].clone(),
                    },
                    RawWorkflowLeaseOperation::Expire {
                        expected_revision,
                        outcomes,
                    } => WorkflowLeaseOperation::Expire {
                        expected_revision,
                        stale: constructions[outcomes[0]].clone(),
                        active: constructions[outcomes[1]].clone(),
                    },
                    RawWorkflowLeaseOperation::Fence {
                        owner,
                        fencing_token,
                        expected_revision,
                        outcomes,
                    } => WorkflowLeaseOperation::Fence {
                        owner,
                        fencing_token,
                        expected_revision,
                        stale: constructions[outcomes[0]].clone(),
                        invalid: constructions[outcomes[1]].clone(),
                        expired: constructions[outcomes[2]].clone(),
                    },
                };
                Instruction::WorkflowLease {
                    binding,
                    fields,
                    operation,
                }
            }
            RawInstruction::EmitEvent {
                event_id,
                fields,
                span,
            } => Instruction::EmitEvent(
                EventConstruction::new(event_id, fields, schema, &expressions)
                    .map_err(|_| vec![ir_diagnostic(span)])?,
            ),
        };
        instructions.push(instruction);
    }
    instructions.push(Instruction::Return(
        constructions[success_occurrence].clone(),
    ));

    let execution_class = if command.bindings.iter().any(|binding| {
        matches!(
            binding.mode,
            BindingMode::Mutate | BindingMode::Create | BindingMode::Delete
        )
    }) || !command.effects.is_empty()
    {
        ExecutionClass::IdempotentMutation
    } else {
        ExecutionClass::ReadOnly
    };
    let idempotency_input = if execution_class == ExecutionClass::IdempotentMutation {
        command.idempotency.as_ref().and_then(|root| {
            root.expressions
                .node(root.root.id)
                .and_then(|node| match node.kind {
                    ExpressionKind::InputField(field) => Some(field),
                    _ => None,
                })
        })
    } else {
        None
    };
    let locality = locality.ok_or_else(|| vec![ir_diagnostic(command.span)])?;
    let service_values = command
        .service_values
        .iter()
        .map(|value| {
            let field = FieldSchema::new(
                value.field.id,
                value.field.name.clone(),
                value.field.value_type.clone(),
            )
            .map_err(|_| ir_diagnostic(value.field.name_span))?;
            let kind = match value.kind {
                crate::hir::HirServiceValueKind::UuidV7 => ServiceValueKind::UuidV7,
                crate::hir::HirServiceValueKind::TransactionTime => {
                    ServiceValueKind::TransactionTime
                }
            };
            ServiceValueSchema::new(field, kind).map_err(|_| ir_diagnostic(value.field.name_span))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|diagnostic| vec![diagnostic])?;
    let result = if let Some(expansion) = &command.collection_expansion {
        let (first_instruction, instruction_count) =
            repeated_instruction_range.ok_or_else(|| vec![ir_diagnostic(expansion.span)])?;
        let expansion = CollectionExpansionPlanV1::new(
            expansion.input_field,
            expansion.minimum_elements,
            expansion.maximum_elements,
            expansion.element_type.clone(),
            expansion.first_binding,
            expansion.binding_count,
            first_instruction,
            instruction_count,
            CollectionDuplicatePolicyV1::Reject,
        )
        .map_err(|_| vec![ir_diagnostic(expansion.span)])?;
        if command.invocation_class == CommandInvocationClass::Reimport {
            CommandPlan::new_reimport_collection(
                command.id,
                contract_lineage,
                command.name.clone(),
                hir.contract_version,
                input_schema,
                outcome_schemas,
                command.success.id,
                expressions,
                binding_plans,
                root_validation_reads,
                locality,
                commit_checks,
                instructions,
                expansion,
                schema,
            )
        } else {
            CommandPlan::new_collection_with_secret_reveals(
                command.id,
                contract_lineage,
                command.name.clone(),
                hir.contract_version,
                input_schema,
                service_values,
                outcome_schemas,
                command.success.id,
                idempotency_input,
                expressions,
                binding_plans,
                root_validation_reads,
                locality,
                commit_checks,
                instructions,
                expansion,
                secret_reveals,
                execution_class,
                schema,
            )
        }
    } else {
        CommandPlan::new_with_service_values_and_secret_reveals(
            command.id,
            contract_lineage,
            command.name.clone(),
            hir.contract_version,
            input_schema,
            service_values,
            outcome_schemas,
            command.success.id,
            idempotency_input,
            expressions,
            binding_plans,
            root_validation_reads,
            locality,
            commit_checks,
            instructions,
            secret_reveals,
            execution_class,
            schema,
        )
    };
    result.map_err(|error| vec![ir_error_diagnostic(error, command.span)])
}

fn lower_secret_reveals(command: &HirCommand, outcomes: &[&HirOutcome]) -> Vec<SecretRevealSpecV1> {
    let mut reveals = Vec::new();
    for outcome in outcomes {
        for field in &outcome.fields {
            reveals.extend(field.reveals.iter().map(|source| {
                SecretRevealSpecV1::new(
                    source.binding,
                    source.field,
                    field.value.id,
                    SecretRevealDestinationV1::OutcomeField {
                        outcome: outcome.id,
                        field: field.id,
                    },
                )
            }));
        }
    }
    for effect in &command.effects {
        match effect {
            HirEffect::Set {
                binding,
                field,
                value,
                reveals: sources,
                ..
            } => reveals.extend(sources.iter().map(|source| {
                SecretRevealSpecV1::new(
                    source.binding,
                    source.field,
                    value.id,
                    SecretRevealDestinationV1::EntityField {
                        binding: *binding,
                        field: *field,
                    },
                )
            })),
            HirEffect::Embed { .. } => {}
            HirEffect::Emit {
                event_id, fields, ..
            } => {
                for field in fields {
                    reveals.extend(field.reveals.iter().map(|source| {
                        SecretRevealSpecV1::new(
                            source.binding,
                            source.field,
                            field.value.id,
                            SecretRevealDestinationV1::EventField {
                                event_type: *event_id,
                                field: field.id,
                            },
                        )
                    }));
                }
            }
            HirEffect::WorkflowTransition { .. } | HirEffect::WorkflowLease { .. } => {}
        }
    }
    reveals.sort_unstable();
    reveals.dedup();
    reveals
}

fn validate_event_partition_proofs(
    schema: &SchemaIr,
    expressions: &HirExpressionArena,
    locality: Option<&LocalityPlan>,
    instructions: &[RawInstruction],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) {
    for instruction in instructions {
        let RawInstruction::EmitEvent {
            event_id,
            fields,
            span,
        } = instruction
        else {
            continue;
        };
        let Some(partition) = schema.event(*event_id).and_then(EventSchema::partition) else {
            continue;
        };
        let Some(locality) = locality else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidEvent,
                *span,
            ));
            continue;
        };
        if partition.key_schema() != locality.partition_schema() {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::CrossPartitionMutation,
                *span,
            ));
            continue;
        }
        let partition_expressions = partition
            .fields()
            .iter()
            .map(|field_id| {
                fields
                    .iter()
                    .find(|field| field.field_id() == *field_id)
                    .map(|field| field.expression())
            })
            .collect::<Option<Vec<_>>>();
        let Some(partition_expressions) = partition_expressions else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidEvent,
                *span,
            ));
            continue;
        };
        let supplied = partition_expressions
            .iter()
            .map(|expression| command_expression_fingerprint(expressions, *expression))
            .collect::<Vec<_>>();
        let expected = [command_expression_fingerprint(
            expressions,
            locality.partition_expression(),
        )];
        if supplied.as_slice() != expected {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::CrossPartitionMutation,
                *span,
            ));
        }
    }
}

fn normalize_outcomes<'a>(
    command: &HirCommand,
    occurrences: &[&'a HirOutcome],
    expressions: &mut HirExpressionArena,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> (Vec<OutcomeSchema>, Vec<NormalizedOutcome<'a>>) {
    let mut by_id = BTreeMap::<_, Vec<&HirOutcome>>::new();
    for occurrence in occurrences {
        by_id.entry(occurrence.id).or_default().push(occurrence);
    }
    let mut schemas = Vec::new();
    for (outcome_id, occurrences) in &by_id {
        let mut fields = BTreeMap::<FieldId, (&str, &crate::hir::HirExpressionRoot, Span)>::new();
        for occurrence in occurrences {
            for field in &occurrence.fields {
                fields.entry(field.id).or_insert((
                    field.name.as_str(),
                    &field.value,
                    field.name_span,
                ));
            }
        }
        let mut schema_fields = Vec::new();
        for (field_id, (name, expression, span)) in fields {
            match FieldSchema::new(field_id, name.to_owned(), expression.value_type.clone()) {
                Ok(field) => schema_fields.push(field),
                Err(_) => diagnostics.push(ir_diagnostic(span)),
            }
        }
        let record = RecordSchema::new(
            RecordTypeRef::CommandOutcome {
                command_id: command.id,
                outcome_id: *outcome_id,
            },
            schema_fields,
        );
        let name = occurrences[0].name.clone();
        match record.and_then(|record| OutcomeSchema::new(command.id, *outcome_id, name, record)) {
            Ok(schema) => schemas.push(schema),
            Err(_) => diagnostics.push(ir_diagnostic(occurrences[0].span)),
        }
    }
    schemas.sort_by_key(OutcomeSchema::id);
    let schemas_by_id = schemas
        .iter()
        .map(|schema| (schema.id(), schema))
        .collect::<BTreeMap<_, _>>();
    let mut normalized = Vec::new();
    for occurrence in occurrences {
        let Some(schema) = schemas_by_id.get(&occurrence.id).copied() else {
            continue;
        };
        let mut fields = occurrence
            .fields
            .iter()
            .map(|field| (field.id, field.value.id))
            .collect::<BTreeMap<_, _>>();
        for field in schema.payload().fields() {
            if fields.contains_key(&field.id()) {
                continue;
            }
            if !field.value_type().is_optional() {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidOutcome,
                    occurrence.span,
                ));
                continue;
            }
            match push_hir_node(
                expressions,
                ExpressionKind::Constant(CanonicalValue::Null),
                field.value_type().clone(),
                occurrence.span,
            ) {
                Ok(expression) => {
                    fields.insert(field.id(), expression);
                }
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
        normalized.push(NormalizedOutcome {
            source: occurrence,
            fields,
        });
    }
    (schemas, normalized)
}

fn lower_locality(
    hir: &TypedContractHir,
    schema: &SchemaIr,
    command: &HirCommand,
    expressions: &mut HirExpressionArena,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> (Option<LocalityPlan>, Option<riffdb_types::AggregateTypeId>) {
    let Some(anchor) = command
        .bindings
        .iter()
        .find(|binding| {
            matches!(
                binding.mode,
                BindingMode::Mutate | BindingMode::Create | BindingMode::Delete
            )
        })
        .or_else(|| command.bindings.first())
    else {
        return (None, None);
    };
    let Some(aggregate_hir) = hir.aggregate_for_entity(anchor.entity_id) else {
        return (None, None);
    };
    let aggregate_id = aggregate_hir.id;
    let Some(aggregate) = schema.aggregate(aggregate_id) else {
        diagnostics.push(ir_diagnostic(aggregate_hir.span));
        return (None, Some(aggregate_id));
    };
    let Some(root_entity) = hir.entity(aggregate_hir.root) else {
        diagnostics.push(ir_diagnostic(aggregate_hir.root_span));
        return (None, Some(aggregate_id));
    };
    let partition_replacements = root_entity
        .key_fields
        .iter()
        .copied()
        .zip(anchor.arguments.iter().map(|argument| argument.id))
        .collect::<BTreeMap<_, _>>();
    let partition_expression = match append_schema_expression(
        &aggregate_hir.keys.expressions,
        aggregate_hir.keys.partition.id,
        expressions,
        SchemaReplacement::Existing(&partition_replacements),
        aggregate_hir.keys.partition.span,
    ) {
        Ok(expression) => expression,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return (None, Some(aggregate_id));
        }
    };
    let selected = command
        .bindings
        .iter()
        .filter(|binding| {
            matches!(
                binding.mode,
                BindingMode::Mutate | BindingMode::Create | BindingMode::Delete
            )
        })
        .collect::<Vec<_>>();
    let mut conflicts = Vec::new();
    for binding in selected {
        let replacements = root_entity
            .key_fields
            .iter()
            .copied()
            .zip(binding.arguments.iter().map(|argument| argument.id))
            .collect::<BTreeMap<_, _>>();
        let mut conflict_expressions = Vec::new();
        for source in &aggregate_hir.keys.conflicts {
            match append_schema_expression(
                &aggregate_hir.keys.expressions,
                source.id,
                expressions,
                SchemaReplacement::Existing(&replacements),
                source.span,
            ) {
                Ok(expression) => conflict_expressions.push(expression),
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
        match ConflictDerivationPlan::new(
            aggregate.keys().conflict_schema().clone(),
            conflict_expressions,
        ) {
            Ok(conflict) => conflicts.push(conflict),
            Err(_) => diagnostics.push(ir_diagnostic(binding.span)),
        }
    }
    match LocalityPlan::new(
        aggregate_id,
        aggregate.keys().partition_schema().clone(),
        partition_expression,
        conflicts,
    ) {
        Ok(locality) => (Some(locality), Some(aggregate_id)),
        Err(_) => {
            diagnostics.push(ir_diagnostic(aggregate_hir.span));
            (None, Some(aggregate_id))
        }
    }
}

fn lower_commit_checks(
    hir: &TypedContractHir,
    schema: &SchemaIr,
    command: &HirCommand,
    aggregate_id: Option<riffdb_types::AggregateTypeId>,
    expressions: &mut HirExpressionArena,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> (Vec<RawCommitCheck>, Vec<RawRootValidationRead>) {
    let mut checks = Vec::new();
    for binding in command
        .bindings
        .iter()
        .filter(|binding| matches!(binding.mode, BindingMode::Mutate | BindingMode::Create))
    {
        let Some(entity) = hir.entity(binding.entity_id) else {
            diagnostics.push(ir_diagnostic(binding.entity_span));
            continue;
        };
        for invariant in &entity.invariants {
            match append_schema_expression(
                &invariant.expressions,
                invariant.expression.id,
                expressions,
                SchemaReplacement::Binding(binding.id),
                invariant.expression.span,
            ) {
                Ok(predicate) => checks.push(RawCommitCheck {
                    invariant_id: invariant.id,
                    predicate,
                    source_bindings: vec![binding.id],
                    root_validation_reads: Vec::new(),
                    span: invariant.name_span,
                }),
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
    }
    let Some(aggregate_id) = aggregate_id else {
        return (checks, Vec::new());
    };
    let Some(aggregate) = hir.aggregate(aggregate_id) else {
        diagnostics.push(ir_diagnostic(command.span));
        return (checks, Vec::new());
    };
    if aggregate.invariants.is_empty() {
        return (checks, Vec::new());
    }
    let Some(root) = hir.entity(aggregate.root) else {
        diagnostics.push(ir_diagnostic(aggregate.root_span));
        return (checks, Vec::new());
    };
    let Some(root_schema) = schema.entity(root.id) else {
        diagnostics.push(ir_diagnostic(root.span));
        return (checks, Vec::new());
    };

    let mut accessed_fields = BTreeSet::new();
    for invariant in &aggregate.invariants {
        match invariant
            .expressions
            .dependencies(invariant.expression.id, invariant.expression.span)
        {
            Ok(dependencies) => {
                accessed_fields.extend(
                    dependencies
                        .schema_fields()
                        .iter()
                        .filter_map(|(entity, field)| (*entity == root.id).then_some(*field)),
                );
            }
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
    }
    let accessed_fields = accessed_fields.into_iter().collect::<Vec<_>>();

    let mut targets = Vec::<RootValidationTarget>::new();
    for binding in command.bindings.iter().filter(|binding| {
        matches!(
            binding.mode,
            BindingMode::Mutate | BindingMode::Create | BindingMode::Delete
        )
    }) {
        if binding.mode == BindingMode::Delete && binding.entity_id == aggregate.root {
            continue;
        }
        let Some((key_expressions, fingerprint)) =
            root_key_derivation(command, binding, root.key_fields.len())
        else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidBinding,
                binding.entity_span,
            ));
            continue;
        };
        if targets
            .iter()
            .any(|target| target.fingerprint == fingerprint)
        {
            continue;
        }
        targets.push(RootValidationTarget {
            source_binding: binding.id,
            key_expressions,
            fingerprint,
            span: binding.entity_span,
        });
    }

    let mut root_validation_reads = Vec::new();
    for target in targets {
        let source_root = command.bindings.iter().find(|binding| {
            binding.entity_id == root.id
                && root_key_derivation(command, binding, root.key_fields.len())
                    .is_some_and(|(_, fingerprint)| fingerprint == target.fingerprint)
        });
        let (replacement, source_bindings, root_validation_subjects) =
            if let Some(source_root) = source_root {
                (
                    SchemaReplacement::Binding(source_root.id),
                    vec![source_root.id],
                    Vec::new(),
                )
            } else {
                let Ok(index) = u32::try_from(root_validation_reads.len()) else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::BoundExceeded,
                        target.span,
                    ));
                    continue;
                };
                let id = RootValidationReadId::new(index);
                root_validation_reads.push(RawRootValidationRead {
                    id,
                    source_binding: target.source_binding,
                    entity_type: root.id,
                    key_schema: root_schema.primary_key().clone(),
                    key_expressions: target.key_expressions,
                    accessed_fields: accessed_fields.clone(),
                    span: target.span,
                });
                (SchemaReplacement::RootValidation(id), Vec::new(), vec![id])
            };
        for invariant in &aggregate.invariants {
            match append_schema_expression(
                &invariant.expressions,
                invariant.expression.id,
                expressions,
                replacement,
                invariant.expression.span,
            ) {
                Ok(predicate) => checks.push(RawCommitCheck {
                    invariant_id: invariant.id,
                    predicate,
                    source_bindings: source_bindings.clone(),
                    root_validation_reads: root_validation_subjects.clone(),
                    span: invariant.name_span,
                }),
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
    }
    (checks, root_validation_reads)
}

fn root_key_derivation(
    command: &HirCommand,
    binding: &HirBinding,
    root_key_field_count: usize,
) -> Option<(Vec<ExprId>, Vec<Vec<u8>>)> {
    let roots = binding
        .arguments
        .get(..root_key_field_count)?
        .iter()
        .map(|argument| argument.id)
        .collect::<Vec<_>>();
    let fingerprint = roots
        .iter()
        .map(|root| command_expression_fingerprint(&command.expressions, *root))
        .collect();
    Some((roots, fingerprint))
}

fn append_schema_expression(
    source: &HirExpressionArena,
    root: ExprId,
    target: &mut HirExpressionArena,
    replacement: SchemaReplacement<'_>,
    enclosing_span: Span,
) -> Result<ExprId, CompilerDiagnostic> {
    append_schema_expression_node(
        source,
        root,
        target,
        &replacement,
        &mut BTreeMap::new(),
        enclosing_span,
    )
}

fn append_schema_expression_node(
    source: &HirExpressionArena,
    id: ExprId,
    target: &mut HirExpressionArena,
    replacement: &SchemaReplacement<'_>,
    memo: &mut BTreeMap<ExprId, ExprId>,
    enclosing_span: Span,
) -> Result<ExprId, CompilerDiagnostic> {
    if let Some(mapped) = memo.get(&id).copied() {
        return Ok(mapped);
    }
    let node = source
        .node(id)
        .ok_or_else(|| ir_diagnostic(enclosing_span))?;
    if let ExpressionKind::SchemaField { field, .. } = node.kind {
        let mapped = match replacement {
            SchemaReplacement::Existing(replacements) => replacements
                .get(&field)
                .copied()
                .ok_or_else(|| ir_diagnostic(node.span))?,
            SchemaReplacement::Binding(binding) => push_hir_node(
                target,
                ExpressionKind::BoundField {
                    binding: *binding,
                    field,
                },
                node.value_type.clone(),
                node.span,
            )?,
            SchemaReplacement::RootValidation(read) => push_hir_node(
                target,
                ExpressionKind::RootValidationField { read: *read, field },
                node.value_type.clone(),
                node.span,
            )?,
        };
        memo.insert(id, mapped);
        return Ok(mapped);
    }
    let kind = match &node.kind {
        ExpressionKind::Unary { operator, operand } => ExpressionKind::Unary {
            operator: *operator,
            operand: append_schema_expression_node(
                source,
                *operand,
                target,
                replacement,
                memo,
                enclosing_span,
            )?,
        },
        ExpressionKind::Binary {
            operator,
            left,
            right,
        } => ExpressionKind::Binary {
            operator: *operator,
            left: append_schema_expression_node(
                source,
                *left,
                target,
                replacement,
                memo,
                enclosing_span,
            )?,
            right: append_schema_expression_node(
                source,
                *right,
                target,
                replacement,
                memo,
                enclosing_span,
            )?,
        },
        kind => kind.clone(),
    };
    let mapped = push_hir_node(target, kind, node.value_type.clone(), node.span)?;
    memo.insert(id, mapped);
    Ok(mapped)
}

fn push_hir_node(
    arena: &mut HirExpressionArena,
    kind: ExpressionKind,
    value_type: riffdb_contract_ir::ValueType,
    span: Span,
) -> Result<ExprId, CompilerDiagnostic> {
    let index = u32::try_from(arena.nodes.len())
        .map_err(|_| CompilerDiagnostic::new(CompilerDiagnosticCode::BoundExceeded, span))?;
    arena.nodes.push(HirExpressionNode {
        kind,
        value_type,
        span,
    });
    Ok(ExprId::new(index))
}

fn command_instructions(
    command: &HirCommand,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> (Vec<RawInstruction>, Option<(u32, usize)>) {
    let mut requirements = Vec::new();
    let rejection_base: usize = command
        .bindings
        .iter()
        .map(|binding| {
            1usize
                + usize::from(binding.restriction_failure.is_some())
                + usize::from(binding.cascade_failure.is_some())
        })
        .sum();
    for (index, requirement) in command.requirements.iter().enumerate() {
        let Ok(requirement_index) = u32::try_from(index) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::BoundExceeded,
                requirement.name_span,
            ));
            continue;
        };
        requirements.push(RawInstruction::Require {
            requirement_index,
            predicate: requirement.condition.id,
            rejection_occurrence: rejection_base + index,
        });
    }
    let mut effects = Vec::new();
    let mut effect_occurrence = rejection_base + command.requirements.len();
    for effect in &command.effects {
        match effect {
            HirEffect::Set {
                binding,
                field,
                value,
                ..
            } => effects.push(RawInstruction::SetField {
                binding: *binding,
                field: *field,
                value: value.id,
            }),
            HirEffect::Embed {
                binding,
                field,
                value,
                model_identity,
                model_version,
                ..
            } => effects.push(RawInstruction::SetEmbedding {
                binding: *binding,
                field: *field,
                value: value.id,
                model_identity: model_identity.id,
                model_version: model_version.id,
            }),
            HirEffect::Emit {
                event_id,
                fields,
                event_span,
                ..
            } => {
                let mut fields = fields
                    .iter()
                    .map(|field| FieldExpression::new(field.id, field.value.id))
                    .collect::<Vec<_>>();
                fields.sort_by_key(|field| field.field_id());
                effects.push(RawInstruction::EmitEvent {
                    event_id: *event_id,
                    fields,
                    span: *event_span,
                });
            }
            HirEffect::WorkflowTransition {
                binding,
                state_field,
                source_states,
                destination,
                expected_revision,
                ..
            } => {
                effects.push(RawInstruction::WorkflowTransition {
                    binding: *binding,
                    state_field: *state_field,
                    source_states: source_states.clone(),
                    destination: *destination,
                    expected_revision: expected_revision.id,
                    stale_occurrence: effect_occurrence,
                    illegal_occurrence: effect_occurrence + 1,
                });
                effect_occurrence += 2;
            }
            HirEffect::WorkflowLease {
                binding,
                owner_field,
                expiry_field,
                fencing_token_field,
                attempt_field,
                minimum_duration_seconds,
                maximum_duration_seconds,
                operation,
                ..
            } => {
                let base = effect_occurrence;
                let operation = match operation.as_ref() {
                    HirWorkflowLeaseOperation::Claim {
                        owner,
                        duration_seconds,
                        expected_revision,
                        ..
                    } => {
                        effect_occurrence += 4;
                        RawWorkflowLeaseOperation::Claim {
                            owner: owner.id,
                            duration_seconds: duration_seconds.id,
                            expected_revision: expected_revision.id,
                            outcomes: [base, base + 1, base + 2, base + 3],
                        }
                    }
                    HirWorkflowLeaseOperation::Renew {
                        owner,
                        fencing_token,
                        duration_seconds,
                        expected_revision,
                        ..
                    } => {
                        effect_occurrence += 4;
                        RawWorkflowLeaseOperation::Renew {
                            owner: owner.id,
                            fencing_token: fencing_token.id,
                            duration_seconds: duration_seconds.id,
                            expected_revision: expected_revision.id,
                            outcomes: [base, base + 1, base + 2, base + 3],
                        }
                    }
                    HirWorkflowLeaseOperation::Release {
                        owner,
                        fencing_token,
                        expected_revision,
                        ..
                    } => {
                        effect_occurrence += 2;
                        RawWorkflowLeaseOperation::Release {
                            owner: owner.id,
                            fencing_token: fencing_token.id,
                            expected_revision: expected_revision.id,
                            outcomes: [base, base + 1],
                        }
                    }
                    HirWorkflowLeaseOperation::Expire {
                        expected_revision, ..
                    } => {
                        effect_occurrence += 2;
                        RawWorkflowLeaseOperation::Expire {
                            expected_revision: expected_revision.id,
                            outcomes: [base, base + 1],
                        }
                    }
                    HirWorkflowLeaseOperation::Fence {
                        owner,
                        fencing_token,
                        expected_revision,
                        ..
                    } => {
                        effect_occurrence += 3;
                        RawWorkflowLeaseOperation::Fence {
                            owner: owner.id,
                            fencing_token: fencing_token.id,
                            expected_revision: expected_revision.id,
                            outcomes: [base, base + 1, base + 2],
                        }
                    }
                };
                effects.push(RawInstruction::WorkflowLease {
                    binding: *binding,
                    fields: WorkflowLeaseFields {
                        owner_field: *owner_field,
                        expiry_field: *expiry_field,
                        fencing_token_field: *fencing_token_field,
                        attempt_field: *attempt_field,
                        minimum_duration_seconds: *minimum_duration_seconds,
                        maximum_duration_seconds: *maximum_duration_seconds,
                    },
                    operation,
                });
            }
        }
    }
    if let Some(expansion) = &command.collection_expansion {
        if expansion.repeated_requirement_count > requirements.len()
            || expansion.repeated_effect_count > effects.len()
        {
            diagnostics.push(ir_diagnostic(expansion.span));
            return (Vec::new(), None);
        }
        let mut instructions = Vec::with_capacity(requirements.len() + effects.len());
        instructions.extend(requirements.drain(..expansion.repeated_requirement_count));
        instructions.extend(effects.drain(..expansion.repeated_effect_count));
        let instruction_count = instructions.len();
        instructions.extend(requirements);
        instructions.extend(effects);
        (instructions, Some((0, instruction_count)))
    } else {
        requirements.extend(effects);
        (requirements, None)
    }
}

fn collect_influential_roots(
    checks: &[RawCommitCheck],
    instructions: &[RawInstruction],
    outcomes: &[OutcomeConstruction],
) -> Vec<ExprId> {
    let mut roots = checks
        .iter()
        .map(|check| check.predicate)
        .collect::<Vec<_>>();
    for outcome in outcomes {
        roots.extend(
            outcome
                .payload()
                .fields()
                .iter()
                .map(|field| field.expression()),
        );
    }
    for instruction in instructions {
        match instruction {
            RawInstruction::Require { predicate, .. } => roots.push(*predicate),
            RawInstruction::SetField { value, .. } => roots.push(*value),
            RawInstruction::SetEmbedding {
                value,
                model_identity,
                model_version,
                ..
            } => roots.extend([*value, *model_identity, *model_version]),
            RawInstruction::WorkflowTransition {
                expected_revision, ..
            } => roots.push(*expected_revision),
            RawInstruction::WorkflowLease { operation, .. } => match operation {
                RawWorkflowLeaseOperation::Claim {
                    owner,
                    duration_seconds,
                    expected_revision,
                    ..
                } => roots.extend([*owner, *duration_seconds, *expected_revision]),
                RawWorkflowLeaseOperation::Renew {
                    owner,
                    fencing_token,
                    duration_seconds,
                    expected_revision,
                    ..
                } => roots.extend([
                    *owner,
                    *fencing_token,
                    *duration_seconds,
                    *expected_revision,
                ]),
                RawWorkflowLeaseOperation::Release {
                    owner,
                    fencing_token,
                    expected_revision,
                    ..
                }
                | RawWorkflowLeaseOperation::Fence {
                    owner,
                    fencing_token,
                    expected_revision,
                    ..
                } => roots.extend([*owner, *fencing_token, *expected_revision]),
                RawWorkflowLeaseOperation::Expire {
                    expected_revision, ..
                } => roots.push(*expected_revision),
            },
            RawInstruction::EmitEvent { fields, .. } => {
                roots.extend(fields.iter().map(|field| field.expression()));
            }
        }
    }
    roots
}

fn read_dependencies(
    arena: &riffdb_contract_ir::ExpressionArena,
    binding_count: usize,
    roots: &[ExprId],
    span: Span,
) -> Result<(Vec<BTreeSet<FieldId>>, Vec<bool>), CompilerDiagnostic> {
    let mut fields = vec![BTreeSet::new(); binding_count];
    let mut complete = vec![false; binding_count];
    for root in roots {
        let dependencies = arena.dependencies(*root).map_err(|_| ir_diagnostic(span))?;
        for binding in dependencies.complete_bindings() {
            let index = binding.get() as usize;
            if index >= complete.len() {
                return Err(ir_diagnostic(span));
            }
            complete[index] = true;
        }
        for (binding, field) in dependencies.bound_fields() {
            let index = binding.get() as usize;
            if index >= fields.len() {
                return Err(ir_diagnostic(span));
            }
            fields[index].insert(*field);
        }
    }
    Ok((fields, complete))
}

fn ir_diagnostic(span: Span) -> CompilerDiagnostic {
    CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidIr, span)
}

fn ir_error_diagnostic(error: IrValidationError, span: Span) -> CompilerDiagnostic {
    let code = match error {
        IrValidationError::LimitExceeded { .. } | IrValidationError::SizeOverflow { .. } => {
            CompilerDiagnosticCode::BoundExceeded
        }
        _ => CompilerDiagnosticCode::InvalidIr,
    };
    CompilerDiagnostic::new(code, span)
}

#[cfg(test)]
mod tests {
    use riffdb_contract_syntax::parse_contract;

    use super::*;
    use crate::hir::lower_contract_hir;
    use crate::schema_lowering::lower_schema;
    use crate::symbols::allocate_genesis_symbols;
    use crate::typecheck::resolve_declared_types;

    fn compile_commands(source: &str) -> Vec<CommandPlan> {
        let document = parse_contract(source).expect("syntax");
        let symbols = allocate_genesis_symbols(&document).expect("symbols");
        let types = resolve_declared_types(&document, &symbols).expect("types");
        let hir = lower_contract_hir(&document, &symbols, &types).expect("HIR");
        let schema = lower_schema(&hir).expect("schema");
        lower_commands(&hir, &schema).expect("commands")
    }

    #[test]
    fn canonical_budget_commands_lower_to_hashed_checked_plans() {
        let commands = compile_commands(include_str!("../../../contracts/examples/budget.riff"));
        assert_eq!(commands.len(), 2);
        assert!(
            commands
                .iter()
                .all(|plan| plan.plan_hash().as_bytes() != &[0; 32])
        );
        let create = commands
            .iter()
            .find(|plan| plan.name() == "CreateBudget")
            .expect("create plan");
        assert_eq!(create.commit_checks().len(), 2);
        assert_eq!(create.bindings().len(), 1);
    }

    #[test]
    fn external_reads_do_not_expand_mutation_conflict_derivation() {
        let source = r#"
contract Example version 1 {
  entity Account { key (tenant: uuid, account_id: uuid) }
  entity Entry { key (tenant: uuid, entry_id: uuid) }
  aggregate Accounts {
    root Account
    partition_by tenant
    conflict_key (tenant)
  }
  aggregate Entries {
    root Entry
    partition_by tenant
    conflict_key (tenant, entry_id)
  }
  command CreateEntry {
    input request_key: string<16>
    input tenant: uuid
    input account_id: uuid
    input entry_id: uuid
    idempotency_key request_key
    read Account(tenant, account_id) as account else MissingAccount {}
    create Entry(tenant, entry_id) as entry else EntryExists {}
    return Created { entry: entry }
  }
}
"#;
        let commands = compile_commands(source);
        let locality = commands[0].locality();
        assert_eq!(locality.conflict_keys().len(), 1);
        assert_eq!(
            locality.conflict_keys()[0].schema().components().len(),
            2,
            "the mutation aggregate, not the first external read, owns conflicts"
        );
    }

    #[test]
    fn multiple_binding_failures_preserve_ascending_id_priority_and_share_root_read() {
        let source = r#"
contract Example version 1 {
  entity Root {
    key (tenant: uuid, root_id: uuid)
    field total: i64
  }
  entity Child {
    key (tenant: uuid, root_id: uuid, child_id: uuid)
    field amount: i64
  }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
    invariant non_negative: total >= 0
    invariant root_exists: 1 == 1
  }
  command ChangeChildren {
    input request_key: string<128>
    input tenant: uuid
    input root_id: uuid
    input first_child: uuid
    input second_child: uuid
    input amount: i64
    idempotency_key request_key
    mutate Child(tenant, root_id, first_child) as first else MissingFirst {}
    mutate Child(tenant, root_id, second_child) as second else MissingSecond {}
    set first.amount = amount
    set second.amount = amount
    return Changed { first: first, second: second }
  }
}
"#;
        let commands = compile_commands(source);
        let plan = &commands[0];
        assert_eq!(plan.bindings()[0].id(), BindingId::new(0));
        assert_eq!(plan.bindings()[1].id(), BindingId::new(1));
        assert_ne!(
            plan.bindings()[0].failure().outcome_id(),
            plan.bindings()[1].failure().outcome_id()
        );
        assert_eq!(
            plan.outcomes()
                .iter()
                .find(|outcome| { outcome.id() == plan.bindings()[0].failure().outcome_id() })
                .expect("first failure outcome")
                .name(),
            "MissingFirst"
        );
        assert_eq!(
            plan.outcomes()
                .iter()
                .find(|outcome| { outcome.id() == plan.bindings()[1].failure().outcome_id() })
                .expect("second failure outcome")
                .name(),
            "MissingSecond"
        );
        assert_ne!(
            &plan.bindings()[0].key_expressions()[..2],
            &plan.bindings()[1].key_expressions()[..2]
        );
        assert_eq!(plan.root_validation_reads().len(), 1);
        let read = &plan.root_validation_reads()[0];
        assert_eq!(read.id(), RootValidationReadId::new(0));
        assert_eq!(read.source_binding(), BindingId::new(0));
        assert_eq!(read.key_expressions().len(), 2);
        assert_eq!(read.accessed_fields().len(), 1);
        assert_eq!(plan.commit_checks().len(), 2);
        assert!(plan.commit_checks().iter().all(|check| {
            check.source_bindings().is_empty()
                && check.root_validation_reads() == [RootValidationReadId::new(0)]
        }));
        assert!(plan.expressions().nodes().iter().any(|node| matches!(
            node.kind(),
            ExpressionKind::RootValidationField {
                read,
                ..
            } if *read == RootValidationReadId::new(0)
        )));
    }

    #[test]
    fn exact_source_root_binding_supplies_aggregate_invariant_subject() {
        let source = r#"
contract Example version 1 {
  entity Root {
    key (tenant: uuid, root_id: uuid)
    field total: i64
  }
  entity Child {
    key (tenant: uuid, root_id: uuid, child_id: uuid)
    field amount: i64
  }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
    invariant non_negative: total >= 0
  }
  command ChangeChild {
    input request_key: string<128>
    input tenant: uuid
    input root_id: uuid
    input child_id: uuid
    input amount: i64
    idempotency_key request_key
    read Root(tenant, root_id) as root_row else MissingRoot {}
    mutate Child(tenant, root_id, child_id) as child_row else MissingChild {}
    set child_row.amount = amount
    return Changed { root_row: root_row, child_row: child_row }
  }
}
"#;
        let commands = compile_commands(source);
        let plan = &commands[0];
        assert_ne!(
            plan.bindings()[0].key_expressions(),
            &plan.bindings()[1].key_expressions()[..2]
        );
        assert!(plan.root_validation_reads().is_empty());
        assert_eq!(plan.commit_checks().len(), 1);
        assert_eq!(
            plan.commit_checks()[0].source_bindings(),
            [BindingId::new(0)]
        );
        assert!(plan.commit_checks()[0].root_validation_reads().is_empty());
        assert!(plan.expressions().nodes().iter().any(|node| matches!(
            node.kind(),
            ExpressionKind::BoundField {
                binding,
                ..
            } if *binding == BindingId::new(0)
        )));
        assert!(
            !plan
                .expressions()
                .nodes()
                .iter()
                .any(|node| matches!(node.kind(), ExpressionKind::RootValidationField { .. }))
        );
    }

    #[test]
    fn distinct_root_derivations_receive_dense_source_order_read_ids() {
        let source = r#"
contract Example version 1 {
  entity Root {
    key (tenant: uuid, root_id: uuid)
    field total: i64
  }
  entity Child {
    key (tenant: uuid, root_id: uuid, child_id: uuid)
    field amount: i64
  }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
    invariant non_negative: total >= 0
  }
  command ChangeChildren {
    input request_key: string<128>
    input tenant: uuid
    input first_root: uuid
    input second_root: uuid
    input first_child: uuid
    input second_child: uuid
    input amount: i64
    idempotency_key request_key
    mutate Child(tenant, first_root, first_child) as first else MissingFirst {}
    mutate Child(tenant, second_root, second_child) as second else MissingSecond {}
    set first.amount = amount
    set second.amount = amount
    return Changed { first: first, second: second }
  }
}
"#;
        let commands = compile_commands(source);
        let plan = &commands[0];
        assert_eq!(plan.root_validation_reads().len(), 2);
        assert_eq!(
            plan.root_validation_reads()
                .iter()
                .map(|read| (read.id(), read.source_binding()))
                .collect::<Vec<_>>(),
            [
                (RootValidationReadId::new(0), BindingId::new(0)),
                (RootValidationReadId::new(1), BindingId::new(1)),
            ]
        );
        assert_eq!(plan.commit_checks().len(), 2);
        assert_eq!(
            plan.commit_checks()
                .iter()
                .flat_map(|check| check.root_validation_reads().iter().copied())
                .collect::<Vec<_>>(),
            [RootValidationReadId::new(0), RootValidationReadId::new(1)]
        );
    }

    #[test]
    fn constant_aggregate_invariant_keeps_root_subject_without_field_dependencies() {
        let source = r#"
contract Example version 1 {
  entity Root { key (tenant: uuid, root_id: uuid) }
  entity Child {
    key (tenant: uuid, root_id: uuid, child_id: uuid)
    field amount: i64
  }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
    invariant root_exists: 1 == 1
  }
  command ChangeChild {
    input request_key: string<128>
    input tenant: uuid
    input root_id: uuid
    input child_id: uuid
    input amount: i64
    idempotency_key request_key
    mutate Child(tenant, root_id, child_id) as child_row else MissingChild {}
    set child_row.amount = amount
    return Changed { child_row: child_row }
  }
}
"#;
        let commands = compile_commands(source);
        let plan = &commands[0];
        assert_eq!(plan.root_validation_reads().len(), 1);
        assert!(plan.root_validation_reads()[0].accessed_fields().is_empty());
        assert_eq!(plan.commit_checks().len(), 1);
        assert!(plan.commit_checks()[0].source_bindings().is_empty());
        assert_eq!(
            plan.commit_checks()[0].root_validation_reads(),
            [RootValidationReadId::new(0)]
        );
        assert!(
            !plan
                .expressions()
                .nodes()
                .iter()
                .any(|node| matches!(node.kind(), ExpressionKind::RootValidationField { .. }))
        );
    }
}
