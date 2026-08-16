//! Checked executable schema lowering from compiler-private typed HIR.

use riffdb_contract_ir::{
    AggregateKeyPlan, AggregateSchema, CascadeRelationshipSpecV1, DeletePolicySchemaV1,
    EntitySchema, EnumSchema, EnumVariantSchema, EventPartitionSchema, EventPolicyAnchorFieldV1,
    EventPolicyAnchorV1, EventSchema, FieldSchema, IndexSchema, InvariantPlan, KeyComponentSchema,
    KeyPurpose, KeySchema, RecordSchema, RecordTypeRef, RelationshipSchema, SchemaIr,
    UniqueKeySchema, ValueType, WorkflowLeaseSchema, WorkflowSchema, WorkflowTransitionSchema,
};
use riffdb_contract_syntax::Span;
use riffdb_types::{EnumTypeId, EnumVariantId};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::hir::{HirDeletePolicy, HirEffect, HirInvariant, TypedContractHir};

/// Lowers the complete checked workflow catalog into span-free IR.
pub(crate) fn lower_workflow_catalog(
    hir: &TypedContractHir,
) -> Result<Vec<WorkflowSchema>, CompilerDiagnostics> {
    hir.workflows
        .iter()
        .map(|workflow| {
            let transitions = workflow
                .transitions
                .iter()
                .map(|transition| {
                    WorkflowTransitionSchema::new(
                        transition.name.clone(),
                        transition.source_states.clone(),
                        transition.destination,
                    )
                    .map_err(|_| {
                        CompilerDiagnostics::single(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowTransition,
                            transition.span,
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let lease = workflow
                .lease
                .as_ref()
                .map(|lease| {
                    WorkflowLeaseSchema::new(
                        lease.name.clone(),
                        lease.owner_field,
                        lease.expiry_field,
                        lease.fencing_token_field,
                        lease.attempt_field,
                        lease.minimum_duration_seconds,
                        lease.maximum_duration_seconds,
                    )
                    .map_err(|_| {
                        CompilerDiagnostics::single(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidWorkflowLease,
                            lease.span,
                        ))
                    })
                })
                .transpose()?;
            WorkflowSchema::new(
                workflow.name.clone(),
                workflow.entity_id,
                workflow.state_field,
                workflow.state_enum,
                workflow.initial_state,
                transitions,
                lease,
            )
            .map_err(|_| {
                CompilerDiagnostics::single(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidWorkflow,
                    workflow.span,
                ))
            })
        })
        .collect()
}

/// Lowers the complete typed HIR schema into checked executable IR.
pub(crate) fn lower_schema(hir: &TypedContractHir) -> Result<SchemaIr, CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    validate_relationship_declarations(hir)?;
    validate_unique_declarations(hir)?;
    let enums = lower_enums(hir, &mut diagnostics);
    let entities = lower_entities(hir, &mut diagnostics);
    let aggregates = lower_aggregates(hir, &mut diagnostics);
    let events = lower_events(hir, &entities, &aggregates, &mut diagnostics);
    let relationships = lower_relationships(hir, &mut diagnostics);
    let unique_keys = lower_unique_keys(hir, &mut diagnostics);
    let delete_policies = lower_delete_policies(hir)?;
    if !diagnostics.is_empty() {
        return Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"));
    }
    let base = SchemaIr::with_integrity(
        entities.clone(),
        events.clone(),
        enums.clone(),
        aggregates.clone(),
        relationships.clone(),
        unique_keys.clone(),
    )
    .map_err(|_| CompilerDiagnostics::single(ir_diagnostic(hir.span)))?;
    for (policy, span) in &delete_policies {
        if matches!(
            policy.mode(),
            riffdb_contract_ir::DeletePolicyModeV1::Cascade { .. }
        ) {
            continue;
        }
        SchemaIr::with_integrity_and_delete_policies(
            entities.clone(),
            events.clone(),
            enums.clone(),
            aggregates.clone(),
            relationships.clone(),
            unique_keys.clone(),
            vec![policy.clone()],
        )
        .map_err(|_| {
            CompilerDiagnostics::single(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidDeletePolicy,
                *span,
            ))
        })?;
    }
    if let Some((_, cascade_span)) = delete_policies.iter().find(|(policy, _)| {
        matches!(
            policy.mode(),
            riffdb_contract_ir::DeletePolicyModeV1::Cascade { .. }
        )
    }) {
        SchemaIr::with_integrity_and_delete_policies(
            entities.clone(),
            events.clone(),
            enums.clone(),
            aggregates.clone(),
            relationships.clone(),
            unique_keys.clone(),
            delete_policies
                .iter()
                .map(|(policy, _)| policy.clone())
                .collect(),
        )
        .map_err(|_| {
            CompilerDiagnostics::single(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidDeletePolicy,
                *cascade_span,
            ))
        })?;
    }
    let vector_field_specs = hir
        .entities
        .iter()
        .flat_map(|entity| {
            entity.vector_fields.iter().map(|vector_field| {
                riffdb_contract_ir::VectorFieldSpecV1::new(
                    entity.id,
                    vector_field.field_id,
                    vector_field.metric,
                    vector_field.source_fields.clone(),
                    vector_field.stale_entity_count_threshold,
                )
                .map(|spec| (spec, vector_field.span))
                .map_err(|_| {
                    CompilerDiagnostics::single(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidType,
                        vector_field.span,
                    ))
                })
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let vector_ann_specs = hir
        .entities
        .iter()
        .flat_map(|entity| {
            entity.vector_fields.iter().filter_map(|vector_field| {
                let (Some(row_threshold), Some(recall_target_bps)) = (
                    vector_field.ann_row_threshold,
                    vector_field.recall_target_bps,
                ) else {
                    return None;
                };
                Some(
                    riffdb_contract_ir::VectorAnnSpecV1::new(
                        entity.id,
                        vector_field.field_id,
                        row_threshold,
                        recall_target_bps,
                    )
                    .map(|spec| (spec, vector_field.span))
                    .map_err(|_| {
                        CompilerDiagnostics::single(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidType,
                            vector_field.span,
                        ))
                    }),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Probe each spec individually so a schema-validation failure carries the
    // offending `vector_field`'s span, not the whole-contract span (the same
    // per-item probe the delete-policy path above uses).
    for (spec, span) in &vector_field_specs {
        base.clone()
            .with_vector_field_specs(vec![spec.clone()])
            .map_err(|_| {
                CompilerDiagnostics::single(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidType,
                    *span,
                ))
            })?;
    }
    let schema = if delete_policies.is_empty() {
        base
    } else {
        SchemaIr::with_integrity_and_delete_policies(
            entities,
            events,
            enums,
            aggregates,
            relationships,
            unique_keys,
            delete_policies
                .into_iter()
                .map(|(policy, _)| policy)
                .collect(),
        )
        .map_err(|_| CompilerDiagnostics::single(ir_diagnostic(hir.span)))?
    };
    // Secret-field classifications (ADR-0118). Duplicate names and unknown
    // fields are already rejected by symbol resolution, and the grammar keeps
    // the modifier off key fields, so schema attachment cannot fail for a
    // source-derived spec; the contract-span fallback covers programmatic
    // invariance only.
    let secret_field_specs = hir
        .entities
        .iter()
        .flat_map(|entity| {
            entity
                .fields
                .iter()
                .filter(|field| field.secret_span.is_some())
                .map(|field| riffdb_contract_ir::SecretFieldSpecV1::new(entity.id, field.id))
        })
        .collect::<Vec<_>>();
    // Cross-spec failures (duplicate specs for one field) have no single
    // offending declaration; only those fall back to the contract span.
    schema
        .with_vector_field_specs(
            vector_field_specs
                .into_iter()
                .map(|(spec, _)| spec)
                .collect(),
        )
        .and_then(|schema| schema.with_secret_field_specs(secret_field_specs))
        .and_then(|schema| {
            schema
                .with_vector_ann_specs(vector_ann_specs.into_iter().map(|(spec, _)| spec).collect())
        })
        .map_err(|_| CompilerDiagnostics::single(ir_diagnostic(hir.span)))
}

fn lower_delete_policies(
    hir: &TypedContractHir,
) -> Result<Vec<(DeletePolicySchemaV1, Span)>, CompilerDiagnostics> {
    hir.entities
        .iter()
        .filter_map(|entity| match entity.delete_policy.as_ref()? {
            HirDeletePolicy::NoInbound { span } => {
                Some(Ok((DeletePolicySchemaV1::no_inbound(entity.id), *span)))
            }
            HirDeletePolicy::Restrict {
                span,
                source_entity,
                index_id,
            } => Some(Ok((
                DeletePolicySchemaV1::restrict(entity.id, *source_entity, *index_id),
                *span,
            ))),
            HirDeletePolicy::Cascade {
                span,
                relationships,
            } => {
                let entries = relationships
                    .iter()
                    .map(|relationship| {
                        CascadeRelationshipSpecV1::new(
                            relationship.source_entity,
                            relationship.relationship_name.clone(),
                            relationship.index_id,
                            relationship.maximum as u16,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| {
                        CompilerDiagnostics::single(CompilerDiagnostic::new(
                            CompilerDiagnosticCode::InvalidDeletePolicy,
                            *span,
                        ))
                    });
                Some(entries.and_then(|mut entries| {
                    entries.sort_by(|left, right| {
                        left.source_entity()
                            .cmp(&right.source_entity())
                            .then_with(|| left.relationship_name().cmp(right.relationship_name()))
                    });
                    DeletePolicySchemaV1::cascade(entity.id, entries)
                        .map(|policy| (policy, *span))
                        .map_err(|_| {
                            CompilerDiagnostics::single(CompilerDiagnostic::new(
                                CompilerDiagnosticCode::InvalidDeletePolicy,
                                *span,
                            ))
                        })
                }))
            }
        })
        .collect()
}

pub(crate) fn validate_unique_declarations(
    hir: &TypedContractHir,
) -> Result<(), CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    for entity in &hir.entities {
        let Some(owner) = hir.aggregate_for_entity(entity.id) else {
            continue;
        };
        let Some(root) = hir.entity(owner.root) else {
            continue;
        };
        let Some(route) = aggregate_partition_route_fields(owner, root, entity) else {
            continue;
        };
        for unique in entity.indexes.iter().filter(|index| index.unique) {
            let fields = unique
                .fields
                .iter()
                .map(|field| field.0)
                .collect::<Vec<_>>();
            let canonical_route = fields
                .get(..route.len())
                .is_some_and(|prefix| prefix == route.as_slice());
            let required_key_types = unique.fields.iter().all(|(field_id, _)| {
                entity
                    .fields
                    .iter()
                    .find(|field| field.id == *field_id)
                    .is_some_and(|field| {
                        !field.value_type.is_optional()
                            && key_component(field.value_type.clone(), hir).is_ok()
                    })
            });
            if !canonical_route || !required_key_types {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidUniqueKey,
                    unique.span,
                ));
            }
        }
    }
    if diagnostics.is_empty() {
        Ok(())
    } else {
        Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"))
    }
}

fn aggregate_partition_route_fields(
    owner: &crate::hir::HirAggregate,
    root: &crate::hir::HirEntity,
    entity: &crate::hir::HirEntity,
) -> Option<Vec<riffdb_types::FieldId>> {
    let mut dependencies = std::collections::BTreeSet::new();
    let mut pending = vec![owner.keys.partition.id];
    let mut visited = std::collections::BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let node = owner.keys.expressions.node(id)?;
        match &node.kind {
            riffdb_contract_ir::ExpressionKind::SchemaField { entity_type, field }
                if *entity_type == root.id =>
            {
                dependencies.insert(*field);
            }
            riffdb_contract_ir::ExpressionKind::Unary { operand, .. } => pending.push(*operand),
            riffdb_contract_ir::ExpressionKind::Binary { left, right, .. } => {
                pending.push(*right);
                pending.push(*left);
            }
            riffdb_contract_ir::ExpressionKind::Constant(_) => {}
            _ => return None,
        }
    }
    root.key_fields
        .iter()
        .enumerate()
        .filter(|(_, field)| dependencies.contains(field))
        .map(|(position, _)| entity.key_fields.get(position).copied())
        .collect()
}

pub(crate) fn validate_relationship_declarations(
    hir: &TypedContractHir,
) -> Result<(), CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    for source in &hir.entities {
        let mut names = std::collections::BTreeSet::new();
        for relationship in &source.relationships {
            let Some(target) = hir.entity(relationship.target_entity) else {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidRelationship,
                    relationship.target_entity_span,
                ));
                continue;
            };
            let valid_name = names.insert(relationship.name.as_str());
            let complete_target = relationship
                .target_fields
                .iter()
                .map(|field| field.0)
                .eq(target.key_fields.iter().copied());
            let matching_arity =
                relationship.source_fields.len() == relationship.target_fields.len();
            let exact_types = matching_arity
                && relationship
                    .source_fields
                    .iter()
                    .zip(&relationship.target_fields)
                    .all(|(source_field, target_field)| {
                        source
                            .fields
                            .iter()
                            .find(|field| field.id == source_field.0)
                            .zip(
                                target
                                    .fields
                                    .iter()
                                    .find(|field| field.id == target_field.0),
                            )
                            .is_some_and(|(source, target)| {
                                !source.value_type.is_optional()
                                    && source.value_type == target.value_type
                            })
                    });
            let same_partition = hir
                .aggregate_for_entity(source.id)
                .zip(hir.aggregate_for_entity(target.id))
                .is_some_and(|(source_owner, target_owner)| {
                    relationship_partitions_match(
                        hir,
                        source,
                        target,
                        relationship,
                        source_owner,
                        target_owner,
                    )
                });
            if !(valid_name && complete_target && matching_arity && exact_types && same_partition) {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidRelationship,
                    relationship.name_span,
                ));
            }
        }
    }
    if diagnostics.is_empty() {
        Ok(())
    } else {
        Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"))
    }
}

fn relationship_partitions_match(
    hir: &TypedContractHir,
    source: &crate::hir::HirEntity,
    target: &crate::hir::HirEntity,
    relationship: &crate::hir::HirRelationship,
    source_owner: &crate::hir::HirAggregate,
    target_owner: &crate::hir::HirAggregate,
) -> bool {
    let Some(source_root) = hir.entity(source_owner.root) else {
        return false;
    };
    let Some(target_root) = hir.entity(target_owner.root) else {
        return false;
    };
    let mut pending = vec![(
        source_owner.keys.partition.id,
        target_owner.keys.partition.id,
    )];
    let mut visited = std::collections::BTreeSet::new();
    while let Some((source_id, target_id)) = pending.pop() {
        if !visited.insert((source_id, target_id)) {
            continue;
        }
        let Some(source_node) = source_owner.keys.expressions.node(source_id) else {
            return false;
        };
        let Some(target_node) = target_owner.keys.expressions.node(target_id) else {
            return false;
        };
        if source_node.value_type != target_node.value_type {
            return false;
        }
        match (&source_node.kind, &target_node.kind) {
            (
                riffdb_contract_ir::ExpressionKind::Constant(left),
                riffdb_contract_ir::ExpressionKind::Constant(right),
            ) if left == right => {}
            (
                riffdb_contract_ir::ExpressionKind::SchemaField {
                    entity_type: left_entity,
                    field: left_field,
                },
                riffdb_contract_ir::ExpressionKind::SchemaField {
                    entity_type: right_entity,
                    field: right_field,
                },
            ) => {
                if *left_entity != source_root.id || *right_entity != target_root.id {
                    return false;
                }
                let Some(left_position) = source_root
                    .key_fields
                    .iter()
                    .position(|candidate| candidate == left_field)
                else {
                    return false;
                };
                let Some(source_partition_field) = source.key_fields.get(left_position).copied()
                else {
                    return false;
                };
                let Some(right_position) = target_root
                    .key_fields
                    .iter()
                    .position(|candidate| candidate == right_field)
                else {
                    return false;
                };
                let Some(target_partition_field) = target.key_fields.get(right_position).copied()
                else {
                    return false;
                };
                let Some(mapped_position) = relationship
                    .target_fields
                    .iter()
                    .position(|(field, _)| *field == target_partition_field)
                else {
                    return false;
                };
                if relationship.source_fields[mapped_position].0 != source_partition_field {
                    return false;
                }
            }
            (
                riffdb_contract_ir::ExpressionKind::Unary {
                    operator: left_operator,
                    operand: left_operand,
                },
                riffdb_contract_ir::ExpressionKind::Unary {
                    operator: right_operator,
                    operand: right_operand,
                },
            ) if left_operator == right_operator => pending.push((*left_operand, *right_operand)),
            (
                riffdb_contract_ir::ExpressionKind::Binary {
                    operator: left_operator,
                    left: left_left,
                    right: left_right,
                },
                riffdb_contract_ir::ExpressionKind::Binary {
                    operator: right_operator,
                    left: right_left,
                    right: right_right,
                },
            ) if left_operator == right_operator => {
                pending.push((*left_left, *right_left));
                pending.push((*left_right, *right_right));
            }
            _ => return false,
        }
    }
    true
}

fn lower_relationships(
    hir: &TypedContractHir,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<RelationshipSchema> {
    let mut lowered = Vec::new();
    for entity in &hir.entities {
        for relationship in &entity.relationships {
            match RelationshipSchema::new(
                relationship.name.clone(),
                entity.id,
                relationship
                    .source_fields
                    .iter()
                    .map(|field| field.0)
                    .collect(),
                relationship.target_entity,
                relationship
                    .target_fields
                    .iter()
                    .map(|field| field.0)
                    .collect(),
            ) {
                Ok(relationship) => lowered.push(relationship),
                Err(_) => diagnostics.push(ir_diagnostic(relationship.name_span)),
            }
        }
    }
    lowered
}

fn lower_unique_keys(
    hir: &TypedContractHir,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<UniqueKeySchema> {
    let mut lowered = Vec::new();
    for entity in &hir.entities {
        for unique in entity.indexes.iter().filter(|index| index.unique) {
            match UniqueKeySchema::new(
                unique.name.clone(),
                entity.id,
                unique.id,
                unique.fields.iter().map(|field| field.0).collect(),
            ) {
                Ok(unique) => lowered.push(unique),
                Err(_) => diagnostics.push(ir_diagnostic(unique.span)),
            }
        }
    }
    lowered
}

fn lower_enums(
    hir: &TypedContractHir,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<EnumSchema> {
    hir.enums
        .iter()
        .filter_map(|enumeration| {
            let variants = enumeration
                .variants
                .iter()
                .filter_map(|variant| {
                    EnumVariantSchema::new(variant.id, variant.name.clone())
                        .map_err(|_| ir_diagnostic(variant.span))
                        .map_err(|diagnostic| diagnostics.push(diagnostic))
                        .ok()
                })
                .collect();
            EnumSchema::new(enumeration.id, enumeration.name.clone(), variants)
                .map_err(|_| ir_diagnostic(enumeration.span))
                .map_err(|diagnostic| diagnostics.push(diagnostic))
                .ok()
        })
        .collect()
}

fn lower_entities(
    hir: &TypedContractHir,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<EntitySchema> {
    let mut result = Vec::new();
    for entity in &hir.entities {
        let record_fields = entity
            .fields
            .iter()
            .filter_map(|field| {
                FieldSchema::new(field.id, field.name.clone(), field.value_type.clone())
                    .map_err(|_| ir_diagnostic(field.name_span))
                    .map_err(|diagnostic| diagnostics.push(diagnostic))
                    .ok()
            })
            .collect();
        let Ok(record) = RecordSchema::new(RecordTypeRef::Entity(entity.id), record_fields) else {
            diagnostics.push(ir_diagnostic(entity.span));
            continue;
        };
        let mut key_components = Vec::new();
        for field_id in &entity.key_fields {
            let Some(field) = entity.fields.iter().find(|field| field.id == *field_id) else {
                diagnostics.push(ir_diagnostic(entity.span));
                continue;
            };
            match key_component(field.value_type.clone(), hir) {
                Ok(component) => key_components.push(component),
                Err(code) => diagnostics.push(CompilerDiagnostic::new(code, field.type_span)),
            }
        }
        let Ok(primary_key) = KeySchema::new(KeyPurpose::Entity(entity.id), key_components) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::BoundExceeded,
                entity.span,
            ));
            continue;
        };
        let invariants = entity
            .invariants
            .iter()
            .filter_map(|invariant| lower_invariant(invariant, diagnostics))
            .collect();
        let mut indexes = Vec::new();
        for index in &entity.indexes {
            let mut components = Vec::new();
            for ((field_id, span), encoding) in index.fields.iter().zip(&index.encodings) {
                let Some(field) = entity.fields.iter().find(|field| field.id == *field_id) else {
                    diagnostics.push(ir_diagnostic(*span));
                    continue;
                };
                match operational_index_components(field.value_type.clone(), *encoding, hir) {
                    Ok(lowered) => components.extend(lowered),
                    Err(code) => diagnostics.push(CompilerDiagnostic::new(code, *span)),
                }
            }
            let key_schema = KeySchema::index(index.id, entity.id, components, primary_key.clone());
            match key_schema.and_then(|key_schema| {
                IndexSchema::with_encodings(
                    index.id,
                    index.name.clone(),
                    index.fields.iter().map(|field| field.0).collect(),
                    index.encodings.clone(),
                    key_schema,
                )
            }) {
                Ok(schema) => indexes.push(schema),
                Err(_) => diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::BoundExceeded,
                    index.span,
                )),
            }
        }
        match EntitySchema::new(
            entity.id,
            entity.name.clone(),
            record,
            entity.key_fields.clone(),
            primary_key,
            invariants,
            indexes,
        ) {
            Ok(schema) => result.push(schema),
            Err(_) => diagnostics.push(ir_diagnostic(entity.span)),
        }
    }
    result
}

fn lower_events(
    hir: &TypedContractHir,
    entities: &[EntitySchema],
    aggregates: &[AggregateSchema],
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<EventSchema> {
    let mut result = Vec::new();
    for event in &hir.events {
        let fields = event
            .fields
            .iter()
            .filter_map(|field| {
                FieldSchema::new(field.id, field.name.clone(), field.value_type.clone())
                    .map_err(|_| ir_diagnostic(field.name_span))
                    .map_err(|diagnostic| diagnostics.push(diagnostic))
                    .ok()
            })
            .collect();
        let Ok(record) = RecordSchema::new(RecordTypeRef::Event(event.id), fields) else {
            diagnostics.push(ir_diagnostic(event.span));
            continue;
        };
        let Some(partition_span) = event.partition_span else {
            match EventSchema::new(event.id, event.name.clone(), record) {
                Ok(schema) => result.push(schema),
                Err(_) => diagnostics.push(ir_diagnostic(event.span)),
            }
            continue;
        };

        let mut aggregate_id = None;
        for command in &hir.commands {
            for effect in &command.effects {
                let HirEffect::Emit {
                    event_id,
                    event_span,
                    ..
                } = effect
                else {
                    continue;
                };
                if *event_id != event.id {
                    continue;
                }
                let command_aggregate = command
                    .bindings
                    .iter()
                    .find(|binding| {
                        matches!(
                            binding.mode,
                            riffdb_contract_ir::BindingMode::Mutate
                                | riffdb_contract_ir::BindingMode::Create
                        )
                    })
                    .or_else(|| command.bindings.first())
                    .and_then(|binding| hir.aggregate_for_entity(binding.entity_id))
                    .map(|aggregate| aggregate.id);
                let Some(command_aggregate) = command_aggregate else {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidEvent,
                        *event_span,
                    ));
                    continue;
                };
                if aggregate_id.is_some_and(|expected| expected != command_aggregate) {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::CrossPartitionMutation,
                        *event_span,
                    ));
                } else {
                    aggregate_id = Some(command_aggregate);
                }
            }
        }
        let Some(aggregate) =
            aggregate_id.and_then(|id| aggregates.iter().find(|aggregate| aggregate.id() == id))
        else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidEvent,
                partition_span,
            ));
            continue;
        };
        let partition = EventPartitionSchema::new(
            event
                .partition_fields
                .iter()
                .map(|(field, _)| *field)
                .collect(),
            aggregate.keys().partition_schema().clone(),
            &record,
        );
        match partition
            .and_then(|partition| {
                EventSchema::partitioned(event.id, event.name.clone(), record, partition)
            })
            .and_then(|schema| {
                let Some(anchor) = &event.policy_anchor else {
                    return Ok(schema);
                };
                let entity = entities
                    .iter()
                    .find(|entity| entity.id() == anchor.source_entity)
                    .ok_or(riffdb_contract_ir::IrValidationError::InvalidReference {
                        kind: "event policy anchor entity",
                    })?;
                let partition = schema.partition().ok_or(
                    riffdb_contract_ir::IrValidationError::InvalidReference {
                        kind: "event policy anchor partition",
                    },
                )?;
                let checked = EventPolicyAnchorV1::new(
                    anchor.source_entity,
                    anchor
                        .key_fields
                        .iter()
                        .map(|(source, payload)| EventPolicyAnchorFieldV1::new(*source, *payload))
                        .collect(),
                    anchor.read_policy.clone(),
                    entity,
                    schema.payload(),
                    partition,
                )?;
                schema.with_policy_anchor(checked, entity)
            }) {
            Ok(schema) => result.push(schema),
            Err(_) => diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidEvent,
                event
                    .policy_anchor
                    .as_ref()
                    .map_or(partition_span, |anchor| anchor.span),
            )),
        }
    }
    result
}

fn lower_aggregates(
    hir: &TypedContractHir,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Vec<AggregateSchema> {
    let mut result = Vec::new();
    for aggregate in &hir.aggregates {
        let arena = match aggregate.keys.expressions.to_ir(aggregate.span) {
            Ok(arena) => arena,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                continue;
            }
        };
        let partition_component =
            match key_component(aggregate.keys.partition.value_type.clone(), hir) {
                Ok(component) => component,
                Err(code) => {
                    diagnostics.push(CompilerDiagnostic::new(code, aggregate.keys.partition.span));
                    continue;
                }
            };
        let mut conflict_components = Vec::new();
        for conflict in &aggregate.keys.conflicts {
            match key_component(conflict.value_type.clone(), hir) {
                Ok(component) => conflict_components.push(component),
                Err(code) => diagnostics.push(CompilerDiagnostic::new(code, conflict.span)),
            }
        }
        let keys = KeySchema::new(
            KeyPurpose::Partition(aggregate.id),
            vec![partition_component],
        )
        .and_then(|partition_schema| {
            KeySchema::new(KeyPurpose::Conflict(aggregate.id), conflict_components).and_then(
                |conflict_schema| {
                    AggregateKeyPlan::new(
                        arena,
                        aggregate.keys.partition.id,
                        aggregate
                            .keys
                            .conflicts
                            .iter()
                            .map(|root| root.id)
                            .collect(),
                        partition_schema,
                        conflict_schema,
                    )
                },
            )
        });
        let Ok(keys) = keys else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::BoundExceeded,
                aggregate.span,
            ));
            continue;
        };
        let invariants = aggregate
            .invariants
            .iter()
            .filter_map(|invariant| lower_invariant(invariant, diagnostics))
            .collect();
        match AggregateSchema::new(
            aggregate.id,
            aggregate.name.clone(),
            aggregate.root,
            aggregate.children.iter().map(|child| child.0).collect(),
            keys,
            invariants,
        ) {
            Ok(schema) => result.push(schema),
            Err(_) => diagnostics.push(ir_diagnostic(aggregate.span)),
        }
    }
    result
}

fn lower_invariant(
    invariant: &HirInvariant,
    diagnostics: &mut Vec<CompilerDiagnostic>,
) -> Option<InvariantPlan> {
    let arena = invariant
        .expressions
        .to_ir(invariant.expression.span)
        .map_err(|diagnostic| diagnostics.push(diagnostic))
        .ok()?;
    InvariantPlan::new(
        invariant.id,
        invariant.name.clone(),
        arena,
        invariant.expression.id,
    )
    .map_err(|_| ir_diagnostic(invariant.name_span))
    .map_err(|diagnostic| diagnostics.push(diagnostic))
    .ok()
}

fn key_component(
    value_type: ValueType,
    hir: &TypedContractHir,
) -> Result<KeyComponentSchema, CompilerDiagnosticCode> {
    let variants = match value_type.enum_type_id() {
        Some(enum_id) => enum_variants(enum_id, hir),
        None => Vec::new(),
    };
    KeyComponentSchema::new(value_type, variants).map_err(|error| match error {
        riffdb_contract_ir::IrValidationError::LimitExceeded { .. }
        | riffdb_contract_ir::IrValidationError::SizeOverflow { .. } => {
            CompilerDiagnosticCode::BoundExceeded
        }
        _ => CompilerDiagnosticCode::InvalidType,
    })
}

fn operational_index_components(
    logical: ValueType,
    encoding: riffdb_contract_ir::IndexFieldEncodingV1,
    hir: &TypedContractHir,
) -> Result<Vec<KeyComponentSchema>, CompilerDiagnosticCode> {
    use riffdb_contract_ir::{
        IndexFieldEncodingV1, TextKeyProfileV1, UNICODE_FOLD_V1_MAXIMUM_EXPANSION,
    };

    match encoding {
        IndexFieldEncodingV1::Canonical => key_component(logical, hir).map(|value| vec![value]),
        IndexFieldEncodingV1::Presence => {
            let inner = logical
                .optional_inner()
                .filter(|inner| inner.is_authoritative_key_scalar())
                .ok_or(CompilerDiagnosticCode::InvalidType)?;
            Ok(vec![
                key_component(ValueType::u64(), hir)?,
                key_component(inner.clone(), hir)?,
            ])
        }
        IndexFieldEncodingV1::TextKey(profile) => {
            let maximum = logical
                .byte_bound()
                .filter(|_| logical.tag() == riffdb_contract_ir::ValueTypeTag::String)
                .ok_or(CompilerDiagnosticCode::InvalidType)?;
            let maximum = match profile {
                TextKeyProfileV1::BinaryUtf8 => maximum,
                TextKeyProfileV1::UnicodeFold => {
                    let _reserved_bound = maximum
                        .checked_mul(UNICODE_FOLD_V1_MAXIMUM_EXPANSION)
                        .ok_or(CompilerDiagnosticCode::BoundExceeded)?;
                    return Err(CompilerDiagnosticCode::InvalidType);
                }
            };
            riffdb_contract_ir::KeyComponentSchema::ordered_bytes(maximum)
                .map_err(|_| CompilerDiagnosticCode::BoundExceeded)
                .map(|value| vec![value])
        }
    }
}

fn enum_variants(enum_id: EnumTypeId, hir: &TypedContractHir) -> Vec<EnumVariantId> {
    hir.enums
        .iter()
        .find(|enumeration| enumeration.id == enum_id)
        .map(|enumeration| {
            enumeration
                .variants
                .iter()
                .map(|variant| variant.id)
                .collect()
        })
        .unwrap_or_default()
}

fn ir_diagnostic(span: Span) -> CompilerDiagnostic {
    CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidIr, span)
}

#[cfg(test)]
mod tests {
    use riffdb_contract_syntax::parse_contract;

    use super::*;
    use crate::hir::lower_contract_hir;
    use crate::symbols::allocate_genesis_symbols;
    use crate::typecheck::resolve_declared_types;

    fn hir(source: &str) -> TypedContractHir {
        let document = parse_contract(source).expect("syntax");
        let symbols = allocate_genesis_symbols(&document).expect("symbols");
        let types = resolve_declared_types(&document, &symbols).expect("types");
        lower_contract_hir(&document, &symbols, &types).expect("HIR")
    }

    #[test]
    fn canonical_budget_lowers_structural_schema_and_aggregate_keys() {
        let hir = hir(include_str!("../../../contracts/examples/budget.riff"));
        let schema = lower_schema(&hir).expect("schema");
        assert_eq!(schema.entities().len(), 1);
        assert_eq!(schema.events().len(), 1);
        assert_eq!(schema.aggregates().len(), 1);
        assert_eq!(schema.entities()[0].invariants().len(), 2);
        assert_eq!(
            schema.aggregates()[0].keys().conflict_expressions().len(),
            2
        );
    }

    #[test]
    fn decimal_authoritative_key_rejects_at_declared_type_span() {
        let source = r#"
contract Invalid version 1 {
  entity Row { key (amount: decimal<8,2>) }
}
"#;
        let hir = hir(source);
        let diagnostics = lower_schema(&hir).expect_err("rejects");
        let diagnostic = diagnostics
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidType)
            .expect("invalid type");
        let start = source.find("decimal<8,2>").expect("type start");
        assert_eq!(
            diagnostic.primary_span(),
            Span::new(start, start + 12).expect("span")
        );
    }

    #[test]
    fn vector_field_compiles_to_vector_typed_entity_field() {
        let source = r#"
contract Docs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: string<65536>
    vector_field embedding(1536, cosine, (title, body), staleness_slo 60)
  }
}
"#;
        let hir = hir(source);
        let schema = lower_schema(&hir).expect("schema");
        assert_eq!(schema.entities().len(), 1);
        let entity = &schema.entities()[0];
        // key fields (org_id, doc_id) + regular fields (title, body) + vector field (embedding) = 5
        assert_eq!(entity.record().fields().len(), 5);
        let embedding_field = entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "embedding")
            .expect("embedding field present");
        assert_eq!(
            embedding_field.value_type().tag(),
            riffdb_contract_ir::ValueTypeTag::Vector
        );
        assert_eq!(
            embedding_field
                .value_type()
                .vector_dimension()
                .map(|d| d.get()),
            Some(1536)
        );
    }

    /// Lowers a vector_field contract expecting rejection, returning the
    /// diagnostic matching `code` at `spanned` (AGENTS.md: every compiler
    /// diagnostic needs a source-span snapshot plus semantic assertion).
    fn vector_rejection(source: &str, code: CompilerDiagnosticCode, spanned: &str) -> Span {
        let document = parse_contract(source).expect("syntax");
        let symbols = allocate_genesis_symbols(&document).expect("symbols");
        let types = resolve_declared_types(&document, &symbols).expect("types");
        let diagnostics = lower_contract_hir(&document, &symbols, &types).expect_err("rejects");
        let start = source.find(spanned).expect("spanned text present");
        let expected = Span::new(start, start + spanned.len()).expect("span");
        let found = diagnostics
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == code && diagnostic.primary_span() == expected)
            .unwrap_or_else(|| panic!("expected {code:?} at {expected:?}; found {diagnostics:?}"));
        found.primary_span()
    }

    #[test]
    fn vector_field_rejects_zero_dimension_at_its_span() {
        vector_rejection(
            r#"
contract Invalid version 1 {
  entity Document {
    key (doc_id: uuid)
    field title: string<256>
    vector_field embedding(0, cosine, (title), staleness_slo 60)
  }
}
"#,
            CompilerDiagnosticCode::BoundExceeded,
            "0",
        );
    }

    #[test]
    fn vector_field_rejects_oversized_dimension_at_its_span() {
        vector_rejection(
            r#"
contract Invalid version 1 {
  entity Document {
    key (doc_id: uuid)
    field title: string<256>
    vector_field embedding(4097, cosine, (title), staleness_slo 60)
  }
}
"#,
            CompilerDiagnosticCode::BoundExceeded,
            "4097",
        );
    }

    #[test]
    fn vector_field_rejects_zero_staleness_slo_at_its_span() {
        vector_rejection(
            r#"
contract Invalid version 1 {
  entity Document {
    key (doc_id: uuid)
    field title: string<256>
    vector_field embedding(128, cosine, (title), staleness_slo 0)
  }
}
"#,
            CompilerDiagnosticCode::BoundExceeded,
            "0",
        );
    }

    #[test]
    fn vector_field_rejects_an_empty_source_field_list_in_the_grammar() {
        // An empty source-field list is unrepresentable: the grammar requires
        // at least one identifier, so rejection happens at parse time (the
        // HIR's MissingDeclaration check remains as a defensive second layer).
        let source = r#"
contract Invalid version 1 {
  entity Document {
    key (doc_id: uuid)
    field title: string<256>
    vector_field embedding(128, cosine, (), staleness_slo 60)
  }
}
"#;
        assert!(parse_contract(source).is_err());
    }

    #[test]
    fn vector_field_rejects_unknown_source_field_at_its_span() {
        vector_rejection(
            r#"
contract Invalid version 1 {
  entity Document {
    key (doc_id: uuid)
    field title: string<256>
    vector_field embedding(1536, cosine, (title, nonexistent), staleness_slo 60)
  }
}
"#,
            CompilerDiagnosticCode::UnknownName,
            "nonexistent",
        );
    }

    /// A repeated source field is a diagnostic at the DUPLICATE occurrence's
    /// span, with the first occurrence as the related span. The gate is
    /// symbol allocation (`validate_unique_spanned_names`), which has
    /// rejected this since WP-591 — so the HIR-level `dedup()` this fix
    /// round replaced with a diagnostic was dead masking, not a reachable
    /// silent mutation. This test pins the gate, which previously had no
    /// vector-source-field coverage at all.
    #[test]
    fn vector_field_rejects_duplicate_source_fields_at_the_duplicate_span() {
        let source = r#"
contract Invalid version 1 {
  entity Document {
    key (doc_id: uuid)
    field title: string<256>
    field body: string<65536>
    vector_field embedding(1536, cosine, (title, body, title), staleness_slo 60)
  }
}
"#;
        let document = parse_contract(source).expect("syntax");
        let diagnostics = allocate_genesis_symbols(&document)
            .expect_err("a duplicate source field must be rejected at symbol allocation");
        // The diagnostic points at the SECOND `title` (the last occurrence
        // in the source), with the first occurrence related.
        let duplicate_start = source.rfind("title").expect("duplicate present");
        let expected = Span::new(duplicate_start, duplicate_start + "title".len()).expect("span");
        let first_start = source.find("(title").expect("first occurrence present") + 1;
        let related = Span::new(first_start, first_start + "title".len()).expect("span");
        assert!(
            diagnostics.as_slice().iter().any(|diagnostic| {
                diagnostic.code() == CompilerDiagnosticCode::DuplicateName
                    && diagnostic.primary_span() == expected
                    && diagnostic.related_span() == Some(related)
            }),
            "expected DuplicateName at {expected:?} related {related:?}; found {diagnostics:?}"
        );
    }
}
