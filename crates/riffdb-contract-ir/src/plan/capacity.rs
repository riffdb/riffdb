//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_copy_v1!(BindingMode);

riffdb_types::inspect_owned_capacity_struct_v1!(BindingPlan {
    owned: [
        id,
        name,
        mode,
        entity_type,
        key_schema,
        key_expressions,
        accessed_fields,
        complete_record_access,
        initializer,
        failure,
        restriction_failure,
        cascade_failure,
        indexed_selection
    ],
    inline: []
});

impl riffdb_types::InspectOwnedCapacityV1 for CapabilityRequirement {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::InvokeCommand {
                lineage,
                command_id,
            } => {
                inspection.visit(lineage)?;
                inspection.visit(command_id)?;
            }
        }
        Ok(())
    }
}

riffdb_types::inspect_owned_capacity_copy_v1!(CollectionDuplicatePolicyV1);

riffdb_types::inspect_owned_capacity_struct_v1!(CollectionExpansionPlanV1 {
    owned: [
        input_field,
        minimum_elements,
        maximum_elements,
        element_type,
        first_binding,
        binding_count,
        first_instruction,
        instruction_count,
        duplicate_policy,
        maximum_aggregate_element_bytes,
        maximum_copy_coefficient
    ],
    inline: []
});

impl riffdb_types::InspectOwnedCapacityV1 for CommandDecisionActionV1 {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::Apply {
                bindings,
                instructions,
            } => {
                inspection.visit(bindings)?;
                inspection.visit(instructions)?;
            }
            Self::NoEffect => {}
            Self::Reject(field_0) => {
                inspection.visit(field_0)?;
            }
        }
        Ok(())
    }
}

riffdb_types::inspect_owned_capacity_struct_v1!(CommandDecisionArmV1 {
    owned: [predicate, action],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(CommandDecisionPlanV1 {
    owned: [binding, collection_local, when_arms, else_action],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(CommandInvocationClass);

riffdb_types::inspect_owned_capacity_struct_v1!(CommandPlan {
    owned: [
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
        retry_policy,
        required_capability,
        plan_hash,
        write_proofs
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(CommitCheckPlan {
    owned: [
        invariant_id,
        predicate,
        source_bindings,
        root_validation_reads
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(ConflictDerivationPlan {
    owned: [schema, expressions],
    inline: []
});

impl riffdb_types::InspectOwnedCapacityV1 for DeleteCheckModeV1 {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::NoInbound => {}
            Self::Restrict {
                source_entity,
                index_id,
            } => {
                inspection.visit(source_entity)?;
                inspection.visit(index_id)?;
            }
            Self::Cascade { relationships } => {
                inspection.visit(relationships)?;
            }
        }
        Ok(())
    }
}

riffdb_types::inspect_owned_capacity_struct_v1!(DeleteCheckPlanV1 {
    owned: [binding, mode],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(EventConstruction {
    owned: [event_type, payload],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(ExecutionClass);

riffdb_types::inspect_owned_capacity_copy_v1!(FieldExpression);

impl riffdb_types::InspectOwnedCapacityV1 for Instruction {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::Require {
                requirement_index,
                predicate,
                reject,
            } => {
                inspection.visit(requirement_index)?;
                inspection.visit(predicate)?;
                inspection.visit(reject)?;
            }
            Self::SetField {
                binding,
                field,
                value,
                guard,
            } => {
                inspection.visit(binding)?;
                inspection.visit(field)?;
                inspection.visit(value)?;
                inspection.visit(guard)?;
            }
            Self::SetEmbedding {
                binding,
                field,
                value,
                model_identity,
                model_version,
            } => {
                inspection.visit(binding)?;
                inspection.visit(field)?;
                inspection.visit(value)?;
                inspection.visit(model_identity)?;
                inspection.visit(model_version)?;
            }
            Self::WorkflowTransition {
                binding,
                state_field,
                source_states,
                destination,
                expected_revision,
                stale,
                illegal,
            } => {
                inspection.visit(binding)?;
                inspection.visit(state_field)?;
                inspection.visit(source_states)?;
                inspection.visit(destination)?;
                inspection.visit(expected_revision)?;
                inspection.visit(stale)?;
                inspection.visit(illegal)?;
            }
            Self::WorkflowLease {
                binding,
                fields,
                operation,
            } => {
                inspection.visit(binding)?;
                inspection.visit(fields)?;
                inspection.visit(operation)?;
            }
            Self::EmitEvent(field_0) => {
                inspection.visit(field_0)?;
            }
            Self::Return(field_0) => {
                inspection.visit(field_0)?;
            }
        }
        Ok(())
    }
}

riffdb_types::inspect_owned_capacity_struct_v1!(LocalityPlan {
    owned: [
        aggregate_id,
        partition_schema,
        partition_expression,
        conflict_keys
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(ObjectConstruction {
    owned: [record, fields],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(OutcomeConstruction {
    owned: [outcome_id, payload],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(OutcomeSchema {
    owned: [id, name, payload],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(RelationshipCheckPlan {
    owned: [relationship_name, source_binding, target_binding],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(RetryPolicy);

riffdb_types::inspect_owned_capacity_struct_v1!(RootValidationReadPlan {
    owned: [
        id,
        source_binding,
        entity_type,
        key_schema,
        key_expressions,
        accessed_fields
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(SecretRevealDestinationV1);

riffdb_types::inspect_owned_capacity_copy_v1!(SecretRevealSpecV1);

riffdb_types::inspect_owned_capacity_struct_v1!(UniqueConflictPlan {
    owned: [unique_name, index_id, source_binding, schema, expressions],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(WorkflowLeaseFields {
    owned: [
        owner_field,
        expiry_field,
        fencing_token_field,
        attempt_field,
        minimum_duration_seconds,
        maximum_duration_seconds
    ],
    inline: []
});

impl riffdb_types::InspectOwnedCapacityV1 for WorkflowLeaseOperation {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::Claim {
                owner,
                duration_seconds,
                expected_revision,
                stale,
                unavailable,
                invalid,
                exhausted,
            } => {
                inspection.visit(owner)?;
                inspection.visit(duration_seconds)?;
                inspection.visit(expected_revision)?;
                inspection.visit(stale)?;
                inspection.visit(unavailable)?;
                inspection.visit(invalid)?;
                inspection.visit(exhausted)?;
            }
            Self::Renew {
                owner,
                fencing_token,
                duration_seconds,
                expected_revision,
                stale,
                invalid,
                expired,
                exhausted,
            } => {
                inspection.visit(owner)?;
                inspection.visit(fencing_token)?;
                inspection.visit(duration_seconds)?;
                inspection.visit(expected_revision)?;
                inspection.visit(stale)?;
                inspection.visit(invalid)?;
                inspection.visit(expired)?;
                inspection.visit(exhausted)?;
            }
            Self::Release {
                owner,
                fencing_token,
                expected_revision,
                stale,
                invalid,
            } => {
                inspection.visit(owner)?;
                inspection.visit(fencing_token)?;
                inspection.visit(expected_revision)?;
                inspection.visit(stale)?;
                inspection.visit(invalid)?;
            }
            Self::Expire {
                expected_revision,
                stale,
                active,
            } => {
                inspection.visit(expected_revision)?;
                inspection.visit(stale)?;
                inspection.visit(active)?;
            }
            Self::Fence {
                owner,
                fencing_token,
                expected_revision,
                stale,
                invalid,
                expired,
            } => {
                inspection.visit(owner)?;
                inspection.visit(fencing_token)?;
                inspection.visit(expected_revision)?;
                inspection.visit(stale)?;
                inspection.visit(invalid)?;
                inspection.visit(expired)?;
            }
        }
        Ok(())
    }
}

riffdb_types::inspect_owned_capacity_copy_v1!(WriteGuard);
