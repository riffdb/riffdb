//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_struct_v1!(AggregateKeyPlan {
    owned: [
        expressions,
        partition_expression,
        conflict_expressions,
        partition_schema,
        conflict_schema
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(AggregateSchema {
    owned: [id, name, root, children, keys, invariants],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(CascadeRelationshipSpecV1 {
    owned: [source_entity, relationship_name, index_id, maximum],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(CommandInputSchema {
    owned: [command_id, record],
    inline: []
});

impl riffdb_types::InspectOwnedCapacityV1 for DeletePolicyModeV1 {
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

riffdb_types::inspect_owned_capacity_struct_v1!(DeletePolicySchemaV1 {
    owned: [target_entity, mode],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(EntitySchema {
    owned: [
        id,
        name,
        record,
        primary_key_fields,
        primary_key,
        invariants,
        indexes
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(EnumSchema {
    owned: [id, name, variants],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(EnumVariantSchema {
    owned: [id, name],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(EventPartitionSchema {
    owned: [fields, key_schema],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(EventPolicyAnchorFieldV1);

riffdb_types::inspect_owned_capacity_struct_v1!(EventPolicyAnchorV1 {
    owned: [source_entity, key_fields, read_policy],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(EventSchema {
    owned: [id, name, payload, partition, policy_anchor],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(IndexSchema {
    owned: [id, name, fields, encodings, cover_fields, key_schema],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(InvariantPlan {
    owned: [id, name, expressions, predicate],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(LongPatternSpecV1 {
    owned: [
        entity,
        index,
        name,
        field,
        profile,
        operators,
        bounds,
        replay_age_seconds,
        replay_bytes,
        replay_backlog,
        retained_generations,
        stale_entity_count_threshold
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(RecordSchema {
    owned: [owner, fields],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(RelationshipSchema {
    owned: [
        name,
        source_entity,
        source_fields,
        target_entity,
        target_fields
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(ColumnarProjectionSchemaV1 {
    owned: [name, entity, org_scope_field, projected_fields],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(SchemaIr {
    owned: [
        entities,
        events,
        enums,
        aggregates,
        relationships,
        unique_keys,
        delete_policies,
        vector_field_specs,
        vector_ann_specs,
        vector_production_specs,
        text_index_specs,
        long_pattern_specs,
        columnar_projection_specs,
        secret_field_specs
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(SecretFieldSpecV1 {
    owned: [entity, field],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(TextIndexSourceV1);

riffdb_types::inspect_owned_capacity_struct_v1!(TextIndexSpecV1 {
    owned: [
        entity,
        index,
        analyzer,
        source_fields,
        stale_entity_count_threshold,
        replay_age_seconds,
        replay_bytes,
        replay_backlog,
        result_model,
        max_terms,
        max_candidates,
        max_results
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(UniqueKeySchema {
    owned: [name, source_entity, index_id, fields],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(VectorAnnSpecV1);

riffdb_types::inspect_owned_capacity_struct_v1!(VectorFieldSpecV1 {
    owned: [
        entity,
        field,
        metric,
        source_fields,
        stale_entity_count_threshold
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(VectorProductionSpecV1 {
    owned: [
        entity,
        field,
        metadata,
        replay_age_seconds,
        replay_bytes,
        replay_backlog
    ],
    inline: []
});
