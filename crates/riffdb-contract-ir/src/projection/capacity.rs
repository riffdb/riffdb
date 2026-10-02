//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_copy_v1!(ProjectionAggregation);

riffdb_types::inspect_owned_capacity_copy_v1!(ProjectionFrontierPolicy);

riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionGroupComponentSchema {
    owned: [value_type, enum_variants, maximum_framed_bytes],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionGroupSchema {
    owned: [
        projection_id,
        codec_version,
        group_components,
        measures,
        maximum_complete_key_bytes,
        maximum_stored_state_bytes
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionMeasurePlan {
    owned: [field, aggregation, expression],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionPlan {
    owned: [
        projection_id,
        name,
        source_event,
        expressions,
        filter,
        key_expressions,
        measures,
        frontier,
        group_schema,
        plan_hash
    ],
    inline: []
});
