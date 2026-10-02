//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_struct_v1!(WorkflowCatalog {
    owned: [workflows],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(WorkflowLeaseSchema {
    owned: [
        name,
        owner_field,
        expiry_field,
        fencing_token_field,
        attempt_field,
        minimum_duration_seconds,
        maximum_duration_seconds
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(WorkflowSchema {
    owned: [
        name,
        entity,
        state_field,
        state_enum,
        initial_state,
        transitions,
        lease
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(WorkflowTransitionSchema {
    owned: [name, source_states, destination],
    inline: []
});
