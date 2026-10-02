//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_copy_v1!(WriteEvidence);

riffdb_types::inspect_owned_capacity_copy_v1!(WriteLocation);

riffdb_types::inspect_owned_capacity_struct_v1!(WriteProof {
    owned: [location, evidence],
    inline: []
});
