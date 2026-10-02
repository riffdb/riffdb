//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_struct_v1!(FieldSchema {
    owned: [id, name, value_type],
    inline: []
});
