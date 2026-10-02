//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_struct_v1!(PrincipalFactSchemaV1 {
    owned: [name, value_type],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(RowPolicyCatalogV1 {
    owned: [facts, policies],
    inline: []
});
