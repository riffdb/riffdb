//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_struct_v1!(GeneratedSchemaArtifact {
    owned: [key, canonical_json, hash],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(SchemaArtifactKey);
