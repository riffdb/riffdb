//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_struct_v1!(GeneratedSchemaArtifact {
    owned: [key, canonical_json, hash],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(SchemaArtifactKey);

impl riffdb_types::InspectOwnedCapacityV1 for Json {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::Null => Ok(()),
            Self::Bool(value) => inspection.inline(value),
            Self::Number(value) | Self::String(value) => inspection.visit(value),
            Self::Array(values) => inspection.visit(values),
            Self::Object(values) => inspection.visit(values),
        }
    }
}
