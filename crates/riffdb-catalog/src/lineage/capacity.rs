//! Allocation inventory leaves lineage semantic ceilings and proofs unchanged.
use super::*;

riffdb_types::inspect_owned_capacity_copy_v1!(RecordOwnerV1);
riffdb_types::inspect_owned_capacity_copy_v1!(FieldIntroduction);
riffdb_types::inspect_owned_capacity_struct_v1!(OwnerFieldIntroductions {
    owned: [fields],
    inline: []
});
riffdb_types::inspect_owned_capacity_struct_v1!(LineageMaterializationProof {
    owned: [lineage, bundles, field_introductions],
    inline: [canonical_bundle_bytes, semantic_proof_bytes],
    lifecycle: lifecycle
});
