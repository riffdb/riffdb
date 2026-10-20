//! Exhaustive owned-capacity declarations for the two ADR-0318 wire owners.
use crate::storage::v1 as wire;
use riffdb_types::InspectOwnedCapacityV1;

// Exhaustive owning declaration: adding a Unit field must fail compilation.
riffdb_types::inspect_owned_capacity_struct_v1!(wire::UnitV1 {
    owned: [],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(wire::StoredAuthoritativeCatalogTransitionV1 {
    owned: [
        database_id,
        publication_binding,
        target_catalog_digest,
        target_registry_digest,
        initial_source_allocator,
        origin
    ],
    inline: [administration_sequence]
});
riffdb_types::inspect_owned_capacity_struct_v1!(
    wire::stored_authoritative_catalog_transition_v1::V2ToV3 {
        owned: [
            predecessor_history,
            original_registry_envelope,
            original_active_admission_envelope,
            legacy_controls_commitment
        ],
        inline: []
    }
);
impl InspectOwnedCapacityV1 for wire::stored_authoritative_catalog_transition_v1::Origin {
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::FreshV3(value) => inspection.visit(value),
            Self::V2ToV3(value) => inspection.visit(value),
        }
    }
}
riffdb_types::inspect_owned_capacity_struct_v1!(wire::StoredSourceInheritanceAdministrationV1 {
    owned: [
        predecessor_history,
        successor_lineage,
        anchor_binding,
        original_origin,
        authorized_selection_commitment,
        source_witnesses,
        source_allocator,
        incarnation,
        registry,
        watermark
    ],
    inline: [administration_sequence, cause]
});
riffdb_types::inspect_owned_capacity_struct_v1!(
    wire::stored_source_inheritance_administration_v1::SuccessorLineage {
        owned: [database_id, catalog_digest],
        inline: [history_incarnation, leadership_epoch]
    }
);
riffdb_types::inspect_owned_capacity_struct_v1!(
    wire::stored_source_inheritance_administration_v1::OriginReference {
        owned: [canonical_envelope_digest],
        inline: [administration_sequence]
    }
);
riffdb_types::inspect_owned_capacity_struct_v1!(
    wire::stored_source_inheritance_administration_v1::PredecessorScalar {
        owned: [state],
        inline: []
    }
);
impl InspectOwnedCapacityV1
    for wire::stored_source_inheritance_administration_v1::predecessor_scalar::State
{
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        match self {
            Self::Absent(value) => inspection.visit(value),
            Self::PresentEnvelope(value) => inspection.visit(value),
        }
    }
}
