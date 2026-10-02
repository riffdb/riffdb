//! Private catalog ownership is exhaustive and follows actual shared roots.
use super::*;

riffdb_types::inspect_owned_capacity_struct_v1!(ValidatedContractBundleInner {
    owned: [bundle, enum_variant_names],
    inline: [],
    lifecycle: lifecycle
});
riffdb_types::inspect_owned_capacity_tuple_v1!(
    ValidatedContractBundle,
    Arc<ValidatedContractBundleInner>
);
riffdb_types::inspect_owned_capacity_struct_v1!(ActiveCommandPlanMap {
    owned: [plans],
    inline: [],
    lifecycle: lifecycle
});
riffdb_types::inspect_owned_capacity_struct_v1!(ActiveCatalogPublished {
    owned: [pointer, bundle, lineage_proof, active_plans],
    inline: [],
    lifecycle: lifecycle
});
riffdb_types::inspect_owned_capacity_struct_v1!(ActiveCatalogSnapshot {
    owned: [published],
    inline: []
});
riffdb_types::inspect_owned_capacity_struct_v1!(ResolvedExecutablePlan {
    owned: [reference, plan, bundle, lineage_proof],
    inline: [executing_ordinal]
});
