//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_copy_v1!(CompatibilityClass);

riffdb_types::inspect_owned_capacity_copy_v1!(CompatibilityCode);

riffdb_types::inspect_owned_capacity_struct_v1!(CompatibilityEntry {
    owned: [code, affected_path, path_key],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(CompatibilityReport {
    owned: [overall, entries],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(StableAffectedPath {
    owned: [segments],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(StablePathSegment {
    owned: [],
    inline: [kind, stable_id]
});
