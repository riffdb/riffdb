//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_struct_v1!(ContractBundle {
    owned: [
        format_version,
        grammar_version,
        ir_version,
        compiler_version,
        lineage,
        contract_version,
        parent,
        source_hash,
        plan_root_hash,
        ledger,
        schema,
        workflows,
        row_policies,
        commands,
        projections,
        schema_artifacts,
        mcp_command_names,
        compatibility,
        canonical_bytes,
        bundle_hash
    ],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(LineageAlias {
    owned: [identity, id],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(LineageAllocation {
    owned: [namespace, max_allocated, entries],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(LineageEntry {
    owned: [id, identity, state],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(LineageEntryState);

riffdb_types::inspect_owned_capacity_struct_v1!(LineageLedgerV1 {
    owned: [version, allocations, aliases],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(ParentBundleRef);

riffdb_types::inspect_owned_capacity_struct_v1!(StableIdAllocationNamespace {
    owned: [tag, owner_kind, owner_ids],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(StableIdNamespace {
    owned: [tag, owner_kind, owner_ids],
    inline: []
});

riffdb_types::inspect_owned_capacity_copy_v1!(StableIdNamespaceTag);

riffdb_types::inspect_owned_capacity_struct_v1!(StableIdentity {
    owned: [namespace, name],
    inline: []
});
