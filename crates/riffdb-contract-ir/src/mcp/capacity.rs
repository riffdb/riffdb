//! Exhaustive non-durable ownership inventory for provider admission.
use super::*;

riffdb_types::inspect_owned_capacity_struct_v1!(McpCommandNameEntryV2 {
    owned: [command_id, source_command_name, tool_name],
    inline: []
});

riffdb_types::inspect_owned_capacity_struct_v1!(McpCommandNameRegistryV2 {
    owned: [version, lineage, source_contract_name, entries],
    inline: []
});

riffdb_types::inspect_owned_capacity_tuple_v1!(McpCommandToolNameV2, String);
