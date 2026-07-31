//! Checked transport-neutral MCP command-name metadata.

use std::collections::BTreeSet;

use riffdb_types::{CommandId, ContractLineage};

use crate::{IrValidationError, checked_len, validate_source_name};

/// Immutable command-name registry version.
pub const MCP_COMMAND_NAME_REGISTRY_VERSION_V2: u32 = 2;

/// Maximum complete MCP command tool-name bytes.
pub const MAX_MCP_COMMAND_TOOL_NAME_BYTES: usize = 128;

/// One complete checked ADR-0064 command tool name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct McpCommandToolNameV2(String);

impl McpCommandToolNameV2 {
    /// Checks a compiler-produced name against exact source identifiers.
    pub fn new_checked(
        contract_source_name: &str,
        command_source_name: &str,
        value: impl Into<String>,
    ) -> Result<Self, IrValidationError> {
        validate_source_name(contract_source_name, "MCP contract source")?;
        validate_source_name(command_source_name, "MCP command source")?;
        let value = value.into();
        checked_len(
            "MCP command tool name",
            value.len(),
            MAX_MCP_COMMAND_TOOL_NAME_BYTES,
        )?;
        let expected = normalized_name(contract_source_name, command_source_name)?;
        if value != expected {
            return Err(IrValidationError::InvalidMcpName {
                reason: "MCP command name does not match exact ADR-0064 derivation",
            });
        }
        Ok(Self(value))
    }

    /// Complete ASCII public tool name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One stable command/name binding in the v2 registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpCommandNameEntryV2 {
    command_id: CommandId,
    source_command_name: String,
    tool_name: McpCommandToolNameV2,
}

impl McpCommandNameEntryV2 {
    /// Creates a checked stable command/name binding.
    pub fn new(
        command_id: CommandId,
        contract_source_name: &str,
        source_command_name: impl Into<String>,
        tool_name: impl Into<String>,
    ) -> Result<Self, IrValidationError> {
        let source_command_name = source_command_name.into();
        let tool_name = McpCommandToolNameV2::new_checked(
            contract_source_name,
            &source_command_name,
            tool_name,
        )?;
        Ok(Self {
            command_id,
            source_command_name,
            tool_name,
        })
    }

    /// Stable command ID.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }
    /// Exact source command identifier.
    #[must_use]
    pub fn source_command_name(&self) -> &str {
        &self.source_command_name
    }
    /// Complete checked public tool name.
    #[must_use]
    pub const fn tool_name(&self) -> &McpCommandToolNameV2 {
        &self.tool_name
    }
}

/// Complete deterministic compiler-owned command-name registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpCommandNameRegistryV2 {
    version: u32,
    lineage: ContractLineage,
    source_contract_name: String,
    entries: Vec<McpCommandNameEntryV2>,
}

impl McpCommandNameRegistryV2 {
    /// Creates a canonical registry. Bundle construction checks completeness.
    pub fn new(
        lineage: ContractLineage,
        source_contract_name: impl Into<String>,
        mut entries: Vec<McpCommandNameEntryV2>,
    ) -> Result<Self, IrValidationError> {
        let source_contract_name = source_contract_name.into();
        validate_source_name(&source_contract_name, "MCP contract source")?;
        if lineage.as_str() != source_contract_name {
            return Err(IrValidationError::InvalidMcpName {
                reason: "MCP registry source contract does not equal contract lineage",
            });
        }
        checked_len("MCP command-name entries", entries.len(), 4_096)?;
        entries.sort_unstable_by_key(McpCommandNameEntryV2::command_id);
        if entries
            .windows(2)
            .any(|pair| pair[0].command_id == pair[1].command_id)
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "MCP command-name entries",
            });
        }
        let mut names = BTreeSet::new();
        for entry in &entries {
            McpCommandToolNameV2::new_checked(
                &source_contract_name,
                &entry.source_command_name,
                entry.tool_name.as_str(),
            )?;
            if !names.insert(entry.tool_name.as_str()) {
                return Err(IrValidationError::McpNameCollision);
            }
        }
        Ok(Self {
            version: MCP_COMMAND_NAME_REGISTRY_VERSION_V2,
            lineage,
            source_contract_name,
            entries,
        })
    }

    /// Registry version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }
    /// Exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }
    /// Exact source contract identifier.
    #[must_use]
    pub fn source_contract_name(&self) -> &str {
        &self.source_contract_name
    }
    /// Entries in increasing `CommandId` order.
    #[must_use]
    pub fn entries(&self) -> &[McpCommandNameEntryV2] {
        &self.entries
    }
    /// Looks up one stable command.
    #[must_use]
    pub fn get(&self, command_id: CommandId) -> Option<&McpCommandNameEntryV2> {
        self.entries
            .binary_search_by_key(&command_id, McpCommandNameEntryV2::command_id)
            .ok()
            .map(|index| &self.entries[index])
    }
}

fn normalized_name(contract: &str, command: &str) -> Result<String, IrValidationError> {
    let contract = normalize_segment(contract)?;
    let command = normalize_segment(command)?;
    let value = format!("riffdb_cmd_{contract}_{command}");
    checked_len(
        "MCP command tool name",
        value.len(),
        MAX_MCP_COMMAND_TOOL_NAME_BYTES,
    )?;
    Ok(value)
}

fn normalize_segment(value: &str) -> Result<String, IrValidationError> {
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        output.push(char::from(byte.to_ascii_lowercase()));
    }
    if output.is_empty()
        || !output.as_bytes()[0].is_ascii_lowercase()
        || output
            .bytes()
            .any(|byte| !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
    {
        return Err(IrValidationError::InvalidMcpName {
            reason: "MCP command segment does not match [a-z][a-z0-9_]*",
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_v2_name_never_invents_word_boundaries() {
        let name = McpCommandToolNameV2::new_checked(
            "LegalSpend",
            "AllocateBudget",
            "riffdb_cmd_legalspend_allocatebudget",
        )
        .expect("name");
        assert_eq!(name.as_str(), "riffdb_cmd_legalspend_allocatebudget");
        assert!(
            McpCommandToolNameV2::new_checked(
                "LegalSpend",
                "AllocateBudget",
                "riffdb_cmd_legal_spend_allocate_budget",
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_leading_underscore_and_case_collisions() {
        assert!(
            McpCommandToolNameV2::new_checked("_Legal", "Run", "riffdb_cmd__legal_run").is_err()
        );
        let lineage = ContractLineage::new("LegalSpend").expect("lineage");
        let entries = vec![
            McpCommandNameEntryV2::new(
                CommandId::first(),
                "LegalSpend",
                "Run",
                "riffdb_cmd_legalspend_run",
            )
            .expect("entry"),
            McpCommandNameEntryV2::new(
                CommandId::new(2).expect("id"),
                "LegalSpend",
                "RUN",
                "riffdb_cmd_legalspend_run",
            )
            .expect("entry"),
        ];
        assert!(McpCommandNameRegistryV2::new(lineage, "LegalSpend", entries).is_err());
    }
}
