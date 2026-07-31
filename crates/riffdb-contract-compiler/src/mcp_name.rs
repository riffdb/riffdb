//! ADR-0064 command tool-name derivation.

use std::collections::BTreeMap;

use riffdb_contract_ir::{McpCommandNameEntryV2, McpCommandNameRegistryV2};
use riffdb_contract_syntax::{Span, Spanned};
use riffdb_types::{CommandId, ContractLineage};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};

const TOOL_PREFIX: &str = "riffdb_cmd_";
const MAX_TOOL_NAME_BYTES: usize = 128;

/// One compiler-private derived name before checked IR registry construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DerivedCommandToolName {
    pub(crate) command_id: CommandId,
    pub(crate) source_identifier: String,
    pub(crate) complete_name: String,
}

/// Derives the complete deterministic ADR-0064 v2 registry in stable command-ID order.
pub(crate) fn derive_command_tool_names(
    contract: &Spanned<String>,
    commands: &[(CommandId, Spanned<String>)],
) -> Result<Vec<DerivedCommandToolName>, CompilerDiagnostics> {
    let contract_segment = match normalize_segment(&contract.value) {
        Some(segment) => segment,
        None => {
            return Err(CompilerDiagnostics::single(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidCommandToolName,
                contract.span,
            )));
        }
    };

    let mut ordered = commands.to_vec();
    ordered.sort_by_key(|(command_id, _)| *command_id);
    let mut names = Vec::with_capacity(ordered.len());
    let mut first_by_complete_name: BTreeMap<String, Span> = BTreeMap::new();
    let mut diagnostics = Vec::new();

    for (command_id, command) in ordered {
        let Some(command_segment) = normalize_segment(&command.value) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidCommandToolName,
                command.span,
            ));
            continue;
        };
        let complete_name = format!("{TOOL_PREFIX}{contract_segment}_{command_segment}");
        if complete_name.len() > MAX_TOOL_NAME_BYTES {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::CommandToolNameTooLong,
                command.span,
            ));
            continue;
        }
        if let Some(first_span) = first_by_complete_name.get(&complete_name) {
            diagnostics.push(
                CompilerDiagnostic::new(
                    CompilerDiagnosticCode::CommandToolNameCollision,
                    command.span,
                )
                .with_related_span(*first_span),
            );
            continue;
        }
        first_by_complete_name.insert(complete_name.clone(), command.span);
        names.push(DerivedCommandToolName {
            command_id,
            source_identifier: command.value,
            complete_name,
        });
    }

    if diagnostics.is_empty() {
        Ok(names)
    } else {
        Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"))
    }
}

/// Constructs the IR-owned checked ADR-0020 registry from derived compiler names.
pub(crate) fn build_command_tool_registry(
    contract: &Spanned<String>,
    commands: &[(CommandId, Spanned<String>)],
) -> Result<McpCommandNameRegistryV2, CompilerDiagnostics> {
    let lineage = ContractLineage::new(contract.value.clone()).map_err(|_| {
        CompilerDiagnostics::single(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidCommandToolName,
            contract.span,
        ))
    })?;
    let entries = derive_command_tool_names(contract, commands)?
        .into_iter()
        .map(|derived| {
            McpCommandNameEntryV2::new(
                derived.command_id,
                &contract.value,
                derived.source_identifier,
                derived.complete_name,
            )
            .map_err(|_| {
                CompilerDiagnostics::single(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidCommandToolName,
                    contract.span,
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    McpCommandNameRegistryV2::new(lineage, contract.value.clone(), entries).map_err(|_| {
        CompilerDiagnostics::single(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidCommandToolName,
            contract.span,
        ))
    })
}

fn normalize_segment(source_identifier: &str) -> Option<String> {
    let mut bytes = source_identifier.bytes();
    let first = bytes.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }

    let mut normalized = String::with_capacity(source_identifier.len());
    normalized.push(first.to_ascii_lowercase() as char);
    for byte in bytes {
        if !(byte.is_ascii_alphanumeric() || byte == b'_') {
            return None;
        }
        normalized.push(byte.to_ascii_lowercase() as char);
    }
    Some(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spanned(value: impl Into<String>, start: usize) -> Spanned<String> {
        let value = value.into();
        let span = Span::new(start, start + value.len()).expect("valid span");
        Spanned::new(value, span)
    }

    fn command_id(value: u32) -> CommandId {
        CommandId::new(value).expect("nonzero command ID")
    }

    #[test]
    fn exact_v1_golden_names_do_not_invent_word_boundaries() {
        let names = derive_command_tool_names(
            &spanned("Legal_Spend", 0),
            &[(command_id(1), spanned("Allocate_Budget2", 20))],
        )
        .expect("valid name");
        assert_eq!(
            names[0].complete_name,
            "riffdb_cmd_legal_spend_allocate_budget2"
        );

        let names = derive_command_tool_names(
            &spanned("LegalSpend", 0),
            &[(command_id(1), spanned("AllocateBudget", 20))],
        )
        .expect("valid name");
        assert_eq!(
            names[0].complete_name,
            "riffdb_cmd_legalspend_allocatebudget"
        );
    }

    #[test]
    fn entries_are_ordered_by_stable_command_id() {
        let names = derive_command_tool_names(
            &spanned("Contract", 0),
            &[
                (command_id(3), spanned("Third", 30)),
                (command_id(1), spanned("First", 10)),
                (command_id(2), spanned("Second", 20)),
            ],
        )
        .expect("valid names");
        assert_eq!(
            names
                .iter()
                .map(|entry| entry.command_id.get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn leading_underscore_and_over_length_names_reject() {
        let invalid = derive_command_tool_names(
            &spanned("_Contract", 0),
            &[(command_id(1), spanned("Command", 20))],
        )
        .expect_err("leading underscore rejects");
        assert_eq!(
            invalid.as_slice()[0].code(),
            CompilerDiagnosticCode::InvalidCommandToolName
        );

        let contract = "C".repeat(60);
        // Prefix (11), contract (60), separator (1), and command (56) total 128.
        derive_command_tool_names(
            &spanned(contract.clone(), 0),
            &[(command_id(1), spanned("D".repeat(56), 100))],
        )
        .expect("128-byte complete name is valid");
        let too_long = derive_command_tool_names(
            &spanned(contract, 0),
            &[(command_id(1), spanned("D".repeat(57), 100))],
        )
        .expect_err("129-byte complete name rejects");
        assert_eq!(
            too_long.as_slice()[0].code(),
            CompilerDiagnosticCode::CommandToolNameTooLong
        );
    }

    #[test]
    fn case_only_collision_uses_stable_id_order_and_both_spans() {
        let first = spanned("Allocate", 50);
        let second = spanned("ALLOCATE", 10);
        let diagnostics = derive_command_tool_names(
            &spanned("Budget", 0),
            &[
                (command_id(2), first.clone()),
                (command_id(1), second.clone()),
            ],
        )
        .expect_err("normalized collision rejects");
        let collision = &diagnostics.as_slice()[0];
        assert_eq!(
            collision.code(),
            CompilerDiagnosticCode::CommandToolNameCollision
        );
        assert_eq!(collision.primary_span(), first.span);
        assert_eq!(collision.related_span(), Some(second.span));
    }
}
