//! Total compiler entry points and phase orchestration.

use std::error::Error;
use std::fmt;

use riffdb_contract_ir::{
    CompatibilityReport, ContractBundle, ContractCandidateV1, IrValidationError, compare_successor,
};
use riffdb_contract_syntax::{SyntaxDiagnostics, parse_contract};
use riffdb_types::hash_source;

use crate::bundle_lowering::{BundleParts, assemble_bundle};
use crate::command_analysis::validate_commands;
use crate::command_lowering::lower_commands;
use crate::diagnostic::CompilerDiagnostics;
use crate::hir::lower_contract_hir;
use crate::locality::analyze_locality;
use crate::mcp_name::build_command_tool_registry;
use crate::projection_lowering::lower_projections;
use crate::schema_lowering::{
    lower_schema, validate_relationship_declarations, validate_unique_declarations,
};
use crate::symbols::{allocate_genesis_symbols, allocate_successor_symbols};
use crate::typecheck::resolve_declared_types;

/// A total parsing or semantic-compilation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompilationError {
    /// Source could not be parsed as the closed grammar version 1.
    Syntax(SyntaxDiagnostics),
    /// Parsed source failed one or more bounded semantic checks.
    Semantic(CompilerDiagnostics),
}

impl CompilationError {
    /// Returns syntax diagnostics when parsing failed.
    #[must_use]
    pub const fn syntax(&self) -> Option<&SyntaxDiagnostics> {
        match self {
            Self::Syntax(diagnostics) => Some(diagnostics),
            Self::Semantic(_) => None,
        }
    }

    /// Returns compiler diagnostics when semantic analysis failed.
    #[must_use]
    pub const fn semantic(&self) -> Option<&CompilerDiagnostics> {
        match self {
            Self::Syntax(_) => None,
            Self::Semantic(diagnostics) => Some(diagnostics),
        }
    }
}

impl fmt::Display for CompilationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax(diagnostics) => diagnostics.fmt(formatter),
            Self::Semantic(diagnostics) => diagnostics.fmt(formatter),
        }
    }
}

impl Error for CompilationError {}

/// Parses and performs source namespace, stable-ID, and ADR-0020 registry validation.
///
/// This entry point is useful to editors and governance checks that need total
/// diagnostics without emitting a bundle. Executable compilation performs the
/// same phases before checked IR lowering.
pub fn validate_contract_source(source: &str) -> Result<(), CompilationError> {
    let document = parse_contract(source).map_err(CompilationError::Syntax)?;
    let symbols = allocate_genesis_symbols(&document).map_err(CompilationError::Semantic)?;
    let types = resolve_declared_types(&document, &symbols).map_err(CompilationError::Semantic)?;
    let hir =
        lower_contract_hir(&document, &symbols, &types).map_err(CompilationError::Semantic)?;
    validate_relationship_declarations(&hir).map_err(CompilationError::Semantic)?;
    validate_unique_declarations(&hir).map_err(CompilationError::Semantic)?;
    validate_commands(&hir).map_err(CompilationError::Semantic)?;
    let locality = analyze_locality(&hir).map_err(CompilationError::Semantic)?;
    let _owned_entity_count = locality.entity_owner.len();
    let schema = lower_schema(&hir).map_err(CompilationError::Semantic)?;
    lower_commands(&hir, &schema).map_err(CompilationError::Semantic)?;
    lower_projections(&hir, &schema).map_err(CompilationError::Semantic)?;
    build_command_tool_registry(
        &riffdb_contract_syntax::Spanned::new(hir.name.clone(), hir.name_span),
        &hir.command_names(),
    )
    .map_err(CompilationError::Semantic)?;
    Ok(())
}

/// Compiles a lineage genesis source into one immutable checked bundle.
pub fn compile_contract_source(source: &str) -> Result<ContractBundle, CompilationError> {
    compile(source, None)
}

/// Compiles an exact-parent successor while retaining lineage IDs and tombstones.
pub fn compile_contract_successor(
    source: &str,
    parent: &ContractBundle,
) -> Result<ContractBundle, CompilationError> {
    compile(source, Some(parent))
}

fn compile(
    source: &str,
    parent: Option<&ContractBundle>,
) -> Result<ContractBundle, CompilationError> {
    let document = parse_contract(source).map_err(CompilationError::Syntax)?;
    let symbols = match parent {
        Some(parent) => {
            if parent.lineage().as_str() != document.contract.value.name.value {
                return Err(semantic_error(
                    crate::diagnostic::CompilerDiagnosticCode::InvalidParent,
                    document.contract.value.name.span,
                ));
            }
            let symbols = allocate_successor_symbols(&document, parent.ledger())
                .map_err(CompilationError::Semantic)?;
            if symbols.contract_version <= parent.contract_version() {
                return Err(semantic_error(
                    crate::diagnostic::CompilerDiagnosticCode::InvalidParent,
                    document.contract.value.version.span,
                ));
            }
            symbols
        }
        None => allocate_genesis_symbols(&document).map_err(CompilationError::Semantic)?,
    };
    let types = resolve_declared_types(&document, &symbols).map_err(CompilationError::Semantic)?;
    let hir =
        lower_contract_hir(&document, &symbols, &types).map_err(CompilationError::Semantic)?;
    validate_relationship_declarations(&hir).map_err(CompilationError::Semantic)?;
    validate_unique_declarations(&hir).map_err(CompilationError::Semantic)?;
    validate_commands(&hir).map_err(CompilationError::Semantic)?;
    analyze_locality(&hir).map_err(CompilationError::Semantic)?;
    let schema = lower_schema(&hir).map_err(CompilationError::Semantic)?;
    let commands = lower_commands(&hir, &schema).map_err(CompilationError::Semantic)?;
    let projections = lower_projections(&hir, &schema).map_err(CompilationError::Semantic)?;
    let mcp_names = build_command_tool_registry(
        &riffdb_contract_syntax::Spanned::new(hir.name.clone(), hir.name_span),
        &hir.command_names(),
    )
    .map_err(CompilationError::Semantic)?;
    let parts = BundleParts {
        schema,
        commands,
        projections,
        mcp_names,
    };
    let compatibility = match parent {
        Some(parent) => ContractCandidateV1::new(
            &parts.schema,
            &parts.commands,
            &parts.projections,
            &parts.mcp_names,
        )
        .and_then(|candidate| compare_successor(parent, candidate)),
        None => Ok(CompatibilityReport::genesis()),
    }
    .map_err(|error| ir_compilation_error(error, document.contract.span))?;
    assemble_bundle(
        &symbols,
        hash_source(source.as_bytes()),
        parent,
        parts,
        compatibility,
    )
    .map_err(|error| ir_compilation_error(error, document.contract.span))
}

fn ir_compilation_error(
    error: IrValidationError,
    span: riffdb_contract_syntax::Span,
) -> CompilationError {
    let code = match error {
        IrValidationError::LimitExceeded { .. } | IrValidationError::SizeOverflow { .. } => {
            crate::diagnostic::CompilerDiagnosticCode::BoundExceeded
        }
        _ => crate::diagnostic::CompilerDiagnosticCode::InvalidIr,
    };
    semantic_error(code, span)
}

fn semantic_error(
    code: crate::diagnostic::CompilerDiagnosticCode,
    span: riffdb_contract_syntax::Span,
) -> CompilationError {
    CompilationError::Semantic(CompilerDiagnostics::single(
        crate::diagnostic::CompilerDiagnostic::new(code, span),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::diagnostic::CompilerDiagnosticCode;
    use riffdb_contract_ir::{
        CommandExplain, CompatibilityClass, CompatibilityCode, ContractBundle, ExpressionKind,
        LineageEntryState, UnaryOperator, ValueType, ValueTypeTag,
    };
    use riffdb_types::CanonicalValue;

    fn assert_semantic_diagnostic_at(
        source: &str,
        code: CompilerDiagnosticCode,
        exact_source: &str,
    ) {
        let error = validate_contract_source(source).expect_err("source must reject");
        let diagnostic = error
            .semantic()
            .expect("semantic diagnostics")
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == code)
            .unwrap_or_else(|| {
                panic!(
                    "expected diagnostic code {code:?}, got {:?}",
                    error
                        .semantic()
                        .expect("semantic diagnostics")
                        .as_slice()
                        .iter()
                        .map(|diagnostic| diagnostic.code())
                        .collect::<Vec<_>>()
                )
            });
        let start = source.find(exact_source).expect("source span");
        assert_eq!(diagnostic.primary_span().start() as usize, start);
        assert_eq!(
            diagnostic.primary_span().end() as usize,
            start + exact_source.len()
        );
    }

    fn amplified_outcome_source(entity_fields: usize, outcome_fields: usize) -> String {
        let entity_fields = (0..entity_fields)
            .map(|index| format!("    field value_{index:04}: string<128>\n"))
            .collect::<String>();
        let outcome_fields = (0..outcome_fields)
            .map(|index| format!("      item_{index:04}: row"))
            .collect::<Vec<_>>()
            .join(",\n");
        format!(
            "contract AmplifiedOutcome version 1 {{\n  entity Row {{\n    key (id: uuid)\n{entity_fields}  }}\n  aggregate Rows {{ root Row partition_by id conflict_key (id) }}\n  command Expand {{\n    input id: uuid\n    read Row(id) as row else Missing {{ id: id }}\n    return Expanded {{\n{outcome_fields}\n    }}\n  }}\n}}\n"
        )
    }

    fn indexed_mutation_source(index_count: usize) -> String {
        let indexes = (0..index_count)
            .map(|index| format!("    index by_value_{index:04} (value)\n"))
            .collect::<String>();
        format!(
            "contract IndexedMutation version 1 {{\n  entity Row {{\n    key (id: uuid)\n    field value: i64\n{indexes}  }}\n  aggregate Rows {{ root Row partition_by id conflict_key (id) }}\n  command Change {{\n    input idempotency_key: string<128>\n    input id: uuid\n    idempotency_key idempotency_key\n    mutate Row(id) as row else Missing {{ id: id }}\n    set row.value = 1\n    return Changed {{ row: row }}\n  }}\n}}\n"
        )
    }

    #[test]
    fn canonical_budget_source_passes_pre_ir_validation() {
        validate_contract_source(include_str!("../../../contracts/examples/budget.riff"))
            .expect("canonical budget source is valid");
    }

    #[test]
    fn canonical_budget_compiles_to_one_reproducible_complete_bundle() {
        let source = include_str!("../../../contracts/examples/budget.riff");
        let first = compile_contract_source(source).expect("first bundle");
        let second = compile_contract_source(source).expect("second bundle");
        assert_eq!(first.canonical_bytes(), second.canonical_bytes());
        assert_eq!(first.bundle_hash(), second.bundle_hash());
        assert_eq!(first.commands().len(), 2);
        assert_eq!(first.projections().len(), 1);
        assert_eq!(first.schema_artifacts().len(), 7);
        assert_eq!(first.mcp_command_names().entries().len(), 2);
        assert!(first.compatibility().entries().is_empty());

        let decoded = ContractBundle::decode(first.canonical_bytes()).expect("decode bundle");
        assert_eq!(decoded, first);
    }

    #[test]
    fn canonical_budget_source_matches_the_accepted_parser_fixture() {
        assert_eq!(
            include_str!("../../../contracts/examples/budget.riff"),
            include_str!("../../../contracts/parser-fixtures/valid/legal_spend.riff"),
        );
    }

    #[test]
    fn outcome_name_type_compiles_without_ambiguity() {
        let source = r#"
contract OutcomeType version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Find {
    input id: uuid
    read Row(id) as row else type { id: id }
    return Found { row: row }
  }
}
"#;
        let bundle = compile_contract_source(source).expect("outcome named type compiles");
        assert_eq!(bundle.commands()[0].outcomes()[0].name(), "type");
        assert!(bundle.schema_artifacts().iter().any(|artifact| {
            artifact
                .canonical_json()
                .contains("\"type\":{\"const\":\"type\"}")
        }));
    }

    #[test]
    fn referenced_enum_registry_is_part_of_command_and_projection_hashes() {
        fn source(enum_declaration: &str, enum_name: &str) -> String {
            format!(
                r#"
contract EnumHash version 1 {{
  {enum_declaration}
  entity Row {{ key (id: uuid) field status: {enum_name} }}
  event Changed {{ id: uuid status: {enum_name} }}
  aggregate Rows {{ root Row partition_by id conflict_key (id) }}
  command Find {{
    input id: uuid
    read Row(id) as row else Missing {{ id: id }}
    return Found {{ id: id }}
  }}
  projection ByState {{
    source event Changed
    key (status)
    measure total = count()
    frontier transactionally_ordered
  }}
}}
"#,
            )
        }

        let base =
            compile_contract_source(&source("enum State { Open, Closed }", "State")).expect("base");
        let renamed = compile_contract_source(&source("enum Status { Open, Closed }", "Status"))
            .expect("renamed enum");
        assert_eq!(
            base.commands()[0].plan_hash(),
            renamed.commands()[0].plan_hash()
        );
        assert_eq!(
            base.projections()[0].plan_hash(),
            renamed.projections()[0].plan_hash()
        );

        let changed = compile_contract_source(&source("enum State { Open, Sealed }", "State"))
            .expect("changed");
        assert_ne!(
            base.commands()[0].plan_hash(),
            changed.commands()[0].plan_hash()
        );
        assert_ne!(
            base.projections()[0].plan_hash(),
            changed.projections()[0].plan_hash()
        );

        let unrelated = compile_contract_source(&source(
            "enum State { Open, Closed } enum ZzzUnused { Whatever }",
            "State",
        ))
        .expect("unrelated enum");
        assert_eq!(
            base.commands()[0].plan_hash(),
            unrelated.commands()[0].plan_hash()
        );
        assert_eq!(
            base.projections()[0].plan_hash(),
            unrelated.projections()[0].plan_hash()
        );
    }

    #[test]
    fn unknown_aggregate_child_and_command_entity_are_not_silently_dropped() {
        let unknown_root = r#"
contract UnknownRoot version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Missing partition_by id conflict_key (id) }
}
"#;
        assert_semantic_diagnostic_at(unknown_root, CompilerDiagnosticCode::UnknownName, "Missing");

        let unknown_child = r#"
contract UnknownChild version 1 {
  entity Root { key (id: uuid) }
  aggregate Family {
    root Root
    child Missing
    partition_by id
    conflict_key (id)
  }
}
"#;
        assert_semantic_diagnostic_at(
            unknown_child,
            CompilerDiagnosticCode::UnknownName,
            "Missing",
        );

        let unknown_binding_entity = r#"
contract UnknownBindingEntity version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Find {
    input id: uuid
    read Missing(id) as row else Absent { id: id }
    return Found { id: id }
  }
}
"#;
        assert_semantic_diagnostic_at(
            unknown_binding_entity,
            CompilerDiagnosticCode::UnknownName,
            "Missing",
        );

        let unknown_index_field = r#"
contract UnknownIndexField version 1 {
  entity Row {
    key (id: uuid)
    index by_value (missing)
  }
}
"#;
        assert_semantic_diagnostic_at(
            unknown_index_field,
            CompilerDiagnosticCode::UnknownName,
            "missing",
        );

        let unknown_binding_path = r#"
contract UnknownBindingPath version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Find {
    input id: uuid
    read Row(id) as row else Missing { id: id }
    return Found { leaked: absent.id }
  }
}
"#;
        assert_semantic_diagnostic_at(
            unknown_binding_path,
            CompilerDiagnosticCode::UnknownName,
            "absent.id",
        );
    }

    #[test]
    fn commands_without_an_owned_mutable_partition_reject_at_the_required_span() {
        let event_only = r#"
contract UnownedEvent version 1 {
  entity Row { key (id: uuid) }
  event Changed { id: uuid }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Notify {
    input request: string<128>
    input id: uuid
    idempotency_key request
    read Row(id) as row else Missing { id: id }
    emit Changed { id: id }
    return Notified { row: row }
  }
}
"#;
        let error = validate_contract_source(event_only).expect_err("event-only command rejects");
        let diagnostics = error.semantic().expect("semantic diagnostics").as_slice();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].code(),
            CompilerDiagnosticCode::InvalidBinding
        );
        let event_start = event_only.find("emit Changed").expect("emit effect") + "emit ".len();
        assert_eq!(diagnostics[0].primary_span().start() as usize, event_start);
        assert_eq!(
            diagnostics[0].primary_span().end() as usize,
            event_start + "Changed".len()
        );
        assert_eq!(
            CompilerDiagnosticCode::InvalidBinding.help(),
            Some("bind an entity owned by the command's one aggregate")
        );

        let set_through_read = r#"
contract UnownedSet version 1 {
  entity Row { key (id: uuid) field value: i64 }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input request: string<128>
    input id: uuid
    idempotency_key request
    read Row(id) as row else Missing { id: id }
    set row.value = 1
    return Changed { row: row }
  }
}
"#;
        let error = validate_contract_source(set_through_read)
            .expect_err("set without mutable binding rejects");
        let diagnostics = error.semantic().expect("semantic diagnostics").as_slice();
        assert_eq!(diagnostics.len(), 2);
        let target_start = set_through_read.find("row.value").expect("set target");
        for code in [
            CompilerDiagnosticCode::InvalidBinding,
            CompilerDiagnosticCode::InvalidMutation,
        ] {
            let diagnostic = diagnostics
                .iter()
                .find(|diagnostic| diagnostic.code() == code)
                .expect("required effect diagnostic");
            assert_eq!(diagnostic.primary_span().start() as usize, target_start);
            assert_eq!(
                diagnostic.primary_span().end() as usize,
                target_start + "row.value".len()
            );
        }

        let no_bindings = r#"
contract UnboundRead version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Inspect {
    input id: uuid
    return Empty { id: id }
  }
}
"#;
        let error = validate_contract_source(no_bindings).expect_err("unbound command rejects");
        let diagnostics = error.semantic().expect("semantic diagnostics").as_slice();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].code(),
            CompilerDiagnosticCode::InvalidBinding
        );
        let command_start = no_bindings.find("Inspect").expect("command name");
        assert_eq!(
            diagnostics[0].primary_span().start() as usize,
            command_start
        );
        assert_eq!(
            diagnostics[0].primary_span().end() as usize,
            command_start + "Inspect".len()
        );
    }

    #[test]
    fn core_diagnostic_codes_retain_their_required_primary_spans() {
        let invalid_version = "contract InvalidVersion version 0 {}";
        assert_semantic_diagnostic_at(
            invalid_version,
            CompilerDiagnosticCode::InvalidContractVersion,
            "0",
        );

        let missing_key = "contract MissingKey version 1 { entity Row { field value: i64 } }";
        assert_semantic_diagnostic_at(
            missing_key,
            CompilerDiagnosticCode::MissingDeclaration,
            "Row",
        );

        let ambiguous_enum_path = r#"
contract AmbiguousPath version 1 {
  enum State { Open }
  entity Row { key (id: uuid) field Open: bool }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Find {
    input id: uuid
    read Row(id) as State else Missing { id: id }
    require unambiguous: State.Open else Rejected {}
    return Found { id: id }
  }
}
"#;
        assert_semantic_diagnostic_at(
            ambiguous_enum_path,
            CompilerDiagnosticCode::InvalidExpression,
            "State.Open",
        );

        let primary_key_mutation = r#"
contract KeyMutation version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    mutate Row(id) as row else Missing { id: id }
    set row.id = id
    return Changed { id: id }
  }
}
"#;
        assert_semantic_diagnostic_at(
            primary_key_mutation,
            CompilerDiagnosticCode::InvalidMutation,
            "row.id",
        );

        let incomplete_event = r#"
contract IncompleteEvent version 1 {
  entity Row { key (id: uuid) }
  event Changed { id: uuid }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    mutate Row(id) as row else Missing { id: id }
    emit Changed {}
    return Complete { id: id }
  }
}
"#;
        assert_semantic_diagnostic_at(incomplete_event, CompilerDiagnosticCode::InvalidEvent, "{}");

        let non_key_partition = r#"
contract NonKeyPartition version 1 {
  entity Row { key (id: uuid) field partition_value: uuid }
  aggregate Rows {
    root Row
    partition_by partition_value
    conflict_key (id)
  }
}
"#;
        let error = validate_contract_source(non_key_partition).expect_err("partition rejects");
        let diagnostic = error
            .semantic()
            .expect("semantic diagnostics")
            .as_slice()
            .iter()
            .find(|diagnostic| {
                diagnostic.code() == CompilerDiagnosticCode::ConflictNotInputComputable
            })
            .expect("C016");
        let start = non_key_partition
            .rfind("partition_value")
            .expect("expression");
        assert_eq!(diagnostic.primary_span().start() as usize, start);
        assert_eq!(
            diagnostic.primary_span().end() as usize,
            start + "partition_value".len()
        );

        let invalid_projection_sum = r#"
contract InvalidProjectionSum version 1 {
  event Flagged { enabled: bool }
  projection Totals {
    source event Flagged
    key (enabled)
    measure total = sum(enabled)
    frontier transactionally_ordered
  }
}
"#;
        let error = validate_contract_source(invalid_projection_sum)
            .expect_err("boolean projection sum rejects");
        let diagnostic = error
            .semantic()
            .expect("semantic diagnostics")
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidProjection)
            .expect("C019");
        let start = invalid_projection_sum
            .rfind("enabled")
            .expect("sum expression");
        assert_eq!(diagnostic.primary_span().start() as usize, start);
        assert_eq!(
            diagnostic.primary_span().end() as usize,
            start + "enabled".len()
        );

        let oversized_key = concat!(
            "contract OversizedKey version 1 { ",
            "entity Row { key (first: string<4096>, second: string<4096>) } }",
        );
        assert_semantic_diagnostic_at(oversized_key, CompilerDiagnosticCode::BoundExceeded, "Row");
    }

    #[test]
    fn success_outcome_collision_points_to_success_and_first_rejection() {
        let source = r#"
contract OutcomeCollision version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Find {
    input id: uuid
    read Row(id) as row else Same { id: id }
    return Same { id: id }
  }
}
"#;
        let error = validate_contract_source(source).expect_err("outcome collision rejects");
        let diagnostic = error
            .semantic()
            .expect("semantic diagnostics")
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidOutcome)
            .expect("C014");
        let rejection = source.find("Same").expect("rejection outcome");
        let success = source.rfind("Same").expect("success outcome");
        assert_eq!(diagnostic.primary_span().start() as usize, success);
        assert_eq!(diagnostic.primary_span().end() as usize, success + 4);
        assert_eq!(
            diagnostic
                .related_span()
                .expect("related rejection")
                .start() as usize,
            rejection
        );
    }

    #[test]
    fn successor_same_version_points_to_version_literal() {
        let genesis_source = r#"
contract SameVersion version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Find {
    input id: uuid
    read Row(id) as row else Missing { id: id }
    return Found { id: id }
  }
}
"#;
        let parent = compile_contract_source(genesis_source).expect("genesis");
        let error = compile_contract_successor(genesis_source, &parent)
            .expect_err("same version successor rejects");
        let diagnostic = error
            .semantic()
            .expect("semantic diagnostics")
            .as_slice()
            .iter()
            .find(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidParent)
            .expect("C022");
        let start = genesis_source.find("version 1").expect("version") + "version ".len();
        assert_eq!(diagnostic.primary_span().start() as usize, start);
        assert_eq!(diagnostic.primary_span().end() as usize, start + 1);
    }

    #[test]
    fn command_tool_registry_is_source_declaration_order_independent() {
        fn visit_permutations<T>(values: &mut [T], index: usize, visit: &mut impl FnMut(&[T])) {
            if index == values.len() {
                visit(values);
                return;
            }
            for selected in index..values.len() {
                values.swap(index, selected);
                visit_permutations(values, index + 1, visit);
                values.swap(index, selected);
            }
        }

        let mut declarations = ["Zulu", "Alpha", "Middle"];
        let mut expected = None;
        let mut permutations = 0;
        visit_permutations(&mut declarations, 0, &mut |commands| {
            let commands = commands
                .iter()
                .map(|name| {
                    format!(
                        "command {name} {{ input id: uuid read Row(id) as row else Missing {{ id: id }} return Found {{ id: id }} }}"
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            let source = format!(
                "contract ToolOrder version 1 {{ entity Row {{ key (id: uuid) }} aggregate Rows {{ root Row partition_by id conflict_key (id) }} {commands} }}"
            );
            let bundle = compile_contract_source(&source).expect("permutation compiles");
            let registry = bundle.mcp_command_names();
            let canonical = registry
                .entries()
                .iter()
                .map(|entry| {
                    (
                        entry.command_id().get(),
                        entry.source_command_name().to_owned(),
                        entry.tool_name().as_str().to_owned(),
                    )
                })
                .collect::<Vec<_>>();
            match &expected {
                Some(expected) => assert_eq!(&canonical, expected),
                None => expected = Some(canonical),
            }
            permutations += 1;
        });
        assert_eq!(permutations, 6);
        assert_eq!(
            expected.expect("one permutation"),
            vec![
                (1, "Zulu".to_owned(), "riffdb.cmd.toolorder.zulu".to_owned()),
                (
                    2,
                    "Alpha".to_owned(),
                    "riffdb.cmd.toolorder.alpha".to_owned(),
                ),
                (
                    3,
                    "Middle".to_owned(),
                    "riffdb.cmd.toolorder.middle".to_owned(),
                ),
            ]
        );
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]

        #[test]
        fn public_compiler_entry_points_are_total_for_arbitrary_bounded_utf8(
            characters in proptest::collection::vec(proptest::prelude::any::<char>(), 0..=2_048),
        ) {
            let source = characters.into_iter().collect::<String>();
            let validation = validate_contract_source(&source);
            let compilation = compile_contract_source(&source);
            for result in [validation.map(|()| None), compilation.map(Some)] {
                match result {
                    Ok(Some(bundle)) => {
                        proptest::prop_assert_eq!(
                            ContractBundle::decode(bundle.canonical_bytes()).expect("compiled bundle decodes"),
                            bundle,
                        );
                    }
                    Ok(None) => {}
                    Err(CompilationError::Syntax(diagnostics)) => {
                        proptest::prop_assert!(!diagnostics.is_empty());
                    }
                    Err(CompilationError::Semantic(diagnostics)) => {
                        proptest::prop_assert!(!diagnostics.is_empty());
                        proptest::prop_assert!(diagnostics.len() <= crate::diagnostic::MAX_COMPILER_DIAGNOSTICS);
                    }
                }
            }
        }
    }

    #[test]
    fn successor_retains_ids_and_unchanged_plan_hashes() {
        let genesis = compile_contract_source(&evolution_source(1, false)).expect("genesis");
        let successor =
            compile_contract_successor(&evolution_source(2, false), &genesis).expect("successor");
        assert_eq!(
            genesis.commands()[0].command_id(),
            successor.commands()[0].command_id()
        );
        assert_eq!(
            genesis.commands()[0].plan_hash(),
            successor.commands()[0].plan_hash()
        );
        assert_eq!(
            successor.parent().expect("parent").bundle_hash(),
            genesis.bundle_hash()
        );
        assert_eq!(successor.compatibility().entries().len(), 1);
        assert_eq!(
            successor.compatibility().entries()[0].code(),
            CompatibilityCode::NoSemanticChange
        );
    }

    #[test]
    fn command_rename_allocates_a_new_public_tool_identity_and_reports_it() {
        let genesis =
            compile_contract_source(&mcp_evolution_source(1, "Allocate")).expect("genesis");
        let unchanged = compile_contract_successor(&mcp_evolution_source(2, "Allocate"), &genesis)
            .expect("unchanged successor");
        assert_eq!(genesis.mcp_command_names(), unchanged.mcp_command_names());
        assert_eq!(
            unchanged.compatibility().entries()[0].code(),
            CompatibilityCode::NoSemanticChange
        );

        let renamed = compile_contract_successor(&mcp_evolution_source(2, "Reallocate"), &genesis)
            .expect("renamed successor");
        let [old] = genesis.mcp_command_names().entries() else {
            panic!("one genesis command")
        };
        let [new] = renamed.mcp_command_names().entries() else {
            panic!("one renamed command")
        };
        assert!(new.command_id() > old.command_id());
        assert_eq!(new.source_command_name(), "Reallocate");
        assert_eq!(
            new.tool_name().as_str(),
            "riffdb.cmd.toolevolution.reallocate"
        );
        assert_eq!(
            renamed.compatibility().overall(),
            CompatibilityClass::Incompatible
        );
        assert_eq!(
            renamed
                .compatibility()
                .entries()
                .iter()
                .map(|entry| (entry.code(), entry.affected_path()))
                .collect::<Vec<_>>(),
            vec![
                (CompatibilityCode::AddedCommand, "command:2"),
                (CompatibilityCode::RemovedIdentity, "command:1"),
                (
                    CompatibilityCode::RemovedIdentity,
                    "command:1/input/field:1"
                ),
                (CompatibilityCode::RemovedIdentity, "command:1/outcome:1"),
                (
                    CompatibilityCode::RemovedIdentity,
                    "command:1/outcome:1/field:1",
                ),
                (CompatibilityCode::RemovedIdentity, "command:1/outcome:2"),
                (
                    CompatibilityCode::RemovedIdentity,
                    "command:1/outcome:2/field:1",
                ),
            ]
        );
        let command_entries = renamed
            .ledger()
            .allocations()
            .iter()
            .filter(|allocation| {
                allocation.namespace().tag() == riffdb_contract_ir::StableIdNamespaceTag::Command
            })
            .flat_map(|allocation| allocation.entries())
            .map(|entry| (entry.id(), entry.name(), entry.state()))
            .collect::<Vec<_>>();
        assert_eq!(
            command_entries,
            vec![
                (1, "Allocate", LineageEntryState::Tombstone),
                (2, "Reallocate", LineageEntryState::Active),
            ]
        );
    }

    #[test]
    fn successor_adds_after_history_and_rejects_tombstone_resurrection() {
        let genesis = compile_contract_source(&evolution_source(1, false)).expect("genesis");
        let genesis_keep_hash = genesis
            .commands()
            .iter()
            .find(|command| command.name() == "Keep")
            .expect("genesis Keep")
            .plan_hash();
        let added =
            compile_contract_successor(&evolution_source(2, true), &genesis).expect("addition");
        let keep = added
            .commands()
            .iter()
            .find(|command| command.name() == "Keep")
            .expect("Keep");
        let add = added
            .commands()
            .iter()
            .find(|command| command.name() == "Add")
            .expect("Add");
        assert!(add.command_id() > keep.command_id());
        assert_eq!(keep.plan_hash(), genesis_keep_hash);
        assert!(
            added
                .compatibility()
                .entries()
                .iter()
                .any(|entry| { entry.code() == CompatibilityCode::AddedCommand })
        );

        let removed = compile_contract_successor(&evolution_source(3, false), &added)
            .expect("removal bundle");
        assert!(removed
            .ledger()
            .allocations()
            .iter()
            .flat_map(|allocation| allocation.entries())
            .any(|entry| entry.name() == "Add" && entry.state() == LineageEntryState::Tombstone));
        let error = compile_contract_successor(&evolution_source(4, true), &removed)
            .expect_err("tombstone resurrection rejects");
        assert!(
            error
                .semantic()
                .expect("semantic")
                .as_slice()
                .iter()
                .any(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::StableIdAllocation)
        );
    }

    #[test]
    fn optional_entity_field_exposed_by_existing_outcome_requires_explicit_version() {
        let genesis = compile_contract_source(&evolution_source(1, false)).expect("genesis");
        let successor_source = evolution_source(2, false).replace(
            "field value: i64",
            "field value: i64 field note: optional<string<8>>",
        );
        let successor = compile_contract_successor(&successor_source, &genesis).expect("successor");
        let codes = successor
            .compatibility()
            .entries()
            .iter()
            .map(|entry| entry.code())
            .collect::<BTreeSet<_>>();
        assert!(codes.contains(&CompatibilityCode::AddedOptionalField));
        assert!(codes.contains(&CompatibilityCode::AddedOptionalOutcomeField));
        assert_eq!(
            successor.compatibility().overall(),
            CompatibilityClass::RequiresExplicitVersion
        );
    }

    #[test]
    fn optional_event_field_and_generated_null_remain_compatible() {
        let genesis = compile_contract_source(&event_evolution_source(1, false)).expect("genesis");
        let successor = compile_contract_successor(&event_evolution_source(2, true), &genesis)
            .expect("successor");
        assert_eq!(
            successor.compatibility().overall(),
            CompatibilityClass::Compatible
        );
        assert_eq!(successor.compatibility().entries().len(), 1);
        assert_eq!(
            successor.compatibility().entries()[0].code(),
            CompatibilityCode::AddedOptionalField
        );
    }

    #[test]
    fn optional_numeric_contexts_accept_signed_minimum_decimal_and_money() {
        let source = r#"
contract OptionalNumerics version 1 {
  entity Row {
    key (id: uuid)
    field minimum: optional<i64>
    field decimal_value: optional<decimal<28,2>>
    field money_value: optional<money<USD>>
  }
  event Changed {
    minimum: optional<i64>
    decimal_value: optional<decimal<28,2>>
    money_value: optional<money<USD>>
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    mutate Row(id) as row else Missing { id: id }
    set row.minimum = -9223372036854775808
    set row.decimal_value = -1.25
    set row.money_value = -2.50
    emit Changed {
      minimum: -9223372036854775808,
      decimal_value: -1.25,
      money_value: -2.50
    }
    return Updated { row: row, minimum: -9223372036854775808 }
  }
}
"#;
        let bundle = compile_contract_source(source).expect("optional numerics compile");
        let expressions = bundle.commands()[0].expressions();
        assert_eq!(
            expressions
                .nodes()
                .iter()
                .filter(|node| matches!(
                    node.kind(),
                    ExpressionKind::Constant(CanonicalValue::I64(i64::MIN))
                ))
                .count(),
            3
        );
        for tag in [ValueTypeTag::Decimal, ValueTypeTag::Money] {
            assert_eq!(
                expressions
                    .nodes()
                    .iter()
                    .filter(|node| {
                        node.result_type().tag() == tag
                            && matches!(
                                node.kind(),
                                ExpressionKind::Unary {
                                    operator: UnaryOperator::Negate,
                                    ..
                                }
                            )
                    })
                    .count(),
                2
            );
        }
    }

    #[test]
    fn optional_assignment_context_keeps_binary_arithmetic_nonoptional() {
        let source = r#"
contract OptionalArithmetic version 1 {
  entity Row {
    key (id: uuid)
    field calculated: optional<i64>
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input idempotency_key: string<128>
    input id: uuid
    input amount: i64
    idempotency_key idempotency_key
    mutate Row(id) as row else Missing { id: id }
    set row.calculated = amount + 1
    return Updated { row: row }
  }
}
"#;
        let bundle = compile_contract_source(source).expect("optional arithmetic compiles");
        let command = &bundle.commands()[0];
        let binary = command
            .expressions()
            .nodes()
            .iter()
            .find(|node| matches!(node.kind(), ExpressionKind::Binary { .. }))
            .expect("binary arithmetic node");
        assert_eq!(binary.result_type(), &ValueType::i64());
        let destination = command.bindings()[0].entity_type();
        let field = bundle
            .schema()
            .entity(destination)
            .expect("entity")
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "calculated")
            .expect("optional destination");
        assert_eq!(
            field.value_type(),
            &ValueType::optional(ValueType::i64()).expect("optional")
        );
    }

    #[test]
    fn amplified_generated_outcome_schema_reports_bound_exceeded() {
        compile_contract_source(&amplified_outcome_source(64, 128))
            .expect("bounded generated outcome schema compiles");
        let source = amplified_outcome_source(64, 512);
        assert_semantic_diagnostic_at(&source, CompilerDiagnosticCode::BoundExceeded, "Expand");
        assert_eq!(
            CompilerDiagnosticCode::BoundExceeded.help(),
            Some("reduce declared bounds or the number of schema components")
        );
    }

    #[test]
    fn mutation_index_cross_product_is_rejected_during_checked_plan_lowering() {
        compile_contract_source(&indexed_mutation_source(1_365))
            .expect("exact combined validation-target boundary compiles");
        let source = indexed_mutation_source(1_366);
        assert_semantic_diagnostic_at(&source, CompilerDiagnosticCode::BoundExceeded, "Change");
    }

    #[test]
    fn optional_event_addition_does_not_hide_an_unrelated_plan_change() {
        let genesis = compile_contract_source(&event_evolution_source(1, false)).expect("genesis");
        let successor_source = event_evolution_source(2, true)
            .replace("set changed_row.value = 1", "set changed_row.value = 2");
        let successor = compile_contract_successor(&successor_source, &genesis).expect("successor");
        let codes = successor
            .compatibility()
            .entries()
            .iter()
            .map(|entry| entry.code())
            .collect::<BTreeSet<_>>();
        assert!(codes.contains(&CompatibilityCode::AddedOptionalField));
        assert!(codes.contains(&CompatibilityCode::ExistingPlanChange));
        assert_eq!(
            successor.compatibility().overall(),
            CompatibilityClass::Incompatible
        );
    }

    #[test]
    fn optional_outcome_addition_does_not_hide_an_unrelated_plan_change() {
        let genesis = compile_contract_source(&event_evolution_source(1, false)).expect("genesis");
        let successor_source = event_evolution_source(2, false)
            .replace(
                "field value: i64",
                "field value: i64 field note: optional<string<8>>",
            )
            .replace("set changed_row.value = 1", "set changed_row.value = 2");
        let successor = compile_contract_successor(&successor_source, &genesis).expect("successor");
        let codes = successor
            .compatibility()
            .entries()
            .iter()
            .map(|entry| entry.code())
            .collect::<BTreeSet<_>>();
        assert!(codes.contains(&CompatibilityCode::AddedOptionalField));
        assert!(codes.contains(&CompatibilityCode::AddedOptionalOutcomeField));
        assert!(codes.contains(&CompatibilityCode::ExistingPlanChange));
        assert_eq!(
            successor.compatibility().overall(),
            CompatibilityClass::Incompatible
        );
    }

    #[test]
    fn added_outcome_requires_explicit_version_without_spurious_plan_incompatibility() {
        let genesis = compile_contract_source(&evolution_source(1, false)).expect("genesis");
        let successor_source = evolution_source(2, false).replace(
            "return KeepFound { row: kept_row }",
            concat!(
                "require positive: 1 > 0 else AddedOutcome {}\n    ",
                "return KeepFound { row: kept_row }",
            ),
        );
        let successor = compile_contract_successor(&successor_source, &genesis).expect("successor");
        assert_eq!(
            successor.compatibility().overall(),
            CompatibilityClass::RequiresExplicitVersion
        );
        assert_eq!(successor.compatibility().entries().len(), 1);
        assert_eq!(
            successor.compatibility().entries()[0].code(),
            CompatibilityCode::AddedOutcome
        );
    }

    #[test]
    fn parser_and_semantic_diagnostics_remain_distinct() {
        let syntax = validate_contract_source("not a contract").expect_err("syntax rejects");
        assert!(syntax.syntax().is_some());
        assert!(syntax.semantic().is_none());

        let collision = r#"
contract Example version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Allocate {
    input id: uuid
    read Row(id) as row else Missing { id: id }
    return Found { row: row }
  }
  command ALLOCATE {
    input id: uuid
    read Row(id) as row else Missing { id: id }
    return Found { row: row }
  }
}
"#;
        let semantic = validate_contract_source(collision).expect_err("collision rejects");
        assert_eq!(
            semantic
                .semantic()
                .expect("semantic diagnostics")
                .as_slice()[0]
                .code(),
            CompilerDiagnosticCode::CommandToolNameCollision
        );
    }

    #[test]
    fn required_relationship_is_canonical_and_exact_read_is_preserved() {
        let source = relationship_source(
            "reference parent_ref (tenant_id, parent_id) -> Parent(tenant_id, parent_id)",
            concat!(
                "read Parent(tenant_id, parent_id) as parent ",
                "else ParentMissing { parent_id: parent_id }\n    ",
                "create Child(tenant_id, child_id) as row ",
                "else ChildExists { child_id: child_id }"
            ),
        );
        let bundle = compile_contract_source(&source).expect("safe relationship compiles");
        let relationship = &bundle.schema().relationships()[0];
        assert_eq!(relationship.name(), "parent_ref");
        assert_eq!(relationship.source_fields().len(), 2);
        assert_eq!(relationship.target_fields().len(), 2);
        let command = &bundle.commands()[0];
        assert_eq!(command.bindings().len(), 2);
        assert_eq!(
            command.bindings()[0].mode(),
            riffdb_contract_ir::BindingMode::Read
        );
        assert_eq!(command.relationship_checks().len(), 1);
        let explanation = CommandExplain::from_plan(command).render_text();
        assert!(explanation.contains(
            "relationship:parent_ref source-binding:1 exact-target-read:0 commit-revalidated:true"
        ));
        let decoded = ContractBundle::decode(bundle.canonical_bytes()).expect("round trip");
        assert_eq!(decoded, bundle);
    }

    #[test]
    fn relationship_change_on_mutation_requires_and_preserves_exact_read() {
        let safe = relationship_mutation_source(concat!(
            "read Parent(tenant_id, parent_id) as parent ",
            "else ParentMissing { parent_id: parent_id }\n    "
        ));
        let bundle = compile_contract_source(&safe).expect("safe mutation compiles");
        let command = &bundle.commands()[0];
        assert_eq!(command.relationship_checks().len(), 1);
        assert_eq!(command.relationship_checks()[0].target_binding().get(), 0);
        assert_eq!(command.relationship_checks()[0].source_binding().get(), 1);

        let dangling = relationship_mutation_source("");
        assert_semantic_diagnostic_at(
            &dangling,
            CompilerDiagnosticCode::MissingRelationshipRead,
            "parent_ref",
        );
    }

    #[test]
    fn adding_relationship_is_an_explicit_incompatible_invariant_change() {
        let genesis_source = relationship_source(
            "",
            concat!(
                "read Parent(tenant_id, parent_id) as parent ",
                "else ParentMissing { parent_id: parent_id }\n    ",
                "create Child(tenant_id, child_id) as row ",
                "else ChildExists { child_id: child_id }"
            ),
        );
        let genesis = compile_contract_source(&genesis_source).expect("genesis");
        let successor_source = relationship_source(
            "reference parent_ref (tenant_id, parent_id) -> Parent(tenant_id, parent_id)",
            concat!(
                "read Parent(tenant_id, parent_id) as parent ",
                "else ParentMissing { parent_id: parent_id }\n    ",
                "create Child(tenant_id, child_id) as row ",
                "else ChildExists { child_id: child_id }"
            ),
        )
        .replace("version 1", "version 2");
        let successor =
            compile_contract_successor(&successor_source, &genesis).expect("successor compiles");
        assert_eq!(
            successor.compatibility().overall(),
            CompatibilityClass::Incompatible
        );
        assert!(
            successor
                .compatibility()
                .entries()
                .iter()
                .any(|entry| entry.code() == CompatibilityCode::InvariantChange)
        );
    }

    #[test]
    fn relationship_change_without_dominating_exact_read_fails_at_declaration() {
        for bindings in [
            concat!(
                "create Child(tenant_id, child_id) as row ",
                "else ChildExists { child_id: child_id }"
            ),
            concat!(
                "create Child(tenant_id, child_id) as row ",
                "else ChildExists { child_id: child_id }\n    ",
                "read Parent(tenant_id, parent_id) as parent ",
                "else ParentMissing { parent_id: parent_id }"
            ),
            concat!(
                "read Parent(tenant_id, wrong_parent_id) as parent ",
                "else ParentMissing { parent_id: wrong_parent_id }\n    ",
                "create Child(tenant_id, child_id) as row ",
                "else ChildExists { child_id: child_id }"
            ),
        ] {
            let source = relationship_source(
                "reference parent_ref (tenant_id, parent_id) -> Parent(tenant_id, parent_id)",
                bindings,
            );
            assert_semantic_diagnostic_at(
                &source,
                CompilerDiagnosticCode::MissingRelationshipRead,
                "parent_ref",
            );
        }
    }

    #[test]
    fn partial_and_cross_partition_relationships_fail_closed() {
        let partial = relationship_source(
            "reference parent_ref (parent_id) -> Parent(parent_id)",
            concat!(
                "read Parent(tenant_id, parent_id) as parent ",
                "else ParentMissing { parent_id: parent_id }\n    ",
                "create Child(tenant_id, child_id) as row ",
                "else ChildExists { child_id: child_id }"
            ),
        );
        assert_semantic_diagnostic_at(
            &partial,
            CompilerDiagnosticCode::InvalidRelationship,
            "parent_ref",
        );

        let cross = relationship_source(
            "reference parent_ref (tenant_id, parent_id) -> External(tenant_id, parent_id)",
            concat!(
                "read External(tenant_id, parent_id) as parent ",
                "else ParentMissing { parent_id: parent_id }\n    ",
                "create Child(tenant_id, child_id) as row ",
                "else ChildExists { child_id: child_id }"
            ),
        )
        .replace(
            "aggregate Family {",
            concat!(
                "aggregate ExternalFamily { root External partition_by tenant_id ",
                "conflict_key (tenant_id, parent_id) }\n  aggregate Family {"
            ),
        )
        .replace("    child External\n", "");
        assert_semantic_diagnostic_at(
            &cross,
            CompilerDiagnosticCode::InvalidRelationship,
            "parent_ref",
        );
    }

    #[test]
    fn relationship_negative_corpus_has_stable_source_spanned_diagnostics() {
        for (source, code) in [
            (
                include_str!("../../../fixtures/compiler/relationships/dangling-create.riff"),
                CompilerDiagnosticCode::MissingRelationshipRead,
            ),
            (
                include_str!("../../../fixtures/compiler/relationships/late-read.riff"),
                CompilerDiagnosticCode::MissingRelationshipRead,
            ),
            (
                include_str!("../../../fixtures/compiler/relationships/partial-target.riff"),
                CompilerDiagnosticCode::InvalidRelationship,
            ),
            (
                include_str!("../../../fixtures/compiler/relationships/cross-partition.riff"),
                CompilerDiagnosticCode::InvalidRelationship,
            ),
            (
                include_str!("../../../fixtures/compiler/relationships/optional-source.riff"),
                CompilerDiagnosticCode::InvalidRelationship,
            ),
            (
                include_str!("../../../fixtures/compiler/relationships/type-mismatch.riff"),
                CompilerDiagnosticCode::InvalidRelationship,
            ),
        ] {
            let error = validate_contract_source(source).expect_err("negative fixture rejects");
            let diagnostic = error
                .semantic()
                .expect("semantic diagnostic")
                .as_slice()
                .iter()
                .find(|diagnostic| diagnostic.code() == code)
                .expect("expected relationship diagnostic");
            assert_eq!(
                &source[diagnostic.primary_span().start() as usize
                    ..diagnostic.primary_span().end() as usize],
                "parent"
            );
        }
    }

    fn relationship_source(reference: &str, bindings: &str) -> String {
        format!(
            r#"
contract Relationships version 1 {{
  entity Tenant {{
    key (tenant_id: uuid)
  }}
  entity Parent {{
    key (tenant_id: uuid, parent_id: uuid)
  }}
  entity External {{
    key (tenant_id: uuid, parent_id: uuid)
  }}
  entity Child {{
    key (tenant_id: uuid, child_id: uuid)
    field parent_id: uuid
    {reference}
  }}
  aggregate Family {{
    root Tenant
    child Parent
    child External
    child Child
    partition_by tenant_id
    conflict_key (tenant_id)
  }}
  command CreateChild {{
    input idempotency_key: string<128>
    input tenant_id: uuid
    input parent_id: uuid
    input wrong_parent_id: uuid
    input child_id: uuid
    idempotency_key idempotency_key
    {bindings}
    set row.parent_id = parent_id
    return Created {{ record: row }}
  }}
}}
"#
        )
    }

    fn relationship_mutation_source(parent_read: &str) -> String {
        format!(
            r#"
contract RelationshipMutation version 1 {{
  entity Tenant {{ key (tenant_id: uuid) }}
  entity Parent {{ key (tenant_id: uuid, parent_id: uuid) }}
  entity Child {{
    key (tenant_id: uuid, child_id: uuid)
    field parent_id: uuid
    reference parent_ref (tenant_id, parent_id) -> Parent(tenant_id, parent_id)
  }}
  aggregate Family {{
    root Tenant
    child Parent
    child Child
    partition_by tenant_id
    conflict_key (tenant_id)
  }}
  command ChangeChildParent {{
    input idempotency_key: string<128>
    input tenant_id: uuid
    input parent_id: uuid
    input child_id: uuid
    idempotency_key idempotency_key
    {parent_read}mutate Child(tenant_id, child_id) as row
      else ChildMissing {{ child_id: child_id }}
    set row.parent_id = parent_id
    return Changed {{ record: row }}
  }}
}}
"#
        )
    }

    #[test]
    fn declared_unique_key_adds_input_computable_conflict_to_create_and_change() {
        let source = unique_source("set user.email = email");
        let bundle = compile_contract_source(&source).expect("unique commands compile");
        assert_eq!(bundle.schema().unique_keys().len(), 1);
        let create = bundle
            .commands()
            .iter()
            .find(|command| command.name() == "CreateUser")
            .expect("create");
        let change = bundle
            .commands()
            .iter()
            .find(|command| command.name() == "ChangeEmail")
            .expect("change");
        for command in [create, change] {
            assert_eq!(command.unique_conflicts().len(), 1);
            assert_eq!(command.unique_conflicts()[0].unique_name(), "user_email");
            assert_eq!(command.unique_conflicts()[0].expressions().len(), 2);
            assert!(
                CommandExplain::from_plan(command)
                    .render_text()
                    .contains("unique:user_email")
            );
        }
        assert_eq!(create.locality().conflict_keys().len(), 1);
        assert_eq!(change.locality().conflict_keys().len(), 1);
        assert_eq!(
            ContractBundle::decode(bundle.canonical_bytes()).expect("round trip"),
            bundle
        );
    }

    #[test]
    fn changed_unique_value_must_be_input_computable() {
        let source = unique_source("set user.email = user.email");
        assert_semantic_diagnostic_at(
            &source,
            CompilerDiagnosticCode::UniqueKeyNotInputComputable,
            "user_email",
        );
    }

    fn unique_source(change_effect: &str) -> String {
        format!(
            r#"
contract UniqueUsers version 1 {{
  entity Organization {{ key (organization_id: uuid) }}
  entity User {{
    key (organization_id: uuid, user_id: uuid)
    field email: string<128>
    unique user_email (organization_id, email)
  }}
  aggregate OrganizationRoot {{
    root Organization
    child User
    partition_by organization_id
    conflict_key (organization_id)
  }}
  command CreateUser {{
    input request: string<128>
    input organization_id: uuid
    input user_id: uuid
    input email: string<128>
    idempotency_key request
    create User(organization_id, user_id) as user else UserExists {{}}
    set user.email = email
    return Created {{ user: user }}
  }}
  command ChangeEmail {{
    input request: string<128>
    input organization_id: uuid
    input user_id: uuid
    input email: string<128>
    idempotency_key request
    mutate User(organization_id, user_id) as user else UserMissing {{}}
    {change_effect}
    return Changed {{ user: user }}
  }}
}}
"#
        )
    }

    fn evolution_source(version: u64, add_command: bool) -> String {
        let additional = if add_command {
            r#"
  command Add {
    input id: uuid
    read Row(id) as added_row else AddMissing { id: id }
    return AddFound { row: added_row }
  }
"#
        } else {
            ""
        };
        format!(
            r#"
contract Evolution version {version} {{
  entity Row {{ key (id: uuid) field value: i64 }}
  aggregate Rows {{ root Row partition_by id conflict_key (id) }}
  command Keep {{
    input id: uuid
    read Row(id) as kept_row else KeepMissing {{ id: id }}
    return KeepFound {{ row: kept_row }}
  }}
{additional}}}
"#
        )
    }

    fn event_evolution_source(version: u64, add_optional: bool) -> String {
        let optional = if add_optional {
            "note: optional<string<8>>"
        } else {
            ""
        };
        format!(
            r#"
contract EventEvolution version {version} {{
  entity Row {{ key (id: uuid) field value: i64 }}
  event Changed {{ id: uuid {optional} }}
  aggregate Rows {{ root Row partition_by id conflict_key (id) }}
  command Change {{
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    mutate Row(id) as changed_row else Missing {{ id: id }}
    set changed_row.value = 1
    emit Changed {{ id: id }}
    return ChangedOutcome {{ row: changed_row }}
  }}
}}
"#
        )
    }

    fn mcp_evolution_source(version: u64, command_name: &str) -> String {
        format!(
            r#"
contract ToolEvolution version {version} {{
  entity Row {{ key (id: uuid) }}
  aggregate Rows {{ root Row partition_by id conflict_key (id) }}
  command {command_name} {{
    input id: uuid
    read Row(id) as row else Missing {{ id: id }}
    return Found {{ row: row }}
  }}
}}
"#
        )
    }
}
