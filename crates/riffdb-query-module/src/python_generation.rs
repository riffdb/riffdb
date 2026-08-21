//! Deterministic Python application-binding generation.

use std::collections::BTreeSet;
use std::fmt::{self, Write as _};

use riffdb_contract_ir::{ContractBundle, RecordTypeRef, ValueType, ValueTypeTag};
use riffdb_contract_syntax::ast::{Binding, Declaration, EntityItem, OutcomeExpression};
use riffdb_query_ir::{
    NamedTypeSchema, ReactiveModulePlanV1, ReactiveOperationPlanV1, ReactiveParameterV1,
};
use riffdb_riffql_syntax::{FieldSelection, Selection};

use crate::QueryModule;
use crate::generation::{
    RustCompactResultShape, embedding_command_facades, rust_compact_result_shape,
    vector_inspection_facades, workflow_revision_bindings, workflow_success_outcome_name,
};

/// Source symbol responsible for one Python name collision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonGenerationError {
    query_name: Option<String>,
    symbol_path: Vec<String>,
}

/// Located Python generation diagnostic in one caller-owned source file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PythonGenerationLocation {
    query_name: Option<String>,
    symbol_path: Vec<String>,
    span: (u32, u32),
}

impl PythonGenerationLocation {
    /// Query source name, or `None` for the contract source.
    #[must_use]
    pub fn query_name(&self) -> Option<&str> {
        self.query_name.as_deref()
    }

    /// Stable symbolic source path.
    #[must_use]
    pub fn symbol_path(&self) -> &[String] {
        &self.symbol_path
    }

    /// Half-open UTF-8 byte span in the selected source.
    #[must_use]
    pub const fn span(&self) -> (u32, u32) {
        self.span
    }
}

impl PythonGenerationError {
    fn contract(symbol_path: Vec<String>) -> Self {
        Self {
            query_name: None,
            symbol_path,
        }
    }

    fn query(query_name: &str, symbol_path: Vec<String>) -> Self {
        Self {
            query_name: Some(query_name.to_owned()),
            symbol_path,
        }
    }

    /// Locates the rejected source symbol using the parser-owned AST spans.
    #[must_use]
    pub fn locate(
        &self,
        contract_source: &str,
        query_sources: &[(&str, &str)],
    ) -> Option<PythonGenerationLocation> {
        let span = match self.query_name.as_deref() {
            Some(query_name) => {
                let source = query_sources
                    .iter()
                    .find_map(|(name, source)| (*name == query_name).then_some(*source))?;
                locate_query_symbol(source, &self.symbol_path)?
            }
            None => locate_contract_symbol(contract_source, &self.symbol_path)?,
        };
        Some(PythonGenerationLocation {
            query_name: self.query_name.clone(),
            symbol_path: self.symbol_path.clone(),
            span,
        })
    }
}

impl fmt::Display for PythonGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application symbols collide after Python name normalization")
    }
}

impl std::error::Error for PythonGenerationError {}

fn locate_contract_symbol(source: &str, path: &[String]) -> Option<(u32, u32)> {
    let document = riffdb_contract_syntax::parse_contract(source).ok()?;
    let contract = &document.contract.value;
    match path {
        [kind, name] if kind == "contract" && contract.name.value == *name => {
            Some(contract_span(contract.name.span))
        }
        [kind, name] if kind == "enum" => contract.declarations.iter().find_map(|declaration| {
            let Declaration::Enum(enumeration) = &declaration.value else {
                return None;
            };
            (enumeration.name.value == *name).then(|| contract_span(enumeration.name.span))
        }),
        [kind, enum_name, variant_kind, variant] if kind == "enum" && variant_kind == "variant" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Enum(enumeration) = &declaration.value else {
                    return None;
                };
                (enumeration.name.value == *enum_name)
                    .then(|| {
                        enumeration
                            .variants
                            .iter()
                            .find(|candidate| candidate.value == *variant)
                            .map(|candidate| contract_span(candidate.span))
                    })
                    .flatten()
            })
        }
        [kind, entity_name] if kind == "entity" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Entity(entity) = &declaration.value else {
                    return None;
                };
                (entity.name.value == *entity_name).then(|| contract_span(entity.name.span))
            })
        }
        [kind, entity_name, field_kind, field] if kind == "entity" && field_kind == "field" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Entity(entity) = &declaration.value else {
                    return None;
                };
                if entity.name.value != *entity_name {
                    return None;
                }
                entity.items.iter().find_map(|item| match &item.value {
                    EntityItem::Key(key) => key
                        .fields
                        .iter()
                        .find(|candidate| candidate.value.name.value == *field)
                        .map(|candidate| contract_span(candidate.value.name.span)),
                    EntityItem::Field(candidate) if candidate.name.value == *field => {
                        Some(contract_span(candidate.name.span))
                    }
                    EntityItem::Field(_)
                    | EntityItem::DeletePolicy(_)
                    | EntityItem::Invariant(_)
                    | EntityItem::Index(_)
                    | EntityItem::Unique(_)
                    | EntityItem::Reference(_)
                    | EntityItem::VectorField(_) => None,
                })
            })
        }
        [kind, command_name] if kind == "command" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Command(command) = &declaration.value else {
                    return None;
                };
                (command.name.value == *command_name).then(|| contract_span(command.name.span))
            })
        }
        [kind, command_name, input_kind, input] if kind == "command" && input_kind == "input" => {
            contract.declarations.iter().find_map(|declaration| {
                let Declaration::Command(command) = &declaration.value else {
                    return None;
                };
                (command.name.value == *command_name)
                    .then(|| {
                        command
                            .inputs
                            .iter()
                            .find(|candidate| candidate.value.field.name.value == *input)
                            .map(|candidate| contract_span(candidate.value.field.name.span))
                    })
                    .flatten()
            })
        }
        [kind, command_name, outcome_kind, outcome]
            if kind == "command" && outcome_kind == "outcome" =>
        {
            locate_contract_outcome(contract, command_name, outcome)
                .map(|candidate| contract_span(candidate.name.span))
        }
        [kind, command_name, outcome_kind, outcome, field_kind, field]
            if kind == "command" && outcome_kind == "outcome" && field_kind == "field" =>
        {
            locate_contract_outcome(contract, command_name, outcome).and_then(|candidate| {
                candidate
                    .payload
                    .value
                    .fields
                    .iter()
                    .find(|candidate| candidate.value.name.value == *field)
                    .map(|candidate| contract_span(candidate.value.name.span))
            })
        }
        _ => None,
    }
}

fn locate_contract_outcome<'a>(
    contract: &'a riffdb_contract_syntax::ast::Contract,
    command_name: &str,
    outcome_name: &str,
) -> Option<&'a OutcomeExpression> {
    let command = contract.declarations.iter().find_map(|declaration| {
        let Declaration::Command(command) = &declaration.value else {
            return None;
        };
        (command.name.value == command_name).then_some(command)
    })?;
    command
        .bindings
        .iter()
        .map(|binding| match &binding.value {
            Binding::Read(binding)
            | Binding::Mutate(binding)
            | Binding::Create(binding)
            | Binding::Delete(binding) => &binding.failure.value,
        })
        .chain(
            command
                .requirements
                .iter()
                .map(|requirement| &requirement.value.rejection.value),
        )
        .chain(std::iter::once(&command.return_clause.value.outcome.value))
        .find(|outcome| outcome.name.value == outcome_name)
}

fn contract_span(span: riffdb_contract_syntax::Span) -> (u32, u32) {
    (span.start(), span.end())
}

fn locate_query_symbol(source: &str, path: &[String]) -> Option<(u32, u32)> {
    let document = riffdb_riffql_syntax::parse_query(source).ok()?;
    let [kind, query_name, rest @ ..] = path else {
        return None;
    };
    if kind != "query"
        || document
            .name
            .as_ref()
            .is_some_and(|name| name.value.as_str() != query_name)
    {
        return None;
    }
    match rest {
        [] => document.name.map(|name| query_span(name.span)),
        [parameter_kind, parameter, ..] if parameter_kind == "parameter" => document
            .parameters
            .iter()
            .find(|candidate| candidate.name.value.as_str() == parameter)
            .map(|candidate| query_span(candidate.name.span)),
        [outcome_kind, outcome] if outcome_kind == "outcome" => document
            .body
            .outcomes
            .iter()
            .chain(document.body.outcome.iter())
            .find(|candidate| candidate.value.as_str() == outcome)
            .map(|candidate| query_span(candidate.span)),
        [field_kind, fields @ ..] if field_kind == "field" && !fields.is_empty() => {
            locate_query_field(&document.body.selection, fields)
        }
        _ => None,
    }
}

fn locate_query_field(selection: &Selection, path: &[String]) -> Option<(u32, u32)> {
    let (name, remaining) = path.split_first()?;
    let field = selection
        .fields
        .iter()
        .find(|field| query_field_name(field) == name)?;
    if remaining.is_empty() {
        return Some(query_field_span(field));
    }
    field
        .nested
        .as_ref()
        .and_then(|nested| locate_query_field(nested, remaining))
        .or_else(|| Some(query_field_span(field)))
}

fn query_field_name(field: &FieldSelection) -> &str {
    field.alias.as_ref().map_or_else(
        || {
            field
                .source
                .value
                .0
                .last()
                .expect("the parser only produces nonempty paths")
                .value
                .as_str()
        },
        |alias| alias.value.as_str(),
    )
}

fn query_field_span(field: &FieldSelection) -> (u32, u32) {
    field.alias.as_ref().map_or_else(
        || {
            query_span(
                field
                    .source
                    .value
                    .0
                    .last()
                    .expect("the parser only produces nonempty paths")
                    .span,
            )
        },
        |alias| query_span(alias.span),
    )
}

const fn query_span(span: riffdb_riffql_syntax::Span) -> (u32, u32) {
    (span.start, span.end)
}

/// Generates one complete, identity-pinned Python application module.
pub fn generate_python_client(
    module: &QueryModule,
    contract: &ContractBundle,
) -> Result<String, PythonGenerationError> {
    validate_names(module, contract)?;
    let has_vector_inspection = !vector_inspection_facades(module, contract).is_empty();
    let vector_runtime_imports = if has_vector_inspection {
        "\n             TypedVectorInspectionResult, VectorInspectionOptions, VectorModelVersionResult,\n\
             VectorStalenessResult, WorkflowSuccessorRevision"
    } else {
        " WorkflowSuccessorRevision"
    };
    let vector_binding_import = if has_vector_inspection {
        ", encode_value"
    } else {
        ""
    };
    let vector_typing_import = if has_vector_inspection { ", cast" } else { "" };
    let mut output = String::new();
    writeln!(
        output,
        "# @generated by riffdb-query-module; do not edit.\n\
         from __future__ import annotations\n\n\
         from collections.abc import Callable, Sequence\n\
         from dataclasses import dataclass, field\n\
         from decimal import Decimal\n\
         from enum import StrEnum\n\
         from typing import Annotated, Final, Literal, TypeAlias{vector_typing_import}\n\
         from uuid import UUID\n\n\
         from riffdb_application import (\n\
             AsyncApplicationTransport, AttemptBudget, CommandBatchOptions,\n\
             CommandBatchProgress, CommandBatchResult, Money, QueryOptions, RiffDate,\n\
             SyncApplicationTransport, Timestamp, TypedCommandResult, TypedQueryResult,{vector_runtime_imports},\n\
         )\n\
         from riffdb_application._binding import decode_variant, encode_record{vector_binding_import}\n"
    )
    .expect("String writes cannot fail");
    output.push_str(
        "def _compact_tag(value: object, tag: str, keys: frozenset[str]) -> dict[str, object]:\n    if not isinstance(value, dict) or value.get(\"$riffdb\") != tag or frozenset(value) != keys:\n        raise ValueError(\"invalid RiffDB compact value\")\n    return value\n\n",
    );
    writeln!(
        output,
        "CONTRACT_LINEAGE: Final[str] = {:?}\nCONTRACT_VERSION: Final[int] = {}\n\
         CONTRACT_BUNDLE_HASH: Final[str] = {:?}\nQUERY_MODULE_HASH: Final[str] = {:?}\n",
        module.contract_lineage().as_str(),
        module.contract_version().get(),
        hex(module.contract_hash().as_bytes()),
        hex(module.identity().as_bytes()),
    )
    .expect("String writes cannot fail");

    for enumeration in contract.schema().enums() {
        writeln!(output, "class {}(StrEnum):", pascal(enumeration.name()))
            .expect("String writes cannot fail");
        for variant in enumeration.variants() {
            writeln!(
                output,
                "    {} = {:?}",
                screaming_snake(variant.name()),
                variant.name()
            )
            .expect("String writes cannot fail");
        }
        output.push('\n');
    }

    for entity in contract.schema().entities() {
        emit_contract_record(
            &mut output,
            &pascal(entity.name()),
            entity
                .record()
                .fields()
                .iter()
                .map(|field| (field.name(), field.value_type())),
            contract,
        );
    }

    for query in module.queries() {
        let wire_name = query.name();
        let name = pascal(wire_name);
        let schemas = query.plan().schemas();
        for parameter in schemas.parameters() {
            emit_named_nested(
                &mut output,
                &format!("{name}Params{}", pascal(parameter.name())),
                parameter.value_type(),
                contract,
                false,
            );
        }
        writeln!(
            output,
            "{}_QUERY_PLAN_HASH: Final[str] = {:?}",
            screaming_snake(wire_name),
            hex(query.plan().identity().as_bytes())
        )
        .expect("String writes cannot fail");
        if !query.plan().secret_outputs().is_empty() {
            writeln!(
                output,
                "{}_SECRET_OUTPUTS: Final[tuple[tuple[str, str, str], ...]] = (",
                screaming_snake(wire_name)
            )
            .expect("String writes cannot fail");
            for secret in query.plan().secret_outputs() {
                writeln!(
                    output,
                    "    ({:?}, {:?}, {:?}),",
                    wire_name,
                    secret.entity(),
                    secret.field()
                )
                .expect("String writes cannot fail");
            }
            output.push_str(")\n\n");
        }
        writeln!(
            output,
            "\n@dataclass(frozen=True, slots=True, kw_only=True)\nclass {name}Params:"
        )
        .expect("String writes cannot fail");
        if schemas.parameters().is_empty() {
            output.push_str("    pass\n");
        }
        for parameter in schemas.parameters() {
            let nested = format!("{name}Params{}", pascal(parameter.name()));
            let defaulted = parameter.has_default()
                || is_cursor(parameter.value_type())
                || matches!(parameter.value_type(), NamedTypeSchema::Optional(_));
            let mut value_type = python_named_type(parameter.value_type(), &nested, contract);
            let default = if defaulted {
                if !value_type.ends_with(" | None") {
                    value_type.push_str(" | None");
                }
                " = None"
            } else {
                ""
            };
            writeln!(
                output,
                "    {}: {value_type}{default}",
                python_identifier(parameter.name())
            )
            .expect("String writes cannot fail");
        }
        output.push('\n');
        for branch in schemas.results() {
            let branch_name = format!("{name}{}", pascal(branch.name()));
            for result_field in branch.fields() {
                emit_named_nested(
                    &mut output,
                    &format!("{branch_name}{}", pascal(result_field.name())),
                    result_field.value_type(),
                    contract,
                    !query.plan().secret_outputs().is_empty(),
                );
            }
            writeln!(
                output,
                "@dataclass(frozen=True, slots=True, kw_only=True)\nclass {branch_name}:"
            )
            .expect("String writes cannot fail");
            for result_field in branch.fields() {
                let nested = format!("{branch_name}{}", pascal(result_field.name()));
                writeln!(
                    output,
                    "    {}: {}{}",
                    python_identifier(result_field.name()),
                    python_named_type(result_field.value_type(), &nested, contract),
                    if query.plan().secret_outputs().is_empty() {
                        ""
                    } else {
                        " = field(repr=False)"
                    }
                )
                .expect("String writes cannot fail");
            }
            writeln!(
                output,
                "    outcome: Literal[{:?}] = field(default={:?}, init=False)\n",
                branch.name(),
                branch.name()
            )
            .expect("String writes cannot fail");
        }
        write!(output, "{name}Result: TypeAlias = ").expect("String writes cannot fail");
        for (index, branch) in schemas.results().iter().enumerate() {
            if index != 0 {
                output.push_str(" | ");
            }
            write!(output, "{name}{}", pascal(branch.name())).expect("String writes cannot fail");
        }
        output.push_str("\n\n");
        if let Some(shape) = query.plan().common_covered_result().and_then(
            |(result_name, layout, selected_fields)| {
                rust_compact_result_shape(
                    schemas,
                    &result_name,
                    &layout,
                    &selected_fields,
                    contract,
                )
            },
        ) {
            emit_python_compact_query_decoder(&mut output, &name, &shape, contract);
        }
    }

    let mut commands = contract
        .commands()
        .iter()
        .filter(|command| !command.is_reimport())
        .collect::<Vec<_>>();
    commands.sort_by(|left, right| left.name().cmp(right.name()));
    for command in &commands {
        let wire_name = command.name();
        let name = pascal(wire_name);
        emit_contract_record(
            &mut output,
            &format!("{name}Input"),
            command
                .input()
                .record()
                .fields()
                .iter()
                .map(|field| (field.name(), field.value_type())),
            contract,
        );
        emit_python_embedding_constructors(&mut output, command, contract);
        writeln!(
            output,
            "{}_PLAN_HASH: Final[str] = {:?}",
            screaming_snake(wire_name),
            hex(command.plan_hash().as_bytes())
        )
        .expect("String writes cannot fail");
        for outcome in command.outcomes() {
            let outcome_name = format!("{name}{}", pascal(outcome.name()));
            writeln!(
                output,
                "@dataclass(frozen=True, slots=True, kw_only=True)\nclass {outcome_name}:"
            )
            .expect("String writes cannot fail");
            for payload_field in outcome.payload().fields() {
                writeln!(
                    output,
                    "    {}: {}",
                    python_identifier(payload_field.name()),
                    python_contract_type(payload_field.value_type(), contract)
                )
                .expect("String writes cannot fail");
            }
            writeln!(
                output,
                "    outcome: Literal[{:?}] = field(default={:?}, init=False)\n",
                outcome.name(),
                outcome.name()
            )
            .expect("String writes cannot fail");
        }
        write!(output, "{name}Outcome: TypeAlias = ").expect("String writes cannot fail");
        for (index, outcome) in command.outcomes().iter().enumerate() {
            if index != 0 {
                output.push_str(" | ");
            }
            write!(output, "{name}{}", pascal(outcome.name())).expect("String writes cannot fail");
        }
        output.push_str("\n\n");
    }

    emit_client(&mut output, module, contract, &commands, false);
    emit_client(&mut output, module, contract, &commands, true);
    while output.ends_with("\n\n") {
        output.pop();
    }
    Ok(output)
}

fn emit_python_embedding_constructors(
    output: &mut String,
    command: &riffdb_contract_ir::CommandPlan,
    contract: &ContractBundle,
) {
    let command_name = pascal(command.name());
    for facade in embedding_command_facades(command, contract) {
        let field_constant = screaming_snake(&facade.vector_field_name);
        let prefix = screaming_snake(command.name());
        writeln!(
            output,
            "{prefix}_{field_constant}_MODEL_IDENTITY: Final[str] = {:?}\n{prefix}_{field_constant}_MODEL_VERSION: Final[str] = {:?}",
            facade.model_identity, facade.model_version
        )
        .expect("String writes cannot fail");
        let function = python_identifier(&format!(
            "{}_for_{}",
            command.name(),
            facade.vector_field_name
        ));
        writeln!(output, "def {function}(*,").expect("String writes cannot fail");
        for field in command.input().record().fields().iter().filter(|field| {
            field.name() != facade.model_input_name && field.name() != facade.version_input_name
        }) {
            writeln!(
                output,
                "    {}: {},",
                python_identifier(field.name()),
                python_contract_type(field.value_type(), contract)
            )
            .expect("String writes cannot fail");
        }
        writeln!(
            output,
            ") -> {command_name}Input:\n    return {command_name}Input("
        )
        .expect("String writes cannot fail");
        for field in command.input().record().fields() {
            let field_name = python_identifier(field.name());
            if field.name() == facade.model_input_name {
                writeln!(
                    output,
                    "        {field_name}={prefix}_{field_constant}_MODEL_IDENTITY,"
                )
                .expect("String writes cannot fail");
            } else if field.name() == facade.version_input_name {
                writeln!(
                    output,
                    "        {field_name}={prefix}_{field_constant}_MODEL_VERSION,"
                )
                .expect("String writes cannot fail");
            } else {
                writeln!(output, "        {field_name}={field_name},")
                    .expect("String writes cannot fail");
            }
        }
        writeln!(output, "    )\n").expect("String writes cannot fail");
        writeln!(
            output,
            "def {function}_model(value: {command_name}Input) -> tuple[str, str]:\n    return (value.{}, value.{})\n",
            python_identifier(&facade.model_input_name),
            python_identifier(&facade.version_input_name),
        )
        .expect("String writes cannot fail");
    }
}

/// Generates one complete Python application module including native-backed
/// async reactive iterators. The presentation layer never imports gRPC.
pub fn generate_python_application_client(
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> Result<String, PythonGenerationError> {
    let mut output = generate_python_client(module, contract)?;
    if reactive_modules.is_empty() {
        return Ok(output);
    }
    output.push_str(
        "\n\nfrom collections.abc import AsyncIterator\nfrom typing import Any, cast\n\
         from riffdb_application._binding import decode_record, encode_reactive_record\n\n",
    );
    for reactive in reactive_modules {
        emit_python_reactive_module(&mut output, module, contract, reactive);
    }
    while output.ends_with("\n\n") {
        output.pop();
    }
    Ok(output)
}

fn emit_python_reactive_module(
    output: &mut String,
    module: &QueryModule,
    contract: &ContractBundle,
    reactive: &ReactiveModulePlanV1,
) {
    writeln!(
        output,
        "{}_REACTIVE_MODULE_HASH: Final[str] = {:?}\n",
        screaming_snake(reactive.name()),
        hex(reactive.identity().as_bytes())
    )
    .expect("String writes cannot fail");
    for operation in reactive.operations() {
        match operation.plan() {
            ReactiveOperationPlanV1::Stream {
                parameters, events, ..
            } => {
                let name = pascal(operation.name().as_str());
                emit_python_reactive_parameters(output, &name, parameters, contract);
                for event in events {
                    writeln!(
                        output,
                        "@dataclass(frozen=True, slots=True, kw_only=True)\nclass {name}{}:",
                        pascal(event.name())
                    )
                    .expect("String writes cannot fail");
                    writeln!(
                        output,
                        "    type: Literal[{:?}] = field(default={:?}, init=False)",
                        event.name(),
                        event.name()
                    )
                    .expect("String writes cannot fail");
                    for field in event.fields() {
                        writeln!(
                            output,
                            "    {}: {}",
                            python_identifier(field.name()),
                            python_reactive_type(field.type_name(), contract)
                        )
                        .expect("String writes cannot fail");
                    }
                    output.push('\n');
                }
                write!(output, "{name}Event: TypeAlias = ").expect("String writes cannot fail");
                for (index, event) in events.iter().enumerate() {
                    if index != 0 {
                        output.push_str(" | ");
                    }
                    write!(output, "{name}{}", pascal(event.name()))
                        .expect("String writes cannot fail");
                }
                output.push_str("\n\n");
                writeln!(
                    output,
                    "@dataclass(frozen=True, slots=True, kw_only=True)\nclass {name}Delivery:\n    event: {name}Event\n    event_id: str\n    attempt: int\n    lease_token: str\n    expires_at: dict[str, Any]\n    history_incarnation: int\n\n@dataclass(frozen=True, slots=True, kw_only=True)\nclass {name}Batch:\n    events: tuple[{name}Delivery, ...]\n    status: dict[str, Any]\n    disposition: Literal[\"ready\", \"wait_timed_out\", \"bounded_progress\"]\n    wait_timed_out: bool\n"
                )
                .expect("String writes cannot fail");
            }
            ReactiveOperationPlanV1::Watch {
                parameters, query, ..
            } => {
                let name = pascal(operation.name().as_str());
                emit_python_reactive_parameters(output, &name, parameters, contract);
                for variant in ["Snapshot", "Patch", "Reset", "Checkpoint", "Terminal"] {
                    writeln!(
                        output,
                        "@dataclass(frozen=True, slots=True, kw_only=True)\nclass {name}{variant}:\n    type: Literal[{:?}] = field(default={:?}, init=False)\n    value: {}Result | None = None\n    cursor: str | None = None\n    metadata: dict[str, Any] = field(default_factory=dict)\n",
                        variant.to_ascii_lowercase(),
                        variant.to_ascii_lowercase(),
                        query.query_name()
                    )
                    .expect("String writes cannot fail");
                }
                writeln!(
                    output,
                    "{name}Update: TypeAlias = {name}Snapshot | {name}Patch | {name}Reset | {name}Checkpoint | {name}Terminal\n"
                )
                .expect("String writes cannot fail");
            }
            ReactiveOperationPlanV1::Subscription {
                parameters,
                stream_name,
                ..
            } => {
                let name = pascal(operation.name().as_str());
                let stream = pascal(stream_name.as_str());
                emit_python_reactive_parameters(output, &name, parameters, contract);
                writeln!(
                    output,
                    "@dataclass(frozen=True, slots=True, kw_only=True)\nclass {name}Reaction:\n    name: str\n    command_name: str\n    command_id: int\n    causation_token: str\n\n@dataclass(frozen=True, slots=True, kw_only=True)\nclass {name}Item:\n    event: {stream}Event\n    event_id: str\n    attempt: int\n    lease_token: str\n    expires_at: dict[str, Any]\n    history_incarnation: int\n    context_head: int\n    hydrations: tuple[dict[str, Any], ...]\n    available_reactions: tuple[{name}Reaction, ...]\n"
                )
                .expect("String writes cannot fail");
            }
        }
    }
    let base = pascal(module.contract_lineage().as_str());
    writeln!(
        output,
        "class Async{base}ReactiveClient(Async{base}Client):"
    )
    .expect("String writes cannot fail");
    let mut emitted = false;
    for operation in reactive.operations() {
        match operation.plan() {
            ReactiveOperationPlanV1::Stream { events, .. } => {
                emitted = true;
                let name = pascal(operation.name().as_str());
                writeln!(output, "    async def next_{method}(self, parameters: {name}Params, consumer_name: str, *, batch_limit: int = 1, in_flight_limit: int = 16, lease_seconds: int = 60, maximum_wait_nanos: int = 0) -> {name}Batch:\n        variants = {{", method = python_identifier(&snake(operation.name().as_str()))).expect("String writes cannot fail");
                for event in events {
                    writeln!(
                        output,
                        "            {:?}: {name}{},",
                        event.name(),
                        pascal(event.name())
                    )
                    .expect("String writes cannot fail");
                }
                writeln!(
                    output,
                    "        }}\n        raw_batch = await self._transport._consume_event_batch(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA), consumer_name=consumer_name, batch_limit=batch_limit, in_flight_limit=in_flight_limit, lease_seconds=lease_seconds, maximum_wait_nanos=maximum_wait_nanos)\n        deliveries: list[{name}Delivery] = []\n        for item in cast(list[dict[str, Any]], raw_batch[\"events\"]):\n            raw = dict(item)\n            event_type = raw.pop(\"type\")\n            if not isinstance(event_type, str): raise ValueError(\"invalid RiffDB event type\")\n            delivery_value = raw.pop(\"_delivery\")\n            if not isinstance(delivery_value, dict): raise ValueError(\"invalid RiffDB event delivery\")\n            delivery = cast(dict[str, Any], delivery_value)\n            event_class = variants.get(event_type)\n            if event_class is None: raise ValueError(\"undeclared RiffDB event\")\n            deliveries.append({name}Delivery(event=decode_record(event_class, raw), event_id=delivery[\"event_id\"], attempt=delivery[\"attempt\"], lease_token=delivery[\"lease_token\"], expires_at=delivery[\"expires_at\"], history_incarnation=delivery[\"history_incarnation\"]))\n        return {name}Batch(events=tuple(deliveries), status=cast(dict[str, Any], raw_batch[\"status\"]), disposition=cast(Literal[\"ready\", \"wait_timed_out\", \"bounded_progress\"], raw_batch[\"disposition\"]), wait_timed_out=cast(bool, raw_batch[\"wait_timed_out\"]))\n\n    async def {method}(self, parameters: {name}Params, consumer_name: str) -> AsyncIterator[{name}Delivery]:\n        while True:\n            batch = await self.next_{method}(parameters, consumer_name, maximum_wait_nanos=30_000_000_000)\n            for delivery in batch.events:\n                yield delivery\n",
                    module = screaming_snake(reactive.name()),
                    operation = operation.name().as_str(),
                    method = python_identifier(&snake(operation.name().as_str())),
                )
                .expect("String writes cannot fail");
                writeln!(
                    output,
                    "    async def ack_{method}(self, parameters: {name}Params, consumer_name: str, delivery: {name}Delivery) -> str:\n        encoded = encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA)\n        return await self._transport._acknowledge_event(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encoded, consumer_name=consumer_name, event_id=delivery.event_id, lease_token=delivery.lease_token, history_incarnation=delivery.history_incarnation)\n\n    async def nack_{method}(self, parameters: {name}Params, consumer_name: str, delivery: {name}Delivery, retry_delay_nanos: int = 0) -> str:\n        encoded = encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA)\n        return await self._transport._negative_acknowledge_event(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encoded, consumer_name=consumer_name, event_id=delivery.event_id, lease_token=delivery.lease_token, history_incarnation=delivery.history_incarnation, retry_delay_nanos=retry_delay_nanos)\n\n    async def seek_{method}(self, parameters: {name}Params, consumer_name: str, checkpoint: str = \"before-first\") -> str:\n        encoded = encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA)\n        return await self._transport._seek_event_consumer(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encoded, consumer_name=consumer_name, checkpoint=checkpoint)\n\n    async def seek_protected_{method}(self, parameters: {name}Params, consumer_name: str, progress_cursor: str) -> str:\n        encoded = encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA)\n        return await self._transport._seek_event_consumer(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encoded, consumer_name=consumer_name, progress_cursor=progress_cursor)\n\n    async def {method}_status(self, parameters: {name}Params, consumer_name: str) -> dict[str, Any] | None:\n        encoded = encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA)\n        return await self._transport._event_consumer_status(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encoded, consumer_name=consumer_name)\n",
                    method = python_identifier(&snake(operation.name().as_str())),
                    module = screaming_snake(reactive.name()),
                    operation = operation.name().as_str(),
                )
                .expect("String writes cannot fail");
            }
            ReactiveOperationPlanV1::Watch { query, .. } => {
                emitted = true;
                let name = pascal(operation.name().as_str());
                writeln!(output, "    async def watch_{method}(self, parameters: {name}Params, cursor: str | None = None) -> AsyncIterator[{name}Update]:\n        outcomes = {{", method = python_identifier(&snake(operation.name().as_str()))).expect("String writes cannot fail");
                let query_module = module
                    .query(query.query_name())
                    .expect("reactive compiler retained exact query dependency");
                for branch in query_module.plan().schemas().results() {
                    writeln!(
                        output,
                        "            {:?}: {}{},",
                        branch.name(),
                        pascal(query.query_name()),
                        pascal(branch.name())
                    )
                    .expect("String writes cannot fail");
                }
                writeln!(
                    output,
                    "        }}\n        variants = {{\"snapshot\": {name}Snapshot, \"patch\": {name}Patch, \"reset\": {name}Reset, \"checkpoint\": {name}Checkpoint, \"terminal\": {name}Terminal}}\n        encoded = encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA)\n        async for update in self._transport._watch_named_query(reactive_module_hash={}_REACTIVE_MODULE_HASH, operation_name={:?}, parameters=encoded, cursor=cursor):\n            kind = update[\"type\"]\n            update_class = variants.get(kind)\n            if update_class is None: raise ValueError(\"undeclared RiffDB live update\")\n            value = decode_variant(outcomes, update[\"value\"]) if kind in {{\"snapshot\", \"reset\"}} else None\n            yield update_class(value=value, cursor=update.get(\"cursor\"), metadata={{key: value for key, value in update.items() if key not in {{\"type\", \"value\", \"cursor\"}}}})\n",
                    screaming_snake(reactive.name()),
                    operation.name().as_str()
                )
                .expect("String writes cannot fail");
            }
            ReactiveOperationPlanV1::Subscription {
                stream_name,
                reactions,
                ..
            } => {
                emitted = true;
                let name = pascal(operation.name().as_str());
                let stream_operation = reactive
                    .operations()
                    .iter()
                    .find(|candidate| candidate.name() == stream_name)
                    .expect("reactive compiler retained exact stream dependency");
                let ReactiveOperationPlanV1::Stream { events, .. } = stream_operation.plan() else {
                    unreachable!("subscription stream dependency is a stream");
                };
                writeln!(
                    output,
                    "    async def next_{method}(self, parameters: {name}Params, consumer_name: str, maximum_wait_nanos: int = 30_000_000_000) -> {name}Item | None:\n        variants = {{",
                    method = python_identifier(&snake(operation.name().as_str())),
                )
                .expect("String writes cannot fail");
                for event in events {
                    writeln!(
                        output,
                        "            {:?}: {}{},",
                        event.name(),
                        pascal(stream_name.as_str()),
                        pascal(event.name()),
                    )
                    .expect("String writes cannot fail");
                }
                writeln!(
                    output,
                    "        }}\n        raw = await self._transport._consume_contextual_subscription(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA), consumer_name=consumer_name, maximum_wait_nanos=maximum_wait_nanos)\n        if raw is None: return None\n        event_class = variants.get(raw[\"type\"])\n        if event_class is None: raise ValueError(\"undeclared RiffDB event\")\n        return {name}Item(event=decode_record(event_class, raw[\"event\"]), event_id=raw[\"event_id\"], attempt=raw[\"attempt\"], lease_token=raw[\"lease_token\"], expires_at=raw[\"expires_at\"], history_incarnation=raw[\"history_incarnation\"], context_head=raw[\"context_head\"], hydrations=tuple(raw[\"hydrations\"]), available_reactions=tuple({name}Reaction(**reaction) for reaction in raw[\"available_reactions\"]))\n\n    async def ack_{method}(self, parameters: {name}Params, consumer_name: str, item: {name}Item) -> str:\n        return await self._transport._acknowledge_contextual_item(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA), consumer_name=consumer_name, item=item)\n\n    async def nack_{method}(self, parameters: {name}Params, consumer_name: str, item: {name}Item, retry_delay_nanos: int = 0) -> str:\n        return await self._transport._negative_acknowledge_contextual_item(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA), consumer_name=consumer_name, item=item, retry_delay_nanos=retry_delay_nanos)\n\n    async def {method}_status(self, parameters: {name}Params, consumer_name: str) -> dict[str, Any] | None:\n        return await self._transport._contextual_subscription_status(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA), consumer_name=consumer_name)\n",
                    method = python_identifier(&snake(operation.name().as_str())),
                    module = screaming_snake(reactive.name()),
                    operation = operation.name().as_str(),
                )
                .expect("String writes cannot fail");
                for reaction in reactions {
                    let command = contract
                        .commands()
                        .iter()
                        .find(|command| command.name() == reaction.command_name())
                        .expect("reactive compiler retained exact command dependency");
                    let command_name = pascal(command.name());
                    let workflow_revisions = workflow_revision_bindings(command);
                    let success_outcome = workflow_success_outcome_name(command);
                    writeln!(
                        output,
                        "    async def react_{reaction_method}(self, parameters: {name}Params, consumer_name: str, item: {name}Item, input: {command_name}Input) -> TypedCommandResult[{command_name}Outcome]:\n        reaction = next((value for value in item.available_reactions if value.name == {reaction_name:?} and value.command_name == {command:?}), None)\n        if reaction is None: raise ValueError(\"contextual reaction is unavailable\")\n        raw = await self._transport._execute_contextual_reaction(reactive_module_hash={module}_REACTIVE_MODULE_HASH, operation_name={operation:?}, parameters=encode_reactive_record(parameters, {name}_PARAMETER_SCHEMA), consumer_name=consumer_name, reaction_name=reaction.name, command_id=reaction.command_id, causation_token=reaction.causation_token, contract_lineage=CONTRACT_LINEAGE, contract_version=CONTRACT_VERSION, command_name={command:?}, plan_hash={plan_constant}_PLAN_HASH, input=encode_record(input))\n        outcomes = {{",
                        reaction_method = python_identifier(&snake(reaction.reaction_name())),
                        reaction_name = reaction.reaction_name(),
                        command = command.name(),
                        module = screaming_snake(reactive.name()),
                        operation = operation.name().as_str(),
                        plan_constant = screaming_snake(command.name()),
                    )
                    .expect("String writes cannot fail");
                    for outcome in command.outcomes() {
                        writeln!(
                            output,
                            "            {:?}: {command_name}{},",
                            outcome.name(),
                            pascal(outcome.name()),
                        )
                        .expect("String writes cannot fail");
                    }
                    if workflow_revisions.is_empty() {
                        output.push_str("        }\n        return raw._map_outcome(lambda value: decode_variant(outcomes, value))\n\n");
                    } else {
                        writeln!(
                            output,
                            "        }}\n        result: TypedCommandResult[{command_name}Outcome] = raw._map_outcome(lambda value: decode_variant(outcomes, value))"
                        )
                        .expect("String writes cannot fail");
                        writeln!(
                            output,
                            "        if not isinstance(result.outcome, {command_name}{}):\n            return result._with_workflow_revisions(())",
                            pascal(success_outcome),
                        )
                        .expect("String writes cannot fail");
                        for revision in &workflow_revisions {
                            writeln!(
                                output,
                                "        if input.{} >= 2**64 - 1:\n            raise ValueError(\"RiffDB workflow successor revision overflow\")",
                                python_identifier(revision.input_name),
                            )
                            .expect("String writes cannot fail");
                        }
                        output.push_str("        return result._with_workflow_revisions((\n");
                        for revision in workflow_revisions {
                            writeln!(
                                output,
                                "            WorkflowSuccessorRevision(binding={:?}, revision=input.{} + 1),",
                                revision.binding_name,
                                python_identifier(revision.input_name),
                            )
                            .expect("String writes cannot fail");
                        }
                        output.push_str("        ))\n\n");
                    }
                }
            }
        }
    }
    if !emitted {
        output.push_str("    pass\n");
    }
}

fn emit_python_reactive_parameters(
    output: &mut String,
    operation: &str,
    parameters: &[ReactiveParameterV1],
    contract: &ContractBundle,
) {
    writeln!(
        output,
        "@dataclass(frozen=True, slots=True, kw_only=True)\nclass {operation}Params:"
    )
    .expect("String writes cannot fail");
    for parameter in parameters {
        writeln!(
            output,
            "    {}: {}",
            python_identifier(parameter.name()),
            python_reactive_type(parameter.type_name(), contract)
        )
        .expect("String writes cannot fail");
    }
    if parameters.is_empty() {
        output.push_str("    pass\n");
    }
    let schema = parameters
        .iter()
        .map(|parameter| {
            (
                parameter.name().to_owned(),
                python_reactive_schema(parameter.type_name(), contract),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    writeln!(
        output,
        "\n{operation}_PARAMETER_SCHEMA: dict[str, dict[str, object]] = {}\n",
        serde_json::to_string(&schema).expect("reactive schema JSON")
    )
    .expect("String writes cannot fail");
}

fn python_reactive_schema(type_name: &str, contract: &ContractBundle) -> serde_json::Value {
    if let Some(enumeration) = contract
        .schema()
        .enums()
        .iter()
        .find(|value| value.name() == type_name)
    {
        return serde_json::json!({
            "kind": "enum",
            "type_id": enumeration.id().get(),
            "variants": enumeration.variants().iter().map(|variant| {
                (variant.name().to_owned(), serde_json::json!(variant.id().get()))
            }).collect::<serde_json::Map<_, _>>(),
        });
    }
    if let Some((precision, scale)) = decimal_type_parts(type_name) {
        return serde_json::json!({"kind":"decimal", "precision":precision, "scale":scale});
    }
    if let Some(currency) = money_type_currency(type_name) {
        return serde_json::json!({"kind":"money", "precision":38, "scale":2, "currency":currency});
    }
    if let Some(dimension) = vector_type_dimension(type_name) {
        return serde_json::json!({"kind":"vector", "dimension":dimension});
    }
    let kind = if type_name.starts_with("bytes<") {
        "bytes"
    } else if type_name.starts_with("string<") || type_name == "cursor" {
        "string"
    } else if type_name == "limit" {
        "u64"
    } else {
        type_name
    };
    serde_json::json!({"kind":kind})
}

fn decimal_type_parts(type_name: &str) -> Option<(u8, u8)> {
    let body = type_name.strip_prefix("decimal<")?.strip_suffix('>')?;
    let (precision, scale) = body.split_once(',')?;
    Some((precision.parse().ok()?, scale.parse().ok()?))
}

fn money_type_currency(type_name: &str) -> Option<String> {
    type_name
        .strip_prefix("money<")?
        .strip_suffix('>')
        .map(str::to_owned)
}

fn vector_type_dimension(type_name: &str) -> Option<u32> {
    type_name
        .strip_prefix("vector<")?
        .strip_suffix('>')?
        .parse()
        .ok()
}

fn python_reactive_type(type_name: &str, contract: &ContractBundle) -> String {
    if contract
        .schema()
        .enums()
        .iter()
        .any(|value| value.name() == type_name)
    {
        return pascal(type_name);
    }
    if type_name.starts_with("decimal<") {
        return "Decimal".to_owned();
    }
    if type_name.starts_with("money<") {
        return "Money".to_owned();
    }
    if type_name.starts_with("bytes<") {
        return "bytes".to_owned();
    }
    if let Some(dimension) = vector_type_dimension(type_name) {
        return format!("Annotated[tuple[float, ...], \"vector<{dimension}>\"]");
    }
    if type_name.starts_with("string<") {
        return "str".to_owned();
    }
    match type_name {
        "bool" => "bool",
        "i64" | "u64" => "int",
        "uuid" => "UUID",
        "date" => "RiffDate",
        "timestamp" => "Timestamp",
        "limit" => "int",
        "cursor" => "str",
        _ => "str",
    }
    .to_owned()
}

fn emit_python_compact_query_decoder(
    output: &mut String,
    name: &str,
    shape: &RustCompactResultShape,
    contract: &ContractBundle,
) {
    let method = python_identifier(&snake(name));
    let nested = format!(
        "{name}{}{}",
        pascal(&shape.outcome),
        pascal(&shape.result_name)
    );
    let branch = format!("{name}{}", pascal(&shape.outcome));
    let fields = shape
        .fields
        .iter()
        .map(|field| format!("{:?}", field.name))
        .collect::<Vec<_>>()
        .join(", ");
    writeln!(output, "def _decode_{method}_compact(value: object) -> {name}Result:\n    if not isinstance(value, dict) or frozenset(value) != frozenset((\"$riffdb_compact\",)):\n        raise ValueError(\"invalid RiffDB compact result\")\n    compact = value[\"$riffdb_compact\"]\n    if not isinstance(compact, dict) or frozenset(compact) != frozenset((\"outcome\", \"result_name\", \"entity\", \"fields\", \"rows\")) or compact[\"outcome\"] != {:?} or compact[\"result_name\"] != {:?} or compact[\"entity\"] != {:?} or compact[\"fields\"] != [{fields}] or not isinstance(compact[\"rows\"], list) or len(compact[\"rows\"]) > {}:\n        raise ValueError(\"invalid RiffDB compact result\")\n    decoded: list[{nested}] = []\n    for row in compact[\"rows\"]:\n        if not isinstance(row, list) or len(row) != {}:\n            raise ValueError(\"invalid RiffDB compact result\")\n        decoded.append({nested}(\n", shape.outcome, shape.result_name, shape.entity, shape.maximum_rows, shape.fields.len()).expect("String writes cannot fail");
    for (index, field) in shape.fields.iter().enumerate() {
        writeln!(
            output,
            "            {}={},",
            python_identifier(&field.name),
            python_decode_compact_expr(&format!("row[{index}]"), &field.value_type, contract)
        )
        .expect("String writes cannot fail");
    }
    writeln!(
        output,
        "        ))\n    return {branch}({}=tuple(decoded))\n",
        python_identifier(&shape.result_name)
    )
    .expect("String writes cannot fail");
}

fn python_decode_compact_expr(value: &str, ty: &ValueType, contract: &ContractBundle) -> String {
    if let Some(inner) = ty.optional_inner() {
        return format!(
            "None if {value} is None else {}",
            python_decode_compact_expr(value, inner, contract)
        );
    }
    match ty.tag() {
        ValueTypeTag::Bool => format!(
            "({value} if type({value}) is bool else (_ for _ in ()).throw(ValueError(\"invalid RiffDB compact bool\")))"
        ),
        ValueTypeTag::I64 => format!(
            "({value} if type({value}) is int and -(2**63) <= {value} < 2**63 else (_ for _ in ()).throw(ValueError(\"invalid RiffDB compact i64\")))"
        ),
        ValueTypeTag::U64 => format!(
            "({value} if type({value}) is int and 0 <= {value} < 2**64 else (_ for _ in ()).throw(ValueError(\"invalid RiffDB compact u64\")))"
        ),
        ValueTypeTag::String => format!(
            "({value} if isinstance({value}, str) and len({value}.encode(\"utf-8\")) <= {} else (_ for _ in ()).throw(ValueError(\"invalid RiffDB compact string\")))",
            ty.byte_bound().expect("string bound")
        ),
        ValueTypeTag::Uuid => format!(
            "UUID(str(_compact_tag({value}, \"uuid\", frozenset((\"$riffdb\", \"value\")))[\"value\"]))"
        ),
        ValueTypeTag::Date => format!(
            "RiffDate(int(_compact_tag({value}, \"date\", frozenset((\"$riffdb\", \"value\")))[\"value\"]))"
        ),
        ValueTypeTag::Timestamp => format!(
            "Timestamp(seconds=int(_compact_tag({value}, \"timestamp\", frozenset((\"$riffdb\", \"seconds\", \"nanos\")))[\"seconds\"]), nanos=int(_compact_tag({value}, \"timestamp\", frozenset((\"$riffdb\", \"seconds\", \"nanos\")))[\"nanos\"]))"
        ),
        ValueTypeTag::Enum => {
            let enumeration = contract
                .schema()
                .enumeration(ty.enum_type_id().expect("enum identity"))
                .expect("validated enum");
            format!(
                "{}(str(_compact_tag({value}, \"enum\", frozenset((\"$riffdb\", \"value\")))[\"value\"]))",
                pascal(enumeration.name())
            )
        }
        ValueTypeTag::Optional => unreachable!("handled above"),
        _ => "(_ for _ in ()).throw(ValueError(\"unsupported RiffDB compact value\"))".to_owned(),
    }
}

fn emit_client(
    output: &mut String,
    module: &QueryModule,
    contract: &ContractBundle,
    commands: &[&riffdb_contract_ir::CommandPlan],
    asynchronous: bool,
) {
    let prefix = if asynchronous { "Async" } else { "" };
    let transport = if asynchronous {
        "AsyncApplicationTransport"
    } else {
        "SyncApplicationTransport"
    };
    let await_token = if asynchronous { "await " } else { "" };
    writeln!(
        output,
        "class {prefix}{}Client:\n    def __init__(self, transport: {transport}, command_attempts: AttemptBudget) -> None:\n        self._transport = transport\n        self._command_attempts = command_attempts\n",
        pascal(module.contract_lineage().as_str())
    )
    .expect("String writes cannot fail");
    for query in module.queries() {
        let wire_name = query.name();
        let name = pascal(wire_name);
        let method = python_identifier(&snake(wire_name));
        let async_token = if asynchronous { "async " } else { "" };
        let compact = query
            .plan()
            .common_covered_result()
            .and_then(|(result_name, layout, selected_fields)| {
                rust_compact_result_shape(
                    query.plan().schemas(),
                    &result_name,
                    &layout,
                    &selected_fields,
                    contract,
                )
            })
            .is_some();
        let compact_argument = if compact {
            "            accept_compact_result=True,\n"
        } else {
            ""
        };
        writeln!(
            output,
            "    {async_token}def {method}(self, parameters: {name}Params, options: QueryOptions = QueryOptions()) -> TypedQueryResult[{name}Result]:\n        raw = {await_token}self._transport._execute_named_query(\n            contract_lineage=CONTRACT_LINEAGE, contract_version=CONTRACT_VERSION,\n            contract_bundle_hash=CONTRACT_BUNDLE_HASH, module_hash=QUERY_MODULE_HASH,\n            query_name={wire_name:?}, plan_hash={constant}_QUERY_PLAN_HASH,\n            parameters=encode_record(parameters), options=options,\n{compact_argument}        )\n        outcomes = {{",
            constant = screaming_snake(wire_name),
        )
        .expect("String writes cannot fail");
        for branch in query.plan().schemas().results() {
            writeln!(
                output,
                "            {:?}: {name}{},",
                branch.name(),
                pascal(branch.name())
            )
            .expect("String writes cannot fail");
        }
        if compact {
            writeln!(output, "        }}\n        return raw._map_value(lambda value: _decode_{method}_compact(value) if isinstance(value, dict) and \"$riffdb_compact\" in value else decode_variant(outcomes, value))\n").expect("String writes cannot fail");
        } else {
            output.push_str("        }\n        return raw._map_value(lambda value: decode_variant(outcomes, value))\n\n");
        }
    }
    for inspection in vector_inspection_facades(module, contract) {
        let function = format!(
            "inspect_{}_{}",
            snake(&inspection.entity),
            snake(&inspection.field)
        );
        let partition_type = python_contract_type(&inspection.partition_type, contract);
        for (suffix, kind, result_type) in [
            ("staleness", "staleness", "VectorStalenessResult"),
            (
                "model_versions",
                "model_versions",
                "VectorModelVersionResult",
            ),
        ] {
            let async_token = if asynchronous { "async " } else { "" };
            writeln!(
                output,
                "    {async_token}def {function}_{suffix}(self, partition: {partition_type}, limit: int = 50, options: VectorInspectionOptions = VectorInspectionOptions()) -> TypedVectorInspectionResult[{result_type}]:\n        if type(limit) is not int or not 1 <= limit <= 500:\n            raise ValueError(\"invalid vector inspection limit\")\n        raw = {await_token}self._transport._inspect_vector_state(\n            contract_lineage=CONTRACT_LINEAGE, contract_version=CONTRACT_VERSION,\n            contract_bundle_hash=CONTRACT_BUNDLE_HASH, entity={entity:?}, field={field:?},\n            inspection_kind={kind:?}, partition=encode_value(partition, {partition_type}),\n            limit=limit, options=options,\n        )\n        return cast(TypedVectorInspectionResult[{result_type}], raw)\n",
                entity = inspection.entity,
                field = inspection.field,
            )
            .expect("String writes cannot fail");
        }
    }
    for command in commands {
        let wire_name = command.name();
        let name = pascal(wire_name);
        let method = python_identifier(&snake(wire_name));
        let async_token = if asynchronous { "async " } else { "" };
        let workflow_revisions = workflow_revision_bindings(command);
        let success_outcome = workflow_success_outcome_name(command);
        let collection_validation = command.collection_expansion().map(|expansion| {
            let field = command
                .input()
                .record()
                .field(expansion.input_field())
                .expect("validated collection input field");
            format!(
                "        if not {minimum} <= len(input.{field}) <= {maximum}:\n            raise ValueError({message:?})\n",
                minimum = expansion.minimum_elements(),
                maximum = expansion.maximum_elements(),
                field = python_identifier(field.name()),
                message = format!(
                    "invalid bounded collection length for {}.{}",
                    command.name(),
                    field.name()
                ),
            )
        });
        writeln!(
            output,
            "    {async_token}def {method}(self, input: {name}Input) -> TypedCommandResult[{name}Outcome]:\n{collection_validation}        raw = {await_token}self._transport._execute_command(\n            contract_lineage=CONTRACT_LINEAGE, contract_version=CONTRACT_VERSION,\n            command_name={wire_name:?}, plan_hash={constant}_PLAN_HASH,\n            input=encode_record(input), attempts=self._command_attempts,\n        )\n        outcomes = {{",
            collection_validation = collection_validation.as_deref().unwrap_or(""),
            constant = screaming_snake(wire_name)
        )
        .expect("String writes cannot fail");
        for outcome in command.outcomes() {
            writeln!(
                output,
                "            {:?}: {name}{},",
                outcome.name(),
                pascal(outcome.name())
            )
            .expect("String writes cannot fail");
        }
        if workflow_revisions.is_empty() {
            output.push_str("        }\n        return raw._map_outcome(lambda value: decode_variant(outcomes, value))\n\n");
        } else {
            writeln!(
                output,
                "        }}\n        result: TypedCommandResult[{name}Outcome] = raw._map_outcome(lambda value: decode_variant(outcomes, value))"
            )
            .expect("String writes cannot fail");
            writeln!(
                output,
                "        if not isinstance(result.outcome, {name}{}):\n            return result._with_workflow_revisions(())",
                pascal(success_outcome),
            )
            .expect("String writes cannot fail");
            for revision in &workflow_revisions {
                writeln!(
                    output,
                    "        if input.{} >= 2**64 - 1:\n            raise ValueError(\"RiffDB workflow successor revision overflow\")",
                    python_identifier(revision.input_name),
                )
                .expect("String writes cannot fail");
            }
            output.push_str("        return result._with_workflow_revisions((\n");
            for revision in workflow_revisions {
                writeln!(
                    output,
                    "            WorkflowSuccessorRevision(binding={:?}, revision=input.{} + 1),",
                    revision.binding_name,
                    python_identifier(revision.input_name),
                )
                .expect("String writes cannot fail");
            }
            output.push_str("        ))\n\n");
        }
        let batch_await = if asynchronous { "await " } else { "" };
        let batch_collection_validation =
            collection_validation
                .as_deref()
                .map_or_else(String::new, |validation| {
                    let nested = validation
                        .replace("        if ", "            if ")
                        .replace("\n            raise", "\n                raise");
                    format!("        for input in inputs:\n{nested}")
                });
        writeln!(
            output,
            "    {async_token}def {method}_batch(\n        self, inputs: Sequence[{name}Input], options: CommandBatchOptions,\n        progress: Callable[[CommandBatchProgress], None] | None = None,\n    ) -> CommandBatchResult[{name}Outcome]:\n{batch_collection_validation}        return {batch_await}self._transport._command_batch(inputs, options, self.{method}, progress)\n"
        )
        .expect("String writes cannot fail");
    }
}

fn emit_contract_record<'a>(
    output: &mut String,
    name: &str,
    fields: impl Iterator<Item = (&'a str, &'a ValueType)>,
    contract: &ContractBundle,
) {
    let fields = fields.collect::<Vec<_>>();
    writeln!(
        output,
        "@dataclass(frozen=True, slots=True, kw_only=True)\nclass {name}:"
    )
    .expect("String writes cannot fail");
    if fields.is_empty() {
        output.push_str("    pass\n");
    }
    for (field, value_type) in fields {
        writeln!(
            output,
            "    {}: {}",
            python_identifier(field),
            python_contract_type(value_type, contract)
        )
        .expect("String writes cannot fail");
    }
    output.push('\n');
}

fn emit_named_nested(
    output: &mut String,
    name: &str,
    value_type: &NamedTypeSchema,
    contract: &ContractBundle,
    redacted_debug: bool,
) {
    match value_type {
        NamedTypeSchema::Optional(inner)
        | NamedTypeSchema::Set(inner)
        | NamedTypeSchema::List { element: inner, .. } => {
            emit_named_nested(output, name, inner, contract, redacted_debug);
        }
        NamedTypeSchema::Record(fields) => {
            for field in fields {
                emit_named_nested(
                    output,
                    &format!("{name}{}", pascal(field.name())),
                    field.value_type(),
                    contract,
                    redacted_debug,
                );
            }
            writeln!(
                output,
                "@dataclass(frozen=True, slots=True, kw_only=True)\nclass {name}:"
            )
            .expect("String writes cannot fail");
            if fields.is_empty() {
                output.push_str("    pass\n");
            }
            for field in fields {
                writeln!(
                    output,
                    "    {}: {}{}",
                    python_identifier(field.name()),
                    python_named_type(
                        field.value_type(),
                        &format!("{name}{}", pascal(field.name())),
                        contract
                    ),
                    if redacted_debug {
                        " = field(repr=False)"
                    } else {
                        ""
                    }
                )
                .expect("String writes cannot fail");
            }
            output.push('\n');
        }
        NamedTypeSchema::Scalar(_) | NamedTypeSchema::Cursor | NamedTypeSchema::Limit => {}
    }
}

fn python_named_type(
    value_type: &NamedTypeSchema,
    nested_name: &str,
    contract: &ContractBundle,
) -> String {
    match value_type {
        NamedTypeSchema::Scalar(name) => match name.as_str() {
            "bool" => "bool".to_owned(),
            "i64" => "Annotated[int, \"i64\"]".to_owned(),
            "u64" => "Annotated[int, \"u64\"]".to_owned(),
            "uuid" => "UUID".to_owned(),
            "timestamp" => "Timestamp".to_owned(),
            "date" => "RiffDate".to_owned(),
            value if value.starts_with("bytes<") => "bytes".to_owned(),
            value if value.starts_with("vector<") => format!(
                "Annotated[tuple[float, ...], \"vector<{}>\"]",
                vector_type_dimension(value).expect("vector dimension")
            ),
            value if value.starts_with("decimal<") => "Decimal".to_owned(),
            value if value.starts_with("money<") => "Money".to_owned(),
            value
                if contract
                    .schema()
                    .enums()
                    .iter()
                    .any(|enumeration| enumeration.name() == value) =>
            {
                pascal(value)
            }
            _ => "str".to_owned(),
        },
        NamedTypeSchema::Optional(inner) => {
            format!("{} | None", python_named_type(inner, nested_name, contract))
        }
        NamedTypeSchema::Set(inner) | NamedTypeSchema::List { element: inner, .. } => {
            format!(
                "tuple[{}, ...]",
                python_named_type(inner, nested_name, contract)
            )
        }
        NamedTypeSchema::Record(_) => nested_name.to_owned(),
        NamedTypeSchema::Cursor => "str".to_owned(),
        NamedTypeSchema::Limit => "Annotated[int, \"u64\"]".to_owned(),
    }
}

fn python_contract_type(value_type: &ValueType, contract: &ContractBundle) -> String {
    if let Some(inner) = value_type.optional_inner() {
        return format!("{} | None", python_contract_type(inner, contract));
    }
    if let Some((inner, _)) = value_type.list_parts() {
        return format!("tuple[{}, ...]", python_contract_type(inner, contract));
    }
    match value_type.tag() {
        ValueTypeTag::Bool => "bool".to_owned(),
        ValueTypeTag::I64 => "Annotated[int, \"i64\"]".to_owned(),
        ValueTypeTag::U64 => "Annotated[int, \"u64\"]".to_owned(),
        ValueTypeTag::Bytes => "bytes".to_owned(),
        ValueTypeTag::Decimal => "Decimal".to_owned(),
        ValueTypeTag::Money => "Money".to_owned(),
        ValueTypeTag::Timestamp => "Timestamp".to_owned(),
        ValueTypeTag::Date => "RiffDate".to_owned(),
        ValueTypeTag::Uuid => "UUID".to_owned(),
        ValueTypeTag::Enum => value_type
            .enum_type_id()
            .and_then(|id| contract.schema().enumeration(id))
            .map_or_else(|| "str".to_owned(), |value| pascal(value.name())),
        ValueTypeTag::Record => match value_type.record_ref() {
            Some(RecordTypeRef::Entity(id)) => contract
                .schema()
                .entity(*id)
                .map_or_else(|| "str".to_owned(), |entity| pascal(entity.name())),
            _ => "str".to_owned(),
        },
        ValueTypeTag::String => "str".to_owned(),
        ValueTypeTag::Vector => format!(
            "Annotated[tuple[float, ...], \"vector<{}>\"]",
            value_type
                .vector_dimension()
                .expect("vector dimension")
                .get()
        ),
        ValueTypeTag::Optional | ValueTypeTag::List => {
            unreachable!("handled above")
        }
    }
}

fn validate_names(
    module: &QueryModule,
    contract: &ContractBundle,
) -> Result<(), PythonGenerationError> {
    let mut top_level = PYTHON_RESERVED_TOP_LEVEL
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    let mut methods = BTreeSet::new();

    for enumeration in contract.schema().enums() {
        insert_name(
            &mut top_level,
            pascal(enumeration.name()),
            PythonGenerationError::contract(vec!["enum".to_owned(), enumeration.name().to_owned()]),
        )?;
        validate_normalized_names(
            enumeration.variants().iter().map(|variant| {
                (
                    screaming_snake(variant.name()),
                    PythonGenerationError::contract(vec![
                        "enum".to_owned(),
                        enumeration.name().to_owned(),
                        "variant".to_owned(),
                        variant.name().to_owned(),
                    ]),
                )
            }),
            false,
        )?;
    }
    for entity in contract.schema().entities() {
        insert_name(
            &mut top_level,
            pascal(entity.name()),
            PythonGenerationError::contract(vec!["entity".to_owned(), entity.name().to_owned()]),
        )?;
        validate_normalized_names(
            entity.record().fields().iter().map(|field| {
                (
                    python_identifier(field.name()),
                    PythonGenerationError::contract(vec![
                        "entity".to_owned(),
                        entity.name().to_owned(),
                        "field".to_owned(),
                        field.name().to_owned(),
                    ]),
                )
            }),
            false,
        )?;
    }
    for query in module.queries() {
        let name = pascal(query.name());
        let query_origin = || {
            PythonGenerationError::query(
                query.name(),
                vec!["query".to_owned(), query.name().to_owned()],
            )
        };
        insert_name(&mut top_level, format!("{name}Params"), query_origin())?;
        insert_name(&mut top_level, format!("{name}Result"), query_origin())?;
        insert_name(
            &mut top_level,
            format!("{}_QUERY_PLAN_HASH", screaming_snake(query.name())),
            query_origin(),
        )?;
        insert_name(
            &mut methods,
            python_identifier(&snake(query.name())),
            query_origin(),
        )?;

        let schemas = query.plan().schemas();
        validate_normalized_names(
            schemas.parameters().iter().map(|field| {
                (
                    python_identifier(field.name()),
                    PythonGenerationError::query(
                        query.name(),
                        vec![
                            "query".to_owned(),
                            query.name().to_owned(),
                            "parameter".to_owned(),
                            field.name().to_owned(),
                        ],
                    ),
                )
            }),
            false,
        )?;
        for parameter in schemas.parameters() {
            register_nested_names(
                &mut top_level,
                &format!("{name}Params{}", pascal(parameter.name())),
                parameter.value_type(),
                PythonGenerationError::query(
                    query.name(),
                    vec![
                        "query".to_owned(),
                        query.name().to_owned(),
                        "parameter".to_owned(),
                        parameter.name().to_owned(),
                    ],
                ),
            )?;
        }
        for branch in schemas.results() {
            let branch_name = format!("{name}{}", pascal(branch.name()));
            insert_name(
                &mut top_level,
                branch_name.clone(),
                PythonGenerationError::query(
                    query.name(),
                    vec![
                        "query".to_owned(),
                        query.name().to_owned(),
                        "outcome".to_owned(),
                        branch.name().to_owned(),
                    ],
                ),
            )?;
            validate_normalized_names(
                branch.fields().iter().map(|field| {
                    (
                        python_identifier(field.name()),
                        PythonGenerationError::query(
                            query.name(),
                            vec![
                                "query".to_owned(),
                                query.name().to_owned(),
                                "field".to_owned(),
                                field.name().to_owned(),
                            ],
                        ),
                    )
                }),
                true,
            )?;
            for field in branch.fields() {
                register_nested_names(
                    &mut top_level,
                    &format!("{branch_name}{}", pascal(field.name())),
                    field.value_type(),
                    PythonGenerationError::query(
                        query.name(),
                        vec![
                            "query".to_owned(),
                            query.name().to_owned(),
                            "field".to_owned(),
                            field.name().to_owned(),
                        ],
                    ),
                )?;
            }
        }
    }
    for command in contract.commands() {
        let name = pascal(command.name());
        let command_origin = || {
            PythonGenerationError::contract(vec!["command".to_owned(), command.name().to_owned()])
        };
        insert_name(&mut top_level, format!("{name}Input"), command_origin())?;
        insert_name(&mut top_level, format!("{name}Outcome"), command_origin())?;
        insert_name(
            &mut top_level,
            format!("{}_PLAN_HASH", screaming_snake(command.name())),
            command_origin(),
        )?;
        let method = python_identifier(&snake(command.name()));
        insert_name(&mut methods, method.clone(), command_origin())?;
        insert_name(&mut methods, format!("{method}_batch"), command_origin())?;
        validate_normalized_names(
            command.input().record().fields().iter().map(|field| {
                (
                    python_identifier(field.name()),
                    PythonGenerationError::contract(vec![
                        "command".to_owned(),
                        command.name().to_owned(),
                        "input".to_owned(),
                        field.name().to_owned(),
                    ]),
                )
            }),
            false,
        )?;
        for outcome in command.outcomes() {
            insert_name(
                &mut top_level,
                format!("{name}{}", pascal(outcome.name())),
                PythonGenerationError::contract(vec![
                    "command".to_owned(),
                    command.name().to_owned(),
                    "outcome".to_owned(),
                    outcome.name().to_owned(),
                ]),
            )?;
            validate_normalized_names(
                outcome.payload().fields().iter().map(|field| {
                    (
                        python_identifier(field.name()),
                        PythonGenerationError::contract(vec![
                            "command".to_owned(),
                            command.name().to_owned(),
                            "outcome".to_owned(),
                            outcome.name().to_owned(),
                            "field".to_owned(),
                            field.name().to_owned(),
                        ]),
                    )
                }),
                true,
            )?;
        }
    }
    insert_name(
        &mut top_level,
        format!("{}Client", pascal(module.contract_lineage().as_str())),
        PythonGenerationError::contract(vec![
            "contract".to_owned(),
            module.contract_lineage().as_str().to_owned(),
        ]),
    )?;
    insert_name(
        &mut top_level,
        format!("Async{}Client", pascal(module.contract_lineage().as_str())),
        PythonGenerationError::contract(vec![
            "contract".to_owned(),
            module.contract_lineage().as_str().to_owned(),
        ]),
    )?;
    Ok(())
}

fn register_nested_names(
    top_level: &mut BTreeSet<String>,
    name: &str,
    value_type: &NamedTypeSchema,
    origin: PythonGenerationError,
) -> Result<(), PythonGenerationError> {
    match value_type {
        NamedTypeSchema::Optional(inner)
        | NamedTypeSchema::Set(inner)
        | NamedTypeSchema::List { element: inner, .. } => {
            register_nested_names(top_level, name, inner, origin)
        }
        NamedTypeSchema::Record(fields) => {
            insert_name(top_level, name.to_owned(), origin.clone())?;
            validate_normalized_names(
                fields.iter().map(|field| {
                    let mut field_origin = origin.clone();
                    field_origin.symbol_path.push(field.name().to_owned());
                    (python_identifier(field.name()), field_origin)
                }),
                false,
            )?;
            for field in fields {
                let mut field_origin = origin.clone();
                field_origin.symbol_path.push(field.name().to_owned());
                register_nested_names(
                    top_level,
                    &format!("{name}{}", pascal(field.name())),
                    field.value_type(),
                    field_origin,
                )?;
            }
            Ok(())
        }
        NamedTypeSchema::Scalar(_) | NamedTypeSchema::Cursor | NamedTypeSchema::Limit => Ok(()),
    }
}

fn validate_normalized_names(
    names: impl IntoIterator<Item = (String, PythonGenerationError)>,
    reserve_outcome: bool,
) -> Result<(), PythonGenerationError> {
    let mut normalized = BTreeSet::new();
    if reserve_outcome {
        normalized.insert("outcome".to_owned());
    }
    for (name, origin) in names {
        insert_name(&mut normalized, name, origin)?;
    }
    Ok(())
}

fn insert_name(
    names: &mut BTreeSet<String>,
    name: String,
    origin: PythonGenerationError,
) -> Result<(), PythonGenerationError> {
    if name.is_empty() || !names.insert(name) {
        return Err(origin);
    }
    Ok(())
}

const PYTHON_RESERVED_TOP_LEVEL: &[&str] = &[
    "Annotated",
    "AsyncApplicationTransport",
    "AttemptBudget",
    "CONTRACT_BUNDLE_HASH",
    "CONTRACT_LINEAGE",
    "CONTRACT_VERSION",
    "Callable",
    "CommandBatchOptions",
    "CommandBatchProgress",
    "CommandBatchResult",
    "Decimal",
    "Final",
    "Literal",
    "Money",
    "QUERY_MODULE_HASH",
    "QueryOptions",
    "RiffDate",
    "Sequence",
    "StrEnum",
    "SyncApplicationTransport",
    "Timestamp",
    "TypeAlias",
    "TypedCommandResult",
    "TypedQueryResult",
    "UUID",
    "dataclass",
    "decode_variant",
    "encode_record",
    "field",
];

fn is_cursor(value_type: &NamedTypeSchema) -> bool {
    matches!(value_type, NamedTypeSchema::Cursor)
        || matches!(value_type, NamedTypeSchema::Optional(inner) if is_cursor(inner))
}

fn python_identifier(name: &str) -> String {
    let value = snake(name);
    if PYTHON_KEYWORDS.contains(&value.as_str()) {
        format!("{value}_")
    } else {
        value
    }
}

const PYTHON_KEYWORDS: &[&str] = &[
    "and", "as", "assert", "async", "await", "break", "case", "class", "continue", "def", "del",
    "elif", "else", "except", "false", "finally", "for", "from", "global", "if", "import", "in",
    "is", "lambda", "match", "none", "nonlocal", "not", "or", "pass", "raise", "return", "true",
    "try", "while", "with", "yield",
];

fn snake(name: &str) -> String {
    separated(name, '_', false)
}

fn screaming_snake(name: &str) -> String {
    separated(name, '_', true)
}

fn pascal(name: &str) -> String {
    name.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .flat_map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|character| character.to_ascii_uppercase())
                .into_iter()
                .chain(chars)
        })
        .collect()
}

fn separated(name: &str, separator: char, uppercase: bool) -> String {
    let mut output = String::new();
    let mut previous_lower = false;
    for character in name.chars() {
        if !character.is_ascii_alphanumeric() {
            if !output.ends_with(separator) && !output.is_empty() {
                output.push(separator);
            }
            previous_lower = false;
        } else {
            if character.is_ascii_uppercase() && previous_lower {
                output.push(separator);
            }
            output.push(if uppercase {
                character.to_ascii_uppercase()
            } else {
                character.to_ascii_lowercase()
            });
            previous_lower = character.is_ascii_lowercase() || character.is_ascii_digit();
        }
    }
    output
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("String writes cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{
        PythonGenerationError, locate_contract_symbol, locate_query_symbol, python_identifier,
        validate_normalized_names,
    };

    #[test]
    fn keywords_gain_one_suffix_and_collisions_are_rejected() {
        assert_eq!(python_identifier("class"), "class_");
        assert_eq!(python_identifier("ordinaryName"), "ordinary_name");
        let first = PythonGenerationError::contract(vec!["first".to_owned()]);
        let second = PythonGenerationError::contract(vec!["second".to_owned()]);
        assert_eq!(
            validate_normalized_names(
                [
                    (python_identifier("class"), first),
                    (python_identifier("class_"), second.clone()),
                ],
                false,
            ),
            Err(second.clone())
        );
        assert_eq!(
            validate_normalized_names([(python_identifier("outcome"), second.clone())], true),
            Err(second)
        );
    }

    #[test]
    fn generation_symbols_resolve_to_exact_contract_and_query_spans() {
        let contract = "contract Demo version 1 {\n  entity Item {\n    key (id: uuid)\n    field class_: string<32>\n  }\n}\n";
        let contract_span = locate_contract_symbol(
            contract,
            &[
                "entity".to_owned(),
                "Item".to_owned(),
                "field".to_owned(),
                "class_".to_owned(),
            ],
        )
        .expect("contract field span");
        assert_eq!(
            &contract[contract_span.0 as usize..contract_span.1 as usize],
            "class_"
        );

        let query = "query ItemPage($id: Item.id) {\n  one item from Item where id == $id else NotFound\n  return Found { renamed: item { id } }\n  outcomes Found | NotFound\n}\n";
        let query_span = locate_query_symbol(
            query,
            &[
                "query".to_owned(),
                "ItemPage".to_owned(),
                "field".to_owned(),
                "renamed".to_owned(),
            ],
        )
        .expect("query alias span");
        assert_eq!(
            &query[query_span.0 as usize..query_span.1 as usize],
            "renamed"
        );
    }
}
