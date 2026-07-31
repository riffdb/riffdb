//! Deterministic fixture generator for the canonical LegalSpend contract.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use riffdb_contract_compiler::{
    CompilationError, CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics,
    compile_contract_source, compile_contract_successor, validate_contract_source,
};
use riffdb_contract_ir::{
    BinaryOperator, BindingMode, CapabilityRequirement, CommandExplain, CompatibilityClass,
    ContractBundle, ExecutionClass, ExpressionArena, ExpressionKind, FieldSchema, Instruction,
    IrValidationError, KeyPurpose, KeySchema, LineageEntryState, ObjectConstruction,
    ProjectionAggregation, ProjectionFrontierPolicy, ProjectionGroupComponentSchema,
    ProjectionGroupSchema, RecordSchema, RecordTypeRef, RetryPolicy, StableIdNamespaceTag,
    UnaryOperator, ValueType, ValueTypeTag,
};
use riffdb_contract_syntax::{Span, SyntaxDiagnosticCode, ast::Declaration, parse_contract};
use riffdb_types::{
    CanonicalValue, Date, Decimal, DecimalSpec, FieldId, Money, ProjectionGeneration,
    ProjectionGroupKeyBuilder, ProjectionId, ProjectionIdentity, Timestamp, encode_canonical_value,
};

const BUDGET_SOURCE: &str = include_str!("../../../contracts/examples/budget.riff");
const RELATIONSHIP_FIXTURES: &[(&str, &str)] = &[
    (
        "cross-partition.riff",
        include_str!("../../../fixtures/compiler/relationships/cross-partition.riff"),
    ),
    (
        "dangling-create.riff",
        include_str!("../../../fixtures/compiler/relationships/dangling-create.riff"),
    ),
    (
        "late-read.riff",
        include_str!("../../../fixtures/compiler/relationships/late-read.riff"),
    ),
    (
        "optional-source.riff",
        include_str!("../../../fixtures/compiler/relationships/optional-source.riff"),
    ),
    (
        "partial-target.riff",
        include_str!("../../../fixtures/compiler/relationships/partial-target.riff"),
    ),
    (
        "type-mismatch.riff",
        include_str!("../../../fixtures/compiler/relationships/type-mismatch.riff"),
    ),
];

const OPTIONAL_ARITHMETIC_SOURCE: &str = r#"
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

const ROOT_VALIDATION_FIELD_SOURCE: &str = r#"
contract RootValidationFieldFixture version 1 {
  entity Root {
    key (tenant: uuid, root_id: uuid)
    field enabled: bool
  }
  entity Child {
    key (tenant: uuid, root_id: uuid, child_id: uuid)
    field amount: i64
  }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
    invariant root_enabled: enabled
  }
  command ChangeChild {
    input request_key: string<128>
    input tenant: uuid
    input root_id: uuid
    input child_id: uuid
    input amount: i64
    idempotency_key request_key
    mutate Child(tenant, root_id, child_id) as child_row else MissingChild {}
    set child_row.amount = amount
    return Changed { child_row: child_row }
  }
}
"#;

const ROOT_VALIDATION_CONSTANT_SOURCE: &str = r#"
contract RootValidationConstantFixture version 1 {
  entity Root {
    key (tenant: uuid, root_id: uuid)
  }
  entity Child {
    key (tenant: uuid, root_id: uuid, child_id: uuid)
    field amount: i64
  }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
    invariant root_exists: 1 == 1
  }
  command ChangeChild {
    input request_key: string<128>
    input tenant: uuid
    input root_id: uuid
    input child_id: uuid
    input amount: i64
    idempotency_key request_key
    mutate Child(tenant, root_id, child_id) as child_row else MissingChild {}
    set child_row.amount = amount
    return Changed { child_row: child_row }
  }
}
"#;

fn main() -> Result<(), Box<dyn Error>> {
    let output_root = parse_output_root()?;
    let fixture_root = output_root.join("fixtures/compiler");
    let schema_root = fixture_root.join("schemas");
    fs::create_dir_all(&schema_root)?;

    let bundle = compile_contract_source(BUDGET_SOURCE)?;
    fs::write(fixture_root.join("bundle.bin"), bundle.canonical_bytes())?;
    fs::write(
        fixture_root.join("bundle-hash.txt"),
        format!("{}\n", hex(bundle.bundle_hash().as_bytes())),
    )?;
    fs::write(
        fixture_root.join("plan-root-hash.txt"),
        format!("{}\n", hex(bundle.plan_root_hash().as_bytes())),
    )?;
    fs::write(
        fixture_root.join("bundle-metadata.txt"),
        render_bundle_metadata(&bundle)?,
    )?;
    fs::write(
        fixture_root.join("command-plans.txt"),
        render_command_plans(&bundle)?,
    )?;
    fs::write(
        fixture_root.join("projection-plans.txt"),
        render_projection_plans(&bundle)?,
    )?;
    fs::write(
        fixture_root.join("lineage-ledger.txt"),
        render_lineage_ledger(&bundle)?,
    )?;
    fs::write(
        fixture_root.join("compatibility.txt"),
        render_compatibility(&bundle)?,
    )?;

    let mut names = String::new();
    writeln!(names, "version=1")?;
    writeln!(
        names,
        "contract={}",
        bundle.mcp_command_names().source_contract_name()
    )?;
    for entry in bundle.mcp_command_names().entries() {
        writeln!(
            names,
            "command={} source={} tool={}",
            entry.command_id().get(),
            entry.source_command_name(),
            entry.tool_name().as_str(),
        )?;
    }
    fs::write(fixture_root.join("command-tool-names.txt"), names)?;

    fs::write(
        fixture_root.join("command-explain.txt"),
        render_command_explains(&bundle)?,
    )?;

    let (identities, group_keys, maxima) = render_projection_vectors(&bundle)?;
    fs::write(fixture_root.join("projection-identities.txt"), identities)?;
    fs::write(fixture_root.join("projection-group-keys.txt"), group_keys)?;
    fs::write(fixture_root.join("projection-maxima.txt"), maxima)?;

    for artifact in bundle.schema_artifacts() {
        let file_name = format!(
            "{:02x}-{:08x}.json",
            artifact.key().tag(),
            artifact.key().stable_id(),
        );
        fs::write(
            schema_root.join(file_name),
            format!("{}\n", artifact.canonical_json()),
        )?;
    }

    fs::write(
        fixture_root.join("diagnostics.txt"),
        diagnostic_snapshots()?,
    )?;
    fs::write(
        fixture_root.join("projection-rejections.txt"),
        projection_rejection_snapshots()?,
    )?;
    fs::write(
        fixture_root.join("optional-context.txt"),
        optional_context_snapshot()?,
    )?;
    fs::write(fixture_root.join("late-bounds.txt"), late_bound_snapshot()?)?;
    generate_relationship_source_fixtures(&fixture_root)?;
    generate_root_validation_fixtures(&fixture_root)?;
    generate_mcp_evolution_fixture(&fixture_root)?;
    Ok(())
}

fn generate_relationship_source_fixtures(
    fixture_root: &std::path::Path,
) -> Result<(), Box<dyn Error>> {
    let root = fixture_root.join("relationships");
    fs::create_dir_all(&root)?;
    for (name, source) in RELATIONSHIP_FIXTURES {
        fs::write(root.join(name), source)?;
    }
    Ok(())
}

fn generate_mcp_evolution_fixture(fixture_root: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let genesis = compile_contract_source(&mcp_evolution_source(1, "Allocate"))?;
    let unchanged = compile_contract_successor(&mcp_evolution_source(2, "Allocate"), &genesis)?;
    let renamed = compile_contract_successor(&mcp_evolution_source(2, "Reallocate"), &genesis)?;

    if genesis.mcp_command_names() != unchanged.mcp_command_names() {
        return Err("unchanged successor changed the MCP command-name registry".into());
    }
    if unchanged.compatibility().overall() != CompatibilityClass::Compatible
        || unchanged.compatibility().entries().len() != 1
        || unchanged.compatibility().entries()[0].code().as_str() != "RDB-K001"
    {
        return Err("unchanged MCP registry did not produce exact no-change compatibility".into());
    }
    let [old_entry] = genesis.mcp_command_names().entries() else {
        return Err("MCP evolution genesis must have one registry entry".into());
    };
    let [renamed_entry] = renamed.mcp_command_names().entries() else {
        return Err("MCP evolution successor must have one registry entry".into());
    };
    if renamed_entry.command_id() <= old_entry.command_id()
        || renamed_entry.source_command_name() != "Reallocate"
        || renamed_entry.tool_name().as_str() != "riffdb_cmd_toolevolution_reallocate"
        || renamed.compatibility().overall() != CompatibilityClass::Incompatible
    {
        return Err("renamed MCP command did not produce the canonical new public identity".into());
    }
    let has_added = renamed
        .compatibility()
        .entries()
        .iter()
        .any(|entry| entry.code().as_str() == "RDB-K010");
    let has_removed = renamed
        .compatibility()
        .entries()
        .iter()
        .any(|entry| entry.code().as_str() == "RDB-K100");
    if !has_added || !has_removed {
        return Err("renamed MCP command did not expose its public compatibility change".into());
    }

    fs::write(
        fixture_root.join("mcp-command-evolution.txt"),
        render_mcp_evolution(&genesis, &unchanged, &renamed)?,
    )?;
    Ok(())
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

fn render_mcp_evolution(
    genesis: &ContractBundle,
    unchanged: &ContractBundle,
    renamed: &ContractBundle,
) -> Result<String, std::fmt::Error> {
    let mut output = String::new();
    writeln!(output, "format=riffdb-mcp-command-evolution-fixture-v1")?;
    for (label, bundle) in [
        ("genesis", genesis),
        ("unchanged", unchanged),
        ("renamed", renamed),
    ] {
        writeln!(output, "\n[{label}]")?;
        for entry in bundle.mcp_command_names().entries() {
            writeln!(
                output,
                "registry command={} source={} tool={}",
                entry.command_id().get(),
                entry.source_command_name(),
                entry.tool_name().as_str()
            )?;
        }
        writeln!(
            output,
            "compatibility={}",
            compatibility_class(bundle.compatibility().overall())
        )?;
        for entry in bundle.compatibility().entries() {
            writeln!(
                output,
                "compatibility_entry code={} class={} path={}",
                entry.code().as_str(),
                compatibility_class(entry.class()),
                entry.affected_path()
            )?;
        }
        for allocation in bundle
            .ledger()
            .allocations()
            .iter()
            .filter(|allocation| allocation.namespace().tag() == StableIdNamespaceTag::Command)
        {
            for entry in allocation.entries() {
                writeln!(
                    output,
                    "command_identity id={} name={} state={}",
                    entry.id(),
                    entry.name(),
                    lineage_state(entry.state())
                )?;
            }
        }
    }
    Ok(output)
}

fn generate_root_validation_fixtures(fixture_root: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let root = fixture_root.join("root-validation");
    fs::create_dir_all(&root)?;
    generate_root_validation_case(&root, "field-dependent", ROOT_VALIDATION_FIELD_SOURCE, true)?;
    generate_root_validation_case(
        &root,
        "constant-invariant",
        ROOT_VALIDATION_CONSTANT_SOURCE,
        false,
    )?;
    Ok(())
}

fn generate_root_validation_case(
    root: &std::path::Path,
    name: &str,
    source: &str,
    field_dependent: bool,
) -> Result<(), Box<dyn Error>> {
    let genesis = compile_contract_source(source)?;
    let successor_source = source.replacen(" version 1 ", " version 2 ", 1);
    if successor_source == source {
        return Err("root-validation fixture version marker was not replaced".into());
    }
    let successor = compile_contract_successor(&successor_source, &genesis)?;
    validate_root_validation_case(&genesis, &successor, field_dependent)?;

    let decoded = ContractBundle::decode(genesis.canonical_bytes())?;
    if decoded.canonical_bytes() != genesis.canonical_bytes() {
        return Err("root-validation bundle did not round-trip canonically".into());
    }

    fs::write(
        root.join(format!("{name}-bundle-v1.bin")),
        genesis.canonical_bytes(),
    )?;
    fs::write(
        root.join(format!("{name}-bundle-v2.bin")),
        successor.canonical_bytes(),
    )?;
    fs::write(
        root.join(format!("{name}-summary.txt")),
        render_root_validation_summary(name, &genesis, &successor)?,
    )?;
    fs::write(
        root.join(format!("{name}-plan.txt")),
        render_command_plans(&genesis)?,
    )?;
    fs::write(
        root.join(format!("{name}-explain.txt")),
        render_command_explains(&genesis)?,
    )?;
    fs::write(
        root.join(format!("{name}-successor-compatibility.txt")),
        render_compatibility(&successor)?,
    )?;
    Ok(())
}

fn validate_root_validation_case(
    genesis: &ContractBundle,
    successor: &ContractBundle,
    field_dependent: bool,
) -> Result<(), Box<dyn Error>> {
    let [genesis_command] = genesis.commands() else {
        return Err("root-validation fixture must compile exactly one command".into());
    };
    let [successor_command] = successor.commands() else {
        return Err("root-validation successor must compile exactly one command".into());
    };
    let [read] = genesis_command.root_validation_reads() else {
        return Err("root-validation fixture must compile exactly one internal read".into());
    };
    if read.accessed_fields().is_empty() == field_dependent {
        return Err("root-validation fixture has the wrong accessed-field shape".into());
    }
    let has_root_field = genesis_command
        .expressions()
        .nodes()
        .iter()
        .any(|node| matches!(node.kind(), ExpressionKind::RootValidationField { .. }));
    if has_root_field != field_dependent {
        return Err("root-validation fixture has the wrong expression shape".into());
    }
    if genesis_command.plan_hash() != successor_command.plan_hash() {
        return Err("unchanged root-validation successor changed PlanHash".into());
    }
    if successor.compatibility().overall() != CompatibilityClass::Compatible
        || successor.compatibility().entries().len() != 1
        || successor.compatibility().entries()[0].code().as_str() != "RDB-K001"
        || successor.compatibility().entries()[0].affected_path() != "contract"
    {
        return Err(format!(
            "unchanged root-validation successor is not compatible:\n{}",
            render_compatibility(successor)?
        )
        .into());
    }
    Ok(())
}

fn render_root_validation_summary(
    name: &str,
    genesis: &ContractBundle,
    successor: &ContractBundle,
) -> Result<String, std::fmt::Error> {
    let command = &genesis.commands()[0];
    let read = &command.root_validation_reads()[0];
    let mut output = String::new();
    writeln!(output, "format=riffdb-root-validation-fixture-v1")?;
    writeln!(output, "case={name}")?;
    writeln!(
        output,
        "bundle_v1_bytes={}",
        genesis.canonical_bytes().len()
    )?;
    writeln!(
        output,
        "bundle_v1_hash={}",
        hex(genesis.bundle_hash().as_bytes())
    )?;
    writeln!(
        output,
        "bundle_v2_bytes={}",
        successor.canonical_bytes().len()
    )?;
    writeln!(
        output,
        "bundle_v2_hash={}",
        hex(successor.bundle_hash().as_bytes())
    )?;
    writeln!(output, "plan_hash={}", hex(command.plan_hash().as_bytes()))?;
    writeln!(output, "root_read_id={}", read.id().get())?;
    writeln!(output, "source_binding={}", read.source_binding().get())?;
    writeln!(output, "root_entity={}", read.entity_type().get())?;
    writeln!(
        output,
        "key_expressions={}",
        ids(read.key_expressions().iter().map(|id| id.get()))
    )?;
    writeln!(
        output,
        "accessed_fields={}",
        ids(read.accessed_fields().iter().map(|id| id.get()))
    )?;
    writeln!(
        output,
        "root_field_expressions={}",
        command
            .expressions()
            .nodes()
            .iter()
            .filter(|node| matches!(node.kind(), ExpressionKind::RootValidationField { .. }))
            .count()
    )?;
    writeln!(
        output,
        "successor_compatibility={}",
        compatibility_class(successor.compatibility().overall())
    )?;
    writeln!(
        output,
        "successor_compatibility_entries={}",
        successor.compatibility().entries().len()
    )?;
    writeln!(
        output,
        "successor_compatibility_code={}",
        successor.compatibility().entries()[0].code().as_str()
    )?;
    Ok(output)
}

fn parse_output_root() -> Result<PathBuf, Box<dyn Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [flag, root] if flag == "--output-root" => Ok(PathBuf::from(root)),
        _ => Err("usage: generate_contract_fixtures --output-root <path>".into()),
    }
}

fn render_bundle_metadata(bundle: &ContractBundle) -> Result<String, std::fmt::Error> {
    let mut output = String::new();
    writeln!(output, "bundle_format={}", bundle.format_version())?;
    writeln!(output, "grammar={}", bundle.grammar_version())?;
    writeln!(output, "executable_ir={}", bundle.ir_version())?;
    writeln!(output, "compiler={}", bundle.compiler_version())?;
    writeln!(output, "lineage={}", bundle.lineage().as_str())?;
    writeln!(
        output,
        "contract_version={}",
        bundle.contract_version().get()
    )?;
    match bundle.parent() {
        Some(parent) => {
            writeln!(output, "parent_version={}", parent.contract_version().get())?;
            writeln!(
                output,
                "parent_hash={}",
                hex(parent.bundle_hash().as_bytes())
            )?;
        }
        None => writeln!(output, "parent=none")?,
    }
    writeln!(
        output,
        "source_hash={}",
        hex(bundle.source_hash().as_bytes())
    )?;
    writeln!(
        output,
        "plan_root_hash={}",
        hex(bundle.plan_root_hash().as_bytes())
    )?;
    writeln!(
        output,
        "bundle_hash={}",
        hex(bundle.bundle_hash().as_bytes())
    )?;
    writeln!(output, "canonical_bytes={}", bundle.canonical_bytes().len())?;
    writeln!(output, "commands={}", bundle.commands().len())?;
    writeln!(output, "projections={}", bundle.projections().len())?;
    writeln!(
        output,
        "schema_artifacts={}",
        bundle.schema_artifacts().len()
    )?;
    Ok(output)
}

fn render_command_plans(bundle: &ContractBundle) -> Result<String, Box<dyn Error>> {
    let mut output = String::new();
    writeln!(output, "format=riffdb-command-plan-fixture-v1")?;
    for command in bundle.commands() {
        writeln!(output, "\n[command {}]", command.command_id().get())?;
        writeln!(output, "name={}", command.name())?;
        writeln!(
            output,
            "contract_version={}",
            command.contract_version().get()
        )?;
        writeln!(output, "plan_hash={}", hex(command.plan_hash().as_bytes()))?;
        writeln!(
            output,
            "execution={}",
            execution_class(command.execution_class())
        )?;
        writeln!(output, "retry={}", retry_policy(command.retry_policy()))?;
        writeln!(
            output,
            "capability={}",
            capability(command.required_capability())
        )?;
        writeln!(
            output,
            "idempotency_input={}",
            optional_id(command.idempotency_input().map(|id| id.get()))
        )?;
        writeln!(
            output,
            "success_outcome={}",
            command.success_outcome().get()
        )?;
        writeln!(
            output,
            "input_owner={}",
            record_type(command.input().record().owner())
        )?;
        render_record_fields(&mut output, "input", command.input().record())?;

        writeln!(output, "outcome_count={}", command.outcomes().len())?;
        for outcome in command.outcomes() {
            writeln!(
                output,
                "outcome={} name={} owner={}",
                outcome.id().get(),
                outcome.name(),
                record_type(outcome.payload().owner())
            )?;
            render_record_fields(
                &mut output,
                &format!("outcome.{}", outcome.id().get()),
                outcome.payload(),
            )?;
        }

        render_expression_arena(&mut output, command.expressions())?;

        writeln!(output, "binding_count={}", command.bindings().len())?;
        for binding in command.bindings() {
            writeln!(
                output,
                "binding={} name={} mode={} entity={} keys={} reads={} complete={} failure={}",
                binding.id().get(),
                binding.name(),
                binding_mode(binding.mode()),
                binding.entity_type().get(),
                ids(binding.key_expressions().iter().map(|id| id.get())),
                ids(binding.accessed_fields().iter().map(|id| id.get())),
                binding.complete_record_access(),
                render_object(binding.failure().payload()),
            )?;
            render_key_schema(&mut output, "binding.key", binding.key_schema())?;
        }

        writeln!(
            output,
            "root_validation_read_count={}",
            command.root_validation_reads().len()
        )?;
        for read in command.root_validation_reads() {
            writeln!(
                output,
                "root_validation_read={} source_binding={} entity={} keys={} reads={}",
                read.id().get(),
                read.source_binding().get(),
                read.entity_type().get(),
                ids(read.key_expressions().iter().map(|id| id.get())),
                ids(read.accessed_fields().iter().map(|id| id.get())),
            )?;
            render_key_schema(&mut output, "root_validation.key", read.key_schema())?;
        }

        let locality = command.locality();
        writeln!(
            output,
            "locality.aggregate={} partition_expression={}",
            locality.aggregate_id().get(),
            locality.partition_expression().get()
        )?;
        render_key_schema(
            &mut output,
            "locality.partition",
            locality.partition_schema(),
        )?;
        writeln!(
            output,
            "locality.conflict_count={}",
            locality.conflict_keys().len()
        )?;
        for (index, conflict) in locality.conflict_keys().iter().enumerate() {
            writeln!(
                output,
                "locality.conflict.{index}.expressions={}",
                ids(conflict.expressions().iter().map(|id| id.get()))
            )?;
            render_key_schema(&mut output, "locality.conflict.key", conflict.schema())?;
        }

        writeln!(
            output,
            "commit_check_count={}",
            command.commit_checks().len()
        )?;
        for check in command.commit_checks() {
            writeln!(
                output,
                "commit_check invariant={} predicate={} bindings={} root_reads={}",
                check.invariant_id().get(),
                check.predicate().get(),
                ids(check.source_bindings().iter().map(|id| id.get())),
                ids(check.root_validation_reads().iter().map(|id| id.get())),
            )?;
        }

        writeln!(output, "instruction_count={}", command.instructions().len())?;
        for (index, instruction) in command.instructions().iter().enumerate() {
            writeln!(
                output,
                "instruction.{index}={}",
                render_instruction(instruction)
            )?;
        }
    }
    Ok(output)
}

fn render_projection_plans(bundle: &ContractBundle) -> Result<String, Box<dyn Error>> {
    let mut output = String::new();
    writeln!(output, "format=riffdb-projection-plan-fixture-v1")?;
    for projection in bundle.projections() {
        writeln!(
            output,
            "\n[projection {}]",
            projection.projection_id().get()
        )?;
        writeln!(output, "name={}", projection.name())?;
        writeln!(output, "source_event={}", projection.source_event().get())?;
        writeln!(
            output,
            "plan_hash={}",
            hex(projection.plan_hash().as_bytes())
        )?;
        writeln!(
            output,
            "frontier={}",
            projection_frontier(projection.frontier())
        )?;
        writeln!(
            output,
            "filter={}",
            optional_id(projection.filter().map(|id| id.get()))
        )?;
        writeln!(
            output,
            "key_expressions={}",
            ids(projection.key_expressions().iter().map(|id| id.get()))
        )?;
        render_expression_arena(&mut output, projection.expressions())?;

        let schema = projection.group_schema();
        writeln!(output, "group.codec={}", schema.codec_version())?;
        writeln!(
            output,
            "group.components={}",
            schema.group_components().len()
        )?;
        for (index, component) in schema.group_components().iter().enumerate() {
            writeln!(
                output,
                "group.component.{index}=type:{} variants:{} maximum_framed_bytes:{}",
                value_type(component.value_type()),
                ids(component.enum_variants().iter().map(|id| id.get())),
                component.maximum_framed_bytes(),
            )?;
        }
        render_record_fields(&mut output, "group.measures", schema.measures())?;
        writeln!(output, "measure_count={}", projection.measures().len())?;
        for measure in projection.measures() {
            writeln!(
                output,
                "measure={} name={} type={} aggregation={} expression={}",
                measure.field().id().get(),
                measure.field().name(),
                value_type(measure.field().value_type()),
                projection_aggregation(measure.aggregation()),
                optional_id(measure.expression().map(|id| id.get())),
            )?;
        }
        writeln!(
            output,
            "maximum_complete_key_bytes={}",
            schema.maximum_complete_key_bytes()
        )?;
        writeln!(
            output,
            "maximum_stored_state_bytes={}",
            schema.maximum_stored_state_bytes()
        )?;
    }
    Ok(output)
}

fn render_lineage_ledger(bundle: &ContractBundle) -> Result<String, std::fmt::Error> {
    let mut output = String::new();
    writeln!(output, "format=riffdb-lineage-ledger-fixture-v1")?;
    writeln!(output, "version={}", bundle.ledger().version())?;
    writeln!(
        output,
        "allocation_count={}",
        bundle.ledger().allocations().len()
    )?;
    for (index, allocation) in bundle.ledger().allocations().iter().enumerate() {
        let namespace = allocation.namespace();
        writeln!(
            output,
            "allocation.{index}=tag:{} owner_kind:0x{:02x} owners:{} max_allocated:{} entries:{}",
            stable_namespace_tag(namespace.tag()),
            namespace.owner_kind(),
            ids(namespace.owner_ids().iter().copied()),
            allocation.max_allocated(),
            allocation.entries().len(),
        )?;
        for entry in allocation.entries() {
            let identity = entry.identity();
            let identity_namespace = identity.namespace();
            writeln!(
                output,
                "entry id:{} state:{} identity_tag:{} owner_kind:0x{:02x} owners:{} name:{}",
                entry.id(),
                lineage_state(entry.state()),
                stable_namespace_tag(identity_namespace.tag()),
                identity_namespace.owner_kind(),
                ids(identity_namespace.owner_ids().iter().copied()),
                identity.name(),
            )?;
        }
    }
    Ok(output)
}

fn render_compatibility(bundle: &ContractBundle) -> Result<String, std::fmt::Error> {
    let mut output = String::new();
    let report = bundle.compatibility();
    writeln!(output, "format=riffdb-compatibility-report-fixture-v1")?;
    writeln!(output, "overall={}", compatibility_class(report.overall()))?;
    writeln!(output, "entry_count={}", report.entries().len())?;
    for entry in report.entries() {
        writeln!(
            output,
            "entry code={} class={} path={}",
            entry.code().as_str(),
            compatibility_class(entry.class()),
            entry.affected_path(),
        )?;
    }
    Ok(output)
}

fn render_command_explains(bundle: &ContractBundle) -> Result<String, Box<dyn Error>> {
    let mut output = String::new();
    writeln!(output, "format=riffdb-command-explain-fixture-v1")?;
    for command in bundle.commands() {
        let explain = CommandExplain::from_plan(command);
        writeln!(output, "\n[command {}]", explain.command_id().get())?;
        writeln!(
            output,
            "execution={}",
            execution_class(explain.execution_class())
        )?;
        writeln!(
            output,
            "partition_components={}",
            explain.partition_component_count()
        )?;
        writeln!(
            output,
            "partition_expression={}",
            explain.partition_expression().get()
        )?;
        writeln!(output, "conflict_keys={}", explain.conflict_key_count())?;
        writeln!(
            output,
            "bindings={}",
            ids(explain.bindings().iter().map(|id| id.get()))
        )?;
        writeln!(
            output,
            "read_fields={}",
            pairs(
                explain
                    .read_fields()
                    .iter()
                    .map(|(binding, field)| { (binding.get(), field.get()) })
            )
        )?;
        writeln!(
            output,
            "write_fields={}",
            pairs(
                explain
                    .write_fields()
                    .iter()
                    .map(|(binding, field)| { (binding.get(), field.get()) })
            )
        )?;
        writeln!(
            output,
            "invariants={}",
            ids(explain.invariants().iter().map(|id| id.get()))
        )?;
        writeln!(
            output,
            "events={}",
            ids(explain.events().iter().map(|id| id.get()))
        )?;
        writeln!(
            output,
            "outcomes={}",
            ids(explain.outcomes().iter().map(|id| id.get()))
        )?;
        writeln!(
            output,
            "root_validation_read_count={}",
            explain.root_validation_reads().len()
        )?;
        for read in explain.root_validation_reads() {
            writeln!(
                output,
                "root_validation_read={} source_binding={} entity={} keys={} reads={}",
                read.id().get(),
                read.source_binding().get(),
                read.entity_type().get(),
                ids(read.key_expressions().iter().map(|id| id.get())),
                ids(read.accessed_fields().iter().map(|id| id.get())),
            )?;
        }
        writeln!(
            output,
            "commit_check_count={}",
            explain.commit_checks().len()
        )?;
        for check in explain.commit_checks() {
            writeln!(
                output,
                "commit_check invariant={} predicate={} bindings={} root_reads={}",
                check.invariant_id().get(),
                check.predicate().get(),
                ids(check.source_bindings().iter().map(|id| id.get())),
                ids(check.root_validation_reads().iter().map(|id| id.get())),
            )?;
        }
        render_expression_arena(&mut output, explain.expressions())?;
    }
    Ok(output)
}

fn render_projection_vectors(
    bundle: &ContractBundle,
) -> Result<(String, String, String), Box<dyn Error>> {
    let mut identities = String::from("format=riffdb-projection-identity-fixture-v1\n");
    let mut group_keys = String::from("format=riffdb-projection-group-key-fixture-v1\n");
    let mut maxima = String::from("format=riffdb-projection-maxima-fixture-v1\n");
    for projection in bundle.projections() {
        let identity = ProjectionIdentity::new(
            bundle.lineage().clone(),
            projection.projection_id(),
            projection.plan_hash(),
        );
        writeln!(
            identities,
            "projection={} lineage={} plan_hash={} canonical={}",
            projection.projection_id().get(),
            identity.contract_lineage().as_str(),
            hex(identity.plan_hash().as_bytes()),
            hex(&identity.to_canonical_bytes()),
        )?;

        let values = projection
            .group_schema()
            .group_components()
            .iter()
            .map(fixture_projection_value)
            .collect::<Result<Vec<_>, _>>()?;
        let mut builder = ProjectionGroupKeyBuilder::new(identity, ProjectionGeneration::first());
        for value in &values {
            builder.push_component(value.clone())?;
        }
        let key = builder.finish()?;
        let encoded_values = values
            .iter()
            .map(|value| encode_canonical_value(value).map(|bytes| hex(&bytes)))
            .collect::<Result<Vec<_>, _>>()?
            .join(",");
        writeln!(
            group_keys,
            "projection={} generation={} components=[{}] key={}",
            projection.projection_id().get(),
            key.generation().get(),
            encoded_values,
            hex(key.as_bytes()),
        )?;
        writeln!(
            maxima,
            "projection={} plan_hash={} complete_key_bytes={} stored_state_bytes={}",
            projection.projection_id().get(),
            hex(projection.plan_hash().as_bytes()),
            projection.group_schema().maximum_complete_key_bytes(),
            projection.group_schema().maximum_stored_state_bytes(),
        )?;
    }
    Ok((identities, group_keys, maxima))
}

fn fixture_projection_value(
    component: &riffdb_contract_ir::ProjectionGroupComponentSchema,
) -> Result<CanonicalValue, Box<dyn Error>> {
    let value_type = component.value_type();
    let value = match value_type.tag() {
        ValueTypeTag::Bool => CanonicalValue::Bool(false),
        ValueTypeTag::I64 => CanonicalValue::I64(2_026),
        ValueTypeTag::U64 => CanonicalValue::U64(2_026),
        ValueTypeTag::Decimal => {
            let spec = value_type
                .decimal_spec()
                .ok_or("decimal type without a specification")?;
            CanonicalValue::Decimal(Decimal::new(spec, 0)?)
        }
        ValueTypeTag::Money => {
            let currency = value_type
                .currency()
                .ok_or("money type without a currency")?;
            let spec = DecimalSpec::new(riffdb_types::MAX_DECIMAL_PRECISION, 2)?;
            CanonicalValue::Money(Money::new(currency, Decimal::new(spec, 0)?))
        }
        ValueTypeTag::String => CanonicalValue::string("fixture")?,
        ValueTypeTag::Bytes => CanonicalValue::bytes([0x66, 0x78])?,
        ValueTypeTag::Timestamp => CanonicalValue::Timestamp(Timestamp::new(0, 0)?),
        ValueTypeTag::Date => CanonicalValue::Date(Date::new(0)),
        ValueTypeTag::Uuid => CanonicalValue::Uuid([0x11; 16]),
        ValueTypeTag::Enum => CanonicalValue::Enum {
            type_id: value_type.enum_type_id().ok_or("enum type without an ID")?,
            variant_id: *component
                .enum_variants()
                .first()
                .ok_or("enum projection component without a variant")?,
        },
        ValueTypeTag::Optional | ValueTypeTag::List | ValueTypeTag::Record => {
            return Err("projection fixture encountered a non-scalar group component".into());
        }
    };
    value_type.validate_value(&value)?;
    Ok(value)
}

fn render_expression_arena(
    output: &mut String,
    arena: &ExpressionArena,
) -> Result<(), Box<dyn Error>> {
    writeln!(output, "expression_count={}", arena.len())?;
    for (index, node) in arena.nodes().iter().enumerate() {
        let kind = match node.kind() {
            ExpressionKind::Constant(value) => {
                format!("constant:{}", hex(&encode_canonical_value(value)?))
            }
            ExpressionKind::InputField(field) => format!("input_field:{}", field.get()),
            ExpressionKind::CompleteBinding(binding) => {
                format!("complete_binding:{}", binding.get())
            }
            ExpressionKind::BoundField { binding, field } => {
                format!("bound_field:{}:{}", binding.get(), field.get())
            }
            ExpressionKind::SchemaField { entity_type, field } => {
                format!("schema_field:{}:{}", entity_type.get(), field.get())
            }
            ExpressionKind::RootValidationField { read, field } => {
                format!("root_validation_field:{}:{}", read.get(), field.get())
            }
            ExpressionKind::SourceEventField(field) => {
                format!("source_event_field:{}", field.get())
            }
            ExpressionKind::TransactionTime => "transaction_time".to_owned(),
            ExpressionKind::TransactionDate => "transaction_date".to_owned(),
            ExpressionKind::Unary { operator, operand } => {
                format!("unary:{}:{}", unary_operator(*operator), operand.get())
            }
            ExpressionKind::Binary {
                operator,
                left,
                right,
            } => format!(
                "binary:{}:{}:{}",
                binary_operator(*operator),
                left.get(),
                right.get()
            ),
        };
        writeln!(
            output,
            "expression.{index}=type:{} kind:{}",
            value_type(node.result_type()),
            kind
        )?;
    }
    Ok(())
}

fn render_key_schema(
    output: &mut String,
    prefix: &str,
    schema: &KeySchema,
) -> Result<(), std::fmt::Error> {
    writeln!(
        output,
        "{prefix}=purpose:{} components:{} maximum_encoded_bytes:{}",
        key_purpose(schema.purpose()),
        schema.components().len(),
        schema.maximum_encoded_bytes(),
    )?;
    for (index, component) in schema.components().iter().enumerate() {
        writeln!(
            output,
            "{prefix}.component.{index}=type:{} variants:{} maximum_payload_bytes:{}",
            value_type(component.value_type()),
            ids(component.enum_variants().iter().map(|id| id.get())),
            component.maximum_payload_bytes(),
        )?;
    }
    Ok(())
}

fn render_record_fields(
    output: &mut String,
    prefix: &str,
    record: &RecordSchema,
) -> Result<(), std::fmt::Error> {
    writeln!(output, "{prefix}.field_count={}", record.fields().len())?;
    for field in record.fields() {
        writeln!(
            output,
            "{prefix}.field.{}=name:{} type:{}",
            field.id().get(),
            field.name(),
            value_type(field.value_type()),
        )?;
    }
    Ok(())
}

fn render_instruction(instruction: &Instruction) -> String {
    match instruction {
        Instruction::Require {
            requirement_index,
            predicate,
            reject,
        } => format!(
            "require index:{} predicate:{} reject:{}",
            requirement_index,
            predicate.get(),
            render_object(reject.payload())
        ),
        Instruction::SetField {
            binding,
            field,
            value,
        } => format!(
            "set binding:{} field:{} value:{}",
            binding.get(),
            field.get(),
            value.get()
        ),
        Instruction::EmitEvent(event) => format!(
            "emit event:{} payload:{}",
            event.event_type().get(),
            render_object(event.payload())
        ),
        Instruction::Return(outcome) => format!(
            "return outcome:{} payload:{}",
            outcome.outcome_id().get(),
            render_object(outcome.payload())
        ),
    }
}

fn render_object(object: &ObjectConstruction) -> String {
    let fields = object
        .fields()
        .iter()
        .map(|field| format!("{}:{}", field.field_id().get(), field.expression().get()))
        .collect::<Vec<_>>()
        .join(",");
    format!("owner:{} fields:[{}]", record_type(object.record()), fields)
}

fn value_type(value: &ValueType) -> String {
    match value.tag() {
        ValueTypeTag::Bool => "bool".to_owned(),
        ValueTypeTag::I64 => "i64".to_owned(),
        ValueTypeTag::U64 => "u64".to_owned(),
        ValueTypeTag::Decimal => {
            let spec = value
                .decimal_spec()
                .expect("decimal type has a specification");
            format!("decimal<{},{}>", spec.precision(), spec.scale())
        }
        ValueTypeTag::Money => {
            let currency = value.currency().expect("money type has a currency");
            format!("money<{}>", String::from_utf8_lossy(currency.as_bytes()))
        }
        ValueTypeTag::String => {
            format!(
                "string<{}>",
                value.byte_bound().expect("string type has a bound")
            )
        }
        ValueTypeTag::Bytes => {
            format!(
                "bytes<{}>",
                value.byte_bound().expect("bytes type has a bound")
            )
        }
        ValueTypeTag::Timestamp => "timestamp".to_owned(),
        ValueTypeTag::Date => "date".to_owned(),
        ValueTypeTag::Uuid => "uuid".to_owned(),
        ValueTypeTag::Enum => format!(
            "enum<{}>",
            value.enum_type_id().expect("enum type has an ID").get()
        ),
        ValueTypeTag::Optional => format!(
            "optional<{}>",
            value_type(
                value
                    .optional_inner()
                    .expect("optional type has an inner type")
            )
        ),
        ValueTypeTag::List => {
            let (element, maximum) = value.list_parts().expect("list type has parts");
            format!("list<{},{}>", value_type(element), maximum)
        }
        ValueTypeTag::Record => format!(
            "record<{}>",
            record_type(value.record_ref().expect("record type has an owner"))
        ),
    }
}

fn record_type(record: &RecordTypeRef) -> String {
    match record {
        RecordTypeRef::Entity(id) => format!("entity:{}", id.get()),
        RecordTypeRef::Event(id) => format!("event:{}", id.get()),
        RecordTypeRef::CommandInput(id) => format!("command-input:{}", id.get()),
        RecordTypeRef::CommandOutcome {
            command_id,
            outcome_id,
        } => format!("command-outcome:{}:{}", command_id.get(), outcome_id.get()),
        RecordTypeRef::ProjectionResult(id) => format!("projection-result:{}", id.get()),
    }
}

fn key_purpose(purpose: KeyPurpose) -> String {
    match purpose {
        KeyPurpose::Entity(id) => format!("entity:{}", id.get()),
        KeyPurpose::Partition(id) => format!("partition:{}", id.get()),
        KeyPurpose::Conflict(id) => format!("conflict:{}", id.get()),
        KeyPurpose::Index {
            index_id,
            entity_type,
        } => format!("index:{}:entity:{}", index_id.get(), entity_type.get()),
    }
}

fn unary_operator(operator: UnaryOperator) -> &'static str {
    match operator {
        UnaryOperator::Not => "not",
        UnaryOperator::Negate => "negate",
    }
}

fn binary_operator(operator: BinaryOperator) -> &'static str {
    match operator {
        BinaryOperator::Multiply => "multiply",
        BinaryOperator::Divide => "divide",
        BinaryOperator::Add => "add",
        BinaryOperator::Subtract => "subtract",
        BinaryOperator::Equal => "equal",
        BinaryOperator::NotEqual => "not-equal",
        BinaryOperator::Less => "less",
        BinaryOperator::LessEqual => "less-equal",
        BinaryOperator::Greater => "greater",
        BinaryOperator::GreaterEqual => "greater-equal",
        BinaryOperator::And => "and",
        BinaryOperator::Or => "or",
    }
}

fn binding_mode(mode: BindingMode) -> &'static str {
    match mode {
        BindingMode::Read => "read",
        BindingMode::Mutate => "mutate",
        BindingMode::Create => "create",
    }
}

fn execution_class(class: ExecutionClass) -> &'static str {
    match class {
        ExecutionClass::ReadOnly => "read-only",
        ExecutionClass::IdempotentMutation => "idempotent-mutation",
    }
}

fn retry_policy(policy: RetryPolicy) -> &'static str {
    match policy {
        RetryPolicy::BoundedFullReevaluation => "bounded-full-reevaluation",
    }
}

fn capability(capability: &CapabilityRequirement) -> String {
    match capability {
        CapabilityRequirement::InvokeCommand {
            lineage,
            command_id,
        } => {
            format!("invoke-command:{}:{}", lineage.as_str(), command_id.get())
        }
    }
}

fn projection_aggregation(aggregation: ProjectionAggregation) -> &'static str {
    match aggregation {
        ProjectionAggregation::Count => "count",
        ProjectionAggregation::Sum => "sum",
    }
}

fn projection_frontier(frontier: ProjectionFrontierPolicy) -> &'static str {
    match frontier {
        ProjectionFrontierPolicy::TransactionallyOrdered => "transactionally-ordered",
    }
}

fn stable_namespace_tag(tag: StableIdNamespaceTag) -> &'static str {
    match tag {
        StableIdNamespaceTag::Entity => "entity",
        StableIdNamespaceTag::Event => "event",
        StableIdNamespaceTag::Enum => "enum",
        StableIdNamespaceTag::Aggregate => "aggregate",
        StableIdNamespaceTag::Command => "command",
        StableIdNamespaceTag::Projection => "projection",
        StableIdNamespaceTag::Index => "index",
        StableIdNamespaceTag::Invariant => "invariant",
        StableIdNamespaceTag::Field => "field",
        StableIdNamespaceTag::Outcome => "outcome",
        StableIdNamespaceTag::EnumVariant => "enum-variant",
    }
}

fn lineage_state(state: LineageEntryState) -> &'static str {
    match state {
        LineageEntryState::Active => "active",
        LineageEntryState::Tombstone => "tombstone",
    }
}

fn compatibility_class(class: CompatibilityClass) -> &'static str {
    match class {
        CompatibilityClass::Compatible => "compatible",
        CompatibilityClass::RequiresExplicitVersion => "requires-explicit-version",
        CompatibilityClass::Incompatible => "incompatible",
    }
}

fn projection_boundary_source(value_type: &str, component_count: usize) -> String {
    let components = vec!["group"; component_count].join(", ");
    format!(
        "contract ProjectionBoundary version 1 {{\n  event Source {{ group: {value_type} }}\n  projection Totals {{\n    source event Source\n    key ({components})\n    measure total = count()\n    frontier transactionally_ordered\n  }}\n}}\n"
    )
}

fn projection_spans(source: &str) -> Result<(Span, Span), Box<dyn Error>> {
    let document = parse_contract(source)?;
    document
        .contract
        .value
        .declarations
        .iter()
        .find_map(|declaration| match &declaration.value {
            Declaration::Projection(projection) => {
                Some((projection.name.span, projection.key[0].span))
            }
            _ => None,
        })
        .ok_or_else(|| "projection boundary source has no projection".into())
}

fn semantic_rejection(
    name: &str,
    source: &str,
    expected_code: CompilerDiagnosticCode,
    expected_span: Span,
) -> Result<CompilationError, Box<dyn Error>> {
    let error = validate_contract_source(source)
        .expect_err("generated projection rejection source must fail");
    let diagnostics = error
        .semantic()
        .ok_or_else(|| format!("{name} did not produce semantic diagnostics"))?;
    let matching = diagnostics
        .as_slice()
        .iter()
        .filter(|diagnostic| diagnostic.code() == expected_code)
        .collect::<Vec<_>>();
    if matching.len() != 1 || matching[0].primary_span() != expected_span {
        return Err(format!(
            "{name} did not produce exactly one {} diagnostic at {expected_span:?}",
            expected_code.as_str()
        )
        .into());
    }
    Ok(error)
}

fn projection_measure_schema() -> RecordSchema {
    RecordSchema::new(
        RecordTypeRef::ProjectionResult(ProjectionId::first()),
        vec![FieldSchema::new(FieldId::first(), "total", ValueType::u64()).expect("field")],
    )
    .expect("projection measure schema")
}

fn projection_schema_error(
    component: ProjectionGroupComponentSchema,
    component_count: usize,
) -> IrValidationError {
    ProjectionGroupSchema::new(
        ProjectionId::first(),
        vec![component; component_count],
        projection_measure_schema(),
    )
    .expect_err("projection boundary must reject")
}

fn render_limit_error(
    output: &mut String,
    error: &IrValidationError,
) -> Result<(), Box<dyn Error>> {
    let IrValidationError::LimitExceeded {
        kind,
        actual,
        maximum,
    } = error
    else {
        return Err(format!("expected limit error, got {error}").into());
    };
    writeln!(output, "ir.kind={kind}")?;
    writeln!(output, "ir.actual={actual}")?;
    writeln!(output, "ir.maximum={maximum}")?;
    Ok(())
}

fn projection_rejection_snapshots() -> Result<String, Box<dyn Error>> {
    let mut output = String::from("format=riffdb-projection-boundary-fixture-v1\n");

    for (name, value_type, ir_type) in [
        (
            "optional-component",
            "optional<i64>",
            ValueType::optional(ValueType::i64()).expect("optional"),
        ),
        (
            "collection-component",
            "list<i64,4>",
            ValueType::list(ValueType::i64(), 4).expect("list"),
        ),
    ] {
        let source = projection_boundary_source(value_type, 1);
        let (_, key_span) = projection_spans(&source)?;
        let error = semantic_rejection(
            name,
            &source,
            CompilerDiagnosticCode::InvalidProjection,
            key_span,
        )?;
        let ir_error = ProjectionGroupComponentSchema::new(ir_type, vec![])
            .expect_err("invalid group component");
        let IrValidationError::InvalidProjection { reason } = ir_error else {
            return Err(format!("{name} produced the wrong IR error").into());
        };
        writeln!(output, "\n[{name}]")?;
        writeln!(output, "declared_type={value_type}")?;
        writeln!(output, "component_count=1")?;
        writeln!(output, "expected=RDB-C019")?;
        writeln!(output, "primary={}..{}", key_span.start(), key_span.end())?;
        writeln!(output, "primary_text=group")?;
        writeln!(output, "ir.reason={reason}")?;
        render_compilation_error(&mut output, &error)?;
    }

    let boolean =
        ProjectionGroupComponentSchema::new(ValueType::bool(), vec![]).expect("bool component");
    let at_limit_source = projection_boundary_source("bool", 1_024);
    let (projection_name_span, _) = projection_spans(&at_limit_source)?;
    let at_limit_error = semantic_rejection(
        "component-count-1024",
        &at_limit_source,
        CompilerDiagnosticCode::BoundExceeded,
        projection_name_span,
    )?;
    let at_limit_ir = projection_schema_error(boolean.clone(), 1_024);
    writeln!(output, "\n[component-count-1024]")?;
    writeln!(output, "component_count=1024")?;
    writeln!(output, "component_count_limit=accepted")?;
    writeln!(output, "expected=RDB-C020")?;
    writeln!(
        output,
        "primary={}..{}",
        projection_name_span.start(),
        projection_name_span.end()
    )?;
    writeln!(output, "primary_text=Totals")?;
    render_limit_error(&mut output, &at_limit_ir)?;
    render_compilation_error(&mut output, &at_limit_error)?;

    let above_limit_source = projection_boundary_source("bool", 1_025);
    let last_component_start = above_limit_source
        .rfind("group")
        .ok_or("1,025-component source has no final component")?;
    let last_component_span = Span::new(last_component_start, last_component_start + "group".len())
        .ok_or("invalid final component span")?;
    let above_limit_error = validate_contract_source(&above_limit_source)
        .expect_err("1,025 source components must reject");
    let syntax = above_limit_error
        .syntax()
        .ok_or("1,025 source components did not reject during parsing")?;
    if syntax.len() != 1
        || syntax.as_slice()[0].code() != SyntaxDiagnosticCode::CollectionLimit
        || syntax.as_slice()[0].span() != last_component_span
    {
        return Err("1,025 source components produced the wrong parser boundary".into());
    }
    let above_limit_ir = projection_schema_error(boolean, 1_025);
    writeln!(output, "\n[component-count-1025]")?;
    writeln!(output, "component_count=1025")?;
    writeln!(output, "expected=RDB-S007")?;
    writeln!(
        output,
        "primary={}..{}",
        last_component_span.start(),
        last_component_span.end()
    )?;
    writeln!(output, "primary_text=group")?;
    render_limit_error(&mut output, &above_limit_ir)?;
    render_compilation_error(&mut output, &above_limit_error)?;

    let exact_source = projection_boundary_source("string<3780>", 1);
    let exact = compile_contract_source(&exact_source)?;
    let exact_maximum = exact.projections()[0]
        .group_schema()
        .maximum_complete_key_bytes();
    if exact_maximum != 4_096 {
        return Err(format!("exact projection key maximum was {exact_maximum}").into());
    }
    writeln!(output, "\n[complete-key-4096]")?;
    writeln!(output, "declared_type=string<3780>")?;
    writeln!(output, "expected=accepted")?;
    writeln!(output, "maximum_complete_key_bytes={exact_maximum}")?;
    writeln!(
        output,
        "projection_plan_hash={}",
        hex(exact.projections()[0].plan_hash().as_bytes())
    )?;

    let above_source = projection_boundary_source("string<3781>", 1);
    let (above_name_span, _) = projection_spans(&above_source)?;
    let above_error = semantic_rejection(
        "complete-key-4097",
        &above_source,
        CompilerDiagnosticCode::BoundExceeded,
        above_name_span,
    )?;
    let above_component =
        ProjectionGroupComponentSchema::new(ValueType::string(3_781).expect("string type"), vec![])
            .expect("component");
    let above_ir = projection_schema_error(above_component, 1);
    writeln!(output, "\n[complete-key-4097]")?;
    writeln!(output, "declared_type=string<3781>")?;
    writeln!(output, "expected=RDB-C020")?;
    writeln!(
        output,
        "primary={}..{}",
        above_name_span.start(),
        above_name_span.end()
    )?;
    writeln!(output, "primary_text=Totals")?;
    render_limit_error(&mut output, &above_ir)?;
    render_compilation_error(&mut output, &above_error)?;
    Ok(output)
}

fn optional_context_snapshot() -> Result<String, Box<dyn Error>> {
    let bundle = compile_contract_source(OPTIONAL_ARITHMETIC_SOURCE)?;
    let command = &bundle.commands()[0];
    let (expression_id, binary) = command
        .expressions()
        .nodes()
        .iter()
        .enumerate()
        .find(|(_, node)| matches!(node.kind(), ExpressionKind::Binary { .. }))
        .ok_or("optional arithmetic fixture has no binary expression")?;
    if binary.result_type() != &ValueType::i64() {
        return Err("optional arithmetic result was not nonoptional i64".into());
    }
    let entity = bundle
        .schema()
        .entity(command.bindings()[0].entity_type())
        .ok_or("optional arithmetic entity is missing")?;
    let destination = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "calculated")
        .ok_or("optional arithmetic destination is missing")?;
    let expected_destination = ValueType::optional(ValueType::i64()).expect("optional");
    if destination.value_type() != &expected_destination {
        return Err("optional arithmetic destination has the wrong type".into());
    }
    let source_start = OPTIONAL_ARITHMETIC_SOURCE
        .find("amount + 1")
        .ok_or("optional arithmetic source expression is missing")?;
    let mut output = String::from("format=riffdb-optional-context-fixture-v1\n");
    writeln!(output, "lineage={}", bundle.lineage().as_str())?;
    writeln!(output, "command_id={}", command.command_id().get())?;
    writeln!(output, "plan_hash={}", hex(command.plan_hash().as_bytes()))?;
    writeln!(output, "source_expression=amount + 1")?;
    writeln!(
        output,
        "source_span={}..{}",
        source_start,
        source_start + "amount + 1".len()
    )?;
    writeln!(output, "expression_id={expression_id}")?;
    writeln!(output, "binary_result=i64")?;
    writeln!(output, "destination=optional<i64>")?;
    writeln!(output, "injection=nonoptional-to-optional")?;
    Ok(output)
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

fn late_bound_snapshot() -> Result<String, Box<dyn Error>> {
    let control_source = amplified_outcome_source(64, 128);
    let control = compile_contract_source(&control_source)?;
    let source = amplified_outcome_source(64, 512);
    let start = source.find("Expand").ok_or("command name is missing")?;
    let span = Span::new(start, start + "Expand".len()).ok_or("invalid command span")?;
    let error = semantic_rejection(
        "amplified-generated-outcome-schema",
        &source,
        CompilerDiagnosticCode::BoundExceeded,
        span,
    )?;
    let mut output = String::from("format=riffdb-late-bound-fixture-v1\n");
    writeln!(output, "entity_fields=64")?;
    writeln!(output, "accepted_outcome_fields=128")?;
    writeln!(
        output,
        "accepted_plan_hash={}",
        hex(control.commands()[0].plan_hash().as_bytes())
    )?;
    writeln!(output, "rejected_outcome_fields=512")?;
    writeln!(output, "rejected_source_bytes={}", source.len())?;
    writeln!(output, "expected=RDB-C020")?;
    writeln!(output, "primary={}..{}", span.start(), span.end())?;
    writeln!(output, "primary_text=Expand")?;
    writeln!(
        output,
        "help={}",
        CompilerDiagnosticCode::BoundExceeded
            .help()
            .expect("bound help")
    )?;
    render_compilation_error(&mut output, &error)?;
    Ok(output)
}

fn diagnostic_snapshots() -> Result<String, Box<dyn Error>> {
    let mut cases = Vec::<(&str, CompilerDiagnosticCode, CompilationError)>::new();
    {
        let mut source_case = |name: &'static str,
                               code: CompilerDiagnosticCode,
                               source: String|
         -> Result<(), Box<dyn Error>> {
            let error = match validate_contract_source(&source) {
                Ok(()) => {
                    return Err(format!("diagnostic fixture {name} unexpectedly compiled").into());
                }
                Err(error) => error,
            };
            require_semantic_code(name, code, &error)?;
            cases.push((name, code, error));
            Ok(())
        };

        source_case(
            "RDB-C001-invalid-contract-version",
            CompilerDiagnosticCode::InvalidContractVersion,
            "contract InvalidVersion version 0 {}".to_owned(),
        )?;
        source_case(
            "RDB-C002-duplicate-name",
            CompilerDiagnosticCode::DuplicateName,
            "contract Duplicate version 1 { enum State { Open, Open } }".to_owned(),
        )?;
        source_case(
            "RDB-C003-missing-declaration",
            CompilerDiagnosticCode::MissingDeclaration,
            "contract MissingKey version 1 { entity Row { field value: i64 } }".to_owned(),
        )?;
        source_case(
            "RDB-C004-unknown-name",
            CompilerDiagnosticCode::UnknownName,
            concat!(
                "contract UnknownChild version 1 { ",
                "entity Root { key (id: uuid) } ",
                "aggregate Family { root Root child Missing partition_by id conflict_key (id) } }",
            )
            .to_owned(),
        )?;
        source_case(
            "RDB-C005-invalid-type",
            CompilerDiagnosticCode::InvalidType,
            "contract InvalidType version 1 { entity Row { key (id: Missing) } }".to_owned(),
        )?;
        source_case(
            "RDB-C006-type-mismatch",
            CompilerDiagnosticCode::TypeMismatch,
            concat!(
                "contract TypeMismatch version 1 { ",
                "entity Row { key (id: uuid) invariant invalid: id == 1 } }",
            )
            .to_owned(),
        )?;
        source_case(
            "RDB-C007-invalid-expression",
            CompilerDiagnosticCode::InvalidExpression,
            concat!(
                "contract Ambiguous version 1 { enum State { Open } ",
                "entity Row { key (id: uuid) field Open: bool } ",
                "aggregate Rows { root Row partition_by id conflict_key (id) } ",
                "command Find { input id: uuid read Row(id) as State else Missing { id: id } ",
                "require check: State.Open else Rejected {} return Found { id: id } } }",
            )
            .to_owned(),
        )?;
        source_case(
        "RDB-C008-invalid-aggregate",
        CompilerDiagnosticCode::InvalidAggregate,
        concat!(
            "contract InvalidAggregate version 1 { ",
            "entity Root { key (tenant: uuid) } entity Child { key (other: uuid) } ",
            "aggregate Family { root Root child Child partition_by tenant conflict_key (tenant) } }",
        )
        .to_owned(),
    )?;
        source_case(
        "RDB-C009-invalid-binding",
        CompilerDiagnosticCode::InvalidBinding,
        concat!(
            "contract UnownedEvent version 1 { ",
            "entity Row { key (id: uuid) } event Changed { id: uuid } ",
            "aggregate Rows { root Row partition_by id conflict_key (id) } ",
            "command Notify { input request: string<128> input id: uuid idempotency_key request ",
            "read Row(id) as row else Missing { id: id } emit Changed { id: id } ",
            "return Notified { row: row } } }",
        )
        .to_owned(),
    )?;
        source_case(
            "RDB-C010-missing-idempotency",
            CompilerDiagnosticCode::MissingIdempotency,
            concat!(
                "contract MissingIdempotency version 1 { ",
                "entity Row { key (id: uuid) field value: i64 } ",
                "aggregate Rows { root Row partition_by id conflict_key (id) } ",
                "command Change { input id: uuid mutate Row(id) as row else Missing { id: id } ",
                "set row.value = 1 return Changed { row: row } } }",
            )
            .to_owned(),
        )?;
        source_case(
        "RDB-C011-invalid-idempotency",
        CompilerDiagnosticCode::InvalidIdempotency,
        concat!(
            "contract SecretLeak version 1 { ",
            "entity Row { key (id: uuid) field value: i64 } event Changed { leaked: string<128> } ",
            "aggregate Rows { root Row partition_by id conflict_key (id) } ",
            "command Change { input idempotency_key: string<128> input id: uuid ",
            "idempotency_key idempotency_key mutate Row(id) as row else Missing { id: id } ",
            "set row.value = 1 emit Changed { leaked: idempotency_key } ",
            "return ChangedOutcome { row: row } } }",
        )
        .to_owned(),
    )?;
        source_case(
        "RDB-C012-invalid-creation",
        CompilerDiagnosticCode::InvalidCreation,
        concat!(
            "contract IncompleteCreate version 1 { ",
            "entity Row { key (id: uuid) field first: i64 field second: i64 } ",
            "aggregate Rows { root Row partition_by id conflict_key (id) } ",
            "command Create { input request: string<128> input id: uuid idempotency_key request ",
            "create Row(id) as row else Exists { id: id } set row.first = 1 ",
            "return Created { row: row } } }",
        )
        .to_owned(),
    )?;
        source_case(
        "RDB-C013-invalid-mutation",
        CompilerDiagnosticCode::InvalidMutation,
        concat!(
            "contract KeyMutation version 1 { entity Row { key (id: uuid) } ",
            "aggregate Rows { root Row partition_by id conflict_key (id) } ",
            "command Change { input request: string<128> input id: uuid idempotency_key request ",
            "mutate Row(id) as row else Missing { id: id } set row.id = id ",
            "return Changed { id: id } } }",
        )
        .to_owned(),
    )?;
        source_case(
            "RDB-C014-invalid-outcome",
            CompilerDiagnosticCode::InvalidOutcome,
            concat!(
                "contract OutcomeCollision version 1 { entity Row { key (id: uuid) } ",
                "aggregate Rows { root Row partition_by id conflict_key (id) } ",
                "command Find { input id: uuid read Row(id) as row else Same { id: id } ",
                "return Same { id: id } } }",
            )
            .to_owned(),
        )?;
        source_case(
        "RDB-C015-invalid-event",
        CompilerDiagnosticCode::InvalidEvent,
        concat!(
            "contract IncompleteEvent version 1 { entity Row { key (id: uuid) } ",
            "event Changed { id: uuid } aggregate Rows { root Row partition_by id conflict_key (id) } ",
            "command Change { input request: string<128> input id: uuid idempotency_key request ",
            "mutate Row(id) as row else Missing { id: id } emit Changed {} ",
            "return Complete { id: id } } }",
        )
        .to_owned(),
    )?;
        source_case(
            "RDB-C016-conflict-not-input-computable",
            CompilerDiagnosticCode::ConflictNotInputComputable,
            concat!(
                "contract NonKeyPartition version 1 { ",
                "entity Row { key (id: uuid) field partition_value: uuid } ",
                "aggregate Rows { root Row partition_by partition_value conflict_key (id) } }",
            )
            .to_owned(),
        )?;
        source_case(
        "RDB-C017-cross-partition-mutation",
        CompilerDiagnosticCode::CrossPartitionMutation,
        concat!(
            "contract CrossPartition version 1 { ",
            "entity Row { key (tenant_id: uuid, row_id: uuid) field value: i64 } ",
            "aggregate Rows { root Row partition_by tenant_id conflict_key (tenant_id, row_id) } ",
            "command Change { input request: string<128> input first_tenant: uuid ",
            "input first_row: uuid input second_tenant: uuid input second_row: uuid ",
            "idempotency_key request mutate Row(first_tenant, first_row) as first else MissingFirst {} ",
            "mutate Row(second_tenant, second_row) as second else MissingSecond {} ",
            "set first.value = 1 set second.value = 1 return Changed { first: first, second: second } } }",
        )
        .to_owned(),
    )?;
        source_case(
            "RDB-C018-invalid-relationship",
            CompilerDiagnosticCode::InvalidRelationship,
            include_str!("../../../fixtures/compiler/relationships/partial-target.riff").to_owned(),
        )?;
        source_case(
        "RDB-C019-invalid-projection",
        CompilerDiagnosticCode::InvalidProjection,
        concat!(
            "contract InvalidProjection version 1 { event Flagged { enabled: bool } ",
            "projection Totals { source event Flagged key (enabled) measure total = sum(enabled) ",
            "frontier transactionally_ordered } }",
        )
        .to_owned(),
    )?;
        source_case(
            "RDB-C020-bound-exceeded",
            CompilerDiagnosticCode::BoundExceeded,
            concat!(
                "contract OversizedKey version 1 { ",
                "entity Row { key (first: string<4096>, second: string<4096>) } }",
            )
            .to_owned(),
        )?;
    }

    let genesis_source = diagnostic_lineage_source(1, true);
    let genesis = compile_contract_source(&genesis_source)?;
    let removed_source = diagnostic_lineage_source(2, false);
    let removed = compile_contract_successor(&removed_source, &genesis)?;
    let resurrected_source = diagnostic_lineage_source(3, true);
    let resurrected = compile_contract_successor(&resurrected_source, &removed)
        .expect_err("tombstoned identity must reject");
    require_semantic_code(
        "RDB-C021-stable-id-allocation",
        CompilerDiagnosticCode::StableIdAllocation,
        &resurrected,
    )?;
    cases.push((
        "RDB-C021-stable-id-allocation",
        CompilerDiagnosticCode::StableIdAllocation,
        resurrected,
    ));

    let same_version = compile_contract_successor(&genesis_source, &genesis)
        .expect_err("same version predecessor must reject");
    require_semantic_code(
        "RDB-C022-invalid-parent",
        CompilerDiagnosticCode::InvalidParent,
        &same_version,
    )?;
    cases.push((
        "RDB-C022-invalid-parent",
        CompilerDiagnosticCode::InvalidParent,
        same_version,
    ));

    let synthetic_span = Span::new(23, 37).expect("synthetic fixture span");
    cases.push((
        "RDB-C023-invalid-ir-synthetic-hir",
        CompilerDiagnosticCode::InvalidIr,
        CompilationError::Semantic(CompilerDiagnostics::single(CompilerDiagnostic::new(
            CompilerDiagnosticCode::InvalidIr,
            synthetic_span,
        ))),
    ));

    {
        let name = "RDB-C024-missing-relationship-read";
        let code = CompilerDiagnosticCode::MissingRelationshipRead;
        let source = include_str!("../../../fixtures/compiler/relationships/dangling-create.riff");
        let error =
            validate_contract_source(source).expect_err("dangling relationship must reject");
        require_semantic_code(name, code, &error)?;
        cases.push((name, code, error));
    }

    for (name, code, source) in [
        (
            "RDB-C025-invalid-unique-key",
            CompilerDiagnosticCode::InvalidUniqueKey,
            concat!(
                "contract InvalidUnique version 1 { ",
                "entity Organization { key (organization_id: uuid) } ",
                "entity User { key (organization_id: uuid, user_id: uuid) ",
                "field email: string<128> unique user_email (email) } ",
                "aggregate OrganizationRoot { root Organization child User ",
                "partition_by organization_id conflict_key (organization_id) } }",
            )
            .to_owned(),
        ),
        (
            "RDB-C026-unique-key-not-input-computable",
            CompilerDiagnosticCode::UniqueKeyNotInputComputable,
            diagnostic_unique_non_input_source(),
        ),
    ] {
        let error = validate_contract_source(&source).expect_err("invalid uniqueness fixture");
        require_semantic_code(name, code, &error)?;
        cases.push((name, code, error));
    }

    for (name, code, source) in [
        (
            "RDB-C201-invalid-command-tool-name",
            CompilerDiagnosticCode::InvalidCommandToolName,
            diagnostic_read_contract("_InvalidTool", "Find"),
        ),
        (
            "RDB-C202-command-tool-name-too-long",
            CompilerDiagnosticCode::CommandToolNameTooLong,
            diagnostic_read_contract(&"C".repeat(60), &"D".repeat(57)),
        ),
        (
            "RDB-C203-command-tool-name-collision",
            CompilerDiagnosticCode::CommandToolNameCollision,
            diagnostic_collision_source(),
        ),
    ] {
        let error = validate_contract_source(&source).expect_err("invalid MCP fixture source");
        require_semantic_code(name, code, &error)?;
        cases.push((name, code, error));
    }

    let expected = CompilerDiagnosticCode::ALL
        .into_iter()
        .collect::<BTreeSet<_>>();
    let covered = cases
        .iter()
        .map(|(_, code, _)| *code)
        .collect::<BTreeSet<_>>();
    if covered != expected || cases.len() != expected.len() {
        return Err(
            "semantic diagnostic fixture coverage is not exactly the retained registry".into(),
        );
    }

    let mut output = String::from("format=riffdb-compiler-diagnostic-fixture-v1\n");
    writeln!(output, "\n[semantic-code-registry]")?;
    writeln!(output, "count={}", CompilerDiagnosticCode::ALL.len())?;
    for (index, code) in CompilerDiagnosticCode::ALL.iter().copied().enumerate() {
        writeln!(output, "code.{index}.id={}", code.as_str())?;
        writeln!(output, "code.{index}.message={}", code.summary())?;
        writeln!(
            output,
            "code.{index}.help={}",
            code.help().unwrap_or("none")
        )?;
    }
    for (name, expected, error) in cases {
        writeln!(output, "\n[{name}]")?;
        writeln!(output, "expected={}", expected.as_str())?;
        render_compilation_error(&mut output, &error)?;
    }

    let syntax = validate_contract_source("not a contract").expect_err("syntax fixture rejects");
    writeln!(output, "\n[syntax]")?;
    render_compilation_error(&mut output, &syntax)?;
    let indexed_range = concat!(
        "contract Invalid version 1 { entity Row { key (tenant_id: uuid, row_id: uuid) ",
        "field status: string<16> field value: i64 index by_status (tenant_id, status) } ",
        "aggregate Rows { root Row partition_by tenant_id conflict_key (tenant_id, row_id) } ",
        "command Change { input request: string<128> input tenant_id: uuid input row_id: uuid ",
        "input status: string<16> idempotency_key request ",
        "read Row by by_status(tenant_id, status) as matches else MissingRange {} ",
        "mutate Row(tenant_id, row_id) as target else MissingTarget {} ",
        "set target.value = matches.value return Changed { row: target } } }",
    );
    let indexed_range = validate_contract_source(indexed_range)
        .expect_err("write-influencing indexed range grammar must reject");
    writeln!(output, "\n[write-influencing-indexed-range-read]")?;
    render_compilation_error(&mut output, &indexed_range)?;
    Ok(output)
}

fn require_semantic_code(
    name: &str,
    expected: CompilerDiagnosticCode,
    error: &CompilationError,
) -> Result<(), Box<dyn Error>> {
    if error.semantic().is_some_and(|diagnostics| {
        diagnostics
            .as_slice()
            .iter()
            .any(|diagnostic| diagnostic.code() == expected)
    }) {
        Ok(())
    } else {
        Err(format!(
            "diagnostic fixture {name} did not emit {}",
            expected.as_str()
        )
        .into())
    }
}

fn diagnostic_lineage_source(version: u64, include_command: bool) -> String {
    let command = if include_command {
        "command Find { input id: uuid read Row(id) as row else Missing { id: id } \
         return Found { id: id } }"
    } else {
        ""
    };
    format!(
        "contract DiagnosticLineage version {version} {{ entity Row {{ key (id: uuid) }} \
         aggregate Rows {{ root Row partition_by id conflict_key (id) }} {command} }}"
    )
}

fn diagnostic_read_contract(contract: &str, command: &str) -> String {
    format!(
        "contract {contract} version 1 {{ entity Row {{ key (id: uuid) }} \
         aggregate Rows {{ root Row partition_by id conflict_key (id) }} \
         command {command} {{ input id: uuid read Row(id) as row else Missing {{ id: id }} \
         return Found {{ id: id }} }} }}"
    )
}

fn diagnostic_collision_source() -> String {
    concat!(
        "contract ToolCollision version 1 { entity Row { key (id: uuid) } ",
        "aggregate Rows { root Row partition_by id conflict_key (id) } ",
        "command Allocate { input id: uuid read Row(id) as row else MissingOne { id: id } ",
        "return FoundOne { id: id } } ",
        "command ALLOCATE { input id: uuid read Row(id) as row else MissingTwo { id: id } ",
        "return FoundTwo { id: id } } }",
    )
    .to_owned()
}

fn diagnostic_unique_non_input_source() -> String {
    concat!(
        "contract InvalidUniqueChange version 1 { ",
        "entity Organization { key (organization_id: uuid) } ",
        "entity User { key (organization_id: uuid, user_id: uuid) ",
        "field email: string<128> unique user_email (organization_id, email) } ",
        "aggregate OrganizationRoot { root Organization child User ",
        "partition_by organization_id conflict_key (organization_id) } ",
        "command ChangeEmail { input request: string<128> input organization_id: uuid ",
        "input user_id: uuid idempotency_key request ",
        "mutate User(organization_id, user_id) as user else UserMissing {} ",
        "set user.email = user.email return Changed { user: user } } }",
    )
    .to_owned()
}

fn render_compilation_error(
    output: &mut String,
    error: &CompilationError,
) -> Result<(), std::fmt::Error> {
    if let Some(diagnostics) = error.syntax() {
        writeln!(output, "kind=syntax")?;
        writeln!(output, "count={}", diagnostics.len())?;
        for (index, diagnostic) in diagnostics.as_slice().iter().enumerate() {
            let span = diagnostic.span();
            writeln!(
                output,
                "diagnostic.{index}.code={}",
                diagnostic.code().as_str()
            )?;
            writeln!(
                output,
                "diagnostic.{index}.message={}",
                diagnostic.code().summary()
            )?;
            writeln!(
                output,
                "diagnostic.{index}.primary={}..{}",
                span.start(),
                span.end()
            )?;
            writeln!(
                output,
                "diagnostic.{index}.expected={}",
                diagnostic.expected().join(",")
            )?;
            writeln!(
                output,
                "diagnostic.{index}.help={}",
                diagnostic.code().help().unwrap_or("none")
            )?;
        }
    } else if let Some(diagnostics) = error.semantic() {
        writeln!(output, "kind=semantic")?;
        writeln!(output, "count={}", diagnostics.len())?;
        for (index, diagnostic) in diagnostics.as_slice().iter().enumerate() {
            let span = diagnostic.primary_span();
            writeln!(
                output,
                "diagnostic.{index}.code={}",
                diagnostic.code().as_str()
            )?;
            writeln!(
                output,
                "diagnostic.{index}.message={}",
                diagnostic.code().summary()
            )?;
            writeln!(
                output,
                "diagnostic.{index}.primary={}..{}",
                span.start(),
                span.end()
            )?;
            match diagnostic.related_span() {
                Some(related) => writeln!(
                    output,
                    "diagnostic.{index}.related={}..{}",
                    related.start(),
                    related.end()
                )?,
                None => writeln!(output, "diagnostic.{index}.related=none")?,
            }
            writeln!(
                output,
                "diagnostic.{index}.help={}",
                diagnostic.code().help().unwrap_or("none")
            )?;
        }
    }
    Ok(())
}

fn optional_id(value: Option<u32>) -> String {
    value.map_or_else(|| "none".to_owned(), |value| value.to_string())
}

fn ids(values: impl IntoIterator<Item = u32>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn pairs(values: impl IntoIterator<Item = (u32, u32)>) -> String {
    values
        .into_iter()
        .map(|(left, right)| format!("{left}:{right}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}
