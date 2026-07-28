#![forbid(unsafe_code)]

//! Fixture-scoped renderer for the canonical POC Rust command bindings.
//!
//! `scripts/generate-contract-fixtures --check` is the prerequisite that
//! validates these text fixtures against the typed compiler and `bundle.bin`.
//! This dependency-free client-side renderer validates only the identities and
//! shapes it consumes; it does not decode or independently validate the bundle.

const TEMPLATE: &str = include_str!("legal_spend.rs.in");
const BUNDLE_METADATA: &str = include_str!("../../../fixtures/compiler/bundle-metadata.txt");
const COMMAND_PLANS: &str = include_str!("../../../fixtures/compiler/command-plans.txt");
const LINEAGE_LEDGER: &str = include_str!("../../../fixtures/compiler/lineage-ledger.txt");

const CREATE_INPUTS: &[&str] = &[
    "input.field.1=name:fiscal_year type:i64",
    "input.field.2=name:approved_amount type:decimal<28,2>",
    "input.field.3=name:idempotency_key type:string<128>",
    "input.field.4=name:organization_id type:uuid",
];
const CREATE_OUTCOMES: &[&str] = &[
    "outcome=1 name=BudgetCreated owner=command-outcome:1:1",
    "outcome.1.field_count=1",
    "outcome.1.field.1=name:budget type:record<entity:1>",
    "outcome=2 name=BudgetAlreadyExists owner=command-outcome:1:2",
    "outcome.2.field_count=2",
    "outcome.2.field.1=name:fiscal_year type:i64",
    "outcome.2.field.2=name:organization_id type:uuid",
    "outcome=3 name=InvalidApprovedAmount owner=command-outcome:1:3",
    "outcome.3.field_count=1",
    "outcome.3.field.1=name:minimum type:decimal<2,2>",
];
const CREATE_SEMANTICS: &[&str] = &[
    "expression.0=type:uuid kind:input_field:4",
    "expression.1=type:i64 kind:input_field:1",
    "expression.8=type:decimal<28,2> kind:input_field:2",
    "expression.9=type:decimal<28,2> kind:constant:01041c0200000000000000000000000000000000",
    "expression.10=type:timestamp kind:transaction_time",
    "expression.11=type:record<entity:1> kind:complete_binding:0",
    "binding=0 name=budget mode=create entity=1 keys=0,1 reads=3,5 complete=true failure=owner:command-outcome:1:2 fields:[1:3,2:2]",
    "instruction.1=set binding:0 field:3 value:8",
    "instruction.2=set binding:0 field:5 value:9",
    "instruction.3=set binding:0 field:1 value:10",
    "instruction.4=return outcome:1 payload:owner:command-outcome:1:1 fields:[1:11]",
];
const ALLOCATE_INPUTS: &[&str] = &[
    "input.field.1=name:amount type:decimal<28,2>",
    "input.field.2=name:matter_id type:uuid",
    "input.field.3=name:fiscal_year type:i64",
    "input.field.4=name:idempotency_key type:string<128>",
    "input.field.5=name:organization_id type:uuid",
];
const ALLOCATE_OUTCOMES: &[&str] = &[
    "outcome=1 name=Allocated owner=command-outcome:2:1",
    "outcome.1.field_count=2",
    "outcome.1.field.1=name:budget type:record<entity:1>",
    "outcome.1.field.2=name:remaining type:decimal<28,2>",
    "outcome=2 name=InvalidAmount owner=command-outcome:2:2",
    "outcome.2.field_count=1",
    "outcome.2.field.1=name:minimum type:decimal<2,2>",
    "outcome=3 name=BudgetNotFound owner=command-outcome:2:3",
    "outcome.3.field_count=2",
    "outcome.3.field.1=name:fiscal_year type:i64",
    "outcome.3.field.2=name:organization_id type:uuid",
    "outcome=4 name=InsufficientBudget owner=command-outcome:2:4",
    "outcome.4.field_count=3",
    "outcome.4.field.1=name:approved type:decimal<28,2>",
    "outcome.4.field.2=name:allocated type:decimal<28,2>",
    "outcome.4.field.3=name:requested type:decimal<28,2>",
];
const ALLOCATE_SEMANTICS: &[&str] = &[
    "expression.0=type:uuid kind:input_field:5",
    "expression.1=type:i64 kind:input_field:3",
    "expression.18=type:decimal<28,2> kind:binary:add:16:17",
    "expression.19=type:timestamp kind:transaction_time",
    "expression.24=type:record<entity:1> kind:complete_binding:0",
    "expression.27=type:decimal<28,2> kind:binary:subtract:25:26",
    "binding=0 name=budget mode=mutate entity=1 keys=0,1 reads=3,5 complete=true failure=owner:command-outcome:2:3 fields:[1:3,2:2]",
    "instruction.2=set binding:0 field:5 value:18",
    "instruction.3=set binding:0 field:1 value:19",
    "instruction.5=return outcome:1 payload:owner:command-outcome:2:1 fields:[1:24,2:27]",
];

/// The checked compiler text artifacts that drive this fixture-scoped renderer.
#[derive(Clone, Copy)]
pub(crate) struct FixtureSet<'fixture> {
    pub(crate) bundle_metadata: &'fixture str,
    pub(crate) command_plans: &'fixture str,
    pub(crate) lineage_ledger: &'fixture str,
}

/// Returns the checked-in compiler fixture chain.
#[must_use]
pub(crate) const fn checked_fixtures() -> FixtureSet<'static> {
    FixtureSet {
        bundle_metadata: BUNDLE_METADATA,
        command_plans: COMMAND_PLANS,
        lineage_ledger: LINEAGE_LEDGER,
    }
}

/// Renders the canonical POC Rust bindings from checked-in compiler fixtures.
pub(crate) fn generated_source() -> Result<String, String> {
    generate_from(checked_fixtures())
}

/// Renders bindings from an explicit fixture set.
///
/// This entry point exists so currentness tests can prove fixture changes drive
/// output and unsupported semantic-shape changes fail closed.
pub(crate) fn generate_from(fixtures: FixtureSet<'_>) -> Result<String, String> {
    let metadata = validate_metadata(fixtures.bundle_metadata)?;
    let plans = validate_plans(fixtures.command_plans, metadata.contract_version)?;
    validate_lineage_ledger(fixtures.lineage_ledger)?;

    let mut output = TEMPLATE.to_owned();
    replace_once(
        &mut output,
        "{{CONTRACT_LINEAGE_LITERAL}}",
        &rust_string_literal(metadata.lineage),
    )?;
    replace_once(
        &mut output,
        "{{CONTRACT_VERSION}}",
        &metadata.contract_version.to_string(),
    )?;
    replace_once(
        &mut output,
        "{{CREATE_BUDGET_PLAN_HASH_BYTES}}",
        &rust_hash_bytes(plans.create_hash)?,
    )?;
    replace_once(
        &mut output,
        "{{ALLOCATE_BUDGET_PLAN_HASH_BYTES}}",
        &rust_hash_bytes(plans.allocate_hash)?,
    )?;
    if output.contains("{{") || output.contains("}}") {
        return Err("unresolved Rust SDK template token".to_owned());
    }
    Ok(output)
}

#[derive(Clone, Copy)]
struct Metadata<'fixture> {
    lineage: &'fixture str,
    contract_version: u64,
}

fn validate_metadata(document: &str) -> Result<Metadata<'_>, String> {
    require_value(document, "bundle_format", "1", "bundle metadata")?;
    require_value(document, "grammar", "1", "bundle metadata")?;
    require_value(document, "executable_ir", "1", "bundle metadata")?;
    require_value(document, "compiler", "0.1.0", "bundle metadata")?;
    require_value(document, "parent", "none", "bundle metadata")?;
    require_value(document, "commands", "2", "bundle metadata")?;
    require_value(document, "schema_artifacts", "7", "bundle metadata")?;

    for key in ["source_hash", "plan_root_hash", "bundle_hash"] {
        validate_lower_hex(required_value(document, key, "bundle metadata")?, 32)
            .map_err(|error| format!("invalid bundle metadata {key}: {error}"))?;
    }

    let lineage = required_value(document, "lineage", "bundle metadata")?;
    if lineage != "LegalSpend" {
        return Err(format!(
            "unsupported canonical contract lineage {lineage:?}; expected \"LegalSpend\""
        ));
    }
    let contract_version = required_value(document, "contract_version", "bundle metadata")?
        .parse::<u64>()
        .map_err(|_| "bundle metadata contract_version is not a u64".to_owned())?;
    if contract_version != 1 {
        return Err(format!(
            "unsupported canonical contract version {contract_version}; expected 1"
        ));
    }

    Ok(Metadata {
        lineage,
        contract_version,
    })
}

#[derive(Clone, Copy)]
struct PlanHashes<'fixture> {
    create_hash: &'fixture str,
    allocate_hash: &'fixture str,
}

fn validate_plans(document: &str, contract_version: u64) -> Result<PlanHashes<'_>, String> {
    require_value(
        document,
        "format",
        "riffdb-command-plan-fixture-v1",
        "command plan fixture",
    )?;
    let command_headers = document
        .lines()
        .filter(|line| line.starts_with("[command "))
        .count();
    if command_headers != 2 {
        return Err(format!(
            "command plan fixture contains {command_headers} command blocks; expected 2"
        ));
    }

    let create = command_block(document, 1)?;
    let allocate = command_block(document, 2)?;
    validate_command(
        create,
        1,
        "CreateBudget",
        contract_version,
        3,
        1,
        CREATE_INPUTS,
        CREATE_OUTCOMES,
        CREATE_SEMANTICS,
    )?;
    validate_command(
        allocate,
        2,
        "AllocateBudget",
        contract_version,
        4,
        1,
        ALLOCATE_INPUTS,
        ALLOCATE_OUTCOMES,
        ALLOCATE_SEMANTICS,
    )?;

    let create_hash = required_value(create, "plan_hash", "CreateBudget plan")?;
    validate_lower_hex(create_hash, 32)
        .map_err(|error| format!("invalid CreateBudget plan_hash: {error}"))?;
    let allocate_hash = required_value(allocate, "plan_hash", "AllocateBudget plan")?;
    validate_lower_hex(allocate_hash, 32)
        .map_err(|error| format!("invalid AllocateBudget plan_hash: {error}"))?;

    Ok(PlanHashes {
        create_hash,
        allocate_hash,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_command(
    block: &str,
    command_id: u32,
    name: &str,
    contract_version: u64,
    idempotency_input: u32,
    success_outcome: u32,
    inputs: &[&str],
    outcomes: &[&str],
    semantic_lines: &[&str],
) -> Result<(), String> {
    let context = format!("{name} plan");
    require_value(block, "name", name, &context)?;
    require_value(
        block,
        "contract_version",
        &contract_version.to_string(),
        &context,
    )?;
    require_value(block, "execution", "idempotent-mutation", &context)?;
    require_value(block, "retry", "bounded-full-reevaluation", &context)?;
    require_value(
        block,
        "capability",
        &format!("invoke-command:LegalSpend:{command_id}"),
        &context,
    )?;
    require_value(
        block,
        "input_owner",
        &format!("command-input:{command_id}"),
        &context,
    )?;
    require_value(
        block,
        "idempotency_input",
        &idempotency_input.to_string(),
        &context,
    )?;
    require_value(
        block,
        "success_outcome",
        &success_outcome.to_string(),
        &context,
    )?;
    require_value(
        block,
        "input.field_count",
        &inputs.len().to_string(),
        &context,
    )?;
    require_value(
        block,
        "outcome_count",
        &outcomes
            .iter()
            .filter(|line| line.starts_with("outcome="))
            .count()
            .to_string(),
        &context,
    )?;
    require_exact_lines(block, inputs, &context)?;
    require_exact_lines(block, outcomes, &context)?;
    require_exact_lines(block, semantic_lines, &context)?;

    let actual_inputs = block
        .lines()
        .filter(|line| line.starts_with("input.field."))
        .count();
    if actual_inputs != inputs.len() {
        return Err(format!(
            "{context} contains {actual_inputs} input field entries; expected {}",
            inputs.len()
        ));
    }
    let actual_outcomes = block
        .lines()
        .filter(|line| line.starts_with("outcome="))
        .count();
    let expected_outcomes = outcomes
        .iter()
        .filter(|line| line.starts_with("outcome="))
        .count();
    if actual_outcomes != expected_outcomes {
        return Err(format!(
            "{context} contains {actual_outcomes} outcomes; expected {expected_outcomes}"
        ));
    }
    let actual_outcome_details = block
        .lines()
        .filter(|line| line.starts_with("outcome."))
        .count();
    let expected_outcome_details = outcomes
        .iter()
        .filter(|line| line.starts_with("outcome."))
        .count();
    if actual_outcome_details != expected_outcome_details {
        return Err(format!(
            "{context} contains {actual_outcome_details} outcome detail entries; expected {expected_outcome_details}"
        ));
    }
    Ok(())
}

fn validate_lineage_ledger(document: &str) -> Result<(), String> {
    require_value(
        document,
        "format",
        "riffdb-lineage-ledger-fixture-v1",
        "lineage ledger",
    )?;
    require_value(document, "version", "1", "lineage ledger")?;
    require_value(document, "allocation_count", "22", "lineage ledger")?;

    validate_ledger_block(
        document,
        0,
        "allocation.0=tag:entity owner_kind:0x00 owners: max_allocated:1 entries:1",
        &["entry id:1 state:active identity_tag:entity owner_kind:0x00 owners: name:Budget"],
    )?;
    validate_ledger_block(
        document,
        4,
        "allocation.4=tag:command owner_kind:0x00 owners: max_allocated:2 entries:2",
        &[
            "entry id:1 state:active identity_tag:command owner_kind:0x00 owners: name:CreateBudget",
            "entry id:2 state:active identity_tag:command owner_kind:0x00 owners: name:AllocateBudget",
        ],
    )?;
    validate_ledger_block(
        document,
        8,
        "allocation.8=tag:field owner_kind:0x01 owners:1 max_allocated:5 entries:5",
        &[
            "entry id:1 state:active identity_tag:field owner_kind:0x01 owners:1 name:updated_at",
            "entry id:2 state:active identity_tag:field owner_kind:0x01 owners:1 name:fiscal_year",
            "entry id:3 state:active identity_tag:field owner_kind:0x01 owners:1 name:approved_amount",
            "entry id:4 state:active identity_tag:field owner_kind:0x01 owners:1 name:organization_id",
            "entry id:5 state:active identity_tag:field owner_kind:0x01 owners:1 name:allocated_amount",
        ],
    )?;
    validate_ledger_block(
        document,
        10,
        "allocation.10=tag:field owner_kind:0x03 owners:1 max_allocated:4 entries:4",
        &[
            "entry id:1 state:active identity_tag:field owner_kind:0x03 owners:1 name:fiscal_year",
            "entry id:2 state:active identity_tag:field owner_kind:0x03 owners:1 name:approved_amount",
            "entry id:3 state:active identity_tag:field owner_kind:0x03 owners:1 name:idempotency_key",
            "entry id:4 state:active identity_tag:field owner_kind:0x03 owners:1 name:organization_id",
        ],
    )?;
    validate_ledger_block(
        document,
        11,
        "allocation.11=tag:field owner_kind:0x03 owners:2 max_allocated:5 entries:5",
        &[
            "entry id:1 state:active identity_tag:field owner_kind:0x03 owners:2 name:amount",
            "entry id:2 state:active identity_tag:field owner_kind:0x03 owners:2 name:matter_id",
            "entry id:3 state:active identity_tag:field owner_kind:0x03 owners:2 name:fiscal_year",
            "entry id:4 state:active identity_tag:field owner_kind:0x03 owners:2 name:idempotency_key",
            "entry id:5 state:active identity_tag:field owner_kind:0x03 owners:2 name:organization_id",
        ],
    )?;

    const OUTCOME_FIELD_BLOCKS: &[(usize, &str, &[&str])] = &[
        (
            12,
            "allocation.12=tag:field owner_kind:0x04 owners:1,1 max_allocated:1 entries:1",
            &["entry id:1 state:active identity_tag:field owner_kind:0x04 owners:1,1 name:budget"],
        ),
        (
            13,
            "allocation.13=tag:field owner_kind:0x04 owners:1,2 max_allocated:2 entries:2",
            &[
                "entry id:1 state:active identity_tag:field owner_kind:0x04 owners:1,2 name:fiscal_year",
                "entry id:2 state:active identity_tag:field owner_kind:0x04 owners:1,2 name:organization_id",
            ],
        ),
        (
            14,
            "allocation.14=tag:field owner_kind:0x04 owners:1,3 max_allocated:1 entries:1",
            &["entry id:1 state:active identity_tag:field owner_kind:0x04 owners:1,3 name:minimum"],
        ),
        (
            15,
            "allocation.15=tag:field owner_kind:0x04 owners:2,1 max_allocated:2 entries:2",
            &[
                "entry id:1 state:active identity_tag:field owner_kind:0x04 owners:2,1 name:budget",
                "entry id:2 state:active identity_tag:field owner_kind:0x04 owners:2,1 name:remaining",
            ],
        ),
        (
            16,
            "allocation.16=tag:field owner_kind:0x04 owners:2,2 max_allocated:1 entries:1",
            &["entry id:1 state:active identity_tag:field owner_kind:0x04 owners:2,2 name:minimum"],
        ),
        (
            17,
            "allocation.17=tag:field owner_kind:0x04 owners:2,3 max_allocated:2 entries:2",
            &[
                "entry id:1 state:active identity_tag:field owner_kind:0x04 owners:2,3 name:fiscal_year",
                "entry id:2 state:active identity_tag:field owner_kind:0x04 owners:2,3 name:organization_id",
            ],
        ),
        (
            18,
            "allocation.18=tag:field owner_kind:0x04 owners:2,4 max_allocated:3 entries:3",
            &[
                "entry id:1 state:active identity_tag:field owner_kind:0x04 owners:2,4 name:approved",
                "entry id:2 state:active identity_tag:field owner_kind:0x04 owners:2,4 name:allocated",
                "entry id:3 state:active identity_tag:field owner_kind:0x04 owners:2,4 name:requested",
            ],
        ),
    ];
    for &(index, header, entries) in OUTCOME_FIELD_BLOCKS {
        validate_ledger_block(document, index, header, entries)?;
    }

    validate_ledger_block(
        document,
        20,
        "allocation.20=tag:outcome owner_kind:0x01 owners:1 max_allocated:3 entries:3",
        &[
            "entry id:1 state:active identity_tag:outcome owner_kind:0x01 owners:1 name:BudgetCreated",
            "entry id:2 state:active identity_tag:outcome owner_kind:0x01 owners:1 name:BudgetAlreadyExists",
            "entry id:3 state:active identity_tag:outcome owner_kind:0x01 owners:1 name:InvalidApprovedAmount",
        ],
    )?;
    validate_ledger_block(
        document,
        21,
        "allocation.21=tag:outcome owner_kind:0x01 owners:2 max_allocated:4 entries:4",
        &[
            "entry id:1 state:active identity_tag:outcome owner_kind:0x01 owners:2 name:Allocated",
            "entry id:2 state:active identity_tag:outcome owner_kind:0x01 owners:2 name:InvalidAmount",
            "entry id:3 state:active identity_tag:outcome owner_kind:0x01 owners:2 name:BudgetNotFound",
            "entry id:4 state:active identity_tag:outcome owner_kind:0x01 owners:2 name:InsufficientBudget",
        ],
    )
}

fn validate_ledger_block(
    document: &str,
    index: usize,
    expected_header: &str,
    expected_entries: &[&str],
) -> Result<(), String> {
    let marker = format!("allocation.{index}=");
    let mut lines = document
        .lines()
        .skip_while(|line| !line.starts_with(&marker));
    let header = lines
        .next()
        .ok_or_else(|| format!("lineage ledger is missing allocation {index}"))?;
    if header != expected_header {
        return Err(format!(
            "lineage ledger allocation {index} header changed: {header:?}"
        ));
    }
    let actual_entries = lines
        .take_while(|line| !line.starts_with("allocation."))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if actual_entries != expected_entries {
        return Err(format!("lineage ledger allocation {index} entries changed"));
    }
    Ok(())
}

fn command_block(document: &str, index: usize) -> Result<&str, String> {
    let marker = format!("[command {index}]\n");
    let start = document
        .find(&marker)
        .ok_or_else(|| format!("command plan fixture is missing command block {index}"))?
        + marker.len();
    let remainder = &document[start..];
    let end = remainder.find("\n[command ").unwrap_or(remainder.len());
    Ok(&remainder[..end])
}

fn required_value<'document>(
    document: &'document str,
    key: &str,
    context: &str,
) -> Result<&'document str, String> {
    let mut values = document.lines().filter_map(|line| {
        let (candidate, value) = line.split_once('=')?;
        (candidate == key).then_some(value)
    });
    let value = values
        .next()
        .ok_or_else(|| format!("{context} is missing {key}"))?;
    if values.next().is_some() {
        return Err(format!("{context} contains duplicate {key}"));
    }
    Ok(value)
}

fn require_value(document: &str, key: &str, expected: &str, context: &str) -> Result<(), String> {
    let actual = required_value(document, key, context)?;
    if actual != expected {
        return Err(format!(
            "{context} {key} changed: expected {expected:?}, found {actual:?}"
        ));
    }
    Ok(())
}

fn require_exact_lines(document: &str, expected: &[&str], context: &str) -> Result<(), String> {
    for expected_line in expected {
        let count = document
            .lines()
            .filter(|line| line == expected_line)
            .count();
        if count != 1 {
            return Err(format!(
                "{context} expected exactly one line {expected_line:?}, found {count}"
            ));
        }
    }
    Ok(())
}

fn validate_lower_hex(value: &str, bytes: usize) -> Result<(), String> {
    if value.len() != bytes * 2 {
        return Err(format!(
            "expected {} lowercase hexadecimal characters, found {}",
            bytes * 2,
            value.len()
        ));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("value is not lowercase hexadecimal".to_owned());
    }
    Ok(())
}

fn rust_hash_bytes(value: &str) -> Result<String, String> {
    validate_lower_hex(value, 32)?;
    let mut output = String::new();
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        if index % 16 == 0 {
            output.push_str("    ");
        }
        let text = std::str::from_utf8(pair).map_err(|_| "non-UTF-8 hash".to_owned())?;
        output.push_str("0x");
        output.push_str(text);
        output.push(',');
        if index % 16 == 15 {
            output.push('\n');
        } else {
            output.push(' ');
        }
    }
    output.pop();
    Ok(output)
}

fn rust_string_literal(value: &str) -> String {
    let mut output = String::from("\"");
    for character in value.chars() {
        for escaped in character.escape_default() {
            output.push(escaped);
        }
    }
    output.push('"');
    output
}

fn replace_once(output: &mut String, token: &str, replacement: &str) -> Result<(), String> {
    let count = output.matches(token).count();
    if count != 1 {
        return Err(format!(
            "Rust SDK template contains {count} copies of token {token}"
        ));
    }
    *output = output.replacen(token, replacement, 1);
    Ok(())
}
