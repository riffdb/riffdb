//! Canonical JSON fixture encoding and bounded workload decoding.

use crate::{
    AllocateBudget, Amount, BudgetKey, BudgetOperation, BudgetOutcome, BudgetState, BudgetWorkload,
    CommandObservation, ContentionObservation, ContentionWorkload, CreateBudget, GuaranteeProfile,
    MAX_WORKLOAD_OPERATIONS, MatterId, OperationId, OrganizationId, SequentialObservation,
    SequentialWorkload, WorkloadIdempotencyKey,
};
use serde_json::{Map, Value, json};
use std::error::Error;
use std::fmt;

/// Maximum accepted JSON fixture source size.
pub const MAX_FIXTURE_BYTES: usize = 1_048_576;

/// Renders the one canonical pretty-printed workload representation.
pub fn render_workload_fixture(workload: &BudgetWorkload) -> String {
    render_value(&json!({
        "contention": contention_workload_value(&workload.contention),
        "schema_version": workload.schema_version,
        "sequential": sequential_workload_value(&workload.sequential),
    }))
}

/// Parses only canonical, bounded version-one workload JSON.
pub fn parse_workload_fixture(source: &str) -> Result<BudgetWorkload, FixtureJsonError> {
    if source.len() > MAX_FIXTURE_BYTES {
        return Err(FixtureJsonError::SourceTooLarge);
    }
    let value: Value = serde_json::from_str(source).map_err(|_| FixtureJsonError::Malformed)?;
    let root = as_object(&value, "root")?;
    exact_keys(root, &["contention", "schema_version", "sequential"])?;
    let schema_version = as_u32(field(root, "schema_version")?)?;
    if schema_version != 1 {
        return Err(FixtureJsonError::UnsupportedVersion);
    }
    let workload = BudgetWorkload {
        schema_version,
        sequential: parse_sequential_workload(field(root, "sequential")?)?,
        contention: parse_contention_workload(field(root, "contention")?)?,
    };
    if render_workload_fixture(&workload) != source {
        return Err(FixtureJsonError::NonCanonical);
    }
    Ok(workload)
}

/// Renders a canonical sequential observation fixture.
pub fn render_sequential_observation(observation: &SequentialObservation) -> String {
    render_value(&json!({
        "case_id": observation.case_id.as_str(),
        "final_budgets": observation.final_budgets.iter().map(budget_value).collect::<Vec<_>>(),
        "outcomes": observation.outcomes.iter().map(command_observation_value).collect::<Vec<_>>(),
        "schema_version": 1,
    }))
}

/// Renders a canonical contention observation fixture.
pub fn render_contention_observation(observation: &ContentionObservation) -> String {
    render_value(&json!({
        "case_id": observation.case_id.as_str(),
        "final_budget": observation.final_budget.as_ref().map_or(Value::Null, budget_value),
        "outcomes": observation.outcomes.iter().map(outcome_value).collect::<Vec<_>>(),
        "schema_version": 1,
    }))
}

/// Renders a canonical semantic guarantee profile.
pub fn render_guarantee_profile(profile: &GuaranteeProfile) -> String {
    render_value(&json!({
        "adapter": profile.adapter,
        "assumptions": profile.assumptions,
        "authorization": profile.authorization.as_str(),
        "command_invariants": profile.command_invariants.as_str(),
        "conflict_control": profile.conflict_control,
        "durability": profile.durability,
        "durable_events": profile.durable_events.as_str(),
        "idempotency": profile.idempotency.as_str(),
        "isolation": profile.isolation,
        "outbox": profile.outbox.as_str(),
        "projections": profile.projections.as_str(),
        "provenance": profile.provenance.as_str(),
        "schema_version": 1,
        "timestamp_observation": profile.timestamp_observation,
    }))
}

fn render_value(value: &Value) -> String {
    let mut output = match serde_json::to_string_pretty(value) {
        Ok(output) => output,
        Err(_) => unreachable!("serde_json::Value has no fallible JSON representation"),
    };
    output.push('\n');
    output
}

fn sequential_workload_value(workload: &SequentialWorkload) -> Value {
    json!({
        "case_id": workload.case_id.as_str(),
        "operations": workload.operations.iter().map(operation_value).collect::<Vec<_>>(),
    })
}

fn contention_workload_value(workload: &ContentionWorkload) -> Value {
    json!({
        "case_id": workload.case_id.as_str(),
        "contenders": workload.contenders.iter().map(allocate_value).collect::<Vec<_>>(),
        "seed": create_value(&workload.seed),
    })
}

fn operation_value(operation: &BudgetOperation) -> Value {
    match operation {
        BudgetOperation::Create(command) => create_value(command),
        BudgetOperation::Allocate(command) => allocate_value(command),
    }
}

fn create_value(command: &CreateBudget) -> Value {
    json!({
        "approved_amount": command.approved_amount.to_string(),
        "command": "CreateBudget",
        "fiscal_year": command.key.fiscal_year,
        "idempotency_key": command.idempotency_key.as_str(),
        "operation_id": command.operation_id.as_str(),
        "organization_id": command.key.organization_id.to_string(),
    })
}

fn allocate_value(command: &AllocateBudget) -> Value {
    json!({
        "amount": command.amount.to_string(),
        "command": "AllocateBudget",
        "fiscal_year": command.key.fiscal_year,
        "idempotency_key": command.idempotency_key.as_str(),
        "matter_id": command.matter_id.to_string(),
        "operation_id": command.operation_id.as_str(),
        "organization_id": command.key.organization_id.to_string(),
    })
}

fn command_observation_value(observation: &CommandObservation) -> Value {
    json!({
        "operation_id": observation.operation_id.as_str(),
        "outcome": outcome_value(&observation.outcome),
    })
}

fn budget_value(budget: &BudgetState) -> Value {
    json!({
        "allocated_amount": budget.allocated_amount.to_string(),
        "approved_amount": budget.approved_amount.to_string(),
        "fiscal_year": budget.key.fiscal_year,
        "organization_id": budget.key.organization_id.to_string(),
    })
}

fn key_value(key: BudgetKey) -> Value {
    json!({
        "fiscal_year": key.fiscal_year,
        "organization_id": key.organization_id.to_string(),
    })
}

fn outcome_value(outcome: &BudgetOutcome) -> Value {
    match outcome {
        BudgetOutcome::BudgetCreated { budget } => json!({
            "budget": budget_value(budget),
            "name": "BudgetCreated",
        }),
        BudgetOutcome::BudgetAlreadyExists { key } => json!({
            "key": key_value(*key),
            "name": "BudgetAlreadyExists",
        }),
        BudgetOutcome::InvalidApprovedAmount { minimum } => json!({
            "minimum": minimum.to_string(),
            "name": "InvalidApprovedAmount",
        }),
        BudgetOutcome::BudgetNotFound { key } => json!({
            "key": key_value(*key),
            "name": "BudgetNotFound",
        }),
        BudgetOutcome::InvalidAmount { minimum } => json!({
            "minimum": minimum.to_string(),
            "name": "InvalidAmount",
        }),
        BudgetOutcome::InsufficientBudget {
            approved,
            allocated,
            requested,
        } => json!({
            "allocated": allocated.to_string(),
            "approved": approved.to_string(),
            "name": "InsufficientBudget",
            "requested": requested.to_string(),
        }),
        BudgetOutcome::Allocated { budget, remaining } => json!({
            "budget": budget_value(budget),
            "name": "Allocated",
            "remaining": remaining.to_string(),
        }),
    }
}

fn parse_sequential_workload(value: &Value) -> Result<SequentialWorkload, FixtureJsonError> {
    let object = as_object(value, "sequential")?;
    exact_keys(object, &["case_id", "operations"])?;
    let values = as_array(field(object, "operations")?)?;
    if values.len() > MAX_WORKLOAD_OPERATIONS {
        return Err(FixtureJsonError::CollectionTooLarge);
    }
    let operations = values
        .iter()
        .map(parse_operation)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SequentialWorkload {
        case_id: parse_operation_id(field(object, "case_id")?)?,
        operations,
    })
}

fn parse_contention_workload(value: &Value) -> Result<ContentionWorkload, FixtureJsonError> {
    let object = as_object(value, "contention")?;
    exact_keys(object, &["case_id", "contenders", "seed"])?;
    let contenders = as_array(field(object, "contenders")?)?;
    if contenders.len() != 2 {
        return Err(FixtureJsonError::InvalidShape);
    }
    Ok(ContentionWorkload {
        case_id: parse_operation_id(field(object, "case_id")?)?,
        contenders: [
            parse_allocate(&contenders[0])?,
            parse_allocate(&contenders[1])?,
        ],
        seed: parse_create(field(object, "seed")?)?,
    })
}

fn parse_operation(value: &Value) -> Result<BudgetOperation, FixtureJsonError> {
    let object = as_object(value, "operation")?;
    match as_string(field(object, "command")?)? {
        "CreateBudget" => parse_create(value).map(BudgetOperation::Create),
        "AllocateBudget" => parse_allocate(value).map(BudgetOperation::Allocate),
        _ => Err(FixtureJsonError::InvalidShape),
    }
}

fn parse_create(value: &Value) -> Result<CreateBudget, FixtureJsonError> {
    let object = as_object(value, "CreateBudget")?;
    exact_keys(
        object,
        &[
            "approved_amount",
            "command",
            "fiscal_year",
            "idempotency_key",
            "operation_id",
            "organization_id",
        ],
    )?;
    if as_string(field(object, "command")?)? != "CreateBudget" {
        return Err(FixtureJsonError::InvalidShape);
    }
    Ok(CreateBudget {
        operation_id: parse_operation_id(field(object, "operation_id")?)?,
        idempotency_key: parse_idempotency_key(field(object, "idempotency_key")?)?,
        key: parse_key(object)?,
        approved_amount: parse_amount(field(object, "approved_amount")?)?,
    })
}

fn parse_allocate(value: &Value) -> Result<AllocateBudget, FixtureJsonError> {
    let object = as_object(value, "AllocateBudget")?;
    exact_keys(
        object,
        &[
            "amount",
            "command",
            "fiscal_year",
            "idempotency_key",
            "matter_id",
            "operation_id",
            "organization_id",
        ],
    )?;
    if as_string(field(object, "command")?)? != "AllocateBudget" {
        return Err(FixtureJsonError::InvalidShape);
    }
    Ok(AllocateBudget {
        operation_id: parse_operation_id(field(object, "operation_id")?)?,
        idempotency_key: parse_idempotency_key(field(object, "idempotency_key")?)?,
        key: parse_key(object)?,
        matter_id: MatterId::parse(as_string(field(object, "matter_id")?)?)
            .map_err(|_| FixtureJsonError::InvalidValue)?,
        amount: parse_amount(field(object, "amount")?)?,
    })
}

fn parse_key(object: &Map<String, Value>) -> Result<BudgetKey, FixtureJsonError> {
    Ok(BudgetKey {
        organization_id: OrganizationId::parse(as_string(field(object, "organization_id")?)?)
            .map_err(|_| FixtureJsonError::InvalidValue)?,
        fiscal_year: as_i64(field(object, "fiscal_year")?)?,
    })
}

fn parse_operation_id(value: &Value) -> Result<OperationId, FixtureJsonError> {
    OperationId::new(as_string(value)?).map_err(|_| FixtureJsonError::InvalidValue)
}

fn parse_idempotency_key(value: &Value) -> Result<WorkloadIdempotencyKey, FixtureJsonError> {
    WorkloadIdempotencyKey::new(as_string(value)?).map_err(|_| FixtureJsonError::InvalidValue)
}

fn parse_amount(value: &Value) -> Result<Amount, FixtureJsonError> {
    Amount::parse(as_string(value)?).map_err(|_| FixtureJsonError::InvalidValue)
}

fn as_object<'a>(
    value: &'a Value,
    _path: &'static str,
) -> Result<&'a Map<String, Value>, FixtureJsonError> {
    value.as_object().ok_or(FixtureJsonError::InvalidShape)
}

fn as_array(value: &Value) -> Result<&[Value], FixtureJsonError> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or(FixtureJsonError::InvalidShape)
}

fn as_string(value: &Value) -> Result<&str, FixtureJsonError> {
    value.as_str().ok_or(FixtureJsonError::InvalidShape)
}

fn as_i64(value: &Value) -> Result<i64, FixtureJsonError> {
    value.as_i64().ok_or(FixtureJsonError::InvalidShape)
}

fn as_u32(value: &Value) -> Result<u32, FixtureJsonError> {
    let value = value.as_u64().ok_or(FixtureJsonError::InvalidShape)?;
    u32::try_from(value).map_err(|_| FixtureJsonError::InvalidValue)
}

fn field<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a Value, FixtureJsonError> {
    object.get(name).ok_or(FixtureJsonError::InvalidShape)
}

fn exact_keys(object: &Map<String, Value>, expected: &[&str]) -> Result<(), FixtureJsonError> {
    if object.len() != expected.len() || expected.iter().any(|name| !object.contains_key(*name)) {
        return Err(FixtureJsonError::InvalidShape);
    }
    Ok(())
}

/// A safe fixture decoding failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixtureJsonError {
    /// Source exceeds the fixed document bound.
    SourceTooLarge,
    /// JSON syntax is malformed.
    Malformed,
    /// The fixture version is not supported.
    UnsupportedVersion,
    /// The JSON shape, fields, or command spelling is wrong.
    InvalidShape,
    /// A typed leaf is outside its accepted grammar or bounds.
    InvalidValue,
    /// An input collection exceeds its bound.
    CollectionTooLarge,
    /// Valid typed content does not use the canonical fixture representation.
    NonCanonical,
}

impl fmt::Display for FixtureJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceTooLarge => formatter.write_str("fixture exceeds the 1 MiB source bound"),
            Self::Malformed => formatter.write_str("fixture is malformed JSON"),
            Self::UnsupportedVersion => {
                formatter.write_str("fixture schema version is unsupported")
            }
            Self::InvalidShape => formatter.write_str("fixture has an invalid or unknown shape"),
            Self::InvalidValue => formatter.write_str("fixture contains an invalid typed value"),
            Self::CollectionTooLarge => formatter.write_str("fixture collection exceeds its bound"),
            Self::NonCanonical => {
                formatter.write_str("fixture JSON is not in canonical representation")
            }
        }
    }
}

impl Error for FixtureJsonError {}
