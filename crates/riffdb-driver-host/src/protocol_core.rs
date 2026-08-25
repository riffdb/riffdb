//! Transport-free driver protocol normalization shared by every binding.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use riffdb_client_rust::{
    ApplicationClientError, ApplicationCommand, ApplicationCommandResult, ApplicationContract,
    ApplicationErrorCode, ApplicationValue, AttemptBudget, CallMetadata, ClientError,
    DetailsFreeStatus, NamedQuery, NamedQueryResult, PublicError, QueryOptions,
    StableApplicationClient, app_v1,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{
    DriverDecimal, DriverMoney, DriverRequest, DriverTimestamp, DriverValue, DriverVector,
    FrameCodec, InvokeOptions,
};

const MAX_BINDING_REQUEST_BYTES: usize = 4 * 1_024 * 1_024;
const SYNTHETIC_SCHEMA_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

/// Safe binding-to-core normalization failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolCoreError {
    /// The binding request is malformed or violates a protocol bound.
    InvalidInput,
    /// The shared client rejected construction of a typed application request.
    InvalidApplicationInput,
}

/// Closed binding error produced by the shared protocol core.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingError {
    /// Stable binding-level classification.
    pub kind: &'static str,
    /// Optional safe structured application diagnostic.
    pub details: Option<Value>,
}

/// Maps an application-client failure once for every binding.
#[must_use]
pub fn classify_application_client_error(error: ApplicationClientError) -> BindingError {
    match error {
        ApplicationClientError::InvalidInput => binding_error("invalid_input", None),
        ApplicationClientError::InvalidResponse => binding_error("protocol_error", None),
        ApplicationClientError::IdentifierUnavailable => binding_error("connection_failure", None),
        ApplicationClientError::Client(error) => classify_client_error(error),
    }
}

/// Maps a transport/client failure once for every binding.
#[must_use]
pub fn classify_client_error(error: ClientError) -> BindingError {
    if let Some(error) = error.application_error() {
        return binding_error("application", Some(application_error_json(error)));
    }
    if let Some(error) = error.public_error() {
        return binding_error("application", Some(public_application_error_json(error)));
    }
    let kind = match error {
        ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable)
        | ClientError::ConnectionFailure
        | ClientError::IdentifierGeneration(_)
        | ClientError::Tls(_) => "connection_failure",
        ClientError::OutcomeUnknown(_) => "outcome_unknown",
        ClientError::Protocol(_) => "protocol_error",
        ClientError::Public(_) | ClientError::Application(_) => {
            unreachable!("handled by semantic/public guards")
        }
        ClientError::DetailsFree(_) => "protocol_error",
    };
    binding_error(kind, None)
}

const fn binding_error(kind: &'static str, details: Option<Value>) -> BindingError {
    BindingError { kind, details }
}

fn public_application_error_json(error: &PublicError) -> Value {
    let code = error
        .application_code_hint()
        .unwrap_or_else(|| ApplicationErrorCode::from_public_kind(error.kind()));
    json!({
        "code": code.as_str(),
        "message": code.safe_message(),
        "category": code.category().as_str(),
        "recovery_action": code.recovery_action().as_str(),
        "operation": "ApplicationRequest",
        "contract_lineage": Value::Null,
        "contract_version": Value::Null,
        "operation_symbol": Value::Null,
        "symbol_path": [],
        "source_span": Value::Null,
        "fixes": code.fixes().iter().map(|fix| fix.as_str()).collect::<Vec<_>>(),
        "trace_id": Value::Null,
        "incident_id": error.incident_id().map(ToString::to_string),
    })
}

fn application_error_json(error: &riffdb_client_rust::ApplicationError) -> Value {
    let context = error.context();
    let contract = context.contract();
    json!({
        "code": error.code().as_str(),
        "message": error.safe_message(),
        "category": error.category().as_str(),
        "recovery_action": error.recovery_action().as_str(),
        "operation": error.operation().as_str(),
        "contract_lineage": contract.map(|(lineage, _)| lineage.as_str()),
        "contract_version": contract.map(|(_, version)| version.get()),
        "operation_symbol": context.operation_symbol(),
        "symbol_path": context.symbol_path(),
        "source_span": context.source_span().map(|span| json!({"start": span.start(), "end": span.end()})),
        "fixes": error.fixes().iter().map(|fix| fix.as_str()).collect::<Vec<_>>(),
        "trace_id": context.trace_id().map(|value| value.to_string()),
        "incident_id": error.incident_id().map(ToString::to_string),
    })
}

impl fmt::Display for ProtocolCoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidInput => "driver input is invalid",
            Self::InvalidApplicationInput => "application input is invalid",
        })
    }
}

impl std::error::Error for ProtocolCoreError {}

/// Exact compiled named query normalized by the shared protocol core.
pub struct InProcessQuery {
    /// Typed application query sent by the first-party client.
    pub query: NamedQuery,
    /// Whether compact result carriage was requested.
    pub accept_compact_result: bool,
    /// Whether packed result carriage was requested.
    pub accept_packed_result: bool,
    /// Canonical driver request material used for binding equivalence tests.
    pub request: DriverRequest,
}

/// Exact compiled command normalized by the shared protocol core.
pub struct InProcessCommand {
    /// Typed application command sent by the first-party client.
    pub command: ApplicationCommand,
    /// Closed retry attempt budget.
    pub attempts: AttemptBudget,
    /// Expected contract version on the returned outcome.
    pub expected_contract_version: u64,
    /// Expected plan identity on the returned outcome.
    pub expected_plan_hash: [u8; 32],
    /// Canonical driver request material used for binding equivalence tests.
    pub request: DriverRequest,
}

/// One shared named-query dispatch result before binding presentation.
pub enum QueryDispatchResult {
    /// Ordinary name-addressed query result.
    Records(NamedQueryResult),
    /// Compact or packed wire result retained for generated decoding.
    Wire(app_v1::ExecuteQueryResponse),
}

/// Executes one already-normalized named query through the single driver core.
pub async fn dispatch_named_query(
    client: &mut StableApplicationClient,
    metadata: &CallMetadata,
    query: NamedQuery,
    wire_result: bool,
) -> Result<QueryDispatchResult, ApplicationClientError> {
    if wire_result {
        client
            .execute_named_query_wire(query, metadata)
            .await
            .map(QueryDispatchResult::Wire)
    } else {
        client
            .execute_named_query(query, metadata)
            .await
            .map(QueryDispatchResult::Records)
    }
}

/// Executes one already-normalized command through the single driver core and
/// optionally proves the exact generated response identity.
pub async fn dispatch_command(
    client: &mut StableApplicationClient,
    metadata: &CallMetadata,
    command: ApplicationCommand,
    attempts: AttemptBudget,
    expected_identity: Option<(u64, [u8; 32])>,
) -> Result<ApplicationCommandResult, ApplicationClientError> {
    let result = client.execute_command(command, attempts, metadata).await?;
    if expected_identity.is_some_and(|(version, plan_hash)| {
        result.contract_version != version || result.plan_hash != plan_hash
    }) {
        return Err(ApplicationClientError::InvalidResponse);
    }
    Ok(result)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PythonQueryRequest {
    contract_lineage: String,
    contract_version: u64,
    contract_bundle_hash: String,
    module_hash: String,
    query_name: String,
    plan_hash: String,
    parameters: BTreeMap<String, Value>,
    cursor: Option<String>,
    read_after_commit: Option<u64>,
    #[serde(default)]
    accept_compact_result: bool,
    #[serde(default)]
    accept_packed_result: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PythonCommandRequest {
    contract_lineage: String,
    contract_version: u64,
    command_name: String,
    plan_hash: String,
    input: BTreeMap<String, Value>,
    maximum_submissions: u32,
}

/// Normalizes the existing in-process Python named-query request through the
/// exact same `DriverRequest` and `DriverValue` admission rules as the socket.
pub fn parse_in_process_query(source: &str) -> Result<InProcessQuery, ProtocolCoreError> {
    let parsed: PythonQueryRequest = parse_json(source)?;
    if parsed.accept_packed_result && !parsed.accept_compact_result {
        return Err(ProtocolCoreError::InvalidInput);
    }
    let bundle_hash = parse_hash(&parsed.contract_bundle_hash)?;
    let module_hash = parse_hash(&parsed.module_hash)?;
    let plan_hash = parse_hash(&parsed.plan_hash)?;
    let input = normalize_python_fields(parsed.parameters)?;
    let options = InvokeOptions {
        deadline_millis: 30_000,
        maximum_attempts: 1,
        read_after_commit: parsed.read_after_commit,
        cursor: parsed.cursor.clone(),
        accept_compact_result: parsed.accept_compact_result,
        accept_packed_result: parsed.accept_packed_result,
    };
    let request = validated_invoke(&parsed.query_name, &input, options.clone())?;
    let parameters = lower_driver_fields(input)?;
    let mut query_options = QueryOptions::new();
    if let Some(cursor) = parsed.cursor {
        query_options = query_options.after(cursor);
    }
    if let Some(sequence) = parsed.read_after_commit {
        query_options = query_options.read_after_commit(sequence);
    }
    let query = NamedQuery::new(
        ApplicationContract::Exact {
            lineage: parsed.contract_lineage,
            version: parsed.contract_version,
            bundle_hash: Some(bundle_hash),
        },
        parsed.query_name,
        Some(module_hash),
        parameters,
        None,
    )
    .and_then(|query| {
        let query = query.expect_plan_hash(plan_hash);
        let query = if parsed.accept_packed_result {
            query.accept_packed_result_v1()
        } else {
            query
        };
        query.with_options(query_options)
    })
    .map_err(map_application_input)?;
    Ok(InProcessQuery {
        query,
        accept_compact_result: parsed.accept_compact_result,
        accept_packed_result: parsed.accept_packed_result,
        request,
    })
}

/// Normalizes the existing in-process Python command request through the exact
/// same `DriverRequest` and `DriverValue` admission rules as the socket.
pub fn parse_in_process_command(source: &str) -> Result<InProcessCommand, ProtocolCoreError> {
    let parsed: PythonCommandRequest = parse_json(source)?;
    if parsed.contract_lineage.is_empty() || parsed.contract_lineage.len() > 256 {
        return Err(ProtocolCoreError::InvalidInput);
    }
    let plan_hash = parse_hash(&parsed.plan_hash)?;
    let input = normalize_python_fields(parsed.input)?;
    let options = InvokeOptions {
        deadline_millis: 30_000,
        maximum_attempts: parsed.maximum_submissions,
        read_after_commit: None,
        cursor: None,
        accept_compact_result: false,
        accept_packed_result: false,
    };
    let request = validated_invoke(&parsed.command_name, &input, options)?;
    let input = lower_driver_fields(input)?;
    let command =
        ApplicationCommand::new(parsed.command_name, Some(parsed.contract_version), input)
            .map_err(map_application_input)?;
    let attempts =
        AttemptBudget::new(parsed.maximum_submissions).ok_or(ProtocolCoreError::InvalidInput)?;
    Ok(InProcessCommand {
        command,
        attempts,
        expected_contract_version: parsed.contract_version,
        expected_plan_hash: plan_hash,
        request,
    })
}

/// Applies the authoritative host value grammar and bounds to one legacy
/// in-process binding value.
pub fn normalize_python_value(value: Value) -> Result<ApplicationValue, ProtocolCoreError> {
    normalize_python_value_request(value).map(|(value, _)| value)
}

/// Returns the admitted value and canonical complete request bytes used by the
/// in-process transport. The socket corpus uses this same envelope.
pub fn normalize_python_value_request(
    value: Value,
) -> Result<(ApplicationValue, Vec<u8>), ProtocolCoreError> {
    let fields = BTreeMap::from([("value".to_owned(), normalize_python_driver_value(value)?)]);
    let request = validated_invoke(
        "BindingValue",
        &fields,
        InvokeOptions {
            deadline_millis: 1,
            maximum_attempts: 1,
            read_after_commit: None,
            cursor: None,
            accept_compact_result: false,
            accept_packed_result: false,
        },
    )?;
    let material =
        FrameCodec::encode_request(&request).map_err(|_| ProtocolCoreError::InvalidInput)?;
    let value = lower_driver_value(
        fields
            .into_values()
            .next()
            .ok_or(ProtocolCoreError::InvalidInput)?,
    )?;
    Ok((value, material))
}

/// Converts one admitted protocol value into the API-neutral application graph.
pub fn lower_driver_value(value: DriverValue) -> Result<ApplicationValue, ProtocolCoreError> {
    match value {
        DriverValue::Null => Ok(ApplicationValue::Null),
        DriverValue::Bool(value) => Ok(ApplicationValue::Bool(value)),
        DriverValue::I64(value) => value
            .parse()
            .map(ApplicationValue::I64)
            .map_err(|_| ProtocolCoreError::InvalidInput),
        DriverValue::U64(value) => value
            .parse()
            .map(ApplicationValue::U64)
            .map_err(|_| ProtocolCoreError::InvalidInput),
        DriverValue::String(value) => Ok(ApplicationValue::String(value)),
        DriverValue::Uuid(value) => riffdb_client_rust::ApplicationUuid::from_text(value)
            .map(ApplicationValue::Uuid)
            .map_err(|_| ProtocolCoreError::InvalidInput),
        DriverValue::Enum(value) => Ok(ApplicationValue::Enum(value)),
        DriverValue::Bytes(value) => BASE64
            .decode(value)
            .map(ApplicationValue::Bytes)
            .map_err(|_| ProtocolCoreError::InvalidInput),
        DriverValue::Date(value) => value
            .parse()
            .map(ApplicationValue::Date)
            .map_err(|_| ProtocolCoreError::InvalidInput),
        DriverValue::Timestamp(value) => Ok(ApplicationValue::Timestamp {
            seconds: value
                .seconds
                .parse()
                .map_err(|_| ProtocolCoreError::InvalidInput)?,
            nanos: value.nanos,
        }),
        DriverValue::Decimal(value) => Ok(ApplicationValue::Decimal {
            coefficient_twos_complement: BASE64
                .decode(value.coefficient)
                .map_err(|_| ProtocolCoreError::InvalidInput)?,
            scale: value.scale,
            precision: value.precision,
        }),
        DriverValue::Money(value) => Ok(ApplicationValue::Money {
            currency: value.currency,
            amount: Box::new(ApplicationValue::Decimal {
                coefficient_twos_complement: BASE64
                    .decode(value.amount.coefficient)
                    .map_err(|_| ProtocolCoreError::InvalidInput)?,
                scale: value.amount.scale,
                precision: value.amount.precision,
            }),
        }),
        DriverValue::Vector(value) => riffdb_types::CanonicalVector::new(
            value
                .component_bits
                .into_iter()
                .map(f32::from_bits)
                .collect(),
        )
        .map(ApplicationValue::Vector)
        .map_err(|_| ProtocolCoreError::InvalidInput),
        DriverValue::List(values) => values
            .into_iter()
            .map(lower_driver_value)
            .collect::<Result<Vec<_>, _>>()
            .map(ApplicationValue::List),
        DriverValue::Record(values) => values
            .into_iter()
            .map(|(name, value)| Ok((name, lower_driver_value(value)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(ApplicationValue::Record),
    }
}

/// Converts one API-neutral application value into the canonical binding
/// protocol value. All transports use this response marshaller.
#[must_use]
pub fn raise_driver_value(value: ApplicationValue) -> DriverValue {
    match value {
        ApplicationValue::Null => DriverValue::Null,
        ApplicationValue::Bool(value) => DriverValue::Bool(value),
        ApplicationValue::I64(value) => DriverValue::I64(value.to_string()),
        ApplicationValue::U64(value) => DriverValue::U64(value.to_string()),
        ApplicationValue::Decimal {
            coefficient_twos_complement,
            scale,
            precision,
        } => DriverValue::Decimal(DriverDecimal {
            coefficient: BASE64.encode(coefficient_twos_complement),
            scale,
            precision,
        }),
        ApplicationValue::Money { currency, amount } => match raise_driver_value(*amount) {
            DriverValue::Decimal(amount) => DriverValue::Money(DriverMoney { currency, amount }),
            _ => DriverValue::Null,
        },
        ApplicationValue::String(value) => DriverValue::String(value),
        ApplicationValue::Uuid(value) => DriverValue::Uuid(value.into_string()),
        ApplicationValue::Enum(value) | ApplicationValue::EnumIdentity { name: value, .. } => {
            DriverValue::Enum(value)
        }
        ApplicationValue::Bytes(value) => DriverValue::Bytes(BASE64.encode(value)),
        ApplicationValue::Date(value) => DriverValue::Date(value.to_string()),
        ApplicationValue::Timestamp { seconds, nanos } => DriverValue::Timestamp(DriverTimestamp {
            seconds: seconds.to_string(),
            nanos,
        }),
        ApplicationValue::Vector(value) => DriverValue::Vector(DriverVector {
            component_bits: value
                .components()
                .iter()
                .copied()
                .map(f32::to_bits)
                .collect(),
        }),
        ApplicationValue::List(values) => {
            DriverValue::List(values.into_iter().map(raise_driver_value).collect())
        }
        ApplicationValue::Record(values) => DriverValue::Record(
            values
                .into_iter()
                .map(|(name, value)| (name, raise_driver_value(value)))
                .collect(),
        ),
    }
}

/// Converts one API-neutral result value into the retained Python facade JSON.
/// This presentation adapter does not define a second value registry.
#[must_use]
pub fn application_value_to_python_json(value: ApplicationValue) -> Value {
    match value {
        ApplicationValue::Null => Value::Null,
        ApplicationValue::Bool(value) => Value::Bool(value),
        ApplicationValue::I64(value) => json!(value),
        ApplicationValue::U64(value) => json!(value),
        ApplicationValue::Decimal {
            coefficient_twos_complement,
            scale,
            precision,
        } => json!({
            "$riffdb": "decimal",
            "coefficient": hex(&coefficient_twos_complement),
            "scale": scale,
            "precision": precision,
        }),
        ApplicationValue::Money { currency, amount } => json!({
            "$riffdb": "money",
            "currency": currency,
            "amount": application_value_to_python_json(*amount),
        }),
        ApplicationValue::String(value) => Value::String(value),
        ApplicationValue::Uuid(value) => json!({"$riffdb": "uuid", "value": value.into_string()}),
        ApplicationValue::Enum(value) | ApplicationValue::EnumIdentity { name: value, .. } => {
            json!({"$riffdb": "enum", "value": value})
        }
        ApplicationValue::Bytes(value) => json!({"$riffdb": "bytes", "value": hex(&value)}),
        ApplicationValue::Date(value) => json!({"$riffdb": "date", "value": value}),
        ApplicationValue::Timestamp { seconds, nanos } => {
            json!({"$riffdb": "timestamp", "seconds": seconds, "nanos": nanos})
        }
        ApplicationValue::Vector(value) => {
            json!({"$riffdb": "vector", "components": value.into_components()})
        }
        ApplicationValue::List(values) => Value::Array(
            values
                .into_iter()
                .map(application_value_to_python_json)
                .collect(),
        ),
        ApplicationValue::Record(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(name, value)| (name, application_value_to_python_json(value)))
                .collect(),
        ),
    }
}

fn validated_invoke(
    operation: &str,
    input: &BTreeMap<String, DriverValue>,
    options: InvokeOptions,
) -> Result<DriverRequest, ProtocolCoreError> {
    let request = DriverRequest::Invoke {
        request_id: "in-process-binding".to_owned(),
        operation: operation.to_owned(),
        input_schema_hash: SYNTHETIC_SCHEMA_HASH.to_owned(),
        input: input.clone(),
        options,
    };
    FrameCodec::encode_request(&request).map_err(|_| ProtocolCoreError::InvalidInput)?;
    Ok(request)
}

fn normalize_python_fields(
    fields: BTreeMap<String, Value>,
) -> Result<BTreeMap<String, DriverValue>, ProtocolCoreError> {
    let values = fields
        .into_iter()
        .map(|(name, value)| Ok((name, normalize_python_driver_value(value)?)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    validated_invoke(
        "BindingValue",
        &values,
        InvokeOptions {
            deadline_millis: 1,
            maximum_attempts: 1,
            read_after_commit: None,
            cursor: None,
            accept_compact_result: false,
            accept_packed_result: false,
        },
    )?;
    Ok(values)
}

fn lower_driver_fields(
    fields: BTreeMap<String, DriverValue>,
) -> Result<BTreeMap<String, ApplicationValue>, ProtocolCoreError> {
    fields
        .into_iter()
        .map(|(name, value)| Ok((name, lower_driver_value(value)?)))
        .collect()
}

fn normalize_python_driver_value(value: Value) -> Result<DriverValue, ProtocolCoreError> {
    let object = value.as_object().ok_or(ProtocolCoreError::InvalidInput)?;
    let kind = exact_string(object, "kind")?;
    match kind {
        "null" if object.len() == 1 => Ok(DriverValue::Null),
        "bool" if object.len() == 2 => exact_value(object, "value")?
            .as_bool()
            .map(DriverValue::Bool)
            .ok_or(ProtocolCoreError::InvalidInput),
        "i64" if object.len() == 2 => exact_value(object, "value")?
            .as_i64()
            .map(|value| DriverValue::I64(value.to_string()))
            .ok_or(ProtocolCoreError::InvalidInput),
        "u64" if object.len() == 2 => exact_value(object, "value")?
            .as_u64()
            .map(|value| DriverValue::U64(value.to_string()))
            .ok_or(ProtocolCoreError::InvalidInput),
        "string" if object.len() == 2 => Ok(DriverValue::String(
            exact_string(object, "value")?.to_owned(),
        )),
        "uuid" if object.len() == 2 => {
            Ok(DriverValue::Uuid(exact_string(object, "value")?.to_owned()))
        }
        "enum" if object.len() == 2 => {
            Ok(DriverValue::Enum(exact_string(object, "value")?.to_owned()))
        }
        "bytes" if object.len() == 2 => Ok(DriverValue::Bytes(
            BASE64.encode(parse_hex(exact_string(object, "value")?)?),
        )),
        "date" if object.len() == 2 => exact_value(object, "value")?
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .map(|value| DriverValue::Date(value.to_string()))
            .ok_or(ProtocolCoreError::InvalidInput),
        "timestamp" if object.len() == 3 => Ok(DriverValue::Timestamp(DriverTimestamp {
            seconds: exact_value(object, "seconds")?
                .as_i64()
                .ok_or(ProtocolCoreError::InvalidInput)?
                .to_string(),
            nanos: exact_value(object, "nanos")?
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or(ProtocolCoreError::InvalidInput)?,
        })),
        "decimal" if matches!(object.len(), 3 | 4) => Ok(DriverValue::Decimal(DriverDecimal {
            coefficient: BASE64.encode(parse_hex(exact_string(object, "coefficient")?)?),
            scale: exact_value(object, "scale")?
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or(ProtocolCoreError::InvalidInput)?,
            precision: object
                .get("precision")
                .map(|value| {
                    value
                        .as_u64()
                        .and_then(|value| u32::try_from(value).ok())
                        .ok_or(ProtocolCoreError::InvalidInput)
                })
                .transpose()?,
        })),
        "money" if object.len() == 3 => {
            let DriverValue::Decimal(amount) =
                normalize_python_driver_value(exact_value(object, "amount")?.clone())?
            else {
                return Err(ProtocolCoreError::InvalidInput);
            };
            Ok(DriverValue::Money(DriverMoney {
                currency: exact_string(object, "currency")?.to_owned(),
                amount,
            }))
        }
        "vector" if object.len() == 2 => {
            let component_bits = exact_value(object, "components")?
                .as_array()
                .ok_or(ProtocolCoreError::InvalidInput)?
                .iter()
                .map(|component| {
                    component
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .map(|value| value as f32)
                        .filter(|value| value.is_finite())
                        .map(f32::to_bits)
                        .ok_or(ProtocolCoreError::InvalidInput)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DriverValue::Vector(DriverVector { component_bits }))
        }
        "list" if object.len() == 2 => Ok(DriverValue::List(
            exact_value(object, "value")?
                .as_array()
                .ok_or(ProtocolCoreError::InvalidInput)?
                .iter()
                .cloned()
                .map(normalize_python_driver_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        "record" if object.len() == 2 => Ok(DriverValue::Record(
            exact_value(object, "value")?
                .as_object()
                .ok_or(ProtocolCoreError::InvalidInput)?
                .iter()
                .map(|(name, value)| {
                    Ok((name.clone(), normalize_python_driver_value(value.clone())?))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?,
        )),
        _ => Err(ProtocolCoreError::InvalidInput),
    }
}

fn parse_json<T: for<'de> Deserialize<'de>>(source: &str) -> Result<T, ProtocolCoreError> {
    if source.is_empty() || source.len() > MAX_BINDING_REQUEST_BYTES {
        return Err(ProtocolCoreError::InvalidInput);
    }
    serde_json::from_str(source).map_err(|_| ProtocolCoreError::InvalidInput)
}

fn parse_hash(value: &str) -> Result<[u8; 32], ProtocolCoreError> {
    parse_hex(value)?
        .try_into()
        .map_err(|_| ProtocolCoreError::InvalidInput)
}

fn parse_hex(value: &str) -> Result<Vec<u8>, ProtocolCoreError> {
    if !value.len().is_multiple_of(2) || value.len() > MAX_BINDING_REQUEST_BYTES * 2 {
        return Err(ProtocolCoreError::InvalidInput);
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).map_err(|_| ProtocolCoreError::InvalidInput)?;
            u8::from_str_radix(text, 16).map_err(|_| ProtocolCoreError::InvalidInput)
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("String writes cannot fail");
    }
    output
}

fn exact_value<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a Value, ProtocolCoreError> {
    object.get(key).ok_or(ProtocolCoreError::InvalidInput)
}

fn exact_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, ProtocolCoreError> {
    exact_value(object, key)?
        .as_str()
        .ok_or(ProtocolCoreError::InvalidInput)
}

fn map_application_input(_error: ApplicationClientError) -> ProtocolCoreError {
    ProtocolCoreError::InvalidApplicationInput
}
