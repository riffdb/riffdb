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
    DriverDecimal, DriverMoney, DriverQueryConsistency, DriverRequest, DriverResponse,
    DriverTimestamp, DriverValue, DriverVector, FrameCodec, InvokeOptions,
};

const MAX_BINDING_REQUEST_BYTES: usize = riffdb_types::MAX_ATOMIC_COMMAND_FRAME_BYTES_V2;
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

/// Maps a driver-socket `DriverResponse::Error` once for every binding.
#[must_use]
pub fn classify_driver_error_response(response: &DriverResponse) -> BindingError {
    let DriverResponse::Error {
        code,
        category,
        operation,
        symbol_path,
        authorization_diagnostic,
        contract_lineage,
        contract_version,
        trace_id,
        incident_id,
        outcome_uncertain,
        ..
    } = response
    else {
        return binding_error("protocol_error", None);
    };
    if *outcome_uncertain || code == "RDB-UNCERTAIN-0101" {
        return binding_error("outcome_unknown", None);
    }
    if matches!(code.as_str(), "RDB-DRIVER-0003" | "RDB-DRIVER-0005") {
        return binding_error("invalid_input", None);
    }
    if matches!(
        code.as_str(),
        "RDB-DRIVER-0001" | "RDB-DRIVER-0002" | "RDB-DRIVER-0004"
    ) {
        return binding_error("protocol_error", None);
    }
    if let Some(app_code) = parse_application_error_code(code) {
        if authorization_diagnostic.as_deref().is_some_and(|value| {
            app_code != ApplicationErrorCode::AuthorizationDenied
                || riffdb_errors::AuthorizationDiagnosticCode::from_str_name(value).is_none()
        }) {
            return binding_error("protocol_error", None);
        }
        return binding_error(
            "application",
            Some(json!({
                "code": app_code.as_str(),
                "message": app_code.safe_message(),
                "category": app_code.category().as_str(),
                "recovery_action": app_code.recovery_action().as_str(),
                "operation": operation
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .unwrap_or("ApplicationRequest"),
                "contract_lineage": contract_lineage,
                "contract_version": contract_version,
                "operation_symbol": operation,
                "symbol_path": symbol_path,
                "authorization_diagnostic": authorization_diagnostic,
                "source_span": Value::Null,
                "fixes": app_code.fixes().iter().map(|fix| fix.as_str()).collect::<Vec<_>>(),
                "trace_id": trace_id,
                "incident_id": incident_id,
            })),
        );
    }
    if category == "transport" {
        return binding_error("connection_failure", None);
    }
    binding_error("protocol_error", None)
}

fn parse_application_error_code(code: &str) -> Option<ApplicationErrorCode> {
    [
        ApplicationErrorCode::InvalidRequest,
        ApplicationErrorCode::InputInvalid,
        ApplicationErrorCode::AuthorizationDenied,
        ApplicationErrorCode::ContractMismatch,
        ApplicationErrorCode::QueryInvalid,
        ApplicationErrorCode::QueryUnavailable,
        ApplicationErrorCode::ModuleUnavailable,
        ApplicationErrorCode::CursorInvalid,
        ApplicationErrorCode::ResponseTooLarge,
        ApplicationErrorCode::StorageUnavailable,
        ApplicationErrorCode::OutcomeUnknown,
        ApplicationErrorCode::OperationCancelled,
        ApplicationErrorCode::DeadlineExceeded,
        ApplicationErrorCode::InternalDefect,
        ApplicationErrorCode::IdempotencyKeyReuse,
        ApplicationErrorCode::CommandExecutionFailed,
        ApplicationErrorCode::CapabilityRevoked,
        ApplicationErrorCode::ProtocolInvalid,
        ApplicationErrorCode::HistoryIncarnationMismatch,
        ApplicationErrorCode::HistoryPruned,
        ApplicationErrorCode::Overloaded,
        ApplicationErrorCode::ProjectionDiverged,
        ApplicationErrorCode::SnapshotRetired,
        ApplicationErrorCode::FreshnessUnsatisfied,
        ApplicationErrorCode::ProjectedSourceRequired,
        ApplicationErrorCode::FollowerMode,
        ApplicationErrorCode::PrimaryFenced,
    ]
    .into_iter()
    .find(|candidate| candidate.as_str() == code)
}

const fn binding_error(kind: &'static str, details: Option<Value>) -> BindingError {
    BindingError { kind, details }
}

fn public_application_error_json(error: &PublicError) -> Value {
    let code = error
        .application_code_hint()
        .unwrap_or_else(|| ApplicationErrorCode::from_public_kind(error.kind()));
    let authorization_diagnostic = (code == ApplicationErrorCode::AuthorizationDenied)
        .then(|| error.authorization_diagnostic())
        .flatten()
        .map(|diagnostic| diagnostic.as_str());
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
        "authorization_diagnostic": authorization_diagnostic,
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
        "authorization_diagnostic": context.authorization_diagnostic().map(|diagnostic| diagnostic.as_str()),
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
    /// Expected contract lineage on the active application session.
    pub contract_lineage: String,
    /// Expected contract version on the active application session.
    pub contract_version: u64,
    /// Expected contract bundle hash (hex) on the active application session.
    pub contract_bundle_hash: String,
    /// Expected query module hash (hex).
    pub module_hash: String,
    /// Query operation name.
    pub query_name: String,
    /// Expected query plan hash (hex).
    pub plan_hash: String,
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
    /// Expected contract lineage on the active application session.
    pub contract_lineage: String,
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
    query_consistency: Option<DriverQueryConsistency>,
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
        query_consistency: parsed.query_consistency,
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
    if parsed.query_consistency == Some(DriverQueryConsistency::AdmissionHead) {
        query_options = query_options.at_least_admission_head();
    }
    let query = NamedQuery::new(
        ApplicationContract::Exact {
            lineage: parsed.contract_lineage.clone(),
            version: parsed.contract_version,
            bundle_hash: Some(bundle_hash),
        },
        parsed.query_name.clone(),
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
        contract_lineage: parsed.contract_lineage,
        contract_version: parsed.contract_version,
        contract_bundle_hash: parsed.contract_bundle_hash,
        module_hash: parsed.module_hash,
        query_name: parsed.query_name,
        plan_hash: parsed.plan_hash,
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
        query_consistency: None,
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
        contract_lineage: parsed.contract_lineage,
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
            query_consistency: None,
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
            query_consistency: None,
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

/// Validates one driver role selector against the closed ASCII token grammar.
pub fn validate_driver_role_name(role: &str) -> Result<String, BindingError> {
    if role.is_empty()
        || role.len() > 128
        || !role.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(binding_error("invalid_input", None));
    }
    Ok(role.to_owned())
}

/// Verified session identity established by a `DriverResponse::Handshake` ACK.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedSocketSession {
    /// Active contract lineage confirmed by the host.
    pub contract_lineage: String,
    /// Active contract version confirmed by the host.
    pub contract_version: u64,
    /// Active contract bundle hash (64-char hex) confirmed by the host.
    pub contract_bundle_hash: String,
    /// Active database alias confirmed by the host.
    pub database: String,
    /// Active role name confirmed by the host.
    pub role: String,
}

/// First-party Rust Unix domain socket client state for target-language drivers.
pub struct DriverSocketState {
    socket_path: std::path::PathBuf,
    database: String,
    role: String,
    io_timeout: std::time::Duration,
    client_id: riffdb_types::RequestId,
    next_seq: u64,
    stream: Option<tokio::net::UnixStream>,
    session: Option<VerifiedSocketSession>,
}

impl DriverSocketState {
    fn parse_socket_path(raw: &str) -> Result<std::path::PathBuf, BindingError> {
        let stripped = raw.strip_prefix("unix://").unwrap_or(raw);
        let path = std::path::Path::new(stripped);
        if !path.is_absolute()
            || path.as_os_str().as_encoded_bytes().len() > 4_096
            || std::os::unix::net::SocketAddr::from_pathname(path).is_err()
        {
            return Err(binding_error("invalid_input", None));
        }
        Ok(path.to_path_buf())
    }

    async fn open_stream(path: &std::path::Path) -> Result<tokio::net::UnixStream, BindingError> {
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::net::UnixStream::connect(path),
        )
        .await
        {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(_)) | Err(_) => Err(binding_error("connection_failure", None)),
        }
    }

    /// Connects to a `riffdb-driverd` Unix domain socket with bounded connect timeout.
    pub async fn connect(
        raw_path: &str,
        database: Option<&str>,
        role: Option<&str>,
    ) -> Result<Self, BindingError> {
        let socket_path = Self::parse_socket_path(raw_path)?;
        let validated_role = match role {
            Some(value) if !value.is_empty() => validate_driver_role_name(value)?,
            _ => String::new(),
        };
        let client_id = riffdb_client_rust::generate_request_id()
            .map_err(|_| binding_error("connection_failure", None))?;
        let stream = Self::open_stream(&socket_path).await?;
        Ok(Self {
            socket_path,
            database: database.unwrap_or_default().to_owned(),
            role: validated_role,
            io_timeout: std::time::Duration::from_secs(35),
            client_id,
            next_seq: 0,
            stream: Some(stream),
            session: None,
        })
    }

    /// Closes any active stream and clears the cached session.
    pub fn close(&mut self) {
        self.stream = None;
        self.session = None;
    }

    fn is_idle_stream_open(stream: &tokio::net::UnixStream) -> bool {
        let mut probe = [0_u8; 1];
        matches!(
            stream.try_read(&mut probe),
            Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock
        )
    }

    async fn handshake_on_stream(
        &mut self,
        mut stream: tokio::net::UnixStream,
        contract_lineage: &str,
        contract_version: u64,
        contract_bundle_hash: Option<&str>,
    ) -> Result<(tokio::net::UnixStream, VerifiedSocketSession), (bool, BindingError)> {
        self.next_seq = self.next_seq.wrapping_add(1);
        let request_id = format!("py-hs-{}-{}", self.client_id, self.next_seq);
        let request = DriverRequest::Handshake {
            request_id: request_id.clone(),
            protocol_version: crate::protocol::DRIVER_PROTOCOL_VERSION,
            application_manifest_hash: String::new(),
            operation_catalog_hash: String::new(),
            contract_lineage: contract_lineage.to_owned(),
            contract_version,
            contract_bundle_hash: contract_bundle_hash.unwrap_or_default().to_owned(),
            value_registry_hash: crate::host::DRIVER_VALUE_REGISTRY_HASH.to_owned(),
            error_registry_hash: crate::host::DRIVER_ERROR_REGISTRY_HASH.to_owned(),
            database: self.database.clone(),
            role: self.role.clone(),
            role_definition_hash: String::new(),
            remote_identity_hash: String::new(),
        };
        let frame = FrameCodec::encode_request(&request)
            .map_err(|_| (false, binding_error("invalid_input", None)))?;
        let timeout_budget = self.io_timeout.min(std::time::Duration::from_secs(10));
        let response = match tokio::time::timeout(timeout_budget, async {
            use tokio::io::AsyncWriteExt as _;
            stream
                .write_all(&frame)
                .await
                .map_err(|_| crate::protocol::ProtocolError::Io)?;
            FrameCodec::read_response(&mut stream).await
        })
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(
                crate::protocol::ProtocolError::Io | crate::protocol::ProtocolError::Truncated,
            ))
            | Err(_) => {
                return Err((true, binding_error("connection_failure", None)));
            }
            Ok(Err(
                crate::protocol::ProtocolError::InvalidFrameLength
                | crate::protocol::ProtocolError::InvalidMessage
                | crate::protocol::ProtocolError::InvalidBounds,
            )) => {
                return Err((false, binding_error("protocol_error", None)));
            }
        };
        match response {
            DriverResponse::Error {
                request_id: ref error_id,
                ..
            } => {
                // An error for another (or no) handshake is a correlation failure,
                // never this handshake's own rejection.
                if error_id.as_deref() != Some(request_id.as_str()) {
                    return Err((false, binding_error("protocol_error", None)));
                }
                Err((false, classify_driver_error_response(&response)))
            }
            DriverResponse::Handshake {
                request_id: ack_id,
                protocol_version,
                driver_identity,
                contract_lineage: ack_lineage,
                contract_version: ack_version,
                contract_bundle_hash: ack_bundle_hash,
                database: ack_database,
                role: ack_role,
                ..
            } => {
                if ack_id != request_id
                    || protocol_version != crate::protocol::DRIVER_PROTOCOL_VERSION
                    || driver_identity != crate::host::DRIVER_IDENTITY
                    || ack_lineage != contract_lineage
                    || ack_version != contract_version
                    || ack_bundle_hash.len() != 64
                    || contract_bundle_hash.is_some_and(|expected| ack_bundle_hash != expected)
                    || (!self.database.is_empty() && ack_database != self.database)
                    || (!self.role.is_empty() && ack_role != self.role)
                {
                    return Err((false, binding_error("protocol_error", None)));
                }
                Ok((
                    stream,
                    VerifiedSocketSession {
                        contract_lineage: ack_lineage,
                        contract_version: ack_version,
                        contract_bundle_hash: ack_bundle_hash,
                        database: ack_database,
                        role: ack_role,
                    },
                ))
            }
            _ => Err((false, binding_error("protocol_error", None))),
        }
    }

    async fn ensure_session(
        &mut self,
        contract_lineage: &str,
        contract_version: u64,
        contract_bundle_hash: Option<&str>,
    ) -> Result<(tokio::net::UnixStream, VerifiedSocketSession), BindingError> {
        match (self.stream.take(), self.session.take()) {
            (Some(stream), Some(session)) => {
                let matches_session = session.contract_lineage == contract_lineage
                    && session.contract_version == contract_version
                    && contract_bundle_hash
                        .is_none_or(|expected| session.contract_bundle_hash == expected);
                if matches_session && Self::is_idle_stream_open(&stream) {
                    return Ok((stream, session));
                }
                drop(stream);
            }
            (Some(stream), None) => {
                if Self::is_idle_stream_open(&stream) {
                    match self
                        .handshake_on_stream(
                            stream,
                            contract_lineage,
                            contract_version,
                            contract_bundle_hash,
                        )
                        .await
                    {
                        Ok(established) => return Ok(established),
                        Err((false, err)) => return Err(err),
                        Err((true, _)) => {}
                    }
                }
            }
            (None, _) => {}
        }
        let fresh_stream = Self::open_stream(&self.socket_path).await?;
        self.handshake_on_stream(
            fresh_stream,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
        )
        .await
        .map_err(|(_, err)| err)
    }

    /// Invokes one request over the verified driver socket session.
    pub async fn invoke(
        &mut self,
        contract_lineage: &str,
        contract_version: u64,
        contract_bundle_hash: Option<&str>,
        mut invoke_request: DriverRequest,
        is_command: bool,
    ) -> Result<(DriverResponse, VerifiedSocketSession), BindingError> {
        let (mut stream, session) = self
            .ensure_session(contract_lineage, contract_version, contract_bundle_hash)
            .await?;
        self.next_seq = self.next_seq.wrapping_add(1);
        let expected_request_id = format!("py-op-{}-{}", self.client_id, self.next_seq);
        if let DriverRequest::Invoke { request_id, .. } = &mut invoke_request {
            *request_id = expected_request_id.clone();
        }
        let frame = match FrameCodec::encode_request(&invoke_request) {
            Ok(frame) => frame,
            Err(_) => {
                self.stream = Some(stream);
                self.session = Some(session);
                return Err(binding_error("invalid_input", None));
            }
        };
        let io_result = tokio::time::timeout(self.io_timeout, async {
            use tokio::io::AsyncWriteExt as _;
            stream
                .write_all(&frame)
                .await
                .map_err(|_| crate::protocol::ProtocolError::Io)?;
            FrameCodec::read_response(&mut stream).await
        })
        .await;
        // From here on the request may have been read and executed by the host.
        // Only a correlated, well-formed reply establishes a disposition; any
        // other failure leaves a command's outcome uncertain and the stream is
        // dropped rather than reused.
        let unusable = binding_error(
            if is_command {
                "outcome_unknown"
            } else {
                "protocol_error"
            },
            None,
        );
        let response = match io_result {
            Ok(Ok(response)) => response,
            Ok(Err(
                crate::protocol::ProtocolError::Io | crate::protocol::ProtocolError::Truncated,
            ))
            | Err(_) => {
                return Err(binding_error(
                    if is_command {
                        "outcome_unknown"
                    } else {
                        "connection_failure"
                    },
                    None,
                ));
            }
            Ok(Err(
                crate::protocol::ProtocolError::InvalidFrameLength
                | crate::protocol::ProtocolError::InvalidMessage
                | crate::protocol::ProtocolError::InvalidBounds,
            )) => return Err(unusable),
        };
        let correlated = match &response {
            DriverResponse::Error { request_id, .. } => {
                request_id.as_deref() == Some(expected_request_id.as_str())
            }
            DriverResponse::Result { request_id, .. } => request_id == &expected_request_id,
            DriverResponse::CompactQueryResult { request_id, .. }
            | DriverResponse::PackedQueryResult { request_id, .. } => {
                !is_command && request_id == &expected_request_id
            }
            _ => false,
        };
        if !correlated {
            return Err(unusable);
        }
        self.stream = Some(stream);
        self.session = Some(session.clone());
        if matches!(response, DriverResponse::Error { .. }) {
            return Err(classify_driver_error_response(&response));
        }
        Ok((response, session))
    }
}

#[cfg(test)]
mod socket_client_tests {
    use super::*;
    use crate::host::DRIVER_IDENTITY;
    use crate::protocol::DRIVER_PROTOCOL_VERSION;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::net::UnixListener;

    static NEXT_SOCKET_ID: AtomicU64 = AtomicU64::new(1);

    struct ScopedSocketPath(std::path::PathBuf);

    impl ScopedSocketPath {
        fn new(label: &str) -> Self {
            let id = NEXT_SOCKET_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "riffdb-py-sock-{label}-{}-{id}.sock",
                std::process::id()
            ));
            let _ = std::fs::remove_file(&path);
            Self(path)
        }
    }

    impl Drop for ScopedSocketPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn sample_invoke(operation: &str) -> DriverRequest {
        DriverRequest::Invoke {
            request_id: "initial".to_owned(),
            operation: operation.to_owned(),
            input_schema_hash: "1".repeat(64),
            input: BTreeMap::new(),
            options: InvokeOptions {
                deadline_millis: 5_000,
                maximum_attempts: 1,
                read_after_commit: None,
                cursor: None,
                query_consistency: None,
                accept_compact_result: false,
                accept_packed_result: false,
            },
        }
    }

    // req: DRV-001, DRV-005, DRV-012
    #[tokio::test]
    async fn socket_command_post_send_disconnect_and_timeout_report_outcome_unknown() {
        let sock = ScopedSocketPath::new("uncertain");
        let listener = UnixListener::bind(&sock.0).expect("bind");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept 1");
            let hs = FrameCodec::read_request(&mut stream)
                .await
                .expect("read hs");
            let DriverRequest::Handshake { request_id, .. } = hs else {
                panic!("expected handshake");
            };
            FrameCodec::write_response(
                &mut stream,
                &DriverResponse::Handshake {
                    request_id,
                    protocol_version: DRIVER_PROTOCOL_VERSION,
                    driver_identity: DRIVER_IDENTITY.to_owned(),
                    application_manifest_hash: "a".repeat(64),
                    operation_catalog_hash: "b".repeat(64),
                    contract_lineage: "orders".to_owned(),
                    contract_version: 1,
                    contract_bundle_hash: "c".repeat(64),
                    database: "default".to_owned(),
                    role: "writer".to_owned(),
                    role_definition_hash: "d".repeat(64),
                    remote_identity_hash: "e".repeat(64),
                },
            )
            .await
            .expect("write hs");
            let _invoke = FrameCodec::read_request(&mut stream)
                .await
                .expect("read invoke");
            drop(stream);

            let (mut stream2, _) = listener.accept().await.expect("accept 2");
            let hs2 = FrameCodec::read_request(&mut stream2)
                .await
                .expect("read hs 2");
            let DriverRequest::Handshake { request_id, .. } = hs2 else {
                panic!("expected handshake 2");
            };
            FrameCodec::write_response(
                &mut stream2,
                &DriverResponse::Handshake {
                    request_id,
                    protocol_version: DRIVER_PROTOCOL_VERSION,
                    driver_identity: DRIVER_IDENTITY.to_owned(),
                    application_manifest_hash: "a".repeat(64),
                    operation_catalog_hash: "b".repeat(64),
                    contract_lineage: "orders".to_owned(),
                    contract_version: 1,
                    contract_bundle_hash: "c".repeat(64),
                    database: "default".to_owned(),
                    role: "writer".to_owned(),
                    role_definition_hash: "d".repeat(64),
                    remote_identity_hash: "e".repeat(64),
                },
            )
            .await
            .expect("write hs 2");
            let _invoke2 = FrameCodec::read_request(&mut stream2)
                .await
                .expect("read invoke 2");
            tokio::time::sleep(Duration::from_millis(250)).await;
        });

        let mut client = DriverSocketState::connect(
            sock.0.to_str().expect("utf8"),
            Some("default"),
            Some("writer"),
        )
        .await
        .expect("connect");
        client.io_timeout = Duration::from_millis(80);

        let err1 = client
            .invoke("orders", 1, None, sample_invoke("CreateOrder"), true)
            .await
            .expect_err("post-send EOF on command must fail");
        assert_eq!(err1.kind, "outcome_unknown");

        let err2 = client
            .invoke("orders", 1, None, sample_invoke("CreateOrder"), true)
            .await
            .expect_err("post-send timeout on command must fail");
        assert_eq!(err2.kind, "outcome_unknown");

        server.await.expect("server task");
    }

    #[tokio::test]
    async fn socket_handshake_verifies_ack_identity_role_and_bundle_hash() {
        let sock = ScopedSocketPath::new("ack");
        let listener = UnixListener::bind(&sock.0).expect("bind");

        let server = tokio::spawn(async move {
            let (mut stream1, _) = listener.accept().await.expect("accept 1");
            let hs1 = FrameCodec::read_request(&mut stream1)
                .await
                .expect("read hs 1");
            let DriverRequest::Handshake {
                request_id,
                role,
                contract_bundle_hash,
                ..
            } = hs1
            else {
                panic!("expected handshake 1");
            };
            assert_eq!(role, "reader");
            assert_eq!(contract_bundle_hash, "c".repeat(64));
            FrameCodec::write_response(
                &mut stream1,
                &DriverResponse::Handshake {
                    request_id,
                    protocol_version: DRIVER_PROTOCOL_VERSION,
                    driver_identity: DRIVER_IDENTITY.to_owned(),
                    application_manifest_hash: "a".repeat(64),
                    operation_catalog_hash: "b".repeat(64),
                    contract_lineage: "orders".to_owned(),
                    contract_version: 1,
                    contract_bundle_hash: "c".repeat(64),
                    database: "default".to_owned(),
                    role: "reader".to_owned(),
                    role_definition_hash: "d".repeat(64),
                    remote_identity_hash: "e".repeat(64),
                },
            )
            .await
            .expect("write hs 1");
            let invoke1 = FrameCodec::read_request(&mut stream1)
                .await
                .expect("read invoke 1");
            let DriverRequest::Invoke { request_id, .. } = invoke1 else {
                panic!("expected invoke 1");
            };
            FrameCodec::write_response(
                &mut stream1,
                &DriverResponse::Result {
                    request_id,
                    value: DriverValue::Record(BTreeMap::from([(
                        "outcome".to_owned(),
                        DriverValue::Enum("Found".to_owned()),
                    )])),
                    application_head: Some(7),
                    cursor: None,
                    replayed: false,
                },
            )
            .await
            .expect("write result 1");

            let (mut stream2, _) = listener.accept().await.expect("accept 2");
            let hs2 = FrameCodec::read_request(&mut stream2)
                .await
                .expect("read hs 2");
            let DriverRequest::Handshake {
                request_id,
                contract_bundle_hash,
                ..
            } = hs2
            else {
                panic!("expected handshake 2");
            };
            assert_eq!(contract_bundle_hash, "f".repeat(64));
            FrameCodec::write_response(
                &mut stream2,
                &DriverResponse::Handshake {
                    request_id,
                    protocol_version: DRIVER_PROTOCOL_VERSION,
                    driver_identity: DRIVER_IDENTITY.to_owned(),
                    application_manifest_hash: "a".repeat(64),
                    operation_catalog_hash: "b".repeat(64),
                    contract_lineage: "orders".to_owned(),
                    contract_version: 1,
                    contract_bundle_hash: "f".repeat(64),
                    database: "default".to_owned(),
                    role: "wrong-role".to_owned(),
                    role_definition_hash: "d".repeat(64),
                    remote_identity_hash: "e".repeat(64),
                },
            )
            .await
            .expect("write hs 2");
        });

        let mut client = DriverSocketState::connect(
            sock.0.to_str().expect("utf8"),
            Some("default"),
            Some("reader"),
        )
        .await
        .expect("connect");

        let bundle_c = "c".repeat(64);
        let (_, session1) = client
            .invoke(
                "orders",
                1,
                Some(&bundle_c),
                sample_invoke("GetOrder"),
                false,
            )
            .await
            .expect("first query succeeds");
        assert_eq!(session1.role, "reader");
        assert_eq!(session1.contract_bundle_hash, bundle_c);

        let bundle_f = "f".repeat(64);
        let err = client
            .invoke(
                "orders",
                1,
                Some(&bundle_f),
                sample_invoke("GetOrder"),
                false,
            )
            .await
            .expect_err("mismatched handshake ACK role must fail");
        assert_eq!(err.kind, "protocol_error");

        server.await.expect("server task");
    }

    fn handshake_ack(request_id: String) -> DriverResponse {
        DriverResponse::Handshake {
            request_id,
            protocol_version: DRIVER_PROTOCOL_VERSION,
            driver_identity: DRIVER_IDENTITY.to_owned(),
            application_manifest_hash: "a".repeat(64),
            operation_catalog_hash: "b".repeat(64),
            contract_lineage: "orders".to_owned(),
            contract_version: 1,
            contract_bundle_hash: "c".repeat(64),
            database: "default".to_owned(),
            role: "writer".to_owned(),
            role_definition_hash: "d".repeat(64),
            remote_identity_hash: "e".repeat(64),
        }
    }

    fn driver_error(request_id: Option<&str>, code: &str) -> DriverResponse {
        DriverResponse::Error {
            request_id: request_id.map(str::to_owned),
            code: code.to_owned(),
            category: "input".to_owned(),
            operation: Some("OtherCommand".to_owned()),
            symbol_path: Vec::new(),
            authorization_diagnostic: None,
            contract_lineage: None,
            contract_version: None,
            trace_id: None,
            incident_id: None,
            message: "driver invocation is invalid".to_owned(),
            retryability: "not_retryable".to_owned(),
            recovery_action: "correct_request".to_owned(),
            outcome_uncertain: false,
        }
    }

    fn found_result(request_id: String) -> DriverResponse {
        DriverResponse::Result {
            request_id,
            value: DriverValue::Record(BTreeMap::from([(
                "outcome".to_owned(),
                DriverValue::Enum("Found".to_owned()),
            )])),
            application_head: Some(7),
            cursor: None,
            replayed: false,
        }
    }

    /// Accepts one connection and completes an exact handshake on it.
    async fn accept_verified(listener: &UnixListener) -> tokio::net::UnixStream {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let DriverRequest::Handshake { request_id, .. } = FrameCodec::read_request(&mut stream)
            .await
            .expect("read handshake")
        else {
            panic!("expected handshake");
        };
        FrameCodec::write_response(&mut stream, &handshake_ack(request_id))
            .await
            .expect("write handshake ack");
        stream
    }

    async fn read_invoke_id(stream: &mut tokio::net::UnixStream) -> String {
        let DriverRequest::Invoke { request_id, .. } =
            FrameCodec::read_request(stream).await.expect("read invoke")
        else {
            panic!("expected invoke");
        };
        request_id
    }

    /// The client must discard a connection whose correlation or framing failed:
    /// the peer observes end-of-stream instead of the next request.
    async fn assert_client_discarded(mut stream: tokio::net::UnixStream) {
        assert!(
            FrameCodec::read_request(&mut stream).await.is_err(),
            "client reused a connection after a correlation/framing failure"
        );
    }

    async fn connect_writer(path: &std::path::Path) -> DriverSocketState {
        let mut client = DriverSocketState::connect(
            path.to_str().expect("utf8"),
            Some("default"),
            Some("writer"),
        )
        .await
        .expect("connect");
        // A bound on a hung test only; every exchange below is driven by explicit frames.
        client.io_timeout = Duration::from_secs(10);
        client
    }

    // req: DRV-001, DRV-005, DRV-012
    #[tokio::test]
    async fn socket_error_response_with_unrelated_request_id_is_not_attributed_to_invocation() {
        let sock = ScopedSocketPath::new("err-id");
        let listener = UnixListener::bind(&sock.0).expect("bind");

        let server = tokio::spawn(async move {
            // Command: an Error for another request after the command was sent.
            let mut stream = accept_verified(&listener).await;
            let _ = read_invoke_id(&mut stream).await;
            FrameCodec::write_response(
                &mut stream,
                &driver_error(Some("unrelated-request"), "RDB-DRIVER-0003"),
            )
            .await
            .expect("write unrelated error");
            assert_client_discarded(stream).await;

            // Query: an Error without any request identity.
            let mut stream = accept_verified(&listener).await;
            let _ = read_invoke_id(&mut stream).await;
            FrameCodec::write_response(&mut stream, &driver_error(None, "RDB-INPUT-0101"))
                .await
                .expect("write anonymous error");
            assert_client_discarded(stream).await;

            // A correlated Error is the request's own failure and keeps the session.
            let mut stream = accept_verified(&listener).await;
            let id = read_invoke_id(&mut stream).await;
            FrameCodec::write_response(&mut stream, &driver_error(Some(&id), "RDB-DRIVER-0003"))
                .await
                .expect("write correlated error");
            let id = read_invoke_id(&mut stream).await;
            FrameCodec::write_response(&mut stream, &found_result(id))
                .await
                .expect("write result");
        });

        let mut client = connect_writer(&sock.0).await;

        let err = client
            .invoke("orders", 1, None, sample_invoke("CreateOrder"), true)
            .await
            .expect_err("an unrelated error must not complete the command");
        assert_eq!(err.kind, "outcome_unknown");
        assert_eq!(err.details, None);

        let err = client
            .invoke("orders", 1, None, sample_invoke("GetOrder"), false)
            .await
            .expect_err("an anonymous error must not complete the query");
        assert_eq!(err.kind, "protocol_error");
        assert_eq!(err.details, None);

        let err = client
            .invoke("orders", 1, None, sample_invoke("GetOrder"), false)
            .await
            .expect_err("a correlated error is the query's failure");
        assert_eq!(err.kind, "invalid_input");
        client
            .invoke("orders", 1, None, sample_invoke("GetOrder"), false)
            .await
            .expect("session survives a correlated error");

        server.await.expect("server task");
    }

    // req: DRV-001, DRV-005
    #[tokio::test]
    async fn socket_handshake_error_with_unrelated_request_id_is_a_protocol_error() {
        let sock = ScopedSocketPath::new("hs-err-id");
        let listener = UnixListener::bind(&sock.0).expect("bind");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let DriverRequest::Handshake { .. } = FrameCodec::read_request(&mut stream)
                .await
                .expect("read handshake")
            else {
                panic!("expected handshake");
            };
            FrameCodec::write_response(
                &mut stream,
                &driver_error(Some("unrelated-request"), "RDB-DRIVER-0003"),
            )
            .await
            .expect("write unrelated handshake error");
            assert_client_discarded(stream).await;
        });

        let mut client = connect_writer(&sock.0).await;
        let err = client
            .invoke("orders", 1, None, sample_invoke("CreateOrder"), true)
            .await
            .expect_err("an unrelated handshake error must not be classified");
        assert_eq!(err.kind, "protocol_error");
        assert_eq!(err.details, None);

        server.await.expect("server task");
    }

    /// One unusable reply to an invocation that was already written.
    enum UnusableReply {
        Raw(Vec<u8>),
        Response(Box<DriverResponse>),
        WrongSuccessId,
    }

    fn unusable_replies() -> Vec<(&'static str, UnusableReply)> {
        let mut oversized = u32::try_from(crate::protocol::MAX_DRIVER_FRAME_BYTES + 1)
            .expect("frame bound fits u32")
            .to_be_bytes()
            .to_vec();
        oversized.extend_from_slice(b"{}");
        let empty_message = br#"{"type":"error","request_id":null,"code":"RDB-DRIVER-0003","category":"input","operation":null,"symbol_path":[],"contract_lineage":null,"contract_version":null,"trace_id":null,"incident_id":null,"message":"","retryability":"not_retryable","recovery_action":"correct_request","outcome_uncertain":false}"#;
        let mut invalid_bounds = u32::try_from(empty_message.len())
            .expect("small frame")
            .to_be_bytes()
            .to_vec();
        invalid_bounds.extend_from_slice(empty_message);
        assert!(matches!(
            FrameCodec::decode_response(&invalid_bounds),
            Err(crate::protocol::ProtocolError::InvalidBounds)
        ));
        vec![
            (
                "malformed body",
                UnusableReply::Raw(vec![0, 0, 0, 2, b'{', b'}']),
            ),
            ("oversized prefix", UnusableReply::Raw(oversized)),
            ("invalid bounds", UnusableReply::Raw(invalid_bounds)),
            ("mismatched success id", UnusableReply::WrongSuccessId),
            (
                "non-result variant",
                UnusableReply::Response(Box::new(handshake_ack("py-hs-0".to_owned()))),
            ),
        ]
    }

    // req: DRV-001, DRV-005, DRV-012
    #[tokio::test]
    async fn socket_command_unusable_post_send_response_reports_outcome_unknown() {
        for is_command in [true, false] {
            let sock = ScopedSocketPath::new("unusable");
            let listener = UnixListener::bind(&sock.0).expect("bind");
            let cases = unusable_replies();
            let labels = cases.iter().map(|(label, _)| *label).collect::<Vec<_>>();

            let server = tokio::spawn(async move {
                for (_, reply) in cases {
                    let mut stream = accept_verified(&listener).await;
                    let id = read_invoke_id(&mut stream).await;
                    match reply {
                        UnusableReply::Raw(bytes) => {
                            use tokio::io::AsyncWriteExt as _;
                            stream.write_all(&bytes).await.expect("write raw reply");
                        }
                        UnusableReply::Response(response) => {
                            FrameCodec::write_response(&mut stream, &response)
                                .await
                                .expect("write reply");
                        }
                        UnusableReply::WrongSuccessId => {
                            FrameCodec::write_response(
                                &mut stream,
                                &found_result(format!("{id}-other")),
                            )
                            .await
                            .expect("write mismatched result");
                        }
                    }
                    assert_client_discarded(stream).await;
                }
            });

            // Each case is one accepted connection; the first reuses the eager
            // connection opened by `connect`.
            let mut client = connect_writer(&sock.0).await;
            for label in labels {
                let err = client
                    .invoke("orders", 1, None, sample_invoke("CreateOrder"), is_command)
                    .await
                    .expect_err("an unusable reply must fail");
                let expected = if is_command {
                    "outcome_unknown"
                } else {
                    "protocol_error"
                };
                assert_eq!(err.kind, expected, "{label} (command: {is_command})");
                assert_eq!(err.details, None, "{label}");
            }

            server.await.expect("server task");
        }
    }
}
