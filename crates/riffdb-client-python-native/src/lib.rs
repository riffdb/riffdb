#![forbid(unsafe_code)]

//! Private PyO3 bridge for the stable RiffDB application client.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule, PyType};
use riffdb_client_rust::{
    ApplicationCardinality, ApplicationClientError, ApplicationCommand, ApplicationContract,
    ApplicationRecord, ApplicationValue, AttemptBudget, BearerCredential, CallMetadata,
    ClientError, DatabaseAlias, DetailsFreeStatus, NamedQuery, QueryOptions,
    StableApplicationClient, TraceParent, load_protected_bearer_credential,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::runtime::{Builder, Runtime};

const MAX_BRIDGE_BYTES: usize = 4 * 1_024 * 1_024;
const MAX_BRIDGE_DEPTH: usize = 32;
const MAX_BRIDGE_VALUES: usize = 100_000;

mod exceptions {
    #![allow(missing_docs)]

    use pyo3::create_exception;
    use pyo3::exceptions::PyException;

    create_exception!(_native, NativeError, PyException);
}

use exceptions::NativeError;

#[pyclass(
    name = "_BearerCredential",
    module = "riffdb_application._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
struct NativeBearerCredential {
    inner: BearerCredential,
}

#[pymethods]
impl NativeBearerCredential {
    #[new]
    fn new(token: &str) -> PyResult<Self> {
        BearerCredential::new(token)
            .map(|inner| Self { inner })
            .map_err(|_| native_error("invalid_input", None))
    }

    #[staticmethod]
    fn from_protected_file(path: &str) -> PyResult<Self> {
        load_protected_bearer_credential(Path::new(path))
            .map(|inner| Self { inner })
            .map_err(|_| native_error("invalid_input", None))
    }

    fn has_same_presentation(&self, other: &Self) -> bool {
        self.inner.has_same_presentation(&other.inner)
    }

    fn __repr__(&self) -> &'static str {
        "BearerCredential([REDACTED])"
    }

    fn __reduce__(&self) -> PyResult<()> {
        Err(PyTypeError::new_err(
            "bearer credentials cannot be serialized",
        ))
    }
}

#[pyclass(
    name = "_CallMetadata",
    module = "riffdb_application._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
struct NativeCallMetadata {
    inner: CallMetadata,
}

#[pymethods]
impl NativeCallMetadata {
    #[new]
    #[pyo3(signature = (credential=None, database=None, trace_parent=None))]
    fn new(
        credential: Option<PyRef<'_, NativeBearerCredential>>,
        database: Option<String>,
        trace_parent: Option<String>,
    ) -> PyResult<Self> {
        let mut inner = credential.map_or_else(CallMetadata::default, |value| {
            CallMetadata::authenticated(value.inner.clone())
        });
        if let Some(database) = database {
            inner = inner.with_database(
                DatabaseAlias::new(database).map_err(|_| native_error("invalid_input", None))?,
            );
        }
        if let Some(trace_parent) = trace_parent {
            inner = inner.with_trace_parent(
                TraceParent::new(&trace_parent).map_err(|_| native_error("invalid_input", None))?,
            );
        }
        Ok(Self { inner })
    }

    fn __repr__(&self) -> &'static str {
        "CallMetadata([CHECKED])"
    }
}

#[pyclass(name = "_SyncClient", module = "riffdb_application._native")]
struct NativeSyncClient {
    runtime: Runtime,
    client: Mutex<Option<StableApplicationClient>>,
    metadata: CallMetadata,
}

#[pymethods]
impl NativeSyncClient {
    #[classmethod]
    fn connect_uri(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        endpoint: String,
        metadata: PyRef<'_, NativeCallMetadata>,
    ) -> PyResult<Self> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .map_err(|_| native_error("connection_failure", None))?;
        let client = py
            .detach(|| runtime.block_on(StableApplicationClient::connect_uri(endpoint)))
            .map_err(client_error)?;
        Ok(Self {
            runtime,
            client: Mutex::new(Some(client)),
            metadata: metadata.inner.clone(),
        })
    }

    fn execute_named_query(&self, py: Python<'_>, request: &str) -> PyResult<String> {
        let request = parse_query(request)?;
        let mut client = self.client()?;
        let result = py
            .detach(|| {
                self.runtime
                    .block_on(client.execute_named_query(request, &self.metadata))
            })
            .map_err(application_client_error)?;
        render_query_result(result)
    }

    fn execute_command(&self, py: Python<'_>, request: &str) -> PyResult<String> {
        let request = parse_command(request)?;
        let mut client = self.client()?;
        let result = py
            .detach(|| {
                self.runtime.block_on(client.execute_command(
                    request.command,
                    request.attempts,
                    &self.metadata,
                ))
            })
            .map_err(application_client_error)?;
        validate_command_identity(&request.expected, &result)?;
        render_command_result(result)
    }

    fn close(&self) -> PyResult<()> {
        self.client
            .lock()
            .map_err(|_| native_error("connection_failure", None))?
            .take();
        Ok(())
    }
}

impl NativeSyncClient {
    fn client(&self) -> PyResult<StableApplicationClient> {
        self.client
            .lock()
            .map_err(|_| native_error("connection_failure", None))?
            .as_ref()
            .cloned()
            .ok_or_else(|| native_error("connection_failure", None))
    }
}

#[pyclass(
    name = "_AsyncClient",
    module = "riffdb_application._native",
    skip_from_py_object
)]
struct NativeAsyncClient {
    client: Mutex<Option<StableApplicationClient>>,
    metadata: CallMetadata,
}

#[pyfunction]
fn connect_async<'py>(
    py: Python<'py>,
    endpoint: String,
    metadata: PyRef<'_, NativeCallMetadata>,
) -> PyResult<Bound<'py, PyAny>> {
    let metadata = metadata.inner.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let client = StableApplicationClient::connect_uri(endpoint)
            .await
            .map_err(client_error)?;
        Python::attach(|py| {
            Py::new(
                py,
                NativeAsyncClient {
                    client: Mutex::new(Some(client)),
                    metadata,
                },
            )
        })
    })
}

#[pyfunction]
fn validate_bridge_value(source: &str) -> PyResult<()> {
    let value: Value = parse_json(source)?;
    parse_value(value, 0, &mut ValueBudget::default()).map(|_| ())
}

#[pymethods]
impl NativeAsyncClient {
    fn execute_named_query<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_query(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .execute_named_query(request, &metadata)
                .await
                .map_err(application_client_error)?;
            render_query_result(result)
        })
    }

    fn execute_command<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_command(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .execute_command(request.command, request.attempts, &metadata)
                .await
                .map_err(application_client_error)?;
            validate_command_identity(&request.expected, &result)?;
            render_command_result(result)
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.client
            .lock()
            .map_err(|_| native_error("connection_failure", None))?
            .take();
        pyo3_async_runtimes::tokio::future_into_py(py, async { Ok(()) })
    }
}

impl NativeAsyncClient {
    fn client(&self) -> PyResult<StableApplicationClient> {
        self.client
            .lock()
            .map_err(|_| native_error("connection_failure", None))?
            .as_ref()
            .cloned()
            .ok_or_else(|| native_error("connection_failure", None))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRequest {
    contract_lineage: String,
    contract_version: u64,
    contract_bundle_hash: String,
    module_hash: String,
    query_name: String,
    plan_hash: String,
    parameters: BTreeMap<String, Value>,
    cursor: Option<String>,
    read_after_commit: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandRequest {
    contract_lineage: String,
    contract_version: u64,
    command_name: String,
    plan_hash: String,
    input: BTreeMap<String, Value>,
    maximum_submissions: u32,
}

fn parse_query(source: &str) -> PyResult<NamedQuery> {
    let request: QueryRequest = parse_json(source)?;
    let bundle_hash = parse_hash(&request.contract_bundle_hash)?;
    let module_hash = parse_hash(&request.module_hash)?;
    let plan_hash = parse_hash(&request.plan_hash)?;
    let mut budget = ValueBudget::default();
    let parameters = request
        .parameters
        .into_iter()
        .map(|(name, value)| Ok((name, parse_value(value, 0, &mut budget)?)))
        .collect::<PyResult<BTreeMap<_, _>>>()?;
    let mut options = QueryOptions::new();
    if let Some(cursor) = request.cursor {
        options = options.after(cursor);
    }
    if let Some(sequence) = request.read_after_commit {
        options = options.read_after_commit(sequence);
    }
    NamedQuery::new(
        ApplicationContract::Exact {
            lineage: request.contract_lineage,
            version: request.contract_version,
            bundle_hash: Some(bundle_hash),
        },
        request.query_name,
        Some(module_hash),
        parameters,
        None,
    )
    .and_then(|query| query.expect_plan_hash(plan_hash).with_options(options))
    .map_err(application_client_error)
}

struct ParsedCommand {
    command: ApplicationCommand,
    attempts: AttemptBudget,
    expected: CommandResponseIdentity,
}

struct CommandResponseIdentity {
    contract_version: u64,
    plan_hash: [u8; 32],
}

fn parse_command(source: &str) -> PyResult<ParsedCommand> {
    let request: CommandRequest = parse_json(source)?;
    let plan_hash = parse_hash(&request.plan_hash)?;
    if request.contract_lineage.is_empty() || request.contract_lineage.len() > 256 {
        return Err(native_error("invalid_input", None));
    }
    let mut budget = ValueBudget::default();
    let input = request
        .input
        .into_iter()
        .map(|(name, value)| Ok((name, parse_value(value, 0, &mut budget)?)))
        .collect::<PyResult<BTreeMap<_, _>>>()?;
    let command =
        ApplicationCommand::new(request.command_name, Some(request.contract_version), input)
            .map_err(application_client_error)?;
    let attempts = AttemptBudget::new(request.maximum_submissions)
        .ok_or_else(|| native_error("invalid_input", None))?;
    Ok(ParsedCommand {
        command,
        attempts,
        expected: CommandResponseIdentity {
            contract_version: request.contract_version,
            plan_hash,
        },
    })
}

fn validate_command_identity(
    expected: &CommandResponseIdentity,
    result: &riffdb_client_rust::ApplicationCommandResult,
) -> PyResult<()> {
    if result.contract_version != expected.contract_version
        || result.plan_hash != expected.plan_hash
    {
        return Err(native_error("protocol_error", None));
    }
    Ok(())
}

#[derive(Default)]
struct ValueBudget {
    values: usize,
}

fn parse_value(value: Value, depth: usize, budget: &mut ValueBudget) -> PyResult<ApplicationValue> {
    if depth > MAX_BRIDGE_DEPTH {
        return Err(native_error("invalid_input", None));
    }
    budget.values = budget
        .values
        .checked_add(1)
        .ok_or_else(|| native_error("invalid_input", None))?;
    if budget.values > MAX_BRIDGE_VALUES {
        return Err(native_error("invalid_input", None));
    }
    let object = value
        .as_object()
        .ok_or_else(|| native_error("invalid_input", None))?;
    let kind = exact_string(object, "kind")?;
    match kind {
        "null" if object.len() == 1 => Ok(ApplicationValue::Null),
        "bool" if object.len() == 2 => exact_value(object, "value")?
            .as_bool()
            .map(ApplicationValue::Bool)
            .ok_or_else(|| native_error("invalid_input", None)),
        "i64" if object.len() == 2 => exact_value(object, "value")?
            .as_i64()
            .map(ApplicationValue::I64)
            .ok_or_else(|| native_error("invalid_input", None)),
        "u64" if object.len() == 2 => exact_value(object, "value")?
            .as_u64()
            .map(ApplicationValue::U64)
            .ok_or_else(|| native_error("invalid_input", None)),
        "string" if object.len() == 2 => Ok(ApplicationValue::String(
            exact_string(object, "value")?.to_owned(),
        )),
        "uuid" if object.len() == 2 => Ok(ApplicationValue::Uuid(
            exact_string(object, "value")?.to_owned(),
        )),
        "enum" if object.len() == 2 => Ok(ApplicationValue::Enum(
            exact_string(object, "value")?.to_owned(),
        )),
        "bytes" if object.len() == 2 => Ok(ApplicationValue::Bytes(parse_hex(exact_string(
            object, "value",
        )?)?)),
        "date" if object.len() == 2 => Ok(ApplicationValue::Date(
            exact_value(object, "value")?
                .as_i64()
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| native_error("invalid_input", None))?,
        )),
        "timestamp" if object.len() == 3 => Ok(ApplicationValue::Timestamp {
            seconds: exact_value(object, "seconds")?
                .as_i64()
                .ok_or_else(|| native_error("invalid_input", None))?,
            nanos: exact_value(object, "nanos")?
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value < 1_000_000_000)
                .ok_or_else(|| native_error("invalid_input", None))?,
        }),
        "decimal" if matches!(object.len(), 3 | 4) => Ok(ApplicationValue::Decimal {
            coefficient_twos_complement: parse_hex(exact_string(object, "coefficient")?)?,
            scale: exact_value(object, "scale")?
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| native_error("invalid_input", None))?,
            precision: object
                .get("precision")
                .map(|value| {
                    value
                        .as_u64()
                        .and_then(|value| u32::try_from(value).ok())
                        .ok_or_else(|| native_error("invalid_input", None))
                })
                .transpose()?,
        }),
        "money" if object.len() == 3 => Ok(ApplicationValue::Money {
            currency: exact_string(object, "currency")?.to_owned(),
            amount: Box::new(parse_value(
                exact_value(object, "amount")?.clone(),
                depth + 1,
                budget,
            )?),
        }),
        "list" if object.len() == 2 => Ok(ApplicationValue::List(
            exact_value(object, "value")?
                .as_array()
                .ok_or_else(|| native_error("invalid_input", None))?
                .iter()
                .cloned()
                .map(|value| parse_value(value, depth + 1, budget))
                .collect::<PyResult<Vec<_>>>()?,
        )),
        "record" if object.len() == 2 => Ok(ApplicationValue::Record(
            exact_value(object, "value")?
                .as_object()
                .ok_or_else(|| native_error("invalid_input", None))?
                .iter()
                .map(|(name, value)| {
                    Ok((name.clone(), parse_value(value.clone(), depth + 1, budget)?))
                })
                .collect::<PyResult<BTreeMap<_, _>>>()?,
        )),
        _ => Err(native_error("invalid_input", None)),
    }
}

fn render_query_result(result: riffdb_client_rust::NamedQueryResult) -> PyResult<String> {
    let mut value = Map::new();
    value.insert("outcome".to_owned(), Value::String(result.outcome));
    for (name, field) in result.fields {
        let value_json = match field.cardinality {
            ApplicationCardinality::One => record_to_json(
                field
                    .records
                    .into_iter()
                    .next()
                    .ok_or_else(|| native_error("protocol_error", None))?,
            ),
            ApplicationCardinality::Maybe => field
                .records
                .into_iter()
                .next()
                .map(record_to_json)
                .unwrap_or(Value::Null),
            ApplicationCardinality::Many => {
                Value::Array(field.records.into_iter().map(record_to_json).collect())
            }
        };
        value.insert(name, value_json);
    }
    serialize(&json!({
        "value": value,
        "identity": {
            "contract_lineage": result.identity.contract_lineage,
            "contract_version": result.identity.contract_version,
            "contract_bundle_hash": hex(&result.identity.contract_bundle_hash),
            "module_hash": hex(&result.identity.module_hash),
            "query_name": result.identity.query_name,
            "plan_hash": hex(&result.identity.plan_hash),
        },
        "application_head": result.application_head,
        "next_cursor": result.next_cursor,
    }))
}

fn render_command_result(result: riffdb_client_rust::ApplicationCommandResult) -> PyResult<String> {
    let mut outcome = match result.outcome_value {
        Some(ApplicationValue::Record(fields)) => fields
            .into_iter()
            .map(|(name, value)| (name, value_to_json(value)))
            .collect(),
        Some(value) => {
            let mut object = Map::new();
            object.insert("value".to_owned(), value_to_json(value));
            object
        }
        None => Map::new(),
    };
    if let Some(name) = result.outcome {
        outcome.insert("outcome".to_owned(), Value::String(name));
    }
    serialize(&json!({
        "outcome": outcome,
        "commit_sequence": result.commit_sequence,
        "contract_version": result.contract_version,
        "plan_hash": hex(&result.plan_hash),
        "replayed": result.replayed,
        "outcome_uri": result.outcome_uri,
    }))
}

fn record_to_json(record: ApplicationRecord) -> Value {
    Value::Object(
        record
            .fields
            .into_iter()
            .map(|(name, value)| (name, value_to_json(value)))
            .collect(),
    )
}

fn value_to_json(value: ApplicationValue) -> Value {
    match value {
        ApplicationValue::Null => Value::Null,
        ApplicationValue::Bool(value) => Value::Bool(value),
        ApplicationValue::I64(value) => json!(value),
        ApplicationValue::U64(value) => json!(value),
        ApplicationValue::Decimal {
            coefficient_twos_complement,
            scale,
            precision,
        } => {
            json!({"$riffdb": "decimal", "coefficient": hex(&coefficient_twos_complement), "scale": scale, "precision": precision})
        }
        ApplicationValue::Money { currency, amount } => {
            json!({"$riffdb": "money", "currency": currency, "amount": value_to_json(*amount)})
        }
        ApplicationValue::String(value) => Value::String(value),
        ApplicationValue::Uuid(value) => json!({"$riffdb": "uuid", "value": value}),
        ApplicationValue::Enum(value) => json!({"$riffdb": "enum", "value": value}),
        ApplicationValue::Bytes(value) => json!({"$riffdb": "bytes", "value": hex(&value)}),
        ApplicationValue::Date(value) => json!({"$riffdb": "date", "value": value}),
        ApplicationValue::Timestamp { seconds, nanos } => {
            json!({"$riffdb": "timestamp", "seconds": seconds, "nanos": nanos})
        }
        ApplicationValue::List(values) => {
            Value::Array(values.into_iter().map(value_to_json).collect())
        }
        ApplicationValue::Record(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(name, value)| (name, value_to_json(value)))
                .collect(),
        ),
    }
}

fn parse_json<T: for<'de> Deserialize<'de>>(source: &str) -> PyResult<T> {
    if source.is_empty() || source.len() > MAX_BRIDGE_BYTES {
        return Err(native_error("invalid_input", None));
    }
    serde_json::from_str(source).map_err(|_| native_error("invalid_input", None))
}

fn serialize(value: &Value) -> PyResult<String> {
    serde_json::to_string(value).map_err(|_| native_error("protocol_error", None))
}

fn parse_hash(value: &str) -> PyResult<[u8; 32]> {
    let bytes = parse_hex(value)?;
    bytes
        .try_into()
        .map_err(|_| native_error("invalid_input", None))
}

fn parse_hex(value: &str) -> PyResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) || value.len() > MAX_BRIDGE_BYTES * 2 {
        return Err(native_error("invalid_input", None));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text =
                std::str::from_utf8(pair).map_err(|_| native_error("invalid_input", None))?;
            u8::from_str_radix(text, 16).map_err(|_| native_error("invalid_input", None))
        })
        .collect()
}

fn exact_value<'a>(object: &'a Map<String, Value>, key: &str) -> PyResult<&'a Value> {
    object
        .get(key)
        .ok_or_else(|| native_error("invalid_input", None))
}

fn exact_string<'a>(object: &'a Map<String, Value>, key: &str) -> PyResult<&'a str> {
    exact_value(object, key)?
        .as_str()
        .ok_or_else(|| native_error("invalid_input", None))
}

fn application_client_error(error: ApplicationClientError) -> PyErr {
    match error {
        ApplicationClientError::InvalidInput => native_error("invalid_input", None),
        ApplicationClientError::InvalidResponse => native_error("protocol_error", None),
        ApplicationClientError::IdentifierUnavailable => native_error("connection_failure", None),
        ApplicationClientError::Client(error) => client_error(error),
    }
}

fn client_error(error: ClientError) -> PyErr {
    if let Some(error) = error.application_error() {
        return native_error("application", Some(application_error_json(error)));
    }
    match error {
        ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable)
        | ClientError::ConnectionFailure
        | ClientError::IdentifierGeneration(_) => native_error("connection_failure", None),
        ClientError::OutcomeUnknown(_) => native_error("outcome_unknown", None),
        ClientError::Protocol(_) => native_error("protocol_error", None),
        ClientError::Public(_) | ClientError::Application(_) | ClientError::DetailsFree(_) => {
            native_error("protocol_error", None)
        }
    }
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

fn native_error(kind: &str, details: Option<Value>) -> PyErr {
    let details = details
        .and_then(|value| serde_json::to_string(&value).ok())
        .unwrap_or_default();
    NativeError::new_err((kind.to_owned(), details))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("String writes cannot fail");
    }
    output
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("NativeError", module.py().get_type::<NativeError>())?;
    module.add_class::<NativeBearerCredential>()?;
    module.add_class::<NativeCallMetadata>()?;
    module.add_class::<NativeSyncClient>()?;
    module.add_class::<NativeAsyncClient>()?;
    module.add_function(wrap_pyfunction!(connect_async, module)?)?;
    module.add_function(wrap_pyfunction!(validate_bridge_value, module)?)?;
    Ok(())
}
