#![forbid(unsafe_code)]

//! Private PyO3 bridge for the stable RiffDB application client.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule, PyType};
use riffdb_client_rust::{
    ApplicationCardinality, ApplicationClientError, ApplicationCommand, ApplicationContextualBatch,
    ApplicationContextualReaction, ApplicationContract, ApplicationEventBatch,
    ApplicationEventCheckpoint, ApplicationEventConsumerPublicStatus, ApplicationEventId,
    ApplicationEventLeaseEvidence, ApplicationEventMutationResult, ApplicationEventPullDisposition,
    ApplicationLiveQueryUpdate, ApplicationReactiveOperation, ApplicationRecord, ApplicationValue,
    AttemptBudget, BearerCredential, CallMetadata, ClientError, DatabaseAlias, DetailsFreeStatus,
    EventConsumerOptions, LiveQueryCursor, NamedQuery, QueryOptions, StableApplicationClient,
    TraceParent, load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
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

    #[classmethod]
    // PyO3 exposes this as keyword-only constructor fields; retaining the
    // closed scalar boundary avoids accepting an unvalidated mapping.
    #[allow(clippy::too_many_arguments)]
    fn connect_verified_tls(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        endpoint: String,
        trust_root: String,
        server_name: String,
        pool_connections: u32,
        streams_per_connection: u32,
        metadata: PyRef<'_, NativeCallMetadata>,
    ) -> PyResult<Self> {
        let tls = tls_config(
            endpoint,
            trust_root,
            server_name,
            pool_connections,
            streams_per_connection,
        )?;
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .map_err(|_| native_error("connection_failure", None))?;
        let client = py
            .detach(|| runtime.block_on(StableApplicationClient::connect_verified_tls(&tls)))
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
#[pyo3(signature = (
    endpoint,
    trust_root,
    server_name,
    pool_connections,
    streams_per_connection,
    metadata
))]
fn connect_async_verified_tls<'py>(
    py: Python<'py>,
    endpoint: String,
    trust_root: String,
    server_name: String,
    pool_connections: u32,
    streams_per_connection: u32,
    metadata: PyRef<'_, NativeCallMetadata>,
) -> PyResult<Bound<'py, PyAny>> {
    let tls = tls_config(
        endpoint,
        trust_root,
        server_name,
        pool_connections,
        streams_per_connection,
    )?;
    let metadata = metadata.inner.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let client = StableApplicationClient::connect_verified_tls(&tls)
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

    fn consume_event_stream<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_reactive_consumer(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let batch = client
                .consume_event_stream(&request.consumer, request.options, &metadata)
                .await
                .map_err(application_client_error)?;
            render_event_batch(batch)
        })
    }

    fn consume_contextual_subscription<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_contextual_consumer(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let batch = client
                .consume_contextual_subscription(
                    &request.consumer,
                    request.maximum_wait_nanos,
                    &metadata,
                )
                .await
                .map_err(application_client_error)?;
            render_contextual_batch(batch)
        })
    }

    fn mutate_contextual_subscription<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_reactive_mutation(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = match request.action.as_str() {
                "ack" => {
                    client
                        .acknowledge_contextual_lease(
                            &request.consumer,
                            &request.evidence,
                            &metadata,
                        )
                        .await
                }
                "nack" => {
                    client
                        .negative_acknowledge_contextual_lease(
                            &request.consumer,
                            &request.evidence,
                            request.retry_delay_nanos,
                            &metadata,
                        )
                        .await
                }
                _ => return Err(native_error("invalid_input", None)),
            }
            .map_err(application_client_error)?;
            render_event_mutation(result)
        })
    }

    fn contextual_subscription_status<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let consumer = parse_reactive_identity(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .contextual_subscription_status(&consumer, &metadata)
                .await
                .map_err(application_client_error)?;
            serialize(
                &result
                    .map(exact_event_status_json)
                    .unwrap_or(serde_json::Value::Null),
            )
        })
    }

    fn execute_contextual_reaction<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_contextual_reaction(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .execute_contextual_reaction(
                    &request.consumer,
                    &request.reaction,
                    request.command,
                    &metadata,
                )
                .await
                .map_err(application_client_error)?;
            validate_command_identity(&request.expected, &result)?;
            render_command_result(result)
        })
    }

    fn mutate_event_consumer<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_reactive_mutation(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = match request.action.as_str() {
                "ack" => {
                    client
                        .acknowledge_event_lease(&request.consumer, &request.evidence, &metadata)
                        .await
                }
                "nack" => {
                    client
                        .negative_acknowledge_event_lease(
                            &request.consumer,
                            &request.evidence,
                            request.retry_delay_nanos,
                            &metadata,
                        )
                        .await
                }
                _ => return Err(native_error("invalid_input", None)),
            }
            .map_err(application_client_error)?;
            render_event_mutation(result)
        })
    }

    fn seek_event_consumer<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_reactive_seek(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .seek_event_consumer(&request.consumer, request.checkpoint, &metadata)
                .await
                .map_err(application_client_error)?;
            render_event_mutation(result)
        })
    }

    fn event_consumer_status<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let consumer = parse_reactive_identity(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .event_consumer_status(&consumer, &metadata)
                .await
                .map_err(application_client_error)?;
            serialize(
                &result
                    .map(exact_event_status_json)
                    .unwrap_or(serde_json::Value::Null),
            )
        })
    }

    fn watch_named_query<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_live_query(&request)?;
        let mut client = self.client()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stream = client
                .watch_named_query(&request.operation, request.cursor, &metadata)
                .await
                .map_err(application_client_error)?;
            let update = stream
                .message()
                .await
                .map_err(application_client_error)?
                .ok_or_else(|| native_error("connection_failure", None))?;
            render_live_update(update)
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReactiveConsumerRequest {
    reactive_module_hash: String,
    operation_name: String,
    parameters: BTreeMap<String, Value>,
    consumer_name: String,
    batch_limit: Option<u32>,
    in_flight_limit: Option<u32>,
    lease_seconds: Option<u64>,
    maximum_wait_nanos: Option<u64>,
}

struct ParsedReactiveConsumer {
    consumer: riffdb_client_rust::ApplicationEventConsumer,
    options: EventConsumerOptions,
}

struct ParsedContextualConsumer {
    consumer: riffdb_client_rust::ApplicationEventConsumer,
    maximum_wait_nanos: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextualConsumerRequest {
    reactive_module_hash: String,
    operation_name: String,
    parameters: BTreeMap<String, Value>,
    consumer_name: String,
    maximum_wait_nanos: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextualReactionRequest {
    reactive_module_hash: String,
    operation_name: String,
    parameters: BTreeMap<String, Value>,
    consumer_name: String,
    reaction_name: String,
    command_id: u32,
    causation_token: String,
    contract_lineage: String,
    contract_version: u64,
    command_name: String,
    plan_hash: String,
    input: BTreeMap<String, Value>,
}

struct ParsedContextualReaction {
    consumer: riffdb_client_rust::ApplicationEventConsumer,
    reaction: ApplicationContextualReaction,
    command: ApplicationCommand,
    expected: CommandResponseIdentity,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReactiveIdentityRequest {
    reactive_module_hash: String,
    operation_name: String,
    parameters: BTreeMap<String, Value>,
    consumer_name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReactiveMutationRequest {
    reactive_module_hash: String,
    operation_name: String,
    parameters: BTreeMap<String, Value>,
    consumer_name: String,
    action: String,
    event_id: String,
    lease_token: String,
    history_incarnation: u64,
    retry_delay_nanos: Option<u64>,
}

struct ParsedReactiveMutation {
    consumer: riffdb_client_rust::ApplicationEventConsumer,
    action: String,
    evidence: ApplicationEventLeaseEvidence,
    retry_delay_nanos: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReactiveSeekRequest {
    reactive_module_hash: String,
    operation_name: String,
    parameters: BTreeMap<String, Value>,
    consumer_name: String,
    checkpoint: String,
}

struct ParsedReactiveSeek {
    consumer: riffdb_client_rust::ApplicationEventConsumer,
    checkpoint: ApplicationEventCheckpoint,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveQueryRequest {
    reactive_module_hash: String,
    operation_name: String,
    parameters: BTreeMap<String, Value>,
    cursor: Option<String>,
}

struct ParsedLiveQuery {
    operation: ApplicationReactiveOperation,
    cursor: Option<LiveQueryCursor>,
}

fn parse_reactive_values(
    module_hash: String,
    operation_name: String,
    parameters: BTreeMap<String, Value>,
) -> PyResult<ApplicationReactiveOperation> {
    let mut budget = ValueBudget::default();
    let parameters = parameters
        .into_iter()
        .map(|(name, value)| Ok((name, parse_value(value, 0, &mut budget)?)))
        .collect::<PyResult<BTreeMap<_, _>>>()?;
    ApplicationReactiveOperation::new(parse_hash(&module_hash)?, operation_name, parameters)
        .map_err(application_client_error)
}

fn parse_reactive_consumer(source: &str) -> PyResult<ParsedReactiveConsumer> {
    let request: ReactiveConsumerRequest = parse_json(source)?;
    let operation = parse_reactive_values(
        request.reactive_module_hash,
        request.operation_name,
        request.parameters,
    )?;
    let consumer =
        riffdb_client_rust::ApplicationEventConsumer::new(operation, request.consumer_name)
            .map_err(application_client_error)?;
    Ok(ParsedReactiveConsumer {
        consumer,
        options: EventConsumerOptions {
            batch_limit: request.batch_limit.unwrap_or(1),
            in_flight_limit: request.in_flight_limit.unwrap_or(16),
            lease_seconds: request.lease_seconds.unwrap_or(60),
            maximum_wait_nanos: request.maximum_wait_nanos.unwrap_or(30_000_000_000),
        },
    })
}

fn parse_contextual_consumer(source: &str) -> PyResult<ParsedContextualConsumer> {
    let request: ContextualConsumerRequest = parse_json(source)?;
    let maximum_wait_nanos = request.maximum_wait_nanos.unwrap_or(30_000_000_000);
    if maximum_wait_nanos > 30_000_000_000 {
        return Err(native_error("invalid_input", None));
    }
    Ok(ParsedContextualConsumer {
        consumer: reactive_consumer(
            request.reactive_module_hash,
            request.operation_name,
            request.parameters,
            request.consumer_name,
        )?,
        maximum_wait_nanos,
    })
}

fn parse_contextual_reaction(source: &str) -> PyResult<ParsedContextualReaction> {
    let request: ContextualReactionRequest = parse_json(source)?;
    if request.contract_lineage.is_empty() || request.contract_lineage.len() > 256 {
        return Err(native_error("invalid_input", None));
    }
    let plan_hash = parse_hash(&request.plan_hash)?;
    let mut budget = ValueBudget::default();
    let input = request
        .input
        .into_iter()
        .map(|(name, value)| Ok((name, parse_value(value, 0, &mut budget)?)))
        .collect::<PyResult<BTreeMap<_, _>>>()?;
    let command = ApplicationCommand::new(
        request.command_name.clone(),
        Some(request.contract_version),
        input,
    )
    .map_err(application_client_error)?;
    Ok(ParsedContextualReaction {
        consumer: reactive_consumer(
            request.reactive_module_hash,
            request.operation_name,
            request.parameters,
            request.consumer_name,
        )?,
        reaction: ApplicationContextualReaction::checked(
            request.reaction_name,
            request.command_name,
            request.command_id,
            parse_hex(&request.causation_token)?,
        )
        .map_err(application_client_error)?,
        command,
        expected: CommandResponseIdentity {
            contract_version: request.contract_version,
            plan_hash,
        },
    })
}

fn reactive_consumer(
    module_hash: String,
    operation_name: String,
    parameters: BTreeMap<String, Value>,
    consumer_name: String,
) -> PyResult<riffdb_client_rust::ApplicationEventConsumer> {
    riffdb_client_rust::ApplicationEventConsumer::new(
        parse_reactive_values(module_hash, operation_name, parameters)?,
        consumer_name,
    )
    .map_err(application_client_error)
}

fn parse_reactive_identity(source: &str) -> PyResult<riffdb_client_rust::ApplicationEventConsumer> {
    let request: ReactiveIdentityRequest = parse_json(source)?;
    reactive_consumer(
        request.reactive_module_hash,
        request.operation_name,
        request.parameters,
        request.consumer_name,
    )
}

fn parse_reactive_mutation(source: &str) -> PyResult<ParsedReactiveMutation> {
    let request: ReactiveMutationRequest = parse_json(source)?;
    let event_id = parse_event_id(&request.event_id)?;
    Ok(ParsedReactiveMutation {
        consumer: reactive_consumer(
            request.reactive_module_hash,
            request.operation_name,
            request.parameters,
            request.consumer_name,
        )?,
        action: request.action,
        evidence: ApplicationEventLeaseEvidence::new(
            event_id,
            parse_hex(&request.lease_token)?,
            request.history_incarnation,
        )
        .map_err(application_client_error)?,
        retry_delay_nanos: request.retry_delay_nanos.unwrap_or(0),
    })
}

fn parse_reactive_seek(source: &str) -> PyResult<ParsedReactiveSeek> {
    let request: ReactiveSeekRequest = parse_json(source)?;
    let checkpoint = if request.checkpoint == "before-first" {
        ApplicationEventCheckpoint::BeforeFirst
    } else {
        ApplicationEventCheckpoint::After(parse_event_id(&request.checkpoint)?)
    };
    Ok(ParsedReactiveSeek {
        consumer: reactive_consumer(
            request.reactive_module_hash,
            request.operation_name,
            request.parameters,
            request.consumer_name,
        )?,
        checkpoint,
    })
}

fn parse_event_id(value: &str) -> PyResult<ApplicationEventId> {
    let (commit_sequence, event_ordinal) = value
        .split_once(':')
        .ok_or_else(|| native_error("invalid_input", None))?;
    let commit_sequence = commit_sequence
        .parse::<u64>()
        .map_err(|_| native_error("invalid_input", None))?;
    let event_ordinal = event_ordinal
        .parse::<u32>()
        .map_err(|_| native_error("invalid_input", None))?;
    if commit_sequence == 0 {
        return Err(native_error("invalid_input", None));
    }
    Ok(ApplicationEventId {
        commit_sequence,
        event_ordinal,
    })
}

fn parse_live_query(source: &str) -> PyResult<ParsedLiveQuery> {
    let request: LiveQueryRequest = parse_json(source)?;
    Ok(ParsedLiveQuery {
        operation: parse_reactive_values(
            request.reactive_module_hash,
            request.operation_name,
            request.parameters,
        )?,
        cursor: request
            .cursor
            .map(|value| {
                LiveQueryCursor::from_standard_base64(&value).map_err(application_client_error)
            })
            .transpose()?,
    })
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
            riffdb_client_rust::ApplicationUuid::from_text(
                exact_string(object, "value")?.to_owned(),
            )
            .map_err(|_| native_error("invalid_input", None))?,
        )),
        "enum" if object.len() == 2 => Ok(ApplicationValue::Enum(
            exact_string(object, "value")?.to_owned(),
        )),
        "enum" if object.len() == 4 => Ok(ApplicationValue::EnumIdentity {
            type_id: exact_value(object, "type_id")?
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value != 0)
                .ok_or_else(|| native_error("invalid_input", None))?,
            variant_id: exact_value(object, "variant_id")?
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value != 0)
                .ok_or_else(|| native_error("invalid_input", None))?,
            name: exact_string(object, "value")?.to_owned(),
        }),
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

fn render_event_batch(batch: ApplicationEventBatch) -> PyResult<String> {
    let events = batch
        .events
        .into_iter()
        .map(|delivery| {
            let event = delivery.event;
            json!({
                "event_id": format!("{}:{}", event.id.commit_sequence, event.id.event_ordinal),
                "type": event.name,
                "fields": event.fields.into_iter().map(|(name, value)| (name, value_to_json(value))).collect::<Map<_, _>>(),
                "attempt": delivery.attempt,
                "lease_token": hex(&delivery.lease_token),
                "expires_at": {"seconds": delivery.expires_at.0, "nanos": delivery.expires_at.1},
                "history_incarnation": event.history_incarnation,
            })
        })
        .collect::<Vec<_>>();
    serialize(&json!({
        "events": events,
        "status": event_status_json(batch.status),
        "wait_timed_out": batch.wait_timed_out,
        "disposition": event_disposition_name(batch.disposition),
    }))
}

fn render_contextual_batch(batch: ApplicationContextualBatch) -> PyResult<String> {
    let items = batch
        .items
        .into_iter()
        .map(|item| {
            let delivery = item.delivery;
            let event = delivery.event;
            let hydrations = item
                .hydrations
                .into_iter()
                .map(|hydration| {
                    let fields = hydration
                        .fields
                        .into_iter()
                        .map(|(name, field)| {
                            let value = match field.cardinality {
                                ApplicationCardinality::One => field
                                    .records
                                    .into_iter()
                                    .next()
                                    .map(record_to_json)
                                    .unwrap_or(Value::Null),
                                ApplicationCardinality::Maybe => field
                                    .records
                                    .into_iter()
                                    .next()
                                    .map(record_to_json)
                                    .unwrap_or(Value::Null),
                                ApplicationCardinality::Many => Value::Array(
                                    field.records.into_iter().map(record_to_json).collect(),
                                ),
                            };
                            (name, value)
                        })
                        .collect::<Map<_, _>>();
                    json!({"name": hydration.name, "outcome": hydration.outcome, "fields": fields})
                })
                .collect::<Vec<_>>();
            let available_reactions = item
                .available_reactions
                .into_iter()
                .map(|reaction| {
                    json!({
                        "name": reaction.name,
                        "command_name": reaction.command_name,
                        "command_id": reaction.command_id,
                        "causation_token": hex(reaction.causation_token()),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "event_id": format!("{}:{}", event.id.commit_sequence, event.id.event_ordinal),
                "type": event.name,
                "event": event.fields.into_iter().map(|(name, value)| (name, value_to_json(value))).collect::<Map<_, _>>(),
                "attempt": delivery.attempt,
                "lease_token": hex(&delivery.lease_token),
                "expires_at": {"seconds": delivery.expires_at.0, "nanos": delivery.expires_at.1},
                "history_incarnation": event.history_incarnation,
                "context_head": item.context_head,
                "hydrations": hydrations,
                "available_reactions": available_reactions,
            })
        })
        .collect::<Vec<_>>();
    serialize(&json!({
        "items": items,
        "status": event_status_json(batch.status),
        "wait_timed_out": batch.wait_timed_out,
        "disposition": event_disposition_name(batch.disposition),
    }))
}

fn render_event_mutation(result: ApplicationEventMutationResult) -> PyResult<String> {
    let result = match result {
        ApplicationEventMutationResult::Applied => "applied",
        ApplicationEventMutationResult::StateChanged => "state_changed",
        ApplicationEventMutationResult::NotFound => "not_found",
        ApplicationEventMutationResult::OutstandingLease => "outstanding_lease",
        ApplicationEventMutationResult::StaleLease => "stale_lease",
        ApplicationEventMutationResult::LeaseExpired => "lease_expired",
    };
    serialize(&json!({"result": result}))
}

fn event_status_json(status: ApplicationEventConsumerPublicStatus) -> serde_json::Value {
    match status {
        ApplicationEventConsumerPublicStatus::Protected(status) => json!({
            "kind": "protected",
            "history_incarnation": status.history_incarnation,
            "progress_cursor": hex(status.progress_cursor.as_bytes()),
        }),
        ApplicationEventConsumerPublicStatus::Exact(status) => exact_event_status_json(status),
    }
}

fn exact_event_status_json(
    status: riffdb_client_rust::ApplicationEventConsumerStatus,
) -> serde_json::Value {
    let checkpoint = match status.checkpoint {
        ApplicationEventCheckpoint::BeforeFirst => "before-first".to_owned(),
        ApplicationEventCheckpoint::After(event_id) => {
            format!("{}:{}", event_id.commit_sequence, event_id.event_ordinal)
        }
    };
    json!({
        "revision": status.revision,
        "checkpoint": checkpoint,
        "history_incarnation": status.history_incarnation,
        "live_leases": status.live_leases,
        "retries": status.retries,
        "dead_letters": status.dead_letters,
    })
}

fn event_disposition_name(disposition: ApplicationEventPullDisposition) -> &'static str {
    match disposition {
        ApplicationEventPullDisposition::Ready => "ready",
        ApplicationEventPullDisposition::WaitTimedOut => "wait_timed_out",
        ApplicationEventPullDisposition::BoundedProgress => "bounded_progress",
    }
}

fn render_live_update(update: ApplicationLiveQueryUpdate) -> PyResult<String> {
    let value = match update {
        ApplicationLiveQueryUpdate::Snapshot {
            result,
            cursor,
            history_incarnation,
            application_head,
        } => json!({
            "type": "snapshot",
            "value": named_result_value(result)?,
            "cursor": cursor.to_standard_base64(),
            "history_incarnation": history_incarnation,
            "application_head": application_head,
        }),
        ApplicationLiveQueryUpdate::Patch(patch) => json!({
            "type": "patch",
            "result_field": patch.result_field,
            "operations": patch.operations.into_iter().map(live_patch_json).collect::<Vec<_>>(),
            "cursor": patch.checkpoint.cursor.to_standard_base64(),
            "history_incarnation": patch.checkpoint.history_incarnation,
            "application_head": patch.checkpoint.application_head,
        }),
        ApplicationLiveQueryUpdate::Reset {
            reason,
            result,
            cursor,
            history_incarnation,
            application_head,
        } => json!({
            "type": "reset",
            "reason": reason,
            "value": named_result_value(result)?,
            "cursor": cursor.to_standard_base64(),
            "history_incarnation": history_incarnation,
            "application_head": application_head,
        }),
        ApplicationLiveQueryUpdate::Checkpoint(checkpoint) => json!({
            "type": "checkpoint",
            "cursor": checkpoint.cursor.to_standard_base64(),
            "history_incarnation": checkpoint.history_incarnation,
            "application_head": checkpoint.application_head,
        }),
        ApplicationLiveQueryUpdate::Terminal(terminal) => json!({
            "type": "terminal",
            "reason": terminal.reason,
            "last_frontier": terminal.last_frontier.map(|(history_incarnation, application_head)| json!({
                "history_incarnation": history_incarnation,
                "application_head": application_head,
            })),
        }),
    };
    serialize(&value)
}

fn named_result_value(result: riffdb_client_rust::NamedQueryResult) -> PyResult<Value> {
    let mut value = Map::new();
    value.insert("outcome".to_owned(), Value::String(result.outcome));
    for (name, field) in result.fields {
        let rendered = match field.cardinality {
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
        value.insert(name, rendered);
    }
    Ok(Value::Object(value))
}

fn live_patch_json(operation: riffdb_client_rust::LiveQueryPatchOperation) -> Value {
    use riffdb_client_rust::LiveQueryPatchOperation;
    match operation {
        LiveQueryPatchOperation::Insert { index, record } => {
            json!({"type":"insert", "index":index, "record":record_to_json(record)})
        }
        LiveQueryPatchOperation::Remove { index, key } => json!({
            "type":"remove", "index":index,
            "key": key.into_iter().map(|(name, value)| (name, value_to_json(value))).collect::<Map<_, _>>(),
        }),
        LiveQueryPatchOperation::Replace { index, record } => {
            json!({"type":"replace", "index":index, "record":record_to_json(record)})
        }
        LiveQueryPatchOperation::Move { from, to, key } => json!({
            "type":"move", "from":from, "to":to,
            "key": key.into_iter().map(|(name, value)| (name, value_to_json(value))).collect::<Map<_, _>>(),
        }),
    }
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
        ApplicationValue::Uuid(value) => json!({"$riffdb": "uuid", "value": value.into_string()}),
        ApplicationValue::Enum(value) => json!({"$riffdb": "enum", "value": value}),
        ApplicationValue::EnumIdentity { name, .. } => {
            json!({"$riffdb": "enum", "value": name})
        }
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

fn tls_config(
    endpoint: String,
    trust_root: String,
    server_name: String,
    pool_connections: u32,
    streams_per_connection: u32,
) -> PyResult<TlsClientConfig> {
    let endpoint = CanonicalHttpsEndpoint::parse(&endpoint)
        .map_err(|_| native_error("invalid_input", None))?;
    let trust_root = ProtectedFilePath::new(Path::new(&trust_root).to_path_buf())
        .map_err(|_| native_error("invalid_input", None))?;
    let server_name =
        TlsServerIdentity::parse(&server_name).map_err(|_| native_error("invalid_input", None))?;
    let pool_connections = NonZeroU32::new(pool_connections)
        .filter(|value| value.get() <= 16)
        .ok_or_else(|| native_error("invalid_input", None))?;
    let streams_per_connection = NonZeroU32::new(streams_per_connection)
        .filter(|value| value.get() <= 256)
        .ok_or_else(|| native_error("invalid_input", None))?;
    TlsClientConfig::new(
        endpoint,
        trust_root,
        server_name,
        Duration::from_secs(5),
        Duration::from_secs(30),
        pool_connections,
        streams_per_connection,
    )
    .map_err(|_| native_error("invalid_input", None))
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
    native_error(non_application_client_error_kind(&error), None)
}

fn non_application_client_error_kind(error: &ClientError) -> &'static str {
    match error {
        ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable)
        | ClientError::ConnectionFailure
        | ClientError::IdentifierGeneration(_)
        | ClientError::Tls(_) => "connection_failure",
        ClientError::OutcomeUnknown(_) => "outcome_unknown",
        ClientError::Protocol(_) => "protocol_error",
        ClientError::Public(_) | ClientError::Application(_) | ClientError::DetailsFree(_) => {
            "protocol_error"
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
    module.add_function(wrap_pyfunction!(connect_async_verified_tls, module)?)?;
    module.add_function(wrap_pyfunction!(validate_bridge_value, module)?)?;
    Ok(())
}
