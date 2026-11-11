#![forbid(unsafe_code)]

//! Private PyO3 bridge for the stable RiffDB application client.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule, PyType};
use riffdb_config::{
    CanonicalHttpsEndpoint, ClientTrustRootConfig, ProtectedFilePath, TlsClientConfig,
    TlsServerIdentity,
};
use riffdb_driver_host::in_process::{
    ApplicationCardinality, ApplicationClientError, ApplicationCommand, ApplicationCommandResult,
    ApplicationContextualBatch, ApplicationContextualReaction, ApplicationContract,
    ApplicationEventBatch, ApplicationEventCheckpoint, ApplicationEventConsumer,
    ApplicationEventConsumerPublicStatus, ApplicationEventConsumerStatus, ApplicationEventId,
    ApplicationEventLeaseEvidence, ApplicationEventMutationResult, ApplicationEventProgressCursor,
    ApplicationEventPullDisposition, ApplicationLiveQueryStream, ApplicationLiveQueryUpdate,
    ApplicationReactiveOperation, ApplicationRecord, ApplicationValue, BearerCredential,
    CallMetadata, ClientError, DatabaseAlias, EventConsumerOptions, LiveQueryCursor,
    LiveQueryPatchOperation, NamedQueryResult, StableApplicationClient, TraceParent,
    VectorStateInspection, VectorStateInspectionKind, VectorStateInspectionResult, app_v1,
    load_protected_bearer_credential, raise_query_result, raise_value as raise_wire_value,
};
use riffdb_driver_host::{
    BindingError, DriverRequest, DriverResponse, DriverSocketState, DriverValue, InvokeOptions,
    ProtocolCoreError, QueryDispatchResult, application_value_to_python_json,
    classify_application_client_error, classify_client_error, dispatch_command,
    dispatch_named_query, lower_driver_value, normalize_python_value, parse_in_process_command,
    parse_in_process_query, raise_driver_value, validate_driver_role_name,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::runtime::{Builder, Runtime};

const MAX_BRIDGE_BYTES: usize = 4 * 1_024 * 1_024;

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
    frozen
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

#[pyclass(name = "_CallMetadata", module = "riffdb_application._native", frozen)]
#[derive(Clone)]
struct NativeCallMetadata {
    inner: CallMetadata,
    database: Option<String>,
    role: Option<String>,
}

#[pymethods]
impl NativeCallMetadata {
    #[new]
    #[pyo3(signature = (credential=None, database=None, trace_parent=None, role=None))]
    fn new(
        credential: Option<PyRef<'_, NativeBearerCredential>>,
        database: Option<String>,
        trace_parent: Option<String>,
        role: Option<String>,
    ) -> PyResult<Self> {
        let mut inner = credential.map_or_else(CallMetadata::default, |value| {
            CallMetadata::authenticated(value.inner.clone())
        });
        let validated_db = if let Some(database) = database {
            let alias = DatabaseAlias::new(database.clone())
                .map_err(|_| native_error("invalid_input", None))?;
            inner = inner.with_database(alias);
            Some(database)
        } else {
            None
        };
        if let Some(trace_parent) = trace_parent {
            inner = inner.with_trace_parent(
                TraceParent::new(&trace_parent).map_err(|_| native_error("invalid_input", None))?,
            );
        }
        let validated_role = role
            .as_deref()
            .map(validate_driver_role_name)
            .transpose()
            .map_err(binding_error)?;
        Ok(Self {
            inner,
            database: validated_db,
            role: validated_role,
        })
    }

    fn __repr__(&self) -> &'static str {
        "CallMetadata([CHECKED])"
    }
}

#[derive(Clone)]
enum ClientBackend {
    Direct(Box<StableApplicationClient>),
    DriverSocket(std::sync::Arc<tokio::sync::Mutex<DriverSocketState>>),
}

fn is_driver_socket_endpoint(endpoint: &str) -> bool {
    endpoint.starts_with("unix://") || endpoint.starts_with('/')
}

async fn execute_socket_named_query(
    socket: &tokio::sync::Mutex<DriverSocketState>,
    parsed: riffdb_driver_host::InProcessQuery,
) -> PyResult<String> {
    let lineage = parsed.contract_lineage;
    let version = parsed.contract_version;
    let bundle_hash_hex = parsed.contract_bundle_hash;
    let module_hash_hex = parsed.module_hash;
    let query_name = parsed.query_name;
    let plan_hash_hex = parsed.plan_hash;
    let mut invoke_request = parsed.request;
    if let DriverRequest::Invoke {
        input_schema_hash, ..
    } = &mut invoke_request
    {
        *input_schema_hash = plan_hash_hex.clone();
    }
    let (response, session) = socket
        .lock()
        .await
        .invoke(
            &lineage,
            version,
            Some(&bundle_hash_hex),
            invoke_request,
            false,
        )
        .await
        .map_err(binding_error)?;
    let identity = json!({
        "contract_lineage": session.contract_lineage,
        "contract_version": session.contract_version,
        "contract_bundle_hash": session.contract_bundle_hash,
        "module_hash": module_hash_hex,
        "query_name": query_name,
        "plan_hash": plan_hash_hex,
    });
    match response {
        DriverResponse::PackedQueryResult {
            outcome,
            result_name,
            entity,
            fields,
            row_count,
            columns,
            application_head,
            cursor,
            ..
        } => {
            let columns_json = columns
                .into_iter()
                .map(|col| json!({ "data": col.data, "offsets": col.offsets }))
                .collect::<Vec<_>>();
            serialize(&json!({
                "value": {
                    "$riffdb_packed": {
                        "outcome": outcome,
                        "result_name": result_name,
                        "entity": entity,
                        "fields": fields,
                        "row_count": row_count,
                        "columns": columns_json,
                    }
                },
                "identity": identity,
                "application_head": application_head,
                "next_cursor": cursor,
            }))
        }
        DriverResponse::CompactQueryResult {
            outcome,
            result_name,
            entity,
            fields,
            rows,
            application_head,
            cursor,
            ..
        } => {
            let rows_json = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| {
                            lower_driver_value(cell)
                                .map(value_to_json)
                                .map_err(|_| native_error("protocol_error", None))
                        })
                        .collect::<PyResult<Vec<_>>>()
                })
                .collect::<PyResult<Vec<_>>>()?;
            serialize(&json!({
                "value": {
                    "$riffdb_compact": {
                        "outcome": outcome,
                        "result_name": result_name,
                        "entity": entity,
                        "fields": fields,
                        "rows": rows_json,
                    }
                },
                "identity": identity,
                "application_head": application_head,
                "next_cursor": cursor,
            }))
        }
        DriverResponse::Result {
            value,
            application_head,
            cursor,
            ..
        } => {
            let value_json = driver_record_with_outcome_to_python_json(value)?;
            serialize(&json!({
                "value": value_json,
                "identity": identity,
                "application_head": application_head.unwrap_or(0),
                "next_cursor": cursor,
            }))
        }
        _ => Err(native_error("protocol_error", None)),
    }
}

async fn execute_socket_command(
    socket: &tokio::sync::Mutex<DriverSocketState>,
    parsed: riffdb_driver_host::InProcessCommand,
) -> PyResult<String> {
    let plan_hash_hex = hex(&parsed.expected_plan_hash);
    let mut invoke_request = parsed.request;
    if let DriverRequest::Invoke {
        input_schema_hash, ..
    } = &mut invoke_request
    {
        *input_schema_hash = plan_hash_hex.clone();
    }
    let (response, session) = socket
        .lock()
        .await
        .invoke(
            &parsed.contract_lineage,
            parsed.expected_contract_version,
            None,
            invoke_request,
            true,
        )
        .await
        .map_err(binding_error)?;
    // The host may have committed: a reply that cannot be presented exactly
    // leaves the outcome uncertain rather than reporting a protocol failure.
    let unpresentable = || native_error("outcome_unknown", None);
    if session.contract_version != parsed.expected_contract_version {
        return Err(unpresentable());
    }
    let DriverResponse::Result {
        value,
        application_head,
        cursor,
        replayed,
        ..
    } = response
    else {
        return Err(unpresentable());
    };
    let outcome = driver_record_with_outcome_to_python_json(value).map_err(|_| unpresentable())?;
    serialize(&json!({
        "outcome": outcome,
        "commit_sequence": application_head,
        "contract_version": session.contract_version,
        "plan_hash": plan_hash_hex,
        "replayed": replayed,
        "outcome_uri": cursor,
    }))
}

async fn execute_socket_vector_inspection(
    socket: &tokio::sync::Mutex<DriverSocketState>,
    parsed: ParsedVectorInspection,
) -> PyResult<String> {
    let invoke_request = DriverRequest::Invoke {
        request_id: "in-process-binding".to_owned(),
        operation: parsed.operation,
        input_schema_hash: parsed.contract_bundle_hash.clone(),
        input: BTreeMap::from([
            ("partition".to_owned(), parsed.partition),
            (
                "limit".to_owned(),
                DriverValue::U64(parsed.limit.to_string()),
            ),
        ]),
        options: InvokeOptions {
            deadline_millis: 30_000,
            maximum_attempts: 1,
            read_after_commit: None,
            cursor: parsed.cursor.map(|bytes| BASE64.encode(bytes)),
            query_consistency: None,
            accept_compact_result: false,
            accept_packed_result: false,
        },
    };
    let (response, _session) = socket
        .lock()
        .await
        .invoke(
            &parsed.contract_lineage,
            parsed.contract_version,
            Some(&parsed.contract_bundle_hash),
            invoke_request,
            false,
        )
        .await
        .map_err(binding_error)?;
    let DriverResponse::Result { value, cursor, .. } = response else {
        return Err(native_error("protocol_error", None));
    };
    let mut record = driver_record_with_outcome_to_python_json(value)?;
    if let Some(kind_obj) = record.get("kind").and_then(Value::as_object)
        && kind_obj.get("$riffdb").and_then(Value::as_str) == Some("enum")
        && let Some(kind_name) = kind_obj.get("value").and_then(Value::as_str)
    {
        record.insert("kind".to_owned(), Value::String(kind_name.to_owned()));
    }
    if matches!(
        record.get("kind").and_then(Value::as_str),
        Some("stale_entities" | "outdated_model_entities")
    ) {
        let next_cursor_hex = match cursor {
            Some(cursor_b64) => Some(
                BASE64
                    .decode(cursor_b64)
                    .map(|bytes| hex(&bytes))
                    .map_err(|_| native_error("protocol_error", None))?,
            ),
            None => None,
        };
        record.insert(
            "next_cursor".to_owned(),
            next_cursor_hex.map_or(Value::Null, Value::String),
        );
        if let Some(items) = record.get_mut("items").and_then(Value::as_array_mut) {
            for item in items {
                if let Some(item_obj) = item.as_object_mut()
                    && let Some(key_hex) = item_obj
                        .get("entity_key")
                        .and_then(Value::as_object)
                        .and_then(|key_obj| key_obj.get("value"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                {
                    item_obj.insert("entity_key".to_owned(), Value::String(key_hex));
                }
            }
        }
    }
    serialize(&Value::Object(record))
}

fn driver_record_with_outcome_to_python_json(value: DriverValue) -> PyResult<Map<String, Value>> {
    let DriverValue::Record(fields) = value else {
        return Err(native_error("protocol_error", None));
    };
    let mut output = Map::new();
    for (name, field_value) in fields {
        if name == "outcome"
            && let DriverValue::Enum(outcome_name) = field_value
        {
            output.insert(name, Value::String(outcome_name));
        } else {
            let lowered = lower_driver_value(field_value)
                .map_err(|_| native_error("protocol_error", None))?;
            output.insert(name, value_to_json(lowered));
        }
    }
    Ok(output)
}

#[pyclass(name = "_SyncClient", module = "riffdb_application._native")]
struct NativeSyncClient {
    runtime: Runtime,
    client: Mutex<Option<ClientBackend>>,
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
        if is_driver_socket_endpoint(&endpoint) {
            return Self::connect_driver_socket(_class, py, endpoint, metadata, None);
        }
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
            client: Mutex::new(Some(ClientBackend::Direct(Box::new(client)))),
            metadata: metadata.inner.clone(),
        })
    }

    #[classmethod]
    #[pyo3(signature = (socket_path, metadata, role=None))]
    fn connect_driver_socket(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        socket_path: String,
        metadata: PyRef<'_, NativeCallMetadata>,
        role: Option<String>,
    ) -> PyResult<Self> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .map_err(|_| native_error("connection_failure", None))?;
        let database = metadata.database.clone();
        let effective_role = role.or_else(|| metadata.role.clone());
        let socket = py
            .detach(|| {
                runtime.block_on(DriverSocketState::connect(
                    &socket_path,
                    database.as_deref(),
                    effective_role.as_deref(),
                ))
            })
            .map_err(binding_error)?;
        Ok(Self {
            runtime,
            client: Mutex::new(Some(ClientBackend::DriverSocket(std::sync::Arc::new(
                tokio::sync::Mutex::new(socket),
            )))),
            metadata: metadata.inner.clone(),
        })
    }

    #[classmethod]
    // PyO3 exposes this as keyword-only constructor fields; retaining the
    // closed scalar boundary avoids accepting an unvalidated mapping.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        endpoint,
        trust_root=None,
        server_name=None,
        pool_connections=4,
        streams_per_connection=64,
        metadata=None
    ))]
    fn connect_verified_tls(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        endpoint: String,
        trust_root: Option<String>,
        server_name: Option<String>,
        pool_connections: u32,
        streams_per_connection: u32,
        metadata: Option<PyRef<'_, NativeCallMetadata>>,
    ) -> PyResult<Self> {
        let tls = tls_config(
            endpoint,
            trust_root,
            server_name,
            pool_connections,
            streams_per_connection,
        )?;
        let metadata = metadata.map_or_else(CallMetadata::default, |m| m.inner.clone());
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
            client: Mutex::new(Some(ClientBackend::Direct(Box::new(client)))),
            metadata,
        })
    }

    fn execute_named_query(&self, py: Python<'_>, request: &str) -> PyResult<String> {
        let request = parse_in_process_query(request).map_err(protocol_core_error)?;
        match self.backend()? {
            ClientBackend::Direct(mut client) => {
                let wire = request.accept_compact_result || request.accept_packed_result;
                let result = py
                    .detach(|| {
                        self.runtime.block_on(dispatch_named_query(
                            &mut client,
                            &self.metadata,
                            request.query,
                            wire,
                        ))
                    })
                    .map_err(application_client_error)?;
                match result {
                    QueryDispatchResult::Wire(result) => render_wire_query_result(result),
                    QueryDispatchResult::Records(result) => render_query_result(result),
                }
            }
            ClientBackend::DriverSocket(socket) => py.detach(|| {
                self.runtime
                    .block_on(execute_socket_named_query(&socket, request))
            }),
        }
    }

    fn execute_command(&self, py: Python<'_>, request: &str) -> PyResult<String> {
        let request = parse_in_process_command(request).map_err(protocol_core_error)?;
        match self.backend()? {
            ClientBackend::Direct(mut client) => {
                let result = py
                    .detach(|| {
                        self.runtime.block_on(dispatch_command(
                            &mut client,
                            &self.metadata,
                            request.command,
                            request.attempts,
                            Some((
                                request.expected_contract_version,
                                request.expected_plan_hash,
                            )),
                        ))
                    })
                    .map_err(application_client_error)?;
                render_command_result(result)
            }
            ClientBackend::DriverSocket(socket) => py.detach(|| {
                self.runtime
                    .block_on(execute_socket_command(&socket, request))
            }),
        }
    }

    fn inspect_vector_state(&self, py: Python<'_>, request: &str) -> PyResult<String> {
        let request = parse_vector_inspection(request)?;
        match self.backend()? {
            ClientBackend::Direct(mut client) => {
                let result = py
                    .detach(|| {
                        self.runtime.block_on(
                            client.inspect_vector_state(request.inspection, &self.metadata),
                        )
                    })
                    .map_err(application_client_error)?;
                render_vector_inspection(result)
            }
            ClientBackend::DriverSocket(socket) => py.detach(|| {
                self.runtime
                    .block_on(execute_socket_vector_inspection(&socket, request))
            }),
        }
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let removed = self
            .client
            .lock()
            .map_err(|_| native_error("connection_failure", None))?
            .take();
        if let Some(ClientBackend::DriverSocket(socket)) = removed {
            py.detach(|| {
                self.runtime.block_on(async move {
                    socket.lock().await.close();
                });
            });
        }
        Ok(())
    }
}

impl NativeSyncClient {
    fn backend(&self) -> PyResult<ClientBackend> {
        self.client
            .lock()
            .map_err(|_| native_error("connection_failure", None))?
            .as_ref()
            .cloned()
            .ok_or_else(|| native_error("connection_failure", None))
    }
}

const MAX_RETAINED_WATCH_STREAMS: usize = 64;

struct RetainedWatchStream {
    operation: ApplicationReactiveOperation,
    cursor: LiveQueryCursor,
    stream: ApplicationLiveQueryStream,
}

#[pyclass(name = "_AsyncClient", module = "riffdb_application._native")]
struct NativeAsyncClient {
    client: Mutex<Option<ClientBackend>>,
    metadata: CallMetadata,
    watches: std::sync::Arc<Mutex<std::collections::VecDeque<RetainedWatchStream>>>,
}

#[pyfunction]
fn connect_async<'py>(
    py: Python<'py>,
    endpoint: String,
    metadata: PyRef<'_, NativeCallMetadata>,
) -> PyResult<Bound<'py, PyAny>> {
    if is_driver_socket_endpoint(&endpoint) {
        return connect_async_driver_socket(py, endpoint, metadata, None);
    }
    let metadata = metadata.inner.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let client = StableApplicationClient::connect_uri(endpoint)
            .await
            .map_err(client_error)?;
        Python::attach(|py| {
            Py::new(
                py,
                NativeAsyncClient {
                    client: Mutex::new(Some(ClientBackend::Direct(Box::new(client)))),
                    metadata,
                    watches: std::sync::Arc::new(Mutex::new(std::collections::VecDeque::new())),
                },
            )
        })
    })
}

#[pyfunction]
#[pyo3(signature = (socket_path, metadata, role=None))]
fn connect_async_driver_socket<'py>(
    py: Python<'py>,
    socket_path: String,
    metadata: PyRef<'_, NativeCallMetadata>,
    role: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let call_metadata = metadata.inner.clone();
    let database = metadata.database.clone();
    let effective_role = role.or_else(|| metadata.role.clone());
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let socket = DriverSocketState::connect(
            &socket_path,
            database.as_deref(),
            effective_role.as_deref(),
        )
        .await
        .map_err(binding_error)?;
        Python::attach(|py| {
            Py::new(
                py,
                NativeAsyncClient {
                    client: Mutex::new(Some(ClientBackend::DriverSocket(std::sync::Arc::new(
                        tokio::sync::Mutex::new(socket),
                    )))),
                    metadata: call_metadata,
                    watches: std::sync::Arc::new(Mutex::new(std::collections::VecDeque::new())),
                },
            )
        })
    })
}

#[pyfunction]
#[pyo3(signature = (
    endpoint,
    trust_root=None,
    server_name=None,
    pool_connections=4,
    streams_per_connection=64,
    metadata=None
))]
fn connect_async_verified_tls<'py>(
    py: Python<'py>,
    endpoint: String,
    trust_root: Option<String>,
    server_name: Option<String>,
    pool_connections: u32,
    streams_per_connection: u32,
    metadata: Option<PyRef<'_, NativeCallMetadata>>,
) -> PyResult<Bound<'py, PyAny>> {
    let tls = tls_config(
        endpoint,
        trust_root,
        server_name,
        pool_connections,
        streams_per_connection,
    )?;
    let metadata = metadata.map_or_else(CallMetadata::default, |m| m.inner.clone());
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let client = StableApplicationClient::connect_verified_tls(&tls)
            .await
            .map_err(client_error)?;
        Python::attach(|py| {
            Py::new(
                py,
                NativeAsyncClient {
                    client: Mutex::new(Some(ClientBackend::Direct(Box::new(client)))),
                    metadata,
                    watches: std::sync::Arc::new(Mutex::new(std::collections::VecDeque::new())),
                },
            )
        })
    })
}

#[pyfunction]
fn validate_bridge_value(source: &str) -> PyResult<()> {
    let value: Value = parse_json(source)?;
    normalize_python_value(value)
        .map(|_| ())
        .map_err(protocol_core_error)
}

#[pymethods]
impl NativeAsyncClient {
    fn execute_named_query<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_in_process_query(&request).map_err(protocol_core_error)?;
        let backend = self.backend()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match backend {
                ClientBackend::Direct(mut client) => {
                    let wire = request.accept_compact_result || request.accept_packed_result;
                    match dispatch_named_query(&mut client, &metadata, request.query, wire)
                        .await
                        .map_err(application_client_error)?
                    {
                        QueryDispatchResult::Wire(result) => render_wire_query_result(result),
                        QueryDispatchResult::Records(result) => render_query_result(result),
                    }
                }
                ClientBackend::DriverSocket(socket) => {
                    execute_socket_named_query(&socket, request).await
                }
            }
        })
    }

    fn execute_command<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_in_process_command(&request).map_err(protocol_core_error)?;
        let backend = self.backend()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match backend {
                ClientBackend::Direct(mut client) => {
                    let result = dispatch_command(
                        &mut client,
                        &metadata,
                        request.command,
                        request.attempts,
                        Some((
                            request.expected_contract_version,
                            request.expected_plan_hash,
                        )),
                    )
                    .await
                    .map_err(application_client_error)?;
                    render_command_result(result)
                }
                ClientBackend::DriverSocket(socket) => {
                    execute_socket_command(&socket, request).await
                }
            }
        })
    }

    fn inspect_vector_state<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = parse_vector_inspection(&request)?;
        let backend = self.backend()?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match backend {
                ClientBackend::Direct(mut client) => {
                    let result = client
                        .inspect_vector_state(request.inspection, &metadata)
                        .await
                        .map_err(application_client_error)?;
                    render_vector_inspection(result)
                }
                ClientBackend::DriverSocket(socket) => {
                    execute_socket_vector_inspection(&socket, request).await
                }
            }
        })
    }

    fn consume_event_stream<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.reactive_client()?;
        let request = parse_reactive_consumer(&request)?;
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
        let mut client = self.reactive_client()?;
        let request = parse_contextual_consumer(&request)?;
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
        let mut client = self.reactive_client()?;
        let request = parse_reactive_mutation(&request)?;
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
        let mut client = self.reactive_client()?;
        let consumer = parse_reactive_identity(&request)?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .contextual_subscription_status(&consumer, &metadata)
                .await
                .map_err(application_client_error)?;
            serialize(
                &result
                    .map(event_status_json)
                    .unwrap_or(serde_json::Value::Null),
            )
        })
    }

    fn execute_contextual_reaction<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.reactive_client()?;
        let request = parse_contextual_reaction(&request)?;
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
            validate_command_identity(
                request.expected.contract_version,
                request.expected.plan_hash,
                &result,
            )?;
            render_command_result(result)
        })
    }

    fn mutate_event_consumer<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.reactive_client()?;
        let request = parse_reactive_mutation(&request)?;
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
        let mut client = self.reactive_client()?;
        let request = parse_reactive_seek(&request)?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = match request.target {
                ParsedReactiveSeekTarget::Exact(checkpoint) => {
                    client
                        .seek_event_consumer(&request.consumer, checkpoint, &metadata)
                        .await
                }
                ParsedReactiveSeekTarget::Protected(cursor) => {
                    client
                        .seek_protected_event_consumer(&request.consumer, cursor, &metadata)
                        .await
                }
            }
            .map_err(application_client_error)?;
            render_event_mutation(result)
        })
    }

    fn event_consumer_status<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.reactive_client()?;
        let consumer = parse_reactive_identity(&request)?;
        let metadata = self.metadata.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .event_consumer_status(&consumer, &metadata)
                .await
                .map_err(application_client_error)?;
            serialize(
                &result
                    .map(event_status_json)
                    .unwrap_or(serde_json::Value::Null),
            )
        })
    }

    fn watch_named_query<'py>(
        &self,
        py: Python<'py>,
        request: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut client = self.reactive_client()?;
        let request = parse_live_query(&request)?;
        let metadata = self.metadata.clone();
        let watches = self.watches.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(ref resume_cursor) = request.cursor
                && let Some(mut stream) = take_watch(&watches, &request.operation, resume_cursor)
                && let Some(update) = stream.message().await.map_err(application_client_error)?
            {
                if let Some(next_cursor) = live_update_next_cursor(&update) {
                    store_watch(&watches, request.operation, next_cursor, stream);
                }
                return render_live_update(update);
            }
            let mut stream = client
                .watch_named_query(&request.operation, request.cursor, &metadata)
                .await
                .map_err(application_client_error)?;
            let update = stream
                .message()
                .await
                .map_err(application_client_error)?
                .ok_or_else(|| native_error("connection_failure", None))?;
            if let Some(next_cursor) = live_update_next_cursor(&update) {
                store_watch(&watches, request.operation, next_cursor, stream);
            }
            render_live_update(update)
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let removed = self
            .client
            .lock()
            .map_err(|_| native_error("connection_failure", None))?
            .take();
        if let Ok(mut watches) = self.watches.lock() {
            watches.clear();
        }
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(ClientBackend::DriverSocket(socket)) = removed {
                socket.lock().await.close();
            }
            Ok(())
        })
    }
}

impl NativeAsyncClient {
    fn backend(&self) -> PyResult<ClientBackend> {
        self.client
            .lock()
            .map_err(|_| native_error("connection_failure", None))?
            .as_ref()
            .cloned()
            .ok_or_else(|| native_error("connection_failure", None))
    }

    /// Reactive operations (event consumers, contextual subscriptions and
    /// reactions, live queries) are carried only by the direct gRPC client. The
    /// driver socket generation has no request form this binding can address
    /// them with, so they are refused before any parsing or I/O with a closed,
    /// transport-specific classification rather than a peer-protocol failure.
    fn reactive_client(&self) -> PyResult<StableApplicationClient> {
        match self.backend()? {
            ClientBackend::Direct(client) => Ok(*client),
            ClientBackend::DriverSocket(_) => Err(native_error("unsupported_transport", None)),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorInspectionRequest {
    contract_lineage: String,
    contract_version: u64,
    contract_bundle_hash: String,
    entity: String,
    field: String,
    inspection_kind: String,
    partition: Value,
    limit: u32,
    cursor: Option<String>,
}

struct ParsedVectorInspection {
    inspection: VectorStateInspection,
    contract_lineage: String,
    contract_version: u64,
    contract_bundle_hash: String,
    operation: String,
    partition: DriverValue,
    limit: u32,
    cursor: Option<Vec<u8>>,
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
    consumer: ApplicationEventConsumer,
    options: EventConsumerOptions,
}

struct ParsedContextualConsumer {
    consumer: ApplicationEventConsumer,
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
    consumer: ApplicationEventConsumer,
    reaction: ApplicationContextualReaction,
    command: ApplicationCommand,
    expected: CommandResponseIdentity,
}

struct CommandResponseIdentity {
    contract_version: u64,
    plan_hash: [u8; 32],
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
    consumer: ApplicationEventConsumer,
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
    checkpoint: Option<String>,
    progress_cursor: Option<String>,
}

struct ParsedReactiveSeek {
    consumer: ApplicationEventConsumer,
    target: ParsedReactiveSeekTarget,
}

enum ParsedReactiveSeekTarget {
    Exact(ApplicationEventCheckpoint),
    Protected(ApplicationEventProgressCursor),
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
    let parameters = parameters
        .into_iter()
        .map(|(name, value)| {
            Ok((
                name,
                normalize_python_value(value).map_err(protocol_core_error)?,
            ))
        })
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
    let consumer = ApplicationEventConsumer::new(operation, request.consumer_name)
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
    let input = request
        .input
        .into_iter()
        .map(|(name, value)| {
            Ok((
                name,
                normalize_python_value(value).map_err(protocol_core_error)?,
            ))
        })
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
) -> PyResult<ApplicationEventConsumer> {
    ApplicationEventConsumer::new(
        parse_reactive_values(module_hash, operation_name, parameters)?,
        consumer_name,
    )
    .map_err(application_client_error)
}

fn parse_reactive_identity(source: &str) -> PyResult<ApplicationEventConsumer> {
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
    let target = match (request.checkpoint, request.progress_cursor) {
        (Some(checkpoint), None) if checkpoint == "before-first" => {
            ParsedReactiveSeekTarget::Exact(ApplicationEventCheckpoint::BeforeFirst)
        }
        (Some(checkpoint), None) => ParsedReactiveSeekTarget::Exact(
            ApplicationEventCheckpoint::After(parse_event_id(&checkpoint)?),
        ),
        (None, Some(cursor)) => ParsedReactiveSeekTarget::Protected(
            ApplicationEventProgressCursor::new(parse_hex(&cursor)?)
                .map_err(application_client_error)?,
        ),
        _ => return Err(native_error("invalid_input", None)),
    };
    Ok(ParsedReactiveSeek {
        consumer: reactive_consumer(
            request.reactive_module_hash,
            request.operation_name,
            request.parameters,
            request.consumer_name,
        )?,
        target,
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

fn parse_vector_inspection(source: &str) -> PyResult<ParsedVectorInspection> {
    let request: VectorInspectionRequest = parse_json(source)?;
    let bundle_hash = parse_hash(&request.contract_bundle_hash)?;
    let partition = normalize_python_value(request.partition).map_err(protocol_core_error)?;
    let partition_driver = raise_driver_value(partition.clone());
    let kind = match request.inspection_kind.as_str() {
        "staleness" => VectorStateInspectionKind::Stale,
        "model_versions" => VectorStateInspectionKind::OutdatedModel,
        _ => return Err(native_error("invalid_input", None)),
    };
    let operation = format!(
        "{}.{}:{}",
        request.entity, request.field, request.inspection_kind
    );
    let cursor_bytes = request.cursor.as_deref().map(parse_hex).transpose()?;
    let mut inspection = VectorStateInspection::new(
        ApplicationContract::Exact {
            lineage: request.contract_lineage.clone(),
            version: request.contract_version,
            bundle_hash: Some(bundle_hash),
        },
        request.entity,
        request.field,
        partition,
        kind,
        request.limit,
    );
    if let Some(ref cursor) = cursor_bytes {
        inspection = inspection.after(cursor.clone());
    }
    Ok(ParsedVectorInspection {
        inspection,
        contract_lineage: request.contract_lineage,
        contract_version: request.contract_version,
        contract_bundle_hash: request.contract_bundle_hash,
        operation,
        partition: partition_driver,
        limit: request.limit,
        cursor: cursor_bytes,
    })
}

fn validate_command_identity(
    expected_contract_version: u64,
    expected_plan_hash: [u8; 32],
    result: &ApplicationCommandResult,
) -> PyResult<()> {
    if result.contract_version != expected_contract_version
        || result.plan_hash != expected_plan_hash
    {
        return Err(native_error("protocol_error", None));
    }
    Ok(())
}

fn render_query_result(result: NamedQueryResult) -> PyResult<String> {
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

fn render_vector_inspection(result: VectorStateInspectionResult) -> PyResult<String> {
    let value = match result {
        VectorStateInspectionResult::StalenessSummary(summary) => json!({
            "kind": "staleness_summary",
            "total_entities": summary.total_entities,
            "stale_count": summary.stale_count,
            "stale_entity_count_threshold": summary.stale_entity_count_threshold,
            "slo_breached": summary.slo_breached,
        }),
        VectorStateInspectionResult::StaleEntities(page) => json!({
            "kind": "stale_entities",
            "items": page.items.into_iter().map(|item| json!({
                "entity_key": hex(&item.entity_key),
                "newest_source_write": item.newest_source_write,
                "embedding_write": item.embedding_write,
            })).collect::<Vec<_>>(),
            "next_cursor": page.next_cursor.as_deref().map(hex),
            "observed_frontier": page.observed_frontier,
        }),
        VectorStateInspectionResult::ModelVersionSummary(summary) => json!({
            "kind": "model_version_summary",
            "current_count": summary.current_count,
            "outdated_count": summary.outdated_count,
        }),
        VectorStateInspectionResult::OutdatedModelEntities(page) => json!({
            "kind": "outdated_model_entities",
            "items": page.items.into_iter().map(|item| json!({
                "entity_key": hex(&item.entity_key),
                "model": item.model,
                "model_version": item.model_version,
                "embedding_write": item.embedding_write,
            })).collect::<Vec<_>>(),
            "next_cursor": page.next_cursor.as_deref().map(hex),
            "observed_frontier": page.observed_frontier,
        }),
    };
    serialize(&value)
}

fn render_wire_query_result(response: app_v1::ExecuteQueryResponse) -> PyResult<String> {
    if response.selected_result_encoding == app_v1::NamedResultEncoding::PackedV1 as i32 {
        if !response.fields.is_empty() || response.compact_result.is_some() {
            return Err(native_error("protocol_error", None));
        }
        let identity = response
            .identity
            .ok_or_else(|| native_error("protocol_error", None))?;
        let packed = response
            .packed_result
            .ok_or_else(|| native_error("protocol_error", None))?;
        let columns = packed
            .columns
            .into_iter()
            .map(|column| {
                json!({
                    "data": BASE64.encode(column.data),
                    "offsets": column.offsets,
                })
            })
            .collect::<Vec<_>>();
        return serialize(&json!({
            "value": {
                "$riffdb_packed": {
                    "outcome": response.outcome,
                    "result_name": packed.name,
                    "entity": packed.entity,
                    "fields": packed.fields,
                    "row_count": packed.row_count,
                    "columns": columns,
                }
            },
            "identity": {
                "contract_lineage": identity.contract_lineage,
                "contract_version": identity.contract_version,
                "contract_bundle_hash": hex(&identity.contract_bundle_hash),
                "module_hash": identity.module_hash.as_deref().map(hex).ok_or_else(|| native_error("protocol_error", None))?,
                "query_name": identity.query_name.ok_or_else(|| native_error("protocol_error", None))?,
                "plan_hash": hex(&identity.plan_hash),
            },
            "application_head": response.application_head,
            "next_cursor": response.next_cursor,
        }));
    }
    if response.selected_result_encoding != app_v1::NamedResultEncoding::CompactV1 as i32 {
        return render_query_result(
            raise_query_result(response).map_err(application_client_error)?,
        );
    }
    if !response.fields.is_empty() {
        return Err(native_error("protocol_error", None));
    }
    let identity = response
        .identity
        .ok_or_else(|| native_error("protocol_error", None))?;
    let compact = response
        .compact_result
        .ok_or_else(|| native_error("protocol_error", None))?;
    let rows = compact
        .rows
        .into_iter()
        .map(|row| {
            row.values
                .into_iter()
                .map(|value| {
                    raise_wire_value(value)
                        .map(value_to_json)
                        .map_err(application_client_error)
                })
                .collect::<PyResult<Vec<_>>>()
        })
        .collect::<PyResult<Vec<_>>>()?;
    serialize(&json!({
        "value": {
            "$riffdb_compact": {
                "outcome": response.outcome,
                "result_name": compact.name,
                "entity": compact.entity,
                "fields": compact.fields,
                "rows": rows,
            }
        },
        "identity": {
            "contract_lineage": identity.contract_lineage,
            "contract_version": identity.contract_version,
            "contract_bundle_hash": hex(&identity.contract_bundle_hash),
            "module_hash": identity.module_hash.as_deref().map(hex).ok_or_else(|| native_error("protocol_error", None))?,
            "query_name": identity.query_name.ok_or_else(|| native_error("protocol_error", None))?,
            "plan_hash": hex(&identity.plan_hash),
        },
        "application_head": response.application_head,
        "next_cursor": response.next_cursor,
    }))
}

fn render_command_result(result: ApplicationCommandResult) -> PyResult<String> {
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

fn exact_event_status_json(status: ApplicationEventConsumerStatus) -> serde_json::Value {
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

fn live_update_next_cursor(update: &ApplicationLiveQueryUpdate) -> Option<LiveQueryCursor> {
    match update {
        ApplicationLiveQueryUpdate::Snapshot { cursor, .. }
        | ApplicationLiveQueryUpdate::Reset { cursor, .. } => Some(cursor.clone()),
        ApplicationLiveQueryUpdate::Patch(patch) => Some(patch.checkpoint.cursor.clone()),
        ApplicationLiveQueryUpdate::Checkpoint(checkpoint) => Some(checkpoint.cursor.clone()),
        ApplicationLiveQueryUpdate::Terminal(_) => None,
    }
}

fn take_watch(
    watches: &Mutex<std::collections::VecDeque<RetainedWatchStream>>,
    operation: &ApplicationReactiveOperation,
    cursor: &LiveQueryCursor,
) -> Option<ApplicationLiveQueryStream> {
    let mut watches = watches
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let index = watches
        .iter()
        .position(|entry| &entry.operation == operation && &entry.cursor == cursor)?;
    watches.remove(index).map(|entry| entry.stream)
}

fn store_watch(
    watches: &Mutex<std::collections::VecDeque<RetainedWatchStream>>,
    operation: ApplicationReactiveOperation,
    cursor: LiveQueryCursor,
    stream: ApplicationLiveQueryStream,
) {
    let mut watches = watches
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while watches.len() >= MAX_RETAINED_WATCH_STREAMS {
        watches.pop_front();
    }
    watches.push_back(RetainedWatchStream {
        operation,
        cursor,
        stream,
    });
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

fn named_result_value(result: NamedQueryResult) -> PyResult<Value> {
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

fn live_patch_json(operation: LiveQueryPatchOperation) -> Value {
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
    application_value_to_python_json(value)
}

fn parse_json<T: for<'de> Deserialize<'de>>(source: &str) -> PyResult<T> {
    if source.is_empty() || source.len() > MAX_BRIDGE_BYTES {
        return Err(native_error("invalid_input", None));
    }
    serde_json::from_str(source).map_err(|_| native_error("invalid_input", None))
}

fn tls_config(
    endpoint: String,
    trust_root: Option<String>,
    server_name: Option<String>,
    pool_connections: u32,
    streams_per_connection: u32,
) -> PyResult<TlsClientConfig> {
    let endpoint = CanonicalHttpsEndpoint::parse(&endpoint)
        .map_err(|_| native_error("invalid_input", None))?;
    let trust_root = match trust_root.filter(|value| !value.is_empty()) {
        Some(path) => ClientTrustRootConfig::ExplicitPemFile(
            ProtectedFilePath::new(Path::new(&path).to_path_buf())
                .map_err(|_| native_error("invalid_input", None))?,
        ),
        None => ClientTrustRootConfig::SystemCaBundle,
    };
    let server_name = match server_name.filter(|value| !value.is_empty()) {
        Some(name) => {
            TlsServerIdentity::parse(&name).map_err(|_| native_error("invalid_input", None))?
        }
        None => endpoint.identity().clone(),
    };
    let pool_connections = NonZeroU32::new(pool_connections)
        .filter(|value| value.get() <= 16)
        .ok_or_else(|| native_error("invalid_input", None))?;
    let streams_per_connection = NonZeroU32::new(streams_per_connection)
        .filter(|value| value.get() <= 256)
        .ok_or_else(|| native_error("invalid_input", None))?;
    TlsClientConfig::with_trust_root_config(
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

fn application_client_error(error: ApplicationClientError) -> PyErr {
    binding_error(classify_application_client_error(error))
}

fn protocol_core_error(_error: ProtocolCoreError) -> PyErr {
    native_error("invalid_input", None)
}

fn client_error(error: ClientError) -> PyErr {
    binding_error(classify_client_error(error))
}

fn binding_error(error: BindingError) -> PyErr {
    native_error(error.kind, error.details)
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
    module.add_function(wrap_pyfunction!(connect_async_driver_socket, module)?)?;
    module.add_function(wrap_pyfunction!(connect_async_verified_tls, module)?)?;
    module.add_function(wrap_pyfunction!(validate_bridge_value, module)?)?;
    Ok(())
}
