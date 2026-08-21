use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use riffdb_client_rust::{
    ApplicationCardinality, ApplicationClientError, ApplicationCommand, ApplicationContextualBatch,
    ApplicationContextualReaction, ApplicationContract, ApplicationEventBatch,
    ApplicationEventCheckpoint, ApplicationEventConsumer, ApplicationEventConsumerPublicStatus,
    ApplicationEventConsumerStatus, ApplicationEventId, ApplicationEventLeaseEvidence,
    ApplicationEventMutationResult, ApplicationEventPullDisposition, ApplicationLiveQueryUpdate,
    ApplicationReactiveOperation, ApplicationRecord, ApplicationUuid, ApplicationValue,
    AttemptBudget, CallMetadata, EventConsumerOptions, LiveQueryCursor, LiveQueryPatchOperation,
    NamedQuery, QueryOptions, StableApplicationClient, VectorModelVersionItem, VectorStalenessItem,
    VectorStateInspection, VectorStateInspectionKind, VectorStateInspectionResult, app_v1,
    raise_query_result as raise_wire_query_result, raise_value as raise_wire_value,
};
use riffdb_config::TlsClientConfig;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, watch};

use crate::catalog::{
    ApplicationCatalog, OperationKind, OperationSpec, ReactiveKind, VectorInspectionKind,
};
use crate::protocol::{
    DRIVER_PROTOCOL_VERSION, DRIVER_PROTOCOL_VERSION_V1, DriverBatchItem, DriverBatchOutcome,
    DriverDecimal, DriverMoney, DriverRequest, DriverResponse, DriverTimestamp, DriverValue,
    DriverVector, InvokeOptions,
};

/// Exact alpha host build identity.
pub const DRIVER_IDENTITY: &str = concat!("riffdb-driverd/", env!("CARGO_PKG_VERSION"));
/// Exact alpha application-value registry identity.
pub const DRIVER_VALUE_REGISTRY_HASH: &str =
    "8e1681ddf5e6a82e7fa646f9737128ad7e36f54f8b5846ac6e33e732125407e5";
#[cfg(test)]
const DRIVER_VALUE_REGISTRY_DESCRIPTION: &str = "riffdb-driver-value-registry-v2:null,bool,i64,u64,string,uuid,enum,bytes,date,timestamp,decimal,money,vector<f32bits[1..4096]>,list,record";
/// Exact alpha structured-error registry identity.
pub const DRIVER_ERROR_REGISTRY_HASH: &str =
    "b94d685ecbc18f2369a2bfa1a53139d06100699c4ee41b31c86d6a7e17039850";
const MAX_POOL_CONNECTIONS: usize = 16;
const MAX_QUEUED_OPERATIONS: usize = 4_096;
const REACTIVE_PROCESSING_ALLOWANCE: Duration = Duration::from_secs(5);

/// Bounded reusable pool of verified application-only connections.
pub struct DriverPool {
    clients: Vec<StableApplicationClient>,
    next: AtomicUsize,
    admission: Arc<Semaphore>,
}

impl DriverPool {
    /// Connects the configured number of independent verified TLS channels.
    pub async fn connect_verified_tls(config: &TlsClientConfig) -> Result<Self, DriverHostError> {
        let count = usize::try_from(config.max_pool_connections().get())
            .map_err(|_| DriverHostError::InvalidConfiguration)?;
        if count == 0 || count > MAX_POOL_CONNECTIONS {
            return Err(DriverHostError::InvalidConfiguration);
        }
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(
                StableApplicationClient::connect_verified_tls(config)
                    .await
                    .map_err(|_| DriverHostError::RemoteUnavailable)?,
            );
        }
        let streams = usize::try_from(config.max_streams_per_connection().get())
            .map_err(|_| DriverHostError::InvalidConfiguration)?;
        let capacity = count
            .checked_mul(streams)
            .map(|value| value.min(MAX_QUEUED_OPERATIONS))
            .filter(|value| *value != 0)
            .ok_or(DriverHostError::InvalidConfiguration)?;
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
            admission: Arc::new(Semaphore::new(capacity)),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_clients(
        clients: Vec<StableApplicationClient>,
        capacity: usize,
    ) -> Result<Self, DriverHostError> {
        if clients.is_empty()
            || clients.len() > MAX_POOL_CONNECTIONS
            || capacity == 0
            || capacity > MAX_QUEUED_OPERATIONS
        {
            return Err(DriverHostError::InvalidConfiguration);
        }
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
            admission: Arc::new(Semaphore::new(capacity)),
        })
    }

    fn try_admit(&self) -> Result<OwnedSemaphorePermit, DriverHostError> {
        self.admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| DriverHostError::Overloaded)
    }

    fn select(&self) -> StableApplicationClient {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        self.clients[index].clone()
    }

    /// Verifies every pooled TLS channel against the active remote contract
    /// before the local socket can advertise the pinned application identity.
    pub async fn verify_remote_identity(
        &self,
        catalog: &ApplicationCatalog,
        metadata: &CallMetadata,
    ) -> Result<(), DriverHostError> {
        for client in &self.clients {
            let mut client = client.clone();
            client
                .verify_active_contract(
                    catalog.database(),
                    catalog.contract_lineage(),
                    catalog.contract_version(),
                    catalog.contract_bundle_hash(),
                    metadata,
                )
                .await
                .map_err(|_| DriverHostError::RemoteIdentityMismatch)?;
        }
        Ok(())
    }
}

/// Long-lived application-only host state shared by local connections.
#[derive(Clone)]
pub struct DriverHost {
    inner: Arc<DriverHostInner>,
}

struct DriverHostInner {
    catalog: ApplicationCatalog,
    pool: DriverPool,
    metadata: CallMetadata,
    remote_identity_hash: String,
    inflight: Mutex<BTreeMap<String, InflightRequest>>,
}

struct InflightRequest {
    cancel: watch::Sender<bool>,
    command: bool,
}

impl DriverHost {
    /// Creates one host from already verified exact identities and remote pool.
    pub fn new(
        catalog: ApplicationCatalog,
        pool: DriverPool,
        metadata: CallMetadata,
        remote_identity_hash: String,
    ) -> Result<Self, DriverHostError> {
        if remote_identity_hash.len() != 64
            || !remote_identity_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(DriverHostError::InvalidConfiguration);
        }
        Ok(Self {
            inner: Arc::new(DriverHostInner {
                catalog,
                pool,
                metadata,
                remote_identity_hash,
                inflight: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// Validates one local handshake without contacting storage.
    #[must_use]
    pub fn handshake(&self, request: &DriverRequest) -> DriverResponse {
        let DriverRequest::Handshake {
            request_id,
            protocol_version,
            application_manifest_hash,
            operation_catalog_hash,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            value_registry_hash,
            error_registry_hash,
            database,
            role,
            role_definition_hash,
            remote_identity_hash,
        } = request
        else {
            return local_error(
                None,
                None,
                "RDB-DRIVER-0001",
                "protocol",
                "driver handshake is required",
                false,
                "restart_handshake",
            );
        };
        if !matches!(
            *protocol_version,
            DRIVER_PROTOCOL_VERSION_V1 | DRIVER_PROTOCOL_VERSION
        ) || application_manifest_hash != &self.inner.catalog.application_manifest_hash()
            || operation_catalog_hash != &self.inner.catalog.catalog_hash()
            || contract_lineage != self.inner.catalog.contract_lineage()
            || *contract_version != self.inner.catalog.contract_version()
            || contract_bundle_hash != &self.inner.catalog.contract_bundle_hash_hex()
            || value_registry_hash != DRIVER_VALUE_REGISTRY_HASH
            || error_registry_hash != DRIVER_ERROR_REGISTRY_HASH
            || database != self.inner.catalog.database()
            || role != self.inner.catalog.role()
            || role_definition_hash != &self.inner.catalog.role_definition_hash()
            || remote_identity_hash != &self.inner.remote_identity_hash
        {
            return local_error(
                Some(request_id.clone()),
                None,
                "RDB-DRIVER-0002",
                "identity",
                "driver application identity does not match",
                false,
                "regenerate_bindings",
            );
        }
        DriverResponse::Handshake {
            request_id: request_id.clone(),
            protocol_version: *protocol_version,
            driver_identity: DRIVER_IDENTITY.to_owned(),
            application_manifest_hash: self.inner.catalog.application_manifest_hash(),
            operation_catalog_hash: self.inner.catalog.catalog_hash(),
            contract_lineage: self.inner.catalog.contract_lineage().to_owned(),
            contract_version: self.inner.catalog.contract_version(),
            contract_bundle_hash: self.inner.catalog.contract_bundle_hash_hex(),
            database: self.inner.catalog.database().to_owned(),
            role: self.inner.catalog.role().to_owned(),
            role_definition_hash: self.inner.catalog.role_definition_hash(),
            remote_identity_hash: self.inner.remote_identity_hash.clone(),
        }
    }

    /// Executes one generated catalog operation with bounded admission and deadline.
    pub async fn invoke(&self, request: DriverRequest) -> DriverResponse {
        self.invoke_with_parent_cancellation(request, None).await
    }

    async fn invoke_with_parent_cancellation(
        &self,
        request: DriverRequest,
        mut parent_cancelled: Option<watch::Receiver<bool>>,
    ) -> DriverResponse {
        let DriverRequest::Invoke {
            request_id,
            operation,
            input_schema_hash,
            input,
            options,
        } = request
        else {
            return local_error(
                None,
                None,
                "RDB-DRIVER-0003",
                "input",
                "driver invocation is invalid",
                false,
                "correct_request",
            );
        };
        let Some(spec) = self.inner.catalog.operation(&operation).cloned() else {
            return local_error(
                Some(request_id),
                Some(operation),
                "RDB-DRIVER-0004",
                "authorization",
                "operation is absent from the exact generated catalog",
                false,
                "regenerate_bindings",
            );
        };
        if input_schema_hash != spec.input_schema_hash() {
            return local_error(
                Some(request_id),
                Some(operation),
                "RDB-DRIVER-0002",
                "identity",
                "generated operation schema does not match",
                false,
                "regenerate_bindings",
            );
        }
        let permit = match self.inner.pool.try_admit() {
            Ok(permit) => permit,
            Err(_) => {
                return local_error(
                    Some(request_id),
                    Some(operation),
                    "RDB-CAPACITY-0101",
                    "capacity",
                    "driver host is over capacity",
                    false,
                    "retry_later",
                );
            }
        };
        let (cancel, mut cancelled) = watch::channel(false);
        {
            let mut inflight = self.inner.inflight.lock().await;
            if inflight.contains_key(&request_id) {
                return local_error(
                    Some(request_id),
                    Some(operation),
                    "RDB-DRIVER-0005",
                    "input",
                    "driver request identity is already in flight",
                    false,
                    "correct_request",
                );
            }
            inflight.insert(
                request_id.clone(),
                InflightRequest {
                    cancel,
                    command: spec.kind() == OperationKind::Command,
                },
            );
        }
        let deadline = invocation_deadline(
            spec.kind(),
            spec.reactive_action(),
            Duration::from_millis(options.deadline_millis),
        );
        let execution = self.execute(request_id.clone(), &spec, input, options, permit);
        let response = tokio::select! {
            result = tokio::time::timeout(deadline, execution) => match result {
                Ok(response) => response,
                Err(_) => local_error(Some(request_id.clone()), Some(operation.clone()), "RDB-APP-0003", "control", "application request deadline elapsed", spec.kind() == OperationKind::Command, if spec.kind() == OperationKind::Command { "resolve_with_same_idempotency_key" } else { "retry" }),
            },
            changed = cancelled.changed() => {
                let _ = changed;
                local_error(Some(request_id.clone()), Some(operation.clone()), "RDB-APP-0002", "control", "application request was cancelled", spec.kind() == OperationKind::Command, if spec.kind() == OperationKind::Command { "resolve_with_same_idempotency_key" } else { "none" })
            },
            changed = async {
                match parent_cancelled.as_mut() {
                    Some(cancelled) => cancelled.changed().await,
                    None => std::future::pending().await,
                }
            } => {
                let _ = changed;
                local_error(Some(request_id.clone()), Some(operation.clone()), "RDB-APP-0002", "control", "application batch was cancelled", spec.kind() == OperationKind::Command, if spec.kind() == OperationKind::Command { "resolve_with_same_idempotency_key" } else { "none" })
            },
        };
        self.inner.inflight.lock().await.remove(&request_id);
        response
    }

    /// Cancels one exact in-flight local request without claiming a command did not commit.
    pub async fn cancel(&self, request_id: String, target_request_id: String) -> DriverResponse {
        let (terminal, uncertain) = {
            let inflight = self.inner.inflight.lock().await;
            match inflight.get(&target_request_id) {
                Some(target) => {
                    let _ = target.cancel.send(true);
                    (false, target.command)
                }
                None => (true, false),
            }
        };
        DriverResponse::Cancelled {
            request_id,
            target_request_id,
            terminal,
            outcome_uncertain: uncertain,
        }
    }

    /// Executes independently idempotent generated commands with bounded
    /// concurrency and a contiguous resume checkpoint.
    pub async fn batch(&self, request: DriverRequest) -> DriverResponse {
        let DriverRequest::Batch {
            request_id,
            operation,
            input_schema_hash,
            items,
            concurrency,
            checkpoint,
            options,
        } = request
        else {
            return local_error(
                None,
                None,
                "RDB-DRIVER-0003",
                "input",
                "driver batch is invalid",
                false,
                "correct_request",
            );
        };
        let Some(spec) = self.inner.catalog.operation(&operation) else {
            return local_error(
                Some(request_id),
                Some(operation),
                "RDB-DRIVER-0004",
                "authorization",
                "operation is absent from the exact generated catalog",
                false,
                "regenerate_bindings",
            );
        };
        if spec.kind() != OperationKind::Command || input_schema_hash != spec.input_schema_hash() {
            return local_error(
                Some(request_id),
                Some(operation),
                "RDB-DRIVER-0002",
                "identity",
                "generated command schema does not match",
                false,
                "regenerate_bindings",
            );
        }
        let total = match u32::try_from(items.len()) {
            Ok(value) => value,
            Err(_) => {
                return local_error(
                    Some(request_id),
                    Some(operation),
                    "RDB-INPUT-0101",
                    "input",
                    "application input is invalid",
                    false,
                    "correct_request",
                );
            }
        };
        let (cancel, cancelled) = watch::channel(false);
        {
            let mut inflight = self.inner.inflight.lock().await;
            if inflight.contains_key(&request_id) {
                return local_error(
                    Some(request_id),
                    Some(operation),
                    "RDB-DRIVER-0005",
                    "input",
                    "driver request identity is already in flight",
                    false,
                    "correct_request",
                );
            }
            inflight.insert(
                request_id.clone(),
                InflightRequest {
                    cancel,
                    command: true,
                },
            );
        }
        let limit = Arc::new(Semaphore::new(usize::try_from(concurrency).unwrap_or(1)));
        let mut tasks = tokio::task::JoinSet::new();
        let prefix = request_id.chars().take(96).collect::<String>();
        for (index, input) in items
            .into_iter()
            .enumerate()
            .skip(usize::try_from(checkpoint).unwrap_or(usize::MAX))
        {
            let host = self.clone();
            let permit = limit.clone();
            let operation = operation.clone();
            let input_schema_hash = input_schema_hash.clone();
            let options = options.clone();
            let cancelled = cancelled.clone();
            let item_request_id = format!("{prefix}.{index}");
            tasks.spawn(async move {
                let _permit = permit.acquire_owned().await.ok();
                let response = host
                    .invoke_with_parent_cancellation(
                        DriverRequest::Invoke {
                            request_id: item_request_id,
                            operation,
                            input_schema_hash,
                            input,
                            options,
                        },
                        Some(cancelled),
                    )
                    .await;
                (index, response)
            });
        }
        let mut completed = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            if let Ok((index, response)) = joined {
                completed.insert(index, response);
            }
        }
        self.inner.inflight.lock().await.remove(&request_id);
        let mut next = usize::try_from(checkpoint).unwrap_or(0);
        while completed
            .get(&next)
            .is_some_and(|response| matches!(response, DriverResponse::Result { .. }))
        {
            next += 1;
        }
        let items = completed
            .into_iter()
            .filter_map(|(index, response)| {
                let index = u32::try_from(index).ok()?;
                let outcome = match response {
                    DriverResponse::Result {
                        value,
                        application_head,
                        cursor,
                        replayed,
                        ..
                    } => DriverBatchOutcome::Result {
                        value,
                        commit_sequence: application_head,
                        outcome_uri: cursor,
                        replayed,
                    },
                    DriverResponse::Error {
                        code,
                        category,
                        operation,
                        symbol_path,
                        contract_lineage,
                        contract_version,
                        trace_id,
                        incident_id,
                        message,
                        recovery_action,
                        outcome_uncertain,
                        ..
                    } => DriverBatchOutcome::Error {
                        code,
                        category,
                        operation,
                        symbol_path,
                        contract_lineage,
                        contract_version,
                        trace_id,
                        incident_id,
                        message,
                        recovery_action,
                        outcome_uncertain,
                    },
                    _ => DriverBatchOutcome::Error {
                        code: "RDB-PROTOCOL-0101".to_owned(),
                        category: "protocol".to_owned(),
                        operation: Some(operation.clone()),
                        symbol_path: Vec::new(),
                        contract_lineage: None,
                        contract_version: None,
                        trace_id: None,
                        incident_id: None,
                        message: "the RiffDB peer returned an invalid application response"
                            .to_owned(),
                        recovery_action: "contact_operator".to_owned(),
                        outcome_uncertain: true,
                    },
                };
                Some(DriverBatchItem { index, outcome })
            })
            .collect();
        DriverResponse::BatchResult {
            request_id,
            items,
            checkpoint: u32::try_from(next).unwrap_or(total),
            total,
        }
    }

    async fn execute(
        &self,
        request_id: String,
        spec: &OperationSpec,
        input: BTreeMap<String, DriverValue>,
        options: InvokeOptions,
        _permit: OwnedSemaphorePermit,
    ) -> DriverResponse {
        let application_input = match input
            .into_iter()
            .map(|(name, value)| Ok((name, lower_value(value)?)))
            .collect::<Result<BTreeMap<_, _>, DriverHostError>>()
        {
            Ok(input) => input,
            Err(_) => {
                return local_error(
                    Some(request_id),
                    Some(spec.public_name().to_owned()),
                    "RDB-INPUT-0101",
                    "input",
                    "application input is invalid",
                    false,
                    "correct_request",
                );
            }
        };
        let mut client = self.inner.pool.select();
        match spec.kind() {
            OperationKind::Command => {
                if options.accept_compact_result {
                    return local_error(
                        Some(request_id),
                        Some(spec.public_name().to_owned()),
                        "RDB-INPUT-0101",
                        "input",
                        "application input is invalid",
                        false,
                        "correct_request",
                    );
                }
                let command = match ApplicationCommand::new(
                    spec.symbol(),
                    Some(self.inner.catalog.contract_version()),
                    application_input,
                ) {
                    Ok(command) => command,
                    Err(error) => return application_error(request_id, spec, error, true),
                };
                let attempts = AttemptBudget::new(options.maximum_attempts)
                    .expect("protocol checked nonzero attempt bound");
                match client
                    .execute_command(command, attempts, &self.inner.metadata)
                    .await
                {
                    Ok(result) => {
                        let mut fields = match result.outcome_value.map(raise_value) {
                            Some(DriverValue::Record(fields)) => fields,
                            Some(value) => BTreeMap::from([("value".to_owned(), value)]),
                            None => BTreeMap::new(),
                        };
                        if let Some(outcome) = result.outcome {
                            fields.insert("outcome".to_owned(), DriverValue::Enum(outcome));
                        }
                        DriverResponse::Result {
                            request_id,
                            value: DriverValue::Record(fields),
                            application_head: result.commit_sequence,
                            cursor: result.outcome_uri,
                            replayed: result.replayed,
                        }
                    }
                    Err(error) => application_error(request_id, spec, error, true),
                }
            }
            OperationKind::Query => {
                let accept_compact_result = options.accept_compact_result;
                let contract = ApplicationContract::Exact {
                    lineage: self.inner.catalog.contract_lineage().to_owned(),
                    version: self.inner.catalog.contract_version(),
                    bundle_hash: Some(self.inner.catalog.contract_bundle_hash()),
                };
                let mut query_options = QueryOptions::new();
                if let Some(cursor) = options.cursor {
                    query_options = query_options.after(cursor);
                }
                if let Some(frontier) = options.read_after_commit {
                    query_options = query_options.read_after_commit(frontier);
                }
                let query = NamedQuery::new(
                    contract,
                    spec.symbol(),
                    spec.module_hash(),
                    application_input,
                    None,
                )
                .and_then(|query| {
                    let query = spec
                        .plan_hash()
                        .map_or(query.clone(), |hash| query.expect_plan_hash(hash));
                    query.with_options(query_options)
                });
                let query = match query {
                    Ok(query) => query,
                    Err(error) => return application_error(request_id, spec, error, false),
                };
                if accept_compact_result {
                    match client
                        .execute_named_query_wire(query, &self.inner.metadata)
                        .await
                    {
                        Ok(response)
                            if response.selected_result_encoding
                                == app_v1::NamedResultEncoding::CompactV1 as i32 =>
                        {
                            let compact = match response.compact_result {
                                Some(compact) if response.fields.is_empty() => compact,
                                _ => {
                                    return application_error(
                                        request_id,
                                        spec,
                                        ApplicationClientError::InvalidResponse,
                                        false,
                                    );
                                }
                            };
                            let rows = compact
                                .rows
                                .into_iter()
                                .map(|row| {
                                    row.values
                                        .into_iter()
                                        .map(|value| {
                                            raise_wire_value(value).map(raise_value).map_err(|_| ())
                                        })
                                        .collect::<Result<Vec<_>, _>>()
                                })
                                .collect::<Result<Vec<_>, _>>();
                            let rows = match rows {
                                Ok(rows) => rows,
                                Err(()) => {
                                    return application_error(
                                        request_id,
                                        spec,
                                        ApplicationClientError::InvalidResponse,
                                        false,
                                    );
                                }
                            };
                            DriverResponse::CompactQueryResult {
                                request_id,
                                outcome: response.outcome,
                                result_name: compact.name,
                                entity: compact.entity,
                                fields: compact.fields,
                                rows,
                                application_head: response.application_head,
                                cursor: response.next_cursor,
                            }
                        }
                        Ok(response) => match raise_wire_query_result(response) {
                            Ok(result) => legacy_query_response(request_id, result),
                            Err(error) => application_error(request_id, spec, error, false),
                        },
                        Err(error) => application_error(request_id, spec, error, false),
                    }
                } else {
                    match client
                        .execute_named_query(query, &self.inner.metadata)
                        .await
                    {
                        Ok(result) => legacy_query_response(request_id, result),
                        Err(error) => application_error(request_id, spec, error, false),
                    }
                }
            }
            OperationKind::VectorInspection => {
                if options.accept_compact_result || options.read_after_commit.is_some() {
                    return local_error(
                        Some(request_id),
                        Some(spec.public_name().to_owned()),
                        "RDB-INPUT-0101",
                        "input",
                        "application input is invalid",
                        false,
                        "correct_request",
                    );
                }
                let target = match spec.vector_target() {
                    Some(target) => target,
                    None => {
                        return application_error(
                            request_id,
                            spec,
                            ApplicationClientError::InvalidResponse,
                            false,
                        );
                    }
                };
                let mut input = application_input;
                let partition = match input.remove("partition") {
                    Some(value) => value,
                    None => {
                        return application_error(
                            request_id,
                            spec,
                            ApplicationClientError::InvalidInput,
                            false,
                        );
                    }
                };
                let limit = match input.remove("limit") {
                    Some(ApplicationValue::U64(value)) => match u32::try_from(value) {
                        Ok(value) if (1..=500).contains(&value) => value,
                        _ => {
                            return application_error(
                                request_id,
                                spec,
                                ApplicationClientError::InvalidInput,
                                false,
                            );
                        }
                    },
                    _ => {
                        return application_error(
                            request_id,
                            spec,
                            ApplicationClientError::InvalidInput,
                            false,
                        );
                    }
                };
                if !input.is_empty() {
                    return application_error(
                        request_id,
                        spec,
                        ApplicationClientError::InvalidInput,
                        false,
                    );
                }
                let kind = match target.kind() {
                    VectorInspectionKind::Staleness => VectorStateInspectionKind::Stale,
                    VectorInspectionKind::ModelVersions => VectorStateInspectionKind::OutdatedModel,
                };
                let mut inspection = VectorStateInspection::new(
                    ApplicationContract::Exact {
                        lineage: self.inner.catalog.contract_lineage().to_owned(),
                        version: self.inner.catalog.contract_version(),
                        bundle_hash: Some(self.inner.catalog.contract_bundle_hash()),
                    },
                    target.entity(),
                    target.field(),
                    partition,
                    kind,
                    limit,
                );
                if let Some(cursor) = options.cursor {
                    let cursor = match BASE64.decode(cursor) {
                        Ok(cursor) => cursor,
                        Err(_) => {
                            return application_error(
                                request_id,
                                spec,
                                ApplicationClientError::InvalidInput,
                                false,
                            );
                        }
                    };
                    inspection = inspection.after(cursor);
                }
                match client
                    .inspect_vector_state(inspection, &self.inner.metadata)
                    .await
                {
                    Ok(result) => vector_inspection_response(request_id, result),
                    Err(error) => application_error(request_id, spec, error, false),
                }
            }
            OperationKind::Reactive => {
                if options.accept_compact_result {
                    return local_error(
                        Some(request_id),
                        Some(spec.public_name().to_owned()),
                        "RDB-INPUT-0101",
                        "input",
                        "application input is invalid",
                        false,
                        "correct_request",
                    );
                }
                self.execute_reactive(request_id, spec, application_input, options, &mut client)
                    .await
            }
        }
    }

    async fn execute_reactive(
        &self,
        request_id: String,
        spec: &OperationSpec,
        mut input: BTreeMap<String, ApplicationValue>,
        options: InvokeOptions,
        client: &mut StableApplicationClient,
    ) -> DriverResponse {
        let parameters = match take_record(&mut input, "parameters") {
            Ok(value) => value,
            Err(error) => return application_error(request_id, spec, error, false),
        };
        let module_hash = match spec.module_hash() {
            Some(value) => value,
            None => {
                return local_error(
                    Some(request_id),
                    Some(spec.public_name().to_owned()),
                    "RDB-DRIVER-0002",
                    "identity",
                    "generated reactive identity does not match",
                    false,
                    "regenerate_bindings",
                );
            }
        };
        let operation =
            match ApplicationReactiveOperation::new(module_hash, spec.symbol(), parameters) {
                Ok(value) => value,
                Err(error) => return application_error(request_id, spec, error, false),
            };
        let action = spec.reactive_action().unwrap_or_default();
        let result: Result<(DriverValue, Option<u64>, Option<String>), ApplicationClientError> =
            async {
                match (spec.reactive_kind(), action) {
                    (Some(ReactiveKind::Stream), "next") => {
                        let consumer =
                            consumer(operation, take_string(&mut input, "consumer_name")?)?;
                        let batch_limit = take_bounded_u64(&mut input, "batch_limit", 1, 1, 64)?;
                        let in_flight_limit =
                            take_bounded_u64(&mut input, "in_flight_limit", 16, 1, 64)?;
                        let lease_seconds =
                            take_bounded_u64(&mut input, "lease_seconds", 60, 5, 900)?;
                        let wait = options
                            .deadline_millis
                            .saturating_mul(1_000_000)
                            .min(30_000_000_000);
                        let batch = client
                            .consume_event_stream(
                                &consumer,
                                EventConsumerOptions {
                                    batch_limit: u32::try_from(batch_limit)
                                        .map_err(|_| ApplicationClientError::InvalidInput)?,
                                    in_flight_limit: u32::try_from(in_flight_limit)
                                        .map_err(|_| ApplicationClientError::InvalidInput)?,
                                    lease_seconds,
                                    maximum_wait_nanos: wait,
                                },
                                &self.inner.metadata,
                            )
                            .await?;
                        Ok((raise_event_batch(batch), None, None))
                    }
                    (Some(ReactiveKind::Stream), "ack") | (Some(ReactiveKind::Stream), "nack") => {
                        let consumer =
                            consumer(operation, take_string(&mut input, "consumer_name")?)?;
                        let evidence = take_lease(&mut input)?;
                        let outcome = if action == "ack" {
                            client
                                .acknowledge_event_lease(&consumer, &evidence, &self.inner.metadata)
                                .await?
                        } else {
                            let retry_delay_nanos = take_bounded_u64(
                                &mut input,
                                "retry_delay_nanos",
                                0,
                                0,
                                300_000_000_000,
                            )?;
                            client
                                .negative_acknowledge_event_lease(
                                    &consumer,
                                    &evidence,
                                    retry_delay_nanos,
                                    &self.inner.metadata,
                                )
                                .await?
                        };
                        Ok((mutation_result(outcome), None, None))
                    }
                    (Some(ReactiveKind::Stream), "seek") => {
                        let consumer =
                            consumer(operation, take_string(&mut input, "consumer_name")?)?;
                        let checkpoint = parse_checkpoint(&take_string(&mut input, "checkpoint")?)?;
                        let outcome = client
                            .seek_event_consumer(&consumer, checkpoint, &self.inner.metadata)
                            .await?;
                        Ok((mutation_result(outcome), None, None))
                    }
                    (Some(ReactiveKind::Stream), "status") => {
                        let consumer =
                            consumer(operation, take_string(&mut input, "consumer_name")?)?;
                        let status = client
                            .event_consumer_status(&consumer, &self.inner.metadata)
                            .await?;
                        Ok((
                            status
                                .map(raise_consumer_public_status)
                                .unwrap_or(DriverValue::Null),
                            None,
                            None,
                        ))
                    }
                    (Some(ReactiveKind::Subscription), "next") => {
                        let consumer =
                            consumer(operation, take_string(&mut input, "consumer_name")?)?;
                        let wait = options
                            .deadline_millis
                            .saturating_mul(1_000_000)
                            .min(30_000_000_000);
                        let batch = client
                            .consume_contextual_subscription(&consumer, wait, &self.inner.metadata)
                            .await?;
                        Ok((raise_contextual_batch(batch), None, None))
                    }
                    (Some(ReactiveKind::Subscription), "ack")
                    | (Some(ReactiveKind::Subscription), "nack") => {
                        let consumer =
                            consumer(operation, take_string(&mut input, "consumer_name")?)?;
                        let evidence = take_lease(&mut input)?;
                        let outcome = if action == "ack" {
                            client
                                .acknowledge_contextual_lease(
                                    &consumer,
                                    &evidence,
                                    &self.inner.metadata,
                                )
                                .await?
                        } else {
                            let retry_delay_nanos = take_bounded_u64(
                                &mut input,
                                "retry_delay_nanos",
                                0,
                                0,
                                300_000_000_000,
                            )?;
                            client
                                .negative_acknowledge_contextual_lease(
                                    &consumer,
                                    &evidence,
                                    retry_delay_nanos,
                                    &self.inner.metadata,
                                )
                                .await?
                        };
                        Ok((mutation_result(outcome), None, None))
                    }
                    (Some(ReactiveKind::Subscription), "status") => {
                        let consumer =
                            consumer(operation, take_string(&mut input, "consumer_name")?)?;
                        let status = client
                            .contextual_subscription_status(&consumer, &self.inner.metadata)
                            .await?;
                        Ok((
                            status
                                .map(raise_consumer_public_status)
                                .unwrap_or(DriverValue::Null),
                            None,
                            None,
                        ))
                    }
                    (Some(ReactiveKind::Subscription), action) if action.starts_with("react_") => {
                        let consumer =
                            consumer(operation, take_string(&mut input, "consumer_name")?)?;
                        let reaction = spec
                            .reaction()
                            .ok_or(ApplicationClientError::InvalidInput)?;
                        let token =
                            take_string(&mut input, "causation_token").and_then(|value| {
                                BASE64
                                    .decode(value)
                                    .map_err(|_| ApplicationClientError::InvalidInput)
                            })?;
                        let reaction = ApplicationContextualReaction::checked(
                            reaction.name(),
                            reaction.command_name(),
                            reaction.command_id(),
                            token,
                        )?;
                        let command = ApplicationCommand::new(
                            reaction.command_name.clone(),
                            Some(self.inner.catalog.contract_version()),
                            take_record(&mut input, "input")?,
                        )?;
                        let result = client
                            .execute_contextual_reaction(
                                &consumer,
                                &reaction,
                                command,
                                &self.inner.metadata,
                            )
                            .await?;
                        let mut fields = match result.outcome_value.map(raise_value) {
                            Some(DriverValue::Record(fields)) => fields,
                            Some(value) => BTreeMap::from([("value".to_owned(), value)]),
                            None => BTreeMap::new(),
                        };
                        if let Some(outcome) = result.outcome {
                            fields.insert("outcome".to_owned(), DriverValue::Enum(outcome));
                        }
                        Ok((
                            DriverValue::Record(fields),
                            result.commit_sequence,
                            result.outcome_uri,
                        ))
                    }
                    (Some(ReactiveKind::Watch), "watch") => {
                        let cursor = match input.remove("cursor") {
                            Some(ApplicationValue::String(value)) => {
                                Some(LiveQueryCursor::from_standard_base64(&value)?)
                            }
                            Some(ApplicationValue::Null) | None => options
                                .cursor
                                .map(|value| LiveQueryCursor::from_standard_base64(&value))
                                .transpose()?,
                            _ => return Err(ApplicationClientError::InvalidInput),
                        };
                        let mut stream = client
                            .watch_named_query(&operation, cursor, &self.inner.metadata)
                            .await?;
                        let update = stream
                            .message()
                            .await?
                            .ok_or(ApplicationClientError::InvalidResponse)?;
                        let (value, head, cursor) = raise_live_update(update);
                        Ok((value, head, cursor))
                    }
                    _ => Err(ApplicationClientError::InvalidInput),
                }
            }
            .await;
        match result {
            Ok((value, application_head, cursor)) => DriverResponse::Result {
                request_id,
                value,
                application_head,
                cursor,
                replayed: false,
            },
            Err(error) => application_error(request_id, spec, error, action.starts_with("react_")),
        }
    }
}

fn vector_inspection_response(
    request_id: String,
    result: VectorStateInspectionResult,
) -> DriverResponse {
    let (fields, application_head, cursor) = match result {
        VectorStateInspectionResult::StalenessSummary(summary) => (
            BTreeMap::from([
                (
                    "kind".to_owned(),
                    DriverValue::Enum("staleness_summary".to_owned()),
                ),
                (
                    "total_entities".to_owned(),
                    DriverValue::U64(summary.total_entities.to_string()),
                ),
                (
                    "stale_count".to_owned(),
                    DriverValue::U64(summary.stale_count.to_string()),
                ),
                (
                    "stale_entity_count_threshold".to_owned(),
                    DriverValue::U64(summary.stale_entity_count_threshold.to_string()),
                ),
                (
                    "slo_breached".to_owned(),
                    DriverValue::Bool(summary.slo_breached),
                ),
            ]),
            None,
            None,
        ),
        VectorStateInspectionResult::StaleEntities(page) => {
            let application_head = page.observed_frontier;
            let cursor = page.next_cursor;
            let items = page.items.into_iter().map(vector_staleness_item).collect();
            vector_inspection_page("stale_entities", items, application_head, cursor)
        }
        VectorStateInspectionResult::ModelVersionSummary(summary) => (
            BTreeMap::from([
                (
                    "kind".to_owned(),
                    DriverValue::Enum("model_version_summary".to_owned()),
                ),
                (
                    "current_count".to_owned(),
                    DriverValue::U64(summary.current_count.to_string()),
                ),
                (
                    "outdated_count".to_owned(),
                    DriverValue::U64(summary.outdated_count.to_string()),
                ),
            ]),
            None,
            None,
        ),
        VectorStateInspectionResult::OutdatedModelEntities(page) => {
            let application_head = page.observed_frontier;
            let cursor = page.next_cursor;
            let items = page
                .items
                .into_iter()
                .map(vector_model_version_item)
                .collect();
            vector_inspection_page("outdated_model_entities", items, application_head, cursor)
        }
    };
    DriverResponse::Result {
        request_id,
        value: DriverValue::Record(fields),
        application_head,
        cursor,
        replayed: false,
    }
}

fn vector_inspection_page(
    kind: &str,
    items: Vec<DriverValue>,
    application_head: Option<u64>,
    next_cursor: Option<Vec<u8>>,
) -> (BTreeMap<String, DriverValue>, Option<u64>, Option<String>) {
    let cursor = next_cursor.map(|cursor| BASE64.encode(cursor));
    (
        BTreeMap::from([
            ("kind".to_owned(), DriverValue::Enum(kind.to_owned())),
            ("items".to_owned(), DriverValue::List(items)),
            (
                "observed_frontier".to_owned(),
                application_head.map_or(DriverValue::Null, |value| {
                    DriverValue::U64(value.to_string())
                }),
            ),
        ]),
        application_head,
        cursor,
    )
}

fn vector_staleness_item(item: VectorStalenessItem) -> DriverValue {
    DriverValue::Record(BTreeMap::from([
        (
            "entity_key".to_owned(),
            DriverValue::Bytes(BASE64.encode(item.entity_key)),
        ),
        (
            "newest_source_write".to_owned(),
            DriverValue::U64(item.newest_source_write.to_string()),
        ),
        (
            "embedding_write".to_owned(),
            item.embedding_write.map_or(DriverValue::Null, |value| {
                DriverValue::U64(value.to_string())
            }),
        ),
    ]))
}

fn vector_model_version_item(item: VectorModelVersionItem) -> DriverValue {
    DriverValue::Record(BTreeMap::from([
        (
            "entity_key".to_owned(),
            DriverValue::Bytes(BASE64.encode(item.entity_key)),
        ),
        ("model".to_owned(), DriverValue::String(item.model)),
        (
            "model_version".to_owned(),
            DriverValue::String(item.model_version),
        ),
        (
            "embedding_write".to_owned(),
            DriverValue::U64(item.embedding_write.to_string()),
        ),
    ]))
}

fn legacy_query_response(
    request_id: String,
    result: riffdb_client_rust::NamedQueryResult,
) -> DriverResponse {
    let mut fields = BTreeMap::from([("outcome".to_owned(), DriverValue::Enum(result.outcome))]);
    for (name, field) in result.fields {
        let value = match field.cardinality {
            ApplicationCardinality::One | ApplicationCardinality::Maybe => field
                .records
                .into_iter()
                .next()
                .map(raise_record)
                .unwrap_or(DriverValue::Null),
            ApplicationCardinality::Many => {
                DriverValue::List(field.records.into_iter().map(raise_record).collect())
            }
        };
        fields.insert(name, value);
    }
    DriverResponse::Result {
        request_id,
        value: DriverValue::Record(fields),
        application_head: Some(result.application_head),
        cursor: result.next_cursor,
        replayed: false,
    }
}

fn invocation_deadline(
    kind: OperationKind,
    reactive_action: Option<&str>,
    caller_wait: Duration,
) -> Duration {
    if kind == OperationKind::Reactive && reactive_action == Some("next") {
        caller_wait.saturating_add(REACTIVE_PROCESSING_ALLOWANCE)
    } else {
        caller_wait
    }
}

/// Closed host setup/runtime failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverHostError {
    /// One pool size or remote transport bound is invalid.
    InvalidConfiguration,
    /// The verified remote endpoint could not be reached.
    RemoteUnavailable,
    /// The authenticated remote database does not match the exact application lock.
    RemoteIdentityMismatch,
    /// The bounded local admission queue is full.
    Overloaded,
    /// A local typed value is malformed or out of range.
    InvalidValue,
}
impl fmt::Display for DriverHostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfiguration => "driver host configuration is invalid",
            Self::RemoteUnavailable => "driver remote endpoint is unavailable",
            Self::RemoteIdentityMismatch => "driver remote application identity does not match",
            Self::Overloaded => "driver host is over capacity",
            Self::InvalidValue => "driver application value is invalid",
        })
    }
}
impl std::error::Error for DriverHostError {}

fn application_error(
    request_id: String,
    spec: &OperationSpec,
    error: ApplicationClientError,
    command: bool,
) -> DriverResponse {
    if let Some(semantic) = error.semantic_error() {
        let code = semantic.code();
        let context = semantic.context();
        let (contract_lineage, contract_version) = context
            .contract()
            .map_or((None, None), |(lineage, version)| {
                (Some(lineage.as_str().to_owned()), Some(version.get()))
            });
        return DriverResponse::Error {
            request_id: Some(request_id),
            code: code.as_str().to_owned(),
            category: code.category().as_str().to_owned(),
            operation: context
                .operation_symbol()
                .map(str::to_owned)
                .or_else(|| Some(spec.public_name().to_owned())),
            symbol_path: context.symbol_path().to_vec(),
            contract_lineage,
            contract_version,
            trace_id: context.trace_id().map(|value| value.to_string()),
            incident_id: semantic.incident_id().map(ToString::to_string),
            message: code.safe_message().to_owned(),
            retryability: if matches!(
                code.recovery_action(),
                riffdb_errors::ApplicationRecoveryAction::Retry
            ) {
                "retryable"
            } else {
                "not_retryable"
            }
            .to_owned(),
            recovery_action: code.recovery_action().as_str().to_owned(),
            outcome_uncertain: code == riffdb_errors::ApplicationErrorCode::OutcomeUnknown,
        };
    }
    let (code, category, message, uncertain, recovery) = match &error {
        ApplicationClientError::InvalidInput => (
            "RDB-INPUT-0101",
            "input",
            "application input is invalid",
            false,
            "correct_request",
        ),
        ApplicationClientError::InvalidResponse => (
            "RDB-PROTOCOL-0101",
            "protocol",
            "the RiffDB peer returned an invalid application response",
            command,
            "contact_operator",
        ),
        ApplicationClientError::IdentifierUnavailable => (
            "RDB-INTERNAL-0001",
            "internal",
            "an internal error occurred",
            false,
            "contact_operator",
        ),
        ApplicationClientError::Client(riffdb_client_rust::ClientError::OutcomeUnknown(_)) => (
            "RDB-UNCERTAIN-0101",
            "uncertainty",
            "command outcome is not yet known",
            true,
            "resolve_with_same_idempotency_key",
        ),
        ApplicationClientError::Client(riffdb_client_rust::ClientError::DetailsFree(status)) => {
            match status {
                riffdb_client_rust::DetailsFreeStatus::Unauthenticated => (
                    "RDB-AUTH-0214",
                    "authorization",
                    "application operation is not authorized",
                    false,
                    "obtain_permission",
                ),
                riffdb_client_rust::DetailsFreeStatus::Cancelled => (
                    "RDB-APP-0002",
                    "control",
                    "application request was cancelled",
                    command,
                    if command {
                        "resolve_with_same_idempotency_key"
                    } else {
                        "none"
                    },
                ),
                riffdb_client_rust::DetailsFreeStatus::DeadlineExceeded => (
                    "RDB-APP-0003",
                    "control",
                    "application request deadline elapsed",
                    command,
                    if command {
                        "resolve_with_same_idempotency_key"
                    } else {
                        "retry"
                    },
                ),
                riffdb_client_rust::DetailsFreeStatus::ResponseTooLarge => (
                    "RDB-RESOURCE-0101",
                    "resource",
                    "application result exceeds the service limit",
                    false,
                    "correct_request",
                ),
                riffdb_client_rust::DetailsFreeStatus::EmergencyInternal => (
                    "RDB-INTERNAL-0001",
                    "internal",
                    "an internal error occurred",
                    command,
                    "contact_operator",
                ),
                riffdb_client_rust::DetailsFreeStatus::TransportUnavailable => (
                    "RDB-DRIVER-0101",
                    "transport",
                    "verified remote transport is unavailable",
                    command,
                    if command {
                        "resolve_with_same_idempotency_key"
                    } else {
                        "retry"
                    },
                ),
            }
        }
        ApplicationClientError::Client(client) if client.public_error().is_some() => {
            let public = client.public_error().expect("guarded");
            let code = riffdb_errors::ApplicationErrorCode::from_public_kind(public.kind());
            (
                code.as_str(),
                code.category().as_str(),
                code.safe_message(),
                command && code == riffdb_errors::ApplicationErrorCode::OutcomeUnknown,
                code.recovery_action().as_str(),
            )
        }
        ApplicationClientError::Client(
            riffdb_client_rust::ClientError::ConnectionFailure
            | riffdb_client_rust::ClientError::Tls(_),
        ) => (
            "RDB-DRIVER-0101",
            "transport",
            "verified remote transport is unavailable",
            command,
            if command {
                "resolve_with_same_idempotency_key"
            } else {
                "retry"
            },
        ),
        ApplicationClientError::Client(riffdb_client_rust::ClientError::Protocol(_)) => (
            "RDB-PROTOCOL-0101",
            "protocol",
            "the RiffDB peer returned an invalid application response",
            command,
            "contact_operator",
        ),
        ApplicationClientError::Client(riffdb_client_rust::ClientError::IdentifierGeneration(
            _,
        )) => (
            "RDB-INTERNAL-0001",
            "internal",
            "an internal error occurred",
            false,
            "contact_operator",
        ),
        ApplicationClientError::Client(
            riffdb_client_rust::ClientError::Application(_)
            | riffdb_client_rust::ClientError::Public(_),
        ) => unreachable!("handled by semantic/public guards"),
    };
    local_error(
        Some(request_id),
        Some(spec.public_name().to_owned()),
        code,
        category,
        message,
        uncertain,
        recovery,
    )
}

fn local_error(
    request_id: Option<String>,
    operation: Option<String>,
    code: &str,
    category: &str,
    message: &str,
    outcome_uncertain: bool,
    recovery_action: &str,
) -> DriverResponse {
    DriverResponse::Error {
        request_id,
        code: code.to_owned(),
        category: category.to_owned(),
        operation,
        symbol_path: Vec::new(),
        contract_lineage: None,
        contract_version: None,
        trace_id: None,
        incident_id: None,
        message: message.to_owned(),
        retryability: if recovery_action == "retry" || recovery_action == "retry_later" {
            "retryable"
        } else {
            "not_retryable"
        }
        .to_owned(),
        recovery_action: recovery_action.to_owned(),
        outcome_uncertain,
    }
}

fn lower_value(value: DriverValue) -> Result<ApplicationValue, DriverHostError> {
    match value {
        DriverValue::Null => Ok(ApplicationValue::Null),
        DriverValue::Bool(value) => Ok(ApplicationValue::Bool(value)),
        DriverValue::I64(value) => value
            .parse()
            .map(ApplicationValue::I64)
            .map_err(|_| DriverHostError::InvalidValue),
        DriverValue::U64(value) => value
            .parse()
            .map(ApplicationValue::U64)
            .map_err(|_| DriverHostError::InvalidValue),
        DriverValue::String(value) => Ok(ApplicationValue::String(value)),
        DriverValue::Uuid(value) => ApplicationUuid::from_text(value)
            .map(ApplicationValue::Uuid)
            .map_err(|_| DriverHostError::InvalidValue),
        DriverValue::Enum(value) => Ok(ApplicationValue::Enum(value)),
        DriverValue::Bytes(value) => BASE64
            .decode(value)
            .map(ApplicationValue::Bytes)
            .map_err(|_| DriverHostError::InvalidValue),
        DriverValue::Date(value) => value
            .parse()
            .map(ApplicationValue::Date)
            .map_err(|_| DriverHostError::InvalidValue),
        DriverValue::Timestamp(value) => Ok(ApplicationValue::Timestamp {
            seconds: value
                .seconds
                .parse()
                .map_err(|_| DriverHostError::InvalidValue)?,
            nanos: value.nanos,
        }),
        DriverValue::Decimal(value) => Ok(ApplicationValue::Decimal {
            coefficient_twos_complement: BASE64
                .decode(value.coefficient)
                .map_err(|_| DriverHostError::InvalidValue)?,
            scale: value.scale,
            precision: value.precision,
        }),
        DriverValue::Money(value) => Ok(ApplicationValue::Money {
            currency: value.currency,
            amount: Box::new(ApplicationValue::Decimal {
                coefficient_twos_complement: BASE64
                    .decode(value.amount.coefficient)
                    .map_err(|_| DriverHostError::InvalidValue)?,
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
        .map_err(|_| DriverHostError::InvalidValue),
        DriverValue::List(values) => values
            .into_iter()
            .map(lower_value)
            .collect::<Result<Vec<_>, _>>()
            .map(ApplicationValue::List),
        DriverValue::Record(values) => values
            .into_iter()
            .map(|(name, value)| Ok((name, lower_value(value)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(ApplicationValue::Record),
    }
}
fn raise_value(value: ApplicationValue) -> DriverValue {
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
        ApplicationValue::Money { currency, amount } => match raise_value(*amount) {
            DriverValue::Decimal(amount) => DriverValue::Money(DriverMoney { currency, amount }),
            _ => DriverValue::Null,
        },
        ApplicationValue::String(value) => DriverValue::String(value),
        ApplicationValue::Uuid(value) => DriverValue::Uuid(value.into_string()),
        ApplicationValue::Enum(value) => DriverValue::Enum(value),
        ApplicationValue::EnumIdentity { name, .. } => DriverValue::Enum(name),
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
            DriverValue::List(values.into_iter().map(raise_value).collect())
        }
        ApplicationValue::Record(values) => DriverValue::Record(
            values
                .into_iter()
                .map(|(name, value)| (name, raise_value(value)))
                .collect(),
        ),
    }
}
fn raise_record(record: ApplicationRecord) -> DriverValue {
    DriverValue::Record(
        record
            .fields
            .into_iter()
            .map(|(name, value)| (name, raise_value(value)))
            .collect(),
    )
}

fn take_string(
    input: &mut BTreeMap<String, ApplicationValue>,
    name: &str,
) -> Result<String, ApplicationClientError> {
    match input.remove(name) {
        Some(ApplicationValue::String(value)) | Some(ApplicationValue::Enum(value)) => Ok(value),
        _ => Err(ApplicationClientError::InvalidInput),
    }
}
fn take_record(
    input: &mut BTreeMap<String, ApplicationValue>,
    name: &str,
) -> Result<BTreeMap<String, ApplicationValue>, ApplicationClientError> {
    match input.remove(name) {
        Some(ApplicationValue::Record(value)) => Ok(value),
        _ => Err(ApplicationClientError::InvalidInput),
    }
}
fn take_bounded_u64(
    input: &mut BTreeMap<String, ApplicationValue>,
    name: &str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<u64, ApplicationClientError> {
    let value = match input.remove(name) {
        None => default,
        Some(ApplicationValue::U64(value)) => value,
        Some(ApplicationValue::I64(value)) => {
            u64::try_from(value).map_err(|_| ApplicationClientError::InvalidInput)?
        }
        Some(ApplicationValue::String(value)) => value
            .parse()
            .map_err(|_| ApplicationClientError::InvalidInput)?,
        _ => return Err(ApplicationClientError::InvalidInput),
    };
    if !(minimum..=maximum).contains(&value) {
        return Err(ApplicationClientError::InvalidInput);
    }
    Ok(value)
}
fn consumer(
    operation: ApplicationReactiveOperation,
    name: String,
) -> Result<ApplicationEventConsumer, ApplicationClientError> {
    ApplicationEventConsumer::new(operation, name)
}
fn parse_event_id(value: &str) -> Result<ApplicationEventId, ApplicationClientError> {
    let (commit, ordinal) = value
        .split_once(':')
        .ok_or(ApplicationClientError::InvalidInput)?;
    let commit_sequence = commit
        .parse()
        .map_err(|_| ApplicationClientError::InvalidInput)?;
    let event_ordinal = ordinal
        .parse()
        .map_err(|_| ApplicationClientError::InvalidInput)?;
    if commit_sequence == 0 {
        return Err(ApplicationClientError::InvalidInput);
    }
    Ok(ApplicationEventId {
        commit_sequence,
        event_ordinal,
    })
}
fn take_lease(
    input: &mut BTreeMap<String, ApplicationValue>,
) -> Result<ApplicationEventLeaseEvidence, ApplicationClientError> {
    let event_id = parse_event_id(&take_string(input, "event_id")?)?;
    let lease = BASE64
        .decode(take_string(input, "lease_token")?)
        .map_err(|_| ApplicationClientError::InvalidInput)?;
    let history = take_bounded_u64(input, "history_incarnation", 0, 1, u64::MAX)?;
    ApplicationEventLeaseEvidence::new(event_id, lease, history)
}
fn parse_checkpoint(value: &str) -> Result<ApplicationEventCheckpoint, ApplicationClientError> {
    if value == "before-first" {
        Ok(ApplicationEventCheckpoint::BeforeFirst)
    } else {
        parse_event_id(value).map(ApplicationEventCheckpoint::After)
    }
}

fn event_id(value: ApplicationEventId) -> DriverValue {
    DriverValue::String(format!("{}:{}", value.commit_sequence, value.event_ordinal))
}
fn mutation_result(value: ApplicationEventMutationResult) -> DriverValue {
    DriverValue::Enum(
        match value {
            ApplicationEventMutationResult::Applied => "applied",
            ApplicationEventMutationResult::StateChanged => "state_changed",
            ApplicationEventMutationResult::NotFound => "not_found",
            ApplicationEventMutationResult::OutstandingLease => "outstanding_lease",
            ApplicationEventMutationResult::StaleLease => "stale_lease",
            ApplicationEventMutationResult::LeaseExpired => "lease_expired",
        }
        .to_owned(),
    )
}
fn raise_checkpoint(value: ApplicationEventCheckpoint) -> DriverValue {
    match value {
        ApplicationEventCheckpoint::BeforeFirst => DriverValue::String("before-first".to_owned()),
        ApplicationEventCheckpoint::After(id) => event_id(id),
    }
}
fn raise_consumer_status(value: ApplicationEventConsumerStatus) -> DriverValue {
    DriverValue::Record(BTreeMap::from([
        (
            "revision".to_owned(),
            DriverValue::U64(value.revision.to_string()),
        ),
        ("checkpoint".to_owned(), raise_checkpoint(value.checkpoint)),
        (
            "history_incarnation".to_owned(),
            DriverValue::U64(value.history_incarnation.to_string()),
        ),
        (
            "live_leases".to_owned(),
            DriverValue::U64(u64::from(value.live_leases).to_string()),
        ),
        (
            "retries".to_owned(),
            DriverValue::U64(u64::from(value.retries).to_string()),
        ),
        (
            "dead_letters".to_owned(),
            DriverValue::U64(u64::from(value.dead_letters).to_string()),
        ),
    ]))
}

fn raise_consumer_public_status(value: ApplicationEventConsumerPublicStatus) -> DriverValue {
    match value {
        ApplicationEventConsumerPublicStatus::Exact(status) => raise_consumer_status(status),
        ApplicationEventConsumerPublicStatus::Protected(status) => {
            DriverValue::Record(BTreeMap::from([
                ("kind".to_owned(), DriverValue::Enum("protected".to_owned())),
                (
                    "history_incarnation".to_owned(),
                    DriverValue::U64(status.history_incarnation.to_string()),
                ),
                (
                    "progress_cursor".to_owned(),
                    DriverValue::Bytes(BASE64.encode(status.progress_cursor.as_bytes())),
                ),
            ]))
        }
    }
}

fn raise_event_disposition(value: ApplicationEventPullDisposition) -> DriverValue {
    DriverValue::Enum(
        match value {
            ApplicationEventPullDisposition::Ready => "ready",
            ApplicationEventPullDisposition::WaitTimedOut => "wait_timed_out",
            ApplicationEventPullDisposition::BoundedProgress => "bounded_progress",
        }
        .to_owned(),
    )
}
fn raise_delivery(value: riffdb_client_rust::ApplicationEventDelivery) -> DriverValue {
    let event = value.event;
    DriverValue::Record(BTreeMap::from([
        ("event_id".to_owned(), event_id(event.id)),
        ("event_name".to_owned(), DriverValue::Enum(event.name)),
        (
            "writer_contract_version".to_owned(),
            DriverValue::U64(event.writer_contract_version.to_string()),
        ),
        (
            "command_name".to_owned(),
            DriverValue::String(event.command_name),
        ),
        ("actor_kind".to_owned(), DriverValue::Enum(event.actor_kind)),
        (
            "provenance_uri".to_owned(),
            DriverValue::String(event.provenance_uri),
        ),
        (
            "history_incarnation".to_owned(),
            DriverValue::U64(event.history_incarnation.to_string()),
        ),
        (
            "fields".to_owned(),
            DriverValue::Record(
                event
                    .fields
                    .into_iter()
                    .map(|(name, value)| (name, raise_value(value)))
                    .collect(),
            ),
        ),
        (
            "attempt".to_owned(),
            DriverValue::U64(u64::from(value.attempt).to_string()),
        ),
        (
            "lease_token".to_owned(),
            DriverValue::String(BASE64.encode(value.lease_token)),
        ),
        (
            "expires_at".to_owned(),
            DriverValue::Timestamp(DriverTimestamp {
                seconds: value.expires_at.0.to_string(),
                nanos: value.expires_at.1,
            }),
        ),
    ]))
}
fn raise_event_batch(value: ApplicationEventBatch) -> DriverValue {
    DriverValue::Record(BTreeMap::from([
        (
            "events".to_owned(),
            DriverValue::List(value.events.into_iter().map(raise_delivery).collect()),
        ),
        (
            "status".to_owned(),
            raise_consumer_public_status(value.status),
        ),
        (
            "disposition".to_owned(),
            raise_event_disposition(value.disposition),
        ),
        (
            "wait_timed_out".to_owned(),
            DriverValue::Bool(value.wait_timed_out),
        ),
    ]))
}
fn raise_contextual_batch(value: ApplicationContextualBatch) -> DriverValue {
    let items = value
        .items
        .into_iter()
        .map(|item| {
            let hydrations = item
                .hydrations
                .into_iter()
                .map(|hydration| {
                    let mut fields = BTreeMap::from([
                        ("name".to_owned(), DriverValue::String(hydration.name)),
                        ("outcome".to_owned(), DriverValue::Enum(hydration.outcome)),
                    ]);
                    for (name, field) in hydration.fields {
                        let value = match field.cardinality {
                            ApplicationCardinality::One | ApplicationCardinality::Maybe => field
                                .records
                                .into_iter()
                                .next()
                                .map(raise_record)
                                .unwrap_or(DriverValue::Null),
                            ApplicationCardinality::Many => DriverValue::List(
                                field.records.into_iter().map(raise_record).collect(),
                            ),
                        };
                        fields.insert(name, value);
                    }
                    DriverValue::Record(fields)
                })
                .collect();
            let reactions = item
                .available_reactions
                .into_iter()
                .map(|reaction| {
                    let causation_token = BASE64.encode(reaction.causation_token());
                    DriverValue::Record(BTreeMap::from([
                        ("name".to_owned(), DriverValue::String(reaction.name)),
                        (
                            "command_name".to_owned(),
                            DriverValue::String(reaction.command_name),
                        ),
                        (
                            "command_id".to_owned(),
                            DriverValue::U64(u64::from(reaction.command_id).to_string()),
                        ),
                        (
                            "causation_token".to_owned(),
                            DriverValue::String(causation_token),
                        ),
                    ]))
                })
                .collect();
            DriverValue::Record(BTreeMap::from([
                ("delivery".to_owned(), raise_delivery(item.delivery)),
                (
                    "context_head".to_owned(),
                    DriverValue::U64(item.context_head.to_string()),
                ),
                ("hydrations".to_owned(), DriverValue::List(hydrations)),
                (
                    "available_reactions".to_owned(),
                    DriverValue::List(reactions),
                ),
            ]))
        })
        .collect();
    DriverValue::Record(BTreeMap::from([
        ("items".to_owned(), DriverValue::List(items)),
        (
            "status".to_owned(),
            raise_consumer_public_status(value.status),
        ),
        (
            "disposition".to_owned(),
            raise_event_disposition(value.disposition),
        ),
        (
            "wait_timed_out".to_owned(),
            DriverValue::Bool(value.wait_timed_out),
        ),
    ]))
}
fn raise_query_result(result: riffdb_client_rust::NamedQueryResult) -> DriverValue {
    let mut fields = BTreeMap::from([("outcome".to_owned(), DriverValue::Enum(result.outcome))]);
    for (name, field) in result.fields {
        let value = match field.cardinality {
            ApplicationCardinality::One | ApplicationCardinality::Maybe => field
                .records
                .into_iter()
                .next()
                .map(raise_record)
                .unwrap_or(DriverValue::Null),
            ApplicationCardinality::Many => {
                DriverValue::List(field.records.into_iter().map(raise_record).collect())
            }
        };
        fields.insert(name, value);
    }
    DriverValue::Record(fields)
}
fn checkpoint_record(history: u64, head: u64, cursor: &LiveQueryCursor) -> DriverValue {
    DriverValue::Record(BTreeMap::from([
        (
            "history_incarnation".to_owned(),
            DriverValue::U64(history.to_string()),
        ),
        (
            "application_head".to_owned(),
            DriverValue::U64(head.to_string()),
        ),
        (
            "cursor".to_owned(),
            DriverValue::String(cursor.to_standard_base64()),
        ),
    ]))
}
fn raise_live_update(
    value: ApplicationLiveQueryUpdate,
) -> (DriverValue, Option<u64>, Option<String>) {
    match value {
        ApplicationLiveQueryUpdate::Snapshot {
            result,
            cursor,
            history_incarnation,
            application_head,
        } => (
            DriverValue::Record(BTreeMap::from([
                ("kind".to_owned(), DriverValue::Enum("snapshot".to_owned())),
                ("result".to_owned(), raise_query_result(result)),
                (
                    "checkpoint".to_owned(),
                    checkpoint_record(history_incarnation, application_head, &cursor),
                ),
            ])),
            Some(application_head),
            Some(cursor.to_standard_base64()),
        ),
        ApplicationLiveQueryUpdate::Reset {
            reason,
            result,
            cursor,
            history_incarnation,
            application_head,
        } => (
            DriverValue::Record(BTreeMap::from([
                ("kind".to_owned(), DriverValue::Enum("reset".to_owned())),
                ("reason".to_owned(), DriverValue::Enum(reason)),
                ("result".to_owned(), raise_query_result(result)),
                (
                    "checkpoint".to_owned(),
                    checkpoint_record(history_incarnation, application_head, &cursor),
                ),
            ])),
            Some(application_head),
            Some(cursor.to_standard_base64()),
        ),
        ApplicationLiveQueryUpdate::Checkpoint(checkpoint) => (
            DriverValue::Record(BTreeMap::from([
                (
                    "kind".to_owned(),
                    DriverValue::Enum("checkpoint".to_owned()),
                ),
                (
                    "checkpoint".to_owned(),
                    checkpoint_record(
                        checkpoint.history_incarnation,
                        checkpoint.application_head,
                        &checkpoint.cursor,
                    ),
                ),
            ])),
            Some(checkpoint.application_head),
            Some(checkpoint.cursor.to_standard_base64()),
        ),
        ApplicationLiveQueryUpdate::Terminal(terminal) => (
            DriverValue::Record(BTreeMap::from([
                ("kind".to_owned(), DriverValue::Enum("terminal".to_owned())),
                ("reason".to_owned(), DriverValue::Enum(terminal.reason)),
                (
                    "last_frontier".to_owned(),
                    terminal
                        .last_frontier
                        .map(|(history, head)| {
                            DriverValue::Record(BTreeMap::from([
                                (
                                    "history_incarnation".to_owned(),
                                    DriverValue::U64(history.to_string()),
                                ),
                                (
                                    "application_head".to_owned(),
                                    DriverValue::U64(head.to_string()),
                                ),
                            ]))
                        })
                        .unwrap_or(DriverValue::Null),
                ),
            ])),
            terminal.last_frontier.map(|(_, head)| head),
            None,
        ),
        ApplicationLiveQueryUpdate::Patch(patch) => {
            let head = patch.checkpoint.application_head;
            let cursor = patch.checkpoint.cursor.to_standard_base64();
            let operations = patch
                .operations
                .into_iter()
                .map(|operation| match operation {
                    LiveQueryPatchOperation::Insert { index, record } => {
                        DriverValue::Record(BTreeMap::from([
                            (
                                "operation".to_owned(),
                                DriverValue::Enum("insert".to_owned()),
                            ),
                            (
                                "index".to_owned(),
                                DriverValue::U64(u64::from(index).to_string()),
                            ),
                            ("record".to_owned(), raise_record(record)),
                        ]))
                    }
                    LiveQueryPatchOperation::Remove { index, key } => {
                        DriverValue::Record(BTreeMap::from([
                            (
                                "operation".to_owned(),
                                DriverValue::Enum("remove".to_owned()),
                            ),
                            (
                                "index".to_owned(),
                                DriverValue::U64(u64::from(index).to_string()),
                            ),
                            (
                                "key".to_owned(),
                                DriverValue::Record(
                                    key.into_iter()
                                        .map(|(name, value)| (name, raise_value(value)))
                                        .collect(),
                                ),
                            ),
                        ]))
                    }
                    LiveQueryPatchOperation::Replace { index, record } => {
                        DriverValue::Record(BTreeMap::from([
                            (
                                "operation".to_owned(),
                                DriverValue::Enum("replace".to_owned()),
                            ),
                            (
                                "index".to_owned(),
                                DriverValue::U64(u64::from(index).to_string()),
                            ),
                            ("record".to_owned(), raise_record(record)),
                        ]))
                    }
                    LiveQueryPatchOperation::Move { from, to, key } => {
                        DriverValue::Record(BTreeMap::from([
                            ("operation".to_owned(), DriverValue::Enum("move".to_owned())),
                            (
                                "from".to_owned(),
                                DriverValue::U64(u64::from(from).to_string()),
                            ),
                            ("to".to_owned(), DriverValue::U64(u64::from(to).to_string())),
                            (
                                "key".to_owned(),
                                DriverValue::Record(
                                    key.into_iter()
                                        .map(|(name, value)| (name, raise_value(value)))
                                        .collect(),
                                ),
                            ),
                        ]))
                    }
                })
                .collect();
            (
                DriverValue::Record(BTreeMap::from([
                    ("kind".to_owned(), DriverValue::Enum("patch".to_owned())),
                    (
                        "result_field".to_owned(),
                        DriverValue::String(patch.result_field),
                    ),
                    ("operations".to_owned(), DriverValue::List(operations)),
                    (
                        "checkpoint".to_owned(),
                        checkpoint_record(
                            patch.checkpoint.history_incarnation,
                            head,
                            &patch.checkpoint.cursor,
                        ),
                    ),
                ])),
                Some(head),
                Some(cursor),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    use sha2::{Digest as _, Sha256};
    use tonic::transport::Endpoint;

    #[test]
    fn value_registry_hash_receipts_the_exact_vector_successor() {
        let mut actual = String::with_capacity(64);
        for byte in Sha256::digest(DRIVER_VALUE_REGISTRY_DESCRIPTION) {
            write!(actual, "{byte:02x}").expect("string write");
        }
        assert_eq!(actual, DRIVER_VALUE_REGISTRY_HASH);
    }

    #[test]
    fn reactive_poll_reserves_bounded_processing_time_after_caller_wait() {
        assert_eq!(
            invocation_deadline(
                OperationKind::Reactive,
                Some("next"),
                Duration::from_millis(1),
            ),
            Duration::from_millis(1) + REACTIVE_PROCESSING_ALLOWANCE,
        );
        assert_eq!(
            invocation_deadline(
                OperationKind::Reactive,
                Some("ack"),
                Duration::from_millis(1),
            ),
            Duration::from_millis(1),
        );
        assert_eq!(
            invocation_deadline(OperationKind::Command, None, Duration::from_secs(2)),
            Duration::from_secs(2),
        );
    }

    #[test]
    fn contextual_lease_accepts_the_generated_unsigned_history_value() {
        let mut input = BTreeMap::from([
            (
                "event_id".to_owned(),
                ApplicationValue::String("1:0".to_owned()),
            ),
            (
                "lease_token".to_owned(),
                ApplicationValue::String(BASE64.encode([7_u8; 32])),
            ),
            ("history_incarnation".to_owned(), ApplicationValue::U64(1)),
        ]);
        let _evidence = take_lease(&mut input).expect("generated lease evidence");
        assert!(input.is_empty());
    }

    fn host() -> DriverHost {
        let catalog = ApplicationCatalog::from_exact_artifacts(
            include_bytes!("../../../examples/agent-alpha/riffdb.application.lock.json"),
            include_bytes!("../../../examples/agent-alpha/generated/riffdb.application.exact.json"),
            include_bytes!("../../../examples/agent-alpha/generated/mcp/tools.json"),
            "default",
            "AgentAlphaApplication",
        )
        .expect("catalog");
        let channel = Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
        let pool =
            DriverPool::from_clients(vec![StableApplicationClient::from_channel(channel)], 1)
                .expect("pool");
        DriverHost::new(catalog, pool, CallMetadata::default(), "00".repeat(32)).expect("host")
    }

    #[tokio::test]
    async fn cancellation_never_claims_a_pending_command_failed() {
        let host = host();
        let (command_cancel, _) = watch::channel(false);
        let (query_cancel, _) = watch::channel(false);
        {
            let mut inflight = host.inner.inflight.lock().await;
            inflight.insert(
                "command-1".to_owned(),
                InflightRequest {
                    cancel: command_cancel,
                    command: true,
                },
            );
            inflight.insert(
                "query-1".to_owned(),
                InflightRequest {
                    cancel: query_cancel,
                    command: false,
                },
            );
        }
        assert_eq!(
            host.cancel("cancel-1".to_owned(), "command-1".to_owned())
                .await,
            DriverResponse::Cancelled {
                request_id: "cancel-1".to_owned(),
                target_request_id: "command-1".to_owned(),
                terminal: false,
                outcome_uncertain: true,
            }
        );
        assert_eq!(
            host.cancel("cancel-2".to_owned(), "query-1".to_owned())
                .await,
            DriverResponse::Cancelled {
                request_id: "cancel-2".to_owned(),
                target_request_id: "query-1".to_owned(),
                terminal: false,
                outcome_uncertain: false,
            }
        );
    }
}
