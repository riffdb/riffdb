//! Closed, payload-free service telemetry and health hooks.

/// Closed stage at which command capacity admission rejected a request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapacityRejectionStage {
    /// Coordinator queue depth is full.
    QueueDepth,
    /// Independent retained-byte budget is exhausted.
    RetainedBytes,
}

impl CapacityRejectionStage {
    /// Every rejection stage in stable metric order.
    pub const ALL: [Self; 2] = [Self::QueueDepth, Self::RetainedBytes];
}

/// Closed stages of the end-to-end symbolic read pipeline.
///
/// Stage identities are redaction-safe metric labels only. They never carry
/// application values, plan hashes, or request parameters.
///
/// Transport residual stages (`TransportAdapt`, `Authn`, `AdmissionContext`,
/// `SpawnDispatch`, `EncodeConvert`) decompose the client-visible gap outside
/// the original seven service-side stages. Codec internals stay uninstrumented.
///
/// Two members are deliberately *envelopes* rather than members of the
/// partition, and are the only members that overlap another member:
///
/// - [`Self::ServerHandler`] spans the complete unary handler, so it contains
///   every other stage recorded for that request. `ServerHandler` minus the sum
///   of the partition members is the server-side time no stage claims.
/// - [`Self::ServiceAwait`] spans the awaited application-service call inside
///   that handler, so it contains `SpawnDispatch` through [`Self::AuditFinish`].
///   `ServiceAwait` minus those members isolates spawn queueing and the
///   completion handoff from transport-side cost.
///
/// [`Self::partitions_request`] separates the two groups, so a caller that sums
/// stages never double counts. Every other member remains non-overlapping.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReadPipelineStage {
    /// gRPC request split and protobuf → domain conversion.
    TransportAdapt,
    /// Credential authentication for a normal request.
    Authn,
    /// Lifecycle admission and request-context assembly excluding authentication.
    AdmissionContext,
    /// Service spawn submission until the job body first runs.
    SpawnDispatch,
    /// Named-query contract selection and module/query plan lookup.
    PlanLookup,
    /// Parameter materialization and cursor-lookup identity construction.
    ParamMaterialize,
    /// Audit begin for the symbolic query invocation.
    AuthorizeBegin,
    /// Pre-execution current-policy reauthorization.
    AuthorizePre,
    /// Authorized page execution (fence + snapshot + execute).
    Execute,
    /// Post-execution current-policy reauthorization.
    AuthorizePost,
    /// Snapshot-to-response projection assembly.
    ResponseBuild,
    /// Domain result → protobuf response conversion.
    EncodeConvert,
    /// Audit finish and cursor publication after the response is assembled.
    AuditFinish,
    /// Awaited application-service call inside the unary handler (envelope).
    ServiceAwait,
    /// Complete unary read handler, entry to response return (envelope).
    ServerHandler,
}

impl ReadPipelineStage {
    /// Every read-pipeline stage in stable metric and shutdown-line order.
    ///
    /// New stages append. Existing positions never move, so an older evidence
    /// reader keeps reading the same prefix it always did.
    pub const ALL: [Self; 15] = [
        Self::TransportAdapt,
        Self::Authn,
        Self::AdmissionContext,
        Self::SpawnDispatch,
        Self::PlanLookup,
        Self::ParamMaterialize,
        Self::AuthorizeBegin,
        Self::AuthorizePre,
        Self::Execute,
        Self::AuthorizePost,
        Self::ResponseBuild,
        Self::EncodeConvert,
        Self::AuditFinish,
        Self::ServiceAwait,
        Self::ServerHandler,
    ];

    /// Stable snake_case label value for the `{stage}` metric dimension.
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::TransportAdapt => "transport_adapt",
            Self::Authn => "authn",
            Self::AdmissionContext => "admission_context",
            Self::SpawnDispatch => "spawn_dispatch",
            Self::PlanLookup => "plan_lookup",
            Self::ParamMaterialize => "param_materialize",
            Self::AuthorizeBegin => "authorize_begin",
            Self::AuthorizePre => "authorize_pre",
            Self::Execute => "execute",
            Self::AuthorizePost => "authorize_post",
            Self::ResponseBuild => "response_build",
            Self::EncodeConvert => "encode_convert",
            Self::AuditFinish => "audit_finish",
            Self::ServiceAwait => "service_await",
            Self::ServerHandler => "server_handler",
        }
    }

    /// Reports whether this stage is a member of the non-overlapping partition.
    ///
    /// `false` identifies the two envelope stages ([`Self::ServiceAwait`] and
    /// [`Self::ServerHandler`]), which contain other stages by construction. A
    /// caller summing stage time must sum only partition members.
    #[must_use]
    pub const fn partitions_request(self) -> bool {
        !matches!(self, Self::ServiceAwait | Self::ServerHandler)
    }
}

/// Closed non-overlapping stages around one successful mutating command.
///
/// These stages account for API-neutral preparation and completion plus the
/// public gRPC adapter. Coordinator queue, execution, journal, fence, and
/// publication remain owned by commit telemetry and are not duplicated here.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WriteServiceStage {
    /// gRPC request split, bounded conversion, authentication, and context assembly.
    TransportAdapt,
    /// API-neutral command selection, inspection, normalization, audit, authorization,
    /// preparation construction, and bounded coordinator submission.
    ServicePrepare,
    /// Awaiting the submitted coordinator receipt through authoritative completion.
    CoordinatorAwait,
    /// Outcome verification and API-neutral response assembly after completion.
    ServiceFinish,
    /// Domain result to bounded protobuf response conversion.
    EncodeConvert,
}

impl WriteServiceStage {
    /// Every stage in stable shutdown-evidence order.
    pub const ALL: [Self; 5] = [
        Self::TransportAdapt,
        Self::ServicePrepare,
        Self::CoordinatorAwait,
        Self::ServiceFinish,
        Self::EncodeConvert,
    ];

    /// Stable snake-case evidence label.
    #[must_use]
    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::TransportAdapt => "transport_adapt",
            Self::ServicePrepare => "service_prepare",
            Self::CoordinatorAwait => "coordinator_await",
            Self::ServiceFinish => "service_finish",
            Self::EncodeConvert => "encode_convert",
        }
    }
}

/// Redaction-safe service orchestration telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceTelemetryEvent {
    /// One API-neutral operation reached its final caller-visible disposition.
    OperationTerminal {
        /// Closed service operation.
        operation: riffdb_types::ServiceOperationV1,
        /// Trusted transport classification fixed by the request context.
        ingress: riffdb_types::ServiceIngressKindV1,
        /// Closed caller-visible terminal class.
        terminal: ServiceTerminalClass,
        /// Process-local elapsed time for the complete contained operation.
        elapsed: std::time::Duration,
    },
    /// A required service-audit operation was unavailable or uncertain.
    AuditUnavailable {
        /// Closed operation whose audit lifecycle failed.
        operation: riffdb_types::ServiceOperationV1,
    },
    /// A lower integrity or proof join failed without caller-controlled detail.
    InternalIntegrity {
        /// Closed operation being processed.
        operation: riffdb_types::ServiceOperationV1,
    },
    /// A cursor source, registry, or monotonic-clock operation failed closed.
    CursorUnavailable,
    /// A live cursor was evicted to make room for a newer registration.
    CursorEvicted,
    /// One internal read attempt failed with a closed transient and will retry.
    ReadRetryAttempt {
        /// Stable operation name for telemetry only.
        operation: riffdb_types::ServiceOperationV1,
        /// 1-based attempt index within the closed retry budget.
        attempt: u32,
    },
    /// The closed internal read-retry budget was exhausted.
    ReadRetryExhausted {
        /// Stable operation name for telemetry only.
        operation: riffdb_types::ServiceOperationV1,
    },
    /// A post-establishment stream was closed at a current-policy safe point.
    StreamClosedByPolicy,
    /// Command capacity admission rejected the request before accept.
    CapacityRejected {
        /// Closed service operation.
        operation: riffdb_types::ServiceOperationV1,
        /// Trusted transport classification fixed by the request context.
        ingress: riffdb_types::ServiceIngressKindV1,
        /// Closed capacity stage that rejected the request.
        stage: CapacityRejectionStage,
    },
    /// One bounded service-side read pipeline stage completed.
    ReadPipelineStageCompleted {
        /// Closed stage identity; no application values are retained.
        stage: ReadPipelineStage,
        /// Wall duration of this service stage.
        elapsed: std::time::Duration,
    },
    /// One bounded non-overlapping mutating-command service stage completed.
    WriteServiceStageCompleted {
        /// Closed stage identity; no command input or application identity is retained.
        stage: WriteServiceStage,
        /// Wall duration of this service stage.
        elapsed: std::time::Duration,
    },
}

/// Closed terminal classes for API-neutral service telemetry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ServiceTerminalClass {
    /// A complete result was released.
    Succeeded,
    /// Bounded request validation failed.
    Validation,
    /// An idempotency identity was reused with different input.
    IdempotencyMismatch,
    /// Current authorization denied the operation.
    AuthorizationDenied,
    /// Conflict acquisition or the retry budget reached its deadline.
    ConcurrencyDeadlineExceeded,
    /// The requested contract or plan was not current.
    ContractMismatch,
    /// A required authoritative dependency was unavailable.
    StorageUnavailable,
    /// Authoritative completion could not yet be determined.
    OutcomeUnknown,
    /// A checked internal invariant failed.
    InternalDefect,
    /// Deterministic command evaluation reached a declared failure.
    CommandExecutionFailed,
    /// Cancellation was proven at a safe point.
    Cancelled,
    /// The request deadline elapsed at a safe point.
    DeadlineExceeded,
    /// The complete response exceeded the service ceiling.
    ResponseTooLarge,
    /// Internal containment could not obtain an incident identity.
    EmergencyInternal,
    /// Observed history predates a database restore.
    HistoryIncarnationMismatch,
    /// Requested history was retired by retention prune.
    HistoryPruned,
    /// Admission rejected the request because the service is over capacity.
    Overloaded,
}

impl ServiceTerminalClass {
    /// Every terminal class in stable metric order.
    pub const ALL: [Self; 17] = [
        Self::Succeeded,
        Self::Validation,
        Self::IdempotencyMismatch,
        Self::AuthorizationDenied,
        Self::ConcurrencyDeadlineExceeded,
        Self::ContractMismatch,
        Self::StorageUnavailable,
        Self::OutcomeUnknown,
        Self::InternalDefect,
        Self::CommandExecutionFailed,
        Self::Cancelled,
        Self::DeadlineExceeded,
        Self::ResponseTooLarge,
        Self::EmergencyInternal,
        Self::HistoryIncarnationMismatch,
        Self::HistoryPruned,
        Self::Overloaded,
    ];
}

/// Trusted sink that receives only closed, payload-free service events.
pub trait ServiceTelemetry: Send + Sync {
    /// Records one bounded event without request values, credentials, or diagnostics.
    fn record(&self, event: ServiceTelemetryEvent);
}

/// Trusted diagnostic sink for owned internal sources correlated by incident ID.
///
/// This boundary is disjoint from public telemetry and transport output. Its
/// implementation must apply redaction before any external subscriber sees an
/// error source.
pub trait ServiceDiagnostics: Send + Sync {
    /// Retains one internal source under its already assigned incident identity.
    fn record_internal(&self, error: riffdb_errors::InternalError);
}

/// No-op telemetry for compositions that do not install an observer.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopServiceTelemetry;

impl ServiceTelemetry for NoopServiceTelemetry {
    fn record(&self, _event: ServiceTelemetryEvent) {}
}

/// Closed reasons the service must fail authoritative readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoritativeReadinessFailure {
    /// A mandatory audit clock or append could not be established safely.
    AuditUnavailable,
    /// Unknown authoritative write status fenced the coordinator.
    CoordinatorFenced,
    /// A checked semantic join or authoritative observation failed integrity.
    Integrity,
}

/// Lifecycle hook for fail-closed authoritative readiness transitions.
pub trait ServiceHealthHooks: Send + Sync {
    /// Records a monotonic readiness failure for the current process lifecycle.
    fn fail_authoritative_readiness(&self, reason: AuthoritativeReadinessFailure);
}
