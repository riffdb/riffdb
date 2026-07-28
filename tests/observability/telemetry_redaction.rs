//! Cross-crate redaction, cardinality, health, and incident-source evidence.

use std::collections::{HashSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use riffdb_api_mcp::{
    McpListChangeKind, McpRiskClass, McpSchemaFailurePhase, McpTelemetry, McpTelemetryEvent,
    McpTransportKind,
};
use riffdb_auth::{AuthenticationRejection, AuthenticationTelemetry, AuthenticationTelemetryEvent};
use riffdb_commit::{
    CommitCallTerminal, CommitCommandTerminal, CommitIdempotencyObservation, CommitTelemetry,
    CommitTelemetryEvent, CommitUncertaintyResolution, CommitUncertaintyStage,
};
use riffdb_errors::{IncidentIdSource, IncidentIdSourceError, InternalError};
use riffdb_observability::{
    AgentSessionTelemetryHash, AuthoritativeComponent, AuthoritativeCondition,
    CommandWorkCountError, CommandWorkCounts, DerivedComponent, DerivedCondition, DerivedFinding,
    HISTOGRAM_UPPER_BOUNDS, HealthClassification, HealthRegistry, IncidentClass,
    IncidentReportError, MAX_COMMAND_METRIC_SERIES, MAX_DERIVED_FINDINGS_PER_COMPONENT,
    MAX_METRIC_SERIES, MAX_TRACE_RECORDS, MetricKey, MetricSemantics, Observability,
    PrincipalIdTelemetryHash, REQUIRED_COMMAND_SPAN_FIELDS, REQUIRED_METRIC_INVENTORY,
    RequiredCounter, RequiredGauge, RequiredHistogram, SafeTraceLayer, TraceKind, request_span,
};
use riffdb_policy::{AuthorizationTelemetry, AuthorizationTelemetryEvent, PolicyCode};
use riffdb_service::{
    AuthoritativeReadinessFailure, ServiceDiagnostics, ServiceHealthHooks, ServiceTelemetry,
    ServiceTelemetryEvent, ServiceTerminalClass,
};
use riffdb_types::{
    CommandId, CommitSequence, ContractVersion, IncidentId, MAX_COMMAND_CONFLICT_KEYS_V1,
    OutcomeId, PartitionKeyHash, PlanHash, RequestId, ServiceIngressKindV1, ServiceOperationV1,
};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};

const SECRET_CANARY: &str = "riffdb-secret-token-canary-never-export";

struct ScriptedIncidentIds {
    values: Mutex<VecDeque<Result<IncidentId, IncidentIdSourceError>>>,
}

impl ScriptedIncidentIds {
    fn new(values: impl IntoIterator<Item = Result<IncidentId, IncidentIdSourceError>>) -> Self {
        Self {
            values: Mutex::new(values.into_iter().collect()),
        }
    }
}

impl IncidentIdSource for ScriptedIncidentIds {
    fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError> {
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap_or(Err(IncidentIdSourceError))
    }
}

#[derive(Debug)]
struct SecretSource;

impl fmt::Display for SecretSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(SECRET_CANARY)
    }
}

impl Error for SecretSource {}

fn incident(unix_milliseconds: u64, discriminator: u8) -> IncidentId {
    let mut random = [0_u8; 10];
    random[9] = discriminator;
    IncidentId::from_unix_milliseconds_and_random(unix_milliseconds, random)
        .expect("fixture timestamp is in the UUIDv7 range")
}

fn request(unix_milliseconds: u64, discriminator: u8) -> RequestId {
    let mut random = [0_u8; 10];
    random[9] = discriminator;
    RequestId::from_unix_milliseconds_and_random(unix_milliseconds, random)
        .expect("fixture timestamp is in the UUIDv7 range")
}

fn mark_authoritative_ready(health: &HealthRegistry) {
    for component in [
        AuthoritativeComponent::Storage,
        AuthoritativeComponent::Catalog,
        AuthoritativeComponent::CommitCoordinator,
    ] {
        health.set_authoritative(component, AuthoritativeCondition::Healthy);
    }
    for component in [DerivedComponent::Outbox, DerivedComponent::Projection] {
        health
            .set_derived(component, DerivedCondition::Healthy, true, Vec::new())
            .expect("healthy fixture is consistent");
    }
}

#[test]
fn internal_error_sources_never_enter_exported_telemetry() {
    let source = Arc::new(ScriptedIncidentIds::new([]));
    let observability = Observability::new(source, 16).expect("bounded observability");
    let internal_id = incident(3, 3);

    ServiceDiagnostics::record_internal(
        &observability,
        InternalError::new(internal_id, SecretSource),
    );
    ServiceTelemetry::record(
        &observability,
        ServiceTelemetryEvent::AuditUnavailable {
            operation: ServiceOperationV1::ExecuteCommand,
        },
    );
    AuthenticationTelemetry::record(
        &observability,
        AuthenticationTelemetryEvent::Rejected(AuthenticationRejection::MalformedCredential),
    );
    AuthorizationTelemetry::record(
        &observability,
        AuthorizationTelemetryEvent::Denied(PolicyCode::MissingPermission),
    );

    let exported = format!(
        "{:?}{:?}{:?}{:?}",
        observability,
        observability.incident_snapshot(),
        observability.metrics().snapshot(),
        observability.traces().snapshot()
    );
    assert!(!exported.contains(SECRET_CANARY));
    assert_eq!(
        observability
            .metrics()
            .value(MetricKey::Incident(IncidentClass::InternalError)),
        1
    );
    assert_eq!(
        observability.incident_snapshot()[0].incident_id(),
        internal_id
    );
}

#[test]
fn incident_source_is_injected_fail_closed_and_preserves_observation_order() {
    let newer = incident(20, 2);
    let older = incident(10, 1);
    assert!(newer > older, "fixture must expose accidental UUID sorting");
    let source = Arc::new(ScriptedIncidentIds::new([
        Ok(newer),
        Ok(older),
        Err(IncidentIdSourceError),
    ]));
    let observability = Observability::new(source, 16).expect("bounded observability");
    mark_authoritative_ready(observability.health());

    assert_eq!(
        observability.report_incident(IncidentClass::ProviderFailure),
        Ok(newer)
    );
    assert_eq!(
        observability.report_incident(IncidentClass::Integrity),
        Ok(older)
    );
    let records = observability.incident_snapshot();
    assert_eq!(
        records
            .iter()
            .map(|record| record.incident_id())
            .collect::<Vec<_>>(),
        vec![newer, older]
    );

    assert_eq!(
        observability.report_incident(IncidentClass::Audit),
        Err(IncidentReportError::SourceUnavailable)
    );
    assert_eq!(observability.incident_snapshot().len(), 2);
    assert_eq!(
        observability
            .metrics()
            .value(MetricKey::IncidentSourceFailure),
        1
    );
    assert_eq!(
        observability.health().snapshot().classification(),
        HealthClassification::NotReady
    );
    assert_eq!(
        observability
            .traces()
            .snapshot()
            .last()
            .map(|trace| trace.kind()),
        Some(TraceKind::IncidentSourceUnavailable)
    );
}

#[test]
fn metric_cardinality_and_labels_are_closed() {
    let registry = riffdb_observability::MetricRegistry::new();
    let snapshot = registry.snapshot();
    assert_eq!(snapshot.len(), MAX_METRIC_SERIES);

    let unique_series = snapshot
        .iter()
        .map(|sample| {
            (
                sample.name,
                sample.labels[0].map(|label| (label.key, label.value)),
                sample.labels[1].map(|label| (label.key, label.value)),
            )
        })
        .collect::<HashSet<_>>();
    assert_eq!(unique_series.len(), MAX_METRIC_SERIES);
    assert!(
        snapshot
            .iter()
            .flat_map(|sample| sample.labels.iter().flatten())
            .all(|label| !label.value.contains(SECRET_CANARY))
    );
}

#[test]
fn health_separates_authoritative_readiness_from_derived_degradation() {
    let health = HealthRegistry::new();
    let initial = health.snapshot();
    assert_eq!(initial.classification(), HealthClassification::NotReady);
    assert_eq!(initial.last_commit_sequence(), None);
    assert_eq!(
        health.set_derived(
            DerivedComponent::Projection,
            DerivedCondition::Degraded,
            false,
            vec![DerivedFinding::RecoveryPending; MAX_DERIVED_FINDINGS_PER_COMPONENT + 1],
        ),
        Err(riffdb_observability::HealthUpdateError::TooManyFindings)
    );

    mark_authoritative_ready(&health);
    assert_eq!(
        health.snapshot().classification(),
        HealthClassification::Ready
    );
    assert_eq!(health.snapshot().last_commit_sequence(), None);

    health
        .set_derived(
            DerivedComponent::Projection,
            DerivedCondition::Degraded,
            false,
            vec![DerivedFinding::WorkerStopped],
        )
        .expect("bounded derived finding");
    let degraded = health.snapshot();
    assert!(degraded.authoritative_ready());
    assert_eq!(degraded.classification(), HealthClassification::Degraded);
    assert_eq!(degraded.last_commit_sequence(), None);

    let second = CommitSequence::new(2).expect("nonzero sequence");
    let first = CommitSequence::first();
    health.observe_commit(second).expect("forward commit");
    assert_eq!(
        health.observe_commit(first),
        Err(riffdb_observability::HealthUpdateError::CommitSequenceRegressed)
    );
    assert_eq!(
        health.snapshot().classification(),
        HealthClassification::NotReady
    );
}

#[test]
fn trace_layer_rejects_text_fields_and_foreign_targets() {
    let layer = SafeTraceLayer::new(8).expect("bounded layer");
    let collector = layer.collector();
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::event!(
            target: riffdb_observability::SAFE_TRACE_TARGET,
            tracing::Level::INFO,
            secret = SECRET_CANARY
        );
        tracing::event!(
            target: "riffdb_api_mcp::wire",
            tracing::Level::INFO,
            secret = SECRET_CANARY
        );
    });
    assert!(collector.snapshot().is_empty());
}

#[test]
fn request_spans_have_only_closed_correlation_fields() {
    let subscriber = tracing_subscriber::registry();
    tracing::subscriber::with_default(subscriber, || {
        let span = request_span(
            request(40, 4),
            ServiceIngressKindV1::Grpc,
            ServiceOperationV1::ExecuteCommand,
        );
        span.record_actor(
            PrincipalIdTelemetryHash::from_bytes([1; 32]),
            Some(AgentSessionTelemetryHash::from_bytes([2; 32])),
        );
        span.record_command(
            CommandId::first(),
            ContractVersion::new(2).expect("nonzero version"),
            PlanHash::from_bytes([3; 32]),
        );
        span.record_work(
            PartitionKeyHash::from_bytes([4; 32]),
            CommandWorkCounts::new(1, 2, 3, 4).expect("bounded counts"),
        );
        span.record_lock_wait(Duration::from_millis(5));
        span.record_commit_timing(Duration::from_millis(6), Duration::from_millis(7));
        span.record_outcome(CommitSequence::first(), OutcomeId::first(), false);
        span.in_scope(|| {});
    });
    assert_eq!(
        REQUIRED_COMMAND_SPAN_FIELDS,
        [
            "request_id",
            "transport",
            "principal_id_hash",
            "agent_session_id_hash",
            "command_id",
            "contract_version",
            "plan_hash",
            "partition_key_hash",
            "conflict_key_count",
            "lock_wait_ms",
            "read_dependency_count",
            "mutation_count",
            "outbox_event_count",
            "commit_sequence",
            "storage_queue_ms",
            "commit_ms",
            "outcome_type",
            "replayed",
        ]
    );
}

#[test]
fn required_metrics_freeze_semantics_and_current_gauges_do_not_fake_zero() {
    let names = REQUIRED_METRIC_INVENTORY
        .iter()
        .map(|descriptor| descriptor.name)
        .collect::<HashSet<_>>();
    assert_eq!(names.len(), REQUIRED_METRIC_INVENTORY.len());
    assert_eq!(
        REQUIRED_METRIC_INVENTORY
            .iter()
            .filter(|entry| entry.semantics == MetricSemantics::Counter)
            .count(),
        RequiredCounter::ALL.len()
    );
    assert_eq!(
        REQUIRED_METRIC_INVENTORY
            .iter()
            .filter(|entry| entry.semantics == MetricSemantics::Histogram)
            .count(),
        RequiredHistogram::ALL.len()
    );
    assert_eq!(
        REQUIRED_METRIC_INVENTORY
            .iter()
            .filter(|entry| entry.semantics == MetricSemantics::CurrentGauge)
            .count(),
        RequiredGauge::ALL.len()
    );

    let registry = riffdb_observability::MetricRegistry::new();
    registry.increment_required_counter(RequiredCounter::Commits);
    assert_eq!(registry.required_counter(RequiredCounter::Commits), 1);
    registry.observe_required_histogram(RequiredHistogram::CommitBatchSize, 4);
    registry.observe_required_histogram(RequiredHistogram::CommitBatchSize, 9);
    let histogram = registry.required_histogram(RequiredHistogram::CommitBatchSize);
    assert_eq!(histogram.count, 2);
    assert_eq!(histogram.sum, 13);
    assert_eq!(
        histogram.cumulative_buckets[HISTOGRAM_UPPER_BOUNDS.len() - 1],
        2
    );
    let four_bucket = HISTOGRAM_UPPER_BOUNDS
        .iter()
        .position(|bound| *bound == 4)
        .expect("four bucket");
    assert_eq!(histogram.cumulative_buckets[four_bucket], 1);
    assert_eq!(
        registry.required_gauge(RequiredGauge::ActiveContractVersion),
        None
    );
    registry.set_required_gauge(RequiredGauge::ActiveContractVersion, 0);
    assert_eq!(
        registry.required_gauge(RequiredGauge::ActiveContractVersion),
        Some(0)
    );
    registry.clear_required_gauge(RequiredGauge::ActiveContractVersion);
    assert_eq!(
        registry.required_gauge(RequiredGauge::ActiveContractVersion),
        None
    );
}

#[test]
fn owner_adapters_map_closed_events_to_required_metrics_and_traces() {
    let source = Arc::new(ScriptedIncidentIds::new([]));
    let observability = Observability::new(source, 32).expect("bounded observability");

    ServiceTelemetry::record(
        &observability,
        ServiceTelemetryEvent::OperationTerminal {
            operation: ServiceOperationV1::ExecuteCommand,
            ingress: ServiceIngressKindV1::Grpc,
            terminal: ServiceTerminalClass::Succeeded,
            elapsed: Duration::from_micros(7),
        },
    );
    CommitTelemetry::record(
        &observability,
        CommitTelemetryEvent::StorageQueueCompleted {
            command_id: CommandId::first(),
            ingress: ServiceIngressKindV1::Grpc,
            elapsed: Duration::from_micros(11),
        },
    );
    CommitTelemetry::record(
        &observability,
        CommitTelemetryEvent::CommandTerminal {
            command_id: CommandId::first(),
            ingress: ServiceIngressKindV1::Grpc,
            terminal: CommitCommandTerminal::OutcomeReplay,
            elapsed: Duration::from_micros(13),
        },
    );
    CommitTelemetry::record(
        &observability,
        CommitTelemetryEvent::IdempotencyObserved {
            observation: CommitIdempotencyObservation::Hit,
        },
    );
    CommitTelemetry::record(
        &observability,
        CommitTelemetryEvent::CommitCallCompleted {
            terminal: CommitCallTerminal::Committed,
            elapsed: Duration::from_micros(17),
            batch_size: 1,
            synchronous: true,
        },
    );
    CommitTelemetry::record(
        &observability,
        CommitTelemetryEvent::UncertaintyResolved {
            stage: CommitUncertaintyStage::CommandCommit,
            resolution: CommitUncertaintyResolution::Outcome,
        },
    );
    McpTelemetry::record(
        &observability,
        McpTelemetryEvent::SessionOpened {
            transport: McpTransportKind::Stdio,
        },
    );
    McpTelemetry::record(
        &observability,
        McpTelemetryEvent::ToolCall {
            risk: McpRiskClass::DynamicCommand,
        },
    );
    McpTelemetry::record(
        &observability,
        McpTelemetryEvent::SchemaFailure {
            phase: McpSchemaFailurePhase::Input,
        },
    );
    McpTelemetry::record(&observability, McpTelemetryEvent::AuthorizationDenied);
    McpTelemetry::record(
        &observability,
        McpTelemetryEvent::ListChangeNotification {
            kind: McpListChangeKind::Tools,
        },
    );
    McpTelemetry::record(
        &observability,
        McpTelemetryEvent::SessionClosed {
            transport: McpTransportKind::Stdio,
        },
    );

    let metrics = observability.metrics();
    assert_eq!(
        metrics.value(MetricKey::ServiceOperationTerminal(
            ServiceTerminalClass::Succeeded
        )),
        1
    );
    assert_eq!(
        metrics
            .required_histogram(RequiredHistogram::StorageQueueLatencyMicroseconds)
            .sum,
        11
    );
    assert_eq!(
        metrics.required_counter(RequiredCounter::CommandRequests),
        1
    );
    assert_eq!(
        metrics
            .required_histogram(RequiredHistogram::CommandLatencyMicroseconds)
            .sum,
        13
    );
    assert_eq!(
        metrics.required_counter(RequiredCounter::IdempotencyHits),
        1
    );
    assert_eq!(metrics.required_counter(RequiredCounter::Commits), 2);
    assert_eq!(
        metrics
            .required_histogram(RequiredHistogram::CommitDurationMicroseconds)
            .sum,
        17
    );
    assert_eq!(
        metrics
            .required_histogram(RequiredHistogram::CommitBatchSize)
            .sum,
        1
    );
    assert_eq!(
        metrics
            .required_histogram(RequiredHistogram::DurableFlushDurationMicroseconds)
            .sum,
        17
    );
    assert_eq!(
        metrics.required_counter(RequiredCounter::IdempotencyUncertaintyRecoveries),
        1
    );
    assert_eq!(metrics.required_gauge(RequiredGauge::McpSessions), Some(0));
    assert_eq!(metrics.mcp_tool_calls(McpRiskClass::DynamicCommand), 1);
    assert_eq!(metrics.required_counter(RequiredCounter::McpToolCalls), 1);
    assert_eq!(
        metrics.required_counter(RequiredCounter::McpSchemaFailures),
        1
    );
    assert_eq!(
        metrics.required_counter(RequiredCounter::McpAuthorizationDenials),
        1
    );
    assert_eq!(
        metrics.required_counter(RequiredCounter::McpListChangeNotifications),
        1
    );

    let command = metrics.command_snapshot();
    assert_eq!(command.len(), 1);
    assert_eq!(command[0].dimensions.command_id(), CommandId::first());
    assert_eq!(command[0].dimensions.ingress(), ServiceIngressKindV1::Grpc);
    assert_eq!(
        command[0].dimensions.terminal(),
        CommitCommandTerminal::OutcomeReplay
    );
    assert_eq!(command[0].requests, 1);
    assert_eq!(command[0].latency_microseconds.sum, 13);

    let trace_kinds = observability
        .traces()
        .snapshot()
        .into_iter()
        .map(|record| record.kind())
        .collect::<Vec<_>>();
    assert_eq!(
        trace_kinds
            .iter()
            .filter(|kind| **kind == TraceKind::ServiceOperationTerminal)
            .count(),
        1
    );
    assert_eq!(
        trace_kinds
            .iter()
            .filter(|kind| **kind == TraceKind::Commit)
            .count(),
        5
    );
    assert_eq!(
        trace_kinds
            .iter()
            .filter(|kind| **kind == TraceKind::Mcp)
            .count(),
        6
    );
}

#[test]
fn typed_command_metric_dimensions_fail_closed_at_the_hard_cap() {
    let registry = riffdb_observability::MetricRegistry::new();
    for raw in 1..=MAX_COMMAND_METRIC_SERIES {
        let raw = u32::try_from(raw).expect("test cap fits a command ID");
        assert!(registry.observe_command(
            CommandId::new(raw).expect("nonzero command ID"),
            ServiceIngressKindV1::Grpc,
            CommitCommandTerminal::FirstCommit,
            1,
        ));
    }
    assert!(
        !registry.observe_command(
            CommandId::new(
                u32::try_from(MAX_COMMAND_METRIC_SERIES + 1).expect("test cap fits a command ID")
            )
            .expect("nonzero command ID"),
            ServiceIngressKindV1::Grpc,
            CommitCommandTerminal::FirstCommit,
            1,
        )
    );
    assert_eq!(registry.command_snapshot().len(), MAX_COMMAND_METRIC_SERIES);
}

#[test]
fn typed_trace_counts_and_collector_capacity_fail_closed() {
    assert_eq!(
        CommandWorkCounts::new(MAX_COMMAND_CONFLICT_KEYS_V1 + 1, 0, 0, 0),
        Err(CommandWorkCountError::ConflictKeys)
    );
    assert_eq!(
        CommandWorkCounts::new(0, riffdb_storage_api::MAX_READ_DEPENDENCIES + 1, 0, 0),
        Err(CommandWorkCountError::ReadDependencies)
    );
    assert!(SafeTraceLayer::new(0).is_err());
    assert!(SafeTraceLayer::new(MAX_TRACE_RECORDS + 1).is_err());
}

#[derive(Clone, Default)]
struct SiblingCapture {
    records: Arc<Mutex<Vec<String>>>,
}

impl<S> Layer<S> for SiblingCapture
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut text = String::new();
        event.record(&mut TextVisitor(&mut text));
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(text);
    }
}

struct TextVisitor<'a>(&'a mut String);

impl Visit for TextVisitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        let _result = write!(self.0, "{}={value:?};", field.name());
    }
}

#[test]
fn supported_api_redacts_before_both_subscriber_layers() {
    let safe = SafeTraceLayer::new(8).expect("safe layer");
    let safe_records = safe.collector();
    let sibling = SiblingCapture::default();
    let sibling_records = Arc::clone(&sibling.records);
    let subscriber = tracing_subscriber::registry().with(safe).with(sibling);
    let source = Arc::new(ScriptedIncidentIds::new([]));
    let observability = Observability::new(source, 8).expect("observability");

    tracing::subscriber::with_default(subscriber, || {
        ServiceDiagnostics::record_internal(
            &observability,
            InternalError::new(incident(50, 5), SecretSource),
        );
        ServiceTelemetry::record(&observability, ServiceTelemetryEvent::CursorUnavailable);
        ServiceTelemetry::record(
            &observability,
            ServiceTelemetryEvent::OperationTerminal {
                operation: ServiceOperationV1::ExecuteCommand,
                ingress: ServiceIngressKindV1::Grpc,
                terminal: ServiceTerminalClass::InternalDefect,
                elapsed: Duration::from_micros(1),
            },
        );
        CommitTelemetry::record(
            &observability,
            CommitTelemetryEvent::CommandTerminal {
                command_id: CommandId::first(),
                ingress: ServiceIngressKindV1::Grpc,
                terminal: CommitCommandTerminal::Failed(
                    riffdb_commit::CommandExecutionErrorKind::InternalDefect,
                ),
                elapsed: Duration::from_micros(1),
            },
        );
        McpTelemetry::record(
            &observability,
            McpTelemetryEvent::ToolCall {
                risk: McpRiskClass::DynamicCommand,
            },
        );
    });

    assert_eq!(safe_records.snapshot().len(), 5);
    let sibling_text = sibling_records
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .join("");
    assert!(!sibling_text.contains(SECRET_CANARY));
}

#[test]
fn observability_and_diagnostics_have_no_direct_entropy_api() {
    let observability_manifest = include_str!("../../crates/riffdb-observability/Cargo.toml");
    let diagnostics_manifest = include_str!("../../crates/riffdb-diagnostics/Cargo.toml");
    for manifest in [observability_manifest, diagnostics_manifest] {
        assert!(!manifest.contains("\ngetrandom"));
        assert!(!manifest.contains("\nrand "));
        assert!(!manifest.contains("\nrand="));
        assert!(!manifest.contains("\nuuid"));
    }

    for source in [
        include_str!("../../crates/riffdb-observability/src/health.rs"),
        include_str!("../../crates/riffdb-observability/src/metrics.rs"),
        include_str!("../../crates/riffdb-observability/src/telemetry.rs"),
        include_str!("../../crates/riffdb-observability/src/tracing_layer.rs"),
        include_str!("../../crates/riffdb-diagnostics/src/render.rs"),
    ] {
        assert!(!source.contains("SystemTime"));
        assert!(!source.contains("getrandom::"));
        assert!(!source.contains("rand::"));
        assert!(!source.contains("uuid::"));
    }
}

#[test]
fn upstream_readiness_hook_is_monotonic_and_redaction_safe() {
    let source = Arc::new(ScriptedIncidentIds::new([]));
    let observability = Observability::new(source, 8).expect("bounded observability");
    mark_authoritative_ready(observability.health());

    ServiceHealthHooks::fail_authoritative_readiness(
        &observability,
        AuthoritativeReadinessFailure::CoordinatorFenced,
    );
    assert_eq!(
        observability.health().snapshot().classification(),
        HealthClassification::NotReady
    );
    assert_eq!(
        observability
            .metrics()
            .value(MetricKey::AuthoritativeReadinessFailure(
                AuthoritativeReadinessFailure::CoordinatorFenced
            )),
        1
    );
}
