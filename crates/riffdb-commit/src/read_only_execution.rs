//! Synchronous core execution for unjournaled grammar-v1 command reads.

use std::{
    error::Error,
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    time::Instant,
};

use riffdb_catalog::{CommandSnapshotMaterialization, ResolvedExecutablePlan};
use riffdb_conflict::CancellationToken;
use riffdb_invariant::InputDerivedCommandFacts;
use riffdb_runtime::{ExecutionFault, ExecutionResult, TransactionContext, execute_command};
use riffdb_storage_api::{
    DeclaredOutcome, EntityTarget, EvaluationBudget, ExecutablePlanRef, ReadSnapshot,
    SnapshotReader, SnapshotRequest, StorageError, StorageErrorKind,
};
use riffdb_types::{ExecutionFailureCode, LogicalTime};

use crate::{
    clock::{AdmissionClock, AdmissionClockError},
    read_only_preparation::{ReadOnlyExecutionPreparation, ReadOnlyExecutionPreparationParts},
};

/// One exact declared result produced without a command journal or application commit.
#[derive(Clone, Eq, PartialEq)]
pub struct ReadOnlyExecuted {
    plan: ExecutablePlanRef,
    outcome: DeclaredOutcome,
}

impl ReadOnlyExecuted {
    /// Borrows the complete immutable plan identity used for this invocation.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Borrows the declared outcome produced by deterministic execution.
    #[must_use]
    pub const fn outcome(&self) -> &DeclaredOutcome {
        &self.outcome
    }

    /// Recovers the exact plan identity and declared outcome without translation.
    #[must_use]
    pub fn into_parts(self) -> (ExecutablePlanRef, DeclaredOutcome) {
        (self.plan, self.outcome)
    }
}

impl fmt::Debug for ReadOnlyExecuted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReadOnlyExecuted([REDACTED])")
    }
}

/// Closed semantic result of one unjournaled read-only invocation.
#[derive(Clone, Eq, PartialEq)]
pub enum ReadOnlyExecutionResult {
    /// Deterministic execution produced one declared outcome.
    Executed(ReadOnlyExecuted),
    /// Deterministic execution failed without creating any durable command state.
    ExecutionFailed(ExecutionFailureCode),
}

impl fmt::Debug for ReadOnlyExecutionResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Executed(_) => "ReadOnlyExecutionResult::Executed([REDACTED])",
            Self::ExecutionFailed(_) => "ReadOnlyExecutionResult::ExecutionFailed([REDACTED])",
        })
    }
}

/// Internal control-failure classification for actor integration.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ReadOnlyExecutionCoreErrorKind {
    Cancelled,
    DeadlineExceeded,
    StorageUnavailable,
    InternalDefect,
}

impl ReadOnlyExecutionCoreErrorKind {
    const fn safe_message(self) -> &'static str {
        match self {
            Self::Cancelled => "read-only execution was cancelled",
            Self::DeadlineExceeded => "read-only execution deadline elapsed",
            Self::StorageUnavailable => "read-only storage is unavailable",
            Self::InternalDefect => "read-only execution encountered an internal defect",
        }
    }
}

/// Redacted internal failure retained until the coordinator actor maps it.
pub(crate) struct ReadOnlyExecutionCoreError {
    kind: ReadOnlyExecutionCoreErrorKind,
    detail: ReadOnlyExecutionCoreErrorDetail,
}

#[allow(dead_code)] // Retained for trusted telemetry; never exposed by Error::source.
enum ReadOnlyExecutionCoreErrorDetail {
    None,
    Clock(AdmissionClockError),
    Storage(StorageError),
    EvaluationPanicked,
    Integrity,
}

impl ReadOnlyExecutionCoreError {
    #[must_use]
    pub(crate) const fn kind(&self) -> ReadOnlyExecutionCoreErrorKind {
        self.kind
    }

    /// Whether continuing authoritative service would risk accepting corrupt state.
    #[must_use]
    pub(crate) fn requires_readiness_stop(&self) -> bool {
        match &self.detail {
            ReadOnlyExecutionCoreErrorDetail::Storage(error) => {
                error.kind() != StorageErrorKind::Unavailable
            }
            ReadOnlyExecutionCoreErrorDetail::Integrity => true,
            ReadOnlyExecutionCoreErrorDetail::None
            | ReadOnlyExecutionCoreErrorDetail::Clock(_)
            | ReadOnlyExecutionCoreErrorDetail::EvaluationPanicked => false,
        }
    }

    const fn without_detail(kind: ReadOnlyExecutionCoreErrorKind) -> Self {
        Self {
            kind,
            detail: ReadOnlyExecutionCoreErrorDetail::None,
        }
    }

    const fn clock(error: AdmissionClockError) -> Self {
        Self {
            kind: ReadOnlyExecutionCoreErrorKind::InternalDefect,
            detail: ReadOnlyExecutionCoreErrorDetail::Clock(error),
        }
    }

    fn storage(error: StorageError) -> Self {
        let kind = if error.kind() == StorageErrorKind::Unavailable {
            ReadOnlyExecutionCoreErrorKind::StorageUnavailable
        } else {
            ReadOnlyExecutionCoreErrorKind::InternalDefect
        };
        Self {
            kind,
            detail: ReadOnlyExecutionCoreErrorDetail::Storage(error),
        }
    }

    const fn evaluation_panicked() -> Self {
        Self {
            kind: ReadOnlyExecutionCoreErrorKind::InternalDefect,
            detail: ReadOnlyExecutionCoreErrorDetail::EvaluationPanicked,
        }
    }

    const fn integrity() -> Self {
        Self {
            kind: ReadOnlyExecutionCoreErrorKind::InternalDefect,
            detail: ReadOnlyExecutionCoreErrorDetail::Integrity,
        }
    }
}

impl fmt::Debug for ReadOnlyExecutionCoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadOnlyExecutionCoreError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ReadOnlyExecutionCoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for ReadOnlyExecutionCoreError {}

/// Executes one authorized read-only preparation without durable command state.
///
/// The function is synchronous. It samples logical time exactly once after the
/// first request-control check, reads one complete owned snapshot, closes that
/// read view inside the injected storage port, applies catalog-owned lineage
/// materialization, and invokes the deterministic runtime under panic
/// containment. It acquires no conflict capability and opens no write path.
pub(crate) fn drive_read_only_execution<R>(
    snapshots: &R,
    admission_clock: &dyn AdmissionClock,
    preparation: ReadOnlyExecutionPreparation,
) -> Result<ReadOnlyExecutionResult, ReadOnlyExecutionCoreError>
where
    R: SnapshotReader + ?Sized,
{
    let ReadOnlyExecutionPreparationParts {
        resolved_plan,
        normalized_input,
        input_facts,
        authorization,
        request_id,
        deadline,
        cancellation,
    } = preparation.into_parts();

    check_request_control(deadline, &cancellation)?;
    let logical_time = LogicalTime::new(
        admission_clock
            .now()
            .map_err(ReadOnlyExecutionCoreError::clock)?,
    );
    check_request_control(deadline, &cancellation)?;

    let snapshot_request = derive_snapshot_request(&resolved_plan, &input_facts)?;
    check_request_control(deadline, &cancellation)?;
    let raw_snapshot = snapshots
        .read_snapshot(snapshot_request.clone())
        .map_err(ReadOnlyExecutionCoreError::storage)?;
    check_request_control(deadline, &cancellation)?;
    if !snapshot_matches_request(&snapshot_request, &raw_snapshot) {
        return Err(ReadOnlyExecutionCoreError::integrity());
    }

    let context = TransactionContext::new(
        request_id,
        authorization.actor().clone(),
        resolved_plan.reference().clone(),
        logical_time,
        input_facts.partition_key().clone(),
    );
    let materialized = resolved_plan
        .materialize_command_snapshot(raw_snapshot)
        .map_err(|_| ReadOnlyExecutionCoreError::integrity())?;
    check_request_control(deadline, &cancellation)?;
    let snapshot = match materialized {
        CommandSnapshotMaterialization::Ready(snapshot) => snapshot,
        CommandSnapshotMaterialization::ResourceLimit(_) => {
            return Ok(ReadOnlyExecutionResult::ExecutionFailed(
                ExecutionFailureCode::ResourceLimit,
            ));
        }
    };

    let execution = catch_unwind(AssertUnwindSafe(|| {
        execute_command(
            snapshot.resolved_plan().bundle().bundle(),
            &normalized_input,
            snapshot.snapshot(),
            &context,
            EvaluationBudget::v1(),
        )
    }))
    .map_err(|_| ReadOnlyExecutionCoreError::evaluation_panicked())?;
    check_request_control(deadline, &cancellation)?;

    finish_runtime_execution(snapshot.resolved_plan().reference().clone(), execution)
}

fn finish_runtime_execution(
    plan: ExecutablePlanRef,
    execution: Result<ExecutionResult, ExecutionFault>,
) -> Result<ReadOnlyExecutionResult, ReadOnlyExecutionCoreError> {
    match execution {
        Ok(ExecutionResult::ReadOnly(outcome)) => {
            Ok(ReadOnlyExecutionResult::Executed(ReadOnlyExecuted {
                plan,
                outcome,
            }))
        }
        Err(ExecutionFault::Arithmetic) => Ok(ReadOnlyExecutionResult::ExecutionFailed(
            ExecutionFailureCode::ArithmeticFault,
        )),
        Err(ExecutionFault::ResourceLimit) => Ok(ReadOnlyExecutionResult::ExecutionFailed(
            ExecutionFailureCode::ResourceLimit,
        )),
        Ok(ExecutionResult::CommitRequired(_)) | Err(ExecutionFault::Integrity) => {
            Err(ReadOnlyExecutionCoreError::integrity())
        }
    }
}

fn derive_snapshot_request(
    resolved_plan: &ResolvedExecutablePlan,
    input_facts: &InputDerivedCommandFacts,
) -> Result<SnapshotRequest, ReadOnlyExecutionCoreError> {
    let plan = resolved_plan.plan();
    let binding_keys = input_facts.binding_entity_keys();
    let root_keys = input_facts.root_validation_entity_keys();
    if plan.bindings().len() != binding_keys.len()
        || plan.root_validation_reads().len() != root_keys.len()
        || !input_facts.declared_conflict_keys().is_empty()
    {
        return Err(ReadOnlyExecutionCoreError::integrity());
    }

    let binding_targets = plan
        .bindings()
        .iter()
        .zip(binding_keys)
        .map(|(binding, key)| EntityTarget::new(binding.entity_type(), key.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ReadOnlyExecutionCoreError::integrity())?;
    let root_validation_targets = plan
        .root_validation_reads()
        .iter()
        .zip(root_keys)
        .map(|(read, key)| EntityTarget::new(read.entity_type(), key.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ReadOnlyExecutionCoreError::integrity())?;

    SnapshotRequest::new(
        resolved_plan.reference().clone(),
        binding_targets,
        root_validation_targets,
        Vec::new(),
    )
    .map_err(|_| ReadOnlyExecutionCoreError::integrity())
}

fn snapshot_matches_request(request: &SnapshotRequest, snapshot: &ReadSnapshot) -> bool {
    snapshot.plan() == request.plan()
        && snapshot.bindings().len() == request.binding_targets().len()
        && snapshot
            .bindings()
            .iter()
            .zip(request.binding_targets())
            .all(|(observation, target)| observation.target() == target)
        && snapshot.root_validations().len() == request.root_validation_targets().len()
        && snapshot
            .root_validations()
            .iter()
            .zip(request.root_validation_targets())
            .all(|(observation, target)| observation.target() == target)
        && snapshot.ranges().len() == request.range_targets().len()
        && snapshot
            .ranges()
            .iter()
            .zip(request.range_targets())
            .all(|(observation, target)| observation.target() == target)
}

fn check_request_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ReadOnlyExecutionCoreError> {
    if cancellation.is_cancelled() {
        return Err(ReadOnlyExecutionCoreError::without_detail(
            ReadOnlyExecutionCoreErrorKind::Cancelled,
        ));
    }
    if Instant::now() >= deadline {
        return Err(ReadOnlyExecutionCoreError::without_detail(
            ReadOnlyExecutionCoreErrorKind::DeadlineExceeded,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        collections::BTreeMap,
        num::NonZeroU16,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use riffdb_catalog::{ResolvedExecutablePlan, ValidatedContractBundle};
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_contract_ir::{CommandPlan, RecordSchema};
    use riffdb_invariant::derive_input_command_facts;
    use riffdb_policy::{
        AgentSessionAdmissionPolicy, AuthorizationClock, AuthorizationClockError,
        AuthorizedCommandExecution, CommandExecutionClass, CurrentAuthorizer, Decision,
        NoopAuthorizationTelemetry, OperationRequest, UntrustedInvocationClaims,
    };
    use riffdb_storage_api::{
        DurableKeySchemaBindingV1, EntityObservation, ReadSnapshot, SnapshotRequest, StorageError,
        StoredEntityRecordV1,
    };
    use riffdb_testkit::authorization::{
        AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
    };
    use riffdb_types::{
        ActorId, ActorKind, Audience, CanonicalRecord, CanonicalValue, CapabilityGrantV1,
        CapabilityPermissionV1, CapabilityPermissionsV1, DatabaseId, EntityVersion, Environment,
        FieldId, OutcomeId, PartitionScopeV1, PlanHash, RequestId, TenantScope, Timestamp,
    };

    use super::*;
    use crate::command_preparation::{CommandCancellationHandle, CommandRequestControl};

    const PRINCIPAL: &str = "read-only-principal-secret-canary";
    const READ_ONLY_SOURCE: &str = r#"
contract ReadOnlyRows version 1 {
  entity Row {
    key (id: i64)
    field value: i64
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
  command ReadRow {
    input id: i64
    read Row(id) as row else Missing { id: id }
    return Found { value: row.value + 1, observed_at: tx.time }
  }
}
"#;

    struct ReadOnlyFixture {
        database_id: DatabaseId,
        environment: Environment,
        resolved_plan: ResolvedExecutablePlan,
        normalized_input: CanonicalRecord,
        reference: ExecutablePlanRef,
        expected_request: SnapshotRequest,
        raw_snapshot: ReadSnapshot,
        found_outcome: OutcomeId,
        observed_at_field: FieldId,
    }

    impl ReadOnlyFixture {
        fn new(stored_value: i64) -> Self {
            let compiled =
                compile_contract_source(READ_ONLY_SOURCE).expect("read-only source compiles");
            let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
                .expect("catalog accepts read-only bundle");
            let plan = command(bundle.bundle());
            let reference = ExecutablePlanRef::new(
                bundle.lineage().clone(),
                bundle.contract_version(),
                bundle.bundle_hash(),
                plan.command_id(),
                plan.plan_hash(),
            );
            let normalized_input =
                input_record(plan.input().record(), [("id", CanonicalValue::I64(7))]);
            let facts = derive_input_command_facts(plan, normalized_input.clone())
                .expect("read-only input facts");
            assert!(facts.declared_conflict_keys().is_empty());
            let target = EntityTarget::new(
                plan.bindings()[0].entity_type(),
                facts.binding_entity_keys()[0].clone(),
            )
            .expect("exact binding target");
            let entity = bundle
                .bundle()
                .schema()
                .entities()
                .iter()
                .find(|entity| entity.name() == "Row")
                .expect("Row schema");
            let fields = input_record(
                entity.record(),
                [
                    ("id", CanonicalValue::I64(7)),
                    ("value", CanonicalValue::I64(stored_value)),
                ],
            );
            let stored = StoredEntityRecordV1::new(
                target.clone(),
                EntityVersion::first(),
                bundle.contract_version(),
                DurableKeySchemaBindingV1::from_plan(&reference),
                fields,
            )
            .expect("stored read fixture");
            let expected_request =
                SnapshotRequest::new(reference.clone(), vec![target], Vec::new(), Vec::new())
                    .expect("exact snapshot request");
            let raw_snapshot = ReadSnapshot::new(
                &expected_request,
                None,
                vec![EntityObservation::Present(stored)],
                Vec::new(),
                Vec::new(),
            )
            .expect("complete owned snapshot");
            let found = plan
                .outcomes()
                .iter()
                .find(|outcome| outcome.name() == "Found")
                .expect("Found outcome");
            let observed_at_field = found
                .payload()
                .fields()
                .iter()
                .find(|field| field.name() == "observed_at")
                .expect("observed_at field")
                .id();
            let found_outcome = found.id();
            let resolved_plan = crate::test_support::resolve_genesis_plan(&bundle, &reference)
                .expect("exact read-only plan resolves");

            Self {
                database_id: database(1),
                environment: environment(),
                resolved_plan,
                normalized_input,
                reference,
                expected_request,
                raw_snapshot,
                found_outcome,
                observed_at_field,
            }
        }

        fn prepare(
            &self,
            deadline: Instant,
        ) -> (ReadOnlyExecutionPreparation, CommandCancellationHandle) {
            let facts = derive_input_command_facts(
                self.resolved_plan.plan(),
                self.normalized_input.clone(),
            )
            .expect("read-only input facts");
            let (control, cancellation) = CommandRequestControl::new(deadline);
            let preparation = ReadOnlyExecutionPreparation::new(
                self.database_id,
                &self.environment,
                self.resolved_plan.clone(),
                self.normalized_input.clone(),
                facts,
                self.authorization(),
                request_id(2),
                control,
            )
            .expect("exact read-only preparation");
            (preparation, cancellation)
        }

        fn authorization(&self) -> AuthorizedCommandExecution {
            authorized(
                self.database_id,
                self.environment.clone(),
                self.reference.clone(),
                self.resolved_plan
                    .plan()
                    .locality()
                    .partition_schema()
                    .encode_partition(&[CanonicalValue::I64(7)])
                    .expect("fixture partition"),
            )
        }
    }

    struct FixedAuthorizationClock(Timestamp);

    impl AuthorizationClock for FixedAuthorizationClock {
        fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
            Ok(self.0)
        }
    }

    struct ScriptedAdmissionClock {
        result: Result<Timestamp, AdmissionClockError>,
        calls: AtomicUsize,
    }

    impl ScriptedAdmissionClock {
        fn fixed(value: Timestamp) -> Self {
            Self {
                result: Ok(value),
                calls: AtomicUsize::new(0),
            }
        }

        fn failing() -> Self {
            Self {
                result: Err(AdmissionClockError),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl AdmissionClock for ScriptedAdmissionClock {
        fn now(&self) -> Result<Timestamp, AdmissionClockError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.result
        }
    }

    struct ScriptedSnapshots {
        expected: SnapshotRequest,
        result: Result<ReadSnapshot, StorageError>,
        calls: Cell<usize>,
        cancel_on_read: Option<CommandCancellationHandle>,
    }

    impl ScriptedSnapshots {
        fn returning(expected: SnapshotRequest, snapshot: ReadSnapshot) -> Self {
            Self {
                expected,
                result: Ok(snapshot),
                calls: Cell::new(0),
                cancel_on_read: None,
            }
        }

        fn failing(expected: SnapshotRequest, kind: StorageErrorKind) -> Self {
            Self {
                expected,
                result: Err(StorageError::new(kind, None)),
                calls: Cell::new(0),
                cancel_on_read: None,
            }
        }
    }

    impl SnapshotReader for ScriptedSnapshots {
        fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
            self.calls.set(self.calls.get() + 1);
            assert!(request == self.expected, "snapshot request must be exact");
            if let Some(cancellation) = &self.cancel_on_read {
                cancellation.cancel();
            }
            self.result.clone()
        }
    }

    fn command(bundle: &riffdb_contract_ir::ContractBundle) -> &CommandPlan {
        bundle
            .commands()
            .iter()
            .find(|plan| plan.name() == "ReadRow")
            .expect("ReadRow plan")
    }

    fn input_record<const N: usize>(
        schema: &RecordSchema,
        fields: [(&str, CanonicalValue); N],
    ) -> CanonicalRecord {
        let by_name = fields.into_iter().collect::<BTreeMap<_, _>>();
        CanonicalRecord::new(
            schema
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.id(),
                        by_name
                            .get(field.name())
                            .unwrap_or_else(|| panic!("missing field {}", field.name()))
                            .clone(),
                    )
                })
                .collect(),
        )
        .expect("canonical record")
    }

    fn authorized(
        database_id: DatabaseId,
        environment: Environment,
        reference: ExecutablePlanRef,
        partition: riffdb_types::PartitionKey,
    ) -> AuthorizedCommandExecution {
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::InvokeCommand(
                reference.contract_lineage().clone(),
                reference.command_id(),
            )])
            .expect("permissions"),
            Vec::new(),
            NonZeroU16::new(10).expect("row bound"),
            Vec::new(),
        )
        .expect("grant");
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database_id,
            environment.clone(),
            ActorId::new(PRINCIPAL).expect("principal"),
            ActorKind::Agent,
            Audience::new("riffdb-read-only-execution").expect("audience"),
            AuthorizationFixtureTimes::new(timestamp(100), timestamp(300), timestamp(150)),
            grant,
        ))
        .expect("authorization fixture");
        let resolver = fixture.current_capability_resolver();
        let clock = FixedAuthorizationClock(timestamp(200));
        let decision = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &NoopAuthorizationTelemetry,
            database_id,
            environment,
        )
        .authorize(
            fixture.authenticated_principal(),
            OperationRequest::execute_command(
                reference.contract_lineage().clone(),
                reference.contract_version(),
                reference.command_id(),
                CommandExecutionClass::ReadOnly,
                partition,
            ),
        )
        .expect("policy decision");
        let Decision::Allow(proof) = decision else {
            panic!("read-only command must be allowed")
        };
        proof
            .into_command_execution(
                UntrustedInvocationClaims::new(None, None, None, None, None),
                AgentSessionAdmissionPolicy::Discard,
            )
            .expect("read-only authorization")
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 17).expect("timestamp")
    }

    fn database(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
            .expect("database ID")
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
            .expect("request ID")
    }

    fn environment() -> Environment {
        Environment::new("development").expect("environment")
    }

    fn future_deadline() -> Instant {
        Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("future deadline")
    }

    #[test]
    fn exact_preparation_rejects_a_different_trusted_database() {
        let fixture = ReadOnlyFixture::new(9);
        let facts = derive_input_command_facts(
            fixture.resolved_plan.plan(),
            fixture.normalized_input.clone(),
        )
        .expect("facts");
        let (control, _) = CommandRequestControl::new(future_deadline());
        let error = ReadOnlyExecutionPreparation::new(
            database(9),
            &fixture.environment,
            fixture.resolved_plan.clone(),
            fixture.normalized_input.clone(),
            facts,
            fixture.authorization(),
            request_id(2),
            control,
        )
        .expect_err("trusted database mismatch must reject");

        assert_eq!(
            error.to_string(),
            "read-only execution proofs are inconsistent"
        );
        assert!(!format!("{error:?}").contains(PRINCIPAL));
    }

    #[test]
    fn success_samples_once_and_returns_exact_plan_and_declared_outcome() {
        let fixture = ReadOnlyFixture::new(9);
        let (preparation, _) = fixture.prepare(future_deadline());
        let snapshots = ScriptedSnapshots::returning(
            fixture.expected_request.clone(),
            fixture.raw_snapshot.clone(),
        );
        let clock = ScriptedAdmissionClock::fixed(timestamp(-41));

        let result = drive_read_only_execution(&snapshots, &clock, preparation)
            .expect("read-only execution succeeds");
        let ReadOnlyExecutionResult::Executed(executed) = result else {
            panic!("read-only execution must return a declared outcome")
        };

        assert_eq!(clock.calls(), 1);
        assert_eq!(snapshots.calls.get(), 1);
        assert!(executed.plan() == &fixture.reference);
        assert_eq!(executed.outcome().outcome_id(), fixture.found_outcome);
        assert_eq!(
            executed
                .outcome()
                .value()
                .fields()
                .iter()
                .find(|(field, _)| *field == fixture.observed_at_field)
                .map(|(_, value)| value),
            Some(&CanonicalValue::Timestamp(timestamp(-41)))
        );
        assert_eq!(format!("{executed:?}"), "ReadOnlyExecuted([REDACTED])");
        assert!(!format!("{executed:?}").contains(PRINCIPAL));
    }

    #[test]
    fn arithmetic_and_resource_faults_are_typed_unjournaled_results() {
        let fixture = ReadOnlyFixture::new(i64::MAX);
        let (preparation, _) = fixture.prepare(future_deadline());
        let snapshots = ScriptedSnapshots::returning(
            fixture.expected_request.clone(),
            fixture.raw_snapshot.clone(),
        );
        let clock = ScriptedAdmissionClock::fixed(timestamp(8));

        assert_eq!(
            drive_read_only_execution(&snapshots, &clock, preparation)
                .expect("arithmetic is a typed deterministic result"),
            ReadOnlyExecutionResult::ExecutionFailed(ExecutionFailureCode::ArithmeticFault)
        );
        assert_eq!(clock.calls(), 1);
        assert_eq!(snapshots.calls.get(), 1);
        assert_eq!(
            finish_runtime_execution(fixture.reference, Err(ExecutionFault::ResourceLimit))
                .expect("resource limit is a typed deterministic result"),
            ReadOnlyExecutionResult::ExecutionFailed(ExecutionFailureCode::ResourceLimit)
        );
    }

    #[test]
    fn cancellation_and_deadline_stop_at_safe_points_without_extra_work() {
        let fixture = ReadOnlyFixture::new(9);
        let (cancelled, cancellation) = fixture.prepare(future_deadline());
        cancellation.cancel();
        let snapshots = ScriptedSnapshots::returning(
            fixture.expected_request.clone(),
            fixture.raw_snapshot.clone(),
        );
        let clock = ScriptedAdmissionClock::fixed(timestamp(1));
        let error = drive_read_only_execution(&snapshots, &clock, cancelled)
            .expect_err("pre-start cancellation must stop");
        assert_eq!(error.kind(), ReadOnlyExecutionCoreErrorKind::Cancelled);
        assert!(!error.requires_readiness_stop());
        assert_eq!(clock.calls(), 0);
        assert_eq!(snapshots.calls.get(), 0);

        let expired = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("past deadline");
        let (expired, _) = fixture.prepare(expired);
        let error = drive_read_only_execution(&snapshots, &clock, expired)
            .expect_err("past deadline must stop");
        assert_eq!(
            error.kind(),
            ReadOnlyExecutionCoreErrorKind::DeadlineExceeded
        );
        assert_eq!(clock.calls(), 0);
        assert_eq!(snapshots.calls.get(), 0);

        let (cancelled_after_read, cancellation) = fixture.prepare(future_deadline());
        let snapshots = ScriptedSnapshots {
            expected: fixture.expected_request,
            result: Ok(fixture.raw_snapshot),
            calls: Cell::new(0),
            cancel_on_read: Some(cancellation),
        };
        let error = drive_read_only_execution(&snapshots, &clock, cancelled_after_read)
            .expect_err("post-read cancellation must stop before evaluation");
        assert_eq!(error.kind(), ReadOnlyExecutionCoreErrorKind::Cancelled);
        assert_eq!(clock.calls(), 1);
        assert_eq!(snapshots.calls.get(), 1);
    }

    #[test]
    fn clock_and_storage_failures_are_redacted_and_fail_closed() {
        let fixture = ReadOnlyFixture::new(9);
        let (preparation, _) = fixture.prepare(future_deadline());
        let snapshots = ScriptedSnapshots::returning(
            fixture.expected_request.clone(),
            fixture.raw_snapshot.clone(),
        );
        let clock = ScriptedAdmissionClock::failing();
        let error = drive_read_only_execution(&snapshots, &clock, preparation)
            .expect_err("clock failure must stop before reading");
        assert_eq!(error.kind(), ReadOnlyExecutionCoreErrorKind::InternalDefect);
        assert!(!error.requires_readiness_stop());
        assert_eq!(clock.calls(), 1);
        assert_eq!(snapshots.calls.get(), 0);
        assert!(error.source().is_none());
        assert!(!format!("{error:?}").contains(PRINCIPAL));

        for (kind, expected, stops) in [
            (
                StorageErrorKind::Unavailable,
                ReadOnlyExecutionCoreErrorKind::StorageUnavailable,
                false,
            ),
            (
                StorageErrorKind::CommitStatusUnknown,
                ReadOnlyExecutionCoreErrorKind::InternalDefect,
                true,
            ),
            (
                StorageErrorKind::CorruptData,
                ReadOnlyExecutionCoreErrorKind::InternalDefect,
                true,
            ),
        ] {
            let fixture = ReadOnlyFixture::new(9);
            let (preparation, _) = fixture.prepare(future_deadline());
            let snapshots = ScriptedSnapshots::failing(fixture.expected_request, kind);
            let clock = ScriptedAdmissionClock::fixed(timestamp(3));
            let error = drive_read_only_execution(&snapshots, &clock, preparation)
                .expect_err("storage failure must not produce an outcome");
            assert_eq!(error.kind(), expected);
            assert_eq!(error.requires_readiness_stop(), stops);
            assert_eq!(clock.calls(), 1);
            assert_eq!(snapshots.calls.get(), 1);
        }
    }

    #[test]
    fn a_snapshot_with_the_wrong_plan_identity_is_an_integrity_stop() {
        let fixture = ReadOnlyFixture::new(9);
        let (preparation, _) = fixture.prepare(future_deadline());
        let wrong_reference = ExecutablePlanRef::new(
            fixture.reference.contract_lineage().clone(),
            fixture.reference.contract_version(),
            fixture.reference.contract_bundle_hash(),
            fixture.reference.command_id(),
            PlanHash::from_bytes([0x91; 32]),
        );
        let wrong_request = SnapshotRequest::new(
            wrong_reference,
            fixture.expected_request.binding_targets().to_vec(),
            fixture.expected_request.root_validation_targets().to_vec(),
            Vec::new(),
        )
        .expect("structurally valid wrong request");
        let wrong_snapshot = ReadSnapshot::new(
            &wrong_request,
            fixture.raw_snapshot.observed_through(),
            fixture.raw_snapshot.bindings().to_vec(),
            fixture.raw_snapshot.root_validations().to_vec(),
            fixture.raw_snapshot.ranges().to_vec(),
        )
        .expect("structurally valid wrong snapshot");
        let snapshots = ScriptedSnapshots::returning(fixture.expected_request, wrong_snapshot);
        let clock = ScriptedAdmissionClock::fixed(timestamp(4));

        let error = drive_read_only_execution(&snapshots, &clock, preparation)
            .expect_err("wrong snapshot identity must fail closed");
        assert_eq!(error.kind(), ReadOnlyExecutionCoreErrorKind::InternalDefect);
        assert!(error.requires_readiness_stop());
        assert_eq!(clock.calls(), 1);
        assert_eq!(snapshots.calls.get(), 1);
    }
}
