//! Production `form_next_unit` falsifiability suite (T1.3).
//!
//! Drives the real selection kernel on `VecDeque<CoordinatorMessage>`. The
//! independent oracle below is a class-sequence specification of the ADR rules
//! — not a line-by-line reimplementation shared with production code.

use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroU64};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use riffdb_catalog::ValidatedContractBundle;
use riffdb_idempotency::{
    CommandIdempotencyScopeV1, IdempotencyDigestCandidatesV1, IdempotencyDigestError,
    IdempotencyDigestProvider, IdempotencyInspectionExecutor, prepare_idempotency_lookup,
};
use riffdb_invariant::derive_input_command_facts;
use riffdb_policy::{
    AgentSessionAdmissionPolicy, AuthorizationClock, AuthorizationClockError,
    CommandExecutionClass, CurrentAuthorizer, Decision, NoopAuthorizationTelemetry,
    OperationRequest, UntrustedInvocationClaims,
};
use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, AdmissionRequestV1, AdmissionResultV1,
    ExecutablePlanRef, IdempotencyKeyDigest, IdempotencyLookupCandidatesV1, StorageError,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, Audience, CanonicalRecord, CanonicalValue, CapabilityGrantV1,
    CapabilityId, CapabilityPermissionV1, CapabilityPermissionsV1, DatabaseId, Decimal,
    DecimalSpec, DigestKeyId, Environment, IdempotencyKey, PartitionScopeV1, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    ServiceOperationV1, TenantScope, Timestamp,
};
use tokio::sync::{Semaphore, oneshot};

use super::*;
use crate::CommandRequestControl;
use crate::command_execution::CommandExecutionLifecycle;
use crate::command_preparation::CommandExecutionPreparation;
use crate::idempotency_inspection::{
    CommandIdempotencyInspectionRequest, prepare_command_idempotency_inspection,
};

// --- Message factories (real CoordinatorMessage values) ---

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

fn request_id(byte: u8) -> RequestId {
    RequestId::from_bytes(uuid_bytes(byte)).expect("request UUIDv7")
}

fn capability_id(byte: u8) -> CapabilityId {
    CapabilityId::from_bytes(uuid_bytes(byte)).expect("capability UUIDv7")
}

struct CheckedInput {
    request_id: RequestId,
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    principal_id: ActorId,
    actor_kind: ActorKind,
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
    ingress: ServiceIngressKindV1,
    targets: ServiceAuditTargetsV1,
    approval_id: Option<ApprovalId>,
    link: ServiceAuditLinkV1,
}

impl AdministrationAuditInputView for CheckedInput {
    fn request_id(&self) -> &RequestId {
        &self.request_id
    }
    fn operation(&self) -> &ServiceOperationV1 {
        &self.operation
    }
    fn phase(&self) -> &ServiceAuditPhaseV1 {
        &self.phase
    }
    fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }
    fn actor_kind(&self) -> &ActorKind {
        &self.actor_kind
    }
    fn capability_id(&self) -> &CapabilityId {
        &self.capability_id
    }
    fn capability_revision(&self) -> &NonZeroU64 {
        &self.capability_revision
    }
    fn ingress(&self) -> &ServiceIngressKindV1 {
        &self.ingress
    }
    fn targets(&self) -> &ServiceAuditTargetsV1 {
        &self.targets
    }
    fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
    fn link(&self) -> &ServiceAuditLinkV1 {
        &self.link
    }
}

fn audit_msg(request: u8) -> CoordinatorMessage {
    let (completion, _) = oneshot::channel();
    CoordinatorMessage::AdministrationAudit {
        submission: AdministrationAuditSubmission::Single(Box::new(CheckedInput {
            request_id: request_id(request),
            operation: ServiceOperationV1::GetHealth,
            phase: ServiceAuditPhaseV1::Denied,
            principal_id: ActorId::new("operator").expect("actor"),
            actor_kind: ActorKind::Human,
            capability_id: capability_id(2),
            capability_revision: NonZeroU64::MIN,
            ingress: ServiceIngressKindV1::Grpc,
            targets: ServiceAuditTargetsV1::empty(),
            approval_id: None,
            link: ServiceAuditLinkV1::None,
        })),
        completion,
    }
}

struct FixtureParts {
    resolved: riffdb_catalog::ResolvedExecutablePlan,
    reference: ExecutablePlanRef,
    normalized_input: CanonicalRecord,
    partition: riffdb_types::PartitionKey,
}

fn fixture_parts() -> &'static FixtureParts {
    static PARTS: OnceLock<FixtureParts> = OnceLock::new();
    PARTS.get_or_init(|| {
        let bundle = ValidatedContractBundle::decode(include_bytes!(
            "../../../../fixtures/compiler/bundle.bin"
        ))
        .expect("compiler fixture");
        let plan = bundle
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == "CreateBudget")
            .expect("CreateBudget");
        let reference = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let normalized_input = CanonicalRecord::new(
            plan.input()
                .record()
                .fields()
                .iter()
                .map(|field| {
                    let value = match field.name() {
                        "idempotency_key" => {
                            CanonicalValue::string("form-kernel-caller").expect("key")
                        }
                        "organization_id" => CanonicalValue::Uuid([0x31; 16]),
                        "fiscal_year" => CanonicalValue::I64(2026),
                        "approved_amount" => CanonicalValue::Decimal(
                            Decimal::new(DecimalSpec::new(28, 2).expect("spec"), 12_500)
                                .expect("decimal"),
                        ),
                        other => panic!("unexpected field {other}"),
                    };
                    (field.id(), value)
                })
                .collect(),
        )
        .expect("input");
        let facts = derive_input_command_facts(plan, normalized_input.clone()).expect("facts");
        let partition = facts.partition_key().clone();
        let resolved =
            crate::test_support::resolve_genesis_plan(&bundle, &reference).expect("resolve plan");
        FixtureParts {
            resolved,
            reference,
            normalized_input,
            partition,
        }
    })
}

fn authorize_fixture() -> riffdb_policy::AuthorizedCommandExecution {
    let parts = fixture_parts();
    let database = DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("db");
    let environment = Environment::new("development").expect("env");
    let principal = "form-kernel-principal";
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::InvokeCommand(
            parts.reference.contract_lineage().clone(),
            parts.reference.command_id(),
        )])
        .expect("perms"),
        Vec::new(),
        NonZeroU16::new(10).expect("row"),
        Vec::new(),
    )
    .expect("grant");
    let auth_fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database,
        environment.clone(),
        ActorId::new(principal).expect("principal"),
        ActorKind::Agent,
        Audience::new("riffdb-form-kernel").expect("audience"),
        AuthorizationFixtureTimes::new(
            Timestamp::new(100, 0).expect("t"),
            Timestamp::new(300, 0).expect("t"),
            Timestamp::new(150, 0).expect("t"),
        ),
        grant,
    ))
    .expect("auth fixture");
    struct FixedClock(Timestamp);
    impl AuthorizationClock for FixedClock {
        fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
            Ok(self.0)
        }
    }
    let decision = CurrentAuthorizer::new(
        &auth_fixture.current_capability_resolver(),
        &FixedClock(Timestamp::new(200, 0).expect("t")),
        &NoopAuthorizationTelemetry,
        database,
        environment,
    )
    .authorize(
        auth_fixture.authenticated_principal(),
        OperationRequest::execute_command(
            parts.reference.contract_lineage().clone(),
            parts.reference.contract_version(),
            parts.reference.command_id(),
            CommandExecutionClass::Mutation,
            parts.partition.clone(),
        ),
    )
    .expect("authorize");
    let Decision::Allow(proof) = decision else {
        panic!("must allow");
    };
    proof
        .into_command_execution(
            UntrustedInvocationClaims::new(None, None, None, None, None),
            AgentSessionAdmissionPolicy::Discard,
        )
        .expect("command auth")
}

struct FixedDigestProvider;
impl IdempotencyDigestProvider for FixedDigestProvider {
    fn digest_candidates(
        &self,
        _: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        IdempotencyDigestCandidatesV1::new(vec![IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest"),
            [0x51; 32],
        )])
    }
}

struct AbsentAdmission;
impl AdmissionRepository for AbsentAdmission {
    fn admit_or_resolve(&self, _: AdmissionRequestV1) -> Result<AdmissionResultV1, StorageError> {
        panic!("inspection only")
    }
    fn lookup_admission(
        &self,
        _: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        Ok(AdmissionLookupResultV1::NotFound)
    }
}

fn command_preparation_with_digest(
    seed: u8,
    caller_key: &str,
    digest_byte: u8,
) -> CommandExecutionPreparation {
    struct SeededDigestProvider(u8);
    impl IdempotencyDigestProvider for SeededDigestProvider {
        fn digest_candidates(
            &self,
            _: &IdempotencyKey,
        ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
            IdempotencyDigestCandidatesV1::new(vec![IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest"),
                [self.0; 32],
            )])
        }
    }

    let parts = fixture_parts();
    let database = DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("db");
    let environment = Environment::new("development").expect("env");
    let principal = ActorId::new("form-kernel-principal").expect("principal");
    let scope = CommandIdempotencyScopeV1::new(
        database,
        environment.clone(),
        TenantScope::Global,
        principal,
        parts.reference.contract_lineage().clone(),
        parts.reference.command_id(),
    );
    let normalized_input = if caller_key == "form-kernel-caller" {
        parts.normalized_input.clone()
    } else {
        CanonicalRecord::new(
            parts
                .resolved
                .plan()
                .input()
                .record()
                .fields()
                .iter()
                .map(|field| {
                    let value = match field.name() {
                        "idempotency_key" => CanonicalValue::string(caller_key).expect("key"),
                        "organization_id" => CanonicalValue::Uuid([0x31; 16]),
                        "fiscal_year" => CanonicalValue::I64(2026),
                        "approved_amount" => CanonicalValue::Decimal(
                            Decimal::new(DecimalSpec::new(28, 2).expect("spec"), 12_500)
                                .expect("decimal"),
                        ),
                        other => panic!("unexpected field {other}"),
                    };
                    (field.id(), value)
                })
                .collect(),
        )
        .expect("input")
    };
    let caller_key = IdempotencyKey::new(caller_key).expect("key");
    let lookup =
        prepare_idempotency_lookup(&scope, &caller_key, &SeededDigestProvider(digest_byte))
            .expect("lookup prep");
    let inspection = IdempotencyInspectionExecutor::new(&AbsentAdmission)
        .inspect(lookup)
        .expect("inspect");
    let idempotency = inspection
        .confirm_input(
            &normalized_input,
            parts
                .resolved
                .plan()
                .idempotency_input()
                .expect("idempotency field"),
            &caller_key,
        )
        .expect("confirm")
        .bind_selected_plan(parts.reference.clone())
        .expect("bind");
    let facts =
        derive_input_command_facts(parts.resolved.plan(), normalized_input.clone()).expect("facts");
    let (control, _) = CommandRequestControl::new(
        Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("deadline"),
    );
    CommandExecutionPreparation::new(
        database,
        &environment,
        parts.resolved.clone(),
        normalized_input,
        idempotency,
        facts,
        authorize_fixture(),
        request_id(seed),
        ServiceIngressKindV1::Grpc,
        control,
    )
    .expect("command preparation")
}

fn command_preparation(seed: u8) -> CommandExecutionPreparation {
    command_preparation_with_digest(seed, "form-kernel-caller", 0x51)
}

fn audited_command_preparation_with_digest(
    seed: u8,
    caller_key: &str,
    digest_byte: u8,
) -> CommandExecutionPreparation {
    command_preparation_with_digest(seed, caller_key, digest_byte)
        .with_audited_lifecycle(Box::new(CheckedInput {
            request_id: request_id(seed),
            operation: ServiceOperationV1::ExecuteCommand,
            phase: ServiceAuditPhaseV1::Started,
            principal_id: ActorId::new("form-kernel-principal").expect("principal"),
            actor_kind: ActorKind::Agent,
            capability_id: capability_id(2),
            capability_revision: NonZeroU64::MIN,
            ingress: ServiceIngressKindV1::Grpc,
            targets: ServiceAuditTargetsV1::empty(),
            approval_id: None,
            link: ServiceAuditLinkV1::None,
        }))
        .expect("audited command preparation")
}

fn audited_command_preparation(seed: u8) -> CommandExecutionPreparation {
    audited_command_preparation_with_digest(seed, "form-kernel-caller", 0x51)
}

fn retained_permit() -> tokio::sync::OwnedSemaphorePermit {
    static SEM: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let sem = SEM.get_or_init(|| Arc::new(Semaphore::new(1_000_000)));
    sem.clone()
        .try_acquire_owned()
        .expect("retained byte permit")
}

fn command_msg_with_id(seed: u8) -> CoordinatorMessage {
    let (completion, _) = oneshot::channel();
    let preparation = command_preparation(seed);
    CoordinatorMessage::Command {
        preparation: Box::new(preparation),
        command_id: fixture_parts().reference.command_id(),
        ingress: ServiceIngressKindV1::Grpc,
        enqueued_at: Instant::now(),
        completion,
        _retained_byte_permit: retained_permit(),
    }
}

struct NoopLifecycle;
impl CommandExecutionLifecycle for NoopLifecycle {
    fn fence(&self) {}
    fn stop(&self) {}
}

fn observation_msg(seed: u8) -> CoordinatorMessage {
    let (completion, _) = oneshot::channel();
    let database = DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("db");
    let environment = Environment::new("development").expect("env");
    let request = CommandIdempotencyInspectionRequest::new(
        database,
        environment,
        TenantScope::Global,
        ActorId::new("form-kernel-principal").expect("principal"),
        fixture_parts().reference.contract_lineage().clone(),
        fixture_parts().reference.command_id(),
        IdempotencyKey::new(format!("obs-{seed}")).expect("key"),
    );
    let prepared =
        prepare_command_idempotency_inspection(&FixedDigestProvider, &NoopLifecycle, request)
            .expect("prepare observation");
    CoordinatorMessage::IdempotencyInspection {
        preparation: Box::new(prepared),
        completion,
    }
}

fn shutdown_msg() -> CoordinatorMessage {
    CoordinatorMessage::Shutdown
}

// --- Independent oracle (class sequences → expected unit shapes) ---

/// Closed message class used only by the oracle and sequence generators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cls {
    /// Mutation command.
    C,
    /// Administration audit (barrier, co-grouped by request_id).
    A,
    /// Deferrable observation (idempotency inspection).
    O,
    /// Non-audit barrier (shutdown).
    S,
}

/// Expected unit shape independent of production `WorkUnit` layout.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Expected {
    CommandGroup {
        len: usize,
        reason: CommitGroupDispatchReason,
    },
    AuditGroup {
        len: usize,
        reason: CommitGroupDispatchReason,
    },
    Observation,
    Shutdown,
    SingleBarrier,
}

/// Independent specification of formation rules from ADR-0060 / T1.3.
///
/// Written as a direct class walk — deliberately not structured as a
/// translation of `form_next_unit`'s match arms.
fn expected_units(seq: &[Cls]) -> Vec<Expected> {
    let mut i = 0;
    let mut out = Vec::new();
    let max = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
    while i < seq.len() {
        match seq[i] {
            Cls::C => {
                let mut len = 0;
                let mut j = i;
                let mut reason = CommitGroupDispatchReason::QueueDrained;
                while j < seq.len() {
                    match seq[j] {
                        Cls::C => {
                            if len >= max {
                                reason = CommitGroupDispatchReason::Full;
                                break;
                            }
                            len += 1;
                            j += 1;
                        }
                        Cls::O => {
                            // Hoist later; do not count as barrier.
                            j += 1;
                        }
                        Cls::A | Cls::S => {
                            reason = CommitGroupDispatchReason::Barrier;
                            break;
                        }
                    }
                }
                // Production re-queues interior observations at the front after
                // the command group; emit them next, then continue past the run.
                out.push(Expected::CommandGroup { len, reason });
                let mut interior_obs = 0;
                let mut pos = i;
                let mut taken = 0;
                while taken < len {
                    match seq[pos] {
                        Cls::C => {
                            taken += 1;
                            pos += 1;
                        }
                        Cls::O => {
                            interior_obs += 1;
                            pos += 1;
                        }
                        _ => unreachable!("command run only walks C/O"),
                    }
                }
                for _ in 0..interior_obs {
                    out.push(Expected::Observation);
                }
                i = pos;
            }
            Cls::A => {
                let mut len = 0;
                let mut reason = CommitGroupDispatchReason::QueueDrained;
                // In our factory each A gets a distinct request_id unless we
                // deliberately reuse seeds. Group until non-A or Full.
                let mut j = i;
                while j < seq.len() && seq[j] == Cls::A {
                    if len >= max {
                        reason = CommitGroupDispatchReason::Full;
                        break;
                    }
                    len += 1;
                    j += 1;
                }
                if reason == CommitGroupDispatchReason::QueueDrained
                    && j < seq.len()
                    && seq[j] != Cls::A
                {
                    reason = CommitGroupDispatchReason::Barrier;
                }
                out.push(Expected::AuditGroup { len, reason });
                i = j;
            }
            Cls::O => {
                out.push(Expected::Observation);
                i += 1;
            }
            Cls::S => {
                out.push(Expected::Shutdown);
                i += 1;
            }
        }
    }
    out
}

fn to_messages(
    seq: &[Cls],
    audit_seed: &mut u8,
    cmd_seed: &mut u8,
    obs_seed: &mut u8,
) -> VecDeque<CoordinatorMessage> {
    let mut q = VecDeque::new();
    for &c in seq {
        match c {
            Cls::C => {
                *cmd_seed = cmd_seed.wrapping_add(1);
                q.push_back(command_msg_with_id(*cmd_seed));
            }
            Cls::A => {
                *audit_seed = audit_seed.wrapping_add(1);
                q.push_back(audit_msg(*audit_seed));
            }
            Cls::O => {
                *obs_seed = obs_seed.wrapping_add(1);
                q.push_back(observation_msg(*obs_seed));
            }
            Cls::S => q.push_back(shutdown_msg()),
        }
    }
    q
}

fn unit_shape(unit: WorkUnit, reason: CommitGroupDispatchReason) -> Expected {
    match unit {
        WorkUnit::CommandGroup(g) => Expected::CommandGroup {
            len: g.len(),
            reason,
        },
        WorkUnit::AuditGroup(g) => Expected::AuditGroup {
            len: g.len(),
            reason,
        },
        WorkUnit::Single(CoordinatorMessage::IdempotencyInspection { .. }) => Expected::Observation,
        WorkUnit::Shutdown => Expected::Shutdown,
        WorkUnit::Single(_) => Expected::SingleBarrier,
    }
}

fn drain_production(mut pending: VecDeque<CoordinatorMessage>) -> Vec<Expected> {
    let mut out = Vec::new();
    while let Some((unit, reason)) = form_next_unit(&mut pending) {
        out.push(unit_shape(unit, reason));
    }
    out
}

fn count_in_out(seq: &[Cls]) -> (usize, usize) {
    let mut a = 0u8;
    let mut c = 0u8;
    let mut o = 0u8;
    let mut pending = to_messages(seq, &mut a, &mut c, &mut o);
    let input_len = pending.len();
    let mut output = 0usize;
    while let Some((unit, _)) = form_next_unit(&mut pending) {
        output += match unit {
            WorkUnit::CommandGroup(g) => g.len(),
            WorkUnit::AuditGroup(g) => g.len(),
            WorkUnit::Single(_) | WorkUnit::Shutdown => 1,
        };
    }
    (input_len, output)
}

// --- Mandated tests 1–8 ---

#[test]
fn command_window_grows_only_before_a_bound_or_hard_barrier() {
    let mut pending = VecDeque::from([command_msg_with_id(1)]);
    assert!(command_prefix_can_grow(&pending));

    pending.push_back(observation_msg(2));
    assert!(command_prefix_can_grow(&pending));

    pending.push_back(audit_msg(3));
    assert!(!command_prefix_can_grow(&pending));

    let full = (0..riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
        .map(|ordinal| command_msg_with_id(u8::try_from(ordinal).expect("bounded ordinal")))
        .collect::<VecDeque<_>>();
    assert!(!command_prefix_can_grow(&full));
}

#[test]
fn form_next_unit_preserves_order_and_loses_nothing() {
    // Property: random class sequences drain exhaustively; message count in == out
    // (modulo observation hoisting, which reorders but does not drop). Oracle
    // shapes must match production.
    let mut rng = 0xC0FFEE_u64;
    let classes = [Cls::C, Cls::A, Cls::O, Cls::S];
    for trial in 0..40 {
        let len = 3 + ((rng >> 3) as usize % 12);
        let mut seq = Vec::with_capacity(len);
        for _ in 0..len {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            // Bias away from Shutdown mid-stream so we exercise grouping.
            let pick = if (rng >> 33).is_multiple_of(10) {
                Cls::S
            } else {
                classes[((rng >> 17) as usize) % 3]
            };
            seq.push(pick);
        }
        // Drop trailing material after first Shutdown for a clean stream.
        if let Some(pos) = seq.iter().position(|c| *c == Cls::S) {
            seq.truncate(pos + 1);
        }
        let mut a = (trial as u8).wrapping_mul(3);
        let mut c = (trial as u8).wrapping_mul(5);
        let mut o = (trial as u8).wrapping_mul(7);
        let pending = to_messages(&seq, &mut a, &mut c, &mut o);
        let produced = drain_production(pending);
        let expected = expected_units(&seq);
        assert_eq!(
            produced, expected,
            "trial {trial} seq={seq:?} produced={produced:?} expected={expected:?}"
        );
        let (inn, out) = count_in_out(&seq);
        assert_eq!(inn, out, "trial {trial}: count in {inn} != out {out}");
    }
}

#[test]
fn command_audit_interleaving_forms_three_ordered_units() {
    // [C,C,A,A,C,C] → CommandGroup[2], AuditGroup[2], CommandGroup[2].
    let seq = [Cls::C, Cls::C, Cls::A, Cls::A, Cls::C, Cls::C];
    let mut a = 10;
    let mut c = 20;
    let mut o = 30;
    let pending = to_messages(&seq, &mut a, &mut c, &mut o);
    let units = drain_production(pending);
    assert_eq!(
        units,
        vec![
            Expected::CommandGroup {
                len: 2,
                reason: CommitGroupDispatchReason::Barrier,
            },
            Expected::AuditGroup {
                len: 2,
                reason: CommitGroupDispatchReason::Barrier,
            },
            Expected::CommandGroup {
                len: 2,
                reason: CommitGroupDispatchReason::QueueDrained,
            },
        ]
    );
}

#[test]
fn duplicate_request_id_audits_form_separate_units() {
    let mut pending = VecDeque::new();
    // Same request_id for both audits.
    pending.push_back(audit_msg(0x42));
    pending.push_back(audit_msg(0x42));
    let (u1, r1) = form_next_unit(&mut pending).expect("first unit");
    assert!(matches!(u1, WorkUnit::AuditGroup(ref g) if g.len() == 1));
    assert_eq!(r1, CommitGroupDispatchReason::Full);
    let (u2, r2) = form_next_unit(&mut pending).expect("second unit");
    assert!(matches!(u2, WorkUnit::AuditGroup(ref g) if g.len() == 1));
    assert_eq!(r2, CommitGroupDispatchReason::QueueDrained);
    assert!(form_next_unit(&mut pending).is_none());
}

#[test]
fn one_above_command_ceiling_forms_full_then_queue_drained() {
    let ceiling = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
    let mut pending = VecDeque::new();
    for ordinal in 0..=ceiling {
        let seed = u8::try_from(ordinal % 256).expect("bounded command seed");
        pending.push_back(command_msg_with_id(seed));
    }
    let (u1, r1) = form_next_unit(&mut pending).expect("full group");
    assert_eq!(r1, CommitGroupDispatchReason::Full);
    assert!(matches!(u1, WorkUnit::CommandGroup(ref g) if g.len() == ceiling));
    let (u2, r2) = form_next_unit(&mut pending).expect("remainder");
    assert_eq!(r2, CommitGroupDispatchReason::QueueDrained);
    assert!(matches!(u2, WorkUnit::CommandGroup(ref g) if g.len() == 1));
    assert!(form_next_unit(&mut pending).is_none());
}

#[test]
fn barrier_is_never_overtaken_by_later_commands() {
    // [C1, A2, C3] → three units in order. MUST fail under barrier-overtaking.
    let mut pending = VecDeque::new();
    pending.push_back(command_msg_with_id(1));
    pending.push_back(audit_msg(2));
    pending.push_back(command_msg_with_id(3));
    let units = drain_production(pending);
    assert_eq!(
        units,
        vec![
            Expected::CommandGroup {
                len: 1,
                reason: CommitGroupDispatchReason::Barrier,
            },
            Expected::AuditGroup {
                len: 1,
                reason: CommitGroupDispatchReason::Barrier,
            },
            Expected::CommandGroup {
                len: 1,
                reason: CommitGroupDispatchReason::QueueDrained,
            },
        ]
    );
}

#[test]
fn deferrable_observation_does_not_break_a_command_run() {
    // [C1, O1, C2] → CommandGroup[C1,C2] then Single(O1).
    let mut pending = VecDeque::new();
    pending.push_back(command_msg_with_id(1));
    pending.push_back(observation_msg(1));
    pending.push_back(command_msg_with_id(2));
    let units = drain_production(pending);
    assert_eq!(
        units,
        vec![
            Expected::CommandGroup {
                len: 2,
                reason: CommitGroupDispatchReason::QueueDrained,
            },
            Expected::Observation,
        ]
    );
}

#[test]
fn shutdown_mid_run_yields_shutdown_after_the_current_command_unit() {
    let mut pending = VecDeque::new();
    pending.push_back(command_msg_with_id(1));
    pending.push_back(command_msg_with_id(2));
    pending.push_back(shutdown_msg());
    pending.push_back(command_msg_with_id(3));
    let units = drain_production(pending);
    assert_eq!(
        units,
        vec![
            Expected::CommandGroup {
                len: 2,
                reason: CommitGroupDispatchReason::Barrier,
            },
            Expected::Shutdown,
            Expected::CommandGroup {
                len: 1,
                reason: CommitGroupDispatchReason::QueueDrained,
            },
        ]
    );
}

#[test]
fn pending_holding_only_deferrable_observations_forms_a_single_unit() {
    let mut pending = VecDeque::new();
    pending.push_back(observation_msg(1));
    pending.push_back(observation_msg(2));
    pending.push_back(observation_msg(3));
    let units = drain_production(pending);
    assert_eq!(
        units,
        vec![
            Expected::Observation,
            Expected::Observation,
            Expected::Observation,
        ]
    );
}

#[test]
fn post_commit_window_requires_two_commands_and_allows_deferrable_observations() {
    let mut pending = VecDeque::new();
    pending.push_back(command_msg_with_id(1));
    assert!(!post_commit_command_window_eligible(&pending));

    pending.push_back(observation_msg(2));
    pending.push_back(command_msg_with_id(3));
    assert!(post_commit_command_window_eligible(&pending));
}

#[test]
fn post_commit_window_never_delays_a_barrier_or_full_prefix() {
    let mut barrier = VecDeque::new();
    barrier.push_back(command_msg_with_id(1));
    barrier.push_back(command_msg_with_id(2));
    barrier.push_back(shutdown_msg());
    assert!(!post_commit_command_window_eligible(&barrier));

    let full = (0..riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
        .map(|index| command_msg_with_id(u8::try_from(index % 255).expect("bounded") + 1))
        .collect::<VecDeque<_>>();
    assert!(!post_commit_command_window_eligible(&full));

    let almost_full = (0..riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS - 1)
        .map(|index| command_msg_with_id(u8::try_from(index).expect("bounded") + 1))
        .collect::<VecDeque<_>>();
    assert!(post_commit_command_window_eligible(&almost_full));
}

#[test]
fn post_commit_collector_drains_ready_arrivals_without_reordering() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        let (sender, mut receiver) = mpsc::channel(4);
        sender
            .send(command_msg_with_id(3))
            .await
            .expect("ready arrival");
        drop(sender);

        let mut pending = VecDeque::from([command_msg_with_id(1), command_msg_with_id(2)]);
        let mut shutting_down = false;
        collect_until_coalesce_deadline(
            &mut receiver,
            &mut pending,
            4,
            Instant::now() + POST_COMMIT_COALESCE_BUDGET,
            &mut shutting_down,
            &WallCoordinatorMonotonicClock,
        )
        .await;

        assert_eq!(pending.len(), 3);
        assert!(shutting_down, "closed intake is observed after ready work");
        let (unit, reason) = form_next_unit(&mut pending).expect("command group");
        assert_eq!(reason, CommitGroupDispatchReason::QueueDrained);
        assert!(matches!(unit, WorkUnit::CommandGroup(group) if group.len() == 3));
    });
}

#[test]
fn audited_singleton_is_eligible_for_standard_deferred_publication() {
    let preparation = audited_command_preparation(1);

    assert!(
        crate::command_execution::command_group_is_deferred_eligible(std::iter::once(&preparation))
    );
}

#[test]
fn completion_edge_delay_is_confined_to_the_direct_redb_oracle() {
    assert!(completion_edge_coalescing_enabled(
        crate::CoordinatorDurability::Sync
    ));
    assert!(!completion_edge_coalescing_enabled(
        crate::CoordinatorDurability::Group
    ));
}

#[test]
fn preflight_rejects_a_group_that_dynamic_compatibility_would_split() {
    let first = audited_command_preparation(1);
    let same_target = audited_command_preparation(2);

    assert!(!crate::command_execution::command_group_is_deferred_eligible([&first, &same_target]));
}

// req: TXN-001, TXN-002, PERF-022
#[test]
fn concurrent_duplicate_idempotent_commands_across_deferred_pipeline_replay_without_stopping_coordinator()
 {
    use std::sync::Mutex;

    use riffdb_idempotency::IdempotencyPipelineFootprintV1;
    use riffdb_storage_api::{
        DeclaredOutcome, DurabilityMode, IdempotencyIdentity, StoredAdmittedProvenanceClaimsV1,
        StoredOutcomeV1,
    };
    use riffdb_types::{
        AdmittedActorContext, CanonicalInputHash, CommitSequence, FieldId, LogicalTime, OutcomeId,
        ProvenanceId, hash_partition_key,
    };

    use crate::command_execution::{
        CommandEvaluationFrontier, CommandGroupDriveResult, SubmittedCommandGroup,
    };
    use crate::command_records::{CheckedCommandGroupCommitResult, CheckedCommandGroupFence};
    use crate::{CommittedOutcome, CommittedOutcomeDisposition};

    let first = audited_command_preparation(1);
    let duplicate = audited_command_preparation(2);

    // Intra-group duplicate idempotency identities must not produce a deferred
    // pipeline footprint, ensuring the writer drains in-flight work and
    // evaluates on the published frontier rather than entering WriterPrivate.
    assert!(
        crate::command_execution::command_group_deferred_pipeline_footprint([&first, &duplicate])
            .is_none(),
        "command group containing duplicate idempotency identities must not enter WriterPrivate"
    );

    // Across separate units, footprints carrying the same idempotency identity
    // must report overlap so in-flight predecessors drain before the duplicate runs.
    let first_footprint = crate::command_execution::command_group_deferred_pipeline_footprint(
        std::iter::once(&first),
    )
    .expect("singleton deferred pipeline footprint");
    let duplicate_footprint = crate::command_execution::command_group_deferred_pipeline_footprint(
        std::iter::once(&duplicate),
    )
    .expect("singleton duplicate deferred pipeline footprint");
    assert!(
        first_footprint.shares_idempotency_identity(&duplicate_footprint),
        "duplicate idempotency submissions across units must report identity overlap"
    );

    fn stored_outcome_for_test(
        sequence: CommitSequence,
        request_seed: u8,
        digest_byte: u8,
    ) -> StoredOutcomeV1 {
        let parts = fixture_parts();
        let database = DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("db");
        let environment = Environment::new("development").expect("env");
        let principal = ActorId::new("form-kernel-principal").expect("principal");
        let actor = AdmittedActorContext::new(
            principal.clone(),
            ActorKind::Agent,
            TenantScope::Global,
            None,
        );
        let identity = IdempotencyIdentity::new(
            database,
            environment,
            TenantScope::Global,
            principal,
            parts.reference.contract_lineage().clone(),
            parts.reference.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [digest_byte; 32],
            ),
        );
        let declared_outcome = DeclaredOutcome::new(
            OutcomeId::new(1).expect("outcome ID"),
            CanonicalRecord::new(vec![(
                FieldId::new(1).expect("field ID"),
                CanonicalValue::Bool(true),
            )])
            .expect("canonical outcome"),
        )
        .expect("declared outcome");
        StoredOutcomeV1::new(
            identity,
            sequence,
            request_id(request_seed),
            parts.reference.clone(),
            CanonicalInputHash::from_bytes([digest_byte; 32]),
            actor,
            LogicalTime::new(Timestamp::new(200, 0).expect("timestamp")),
            parts.partition.clone(),
            hash_partition_key(parts.partition.as_bytes()),
            Vec::new(),
            declared_outcome,
            StoredAdmittedProvenanceClaimsV1::default(),
            ProvenanceId::from_bytes(uuid_bytes(request_seed.wrapping_add(0x40)))
                .expect("provenance UUIDv7"),
            DurabilityMode::Group,
        )
        .expect("stored outcome")
    }

    struct LatchedFence {
        waiting_tx: std::sync::mpsc::SyncSender<()>,
        release_rx: std::sync::mpsc::Receiver<()>,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
        outcomes: Vec<CommittedOutcome>,
    }

    impl CheckedCommandGroupFence for LatchedFence {
        fn requires_pipeline_drain(&self) -> bool {
            false
        }

        fn wait(mut self: Box<Self>) -> CheckedCommandGroupCommitResult {
            let _ = self.waiting_tx.send(());
            self.release_rx
                .recv()
                .expect("test releases in-flight fence");
            if let Some(on_publish) = self.on_publish.take() {
                on_publish();
            }
            CheckedCommandGroupCommitResult::Committed(self.outcomes)
        }
    }

    struct DuplicatePipelineOperations {
        lifecycle: ActorLifecyclePublisher,
        unit_index: usize,
        published_outcomes: Arc<Mutex<Vec<(IdempotencyPipelineFootprintV1, StoredOutcomeV1)>>>,
        observed_frontiers: Arc<Mutex<Vec<(usize, CommandEvaluationFrontier)>>>,
        unit1_waiting_tx: Option<std::sync::mpsc::SyncSender<()>>,
        unit1_release_rx: Option<std::sync::mpsc::Receiver<()>>,
        unit2_entered_tx: Option<std::sync::mpsc::SyncSender<()>>,
        unit2_proceed_rx: Option<std::sync::mpsc::Receiver<()>>,
        unit2_waiting_tx: Option<std::sync::mpsc::SyncSender<()>>,
        unit2_release_rx: Option<std::sync::mpsc::Receiver<()>>,
    }

    impl CoordinatorActorOperations for DuplicatePipelineOperations {
        fn append_audit(
            &mut self,
            _: &dyn AdministrationAuditInputView,
        ) -> Result<(), AdministrationAuditExecutionError> {
            Ok(())
        }

        fn append_audit_group(
            &mut self,
            inputs: &[Box<dyn AdministrationAuditInputView>],
        ) -> Vec<Result<(), AdministrationAuditExecutionError>> {
            (0..inputs.len()).map(|_| Ok(())).collect()
        }

        fn drive_command(&mut self, _: CommandExecutionPreparation) -> LocalCommandFuture<'_> {
            Box::pin(async { Err(CommandExecutionError::coordinator_stopped()) })
        }

        fn drive_command_group(
            &mut self,
            preparations: Vec<CommandExecutionPreparation>,
            evaluation_frontier: CommandEvaluationFrontier,
        ) -> LocalCommandGroupFuture<'_> {
            self.unit_index += 1;
            let unit_index = self.unit_index;
            self.observed_frontiers
                .lock()
                .expect("observed frontiers")
                .push((preparations.len(), evaluation_frontier));

            let footprints = preparations
                .iter()
                .map(|prep| {
                    prep.deferred_pipeline_compatibility()
                        .expect("prepared compatibility")
                        .idempotency_footprint
                })
                .collect::<Vec<_>>();

            // Any duplicate idempotent command evaluated on WriterPrivate would hit
            // the transaction-local storage invariant violation and stop the coordinator.
            let mut batch_seen = Vec::<IdempotencyPipelineFootprintV1>::new();
            let has_duplicate = {
                let published = self.published_outcomes.lock().expect("published outcomes");
                footprints.iter().any(|fp| {
                    let dup = published.iter().any(|(prior, _)| prior.overlaps(fp))
                        || batch_seen.iter().any(|prior| prior.overlaps(fp));
                    batch_seen.push(fp.clone());
                    dup
                })
            };
            if evaluation_frontier == CommandEvaluationFrontier::WriterPrivate && has_duplicate {
                self.lifecycle.stop();
                let count = preparations.len();
                return Box::pin(async move {
                    CommandGroupDriveResult::Complete(
                        (0..count)
                            .map(|_| Err(CommandExecutionError::coordinator_stopped()))
                            .collect(),
                    )
                });
            }

            match unit_index {
                1 => {
                    assert_eq!(preparations.len(), 1);
                    assert_eq!(evaluation_frontier, CommandEvaluationFrontier::Published);
                    let outcome = stored_outcome_for_test(CommitSequence::first(), 1, 0x51);
                    let published = Arc::clone(&self.published_outcomes);
                    let fp = footprints.into_iter().next().expect("unit 1 footprint");
                    let outcome_for_publish = outcome.clone();
                    let fence = LatchedFence {
                        waiting_tx: self.unit1_waiting_tx.take().expect("unit1 waiting tx"),
                        release_rx: self.unit1_release_rx.take().expect("unit1 release rx"),
                        on_publish: Some(Box::new(move || {
                            published
                                .lock()
                                .expect("published outcomes")
                                .push((fp, outcome_for_publish));
                        })),
                        outcomes: vec![CommittedOutcome::first_commit(outcome)],
                    };
                    let submitted = SubmittedCommandGroup::submitted_for_test(1, Box::new(fence));
                    Box::pin(async move { CommandGroupDriveResult::Submitted(submitted) })
                }
                2 => {
                    assert_eq!(preparations.len(), 1);
                    assert_eq!(evaluation_frontier, CommandEvaluationFrontier::Published);
                    self.unit2_entered_tx
                        .take()
                        .expect("unit2 entered tx")
                        .send(())
                        .expect("notify unit2 entered");
                    self.unit2_proceed_rx
                        .take()
                        .expect("unit2 proceed rx")
                        .recv()
                        .expect("wait for unit3 batch submission");
                    // Unit 1 must have published before Unit 2 began evaluation.
                    let outcome = self
                        .published_outcomes
                        .lock()
                        .expect("published outcomes")
                        .iter()
                        .find(|(prior, _)| prior.overlaps(&footprints[0]))
                        .map(|(_, outcome)| outcome.clone())
                        .expect("unit 1 must be drained and published before duplicate unit 2");
                    let fence = LatchedFence {
                        waiting_tx: self.unit2_waiting_tx.take().expect("unit2 waiting tx"),
                        release_rx: self.unit2_release_rx.take().expect("unit2 release rx"),
                        on_publish: None,
                        outcomes: vec![CommittedOutcome::replay(outcome)],
                    };
                    let submitted = SubmittedCommandGroup::submitted_for_test(1, Box::new(fence));
                    Box::pin(async move { CommandGroupDriveResult::Submitted(submitted) })
                }
                3 => {
                    assert_eq!(preparations.len(), 4);
                    assert_eq!(evaluation_frontier, CommandEvaluationFrontier::Published);
                    let outcome = self
                        .published_outcomes
                        .lock()
                        .expect("published outcomes")
                        .iter()
                        .find(|(prior, _)| prior.overlaps(&footprints[0]))
                        .map(|(_, outcome)| outcome.clone())
                        .expect("published outcome for duplicate batch");
                    let results = (0..4)
                        .map(|_| {
                            Ok(CommandExecutionResult::Committed(CommittedOutcome::replay(
                                outcome.clone(),
                            )))
                        })
                        .collect();
                    Box::pin(async move { CommandGroupDriveResult::Complete(results) })
                }
                4 => {
                    assert_eq!(preparations.len(), 1);
                    assert_eq!(evaluation_frontier, CommandEvaluationFrontier::Published);
                    let seq = CommitSequence::new(2).expect("sequence 2");
                    let outcome = stored_outcome_for_test(seq, 7, 0x52);
                    let results = vec![Ok(CommandExecutionResult::Committed(
                        CommittedOutcome::first_commit(outcome),
                    ))];
                    Box::pin(async move { CommandGroupDriveResult::Complete(results) })
                }
                other => panic!("unexpected command group unit {other}"),
            }
        }

        fn inspect_idempotency(
            &mut self,
            _: PreparedCommandIdempotencyInspection,
        ) -> Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError> {
            Err(CommandIdempotencyInspectionError::coordinator_stopped())
        }

        fn drive_read_only(
            &mut self,
            _: ReadOnlyExecutionPreparation,
        ) -> Result<ReadOnlyExecutionResult, CommandExecutionError> {
            Err(CommandExecutionError::coordinator_stopped())
        }

        fn deploy_catalog(
            &mut self,
            _: CatalogDeploymentPreparation,
        ) -> Result<CatalogDeploymentResult, ControlPlaneExecutionError> {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }

        fn deploy_query_module(
            &mut self,
            _: QueryModuleDeploymentPreparation,
        ) -> Result<QueryModuleDeploymentResult, ControlPlaneExecutionError> {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }

        fn publish_reactive_module(
            &mut self,
            _: ReactiveModulePublicationPreparation,
        ) -> Result<ReactiveModulePublicationExecutionResult, ControlPlaneExecutionError> {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }

        fn create_capability(
            &mut self,
            _: CapabilityCreatePreparation,
        ) -> Result<CapabilityCreateExecutionResult, ControlPlaneExecutionError> {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }

        fn fence_primary(
            &mut self,
            _: AuthorizedPrimaryFencePreparation,
        ) -> Result<PrimaryFenceExecutionResult, ControlPlaneExecutionError> {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }

        fn administer_replication(
            &mut self,
            _: AuthorizedReplicationAdministrationPreparation,
        ) -> Result<ReplicationAdministrationExecutionResult, ControlPlaneExecutionError> {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }

        fn maintain_replication(
            &mut self,
        ) -> Result<ReplicationRegistrationMaintenanceResultV1, ControlPlaneExecutionError>
        {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }

        fn revoke_capability(
            &mut self,
            _: CapabilityRevokePreparation,
        ) -> Result<CapabilityRevokeExecutionResult, ControlPlaneExecutionError> {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }

        fn bootstrap_capability(
            &mut self,
            _: CapabilityBootstrapPreparation,
        ) -> Result<CapabilityBootstrapExecutionResult, ControlPlaneExecutionError> {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }

        fn append_bootstrap_terminal(
            &mut self,
            _: CapabilityBootstrapTerminalPreparation,
        ) -> Result<(), ControlPlaneExecutionError> {
            Err(ControlPlaneExecutionError::coordinator_stopped())
        }
    }

    let (unit1_waiting_tx, unit1_waiting_rx) = std::sync::mpsc::sync_channel(1);
    let (unit1_release_tx, unit1_release_rx) = std::sync::mpsc::sync_channel(1);
    let (unit2_entered_tx, unit2_entered_rx) = std::sync::mpsc::sync_channel(1);
    let (unit2_proceed_tx, unit2_proceed_rx) = std::sync::mpsc::sync_channel(1);
    let (unit2_waiting_tx, unit2_waiting_rx) = std::sync::mpsc::sync_channel(1);
    let (unit2_release_tx, unit2_release_rx) = std::sync::mpsc::sync_channel(1);

    let published_outcomes = Arc::new(Mutex::new(Vec::new()));
    let observed_frontiers = Arc::new(Mutex::new(Vec::new()));
    let published_for_ops = Arc::clone(&published_outcomes);
    let frontiers_for_ops = Arc::clone(&observed_frontiers);

    let running = RunningCommandCoordinator::spawn_with_operations(
        CoordinatorWorkloadCapacity::new(16).expect("capacity"),
        false,
        PrimaryAdmissionGate::new(),
        Arc::new(DiscardApplicationCommitNotifications),
        Arc::new(NoopCommitTelemetry),
        move |lifecycle| {
            Box::new(DuplicatePipelineOperations {
                lifecycle,
                unit_index: 0,
                published_outcomes: published_for_ops,
                observed_frontiers: frontiers_for_ops,
                unit1_waiting_tx: Some(unit1_waiting_tx),
                unit1_release_rx: Some(unit1_release_rx),
                unit2_entered_tx: Some(unit2_entered_tx),
                unit2_proceed_rx: Some(unit2_proceed_rx),
                unit2_waiting_tx: Some(unit2_waiting_tx),
                unit2_release_rx: Some(unit2_release_rx),
            })
        },
    )
    .expect("spawn coordinator");
    let executor = running.command_executor();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime");

    runtime.block_on(async {
        let submit = |prep: CommandExecutionPreparation| {
            let executor = executor.clone();
            async move {
                let reservation = executor
                    .reserve_capacity()
                    .await
                    .expect("reserve command capacity");
                reservation
                    .submit(prep)
                    .expect("submit command preparation")
            }
        };

        // 1. Submit the initial command (unit 1); wait until its deferred fence is
        // parked on the completion thread while `in_flight` holds unit 1's footprint.
        let receipt1 = submit(audited_command_preparation(1)).await;
        unit1_waiting_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unit 1 fence parked in completion lane");

        // 2. Queue a post-dispatch hook so that as soon as unit 2 (singleton duplicate)
        // is dispatched to `work_tx` while unit 1 is still in `in_flight`, unit 1's
        // fence is released so `CommandWriter`'s `drain_all_completions` can complete.
        let release_unit1 = unit1_release_tx;
        running.queue_post_dispatch_hook(Box::new(move || {
            let _ = release_unit1.send(());
        }));
        let receipt2 = submit(audited_command_preparation(2)).await;

        // Wait until `CommandWriter` has drained unit 1 and entered `drive_command_group`
        // for unit 2 (`writer_busy == true`).
        unit2_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unit 2 entered drive_command_group after draining unit 1");

        // 3. Submit 4 more concurrent duplicates while `writer_busy == true` so the
        // intake actor coalesces them into a 4-command duplicate batch (unit 3).
        let receipt3 = submit(audited_command_preparation(3)).await;
        let receipt4 = submit(audited_command_preparation(4)).await;
        let receipt5 = submit(audited_command_preparation(5)).await;
        let receipt6 = submit(audited_command_preparation(6)).await;

        // Release unit 2's `drive_command_group` so it submits `unit2_fence` to `in_flight`,
        // and hook unit 3's dispatch to release `unit2_fence` once unit 3 arrives on `work_tx`.
        let release_unit2 = unit2_release_tx;
        running.queue_post_dispatch_hook(Box::new(move || {
            let _ = release_unit2.send(());
        }));
        unit2_proceed_tx
            .send(())
            .expect("allow unit 2 to submit its deferred fence");
        unit2_waiting_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unit 2 fence parked in completion lane");

        // 4. Collect all 6 concurrent duplicate outcomes: exactly 1 FirstCommit and 5 Replays.
        let mut first_commits = 0_usize;
        let mut replays = 0_usize;
        for receipt in [receipt1, receipt2, receipt3, receipt4, receipt5, receipt6] {
            let result = receipt
                .completion()
                .await
                .expect("duplicate command must not fail or stop coordinator");
            let CommandExecutionResult::Committed(outcome) = result else {
                panic!("expected committed outcome");
            };
            assert_eq!(
                outcome.stored_outcome().commit_sequence(),
                CommitSequence::first()
            );
            match outcome.disposition() {
                CommittedOutcomeDisposition::FirstCommit => first_commits += 1,
                CommittedOutcomeDisposition::Replay => replays += 1,
            }
        }
        assert_eq!(first_commits, 1, "exactly one FirstCommit expected");
        assert_eq!(replays, 5, "five idempotent replays expected");
        assert_eq!(
            executor.lifecycle_state(),
            CoordinatorLifecycleState::Accepting,
            "coordinator must remain Accepting after concurrent duplicate idempotent commands"
        );

        // 5. Subsequent write with a distinct idempotency key commits cleanly at sequence 2.
        let distinct = audited_command_preparation_with_digest(7, "form-kernel-caller-next", 0x52);
        let receipt7 = submit(distinct).await;
        let result7 = receipt7
            .completion()
            .await
            .expect("subsequent write after duplicate batch must succeed");
        let CommandExecutionResult::Committed(outcome7) = result7 else {
            panic!("expected committed outcome for subsequent write");
        };
        assert_eq!(
            outcome7.disposition(),
            CommittedOutcomeDisposition::FirstCommit
        );
        assert_eq!(
            outcome7.stored_outcome().commit_sequence(),
            CommitSequence::new(2).expect("sequence 2")
        );
    });

    assert_eq!(
        *observed_frontiers.lock().expect("observed frontiers"),
        vec![
            (1, CommandEvaluationFrontier::Published),
            (1, CommandEvaluationFrontier::Published),
            (4, CommandEvaluationFrontier::Published),
            (1, CommandEvaluationFrontier::Published),
        ]
    );
    running.shutdown().expect("clean coordinator shutdown");
}

fn follower_acknowledgement_request() -> riffdb_storage_api::ReplicationFollowerAcknowledgementV3 {
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [3; 10]).unwrap();
    let lineage = riffdb_storage_api::ChangelogLineageV3::new(
        database,
        1,
        riffdb_types::LeadershipEpochV1::initial(),
    )
    .unwrap();
    let point = riffdb_storage_api::ChangelogHistoryPointV3::new(
        riffdb_storage_api::ChangelogTransactionSequence::new(1).unwrap(),
        [1; 32],
        riffdb_types::DualFrontier::INITIAL,
    );
    riffdb_storage_api::ReplicationFollowerAcknowledgementV3::new(
        riffdb_storage_api::ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
        lineage,
        point,
    )
}

// req: REP-003, REC-001, LOG-001, END-008
#[test]
fn follower_acknowledgement_keeps_its_fifo_position_between_command_groups() {
    let request = follower_acknowledgement_request();
    let (completion, _receiver) = oneshot::channel();
    let mut pending = VecDeque::from([
        command_msg_with_id(1),
        CoordinatorMessage::FollowerAcknowledgement {
            request,
            completion,
        },
        command_msg_with_id(2),
    ]);
    let (first, reason) = form_next_unit(&mut pending).unwrap();
    assert!(matches!(first, WorkUnit::CommandGroup(group) if group.len() == 1));
    assert_eq!(reason, CommitGroupDispatchReason::Barrier);
    let (second, _) = form_next_unit(&mut pending).unwrap();
    assert!(
        matches!(second, WorkUnit::Single(CoordinatorMessage::FollowerAcknowledgement { request: actual, .. }) if actual == request)
    );
    let (last, _) = form_next_unit(&mut pending).unwrap();
    assert!(matches!(last, WorkUnit::CommandGroup(group) if group.len() == 1));
    assert!(form_next_unit(&mut pending).is_none());
}

// req: REP-003, REC-001, LOG-001
#[tokio::test]
async fn follower_acknowledgement_lost_completion_preserves_uncertainty() {
    let (completion, receiver) = oneshot::channel();
    let receipt = FollowerAcknowledgementReceipt { receiver };
    drop(completion);
    assert!(matches!(receipt.completion().await,
        Err(riffdb_storage_api::ChangelogCursorErrorV3::Storage(error))
            if error.kind() == riffdb_storage_api::StorageErrorKind::CommitStatusUnknown));
}

// req: REP-003, REC-001, LOG-001
#[tokio::test]
async fn follower_acknowledgement_fenced_and_stopped_messages_return_certain_refusal() {
    for reject in [reject_message_fenced, reject_message_stopped] {
        let (completion, receiver) = oneshot::channel();
        reject(CoordinatorMessage::FollowerAcknowledgement {
            request: follower_acknowledgement_request(),
            completion,
        });
        assert!(
            matches!(FollowerAcknowledgementReceipt { receiver }.completion().await,
            Err(riffdb_storage_api::ChangelogCursorErrorV3::Storage(error))
                if error.kind() == riffdb_storage_api::StorageErrorKind::Unavailable)
        );
    }
}

fn command_msg_with_byte_units(seed: u8, byte_units: u32) -> CoordinatorMessage {
    let (completion, _) = oneshot::channel();
    let preparation = command_preparation(seed).with_queued_byte_units_for_test(byte_units);
    CoordinatorMessage::Command {
        preparation: Box::new(preparation),
        command_id: fixture_parts().reference.command_id(),
        ingress: ServiceIngressKindV1::Grpc,
        enqueued_at: Instant::now(),
        completion,
        _retained_byte_permit: retained_permit(),
    }
}

// req: PERF-022
#[test]
fn idle_singleton_seals_without_group_formation_wait() {
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

    struct MicrosecondStepClock {
        base: Instant,
        micros: AtomicU64,
    }
    impl CoordinatorMonotonicClock for MicrosecondStepClock {
        fn now(&self) -> Instant {
            let step = self.micros.fetch_add(1, Ordering::Relaxed);
            self.base + Duration::from_micros(step)
        }
        fn sleep_until(&self, _deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(std::future::ready(()))
        }
    }

    #[derive(Default)]
    struct DispatchElapsedTelemetry {
        dispatches: Mutex<Vec<(CommitGroupDispatchReason, u16, u16, Duration)>>,
    }
    impl CommitTelemetry for DispatchElapsedTelemetry {
        fn record(&self, event: CommitTelemetryEvent) {
            if let CommitTelemetryEvent::CommandGroupDispatched {
                reason,
                selected,
                deferred,
                elapsed,
            } = event
            {
                self.dispatches
                    .lock()
                    .expect("dispatches")
                    .push((reason, selected, deferred, elapsed));
            }
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        let (sender, receiver) = mpsc::channel(8);
        let (feedback_tx, feedback_rx) = mpsc::channel(1);
        let (work_tx, work_rx) = std::sync::mpsc::sync_channel(1);
        let telemetry = Arc::new(DispatchElapsedTelemetry::default());
        let clock = Arc::new(MicrosecondStepClock {
            base: Instant::now(),
            micros: AtomicU64::new(0),
        });
        let actor = CommandCoordinatorActor {
            receiver,
            telemetry: Arc::clone(&telemetry) as Arc<dyn CommitTelemetry>,
            lifecycle: ActorLifecyclePublisher {
                lifecycle: Arc::new(AtomicU8::new(LIFECYCLE_ACCEPTING)),
                submission_gate: Arc::new(SubmissionGate::new(PrimaryAdmissionGate::new())),
            },
            feedback: feedback_rx,
            monotonic_clock: clock,
            completion_edge_coalescing_enabled: false,
            post_dispatch_hooks: None,
        };
        let handle = tokio::spawn(actor.run(work_tx, 8));

        // Let the actor park on idle `receiver.recv().await`, then send one singleton.
        tokio::task::yield_now().await;
        sender
            .send(command_msg_with_id(1))
            .await
            .expect("send idle singleton");
        tokio::task::yield_now().await;

        let unit = work_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("idle singleton dispatched");
        assert!(matches!(unit, WorkUnit::CommandGroup(group) if group.len() == 1));
        feedback_tx
            .send(UnitCompleted)
            .await
            .expect("complete singleton");

        sender
            .send(CoordinatorMessage::Shutdown)
            .await
            .expect("send shutdown");
        if let Ok(WorkUnit::Shutdown) = work_rx.recv_timeout(Duration::from_secs(1)) {
            let _ = feedback_tx.send(UnitCompleted).await;
        }
        handle.await.expect("actor joined");

        let dispatches = telemetry.dispatches.lock().expect("dispatches");
        assert_eq!(dispatches.len(), 1);
        let (reason, selected, deferred, elapsed) = dispatches[0];
        assert_eq!(reason, CommitGroupDispatchReason::QueueDrained);
        assert_eq!(selected, 1);
        assert_eq!(deferred, 0);
        // Without a group-formation spin loop, the step clock advances only once
        // between `formation_anchor` and `elapsed_since` (1 µs << 200 µs).
        assert!(
            elapsed < OLDEST_GROUPABLE_TRANSITION_MAX_AGE,
            "idle singleton must seal without waiting for the 200 µs group deadline, got {elapsed:?}"
        );
        assert_eq!(elapsed, Duration::from_micros(1));
    });
}

// req: PERF-022
#[test]
fn group_formation_drains_only_prepared_prefix_within_ceilings() {
    // 1. Transition ceiling (256 commands): drain only the first 256 in admission order.
    let mut over_transition_ceiling = (0..259_usize)
        .map(|idx| command_msg_with_id(u8::try_from(idx % 251 + 1).expect("nonzero seed")))
        .collect::<VecDeque<_>>();
    assert!(!command_prefix_can_grow(&over_transition_ceiling));
    let (first_unit, first_reason) =
        form_next_unit(&mut over_transition_ceiling).expect("first ceiling group");
    assert_eq!(first_reason, CommitGroupDispatchReason::Full);
    assert!(
        matches!(first_unit, WorkUnit::CommandGroup(group) if group.len() == riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
    );
    assert_eq!(over_transition_ceiling.len(), 3);
    assert!(command_prefix_can_grow(&over_transition_ceiling));
    let (remainder_unit, remainder_reason) =
        form_next_unit(&mut over_transition_ceiling).expect("remaining prefix");
    assert_eq!(remainder_reason, CommitGroupDispatchReason::QueueDrained);
    assert!(matches!(remainder_unit, WorkUnit::CommandGroup(group) if group.len() == 3));
    assert!(over_transition_ceiling.is_empty());

    // 2. Prepared byte ceiling (16 MiB = 16_384 KiB units): two 8_192-KiB commands fill
    // the epoch to 16 MiB, so the third prepared command remains in `pending`.
    let half_ceiling_units =
        u32::try_from(riffdb_storage_api::MAX_STAGED_WRITE_BYTES / QUEUED_COMMAND_BYTE_UNIT / 2)
            .expect("half ceiling units");
    let mut exact_byte_ceiling = VecDeque::from([
        command_msg_with_byte_units(1, half_ceiling_units),
        command_msg_with_byte_units(2, half_ceiling_units),
    ]);
    assert!(
        !command_prefix_can_grow(&exact_byte_ceiling),
        "prefix at 16 MiB cannot grow further"
    );
    exact_byte_ceiling.push_back(command_msg_with_byte_units(3, 4));

    let (byte_bounded_unit, byte_bounded_reason) =
        form_next_unit(&mut exact_byte_ceiling).expect("byte-bounded group");
    assert_eq!(byte_bounded_reason, CommitGroupDispatchReason::Full);
    let WorkUnit::CommandGroup(byte_group) = byte_bounded_unit else {
        panic!("expected command group");
    };
    assert_eq!(byte_group.len(), 2);
    assert_eq!(byte_group[0].0.queued_byte_units(), half_ceiling_units);
    assert_eq!(byte_group[1].0.queued_byte_units(), half_ceiling_units);
    assert_eq!(exact_byte_ceiling.len(), 1);

    let (tail_unit, tail_reason) =
        form_next_unit(&mut exact_byte_ceiling).expect("trailing command after byte ceiling");
    assert_eq!(tail_reason, CommitGroupDispatchReason::QueueDrained);
    let WorkUnit::CommandGroup(tail_group) = tail_unit else {
        panic!("expected trailing command group");
    };
    assert_eq!(tail_group.len(), 1);
    assert_eq!(tail_group[0].0.queued_byte_units(), 4);
    assert!(exact_byte_ceiling.is_empty());
}
