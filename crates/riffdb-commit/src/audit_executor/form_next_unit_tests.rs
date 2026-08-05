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

fn command_preparation(seed: u8) -> CommandExecutionPreparation {
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
    let caller_key = IdempotencyKey::new("form-kernel-caller").expect("key");
    let lookup =
        prepare_idempotency_lookup(&scope, &caller_key, &FixedDigestProvider).expect("lookup prep");
    let inspection = IdempotencyInspectionExecutor::new(&AbsentAdmission)
        .inspect(lookup)
        .expect("inspect");
    let idempotency = inspection
        .confirm_input(
            &parts.normalized_input,
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
    let facts = derive_input_command_facts(parts.resolved.plan(), parts.normalized_input.clone())
        .expect("facts");
    let (control, _) = CommandRequestControl::new(
        Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("deadline"),
    );
    CommandExecutionPreparation::new(
        database,
        &environment,
        parts.resolved.clone(),
        parts.normalized_input.clone(),
        idempotency,
        facts,
        authorize_fixture(),
        request_id(seed),
        ServiceIngressKindV1::Grpc,
        control,
    )
    .expect("command preparation")
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
fn sixty_five_commands_form_full_then_queue_drained() {
    let mut pending = VecDeque::new();
    for seed in 1..=65u8 {
        pending.push_back(command_msg_with_id(seed));
    }
    let (u1, r1) = form_next_unit(&mut pending).expect("full group");
    assert_eq!(r1, CommitGroupDispatchReason::Full);
    assert!(matches!(u1, WorkUnit::CommandGroup(ref g) if g.len() == 64));
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
