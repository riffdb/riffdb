//! Pure selection-kernel tests for `form_next_unit` (T1.3 falsifiability).

use std::collections::VecDeque;
use std::num::NonZeroU64;

use riffdb_types::{
    ActorId, ActorKind, ApprovalId, CapabilityId, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1,
};
use tokio::sync::oneshot;

use super::*;

// --- Lightweight audit builders (same as actor_tests) ---

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
    capability_revision: std::num::NonZeroU64,
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
    fn capability_revision(&self) -> &std::num::NonZeroU64 {
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

/// Abstract token used only to falsify the selection rules without full preparations.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    C(u8),
    A(u8),
    O(u8),
    R, // ReadOnly / other barrier
    S,
}

fn tok_class(t: &Tok) -> CommandGroupingClass {
    match t {
        Tok::C(_) => CommandGroupingClass::Command,
        Tok::O(_) => CommandGroupingClass::DeferrableObservation,
        Tok::A(_) | Tok::R | Tok::S => CommandGroupingClass::Barrier,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TokUnit {
    CommandGroup(Vec<u8>),
    AuditGroup(Vec<u8>),
    Single(Tok),
    Shutdown,
}

/// Pure token kernel mirroring `form_next_unit` rules exactly.
fn form_next_token(pending: &mut VecDeque<Tok>) -> Option<(TokUnit, CommitGroupDispatchReason)> {
    let head = pending.front()?.clone();
    match tok_class(&head) {
        CommandGroupingClass::Command => {
            let mut group = Vec::new();
            let mut deferred_obs = VecDeque::new();
            let mut reason = CommitGroupDispatchReason::QueueDrained;
            while let Some(class) = pending.front().map(tok_class) {
                match class {
                    CommandGroupingClass::Command => {
                        if group.len() >= riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
                            reason = CommitGroupDispatchReason::Full;
                            break;
                        }
                        if let Some(Tok::C(id)) = pending.pop_front() {
                            group.push(id);
                        }
                    }
                    CommandGroupingClass::DeferrableObservation => {
                        deferred_obs.push_back(pending.pop_front().unwrap());
                    }
                    CommandGroupingClass::Barrier => {
                        reason = CommitGroupDispatchReason::Barrier;
                        break;
                    }
                }
            }
            while let Some(m) = deferred_obs.pop_back() {
                pending.push_front(m);
            }
            Some((TokUnit::CommandGroup(group), reason))
        }
        CommandGroupingClass::Barrier if matches!(head, Tok::A(_)) => {
            let mut group = Vec::new();
            let mut seen = std::collections::BTreeSet::new();
            let mut reason = CommitGroupDispatchReason::QueueDrained;
            while let Some(Tok::A(id)) = pending.front().cloned() {
                if group.len() >= riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
                    reason = CommitGroupDispatchReason::Full;
                    break;
                }
                pending.pop_front();
                if !seen.insert(id) {
                    pending.push_front(Tok::A(id));
                    reason = CommitGroupDispatchReason::Full;
                    break;
                }
                group.push(id);
            }
            if reason == CommitGroupDispatchReason::QueueDrained
                && pending.front().is_some_and(|t| !matches!(t, Tok::A(_)))
            {
                reason = CommitGroupDispatchReason::Barrier;
            }
            Some((TokUnit::AuditGroup(group), reason))
        }
        CommandGroupingClass::DeferrableObservation => {
            let m = pending.pop_front()?;
            Some((TokUnit::Single(m), CommitGroupDispatchReason::QueueDrained))
        }
        CommandGroupingClass::Barrier => {
            let m = pending.pop_front()?;
            if matches!(m, Tok::S) {
                Some((TokUnit::Shutdown, CommitGroupDispatchReason::QueueDrained))
            } else {
                Some((TokUnit::Single(m), CommitGroupDispatchReason::Barrier))
            }
        }
    }
}

fn drain_all(mut pending: VecDeque<Tok>) -> Vec<TokUnit> {
    let mut units = Vec::new();
    while let Some((unit, _)) = form_next_token(&mut pending) {
        units.push(unit);
    }
    units
}

fn flatten(units: &[TokUnit]) -> Vec<Tok> {
    let mut out = Vec::new();
    for u in units {
        match u {
            TokUnit::CommandGroup(ids) => {
                for id in ids {
                    out.push(Tok::C(*id));
                }
            }
            TokUnit::AuditGroup(ids) => {
                for id in ids {
                    out.push(Tok::A(*id));
                }
            }
            TokUnit::Single(t) => out.push(t.clone()),
            TokUnit::Shutdown => out.push(Tok::S),
        }
    }
    out
}

/// Observations are hoisted behind their enclosing command run.
fn normalize_for_obs_hoist(seq: &[Tok]) -> Vec<Tok> {
    // Apply form_next_token drain and flatten — the kernel output order.
    drain_all(seq.iter().cloned().collect())
        .into_iter()
        .flat_map(|u| match u {
            TokUnit::CommandGroup(ids) => ids.into_iter().map(Tok::C).collect::<Vec<_>>(),
            TokUnit::AuditGroup(ids) => ids.into_iter().map(Tok::A).collect(),
            TokUnit::Single(t) => vec![t],
            TokUnit::Shutdown => vec![Tok::S],
        })
        .collect()
}

#[test]
fn form_next_unit_preserves_order_and_loses_nothing() {
    // Deterministic adversarial sequences (proptest-style coverage without new deps).
    let seeds: Vec<Vec<Tok>> = vec![
        vec![],
        vec![Tok::C(1)],
        vec![Tok::A(1), Tok::A(2)],
        vec![
            Tok::C(1),
            Tok::C(2),
            Tok::A(1),
            Tok::A(2),
            Tok::C(3),
            Tok::C(4),
        ],
        vec![Tok::C(1), Tok::O(1), Tok::C(2)],
        vec![Tok::C(1), Tok::A(9), Tok::C(2)],
        vec![Tok::A(1), Tok::A(1)],
        vec![Tok::S],
        vec![Tok::C(1), Tok::S, Tok::C(2)],
        vec![Tok::O(1), Tok::O(2)],
        vec![Tok::R, Tok::C(1)],
        (1..=70u8).map(Tok::C).collect(),
        {
            let mut v = Vec::new();
            for i in 0..20u8 {
                v.push(Tok::C(i));
                if i % 3 == 0 {
                    v.push(Tok::O(i));
                }
                if i % 5 == 0 {
                    v.push(Tok::A(i));
                }
            }
            v
        },
    ];
    for seq in seeds {
        let original_len = seq.len();
        let units = drain_all(seq.iter().cloned().collect());
        let flat = flatten(&units);
        assert_eq!(
            flat.len(),
            original_len,
            "token count must be preserved: {seq:?} -> {units:?}"
        );
        // Order equals original except DeferrableObservation hoisting behind the
        // enclosing command run (observations that sat between commands move after).
        let expected = normalize_for_obs_hoist(&seq);
        assert_eq!(flat, expected, "order mismatch for {seq:?}");
    }
}

#[test]
fn command_audit_interleaving_forms_three_ordered_units() {
    let units = drain_all(
        [
            Tok::C(1),
            Tok::C(2),
            Tok::A(1),
            Tok::A(2),
            Tok::C(3),
            Tok::C(4),
        ]
        .into_iter()
        .collect(),
    );
    assert_eq!(
        units,
        vec![
            TokUnit::CommandGroup(vec![1, 2]),
            TokUnit::AuditGroup(vec![1, 2]),
            TokUnit::CommandGroup(vec![3, 4]),
        ]
    );
}

#[test]
fn duplicate_request_id_audits_form_separate_units() {
    let units = drain_all([Tok::A(1), Tok::A(1)].into_iter().collect());
    assert_eq!(
        units,
        vec![TokUnit::AuditGroup(vec![1]), TokUnit::AuditGroup(vec![1])]
    );
}

#[test]
fn sixty_five_commands_form_full_then_queue_drained() {
    let seq: VecDeque<_> = (0..65u8).map(Tok::C).collect();
    let mut pending = seq;
    let (u1, r1) = form_next_token(&mut pending).unwrap();
    assert_eq!(r1, CommitGroupDispatchReason::Full);
    assert!(matches!(u1, TokUnit::CommandGroup(ref g) if g.len() == 64));
    let (u2, r2) = form_next_token(&mut pending).unwrap();
    assert_eq!(r2, CommitGroupDispatchReason::QueueDrained);
    assert!(matches!(u2, TokUnit::CommandGroup(ref g) if g.len() == 1));
}

#[test]
fn barrier_is_never_overtaken_by_later_commands() {
    // [C1, A2, C3] → three units in order. Fails if commands after barrier fold in.
    let units = drain_all([Tok::C(1), Tok::A(2), Tok::C(3)].into_iter().collect());
    assert_eq!(
        units,
        vec![
            TokUnit::CommandGroup(vec![1]),
            TokUnit::AuditGroup(vec![2]),
            TokUnit::CommandGroup(vec![3]),
        ]
    );
}

#[test]
fn deferrable_observation_does_not_break_a_command_run() {
    let units = drain_all([Tok::C(1), Tok::O(1), Tok::C(2)].into_iter().collect());
    assert_eq!(
        units,
        vec![
            TokUnit::CommandGroup(vec![1, 2]),
            TokUnit::Single(Tok::O(1)),
        ]
    );
}

#[test]
fn shutdown_mid_run_yields_shutdown_after_the_current_command_unit() {
    let units = drain_all(
        [Tok::C(1), Tok::C(2), Tok::S, Tok::C(3)]
            .into_iter()
            .collect(),
    );
    assert_eq!(
        units,
        vec![
            TokUnit::CommandGroup(vec![1, 2]),
            TokUnit::Shutdown,
            TokUnit::CommandGroup(vec![3]),
        ]
    );
}

#[test]
fn pending_holding_only_deferrable_observations_forms_a_single_unit() {
    let units = drain_all([Tok::O(1), Tok::O(2)].into_iter().collect());
    assert_eq!(
        units,
        vec![TokUnit::Single(Tok::O(1)), TokUnit::Single(Tok::O(2)),]
    );
}

/// Real `form_next_unit` audit path: [A,A] distinct → one AuditGroup.
#[test]
fn real_form_next_unit_groups_distinct_audits() {
    let mut pending = VecDeque::from([audit_msg(1), audit_msg(2), audit_msg(1)]);
    let (u1, r1) = form_next_unit(&mut pending).expect("unit");
    assert!(matches!(u1, WorkUnit::AuditGroup(ref g) if g.len() == 2));
    assert_eq!(r1, CommitGroupDispatchReason::Full); // duplicate boundary
    let (u2, _) = form_next_unit(&mut pending).expect("second");
    assert!(matches!(u2, WorkUnit::AuditGroup(ref g) if g.len() == 1));
}
