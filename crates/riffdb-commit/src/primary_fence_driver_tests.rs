// req: REP-005, SEC-001, STO-012
use super::*;
use riffdb_policy::{CurrentAuthorizer, NoopAuthorizationTelemetry, PrimaryFenceDecision};
use riffdb_storage_api::{
    ChangelogHistoryPointV3, ChangelogTransactionSequence, PrimaryFenceAwaitingDecision,
    PrimaryFenceCandidateTransaction, PrimaryFenceCandidateV1, PrimaryFenceIntentV1,
    PrimaryFenceRefusalV1, PrimaryFenceRequestV1, PrimaryFenceResultV1,
    PrimaryFenceTransactionPort, StoredCapabilityRecordV1, StoredPrimaryFenceAdministrationV1,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityGrantV1, CapabilityPermissionV1,
    CapabilityPermissionsV1, CommitSequence, DatabaseId, DigestKeyId, DualFrontier, Environment,
    LeadershipEpochV1, PartitionScopeV1, ReplicationFenceOperationId,
    ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1, TenantScope,
};
use std::num::NonZeroU16;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

fn time(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).unwrap()
}
fn database() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).unwrap()
}
pub(crate) fn request(seed: u8) -> PrimaryFenceRequestV1 {
    PrimaryFenceRequestV1::new(
        RequestId::from_unix_milliseconds_and_random(2, [seed; 10]).unwrap(),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(3, [3; 10]).unwrap(),
        ReplicationFollowerAuditTargetV1::new(
            database(),
            1,
            LeadershipEpochV1::initial(),
            ReplicationSourceHoldIdV1::new([4; 16]).unwrap(),
        )
        .unwrap(),
        ChangelogTransactionSequence::new(4).unwrap(),
    )
}
fn grant(permission: CapabilityPermissionKindV1, approval: bool) -> CapabilityGrantV1 {
    CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(permission).unwrap(),
        ])
        .unwrap(),
        vec![],
        NonZeroU16::MIN,
        if approval { vec![permission] } else { vec![] },
    )
    .unwrap()
}
pub(crate) fn fixture() -> AuthorizationFixture {
    AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database(),
        Environment::new("test").unwrap(),
        ActorId::new("fence-operator-secret").unwrap(),
        ActorKind::Human,
        Audience::new("grpc").unwrap(),
        AuthorizationFixtureTimes::new(time(100), time(1000), time(150)),
        grant(CapabilityPermissionKindV1::FenceReplicationPrimary, false),
    ))
    .unwrap()
}
struct InitialClock;
impl AuthorizationClock for InitialClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(time(150))
    }
}
pub(crate) fn preparation(
    f: &AuthorizationFixture,
    request: PrimaryFenceRequestV1,
) -> riffdb_policy::AuthorizedPrimaryFencePreparation {
    let resolver = f.current_capability_resolver();
    let PrimaryFenceDecision::Allow(prepared) = CurrentAuthorizer::new(
        &resolver,
        &InitialClock,
        &NoopAuthorizationTelemetry,
        database(),
        Environment::new("test").unwrap(),
    )
    .authorize_primary_fence(f.authenticated_principal(), request)
    .unwrap() else {
        panic!("initial fence authority");
    };
    *prepared
}
fn current(f: &AuthorizationFixture, grant: CapabilityGrantV1) -> StoredCapabilityRecordV1 {
    let p = f.authenticated_principal();
    StoredCapabilityRecordV1::from_stored_parts(
        p.capability_id(),
        p.capability_revision(),
        CapabilityTokenDigest::from_hmac_bytes(DigestKeyId::new(1).unwrap(), [7; 32]),
        database(),
        Environment::new("test").unwrap(),
        p.principal_id().clone(),
        p.actor_kind(),
        vec![Audience::new("grpc").unwrap()],
        time(100),
        time(1000),
        AdministrationSequence::first(),
        request(5).request_id(),
        grant,
        CapabilityLifecycleV1::Active,
    )
    .unwrap()
}
fn record(
    candidate: &PrimaryFenceCandidateV1,
    timestamp: Timestamp,
) -> StoredPrimaryFenceAdministrationV1 {
    let request = candidate.request();
    StoredPrimaryFenceAdministrationV1::new(
        AdministrationSequence::new(3).unwrap(),
        timestamp,
        request.operation_id(),
        request.request_id(),
        candidate.principal().clone(),
        None,
        request.target(),
        request.generation(),
        ChangelogHistoryPointV3::new(
            ChangelogTransactionSequence::new(9).unwrap(),
            [8; 32],
            DualFrontier::new(CommitSequence::new(5), AdministrationSequence::new(2)),
        ),
    )
    .unwrap()
}
#[derive(Clone)]
enum Outcome {
    Applied,
    Replayed(Box<StoredPrimaryFenceAdministrationV1>),
    Refused,
    Error(StorageErrorKind),
    Misbound,
}
struct Probe {
    held: AtomicBool,
    events: Mutex<Vec<&'static str>>,
    intents: Mutex<Vec<(PrimaryFenceCandidateV1, Timestamp)>>,
    current: Option<TransactionCurrentCapabilityObservationV1>,
    outcome: Outcome,
    open_error: Option<StorageErrorKind>,
}
impl Probe {
    fn new(current: Option<TransactionCurrentCapabilityObservationV1>, outcome: Outcome) -> Self {
        Self {
            held: AtomicBool::new(false),
            events: Mutex::new(vec![]),
            intents: Mutex::new(vec![]),
            current,
            outcome,
            open_error: None,
        }
    }
    fn event(&self, value: &'static str) {
        self.events.lock().unwrap().push(value);
    }
}
struct Repository(Arc<Probe>);
struct Held(Arc<Probe>);
impl Drop for Held {
    fn drop(&mut self) {
        assert!(self.0.held.swap(false, Ordering::AcqRel));
        self.0.event("release");
    }
}
struct Candidate {
    held: Held,
    candidate: PrimaryFenceCandidateV1,
}
struct Awaiting {
    held: Held,
    candidate: PrimaryFenceCandidateV1,
}
impl PrimaryFenceTransactionPort for Repository {
    type Candidate = Candidate;
    fn begin_primary_fence_transaction(
        &self,
        candidate: PrimaryFenceCandidateV1,
    ) -> Result<Candidate, StorageError> {
        self.0.event("open");
        if let Some(kind) = self.0.open_error {
            return Err(StorageError::new(kind, None));
        }
        assert!(!self.0.held.swap(true, Ordering::AcqRel));
        Ok(Candidate {
            held: Held(Arc::clone(&self.0)),
            candidate,
        })
    }
}
impl PrimaryFenceCandidateTransaction for Candidate {
    type Awaiting = Awaiting;
    fn read_transaction_current(
        self,
    ) -> Result<(Awaiting, Option<TransactionCurrentCapabilityObservationV1>), StorageError> {
        self.held.0.event("observe");
        let current = self.held.0.current.clone();
        Ok((
            Awaiting {
                held: self.held,
                candidate: self.candidate,
            },
            current,
        ))
    }
    fn abandon(self) {
        self.held.0.event("abandon");
    }
}
impl PrimaryFenceAwaitingDecision for Awaiting {
    fn commit(self, intent: PrimaryFenceIntentV1) -> Result<PrimaryFenceResultV1, StorageError> {
        assert_eq!(intent.candidate(), &self.candidate);
        assert!(self.held.0.held.load(Ordering::Acquire));
        self.held.0.event("commit");
        self.held
            .0
            .intents
            .lock()
            .unwrap()
            .push((intent.candidate().clone(), intent.timestamp()));
        match &self.held.0.outcome {
            Outcome::Applied => Ok(PrimaryFenceResultV1::Applied(Box::new(record(
                &self.candidate,
                intent.timestamp(),
            )))),
            Outcome::Replayed(record) => Ok(PrimaryFenceResultV1::Replayed(record.clone())),
            Outcome::Refused => Ok(PrimaryFenceResultV1::Refused(
                PrimaryFenceRefusalV1::FenceConflict,
            )),
            Outcome::Error(kind) => Err(StorageError::new(*kind, None)),
            Outcome::Misbound => {
                let mut bytes = [0x77; 16];
                bytes[6] = 0x70;
                bytes[8] = 0x80;
                let request = self.candidate.request();
                let changed = PrimaryFenceCandidateV1::new(
                    PrimaryFenceRequestV1::new(
                        request.request_id(),
                        ReplicationFenceOperationId::from_bytes(bytes).unwrap(),
                        request.target(),
                        request.generation(),
                    ),
                    self.candidate.principal().clone(),
                );
                Ok(PrimaryFenceResultV1::Applied(Box::new(record(
                    &changed,
                    intent.timestamp(),
                ))))
            }
        }
    }
    fn abandon(self) {
        self.held.0.event("abandon");
    }
}
struct FinalClock {
    probe: Arc<Probe>,
    result: Result<Timestamp, AuthorizationClockError>,
}
impl AuthorizationClock for FinalClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        assert!(self.probe.held.load(Ordering::Acquire));
        assert_eq!(self.probe.events.lock().unwrap().last(), Some(&"observe"));
        self.probe.event("clock");
        self.result
    }
}
#[derive(Default)]
struct Lifecycle {
    fences: AtomicUsize,
    stops: AtomicUsize,
}
impl CommandExecutionLifecycle for Lifecycle {
    fn fence(&self) {
        self.fences.fetch_add(1, Ordering::Relaxed);
    }
    fn stop(&self) {
        self.stops.fetch_add(1, Ordering::Relaxed);
    }
}
fn observation(f: &AuthorizationFixture) -> TransactionCurrentCapabilityObservationV1 {
    TransactionCurrentCapabilityObservationV1::from_record(&current(
        f,
        grant(CapabilityPermissionKindV1::FenceReplicationPrimary, false),
    ))
}

#[test]
fn primary_fence_driver_samples_final_time_under_current_writer_and_links_exact_receipt() {
    let f = fixture();
    let p = f.authenticated_principal();
    let original = record(
        &PrimaryFenceCandidateV1::new(
            request(6),
            AuditPrincipalV1::new(
                p.principal_id().clone(),
                p.actor_kind(),
                p.capability_id(),
                p.capability_revision(),
            ),
        ),
        time(175),
    );
    for outcome in [
        Outcome::Applied,
        Outcome::Replayed(Box::new(original.clone())),
        Outcome::Refused,
    ] {
        let probe = Arc::new(Probe::new(Some(observation(&f)), outcome.clone()));
        let lifecycle = Lifecycle::default();
        let result = drive_primary_fence(
            &Repository(Arc::clone(&probe)),
            &FinalClock {
                probe: Arc::clone(&probe),
                result: Ok(time(200)),
            },
            &lifecycle,
            preparation(&f, request(7)),
        )
        .unwrap();
        assert_eq!(
            *probe.events.lock().unwrap(),
            ["open", "observe", "clock", "commit", "release"]
        );
        assert_eq!(probe.intents.lock().unwrap()[0].1, time(200));
        assert_eq!(probe.intents.lock().unwrap()[0].0.request(), request(7));
        assert!(!probe.held.load(Ordering::Acquire));
        assert_eq!(lifecycle.fences.load(Ordering::Relaxed), 0);
        assert_eq!(lifecycle.stops.load(Ordering::Relaxed), 0);
        match outcome {
            Outcome::Refused => {
                assert_eq!(result.terminal_audit(), ControlPlaneTerminalAudit::Failed)
            }
            _ => assert_eq!(
                result.terminal_audit(),
                ControlPlaneTerminalAudit::Succeeded(ServiceAuditLinkV1::ControlPlane {
                    administration_sequence: AdministrationSequence::new(3).unwrap()
                })
            ),
        }
        if let Outcome::Replayed(_) = outcome {
            assert_eq!(
                result.outcome(),
                &PrimaryFenceResultV1::Replayed(Box::new(original.clone()))
            );
        }
    }
}

#[test]
fn primary_fence_driver_reauthorizes_every_attempt_before_resolving_replay() {
    let f = fixture();
    for (case, replay) in (0..5).flat_map(|case| [false, true].map(move |replay| (case, replay))) {
        let prepared = preparation(&f, request(7));
        let capability = match case {
            1 => current(
                &f,
                grant(CapabilityPermissionKindV1::FenceReplicationPrimary, false),
            )
            .revoked(
                NonZeroU64::MIN,
                time(170),
                AdministrationSequence::new(2).unwrap(),
                riffdb_types::RevocationReasonCodeV1::Requested,
            )
            .unwrap(),
            2 => current(&f, grant(CapabilityPermissionKindV1::ReadHealth, false)),
            3 => current(
                &f,
                grant(CapabilityPermissionKindV1::FenceReplicationPrimary, true),
            ),
            _ => current(
                &f,
                grant(CapabilityPermissionKindV1::FenceReplicationPrimary, false),
            ),
        };
        let observed = (case != 0)
            .then(|| TransactionCurrentCapabilityObservationV1::from_record(&capability));
        let p = f.authenticated_principal();
        let outcome = if replay {
            Outcome::Replayed(Box::new(record(
                &PrimaryFenceCandidateV1::new(
                    request(6),
                    AuditPrincipalV1::new(
                        p.principal_id().clone(),
                        p.actor_kind(),
                        p.capability_id(),
                        p.capability_revision(),
                    ),
                ),
                time(175),
            )))
        } else {
            Outcome::Applied
        };
        let probe = Arc::new(Probe::new(observed, outcome));
        let lifecycle = Lifecycle::default();
        let error = drive_primary_fence(
            &Repository(Arc::clone(&probe)),
            &FinalClock {
                probe: Arc::clone(&probe),
                result: Ok(time(if case == 4 { 1000 } else { 200 })),
            },
            &lifecycle,
            prepared,
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            ControlPlaneExecutionErrorKind::AuthorizationDenied
        );
        let expected = match case {
            2 => PolicyCode::MissingPermission,
            3 => PolicyCode::ApprovalRequired,
            _ => PolicyCode::InactiveOrStaleCapability,
        };
        assert!(
            matches!(error.detail, ControlPlaneExecutionErrorDetail::Authorization(code) if code == expected)
        );
        assert!(probe.intents.lock().unwrap().is_empty());
        assert!(probe.events.lock().unwrap().contains(&"abandon"));
        assert!(!probe.held.load(Ordering::Acquire));
        assert_eq!(lifecycle.fences.load(Ordering::Relaxed), 0);
        assert_eq!(lifecycle.stops.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn primary_fence_driver_preserves_failure_and_uncertainty_classification() {
    let f = fixture();
    for (kind, expected, fences, stops) in [
        (
            StorageErrorKind::Unavailable,
            ControlPlaneExecutionErrorKind::StorageUnavailable,
            0,
            0,
        ),
        (
            StorageErrorKind::CommitStatusUnknown,
            ControlPlaneExecutionErrorKind::OutcomeUnknown,
            1,
            0,
        ),
        (
            StorageErrorKind::CorruptData,
            ControlPlaneExecutionErrorKind::InternalDefect,
            0,
            1,
        ),
    ] {
        for at_open in [false, true] {
            let mut probe = Probe::new(Some(observation(&f)), Outcome::Error(kind));
            if at_open {
                probe.open_error = Some(kind);
            }
            let probe = Arc::new(probe);
            let lifecycle = Lifecycle::default();
            let error = drive_primary_fence(
                &Repository(Arc::clone(&probe)),
                &FinalClock {
                    probe: Arc::clone(&probe),
                    result: Ok(time(200)),
                },
                &lifecycle,
                preparation(&f, request(7)),
            )
            .unwrap_err();
            assert_eq!(error.kind(), expected);
            assert_eq!(lifecycle.fences.load(Ordering::Relaxed), fences);
            assert_eq!(lifecycle.stops.load(Ordering::Relaxed), stops);
            assert!(!probe.held.load(Ordering::Acquire));
            if at_open {
                assert!(probe.intents.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn primary_fence_driver_clock_failure_abandons_and_misbound_result_stops() {
    let f = fixture();
    for bad_clock in [true, false] {
        let probe = Arc::new(Probe::new(Some(observation(&f)), Outcome::Misbound));
        let lifecycle = Lifecycle::default();
        let result = if bad_clock {
            Err(AuthorizationClockError)
        } else {
            Ok(time(200))
        };
        let error = drive_primary_fence(
            &Repository(Arc::clone(&probe)),
            &FinalClock {
                probe: Arc::clone(&probe),
                result,
            },
            &lifecycle,
            preparation(&f, request(7)),
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            if bad_clock {
                ControlPlaneExecutionErrorKind::StorageUnavailable
            } else {
                ControlPlaneExecutionErrorKind::InternalDefect
            }
        );
        assert_eq!(
            lifecycle.stops.load(Ordering::Relaxed),
            usize::from(!bad_clock)
        );
        assert_eq!(lifecycle.fences.load(Ordering::Relaxed), 0);
        assert_eq!(probe.intents.lock().unwrap().is_empty(), bad_clock);
        assert!(!probe.held.load(Ordering::Acquire));
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ActorOutcome {
    Applied,
    Replayed,
    Refused,
    Unavailable,
    Unknown,
    Denied,
    Corrupt,
}

pub(crate) fn drive_for_actor(
    f: &AuthorizationFixture,
    preparation: AuthorizedPrimaryFencePreparation,
    outcome: ActorOutcome,
    lifecycle: &dyn CommandExecutionLifecycle,
) -> Result<PrimaryFenceExecutionResult, ControlPlaneExecutionError> {
    let p = preparation.principal();
    let prior = record(
        &PrimaryFenceCandidateV1::new(
            request(6),
            AuditPrincipalV1::new(
                p.principal_id().clone(),
                p.actor_kind(),
                p.capability_id(),
                p.capability_revision(),
            ),
        ),
        time(175),
    );
    let selected = match outcome {
        ActorOutcome::Applied => Outcome::Applied,
        ActorOutcome::Replayed => Outcome::Replayed(Box::new(prior)),
        ActorOutcome::Refused | ActorOutcome::Denied => Outcome::Refused,
        ActorOutcome::Unavailable => Outcome::Error(StorageErrorKind::Unavailable),
        ActorOutcome::Unknown => Outcome::Error(StorageErrorKind::CommitStatusUnknown),
        ActorOutcome::Corrupt => Outcome::Error(StorageErrorKind::CorruptData),
    };
    let probe = Arc::new(Probe::new(
        if matches!(outcome, ActorOutcome::Denied) {
            None
        } else {
            Some(observation(f))
        },
        selected,
    ));
    let clock = FinalClock {
        probe: Arc::clone(&probe),
        result: Ok(time(200)),
    };
    drive_primary_fence(&Repository(probe), &clock, lifecycle, preparation)
}
