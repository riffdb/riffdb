#![forbid(unsafe_code)]
//! Real current-policy decisions across controlled source admission and frame waits.
// req: REP-003

use std::{
    future::Future,
    num::NonZeroU16,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use riffdb_auth::AuthenticatedPrincipal;
use riffdb_policy::{
    AuthorizationClock, AuthorizationClockError, AuthorizationError, CurrentAuthorizer, Decision,
    NoopAuthorizationTelemetry, OperationRequest, ReplicationDecision,
};
use riffdb_service::{
    CurrentPolicyPort, ReplicationApplication, ReplicationFailure, ReplicationFrame,
    ReplicationFuture, ReplicationItem, ReplicationItemSource, ReplicationPhase,
    ReplicationRequest, ReplicationSourceHead, ReplicationSourcePort, ReplicationStreamErrorV3,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityGrantV1, CapabilityPermissionKindV1,
    CapabilityPermissionV1, CapabilityPermissionsV1, DatabaseId, DualFrontier, Environment,
    PartitionScopeV1, TenantScope, Timestamp,
};
use tokio::sync::{mpsc, oneshot};

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).unwrap()
}

fn database(seed: u8) -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10]).unwrap()
}

fn environment() -> Environment {
    Environment::new("replication-test").unwrap()
}

struct Policy {
    fixture: AuthorizationFixture,
    now: AtomicI64,
    clock_unavailable: AtomicBool,
}

impl Policy {
    fn new(permission: CapabilityPermissionKindV1) -> Arc<Self> {
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![
                CapabilityPermissionV1::unparameterized(permission).unwrap(),
            ])
            .unwrap(),
            vec![],
            NonZeroU16::MIN,
            vec![],
        )
        .unwrap();
        Arc::new(Self {
            fixture: AuthorizationFixture::new(AuthorizationFixtureConfig::new(
                database(1),
                environment(),
                ActorId::new("replication-administrator").unwrap(),
                ActorKind::Service,
                Audience::new("grpc").unwrap(),
                AuthorizationFixtureTimes::new(timestamp(100), timestamp(200), timestamp(150)),
                grant,
            ))
            .unwrap(),
            now: AtomicI64::new(150),
            clock_unavailable: AtomicBool::new(false),
        })
    }

    fn principal(&self) -> AuthenticatedPrincipal {
        self.fixture.authenticated_principal().clone()
    }
}

impl AuthorizationClock for Policy {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        if self.clock_unavailable.load(Ordering::Acquire) {
            Err(AuthorizationClockError)
        } else {
            Ok(timestamp(self.now.load(Ordering::Acquire)))
        }
    }
}

impl CurrentPolicyPort for Policy {
    fn authorize(
        &self,
        _: &AuthenticatedPrincipal,
        _: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        panic!("replication must use its administrative policy decision")
    }

    fn authorize_replication(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<ReplicationDecision, AuthorizationError> {
        CurrentAuthorizer::new(
            &self.fixture.current_capability_resolver(),
            self,
            &NoopAuthorizationTelemetry,
            database(1),
            environment(),
        )
        .authorize_replication(principal)
    }
}

#[derive(Clone, Copy, Debug)]
enum Change {
    Revoke,
    Expire,
    Missing,
    Unavailable,
    ClockUnavailable,
}

impl Change {
    const ALL: [Self; 5] = [
        Self::Revoke,
        Self::Expire,
        Self::Missing,
        Self::Unavailable,
        Self::ClockUnavailable,
    ];

    fn apply(self, policy: &Policy) -> ReplicationFailure {
        match self {
            Self::Revoke => policy.fixture.revoke_current(timestamp(160)).unwrap(),
            Self::Expire => policy.now.store(200, Ordering::Release),
            Self::Missing => policy.fixture.make_current_missing().unwrap(),
            Self::Unavailable => policy.fixture.make_current_unavailable().unwrap(),
            Self::ClockUnavailable => policy.clock_unavailable.store(true, Ordering::Release),
        }
        match self {
            Self::Revoke | Self::Expire => ReplicationFailure::AuthorizationDenied,
            _ => ReplicationFailure::Unavailable,
        }
    }
}

type Frame = Result<Option<ReplicationItem>, ReplicationFailure>;

#[test]
// req: REP-006
fn lifecycle_policy_port_reloads_authority_and_default_implementations_refuse() {
    support::run_async(async move {
        use riffdb_policy::ReplicationAdministrationDecision;
        let request = riffdb_auth::ReplicationAdministrationRequestV1::register(
            riffdb_types::RequestId::from_unix_milliseconds_and_random(12, [12; 10]).unwrap(),
            riffdb_types::ReplicationFollowerAuditTargetV1::new(
                database(1),
                1,
                riffdb_types::LeadershipEpochV1::initial(),
                riffdb_types::ReplicationSourceHoldIdV1::new([9; 16]).unwrap(),
            )
            .unwrap(),
            riffdb_storage_api::FollowerHoldBudget::new(5).unwrap(),
            None,
        );
        for change in Change::ALL {
            let policy = Policy::new(CapabilityPermissionKindV1::AdministerCapabilities);
            assert!(
                matches!(
                    policy.authorize_replication_administration(&policy.principal(), request),
                    Err(AuthorizationError::CurrentCapabilityUnavailable)
                ),
                "a port without the dedicated implementation must refuse"
            );
            let resolver = policy.fixture.current_capability_resolver();
            let authorizer = CurrentAuthorizer::new(
                &resolver,
                policy.as_ref(),
                &NoopAuthorizationTelemetry,
                database(1),
                environment(),
            );
            let check = || {
                CurrentPolicyPort::authorize_replication_administration(
                    &authorizer,
                    &policy.principal(),
                    request,
                )
            };
            let ReplicationAdministrationDecision::Allow(proof) = check().unwrap() else {
                panic!("current global administrator must pass");
            };
            assert_eq!(proof.request(), request);
            assert_eq!(proof.authorized_at(), timestamp(150));
            match change.apply(&policy) {
                ReplicationFailure::AuthorizationDenied => assert!(matches!(
                    check(),
                    Ok(ReplicationAdministrationDecision::Deny(_))
                )),
                ReplicationFailure::Unavailable => assert!(check().is_err()),
                _ => panic!("fixture has only policy changes"),
            }
        }
        let stream_only = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
        let resolver = stream_only.fixture.current_capability_resolver();
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            stream_only.as_ref(),
            &NoopAuthorizationTelemetry,
            database(1),
            environment(),
        );
        assert!(matches!(
            CurrentPolicyPort::authorize_replication_administration(
                &authorizer,
                &stream_only.principal(),
                request
            ),
            Ok(ReplicationAdministrationDecision::Deny(_))
        ));
    });
}

struct Frames {
    panic_next: Arc<AtomicBool>,
    panic_drop: Arc<AtomicBool>,
    dropped: Arc<tokio::sync::Notify>,
    receiver: mpsc::Receiver<Frame>,
    reads: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl ReplicationItemSource for Frames {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            assert!(
                !self.panic_next.load(Ordering::Acquire),
                "injected frame panic"
            );
            self.receiver.recv().await.unwrap()
        })
    }
}

impl Drop for Frames {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
        self.dropped.notify_one();
        assert!(
            !self.panic_drop.load(Ordering::Acquire),
            "injected source drop panic"
        );
    }
}

struct Source {
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    frames: Mutex<Option<Frames>>,
    opens: AtomicUsize,
    opened: tokio::sync::Notify,
    dropped: Arc<tokio::sync::Notify>,
    failure: Mutex<Option<ReplicationFailure>>,
    panic_after_open: AtomicBool,
    panic_next: Arc<AtomicBool>,
    panic_drop: Arc<AtomicBool>,
}

impl ReplicationSourcePort for Source {
    fn open(&self, _: ReplicationRequest) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        self.opens.fetch_add(1, Ordering::AcqRel);
        self.opened.notify_one();
        let gate = self.gate.lock().unwrap().take();
        let frames = self.frames.lock().unwrap().take().unwrap();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.await.unwrap();
            }
            assert!(
                !self.panic_after_open.load(Ordering::Acquire),
                "injected source panic"
            );
            if let Some(error) = *self.failure.lock().unwrap() {
                return Err(error);
            }
            Ok(Box::new(frames) as Box<dyn ReplicationItemSource>)
        })
    }
}

fn source() -> (
    Arc<Source>,
    mpsc::Sender<Frame>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let (sender, receiver) = mpsc::channel(1);
    let reads = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(tokio::sync::Notify::new());
    let panic_next = Arc::new(AtomicBool::new(false));
    let panic_drop = Arc::new(AtomicBool::new(false));
    let source = Arc::new(Source {
        dropped: dropped.clone(),
        failure: Mutex::new(None),
        panic_after_open: AtomicBool::new(false),
        panic_next: panic_next.clone(),
        panic_drop: panic_drop.clone(),
        gate: Mutex::new(None),
        frames: Mutex::new(Some(Frames {
            panic_next,
            panic_drop,
            dropped,
            receiver,
            reads: Arc::clone(&reads),
            drops: Arc::clone(&drops),
        })),
        opens: AtomicUsize::new(0),
        opened: tokio::sync::Notify::new(),
    });
    (source, sender, reads, drops)
}

fn request() -> ReplicationRequest {
    ReplicationRequest {
        database_id: database(1),
        history_incarnation: 1,
        leadership_epoch: 1,
        after_sequence: 1,
        after_hash: [0x51; 32],
        after_frontier: DualFrontier::INITIAL,
        readable_format: "riffdb-changelog-v3".into(),
        catalog_digest: [0x52; 32],
        maximum_frame_bytes: 32 * 1024 * 1024,
        maximum_transitions: 256,
        phase: ReplicationPhase::Tail,
    }
}

fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn ready<F: Future>(future: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

#[test]
fn replication_rechecks_real_authority_after_source_admission_wait() {
    support::run_async(async move {
        for change in Change::ALL {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, _sender, reads, drops) = source();
            let (release, gate) = oneshot::channel();
            *source.gate.lock().unwrap() = Some(gate);
            let service = ReplicationService::new(policy.clone(), source.clone());
            let mut admission = service.stream_changelog(policy.principal(), request());
            assert!(poll(admission.as_mut()).is_pending());
            ready(async {
                tokio::time::timeout(std::time::Duration::from_secs(30), source.opened.notified())
                    .await
                    .unwrap()
            });
            let expected = change.apply(&policy);
            release.send(()).unwrap();
            assert!(
                matches!(ready(admission), Err(actual) if actual == expected),
                "{change:?}"
            );
            assert_eq!(source.opens.load(Ordering::Acquire), 1);
            assert_eq!(reads.load(Ordering::Acquire), 0);
            assert_eq!(drops.load(Ordering::Acquire), 1);
        }
    });
}

#[test]
// req: REP-004
fn replication_rechecks_real_authority_after_wait_and_never_releases_the_withheld_frame() {
    support::run_async(async move {
        for change in Change::ALL {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, sender, reads, drops) = source();
            let service = ReplicationService::new(policy.clone(), source);
            let mut subscription =
                ready(service.stream_changelog(policy.principal(), request())).unwrap();
            // First release proves this principal can receive bytes before the change.
            let observed_frame = ReplicationItem::Frame(ReplicationFrame::new(
                vec![1, 2, 3],
                Some(ReplicationSourceHead::new(9, riffdb_types::DualFrontier::INITIAL).unwrap()),
            ));
            sender.try_send(Ok(Some(observed_frame.clone()))).unwrap();
            assert_eq!(
                ready(subscription.next_item()),
                Ok(Some(observed_frame.clone()))
            );
            let mut pending = Box::pin(subscription.next_item());
            assert!(poll(pending.as_mut()).is_pending());
            let expected = change.apply(&policy);
            sender.try_send(Ok(Some(observed_frame))).unwrap();
            assert_eq!(ready(pending), Err(expected), "{change:?}");
            policy.fixture.make_current_available().unwrap();
            policy.now.store(150, Ordering::Release);
            policy.clock_unavailable.store(false, Ordering::Release);
            assert_eq!(ready(subscription.next_item()), Ok(None));
            assert_eq!(reads.load(Ordering::Acquire), 2);
            assert_eq!(
                service.harness.audit_submission_count(),
                2,
                "continuations do not audit again"
            );
            drop(subscription);
            assert_eq!(drops.load(Ordering::Acquire), 1);
        }
    });
}

#[test]
fn replication_denies_missing_permission_or_database_mismatch_before_source_access() {
    support::run_async(async move {
        for (permission, requested_database) in [
            (
                CapabilityPermissionKindV1::AdministerCapabilities,
                database(1),
            ),
            (CapabilityPermissionKindV1::ReplicateChangelog, database(2)),
        ] {
            let policy = Policy::new(permission);
            let (source, _sender, reads, _drops) = source();
            let service = ReplicationService::new(policy.clone(), source.clone());
            let mut request = request();
            request.database_id = requested_database;
            assert!(matches!(
                ready(service.stream_changelog(policy.principal(), request)),
                Err(ReplicationFailure::AuthorizationDenied)
            ));
            assert_eq!(source.opens.load(Ordering::Acquire), 0);
            assert_eq!(reads.load(Ordering::Acquire), 0);
        }
    });
}

#[test]
fn replication_cancelled_frame_wait_cannot_resume_an_uncertain_source() {
    support::run_async(async move {
        let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
        let (source, _sender, reads, drops) = source();
        let service = ReplicationService::new(policy.clone(), source);
        let mut subscription =
            ready(service.stream_changelog(policy.principal(), request())).unwrap();
        let mut pending = Box::pin(subscription.next_item());
        assert!(poll(pending.as_mut()).is_pending());
        drop(pending);
        assert_eq!(
            drops.load(Ordering::Acquire),
            1,
            "cancelled pull releases its source immediately"
        );
        assert_eq!(ready(subscription.next_item()), Ok(None));
        assert_eq!(reads.load(Ordering::Acquire), 1);
        drop(subscription);
        assert_eq!(drops.load(Ordering::Acquire), 1);
    });
}

#[test]
fn replication_source_failure_and_invalid_frame_permanently_end_the_subscription() {
    support::run_async(async move {
        for (frame, expected) in [
            (
                Err(ReplicationFailure::Source(
                    ReplicationStreamErrorV3::HistoryPruned,
                )),
                ReplicationFailure::Source(ReplicationStreamErrorV3::HistoryPruned),
            ),
            (
                Ok(Some(ReplicationItem::Frame(vec![].into()))),
                ReplicationFailure::Source(ReplicationStreamErrorV3::CorruptHistory),
            ),
            (
                Ok(Some(ReplicationItem::Frame(
                    vec![0; 32 * 1024 * 1024 + 1].into(),
                ))),
                ReplicationFailure::Source(ReplicationStreamErrorV3::CorruptHistory),
            ),
        ] {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, sender, reads, _drops) = source();
            let service = ReplicationService::new(policy.clone(), source);
            let mut subscription =
                ready(service.stream_changelog(policy.principal(), request())).unwrap();
            sender.try_send(frame).unwrap();
            assert_eq!(ready(subscription.next_item()), Err(expected));
            assert_eq!(ready(subscription.next_item()), Ok(None));
            assert_eq!(reads.load(Ordering::Acquire), 1);
        }
    });
}

fn bootstrap_request() -> ReplicationRequest {
    let mut request = request();
    request.phase = ReplicationPhase::Bootstrap {
        hold_id: [1; 16],
        resume_manifest: vec![],
        after_page: 0,
    };
    request.after_sequence = 0;
    request.after_hash = [0; 32];
    request.after_frontier = DualFrontier::INITIAL;
    request
}

#[test]
fn bootstrap_manifest_and_pages_recheck_current_authority_after_every_wait() {
    support::run_async(async move {
        for change in Change::ALL {
            for page in [false, true] {
                let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
                let (source, sender, reads, drops) = source();
                let service = ReplicationService::new(policy.clone(), source);
                let mut subscription =
                    ready(service.stream_changelog(policy.principal(), bootstrap_request()))
                        .unwrap();
                if page {
                    sender
                        .try_send(Ok(Some(ReplicationItem::BootstrapManifest(vec![1]))))
                        .unwrap();
                    assert_eq!(
                        ready(subscription.next_item()),
                        Ok(Some(ReplicationItem::BootstrapManifest(vec![1])))
                    );
                }
                let mut pending = Box::pin(subscription.next_item());
                assert!(poll(pending.as_mut()).is_pending());
                let expected = change.apply(&policy);
                let item = if page {
                    ReplicationItem::BootstrapPage(vec![2])
                } else {
                    ReplicationItem::BootstrapManifest(vec![1])
                };
                sender.try_send(Ok(Some(item))).unwrap();
                assert_eq!(ready(pending), Err(expected));
                assert_eq!(ready(subscription.next_item()), Ok(None));
                assert_eq!(reads.load(Ordering::Acquire), if page { 2 } else { 1 });
                drop(subscription);
                assert_eq!(drops.load(Ordering::Acquire), 1);
            }
        }
    });
}

#[test]
fn bootstrap_rejects_wrong_phase_items_duplicate_manifest_and_oversized_pages() {
    support::run_async(async move {
        for (requested, prefix, item) in [
            (request(), None, ReplicationItem::BootstrapManifest(vec![1])),
            (
                bootstrap_request(),
                None,
                ReplicationItem::BootstrapPage(vec![2]),
            ),
            (
                bootstrap_request(),
                None,
                ReplicationItem::Frame(vec![2].into()),
            ),
            (
                bootstrap_request(),
                None,
                ReplicationItem::BootstrapManifest(vec![1; 513]),
            ),
            (
                bootstrap_request(),
                Some(ReplicationItem::BootstrapManifest(vec![1])),
                ReplicationItem::BootstrapManifest(vec![1]),
            ),
            (
                bootstrap_request(),
                Some(ReplicationItem::BootstrapManifest(vec![1])),
                ReplicationItem::Frame(vec![2].into()),
            ),
            (
                bootstrap_request(),
                Some(ReplicationItem::BootstrapManifest(vec![1])),
                ReplicationItem::BootstrapPage(vec![2; 32 * 1024 * 1024 + 513]),
            ),
        ] {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, sender, _, _) = source();
            let service = ReplicationService::new(policy.clone(), source);
            let mut subscription =
                ready(service.stream_changelog(policy.principal(), requested)).unwrap();
            if let Some(prefix) = prefix {
                sender.try_send(Ok(Some(prefix.clone()))).unwrap();
                assert_eq!(ready(subscription.next_item()), Ok(Some(prefix)));
            }
            sender.try_send(Ok(Some(item))).unwrap();
            assert_eq!(
                ready(subscription.next_item()),
                Err(ReplicationFailure::Source(
                    ReplicationStreamErrorV3::CorruptHistory
                ))
            );
            assert_eq!(ready(subscription.next_item()), Ok(None));
        }
    });
}

#[test]
fn bootstrap_attachment_and_follower_authority_is_checked_before_source_access_and_after_admission()
{
    support::run_async(async move {
        let attachment = request_selection::attachment_request();
        let mut follower = request();
        follower.phase = ReplicationPhase::Follower { hold_id: [1; 16] };
        for requested in [bootstrap_request(), attachment, follower] {
            let policy = Policy::new(CapabilityPermissionKindV1::AdministerCapabilities);
            let (source, _, reads, _) = source();
            let service = ReplicationService::new(policy.clone(), source.clone());
            assert!(matches!(
                ready(service.stream_changelog(policy.principal(), requested.clone())),
                Err(ReplicationFailure::AuthorizationDenied)
            ));
            assert_eq!(source.opens.load(Ordering::Acquire), 0);
            assert_eq!(reads.load(Ordering::Acquire), 0);
            for change in Change::ALL {
                let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
                let (source, _, reads, drops) = self::source();
                let (release, gate) = oneshot::channel();
                *source.gate.lock().unwrap() = Some(gate);
                let service = ReplicationService::new(policy.clone(), source.clone());
                let mut admission = service.stream_changelog(policy.principal(), requested.clone());
                assert!(poll(admission.as_mut()).is_pending());
                ready(async {
                    tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        source.opened.notified(),
                    )
                    .await
                    .unwrap()
                });
                let expected = change.apply(&policy);
                release.send(()).unwrap();
                assert!(matches!(ready(admission), Err(actual) if actual == expected));
                assert_eq!(source.opens.load(Ordering::Acquire), 1);
                assert_eq!(reads.load(Ordering::Acquire), 0);
                assert_eq!(drops.load(Ordering::Acquire), 1);
            }
        }
    });
}

#[path = "replication_authorization/fence_evidence.rs"]
mod fence_evidence;

#[test]
// req: REP-003, REP-005
fn malformed_attachment_cannot_reach_source_before_audit_target_selection() {
    support::run_async(async move {
        let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
        let (source, _sender, reads, _drops) = source();
        let service = ReplicationService::new(policy.clone(), source.clone());
        let mut request = request();
        request.phase = ReplicationPhase::Attach {
            manifest: vec![1; 512],
        };
        assert!(matches!(
            ready(service.stream_changelog(policy.principal(), request)),
            Err(ReplicationFailure::Source(
                ReplicationStreamErrorV3::InvalidPosition
            ))
        ));
        assert_eq!(source.opens.load(Ordering::Acquire), 0);
        assert_eq!(reads.load(Ordering::Acquire), 0);
    });
}

#[path = "replication_authorization/request_selection.rs"]
mod request_selection;

#[test]
// req: REP-003, REP-005
fn replication_establishment_persists_started_and_succeeded_before_returning_handle() {
    support::run_async(async {
        let mut harness =
            support::ServiceHarness::new(support::ReadCommitMode::ImmediateNotFound, false);
        let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
        let owner = harness.replication_owner(database(1), environment(), policy.clone());
        let (source, _sender, _, _) = source();
        let service = riffdb_service::ReplicationService::new(owner, source);
        let (control, _cancel) = riffdb_service::RequestControl::new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        );
        let context = riffdb_service::RequestContext::from_authenticated_grpc(
            support::request_id(41),
            policy.principal(),
            control,
            None,
        );
        let _stream = service.stream_changelog(context, request()).await.unwrap();
        assert_eq!(harness.audit_submission_count(), 2);
        harness.stop_coordinator();
        assert_eq!(
            harness.audit_phases(41),
            vec![
                riffdb_types::ServiceAuditPhaseV1::Started,
                riffdb_types::ServiceAuditPhaseV1::Succeeded,
            ]
        );
    });
}

#[path = "../../../tests/service/support/mod.rs"]
mod support;

struct ReplicationService {
    next_request: AtomicUsize,
    service: riffdb_service::ReplicationService,
    harness: support::ServiceHarness,
}
impl ReplicationService {
    fn new(policy: Arc<Policy>, source: Arc<dyn ReplicationSourcePort>) -> Self {
        let harness =
            support::ServiceHarness::new(support::ReadCommitMode::ImmediateNotFound, false);
        let owner = harness.replication_owner(database(1), environment(), policy);
        Self {
            service: riffdb_service::ReplicationService::new(owner, source),
            next_request: AtomicUsize::new(42),
            harness,
        }
    }
    fn stream_changelog(
        &self,
        principal: AuthenticatedPrincipal,
        request: ReplicationRequest,
    ) -> ReplicationFuture<'_, riffdb_service::ReplicationSubscription> {
        self.service
            .stream_changelog(self.context(principal), request)
    }
    fn context(&self, principal: AuthenticatedPrincipal) -> riffdb_service::RequestContext {
        let (control, _cancel) = riffdb_service::RequestControl::new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        );
        riffdb_service::RequestContext::from_authenticated_grpc(
            support::request_id(
                u8::try_from(self.next_request.fetch_add(1, Ordering::Relaxed)).unwrap(),
            ),
            principal,
            control,
            None,
        )
    }
    fn primary_fence_source_evidence(
        &self,
        principal: AuthenticatedPrincipal,
        request: riffdb_auth::PrimaryFenceRequestV1,
        applied: riffdb_auth::ChangelogHistoryPointV3,
    ) -> ReplicationFuture<'_, riffdb_auth::PrimaryFenceSourceEvidenceV1> {
        self.service
            .primary_fence_source_evidence(self.context(principal), request, applied)
    }
}

#[path = "replication_authorization/audit.rs"]
mod audit_tests;
