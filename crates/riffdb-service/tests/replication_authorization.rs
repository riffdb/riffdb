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
    ReplicationRequest, ReplicationService, ReplicationSourceHead, ReplicationSourcePort,
    ReplicationStreamErrorV3,
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

struct Frames {
    receiver: mpsc::Receiver<Frame>,
    reads: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl ReplicationItemSource for Frames {
    fn next_item(&mut self) -> ReplicationFuture<'_, Option<ReplicationItem>> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move { self.receiver.recv().await.unwrap() })
    }
}

impl Drop for Frames {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}

struct Source {
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    frames: Mutex<Option<Frames>>,
    opens: AtomicUsize,
}

impl ReplicationSourcePort for Source {
    fn open(&self, _: ReplicationRequest) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        self.opens.fetch_add(1, Ordering::AcqRel);
        let gate = self.gate.lock().unwrap().take();
        let frames = self.frames.lock().unwrap().take().unwrap();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.await.unwrap();
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
    let source = Arc::new(Source {
        gate: Mutex::new(None),
        frames: Mutex::new(Some(Frames {
            receiver,
            reads: Arc::clone(&reads),
            drops: Arc::clone(&drops),
        })),
        opens: AtomicUsize::new(0),
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
    match poll(std::pin::pin!(future)) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("explicitly released operation must be ready"),
    }
}

#[test]
fn replication_rechecks_real_authority_after_source_admission_wait() {
    for change in Change::ALL {
        let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
        let (source, _sender, reads, drops) = source();
        let (release, gate) = oneshot::channel();
        *source.gate.lock().unwrap() = Some(gate);
        let service = ReplicationService::new(policy.clone(), source.clone());
        let mut admission = service.stream_changelog(policy.principal(), request());
        assert!(poll(admission.as_mut()).is_pending());
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
}

#[test]
// req: REP-004
fn replication_rechecks_real_authority_after_wait_and_never_releases_the_withheld_frame() {
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
        drop(subscription);
        assert_eq!(drops.load(Ordering::Acquire), 1);
    }
}

#[test]
fn replication_denies_missing_permission_or_database_mismatch_before_source_access() {
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
}

#[test]
fn replication_cancelled_frame_wait_cannot_resume_an_uncertain_source() {
    let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
    let (source, _sender, reads, drops) = source();
    let service = ReplicationService::new(policy.clone(), source);
    let mut subscription = ready(service.stream_changelog(policy.principal(), request())).unwrap();
    let mut pending = Box::pin(subscription.next_item());
    assert!(poll(pending.as_mut()).is_pending());
    drop(pending);
    assert_eq!(ready(subscription.next_item()), Ok(None));
    assert_eq!(reads.load(Ordering::Acquire), 1);
    drop(subscription);
    assert_eq!(drops.load(Ordering::Acquire), 1);
}

#[test]
fn replication_source_failure_and_invalid_frame_permanently_end_the_subscription() {
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
    for change in Change::ALL {
        for page in [false, true] {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, sender, reads, drops) = source();
            let service = ReplicationService::new(policy.clone(), source);
            let mut subscription =
                ready(service.stream_changelog(policy.principal(), bootstrap_request())).unwrap();
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
}

#[test]
fn bootstrap_rejects_wrong_phase_items_duplicate_manifest_and_oversized_pages() {
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
}

#[test]
fn bootstrap_attachment_and_follower_authority_is_checked_before_source_access_and_after_admission()
{
    let mut attachment = request();
    attachment.phase = ReplicationPhase::Attach {
        manifest: vec![1; 512],
    };
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
            let expected = change.apply(&policy);
            release.send(()).unwrap();
            assert!(matches!(ready(admission), Err(actual) if actual == expected));
            assert_eq!(source.opens.load(Ordering::Acquire), 1);
            assert_eq!(reads.load(Ordering::Acquire), 0);
            assert_eq!(drops.load(Ordering::Acquire), 1);
        }
    }
}
