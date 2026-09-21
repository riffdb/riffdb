//! The real policy gates source evidence before access and after any wait.
// req: REP-005
use super::*;
use riffdb_storage_api::{
    AuditPrincipalV1, ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
    ChangelogTransactionSequence as Sequence, PrimaryFenceRequestV1 as Request,
    PrimaryFenceSourceEvidenceV1 as Evidence, StoredPrimaryFenceAdministrationV1 as Fence,
};
use riffdb_types::{
    AdministrationSequence, CapabilityId, LeadershipEpochV1, ReplicationFenceOperationId,
    ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1 as Phase, ServiceAuditTargetV1, ServiceOperationV1,
};

pub(super) fn evidence(seed: u8) -> Evidence {
    let point = |seq, admin| {
        Point::new(
            Sequence::new(seq).unwrap(),
            [seed; 32],
            DualFrontier::new(None, AdministrationSequence::new(admin)),
        )
    };
    let fence = Fence::new(
        AdministrationSequence::new(3).unwrap(),
        timestamp(150),
        ReplicationFenceOperationId::from_unix_milliseconds_and_random(
            1_700_000_000_000,
            [seed; 10],
        )
        .unwrap(),
        RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10]).unwrap(),
        AuditPrincipalV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10]).unwrap(),
            std::num::NonZeroU64::MIN,
        ),
        None,
        ReplicationFollowerAuditTargetV1::new(
            database(1),
            1,
            LeadershipEpochV1::initial(),
            ReplicationSourceHoldIdV1::new([seed; 16]).unwrap(),
        )
        .unwrap(),
        Sequence::new(2).unwrap(),
        point(4, 2),
    )
    .unwrap();
    let history = History::new(fence.lineage(), point(1, 0), point(5, 3), point(1, 0)).unwrap();
    Evidence::new(fence, point(3, 1), history).unwrap()
}
pub(super) fn selection(evidence: &Evidence) -> Request {
    let fence = evidence.fence();
    Request::new(
        fence.request_id(),
        fence.operation_id(),
        fence.target(),
        fence.generation(),
    )
}
struct EvidenceSource {
    entered: tokio::sync::Notify,
    reads: AtomicUsize,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    result: Result<Evidence, ReplicationFailure>,
}
impl EvidenceSource {
    fn new(result: Result<Evidence, ReplicationFailure>) -> Arc<Self> {
        Arc::new(Self {
            reads: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            gate: Mutex::new(None),
            result,
        })
    }
}
impl ReplicationSourcePort for EvidenceSource {
    fn open(&self, _: ReplicationRequest) -> ReplicationFuture<'_, Box<dyn ReplicationItemSource>> {
        panic!("a proof read must not open a stream")
    }
    fn primary_fence_source_evidence(
        &self,
        _: Request,
        _: Point,
    ) -> ReplicationFuture<'_, Evidence> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        self.entered.notify_one();
        let gate = self.gate.lock().unwrap().take();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.await.unwrap();
            }
            self.result.clone()
        })
    }
}
#[test]
fn primary_fence_source_evidence_rechecks_authority_after_wait() {
    support::run_async(async move {
        for change in Change::ALL {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let value = evidence(1);
            let source = EvidenceSource::new(Ok(value.clone()));
            let (release, gate) = oneshot::channel();
            *source.gate.lock().unwrap() = Some(gate);
            let service = ReplicationService::new(policy.clone(), source.clone());
            let mut pending = service.primary_fence_source_evidence(
                policy.principal(),
                selection(&value),
                value.applied(),
            );
            assert!(poll(pending.as_mut()).is_pending());
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                source.entered.notified(),
            )
            .await
            .unwrap();
            let expected = change.apply(&policy);
            release.send(()).unwrap();
            assert_eq!(ready(pending), Err(expected));
            assert_eq!(source.reads.load(Ordering::Acquire), 1);
        }
    });
}
#[test]
fn primary_fence_source_evidence_denies_before_storage_and_binds_exact_selection() {
    support::run_async(async move {
        let value = evidence(1);
        for permitted in [false, true] {
            let policy = Policy::new(if permitted {
                CapabilityPermissionKindV1::ReplicateChangelog
            } else {
                CapabilityPermissionKindV1::AdministerCapabilities
            });
            let source = EvidenceSource::new(Ok(value.clone()));
            let mut service = ReplicationService::new(policy.clone(), source.clone());
            let result = ready(service.primary_fence_source_evidence(
                policy.principal(),
                selection(&value),
                value.applied(),
            ));
            assert_eq!(
                result,
                if permitted {
                    Ok(value.clone())
                } else {
                    Err(ReplicationFailure::AuthorizationDenied)
                }
            );
            assert_eq!(source.reads.load(Ordering::Acquire), usize::from(permitted));
            service.harness.stop_coordinator();
            let records = service.harness.audit_records(42);
            assert_eq!(
                records.iter().map(|row| row.phase()).collect::<Vec<_>>(),
                if permitted {
                    vec![Phase::Started, Phase::Succeeded]
                } else {
                    vec![Phase::Denied]
                }
            );
            for row in records {
                assert_eq!(row.operation(), ServiceOperationV1::StreamChangelog);
                assert_eq!(
                    row.targets().as_slice(),
                    &[ServiceAuditTargetV1::ReplicationFollower(
                        value.fence().target()
                    )]
                );
                assert_eq!(row.link(), ServiceAuditLinkV1::None);
                assert_eq!(
                    row.principal().unwrap().capability_id(),
                    policy.principal().capability_id()
                );
            }
        }
        let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
        let source = EvidenceSource::new(Ok(value.clone()));
        let mut service = ReplicationService::new(policy.clone(), source.clone());
        let other = evidence(2);
        let selected = selection(&value);
        for request in [
            Request::new(
                selected.request_id(),
                other.fence().operation_id(),
                selected.target(),
                selected.generation(),
            ),
            Request::new(
                selected.request_id(),
                selected.operation_id(),
                other.fence().target(),
                selected.generation(),
            ),
            Request::new(
                selected.request_id(),
                selected.operation_id(),
                selected.target(),
                selected.generation().checked_next().unwrap(),
            ),
        ] {
            assert_eq!(
                ready(service.primary_fence_source_evidence(
                    policy.principal(),
                    request,
                    value.applied()
                )),
                Err(ReplicationFailure::Source(
                    ReplicationStreamErrorV3::CorruptHistory
                ))
            );
        }
        let wrong_point = Point::new(
            value.applied().sequence(),
            [9; 32],
            value.applied().frontier(),
        );
        assert_eq!(
            ready(service.primary_fence_source_evidence(policy.principal(), selected, wrong_point)),
            Err(ReplicationFailure::Source(
                ReplicationStreamErrorV3::CorruptHistory
            ))
        );
        let retry = Request::new(
            other.fence().request_id(),
            selected.operation_id(),
            selected.target(),
            selected.generation(),
        );
        assert_eq!(
            ready(service.primary_fence_source_evidence(
                policy.principal(),
                retry,
                value.applied()
            )),
            Ok(value.clone())
        );
        let foreign = ReplicationFollowerAuditTargetV1::new(
            database(2),
            selected.target().history_incarnation(),
            selected.target().leadership_epoch(),
            selected.target().hold_id(),
        )
        .unwrap();
        let request = Request::new(
            selected.request_id(),
            selected.operation_id(),
            foreign,
            selected.generation(),
        );
        let before = source.reads.load(Ordering::Acquire);
        assert_eq!(
            ready(service.primary_fence_source_evidence(
                policy.principal(),
                request,
                value.applied()
            )),
            Err(ReplicationFailure::AuthorizationDenied)
        );
        assert_eq!(source.reads.load(Ordering::Acquire), before);
        service.harness.stop_coordinator();
        for seed in 42..46 {
            assert_eq!(
                service.harness.audit_phases(seed),
                [Phase::Started, Phase::Failed]
            );
        }
        assert_eq!(
            service.harness.audit_phases(46),
            [Phase::Started, Phase::Succeeded]
        );
        assert_eq!(service.harness.audit_phases(47), [Phase::Denied]);
        let failing = EvidenceSource::new(Err(ReplicationFailure::Unavailable));
        let service = ReplicationService::new(policy.clone(), failing);
        assert_eq!(
            ready(service.primary_fence_source_evidence(
                policy.principal(),
                selected,
                value.applied()
            )),
            Err(ReplicationFailure::Unavailable)
        );
        let (unsupported, _, _, _) = super::source();
        assert_eq!(
            ready(unsupported.primary_fence_source_evidence(selection(&value), value.applied())),
            Err(ReplicationFailure::Unavailable)
        );
    });
}
