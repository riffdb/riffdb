// req: REP-003, REP-005
use super::*;
use riffdb_service::{RequestContext, RequestControl};
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1 as Phase, ServiceOperationV1};

fn context(policy: &Policy) -> (RequestContext, riffdb_service::RequestCancellationHandle) {
    let (control, cancel) =
        RequestControl::new(std::time::Instant::now() + std::time::Duration::from_secs(30));
    (
        RequestContext::from_authenticated_grpc(
            support::request_id(42),
            policy.principal(),
            control,
            None,
        ),
        cancel,
    )
}

async fn entered(source: &Source) {
    tokio::time::timeout(std::time::Duration::from_secs(30), source.opened.notified())
        .await
        .unwrap();
}

#[test]
fn every_stream_phase_preserves_selected_target_principal_and_no_result_link() {
    support::run_async(async {
        let mut follower = request();
        follower.phase = ReplicationPhase::Follower { hold_id: [1; 16] };
        for request in [
            request(),
            follower,
            bootstrap_request(),
            request_selection::attachment_request(),
        ] {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, sender, _, _) = source();
            let mut service = ReplicationService::new(policy.clone(), source);
            sender.try_send(Ok(None)).unwrap();
            let named = !matches!(request.phase, ReplicationPhase::Tail);
            let mut stream = service
                .stream_changelog(policy.principal(), request)
                .await
                .unwrap();
            assert_eq!(service.harness.audit_submission_count(), 2);
            // Establishment succeeds before any frame/page is requested, even
            // when the source has no next item. Bootstrap still needs its manifest.
            if !named {
                assert_eq!(stream.next_item().await, Ok(None));
            }
            drop(stream);
            service.harness.stop_coordinator();
            let records = service.harness.audit_records(42);
            assert_eq!(
                records.iter().map(|r| r.phase()).collect::<Vec<_>>(),
                [Phase::Started, Phase::Succeeded]
            );
            for record in records {
                assert_eq!(record.operation(), ServiceOperationV1::StreamChangelog);
                assert_eq!(record.link(), ServiceAuditLinkV1::None);
                assert_eq!(
                    record.principal().unwrap().capability_id(),
                    policy.principal().capability_id()
                );
                assert_eq!(record.targets().as_slice().len(), usize::from(named));
                if named {
                    assert_eq!(
                        record.targets().as_slice(),
                        &[riffdb_types::ServiceAuditTargetV1::ReplicationFollower(
                            riffdb_types::ReplicationFollowerAuditTargetV1::new(
                                database(1),
                                1,
                                riffdb_types::LeadershipEpochV1::initial(),
                                riffdb_types::ReplicationSourceHoldIdV1::new([1; 16]).unwrap()
                            )
                            .unwrap(),
                        )]
                    );
                }
            }
        }
    });
}

#[test]
fn initial_denial_is_durable_and_audit_outage_never_releases_source_work() {
    support::run_async(async {
        for (allowed, outage) in [(false, false), (false, true), (true, true)] {
            let policy = Policy::new(if allowed {
                CapabilityPermissionKindV1::ReplicateChangelog
            } else {
                CapabilityPermissionKindV1::AdministerCapabilities
            });
            let (source, _sender, reads, _) = source();
            let mut service = ReplicationService::new(policy.clone(), source.clone());
            if outage {
                service.harness.stop_coordinator();
            }
            let expected = if allowed {
                ReplicationFailure::Unavailable
            } else {
                ReplicationFailure::AuthorizationDenied
            };
            assert!(
                matches!(service.stream_changelog(policy.principal(), request()).await, Err(error) if error == expected)
            );
            assert_eq!(source.opens.load(Ordering::Acquire), 0);
            assert_eq!(reads.load(Ordering::Acquire), 0);
            if !outage {
                service.harness.stop_coordinator();
            }
            assert_eq!(
                service.harness.audit_phases(42),
                if outage { vec![] } else { vec![Phase::Denied] }
            );
        }
    });
}

#[test]
fn terminal_audit_outage_withholds_established_handle() {
    support::run_async(async {
        let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
        let (source, _sender, reads, _) = source();
        let (release, gate) = oneshot::channel();
        *source.gate.lock().unwrap() = Some(gate);
        let mut service = ReplicationService::new(policy.clone(), source.clone());
        let (context, _cancel) = context(&policy);
        let pending = service.service.stream_changelog(context, request());
        entered(&source).await;
        assert_eq!(service.harness.audit_submission_count(), 1);
        service.harness.stop_coordinator();
        release.send(()).unwrap();
        assert!(matches!(
            pending.await,
            Err(ReplicationFailure::Unavailable)
        ));
        assert_eq!(reads.load(Ordering::Acquire), 0);
        assert_eq!(service.harness.audit_phases(42), [Phase::Started]);
    });
}

#[test]
fn dropped_transport_drains_source_custody_without_claiming_cancellation() {
    support::run_async(async {
        let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
        let (source, _sender, reads, _) = source();
        let (release, gate) = oneshot::channel();
        *source.gate.lock().unwrap() = Some(gate);
        let mut service = ReplicationService::new(policy.clone(), source.clone());
        let (context, cancel) = context(&policy);
        let mut request = request();
        request.phase = ReplicationPhase::Follower { hold_id: [1; 16] };
        let pending = service.service.stream_changelog(context, request);
        entered(&source).await;
        cancel.cancel();
        drop(pending);
        release.send(()).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            source.dropped.notified(),
        )
        .await
        .unwrap();
        assert_eq!(reads.load(Ordering::Acquire), 0);
        assert_eq!(service.harness.audit_submission_count(), 2);
        service.harness.stop_coordinator();
        assert_eq!(
            service.harness.audit_phases(42),
            [Phase::Started, Phase::Succeeded]
        );
    });
}

#[test]
fn failed_or_panicking_source_custody_is_uncertain_and_read_failure_is_known() {
    support::run_async(async {
        for (custody, panic) in [(false, false), (true, false), (true, true)] {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, _sender, _, _) = source();
            *source.failure.lock().unwrap() = Some(ReplicationFailure::Source(
                ReplicationStreamErrorV3::Unavailable,
            ));
            source.panic_after_open.store(panic, Ordering::Release);
            let mut service = ReplicationService::new(policy.clone(), source);
            let mut request = request();
            if custody {
                request.phase = ReplicationPhase::Follower { hold_id: [1; 16] };
            }
            assert!(
                service
                    .stream_changelog(policy.principal(), request)
                    .await
                    .is_err()
            );
            service.harness.stop_coordinator();
            assert_eq!(
                service.harness.audit_phases(42),
                [
                    Phase::Started,
                    if custody {
                        Phase::OutcomeUncertain
                    } else {
                        Phase::Failed
                    }
                ]
            );
        }
    });
}

#[test]
fn cancellation_before_establishment_is_standalone_and_does_not_open_source() {
    support::run_async(async {
        let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
        let (source, _sender, _, _) = source();
        let mut service = ReplicationService::new(policy.clone(), source.clone());
        let (context, cancel) = context(&policy);
        cancel.cancel();
        assert!(matches!(
            service.service.stream_changelog(context, request()).await,
            Err(ReplicationFailure::Unavailable)
        ));
        assert_eq!(source.opens.load(Ordering::Acquire), 0);
        service.harness.stop_coordinator();
        assert_eq!(service.harness.audit_phases(42), [Phase::Cancelled]);
    });
}

#[test]
fn later_denial_or_panic_fuses_and_releases_source_with_telemetry_only() {
    support::run_async(async {
        for (panic, drop_panic) in [(false, false), (true, false), (false, true)] {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, _sender, reads, drops) = source();
            let mut service = ReplicationService::new(policy.clone(), source.clone());
            let mut stream = service
                .stream_changelog(policy.principal(), request())
                .await
                .unwrap();
            source.panic_drop.store(drop_panic, Ordering::Release);
            if panic {
                source.panic_next.store(true, Ordering::Release);
            } else {
                Change::Revoke.apply(&policy);
            }
            let expected = if panic {
                ReplicationFailure::Unavailable
            } else {
                ReplicationFailure::AuthorizationDenied
            };
            assert_eq!(stream.next_item().await, Err(expected));
            assert_eq!(drops.load(Ordering::Acquire), 1);
            assert_eq!(stream.next_item().await, Ok(None));
            assert_eq!(reads.load(Ordering::Acquire), usize::from(panic));
            assert_eq!(service.harness.audit_submission_count(), 2);
            assert!(
                service
                    .harness
                    .telemetry
                    .events()
                    .iter()
                    .any(|event| if panic {
                        matches!(
                            event,
                            riffdb_service::ServiceTelemetryEvent::InternalIntegrity {
                                operation: ServiceOperationV1::StreamChangelog
                            }
                        )
                    } else {
                        matches!(
                            event,
                            riffdb_service::ServiceTelemetryEvent::StreamClosedByPolicy
                        )
                    })
            );
            service.harness.stop_coordinator();
            assert_eq!(
                service.harness.audit_phases(42),
                [Phase::Started, Phase::Succeeded]
            );
        }
    });
}

#[test]
fn fence_phase_releases_one_exact_observation_and_never_reaudits_continuation() {
    support::run_async(async {
        for case in 0..5 {
            let policy = Policy::new(CapabilityPermissionKindV1::ReplicateChangelog);
            let (source, sender, _, drops) = source();
            let value = fence_evidence::evidence(1);
            let selected = fence_evidence::selection(&value);
            let mut input = request();
            input.phase = ReplicationPhase::FenceEvidence { request: selected };
            input.after_sequence = value.applied().sequence().get();
            input.after_hash = value.applied().history_hash();
            input.after_frontier = value.applied().frontier();
            let mut service = ReplicationService::new(policy.clone(), source);
            let mut stream = service
                .stream_changelog(policy.principal(), input)
                .await
                .unwrap();
            assert_eq!(service.harness.audit_submission_count(), 2);
            if case == 4 {
                Change::Revoke.apply(&policy);
            }
            let returned = if case == 1 {
                fence_evidence::evidence(2)
            } else {
                value.clone()
            };
            sender
                .try_send(Ok(if case == 3 {
                    None
                } else {
                    Some(ReplicationItem::FenceEvidence(Box::new(returned)))
                }))
                .unwrap();
            let first = stream.next_item().await;
            if case == 0 || case == 2 {
                assert_eq!(
                    first,
                    Ok(Some(ReplicationItem::FenceEvidence(Box::new(
                        value.clone()
                    ))))
                );
                sender
                    .try_send(Ok(if case == 2 {
                        Some(ReplicationItem::FenceEvidence(Box::new(value)))
                    } else {
                        None
                    }))
                    .unwrap();
                let second = stream.next_item().await;
                if case == 0 {
                    assert_eq!(second, Ok(None));
                } else {
                    assert!(second.is_err());
                }
            } else {
                assert!(first.is_err());
            }
            assert_eq!(stream.next_item().await, Ok(None));
            assert_eq!(drops.load(Ordering::Acquire), 1);
            assert_eq!(service.harness.audit_submission_count(), 2);
            service.harness.stop_coordinator();
            assert_eq!(
                service.harness.audit_phases(42),
                [Phase::Started, Phase::Succeeded]
            );
            for row in service.harness.audit_records(42) {
                assert_eq!(
                    row.targets().as_slice(),
                    &[riffdb_types::ServiceAuditTargetV1::ReplicationFollower(
                        selected.target()
                    )]
                );
                assert_eq!(row.link(), ServiceAuditLinkV1::None);
            }
        }
    });
}
