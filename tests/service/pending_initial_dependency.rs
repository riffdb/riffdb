//! Pending construction must be intercepted before required audit submission.
//! The actual restored native failure is covered by WP-825's public restore
//! probe; this test isolates the service/coordinator phase boundary.
// req: SEC-005, TXN-042, REC-001, OUT-001
use super::pending_initial_dependency_support::PendingInitialDependencyFixture;
use super::*;

#[test]
fn pending_initial_dependency_defers_first_replay_before_required_audit_submission() {
    run_async(async {
        let mut harness = ServiceHarness::command();
        let original = execute_journaled(&harness, 0xc0).await;
        assert_eq!(original.completion(), JournaledCompletion::Committed);
        let fixture = PendingInitialDependencyFixture::new(&harness);
        let service = fixture.service.clone();
        let (context, _cancellation) = harness.context(0xc1);
        let invocation = service.execute_command(context, harness.execute_command_request());
        let mut invocation = std::pin::pin!(invocation);
        assert!(matches!(
            invocation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        let mut job = fixture.spawner.take();
        fixture.drive_to_dependency_or_audit(&mut job).await;
        assert_eq!(
            fixture.audit_submissions(),
            0,
            "known Pending construction must wait before required audit reaches the actor"
        );
        assert_eq!(
            fixture.lifecycle(),
            riffdb_commit::CoordinatorLifecycleState::Accepting
        );
        assert!(harness.health.failures().is_empty());
        assert!(
            fixture.dependency.records().is_empty(),
            "Pending appends commit nothing"
        );
        fixture.dependency.release();
        job.await;
        let result = invocation
            .await
            .expect("the admitted original ACK remains replayable");
        let ExecuteCommandResult::Journaled(replayed) = result else {
            panic!("journaled mutation");
        };
        assert_eq!(replayed.completion(), JournaledCompletion::Replayed);
        assert_eq!(replayed.commit_sequence(), original.commit_sequence());
        assert_eq!(replayed.provenance_id(), original.provenance_id());
        assert_eq!(replayed.outcome(), original.outcome());
        assert_eq!(
            fixture.audit_submissions(),
            1,
            "one fused required audit pair"
        );
        assert_eq!(
            fixture
                .dependency
                .records()
                .iter()
                .map(|record| record.phase())
                .collect::<Vec<_>>(),
            vec![ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded]
        );
        assert_eq!(
            fixture.lifecycle(),
            riffdb_commit::CoordinatorLifecycleState::Accepting
        );
        assert!(harness.health.failures().is_empty());
        drop(fixture);
        harness.stop_coordinator();
    });
}

fn assert_no_initial_work(harness: &ServiceHarness, fixture: &PendingInitialDependencyFixture) {
    assert_eq!(fixture.audit_submissions(), 0);
    assert!(fixture.dependency.records().is_empty());
    assert_eq!(harness.policy.calls(), 0);
    assert_eq!(harness.ports.prepare_active_calls(), 0);
    assert_eq!(harness.ports.prepare_contract_version_calls(), 0);
    assert_eq!(harness.ports.read_reservations(), 0);
    assert_eq!(harness.ports.read_submissions(), 0);
    assert!(harness.ports.operation_calls().is_empty());
    assert!(harness.health.failures().is_empty());
    assert_eq!(
        fixture.lifecycle(),
        riffdb_commit::CoordinatorLifecycleState::Accepting
    );
}

#[test]
fn pending_initial_dependency_cancellation_before_or_during_wait_accepts_no_work() {
    run_async(async {
        for cancel_before_poll in [true, false] {
            let mut harness = ServiceHarness::command();
            let fixture = PendingInitialDependencyFixture::new(&harness);
            let service = fixture.service.clone();
            let (context, cancellation) = harness.context(0xc2);
            if cancel_before_poll {
                cancellation.cancel();
            }
            let invocation = service.execute_command(context, harness.execute_command_request());
            let mut invocation = std::pin::pin!(invocation);
            assert!(
                invocation
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            let mut job = fixture.spawner.take();
            if !cancel_before_poll {
                fixture.drive_to_dependency_or_audit(&mut job).await;
                assert_no_initial_work(&harness, &fixture);
                cancellation.cancel();
            }
            job.await;
            let failure = invocation
                .await
                .expect_err("unadmitted cancellation cannot execute");
            assert!(matches!(failure, ServiceFailure::Cancelled));
            assert_eq!(
                fixture.dependency.admission_probes(),
                1,
                "the native state is identified even for prestart cancellation"
            );
            assert_no_initial_work(&harness, &fixture);
            fixture.dependency.release();
            drop(fixture);
            let first = execute_journaled(&harness, 0xc3).await;
            assert_eq!(
                first.completion(),
                JournaledCompletion::Committed,
                "cancelled initial admission created no hidden command/outcome"
            );
            harness.stop_coordinator();
        }
    });
}

#[test]
fn pending_initial_dependency_expiry_uses_only_the_original_absolute_scheduler_deadline() {
    run_async(async {
        let mut harness = ServiceHarness::command();
        harness.control_request_deadline();
        let fixture = PendingInitialDependencyFixture::new(&harness);
        let service = fixture.service.clone();
        let (context, _cancellation) = harness.context(0xc4);
        let original_deadline = context.control().deadline();
        let invocation = service.execute_command(context, harness.execute_command_request());
        let mut invocation = std::pin::pin!(invocation);
        assert!(
            invocation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let mut job = fixture.spawner.take();
        fixture.drive_to_dependency_or_audit(&mut job).await;
        assert!(harness.request_deadline_is_waiting());
        assert_eq!(fixture.recorded_deadlines(), vec![original_deadline]);
        assert_no_initial_work(&harness, &fixture);
        harness.elapse_request_deadline();
        job.await;
        let failure = invocation
            .await
            .expect_err("the original request deadline ends Pending admission");
        assert!(matches!(failure, ServiceFailure::DeadlineExceeded));
        assert_eq!(
            fixture.recorded_deadlines(),
            vec![original_deadline],
            "no replacement deadline or terminal-audit wait was introduced"
        );
        assert_no_initial_work(&harness, &fixture);
        drop(fixture);
        assert_eq!(
            execute_journaled(&harness, 0xc5).await.completion(),
            JournaledCompletion::Committed
        );
        harness.stop_coordinator();
    });
}

async fn ready_prestart_matches_ordinary(expired: bool) {
    let mut ordinary = ServiceHarness::command();
    let mut ready = ServiceHarness::command();
    let fixture = PendingInitialDependencyFixture::new(&ready);
    fixture.dependency.release();
    let deadline = if expired {
        Instant::now() - Duration::from_secs(1)
    } else {
        Instant::now() + Duration::from_secs(30)
    };
    let (ordinary_context, ordinary_cancel) = ordinary.context_with_deadline(0xc6, deadline);
    let (ready_context, ready_cancel) = ready.context_with_deadline(0xc6, deadline);
    if !expired {
        ordinary_cancel.cancel();
        ready_cancel.cancel();
    }
    let ordinary_failure = ordinary
        .service
        .execute_command(ordinary_context, ordinary.execute_command_request())
        .await
        .expect_err("ordinary prestart control refuses");
    let service = fixture.service.clone();
    let invocation = service.execute_command(ready_context, ready.execute_command_request());
    let mut invocation = std::pin::pin!(invocation);
    assert!(
        invocation
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    fixture.spawner.take().await;
    let ready_failure = invocation
        .await
        .expect_err("Ready preserves ordinary prestart behavior");
    assert_eq!(
        (
            matches!(ready_failure, ServiceFailure::Cancelled),
            matches!(ready_failure, ServiceFailure::DeadlineExceeded),
            ready_failure.public_error().map(|e| e.kind())
        ),
        (
            matches!(ordinary_failure, ServiceFailure::Cancelled),
            matches!(ordinary_failure, ServiceFailure::DeadlineExceeded),
            ordinary_failure.public_error().map(|e| e.kind())
        ),
    );
    let ordinary_submissions = ordinary.audit_submission_count();
    ordinary.stop_coordinator();
    let phases = fixture
        .dependency
        .records()
        .iter()
        .map(|r| r.phase())
        .collect::<Vec<_>>();
    assert_eq!(phases, ordinary.audit_phases(0xc6));
    assert_eq!(fixture.audit_submissions(), ordinary_submissions);
    assert_eq!(ready.health.failures(), ordinary.health.failures());
    assert_eq!(ready.policy.calls(), ordinary.policy.calls());
    assert_eq!(
        ready.ports.operation_calls(),
        ordinary.ports.operation_calls()
    );
    if !expired {
        // ADR-0007 puts a generic ExecuteCommand in intrinsic audit scope only
        // after checked-plan classification. Cancellation in catalog preparation
        // precedes that boundary in both ordinary and Ready compositions.
        assert!(phases.is_empty());
        assert_eq!(fixture.audit_submissions(), 0);
    } else {
        assert!(
            fixture
                .recorded_deadlines()
                .iter()
                .all(|observed| *observed == deadline),
            "Ready cannot manufacture fresh time to persist an already expired audit"
        );
    }
    assert_eq!(fixture.dependency.admission_probes(), 1);
    drop(fixture);
    ready.stop_coordinator();
}

#[test]
fn ready_initial_dependency_retains_unclassified_command_cancellation_boundary() {
    run_async(ready_prestart_matches_ordinary(false));
}

#[test]
fn ready_initial_dependency_prestart_administrative_cancellation_retains_standalone_audit() {
    run_async(async {
        let mut ordinary = ServiceHarness::operations();
        let mut ready = ServiceHarness::operations();
        let fixture = PendingInitialDependencyFixture::new(&ready);
        fixture.dependency.release();
        let deadline = Instant::now() + Duration::from_secs(30);
        let (ordinary_context, ordinary_cancel) = ordinary.context_with_deadline(0xce, deadline);
        let (ready_context, ready_cancel) = ready.context_with_deadline(0xce, deadline);
        ordinary_cancel.cancel();
        ready_cancel.cancel();
        let ordinary_failure = ordinary
            .service
            .get_commit(ordinary_context, GetCommitRequest::new(sequence(9)))
            .await
            .expect_err("intrinsic administrative read observes prestart cancellation");
        let service = fixture.service.clone();
        let invocation = service.get_commit(ready_context, GetCommitRequest::new(sequence(9)));
        let mut invocation = std::pin::pin!(invocation);
        assert!(
            invocation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        fixture.spawner.take().await;
        let ready_failure = invocation
            .await
            .expect_err("Ready retains intrinsic prestart audit");
        assert!(matches!(ordinary_failure, ServiceFailure::Cancelled));
        assert!(matches!(ready_failure, ServiceFailure::Cancelled));
        assert_eq!(ordinary.audit_submission_count(), 1);
        assert_eq!(fixture.audit_submissions(), 1);
        ordinary.stop_coordinator();
        let ordinary_records = ordinary.audit_records(0xce);
        let records = fixture.dependency.records();
        assert_eq!(ordinary_records.len(), 1);
        assert_eq!(records.len(), 1);
        let expected_targets =
            ServiceAuditTargetsV1::new([riffdb_types::ServiceAuditTargetV1::Commit(sequence(9))])
                .expect("one exact independently request-addressed target");
        for record in ordinary_records.iter().chain(records.iter()) {
            assert_eq!(record.operation(), ServiceOperationV1::GetCommit);
            assert_eq!(record.phase(), ServiceAuditPhaseV1::Cancelled);
            assert_eq!(record.targets(), &expected_targets);
            assert_eq!(record.link(), ServiceAuditLinkV1::None);
        }
        for harness in [&ordinary, &ready] {
            assert_eq!(harness.policy.calls(), 0);
            assert_eq!(harness.ports.read_reservations(), 0);
            assert_eq!(harness.ports.read_submissions(), 0);
            assert!(harness.ports.operation_calls().is_empty());
            assert!(harness.health.failures().is_empty());
        }
        assert_eq!(fixture.dependency.admission_probes(), 1);
        assert_eq!(
            fixture.lifecycle(),
            riffdb_commit::CoordinatorLifecycleState::Accepting
        );
        drop(fixture);
        ready.stop_coordinator();
    });
}

#[test]
fn ready_initial_dependency_expired_request_preserves_exact_ordinary_failure_and_audit_rules() {
    run_async(ready_prestart_matches_ordinary(true));
}

#[test]
fn pending_initial_dependency_release_requires_fresh_revocation_policy_before_replay() {
    run_async(async {
        let mut harness = ServiceHarness::command();
        let original = execute_journaled(&harness, 0xc7).await;
        assert_eq!(original.completion(), JournaledCompletion::Committed);
        let policy_before = harness.policy.calls();
        let catalog_before = harness.ports.prepare_active_calls();
        let fixture = PendingInitialDependencyFixture::new(&harness);
        let service = fixture.service.clone();
        let (context, _cancellation) = harness.context(0xc8);
        let invocation = service.execute_command(context, harness.execute_command_request());
        let mut invocation = std::pin::pin!(invocation);
        assert!(
            invocation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let mut job = fixture.spawner.take();
        fixture.drive_to_dependency_or_audit(&mut job).await;
        assert_eq!(fixture.audit_submissions(), 0);
        assert_eq!(harness.policy.calls(), policy_before);
        assert_eq!(harness.ports.prepare_active_calls(), catalog_before);
        harness.revoke_policy();
        fixture.dependency.release();
        job.await;
        let failure = invocation
            .await
            .expect_err("waiting cannot cache pre-revocation replay authority");
        assert_eq!(
            failure.public_error().map(|e| e.kind()),
            Some(PublicErrorKind::AuthorizationDenied)
        );
        assert!(
            harness.policy.calls() > policy_before,
            "fresh policy runs only after release"
        );
        assert_eq!(
            fixture
                .dependency
                .records()
                .iter()
                .map(|r| r.phase())
                .collect::<Vec<_>>(),
            [ServiceAuditPhaseV1::Denied]
        );
        assert_eq!(
            fixture.audit_submissions(),
            1,
            "fresh prestart denial has one standalone audit"
        );
        assert!(harness.health.failures().is_empty());
        drop(fixture);
        harness.stop_coordinator();
    });
}

#[test]
fn accepted_audit_failure_after_initial_ready_still_stops_or_fences_authoritative_audit() {
    run_async(async {
        let mut harness = ServiceHarness::command();
        assert_eq!(
            execute_journaled(&harness, 0xc9).await.completion(),
            JournaledCompletion::Committed
        );
        let fixture = PendingInitialDependencyFixture::new(&harness);
        fixture.dependency.release();
        fixture.dependency.fail_appends();
        let service = fixture.service.clone();
        let (context, _cancellation) = harness.context(0xca);
        let invocation = service.execute_command(context, harness.execute_command_request());
        let mut invocation = std::pin::pin!(invocation);
        assert!(
            invocation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        fixture.spawner.take().await;
        let failure = invocation
            .await
            .expect_err("a replay result remains private after accepted audit failure");
        assert_eq!(
            failure.public_error().map(|e| e.kind()),
            Some(PublicErrorKind::OutcomeUnknown),
            "the original durable replay is withheld, never replaced with a new result"
        );
        assert_eq!(
            fixture.audit_submissions(),
            1,
            "the actor accepted the actual audit work"
        );
        assert!(fixture.dependency.refused_appends() > 0);
        assert!(fixture.dependency.records().is_empty());
        assert!(matches!(
            fixture.lifecycle(),
            riffdb_commit::CoordinatorLifecycleState::Stopped
                | riffdb_commit::CoordinatorLifecycleState::Fenced
        ));
        assert!(
            harness
                .health
                .failures()
                .contains(&AuthoritativeReadinessFailure::AuditUnavailable)
        );
        assert!(
            harness
                .telemetry
                .events()
                .contains(&ServiceTelemetryEvent::AuditUnavailable {
                    operation: ServiceOperationV1::ExecuteCommand
                })
        );
        drop(fixture);
        harness.stop_coordinator();
    });
}

#[test]
fn ordinary_reads_and_authenticated_health_wait_before_policy_catalog_and_audit_inputs() {
    run_async(async {
        for health in [false, true] {
            let mut harness = ServiceHarness::operations();
            let fixture = PendingInitialDependencyFixture::new(&harness);
            let service = fixture.service.clone();
            let (context, _cancellation) = harness.context(0xcb);
            let invocation = async {
                if health {
                    service
                        .health(
                            riffdb_service::HealthContext::authenticated(context),
                            riffdb_service::HealthRequest,
                        )
                        .await
                        .map(|_| ())
                } else {
                    service
                        .get_commit(context, GetCommitRequest::new(sequence(9)))
                        .await
                        .map(|_| ())
                }
            };
            let mut invocation = std::pin::pin!(invocation);
            assert!(
                invocation
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            let mut job = fixture.spawner.take();
            fixture.drive_to_dependency_or_audit(&mut job).await;
            assert_no_initial_work(&harness, &fixture);
            fixture.dependency.release();
            job.await;
            invocation
                .await
                .expect("ordinary authorized operation proceeds only after native completion");
            assert!(harness.policy.calls() > 0);
            if health {
                assert!(harness.ports.operation_calls().contains(&"health"));
            } else {
                assert_eq!(harness.ports.read_submissions(), 1);
            }
            assert!(harness.health.failures().is_empty());
            assert_eq!(fixture.dependency.admission_probes(), 1);
            drop(fixture);
            harness.stop_coordinator();
        }
    });
}

#[test]
fn pending_probe_unavailable_obeys_control_precedence_but_integrity_is_never_downgraded() {
    run_async(async {
        for integrity in [false, true] {
            for control_state in 0..3 {
                let expired = control_state == 2;
                let mut harness = ServiceHarness::command();
                let fixture = PendingInitialDependencyFixture::new(&harness);
                fixture.dependency.refuse_probe(if integrity {
                    riffdb_service::InitialNativeDependencyError::Integrity
                } else {
                    riffdb_service::InitialNativeDependencyError::Unavailable
                });
                let deadline = if expired {
                    Instant::now() - Duration::from_secs(1)
                } else {
                    Instant::now() + Duration::from_secs(30)
                };
                let (context, cancellation) = harness.context_with_deadline(0xcc, deadline);
                if control_state == 1 {
                    cancellation.cancel();
                }
                let service = fixture.service.clone();
                let invocation =
                    service.execute_command(context, harness.execute_command_request());
                let mut invocation = std::pin::pin!(invocation);
                assert!(
                    invocation
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                fixture.spawner.take().await;
                let failure = invocation
                    .await
                    .expect_err("refused native admission accepts no operation");
                assert_eq!(fixture.audit_submissions(), 0);
                assert!(fixture.dependency.records().is_empty());
                assert_eq!(harness.policy.calls(), 0);
                assert_eq!(harness.ports.prepare_active_calls(), 0);
                assert!(harness.ports.operation_calls().is_empty());
                if integrity {
                    assert_eq!(
                        failure.public_error().map(|e| e.kind()),
                        Some(PublicErrorKind::InternalDefect)
                    );
                    assert_eq!(
                        harness.health.failures(),
                        [AuthoritativeReadinessFailure::Integrity]
                    );
                } else {
                    if expired {
                        assert!(matches!(failure, ServiceFailure::DeadlineExceeded));
                    } else if control_state == 1 {
                        assert!(matches!(failure, ServiceFailure::Cancelled));
                    } else {
                        assert_eq!(
                            failure.public_error().map(|e| e.kind()),
                            Some(PublicErrorKind::StorageUnavailable)
                        );
                    }
                    assert_no_initial_work(&harness, &fixture);
                }
                drop(fixture);
                harness.stop_coordinator();
            }
        }
    });
}
