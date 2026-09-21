// Live drained storage owner; policy/coordinator activation remains separate.
// req: REP-005, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    PrimaryFenceAwaitingDecision, PrimaryFenceCandidateTransaction, PrimaryFenceCandidateV1,
    PrimaryFenceIntentV1, PrimaryFenceResultV1, PrimaryFenceTransactionPort,
};

fn finish(
    ports: &impl PrimaryFenceTransactionPort,
    request: PrimaryFenceRequestV1,
) -> Result<StoredPrimaryFenceAdministrationV1, StorageError> {
    let candidate = PrimaryFenceCandidateV1::new(request, principal(capability_id(3)));
    let (awaiting, _) = ports
        .begin_primary_fence_transaction(candidate.clone())?
        .read_transaction_current()?;
    match awaiting.commit(PrimaryFenceIntentV1::new(
        candidate,
        Timestamp::new(16, 0).unwrap(),
    ))? {
        PrimaryFenceResultV1::Applied(record) | PrimaryFenceResultV1::Replayed(record) => {
            Ok(*record)
        }
        PrimaryFenceResultV1::Refused(_) => panic!("fixture's exact fence must be admitted"),
    }
}

#[test]
fn primary_fence_shared_transaction_port_preserves_exact_replay_and_candidate_binding() {
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let (_path, ports, request) = attached_source(profile);
        let shared = ports.shared_ports();
        let candidate = PrimaryFenceCandidateV1::new(request, principal(capability_id(3)));
        let before = history(&ports);
        let (awaiting, _) = shared
            .begin_primary_fence_transaction(candidate.clone())
            .unwrap()
            .read_transaction_current()
            .unwrap();
        assert!(ports.primary_fence_lease_is_held());
        awaiting.abandon();
        assert!(!ports.primary_fence_lease_is_held());
        assert_eq!(history(&ports), before);
        let changed = PrimaryFenceCandidateV1::new(request, principal(capability_id(4)));
        let (awaiting, _) = shared
            .begin_primary_fence_transaction(candidate.clone())
            .unwrap()
            .read_transaction_current()
            .unwrap();
        assert_eq!(
            awaiting
                .commit(PrimaryFenceIntentV1::new(
                    changed,
                    Timestamp::new(16, 0).unwrap()
                ))
                .unwrap_err()
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        assert!(!ports.primary_fence_lease_is_held());
        assert_eq!(history(&ports), before);
        let (awaiting, _) = shared
            .begin_primary_fence_transaction(candidate.clone())
            .unwrap()
            .read_transaction_current()
            .unwrap();
        let PrimaryFenceResultV1::Applied(record) = awaiting
            .commit(PrimaryFenceIntentV1::new(
                candidate,
                Timestamp::new(16, 0).unwrap(),
            ))
            .unwrap()
        else {
            panic!("fresh fence");
        };
        let frozen = history(&ports);
        let retry = PrimaryFenceRequestV1::new(
            request_id(34),
            request.operation_id(),
            request.target(),
            request.generation(),
        );
        let candidate = PrimaryFenceCandidateV1::new(retry, principal(capability_id(3)));
        let (awaiting, _) = shared
            .begin_primary_fence_transaction(candidate.clone())
            .unwrap()
            .read_transaction_current()
            .unwrap();
        let PrimaryFenceResultV1::Replayed(replayed) = awaiting
            .commit(PrimaryFenceIntentV1::new(
                candidate,
                Timestamp::new(17, 0).unwrap(),
            ))
            .unwrap()
        else {
            panic!("exact retry");
        };
        assert_eq!(replayed, record);
        assert_eq!(history(&ports), frozen);
        assert_eq!(replayed.request_id(), request.request_id());
    }
}

#[test]
fn primary_fence_control_publishes_once_and_replays_the_original_receipt() {
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let (path, ports, request) = attached_source(profile);
        let before = history(&ports);
        let old_pin = ports.published_changelog_snapshot_v3().unwrap();
        let (awaiting, current) = ports
            .begin_primary_fence(request, principal(capability_id(3)))
            .unwrap()
            .read_transaction_current()
            .unwrap();
        assert_eq!(current.unwrap().capability_id(), capability_id(3));
        assert!(ports.primary_fence_lease_is_held());
        let staged = awaiting
            .stage(
                request,
                principal(capability_id(3)),
                Timestamp::new(16, 0).unwrap(),
            )
            .unwrap();
        assert!(ports.primary_fence_lease_is_held());
        assert_eq!(history(&ports), before, "uncommitted fence stays private");
        let record = staged.commit().unwrap();
        assert!(!ports.primary_fence_lease_is_held());
        let after = history(&ports);
        assert_eq!(
            after.tail().sequence(),
            before.tail().sequence().checked_next().unwrap()
        );
        assert_eq!(
            after.tail().frontier().application(),
            before.tail().frontier().application()
        );
        assert_eq!(
            after.tail().frontier().administration(),
            Some(record.administration_sequence())
        );
        assert_eq!(record.observed(), before.tail());
        assert_eq!(old_pin.authoritative_state_v3().unwrap().history(), before);
        assert_eq!(
            ports.read_replication_primary_admission().unwrap().fence(),
            Some(&record)
        );
        let retry = PrimaryFenceRequestV1::new(
            request_id(33),
            request.operation_id(),
            request.target(),
            request.generation(),
        );
        let (awaiting, _) = ports
            .begin_primary_fence(retry, principal(capability_id(3)))
            .unwrap()
            .read_transaction_current()
            .unwrap();
        let replayed = awaiting
            .stage(
                retry,
                principal(capability_id(3)),
                Timestamp::new(17, 0).unwrap(),
            )
            .unwrap();
        assert!(matches!(
            &replayed,
            crate::primary_fence_write::PrimaryFenceCompletion::Replay(_)
        ));
        assert_eq!(replayed.commit().unwrap(), record);
        assert!(!ports.primary_fence_lease_is_held());
        assert_eq!(history(&ports), after);
        drop(old_pin);
        drop(ports);
        let reopened = crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
        assert_eq!(
            history(&reopened).tail().frontier(),
            after.tail().frontier()
        );
        assert!(after.tail().precedes_or_equals(history(&reopened).tail()));
        assert_eq!(
            reopened
                .read_replication_primary_admission()
                .unwrap()
                .fence(),
            Some(&record)
        );
    }
}

#[test]
fn primary_fence_control_cancellation_aborts_at_each_owned_state() {
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let (_path, ports, request) = attached_source(profile);
        let before = history(&ports);
        for phase in 0..3 {
            let candidate = ports
                .begin_primary_fence(request, principal(capability_id(3)))
                .unwrap();
            assert!(ports.primary_fence_lease_is_held());
            if phase == 0 {
                drop(candidate);
            } else {
                let (awaiting, _) = candidate.read_transaction_current().unwrap();
                if phase == 1 {
                    drop(awaiting);
                } else {
                    drop(
                        awaiting
                            .stage(
                                request,
                                principal(capability_id(3)),
                                Timestamp::new(16, 0).unwrap(),
                            )
                            .unwrap(),
                    );
                }
            }
            assert!(!ports.primary_fence_lease_is_held());
            assert_eq!(history(&ports), before);
            assert!(
                ports
                    .read_replication_primary_admission()
                    .unwrap()
                    .fence()
                    .is_none()
            );
            let root = ports.shared.database.begin_read().unwrap();
            assert_eq!(
                crate::changelog_v3_roots::validate_retained_history(&root).unwrap(),
                Some(before)
            );
        }
    }
}

#[test]
fn primary_fence_control_drains_submitted_audit_before_freezing_the_head() {
    let (_path, mut ports, request) = attached_source(crate::RedbCommitProfile::Standard);
    let before = history(&ports);
    let submitted = ports
        .submit_service_audit_group(&[denied_audit(44)])
        .unwrap();
    let riffdb_storage_api::ServiceAuditGroupAppend::Submitted(pending) = submitted else {
        panic!("standard journal audit must be submitted");
    };
    assert_eq!(history(&ports), before);
    let record = finish(&ports, request).unwrap();
    assert!(matches!(
        pending.wait().unwrap().as_slice(),
        [ServiceAuditAppendResult::Appended(_)]
    ));
    assert_eq!(
        record.observed().frontier().administration(),
        before
            .tail()
            .frontier()
            .administration()
            .unwrap()
            .checked_next()
    );
    assert_eq!(
        record.final_application_head(),
        before.tail().frontier().application()
    );
    assert_eq!(
        history(&ports).tail().sequence(),
        record.observed().sequence().checked_next().unwrap()
    );
    assert_eq!(
        ports.read_replication_primary_admission().unwrap().fence(),
        Some(&record)
    );
}

#[test]
fn primary_fence_control_refuses_changed_candidate_and_expired_authority_without_writing() {
    let (_path, ports, request) = attached_source(crate::RedbCommitProfile::Standard);
    let before = history(&ports);
    let changed = PrimaryFenceRequestV1::new(
        request_id(99),
        request.operation_id(),
        request.target(),
        request.generation(),
    );
    for (selected, timestamp) in [
        (changed, Timestamp::new(16, 0).unwrap()),
        (request, Timestamp::new(i64::MAX, 0).unwrap()),
    ] {
        let (awaiting, _) = ports
            .begin_primary_fence(request, principal(capability_id(3)))
            .unwrap()
            .read_transaction_current()
            .unwrap();
        let error = awaiting
            .stage(selected, principal(capability_id(3)), timestamp)
            .err()
            .expect("stale final authorization");
        assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
        assert!(!ports.primary_fence_lease_is_held());
        assert_eq!(history(&ports), before);
        assert!(
            ports
                .read_replication_primary_admission()
                .unwrap()
                .fence()
                .is_none()
        );
    }
    assert_eq!(finish(&ports, request).unwrap().observed(), before.tail());
}

#[test]
fn primary_fence_control_commit_uncertainty_fences_writes_and_reopen_resolves_original() {
    use crate::{RedbTestController, RedbTestOperation};
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        for committed in [false, true] {
            let (path, ports, request) = attached_source(profile);
            drop(ports);
            let controller = if committed {
                RedbTestController::return_unknown_after_commit(RedbTestOperation::PrimaryFence)
            } else {
                RedbTestController::return_before_commit(RedbTestOperation::PrimaryFence)
            };
            let store = RedbStore::open_with_test_controller_and_commit_profile(
                &path.0, profile, controller,
            )
            .unwrap();
            let ports = crate::startup::validate_source_store_fixture(store, inputs());
            let before = history(&ports);
            let error = finish(&ports, request).unwrap_err();
            assert_eq!(
                error.kind(),
                if committed {
                    StorageErrorKind::CommitStatusUnknown
                } else {
                    StorageErrorKind::Unavailable
                }
            );
            assert!(!ports.primary_fence_lease_is_held());
            if committed {
                assert_eq!(
                    finish(&ports, request).unwrap_err().kind(),
                    StorageErrorKind::Unavailable
                );
            } else {
                assert_eq!(history(&ports), before);
                assert!(
                    ports
                        .read_replication_primary_admission()
                        .unwrap()
                        .fence()
                        .is_none()
                );
            }
            drop(ports);
            let reopened =
                crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
            let admission = reopened.read_replication_primary_admission().unwrap();
            assert_eq!(admission.fence().is_some(), committed);
            let record = finish(&reopened, request).unwrap();
            if committed {
                assert_eq!(Some(&record), admission.fence());
                assert_eq!(record.observed(), before.tail());
            }
            assert_eq!(record.operation_id(), request.operation_id());
            assert_eq!(record.request_id(), request.request_id());
            assert_eq!(
                record.final_application_head(),
                before.tail().frontier().application()
            );
        }
    }
}

mod process {
    include!("primary_fence_control_process_tests.rs");
}

mod selection {
    include!("primary_fence_selection_tests.rs");
}

#[test]
fn primary_fence_service_success_links_refuse_substitutions_in_direct_and_journal_audit() {
    for profile in [crate::RedbCommitProfile::Standard, crate::RedbCommitProfile::Hardened] {
        for journal in [false, true] {
            let (path, mut ports, request) = attached_source(profile);
            let record = finish(&ports, request).unwrap();
            for case in 0..5u8 {
                let operation = if case == 1 { ServiceOperationV1::RetireFollower } else { ServiceOperationV1::FenceReplicationPrimary };
                let target = if case == 2 {
                    let t = record.target();
                    ReplicationFollowerAuditTargetV1::new(t.database_id(), t.history_incarnation(), t.leadership_epoch(),
                        ReplicationSourceHoldIdV1::new([0x7a; 16]).unwrap()).unwrap()
                } else { record.target() };
                let actor = if case == 3 { principal(capability_id(4)) } else { record.principal().clone() };
                let sequence = if case == 4 { record.administration_sequence().checked_next().unwrap() } else { record.administration_sequence() };
                let intent = |phase, link| ServiceAuditAppendIntentV1::new(request_id(70 + case), Timestamp::new(20, 0).unwrap(),
                    operation, phase, actor.clone(), ServiceIngressKindV1::Grpc,
                    ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ReplicationFollower(target)]).unwrap(), None, link).unwrap();
                for phase in [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded] {
                    let link = if phase == ServiceAuditPhaseV1::Succeeded { ServiceAuditLinkV1::ControlPlane { administration_sequence: sequence } } else { ServiceAuditLinkV1::None };
                    let batch = [intent(phase, link)];
                    let result = if journal {
                        match ports.submit_service_audit_group(&batch).unwrap() {
                            riffdb_storage_api::ServiceAuditGroupAppend::Complete(result) => result,
                            riffdb_storage_api::ServiceAuditGroupAppend::Submitted(pending) => pending.wait().unwrap(),
                        }
                    } else { ports.append_service_audit_group(&batch).unwrap() };
                    assert_eq!(matches!(result.as_slice(), [ServiceAuditAppendResult::Appended(_)]),
                        phase == ServiceAuditPhaseV1::Started || case == 0);
                }
            }
            drop(ports);
            let reopened = crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
            assert_eq!(reopened.read_replication_primary_admission().unwrap().fence(), Some(&record));
        }
    }
}

mod proof {
    include!("primary_fence_proof_tests.rs");
}
