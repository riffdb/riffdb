// req: REP-005, REC-001
use super::*;

struct AdmissionReader(Option<riffdb_storage_api::ReplicationPrimaryAdmissionV1>);
impl riffdb_storage_api::ReplicationPrimaryAdmissionReadPort for AdmissionReader {
    fn read_replication_primary_admission(
        &self,
    ) -> Result<riffdb_storage_api::ReplicationPrimaryAdmissionV1, StorageError> {
        self.0
            .clone()
            .ok_or_else(|| StorageError::new(StorageErrorKind::CorruptData, None))
    }
}

fn retained_fence() -> riffdb_storage_api::ReplicationPrimaryAdmissionV1 {
    use riffdb_storage_api::{
        ChangelogHistoryPointV3, ChangelogTransactionSequence, ReplicationPrimaryAdmissionV1,
        StoredPrimaryFenceAdministrationV1,
    };
    use riffdb_types::{
        DatabaseId, DualFrontier, LeadershipEpochV1, ReplicationFenceOperationId,
        ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1,
    };
    let input = checked_input(0xb5);
    ReplicationPrimaryAdmissionV1::fenced(
        StoredPrimaryFenceAdministrationV1::new(
            AdministrationSequence::new(3).unwrap(),
            fixed_timestamp(),
            ReplicationFenceOperationId::from_bytes(uuid_bytes(0xb6)).unwrap(),
            input.request_id,
            AuditPrincipalV1::new(
                input.principal_id,
                input.actor_kind,
                input.capability_id,
                input.capability_revision,
            ),
            None,
            ReplicationFollowerAuditTargetV1::new(
                DatabaseId::from_bytes(uuid_bytes(0xb7)).unwrap(),
                2,
                LeadershipEpochV1::new(3).unwrap(),
                ReplicationSourceHoldIdV1::new([0xb8; 16]).unwrap(),
            )
            .unwrap(),
            ChangelogTransactionSequence::new(4).unwrap(),
            ChangelogHistoryPointV3::new(
                ChangelogTransactionSequence::new(9).unwrap(),
                [0xb9; 32],
                DualFrontier::new(None, AdministrationSequence::new(2)),
            ),
        )
        .unwrap(),
    )
}

#[test]
fn startup_primary_admission_seeds_refusal_before_exposing_executors_but_keeps_audit() {
    let fenced = retained_fence();
    for admission in [
        riffdb_storage_api::ReplicationPrimaryAdmissionV1::active(fenced.lineage()).unwrap(),
        fenced,
    ] {
        let is_fenced = admission.fence().is_some();
        let primary =
            PrimaryAdmissionGate::from_repository(&AdmissionReader(Some(admission))).unwrap();
        let probe = Probe::new();
        let repository = RecordingRepository::appending(probe.clone());
        let clock = TestClock::fixed(fixed_timestamp());
        let running = RunningCommandCoordinator::spawn_with_operations(
            capacity(3),
            false,
            primary,
            Arc::new(DiscardApplicationCommitNotifications),
            Arc::new(NoopCommitTelemetry),
            move |_| Box::new(AuditOnlyCoordinatorOperations { repository, clock }),
        )
        .unwrap();
        let command = running.command_executor();
        let reserved = command.try_reserve_capacity().unwrap();
        let submission = reserved.begin_submission();
        if is_fenced {
            assert!(matches!(
                submission,
                Err(CommandExecutionAdmissionError::PrimaryFenced)
            ));
            assert!(matches!(
                block_on(running.control_plane_executor().reserve_capacity())
                    .unwrap()
                    .submit_replication_maintenance(),
                Err(ControlPlaneExecutionAdmissionError::PrimaryFenced)
            ));
        } else {
            drop(submission.unwrap());
        }
        drop(reserved);
        let audit = running.administration_audit_executor();
        block_on(
            block_on(audit.reserve_capacity())
                .unwrap()
                .submit(input(0xba))
                .unwrap()
                .completion(),
        )
        .unwrap();
        assert_eq!(probe.calls.load(Ordering::Acquire), 1);
        assert_eq!(
            command.lifecycle_state(),
            CoordinatorLifecycleState::Accepting
        );
        running.shutdown().unwrap();
    }
}

#[test]
fn startup_primary_admission_read_failure_never_defaults_to_active() {
    assert!(PrimaryAdmissionGate::from_repository(&AdmissionReader(None)).is_err());
}

#[test]
fn primary_pause_and_fence_refuse_authority_without_closing_required_audit() {
    let probe = Probe::new();
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(3),
        RecordingRepository::appending(probe.clone()),
        TestClock::fixed(fixed_timestamp()),
    )
    .unwrap();
    let audit = running.administration_audit_executor();
    let command = running.command_executor();
    let control = running.control_plane_executor();
    let held_command = command.try_reserve_capacity().unwrap();
    let held_control = block_on(control.reserve_capacity()).unwrap();
    let pause = command.submission_gate.primary.pause().unwrap();
    assert!(matches!(
        held_command.begin_submission(),
        Err(CommandExecutionAdmissionError::Draining)
    ));
    assert!(matches!(
        held_control.submit_replication_maintenance(),
        Err(ControlPlaneExecutionAdmissionError::Draining)
    ));
    block_on(
        block_on(audit.reserve_capacity())
            .unwrap()
            .submit(input(0xb1))
            .unwrap()
            .completion(),
    )
    .unwrap();
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    // Simulate only the coordinator's checked durable-outcome handoff. This test
    // proves scheduling/audit separation, not storage activation or fence proof.
    block_on(pause.drain()).finish_fenced();
    assert!(matches!(
        held_command.begin_submission(),
        Err(CommandExecutionAdmissionError::PrimaryFenced)
    ));
    assert!(matches!(
        block_on(control.reserve_capacity())
            .unwrap()
            .submit_replication_maintenance(),
        Err(ControlPlaneExecutionAdmissionError::PrimaryFenced)
    ));
    // Capacity remains available so the shared service can authenticate and
    // durably audit a denial before returning the typed fencing refusal.
    drop(command.try_reserve_capacity().unwrap());
    block_on(
        block_on(audit.reserve_capacity())
            .unwrap()
            .submit(input(0xb2))
            .unwrap()
            .completion(),
    )
    .unwrap();
    assert_eq!(probe.calls.load(Ordering::Acquire), 2);
    // The closed bootstrap terminal lane also retains its audit-only capability.
    drop(
        block_on(control.reserve_capacity())
            .unwrap()
            .into_audit_submission()
            .unwrap(),
    );
    assert_eq!(
        command.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );
    assert_eq!(
        control.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );
    drop(held_command);
    running.shutdown().unwrap();
}

#[test]
fn reserved_capacity_is_not_admitted_work_and_cancelled_pause_restores_submission() {
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(2),
        RecordingRepository::appending(Probe::new()),
        TestClock::fixed(fixed_timestamp()),
    )
    .unwrap();
    let command = running.command_executor();
    let reserved = command.try_reserve_capacity().unwrap();
    let admitted = reserved.begin_submission().unwrap();
    let mut draining = Box::pin(command.submission_gate.primary.pause().unwrap().drain());
    assert!(poll_once(draining.as_mut()).is_pending());
    drop(admitted);
    let Poll::Ready(drained) = poll_once(draining.as_mut()) else {
        panic!("last submission drained");
    };
    // The older capacity permit remains reserved but grants no queued work and
    // does not keep the pause waiting once its synchronous submission was dropped.
    drop(drained);
    drop(reserved.begin_submission().unwrap());
    drop(reserved);
    running.shutdown().unwrap();
}

#[test]
fn primary_fence_cancelled_before_enqueue_releases_only_its_pause() {
    use crate::control_plane::primary_fence::tests as fence;
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(3),
        RecordingRepository::appending(Probe::new()),
        TestClock::fixed(fixed_timestamp()),
    )
    .unwrap();
    let control = running.control_plane_executor();
    let command = running.command_executor();
    let primary = Arc::clone(&command.submission_gate.primary);
    let prior_sender = primary.begin().unwrap();
    let permit = block_on(control.reserve_capacity()).unwrap();
    let fixture = fence::fixture();
    let mut attempt =
        Box::pin(permit.submit_primary_fence(fence::preparation(&fixture, fence::request(12))));
    assert!(poll_once(attempt.as_mut()).is_pending());
    assert!(matches!(
        primary.begin(),
        Err(PrimaryAdmissionRefusal::Draining)
    ));
    drop(attempt);
    drop(
        primary
            .begin()
            .expect("cancelled unsent fence releases pause"),
    );
    drop(prior_sender);
    running.shutdown().unwrap();
}

#[test]
fn primary_fence_shutdown_during_sender_drain_refuses_enqueue_and_releases_pause() {
    use crate::control_plane::primary_fence::tests as fence;
    let running = RunningCommandCoordinator::start_audit_only(
        capacity(3),
        RecordingRepository::appending(Probe::new()),
        TestClock::fixed(fixed_timestamp()),
    )
    .unwrap();
    let control = running.control_plane_executor();
    let primary = Arc::clone(&control.submission_gate.primary);
    let prior_sender = primary.begin().unwrap();
    let permit = block_on(control.reserve_capacity()).unwrap();
    let fixture = fence::fixture();
    let mut attempt =
        Box::pin(permit.submit_primary_fence(fence::preparation(&fixture, fence::request(12))));
    assert!(poll_once(attempt.as_mut()).is_pending());
    // Exact close edge of shutdown, before accepted senders drain.
    control.submission_gate.close();
    drop(prior_sender);
    assert!(block_on(attempt).is_err());
    drop(primary.begin().expect("no durable fence was enqueued"));
    running.shutdown().unwrap();
}
