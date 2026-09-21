// req: REP-005, REC-001, STO-012
use super::*;
use crate::ControlPlaneExecutionErrorKind;
use crate::control_plane::primary_fence::tests::{self as fence, ActorOutcome};

struct FenceOperations {
    fixture: riffdb_testkit::authorization::AuthorizationFixture,
    outcome: ActorOutcome,
    lifecycle: ActorLifecyclePublisher,
    entered: std_mpsc::Sender<()>,
    release: std_mpsc::Receiver<()>,
    audit: RecordingRepository,
    clock: TestClock,
}
impl CoordinatorActorOperations for FenceOperations {
    fn fence_primary(
        &mut self,
        preparation: AuthorizedPrimaryFencePreparation,
    ) -> Result<PrimaryFenceExecutionResult, ControlPlaneExecutionError> {
        self.entered.send(()).unwrap();
        self.release.recv().unwrap();
        fence::drive_for_actor(&self.fixture, preparation, self.outcome, &self.lifecycle)
    }
    fn append_audit(
        &mut self,
        input: &dyn AdministrationAuditInputView,
    ) -> Result<(), AdministrationAuditExecutionError> {
        append_administration_audit(&mut self.audit, &self.clock, input)
    }
    fn drive_command(&mut self, _: CommandExecutionPreparation) -> LocalCommandFuture<'_> {
        Box::pin(async { Err(CommandExecutionError::coordinator_stopped()) })
    }

    fn inspect_idempotency(
        &mut self,
        _: PreparedCommandIdempotencyInspection,
    ) -> Result<InspectedCommandIdempotency, CommandIdempotencyInspectionError> {
        Err(CommandIdempotencyInspectionError::coordinator_stopped())
    }

    fn drive_read_only(
        &mut self,
        _: ReadOnlyExecutionPreparation,
    ) -> Result<ReadOnlyExecutionResult, CommandExecutionError> {
        Err(CommandExecutionError::coordinator_stopped())
    }

    fn deploy_catalog(
        &mut self,
        _: CatalogDeploymentPreparation,
    ) -> Result<CatalogDeploymentResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn deploy_query_module(
        &mut self,
        _: QueryModuleDeploymentPreparation,
    ) -> Result<QueryModuleDeploymentResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn publish_reactive_module(
        &mut self,
        _: ReactiveModulePublicationPreparation,
    ) -> Result<ReactiveModulePublicationExecutionResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn create_capability(
        &mut self,
        _: CapabilityCreatePreparation,
    ) -> Result<CapabilityCreateExecutionResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn administer_replication(
        &mut self,
        _: AuthorizedReplicationAdministrationPreparation,
    ) -> Result<ReplicationAdministrationExecutionResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }
    fn maintain_replication(
        &mut self,
    ) -> Result<ReplicationRegistrationMaintenanceResultV1, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn revoke_capability(
        &mut self,
        _: CapabilityRevokePreparation,
    ) -> Result<CapabilityRevokeExecutionResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn bootstrap_capability(
        &mut self,
        _: CapabilityBootstrapPreparation,
    ) -> Result<CapabilityBootstrapExecutionResult, ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }

    fn append_bootstrap_terminal(
        &mut self,
        _: CapabilityBootstrapTerminalPreparation,
    ) -> Result<(), ControlPlaneExecutionError> {
        Err(ControlPlaneExecutionError::coordinator_stopped())
    }
}

struct Harness {
    running: Option<RunningCommandCoordinator>,
    preparation: Option<AuthorizedPrimaryFencePreparation>,
    entered: std_mpsc::Receiver<()>,
    release: Option<std_mpsc::Sender<()>>,
    audit: Probe,
}
impl Harness {
    fn new(outcome: ActorOutcome, fenced: bool) -> Self {
        let fixture = fence::fixture();
        let preparation = fence::preparation(&fixture, fence::request(12));
        let (entered, entered_rx) = std_mpsc::channel();
        let (release, release_rx) = std_mpsc::channel();
        let audit = Probe::new();
        let repository = RecordingRepository::appending(audit.clone());
        let running = RunningCommandCoordinator::spawn_with_operations(
            capacity(4),
            false,
            PrimaryAdmissionGate::new(),
            Arc::new(DiscardApplicationCommitNotifications),
            Arc::new(NoopCommitTelemetry),
            move |lifecycle| {
                Box::new(FenceOperations {
                    fixture,
                    outcome,
                    lifecycle,
                    entered,
                    release: release_rx,
                    audit: repository,
                    clock: TestClock::fixed(fixed_timestamp()),
                })
            },
        )
        .unwrap();
        if fenced {
            let primary = &running.command_executor().submission_gate.primary;
            block_on(primary.pause().unwrap().drain()).finish_fenced();
        }
        Self {
            running: Some(running),
            preparation: Some(preparation),
            entered: entered_rx,
            release: Some(release),
            audit,
        }
    }
    fn running(&self) -> &RunningCommandCoordinator {
        self.running.as_ref().unwrap()
    }
    fn submit(&mut self) -> PrimaryFenceReceipt {
        let control = self.running().control_plane_executor();
        let permit = block_on(control.reserve_capacity()).unwrap();
        block_on(permit.submit_primary_fence(self.preparation.take().unwrap())).unwrap()
    }
    fn unblock(&mut self) {
        self.release.take().unwrap().send(()).unwrap();
    }
    fn audit_barrier(&self) {
        let executor = self.running().administration_audit_executor();
        block_on(
            block_on(executor.reserve_capacity())
                .unwrap()
                .submit(input(0xc1))
                .unwrap()
                .completion(),
        )
        .unwrap();
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        drop(self.running.take());
    }
}

#[test]
fn primary_fence_queue_orders_prior_senders_and_keeps_owner_after_response_drop() {
    let mut h = Harness::new(ActorOutcome::Applied, false);
    let command = h.running().command_executor();
    let primary = Arc::clone(&command.submission_gate.primary);
    let prior_sender = primary.begin().unwrap();
    let control = h.running().control_plane_executor();
    let permit = block_on(control.reserve_capacity()).unwrap();
    let mut attempt = Box::pin(permit.submit_primary_fence(h.preparation.take().unwrap()));
    assert!(poll_once(attempt.as_mut()).is_pending());
    assert!(h.entered.try_recv().is_err());
    h.audit_barrier(); // audit remains live while prior senders drain
    drop(prior_sender);
    let receipt = block_on(attempt).unwrap();
    h.entered.recv().unwrap();
    assert_eq!(h.audit.calls.load(Ordering::Acquire), 1);
    assert!(matches!(
        primary.begin(),
        Err(PrimaryAdmissionRefusal::Draining)
    ));
    drop(receipt); // the actor, not the response, owns the pause
    assert!(matches!(
        primary.begin(),
        Err(PrimaryAdmissionRefusal::Draining)
    ));
    h.unblock();
    h.audit_barrier(); // explicit FIFO completion witness, no sleep/poll loop
    assert!(matches!(
        primary.begin(),
        Err(PrimaryAdmissionRefusal::Fenced)
    ));
    assert_eq!(
        command.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );
}

#[test]
fn primary_fence_queue_refusals_release_only_active_pause_and_keep_permanent_fence() {
    for fenced in [false, true] {
        for outcome in [
            ActorOutcome::Refused,
            ActorOutcome::Unavailable,
            ActorOutcome::Denied,
        ] {
            let mut h = Harness::new(outcome, fenced);
            let primary = Arc::clone(&h.running().command_executor().submission_gate.primary);
            let receipt = h.submit();
            h.entered.recv().unwrap();
            h.unblock();
            let result = block_on(receipt.completion());
            match outcome {
                ActorOutcome::Refused => assert!(matches!(
                    result.unwrap().outcome(),
                    PrimaryFenceResultV1::Refused(_)
                )),
                ActorOutcome::Unavailable => assert_eq!(
                    result.unwrap_err().kind(),
                    ControlPlaneExecutionErrorKind::StorageUnavailable
                ),
                ActorOutcome::Denied => assert_eq!(
                    result.unwrap_err().kind(),
                    ControlPlaneExecutionErrorKind::AuthorizationDenied
                ),
                _ => unreachable!(),
            }
            if fenced {
                assert!(matches!(
                    primary.begin(),
                    Err(PrimaryAdmissionRefusal::Fenced)
                ));
            } else {
                drop(
                    primary
                        .begin()
                        .expect("certain refusal releases only nondurable pause"),
                );
            }
            h.audit_barrier();
        }
    }
}

#[test]
fn primary_fence_queue_replay_retains_original_receipt_and_does_not_reopen() {
    let mut h = Harness::new(ActorOutcome::Replayed, true);
    let command = h.running().command_executor();
    let receipt = h.submit();
    h.entered.recv().unwrap();
    h.unblock();
    let result = block_on(receipt.completion()).unwrap();
    let PrimaryFenceResultV1::Replayed(record) = result.outcome() else {
        panic!("exact retry");
    };
    assert_eq!(record.request_id(), fence::request(6).request_id());
    assert_ne!(record.request_id(), fence::request(12).request_id());
    assert!(matches!(
        command.submission_gate.primary.begin(),
        Err(PrimaryAdmissionRefusal::Fenced)
    ));
    h.audit_barrier();
}

#[test]
fn primary_fence_queue_uncertainty_and_corruption_never_reopen_authority() {
    for outcome in [ActorOutcome::Unknown, ActorOutcome::Corrupt] {
        let mut h = Harness::new(outcome, false);
        let command = h.running().command_executor();
        let receipt = h.submit();
        h.entered.recv().unwrap();
        h.unblock();
        let error = block_on(receipt.completion()).unwrap_err();
        if matches!(outcome, ActorOutcome::Unknown) {
            assert_eq!(error.kind(), ControlPlaneExecutionErrorKind::OutcomeUnknown);
            assert_eq!(command.lifecycle_state(), CoordinatorLifecycleState::Fenced);
            assert!(matches!(
                command.submission_gate.primary.begin(),
                Err(PrimaryAdmissionRefusal::Fenced)
            ));
        } else {
            assert_eq!(error.kind(), ControlPlaneExecutionErrorKind::InternalDefect);
            assert_eq!(
                command.lifecycle_state(),
                CoordinatorLifecycleState::Stopped
            );
        }
        assert!(command.try_reserve_capacity().is_err());
    }
}
