//! Explicit pre-cutover retry without reconnecting or advancing a follower at boot.
use super::*;
use crate::promotion_admission::{AuditedPromotion, PromotionController};
use crate::replication_bootstrap::BootstrapReceiverJobs;
use riffdb_policy::{CapabilityActivity, TransactionCurrentCapabilityFacts};
use riffdb_service::{FollowerPromotionPortError, PortCompletionSender, PromoteFollowerResult};
use riffdb_storage_api::{
    CapabilityLifecycleV1, CapabilityReader, PrimaryFenceRequestV1,
    ReplicationPromotionFailureV1 as Failure, ReplicationPromotionPhaseV1 as Phase,
    ReplicationPromotionReceiptV1 as Receipt, ReplicationPromotionSelectionV1 as Selection,
    ReplicationPromotionStepV1 as Step, StoredPromotionAdministrationV1,
};
use riffdb_storage_redb::RedbBootstrapReceiverRepository;

#[path = "daemon_promotion_retry_route.rs"]
mod route;

#[cfg(feature = "test-fixtures")]
static PROBE: std::sync::OnceLock<fn(&'static str)> = std::sync::OnceLock::new();

#[cfg(feature = "test-fixtures")]
pub(crate) fn install_probe(probe: fn(&'static str)) -> bool {
    PROBE.set(probe).is_ok()
}

fn edge(_point: &'static str) {
    #[cfg(feature = "test-fixtures")]
    if let Some(probe) = PROBE.get() {
        probe(_point);
    }
}

pub(super) struct Completion {
    sender: PortCompletionSender<PromoteFollowerResult, FollowerPromotionPortError>,
    result: PromoteFollowerResult,
}
impl Completion {
    pub(super) fn complete(self) {
        self.sender.complete(Ok(self.result));
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn await_retry(
    config: &ServerConfig,
    database: &DatabaseConfig,
    mut pending: replication_roles::PendingPromotionStartup,
    inputs: StartupValidationInputs,
    keys: &ProductionDigestKeys,
    clocks: &ProductionWallClocks,
    routes: &GrpcDatabaseRoutes,
    transport: &mut HostedGrpc,
    signal: &mut ProductionShutdownSignal,
) -> Result<Option<(replication_roles::PreparedMaintenanceStartup, Completion)>, DaemonError> {
    let source = database.follower().ok_or(DaemonError::MaintenanceDriver)?;
    loop {
        if pending
            .owner
            .pending_promotion_request()
            .map_err(DaemonError::MaintenanceStorage)?
            != Some(pending.request)
        {
            return Err(DaemonError::MaintenanceDriver);
        }
        let root = database.replication_receiver_root();
        let repository =
            tokio::task::spawn_blocking(move || RedbBootstrapReceiverRepository::open(&root))
                .await
                .map_err(|_| DaemonError::MaintenanceDriver)?
                .map_err(DaemonError::MaintenanceStorage)?;
        let jobs = BootstrapReceiverJobs::from_repository(repository);
        let opened = tokio::select! {
            biased;
            stopped = signal.received() => {
                jobs.drain().await.map_err(DaemonError::MaintenanceStorage)?;
                stopped.map_err(DaemonError::ShutdownSignal)?;
                return Ok(None);
            }
            opened = jobs.reopen_follower(database.database_path().to_path_buf(), inputs.clone(), source.lineage, source.hold, None) => opened,
        };
        let mut receiver = match opened {
            Ok(receiver) => receiver,
            Err(error) => {
                jobs.drain()
                    .await
                    .map_err(DaemonError::MaintenanceStorage)?;
                return Err(DaemonError::MaintenanceStorage(error));
            }
        };
        let (history, state, snapshot) = receiver
            .prepare_promotion_retry()
            .await
            .map_err(DaemonError::MaintenanceStorage)?;
        if history.lineage() != source.lineage
            || !matches!(state.attached_state(), Some((lineage, applied, _))
                if lineage == source.lineage && applied == history.tail())
        {
            return Err(DaemonError::MaintenanceDriver);
        }
        let frozen = pending
            .owner
            .promotion_receipts()
            .map_err(DaemonError::MaintenanceStorage)?
            .selection_for(pending.request.operation_id())
            .cloned();
        if frozen
            .as_ref()
            .is_some_and(|selection| selection.applied() != history.tail())
        {
            return Err(DaemonError::MaintenanceDriver);
        }
        let (controller, mut triggers) = PromotionController::channel();
        let route = Arc::new(route::RetryRoute::new(
            snapshot.clone(),
            config,
            database,
            keys,
            clocks,
            controller,
        )?);
        routes
            .replace(database.alias(), route.clone())
            .map_err(|_| DaemonError::GrpcConfiguration)?;
        // This marker explicitly does not claim application readiness.
        eprintln!("riffdb-promotion-retry-ready-v1");
        edge("retry-ready");
        let admitted = loop {
            let trigger = tokio::select! {
                biased;
                stopped = signal.received() => {
                    route.close();
                    drop(snapshot);
                    receiver.close().await.map_err(DaemonError::MaintenanceStorage)?;
                    stopped.map_err(DaemonError::ShutdownSignal)?;
                    return Ok(None);
                }
                completed = transport.completed() => {
                    route.close();
                    completed?;
                    return Err(DaemonError::TransportEnded);
                }
                trigger = triggers.recv() => trigger.ok_or(DaemonError::MaintenanceDriver)?,
            };
            if let Some(admitted) = trigger
                .audit_retry(&mut pending.owner, pending.request, database.environment())
                .map_err(DaemonError::MaintenanceStorage)?
            {
                break admitted;
            }
        };
        route.close();
        drop(route);
        triggers.close();
        // A bounded submission already accepted before route withdrawal gets
        // its own durable result. Never lose it by dropping the receiver.
        while let Some(queued) = triggers.recv().await {
            if let Some(mut queued) = queued
                .audit_retry(&mut pending.owner, pending.request, database.environment())
                .map_err(DaemonError::MaintenanceStorage)?
            {
                queued
                    .attempt
                    .advance(Step::FailedClosed(Failure::DrainFailed))
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                pending
                    .owner
                    .persist_promotion_receipt(&queued.attempt)
                    .map_err(DaemonError::MaintenanceStorage)?;
                queued
                    .completion
                    .complete(Err(FollowerPromotionPortError::Unavailable));
            }
        }
        let AuditedPromotion {
            mut attempt,
            authorization,
            ingress,
            completion,
        } = admitted;
        let current = snapshot.read_capability(authorization.principal().capability_id());
        drop(snapshot);
        let preparation = async {
            phase(&mut pending.owner, &mut attempt, Phase::Draining)?;
            receiver.close().await.map_err(|_| Failure::DrainFailed)?;
            jobs.drain().await.map_err(|_| Failure::DrainFailed)?;
            phase(&mut pending.owner, &mut attempt, Phase::Offline)?;
            let current = current
                .map_err(|_| Failure::StorageUnavailable)?
                .ok_or(Failure::AuthorizationDenied)?;
            let peer =
                tokio::time::timeout(Duration::from_secs(30), follower::connect(source.clone()))
                    .await
                    .map_err(|_| Failure::FenceUnavailable)?
                    .map_err(|_| Failure::FenceUnavailable)?;
            let request_id = ProductionIdentifierSources::new()
                .request_ids()
                .next_request_id()
                .map_err(|_| Failure::StorageUnavailable)?;
            let proof = tokio::time::timeout(
                Duration::from_secs(30),
                peer.authenticated_primary_fence(
                    PrimaryFenceRequestV1::new(
                        request_id,
                        pending.request.fence_operation_id(),
                        pending.request.target(),
                        pending.request.generation(),
                    ),
                    history.tail(),
                ),
            )
            .await
            .map_err(|_| Failure::FenceUnavailable)?
            .map_err(|_| Failure::FenceInvalid)?
            .into_evidence();
            let selection = match frozen {
                Some(selection) => {
                    if selection.evidence().fence() != proof.fence() {
                        return Err(Failure::FenceInvalid);
                    }
                    selection
                }
                None => Selection::new(pending.request, state, proof)
                    .map_err(|_| Failure::FenceInvalid)?,
            };
            let facts = TransactionCurrentCapabilityFacts::new(
                current.capability_id(),
                current.revision(),
                match current.lifecycle() {
                    CapabilityLifecycleV1::Active => CapabilityActivity::Active,
                    CapabilityLifecycleV1::Revoked { .. } => CapabilityActivity::Revoked,
                },
                current.database_id(),
                current.environment().clone(),
                current.principal_id().clone(),
                current.actor_kind(),
                current.audiences().to_vec(),
                current.issued_at(),
                current.expires_at(),
                current.grant().clone(),
            )
            .map_err(|_| Failure::AuthorizationDenied)?;
            let now = clocks
                .authorization()
                .now()
                .map_err(|_| Failure::StorageUnavailable)?;
            let (_, timestamp) = authorization
                .reauthorize(&facts, now)
                .map_err(|_| Failure::AuthorizationDenied)?
                .into_parts();
            attempt
                .record_selection(selection)
                .map_err(|_| Failure::FenceInvalid)?;
            pending
                .owner
                .persist_promotion_receipt(&attempt)
                .map_err(|_| Failure::StorageUnavailable)?;
            edge("selected");
            phase(&mut pending.owner, &mut attempt, Phase::CutoverPending)?;
            let record = StoredPromotionAdministrationV1::new(attempt.clone(), timestamp, ingress)
                .map_err(|_| Failure::CounterExhausted)?;
            let result = PromoteFollowerResult::from_committed_record(&record, false)
                .ok_or(Failure::FenceInvalid)?;
            Ok((record, result))
        };
        let outcome = tokio::select! {
            biased;
            stopped = signal.received() => {
                // Dropping a managed receiver step requests cancellation but
                // retains its capacity until storage releases the old engine.
                jobs.drain().await.map_err(DaemonError::MaintenanceStorage)?;
                stopped.map_err(DaemonError::ShutdownSignal)?;
                return Ok(None);
            }
            outcome = preparation => outcome,
        };
        let outcome = match outcome {
            Ok((record, result)) => {
                let mut owner = pending.owner;
                let validation = inputs.clone();
                let profile = config.redb_commit_profile();
                let cancellation = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let flag = cancellation.clone();
                let mut cutover = tokio::task::spawn_blocking(move || {
                    let outcome = (|| {
                        if flag.load(Ordering::Acquire) {
                            return Err(Failure::StorageUnavailable);
                        }
                        owner
                            .apply_promotion_cutover(&record)
                            .map_err(|_| Failure::StorageUnavailable)?;
                        edge("cutover-committed");
                        crate::startup::promotion::reconcile_promoted_redb_startup(
                            &mut owner, &record, validation, flag, profile,
                        )
                        .map_err(|_| Failure::ValidationFailed)
                    })();
                    (owner, outcome)
                });
                let returned = tokio::select! {
                    biased;
                    stopped = signal.received() => {
                        cancellation.store(true, Ordering::Release);
                        let (owner, outcome) = cutover.await.map_err(|_| DaemonError::MaintenanceDriver)?;
                        // Release any dormant source engine before its exclusive
                        // maintenance custody, including cancellation after validation.
                        drop(outcome);
                        drop(owner);
                        stopped.map_err(DaemonError::ShutdownSignal)?;
                        // A retained pending or committed row is reconciled at
                        // next startup; dropping completion reports uncertainty.
                        return Ok(None);
                    }
                    returned = &mut cutover => returned.map_err(|_| DaemonError::MaintenanceDriver)?,
                };
                pending.owner = returned.0;
                returned.1.map(|startup| (startup, result))
            }
            Err(error) => Err(error),
        };
        match outcome {
            Ok((startup, result)) => {
                let reconciliation = pending
                    .owner
                    .reconcile_for_startup()
                    .map_err(DaemonError::MaintenanceStorage)?;
                return Ok(Some((
                    replication_roles::PreparedMaintenanceStartup {
                        owner: pending.owner,
                        reconciliation,
                        promoted: Some(startup),
                    },
                    Completion {
                        sender: completion,
                        result,
                    },
                )));
            }
            Err(failure) => {
                let inventory = pending
                    .owner
                    .promotion_receipts()
                    .map_err(DaemonError::MaintenanceStorage)?;
                let mut retained = inventory
                    .receipts()
                    .iter()
                    .find(|row| row.request_id() == attempt.request_id())
                    .cloned()
                    .ok_or(DaemonError::MaintenanceDriver)?;
                let uncertain = matches!(
                    retained.phase(),
                    Phase::CutoverPending
                        | Phase::CutoverCommitted
                        | Phase::Validated
                        | Phase::Succeeded
                );
                if !retained.is_terminal() {
                    let step = if uncertain {
                        Step::Uncertain(Failure::StorageUnavailable)
                    } else if failure == Failure::AuthorizationDenied {
                        Step::Denied(failure)
                    } else {
                        Step::FailedClosed(failure)
                    };
                    retained
                        .advance(step)
                        .map_err(|_| DaemonError::MaintenanceDriver)?;
                    pending
                        .owner
                        .persist_promotion_receipt(&retained)
                        .map_err(DaemonError::MaintenanceStorage)?;
                }
                completion.complete(Err(if uncertain {
                    FollowerPromotionPortError::OutcomeUnknown
                } else if failure == Failure::AuthorizationDenied {
                    FollowerPromotionPortError::AuthorizationDenied
                } else {
                    FollowerPromotionPortError::Unavailable
                }));
                if uncertain {
                    return Err(DaemonError::MaintenanceDriver);
                }
            }
        }
    }
}

fn phase(
    owner: &mut RedbMaintenanceStorage,
    attempt: &mut Receipt,
    phase: Phase,
) -> Result<(), Failure> {
    attempt
        .advance(Step::Phase(phase))
        .map_err(|_| Failure::StorageUnavailable)?;
    owner
        .persist_promotion_receipt(attempt)
        .map_err(|_| Failure::StorageUnavailable)?;
    match phase {
        Phase::Draining => edge("draining"),
        Phase::Offline => edge("offline"),
        Phase::CutoverPending => edge("cutover-pending"),
        _ => {}
    }
    Ok(())
}
