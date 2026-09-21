//! Explicit live follower cutover under the daemon's exclusive maintenance owner.
use super::*;
use crate::promotion_admission::{AuditedPromotion, PromotionTrigger};
use futures_util::FutureExt;
use riffdb_policy::{
    AuthorizationClock, AuthorizedReplicationPromotionPreparation, CapabilityActivity,
    TransactionCurrentCapabilityFacts,
};
use riffdb_service::{FollowerPromotionPortError, PromoteFollowerResult};
use riffdb_storage_api::{
    CapabilityLifecycleV1, CapabilityReader, PrimaryFenceRequestV1,
    ReplicationPromotionFailureV1 as Failure, ReplicationPromotionPhaseV1 as Phase,
    ReplicationPromotionReceiptV1 as Receipt, ReplicationPromotionSelectionV1 as Selection,
    ReplicationPromotionStepV1 as Step, StoredPromotionAdministrationV1,
};
use riffdb_types::ServiceIngressKindV1;

/// True means a new source graph was installed. A pre-drain refusal leaves the
/// existing follower intact; a failure after drain requires quarantine.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    config: &ServerConfig,
    started_at: riffdb_types::Timestamp,
    database_index: usize,
    trigger: PromotionTrigger,
    routes: &GrpcDatabaseRoutes,
    hosted_mcp: &Option<HostedMcp>,
    graphs: &mut [MultiDatabaseGraph],
) -> Result<bool, DaemonError> {
    let database = config
        .databases()
        .get(database_index)
        .ok_or(DaemonError::MaintenanceDriver)?;
    let generation = graphs
        .get_mut(database_index)
        .ok_or(DaemonError::MaintenanceDriver)?;
    let source = database.follower().ok_or(DaemonError::MaintenanceDriver)?;
    let expected = riffdb_types::ReplicationFollowerAuditTargetV1::new(
        source.lineage.database_id(),
        source.lineage.history_incarnation(),
        source.lineage.leadership_epoch(),
        source.hold,
    )
    .ok_or(DaemonError::MaintenanceDriver)?;
    let storage = generation.maintenance.storage();
    let audited = trigger
        .audit(
            &mut *storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?,
            expected,
            database.environment(),
        )
        .map_err(DaemonError::MaintenanceStorage)?;
    let Some(AuditedPromotion {
        mut attempt,
        authorization,
        ingress,
        completion,
    }) = audited
    else {
        return Ok(false);
    };
    // A panic cannot escape the per-database supervisor or produce success.
    // The retained phase determines whether the failure is closed or uncertain.
    let outcome = std::panic::AssertUnwindSafe(execute(
        config,
        database,
        started_at,
        generation,
        routes,
        hosted_mcp,
        &mut attempt,
        *authorization,
        ingress,
    ))
    .catch_unwind()
    .await
    .unwrap_or(Err(Failure::StorageUnavailable));
    match outcome {
        Ok(result) => {
            completion.complete(Ok(result));
            Ok(true)
        }
        Err(failure) => {
            let persisted = (|| {
                let mut owner = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
                // Reconciliation may have advanced the durable row before a
                // later graph build failed. Never overwrite a later phase.
                let inventory = owner
                    .promotion_receipts()
                    .map_err(DaemonError::MaintenanceStorage)?;
                attempt = inventory
                    .receipts()
                    .iter()
                    .find(|r| r.request_id() == attempt.request_id())
                    .cloned()
                    .ok_or(DaemonError::MaintenanceDriver)?;
                let uncertain = matches!(
                    attempt.phase(),
                    Phase::CutoverPending
                        | Phase::CutoverCommitted
                        | Phase::Validated
                        | Phase::Succeeded
                );
                if !attempt.is_terminal() {
                    let step = if uncertain {
                        Step::Uncertain(if failure == Failure::ValidationFailed {
                            failure
                        } else {
                            Failure::StorageUnavailable
                        })
                    } else if failure == Failure::AuthorizationDenied {
                        Step::Denied(failure)
                    } else {
                        Step::FailedClosed(failure)
                    };
                    attempt
                        .advance(step)
                        .map_err(|_| DaemonError::MaintenanceDriver)?;
                    owner
                        .persist_promotion_receipt(&attempt)
                        .map_err(DaemonError::MaintenanceStorage)?;
                }
                Ok(uncertain)
            })();
            let reply = match &persisted {
                Ok(true) | Err(_) => FollowerPromotionPortError::OutcomeUnknown,
                Ok(false) if failure == Failure::AuthorizationDenied => {
                    FollowerPromotionPortError::AuthorizationDenied
                }
                Ok(false) => FollowerPromotionPortError::Unavailable,
            };
            completion.complete(Err(reply));
            persisted?;
            if generation.follower.is_some() {
                Ok(false)
            } else {
                Err(DaemonError::MaintenanceDriver)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    config: &ServerConfig,
    database: &DatabaseConfig,
    started_at: riffdb_types::Timestamp,
    generation: &mut MultiDatabaseGraph,
    routes: &GrpcDatabaseRoutes,
    hosted_mcp: &Option<HostedMcp>,
    attempt: &mut Receipt,
    authorization: AuthorizedReplicationPromotionPreparation,
    ingress: ServiceIngressKindV1,
) -> Result<PromoteFollowerResult, Failure> {
    let source = database.follower().ok_or(Failure::FenceInvalid)?;
    if generation.follower.is_none() {
        return Err(Failure::FenceInvalid);
    }
    let prepared = GenerationInputs::load(config).map_err(|_| Failure::StorageUnavailable)?;
    let before = generation
        .lifecycle
        .follower_position()
        .ok_or(Failure::DrainFailed)?;
    let request = attempt.request();
    let owner = generation.maintenance.storage();
    let frozen = owner
        .lock()
        .map_err(|_| Failure::StorageUnavailable)?
        .promotion_receipts()
        .map_err(|_| Failure::StorageUnavailable)?
        .selection_for(request.operation_id())
        .cloned();
    // A separate configured connection leaves the receiver's single active
    // stream untouched while first-time fence proof is obtained.
    let peer = tokio::time::timeout(Duration::from_secs(30), follower::connect(source.clone()))
        .await
        .map_err(|_| Failure::FenceUnavailable)?
        .map_err(|_| Failure::FenceUnavailable)?;
    let proof_request = || {
        prepared
            .identifiers
            .request_ids()
            .next_request_id()
            .map(|id| {
                PrimaryFenceRequestV1::new(
                    id,
                    request.fence_operation_id(),
                    request.target(),
                    request.generation(),
                )
            })
            .map_err(|_| Failure::StorageUnavailable)
    };
    let initial = tokio::time::timeout(
        Duration::from_secs(30),
        peer.authenticated_primary_fence(proof_request()?, before),
    )
    .await
    .map_err(|_| Failure::FenceUnavailable)?
    .map_err(|_| Failure::FenceInvalid)?
    .into_evidence();
    if frozen
        .as_ref()
        .is_some_and(|s| s.evidence().fence() != initial.fence())
    {
        return Err(Failure::FenceInvalid);
    }
    phase(&owner, attempt, Phase::Draining)?;
    generation.generation = generation
        .generation
        .checked_add(1)
        .ok_or(Failure::CounterExhausted)?;
    generation.lifecycle.stop();
    let (initializing, _activator, issuer) = RiffDbService::begin_initialization();
    let offline = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        initializing,
        issuer,
        RuntimeRoutingState::new(),
        generation.maintenance_lifecycle.clone(),
    ));
    routes
        .replace(&generation.alias, offline.clone())
        .map_err(|_| Failure::DrainFailed)?;
    generation.lifecycle = offline;
    if let Some(hosted) = hosted_mcp {
        hosted
            .suspend(&generation.alias)
            .map_err(|_| Failure::DrainFailed)?;
    }
    generation
        .follower
        .take()
        .ok_or(Failure::DrainFailed)?
        .shutdown()
        .await
        .map_err(|_| Failure::DrainFailed)?;
    phase(&owner, attempt, Phase::Offline)?;
    let path = database.database_path().to_path_buf();
    let inputs = prepared.startup_inputs.clone();
    let capability = authorization.principal().capability_id();
    let (state, current) = tokio::task::spawn_blocking(move || {
        let checked = crate::startup::open_redb_follower_startup(&path, inputs)
            .map_err(|_| Failure::ValidationFailed)?;
        let (_, state, snapshot) = checked
            .applier
            .capture_read_progress_snapshot()
            .map_err(|_| Failure::StorageUnavailable)?;
        let current = snapshot
            .read_capability(capability)
            .map_err(|_| Failure::StorageUnavailable)?;
        drop(snapshot);
        checked.applier.close().map_err(|_| Failure::DrainFailed)?;
        Ok::<_, Failure>((state, current))
    })
    .await
    .map_err(|_| Failure::StorageUnavailable)??;
    let (_, applied, _) = state.attached_state().ok_or(Failure::FenceInvalid)?;
    let final_proof = tokio::time::timeout(
        Duration::from_secs(30),
        peer.authenticated_primary_fence(proof_request()?, applied),
    )
    .await
    .map_err(|_| Failure::FenceUnavailable)?
    .map_err(|_| Failure::FenceInvalid)?
    .into_evidence();
    if final_proof.fence() != initial.fence() {
        return Err(Failure::FenceInvalid);
    }
    let selection = match frozen {
        Some(selected) => {
            if selected.applied() != applied || selected.evidence().fence() != final_proof.fence() {
                return Err(Failure::FenceInvalid);
            }
            // Later administration (including stream audit) may advance the
            // source tail; the immutable original fence/selection stays exact.
            selected
        }
        None => {
            Selection::new(request, state, final_proof).map_err(|_| Failure::CounterExhausted)?
        }
    };
    let current = current.ok_or(Failure::AuthorizationDenied)?;
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
    let now = prepared
        .clocks
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
    owner
        .lock()
        .map_err(|_| Failure::StorageUnavailable)?
        .persist_promotion_receipt(attempt)
        .map_err(|_| Failure::StorageUnavailable)?;
    phase(&owner, attempt, Phase::CutoverPending)?;
    let record = StoredPromotionAdministrationV1::new(attempt.clone(), timestamp, ingress)
        .map_err(|_| Failure::CounterExhausted)?;
    let result = PromoteFollowerResult::from_committed_record(&record, false)
        .ok_or(Failure::FenceInvalid)?;
    let inputs = prepared.startup_inputs.clone();
    let profile = config.redb_commit_profile();
    let startup = tokio::task::spawn_blocking(move || {
        let mut owner = owner.lock().map_err(|_| Failure::StorageUnavailable)?;
        owner
            .apply_promotion_cutover(&record)
            .map_err(|_| Failure::StorageUnavailable)?;
        crate::startup::promotion::reconcile_promoted_redb_startup(
            &mut owner,
            &record,
            inputs,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            profile,
        )
        .map_err(|_| Failure::ValidationFailed)
    })
    .await
    .map_err(|_| Failure::StorageUnavailable)??;
    let (initializing, activator, issuer) = RiffDbService::begin_initialization();
    let routing = RuntimeRoutingState::new();
    let lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        initializing,
        issuer,
        routing.clone(),
        generation.maintenance_lifecycle.clone(),
    ));
    let build = build_info(&startup).map_err(|_| Failure::ValidationFailed)?;
    let graph = ProductionGraphBuilder::new(
        startup,
        activator,
        prepared.digest_keys,
        config,
        database,
        started_at,
        build,
        prepared.identifiers,
        prepared.clocks,
        lifecycle.clone(),
        generation.maintenance.clone(),
    )
    .build()
    .map_err(|_| Failure::ValidationFailed)?;
    // Retain the graph before fallible public installation, so the caller's
    // quarantine path can drain it on every error.
    generation.graph = Some(graph);
    generation.lifecycle = lifecycle;
    generation.routing = routing;
    if let Some(hosted) = hosted_mcp {
        let dependencies = generation
            .hosted_mcp_dependencies()
            .ok_or(Failure::ValidationFailed)?;
        hosted
            .replace(&generation.alias, dependencies)
            .map_err(|_| Failure::ValidationFailed)?;
    }
    routes
        .replace(&generation.alias, generation.lifecycle.clone())
        .map_err(|_| Failure::ValidationFailed)?;
    Ok(result)
}

fn phase(
    owner: &crate::maintenance_adapter::SharedMaintenanceStorage,
    attempt: &mut Receipt,
    next: Phase,
) -> Result<(), Failure> {
    attempt
        .advance(Step::Phase(next))
        .map_err(|_| Failure::StorageUnavailable)?;
    owner
        .lock()
        .map_err(|_| Failure::StorageUnavailable)?
        .persist_promotion_receipt(attempt)
        .map_err(|_| Failure::StorageUnavailable)
}
