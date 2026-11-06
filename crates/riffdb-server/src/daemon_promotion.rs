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
    let generation = graphs
        .get_mut(database_index)
        .ok_or(DaemonError::MaintenanceDriver)?;
    if generation.deprovisioned {
        return Ok(false);
    }
    let database = config
        .databases()
        .iter()
        .find(|db| db.alias() == &generation.alias)
        .ok_or(DaemonError::MaintenanceDriver)?;
    let source = database.follower().ok_or(DaemonError::MaintenanceDriver)?;
    let expected = riffdb_types::ReplicationFollowerAuditTargetV1::new(
        source.lineage.database_id(),
        source.lineage.history_incarnation(),
        source.lineage.leadership_epoch(),
        source.hold,
    )
    .ok_or(DaemonError::MaintenanceDriver)?;
    let storage = generation
        .maintenance
        .as_ref()
        .ok_or(DaemonError::MaintenanceDriver)?
        .storage();
    let audited = trigger
        .audit(
            &mut storage
                .lock()
                .map_err(|_| DaemonError::MaintenanceDriver)?
                .ordinary_mut()
                .map_err(|_| DaemonError::MaintenanceDriver)?,
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
                let mut owner_slot = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
                let mut owner = owner_slot
                    .ordinary_mut()
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                // Reconciliation may have advanced the durable row before a
                // later graph build failed. Never overwrite a later phase.
                let inventory = owner
                    .ordinary_ref()
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
    let before_history = generation
        .lifecycle
        .follower_history()
        .ok_or(Failure::DrainFailed)?;
    let before = before_history.tail();
    let request = attempt.request();
    let maintenance = generation
        .maintenance
        .as_ref()
        .ok_or(Failure::StorageUnavailable)?
        .clone();
    let owner = maintenance.storage();
    let frozen = owner
        .lock()
        .map_err(|_| Failure::StorageUnavailable)?
        .ordinary_ref()
        .map_err(|_| Failure::StorageUnavailable)?
        .promotion_receipts()
        .map_err(|_| Failure::StorageUnavailable)?
        .selection_for(request.operation_id())
        .cloned();
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
    let direct_primary = async {
        // A separate configured connection leaves the receiver's single active
        // stream untouched while first-time fence proof is obtained.
        let peer = tokio::time::timeout(Duration::from_secs(30), follower::connect(source.clone()))
            .await
            .map_err(|_| Failure::FenceUnavailable)?
            .map_err(|_| Failure::FenceUnavailable)?;
        let initial = tokio::time::timeout(
            Duration::from_secs(30),
            peer.authenticated_primary_fence(proof_request()?, before),
        )
        .await
        .map_err(|_| Failure::FenceUnavailable)?
        .map_err(|error| match error {
            riffdb_service::ReplicationFailure::Unavailable => Failure::FenceUnavailable,
            _ => Failure::FenceInvalid,
        })?
        .into_evidence();
        Ok::<_, Failure>((peer, initial))
    }
    .await;
    let (direct_peer, initial) = match direct_primary {
        Ok((peer, initial)) => (Some(peer), initial),
        Err(Failure::FenceUnavailable) if !database.replication().voters.is_empty() => {
            let evidence = collect_quorum_fence_evidence(
                database,
                source,
                &owner,
                &prepared.identifiers,
                attempt,
                before_history,
            )
            .await?;
            (None, evidence)
        }
        Err(error) => return Err(error),
    };
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
    let (after_history, state, current) = tokio::task::spawn_blocking(move || {
        let checked = crate::startup::open_redb_follower_startup(&path, inputs)
            .map_err(|_| Failure::ValidationFailed)?;
        let (history, state, snapshot) = checked
            .applier
            .capture_read_progress_snapshot()
            .map_err(|_| Failure::StorageUnavailable)?;
        let current = snapshot
            .read_capability(capability)
            .map_err(|_| Failure::StorageUnavailable)?;
        drop(snapshot);
        checked.applier.close().map_err(|_| Failure::DrainFailed)?;
        Ok::<_, Failure>((history, state, current))
    })
    .await
    .map_err(|_| Failure::StorageUnavailable)??;
    let (_, applied, _) = state.attached_state().ok_or(Failure::FenceInvalid)?;
    let final_proof = match direct_peer {
        Some(peer) => tokio::time::timeout(
            Duration::from_secs(30),
            peer.authenticated_primary_fence(proof_request()?, applied),
        )
        .await
        .map_err(|_| Failure::FenceUnavailable)?
        .map_err(|_| Failure::FenceInvalid)?
        .into_evidence(),
        None => {
            if applied == before {
                initial.clone()
            } else {
                collect_quorum_fence_evidence(
                    database,
                    source,
                    &owner,
                    &prepared.identifiers,
                    attempt,
                    after_history,
                )
                .await?
            }
        }
    };
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
        .ordinary_mut()
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
        let mut owner_slot = owner.lock().map_err(|_| Failure::StorageUnavailable)?;
        let mut owner = owner_slot
            .ordinary_mut()
            .map_err(|_| Failure::StorageUnavailable)?;
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
        maintenance,
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
        .ordinary_mut()
        .map_err(|_| Failure::StorageUnavailable)?
        .persist_promotion_receipt(attempt)
        .map_err(|_| Failure::StorageUnavailable)
}

pub(crate) fn parse_voter_entry(
    entry: &str,
) -> Option<(
    riffdb_storage_api::ReplicationSourceHoldIdV1,
    Option<riffdb_config::CanonicalHttpsEndpoint>,
    Option<riffdb_config::TlsServerIdentity>,
)> {
    let (hold_hex, rest) = match entry.split_once('@') {
        Some((h, r)) => (h, Some(r)),
        None => (entry, None),
    };
    let hold_bytes = riffdb_types::parse_hex16(hold_hex).ok()?;
    let hold_id = riffdb_storage_api::ReplicationSourceHoldIdV1::new(hold_bytes)?;
    let Some(rest) = rest else {
        return Some((hold_id, None, None));
    };
    let (endpoint_str, server_name_str) = match rest.split_once('#') {
        Some((e, s)) => (e, Some(s)),
        None => (rest, None),
    };
    let endpoint = riffdb_config::CanonicalHttpsEndpoint::parse(endpoint_str).ok()?;
    let server_name = match server_name_str {
        Some(s) => Some(riffdb_config::TlsServerIdentity::parse(s).ok()?),
        None => None,
    };
    Some((hold_id, Some(endpoint), server_name))
}

pub(crate) async fn collect_quorum_fence_evidence(
    database: &DatabaseConfig,
    source: &crate::config::FollowerSourceConfig,
    owner: &crate::maintenance_adapter::SharedMaintenanceStorage,
    identifiers: &ProductionIdentifierSources,
    attempt: &Receipt,
    follower_history: riffdb_storage_api::ChangelogHistoryStateV3,
) -> Result<riffdb_storage_api::PrimaryFenceSourceEvidenceV1, Failure> {
    let applied = follower_history.tail();
    let request = attempt.request();
    let configured_voters = &database.replication().voters;
    if configured_voters.is_empty()
        || configured_voters.len() > riffdb_types::MAX_REPLICATION_VOTERS_V1
    {
        return Err(Failure::FenceInvalid);
    }
    let mut voter_ids = Vec::with_capacity(configured_voters.len());
    let mut remote_peers = Vec::new();
    for entry in configured_voters {
        let (hold_id, endpoint, server_name) =
            parse_voter_entry(entry).ok_or(Failure::FenceInvalid)?;
        if voter_ids.contains(&hold_id) {
            return Err(Failure::FenceInvalid);
        }
        voter_ids.push(hold_id);
        if hold_id != source.hold
            && let Some(endpoint) = endpoint
        {
            remote_peers.push((hold_id, endpoint, server_name));
        }
    }
    if !voter_ids.contains(&source.hold) {
        return Err(Failure::FenceInvalid);
    }
    let proposed_epoch = source
        .lineage
        .leadership_epoch()
        .checked_next()
        .ok_or(Failure::CounterExhausted)?;
    let seq = std::num::NonZeroU64::new(applied.sequence().get()).ok_or(Failure::FenceInvalid)?;
    let candidate_pos = riffdb_types::ReplicationEpochVotePositionV1::new(
        seq,
        applied.history_hash(),
        applied.frontier(),
    );
    let self_vote = {
        let mut slot = owner.lock().map_err(|_| Failure::StorageUnavailable)?;
        let storage = slot
            .ordinary_mut()
            .map_err(|_| Failure::StorageUnavailable)?;
        let existing = storage
            .ordinary_ref()
            .read_promised_leadership_epoch()
            .map_err(|_| Failure::StorageUnavailable)?;
        let can_self_promise = existing.is_none_or(|(promised, candidate)| {
            proposed_epoch > promised || (proposed_epoch == promised && candidate == source.hold)
        });
        if !can_self_promise {
            return Err(Failure::FenceInvalid);
        }
        let ok = storage
            .ordinary_ref()
            .persist_promised_leadership_epoch(proposed_epoch, source.hold)
            .map_err(|_| Failure::StorageUnavailable)?;
        if !ok {
            return Err(Failure::FenceInvalid);
        }
        riffdb_types::ReplicationEpochVoteV1::new(
            source.hold,
            source.lineage.database_id(),
            source.lineage.history_incarnation(),
            proposed_epoch,
            source.hold,
            candidate_pos,
            candidate_pos,
            true,
        )
        .ok_or(Failure::FenceInvalid)?
    };
    let mut collected_votes = vec![self_vote];
    let mut saw_lagging_or_higher_epoch_rejection = false;
    for (peer_hold, endpoint, server_name) in remote_peers {
        let req_id = identifiers
            .request_ids()
            .next_request_id()
            .map_err(|_| Failure::StorageUnavailable)?;
        let vote_request = riffdb_types::ReplicationEpochVoteRequestV1::new(
            req_id,
            request.target(),
            proposed_epoch,
            candidate_pos,
        )
        .ok_or(Failure::FenceInvalid)?;
        let identity = server_name.unwrap_or_else(|| source.tls.expected_server_identity().clone());
        let Ok(tls) = riffdb_config::TlsClientConfig::new(
            endpoint,
            source.tls.trust_root().clone(),
            identity,
            Duration::from_secs(5),
            Duration::from_secs(30),
            std::num::NonZeroU32::MIN,
            std::num::NonZeroU32::MIN,
        ) else {
            continue;
        };
        let peer_config = crate::config::FollowerSourceConfig {
            tls,
            credential: source.credential.clone(),
            database: source.database.clone(),
            lineage: source.lineage,
            hold: peer_hold,
        };
        let Ok(Ok(peer)) =
            tokio::time::timeout(Duration::from_secs(5), follower::connect(peer_config)).await
        else {
            continue;
        };
        let Ok(Ok(vote)) = tokio::time::timeout(
            Duration::from_secs(5),
            peer.request_epoch_vote(vote_request),
        )
        .await
        else {
            continue;
        };
        if vote.granted() {
            collected_votes.push(vote);
        } else if !candidate_pos.is_at_least_as_up_to_date_as(vote.voter_applied())
            || vote.promised_epoch() > proposed_epoch
        {
            saw_lagging_or_higher_epoch_rejection = true;
        }
    }
    let certificate = riffdb_types::QuorumFenceCertificateV1::new(
        source.lineage.database_id(),
        source.lineage.history_incarnation(),
        source.lineage.leadership_epoch(),
        proposed_epoch,
        source.hold,
        candidate_pos,
        &voter_ids,
        collected_votes,
    )
    .map_err(|_| {
        if saw_lagging_or_higher_epoch_rejection {
            Failure::FenceInvalid
        } else {
            Failure::FenceUnavailable
        }
    })?;
    riffdb_storage_api::PrimaryFenceSourceEvidenceV1::from_quorum_fence_certificate(
        &certificate,
        request,
        attempt.request_id(),
        attempt.principal().clone(),
        attempt.timestamp(),
        follower_history,
    )
    .map_err(|_| Failure::FenceInvalid)
}
