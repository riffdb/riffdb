//! Bounded reciprocal validation for the authoritative command graph.

use riffdb_storage_api::{
    IdempotencyIdentityKey, StoredAdmissionStateV1, StoredCommitRecordV1, StoredOutcomeV1,
    StoredProvenanceRecordV1, StructuralFinding, StructuralFindingCode, StructuralFindingScope,
};
use riffdb_types::{CommitSequence, EventId, ProvenanceId};

use crate::state::MemoryState;

pub(crate) fn inspect_admission_graph(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let admission = &state.admissions[index];
    let Ok(identity_key) = admission.identity().storage_key() else {
        return mismatch();
    };
    match admission {
        StoredAdmissionStateV1::StoredOutcome(outcome) => {
            let Some(reciprocal) = committed_admission(state, &identity_key) else {
                return missing();
            };
            if reciprocal.commit_sequence != outcome.commit_sequence() {
                return mismatch();
            }
            let Some(commit) = commit(state, outcome.commit_sequence()) else {
                return missing();
            };
            let Some(commit_index) = commit_admission(state, outcome.commit_sequence()) else {
                return missing();
            };
            if commit_index.identity_key != identity_key || !outcome_matches_commit(outcome, commit)
            {
                return mismatch();
            }
            let Some(provenance) = provenance(state, outcome.provenance_id()) else {
                return missing();
            };
            if !provenance_matches(outcome, commit, provenance) {
                return mismatch();
            }
            None
        }
        StoredAdmissionStateV1::Pending(_) | StoredAdmissionStateV1::ExecutionFailed(_) => {
            if committed_admission(state, &identity_key).is_some() {
                mismatch()
            } else {
                None
            }
        }
    }
}

pub(crate) fn inspect_commit_graph(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let commit = &state.commits[index];
    let Some(reverse) = state.commit_admissions.get(index) else {
        return missing();
    };
    if reverse.commit_sequence != commit.commit_sequence() {
        return mismatch();
    }
    let Some(admission) = admission(state, &reverse.identity_key) else {
        return missing();
    };
    let StoredAdmissionStateV1::StoredOutcome(outcome) = admission else {
        return mismatch();
    };
    if !outcome_matches_commit(outcome, commit) {
        return mismatch();
    }
    let Some(forward) = committed_admission(state, &reverse.identity_key) else {
        return missing();
    };
    if forward.commit_sequence != commit.commit_sequence() {
        return mismatch();
    }
    let Some(provenance) = provenance(state, commit.provenance_id()) else {
        return missing();
    };
    if !provenance_matches(outcome, commit, provenance) {
        return mismatch();
    }

    for reference in commit.entity_references() {
        let Some(latest) = entity_commit(state, reference.target()) else {
            return missing();
        };
        if latest.commit_sequence < commit.commit_sequence() {
            return mismatch();
        }
        if latest.commit_sequence == commit.commit_sequence() {
            let Some(current) = entity(state, reference.target()) else {
                return missing();
            };
            if !reference.matches(current) {
                return mismatch();
            }
        }
    }

    for (ordinal, embedded) in commit.events().iter().enumerate() {
        let Ok(ordinal) = u32::try_from(ordinal) else {
            return mismatch();
        };
        if embedded.event_id() != EventId::new(commit.commit_sequence(), ordinal) {
            return mismatch();
        }
        if !event_hash_is_valid(embedded) {
            return mismatch();
        }
        let Some(event) = event(state, embedded.event_id()) else {
            return missing();
        };
        let Some(intent) = outbox_intent(state, embedded.event_id()) else {
            return missing();
        };
        if event != embedded || intent.event() != embedded {
            return mismatch();
        }
    }
    None
}

pub(crate) fn inspect_entity_graph(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let entity = &state.entities[index];
    let Some(latest) = entity_commit(state, entity.target()) else {
        return missing();
    };
    let Some(commit) = commit(state, latest.commit_sequence) else {
        return missing();
    };
    let Some(reference) = commit
        .entity_references()
        .iter()
        .find(|reference| reference.target() == entity.target())
    else {
        return missing();
    };
    if !reference.matches(entity) {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_entity_commit_index(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let row = &state.entity_commits[index];
    if index > 0 && state.entity_commits[index - 1].target >= row.target {
        return mismatch();
    }
    let Some(entity) = entity(state, &row.target) else {
        return missing();
    };
    let Some(commit) = commit(state, row.commit_sequence) else {
        return missing();
    };
    let Some(reference) = commit
        .entity_references()
        .iter()
        .find(|reference| reference.target() == &row.target)
    else {
        return missing();
    };
    if !reference.matches(entity) {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_commit_admission_index(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let row = &state.commit_admissions[index];
    let expected = u64::try_from(index)
        .ok()
        .and_then(|value| value.checked_add(1))
        .and_then(CommitSequence::new);
    if expected != Some(row.commit_sequence) {
        return mismatch();
    }
    let Some(commit) = state.commits.get(index) else {
        return missing();
    };
    if commit.commit_sequence() != row.commit_sequence {
        return mismatch();
    }
    let Some(admission) = admission(state, &row.identity_key) else {
        return missing();
    };
    let StoredAdmissionStateV1::StoredOutcome(outcome) = admission else {
        return mismatch();
    };
    if outcome.commit_sequence() != row.commit_sequence
        || committed_admission(state, &row.identity_key)
            .is_none_or(|other| other.commit_sequence != row.commit_sequence)
    {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_committed_admission_index(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let row = &state.committed_admissions[index];
    if index > 0 && state.committed_admissions[index - 1].identity_key >= row.identity_key {
        return mismatch();
    }
    let Some(admission) = admission(state, &row.identity_key) else {
        return missing();
    };
    let StoredAdmissionStateV1::StoredOutcome(outcome) = admission else {
        return mismatch();
    };
    let Some(reverse) = commit_admission(state, row.commit_sequence) else {
        return missing();
    };
    if outcome.commit_sequence() != row.commit_sequence || reverse.identity_key != row.identity_key
    {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_provenance_graph(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let provenance = &state.provenance[index];
    let Some(commit) = commit(state, provenance.commit_sequence()) else {
        return missing();
    };
    let Some(reverse) = commit_admission(state, provenance.commit_sequence()) else {
        return missing();
    };
    let Some(admission) = admission(state, &reverse.identity_key) else {
        return missing();
    };
    let StoredAdmissionStateV1::StoredOutcome(outcome) = admission else {
        return mismatch();
    };
    if provenance.provenance_id() != commit.provenance_id()
        || !provenance_matches(outcome, commit, provenance)
    {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_event_graph(state: &MemoryState, index: usize) -> Option<StructuralFinding> {
    let event = &state.events[index];
    if index > 0 && state.events[index - 1].event_id() >= event.event_id() {
        return mismatch();
    }
    if !event_hash_is_valid(event) {
        return mismatch();
    }
    let Some(commit) = commit(state, event.event_id().commit_sequence()) else {
        return missing();
    };
    let Ok(ordinal) = usize::try_from(event.event_id().event_ordinal()) else {
        return mismatch();
    };
    let Some(embedded) = commit.events().get(ordinal) else {
        return missing();
    };
    let Some(intent) = outbox_intent(state, event.event_id()) else {
        return missing();
    };
    let Some(route) = event_route(state, commit.partition_hash(), event.event_id()) else {
        return missing();
    };
    if embedded != event
        || intent.event() != event
        || route.route.event_type_id() != event.event_type_id()
        || route.route.event_hash() != event.event_hash()
    {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_event_route_graph(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let route = &state.event_routes[index];
    if index > 0 && state.event_routes[index - 1].order_key() >= route.order_key() {
        return mismatch();
    }
    let Some(event) = event(state, route.route.event_id()) else {
        return missing();
    };
    let Some(commit) = commit(state, route.route.event_id().commit_sequence()) else {
        return missing();
    };
    let Ok(ordinal) = usize::try_from(route.route.event_id().event_ordinal()) else {
        return mismatch();
    };
    if commit.partition_hash() != route.partition_hash
        || commit.events().get(ordinal) != Some(event)
        || route.route.event_type_id() != event.event_type_id()
        || route.route.event_hash() != event.event_hash()
    {
        return mismatch();
    }
    None
}

pub(crate) fn inspect_outbox_intent_graph(
    state: &MemoryState,
    index: usize,
) -> Option<StructuralFinding> {
    let intent = &state.outbox_intents[index];
    if index > 0 && state.outbox_intents[index - 1].event_id() >= intent.event_id() {
        return mismatch();
    }
    if !event_hash_is_valid(intent.event()) {
        return mismatch();
    }
    let Some(commit) = commit(state, intent.event_id().commit_sequence()) else {
        return missing();
    };
    let Ok(ordinal) = usize::try_from(intent.event_id().event_ordinal()) else {
        return mismatch();
    };
    let Some(embedded) = commit.events().get(ordinal) else {
        return missing();
    };
    let Some(event) = event(state, intent.event_id()) else {
        return missing();
    };
    if embedded != intent.event() || event != intent.event() {
        return mismatch();
    }
    None
}

fn outcome_matches_commit(outcome: &StoredOutcomeV1, commit: &StoredCommitRecordV1) -> bool {
    outcome.commit_sequence() == commit.commit_sequence()
        && outcome.admission_request_id() == commit.admission_request_id()
        && outcome.plan() == commit.plan()
        && outcome.canonical_input_hash() == commit.canonical_input_hash()
        && outcome.actor() == commit.actor()
        && outcome.logical_time() == commit.logical_time()
        && outcome.partition_hash() == commit.partition_hash()
        && outcome.conflict_hashes() == commit.conflict_hashes()
        && outcome.declared_outcome() == commit.declared_outcome()
        && outcome.provenance_id() == commit.provenance_id()
        && outcome.durability_mode() == commit.durability_mode()
}

fn provenance_matches(
    outcome: &StoredOutcomeV1,
    commit: &StoredCommitRecordV1,
    provenance: &StoredProvenanceRecordV1,
) -> bool {
    provenance.provenance_id() == outcome.provenance_id()
        && provenance.commit_sequence() == commit.commit_sequence()
        && provenance.identity() == outcome.identity()
        && provenance.admission_request_id() == commit.admission_request_id()
        && provenance.plan() == commit.plan()
        && provenance.canonical_input_hash() == commit.canonical_input_hash()
        && provenance.actor() == commit.actor()
        && provenance.logical_time() == commit.logical_time()
        && provenance.partition_hash() == commit.partition_hash()
        && provenance.conflict_hashes() == commit.conflict_hashes()
        && provenance.outcome_id() == commit.declared_outcome().outcome_id()
        && provenance.admitted_claims() == outcome.admitted_claims()
        && provenance.event_ids() == commit.outbox_event_ids()
        && provenance.affected_entities().len() == commit.entity_references().len()
        && provenance
            .affected_entities()
            .iter()
            .zip(commit.entity_references())
            .all(|(affected, reference)| {
                affected.target() == reference.target()
                    && affected.entity_version() == reference.entity_version()
            })
}

fn event_hash_is_valid(event: &riffdb_storage_api::StoredDurableEventV1) -> bool {
    riffdb_storage_api::derive_event_hash_v1(
        event.event_id(),
        event.event_type_id(),
        event.payload(),
    )
    .is_ok_and(|hash| hash == event.event_hash())
}

fn entity_commit<'a>(
    state: &'a MemoryState,
    target: &riffdb_storage_api::EntityTarget,
) -> Option<&'a crate::state::EntityCommitIndexRow> {
    let index = state
        .entity_commits
        .binary_search_by(|row| row.target.cmp(target))
        .ok()?;
    if (index > 0 && state.entity_commits[index - 1].target == *target)
        || (index + 1 < state.entity_commits.len()
            && state.entity_commits[index + 1].target == *target)
    {
        return None;
    }
    Some(&state.entity_commits[index])
}

fn entity<'a>(
    state: &'a MemoryState,
    target: &riffdb_storage_api::EntityTarget,
) -> Option<&'a riffdb_storage_api::StoredEntityRecordV1> {
    let index = state
        .entities
        .binary_search_by(|record| record.target().cmp(target))
        .ok()?;
    if (index > 0 && state.entities[index - 1].target() == target)
        || (index + 1 < state.entities.len() && state.entities[index + 1].target() == target)
    {
        return None;
    }
    Some(&state.entities[index])
}

fn admission<'a>(
    state: &'a MemoryState,
    key: &IdempotencyIdentityKey,
) -> Option<&'a StoredAdmissionStateV1> {
    let index = state
        .admissions
        .binary_search_by(|candidate| {
            candidate
                .identity()
                .storage_key()
                .map_or(std::cmp::Ordering::Less, |candidate| candidate.cmp(key))
        })
        .ok()?;
    let matches = |candidate: &StoredAdmissionStateV1| {
        candidate.identity().storage_key().ok().as_ref() == Some(key)
    };
    if !matches(&state.admissions[index])
        || (index > 0 && matches(&state.admissions[index - 1]))
        || (index + 1 < state.admissions.len() && matches(&state.admissions[index + 1]))
    {
        return None;
    }
    Some(&state.admissions[index])
}

fn commit(state: &MemoryState, sequence: CommitSequence) -> Option<&StoredCommitRecordV1> {
    let index = usize::try_from(sequence.get().checked_sub(1)?).ok()?;
    state
        .commits
        .get(index)
        .filter(|record| record.commit_sequence() == sequence)
}

fn commit_admission(
    state: &MemoryState,
    sequence: CommitSequence,
) -> Option<&crate::state::CommitAdmissionIndexRow> {
    let index = usize::try_from(sequence.get().checked_sub(1)?).ok()?;
    state
        .commit_admissions
        .get(index)
        .filter(|row| row.commit_sequence == sequence)
}

fn committed_admission<'a>(
    state: &'a MemoryState,
    key: &IdempotencyIdentityKey,
) -> Option<&'a crate::state::CommittedAdmissionIndexRow> {
    let index = state
        .committed_admissions
        .binary_search_by(|row| row.identity_key.cmp(key))
        .ok()?;
    if (index > 0 && state.committed_admissions[index - 1].identity_key == *key)
        || (index + 1 < state.committed_admissions.len()
            && state.committed_admissions[index + 1].identity_key == *key)
    {
        return None;
    }
    Some(&state.committed_admissions[index])
}

fn provenance(
    state: &MemoryState,
    provenance_id: ProvenanceId,
) -> Option<&StoredProvenanceRecordV1> {
    let index = state
        .provenance
        .binary_search_by_key(&provenance_id, StoredProvenanceRecordV1::provenance_id)
        .ok()?;
    if (index > 0 && state.provenance[index - 1].provenance_id() == provenance_id)
        || (index + 1 < state.provenance.len()
            && state.provenance[index + 1].provenance_id() == provenance_id)
    {
        return None;
    }
    Some(&state.provenance[index])
}

fn event(
    state: &MemoryState,
    event_id: EventId,
) -> Option<&riffdb_storage_api::StoredDurableEventV1> {
    let index = state
        .events
        .binary_search_by_key(&event_id, |event| event.event_id())
        .ok()?;
    if (index > 0 && state.events[index - 1].event_id() == event_id)
        || (index + 1 < state.events.len() && state.events[index + 1].event_id() == event_id)
    {
        return None;
    }
    Some(&state.events[index])
}

fn event_route(
    state: &MemoryState,
    partition_hash: riffdb_types::PartitionKeyHash,
    event_id: EventId,
) -> Option<&crate::state::EventRouteRow> {
    let key = (partition_hash, event_id);
    let index = state
        .event_routes
        .binary_search_by_key(&key, |row| row.order_key())
        .ok()?;
    if (index > 0 && state.event_routes[index - 1].order_key() == key)
        || (index + 1 < state.event_routes.len()
            && state.event_routes[index + 1].order_key() == key)
    {
        return None;
    }
    Some(&state.event_routes[index])
}

fn outbox_intent(
    state: &MemoryState,
    event_id: EventId,
) -> Option<&riffdb_storage_api::StoredOutboxIntentV1> {
    let index = state
        .outbox_intents
        .binary_search_by_key(&event_id, |intent| intent.event_id())
        .ok()?;
    if (index > 0 && state.outbox_intents[index - 1].event_id() == event_id)
        || (index + 1 < state.outbox_intents.len()
            && state.outbox_intents[index + 1].event_id() == event_id)
    {
        return None;
    }
    Some(&state.outbox_intents[index])
}

const fn missing() -> Option<StructuralFinding> {
    Some(StructuralFinding::new(
        StructuralFindingScope::Authoritative,
        StructuralFindingCode::MissingCrossLink,
    ))
}

const fn mismatch() -> Option<StructuralFinding> {
    Some(StructuralFinding::new(
        StructuralFindingScope::Authoritative,
        StructuralFindingCode::CrossLinkMismatch,
    ))
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{
        DeclaredOutcome, DurabilityMode, DurableKeySchemaBindingV1, EntityTarget,
        ExecutablePlanRef, IdempotencyIdentity, IdempotencyKeyDigest, ReadDependencies,
        StoredAdmittedProvenanceClaimsV1, StoredCommitRecordV1, StoredDurableEventV1,
        StoredExecutionFailedV1, StoredOutboxIntentV1, StoredOutcomeV1, StoredPendingAdmissionV1,
        StoredProvenanceRecordV1, StoredReadDependenciesV1, derive_event_hash_v1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CommandId, ContractBundleHash, ContractLineage, ContractVersion,
        DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId, EntityVersion, Environment,
        EventId, EventTypeId, ExecutionFailureCode, LogicalTime, OutcomeId, PartitionKeyBuilder,
        PlanHash, ProvenanceId, RequestId, TenantScope, Timestamp, hash_partition_key,
    };

    use super::*;
    use crate::state::{
        CommitAdmissionIndexRow, CommittedAdmissionIndexRow, EntityCommitIndexRow, EventRouteRow,
    };

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn command_graph() -> (MemoryState, StoredPendingAdmissionV1) {
        let plan = ExecutablePlanRef::new(
            ContractLineage::new("integrity").expect("lineage"),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([0x21; 32]),
            CommandId::first(),
            PlanHash::from_bytes([0x22; 32]),
        );
        let actor = AdmittedActorContext::new(
            ActorId::new("maintainer").expect("actor"),
            ActorKind::Human,
            TenantScope::Global,
            None,
        );
        let identity = IdempotencyIdentity::new(
            DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database"),
            Environment::new("test").expect("environment"),
            TenantScope::Global,
            actor.principal_id().clone(),
            plan.contract_lineage().clone(),
            plan.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key"),
                [0x31; 32],
            ),
        );
        let request_id = RequestId::from_bytes(uuid_bytes(0x12)).expect("request");
        let provenance_id = ProvenanceId::from_bytes(uuid_bytes(0x13)).expect("provenance");
        let logical_time = LogicalTime::new(Timestamp::new(42, 7).expect("timestamp"));
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(1).expect("partition component");
        let partition = partition.finish().expect("partition");
        let partition_hash = hash_partition_key(partition.as_bytes());
        let input_hash = CanonicalInputHash::from_bytes([0x32; 32]);
        let admitted_claims = StoredAdmittedProvenanceClaimsV1::default();
        let pending = StoredPendingAdmissionV1::new(
            identity.clone(),
            input_hash,
            request_id,
            plan.clone(),
            logical_time,
            actor.clone(),
            partition.clone(),
            admitted_claims.clone(),
        )
        .expect("pending");
        let sequence = CommitSequence::first();
        let event_id = EventId::new(sequence, 0);
        let event_type_id = EventTypeId::first();
        let event_payload = CanonicalRecord::new(Vec::new()).expect("event payload");
        let event_hash =
            derive_event_hash_v1(event_id, event_type_id, &event_payload).expect("event hash");
        let event = StoredDurableEventV1::new(event_id, event_type_id, event_payload, event_hash)
            .expect("event");
        let outcome = DeclaredOutcome::new(
            OutcomeId::first(),
            CanonicalRecord::new(Vec::new()).expect("outcome value"),
        )
        .expect("outcome");
        let stored_outcome = StoredOutcomeV1::new(
            identity.clone(),
            sequence,
            request_id,
            plan.clone(),
            input_hash,
            actor.clone(),
            logical_time,
            partition,
            partition_hash,
            Vec::new(),
            outcome.clone(),
            admitted_claims.clone(),
            provenance_id,
            DurabilityMode::Memory,
        )
        .expect("stored outcome");
        let provenance = StoredProvenanceRecordV1::new(
            provenance_id,
            sequence,
            identity.clone(),
            request_id,
            plan.clone(),
            input_hash,
            actor.clone(),
            logical_time,
            partition_hash,
            Vec::new(),
            outcome.outcome_id(),
            Vec::new(),
            vec![event_id],
            admitted_claims,
        )
        .expect("provenance");
        let stored_dependencies = StoredReadDependenciesV1::from_live(
            &ReadDependencies::new(Vec::new()).expect("empty dependencies"),
        )
        .expect("stored dependencies");
        let commit = StoredCommitRecordV1::new(
            sequence,
            request_id,
            plan,
            input_hash,
            actor,
            logical_time,
            partition_hash,
            Vec::new(),
            stored_dependencies,
            Vec::new(),
            vec![event.clone()],
            outcome,
            provenance_id,
            vec![event_id],
            DurabilityMode::Memory,
        )
        .expect("commit");
        let identity_key = identity.storage_key().expect("identity key");
        let mut state = MemoryState::default();
        state
            .admissions
            .push(StoredAdmissionStateV1::StoredOutcome(stored_outcome));
        state.commits.push(commit);
        state.commit_admissions.push(CommitAdmissionIndexRow {
            commit_sequence: sequence,
            identity_key: identity_key.clone(),
        });
        state.committed_admissions.push(CommittedAdmissionIndexRow {
            identity_key,
            commit_sequence: sequence,
        });
        state.provenance.push(provenance);
        state.events.push(event.clone());
        state.event_routes.push(EventRouteRow::new(
            partition_hash,
            riffdb_storage_api::StoredEventRouteV1::new(
                event.event_id(),
                event.event_type_id(),
                event.event_hash(),
            ),
        ));
        state.outbox_intents.push(StoredOutboxIntentV1::new(event));
        (state, pending)
    }

    #[test]
    fn complete_command_graph_is_reciprocal_in_every_direction() {
        let (state, _) = command_graph();
        assert_eq!(inspect_admission_graph(&state, 0), None);
        assert_eq!(inspect_commit_graph(&state, 0), None);
        assert_eq!(inspect_commit_admission_index(&state, 0), None);
        assert_eq!(inspect_committed_admission_index(&state, 0), None);
        assert_eq!(inspect_provenance_graph(&state, 0), None);
        assert_eq!(inspect_event_graph(&state, 0), None);
        assert_eq!(inspect_event_route_graph(&state, 0), None);
        assert_eq!(inspect_outbox_intent_graph(&state, 0), None);
    }

    #[test]
    fn terminal_outcome_and_provenance_claims_must_match_after_recovery() {
        let (mut state, _) = command_graph();
        let StoredAdmissionStateV1::StoredOutcome(original) = &state.admissions[0] else {
            panic!("command graph must contain a terminal outcome");
        };
        let changed_claims = StoredAdmittedProvenanceClaimsV1::new(
            None,
            None,
            Some(riffdb_types::ProvenanceReason::new("changed").expect("reason")),
            None,
        )
        .expect("claims");
        let changed = StoredOutcomeV1::new(
            original.identity().clone(),
            original.commit_sequence(),
            original.admission_request_id(),
            original.plan().clone(),
            original.canonical_input_hash(),
            original.actor().clone(),
            original.logical_time(),
            original.partition_key().clone(),
            original.partition_hash(),
            original.conflict_hashes().to_vec(),
            original.declared_outcome().clone(),
            changed_claims,
            original.provenance_id(),
            original.durability_mode(),
        )
        .expect("individually valid changed outcome");
        state.admissions[0] = StoredAdmissionStateV1::StoredOutcome(changed);

        assert_eq!(inspect_admission_graph(&state, 0), mismatch());
        assert_eq!(inspect_provenance_graph(&state, 0), mismatch());
    }

    #[test]
    fn missing_and_mismatched_event_intents_are_authoritative_findings() {
        let (mut missing_state, _) = command_graph();
        missing_state.outbox_intents.clear();
        assert_eq!(inspect_commit_graph(&missing_state, 0), missing());
        assert_eq!(inspect_event_graph(&missing_state, 0), missing());

        let (mut missing_route_state, _) = command_graph();
        missing_route_state.event_routes.clear();
        assert_eq!(inspect_event_graph(&missing_route_state, 0), missing());

        let (mut mismatch_state, _) = command_graph();
        let original = &mismatch_state.events[0];
        let alternate_payload = CanonicalRecord::new(vec![]).expect("alternate payload");
        let alternate_hash = derive_event_hash_v1(
            original.event_id(),
            EventTypeId::new(2).expect("event type"),
            &alternate_payload,
        )
        .expect("alternate hash");
        let alternate = StoredDurableEventV1::new(
            original.event_id(),
            EventTypeId::new(2).expect("event type"),
            alternate_payload,
            alternate_hash,
        )
        .expect("alternate event");
        mismatch_state.outbox_intents[0] = StoredOutboxIntentV1::new(alternate);
        assert_eq!(inspect_commit_graph(&mismatch_state, 0), mismatch());
        assert_eq!(inspect_outbox_intent_graph(&mismatch_state, 0), mismatch());
    }

    #[test]
    fn execution_failure_cannot_retain_any_commit_graph_link() {
        let (mut state, pending) = command_graph();
        state.admissions[0] = StoredAdmissionStateV1::ExecutionFailed(
            StoredExecutionFailedV1::new(pending, ExecutionFailureCode::ArithmeticFault),
        );
        assert_eq!(inspect_admission_graph(&state, 0), mismatch());
        assert_eq!(inspect_commit_graph(&state, 0), mismatch());
    }

    #[test]
    fn duplicate_identity_index_is_rejected() {
        let (mut state, _) = command_graph();
        let duplicate = CommittedAdmissionIndexRow {
            identity_key: state.committed_admissions[0].identity_key.clone(),
            commit_sequence: CommitSequence::first(),
        };
        state.committed_admissions.push(duplicate);
        assert_eq!(inspect_committed_admission_index(&state, 1), mismatch());
        assert_eq!(inspect_admission_graph(&state, 0), missing());
    }

    #[test]
    fn current_entity_requires_a_latest_commit_post_image() {
        let plan = ExecutablePlanRef::new(
            ContractLineage::new("entity-integrity").expect("lineage"),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([0x61; 32]),
            CommandId::first(),
            PlanHash::from_bytes([0x62; 32]),
        );
        let entity_type = EntityTypeId::first();
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(1).expect("entity component");
        let target = EntityTarget::new(entity_type, key.finish().expect("entity key"))
            .expect("entity target");
        let record = riffdb_storage_api::StoredEntityRecordV1::new(
            target.clone(),
            EntityVersion::first(),
            plan.contract_version(),
            DurableKeySchemaBindingV1::from_plan(&plan),
            CanonicalRecord::new(Vec::new()).expect("entity value"),
        )
        .expect("entity record");
        let mut state = MemoryState::default();
        state.entities.push(record);
        state.entity_commits.push(EntityCommitIndexRow {
            target,
            commit_sequence: CommitSequence::first(),
        });

        assert_eq!(inspect_entity_graph(&state, 0), missing());
        assert_eq!(inspect_entity_commit_index(&state, 0), missing());
    }
}
