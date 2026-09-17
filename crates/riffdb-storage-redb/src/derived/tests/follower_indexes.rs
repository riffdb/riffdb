//! Compare incremental segment indexes with a complete independent table rebuild.
// req: REP-002, REP-003, PERF-007
use super::*;
use crate::transient::TransientIndexes;
use redb::ReadableDatabase;
use riffdb_storage_api::*;
use riffdb_types::{
    AdministrationSequence, CapabilityId, DigestKeyId, Environment, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1,
};
use std::collections::BTreeMap;
use std::sync::Arc;

fn capsule(sequence: CommitSequence) -> StoredCommandCapsuleV2 {
    let (commit, _) = command_graph_at(sequence, 1);
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
    partition.push_u64(1).unwrap();
    let partition = partition.finish().unwrap();
    let identity = IdempotencyIdentity::new(
        database_id(),
        Environment::new("test").unwrap(),
        TenantScope::Global,
        commit.actor().principal_id().clone(),
        commit.plan().contract_lineage().clone(),
        commit.plan().command_id(),
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).unwrap(),
            [u8::try_from(sequence.get()).unwrap(); 32],
        ),
    );
    let claims = StoredAdmittedProvenanceClaimsV1::new(None, None, None, None).unwrap();
    let outcome = StoredOutcomeV1::new(
        identity.clone(),
        sequence,
        commit.admission_request_id(),
        commit.plan().clone(),
        commit.canonical_input_hash(),
        commit.actor().clone(),
        commit.logical_time(),
        partition,
        commit.partition_hash(),
        vec![],
        commit.declared_outcome().clone(),
        claims.clone(),
        commit.provenance_id(),
        commit.durability_mode(),
    )
    .unwrap();
    let provenance = StoredProvenanceRecordV1::new(
        commit.provenance_id(),
        sequence,
        identity,
        commit.admission_request_id(),
        commit.plan().clone(),
        commit.canonical_input_hash(),
        commit.actor().clone(),
        commit.logical_time(),
        commit.partition_hash(),
        vec![],
        commit.declared_outcome().outcome_id(),
        vec![],
        commit.outbox_event_ids().to_vec(),
        claims,
    )
    .unwrap();
    let principal = AuditPrincipalV1::new(
        commit.actor().principal_id().clone(),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(55)).unwrap(),
        std::num::NonZeroU64::MIN,
    );
    let audit = |offset, phase, link| {
        StoredServiceAuditRecordV1::from_stored_parts(
            AdministrationSequence::new(sequence.get() * 2 - offset).unwrap(),
            commit.admission_request_id(),
            timestamp(42),
            ServiceOperationV1::ExecuteCommand,
            phase,
            Some(principal.clone()),
            ServiceIngressKindV1::InProcessTestComparison,
            ServiceAuditTargetsV1::empty(),
            None,
            link,
        )
        .unwrap()
    };
    let started = audit(1, ServiceAuditPhaseV1::Started, ServiceAuditLinkV1::None);
    let terminal = audit(
        0,
        ServiceAuditPhaseV1::Succeeded,
        ServiceAuditLinkV1::Command {
            commit_sequence: sequence,
            provenance_id: commit.provenance_id(),
        },
    );
    StoredCommandCapsuleV2::from_base(
        StoredCommandCapsuleV1::new(outcome, provenance, commit, started, terminal).unwrap(),
        vec![],
    )
    .unwrap()
}

fn segment(
    commands: Vec<StoredCommandCapsuleV2>,
    prior: Option<CommandSegmentDigestV1>,
) -> (StoredCommandSegmentV1, CanonicalStoredEnvelopeV1) {
    let manifest = crate::application::build_command_segment_manifest(
        &commands,
        commands[0].commit_sequence(),
    )
    .unwrap();
    seal_and_encode_command_segment_v1(
        StoredCommandSegmentV1::new(
            database_id(),
            1,
            prior,
            commands,
            manifest,
            CommandSegmentDigestV1::from_bytes([0; 32]),
        )
        .unwrap(),
    )
    .unwrap()
}

fn apply(
    ports: &RedbOperationalPorts,
    indexes: &mut TransientIndexes,
    mut mutations: Vec<AuthoritativeMutationV3>,
) {
    mutations.sort_by_key(|m| (m.namespace(), m.key().to_vec()));
    let write = ports.shared.database.begin_write().unwrap();
    for mutation in &mutations {
        crate::changelog_v3_write::check_predecessor(&write, mutation).unwrap();
        crate::changelog_v3_write::apply_mutation(&write, mutation).unwrap();
    }
    indexes.evict_payloads();
    indexes.apply_follower_receipt(&write, &mutations).unwrap();
    let (entries, bytes) = indexes.payload_stats();
    assert!(entries <= 64 && bytes <= 64 * 1024 * 1024);
    indexes.evict_payloads();
    write.commit().unwrap();
}

fn compare(
    ports: &RedbOperationalPorts,
    indexes: &TransientIndexes,
    all: &StoredCommandSegmentV1,
    present: &[u64],
) {
    let pin = ports.shared.database.begin_read().unwrap();
    let rebuilt = TransientIndexes::rebuild(&pin).unwrap();
    let load = |first| {
        let table = pin.open_table(crate::layout::COMMITS).unwrap();
        Ok(table
            .get(encode_application_sequence_key(first).as_slice())
            .unwrap()
            .map(|row| row.value().to_vec()))
    };
    for entry in all.manifest().entries() {
        let actual = indexes
            .command_derived_member(entry.kind(), entry.exact_key(), load)
            .unwrap();
        let expected = rebuilt
            .command_derived_member(entry.kind(), entry.exact_key(), load)
            .unwrap();
        assert_eq!(
            actual
                .as_ref()
                .map(|(segment, locator)| (segment.segment_digest(), *locator)),
            expected
                .as_ref()
                .map(|(segment, locator)| (segment.segment_digest(), *locator))
        );
        let sequence = all.commands()[usize::from(entry.command_ordinal())]
            .commit_sequence()
            .get();
        assert_eq!(actual.is_some(), present.contains(&sequence));
    }
    assert_eq!(
        indexes.pending_outbox_page(None, 8),
        rebuilt.pending_outbox_page(None, 8)
    );
    assert_eq!(
        indexes.undelivered_outbox_page(None, 8),
        rebuilt.undelivered_outbox_page(None, 8)
    );
    assert_eq!(
        indexes.command_segment_tail(),
        rebuilt.command_segment_tail()
    );
    let partition = all.commands()[0].base().commit().partition_hash();
    assert_eq!(
        indexes
            .partition_event_route_page(partition, None, None, 8)
            .unwrap()
            .unwrap(),
        rebuilt
            .partition_event_route_page(partition, None, None, 8)
            .unwrap()
            .unwrap()
    );
}

#[test]
fn follower_segment_insert_split_and_retirement_match_full_index_rebuild() {
    use AuthoritativeMutationV3 as M;
    use AuthoritativeNamespaceV1 as N;
    let (_path, ports) = operational("follower-segment-indexes");
    let mut indexes = TransientIndexes::default();
    let one = capsule(CommitSequence::first());
    let two = capsule(CommitSequence::new(2).unwrap());
    let (both, encoded_both) = segment(vec![one.clone(), two.clone()], None);
    let key_one = encode_application_sequence_key(CommitSequence::first());
    let key_two = encode_application_sequence_key(CommitSequence::new(2).unwrap());
    apply(
        &ports,
        &mut indexes,
        vec![M::put(N::Commits, &key_one, None, encoded_both.as_bytes()).unwrap()],
    );
    compare(&ports, &indexes, &both, &[1, 2]);
    let event = one.events()[0].clone();
    let event_key = crate::keys::encode_event_key(event.event_id());
    let route_key =
        crate::keys::encode_event_route_key(one.base().commit().partition_hash(), event.event_id());
    let intent =
        crate::codec::encode_outbox_intent_v1(&StoredOutboxIntentV1::new(event.clone())).unwrap();
    let route = crate::codec::encode_event_route_v1(StoredEventRouteV1::new(
        event.event_id(),
        event.event_type_id(),
        event.event_hash(),
    ))
    .unwrap();
    apply(
        &ports,
        &mut indexes,
        vec![
            M::put(N::Outbox, &event_key, None, intent.as_bytes()).unwrap(),
            M::put(N::EventRoutes, &route_key, None, route.as_bytes()).unwrap(),
        ],
    );
    compare(&ports, &indexes, &both, &[1, 2]);
    let (first, encoded_first) = segment(vec![one], None);
    let (_, encoded_second) = segment(vec![two], Some(first.segment_digest()));
    apply(
        &ports,
        &mut indexes,
        vec![
            M::replace(
                N::Commits,
                &key_one,
                encoded_both.as_bytes(),
                encoded_first.as_bytes(),
            )
            .unwrap(),
            M::put(N::Commits, &key_two, None, encoded_second.as_bytes()).unwrap(),
        ],
    );
    compare(&ports, &indexes, &both, &[1, 2]);
    apply(
        &ports,
        &mut indexes,
        vec![M::delete_matching(N::Commits, &key_one, encoded_first.as_bytes()).unwrap()],
    );
    compare(&ports, &indexes, &both, &[2]);
    // Removing segment authority must retain transitional standalone copies.
    assert_eq!(indexes.pending_outbox_page(None, 8).unwrap().0.len(), 2);
    apply(
        &ports,
        &mut indexes,
        vec![
            M::delete_matching(N::Outbox, &event_key, intent.as_bytes()).unwrap(),
            M::delete_matching(N::EventRoutes, &route_key, route.as_bytes()).unwrap(),
        ],
    );
    compare(&ports, &indexes, &both, &[2]);

    apply(
        &ports,
        &mut indexes,
        vec![M::delete_matching(N::Commits, &key_two, encoded_second.as_bytes()).unwrap()],
    );
    compare(&ports, &indexes, &both, &[]);
}

#[test]
fn follower_event_membership_matches_rebuild_without_manifest_lookup_assumptions() {
    let (_path, ports) = operational("follower-event-membership");
    let mut indexes = TransientIndexes::default();
    let command = capsule(CommitSequence::first());
    let complete = crate::application::build_command_segment_manifest(
        std::slice::from_ref(&command),
        command.commit_sequence(),
    )
    .unwrap();
    // The codec accepts a nonempty manifest. This cache layer must derive event
    // membership exactly like rebuild, independently of any catalog proof gate.
    let partial = CommandSegmentManifestV1::new(
        complete
            .entries()
            .iter()
            .filter(|entry| {
                !matches!(
                    entry.kind(),
                    CommandDerivedIndexKindV1::EventRoute
                        | CommandDerivedIndexKindV1::PendingOutbox
                )
            })
            .cloned()
            .collect(),
    )
    .unwrap();
    let (segment, encoded) = seal_and_encode_command_segment_v1(
        StoredCommandSegmentV1::new(
            database_id(),
            1,
            None,
            vec![command],
            partial,
            CommandSegmentDigestV1::from_bytes([0; 32]),
        )
        .unwrap(),
    )
    .unwrap();
    apply(
        &ports,
        &mut indexes,
        vec![
            AuthoritativeMutationV3::put(
                AuthoritativeNamespaceV1::Commits,
                &encode_application_sequence_key(CommitSequence::first()),
                None,
                encoded.as_bytes(),
            )
            .unwrap(),
        ],
    );
    compare(&ports, &indexes, &segment, &[1]);
    assert_eq!(
        indexes.pending_outbox_page(None, 8).unwrap().0,
        vec![EventId::new(CommitSequence::first(), 0)]
    );
}

// req: OUT-001, REP-002
#[test]
fn review_payload_eviction_preserves_exact_members_and_rejects_bad_cold_authority() {
    let mut indexes = TransientIndexes::default();
    let mut snapshots = BTreeMap::new();
    let mut commands = Vec::new();
    let mut prior = None;
    for sequence in 1..=80 {
        let command = capsule(CommitSequence::new(sequence).unwrap());
        let (segment, encoded) = segment(vec![command.clone()], prior);
        prior = Some(segment.segment_digest());
        snapshots.insert(segment.first_commit_sequence(), encoded.as_bytes().to_vec());
        let prior_cache = indexes.payload_stats();
        indexes.apply(
            crate::transient::TransientIndexDelta::CommandSegmentPublished(Arc::new(segment)),
        );
        assert_eq!(
            indexes.payload_stats(),
            prior_cache,
            "publication retains no decoded payload"
        );
        assert_eq!(
            indexes
                .command_at(command.commit_sequence(), |first| Ok(snapshots
                    .get(&first)
                    .cloned()))
                .unwrap(),
            Some(command.clone())
        );
        commands.push(command);
        let (entries, bytes) = indexes.payload_stats();
        assert!(entries <= 64 && bytes <= 64 * 1024 * 1024);
    }
    assert!(
        indexes.payload_stats().0 > 0,
        "small normalized payloads are cacheable"
    );
    assert!(
        indexes.payload_stats().0 < snapshots.len(),
        "history outlives resident payloads"
    );
    for command in &commands {
        assert_eq!(
            indexes
                .command_at(command.commit_sequence(), |first| Ok(snapshots
                    .get(&first)
                    .cloned()))
                .unwrap()
                .as_ref(),
            Some(command)
        );
        indexes.evict_payloads();
        let key = crate::keys::encode_provenance_key(command.base().provenance().provenance_id());
        let (actual, locator) = indexes
            .command_derived_member(CommandDerivedIndexKindV1::Provenance, &key, |first| {
                Ok(snapshots.get(&first).cloned())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            &actual.commands()[usize::from(locator.command_ordinal)],
            command
        );
        let id = command.base().outcome().identity().storage_key().unwrap();
        assert!(
            indexes
                .command_derived_member(
                    CommandDerivedIndexKindV1::Idempotency,
                    crate::keys::encode_idempotency_key(&id),
                    |first| Ok(snapshots.get(&first).cloned())
                )
                .unwrap()
                .is_some()
        );
        let audit = command.base().terminal_audit();
        assert_eq!(
            indexes
                .command_audit_record(audit.administration_sequence(), |first| Ok(snapshots
                    .get(&first)
                    .cloned()))
                .unwrap()
                .as_ref(),
            Some(audit)
        );
    }
    let first = CommitSequence::first();
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            indexes.command_at(first, |key| {
                let bytes = snapshots.get(&key).cloned();
                barrier.wait();
                barrier.wait();
                Ok(bytes)
            })
        });
        barrier.wait();
        // Eviction and another lookup complete while the first reader holds its
        // captured bytes. Cache locking must not encompass its storage loader.
        indexes.evict_payloads();
        indexes
            .command_at(CommitSequence::new(2).unwrap(), |key| {
                Ok(snapshots.get(&key).cloned())
            })
            .unwrap();
        barrier.wait();
        assert_eq!(reader.join().unwrap().unwrap(), Some(commands[0].clone()));
    });
    assert!(indexes.command_at(first, |_| Ok(None)).is_err());
    let wrong = snapshots[&CommitSequence::new(2).unwrap()].clone();
    assert!(indexes.command_at(first, |_| Ok(Some(wrong))).is_err());
    let mut corrupt = snapshots[&first].clone();
    corrupt[0] ^= 0xff;
    assert!(indexes.command_at(first, |_| Ok(Some(corrupt))).is_err());
    assert!(
        indexes
            .command_at(first, |_| Err(StorageError::new(
                StorageErrorKind::Unavailable,
                None
            )))
            .is_err()
    );
    assert_eq!(
        indexes
            .command_at(first, |key| Ok(snapshots.get(&key).cloned()))
            .unwrap()
            .as_ref(),
        Some(&commands[0])
    );
}

// req: REP-002, OUT-001
#[test]
fn review_cold_payload_lookup_keeps_its_old_redb_pin_after_physical_overwrite() {
    let (_path, ports) = operational("review-old-payload-pin");
    let mut indexes = TransientIndexes::default();
    let command = capsule(CommitSequence::first());
    let (_, encoded) = segment(vec![command.clone()], None);
    let key = encode_application_sequence_key(CommitSequence::first());
    apply(
        &ports,
        &mut indexes,
        vec![
            AuthoritativeMutationV3::put(
                AuthoritativeNamespaceV1::Commits,
                &key,
                None,
                encoded.as_bytes(),
            )
            .unwrap(),
        ],
    );
    let old = ports.shared.database.begin_read().unwrap();
    indexes = TransientIndexes::rebuild(&old).unwrap();
    assert_eq!(indexes.payload_stats().0, 0);
    let write = ports.shared.database.begin_write().unwrap();
    {
        let mut table = write.open_table(crate::layout::COMMITS).unwrap();
        table
            .insert(key.as_slice(), b"corrupt successor".as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    let latest = ports.shared.database.begin_read().unwrap();
    let load = |pin: &redb::ReadTransaction, first| {
        let table = pin.open_table(crate::layout::COMMITS).unwrap();
        Ok(table
            .get(encode_application_sequence_key(first).as_slice())
            .unwrap()
            .map(|value| value.value().to_vec()))
    };
    assert_eq!(
        indexes
            .command_at(CommitSequence::first(), |first| load(&old, first))
            .unwrap(),
        Some(command.clone())
    );
    assert!(
        indexes
            .command_at(CommitSequence::first(), |first| load(&latest, first))
            .is_err()
    );
    indexes.evict_payloads();
    // Cold lookup uses the identical pinned authority.
    assert!(
        indexes
            .command_at(CommitSequence::first(), |first| load(&latest, first))
            .is_err()
    );
    assert_eq!(
        indexes
            .command_at(CommitSequence::first(), |first| load(&old, first))
            .unwrap(),
        Some(command)
    );
}

// req: REP-002, OUT-001
#[test]
fn review_multi_command_payload_is_cached_with_its_decoded_ownership_charge() {
    let commands = (1..=64)
        .map(|value| capsule(CommitSequence::new(value).unwrap()))
        .collect::<Vec<_>>();
    let (segment, encoded) = segment(commands.clone(), None);
    let mut indexes = TransientIndexes::default();
    indexes
        .apply(crate::transient::TransientIndexDelta::CommandSegmentPublished(Arc::new(segment)));
    assert_eq!(indexes.payload_stats(), (0, 0));
    for command in &commands {
        assert_eq!(
            indexes
                .command_at(command.commit_sequence(), |_| Ok(Some(
                    encoded.as_bytes().to_vec()
                )))
                .unwrap(),
            Some(command.clone())
        );
    }
    let (entries, bytes) = indexes.payload_stats();
    assert_eq!(entries, 1);
    assert!(bytes <= 64 * 1024 * 1024);
    assert!(bytes > 64 * std::mem::size_of::<StoredCommandCapsuleV2>());
}
