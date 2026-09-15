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
    indexes.apply_follower_receipt(&write, &mutations).unwrap();
    write.commit().unwrap();
}

fn compare(
    ports: &RedbOperationalPorts,
    indexes: &TransientIndexes,
    all: &StoredCommandSegmentV1,
    present: &[u64],
) {
    let rebuilt = TransientIndexes::rebuild(&ports.shared.database.begin_read().unwrap()).unwrap();
    for entry in all.manifest().entries() {
        let actual = indexes
            .command_derived_member(entry.kind(), entry.exact_key())
            .unwrap();
        let expected = rebuilt
            .command_derived_member(entry.kind(), entry.exact_key())
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
