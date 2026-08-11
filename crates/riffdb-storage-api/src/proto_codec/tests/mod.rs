//! Durable semantic codec contract tests.

mod bounds;
mod entity_references;
mod malformed_semantic;
mod migration;
mod relationships;
mod sample;
mod variants;

use std::fmt::Debug;

use riffdb_types::{
    AdministrationSequence, CommitSequence, EventId, EventTypeId, ExecutionFailureCode,
    RowPolicyName, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1,
};

use crate::EncodedPageItem;

use super::*;

fn assert_round_trip<T>(
    value: T,
    encode: impl FnOnce(&T) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError>,
    decode: impl FnOnce(&[u8]) -> Result<EncodedPageItem<T>, DurableCodecError>,
) -> CanonicalStoredEnvelopeV1
where
    T: Debug + Eq,
{
    let encoded = encode(&value).expect("checked sample encodes");
    let decoded = decode(encoded.as_bytes()).expect("canonical sample decodes");
    assert_eq!(decoded.value(), &value);
    assert_eq!(
        decoded.encoded_content_charge(),
        encoded.encoded_content_charge()
    );
    encoded
}

fn semantic_wire_vectors() -> Vec<(&'static str, CanonicalStoredEnvelopeV1)> {
    let mut vectors = Vec::with_capacity(27);
    vectors.push((
        "riffdb.storage.v1.StoredStorageFormatVersionV1",
        assert_round_trip(
            crate::StorageFormatVersion::V1,
            |value| encode_storage_format_version_v1(*value),
            decode_storage_format_version_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredDatabaseIdentityV1",
        assert_round_trip(
            sample::database_id(),
            |value| encode_database_identity_v1(*value),
            decode_database_identity_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredApplicationSequenceAllocatorV1",
        assert_round_trip(
            crate::ApplicationSequenceAllocator::next(CommitSequence::first()),
            |value| encode_application_sequence_allocator_v1(*value),
            decode_application_sequence_allocator_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredAdministrationSequenceAllocatorV1",
        assert_round_trip(
            crate::AdministrationSequenceAllocator::next(AdministrationSequence::first()),
            |value| encode_administration_sequence_allocator_v1(*value),
            decode_administration_sequence_allocator_v1,
        ),
    ));

    let (bundle, active, catalog_administration) = sample::catalog_records();
    vectors.push((
        "riffdb.storage.v1.StoredContractBundleV1",
        assert_round_trip(bundle, encode_contract_bundle_v1, decode_contract_bundle_v1),
    ));
    vectors.push((
        "riffdb.storage.v1.ActiveCatalogPointerV1",
        assert_round_trip(
            active,
            encode_active_catalog_pointer_v1,
            decode_active_catalog_pointer_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredCatalogAdministrationV1",
        assert_round_trip(
            catalog_administration,
            encode_catalog_administration_v1,
            decode_catalog_administration_v1,
        ),
    ));

    let atomic = sample::atomic_record_set();
    vectors.push((
        "riffdb.storage.v1.StoredEntityRecordV1",
        assert_round_trip(
            atomic.entities()[0].post_image().clone(),
            encode_entity_record_v1,
            decode_entity_record_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredIndexEntryV1",
        assert_round_trip(
            sample::legacy_index_record(),
            encode_index_entry_v1,
            decode_index_entry_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredIndexEpochV1",
        assert_round_trip(
            sample::legacy_index_epoch(),
            encode_legacy_index_epoch_v1_fixture,
            decode_legacy_index_epoch_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredPendingAdmissionV1",
        assert_round_trip(
            atomic.expected_pending().clone(),
            encode_pending_admission_legacy_v1_fixture,
            decode_pending_admission_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredExecutionFailedV1",
        assert_round_trip(
            crate::StoredExecutionFailedV1::new(
                atomic.expected_pending().clone(),
                ExecutionFailureCode::ArithmeticFault,
            ),
            encode_execution_failed_legacy_v1_fixture,
            decode_execution_failed_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredOutcomeV1",
        assert_round_trip(
            atomic.stored_outcome().clone(),
            encode_stored_outcome_legacy_v1_fixture,
            decode_stored_outcome_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredDurableEventV1",
        assert_round_trip(
            atomic.events()[0].clone(),
            encode_durable_event_v1,
            decode_durable_event_v1,
        ),
    ));
    let event_id = EventId::new(CommitSequence::first(), 0);
    let event_type = EventTypeId::first();
    let payload = sample::canonical_record(0x43);
    let anchor = crate::StoredEventPolicyAnchorV1::new(
        crate::DurableKeySchemaBindingV1::from_plan(&sample::plan()),
        event_type,
        sample::entity_target(),
        RowPolicyName::new("TicketAccess").expect("policy"),
    );
    let hash = crate::derive_event_hash_v2(event_id, event_type, &payload, &anchor)
        .expect("anchored event hash");
    vectors.push((
        "riffdb.storage.v1.StoredDurableEventV2",
        assert_round_trip(
            crate::StoredDurableEventV2::new(event_id, event_type, payload, hash, anchor)
                .expect("anchored event"),
            encode_durable_event_v2,
            decode_durable_event_v2,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredOutboxIntentV1",
        assert_round_trip(
            atomic.outbox_intents()[0].clone(),
            encode_outbox_intent_legacy_v1,
            decode_outbox_intent_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredProvenanceRecordV1",
        assert_round_trip(
            atomic.provenance().clone(),
            encode_provenance_record_legacy_v1_fixture,
            decode_provenance_record_v1,
        ),
    ));
    vectors.push(("riffdb.storage.v1.StoredCommitRecordV1", {
        let entities = atomic.entities().to_vec();
        assert_round_trip(
            atomic.commit().clone(),
            |commit| encode_commit_record_legacy_v1(commit, &entities),
            decode_commit_record_v1,
        )
    }));

    let (capability, lookup, marker, capability_administration) = sample::capability_records();
    vectors.push((
        "riffdb.storage.v1.CapabilityRecordV1",
        assert_round_trip(
            capability,
            encode_capability_record_v1,
            decode_capability_record_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.CapabilityTokenLookupV1",
        assert_round_trip(
            lookup,
            |value| encode_capability_token_lookup_v1(*value),
            decode_capability_token_lookup_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.CapabilityBootstrapMarkerV1",
        assert_round_trip(
            marker,
            |value| encode_capability_bootstrap_marker_v1(*value),
            decode_capability_bootstrap_marker_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.CapabilityAdministrationAuditV1",
        assert_round_trip(
            capability_administration,
            encode_capability_administration_v1,
            decode_capability_administration_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.ServiceAuditRecordV1",
        assert_round_trip(
            sample::service_audit_record(),
            encode_service_audit_record_legacy_v1,
            decode_service_audit_record_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredOutboxStatusV1",
        assert_round_trip(
            sample::outbox_status(),
            encode_outbox_status_v1,
            decode_outbox_status_v1,
        ),
    ));

    let (schema, state, apply, control) = sample::projection_records();
    vectors.push((
        "riffdb.storage.v1.StoredProjectionStateV1",
        assert_round_trip(state, encode_projection_state_v1, |bytes| {
            decode_projection_state_v1(bytes, &schema)
        }),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredProjectionApplyV1",
        assert_round_trip(
            apply,
            encode_projection_apply_v1,
            decode_projection_apply_v1,
        ),
    ));
    vectors.push((
        "riffdb.storage.v1.StoredProjectionControlV1",
        assert_round_trip(
            control,
            encode_projection_control_v1,
            decode_projection_control_v1,
        ),
    ));
    vectors
}

#[test]
fn every_registered_semantic_record_round_trips_in_registry_order() {
    let vectors = semantic_wire_vectors();
    assert_eq!(vectors.len(), 27);
    for ((name, _), schema) in vectors
        .iter()
        .filter(|(name, _)| *name != "riffdb.storage.v1.StoredDurableEventV2")
        .zip(riffdb_proto::durable::READABLE_RECORD_SCHEMAS.iter())
    {
        assert_eq!(*name, schema.record_type());
    }
    assert!(
        riffdb_proto::durable::readable_record_schema("riffdb.storage.v1.StoredDurableEventV2")
            .is_some()
    );

    let (current, _) = sample::index_records();
    assert_round_trip(current, encode_index_entry_v2, decode_index_entry_v2);

    let atomic = sample::atomic_record_set();
    let event = &atomic.events()[0];
    assert_round_trip(
        crate::StoredEventRouteV1::new(event.event_id(), event.event_type_id(), event.event_hash()),
        |value| encode_event_route_v1(*value),
        decode_event_route_v1,
    );
    assert_round_trip(
        atomic.outbox_intents()[0].clone(),
        encode_outbox_intent_v1,
        |bytes| decode_outbox_intent_v2(bytes, atomic.events()[0].clone()),
    );
    assert_round_trip(atomic.commit().clone(), encode_commit_record_v1, |bytes| {
        decode_commit_record_v3(bytes, atomic.events().to_vec())
    });
    assert_round_trip(
        sample::service_audit_record(),
        encode_service_audit_record_v2,
        decode_service_audit_record,
    );
}

#[test]
fn command_capsule_round_trip_reconstructs_every_existing_view_exactly() {
    let atomic = sample::atomic_record_set();
    let audit_basis = sample::service_audit_record();
    let started = crate::StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::first(),
        audit_basis.request_id(),
        audit_basis.timestamp(),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Started,
        audit_basis.principal().cloned(),
        audit_basis.ingress(),
        audit_basis.targets().clone(),
        audit_basis.approval_id().cloned(),
        ServiceAuditLinkV1::None,
    )
    .expect("command start is valid");
    let terminal_sequence = AdministrationSequence::first()
        .checked_next()
        .expect("second administration sequence");
    let terminal = crate::StoredServiceAuditRecordV1::from_stored_parts(
        terminal_sequence,
        audit_basis.request_id(),
        audit_basis.timestamp(),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Succeeded,
        audit_basis.principal().cloned(),
        audit_basis.ingress(),
        audit_basis.targets().clone(),
        audit_basis.approval_id().cloned(),
        ServiceAuditLinkV1::Command {
            commit_sequence: atomic.commit().commit_sequence(),
            provenance_id: atomic.commit().provenance_id(),
        },
    )
    .expect("command terminal is valid");
    let capsule = crate::StoredCommandCapsuleV1::new(
        atomic.stored_outcome().clone(),
        atomic.provenance().clone(),
        atomic.commit().clone(),
        started,
        terminal,
    )
    .expect("reciprocal capsule");

    let encoded = encode_command_capsule_v1(&capsule).expect("capsule encodes");
    let decoded = decode_command_capsule_v1(encoded.as_bytes(), atomic.events().to_vec())
        .expect("capsule decodes");
    assert_eq!(decoded.value(), &capsule);
    assert_eq!(decoded.value().outcome(), atomic.stored_outcome());
    assert_eq!(decoded.value().provenance(), atomic.provenance());
    assert_eq!(decoded.value().commit(), atomic.commit());

    let locator = crate::StoredCommandLocatorV1::new(atomic.commit().commit_sequence());
    assert_round_trip(
        locator,
        |value| encode_command_locator_v1(*value),
        decode_command_locator_v1,
    );
    for member in [
        crate::StoredCommandAuditMemberV1::Started,
        crate::StoredCommandAuditMemberV1::Terminal,
    ] {
        let locator =
            crate::StoredCommandAuditLocatorV1::new(atomic.commit().commit_sequence(), member);
        assert_round_trip(
            locator,
            |value| encode_command_audit_locator_v1(*value),
            decode_command_audit_locator_v1,
        );
        assert_eq!(decoded.value().audit(member), capsule.audit(member));
    }
}

fn sample_command_capsule_v1() -> (crate::StoredCommandCapsuleV1, crate::AtomicCommandRecordSet) {
    let atomic = sample::atomic_record_set();
    let capsule = sample_command_capsule_v1_from_atomic(&atomic, AdministrationSequence::first());
    (capsule, atomic)
}

fn sample_command_capsule_v1_from_atomic(
    atomic: &crate::AtomicCommandRecordSet,
    started_sequence: AdministrationSequence,
) -> crate::StoredCommandCapsuleV1 {
    let audit_basis = sample::service_audit_record();
    let started = crate::StoredServiceAuditRecordV1::from_stored_parts(
        started_sequence,
        atomic.commit().admission_request_id(),
        audit_basis.timestamp(),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Started,
        audit_basis.principal().cloned(),
        audit_basis.ingress(),
        audit_basis.targets().clone(),
        audit_basis.approval_id().cloned(),
        ServiceAuditLinkV1::None,
    )
    .expect("command start is valid");
    let terminal = crate::StoredServiceAuditRecordV1::from_stored_parts(
        started_sequence
            .checked_next()
            .expect("second administration sequence"),
        atomic.commit().admission_request_id(),
        audit_basis.timestamp(),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Succeeded,
        audit_basis.principal().cloned(),
        audit_basis.ingress(),
        audit_basis.targets().clone(),
        audit_basis.approval_id().cloned(),
        ServiceAuditLinkV1::Command {
            commit_sequence: atomic.commit().commit_sequence(),
            provenance_id: atomic.commit().provenance_id(),
        },
    )
    .expect("command terminal is valid");
    crate::StoredCommandCapsuleV1::new(
        atomic.stored_outcome().clone(),
        atomic.provenance().clone(),
        atomic.commit().clone(),
        started,
        terminal,
    )
    .expect("reciprocal capsule")
}

fn delete_command_segment_fixture(
    transition: crate::CommittedEntityTransitionV1,
) -> CanonicalStoredEnvelopeV1 {
    let sequence = transition.command_sequence();
    let atomic = sample::atomic_record_set_at(
        sequence,
        riffdb_types::RequestId::from_bytes(sample::uuid_v7(0x81)).expect("delete request"),
        riffdb_types::ProvenanceId::from_bytes(sample::uuid_v7(0x82)).expect("delete provenance"),
    );
    let base = sample_command_capsule_v1_from_atomic(
        &atomic,
        AdministrationSequence::new(3).expect("delete started audit"),
    );
    let (outcome, provenance, commit, started, terminal) = base.into_parts();
    let deleted_version = match transition.prior_state() {
        crate::EntityChainStateV1::Live { version, .. } => version,
        _ => panic!("delete fixture requires a live prior"),
    };
    let dependencies = crate::StoredReadDependenciesV1::new(vec![
        crate::StoredReadDependencyV1::EntityObservation {
            target: transition.target().clone(),
            expected: crate::ExpectedEntityState::Present(deleted_version),
        },
    ])
    .expect("delete read dependency");
    let delete_commit = crate::StoredCommitRecordV1::new(
        commit.commit_sequence(),
        commit.admission_request_id(),
        commit.plan().clone(),
        commit.canonical_input_hash(),
        commit.actor().clone(),
        commit.logical_time(),
        commit.partition_hash(),
        commit.conflict_hashes().to_vec(),
        dependencies,
        Vec::new(),
        commit.events().to_vec(),
        commit.declared_outcome().clone(),
        commit.provenance_id(),
        commit.outbox_event_ids().to_vec(),
        commit.durability_mode(),
    )
    .expect("delete commit");
    let delete_provenance = crate::StoredProvenanceRecordV1::new_with_causation(
        provenance.provenance_id(),
        provenance.commit_sequence(),
        provenance.identity().clone(),
        provenance.admission_request_id(),
        provenance.plan().clone(),
        provenance.canonical_input_hash(),
        provenance.actor().clone(),
        provenance.logical_time(),
        provenance.partition_hash(),
        provenance.conflict_hashes().to_vec(),
        provenance.outcome_id(),
        vec![crate::AffectedEntityV1::from_stored_parts(
            transition.target().clone(),
            deleted_version,
        )],
        provenance.event_ids().to_vec(),
        provenance.admitted_claims().clone(),
        provenance.causation(),
    )
    .expect("delete provenance");
    let delete_base = crate::StoredCommandCapsuleV1::new(
        outcome,
        delete_provenance,
        delete_commit,
        started,
        terminal,
    )
    .expect("reciprocal delete capsule");
    let capsule = crate::StoredCommandCapsuleV2::from_base_with_entity_transitions(
        delete_base,
        Vec::new(),
        vec![transition],
    )
    .expect("delete transition capsule");
    let manifest = crate::CommandSegmentManifestV1::new(vec![
        crate::CommandDerivedIndexManifestEntryV1::new(
            crate::CommandDerivedIndexKindV1::Idempotency,
            crate::CommandDerivedMemberV1::Command,
            vec![0x81],
            0,
            0,
            sequence,
        )
        .expect("delete segment manifest entry"),
    ])
    .expect("delete segment manifest");
    let draft = crate::StoredCommandSegmentV1::new(
        sample::database_id(),
        crate::HISTORY_INCARNATION_INITIAL,
        None,
        vec![capsule],
        manifest,
        crate::CommandSegmentDigestV1::from_bytes([0; 32]),
    )
    .expect("delete segment draft");
    seal_and_encode_command_segment_v1(draft)
        .expect("seal delete segment")
        .1
}

#[test]
fn delete_aware_follower_requires_exact_tombstone_reciprocity_before_apply() {
    let create_atomic = sample::atomic_record_set();
    let entity = create_atomic.entities()[0].post_image().clone();
    let value_hash = crate::derive_entity_record_hash_v1(&entity).expect("entity hash");
    let create = crate::CommittedEntityTransitionV1::new(
        CommitSequence::first(),
        0,
        entity.target().clone(),
        crate::EntityChainStateV1::NeverExisted,
        0,
        None,
        crate::EntityChainStateV1::Live {
            version: entity.entity_version(),
            value_hash,
        },
    )
    .expect("create transition");
    let prior_head = crate::EntityChainHeadV1::from_genesis(&create).expect("prior live head");
    let delete_sequence = CommitSequence::new(2).expect("delete sequence");
    let deletion = crate::CommittedEntityTransitionV1::new(
        delete_sequence,
        0,
        entity.target().clone(),
        prior_head.state(),
        prior_head.chain_revision(),
        Some(prior_head.last_transition_hash()),
        crate::EntityChainStateV1::Deleted,
    )
    .expect("delete transition");
    let deleted_head = prior_head.apply(&deletion).expect("deleted head");

    let predecessor = riffdb_types::DualFrontier::new(Some(CommitSequence::first()), None);
    let receipt = crate::ChangelogV2RotationReceipt::new(
        sample::database_id(),
        crate::HISTORY_INCARNATION_INITIAL,
        predecessor,
        [0x31; 32],
    )
    .expect("rotation receipt");
    let manifest = crate::EntityReplicaBootstrapManifestV2::new(
        receipt,
        predecessor,
        receipt.v2_chain_anchor(),
        crate::ValidatedPrefixEntityTransitionCounts {
            live_entity_count: 1,
            deleted_entity_count: 0,
            entity_transition_count: 1,
        },
        crate::EntityTransitionFingerprint::from_sorted_heads([&prior_head])
            .expect("prior head fingerprint"),
    )
    .expect("bootstrap manifest");
    let encoded_entity = encode_entity_record_v1(&entity).expect("encode entity");
    let encoded_prior_head = encode_entity_chain_head_v1(&prior_head).expect("encode prior head");
    let bootstrap_follower = || {
        let bootstrap_row = crate::EntityReplicaBootstrapRowV2::new(
            entity.target().key().as_bytes(),
            Some(encoded_entity.as_bytes()),
            encoded_prior_head.as_bytes(),
        )
        .expect("bootstrap row");
        let mut follower = crate::DeleteAwareEntityFollowerV2::from_bootstrap(receipt, manifest)
            .expect("matching bootstrap");
        follower
            .install_bootstrap_page(vec![bootstrap_row], true)
            .expect("install prior live state");
        follower
    };
    let mut follower = bootstrap_follower();

    let commit = delete_command_segment_fixture(deletion.clone());
    let head = encode_entity_chain_head_v1(&deleted_head).expect("encode deleted head");
    let entries = vec![
        crate::ChangelogEntryV2::put(
            crate::ChangelogEntryClassV2::Commit,
            delete_sequence.to_be_bytes(),
            commit.as_bytes(),
        )
        .expect("commit entry"),
        crate::ChangelogEntryV2::entity_delete_tombstone(deletion.clone()).expect("tombstone"),
        crate::ChangelogEntryV2::put(
            crate::ChangelogEntryClassV2::EntityChainHead,
            entity.target().key().as_bytes(),
            head.as_bytes(),
        )
        .expect("head entry"),
    ];
    let binding = crate::ChangelogFrameBindingV2 {
        database_id: sample::database_id(),
        history_incarnation: crate::HISTORY_INCARNATION_INITIAL,
        chain_hash: receipt.v2_chain_anchor(),
        journal_frame_hash: [0x41; 32],
        journaled: true,
    };
    let covered = riffdb_types::DualFrontier::new(Some(delete_sequence), None);
    let missing = crate::ChangelogFrameV2::new(
        binding,
        predecessor,
        covered,
        vec![entries[0].clone(), entries[2].clone()],
    )
    .expect("structural frame missing tombstone")
    .encode()
    .expect("encode missing frame");
    assert_eq!(
        follower.apply_encoded(missing.as_bytes()),
        Err(crate::ChangelogFrameV2Error::InvalidEntry)
    );
    assert!(
        follower
            .entity_value(entity.target().key().as_bytes())
            .is_some()
    );
    assert_eq!(follower.applied_frontier(), predecessor);

    let complete = crate::ChangelogFrameV2::new(binding, predecessor, covered, entries)
        .expect("complete delete frame")
        .encode()
        .expect("encode complete frame");
    assert_eq!(follower.apply_encoded(complete.as_bytes()), Ok(covered));
    assert!(
        follower
            .entity_value(entity.target().key().as_bytes())
            .is_none()
    );
    assert_eq!(
        follower.chain_head_value(entity.target().key().as_bytes()),
        Some(head.as_bytes())
    );
    assert_eq!(
        follower.apply_encoded(complete.as_bytes()),
        Err(crate::ChangelogFrameV2Error::Gap)
    );

    for stale in [
        crate::CommittedEntityTransitionV1::new(
            delete_sequence,
            0,
            entity.target().clone(),
            crate::EntityChainStateV1::Live {
                version: entity.entity_version(),
                value_hash: riffdb_types::EntityRecordHash::from_bytes([0x91; 32]),
            },
            prior_head.chain_revision(),
            Some(prior_head.last_transition_hash()),
            crate::EntityChainStateV1::Deleted,
        )
        .expect("prior-value-substituted transition"),
        crate::CommittedEntityTransitionV1::new(
            delete_sequence,
            0,
            entity.target().clone(),
            prior_head.state(),
            prior_head.chain_revision(),
            Some(riffdb_types::EntityTransitionHash::from_bytes([0x92; 32])),
            crate::EntityChainStateV1::Deleted,
        )
        .expect("prior-transition-substituted transition"),
    ] {
        let stale_commit = delete_command_segment_fixture(stale.clone());
        let stale_frame = crate::ChangelogFrameV2::new(
            binding,
            predecessor,
            covered,
            vec![
                crate::ChangelogEntryV2::put(
                    crate::ChangelogEntryClassV2::Commit,
                    delete_sequence.to_be_bytes(),
                    stale_commit.as_bytes(),
                )
                .expect("stale commit entry"),
                crate::ChangelogEntryV2::entity_delete_tombstone(stale).expect("stale tombstone"),
                crate::ChangelogEntryV2::put(
                    crate::ChangelogEntryClassV2::EntityChainHead,
                    entity.target().key().as_bytes(),
                    head.as_bytes(),
                )
                .expect("final head entry"),
            ],
        )
        .expect("structural stale frame")
        .encode()
        .expect("encode stale frame");
        let mut follower = bootstrap_follower();
        assert_eq!(
            follower.apply_encoded(stale_frame.as_bytes()),
            Err(crate::ChangelogFrameV2Error::InvalidEntry)
        );
        assert!(
            follower
                .entity_value(entity.target().key().as_bytes())
                .is_some()
        );
        assert_eq!(follower.applied_frontier(), predecessor);
    }
}

#[test]
fn command_segment_write_path_is_byte_identical_for_multi_command_nondefault_fields() {
    let first_atomic = sample::atomic_record_set();
    let second_sequence = CommitSequence::first()
        .checked_next()
        .expect("second commit sequence");
    let second_atomic = sample::atomic_record_set_at_with_causation(
        second_sequence,
        riffdb_types::RequestId::from_bytes(sample::uuid_v7(0x51)).expect("second request"),
        riffdb_types::ProvenanceId::from_bytes(sample::uuid_v7(0x52)).expect("second provenance"),
        Some(crate::StoredCommandCausationV1::new(
            riffdb_types::EventId::new(CommitSequence::first(), 0),
            sample::request_id(),
        )),
    );
    let (_, first_epoch) = sample::index_records();
    let first_transition = crate::IndexEpochAdvanceV1::new(
        first_epoch.target().clone(),
        first_epoch.schema_binding().clone(),
        riffdb_types::IndexEpochPosition::Value(riffdb_types::IndexEpoch::first()),
    )
    .expect("first generation transition");
    let first = crate::StoredCommandCapsuleV2::new(
        sample_command_capsule_v1_from_atomic(&first_atomic, AdministrationSequence::first()),
        first_atomic.events().to_vec(),
        vec![first_transition],
    )
    .expect("first complete V2 capsule");
    let second = crate::StoredCommandCapsuleV2::new(
        sample_command_capsule_v1_from_atomic(
            &second_atomic,
            AdministrationSequence::new(3).expect("third administration sequence"),
        ),
        second_atomic.events().to_vec(),
        second_atomic.index_epochs().to_vec(),
    )
    .expect("second complete V2 capsule");
    let first_sequence = first.commit_sequence();
    let mut entries = vec![
        crate::CommandDerivedIndexManifestEntryV1::new(
            crate::CommandDerivedIndexKindV1::Idempotency,
            crate::CommandDerivedMemberV1::Command,
            vec![0x01, 0x02],
            0,
            0,
            first_sequence,
        )
        .expect("first command entry"),
        crate::CommandDerivedIndexManifestEntryV1::new(
            crate::CommandDerivedIndexKindV1::Provenance,
            crate::CommandDerivedMemberV1::Command,
            vec![0x03, 0x04, 0x05],
            1,
            0,
            first_sequence,
        )
        .expect("second command entry"),
        crate::CommandDerivedIndexManifestEntryV1::new(
            crate::CommandDerivedIndexKindV1::AuditSequence,
            crate::CommandDerivedMemberV1::AuditTerminal,
            vec![0x06],
            1,
            0,
            first_sequence,
        )
        .expect("nonzero command ordinal audit entry"),
        crate::CommandDerivedIndexManifestEntryV1::new(
            crate::CommandDerivedIndexKindV1::EventRoute,
            crate::CommandDerivedMemberV1::Event,
            vec![0x07, 0x08],
            1,
            0,
            first_sequence,
        )
        .expect("event entry"),
    ];
    entries.sort();
    let manifest = crate::CommandSegmentManifestV1::new(entries).expect("canonical rich manifest");
    let placeholder = crate::CommandSegmentDigestV1::from_bytes([0; 32]);
    let draft = crate::StoredCommandSegmentV1::new(
        sample::database_id(),
        crate::HISTORY_INCARNATION_INITIAL + 1,
        Some(crate::CommandSegmentDigestV1::from_bytes([0x91; 32])),
        vec![first, second],
        manifest,
        placeholder,
    )
    .expect("rich structural draft");

    let (sealed, streamed) =
        seal_and_encode_command_segment_v1(draft).expect("write-path sealing succeeds");
    let independent =
        encode_command_segment_v1(&sealed).expect("independent Prost encoding succeeds");
    assert_eq!(streamed, independent);
    assert_eq!(
        decode_command_segment_v1(streamed.as_bytes())
            .expect("streamed segment decodes")
            .value(),
        &sealed
    );
}

#[test]
fn command_segment_write_path_is_byte_identical_for_large_bounded_manifest() {
    let (base, atomic) = sample_command_capsule_v1();
    let capsule = crate::StoredCommandCapsuleV2::new(
        base,
        atomic.events().to_vec(),
        atomic.index_epochs().to_vec(),
    )
    .expect("complete V2 capsule");
    let first = capsule.commit_sequence();
    // This is larger than any maximum 256-command runtime segment can
    // currently derive while remaining below the global durable preflight
    // field-visit budget.
    let entries = (1..=59_000)
        .map(|ordinal| {
            crate::CommandDerivedIndexManifestEntryV1::new(
                crate::CommandDerivedIndexKindV1::Idempotency,
                crate::CommandDerivedMemberV1::Command,
                u32::try_from(ordinal)
                    .expect("bounded manifest ordinal")
                    .to_be_bytes()
                    .to_vec(),
                0,
                0,
                first,
            )
            .expect("bounded manifest entry")
        })
        .collect();
    let manifest =
        crate::CommandSegmentManifestV1::new(entries).expect("maximum canonical manifest");
    let draft = crate::StoredCommandSegmentV1::new(
        sample::database_id(),
        crate::HISTORY_INCARNATION_INITIAL,
        None,
        vec![capsule],
        manifest,
        crate::CommandSegmentDigestV1::from_bytes([0; 32]),
    )
    .expect("maximum-manifest structural draft");

    let (sealed, streamed) =
        seal_and_encode_command_segment_v1(draft).expect("streaming seal accepts the bound");
    assert_eq!(
        streamed,
        encode_command_segment_v1(&sealed).expect("independent encoder accepts the bound")
    );
}

#[test]
fn command_segment_round_trip_proves_hash_manifest_and_semantic_views() {
    let (base, atomic) = sample_command_capsule_v1();
    let service_values = riffdb_types::CanonicalRecord::new(vec![(
        riffdb_types::FieldId::new(9).expect("service field"),
        riffdb_types::CanonicalValue::Uuid(sample::uuid_v7(0x77)),
    )])
    .expect("service values");
    let (outcome, provenance, commit, started, terminal) = base.into_parts();
    let outcome = outcome
        .with_service_values(service_values.clone())
        .expect("service values attach");
    let base = crate::StoredCommandCapsuleV1::new(outcome, provenance, commit, started, terminal)
        .expect("service-value capsule");
    let capsule = crate::StoredCommandCapsuleV2::new(
        base,
        atomic.events().to_vec(),
        atomic.index_epochs().to_vec(),
    )
    .expect("complete V2 capsule");
    assert_round_trip(
        capsule.clone(),
        encode_command_capsule_v2,
        decode_command_capsule_v2,
    );

    let first = capsule.commit_sequence();
    let manifest_entry = crate::CommandDerivedIndexManifestEntryV1::new(
        crate::CommandDerivedIndexKindV1::Idempotency,
        crate::CommandDerivedMemberV1::Command,
        vec![0x01],
        0,
        0,
        first,
    )
    .expect("bounded manifest entry");
    let manifest = crate::CommandSegmentManifestV1::new(vec![manifest_entry.clone()])
        .expect("canonical manifest");
    let placeholder = crate::CommandSegmentDigestV1::from_bytes([0; 32]);
    let draft = crate::StoredCommandSegmentV1::new(
        sample::database_id(),
        crate::HISTORY_INCARNATION_INITIAL,
        None,
        vec![capsule.clone()],
        manifest.clone(),
        placeholder,
    )
    .expect("structural draft");
    let digest = command_segment_digest_v1(&draft);
    let segment = crate::StoredCommandSegmentV1::new(
        sample::database_id(),
        crate::HISTORY_INCARNATION_INITIAL,
        None,
        vec![capsule],
        manifest,
        digest,
    )
    .expect("digest-bound segment");
    let (sealed, sealed_encoded) =
        seal_and_encode_command_segment_v1(draft).expect("write-path sealing is canonical");
    assert_eq!(sealed, segment);
    let encoded = assert_round_trip(
        segment.clone(),
        encode_command_segment_v1,
        decode_command_segment_v1,
    );
    assert_eq!(sealed_encoded, encoded);
    assert_eq!(
        sealed.commands()[0].base().outcome().service_values(),
        &service_values
    );

    let mut corrupt = encoded.into_bytes();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x01;
    assert!(decode_command_segment_v1(&corrupt).is_err());

    let checkpoint_draft = crate::StoredCommandDerivedIndexCheckpointV1::new(
        sample::database_id(),
        crate::HISTORY_INCARNATION_INITIAL,
        [0x42; 32],
        first,
        digest,
        vec![manifest_entry.clone()],
        placeholder,
    )
    .expect("structural checkpoint draft");
    let checkpoint_digest = command_derived_index_checkpoint_digest_v1(&checkpoint_draft);
    let checkpoint = crate::StoredCommandDerivedIndexCheckpointV1::new(
        sample::database_id(),
        crate::HISTORY_INCARNATION_INITIAL,
        [0x42; 32],
        first,
        digest,
        vec![manifest_entry],
        checkpoint_digest,
    )
    .expect("digest-bound checkpoint");
    assert_round_trip(
        checkpoint,
        encode_command_derived_index_checkpoint_v1,
        decode_command_derived_index_checkpoint_v1,
    );
}

#[test]
fn emit_semantic_wire_vectors_for_fixture_regeneration() {
    let legacy_fixture = semantic_wire_fixture(DurableVectorFormat::LegacyV1);
    let compact_fixture = semantic_wire_fixture(DurableVectorFormat::CompactV2);
    print!("{legacy_fixture}");
    if let Some(path) = std::env::var_os("RIFFDB_DURABLE_VECTOR_OUTPUT") {
        let path = std::path::PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create vector parent directory");
        }
        std::fs::write(path, legacy_fixture).expect("write semantic wire fixture");
    }
    if let Some(path) = std::env::var_os("RIFFDB_DURABLE_V2_VECTOR_OUTPUT") {
        let path = std::path::PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create V2 vector parent directory");
        }
        std::fs::write(path, compact_fixture).expect("write compact semantic wire fixture");
    }
    if let Some(path) = std::env::var_os("RIFFDB_DURABLE_INDEX_V2_VECTOR_OUTPUT") {
        let path = std::path::PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create V2 vector parent directory");
        }
        std::fs::write(path, index_v2_wire_fixture()).expect("write V2 semantic wire fixture");
    }
    if let Some(path) = std::env::var_os("RIFFDB_EVENT_REFERENCE_V2_VECTOR_OUTPUT") {
        let path = std::path::PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create event-reference vector parent");
        }
        std::fs::write(path, event_reference_v2_wire_fixture())
            .expect("write event-reference V2 wire fixture");
    }
}

#[test]
fn checked_in_semantic_wire_vectors_are_current() {
    assert_eq!(
        semantic_wire_fixture(DurableVectorFormat::LegacyV1),
        include_str!("../../../../../fixtures/proto/durable-wire-vectors.txt")
    );
    assert_eq!(
        semantic_wire_fixture(DurableVectorFormat::CompactV2),
        include_str!("../../../../../fixtures/proto/durable-wire-vectors-v2.txt")
    );
    assert_eq!(
        index_v2_wire_fixture(),
        include_str!("../../../../../fixtures/proto/durable-index-v2-wire-vector.txt")
    );
    assert_eq!(
        event_reference_v2_wire_fixture(),
        include_str!("../../../../../fixtures/proto/durable-event-reference-v2-wire-vectors.txt")
    );
}

#[derive(Clone, Copy)]
enum DurableVectorFormat {
    LegacyV1,
    CompactV2,
}

fn semantic_wire_fixture(format: DurableVectorFormat) -> String {
    let heading = match format {
        DurableVectorFormat::LegacyV1 => "riffdb-durable-wire-vectors-v1",
        DurableVectorFormat::CompactV2 => "riffdb-durable-wire-vectors-v2",
    };
    let vectors = semantic_wire_vectors();
    let mut fixture = format!("{heading}\nrecords\t{}\n", vectors.len());
    for (name, envelope) in vectors {
        let decoded = riffdb_proto::durable::readable_record_registry()
            .decode(envelope.as_bytes())
            .expect("sample envelope decodes");
        let encoded = match format {
            DurableVectorFormat::LegacyV1 => {
                let schema = riffdb_proto::durable::readable_record_schema(name)
                    .expect("sample schema is readable");
                riffdb_proto::envelope::encode_v1(schema, decoded.payload())
                    .expect("legacy fixture payload remains encodable")
            }
            DurableVectorFormat::CompactV2 => transcode_durable_record_to_v2(envelope.as_bytes())
                .expect("legacy semantic vector transcodes to compact V2")
                .into_bytes(),
        };
        let line = format!("{name}\t{}\t{}\n", hex(decoded.payload()), hex(&encoded));
        fixture.push_str(&line);
    }
    fixture
}

fn index_v2_wire_fixture() -> String {
    let (value, _) = sample::index_records();
    let envelope = assert_round_trip(value, encode_index_entry_v2, decode_index_entry_v2);
    let decoded = riffdb_proto::durable::readable_record_registry()
        .decode(envelope.as_bytes())
        .expect("V2 sample envelope decodes");
    let schema = riffdb_proto::durable::readable_record_schema(decoded.record_type())
        .expect("V2 schema is readable");
    let legacy = riffdb_proto::envelope::encode_v1(schema, decoded.payload())
        .expect("V2 semantic payload remains legacy encodable");
    format!(
        "riffdb-durable-index-v2-wire-vector-v1\n{}\t{}\t{}\n",
        decoded.record_type(),
        hex(decoded.payload()),
        hex(&legacy)
    )
}

fn event_reference_v2_wire_fixture() -> String {
    let atomic = sample::atomic_record_set();
    // Outbox remains V2; current commits are V3 entity-reference records.
    // The commit row is still emitted here so regeneration keeps event-reference
    // companion fixtures aligned with the writable commit schema.
    let vectors = [
        (
            "riffdb.storage.v1.StoredCommitRecordV3",
            encode_commit_record_v1(atomic.commit()).expect("current commit encodes"),
        ),
        (
            "riffdb.storage.v1.StoredOutboxIntentV2",
            encode_outbox_intent_v1(&atomic.outbox_intents()[0])
                .expect("current outbox intent encodes"),
        ),
    ];
    let registry = riffdb_proto::durable::readable_record_registry();
    let mut fixture =
        String::from("riffdb-durable-event-reference-v2-wire-vectors-v1\nrecords\t2\n");
    for (record_type, envelope) in vectors {
        let decoded = registry
            .decode(envelope.as_bytes())
            .expect("event-reference envelope decodes");
        assert_eq!(decoded.record_type(), record_type);
        fixture.push_str(&format!(
            "{record_type}\t{}\t{}\n",
            hex(decoded.payload()),
            hex(envelope.as_bytes())
        ));
    }
    fixture
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("String writes are infallible");
    }
    output
}
