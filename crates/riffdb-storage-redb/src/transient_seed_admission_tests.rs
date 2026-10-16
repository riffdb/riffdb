//! Real V7 command graphs must coexist with admitted ordinary locator ownership.
// req: TXN-042, STO-032, REC-001

use super::*;
use riffdb_storage_api::{
    AffectedEntityV1, AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N,
    CommandPrefixEvidenceV1, CommittedEntityReferenceV2, CommittedEntityTransitionV1,
    DurableKeySchemaBindingV1, EntityChainHeadV1, EntityChainStateV1, EntityTarget,
    ExpectedEntityState, StoredCommandCapsuleV1, StoredCommitRecordV1, StoredEntityRecordV1,
    StoredProvenanceRecordV1, StoredReadDependenciesV1, StoredReadDependencyV1,
};
use riffdb_types::{
    AdministrationSequence, CanonicalRecord, CanonicalValue, DualFrontier, EntityKeyBuilder,
    EntityTypeId, EntityVersion, FieldId,
};

fn seed_capsule(sequence: CommitSequence) -> StoredCommandCapsuleV2 {
    let original = crate::derived::follower_indexes::capsule(sequence);
    let base = original.base();
    let commit = base.commit();
    let mut key = EntityKeyBuilder::new(EntityTypeId::first());
    key.push_u64(sequence.get()).unwrap();
    let target = EntityTarget::new(EntityTypeId::first(), key.finish().unwrap()).unwrap();
    let binding = DurableKeySchemaBindingV1::from_plan(commit.plan());
    let image = StoredEntityRecordV1::new(
        target.clone(),
        EntityVersion::first(),
        binding.contract_version(),
        binding,
        CanonicalRecord::new(
            (1..=8)
                .map(|field| {
                    (
                        FieldId::new(field).unwrap(),
                        CanonicalValue::U64(sequence.get()),
                    )
                })
                .collect(),
        )
        .unwrap(),
    )
    .unwrap();
    let reference = CommittedEntityReferenceV2::from_post_image(&image).unwrap();
    let transition = CommittedEntityTransitionV1::new(
        sequence,
        0,
        target.clone(),
        EntityChainStateV1::NeverExisted,
        0,
        None,
        EntityChainStateV1::Live {
            version: reference.entity_version(),
            value_hash: reference.post_image_hash(),
        },
    )
    .unwrap();
    let head = EntityChainHeadV1::from_genesis(&transition).unwrap();
    let updated_commit = StoredCommitRecordV1::new(
        sequence,
        commit.admission_request_id(),
        commit.plan().clone(),
        commit.canonical_input_hash(),
        commit.actor().clone(),
        commit.logical_time(),
        commit.partition_hash(),
        commit.conflict_hashes().to_vec(),
        StoredReadDependenciesV1::new(vec![StoredReadDependencyV1::EntityObservation {
            target,
            expected: ExpectedEntityState::Absent,
        }])
        .unwrap(),
        vec![reference],
        commit.events().to_vec(),
        commit.declared_outcome().clone(),
        commit.provenance_id(),
        commit.outbox_event_ids().to_vec(),
        commit.durability_mode(),
    )
    .unwrap();
    let provenance = base.provenance();
    let updated_provenance = StoredProvenanceRecordV1::new(
        provenance.provenance_id(),
        sequence,
        provenance.identity().clone(),
        provenance.admission_request_id(),
        provenance.plan().clone(),
        provenance.canonical_input_hash(),
        provenance.actor().clone(),
        provenance.logical_time(),
        provenance.partition_hash(),
        provenance.conflict_hashes().to_vec(),
        provenance.outcome_id(),
        vec![AffectedEntityV1::from_record(&image)],
        commit.outbox_event_ids().to_vec(),
        provenance.admitted_claims().clone(),
    )
    .unwrap();
    let entity_key = crate::keys::encode_entity_key(image.target().key());
    let mut rows = vec![
        AuthoritativeMutationV3::put(
            N::Entities,
            entity_key,
            None,
            riffdb_storage_api::encode_entity_record_v1(&image)
                .unwrap()
                .as_bytes(),
        )
        .unwrap(),
        AuthoritativeMutationV3::put(
            N::EntityChainHeads,
            entity_key,
            None,
            riffdb_storage_api::encode_entity_chain_head_v1(&head)
                .unwrap()
                .as_bytes(),
        )
        .unwrap(),
    ];
    rows.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
    StoredCommandCapsuleV2::from_base_with_entity_transitions(
        StoredCommandCapsuleV1::new(
            base.outcome().clone(),
            updated_provenance,
            updated_commit,
            base.started_audit().clone(),
            base.terminal_audit().clone(),
        )
        .unwrap(),
        vec![],
        vec![transition],
    )
    .unwrap()
    .with_prefix_evidence(
        CommandPrefixEvidenceV1::new(
            DualFrontier::new(
                CommitSequence::new(sequence.get() - 1),
                AdministrationSequence::new((sequence.get() - 1) * 2),
            ),
            DualFrontier::new(
                Some(sequence),
                Some(base.terminal_audit().administration_sequence()),
            ),
            rows,
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn legal_seed_batch_decodes_with_retained_native_ownership_and_preserves_validation() {
    const COMMANDS: usize = 32;
    let commands = (1..=COMMANDS as u64)
        .map(|sequence| seed_capsule(CommitSequence::new(sequence).unwrap()))
        .collect();
    let (segment, encoded) = crate::derived::follower_indexes::segment(commands, None);
    assert_eq!(segment.commands().len(), COMMANDS);
    assert!(segment.commands().len() <= riffdb_storage_api::MAX_STAGED_COMMANDS);
    assert_eq!(segment.manifest().entries().len(), COMMANDS * 8);
    assert!(segment.commands().iter().all(|command| {
        command.events().len() == 1
            && command.entity_transitions().len() == 1
            && command.prefix_evidence().is_some()
    }));
    assert_eq!(
        crate::command_prefix::decode_segment(encoded.as_bytes())
            .unwrap()
            .value(),
        &segment,
        "independent entity images, chain heads, audit frontiers and reciprocal graph links validate"
    );
    let pool = NativeLocatorPool::isolated_for_tests();
    let baseline = pool.charged_bytes();
    // Other admitted native owners remain charged during a command's bounded
    // verification. This is the observed ordinary ownership at the first
    // population refusal; neither the 448-MiB ordinary ceiling nor the
    // maintenance reservation is changed to accommodate this valid batch.
    let pressure = pool
        .admit(382_903_734, LocatorAllocationClass::Ordinary)
        .unwrap();
    let work = riffdb_storage_api::locator_segment_decode_work_bound_v1(encoded.as_bytes())
        .unwrap()
        .unwrap();
    let mut prefix_bytes = 0;
    let shape = riffdb_proto::envelope::inspect_locator_segment_decode_shape_v1(
        encoded.as_bytes(),
        |prefix| {
            prefix_bytes += prefix.len();
            Ok(0)
        },
    )
    .unwrap()
    .unwrap();
    eprintln!(
        "locator-legal-seed-batch-v1 commands={COMMANDS} wire_bytes={} messages={} buffers={} canonical_heap={} prefix_bytes={prefix_bytes} admitted_work={work}",
        encoded.as_bytes().len(),
        shape.messages,
        shape.buffers,
        shape.canonical_heap_bytes,
    );
    let before = LOCATOR_CONSTRUCTION_DECODE_ATTEMPTS.with(std::cell::Cell::get);
    let decoded = decode_native_locator_construction_segment_in_pool(
        &pool,
        encoded.as_bytes(),
        None,
        LocatorAllocationClass::Ordinary,
        None,
    )
    .unwrap_or_else(|error| panic!("valid legal seed batch work={work}: {error:?}"))
    .unwrap();
    assert_eq!(*decoded, segment);
    assert_eq!(
        LOCATOR_CONSTRUCTION_DECODE_ATTEMPTS.with(std::cell::Cell::get),
        before + 1
    );
    drop(decoded);
    assert_eq!(pool.charged_bytes(), baseline + pressure.bytes());
    let mut damaged = encoded.as_bytes().to_vec();
    *damaged.last_mut().unwrap() ^= 1;
    assert_eq!(
        decode_native_locator_construction_segment_in_pool(
            &pool,
            &damaged,
            None,
            LocatorAllocationClass::Ordinary,
            None,
        )
        .unwrap_err()
        .kind(),
        StorageErrorKind::CorruptData
    );
    assert_eq!(pool.charged_bytes(), baseline + pressure.bytes());
    drop(pressure);
    assert_eq!(pool.charged_bytes(), baseline);
}

// req: TXN-042, STO-032, REC-001
#[test]
fn exact_manifest_metadata_fits_retained_pressure_and_retires_after_its_physical_items() {
    // The fixed window holds a real 32-command/256-member metadata owner and
    // its separately admitted keys, retirement slots and event routing map.
    // It is a behavioral capacity regression, not a recomputation of the
    // constructor's reservation formula.
    const RETAINED_METADATA_WINDOW: usize = 152 * 1024;
    let commands = (1..=32)
        .map(|sequence| seed_capsule(CommitSequence::new(sequence).unwrap()))
        .collect();
    let (segment, encoded) = crate::derived::follower_indexes::segment(commands, None);
    assert_eq!(segment.manifest().entries().len(), 256);
    assert_eq!(
        crate::command_prefix::decode_segment(encoded.as_bytes())
            .unwrap()
            .value(),
        &segment
    );
    let pool = NativeLocatorPool::isolated_for_tests();
    let pressure = pool
        .admit(
            crate::locator_memory::LOCATOR_ORDINARY_BYTES - RETAINED_METADATA_WINDOW,
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
    let metadata =
        payload::SegmentMetadata::try_new(&pool, LocatorAllocationClass::Ordinary, &segment)
            .expect("real exact-capacity metadata fits the bounded retained window");
    metadata.validate(&segment).unwrap();
    for (actual, expected) in metadata
        .manifest
        .entries()
        .iter()
        .zip(segment.manifest().entries())
    {
        assert_eq!(actual.kind(), expected.kind());
        assert_eq!(actual.exact_key(), expected.exact_key());
        assert_eq!(
            actual.segment_first_commit_sequence(),
            expected.segment_first_commit_sequence()
        );
        assert_eq!(actual.command_ordinal(), expected.command_ordinal());
        assert_eq!(actual.member(), expected.member());
        assert_eq!(actual.member_ordinal(), expected.member_ordinal());
    }
    // Keep the independent native key allocations alive, so any change below
    // comes from this metadata owner's physical retirement, not its keys.
    let keys: Vec<_> = metadata
        .manifest
        .entries()
        .iter()
        .map(|entry| entry.charged_key())
        .collect();
    let clone = metadata.clone();
    let retained = pool.charged_bytes();
    drop(metadata);
    assert!(!pool.drain_retired_step());
    assert_eq!(
        pool.charged_bytes(),
        retained,
        "a retained alias owns every byte"
    );
    drop(clone);
    assert!(
        pool.drain_retired_step(),
        "256 manifest items consume the complete first quota"
    );
    assert_eq!(
        pool.charged_bytes(),
        retained,
        "the metadata reservation remains until manifest and event storage physically retire"
    );
    while pool.drain_retired_step() {}
    let keys_only = pool.charged_bytes();
    assert!(
        keys_only < retained,
        "metadata storage was physically reclaimed"
    );
    assert!(
        keys_only > pressure.bytes(),
        "separate shared keys remain admitted"
    );
    drop(keys);
    while pool.drain_retired_step() {}
    assert!(
        pool.charged_bytes() < keys_only,
        "key ownership follows its own final references"
    );
    drop(pressure);
    // The process-lifetime queue backing remains charged and reusable.
    let recycled = pool.exhaust_ordinary_for_tests();
    assert!(
        recycled.bytes() > crate::locator_memory::LOCATOR_ORDINARY_BYTES - RETAINED_METADATA_WINDOW
    );
}
