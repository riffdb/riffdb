//! Cross-module structural invariants for the engine-neutral storage boundary.

use riffdb_storage_api::{
    CommitIntent, DeclaredOutcome, DormantPortBundle, DurableKeySchemaBindingV1, EntityMutation,
    EntityObservation, EntityPostImage, EntityTarget, EvaluatedCommand, EvaluationBudget,
    EventIntent, ExecutablePlanRef, ExpectedEntityState, IdempotencyIdentity,
    IdempotencyIdentityKey, IdempotencyKeyDigest, IndexEpochPosition, IndexRangePrefixBuilder,
    IndexRangeTarget, OpenSessionId, PreEvaluationCommitContext, ReadDependencies, ReadDependency,
    ReadSnapshot, RetainedMetadataV1, SnapshotRequest, StorageValueError,
    StoredAdmittedProvenanceClaimsV1, StoredEntityRecordV1, StoredPendingAdmissionV1,
    StoredReadDependenciesV1, StructurallyOpened, TransactionCurrentState,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash, CanonicalRecord,
    CanonicalString, CanonicalValue, CommandId, ConflictKeyHash, ContractBundleHash,
    ContractLineage, ContractVersion, DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId,
    EntityVersion, Environment, EventTypeId, FieldId, IndexEpoch, IndexId, LogicalTime, OutcomeId,
    PartitionKeyBuilder, PlanHash, ProvenanceId, ProvenanceReason, RequestId, TenantId,
    TenantScope, Timestamp, hash_partition_key,
};

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

fn database_id() -> DatabaseId {
    DatabaseId::from_bytes(uuid_bytes(0x11)).expect("valid database UUIDv7")
}

struct TestDormantPorts;

struct TestCompletionAuthority;

impl DormantPortBundle for TestDormantPorts {
    type CompletionAuthority = TestCompletionAuthority;
}

fn request_id() -> RequestId {
    RequestId::from_bytes(uuid_bytes(0x22)).expect("valid request UUIDv7")
}

fn provenance_id() -> ProvenanceId {
    ProvenanceId::from_bytes(uuid_bytes(0x33)).expect("valid provenance UUIDv7")
}

fn tenant_scope() -> TenantScope {
    TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"))
}

fn plan(version: u64) -> ExecutablePlanRef {
    ExecutablePlanRef::new(
        ContractLineage::new("budget").expect("lineage"),
        ContractVersion::new(version).expect("nonzero version"),
        ContractBundleHash::from_bytes([version as u8; 32]),
        CommandId::new(1).expect("command"),
        PlanHash::from_bytes([0x44; 32]),
    )
}

fn target(value: u64) -> EntityTarget {
    let entity_type = EntityTypeId::new(1).expect("entity type");
    let mut builder = EntityKeyBuilder::new(entity_type);
    builder.push_u64(value).expect("bounded component");
    EntityTarget::new(entity_type, builder.finish().expect("entity key")).expect("matching target")
}

fn payload_record(length: usize) -> CanonicalRecord {
    CanonicalRecord::new(vec![(
        FieldId::new(1).expect("field"),
        riffdb_types::CanonicalValue::bytes(vec![0xa5; length]).expect("bounded bytes"),
    )])
    .expect("record")
}

fn pending(plan: ExecutablePlanRef) -> StoredPendingAdmissionV1 {
    pending_with_claims(plan, StoredAdmittedProvenanceClaimsV1::default())
}

fn pending_with_claims(
    plan: ExecutablePlanRef,
    provenance_claims: StoredAdmittedProvenanceClaimsV1,
) -> StoredPendingAdmissionV1 {
    let tenant_scope = tenant_scope();
    let actor_id = ActorId::new("principal-a").expect("actor");
    let actor = AdmittedActorContext::new(
        actor_id.clone(),
        ActorKind::Human,
        tenant_scope.clone(),
        None,
    );
    let identity = IdempotencyIdentity::new(
        database_id(),
        Environment::new("test").expect("environment"),
        tenant_scope,
        actor_id,
        plan.contract_lineage().clone(),
        plan.command_id(),
        IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(1).expect("digest key"), [0x55; 32]),
    );
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition.push_u64(9).expect("partition component");
    StoredPendingAdmissionV1::new(
        identity,
        CanonicalInputHash::from_bytes([0x66; 32]),
        request_id(),
        plan,
        LogicalTime::new(Timestamp::new(42, 7).expect("timestamp")),
        actor,
        partition.finish().expect("partition key"),
        provenance_claims,
    )
    .expect("pending admission")
}

fn conflict_hashes(count: usize) -> Vec<ConflictKeyHash> {
    (0..count)
        .map(|value| {
            let mut bytes = [0u8; 32];
            bytes[..4].copy_from_slice(
                &u32::try_from(value)
                    .expect("bounded conflict count")
                    .to_be_bytes(),
            );
            ConflictKeyHash::from_bytes(bytes)
        })
        .collect()
}

#[test]
fn maximum_idempotency_identity_key_is_exact_and_malformed_forms_reject() {
    let identity = IdempotencyIdentity::new(
        database_id(),
        Environment::new("e".repeat(64)).expect("maximum environment"),
        TenantScope::Tenant(TenantId::new("t".repeat(256)).expect("maximum tenant")),
        ActorId::new("a".repeat(256)).expect("maximum actor"),
        ContractLineage::new("l".repeat(256)).expect("maximum lineage"),
        CommandId::new(u32::MAX).expect("command"),
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(u32::MAX).expect("digest key"),
            [0xaa; 32],
        ),
    );
    let key = identity.storage_key().expect("maximum identity key");
    assert_eq!(key.as_bytes().len(), 908);
    assert_eq!(IdempotencyIdentityKey::decode(key.as_bytes()), Ok(identity));

    let normal = pending(plan(1))
        .identity()
        .storage_key()
        .expect("normal identity key")
        .as_bytes()
        .to_vec();
    let mut bad_tenant_tag = normal.clone();
    let tenant_tag = 2 + 16 + 4 + "test".len();
    bad_tenant_tag[tenant_tag] = 2;
    assert!(IdempotencyIdentityKey::decode(&bad_tenant_tag).is_err());

    let mut bad_scheme = normal.clone();
    let scheme = bad_scheme.len() - (1 + 4 + 32);
    bad_scheme[scheme] = 2;
    assert!(IdempotencyIdentityKey::decode(&bad_scheme).is_err());

    let mut trailing = normal.clone();
    trailing.push(0);
    assert!(IdempotencyIdentityKey::decode(&trailing).is_err());

    let mut impossible_environment = normal.clone();
    impossible_environment[18..22].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(IdempotencyIdentityKey::decode(&impossible_environment).is_err());

    let mut invalid_utf8 = normal;
    invalid_utf8[22] = 0xff;
    assert!(IdempotencyIdentityKey::decode(&invalid_utf8).is_err());
}

#[test]
fn conflicting_duplicate_entity_observations_fail_snapshot_construction() {
    let plan = plan(1);
    let target = target(1);
    let request = SnapshotRequest::new(
        plan.clone(),
        vec![target.clone()],
        vec![target.clone()],
        Vec::new(),
    )
    .expect("structural request");
    let present = StoredEntityRecordV1::new(
        target.clone(),
        EntityVersion::first(),
        plan.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        CanonicalRecord::new(Vec::new()).expect("record"),
    )
    .expect("entity record");
    assert_eq!(
        ReadSnapshot::new(
            &request,
            None,
            vec![EntityObservation::Absent(target)],
            vec![EntityObservation::Present(present)],
            Vec::new(),
        ),
        Err(StorageValueError::IdentityMismatch)
    );
}

#[test]
fn entity_postimage_binding_must_match_its_written_contract_version() {
    let writer = plan(1);
    let wrong_binding = DurableKeySchemaBindingV1::from_plan(&plan(2));
    assert_eq!(
        StoredEntityRecordV1::new(
            target(1),
            EntityVersion::first(),
            writer.contract_version(),
            wrong_binding,
            CanonicalRecord::new(Vec::new()).expect("record"),
        ),
        Err(StorageValueError::IdentityMismatch)
    );
}

#[test]
fn sealed_entity_postimage_materializes_without_reencoding_or_deep_clone() {
    let writer = plan(1);
    let fields = CanonicalRecord::new(vec![(
        FieldId::first(),
        CanonicalValue::String(CanonicalString::new("shared canonical field").expect("string")),
    )])
    .expect("record");
    let post_image = EntityPostImage::new(target(1), writer.contract_version(), fields.clone())
        .expect("post-image");
    let sealed = StoredEntityRecordV1::from_checked_post_image(
        &post_image,
        EntityVersion::first(),
        DurableKeySchemaBindingV1::from_plan(&writer),
    )
    .expect("sealed stored record");
    let full = StoredEntityRecordV1::new(
        target(1),
        EntityVersion::first(),
        writer.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&writer),
        fields,
    )
    .expect("fully reconstructed record");

    assert_eq!(sealed, full);
    assert!(std::ptr::eq(sealed.fields(), post_image.fields()));
    assert_eq!(sealed.fields_encoded(), post_image.fields_encoded());
    assert_eq!(
        riffdb_storage_api::encode_entity_record_v1(&sealed),
        riffdb_storage_api::encode_entity_record_v1(&full)
    );
}

#[test]
fn evaluated_command_rejects_noncanonical_mutation_order() {
    let plan = plan(1);
    let high_target = target(2);
    let low_target = target(1);
    let request = SnapshotRequest::new(
        plan.clone(),
        vec![high_target.clone(), low_target.clone()],
        Vec::new(),
        Vec::new(),
    )
    .expect("request");
    let snapshot = ReadSnapshot::new(
        &request,
        None,
        vec![
            EntityObservation::Absent(high_target.clone()),
            EntityObservation::Absent(low_target.clone()),
        ],
        Vec::new(),
        Vec::new(),
    )
    .expect("snapshot");
    let empty = CanonicalRecord::new(Vec::new()).expect("empty record");
    let high = EntityPostImage::new(high_target, plan.contract_version(), empty.clone())
        .expect("post image");
    let low = EntityPostImage::new(low_target, plan.contract_version(), empty.clone())
        .expect("post image");
    let outcome =
        DeclaredOutcome::new(OutcomeId::new(1).expect("outcome"), empty).expect("declared outcome");
    assert_eq!(
        EvaluatedCommand::new(
            &snapshot,
            vec![EntityMutation::Create(high), EntityMutation::Create(low)],
            Vec::new(),
            outcome,
            EvaluationBudget::v1(),
        ),
        Err(StorageValueError::NonCanonicalOrder)
    );
}

#[test]
fn evaluated_command_requires_snapshot_dependency_for_each_mutation() {
    let plan = plan(1);
    let request = SnapshotRequest::new(plan.clone(), Vec::new(), Vec::new(), Vec::new())
        .expect("empty request");
    let snapshot = ReadSnapshot::new(&request, None, Vec::new(), Vec::new(), Vec::new())
        .expect("empty snapshot");
    let empty = CanonicalRecord::new(Vec::new()).expect("record");
    let mutation = EntityMutation::Create(
        EntityPostImage::new(target(1), plan.contract_version(), empty.clone())
            .expect("post image"),
    );
    let outcome =
        DeclaredOutcome::new(OutcomeId::new(1).expect("outcome"), empty).expect("outcome");
    assert_eq!(
        EvaluatedCommand::new(
            &snapshot,
            vec![mutation],
            Vec::new(),
            outcome,
            EvaluationBudget::v1(),
        ),
        Err(StorageValueError::IdentityMismatch)
    );
}

#[test]
fn commit_intent_requires_exact_pending_plan_and_partition_hash() {
    let plan = plan(1);
    let request = SnapshotRequest::new(plan.clone(), Vec::new(), Vec::new(), Vec::new())
        .expect("empty request");
    let snapshot = ReadSnapshot::new(&request, None, Vec::new(), Vec::new(), Vec::new())
        .expect("empty snapshot");
    let evaluated = EvaluatedCommand::new(
        &snapshot,
        Vec::new(),
        Vec::new(),
        DeclaredOutcome::new(
            OutcomeId::new(1).expect("outcome"),
            CanonicalRecord::new(Vec::new()).expect("record"),
        )
        .expect("declared outcome"),
        EvaluationBudget::v1(),
    )
    .expect("evaluated command");
    let pending = pending(plan);
    let correct_partition_hash = hash_partition_key(pending.partition_key().as_bytes());
    let context =
        PreEvaluationCommitContext::new(pending.clone(), correct_partition_hash, Vec::new())
            .expect("checked pre-evaluation context");
    assert!(CommitIntent::new(context, evaluated, provenance_id()).is_ok());
    assert_eq!(
        PreEvaluationCommitContext::new(
            pending,
            riffdb_types::PartitionKeyHash::from_bytes([0xff; 32]),
            Vec::new(),
        ),
        Err(StorageValueError::IdentityMismatch)
    );
}

#[test]
fn pre_evaluation_context_enforces_the_exact_non_runtime_reserve() {
    let plan = plan(1);
    let base_pending = pending(plan.clone());
    let base = PreEvaluationCommitContext::new(
        base_pending,
        hash_partition_key(pending(plan.clone()).partition_key().as_bytes()),
        Vec::new(),
    )
    .expect("base context")
    .non_runtime_semantic_bytes();
    let reserve = riffdb_types::COMMIT_INTENT_NON_RUNTIME_RESERVE_BYTES;
    let mut conflict_count = (reserve - base) / 32;
    let mut added_claim_bytes = reserve - (base + conflict_count * 32);
    if added_claim_bytes < 5 {
        conflict_count -= 1;
        added_claim_bytes += 32;
    }
    let reason_length = added_claim_bytes.saturating_sub(4);
    let claims = StoredAdmittedProvenanceClaimsV1::new(
        None,
        None,
        Some(ProvenanceReason::new("r".repeat(reason_length)).expect("bounded reason")),
        None,
    )
    .expect("claims");
    let exact_pending = pending_with_claims(plan.clone(), claims);
    let exact = PreEvaluationCommitContext::new(
        exact_pending,
        hash_partition_key(pending(plan.clone()).partition_key().as_bytes()),
        conflict_hashes(conflict_count),
    )
    .expect("exact reserve");
    assert_eq!(exact.non_runtime_semantic_bytes(), reserve);

    let over_claims = StoredAdmittedProvenanceClaimsV1::new(
        None,
        None,
        Some(ProvenanceReason::new("r".repeat(reason_length + 1)).expect("bounded reason")),
        None,
    )
    .expect("claims");
    assert_eq!(
        PreEvaluationCommitContext::new(
            pending_with_claims(plan.clone(), over_claims),
            hash_partition_key(pending(plan).partition_key().as_bytes()),
            conflict_hashes(conflict_count),
        ),
        Err(StorageValueError::LimitExceeded)
    );
}

#[test]
fn initial_metadata_contains_only_canonical_initial_six_category_values() {
    let metadata = RetainedMetadataV1::initial(database_id());
    assert_eq!(metadata.storage_format_version().get(), 2);
    assert_eq!(metadata.database_id(), database_id());
    assert_eq!(
        metadata.application_sequence(),
        riffdb_storage_api::ApplicationSequenceAllocator::initial()
    );
    assert_eq!(
        metadata.administration_sequence(),
        riffdb_storage_api::AdministrationSequenceAllocator::initial()
    );
    assert!(metadata.active_catalog().is_none());
    assert!(metadata.capability_bootstrap().is_none());
}

#[test]
fn structurally_opened_carries_metadata_through_its_consuming_handoff() {
    let database_id = database_id();
    let open_session_id = OpenSessionId::new(7).expect("nonzero open session");
    let metadata = RetainedMetadataV1::initial(database_id);
    let opened = StructurallyOpened::from_finished_session(
        database_id,
        open_session_id,
        metadata.clone(),
        TestDormantPorts,
        TestCompletionAuthority,
    );

    assert_eq!(opened.database_id(), database_id);
    assert_eq!(opened.open_session_id(), open_session_id);
    assert_eq!(opened.retained_metadata(), &metadata);

    let (opened_database_id, opened_session_id, opened_metadata, _) = opened.into_parts();
    assert_eq!(opened_database_id, database_id);
    assert_eq!(opened_session_id, open_session_id);
    assert_eq!(opened_metadata, metadata);
}

#[test]
fn range_prefixes_are_component_built_and_use_exact_bytes_as_identity() {
    let index = IndexId::new(1).expect("index");
    let mut one_component = IndexRangePrefixBuilder::new(index);
    one_component.push_u64(0).expect("bounded component");
    let one_component = one_component.finish();

    let mut eight_components = IndexRangePrefixBuilder::new(index);
    for _ in 0..8 {
        eight_components
            .push_bool(false)
            .expect("bounded component");
    }
    let eight_components = eight_components.finish();

    assert_eq!(one_component.as_bytes(), eight_components.as_bytes());
    assert_eq!(one_component, eight_components);
    assert_eq!(
        one_component.cmp(&eight_components),
        std::cmp::Ordering::Equal
    );

    let empty = IndexRangePrefixBuilder::new(index).finish();
    assert_eq!(empty.as_bytes(), &[0x49, 0x01, 0, 0, 0, 1]);
}

#[test]
fn stored_read_dependencies_match_live_without_losing_any_semantics() {
    let entity = target(1);
    let other_entity = target(2);
    let index = IndexId::new(1).expect("index");
    let mut prefix = IndexRangePrefixBuilder::new(index);
    prefix.push_u64(7).expect("bounded component");
    let range = IndexRangeTarget::new(pending(plan(1)).partition_key().clone(), prefix.finish());
    let epoch = IndexEpoch::new(3).expect("epoch");
    let exact = ReadDependencies::new([
        ReadDependency::EntityObservation {
            target: entity.clone(),
            expected: ExpectedEntityState::Present(EntityVersion::first()),
        },
        ReadDependency::IndexRangeEpoch {
            target: range.clone(),
            expected: IndexEpochPosition::Value(epoch),
        },
    ])
    .expect("canonical dependencies");
    let stored = StoredReadDependenciesV1::from_live(&exact).expect("durable dependencies");
    assert!(stored.matches_live(&exact));

    let reverse_insertion_order = ReadDependencies::new([
        ReadDependency::IndexRangeEpoch {
            target: range.clone(),
            expected: IndexEpochPosition::Value(epoch),
        },
        ReadDependency::EntityObservation {
            target: entity.clone(),
            expected: ExpectedEntityState::Present(EntityVersion::first()),
        },
    ])
    .expect("canonically reordered dependencies");
    assert!(stored.matches_live(&reverse_insertion_order));

    let wrong_entity = ReadDependencies::new([
        ReadDependency::EntityObservation {
            target: other_entity,
            expected: ExpectedEntityState::Present(EntityVersion::first()),
        },
        ReadDependency::IndexRangeEpoch {
            target: range.clone(),
            expected: IndexEpochPosition::Value(epoch),
        },
    ])
    .expect("wrong entity dependencies");
    assert!(!stored.matches_live(&wrong_entity));

    let wrong_entity_state = ReadDependencies::new([
        ReadDependency::EntityObservation {
            target: entity.clone(),
            expected: ExpectedEntityState::Absent,
        },
        ReadDependency::IndexRangeEpoch {
            target: range.clone(),
            expected: IndexEpochPosition::Value(epoch),
        },
    ])
    .expect("wrong entity state dependencies");
    assert!(!stored.matches_live(&wrong_entity_state));

    let mut wrong_prefix = IndexRangePrefixBuilder::new(index);
    wrong_prefix.push_u64(8).expect("bounded component");
    let wrong_range = ReadDependencies::new([
        ReadDependency::EntityObservation {
            target: entity.clone(),
            expected: ExpectedEntityState::Present(EntityVersion::first()),
        },
        ReadDependency::IndexRangeEpoch {
            target: IndexRangeTarget::new(
                pending(plan(1)).partition_key().clone(),
                wrong_prefix.finish(),
            ),
            expected: IndexEpochPosition::Value(epoch),
        },
    ])
    .expect("wrong range dependencies");
    assert!(!stored.matches_live(&wrong_range));

    let wrong_range_state = ReadDependencies::new([
        ReadDependency::EntityObservation {
            target: entity.clone(),
            expected: ExpectedEntityState::Present(EntityVersion::first()),
        },
        ReadDependency::IndexRangeEpoch {
            target: range,
            expected: IndexEpochPosition::BeforeFirst,
        },
    ])
    .expect("wrong range state dependencies");
    assert!(!stored.matches_live(&wrong_range_state));

    let wrong_kind = ReadDependencies::new([
        ReadDependency::EntityObservation {
            target: entity.clone(),
            expected: ExpectedEntityState::Present(EntityVersion::first()),
        },
        ReadDependency::EntityObservation {
            target: target(3),
            expected: ExpectedEntityState::Absent,
        },
    ])
    .expect("wrong dependency kind");
    assert!(!stored.matches_live(&wrong_kind));

    let wrong_length = ReadDependencies::new([ReadDependency::EntityObservation {
        target: entity,
        expected: ExpectedEntityState::Present(EntityVersion::first()),
    }])
    .expect("short dependency set");
    assert!(!stored.matches_live(&wrong_length));
    assert!(
        !StoredReadDependenciesV1::from_live(&ReadDependencies::empty())
            .expect("empty durable dependencies")
            .matches_live(&exact)
    );
}

#[test]
fn aggregate_debug_output_never_contains_business_value_canaries() {
    const CANARY: &str = "storage-debug-secret-canary";
    let plan = plan(1);
    let record = CanonicalRecord::new(vec![(
        FieldId::new(1).expect("field"),
        riffdb_types::CanonicalValue::string(CANARY).expect("bounded string"),
    )])
    .expect("record");
    let entity = StoredEntityRecordV1::new(
        target(1),
        EntityVersion::first(),
        plan.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        record.clone(),
    )
    .expect("stored entity");
    let request = SnapshotRequest::new(
        plan.clone(),
        vec![entity.target().clone()],
        Vec::new(),
        Vec::new(),
    )
    .expect("request");
    let snapshot = ReadSnapshot::new(
        &request,
        None,
        vec![EntityObservation::Present(entity.clone())],
        Vec::new(),
        Vec::new(),
    )
    .expect("snapshot");
    let event = EventIntent::new(EventTypeId::new(1).expect("event"), record.clone())
        .expect("event intent");
    let outcome =
        DeclaredOutcome::new(OutcomeId::new(1).expect("outcome"), record).expect("outcome");
    let evaluated = EvaluatedCommand::new(
        &snapshot,
        Vec::new(),
        vec![event.clone()],
        outcome.clone(),
        EvaluationBudget::v1(),
    )
    .expect("evaluated");
    let pending = pending(plan);
    let context = PreEvaluationCommitContext::new(
        pending.clone(),
        hash_partition_key(pending.partition_key().as_bytes()),
        Vec::new(),
    )
    .expect("pre-evaluation context");
    let intent = CommitIntent::new(context, evaluated.clone(), provenance_id()).expect("intent");

    for debug in [
        format!("{entity:?}"),
        format!("{snapshot:?}"),
        format!("{event:?}"),
        format!("{outcome:?}"),
        format!("{evaluated:?}"),
        format!("{intent:?}"),
    ] {
        assert!(!debug.contains(CANARY), "debug output leaked the canary");
        assert!(debug.contains("REDACTED"));
    }
}

#[test]
fn transaction_current_state_rejects_exactly_one_owned_byte_over() {
    const BULK: usize = 900_000;
    let plan = plan(1);
    let targets = (1..=19).map(target).collect::<Vec<_>>();
    let request = SnapshotRequest::new(plan.clone(), targets.clone(), Vec::new(), Vec::new())
        .expect("bounded request");
    let absent = targets
        .iter()
        .cloned()
        .map(EntityObservation::Absent)
        .collect();
    let snapshot =
        ReadSnapshot::new(&request, None, absent, Vec::new(), Vec::new()).expect("small snapshot");
    let validation = snapshot.validation_request();

    let current = |last_payload: usize| {
        targets
            .iter()
            .enumerate()
            .map(|(index, target)| {
                let payload = if index < 18 { BULK } else { last_payload };
                StoredEntityRecordV1::new(
                    target.clone(),
                    EntityVersion::first(),
                    plan.contract_version(),
                    DurableKeySchemaBindingV1::from_plan(&plan),
                    payload_record(payload),
                )
                .map(EntityObservation::Present)
            })
            .collect::<Result<Vec<_>, _>>()
            .expect("bounded observations")
    };

    let baseline = TransactionCurrentState::new(&validation, current(0), Vec::new(), Vec::new())
        .expect("baseline fits");
    let remaining = riffdb_storage_api::MAX_READ_SNAPSHOT_BYTES - baseline.semantic_bytes();
    drop(baseline);
    assert!(remaining < BULK);

    let exact =
        TransactionCurrentState::new(&validation, current(remaining), Vec::new(), Vec::new())
            .expect("exact boundary fits");
    assert_eq!(
        exact.semantic_bytes(),
        riffdb_storage_api::MAX_READ_SNAPSHOT_BYTES
    );
    drop(exact);
    assert_eq!(
        TransactionCurrentState::new(&validation, current(remaining + 1), Vec::new(), Vec::new(),),
        Err(StorageValueError::LimitExceeded)
    );
}
