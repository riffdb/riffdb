//! The existing fixture record/intent builder; only the vacant terminal path is used.
use super::*;

pub(super) fn build_command_fixture(
    ordinal: u64,
    target_ordinal: u64,
    prior: Option<&CommandFixture>,
    delete: bool,
) -> CommandFixture {
    build_for_bundle(ordinal, target_ordinal, prior, delete, None)
}

pub(super) fn build_runtime_command_fixture(
    bundle: &ValidatedContractBundle,
    ordinal: u64,
) -> CommandFixture {
    build_for_bundle(ordinal, ordinal, None, false, Some(bundle))
}

fn build_for_bundle(
    ordinal: u64,
    target_ordinal: u64,
    prior: Option<&CommandFixture>,
    delete: bool,
    bundle: Option<&ValidatedContractBundle>,
) -> CommandFixture {
    let plan = bundle.map_or_else(plan, projection_fixture::plan_for_bundle);
    let sequence = CommitSequence::new(ordinal).expect("fixture ordinal");
    let ordinal_u8 = u8::try_from(ordinal % 256).expect("bounded fixture ordinal");
    // A superseding write must carry different canonical bytes, or the frame
    // under test could not distinguish the two post-images.
    let payload = if prior.is_some() { 2 } else { 1 };
    let indexed_value = if delete {
        match prior.unwrap().records.entities()[0]
            .live_post_image()
            .unwrap()
            .fields()
            .fields()[1]
            .1
        {
            CanonicalValue::U64(value) => value,
            _ => panic!("fixture value"),
        }
    } else {
        payload
    };
    let (target, index_key, _range) = target_and_index_at(target_ordinal, indexed_value);
    let entity_fields = CanonicalRecord::new(vec![
        (
            FieldId::new(1).unwrap(),
            CanonicalValue::U64(6 + target_ordinal),
        ),
        (FieldId::new(2).unwrap(), CanonicalValue::U64(payload)),
    ])
    .unwrap();
    let prior_entity =
        prior.and_then(|fixture| fixture.records.entities()[0].live_post_image().cloned());
    let prior_epoch = prior.map_or(IndexEpochPosition::BeforeFirst, |fixture| {
        IndexEpochPosition::Value(fixture.write_plan.index_epochs()[0].next())
    });
    let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
    let principal = ActorId::new("principal-a").expect("principal");
    let actor = riffdb_types::AdmittedActorContext::new(
        principal.clone(),
        ActorKind::Human,
        tenant_scope.clone(),
        None,
    );
    let mut idempotency_digest = [0x40_u8.wrapping_add(ordinal_u8); 32];
    if ordinal > u64::from(u8::MAX) {
        idempotency_digest[..8].copy_from_slice(&ordinal.to_be_bytes());
    }
    let identity = IdempotencyIdentity::new(
        database_id(),
        Environment::new("test").expect("environment"),
        tenant_scope,
        principal,
        plan.contract_lineage().clone(),
        plan.command_id(),
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key"),
            idempotency_digest,
        ),
    );
    let request_id = RequestId::from_bytes(ordinal_uuid_bytes(0x30, ordinal)).expect("request ID");
    let provenance_id =
        ProvenanceId::from_bytes(ordinal_uuid_bytes(0x50, ordinal)).expect("provenance ID");
    let logical_time =
        LogicalTime::new(Timestamp::new(1_700_000_001, 0).expect("logical timestamp"));
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(6 + target_ordinal)
        .expect("partition component");
    let partition = partition.finish().expect("partition key");
    let pending = StoredPendingAdmissionV1::new(
        identity.clone(),
        CanonicalInputHash::from_bytes([0x42; 32]),
        request_id,
        plan.clone(),
        logical_time,
        actor.clone(),
        partition.clone(),
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("pending admission");

    let snapshot_request =
        SnapshotRequest::new(plan.clone(), vec![target.clone()], Vec::new(), Vec::new())
            .expect("snapshot request");
    let snapshot = ReadSnapshot::new(
        &snapshot_request,
        None,
        vec![prior_entity.clone().map_or_else(
            || EntityObservation::Absent(target.clone()),
            EntityObservation::Present,
        )],
        Vec::new(),
        Vec::new(),
    )
    .expect("read snapshot");
    let post_image = EntityPostImage::new(
        target.clone(),
        plan.contract_version(),
        entity_fields.clone(),
    )
    .expect("entity post-image");
    let event_type_id = EventTypeId::new(1).expect("event type");
    let event_policy_anchor = riffdb_storage_api::StoredEventPolicyAnchorV1::new(
        DurableKeySchemaBindingV1::from_plan(&plan),
        event_type_id,
        target.clone(),
        riffdb_types::RowPolicyName::new("RowAccess").expect("row policy name"),
    );
    let event_intent =
        EventIntent::new_anchored(event_type_id, record(payload), event_policy_anchor.clone())
            .expect("anchored event intent");
    let declared_outcome =
        DeclaredOutcome::new(OutcomeId::new(1).expect("outcome ID"), record(payload))
            .expect("declared outcome");
    let entity_mutation = if delete {
        let entity = prior_entity
            .as_ref()
            .expect("delete requires a live predecessor");
        EntityMutation::Delete {
            expected_version: entity.entity_version(),
            prior_image: EntityPostImage::new(
                entity.target().clone(),
                plan.contract_version(),
                entity.fields().clone(),
            )
            .expect("delete predecessor image"),
        }
    } else {
        prior_entity.as_ref().map_or_else(
            || EntityMutation::Create(post_image.clone()),
            |entity| EntityMutation::Replace {
                expected_version: entity.entity_version(),
                post_image: post_image.clone(),
            },
        )
    };
    let evaluated = if let Some(bundle) = bundle {
        let input = projection_fixture::input(bundle, target_ordinal, payload);
        let context = riffdb_runtime::TransactionContext::new(
            request_id,
            actor.clone(),
            plan.clone(),
            logical_time,
            partition.clone(),
        );
        let riffdb_runtime::ExecutionResult::CommitRequired(evaluated) =
            riffdb_runtime::execute_command(
                bundle.bundle(),
                &input,
                &snapshot,
                &context,
                EvaluationBudget::v1(),
            )
            .expect("execute the actual compiled projection command")
        else {
            panic!("fixture command requires a durable commit");
        };
        assert!(
            evaluated.mutations() == std::slice::from_ref(&entity_mutation),
            "runtime post-image equals the admitted storage fixture"
        );
        assert_eq!(evaluated.event_intents().len(), 1);
        evaluated
    } else {
        riffdb_storage_api::EvaluatedCommand::new(
            &snapshot,
            vec![entity_mutation],
            vec![event_intent],
            declared_outcome.clone(),
            EvaluationBudget::v1(),
        )
        .expect("evaluated command")
    };
    let event_intent = &evaluated.event_intents()[0];
    let event_type_id = event_intent.event_type_id();
    let event_payload = event_intent.payload().clone();
    let event_policy_anchor = event_intent
        .policy_anchor()
        .expect("actual protected event anchor")
        .clone();
    let declared_outcome = evaluated.outcome().clone();
    let partition_hash = hash_partition_key(partition.as_bytes());
    let context = PreEvaluationCommitContext::new(pending.clone(), partition_hash, Vec::new())
        .expect("commit context");
    let candidates =
        IdempotencyLookupCandidatesV1::new(vec![identity.clone()]).expect("lookup candidates");
    let intent = riffdb_storage_api::CommitIntent::new_for_vacant_terminal_admission(
        context.clone(),
        candidates.clone(),
        evaluated,
        provenance_id,
    )
    .expect("fused terminal commit intent");

    let stored_entity = StoredEntityRecordV1::new(
        target.clone(),
        prior_entity
            .as_ref()
            .map_or_else(EntityVersion::first, |entity| {
                entity
                    .entity_version()
                    .checked_next()
                    .expect("superseding entity version")
            }),
        plan.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        entity_fields,
    )
    .expect("stored entity");
    let mutation = if delete {
        let entity = prior_entity.as_ref().expect("delete predecessor");
        riffdb_storage_api::CommittedEntityMutationV1::delete(
            entity.entity_version(),
            entity.clone(),
        )
        .expect("committed entity delete")
    } else {
        riffdb_storage_api::CommittedEntityMutationV1::new(
            prior_entity
                .as_ref()
                .map_or(ExpectedEntityState::Absent, |entity| {
                    ExpectedEntityState::Present(entity.entity_version())
                }),
            stored_entity,
        )
        .expect("committed entity mutation")
    };
    let index_record = StoredIndexEntryV2::new(
        index_key.clone(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        CanonicalRecord::new(Vec::new()).unwrap(),
        pending.partition_key().clone(),
    )
    .expect("stored index entry");
    let index_mutation = if delete {
        IndexEntryMutationV1::Delete(index_key.clone())
    } else {
        IndexEntryMutationV1::Put(index_record)
    };
    let mut index_mutations = vec![index_mutation];
    if !delete && prior_entity.is_some() {
        let previous = prior.unwrap();
        if previous.index_key != index_key {
            index_mutations.push(IndexEntryMutationV1::Delete(previous.index_key.clone()));
        }
    }
    index_mutations.sort_by(|left, right| left.key().cmp(right.key()));
    let generation = PartitionIndexTarget::new(partition.clone(), index_key.index_id());
    let affected_targets =
        AffectedIndexEpochTargets::new(vec![generation.clone()]).expect("affected targets");
    let affected_current = AffectedEpochCurrentState::new(
        &affected_targets,
        vec![CurrentIndexGenerationObservation::new(
            generation.clone(),
            prior_epoch,
        )],
    )
    .expect("affected current state");
    let epoch_advance = IndexEpochAdvanceV1::new(
        generation,
        DurableKeySchemaBindingV1::from_plan(&plan),
        prior_epoch,
    )
    .expect("epoch advance");
    let upper_bound = match command_write_set_upper_bound_v1(
        &intent,
        &index_mutations,
        std::slice::from_ref(&epoch_advance),
    )
    .expect("canonical encoded upper bound")
    {
        EncodedWriteSetUpperBoundResultV1::Fits(bound) => bound,
        EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_) => {
            panic!("recovery fixture write set must fit the accepted aggregate cap")
        }
    };
    let write_plan = CommandWriteSetPlanV1::new(
        &intent,
        affected_targets.clone(),
        affected_current,
        index_mutations,
        vec![epoch_advance],
        upper_bound,
    )
    .expect("write plan");

    let assignment = AssignedCommandSequence::from_assigned(sequence);
    let event_id = EventId::new(sequence, 0);

    let event = StoredDurableEventV1::new_anchored(
        event_id,
        event_type_id,
        event_payload.clone(),
        riffdb_storage_api::derive_event_hash_v2(
            event_id,
            event_type_id,
            &event_payload,
            &event_policy_anchor,
        )
        .expect("anchored event hash"),
        event_policy_anchor,
    )
    .expect("durable event");
    let stored_outcome = StoredOutcomeV1::new(
        identity.clone(),
        sequence,
        request_id,
        plan.clone(),
        pending.canonical_input_hash(),
        actor.clone(),
        logical_time,
        partition.clone(),
        partition_hash,
        Vec::new(),
        declared_outcome.clone(),
        StoredAdmittedProvenanceClaimsV1::default(),
        provenance_id,
        DurabilityMode::Sync,
    )
    .expect("stored outcome");
    let mutations = vec![mutation];
    let provenance = StoredProvenanceRecordV1::new(
        provenance_id,
        sequence,
        identity,
        request_id,
        plan.clone(),
        pending.canonical_input_hash(),
        actor.clone(),
        logical_time,
        partition_hash,
        Vec::new(),
        declared_outcome.outcome_id(),
        vec![AffectedEntityV1::from_record(mutations[0].post_image())],
        vec![event_id],
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("provenance");
    let commit = riffdb_storage_api::StoredCommitRecordV1::new(
        sequence,
        request_id,
        plan,
        pending.canonical_input_hash(),
        actor,
        logical_time,
        partition_hash,
        Vec::new(),
        StoredReadDependenciesV1::from_live(snapshot.read_dependencies())
            .expect("stored dependencies"),
        mutations
            .iter()
            .map(riffdb_storage_api::CommittedEntityReferenceV2::from_live_mutation)
            .collect::<Result<Vec<_>, _>>()
            .expect("live entity references")
            .into_iter()
            .flatten()
            .collect(),
        vec![event.clone()],
        declared_outcome,
        provenance_id,
        vec![event_id],
        DurabilityMode::Sync,
    )
    .expect("commit record");
    let records = AtomicCommandRecordSet::new(
        assignment,
        mutations,
        write_plan.clone(),
        stored_outcome,
        provenance,
        commit,
    )
    .expect("atomic command record set");

    CommandFixture {
        pending,
        intent,
        affected_targets,
        write_plan,
        records,
        index_key,
    }
}
