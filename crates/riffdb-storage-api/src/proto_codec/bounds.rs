//! Canonical-envelope write-set accounting.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{CanonicalRecord, encode_canonical_record};

use crate::{
    AtomicCommandRecordSet, CommandWriteClassBreakdownV1, CommitIntent, DurabilityMode,
    DurableKeySchemaBindingV1, EncodedWriteSetUpperBound, EntityMutation, ExpectedEntityState,
    IndexEntryMutationV1, IndexEpochAdvanceV1, StoredReadDependenciesV1,
};

use super::{
    COMMIT, CanonicalStoredEnvelopeV1, DurableCodecError, DurableCodecErrorKind, ENTITY, EVENT,
    INDEX_ENTRY, INDEX_EPOCH, OUTCOME, PROVENANCE, binding_to_proto, claims_to_proto,
    declared_outcome_to_proto, dependencies_to_proto, durability_to_proto,
    encode_application_sequence_allocator_v1, encode_commit_record_v1, encode_durable_event_v1,
    encode_entity_record_v1, encode_index_entry_v1, encode_index_epoch_v1, encode_outbox_intent_v1,
    encode_provenance_record_v1, encode_stored_outcome_v1, entity_target_to_proto,
    expected_to_proto, hashes_to_proto, identity_to_proto, index_entry_to_proto,
    index_epoch_to_proto, plan_to_proto, storage_result, timestamp_to_proto,
};

const OUTBOX_INTENT: &str = "riffdb.storage.v1.StoredOutboxIntentV1";
const MAXIMUM_WIDTH_U64: u64 = u64::MAX;
const SIZING_EVENT_HASH: [u8; 32] = [0xff; 32];

/// Canonical envelopes for one final command graph, ready for backend staging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedAtomicCommandRecordSetV1 {
    allocator: CanonicalStoredEnvelopeV1,
    entities: Vec<CanonicalStoredEnvelopeV1>,
    index_entries: Vec<Option<CanonicalStoredEnvelopeV1>>,
    index_epochs: Vec<CanonicalStoredEnvelopeV1>,
    outcome: CanonicalStoredEnvelopeV1,
    events: Vec<CanonicalStoredEnvelopeV1>,
    outbox_intents: Vec<CanonicalStoredEnvelopeV1>,
    provenance: CanonicalStoredEnvelopeV1,
    commit: CanonicalStoredEnvelopeV1,
    actual_charge: CommandWriteClassBreakdownV1,
}

impl EncodedAtomicCommandRecordSetV1 {
    /// Returns allocator metadata to stage with the command graph.
    #[must_use]
    pub const fn allocator(&self) -> &CanonicalStoredEnvelopeV1 {
        &self.allocator
    }

    /// Returns authoritative entity post-image envelopes in canonical order.
    #[must_use]
    pub fn entities(&self) -> &[CanonicalStoredEnvelopeV1] {
        &self.entities
    }

    /// Returns index changes aligned with the semantic mutation list.
    ///
    /// `None` is a physical deletion and therefore has zero encoded-byte charge.
    #[must_use]
    pub fn index_entries(&self) -> &[Option<CanonicalStoredEnvelopeV1>] {
        &self.index_entries
    }

    /// Returns authoritative index-epoch post-image envelopes.
    #[must_use]
    pub fn index_epochs(&self) -> &[CanonicalStoredEnvelopeV1] {
        &self.index_epochs
    }

    /// Returns the sole terminal outcome envelope.
    #[must_use]
    pub const fn outcome(&self) -> &CanonicalStoredEnvelopeV1 {
        &self.outcome
    }

    /// Returns standalone event envelopes in ordinal order.
    #[must_use]
    pub fn events(&self) -> &[CanonicalStoredEnvelopeV1] {
        &self.events
    }

    /// Returns reciprocal outbox-intent envelopes in event order.
    #[must_use]
    pub fn outbox_intents(&self) -> &[CanonicalStoredEnvelopeV1] {
        &self.outbox_intents
    }

    /// Returns the immutable provenance envelope.
    #[must_use]
    pub const fn provenance(&self) -> &CanonicalStoredEnvelopeV1 {
        &self.provenance
    }

    /// Returns the authoritative commit-log envelope.
    #[must_use]
    pub const fn commit(&self) -> &CanonicalStoredEnvelopeV1 {
        &self.commit
    }

    /// Returns exact complete-envelope bytes charged by record class.
    #[must_use]
    pub const fn actual_charge(&self) -> CommandWriteClassBreakdownV1 {
        self.actual_charge
    }
}

/// Computes a tight sequence-free canonical-envelope reservation.
///
/// This creates private Protobuf sizing projections, not semantic records. The
/// projections cannot escape this function or enter decode/persistence APIs.
/// Every variable field is copied from the retained candidate, while future
/// sequence and entity-version varints use their maximum encoded width.
pub fn command_write_set_upper_bound_v1(
    intent: &CommitIntent,
    index_entries: &[IndexEntryMutationV1],
    index_epochs: &[IndexEpochAdvanceV1],
) -> Result<EncodedWriteSetUpperBound, DurableCodecError> {
    let evaluated = intent.evaluated();
    let pending = intent.pending();
    let plan = evaluated.plan();
    let schema_binding = DurableKeySchemaBindingV1::from_plan(plan);

    let entity_messages = evaluated
        .mutations()
        .iter()
        .map(|mutation| sizing_entity(mutation, &schema_binding))
        .collect::<Result<Vec<_>, _>>()?;
    let event_messages = evaluated
        .event_intents()
        .iter()
        .enumerate()
        .map(|(ordinal, event)| {
            let event_ordinal =
                u32::try_from(ordinal).map_err(|_| DurableCodecError::invariant())?;
            Ok(wire::StoredDurableEventV1 {
                event_id: Some(wire::EventIdV1 {
                    commit_sequence: MAXIMUM_WIDTH_U64,
                    event_ordinal,
                }),
                event_type_id: event.event_type_id().get(),
                canonical_payload: canonical_record(event.payload())?,
                event_hash: SIZING_EVENT_HASH.to_vec(),
            })
        })
        .collect::<Result<Vec<_>, DurableCodecError>>()?;
    let event_ids = event_messages
        .iter()
        .map(|event| event.event_id.ok_or_else(DurableCodecError::invariant))
        .collect::<Result<Vec<_>, _>>()?;
    let read_dependencies = storage_result(StoredReadDependenciesV1::from_live(
        evaluated.read_dependencies(),
    ))?;

    let outcome = wire::StoredOutcomeV1 {
        identity: Some(identity_to_proto(pending.identity())),
        commit_sequence: MAXIMUM_WIDTH_U64,
        admission_request_id: pending.admission_request_id().as_bytes().to_vec(),
        plan: Some(plan_to_proto(plan)),
        canonical_input_hash: pending.canonical_input_hash().as_bytes().to_vec(),
        actor: Some(super::actor_to_proto(pending.actor())),
        logical_time: Some(timestamp_to_proto(pending.logical_time().timestamp())),
        partition_hash: intent.partition_hash().as_bytes().to_vec(),
        conflict_hashes: hashes_to_proto(intent.conflict_hashes()),
        declared_outcome: Some(declared_outcome_to_proto(evaluated.outcome())),
        admitted_claims: Some(claims_to_proto(pending.provenance_claims())),
        provenance_id: intent.provenance_id().as_bytes().to_vec(),
        durability_mode: durability_to_proto(DurabilityMode::Memory),
    };
    let affected_entities = entity_messages
        .iter()
        .map(|entity| wire::AffectedEntityV1 {
            target: entity.target.clone(),
            entity_version: entity.entity_version,
        })
        .collect::<Vec<_>>();
    let provenance = wire::StoredProvenanceRecordV1 {
        provenance_id: intent.provenance_id().as_bytes().to_vec(),
        commit_sequence: MAXIMUM_WIDTH_U64,
        identity: Some(identity_to_proto(pending.identity())),
        admission_request_id: pending.admission_request_id().as_bytes().to_vec(),
        plan: Some(plan_to_proto(plan)),
        canonical_input_hash: pending.canonical_input_hash().as_bytes().to_vec(),
        actor: Some(super::actor_to_proto(pending.actor())),
        logical_time: Some(timestamp_to_proto(pending.logical_time().timestamp())),
        partition_hash: intent.partition_hash().as_bytes().to_vec(),
        conflict_hashes: hashes_to_proto(intent.conflict_hashes()),
        outcome_id: evaluated.outcome().outcome_id().get(),
        affected_entities,
        event_ids: event_ids.clone(),
        admitted_claims: Some(claims_to_proto(pending.provenance_claims())),
    };
    let commit = wire::StoredCommitRecordV1 {
        commit_sequence: MAXIMUM_WIDTH_U64,
        admission_request_id: pending.admission_request_id().as_bytes().to_vec(),
        plan: Some(plan_to_proto(plan)),
        canonical_input_hash: pending.canonical_input_hash().as_bytes().to_vec(),
        actor: Some(super::actor_to_proto(pending.actor())),
        logical_time: Some(timestamp_to_proto(pending.logical_time().timestamp())),
        partition_hash: intent.partition_hash().as_bytes().to_vec(),
        conflict_hashes: hashes_to_proto(intent.conflict_hashes()),
        read_dependencies: Some(dependencies_to_proto(&read_dependencies)),
        mutations: evaluated
            .mutations()
            .iter()
            .zip(entity_messages.iter().cloned())
            .map(|(mutation, post_image)| wire::CommittedEntityMutationV1 {
                expected: Some(expected_to_proto(expected_state(mutation))),
                post_image: Some(post_image),
            })
            .collect(),
        events: event_messages.clone(),
        declared_outcome: Some(declared_outcome_to_proto(evaluated.outcome())),
        provenance_id: intent.provenance_id().as_bytes().to_vec(),
        outbox_event_ids: event_ids,
        durability_mode: durability_to_proto(DurabilityMode::Memory),
    };

    let classes =
        CommandWriteClassBreakdownV1::new(
            sizing_charge(
                super::metadata::APPLICATION,
                &wire::StoredApplicationSequenceAllocatorV1 {
                    state: Some(
                        wire::stored_application_sequence_allocator_v1::State::NextCommitSequence(
                            MAXIMUM_WIDTH_U64,
                        ),
                    ),
                },
            )?,
            0,
            sum_sizes(
                entity_messages
                    .iter()
                    .map(|value| sizing_charge(ENTITY, value)),
            )?,
            sum_sizes(index_entries.iter().filter_map(|mutation| match mutation {
                IndexEntryMutationV1::Delete(_) => None,
                IndexEntryMutationV1::Put(value) => {
                    Some(sizing_charge(INDEX_ENTRY, &index_entry_to_proto(value)))
                }
            }))?,
            sum_sizes(index_epochs.iter().map(|value| {
                sizing_charge(INDEX_EPOCH, &index_epoch_to_proto(value.post_image()))
            }))?,
            sizing_charge(OUTCOME, &outcome)?,
            sum_sizes(
                event_messages
                    .iter()
                    .map(|value| sizing_charge(EVENT, value)),
            )?,
            sum_sizes(event_messages.iter().cloned().map(|event| {
                sizing_charge(
                    OUTBOX_INTENT,
                    &wire::StoredOutboxIntentV1 { event: Some(event) },
                )
            }))?,
            sizing_charge(PROVENANCE, &provenance)?,
            sizing_charge(COMMIT, &commit)?,
        )
        .map_err(DurableCodecError::from_storage_value)?;
    EncodedWriteSetUpperBound::new(classes).map_err(DurableCodecError::from_storage_value)
}

/// Encodes every final record and checks exact class charges before staging.
pub fn encode_atomic_command_record_set_v1(
    records: &AtomicCommandRecordSet,
) -> Result<EncodedAtomicCommandRecordSetV1, DurableCodecError> {
    let allocator = encode_application_sequence_allocator_v1(records.next_application_sequence())?;
    let entities = records
        .entities()
        .iter()
        .map(|value| encode_entity_record_v1(value.post_image()))
        .collect::<Result<Vec<_>, _>>()?;
    let index_entries = records
        .index_entries()
        .iter()
        .map(|value| match value {
            IndexEntryMutationV1::Delete(_) => Ok(None),
            IndexEntryMutationV1::Put(value) => encode_index_entry_v1(value).map(Some),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let index_epochs = records
        .index_epochs()
        .iter()
        .map(|value| encode_index_epoch_v1(value.post_image()))
        .collect::<Result<Vec<_>, _>>()?;
    let outcome = encode_stored_outcome_v1(records.stored_outcome())?;
    let events = records
        .events()
        .iter()
        .map(encode_durable_event_v1)
        .collect::<Result<Vec<_>, _>>()?;
    let outbox_intents = records
        .outbox_intents()
        .iter()
        .map(encode_outbox_intent_v1)
        .collect::<Result<Vec<_>, _>>()?;
    let provenance = encode_provenance_record_v1(records.provenance())?;
    let commit = encode_commit_record_v1(records.commit())?;
    let actual_charge = CommandWriteClassBreakdownV1::new(
        allocator.encoded_content_charge().get(),
        0,
        sum_envelope_charges(&entities)?,
        sum_optional_envelope_charges(&index_entries)?,
        sum_envelope_charges(&index_epochs)?,
        outcome.encoded_content_charge().get(),
        sum_envelope_charges(&events)?,
        sum_envelope_charges(&outbox_intents)?,
        provenance.encoded_content_charge().get(),
        commit.encoded_content_charge().get(),
    )
    .map_err(DurableCodecError::from_storage_value)?;
    verify_actual_write_set_charge_v1(
        actual_charge,
        records.presequence_charge().encoded_upper_bound(),
    )?;
    Ok(EncodedAtomicCommandRecordSetV1 {
        allocator,
        entities,
        index_entries,
        index_epochs,
        outcome,
        events,
        outbox_intents,
        provenance,
        commit,
        actual_charge,
    })
}

/// Proves every actual record class and the aggregate fit the reservation.
pub fn verify_actual_write_set_charge_v1(
    actual: CommandWriteClassBreakdownV1,
    reserved: EncodedWriteSetUpperBound,
) -> Result<(), DurableCodecError> {
    let maximum = reserved.classes();
    let fits = actual.allocator() <= maximum.allocator()
        && actual.pending_resolution() <= maximum.pending_resolution()
        && actual.entities() <= maximum.entities()
        && actual.index_entries() <= maximum.index_entries()
        && actual.index_epochs() <= maximum.index_epochs()
        && actual.outcome() <= maximum.outcome()
        && actual.events() <= maximum.events()
        && actual.outbox_intents() <= maximum.outbox_intents()
        && actual.provenance() <= maximum.provenance()
        && actual.commit() <= maximum.commit()
        && actual
            .total()
            .map_err(DurableCodecError::from_storage_value)?
            <= reserved.total();
    if fits {
        Ok(())
    } else {
        Err(DurableCodecError::new(
            DurableCodecErrorKind::ReservationExceeded,
        ))
    }
}

fn sizing_entity(
    mutation: &EntityMutation,
    schema_binding: &DurableKeySchemaBindingV1,
) -> Result<wire::StoredEntityRecordV1, DurableCodecError> {
    let post_image = mutation.post_image();
    Ok(wire::StoredEntityRecordV1 {
        target: Some(entity_target_to_proto(post_image.target())),
        entity_version: MAXIMUM_WIDTH_U64,
        written_by_contract: post_image.written_by_contract().get(),
        schema_binding: Some(binding_to_proto(schema_binding)),
        canonical_fields: canonical_record(post_image.fields())?,
    })
}

const fn expected_state(mutation: &EntityMutation) -> ExpectedEntityState {
    match mutation.expected_version() {
        Some(version) => ExpectedEntityState::Present(version),
        None => ExpectedEntityState::Absent,
    }
}

fn canonical_record(value: &CanonicalRecord) -> Result<Vec<u8>, DurableCodecError> {
    encode_canonical_record(value).map_err(|_| DurableCodecError::invariant())
}

fn sizing_charge<M: prost::Message>(
    record_type: &'static str,
    value: &M,
) -> Result<usize, DurableCodecError> {
    let schema = riffdb_proto::durable::current_record_schema(record_type)
        .ok_or_else(DurableCodecError::invariant)?;
    riffdb_proto::envelope::maximum_encoded_envelope_bytes(schema, value.encoded_len())
        .map_err(DurableCodecError::from_encode_envelope)
}

fn sum_sizes(
    values: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
) -> Result<usize, DurableCodecError> {
    values.into_iter().try_fold(0usize, |total, value| {
        total
            .checked_add(value?)
            .ok_or_else(|| DurableCodecError::new(DurableCodecErrorKind::LimitExceeded))
    })
}

fn sum_envelope_charges(values: &[CanonicalStoredEnvelopeV1]) -> Result<usize, DurableCodecError> {
    sum_sizes(
        values
            .iter()
            .map(|value| Ok(value.encoded_content_charge().get())),
    )
}

fn sum_optional_envelope_charges(
    values: &[Option<CanonicalStoredEnvelopeV1>],
) -> Result<usize, DurableCodecError> {
    sum_sizes(values.iter().filter_map(|value| {
        value
            .as_ref()
            .map(|value| Ok(value.encoded_content_charge().get()))
    }))
}
