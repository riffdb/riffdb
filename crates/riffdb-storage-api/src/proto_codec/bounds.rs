//! Canonical-envelope write-set accounting.

use prost::Message as _;
#[cfg(test)]
use riffdb_types::{CanonicalRecord, CanonicalValue};

use crate::{
    AtomicCommandRecordSet, CommandWriteClassBreakdownV1, CommitIntent, DurabilityMode,
    DurableKeySchemaBindingV1, EncodedWriteSetUpperBound, EntityMutation, IndexEntryMutationV1,
    IndexEpochAdvanceV1, MAX_STAGED_WRITE_BYTES, StoredReadDependenciesV1,
    VectorEvidenceMutationV1, VectorEvidenceTransitionPlanV1,
};

use super::{
    COMMIT, CanonicalStoredEnvelopeV1, DurableCodecError, DurableCodecErrorKind, ENTITY, EVENT,
    EVENT_ROUTE, INDEX_ENTRY, INDEX_EPOCH, OUTCOME_V3, PROVENANCE_V2, binding_to_proto,
    causation_to_proto, claims_to_proto, dependencies_to_proto, durability_to_proto,
    encode_application_sequence_allocator_v1, encode_commit_record_v1, encode_durable_event_v1,
    encode_entity_record_v1, encode_event_route_v1, encode_index_entry_v2, encode_index_epoch_v1,
    encode_outbox_intent_v1, encode_provenance_record_v1, encode_stored_outcome_v1,
    encode_vector_evidence_v1, entity_target_to_proto, identity_to_proto, plan_to_proto,
    storage_result, timestamp_to_proto,
};

const OUTBOX_INTENT: &str = "riffdb.storage.v1.StoredOutboxIntentV2";
const MAXIMUM_WIDTH_U64: u64 = u64::MAX;
const SIZING_EVENT_HASH: [u8; 32] = [0xff; 32];
/// Domain-separated entity-record hash charge (same width as event hashes).
const SIZING_ENTITY_RECORD_HASH: [u8; 32] = [0xff; 32];

const _: () = assert!(SIZING_EVENT_HASH.len() == 32);
// Couple to the real EntityRecordHash width (not a free-floating literal).
const _: () = assert!(
    SIZING_ENTITY_RECORD_HASH.len() == core::mem::size_of::<riffdb_types::EntityRecordHash>()
);
const _: () = assert!(SIZING_EVENT_HASH.len() == SIZING_ENTITY_RECORD_HASH.len());

/// Codec-minted proof that complete sequence-free sizing exceeded only the
/// accepted aggregate cap.
///
/// This witness has no public constructor or fields. Callers can receive and
/// consume it only through [`command_write_set_upper_bound_v1`].
///
/// ```compile_fail
/// use riffdb_storage_api::{
///     AggregateCapExceededOriginV1, EncodedWriteSetUpperBoundResultV1,
/// };
///
/// let _forged = EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(
///     AggregateCapExceededOriginV1 { _codec_origin: () },
/// );
/// ```
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct AggregateCapExceededOriginV1 {
    _codec_origin: (),
}

impl AggregateCapExceededOriginV1 {
    const fn from_final_comparison() -> Self {
        Self { _codec_origin: () }
    }
}

/// Closed result of complete sequence-free durable write-set sizing.
#[derive(Debug, Eq, PartialEq)]
pub enum EncodedWriteSetUpperBoundResultV1 {
    /// Every complete record and the checked aggregate fit the accepted cap.
    Fits(EncodedWriteSetUpperBound),
    /// Every record and checked addition succeeded, but the final aggregate exceeds the cap.
    ExceedsAcceptedAggregateCap(AggregateCapExceededOriginV1),
}

#[derive(Clone, Copy)]
struct RawWriteClassBreakdownV1 {
    allocator: usize,
    pending_resolution: usize,
    entities: usize,
    index_entries: usize,
    index_epochs: usize,
    outcome: usize,
    events: usize,
    outbox_intents: usize,
    provenance: usize,
    commit: usize,
}

impl RawWriteClassBreakdownV1 {
    fn total(self) -> Result<usize, DurableCodecError> {
        sum_sizes([
            Ok(self.allocator),
            Ok(self.pending_resolution),
            Ok(self.entities),
            Ok(self.index_entries),
            Ok(self.index_epochs),
            Ok(self.outcome),
            Ok(self.events),
            Ok(self.outbox_intents),
            Ok(self.provenance),
            Ok(self.commit),
        ])
    }

    fn checked(self) -> Result<CommandWriteClassBreakdownV1, DurableCodecError> {
        CommandWriteClassBreakdownV1::new(
            self.allocator,
            self.pending_resolution,
            self.entities,
            self.index_entries,
            self.index_epochs,
            self.outcome,
            self.events,
            self.outbox_intents,
            self.provenance,
            self.commit,
        )
        .map_err(|_| DurableCodecError::invariant())
    }
}

/// Canonical envelopes for one final command graph, ready for backend staging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedAtomicCommandRecordSetV1 {
    allocator: CanonicalStoredEnvelopeV1,
    entities: Vec<Option<CanonicalStoredEnvelopeV1>>,
    vector_evidence: Vec<Option<CanonicalStoredEnvelopeV1>>,
    index_entries: Vec<Option<CanonicalStoredEnvelopeV1>>,
    index_epochs: Vec<CanonicalStoredEnvelopeV1>,
    outcome: CanonicalStoredEnvelopeV1,
    events: Vec<CanonicalStoredEnvelopeV1>,
    event_routes: Vec<CanonicalStoredEnvelopeV1>,
    outbox_intents: Vec<CanonicalStoredEnvelopeV1>,
    provenance: CanonicalStoredEnvelopeV1,
    commit: CanonicalStoredEnvelopeV1,
    actual_charge: CommandWriteClassBreakdownV1,
}

/// Canonical envelopes shared by the successful-command capsule layout.
///
/// The segment path constructs envelopes only for entity/index state that is
/// stored independently. Outcome, provenance, commit, event, route, and outbox
/// facts are encoded once in the canonical command segment after audit members
/// have been allocated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedCapsuleCommandRecordSetV1 {
    entities: Vec<Option<CanonicalStoredEnvelopeV1>>,
    vector_evidence: Vec<Option<CanonicalStoredEnvelopeV1>>,
    index_entries: Vec<Option<CanonicalStoredEnvelopeV1>>,
    index_epochs: Vec<CanonicalStoredEnvelopeV1>,
}

/// Move-only canonical entity/index preparation produced before sequence assignment.
///
/// Semantic members travel with their encoded envelopes until the complete
/// record graph constructor consumes them.  That constructor joins the index
/// members to the exact reserved write plan before retaining only the canonical
/// envelopes, preventing a caller from pairing equal-size bytes with another
/// command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedCapsuleCommandFragmentsV1 {
    entities: Vec<crate::CommittedEntityMutationV1>,
    index_entries: Vec<crate::IndexEntryMutationV1>,
    entity_references: Vec<crate::CommittedEntityReferenceV2>,
    encoded: PreparedCapsuleEnvelopesV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedCapsuleEnvelopesV1 {
    entities: Vec<Option<CanonicalStoredEnvelopeV1>>,
    index_entries: Vec<Option<CanonicalStoredEnvelopeV1>>,
}

impl PreparedCapsuleCommandFragmentsV1 {
    /// Borrows canonical entity mutations retained with their envelopes.
    #[must_use]
    pub fn entities(&self) -> &[crate::CommittedEntityMutationV1] {
        &self.entities
    }

    /// Borrows exact post-image references proven by this preparation.
    #[must_use]
    pub fn entity_references(&self) -> &[crate::CommittedEntityReferenceV2] {
        &self.entity_references
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<crate::CommittedEntityMutationV1>,
        Vec<crate::IndexEntryMutationV1>,
        Vec<crate::CommittedEntityReferenceV2>,
        PreparedCapsuleEnvelopesV1,
    ) {
        (
            self.entities,
            self.index_entries,
            self.entity_references,
            self.encoded,
        )
    }
}

/// Canonically encodes sequence-free entity and secondary-index postimages once.
pub fn prepare_capsule_command_fragments_v1(
    entities: Vec<crate::CommittedEntityMutationV1>,
    index_entries: Vec<crate::IndexEntryMutationV1>,
) -> Result<PreparedCapsuleCommandFragmentsV1, DurableCodecError> {
    let encoded_entities = entities
        .iter()
        .map(|value| {
            value
                .live_post_image()
                .map(encode_entity_record_v1)
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let encoded_index_entries = index_entries
        .iter()
        .map(|value| match value {
            crate::IndexEntryMutationV1::Delete(_) => Ok(None),
            crate::IndexEntryMutationV1::Put(value) => encode_index_entry_v2(value).map(Some),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let entity_references = entities
        .iter()
        .map(crate::CommittedEntityReferenceV2::from_live_mutation)
        .collect::<Result<Vec<_>, _>>()
        .map_err(DurableCodecError::from_storage_value)?
        .into_iter()
        .flatten()
        .collect();
    Ok(PreparedCapsuleCommandFragmentsV1 {
        entities,
        index_entries,
        entity_references,
        encoded: PreparedCapsuleEnvelopesV1 {
            entities: encoded_entities,
            index_entries: encoded_index_entries,
        },
    })
}

impl EncodedCapsuleCommandRecordSetV1 {
    /// Consumes the checked independently stored envelopes.
    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Vec<Option<CanonicalStoredEnvelopeV1>>,
        Vec<Option<CanonicalStoredEnvelopeV1>>,
        Vec<Option<CanonicalStoredEnvelopeV1>>,
        Vec<CanonicalStoredEnvelopeV1>,
    ) {
        (
            self.entities,
            self.vector_evidence,
            self.index_entries,
            self.index_epochs,
        )
    }
}

impl EncodedAtomicCommandRecordSetV1 {
    /// Returns allocator metadata to stage with the command graph.
    #[must_use]
    pub const fn allocator(&self) -> &CanonicalStoredEnvelopeV1 {
        &self.allocator
    }

    /// Returns entity envelopes aligned with the semantic mutation list.
    ///
    /// `None` is a checked deletion and therefore stores no current row.
    #[must_use]
    pub fn entities(&self) -> &[Option<CanonicalStoredEnvelopeV1>] {
        &self.entities
    }

    /// Returns vector-evidence changes aligned with the semantic mutation list.
    ///
    /// `None` is a checked deletion and therefore stores no current row.
    #[must_use]
    pub fn vector_evidence(&self) -> &[Option<CanonicalStoredEnvelopeV1>] {
        &self.vector_evidence
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

    /// Returns payload-free partition event-route envelopes in ordinal order.
    #[must_use]
    pub fn event_routes(&self) -> &[CanonicalStoredEnvelopeV1] {
        &self.event_routes
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

    /// Consumes the checked record set into its canonical envelopes.
    ///
    /// Storage backends use this after the complete write-set charge has been
    /// proven so the same owned byte buffers can be inserted into storage and
    /// retained by a durability frame without cloning every record payload.
    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        CanonicalStoredEnvelopeV1,
        Vec<Option<CanonicalStoredEnvelopeV1>>,
        Vec<Option<CanonicalStoredEnvelopeV1>>,
        Vec<Option<CanonicalStoredEnvelopeV1>>,
        Vec<CanonicalStoredEnvelopeV1>,
        CanonicalStoredEnvelopeV1,
        Vec<CanonicalStoredEnvelopeV1>,
        Vec<CanonicalStoredEnvelopeV1>,
        Vec<CanonicalStoredEnvelopeV1>,
        CanonicalStoredEnvelopeV1,
        CanonicalStoredEnvelopeV1,
        CommandWriteClassBreakdownV1,
    ) {
        (
            self.allocator,
            self.entities,
            self.vector_evidence,
            self.index_entries,
            self.index_epochs,
            self.outcome,
            self.events,
            self.event_routes,
            self.outbox_intents,
            self.provenance,
            self.commit,
            self.actual_charge,
        )
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
) -> Result<EncodedWriteSetUpperBoundResultV1, DurableCodecError> {
    command_write_set_upper_bound_with_vector_evidence_v1(intent, index_entries, index_epochs, &[])
}

/// Computes a tight sequence-free reservation including vector evidence.
pub fn command_write_set_upper_bound_with_vector_evidence_v1(
    intent: &CommitIntent,
    index_entries: &[IndexEntryMutationV1],
    index_epochs: &[IndexEpochAdvanceV1],
    vector_evidence: &[VectorEvidenceTransitionPlanV1],
) -> Result<EncodedWriteSetUpperBoundResultV1, DurableCodecError> {
    let evaluated = intent.evaluated();
    let pending = intent.pending();
    let plan = evaluated.plan();
    let schema_binding = binding_to_proto(&DurableKeySchemaBindingV1::from_plan(plan));
    let schema_binding_len = schema_binding.encoded_len();
    let read_dependencies = storage_result(StoredReadDependenciesV1::from_live(
        evaluated.read_dependencies(),
    ))?;
    let identity_len = identity_to_proto(pending.identity()).encoded_len();
    let plan_len = plan_to_proto(plan).encoded_len();
    let actor_len = super::actor_to_proto(pending.actor()).encoded_len();
    let logical_time_len = timestamp_to_proto(pending.logical_time().timestamp()).encoded_len();
    let claims_len = claims_to_proto(pending.provenance_claims()).encoded_len();
    let declared_outcome_len = sizing_declared_outcome_len(evaluated.outcome())?;
    let read_dependencies_len = dependencies_to_proto(&read_dependencies).encoded_len();
    let admission_request_id_len = pending.admission_request_id().as_bytes().len();
    let canonical_input_hash_len = pending.canonical_input_hash().as_bytes().len();
    let partition_hash_len = intent.partition_hash().as_bytes().len();
    let provenance_id_len = intent.provenance_id().as_bytes().len();
    let outcome_len = sizing_outcome_len(
        identity_len,
        plan_len,
        actor_len,
        logical_time_len,
        claims_len,
        declared_outcome_len,
        admission_request_id_len,
        canonical_input_hash_len,
        partition_hash_len,
        provenance_id_len,
        pending.partition_key().as_bytes().len(),
        intent
            .conflict_hashes()
            .iter()
            .map(|hash| Ok(hash.as_bytes().len())),
    )?;
    let causation_len = pending
        .causation()
        .map(|value| causation_to_proto(value).encoded_len());
    let outcome_len = sizing_successor_len(outcome_len, causation_len)?;
    let service_values_len = riffdb_types::encode_canonical_record(pending.service_values())
        .map_err(|_| DurableCodecError::invariant())?
        .len();
    let outcome_len = sum_proto_fields([
        message_field_len(1, outcome_len),
        bytes_field_len(2, service_values_len),
    ])?;
    let provenance_len = sizing_provenance_len(
        identity_len,
        plan_len,
        actor_len,
        logical_time_len,
        claims_len,
        admission_request_id_len,
        canonical_input_hash_len,
        partition_hash_len,
        provenance_id_len,
        intent
            .conflict_hashes()
            .iter()
            .map(|hash| Ok(hash.as_bytes().len())),
        evaluated
            .mutations()
            .iter()
            .map(|mutation| sizing_affected_entity_len(mutation, MAXIMUM_WIDTH_U64)),
        (0..evaluated.event_intents().len()).map(|ordinal| {
            let ordinal = u32::try_from(ordinal).map_err(|_| DurableCodecError::invariant())?;
            Ok(sizing_event_id_len(ordinal))
        }),
    )?;
    let provenance_len = sizing_successor_len(provenance_len, causation_len)?;
    let event_reference_lens = || {
        (0..evaluated.event_intents().len()).map(|ordinal| {
            let ordinal = u32::try_from(ordinal).map_err(|_| DurableCodecError::invariant())?;
            let event_id_len = sizing_event_id_len(ordinal);
            sum_proto_fields([
                message_field_len(1, event_id_len),
                bytes_field_len(2, SIZING_EVENT_HASH.len()),
            ])
        })
    };
    let commit_len = sizing_commit_len(
        plan_len,
        actor_len,
        logical_time_len,
        declared_outcome_len,
        read_dependencies_len,
        admission_request_id_len,
        canonical_input_hash_len,
        partition_hash_len,
        provenance_id_len,
        intent
            .conflict_hashes()
            .iter()
            .map(|hash| Ok(hash.as_bytes().len())),
        evaluated.mutations().iter().map(|mutation| {
            conservative_entity_reference_payload_len(mutation.post_image().target())
        }),
        event_reference_lens(),
        (0..evaluated.event_intents().len()).map(|ordinal| {
            let ordinal = u32::try_from(ordinal).map_err(|_| DurableCodecError::invariant())?;
            Ok(sizing_event_id_len(ordinal))
        }),
    )?;

    let event_charge = sum_sizes(evaluated.event_intents().iter().enumerate().map(
        |(ordinal, event)| {
            let event_ordinal =
                u32::try_from(ordinal).map_err(|_| DurableCodecError::invariant())?;
            let payload_len = sizing_event_len(
                event.event_type_id().get(),
                event.payload_encoded_len(),
                event_ordinal,
            )?;
            sizing_charge_len(EVENT, payload_len)
        },
    ))?;
    let event_route_charge = sum_sizes(evaluated.event_intents().iter().enumerate().map(
        |(ordinal, event)| {
            let ordinal = u32::try_from(ordinal).map_err(|_| DurableCodecError::invariant())?;
            let route_len = sum_proto_fields([
                message_field_len(1, sizing_event_id_len(ordinal)),
                varint_field_len(2, event.event_type_id().get()),
                bytes_field_len(3, SIZING_EVENT_HASH.len()),
            ])?;
            sizing_charge_len(EVENT_ROUTE, route_len)
        },
    ))?;

    let raw = RawWriteClassBreakdownV1 {
        allocator: sizing_charge_len(
            super::metadata::APPLICATION,
            key_len(1, prost::encoding::WireType::Varint)
                .checked_add(prost::encoding::encoded_len_varint(MAXIMUM_WIDTH_U64))
                .ok_or_else(DurableCodecError::invariant)?,
        )?,
        pending_resolution: 0,
        entities: sum_sizes(evaluated.mutations().iter().map(|mutation| {
            sizing_entity_len(mutation, schema_binding_len)
                .and_then(|len| sizing_charge_len(ENTITY, len))
        }))?
        .checked_add(sum_sizes(vector_evidence.iter().map(|transition| {
            match transition.materialize(
                riffdb_types::CommitSequence::new(MAXIMUM_WIDTH_U64)
                    .expect("maximum nonzero commit sequence is valid"),
            ) {
                Ok(VectorEvidenceMutationV1::Put(value)) => encode_vector_evidence_v1(&value)
                    .map(|envelope| envelope.encoded_content_charge().get()),
                Ok(VectorEvidenceMutationV1::Delete { .. }) => Ok(0),
                Err(error) => Err(DurableCodecError::from_storage_value(error)),
            }
        }))?)
        .ok_or_else(DurableCodecError::invariant)?,
        index_entries: sum_sizes(index_entries.iter().filter_map(|mutation| match mutation {
            IndexEntryMutationV1::Delete(_) => None,
            IndexEntryMutationV1::Put(value) => Some(
                sizing_index_entry_len(value).and_then(|len| sizing_charge_len(INDEX_ENTRY, len)),
            ),
        }))?,
        index_epochs: sum_sizes(index_epochs.iter().map(|value| {
            sizing_index_epoch_len(value.post_image())
                .and_then(|len| sizing_charge_len(INDEX_EPOCH, len))
        }))?,
        outcome: sizing_charge_len(OUTCOME_V3, outcome_len)?,
        events: event_charge
            .checked_add(event_route_charge)
            .ok_or_else(DurableCodecError::invariant)?,
        outbox_intents: sum_sizes(event_reference_lens().map(|event_reference_len| {
            message_field_len(1, event_reference_len?)
                .and_then(|len| sizing_charge_len(OUTBOX_INTENT, len))
        }))?,
        provenance: sizing_charge_len(PROVENANCE_V2, provenance_len)?,
        commit: sizing_charge_len(COMMIT, commit_len)?,
    };
    finish_upper_bound(raw)
}

fn finish_upper_bound(
    raw: RawWriteClassBreakdownV1,
) -> Result<EncodedWriteSetUpperBoundResultV1, DurableCodecError> {
    if raw.total()? > MAX_STAGED_WRITE_BYTES {
        return Ok(
            EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(
                AggregateCapExceededOriginV1::from_final_comparison(),
            ),
        );
    }
    let classes = raw.checked()?;
    let bound =
        EncodedWriteSetUpperBound::new(classes).map_err(|_| DurableCodecError::invariant())?;
    Ok(EncodedWriteSetUpperBoundResultV1::Fits(bound))
}

/// Encodes every final record and checks exact class charges before staging.
pub fn encode_atomic_command_record_set_v1(
    records: &AtomicCommandRecordSet,
) -> Result<EncodedAtomicCommandRecordSetV1, DurableCodecError> {
    let allocator = encode_application_sequence_allocator_v1(records.next_application_sequence())?;
    let entities = records
        .entities()
        .iter()
        .map(|value| {
            value
                .live_post_image()
                .map(encode_entity_record_v1)
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let vector_evidence = records
        .vector_evidence()
        .iter()
        .map(|value| match value {
            VectorEvidenceMutationV1::Delete { .. } => Ok(None),
            VectorEvidenceMutationV1::Put(value) => encode_vector_evidence_v1(value).map(Some),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let index_entries = records
        .index_entries()
        .iter()
        .map(|value| match value {
            IndexEntryMutationV1::Delete(_) => Ok(None),
            IndexEntryMutationV1::Put(value) => encode_index_entry_v2(value).map(Some),
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
    let event_routes = records
        .events()
        .iter()
        .map(|event| {
            encode_event_route_v1(crate::StoredEventRouteV1::new(
                event.event_id(),
                event.event_type_id(),
                event.event_hash(),
            ))
        })
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
        sum_optional_envelope_charges(&entities)?
            .checked_add(sum_optional_envelope_charges(&vector_evidence)?)
            .ok_or_else(DurableCodecError::invariant)?,
        sum_optional_envelope_charges(&index_entries)?,
        sum_envelope_charges(&index_epochs)?,
        outcome.encoded_content_charge().get(),
        sum_envelope_charges(&events)?
            .checked_add(sum_envelope_charges(&event_routes)?)
            .ok_or_else(DurableCodecError::invariant)?,
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
        vector_evidence,
        index_entries,
        index_epochs,
        outcome,
        events,
        event_routes,
        outbox_intents,
        provenance,
        commit,
        actual_charge,
    })
}

/// Encodes only records stored independently by the command-segment layout.
///
/// Command-owned immutable facts remain in the retained
/// [`AtomicCommandRecordSet`] and are encoded once when the audit-complete
/// segment is constructed. The sequence-free sizing pass already proved their
/// conservative bound; the final bounded segment encoder remains the exact
/// canonical-byte gate before the transaction can commit.
pub fn encode_capsule_command_record_set_v1(
    records: &mut AtomicCommandRecordSet,
) -> Result<EncodedCapsuleCommandRecordSetV1, DurableCodecError> {
    let prepared = records.take_prepared_capsule_envelopes();
    let (entities, index_entries) = match prepared {
        Some(prepared) => (prepared.entities, prepared.index_entries),
        None => {
            let prepared = prepare_capsule_command_fragments_v1(
                records.entities().to_vec(),
                records.index_entries().to_vec(),
            )?;
            let (_, _, _, encoded) = prepared.into_parts();
            (encoded.entities, encoded.index_entries)
        }
    };
    let index_epochs = records
        .index_epochs()
        .iter()
        .map(|value| encode_index_epoch_v1(value.post_image()))
        .collect::<Result<Vec<_>, _>>()?;
    let vector_evidence = records
        .vector_evidence()
        .iter()
        .map(|value| match value {
            VectorEvidenceMutationV1::Delete { .. } => Ok(None),
            VectorEvidenceMutationV1::Put(value) => encode_vector_evidence_v1(value).map(Some),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let reserved = records.presequence_charge().encoded_upper_bound().classes();
    let fits = sum_optional_envelope_charges(&entities)?
        .checked_add(sum_optional_envelope_charges(&vector_evidence)?)
        .ok_or_else(DurableCodecError::invariant)?
        <= reserved.entities()
        && sum_optional_envelope_charges(&index_entries)? <= reserved.index_entries()
        && sum_envelope_charges(&index_epochs)? <= reserved.index_epochs();
    if !fits {
        return Err(DurableCodecError::new(
            DurableCodecErrorKind::ReservationExceeded,
        ));
    }

    Ok(EncodedCapsuleCommandRecordSetV1 {
        entities,
        vector_evidence,
        index_entries,
        index_epochs,
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

fn sizing_entity_len(
    mutation: &EntityMutation,
    schema_binding_len: usize,
) -> Result<usize, DurableCodecError> {
    let post_image = mutation.post_image();
    sum_proto_fields([
        message_field_len(1, entity_target_to_proto(post_image.target()).encoded_len()),
        varint_field_len(2, MAXIMUM_WIDTH_U64),
        varint_field_len(3, post_image.written_by_contract().get()),
        message_field_len(4, schema_binding_len),
        bytes_field_len(5, post_image.fields_encoded_len()),
    ])
}

fn sizing_successor_len(
    base_len: usize,
    causation_len: Option<usize>,
) -> Result<usize, DurableCodecError> {
    let base = message_field_len(1, base_len)?;
    match causation_len {
        Some(causation_len) => base
            .checked_add(message_field_len(2, causation_len)?)
            .ok_or_else(DurableCodecError::invariant),
        None => Ok(base),
    }
}

/// Conservative protobuf payload length for one entity post-image reference.
///
/// Used by write-set reservation; tests assert this charge dominates real
/// encoded [`CommittedEntityReferenceV2`](crate::CommittedEntityReferenceV2) lengths.
pub fn conservative_entity_reference_payload_len(
    target: &crate::EntityTarget,
) -> Result<usize, DurableCodecError> {
    sum_proto_fields([
        message_field_len(1, entity_target_to_proto(target).encoded_len()),
        varint_field_len(2, MAXIMUM_WIDTH_U64),
        bytes_field_len(3, SIZING_ENTITY_RECORD_HASH.len()),
    ])
}

fn sizing_event_id_len(ordinal: u32) -> usize {
    // Both fields are deliberately charged even when ordinal zero would be
    // omitted by proto3. The reservation must dominate every final encoding.
    varint_field_len(1, MAXIMUM_WIDTH_U64)
        .and_then(|sequence| {
            varint_field_len(2, ordinal).and_then(|ordinal| {
                sequence
                    .checked_add(ordinal)
                    .ok_or_else(DurableCodecError::invariant)
            })
        })
        .expect("two bounded protobuf fields fit usize")
}

fn sizing_event_len(
    event_type_id: u32,
    payload_encoded_len: usize,
    ordinal: u32,
) -> Result<usize, DurableCodecError> {
    sum_proto_fields([
        message_field_len(1, sizing_event_id_len(ordinal)),
        varint_field_len(2, event_type_id),
        bytes_field_len(3, payload_encoded_len),
        bytes_field_len(4, SIZING_EVENT_HASH.len()),
    ])
}

fn sizing_declared_outcome_len(
    outcome: &crate::DeclaredOutcome,
) -> Result<usize, DurableCodecError> {
    sum_proto_fields([
        varint_field_len(1, outcome.outcome_id().get()),
        bytes_field_len(2, outcome.value_encoded_len()),
    ])
}

#[allow(clippy::too_many_arguments)]
fn sizing_outcome_len(
    identity_len: usize,
    plan_len: usize,
    actor_len: usize,
    logical_time_len: usize,
    claims_len: usize,
    declared_outcome_len: usize,
    admission_request_id_len: usize,
    canonical_input_hash_len: usize,
    partition_hash_len: usize,
    provenance_id_len: usize,
    partition_key_len: usize,
    conflict_hash_lens: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
) -> Result<usize, DurableCodecError> {
    let fixed = sum_proto_fields([
        message_field_len(1, identity_len),
        varint_field_len(2, MAXIMUM_WIDTH_U64),
        bytes_field_len(3, admission_request_id_len),
        message_field_len(4, plan_len),
        bytes_field_len(5, canonical_input_hash_len),
        message_field_len(6, actor_len),
        message_field_len(7, logical_time_len),
        bytes_field_len(8, partition_hash_len),
        message_field_len(10, declared_outcome_len),
        message_field_len(11, claims_len),
        bytes_field_len(12, provenance_id_len),
        varint_field_len(13, durability_to_proto(DurabilityMode::Memory) as u64),
        bytes_field_len(14, partition_key_len),
    ])?;
    add_repeated_bytes(fixed, 9, conflict_hash_lens)
}

#[allow(clippy::too_many_arguments)]
fn sizing_provenance_len(
    identity_len: usize,
    plan_len: usize,
    actor_len: usize,
    logical_time_len: usize,
    claims_len: usize,
    admission_request_id_len: usize,
    canonical_input_hash_len: usize,
    partition_hash_len: usize,
    provenance_id_len: usize,
    conflict_hash_lens: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
    affected_entity_lens: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
    event_id_lens: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
) -> Result<usize, DurableCodecError> {
    let mut total = sum_proto_fields([
        bytes_field_len(1, provenance_id_len),
        varint_field_len(2, MAXIMUM_WIDTH_U64),
        message_field_len(3, identity_len),
        bytes_field_len(4, admission_request_id_len),
        message_field_len(5, plan_len),
        bytes_field_len(6, canonical_input_hash_len),
        message_field_len(7, actor_len),
        message_field_len(8, logical_time_len),
        bytes_field_len(9, partition_hash_len),
        varint_field_len(11, 1_u64),
        message_field_len(14, claims_len),
    ])?;
    total = add_repeated_bytes(total, 10, conflict_hash_lens)?;
    total = add_repeated_messages(total, 12, affected_entity_lens)?;
    add_repeated_messages(total, 13, event_id_lens)
}

#[allow(clippy::too_many_arguments)]
fn sizing_commit_len(
    plan_len: usize,
    actor_len: usize,
    logical_time_len: usize,
    declared_outcome_len: usize,
    read_dependencies_len: usize,
    admission_request_id_len: usize,
    canonical_input_hash_len: usize,
    partition_hash_len: usize,
    provenance_id_len: usize,
    conflict_hash_lens: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
    entity_reference_lens: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
    event_reference_lens: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
    event_id_lens: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
) -> Result<usize, DurableCodecError> {
    let mut total = sum_proto_fields([
        varint_field_len(1, MAXIMUM_WIDTH_U64),
        bytes_field_len(2, admission_request_id_len),
        message_field_len(3, plan_len),
        bytes_field_len(4, canonical_input_hash_len),
        message_field_len(5, actor_len),
        message_field_len(6, logical_time_len),
        bytes_field_len(7, partition_hash_len),
        message_field_len(9, read_dependencies_len),
        message_field_len(12, declared_outcome_len),
        bytes_field_len(13, provenance_id_len),
        varint_field_len(15, durability_to_proto(DurabilityMode::Memory) as u64),
    ])?;
    total = add_repeated_bytes(total, 8, conflict_hash_lens)?;
    total = add_repeated_messages(total, 10, entity_reference_lens)?;
    total = add_repeated_messages(total, 11, event_reference_lens)?;
    add_repeated_messages(total, 14, event_id_lens)
}

fn sizing_affected_entity_len(
    mutation: &EntityMutation,
    entity_version: u64,
) -> Result<usize, DurableCodecError> {
    sum_proto_fields([
        message_field_len(
            1,
            entity_target_to_proto(mutation.post_image().target()).encoded_len(),
        ),
        varint_field_len(2, entity_version),
    ])
}

fn sizing_index_entry_len(value: &crate::StoredIndexEntryV2) -> Result<usize, DurableCodecError> {
    sum_proto_fields([
        bytes_field_len(1, value.key().as_bytes().len()),
        message_field_len(2, binding_to_proto(value.schema_binding()).encoded_len()),
        bytes_field_len(3, value.covered_values_encoded_len()),
        bytes_field_len(4, value.partition_key().as_bytes().len()),
    ])
}

fn sizing_index_epoch_len(value: &crate::StoredIndexEpochV1) -> Result<usize, DurableCodecError> {
    sum_proto_fields([
        bytes_field_len(1, value.target().partition_key().as_bytes().len()),
        varint_field_len(2, value.target().index_id().get()),
        message_field_len(3, binding_to_proto(value.schema_binding()).encoded_len()),
        varint_field_len(4, value.epoch().get()),
    ])
}

fn sizing_charge_len(
    record_type: &'static str,
    payload_len: usize,
) -> Result<usize, DurableCodecError> {
    let schema = riffdb_proto::durable::current_record_schema(record_type)
        .ok_or_else(DurableCodecError::invariant)?;
    riffdb_proto::envelope::maximum_encoded_compact_record_bytes(schema, payload_len)
        .map_err(DurableCodecError::from_encode_envelope)
}

fn key_len(field_number: u32, wire_type: prost::encoding::WireType) -> usize {
    prost::encoding::encoded_len_varint(u64::from(field_number << 3 | wire_type as u32))
}

fn varint_field_len(field_number: u32, value: impl Into<u64>) -> Result<usize, DurableCodecError> {
    key_len(field_number, prost::encoding::WireType::Varint)
        .checked_add(prost::encoding::encoded_len_varint(value.into()))
        .ok_or_else(DurableCodecError::invariant)
}

fn bytes_field_len(field_number: u32, len: usize) -> Result<usize, DurableCodecError> {
    length_delimited_field_len(field_number, len)
}

fn message_field_len(field_number: u32, len: usize) -> Result<usize, DurableCodecError> {
    length_delimited_field_len(field_number, len)
}

fn length_delimited_field_len(field_number: u32, len: usize) -> Result<usize, DurableCodecError> {
    key_len(field_number, prost::encoding::WireType::LengthDelimited)
        .checked_add(prost::encoding::encoded_len_varint(
            u64::try_from(len).map_err(|_| DurableCodecError::invariant())?,
        ))
        .and_then(|value| value.checked_add(len))
        .ok_or_else(DurableCodecError::invariant)
}

fn sum_proto_fields(
    values: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
) -> Result<usize, DurableCodecError> {
    sum_sizes(values)
}

fn add_repeated_bytes(
    initial: usize,
    field_number: u32,
    values: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
) -> Result<usize, DurableCodecError> {
    values.into_iter().try_fold(initial, |total, len| {
        total
            .checked_add(bytes_field_len(field_number, len?)?)
            .ok_or_else(DurableCodecError::invariant)
    })
}

fn add_repeated_messages(
    initial: usize,
    field_number: u32,
    values: impl IntoIterator<Item = Result<usize, DurableCodecError>>,
) -> Result<usize, DurableCodecError> {
    values.into_iter().try_fold(initial, |total, len| {
        total
            .checked_add(message_field_len(field_number, len?)?)
            .ok_or_else(DurableCodecError::invariant)
    })
}

#[cfg(test)]
fn canonical_record_len(value: &CanonicalRecord) -> Result<usize, DurableCodecError> {
    canonical_record_payload_len(value, 0)?
        .checked_add(1)
        .filter(|len| *len <= riffdb_types::MAX_CANONICAL_DOCUMENT_BYTES)
        .ok_or_else(DurableCodecError::invariant)
}

#[cfg(test)]
fn canonical_record_payload_len(
    value: &CanonicalRecord,
    depth: usize,
) -> Result<usize, DurableCodecError> {
    if depth > riffdb_types::MAX_NESTING_DEPTH {
        return Err(DurableCodecError::invariant());
    }
    let mut total = 5usize;
    let mut previous = None;
    for (field_id, field) in value.fields() {
        if previous.is_some_and(|prior| prior >= field_id.get()) {
            return Err(DurableCodecError::invariant());
        }
        previous = Some(field_id.get());
        total = total
            .checked_add(4)
            .and_then(|value| {
                canonical_value_len(field, depth + 1)
                    .ok()
                    .and_then(|field| value.checked_add(field))
            })
            .ok_or_else(DurableCodecError::invariant)?;
    }
    Ok(total)
}

#[cfg(test)]
fn canonical_value_len(value: &CanonicalValue, depth: usize) -> Result<usize, DurableCodecError> {
    if depth > riffdb_types::MAX_NESTING_DEPTH {
        return Err(DurableCodecError::invariant());
    }
    if let CanonicalValue::Record(record) = value {
        return canonical_record_payload_len(record, depth)?
            .checked_add(1)
            .filter(|len| *len <= riffdb_types::MAX_CANONICAL_DOCUMENT_BYTES)
            .ok_or_else(DurableCodecError::invariant);
    }
    let payload = match value {
        CanonicalValue::Null => 0,
        CanonicalValue::Bool(_) => 1,
        CanonicalValue::I64(_) | CanonicalValue::U64(_) => 8,
        CanonicalValue::Decimal(_) => 18,
        CanonicalValue::Money(_) => 21,
        CanonicalValue::String(value) => 4usize
            .checked_add(value.as_str().len())
            .ok_or_else(DurableCodecError::invariant)?,
        CanonicalValue::Bytes(value) => 4usize
            .checked_add(value.as_bytes().len())
            .ok_or_else(DurableCodecError::invariant)?,
        CanonicalValue::Timestamp(_) => 12,
        CanonicalValue::Date(_) => 4,
        CanonicalValue::Uuid(_) => 16,
        CanonicalValue::Enum { .. } => 8,
        CanonicalValue::List(values) => {
            values.values().iter().try_fold(4usize, |total, value| {
                total
                    .checked_add(canonical_value_len(value, depth + 1)?)
                    .ok_or_else(DurableCodecError::invariant)
            })?
        }
        CanonicalValue::Record(_) => unreachable!("record sizing returns above"),
        CanonicalValue::Vector(vector) => 4usize
            .checked_add(vector.byte_size())
            .ok_or_else(DurableCodecError::invariant)?,
    };
    payload
        .checked_add(2)
        .filter(|len| *len <= riffdb_types::MAX_CANONICAL_DOCUMENT_BYTES)
        .ok_or_else(DurableCodecError::invariant)
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

#[cfg(test)]
mod aggregate_classification_tests {
    use super::*;
    use riffdb_types::{
        CanonicalBytes, CanonicalList, CanonicalString, FieldId, encode_canonical_record,
    };

    #[test]
    fn only_a_successful_final_comparison_produces_the_aggregate_cap_variant() {
        let exact = raw(MAX_STAGED_WRITE_BYTES);
        assert!(matches!(
            finish_upper_bound(exact),
            Ok(EncodedWriteSetUpperBoundResultV1::Fits(bound))
                if bound.total() == MAX_STAGED_WRITE_BYTES
        ));

        let over = raw(MAX_STAGED_WRITE_BYTES + 1);
        assert!(matches!(
            finish_upper_bound(over),
            Ok(EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_))
        ));

        let overflow = RawWriteClassBreakdownV1 {
            allocator: usize::MAX,
            pending_resolution: 1,
            ..raw(0)
        };
        assert_eq!(
            finish_upper_bound(overflow)
                .expect_err("checked aggregate overflow remains a codec error")
                .kind(),
            DurableCodecErrorKind::LimitExceeded
        );
    }

    #[test]
    fn structural_canonical_sizing_matches_the_canonical_encoder() {
        let nested = CanonicalRecord::new(vec![
            (
                FieldId::new(1).expect("field"),
                CanonicalValue::String(CanonicalString::new("hello".to_owned()).expect("string")),
            ),
            (
                FieldId::new(2).expect("field"),
                CanonicalValue::List(
                    CanonicalList::new(vec![
                        CanonicalValue::U64(u64::MAX),
                        CanonicalValue::Bytes(
                            CanonicalBytes::new(vec![0, 1, 2, 3]).expect("bytes"),
                        ),
                    ])
                    .expect("list"),
                ),
            ),
        ])
        .expect("nested record");
        let nested_expected = nested.clone();
        let record = CanonicalRecord::new(vec![
            (
                FieldId::new(1).expect("field"),
                CanonicalValue::Record(nested),
            ),
            (FieldId::new(2).expect("field"), CanonicalValue::Bool(true)),
        ])
        .expect("record");

        assert_eq!(
            canonical_record_len(&nested_expected).expect("nested structural length"),
            encode_canonical_record(&nested_expected)
                .expect("nested canonical encoding")
                .len()
        );
        assert_eq!(
            canonical_record_len(&record).expect("structural length"),
            encode_canonical_record(&record)
                .expect("canonical encoding")
                .len()
        );
    }

    const fn raw(allocator: usize) -> RawWriteClassBreakdownV1 {
        RawWriteClassBreakdownV1 {
            allocator,
            pending_resolution: 0,
            entities: 0,
            index_entries: 0,
            index_epochs: 0,
            outcome: 0,
            events: 0,
            outbox_intents: 0,
            provenance: 0,
            commit: 0,
        }
    }
}
