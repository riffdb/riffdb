use prost::Message;
use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    CommitSequence, DatabaseId, IndexEpoch, IndexEpochPosition, encode_canonical_record,
    hash_command_batch_document,
};

use crate::{
    AffectedEntityV1, CommandDerivedIndexKindV1, CommandDerivedIndexManifestEntryV1,
    CommandDerivedMemberV1, CommandSegmentDigestV1, CommandSegmentManifestV1,
    CommittedEntityTransitionV1, EncodedPageItem, EntityChainStateV1, IndexEpochAdvanceV1,
    StoredCommandCapsuleV1, StoredCommandCapsuleV2, StoredCommandDerivedIndexCheckpointV1,
    StoredCommandSegmentV1, StoredProvenanceRecordV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, binding_to_proto, canonical_record_from_bytes,
    decode_message, decode_record_variant_chain, encode_message,
    encode_structurally_proven_message, fixed, require, storage_result,
};
use super::{application, command_capsule};

const COMMAND_CAPSULE_V1: &str = "riffdb.storage.v1.StoredCommandCapsuleV1";
const COMMAND_CAPSULE_V2: &str = "riffdb.storage.v1.StoredCommandCapsuleV2";
const COMMAND_CAPSULE_V3: &str = "riffdb.storage.v1.StoredCommandCapsuleV3";
const COMMAND_CAPSULE_V4: &str = "riffdb.storage.v1.StoredCommandCapsuleV4";
const COMMAND_CAPSULE_V5: &str = "riffdb.storage.v1.StoredCommandCapsuleV5";
const COMMAND_CAPSULE_V6: &str = "riffdb.storage.v1.StoredCommandCapsuleV6";
const COMMAND_SEGMENT_V1: &str = "riffdb.storage.v1.StoredCommandSegmentV1";
const COMMAND_SEGMENT_V2: &str = "riffdb.storage.v1.StoredCommandSegmentV2";
const COMMAND_SEGMENT_V3: &str = "riffdb.storage.v1.StoredCommandSegmentV3";
const COMMAND_SEGMENT_V4: &str = "riffdb.storage.v1.StoredCommandSegmentV4";
const COMMAND_SEGMENT_V5: &str = "riffdb.storage.v1.StoredCommandSegmentV5";
const COMMAND_DERIVED_INDEX_CHECKPOINT_V1: &str =
    "riffdb.storage.v1.StoredCommandDerivedIndexCheckpointV1";

const MANIFEST_DIGEST_LABEL: &[u8] = b"RIFFDB-COMMAND-SEGMENT-MANIFEST-V1\0";
const SEGMENT_DIGEST_LABEL: &[u8] = b"RIFFDB-COMMAND-SEGMENT-V1\0";
const CHECKPOINT_DIGEST_LABEL: &[u8] = b"RIFFDB-COMMAND-DERIVED-INDEX-CHECKPOINT-V1\0";

fn component_digest(label: &[u8], bytes: &[u8]) -> CommandSegmentDigestV1 {
    let mut preimage = Vec::with_capacity(label.len() + bytes.len());
    preimage.extend_from_slice(label);
    preimage.extend_from_slice(bytes);
    CommandSegmentDigestV1::from_bytes(*hash_command_batch_document(&preimage).as_bytes())
}

fn kind_to_proto(value: CommandDerivedIndexKindV1) -> wire::CommandDerivedIndexKindV1 {
    match value {
        CommandDerivedIndexKindV1::Idempotency => {
            wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindIdempotency
        }
        CommandDerivedIndexKindV1::Provenance => {
            wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindProvenance
        }
        CommandDerivedIndexKindV1::AuditSequence => {
            wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindAuditSequence
        }
        CommandDerivedIndexKindV1::AuditRequest => {
            wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindAuditRequest
        }
        CommandDerivedIndexKindV1::EventRoute => {
            wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindEventRoute
        }
        CommandDerivedIndexKindV1::PendingOutbox => {
            wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindPendingOutbox
        }
    }
}

fn kind_from_proto(value: i32) -> Result<CommandDerivedIndexKindV1, DurableCodecError> {
    match wire::CommandDerivedIndexKindV1::try_from(value)
        .map_err(|_| DurableCodecError::corrupt())?
    {
        wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindIdempotency => {
            Ok(CommandDerivedIndexKindV1::Idempotency)
        }
        wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindProvenance => {
            Ok(CommandDerivedIndexKindV1::Provenance)
        }
        wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindAuditSequence => {
            Ok(CommandDerivedIndexKindV1::AuditSequence)
        }
        wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindAuditRequest => {
            Ok(CommandDerivedIndexKindV1::AuditRequest)
        }
        wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindEventRoute => {
            Ok(CommandDerivedIndexKindV1::EventRoute)
        }
        wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindPendingOutbox => {
            Ok(CommandDerivedIndexKindV1::PendingOutbox)
        }
        wire::CommandDerivedIndexKindV1::CommandDerivedIndexKindUnspecified => {
            Err(DurableCodecError::corrupt())
        }
    }
}

fn member_to_proto(value: CommandDerivedMemberV1) -> wire::CommandDerivedMemberV1 {
    match value {
        CommandDerivedMemberV1::Command => {
            wire::CommandDerivedMemberV1::CommandDerivedMemberCommand
        }
        CommandDerivedMemberV1::AuditStarted => {
            wire::CommandDerivedMemberV1::CommandDerivedMemberAuditStarted
        }
        CommandDerivedMemberV1::AuditTerminal => {
            wire::CommandDerivedMemberV1::CommandDerivedMemberAuditTerminal
        }
        CommandDerivedMemberV1::Event => wire::CommandDerivedMemberV1::CommandDerivedMemberEvent,
    }
}

fn member_from_proto(value: i32) -> Result<CommandDerivedMemberV1, DurableCodecError> {
    match wire::CommandDerivedMemberV1::try_from(value).map_err(|_| DurableCodecError::corrupt())? {
        wire::CommandDerivedMemberV1::CommandDerivedMemberCommand => {
            Ok(CommandDerivedMemberV1::Command)
        }
        wire::CommandDerivedMemberV1::CommandDerivedMemberAuditStarted => {
            Ok(CommandDerivedMemberV1::AuditStarted)
        }
        wire::CommandDerivedMemberV1::CommandDerivedMemberAuditTerminal => {
            Ok(CommandDerivedMemberV1::AuditTerminal)
        }
        wire::CommandDerivedMemberV1::CommandDerivedMemberEvent => {
            Ok(CommandDerivedMemberV1::Event)
        }
        wire::CommandDerivedMemberV1::CommandDerivedMemberUnspecified => {
            Err(DurableCodecError::corrupt())
        }
    }
}

fn manifest_entry_to_proto(
    value: &CommandDerivedIndexManifestEntryV1,
) -> wire::CommandDerivedIndexManifestEntryV1 {
    wire::CommandDerivedIndexManifestEntryV1 {
        kind: kind_to_proto(value.kind()) as i32,
        member: member_to_proto(value.member()) as i32,
        exact_key: value.exact_key().to_vec(),
        command_ordinal: u32::from(value.command_ordinal()),
        member_ordinal: u32::from(value.member_ordinal()),
        segment_first_commit_sequence: value.segment_first_commit_sequence().get(),
    }
}

fn manifest_entry_from_proto(
    value: wire::CommandDerivedIndexManifestEntryV1,
) -> Result<CommandDerivedIndexManifestEntryV1, DurableCodecError> {
    storage_result(CommandDerivedIndexManifestEntryV1::new(
        kind_from_proto(value.kind)?,
        member_from_proto(value.member)?,
        value.exact_key,
        u16::try_from(value.command_ordinal).map_err(|_| DurableCodecError::corrupt())?,
        u16::try_from(value.member_ordinal).map_err(|_| DurableCodecError::corrupt())?,
        CommitSequence::new(value.segment_first_commit_sequence)
            .ok_or_else(DurableCodecError::corrupt)?,
    ))
}

fn manifest_to_proto(value: &CommandSegmentManifestV1) -> wire::CommandSegmentManifestV1 {
    let entries = value
        .entries()
        .iter()
        .map(manifest_entry_to_proto)
        .collect();
    let mut manifest = wire::CommandSegmentManifestV1 {
        entries,
        digest: Vec::new(),
    };
    manifest.digest = component_digest(MANIFEST_DIGEST_LABEL, &manifest.encode_to_vec())
        .as_bytes()
        .to_vec();
    manifest
}

fn manifest_from_proto(
    mut value: wire::CommandSegmentManifestV1,
) -> Result<CommandSegmentManifestV1, DurableCodecError> {
    let supplied = CommandSegmentDigestV1::from_bytes(fixed(value.digest.clone())?);
    value.digest.clear();
    if component_digest(MANIFEST_DIGEST_LABEL, &value.encode_to_vec()) != supplied {
        return Err(DurableCodecError::corrupt());
    }
    storage_result(CommandSegmentManifestV1::new(
        value
            .entries
            .into_iter()
            .map(manifest_entry_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn transition_to_proto(value: &IndexEpochAdvanceV1) -> wire::StoredIndexGenerationTransitionV1 {
    wire::StoredIndexGenerationTransitionV1 {
        post_image: Some(application::index_epoch_to_proto(value.post_image())),
        prior_generation: match value.prior() {
            IndexEpochPosition::BeforeFirst => None,
            IndexEpochPosition::Value(value) => Some(value.get()),
        },
    }
}

fn transition_from_proto(
    value: wire::StoredIndexGenerationTransitionV1,
) -> Result<IndexEpochAdvanceV1, DurableCodecError> {
    let post_image = application::index_epoch_from_proto(require(value.post_image)?)?;
    let prior = match value.prior_generation {
        None => IndexEpochPosition::BeforeFirst,
        Some(value) => IndexEpochPosition::Value(
            IndexEpoch::new(value).ok_or_else(DurableCodecError::corrupt)?,
        ),
    };
    let transition = IndexEpochAdvanceV1::new(
        post_image.target().clone(),
        post_image.schema_binding().clone(),
        prior,
    )
    .map_err(|_| DurableCodecError::corrupt())?;
    if transition.post_image() != &post_image {
        return Err(DurableCodecError::corrupt());
    }
    Ok(transition)
}

fn capsule_v2_to_proto(value: &StoredCommandCapsuleV2) -> wire::StoredCommandCapsuleV2 {
    wire::StoredCommandCapsuleV2 {
        base: Some(command_capsule::command_capsule_to_proto(value.base())),
        events: value
            .events()
            .iter()
            .map(application::event_to_proto)
            .collect(),
        index_generation_transitions: value
            .index_generation_transitions()
            .iter()
            .map(transition_to_proto)
            .collect(),
    }
}

fn event_bytes_for_seal(value: &crate::StoredDurableEventV1) -> Result<Vec<u8>, DurableCodecError> {
    let event_id = value.event_id();
    let mut event_id_bytes = Vec::new();
    append_varint_field(&mut event_id_bytes, 0x08, event_id.commit_sequence().get());
    append_varint_field(
        &mut event_id_bytes,
        0x10,
        u64::from(event_id.event_ordinal()),
    );

    let mut event = Vec::new();
    append_length_delimited_field(&mut event, 0x0a, &event_id_bytes)?;
    append_varint_field(&mut event, 0x10, u64::from(value.event_type_id().get()));
    append_length_delimited_field(&mut event, 0x1a, value.payload_encoded())?;
    append_length_delimited_field(&mut event, 0x22, value.event_hash().as_bytes())?;
    Ok(event)
}

fn transition_bytes_for_seal(value: &IndexEpochAdvanceV1) -> Result<Vec<u8>, DurableCodecError> {
    let post_image = value.post_image();
    let mut post_image_bytes = Vec::new();
    append_length_delimited_field(
        &mut post_image_bytes,
        0x0a,
        post_image.target().partition_key().as_bytes(),
    )?;
    append_varint_field(
        &mut post_image_bytes,
        0x10,
        u64::from(post_image.target().index_id().get()),
    );
    let binding = binding_to_proto(post_image.schema_binding()).encode_to_vec();
    append_length_delimited_field(&mut post_image_bytes, 0x1a, &binding)?;
    append_varint_field(&mut post_image_bytes, 0x20, post_image.epoch().get());

    let mut transition = Vec::new();
    append_length_delimited_field(&mut transition, 0x0a, &post_image_bytes)?;
    if let IndexEpochPosition::Value(prior) = value.prior() {
        append_varint_field(&mut transition, 0x10, prior.get());
    }
    Ok(transition)
}

fn capsule_v2_bytes_for_seal(value: &StoredCommandCapsuleV2) -> Result<Vec<u8>, DurableCodecError> {
    let base = command_capsule::command_capsule_bytes_for_seal(value.base())?;
    let mut capsule = Vec::new();
    append_length_delimited_field(&mut capsule, 0x0a, &base)?;
    for event in value.events() {
        let event = event_bytes_for_seal(event)?;
        append_length_delimited_field(&mut capsule, 0x12, &event)?;
    }
    for transition in value.index_generation_transitions() {
        let transition = transition_bytes_for_seal(transition)?;
        append_length_delimited_field(&mut capsule, 0x1a, &transition)?;
    }
    Ok(capsule)
}

fn capsule_v3_to_proto(value: &StoredCommandCapsuleV2) -> wire::StoredCommandCapsuleV3 {
    wire::StoredCommandCapsuleV3 {
        base: Some(capsule_v2_to_proto(value)),
        canonical_service_values: encode_canonical_record(value.base().outcome().service_values())
            .expect("checked canonical service values must encode"),
    }
}

fn capsule_v3_bytes_for_seal(value: &StoredCommandCapsuleV2) -> Result<Vec<u8>, DurableCodecError> {
    let base = capsule_v2_bytes_for_seal(value)?;
    let service_values = encode_canonical_record(value.base().outcome().service_values())
        .map_err(|_| DurableCodecError::invariant())?;
    let mut capsule = Vec::new();
    append_length_delimited_field(&mut capsule, 0x0a, &base)?;
    append_length_delimited_field(&mut capsule, 0x12, &service_values)?;
    Ok(capsule)
}

fn capsule_v4_bytes_for_seal(value: &StoredCommandCapsuleV2) -> Result<Vec<u8>, DurableCodecError> {
    let base = capsule_v3_bytes_for_seal(value)?;
    let mut capsule = Vec::new();
    append_length_delimited_field(&mut capsule, 0x0a, &base)?;
    for transition in value.entity_transitions() {
        let transition =
            super::entity_transition::entity_transition_to_proto(transition).encode_to_vec();
        append_length_delimited_field(&mut capsule, 0x12, &transition)?;
    }
    Ok(capsule)
}

fn capsule_v2_from_proto(
    value: wire::StoredCommandCapsuleV2,
) -> Result<StoredCommandCapsuleV2, DurableCodecError> {
    let events = value
        .events
        .into_iter()
        .map(application::event_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let base_envelope = encode_message(COMMAND_CAPSULE_V1, &require(value.base)?)?.into_bytes();
    let (base, _) =
        command_capsule::decode_command_capsule_v1(&base_envelope, events.clone())?.into_parts();
    let transitions = value
        .index_generation_transitions
        .into_iter()
        .map(transition_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    storage_result(StoredCommandCapsuleV2::new(base, events, transitions))
}

fn capsule_v3_from_proto(
    value: wire::StoredCommandCapsuleV3,
) -> Result<StoredCommandCapsuleV2, DurableCodecError> {
    let service_values = canonical_record_from_bytes(&value.canonical_service_values)?;
    let capsule = capsule_v2_from_proto(require(value.base)?)?;
    let transitions = capsule.index_generation_transitions().to_vec();
    let (outcome, provenance, commit, started, terminal) = capsule.base().clone().into_parts();
    let outcome = storage_result(outcome.with_service_values(service_values))?;
    let base = storage_result(crate::StoredCommandCapsuleV1::new(
        outcome, provenance, commit, started, terminal,
    ))?;
    storage_result(StoredCommandCapsuleV2::from_base(base, transitions))
}

fn capsule_v4_to_proto(value: &StoredCommandCapsuleV2) -> wire::StoredCommandCapsuleV4 {
    wire::StoredCommandCapsuleV4 {
        base: Some(capsule_v3_to_proto(value)),
        entity_transitions: value
            .entity_transitions()
            .iter()
            .map(super::entity_transition::entity_transition_to_proto)
            .collect(),
    }
}

fn capsule_v4_from_proto(
    value: wire::StoredCommandCapsuleV4,
) -> Result<StoredCommandCapsuleV2, DurableCodecError> {
    let entity_transitions = value
        .entity_transitions
        .into_iter()
        .map(super::entity_transition::entity_transition_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let capsule = capsule_v3_from_proto(require(value.base)?)?;
    let generations = capsule.index_generation_transitions().to_vec();
    let base = base_with_entity_transition_provenance(capsule.base().clone(), &entity_transitions)?;
    storage_result(StoredCommandCapsuleV2::from_base_with_entity_transitions(
        base,
        generations,
        entity_transitions,
    ))
}

fn capsule_requires_v5(value: &StoredCommandCapsuleV2) -> bool {
    value
        .events()
        .iter()
        .any(|event| event.policy_anchor().is_some())
}

/// Least-sufficient immutable wire identity for command-segment members.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandCapsuleWireVersionV1 {
    /// Entity-transition authority without event-policy anchors.
    V4,
    /// Event-policy authority with the historical transition bound.
    V5,
    /// Correlated index-work authority with the expanded transition bound.
    V6,
}

/// Selects the least-sufficient wire identity for one command capsule.
#[must_use]
pub fn command_capsule_wire_version_v1(
    value: &StoredCommandCapsuleV2,
) -> CommandCapsuleWireVersionV1 {
    if value.index_generation_transitions().len() > crate::MAX_INDEX_DELTAS {
        CommandCapsuleWireVersionV1::V6
    } else if capsule_requires_v5(value) {
        CommandCapsuleWireVersionV1::V5
    } else {
        CommandCapsuleWireVersionV1::V4
    }
}

/// Selects the least-sufficient segment-wide member identity.
#[must_use]
pub fn command_segment_wire_version_v1(
    commands: &[StoredCommandCapsuleV2],
) -> CommandCapsuleWireVersionV1 {
    commands
        .iter()
        .map(command_capsule_wire_version_v1)
        .max_by_key(|version| match version {
            CommandCapsuleWireVersionV1::V4 => 0_u8,
            CommandCapsuleWireVersionV1::V5 => 1,
            CommandCapsuleWireVersionV1::V6 => 2,
        })
        .unwrap_or(CommandCapsuleWireVersionV1::V4)
}

fn capsule_v5_to_proto(value: &StoredCommandCapsuleV2) -> wire::StoredCommandCapsuleV5 {
    wire::StoredCommandCapsuleV5 {
        base: Some(command_capsule::command_capsule_to_proto(value.base())),
        events: value
            .events()
            .iter()
            .map(application::event_variant_to_proto)
            .collect(),
        index_generation_transitions: value
            .index_generation_transitions()
            .iter()
            .map(transition_to_proto)
            .collect(),
        canonical_service_values: encode_canonical_record(value.base().outcome().service_values())
            .expect("checked canonical service values must encode"),
        entity_transitions: value
            .entity_transitions()
            .iter()
            .map(super::entity_transition::entity_transition_to_proto)
            .collect(),
    }
}

fn capsule_v6_to_proto(value: &StoredCommandCapsuleV2) -> wire::StoredCommandCapsuleV6 {
    wire::StoredCommandCapsuleV6 {
        base: Some(command_capsule::command_capsule_to_proto(value.base())),
        events: value
            .events()
            .iter()
            .map(application::event_variant_to_proto)
            .collect(),
        index_generation_transitions: value
            .index_generation_transitions()
            .iter()
            .map(transition_to_proto)
            .collect(),
        canonical_service_values: encode_canonical_record(value.base().outcome().service_values())
            .expect("checked canonical service values must encode"),
        entity_transitions: value
            .entity_transitions()
            .iter()
            .map(super::entity_transition::entity_transition_to_proto)
            .collect(),
    }
}

/// Move-only canonical command member prepared before ordered segment sealing.
///
/// Construction is limited to [`prepare_command_segment_capsule_v1`]. The
/// ordered writer joins the retained semantic capsule to this exact sequence
/// identity and wire variant, so crossing a pure encoding lane never requires
/// rebuilding or hashing the immutable member graph.
pub struct PreparedCommandSegmentCapsuleV1 {
    commit_sequence: CommitSequence,
    wire_version: CommandCapsuleWireVersionV1,
    bytes: Vec<u8>,
}

impl PreparedCommandSegmentCapsuleV1 {
    /// Returns the authoritative command sequence bound into these bytes.
    #[must_use]
    pub const fn commit_sequence(&self) -> CommitSequence {
        self.commit_sequence
    }
}

/// Canonically encodes one immutable command-segment member exactly once.
///
/// The returned value contains no storage, ordering, durability, or
/// publication authority. It is suitable for a bounded pure worker and can be
/// consumed only by the checked complete-segment seal below.
pub fn prepare_command_segment_capsule_v1(
    value: &StoredCommandCapsuleV2,
    wire_version: CommandCapsuleWireVersionV1,
) -> Result<PreparedCommandSegmentCapsuleV1, DurableCodecError> {
    let required = command_capsule_wire_version_v1(value);
    if matches!(
        (required, wire_version),
        (
            CommandCapsuleWireVersionV1::V5,
            CommandCapsuleWireVersionV1::V4
        ) | (
            CommandCapsuleWireVersionV1::V6,
            CommandCapsuleWireVersionV1::V4
        ) | (
            CommandCapsuleWireVersionV1::V6,
            CommandCapsuleWireVersionV1::V5
        )
    ) {
        return Err(DurableCodecError::invariant());
    }
    let bytes = match wire_version {
        CommandCapsuleWireVersionV1::V4 => capsule_v4_bytes_for_seal(value)?,
        CommandCapsuleWireVersionV1::V5 => capsule_v5_to_proto(value).encode_to_vec(),
        CommandCapsuleWireVersionV1::V6 => capsule_v6_to_proto(value).encode_to_vec(),
    };
    Ok(PreparedCommandSegmentCapsuleV1 {
        commit_sequence: value.commit_sequence(),
        wire_version,
        bytes,
    })
}

fn capsule_v5_from_proto(
    value: wire::StoredCommandCapsuleV5,
) -> Result<StoredCommandCapsuleV2, DurableCodecError> {
    let events = value
        .events
        .into_iter()
        .map(application::event_variant_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let base_envelope = encode_message(COMMAND_CAPSULE_V1, &require(value.base)?)?.into_bytes();
    let (base, _) =
        command_capsule::decode_command_capsule_v1(&base_envelope, events)?.into_parts();
    let service_values = canonical_record_from_bytes(&value.canonical_service_values)?;
    let (outcome, provenance, commit, started, terminal) = base.into_parts();
    let outcome = storage_result(outcome.with_service_values(service_values))?;
    let base = storage_result(StoredCommandCapsuleV1::new(
        outcome, provenance, commit, started, terminal,
    ))?;
    let generations = value
        .index_generation_transitions
        .into_iter()
        .map(transition_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let entity_transitions = value
        .entity_transitions
        .into_iter()
        .map(super::entity_transition::entity_transition_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let base = base_with_entity_transition_provenance(base, &entity_transitions)?;
    storage_result(StoredCommandCapsuleV2::from_base_with_entity_transitions(
        base,
        generations,
        entity_transitions,
    ))
}

fn capsule_v6_from_proto(
    value: wire::StoredCommandCapsuleV6,
) -> Result<StoredCommandCapsuleV2, DurableCodecError> {
    capsule_v5_from_proto(wire::StoredCommandCapsuleV5 {
        base: value.base,
        events: value.events,
        index_generation_transitions: value.index_generation_transitions,
        canonical_service_values: value.canonical_service_values,
        entity_transitions: value.entity_transitions,
    })
}

fn base_with_entity_transition_provenance(
    base: StoredCommandCapsuleV1,
    transitions: &[CommittedEntityTransitionV1],
) -> Result<StoredCommandCapsuleV1, DurableCodecError> {
    if transitions.is_empty() {
        return Ok(base);
    }
    let mut affected = Vec::with_capacity(transitions.len());
    for transition in transitions {
        let version = match transition.next_state() {
            EntityChainStateV1::Live { version, .. } => version,
            EntityChainStateV1::Deleted => match transition.prior_state() {
                EntityChainStateV1::Live { version, .. } => version,
                EntityChainStateV1::Deleted | EntityChainStateV1::NeverExisted => {
                    return Err(DurableCodecError::corrupt());
                }
            },
            EntityChainStateV1::NeverExisted => return Err(DurableCodecError::corrupt()),
        };
        affected.push(AffectedEntityV1::from_stored_parts(
            transition.target().clone(),
            version,
        ));
    }
    let (outcome, provenance, commit, started, terminal) = base.into_parts();
    let provenance = storage_result(StoredProvenanceRecordV1::new_with_causation(
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
        affected,
        provenance.event_ids().to_vec(),
        provenance.admitted_claims().clone(),
        provenance.causation(),
    ))?;
    storage_result(StoredCommandCapsuleV1::new(
        outcome, provenance, commit, started, terminal,
    ))
}

/// Encodes one complete V2 command capsule.
pub fn encode_command_capsule_v2(
    value: &StoredCommandCapsuleV2,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    match command_capsule_wire_version_v1(value) {
        CommandCapsuleWireVersionV1::V4 => {
            encode_message(COMMAND_CAPSULE_V4, &capsule_v4_to_proto(value))
        }
        CommandCapsuleWireVersionV1::V5 => {
            encode_message(COMMAND_CAPSULE_V5, &capsule_v5_to_proto(value))
        }
        CommandCapsuleWireVersionV1::V6 => {
            encode_message(COMMAND_CAPSULE_V6, &capsule_v6_to_proto(value))
        }
    }
}

/// Decodes one complete V2 command capsule.
pub fn decode_command_capsule_v2(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCommandCapsuleV2>, DurableCodecError> {
    match decode_record_variant_chain(
        encoded,
        &[
            COMMAND_CAPSULE_V6,
            COMMAND_CAPSULE_V5,
            COMMAND_CAPSULE_V4,
            COMMAND_CAPSULE_V3,
            COMMAND_CAPSULE_V2,
        ],
    )? {
        0 => decode_message::<wire::StoredCommandCapsuleV6, _, _>(
            COMMAND_CAPSULE_V6,
            encoded,
            capsule_v6_from_proto,
        ),
        1 => decode_message::<wire::StoredCommandCapsuleV5, _, _>(
            COMMAND_CAPSULE_V5,
            encoded,
            capsule_v5_from_proto,
        ),
        2 => decode_message::<wire::StoredCommandCapsuleV4, _, _>(
            COMMAND_CAPSULE_V4,
            encoded,
            capsule_v4_from_proto,
        ),
        3 => decode_message::<wire::StoredCommandCapsuleV3, _, _>(
            COMMAND_CAPSULE_V3,
            encoded,
            capsule_v3_from_proto,
        ),
        4 => decode_message::<wire::StoredCommandCapsuleV2, _, _>(
            COMMAND_CAPSULE_V2,
            encoded,
            capsule_v2_from_proto,
        ),
        _ => unreachable!("closed durable record variant index"),
    }
}

fn segment_body_v3_to_proto(value: &StoredCommandSegmentV1) -> wire::StoredCommandSegmentBodyV3 {
    wire::StoredCommandSegmentBodyV3 {
        database_id: value.database_id().as_bytes().to_vec(),
        history_incarnation: value.history_incarnation(),
        predecessor_segment_hash: value
            .predecessor_segment_digest()
            .map_or_else(Vec::new, |digest| digest.as_bytes().to_vec()),
        first_commit_sequence: value.first_commit_sequence().get(),
        last_commit_sequence: value.last_commit_sequence().get(),
        first_administration_sequence: value.first_administration_sequence().get(),
        last_administration_sequence: value.last_administration_sequence().get(),
        commands: value.commands().iter().map(capsule_v4_to_proto).collect(),
        manifest: Some(manifest_to_proto(value.manifest())),
    }
}

fn segment_wire_version(value: &StoredCommandSegmentV1) -> CommandCapsuleWireVersionV1 {
    command_segment_wire_version_v1(value.commands())
}

fn segment_body_v4_to_proto(value: &StoredCommandSegmentV1) -> wire::StoredCommandSegmentBodyV4 {
    wire::StoredCommandSegmentBodyV4 {
        database_id: value.database_id().as_bytes().to_vec(),
        history_incarnation: value.history_incarnation(),
        predecessor_segment_hash: value
            .predecessor_segment_digest()
            .map_or_else(Vec::new, |digest| digest.as_bytes().to_vec()),
        first_commit_sequence: value.first_commit_sequence().get(),
        last_commit_sequence: value.last_commit_sequence().get(),
        first_administration_sequence: value.first_administration_sequence().get(),
        last_administration_sequence: value.last_administration_sequence().get(),
        commands: value.commands().iter().map(capsule_v5_to_proto).collect(),
        manifest: Some(manifest_to_proto(value.manifest())),
    }
}

fn segment_body_v5_to_proto(value: &StoredCommandSegmentV1) -> wire::StoredCommandSegmentBodyV5 {
    wire::StoredCommandSegmentBodyV5 {
        database_id: value.database_id().as_bytes().to_vec(),
        history_incarnation: value.history_incarnation(),
        predecessor_segment_hash: value
            .predecessor_segment_digest()
            .map_or_else(Vec::new, |digest| digest.as_bytes().to_vec()),
        first_commit_sequence: value.first_commit_sequence().get(),
        last_commit_sequence: value.last_commit_sequence().get(),
        first_administration_sequence: value.first_administration_sequence().get(),
        last_administration_sequence: value.last_administration_sequence().get(),
        commands: value.commands().iter().map(capsule_v6_to_proto).collect(),
        manifest: Some(manifest_to_proto(value.manifest())),
    }
}

fn segment_from_body_v3(
    body: wire::StoredCommandSegmentBodyV3,
    digest: CommandSegmentDigestV1,
) -> Result<StoredCommandSegmentV1, DurableCodecError> {
    if component_digest(SEGMENT_DIGEST_LABEL, &body.encode_to_vec()) != digest {
        return Err(DurableCodecError::corrupt());
    }
    let first =
        CommitSequence::new(body.first_commit_sequence).ok_or_else(DurableCodecError::corrupt)?;
    let last =
        CommitSequence::new(body.last_commit_sequence).ok_or_else(DurableCodecError::corrupt)?;
    let first_administration =
        riffdb_types::AdministrationSequence::new(body.first_administration_sequence)
            .ok_or_else(DurableCodecError::corrupt)?;
    let last_administration =
        riffdb_types::AdministrationSequence::new(body.last_administration_sequence)
            .ok_or_else(DurableCodecError::corrupt)?;
    let predecessor = if body.predecessor_segment_hash.is_empty() {
        None
    } else {
        Some(CommandSegmentDigestV1::from_bytes(fixed(
            body.predecessor_segment_hash,
        )?))
    };
    let commands = body
        .commands
        .into_iter()
        .map(capsule_v4_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let segment = storage_result(StoredCommandSegmentV1::new(
        DatabaseId::from_bytes(fixed(body.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        body.history_incarnation,
        predecessor,
        commands,
        manifest_from_proto(require(body.manifest)?)?,
        digest,
    ))?;
    if segment.first_commit_sequence() != first
        || segment.last_commit_sequence() != last
        || segment.first_administration_sequence() != first_administration
        || segment.last_administration_sequence() != last_administration
    {
        return Err(DurableCodecError::corrupt());
    }
    Ok(segment)
}

fn segment_from_body_v4(
    body: wire::StoredCommandSegmentBodyV4,
    digest: CommandSegmentDigestV1,
) -> Result<StoredCommandSegmentV1, DurableCodecError> {
    if component_digest(SEGMENT_DIGEST_LABEL, &body.encode_to_vec()) != digest {
        return Err(DurableCodecError::corrupt());
    }
    let first =
        CommitSequence::new(body.first_commit_sequence).ok_or_else(DurableCodecError::corrupt)?;
    let last =
        CommitSequence::new(body.last_commit_sequence).ok_or_else(DurableCodecError::corrupt)?;
    let first_administration =
        riffdb_types::AdministrationSequence::new(body.first_administration_sequence)
            .ok_or_else(DurableCodecError::corrupt)?;
    let last_administration =
        riffdb_types::AdministrationSequence::new(body.last_administration_sequence)
            .ok_or_else(DurableCodecError::corrupt)?;
    let predecessor = if body.predecessor_segment_hash.is_empty() {
        None
    } else {
        Some(CommandSegmentDigestV1::from_bytes(fixed(
            body.predecessor_segment_hash,
        )?))
    };
    let commands = body
        .commands
        .into_iter()
        .map(capsule_v5_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let segment = storage_result(StoredCommandSegmentV1::new(
        DatabaseId::from_bytes(fixed(body.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        body.history_incarnation,
        predecessor,
        commands,
        manifest_from_proto(require(body.manifest)?)?,
        digest,
    ))?;
    if segment.first_commit_sequence() != first
        || segment.last_commit_sequence() != last
        || segment.first_administration_sequence() != first_administration
        || segment.last_administration_sequence() != last_administration
    {
        return Err(DurableCodecError::corrupt());
    }
    Ok(segment)
}

fn segment_from_body_v5(
    body: wire::StoredCommandSegmentBodyV5,
    digest: CommandSegmentDigestV1,
) -> Result<StoredCommandSegmentV1, DurableCodecError> {
    if component_digest(SEGMENT_DIGEST_LABEL, &body.encode_to_vec()) != digest {
        return Err(DurableCodecError::corrupt());
    }
    let first =
        CommitSequence::new(body.first_commit_sequence).ok_or_else(DurableCodecError::corrupt)?;
    let last =
        CommitSequence::new(body.last_commit_sequence).ok_or_else(DurableCodecError::corrupt)?;
    let first_administration =
        riffdb_types::AdministrationSequence::new(body.first_administration_sequence)
            .ok_or_else(DurableCodecError::corrupt)?;
    let last_administration =
        riffdb_types::AdministrationSequence::new(body.last_administration_sequence)
            .ok_or_else(DurableCodecError::corrupt)?;
    let predecessor = if body.predecessor_segment_hash.is_empty() {
        None
    } else {
        Some(CommandSegmentDigestV1::from_bytes(fixed(
            body.predecessor_segment_hash,
        )?))
    };
    let commands = body
        .commands
        .into_iter()
        .map(capsule_v6_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let segment = storage_result(StoredCommandSegmentV1::new(
        DatabaseId::from_bytes(fixed(body.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        body.history_incarnation,
        predecessor,
        commands,
        manifest_from_proto(require(body.manifest)?)?,
        digest,
    ))?;
    if segment.first_commit_sequence() != first
        || segment.last_commit_sequence() != last
        || segment.first_administration_sequence() != first_administration
        || segment.last_administration_sequence() != last_administration
    {
        return Err(DurableCodecError::corrupt());
    }
    Ok(segment)
}

fn append_length_delimited_field(
    output: &mut Vec<u8>,
    key: u8,
    bytes: &[u8],
) -> Result<(), DurableCodecError> {
    let len = u64::try_from(bytes.len()).map_err(|_| DurableCodecError::invariant())?;
    output.push(key);
    prost::encoding::encode_varint(len, output);
    output.extend_from_slice(bytes);
    Ok(())
}

fn append_varint_field(output: &mut Vec<u8>, key: u8, value: u64) {
    if value == 0 {
        return;
    }
    output.push(key);
    prost::encoding::encode_varint(value, output);
}

fn append_manifest_entry(
    output: &mut Vec<u8>,
    value: &CommandDerivedIndexManifestEntryV1,
) -> Result<(), DurableCodecError> {
    let mut entry = Vec::with_capacity(value.exact_key().len().saturating_add(24));
    append_varint_field(&mut entry, 0x08, kind_to_proto(value.kind()) as u64);
    append_varint_field(&mut entry, 0x10, member_to_proto(value.member()) as u64);
    append_length_delimited_field(&mut entry, 0x1a, value.exact_key())?;
    append_varint_field(&mut entry, 0x20, u64::from(value.command_ordinal()));
    append_varint_field(&mut entry, 0x28, u64::from(value.member_ordinal()));
    append_varint_field(
        &mut entry,
        0x30,
        value.segment_first_commit_sequence().get(),
    );
    append_length_delimited_field(output, 0x0a, &entry)
}

fn manifest_bytes_for_seal(value: &CommandSegmentManifestV1) -> Result<Vec<u8>, DurableCodecError> {
    let mut manifest = Vec::new();
    for entry in value.entries() {
        append_manifest_entry(&mut manifest, entry)?;
    }
    let manifest_digest = component_digest(MANIFEST_DIGEST_LABEL, &manifest);
    append_length_delimited_field(&mut manifest, 0x12, manifest_digest.as_bytes())?;
    Ok(manifest)
}

/// Streams the canonical body while calculating and inserting the nested
/// manifest digest. No complete V1 or V2 command capsule wire graph is
/// constructed; at most one bounded nested generated message and one manifest
/// entry are resident at a time. The independent full-Prost encoder remains
/// the validation oracle for this durable byte stream.
fn segment_body_v3_bytes_for_seal(
    value: &StoredCommandSegmentV1,
) -> Result<Vec<u8>, DurableCodecError> {
    let manifest = manifest_bytes_for_seal(value.manifest())?;
    let mut body = Vec::new();
    append_length_delimited_field(&mut body, 0x0a, value.database_id().as_bytes())?;
    append_varint_field(&mut body, 0x10, value.history_incarnation());
    if let Some(predecessor) = value.predecessor_segment_digest() {
        append_length_delimited_field(&mut body, 0x1a, predecessor.as_bytes())?;
    }
    append_varint_field(&mut body, 0x20, value.first_commit_sequence().get());
    append_varint_field(&mut body, 0x28, value.last_commit_sequence().get());
    append_varint_field(&mut body, 0x30, value.first_administration_sequence().get());
    append_varint_field(&mut body, 0x38, value.last_administration_sequence().get());
    for command in value.commands() {
        let command = capsule_v4_bytes_for_seal(command)?;
        append_length_delimited_field(&mut body, 0x42, &command)?;
    }
    append_length_delimited_field(&mut body, 0x4a, &manifest)?;
    Ok(body)
}

fn segment_body_bytes_with_prepared_capsules(
    value: &StoredCommandSegmentV1,
    prepared: &[PreparedCommandSegmentCapsuleV1],
) -> Result<(Vec<u8>, CommandCapsuleWireVersionV1), DurableCodecError> {
    let wire_version = segment_wire_version(value);
    if prepared.len() != value.commands().len() {
        return Err(DurableCodecError::invariant());
    }
    for (command, prepared) in value.commands().iter().zip(prepared) {
        if prepared.commit_sequence != command.commit_sequence()
            || prepared.wire_version != wire_version
        {
            return Err(DurableCodecError::invariant());
        }
    }

    let manifest = manifest_bytes_for_seal(value.manifest())?;
    let mut body = Vec::new();
    append_length_delimited_field(&mut body, 0x0a, value.database_id().as_bytes())?;
    append_varint_field(&mut body, 0x10, value.history_incarnation());
    if let Some(predecessor) = value.predecessor_segment_digest() {
        append_length_delimited_field(&mut body, 0x1a, predecessor.as_bytes())?;
    }
    append_varint_field(&mut body, 0x20, value.first_commit_sequence().get());
    append_varint_field(&mut body, 0x28, value.last_commit_sequence().get());
    append_varint_field(&mut body, 0x30, value.first_administration_sequence().get());
    append_varint_field(&mut body, 0x38, value.last_administration_sequence().get());
    for command in prepared {
        append_length_delimited_field(&mut body, 0x42, &command.bytes)?;
    }
    append_length_delimited_field(&mut body, 0x4a, &manifest)?;
    Ok((body, wire_version))
}

/// Computes the canonical digest for a structurally checked segment.
#[must_use]
pub fn command_segment_digest_v1(value: &StoredCommandSegmentV1) -> CommandSegmentDigestV1 {
    let body = match segment_wire_version(value) {
        CommandCapsuleWireVersionV1::V4 => segment_body_v3_to_proto(value).encode_to_vec(),
        CommandCapsuleWireVersionV1::V5 => segment_body_v4_to_proto(value).encode_to_vec(),
        CommandCapsuleWireVersionV1::V6 => segment_body_v5_to_proto(value).encode_to_vec(),
    };
    component_digest(SEGMENT_DIGEST_LABEL, &body)
}

fn raw_segment_envelope(
    body_bytes: &[u8],
    digest: CommandSegmentDigestV1,
    wire_version: CommandCapsuleWireVersionV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let body_len = u64::try_from(body_bytes.len()).map_err(|_| DurableCodecError::invariant())?;
    let digest_len =
        u64::try_from(digest.as_bytes().len()).map_err(|_| DurableCodecError::invariant())?;
    let mut payload = Vec::with_capacity(
        2_usize
            .saturating_add(prost::encoding::encoded_len_varint(body_len))
            .saturating_add(body_bytes.len())
            .saturating_add(prost::encoding::encoded_len_varint(digest_len))
            .saturating_add(digest.as_bytes().len()),
    );
    append_length_delimited_field(&mut payload, 0x0a, body_bytes)?;
    append_length_delimited_field(&mut payload, 0x12, digest.as_bytes())?;
    match wire_version {
        CommandCapsuleWireVersionV1::V4 => encode_structurally_proven_message::<
            wire::StoredCommandSegmentV3,
        >(COMMAND_SEGMENT_V3, &payload),
        CommandCapsuleWireVersionV1::V5 => encode_structurally_proven_message::<
            wire::StoredCommandSegmentV4,
        >(COMMAND_SEGMENT_V4, &payload),
        CommandCapsuleWireVersionV1::V6 => encode_structurally_proven_message::<
            wire::StoredCommandSegmentV5,
        >(COMMAND_SEGMENT_V5, &payload),
    }
}

fn command_segment_envelope_with_metrics(
    body_bytes: &[u8],
    digest: CommandSegmentDigestV1,
    wire_version: CommandCapsuleWireVersionV1,
) -> Result<(CanonicalStoredEnvelopeV1, usize), DurableCodecError> {
    let raw = raw_segment_envelope(body_bytes, digest, wire_version)?;
    let raw_bytes = raw.as_bytes().len();
    Ok((raw, raw_bytes))
}

/// Fixed-cardinality byte evidence produced while sealing one command segment.
///
/// This contains no key, value, symbol, principal, or codec control. The live
/// writer uses it only to account for the complete raw-equivalent frame and
/// the selected immutable bytes without reconstructing the semantic graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandSegmentEncodingMetricsV1 {
    raw_envelope_bytes: usize,
    selected_envelope_bytes: usize,
}

impl CommandSegmentEncodingMetricsV1 {
    /// Complete current-record bytes before the deterministic compact choice.
    #[must_use]
    pub const fn raw_envelope_bytes(self) -> usize {
        self.raw_envelope_bytes
    }

    /// Complete selected current-or-successor record bytes.
    #[must_use]
    pub const fn selected_envelope_bytes(self) -> usize {
        self.selected_envelope_bytes
    }
}

/// Seals one segment and also returns bounded representation-size evidence.
///
/// The metric is derived from the same two complete envelopes used by the
/// canonical selection rule; it does not trigger a second body construction.
pub fn seal_and_encode_command_segment_with_metrics_v1(
    value: StoredCommandSegmentV1,
) -> Result<
    (
        StoredCommandSegmentV1,
        CanonicalStoredEnvelopeV1,
        CommandSegmentEncodingMetricsV1,
    ),
    DurableCodecError,
> {
    let wire_version = segment_wire_version(&value);
    let body_bytes = match wire_version {
        CommandCapsuleWireVersionV1::V4 => segment_body_v3_bytes_for_seal(&value)?,
        CommandCapsuleWireVersionV1::V5 => segment_body_v4_to_proto(&value).encode_to_vec(),
        CommandCapsuleWireVersionV1::V6 => segment_body_v5_to_proto(&value).encode_to_vec(),
    };
    let digest = component_digest(SEGMENT_DIGEST_LABEL, &body_bytes);
    let value = value.with_segment_digest(digest);
    let (encoded, raw_envelope_bytes) =
        command_segment_envelope_with_metrics(&body_bytes, digest, wire_version)?;
    let metrics = CommandSegmentEncodingMetricsV1 {
        raw_envelope_bytes,
        selected_envelope_bytes: encoded.as_bytes().len(),
    };
    Ok((value, encoded, metrics))
}

/// Seals a segment from checked, already canonical command-member bytes.
///
/// Each prepared member is joined to the retained semantic graph by exact
/// ordinal, commit sequence, and the segment-wide durable variant. A stale,
/// reordered, incomplete, or wrong-variant preparation therefore fails before
/// any durable bytes can escape.
pub fn seal_and_encode_command_segment_with_prepared_capsules_v1(
    value: StoredCommandSegmentV1,
    prepared: Vec<PreparedCommandSegmentCapsuleV1>,
) -> Result<
    (
        StoredCommandSegmentV1,
        CanonicalStoredEnvelopeV1,
        CommandSegmentEncodingMetricsV1,
    ),
    DurableCodecError,
> {
    let (body_bytes, wire_version) = segment_body_bytes_with_prepared_capsules(&value, &prepared)?;
    let digest = component_digest(SEGMENT_DIGEST_LABEL, &body_bytes);
    let value = value.with_segment_digest(digest);
    let (encoded, raw_envelope_bytes) =
        command_segment_envelope_with_metrics(&body_bytes, digest, wire_version)?;
    let metrics = CommandSegmentEncodingMetricsV1 {
        raw_envelope_bytes,
        selected_envelope_bytes: encoded.as_bytes().len(),
    };
    Ok((value, encoded, metrics))
}

/// Seals one structurally checked segment and returns its canonical bytes from
/// the same constructed wire body used to calculate the segment digest.
///
/// This is the authoritative write-path operation. It avoids rebuilding and
/// re-encoding the complete command graph merely to verify a digest that this
/// call has just calculated; [`encode_command_segment_v1`] remains the
/// independent validation path for already sealed values.
pub fn seal_and_encode_command_segment_v1(
    value: StoredCommandSegmentV1,
) -> Result<(StoredCommandSegmentV1, CanonicalStoredEnvelopeV1), DurableCodecError> {
    let (value, encoded, _) = seal_and_encode_command_segment_with_metrics_v1(value)?;
    Ok((value, encoded))
}

/// Encodes one segment after proving its supplied digest matches its canonical body.
pub fn encode_command_segment_v1(
    value: &StoredCommandSegmentV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    match segment_wire_version(value) {
        CommandCapsuleWireVersionV1::V4 => {
            let body = segment_body_v3_to_proto(value);
            let digest = component_digest(SEGMENT_DIGEST_LABEL, &body.encode_to_vec());
            if digest != value.segment_digest() {
                return Err(DurableCodecError::corrupt());
            }
            encode_message(
                COMMAND_SEGMENT_V3,
                &wire::StoredCommandSegmentV3 {
                    body: Some(body),
                    segment_digest: digest.as_bytes().to_vec(),
                },
            )
        }
        CommandCapsuleWireVersionV1::V5 => {
            let body = segment_body_v4_to_proto(value);
            let digest = component_digest(SEGMENT_DIGEST_LABEL, &body.encode_to_vec());
            if digest != value.segment_digest() {
                return Err(DurableCodecError::corrupt());
            }
            encode_message(
                COMMAND_SEGMENT_V4,
                &wire::StoredCommandSegmentV4 {
                    body: Some(body),
                    segment_digest: digest.as_bytes().to_vec(),
                },
            )
        }
        CommandCapsuleWireVersionV1::V6 => {
            let body = segment_body_v5_to_proto(value);
            let digest = component_digest(SEGMENT_DIGEST_LABEL, &body.encode_to_vec());
            if digest != value.segment_digest() {
                return Err(DurableCodecError::corrupt());
            }
            encode_message(
                COMMAND_SEGMENT_V5,
                &wire::StoredCommandSegmentV5 {
                    body: Some(body),
                    segment_digest: digest.as_bytes().to_vec(),
                },
            )
        }
    }
}

/// Decodes and proves one complete segment, its manifests, and hash-chain member.
pub fn decode_command_segment_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCommandSegmentV1>, DurableCodecError> {
    match decode_record_variant_chain(
        encoded,
        &[
            COMMAND_SEGMENT_V5,
            COMMAND_SEGMENT_V4,
            COMMAND_SEGMENT_V3,
            COMMAND_SEGMENT_V2,
            COMMAND_SEGMENT_V1,
        ],
    )? {
        0 => decode_message::<wire::StoredCommandSegmentV5, _, _>(
            COMMAND_SEGMENT_V5,
            encoded,
            |value| {
                let body = require(value.body)?;
                let digest = CommandSegmentDigestV1::from_bytes(fixed(value.segment_digest)?);
                segment_from_body_v5(body, digest)
            },
        ),
        1 => decode_message::<wire::StoredCommandSegmentV4, _, _>(
            COMMAND_SEGMENT_V4,
            encoded,
            |value| {
                let body = require(value.body)?;
                let digest = CommandSegmentDigestV1::from_bytes(fixed(value.segment_digest)?);
                segment_from_body_v4(body, digest)
            },
        ),
        2 => decode_message::<wire::StoredCommandSegmentV3, _, _>(
            COMMAND_SEGMENT_V3,
            encoded,
            |value| {
                let body = require(value.body)?;
                let digest = CommandSegmentDigestV1::from_bytes(fixed(value.segment_digest)?);
                segment_from_body_v3(body, digest)
            },
        ),
        3 => decode_message::<wire::StoredCommandSegmentV2, _, _>(
            COMMAND_SEGMENT_V2,
            encoded,
            |value| {
                let body = require(value.body)?;
                let digest = CommandSegmentDigestV1::from_bytes(fixed(value.segment_digest)?);
                if component_digest(SEGMENT_DIGEST_LABEL, &body.encode_to_vec()) != digest {
                    return Err(DurableCodecError::corrupt());
                }
                let first = CommitSequence::new(body.first_commit_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?;
                let last = CommitSequence::new(body.last_commit_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?;
                let first_administration =
                    riffdb_types::AdministrationSequence::new(body.first_administration_sequence)
                        .ok_or_else(DurableCodecError::corrupt)?;
                let last_administration =
                    riffdb_types::AdministrationSequence::new(body.last_administration_sequence)
                        .ok_or_else(DurableCodecError::corrupt)?;
                let predecessor = if body.predecessor_segment_hash.is_empty() {
                    None
                } else {
                    Some(CommandSegmentDigestV1::from_bytes(fixed(
                        body.predecessor_segment_hash,
                    )?))
                };
                let commands = body
                    .commands
                    .into_iter()
                    .map(capsule_v3_from_proto)
                    .collect::<Result<Vec<_>, _>>()?;
                let segment = storage_result(StoredCommandSegmentV1::new(
                    DatabaseId::from_bytes(fixed(body.database_id)?)
                        .map_err(|_| DurableCodecError::corrupt())?,
                    body.history_incarnation,
                    predecessor,
                    commands,
                    manifest_from_proto(require(body.manifest)?)?,
                    digest,
                ))?;
                if segment.first_commit_sequence() != first
                    || segment.last_commit_sequence() != last
                    || segment.first_administration_sequence() != first_administration
                    || segment.last_administration_sequence() != last_administration
                {
                    return Err(DurableCodecError::corrupt());
                }
                Ok(segment)
            },
        ),
        4 => decode_message::<wire::StoredCommandSegmentV1, _, _>(
            COMMAND_SEGMENT_V1,
            encoded,
            |value| {
                let body = require(value.body)?;
                let digest = CommandSegmentDigestV1::from_bytes(fixed(value.segment_digest)?);
                if component_digest(SEGMENT_DIGEST_LABEL, &body.encode_to_vec()) != digest {
                    return Err(DurableCodecError::corrupt());
                }
                let first = CommitSequence::new(body.first_commit_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?;
                let last = CommitSequence::new(body.last_commit_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?;
                let first_administration =
                    riffdb_types::AdministrationSequence::new(body.first_administration_sequence)
                        .ok_or_else(DurableCodecError::corrupt)?;
                let last_administration =
                    riffdb_types::AdministrationSequence::new(body.last_administration_sequence)
                        .ok_or_else(DurableCodecError::corrupt)?;
                let predecessor = if body.predecessor_segment_hash.is_empty() {
                    None
                } else {
                    Some(CommandSegmentDigestV1::from_bytes(fixed(
                        body.predecessor_segment_hash,
                    )?))
                };
                let commands = body
                    .commands
                    .into_iter()
                    .map(capsule_v2_from_proto)
                    .collect::<Result<Vec<_>, _>>()?;
                let segment = storage_result(StoredCommandSegmentV1::new(
                    DatabaseId::from_bytes(fixed(body.database_id)?)
                        .map_err(|_| DurableCodecError::corrupt())?,
                    body.history_incarnation,
                    predecessor,
                    commands,
                    manifest_from_proto(require(body.manifest)?)?,
                    digest,
                ))?;
                if segment.first_commit_sequence() != first
                    || segment.last_commit_sequence() != last
                    || segment.first_administration_sequence() != first_administration
                    || segment.last_administration_sequence() != last_administration
                {
                    return Err(DurableCodecError::corrupt());
                }
                Ok(segment)
            },
        ),
        _ => unreachable!("closed durable record variant index"),
    }
}

fn checkpoint_to_proto(
    value: &StoredCommandDerivedIndexCheckpointV1,
) -> wire::StoredCommandDerivedIndexCheckpointV1 {
    wire::StoredCommandDerivedIndexCheckpointV1 {
        database_id: value.database_id().as_bytes().to_vec(),
        history_incarnation: value.history_incarnation(),
        registry_digest: value.registry_digest().to_vec(),
        segment_frontier: value.segment_frontier().get(),
        segment_root_digest: value.segment_root_digest().as_bytes().to_vec(),
        entries: value
            .entries()
            .iter()
            .map(manifest_entry_to_proto)
            .collect(),
        checkpoint_digest: Vec::new(),
    }
}

/// Computes the canonical digest for a structurally checked derived checkpoint.
#[must_use]
pub fn command_derived_index_checkpoint_digest_v1(
    value: &StoredCommandDerivedIndexCheckpointV1,
) -> CommandSegmentDigestV1 {
    component_digest(
        CHECKPOINT_DIGEST_LABEL,
        &checkpoint_to_proto(value).encode_to_vec(),
    )
}

/// Encodes a derived checkpoint after proving its digest binding.
pub fn encode_command_derived_index_checkpoint_v1(
    value: &StoredCommandDerivedIndexCheckpointV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let mut wire = checkpoint_to_proto(value);
    let digest = component_digest(CHECKPOINT_DIGEST_LABEL, &wire.encode_to_vec());
    if digest != value.checkpoint_digest() {
        return Err(DurableCodecError::corrupt());
    }
    wire.checkpoint_digest = digest.as_bytes().to_vec();
    encode_message(COMMAND_DERIVED_INDEX_CHECKPOINT_V1, &wire)
}

/// Decodes and verifies one replaceable exact-index checkpoint.
pub fn decode_command_derived_index_checkpoint_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCommandDerivedIndexCheckpointV1>, DurableCodecError> {
    decode_message::<wire::StoredCommandDerivedIndexCheckpointV1, _, _>(
        COMMAND_DERIVED_INDEX_CHECKPOINT_V1,
        encoded,
        |mut value| {
            let supplied =
                CommandSegmentDigestV1::from_bytes(fixed(value.checkpoint_digest.clone())?);
            value.checkpoint_digest.clear();
            if component_digest(CHECKPOINT_DIGEST_LABEL, &value.encode_to_vec()) != supplied {
                return Err(DurableCodecError::corrupt());
            }
            storage_result(StoredCommandDerivedIndexCheckpointV1::new(
                DatabaseId::from_bytes(fixed(value.database_id)?)
                    .map_err(|_| DurableCodecError::corrupt())?,
                value.history_incarnation,
                fixed(value.registry_digest)?,
                CommitSequence::new(value.segment_frontier)
                    .ok_or_else(DurableCodecError::corrupt)?,
                CommandSegmentDigestV1::from_bytes(fixed(value.segment_root_digest)?),
                value
                    .entries
                    .into_iter()
                    .map(manifest_entry_from_proto)
                    .collect::<Result<Vec<_>, _>>()?,
                supplied,
            ))
        },
    )
}
