//! Checked durable mappings for delete-aware entity transitions and rotation.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    AdministrationSequence, CommitSequence, DatabaseId, DualFrontier, EntityRecordHash,
    EntityTransitionHash, EntityVersion,
};

use crate::{
    ChangelogV2RotationReceipt, CommittedEntityTransitionV1, EncodedPageItem, EntityChainHeadV1,
    EntityChainStateV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message,
    entity_target_from_proto, entity_target_to_proto, fixed, require, storage_result,
};

const ENTITY_CHAIN_HEAD_V1: &str = "riffdb.storage.v1.StoredEntityChainHeadV1";
const CHANGELOG_V2_ROTATION_RECEIPT_V1: &str =
    "riffdb.storage.v1.StoredChangelogV2RotationReceiptV1";

pub(super) fn entity_chain_state_to_proto(
    value: EntityChainStateV1,
) -> wire::StoredEntityChainStateV1 {
    match value {
        EntityChainStateV1::NeverExisted => wire::StoredEntityChainStateV1 {
            kind: wire::EntityChainStateKindV1::EntityChainStateKindNeverExisted as i32,
            entity_version: None,
            value_hash: Vec::new(),
        },
        EntityChainStateV1::Live {
            version,
            value_hash,
        } => wire::StoredEntityChainStateV1 {
            kind: wire::EntityChainStateKindV1::EntityChainStateKindLive as i32,
            entity_version: Some(version.get()),
            value_hash: value_hash.as_bytes().to_vec(),
        },
        EntityChainStateV1::Deleted => wire::StoredEntityChainStateV1 {
            kind: wire::EntityChainStateKindV1::EntityChainStateKindDeleted as i32,
            entity_version: None,
            value_hash: Vec::new(),
        },
    }
}

pub(super) fn entity_chain_state_from_proto(
    value: wire::StoredEntityChainStateV1,
) -> Result<EntityChainStateV1, DurableCodecError> {
    let kind = wire::EntityChainStateKindV1::try_from(value.kind)
        .map_err(|_| DurableCodecError::corrupt())?;
    match kind {
        wire::EntityChainStateKindV1::EntityChainStateKindNeverExisted
            if value.entity_version.is_none() && value.value_hash.is_empty() =>
        {
            Ok(EntityChainStateV1::NeverExisted)
        }
        wire::EntityChainStateKindV1::EntityChainStateKindLive => Ok(EntityChainStateV1::Live {
            version: EntityVersion::new(
                value
                    .entity_version
                    .ok_or_else(DurableCodecError::corrupt)?,
            )
            .ok_or_else(DurableCodecError::corrupt)?,
            value_hash: EntityRecordHash::from_bytes(fixed(value.value_hash)?),
        }),
        wire::EntityChainStateKindV1::EntityChainStateKindDeleted
            if value.entity_version.is_none() && value.value_hash.is_empty() =>
        {
            Ok(EntityChainStateV1::Deleted)
        }
        wire::EntityChainStateKindV1::EntityChainStateKindUnspecified
        | wire::EntityChainStateKindV1::EntityChainStateKindNeverExisted
        | wire::EntityChainStateKindV1::EntityChainStateKindDeleted => {
            Err(DurableCodecError::corrupt())
        }
    }
}

pub(super) fn entity_transition_to_proto(
    value: &CommittedEntityTransitionV1,
) -> wire::StoredCommittedEntityTransitionV1 {
    wire::StoredCommittedEntityTransitionV1 {
        command_sequence: value.command_sequence().get(),
        mutation_ordinal: value.mutation_ordinal(),
        target: Some(entity_target_to_proto(value.target())),
        prior_state: Some(entity_chain_state_to_proto(value.prior_state())),
        prior_chain_revision: value.prior_chain_revision(),
        prior_transition_hash: value
            .prior_transition_hash()
            .map(|hash| hash.as_bytes().to_vec()),
        next_state: Some(entity_chain_state_to_proto(value.next_state())),
        transition_hash: value.transition_hash().as_bytes().to_vec(),
    }
}

pub(super) fn entity_transition_from_proto(
    value: wire::StoredCommittedEntityTransitionV1,
) -> Result<CommittedEntityTransitionV1, DurableCodecError> {
    storage_result(CommittedEntityTransitionV1::from_stored_parts(
        CommitSequence::new(value.command_sequence).ok_or_else(DurableCodecError::corrupt)?,
        value.mutation_ordinal,
        entity_target_from_proto(require(value.target)?)?,
        entity_chain_state_from_proto(require(value.prior_state)?)?,
        value.prior_chain_revision,
        value
            .prior_transition_hash
            .map(|hash| fixed(hash).map(EntityTransitionHash::from_bytes))
            .transpose()?,
        entity_chain_state_from_proto(require(value.next_state)?)?,
        EntityTransitionHash::from_bytes(fixed(value.transition_hash)?),
    ))
}

fn entity_chain_head_to_proto(value: &EntityChainHeadV1) -> wire::StoredEntityChainHeadV1 {
    wire::StoredEntityChainHeadV1 {
        target: Some(entity_target_to_proto(value.target())),
        chain_revision: value.chain_revision(),
        state: Some(entity_chain_state_to_proto(value.state())),
        last_command_sequence: value.last_command_sequence().get(),
        last_transition_hash: value.last_transition_hash().as_bytes().to_vec(),
    }
}

fn entity_chain_head_from_proto(
    value: wire::StoredEntityChainHeadV1,
) -> Result<EntityChainHeadV1, DurableCodecError> {
    storage_result(EntityChainHeadV1::from_stored_parts(
        entity_target_from_proto(require(value.target)?)?,
        value.chain_revision,
        entity_chain_state_from_proto(require(value.state)?)?,
        CommitSequence::new(value.last_command_sequence).ok_or_else(DurableCodecError::corrupt)?,
        EntityTransitionHash::from_bytes(fixed(value.last_transition_hash)?),
    ))
}

/// Encodes one authoritative entity-chain head.
pub fn encode_entity_chain_head_v1(
    value: &EntityChainHeadV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(ENTITY_CHAIN_HEAD_V1, &entity_chain_head_to_proto(value))
}

/// Decodes one authoritative entity-chain head.
pub fn decode_entity_chain_head_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<EntityChainHeadV1>, DurableCodecError> {
    decode_message::<wire::StoredEntityChainHeadV1, _, _>(
        ENTITY_CHAIN_HEAD_V1,
        encoded,
        entity_chain_head_from_proto,
    )
}

fn rotation_receipt_to_proto(
    value: ChangelogV2RotationReceipt,
) -> wire::StoredChangelogV2RotationReceiptV1 {
    wire::StoredChangelogV2RotationReceiptV1 {
        database_id: value.database_id().as_bytes().to_vec(),
        history_incarnation: value.history_incarnation(),
        predecessor_application_frontier: value
            .predecessor()
            .application()
            .map(CommitSequence::get),
        predecessor_administration_frontier: value
            .predecessor()
            .administration()
            .map(AdministrationSequence::get),
        terminal_v1_frame_hash: value.v1_terminal_hash().to_vec(),
        v2_chain_anchor: value.v2_chain_anchor().to_vec(),
        receipt_hash: value.receipt_hash().to_vec(),
    }
}

fn rotation_receipt_from_proto(
    value: wire::StoredChangelogV2RotationReceiptV1,
) -> Result<ChangelogV2RotationReceipt, DurableCodecError> {
    let predecessor = DualFrontier::new(
        value
            .predecessor_application_frontier
            .map(|sequence| CommitSequence::new(sequence).ok_or_else(DurableCodecError::corrupt))
            .transpose()?,
        value
            .predecessor_administration_frontier
            .map(|sequence| {
                AdministrationSequence::new(sequence).ok_or_else(DurableCodecError::corrupt)
            })
            .transpose()?,
    );
    ChangelogV2RotationReceipt::from_stored_parts(
        DatabaseId::from_bytes(fixed(value.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        value.history_incarnation,
        predecessor,
        fixed(value.terminal_v1_frame_hash)?,
        fixed(value.v2_chain_anchor)?,
        fixed(value.receipt_hash)?,
    )
    .map_err(|_| DurableCodecError::corrupt())
}

/// Encodes the one durable V1-to-V2 rotation receipt.
pub fn encode_changelog_v2_rotation_receipt_v1(
    value: ChangelogV2RotationReceipt,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        CHANGELOG_V2_ROTATION_RECEIPT_V1,
        &rotation_receipt_to_proto(value),
    )
}

/// Decodes and rederives the V1-to-V2 rotation receipt.
pub fn decode_changelog_v2_rotation_receipt_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<ChangelogV2RotationReceipt>, DurableCodecError> {
    decode_message::<wire::StoredChangelogV2RotationReceiptV1, _, _>(
        CHANGELOG_V2_ROTATION_RECEIPT_V1,
        encoded,
        rotation_receipt_from_proto,
    )
}
