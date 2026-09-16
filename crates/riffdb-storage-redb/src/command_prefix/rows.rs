//! Intrinsic command-to-row checks. Receipt reduction and actual predecessor
//! checks are separate: matching a transaction's final bytes cannot prove an
//! intermediate entity image or its logical index epoch.

use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N, DurableCodecError,
    DurableCodecErrorKind, EncodedPageItem, EntityChainStateV1 as State, StoredCommandCapsuleV2,
    StoredCommandSegmentV1,
};

fn corrupt() -> DurableCodecError {
    DurableCodecError::new(DurableCodecErrorKind::CorruptData)
}

/// Storage-owned decoding also binds retained entity/epoch images to the
/// command facts. These checks intentionally do not assert catalog validity or
/// complete vector/secondary-index transition coverage.
pub(crate) fn decode_segment(
    bytes: &[u8],
) -> Result<EncodedPageItem<StoredCommandSegmentV1>, DurableCodecError> {
    let decoded = decode_segment_images(bytes)?;
    super::predecessor::validate_retained_commands(decoded.value().commands()).map_err(
        |error| {
            DurableCodecError::new(match error.kind() {
                riffdb_storage_api::StorageErrorKind::LimitExceeded => {
                    DurableCodecErrorKind::LimitExceeded
                }
                _ => DurableCodecErrorKind::CorruptData,
            })
        },
    )?;
    Ok(decoded)
}

/// Received groups join against a complete pinned predecessor after net
/// validation, so they do not also need the partial retained-segment pass.
pub(super) fn decode_segment_images(
    bytes: &[u8],
) -> Result<EncodedPageItem<StoredCommandSegmentV1>, DurableCodecError> {
    let decoded = riffdb_storage_api::decode_command_segment_v1(bytes)?;
    for command in decoded.value().commands() {
        validate_rows(command).map_err(nested_error)?;
    }
    Ok(decoded)
}

pub(crate) fn decode_capsule(
    bytes: &[u8],
) -> Result<EncodedPageItem<StoredCommandCapsuleV2>, DurableCodecError> {
    let decoded = riffdb_storage_api::decode_command_capsule_v2(bytes)?;
    validate_rows(decoded.value()).map_err(nested_error)?;
    Ok(decoded)
}

// Only the outer envelope may select the legacy decoder fallback. A wrong
// record type inside a known successor is corruption of that successor.
fn nested_error(error: DurableCodecError) -> DurableCodecError {
    match error.kind() {
        DurableCodecErrorKind::UnexpectedRecordType => corrupt(),
        _ => error,
    }
}

fn required<'a>(
    mutations: &'a [AuthoritativeMutationV3],
    namespace: N,
    key: &[u8],
) -> Result<&'a AuthoritativeMutationV3, DurableCodecError> {
    // Prefix construction already proved strict canonical order and uniqueness.
    let offset = mutations
        .binary_search_by(|row| (row.namespace(), row.key()).cmp(&(namespace, key)))
        .map_err(|_| corrupt())?;
    Ok(&mutations[offset])
}

pub(super) fn validate_rows(command: &StoredCommandCapsuleV2) -> Result<(), DurableCodecError> {
    let Some(prefix) = command.prefix_evidence() else {
        return Ok(());
    };
    super::secondary::validate_post_images(command)?;
    let mutations = prefix.mutations();
    let count = |namespace| {
        mutations
            .iter()
            .filter(|row| row.namespace() == namespace)
            .count()
    };
    let transitions = command.entity_transitions();
    if count(N::Entities) != transitions.len()
        || count(N::EntityChainHeads) != transitions.len()
        || count(N::IndexEpochs) != command.index_generation_transitions().len()
        || (transitions.is_empty() && !command.base().commit().entity_references().is_empty())
    {
        return Err(corrupt());
    }
    for transition in transitions {
        let key = crate::keys::encode_entity_key(transition.target().key());
        let entity = required(mutations, N::Entities, key)?;
        let head = required(mutations, N::EntityChainHeads, key)?;
        let prior_live = matches!(transition.prior_state(), State::Live { .. });
        if entity.expected_hash().is_some() != prior_live
            || head.expected_hash().is_some() != (transition.prior_chain_revision() != 0)
        {
            return Err(corrupt());
        }
        match (transition.next_state(), entity.value()) {
            (
                State::Live {
                    version,
                    value_hash,
                },
                Some(bytes),
            ) => {
                let decoded = riffdb_storage_api::decode_entity_record_v1(bytes)?;
                let record = decoded.value();
                if record.target() != transition.target()
                    || record.entity_version() != version
                    || riffdb_storage_api::derive_entity_record_hash_v1(record)
                        .map_err(|_| corrupt())?
                        != value_hash
                {
                    return Err(corrupt());
                }
            }
            (State::Deleted, None) => {}
            _ => return Err(corrupt()),
        }
        let decoded =
            riffdb_storage_api::decode_entity_chain_head_v1(head.value().ok_or_else(corrupt)?)?;
        let head = decoded.value();
        if head.target() != transition.target()
            || Some(head.chain_revision()) != transition.prior_chain_revision().checked_add(1)
            || head.state() != transition.next_state()
            || head.last_command_sequence() != command.commit_sequence()
            || head.last_transition_hash() != transition.transition_hash()
        {
            return Err(corrupt());
        }
    }
    for advance in command.index_generation_transitions() {
        let key = crate::keys::encode_partition_index_key(advance.target());
        let row = required(mutations, N::IndexEpochs, &key)?;
        if row.expected_hash().is_some()
            != matches!(
                advance.prior(),
                riffdb_storage_api::IndexEpochPosition::Value(_)
            )
            || riffdb_storage_api::decode_index_epoch_v1(row.value().ok_or_else(corrupt)?)?.value()
                != advance.post_image()
        {
            return Err(corrupt());
        }
    }
    // Pending authority may only be removed for this command's outcome identity.
    let identity = command
        .base()
        .outcome()
        .identity()
        .storage_key()
        .map_err(|_| corrupt())?;
    for row in mutations
        .iter()
        .filter(|row| row.namespace() == N::IdempotencyPending)
    {
        if row.value().is_some() || row.key() != crate::keys::encode_idempotency_key(&identity) {
            return Err(corrupt());
        }
    }
    Ok(())
}
