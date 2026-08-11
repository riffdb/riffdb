//! Checked V1 durable validated-prefix checkpoint mappings.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{DatabaseId, SchemaHash};

use crate::{
    EncodedPageItem, EntityChainFingerprint, EntityTransitionFingerprint,
    StoredValidatedPrefixCheckpointV1, StoredValidatedPrefixCheckpointV2,
    ValidatedPrefixCheckpointHash, ValidatedPrefixCheckpointV2Hash,
    ValidatedPrefixEntityTransitionCounts, ValidatedPrefixRetainedSnapshot,
    ValidatedPrefixSequenceCounts,
};

use super::{CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message, fixed};

const CHECKPOINT: &str = "riffdb.storage.v1.StoredValidatedPrefixCheckpointV1";
const CHECKPOINT_V2: &str = "riffdb.storage.v1.StoredValidatedPrefixCheckpointV2";

fn checkpoint_to_proto(
    value: &StoredValidatedPrefixCheckpointV1,
) -> wire::StoredValidatedPrefixCheckpointV1 {
    let counts = value.counts();
    let retained = value.retained();
    wire::StoredValidatedPrefixCheckpointV1 {
        database_id: value.database_id().as_bytes().to_vec(),
        history_incarnation: value.history_incarnation(),
        registry_digest: value.registry_digest().as_bytes().to_vec(),
        checkpoint_commit_sequence: value.checkpoint_commit_sequence(),
        audit_sequence_bound: value.audit_sequence_bound(),
        commits_count: counts.commits_count,
        events_count: counts.events_count,
        event_routes_count: counts.event_routes_count,
        outbox_count: counts.outbox_count,
        outbox_status_count: counts.outbox_status_count,
        idempotency_count: counts.idempotency_count,
        audit_count: counts.audit_count,
        audit_by_request_count: counts.audit_by_request_count,
        entity_chain_fingerprint: value.entity_chain_fingerprint().as_bytes().to_vec(),
        next_application_sequence: retained.next_application_sequence,
        application_sequence_exhausted: retained.application_sequence_exhausted,
        next_administration_sequence: retained.next_administration_sequence,
        administration_sequence_exhausted: retained.administration_sequence_exhausted,
        previous_checkpoint_hash: value
            .previous_checkpoint_hash()
            .map(|hash| hash.as_bytes().to_vec()),
        checkpoint_hash: value.checkpoint_hash().as_bytes().to_vec(),
        retention_watermark_sequence: value.retention_watermark_sequence(),
    }
}

fn checkpoint_from_proto(
    value: wire::StoredValidatedPrefixCheckpointV1,
) -> Result<StoredValidatedPrefixCheckpointV1, DurableCodecError> {
    StoredValidatedPrefixCheckpointV1::from_stored_parts(
        DatabaseId::from_bytes(fixed(value.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        value.history_incarnation,
        SchemaHash::from_bytes(fixed(value.registry_digest)?),
        value.checkpoint_commit_sequence,
        value.audit_sequence_bound,
        ValidatedPrefixSequenceCounts {
            commits_count: value.commits_count,
            events_count: value.events_count,
            event_routes_count: value.event_routes_count,
            outbox_count: value.outbox_count,
            outbox_status_count: value.outbox_status_count,
            idempotency_count: value.idempotency_count,
            audit_count: value.audit_count,
            audit_by_request_count: value.audit_by_request_count,
        },
        EntityChainFingerprint::from_bytes(fixed(value.entity_chain_fingerprint)?),
        ValidatedPrefixRetainedSnapshot {
            next_application_sequence: value.next_application_sequence,
            application_sequence_exhausted: value.application_sequence_exhausted,
            next_administration_sequence: value.next_administration_sequence,
            administration_sequence_exhausted: value.administration_sequence_exhausted,
        },
        value
            .previous_checkpoint_hash
            .map(|hash| fixed(hash).map(ValidatedPrefixCheckpointHash::from_bytes))
            .transpose()?,
        ValidatedPrefixCheckpointHash::from_bytes(fixed(value.checkpoint_hash)?),
        value.retention_watermark_sequence,
    )
    .map_err(DurableCodecError::from_storage_value)
}

/// Decodes one canonical validated-prefix checkpoint envelope.
pub fn decode_validated_prefix_checkpoint_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredValidatedPrefixCheckpointV1>, DurableCodecError> {
    decode_message::<wire::StoredValidatedPrefixCheckpointV1, _, _>(
        CHECKPOINT,
        encoded,
        checkpoint_from_proto,
    )
}

/// Encodes one current delete-aware validated-prefix checkpoint.
pub fn encode_validated_prefix_checkpoint_v2(
    value: &StoredValidatedPrefixCheckpointV2,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if value.computed_hash() != value.checkpoint_hash() {
        return Err(DurableCodecError::corrupt());
    }
    let counts = value.entity_counts();
    encode_message(
        CHECKPOINT_V2,
        &wire::StoredValidatedPrefixCheckpointV2 {
            base: Some(checkpoint_to_proto(value.base())),
            live_entity_count: counts.live_entity_count,
            deleted_entity_count: counts.deleted_entity_count,
            entity_transition_count: counts.entity_transition_count,
            entity_transition_fingerprint: value
                .entity_transition_fingerprint()
                .as_bytes()
                .to_vec(),
            checkpoint_hash: value.checkpoint_hash().as_bytes().to_vec(),
        },
    )
}

/// Decodes and verifies one current delete-aware validated-prefix checkpoint.
pub fn decode_validated_prefix_checkpoint_v2(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredValidatedPrefixCheckpointV2>, DurableCodecError> {
    decode_message::<wire::StoredValidatedPrefixCheckpointV2, _, _>(
        CHECKPOINT_V2,
        encoded,
        |value| {
            StoredValidatedPrefixCheckpointV2::from_stored_parts(
                checkpoint_from_proto(value.base.ok_or_else(DurableCodecError::corrupt)?)?,
                ValidatedPrefixEntityTransitionCounts {
                    live_entity_count: value.live_entity_count,
                    deleted_entity_count: value.deleted_entity_count,
                    entity_transition_count: value.entity_transition_count,
                },
                EntityTransitionFingerprint::from_bytes(fixed(
                    value.entity_transition_fingerprint,
                )?),
                ValidatedPrefixCheckpointV2Hash::from_bytes(fixed(value.checkpoint_hash)?),
            )
            .map_err(DurableCodecError::from_storage_value)
        },
    )
}
