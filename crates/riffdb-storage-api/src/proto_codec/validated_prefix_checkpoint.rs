//! Checked V1 durable validated-prefix checkpoint mappings.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{DatabaseId, SchemaHash};

use crate::{
    EncodedPageItem, EntityChainFingerprint, StoredValidatedPrefixCheckpointV1,
    ValidatedPrefixCheckpointHash, ValidatedPrefixRetainedSnapshot, ValidatedPrefixSequenceCounts,
};

use super::{CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message, fixed};

const CHECKPOINT: &str = "riffdb.storage.v1.StoredValidatedPrefixCheckpointV1";

/// Encodes one canonical validated-prefix checkpoint envelope.
pub fn encode_validated_prefix_checkpoint_v1(
    value: &StoredValidatedPrefixCheckpointV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if value
        .computed_hash()
        .map_err(DurableCodecError::from_storage_value)?
        != value.checkpoint_hash()
    {
        return Err(DurableCodecError::corrupt());
    }
    let counts = value.counts();
    let retained = value.retained();
    encode_message(
        CHECKPOINT,
        &wire::StoredValidatedPrefixCheckpointV1 {
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
        },
    )
}

/// Decodes one canonical validated-prefix checkpoint envelope.
pub fn decode_validated_prefix_checkpoint_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredValidatedPrefixCheckpointV1>, DurableCodecError> {
    decode_message::<wire::StoredValidatedPrefixCheckpointV1, _, _>(CHECKPOINT, encoded, |value| {
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
        )
        .map_err(DurableCodecError::from_storage_value)
    })
}
