//! Crate-private durable-record codec boundary for redb tables.

use std::fmt;

use redb::ReadableTable;
use riffdb_storage_api as storage;

use crate::error::{codec_error, precommit_storage_error, storage_error};
use crate::keys::encode_event_key;

macro_rules! copied_codec {
    ($encode:ident, $decode:ident, $value:ty, $wire_encode:ident, $wire_decode:ident) => {
        #[allow(
            dead_code,
            reason = "WP-070 table implementations consume this codec spine"
        )]
        pub(crate) fn $encode(
            value: $value,
        ) -> Result<storage::CanonicalStoredEnvelopeV1, storage::StorageError> {
            storage::$wire_encode(value).map_err(codec_error)
        }

        #[allow(
            dead_code,
            reason = "WP-070 table implementations consume this codec spine"
        )]
        pub(crate) fn $decode(
            encoded: &[u8],
        ) -> Result<storage::EncodedPageItem<$value>, storage::StorageError> {
            storage::$wire_decode(encoded).map_err(codec_error)
        }
    };
}

macro_rules! borrowed_codec {
    ($encode:ident, $decode:ident, $value:ty, $wire_encode:ident, $wire_decode:ident) => {
        #[allow(
            dead_code,
            reason = "WP-070 table implementations consume this codec spine"
        )]
        pub(crate) fn $encode(
            value: &$value,
        ) -> Result<storage::CanonicalStoredEnvelopeV1, storage::StorageError> {
            storage::$wire_encode(value).map_err(codec_error)
        }

        #[allow(
            dead_code,
            reason = "WP-070 table implementations consume this codec spine"
        )]
        pub(crate) fn $decode(
            encoded: &[u8],
        ) -> Result<storage::EncodedPageItem<$value>, storage::StorageError> {
            storage::$wire_decode(encoded).map_err(codec_error)
        }
    };
}

copied_codec!(
    encode_storage_format_version_v1,
    decode_storage_format_version_v1,
    storage::StorageFormatVersion,
    encode_storage_format_version_v1,
    decode_storage_format_version_v1
);
copied_codec!(
    encode_record_registry_v2,
    decode_record_registry_v2,
    riffdb_types::SchemaHash,
    encode_record_registry_v2,
    decode_record_registry_v2
);
copied_codec!(
    encode_database_identity_v1,
    decode_database_identity_v1,
    riffdb_types::DatabaseId,
    encode_database_identity_v1,
    decode_database_identity_v1
);
copied_codec!(
    encode_application_sequence_allocator_v1,
    decode_application_sequence_allocator_v1,
    storage::ApplicationSequenceAllocator,
    encode_application_sequence_allocator_v1,
    decode_application_sequence_allocator_v1
);
copied_codec!(
    encode_administration_sequence_allocator_v1,
    decode_administration_sequence_allocator_v1,
    storage::AdministrationSequenceAllocator,
    encode_administration_sequence_allocator_v1,
    decode_administration_sequence_allocator_v1
);
copied_codec!(
    encode_history_incarnation_v1,
    decode_history_incarnation_v1,
    u64,
    encode_history_incarnation_v1,
    decode_history_incarnation_v1
);
copied_codec!(
    encode_service_audit_request_index_v1,
    decode_service_audit_request_index_v1,
    storage::StoredServiceAuditRequestIndexV1,
    encode_service_audit_request_index_v1,
    decode_service_audit_request_index_v1
);

borrowed_codec!(
    encode_contract_bundle_v1,
    decode_contract_bundle_v1,
    storage::StoredContractBundleV1,
    encode_contract_bundle_v1,
    decode_contract_bundle_v1
);

pub(crate) fn decode_commit_entity_references(
    encoded: &[u8],
) -> Result<storage::EncodedPageItem<Vec<storage::CommittedEntityReferenceV2>>, storage::StorageError>
{
    storage::decode_commit_entity_references(encoded).map_err(codec_error)
}

pub(crate) fn decode_commit_event_references(
    encoded: &[u8],
) -> Result<storage::EncodedPageItem<Vec<storage::EventReferenceV2>>, storage::StorageError> {
    storage::decode_commit_event_references(encoded).map_err(codec_error)
}

pub(crate) fn decode_commit_record_with_events(
    encoded: &[u8],
    events: Vec<storage::StoredDurableEventV1>,
) -> Result<storage::EncodedPageItem<storage::StoredCommitRecordV1>, storage::StorageError> {
    storage::decode_commit_record_with_events(encoded, events).map_err(codec_error)
}

pub(crate) fn decode_commit_with_event_table<T>(
    encoded: &[u8],
    events: &T,
) -> Result<storage::EncodedPageItem<storage::StoredCommitRecordV1>, storage::StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let references = decode_commit_event_references(encoded)?.into_parts().0;
    let mut loaded = Vec::with_capacity(references.len());
    for reference in references {
        let key = encode_event_key(reference.event_id());
        let row = events
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(storage::StorageErrorKind::CorruptData))?;
        let event = decode_durable_event_v1(row.value())?.into_parts().0;
        if !reference.matches(&event) {
            return Err(storage_error(storage::StorageErrorKind::CorruptData));
        }
        loaded.push(event);
    }
    decode_commit_record_with_events(encoded, loaded)
}
borrowed_codec!(
    encode_active_catalog_pointer_v1,
    decode_active_catalog_pointer_v1,
    storage::ActiveCatalogPointerV1,
    encode_active_catalog_pointer_v1,
    decode_active_catalog_pointer_v1
);

pub(crate) fn decode_outbox_event_reference(
    encoded: &[u8],
) -> Result<storage::EncodedPageItem<storage::EventReferenceV2>, storage::StorageError> {
    storage::decode_outbox_event_reference(encoded).map_err(codec_error)
}

pub(crate) fn decode_outbox_intent_with_event(
    encoded: &[u8],
    event: storage::StoredDurableEventV1,
) -> Result<storage::EncodedPageItem<storage::StoredOutboxIntentV1>, storage::StorageError> {
    storage::decode_outbox_intent_with_event(encoded, event).map_err(codec_error)
}

pub(crate) fn decode_outbox_with_event_table<T>(
    encoded: &[u8],
    events: &T,
) -> Result<storage::EncodedPageItem<storage::StoredOutboxIntentV1>, storage::StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let reference = decode_outbox_event_reference(encoded)?.into_parts().0;
    let key = encode_event_key(reference.event_id());
    let row = events
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(storage::StorageErrorKind::CorruptData))?;
    let event = decode_durable_event_v1(row.value())?.into_parts().0;
    if !reference.matches(&event) {
        return Err(storage_error(storage::StorageErrorKind::CorruptData));
    }
    decode_outbox_intent_with_event(encoded, event)
}
borrowed_codec!(
    encode_catalog_administration_v1,
    decode_catalog_administration_v1,
    storage::StoredCatalogAdministrationV1,
    encode_catalog_administration_v1,
    decode_catalog_administration_v1
);
borrowed_codec!(
    encode_query_module_v1,
    decode_query_module_v1,
    storage::StoredQueryModuleV1,
    encode_query_module_v1,
    decode_query_module_v1
);
borrowed_codec!(
    encode_active_query_module_pointer_v1,
    decode_active_query_module_pointer_v1,
    storage::ActiveQueryModulePointerV1,
    encode_active_query_module_pointer_v1,
    decode_active_query_module_pointer_v1
);
borrowed_codec!(
    encode_query_module_administration_v1,
    decode_query_module_administration_v1,
    storage::StoredQueryModuleAdministrationV1,
    encode_query_module_administration_v1,
    decode_query_module_administration_v1
);

borrowed_codec!(
    encode_entity_record_v1,
    decode_entity_record_v1,
    storage::StoredEntityRecordV1,
    encode_entity_record_v1,
    decode_entity_record_v1
);
borrowed_codec!(
    encode_index_entry_v2,
    decode_index_entry_v2,
    storage::StoredIndexEntryV2,
    encode_index_entry_v2,
    decode_index_entry_v2
);

pub(crate) fn decode_index_migration_row(
    physical_key: &riffdb_types::IndexEntryKey,
    encoded: &[u8],
) -> Result<storage::IndexMigrationRowEvidence, storage::StorageError> {
    storage::decode_index_migration_row(physical_key, encoded).map_err(codec_error)
}
borrowed_codec!(
    encode_index_epoch_v1,
    decode_index_epoch_v1,
    storage::StoredIndexEpochV1,
    encode_index_epoch_v1,
    decode_index_epoch_v1
);

pub(crate) fn decode_legacy_index_epoch_v1(
    encoded: &[u8],
) -> Result<storage::EncodedPageItem<storage::LegacyStoredIndexEpochV1>, storage::StorageError> {
    storage::decode_legacy_index_epoch_v1(encoded).map_err(codec_error)
}
borrowed_codec!(
    encode_pending_admission_v1,
    decode_pending_admission_v1,
    storage::StoredPendingAdmissionV1,
    encode_pending_admission_v1,
    decode_pending_admission_v1
);
borrowed_codec!(
    encode_execution_failed_v1,
    decode_execution_failed_v1,
    storage::StoredExecutionFailedV1,
    encode_execution_failed_v1,
    decode_execution_failed_v1
);
borrowed_codec!(
    encode_stored_outcome_v1,
    decode_stored_outcome_v1,
    storage::StoredOutcomeV1,
    encode_stored_outcome_v1,
    decode_stored_outcome_v1
);
borrowed_codec!(
    encode_durable_event_v1,
    decode_durable_event_v1,
    storage::StoredDurableEventV1,
    encode_durable_event_v1,
    decode_durable_event_v1
);
borrowed_codec!(
    encode_provenance_record_v1,
    decode_provenance_record_v1,
    storage::StoredProvenanceRecordV1,
    encode_provenance_record_v1,
    decode_provenance_record_v1
);
borrowed_codec!(
    encode_commit_record_v1,
    decode_commit_record_v1,
    storage::StoredCommitRecordV1,
    encode_commit_record_v1,
    decode_commit_record_v1
);

borrowed_codec!(
    encode_capability_record_v1,
    decode_capability_record_v1,
    storage::StoredCapabilityRecordV1,
    encode_capability_record_v1,
    decode_capability_record_v1
);
copied_codec!(
    encode_capability_token_lookup_v1,
    decode_capability_token_lookup_v1,
    storage::CapabilityTokenLookupV1,
    encode_capability_token_lookup_v1,
    decode_capability_token_lookup_v1
);
copied_codec!(
    encode_capability_bootstrap_marker_v1,
    decode_capability_bootstrap_marker_v1,
    storage::CapabilityBootstrapMarkerV1,
    encode_capability_bootstrap_marker_v1,
    decode_capability_bootstrap_marker_v1
);
borrowed_codec!(
    encode_capability_administration_v1,
    decode_capability_administration_v1,
    storage::StoredCapabilityAdministrationV1,
    encode_capability_administration_v1,
    decode_capability_administration_v1
);
borrowed_codec!(
    encode_service_audit_record_v1,
    decode_service_audit_record_v1,
    storage::StoredServiceAuditRecordV1,
    encode_service_audit_record_v1,
    decode_service_audit_record_v1
);

borrowed_codec!(
    encode_outbox_intent_v1,
    decode_outbox_intent_v1,
    storage::StoredOutboxIntentV1,
    encode_outbox_intent_v1,
    decode_outbox_intent_v1
);
borrowed_codec!(
    encode_outbox_status_v1,
    decode_outbox_status_v1,
    storage::StoredOutboxStatusV1,
    encode_outbox_status_v1,
    decode_outbox_status_v1
);

#[allow(
    dead_code,
    reason = "WP-070 projection tables consume this codec spine"
)]
pub(crate) fn encode_projection_state_v1(
    value: &storage::StoredProjectionStateV1,
) -> Result<storage::CanonicalStoredEnvelopeV1, storage::StorageError> {
    storage::encode_projection_state_v1(value).map_err(codec_error)
}

#[allow(
    dead_code,
    reason = "WP-070 startup integrity consumes structural rows"
)]
pub(crate) fn decode_projection_state_structural_v1(
    encoded: &[u8],
) -> Result<
    storage::EncodedPageItem<storage::StructurallyDecodedProjectionStateV1>,
    storage::StorageError,
> {
    storage::decode_projection_state_structural_v1(encoded).map_err(codec_error)
}

#[allow(dead_code, reason = "WP-070 projection reads consume checked rows")]
pub(crate) fn decode_projection_state_v1(
    encoded: &[u8],
    schema: &storage::CheckedProjectionSchema,
) -> Result<storage::EncodedPageItem<storage::StoredProjectionStateV1>, storage::StorageError> {
    storage::decode_projection_state_v1(encoded, schema).map_err(codec_error)
}

borrowed_codec!(
    encode_projection_apply_v1,
    decode_projection_apply_v1,
    storage::StoredProjectionApplyV1,
    encode_projection_apply_v1,
    decode_projection_apply_v1
);
borrowed_codec!(
    encode_projection_control_v1,
    decode_projection_control_v1,
    storage::StoredProjectionControlV1,
    encode_projection_control_v1,
    decode_projection_control_v1
);

/// The exact closed value union admitted by the terminal idempotency table.
#[derive(Clone, Eq, PartialEq)]
pub(crate) enum IdempotencyRecordV1 {
    StoredOutcome(storage::StoredOutcomeV1),
    ExecutionFailed(storage::StoredExecutionFailedV1),
}

impl fmt::Debug for IdempotencyRecordV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StoredOutcome(_) => "IdempotencyRecordV1::StoredOutcome([REDACTED])",
            Self::ExecutionFailed(_) => "IdempotencyRecordV1::ExecutionFailed([REDACTED])",
        })
    }
}

#[allow(
    dead_code,
    reason = "WP-070 idempotency table consumes this dispatcher"
)]
pub(crate) fn encode_idempotency_record_v1(
    value: &IdempotencyRecordV1,
) -> Result<storage::CanonicalStoredEnvelopeV1, storage::StorageError> {
    match value {
        IdempotencyRecordV1::StoredOutcome(value) => encode_stored_outcome_v1(value),
        IdempotencyRecordV1::ExecutionFailed(value) => encode_execution_failed_v1(value),
    }
}

#[allow(
    dead_code,
    reason = "WP-070 idempotency table consumes this dispatcher"
)]
pub(crate) fn decode_idempotency_record_v1(
    encoded: &[u8],
) -> Result<storage::EncodedPageItem<IdempotencyRecordV1>, storage::StorageError> {
    match storage::decode_stored_outcome_v1(encoded) {
        Ok(item) => Ok(map_item(item, IdempotencyRecordV1::StoredOutcome)),
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {
            storage::decode_execution_failed_v1(encoded)
                .map(|item| map_item(item, IdempotencyRecordV1::ExecutionFailed))
                .map_err(codec_error)
        }
        Err(error) => Err(codec_error(error)),
    }
}

#[allow(dead_code, reason = "WP-070 audit table consumes this dispatcher")]
pub(crate) fn encode_administration_audit_record_v1(
    value: &storage::StoredAdministrationAuditRecordV1,
) -> Result<storage::CanonicalStoredEnvelopeV1, storage::StorageError> {
    match value {
        storage::StoredAdministrationAuditRecordV1::Catalog(value) => {
            encode_catalog_administration_v1(value)
        }
        storage::StoredAdministrationAuditRecordV1::QueryModule(value) => {
            encode_query_module_administration_v1(value)
        }
        storage::StoredAdministrationAuditRecordV1::Capability(value) => {
            encode_capability_administration_v1(value)
        }
        storage::StoredAdministrationAuditRecordV1::Service(value) => {
            encode_service_audit_record_v1(value)
        }
    }
}

#[allow(dead_code, reason = "WP-070 audit table consumes this dispatcher")]
pub(crate) fn decode_administration_audit_record_v1(
    encoded: &[u8],
) -> Result<
    storage::EncodedPageItem<storage::StoredAdministrationAuditRecordV1>,
    storage::StorageError,
> {
    match storage::decode_catalog_administration_v1(encoded) {
        Ok(item) => {
            return Ok(map_item(
                item,
                storage::StoredAdministrationAuditRecordV1::Catalog,
            ));
        }
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(codec_error(error)),
    }

    match storage::decode_capability_administration_v1(encoded) {
        Ok(item) => {
            return Ok(map_item(
                item,
                storage::StoredAdministrationAuditRecordV1::Capability,
            ));
        }
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(codec_error(error)),
    }

    match storage::decode_query_module_administration_v1(encoded) {
        Ok(item) => Ok(map_item(
            item,
            storage::StoredAdministrationAuditRecordV1::QueryModule,
        )),
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {
            storage::decode_service_audit_record_v1(encoded)
                .map(|item| map_item(item, storage::StoredAdministrationAuditRecordV1::Service))
                .map_err(codec_error)
        }
        Err(error) => Err(codec_error(error)),
    }
}

fn map_item<T, U>(
    item: storage::EncodedPageItem<T>,
    map: impl FnOnce(T) -> U,
) -> storage::EncodedPageItem<U> {
    let (value, charge) = item.into_parts();
    storage::EncodedPageItem::new(map(value), charge)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_idempotency_dispatch_is_closed_and_preserves_exact_charge() {
        for record_type in [
            "riffdb.storage.v1.StoredOutcomeV1",
            "riffdb.storage.v1.StoredExecutionFailedV1",
        ] {
            let bytes = fixture_envelope(record_type);
            let decoded = decode_idempotency_record_v1(&bytes).expect("allowed terminal record");
            assert_eq!(decoded.encoded_content_charge().get(), bytes.len());
            match (record_type, decoded.value()) {
                ("riffdb.storage.v1.StoredOutcomeV1", IdempotencyRecordV1::StoredOutcome(_))
                | (
                    "riffdb.storage.v1.StoredExecutionFailedV1",
                    IdempotencyRecordV1::ExecutionFailed(_),
                ) => {}
                _ => panic!("mixed-table dispatcher returned the wrong closed variant"),
            }

            let encoded = encode_idempotency_record_v1(decoded.value()).expect("record re-encodes");
            assert_eq!(encoded.as_bytes(), bytes);
            assert_eq!(encoded.encoded_content_charge().get(), bytes.len());
        }

        let wrong = fixture_envelope("riffdb.storage.v1.StoredEntityRecordV1");
        assert_eq!(
            decode_idempotency_record_v1(&wrong).unwrap_err().kind(),
            storage::StorageErrorKind::CorruptData
        );
    }

    #[test]
    fn administration_audit_dispatch_is_closed_and_preserves_exact_charge() {
        for record_type in [
            "riffdb.storage.v1.StoredCatalogAdministrationV1",
            "riffdb.storage.v1.CapabilityAdministrationAuditV1",
            "riffdb.storage.v1.ServiceAuditRecordV1",
        ] {
            let bytes = fixture_envelope(record_type);
            let decoded =
                decode_administration_audit_record_v1(&bytes).expect("allowed audit record");
            assert_eq!(decoded.encoded_content_charge().get(), bytes.len());
            match (record_type, decoded.value()) {
                (
                    "riffdb.storage.v1.StoredCatalogAdministrationV1",
                    storage::StoredAdministrationAuditRecordV1::Catalog(_),
                )
                | (
                    "riffdb.storage.v1.CapabilityAdministrationAuditV1",
                    storage::StoredAdministrationAuditRecordV1::Capability(_),
                )
                | (
                    "riffdb.storage.v1.ServiceAuditRecordV1",
                    storage::StoredAdministrationAuditRecordV1::Service(_),
                ) => {}
                _ => panic!("mixed-table dispatcher returned the wrong closed variant"),
            }

            let encoded =
                encode_administration_audit_record_v1(decoded.value()).expect("record re-encodes");
            assert_eq!(encoded.as_bytes(), bytes);
            assert_eq!(encoded.encoded_content_charge().get(), bytes.len());
        }

        let wrong = fixture_envelope("riffdb.storage.v1.StoredCommitRecordV1");
        assert_eq!(
            decode_administration_audit_record_v1(&wrong)
                .unwrap_err()
                .kind(),
            storage::StorageErrorKind::CorruptData
        );
    }

    fn fixture_envelope(record_type: &str) -> Vec<u8> {
        let fixture = include_str!("../../../fixtures/proto/durable-wire-vectors-v2.txt");
        let line = fixture
            .lines()
            .find(|line| line.split('\t').next() == Some(record_type))
            .expect("record fixture exists");
        decode_hex(
            line.split('\t')
                .nth(2)
                .expect("record fixture carries envelope bytes"),
        )
    }

    fn decode_hex(encoded: &str) -> Vec<u8> {
        assert_eq!(encoded.len() % 2, 0, "hex fixture length");
        encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
            .collect()
    }

    fn nibble(value: u8) -> u8 {
        match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            _ => panic!("fixture contains non-lowercase-hex input"),
        }
    }
}
