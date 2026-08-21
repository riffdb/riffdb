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
borrowed_codec!(
    encode_application_installation_campaign_v1,
    decode_application_installation_campaign_v1,
    storage::StoredApplicationInstallationCampaignV1,
    encode_application_installation_campaign_v1,
    decode_application_installation_campaign_v1
);
borrowed_codec!(
    encode_application_export_operation_v1,
    decode_application_export_operation_v1,
    storage::StoredApplicationExportOperationV1,
    encode_application_export_operation_v1,
    decode_application_export_operation_v1
);

pub(crate) fn decode_commit_entity_references(
    encoded: &[u8],
) -> Result<storage::EncodedPageItem<Vec<storage::CommittedEntityReferenceV2>>, storage::StorageError>
{
    match storage::decode_command_capsule_v2(encoded) {
        Ok(capsule) => {
            let (capsule, charge) = capsule.into_parts();
            return Ok(storage::EncodedPageItem::new(
                capsule.base().commit().entity_references().to_vec(),
                charge,
            ));
        }
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(codec_error(error)),
    }
    match storage::decode_command_capsule_entity_references(encoded) {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {
            storage::decode_commit_entity_references(encoded).map_err(codec_error)
        }
        Err(error) => Err(codec_error(error)),
    }
}

pub(crate) fn decode_commit_event_references(
    encoded: &[u8],
) -> Result<storage::EncodedPageItem<storage::DecodedCommitEventReferencesV1>, storage::StorageError>
{
    storage::decode_commit_event_references_with_revision(encoded).map_err(codec_error)
}

pub(crate) fn decode_commit_with_event_table<T>(
    encoded: &[u8],
    events: &T,
) -> Result<storage::EncodedPageItem<storage::StoredCommitRecordV1>, storage::StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    if let Ok(capsule) = decode_command_capsule_with_event_table(encoded, events) {
        let (capsule, charge) = capsule.into_parts();
        return Ok(storage::EncodedPageItem::new(
            capsule.commit().clone(),
            charge,
        ));
    }
    let (prepared, charge) = decode_commit_event_references(encoded)?.into_parts();
    let mut loaded = Vec::with_capacity(prepared.references().len());
    for reference in prepared.references() {
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
    prepared.materialize(loaded, charge).map_err(codec_error)
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
    encode_reactive_module_v1,
    decode_reactive_module_v1,
    storage::StoredReactiveModuleV1,
    encode_reactive_module_v1,
    decode_reactive_module_v1
);
borrowed_codec!(
    encode_reactive_module_administration_v1,
    decode_reactive_module_administration_v1,
    storage::StoredReactiveModuleAdministrationV1,
    encode_reactive_module_administration_v1,
    decode_reactive_module_administration_v1
);
borrowed_codec!(
    encode_event_consumer_v1,
    decode_event_consumer_v1,
    storage::StoredEventConsumerV1,
    encode_event_consumer_v1,
    decode_event_consumer_v1
);
borrowed_codec!(
    encode_event_consumer_delivery_v1,
    decode_event_consumer_delivery_v1,
    storage::StoredEventConsumerDeliveryV1,
    encode_event_consumer_delivery_v1,
    decode_event_consumer_delivery_v1
);

borrowed_codec!(
    encode_entity_record_v1,
    decode_entity_record_v1,
    storage::StoredEntityRecordV1,
    encode_entity_record_v1,
    decode_entity_record_v1
);
borrowed_codec!(
    encode_vector_evidence_v1,
    decode_vector_evidence_v1,
    storage::StoredVectorEvidenceV1,
    encode_vector_evidence_v1,
    decode_vector_evidence_v1
);
borrowed_codec!(
    encode_vector_observation_v1,
    decode_vector_observation_v1,
    storage::VectorObservationCountsV1,
    encode_vector_observation_v1,
    decode_vector_observation_v1
);
borrowed_codec!(
    encode_vector_health_observation_v1,
    decode_vector_health_observation_v1,
    storage::VectorHealthObservationV1,
    encode_vector_health_observation_v1,
    decode_vector_health_observation_v1
);
borrowed_codec!(
    encode_vector_evidence_index_v1,
    decode_vector_evidence_index_v1,
    storage::VectorEvidenceIndexEntryV1,
    encode_vector_evidence_index_v1,
    decode_vector_evidence_index_v1
);
borrowed_codec!(
    encode_vector_projection_control_v1,
    decode_vector_projection_control_v1,
    storage::StoredVectorProjectionControlV1,
    encode_vector_projection_control_v1,
    decode_vector_projection_control_v1
);

pub(crate) fn decode_entity_record_v1_profiled(
    encoded: &[u8],
) -> Result<
    (
        storage::EncodedPageItem<storage::StoredEntityRecordV1>,
        storage::DurableReadDecodeProfileV1,
    ),
    storage::StorageError,
> {
    storage::decode_entity_record_v1_profiled(encoded).map_err(codec_error)
}
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
copied_codec!(
    encode_event_route_v1,
    decode_event_route_v1,
    storage::StoredEventRouteV1,
    encode_event_route_v1,
    decode_event_route_v1
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

pub(crate) fn decode_command_capsule_with_event_table<T>(
    encoded: &[u8],
    events: &T,
) -> Result<storage::EncodedPageItem<storage::StoredCommandCapsuleV1>, storage::StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    match storage::decode_command_capsule_v2(encoded) {
        Ok(capsule) => {
            let (capsule, charge) = capsule.into_parts();
            return Ok(storage::EncodedPageItem::new(
                capsule.base().clone(),
                charge,
            ));
        }
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(codec_error(error)),
    }
    let references = storage::decode_command_capsule_event_references(encoded)
        .map_err(codec_error)?
        .into_parts()
        .0;
    let mut loaded = Vec::with_capacity(references.len());
    for reference in &references {
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
    storage::decode_command_capsule_v1(encoded, loaded).map_err(codec_error)
}
copied_codec!(
    encode_command_locator_v1,
    decode_command_locator_v1,
    storage::StoredCommandLocatorV1,
    encode_command_locator_v1,
    decode_command_locator_v1
);
copied_codec!(
    encode_command_audit_locator_v1,
    decode_command_audit_locator_v1,
    storage::StoredCommandAuditLocatorV1,
    encode_command_audit_locator_v1,
    decode_command_audit_locator_v1
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
    encode_service_audit_record_v2,
    decode_service_audit_record,
    storage::StoredServiceAuditRecordV1,
    encode_service_audit_record_v2,
    decode_service_audit_record
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
    CommandLocator(storage::StoredCommandLocatorV1),
}

impl fmt::Debug for IdempotencyRecordV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::StoredOutcome(_) => "IdempotencyRecordV1::StoredOutcome([REDACTED])",
            Self::ExecutionFailed(_) => "IdempotencyRecordV1::ExecutionFailed([REDACTED])",
            Self::CommandLocator(_) => "IdempotencyRecordV1::CommandLocator([REDACTED])",
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
        IdempotencyRecordV1::CommandLocator(value) => encode_command_locator_v1(*value),
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
            match storage::decode_execution_failed_v1(encoded) {
                Ok(item) => Ok(map_item(item, IdempotencyRecordV1::ExecutionFailed)),
                Err(error)
                    if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    storage::decode_command_locator_v1(encoded)
                        .map(|item| map_item(item, IdempotencyRecordV1::CommandLocator))
                        .map_err(codec_error)
                }
                Err(error) => Err(codec_error(error)),
            }
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
        storage::StoredAdministrationAuditRecordV1::ReactiveModule(value) => {
            encode_reactive_module_administration_v1(value)
        }
        storage::StoredAdministrationAuditRecordV1::Capability(value) => {
            encode_capability_administration_v1(value)
        }
        storage::StoredAdministrationAuditRecordV1::Service(value) => {
            encode_service_audit_record_v2(value)
        }
        storage::StoredAdministrationAuditRecordV1::Retention(value) => {
            storage::encode_retention_administration_v1(value).map_err(codec_error)
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
        Ok(item) => {
            return Ok(map_item(
                item,
                storage::StoredAdministrationAuditRecordV1::QueryModule,
            ));
        }
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(codec_error(error)),
    }

    match storage::decode_reactive_module_administration_v1(encoded) {
        Ok(item) => {
            return Ok(map_item(
                item,
                storage::StoredAdministrationAuditRecordV1::ReactiveModule,
            ));
        }
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(codec_error(error)),
    }

    match storage::decode_retention_administration_v1(encoded) {
        Ok(item) => Ok(map_item(
            item,
            storage::StoredAdministrationAuditRecordV1::Retention,
        )),
        Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {
            storage::decode_service_audit_record(encoded)
                .map(|item| map_item(item, storage::StoredAdministrationAuditRecordV1::Service))
                .map_err(codec_error)
        }
        Err(error) => Err(codec_error(error)),
    }
}

pub(crate) fn decode_administration_audit_with_command_tables<C, E>(
    encoded: &[u8],
    commits: &C,
    events: &E,
) -> Result<
    storage::EncodedPageItem<storage::StoredAdministrationAuditRecordV1>,
    storage::StorageError,
>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    if let Ok(locator) = storage::decode_command_audit_locator_v1(encoded) {
        let (locator, charge) = locator.into_parts();
        let key = crate::keys::encode_application_sequence_key(locator.commit_sequence());
        let mut range = commits
            .range::<&[u8]>((
                std::ops::Bound::Unbounded,
                std::ops::Bound::Included(key.as_slice()),
            ))
            .map_err(precommit_storage_error)?;
        let (physical_key, row) = range
            .next_back()
            .ok_or_else(|| storage_error(storage::StorageErrorKind::CorruptData))?
            .map_err(precommit_storage_error)?;
        let physical_sequence = crate::keys::decode_application_sequence_key(physical_key.value())
            .map_err(|_| storage_error(storage::StorageErrorKind::CorruptData))?;
        let capsule = match storage::decode_command_segment_v1(row.value()) {
            Ok(segment) => {
                let segment = segment.into_parts().0;
                if segment.first_commit_sequence() != physical_sequence
                    || locator.commit_sequence() < physical_sequence
                    || locator.commit_sequence() > segment.last_commit_sequence()
                {
                    return Err(storage_error(storage::StorageErrorKind::CorruptData));
                }
                let ordinal = locator
                    .commit_sequence()
                    .get()
                    .checked_sub(physical_sequence.get())
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| storage_error(storage::StorageErrorKind::CorruptData))?;
                segment
                    .commands()
                    .get(ordinal)
                    .filter(|command| command.commit_sequence() == locator.commit_sequence())
                    .ok_or_else(|| storage_error(storage::StorageErrorKind::CorruptData))?
                    .base()
                    .clone()
            }
            Err(error) if error.kind() == storage::DurableCodecErrorKind::UnexpectedRecordType => {
                if physical_sequence != locator.commit_sequence() {
                    return Err(storage_error(storage::StorageErrorKind::CorruptData));
                }
                decode_command_capsule_with_event_table(row.value(), events)?
                    .into_parts()
                    .0
            }
            Err(error) => return Err(codec_error(error)),
        };
        if capsule.commit_sequence() != locator.commit_sequence() {
            return Err(storage_error(storage::StorageErrorKind::CorruptData));
        }
        return Ok(storage::EncodedPageItem::new(
            storage::StoredAdministrationAuditRecordV1::Service(
                capsule.audit(locator.member()).clone(),
            ),
            charge,
        ));
    }
    decode_administration_audit_record_v1(encoded)
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
            let current = decode_idempotency_record_v1(encoded.as_bytes())
                .expect("current successor re-decodes");
            assert_eq!(current.value(), decoded.value());
            assert_eq!(
                current.encoded_content_charge().get(),
                encoded.as_bytes().len()
            );
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
            if record_type == "riffdb.storage.v1.ServiceAuditRecordV1" {
                assert_ne!(encoded.as_bytes(), bytes);
                let current = decode_administration_audit_record_v1(encoded.as_bytes())
                    .expect("current audit generation decodes");
                assert_eq!(current.value(), decoded.value());
            } else {
                assert_eq!(encoded.as_bytes(), bytes);
                assert_eq!(encoded.encoded_content_charge().get(), bytes.len());
            }
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
