use prost::Message;
use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    CanonicalInputHash, CommitSequence, ConflictKeyHash, ContractVersion, EntityRecordHash,
    EntityVersion, EventHash, EventTypeId, IndexEntryKey, IndexEpoch, IndexId, MAX_KEY_BYTES,
    OutcomeId, PartitionKey, PartitionKeyHash, ProvenanceId, RequestId, encode_canonical_record,
};

use crate::{
    AffectedEntityV1, CommittedEntityMutationV1, CommittedEntityReferenceV2, DurabilityMode,
    EncodedPageItem, EventReferenceV2, IndexMigrationRowEvidence, IndexMigrationSemanticRow,
    LegacyStoredIndexEpochV1, PartitionIndexTarget, StoredCommandCausationV1, StoredCommitRecordV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredEventRouteV1, StoredExecutionFailedV1,
    StoredIndexEntryV1, StoredIndexEntryV2, StoredIndexEpochV1, StoredOutcomeV1,
    StoredPendingAdmissionV1, StoredProvenanceRecordV1, StoredReadDependenciesV1,
    StoredReadDependencyV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, actor_from_proto, actor_to_proto,
    binding_from_proto, binding_to_proto, canonical_record_from_bytes, claims_from_proto,
    claims_to_proto, declared_outcome_from_proto, declared_outcome_to_proto, decode_message,
    decode_record_variant, encode_message, entity_target_from_proto, entity_target_to_proto,
    epoch_from_proto, epoch_to_proto, event_id_from_proto, event_id_to_proto, expected_from_proto,
    expected_to_proto, fixed, identity_from_proto, identity_to_proto, logical_time_from_proto,
    plan_from_proto, plan_to_proto, require, storage_result, structural_prefix_from_bytes,
    timestamp_to_proto,
};

pub(super) const ENTITY: &str = "riffdb.storage.v1.StoredEntityRecordV1";
pub(super) const INDEX_ENTRY: &str = "riffdb.storage.v1.StoredIndexEntryV2";
const LEGACY_INDEX_ENTRY: &str = "riffdb.storage.v1.StoredIndexEntryV1";
pub(super) const INDEX_EPOCH: &str = "riffdb.storage.v1.StoredIndexGenerationV2";
const LEGACY_INDEX_EPOCH: &str = "riffdb.storage.v1.StoredIndexEpochV1";
const PENDING: &str = "riffdb.storage.v1.StoredPendingAdmissionV1";
const PENDING_V2: &str = "riffdb.storage.v1.StoredPendingAdmissionV2";
const EXECUTION_FAILED: &str = "riffdb.storage.v1.StoredExecutionFailedV1";
const EXECUTION_FAILED_V2: &str = "riffdb.storage.v1.StoredExecutionFailedV2";
pub(super) const OUTCOME: &str = "riffdb.storage.v1.StoredOutcomeV1";
pub(super) const OUTCOME_V2: &str = "riffdb.storage.v1.StoredOutcomeV2";
pub(super) const EVENT: &str = "riffdb.storage.v1.StoredDurableEventV1";
pub(super) const EVENT_ROUTE: &str = "riffdb.storage.v1.StoredEventRouteV1";
pub(super) const PROVENANCE: &str = "riffdb.storage.v1.StoredProvenanceRecordV1";
pub(super) const PROVENANCE_V2: &str = "riffdb.storage.v1.StoredProvenanceRecordV2";
pub(super) const COMMIT: &str = "riffdb.storage.v1.StoredCommitRecordV3";
const COMMIT_V2: &str = "riffdb.storage.v1.StoredCommitRecordV2";
const LEGACY_COMMIT: &str = "riffdb.storage.v1.StoredCommitRecordV1";

pub(super) fn durability_to_proto(value: DurabilityMode) -> i32 {
    match value {
        DurabilityMode::Sync => wire::DurabilityModeV1::DurabilityModeSync as i32,
        DurabilityMode::Group => wire::DurabilityModeV1::DurabilityModeGroup as i32,
        DurabilityMode::Memory => wire::DurabilityModeV1::DurabilityModeMemory as i32,
    }
}

pub(super) fn event_reference_to_proto(value: EventReferenceV2) -> wire::EventReferenceV2 {
    wire::EventReferenceV2 {
        event_id: Some(event_id_to_proto(value.event_id())),
        event_hash: value.event_hash().as_bytes().to_vec(),
    }
}

pub(super) fn event_reference_from_proto(
    value: wire::EventReferenceV2,
) -> Result<EventReferenceV2, DurableCodecError> {
    Ok(EventReferenceV2::new(
        event_id_from_proto(require(value.event_id)?)?,
        EventHash::from_bytes(fixed(value.event_hash)?),
    ))
}

fn durability_from_proto(value: i32) -> Result<DurabilityMode, DurableCodecError> {
    match wire::DurabilityModeV1::try_from(value).map_err(|_| DurableCodecError::corrupt())? {
        wire::DurabilityModeV1::DurabilityModeSync => Ok(DurabilityMode::Sync),
        wire::DurabilityModeV1::DurabilityModeGroup => Ok(DurabilityMode::Group),
        wire::DurabilityModeV1::DurabilityModeMemory => Ok(DurabilityMode::Memory),
        wire::DurabilityModeV1::DurabilityModeUnspecified => Err(DurableCodecError::corrupt()),
    }
}

fn execution_code_to_proto(value: riffdb_types::ExecutionFailureCode) -> i32 {
    match value {
        riffdb_types::ExecutionFailureCode::ArithmeticFault => {
            wire::ExecutionFailureCodeV1::ExecutionFailureCodeArithmeticFault as i32
        }
        riffdb_types::ExecutionFailureCode::ResourceLimit => {
            wire::ExecutionFailureCodeV1::ExecutionFailureCodeResourceLimit as i32
        }
        riffdb_types::ExecutionFailureCode::UniqueConflict => {
            wire::ExecutionFailureCodeV1::ExecutionFailureCodeUniqueConflict as i32
        }
    }
}

fn execution_code_from_proto(
    value: i32,
) -> Result<riffdb_types::ExecutionFailureCode, DurableCodecError> {
    match wire::ExecutionFailureCodeV1::try_from(value).map_err(|_| DurableCodecError::corrupt())? {
        wire::ExecutionFailureCodeV1::ExecutionFailureCodeArithmeticFault => {
            Ok(riffdb_types::ExecutionFailureCode::ArithmeticFault)
        }
        wire::ExecutionFailureCodeV1::ExecutionFailureCodeResourceLimit => {
            Ok(riffdb_types::ExecutionFailureCode::ResourceLimit)
        }
        wire::ExecutionFailureCodeV1::ExecutionFailureCodeUniqueConflict => {
            Ok(riffdb_types::ExecutionFailureCode::UniqueConflict)
        }
        wire::ExecutionFailureCodeV1::ExecutionFailureCodeUnspecified => {
            Err(DurableCodecError::corrupt())
        }
    }
}

pub(super) fn entity_to_proto(value: &StoredEntityRecordV1) -> wire::StoredEntityRecordV1 {
    wire::StoredEntityRecordV1 {
        target: Some(entity_target_to_proto(value.target())),
        entity_version: value.entity_version().get(),
        written_by_contract: value.written_by_contract().get(),
        schema_binding: Some(binding_to_proto(value.schema_binding())),
        canonical_fields: value.fields_encoded().to_vec(),
    }
}

fn entity_from_proto(
    value: wire::StoredEntityRecordV1,
) -> Result<StoredEntityRecordV1, DurableCodecError> {
    storage_result(StoredEntityRecordV1::new(
        entity_target_from_proto(require(value.target)?)?,
        EntityVersion::new(value.entity_version).ok_or_else(DurableCodecError::corrupt)?,
        ContractVersion::new(value.written_by_contract).ok_or_else(DurableCodecError::corrupt)?,
        binding_from_proto(require(value.schema_binding)?)?,
        canonical_record_from_bytes(&value.canonical_fields)?,
    ))
}

pub(super) fn index_entry_to_proto(value: &StoredIndexEntryV2) -> wire::StoredIndexEntryV2 {
    wire::StoredIndexEntryV2 {
        index_entry_key: value.key().as_bytes().to_vec(),
        schema_binding: Some(binding_to_proto(value.schema_binding())),
        canonical_covered_values: value.covered_values_encoded().to_vec(),
        partition_key: value.partition_key().as_bytes().to_vec(),
    }
}

fn legacy_index_entry_from_proto(
    value: wire::StoredIndexEntryV1,
) -> Result<StoredIndexEntryV1, DurableCodecError> {
    storage_result(StoredIndexEntryV1::new(
        IndexEntryKey::from_bytes(value.index_entry_key)
            .map_err(|_| DurableCodecError::corrupt())?,
        binding_from_proto(require(value.schema_binding)?)?,
        canonical_record_from_bytes(&value.canonical_covered_values)?,
    ))
}

fn index_entry_from_proto(
    value: wire::StoredIndexEntryV2,
) -> Result<StoredIndexEntryV2, DurableCodecError> {
    storage_result(StoredIndexEntryV2::new(
        IndexEntryKey::from_bytes(value.index_entry_key)
            .map_err(|_| DurableCodecError::corrupt())?,
        binding_from_proto(require(value.schema_binding)?)?,
        canonical_record_from_bytes(&value.canonical_covered_values)?,
        PartitionKey::from_bytes(value.partition_key).map_err(|_| DurableCodecError::corrupt())?,
    ))
}

pub(super) fn index_epoch_to_proto(value: &StoredIndexEpochV1) -> wire::StoredIndexGenerationV2 {
    wire::StoredIndexGenerationV2 {
        partition_key: value.target().partition_key().as_bytes().to_vec(),
        index_id: value.target().index_id().get(),
        schema_binding: Some(binding_to_proto(value.schema_binding())),
        generation: value.epoch().get(),
    }
}

fn index_epoch_from_proto(
    value: wire::StoredIndexGenerationV2,
) -> Result<StoredIndexEpochV1, DurableCodecError> {
    Ok(StoredIndexEpochV1::new(
        PartitionIndexTarget::new(
            PartitionKey::from_bytes(value.partition_key)
                .map_err(|_| DurableCodecError::corrupt())?,
            IndexId::new(value.index_id).ok_or_else(DurableCodecError::corrupt)?,
        ),
        binding_from_proto(require(value.schema_binding)?)?,
        IndexEpoch::new(value.generation).ok_or_else(DurableCodecError::corrupt)?,
    ))
}

#[cfg(any(test, feature = "test-fixtures"))]
fn legacy_index_epoch_to_proto(value: &LegacyStoredIndexEpochV1) -> wire::StoredIndexEpochV1 {
    wire::StoredIndexEpochV1 {
        canonical_index_range_prefix: value.target().as_bytes().to_vec(),
        schema_binding: Some(binding_to_proto(value.schema_binding())),
        epoch: value.epoch().get(),
    }
}

fn legacy_index_epoch_from_proto(
    value: wire::StoredIndexEpochV1,
) -> Result<LegacyStoredIndexEpochV1, DurableCodecError> {
    Ok(LegacyStoredIndexEpochV1::new(
        structural_prefix_from_bytes(value.canonical_index_range_prefix)?,
        binding_from_proto(require(value.schema_binding)?)?,
        IndexEpoch::new(value.epoch).ok_or_else(DurableCodecError::corrupt)?,
    ))
}

fn pending_to_proto(value: &StoredPendingAdmissionV1) -> wire::StoredPendingAdmissionV1 {
    wire::StoredPendingAdmissionV1 {
        identity: Some(identity_to_proto(value.identity())),
        canonical_input_hash: value.canonical_input_hash().as_bytes().to_vec(),
        admission_request_id: value.admission_request_id().as_bytes().to_vec(),
        plan: Some(plan_to_proto(value.plan())),
        logical_time: Some(timestamp_to_proto(value.logical_time().timestamp())),
        actor: Some(actor_to_proto(value.actor())),
        partition_key: value.partition_key().as_bytes().to_vec(),
        provenance_claims: Some(claims_to_proto(value.provenance_claims())),
    }
}

fn pending_from_proto(
    value: wire::StoredPendingAdmissionV1,
) -> Result<StoredPendingAdmissionV1, DurableCodecError> {
    storage_result(StoredPendingAdmissionV1::new(
        identity_from_proto(require(value.identity)?)?,
        CanonicalInputHash::from_bytes(fixed(value.canonical_input_hash)?),
        RequestId::from_bytes(fixed(value.admission_request_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        plan_from_proto(require(value.plan)?)?,
        logical_time_from_proto(require(value.logical_time)?)?,
        actor_from_proto(require(value.actor)?)?,
        PartitionKey::from_bytes(value.partition_key).map_err(|_| DurableCodecError::corrupt())?,
        claims_from_proto(require(value.provenance_claims)?)?,
    ))
}

fn pending_v2_to_proto(value: &StoredPendingAdmissionV1) -> wire::StoredPendingAdmissionV2 {
    wire::StoredPendingAdmissionV2 {
        base: Some(pending_to_proto(value)),
        causation: value.causation().map(causation_to_proto),
    }
}

fn pending_v2_from_proto(
    value: wire::StoredPendingAdmissionV2,
) -> Result<StoredPendingAdmissionV1, DurableCodecError> {
    let base = pending_from_proto(require(value.base)?)?;
    match value.causation {
        Some(causation) => storage_result(base.with_causation(causation_from_proto(causation)?)),
        None => Ok(base),
    }
}

pub(super) fn causation_to_proto(
    value: StoredCommandCausationV1,
) -> wire::StoredCommandCausationV1 {
    wire::StoredCommandCausationV1 {
        causing_event_id: Some(event_id_to_proto(value.causing_event_id())),
        root_request_id: value.root_request_id().as_bytes().to_vec(),
    }
}

fn causation_from_proto(
    value: wire::StoredCommandCausationV1,
) -> Result<StoredCommandCausationV1, DurableCodecError> {
    Ok(StoredCommandCausationV1::new(
        event_id_from_proto(require(value.causing_event_id)?)?,
        RequestId::from_bytes(fixed(value.root_request_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
    ))
}

pub(super) fn hashes_to_proto(values: &[ConflictKeyHash]) -> Vec<Vec<u8>> {
    values
        .iter()
        .map(|value| value.as_bytes().to_vec())
        .collect()
}

fn hashes_from_proto(values: Vec<Vec<u8>>) -> Result<Vec<ConflictKeyHash>, DurableCodecError> {
    values
        .into_iter()
        .map(|value| Ok(ConflictKeyHash::from_bytes(fixed(value)?)))
        .collect()
}

pub(super) fn event_to_proto(value: &StoredDurableEventV1) -> wire::StoredDurableEventV1 {
    wire::StoredDurableEventV1 {
        event_id: Some(event_id_to_proto(value.event_id())),
        event_type_id: value.event_type_id().get(),
        canonical_payload: value.payload_encoded().to_vec(),
        event_hash: value.event_hash().as_bytes().to_vec(),
    }
}

pub(super) fn event_from_proto(
    value: wire::StoredDurableEventV1,
) -> Result<StoredDurableEventV1, DurableCodecError> {
    storage_result(StoredDurableEventV1::new(
        event_id_from_proto(require(value.event_id)?)?,
        EventTypeId::new(value.event_type_id).ok_or_else(DurableCodecError::corrupt)?,
        canonical_record_from_bytes(&value.canonical_payload)?,
        EventHash::from_bytes(fixed(value.event_hash)?),
    ))
}

fn event_route_to_proto(value: StoredEventRouteV1) -> wire::StoredEventRouteV1 {
    wire::StoredEventRouteV1 {
        event_id: Some(event_id_to_proto(value.event_id())),
        event_type_id: value.event_type_id().get(),
        event_hash: value.event_hash().as_bytes().to_vec(),
    }
}

fn event_route_from_proto(
    value: wire::StoredEventRouteV1,
) -> Result<StoredEventRouteV1, DurableCodecError> {
    Ok(StoredEventRouteV1::new(
        event_id_from_proto(require(value.event_id)?)?,
        EventTypeId::new(value.event_type_id).ok_or_else(DurableCodecError::corrupt)?,
        EventHash::from_bytes(fixed(value.event_hash)?),
    ))
}

pub(super) fn dependencies_to_proto(
    value: &StoredReadDependenciesV1,
) -> wire::StoredReadDependenciesV1 {
    use wire::stored_read_dependency_v1::Dependency;
    wire::StoredReadDependenciesV1 {
        dependencies: value
            .as_slice()
            .iter()
            .map(|dependency| wire::StoredReadDependencyV1 {
                dependency: Some(match dependency {
                    StoredReadDependencyV1::EntityObservation { target, expected } => {
                        Dependency::EntityObservation(wire::EntityReadDependencyV1 {
                            target: Some(entity_target_to_proto(target)),
                            expected: Some(expected_to_proto(*expected)),
                        })
                    }
                    StoredReadDependencyV1::IndexRangeEpoch { target, expected } => {
                        Dependency::IndexRangeEpoch(wire::IndexRangeReadDependencyV1 {
                            canonical_index_range_prefix: target.as_bytes().to_vec(),
                            expected: Some(epoch_to_proto(*expected)),
                        })
                    }
                }),
            })
            .collect(),
    }
}

fn dependencies_from_proto(
    value: wire::StoredReadDependenciesV1,
) -> Result<StoredReadDependenciesV1, DurableCodecError> {
    use wire::stored_read_dependency_v1::Dependency;
    let raw = value
        .dependencies
        .into_iter()
        .map(|value| match require(value.dependency)? {
            Dependency::EntityObservation(value) => Ok(StoredReadDependencyV1::EntityObservation {
                target: entity_target_from_proto(require(value.target)?)?,
                expected: expected_from_proto(require(value.expected)?)?,
            }),
            Dependency::IndexRangeEpoch(value) => Ok(StoredReadDependencyV1::IndexRangeEpoch {
                target: structural_prefix_from_bytes(value.canonical_index_range_prefix)?,
                expected: epoch_from_proto(require(value.expected)?)?,
            }),
        })
        .collect::<Result<Vec<_>, DurableCodecError>>()?;
    let checked = storage_result(StoredReadDependenciesV1::new(raw.clone()))?;
    if checked.as_slice() != raw {
        return Err(DurableCodecError::corrupt());
    }
    Ok(checked)
}

/// Encodes one authoritative entity row.
pub fn encode_entity_record_v1(
    value: &StoredEntityRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(ENTITY, &entity_to_proto(value))
}

/// Decodes one authoritative entity row.
pub fn decode_entity_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredEntityRecordV1>, DurableCodecError> {
    decode_message::<wire::StoredEntityRecordV1, _, _>(ENTITY, encoded, entity_from_proto)
}

/// Encodes one current authoritative index-entry post-image.
pub fn encode_index_entry_v2(
    value: &StoredIndexEntryV2,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(INDEX_ENTRY, &index_entry_to_proto(value))
}

/// Decodes one current authoritative index-entry post-image.
pub fn decode_index_entry_v2(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredIndexEntryV2>, DurableCodecError> {
    decode_message::<wire::StoredIndexEntryV2, _, _>(INDEX_ENTRY, encoded, index_entry_from_proto)
}

/// Decodes one legacy migration-only index-entry post-image.
pub fn decode_index_entry_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredIndexEntryV1>, DurableCodecError> {
    decode_message::<wire::StoredIndexEntryV1, _, _>(
        LEGACY_INDEX_ENTRY,
        encoded,
        legacy_index_entry_from_proto,
    )
}

/// Decodes and inseparably binds one physical migration-scan row.
pub fn decode_index_migration_row(
    physical_key: &IndexEntryKey,
    observed_envelope: &[u8],
) -> Result<IndexMigrationRowEvidence, DurableCodecError> {
    let decoded = riffdb_proto::durable::readable_record_registry()
        .decode(observed_envelope)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let row = match decoded.record_type() {
        LEGACY_INDEX_ENTRY => IndexMigrationSemanticRow::V1(legacy_index_entry_from_proto(
            wire::StoredIndexEntryV1::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?,
        )?),
        INDEX_ENTRY => IndexMigrationSemanticRow::V2(index_entry_from_proto(
            wire::StoredIndexEntryV2::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?,
        )?),
        _ => {
            return Err(DurableCodecError::new(
                super::DurableCodecErrorKind::UnexpectedRecordType,
            ));
        }
    };
    if row.key() != physical_key {
        return Err(DurableCodecError::corrupt());
    }
    let conservative_v2_envelope_charge = conservative_index_v2_envelope_charge(&row)?;
    IndexMigrationRowEvidence::from_codec_checked_parts(
        physical_key.clone(),
        row,
        observed_envelope.to_vec(),
        conservative_v2_envelope_charge,
    )
    .map_err(DurableCodecError::from_storage_value)
}

fn conservative_index_v2_envelope_charge(
    row: &IndexMigrationSemanticRow,
) -> Result<crate::EncodedContentCharge, DurableCodecError> {
    let message = wire::StoredIndexEntryV2 {
        index_entry_key: row.key().as_bytes().to_vec(),
        schema_binding: Some(binding_to_proto(row.schema_binding())),
        canonical_covered_values: encode_canonical_record(row.covered_values())
            .map_err(|_| DurableCodecError::invariant())?,
        partition_key: vec![0; MAX_KEY_BYTES],
    };
    let schema = riffdb_proto::durable::current_record_schema(INDEX_ENTRY)
        .ok_or_else(DurableCodecError::invariant)?;
    let bytes =
        riffdb_proto::envelope::maximum_encoded_compact_record_bytes(schema, message.encoded_len())
            .map_err(DurableCodecError::from_encode_envelope)?;
    crate::EncodedContentCharge::new(bytes).ok_or_else(DurableCodecError::invariant)
}

#[cfg(any(test, feature = "test-fixtures"))]
fn encode_index_entry_v1_fixture_inner(
    value: &StoredIndexEntryV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let message = wire::StoredIndexEntryV1 {
        index_entry_key: value.key().as_bytes().to_vec(),
        schema_binding: Some(binding_to_proto(value.schema_binding())),
        canonical_covered_values: encode_canonical_record(value.covered_values())
            .expect("checked index record must encode"),
    };
    let schema = riffdb_proto::durable::readable_record_schema(LEGACY_INDEX_ENTRY)
        .ok_or_else(DurableCodecError::invariant)?;
    let bytes = riffdb_proto::envelope::encode(schema, &message.encode_to_vec())
        .map_err(DurableCodecError::from_encode_envelope)?;
    let charge =
        crate::EncodedContentCharge::new(bytes.len()).ok_or_else(DurableCodecError::invariant)?;
    Ok(CanonicalStoredEnvelopeV1 { bytes, charge })
}

#[cfg(test)]
pub(super) fn encode_index_entry_v1(
    value: &StoredIndexEntryV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_index_entry_v1_fixture_inner(value)
}

/// Encodes one legacy V1 index row for migration and compatibility fixtures.
///
/// This helper is absent unless the non-default `test-fixtures` feature is enabled. It must not
/// be used by a production writer; the writable durable registry selects V2 exclusively.
#[cfg(feature = "test-fixtures")]
pub fn encode_index_entry_v1_fixture(
    value: &StoredIndexEntryV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_index_entry_v1_fixture_inner(value)
}

/// Encodes one authoritative index-range epoch post-image.
pub fn encode_index_epoch_v1(
    value: &StoredIndexEpochV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(INDEX_EPOCH, &index_epoch_to_proto(value))
}

/// Decodes one authoritative index-range epoch post-image.
pub fn decode_index_epoch_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredIndexEpochV1>, DurableCodecError> {
    decode_message::<wire::StoredIndexGenerationV2, _, _>(
        INDEX_EPOCH,
        encoded,
        index_epoch_from_proto,
    )
}

/// Decodes one historical prefix epoch for the bounded generation migration.
pub fn decode_legacy_index_epoch_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<LegacyStoredIndexEpochV1>, DurableCodecError> {
    decode_message::<wire::StoredIndexEpochV1, _, _>(
        LEGACY_INDEX_EPOCH,
        encoded,
        legacy_index_epoch_from_proto,
    )
}

#[cfg(any(test, feature = "test-fixtures"))]
/// Encodes one historical prefix epoch for migration and compatibility fixtures.
pub fn encode_legacy_index_epoch_v1_fixture(
    value: &LegacyStoredIndexEpochV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let message = legacy_index_epoch_to_proto(value);
    let schema = riffdb_proto::durable::readable_record_schema(LEGACY_INDEX_EPOCH)
        .ok_or_else(DurableCodecError::invariant)?;
    let bytes = riffdb_proto::envelope::encode(schema, &message.encode_to_vec())
        .map_err(DurableCodecError::from_encode_envelope)?;
    let charge =
        crate::EncodedContentCharge::new(bytes.len()).ok_or_else(DurableCodecError::invariant)?;
    Ok(CanonicalStoredEnvelopeV1 { bytes, charge })
}

/// Encodes one pending command admission.
pub fn encode_pending_admission_v1(
    value: &StoredPendingAdmissionV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(PENDING_V2, &pending_v2_to_proto(value))
}

#[cfg(test)]
pub(super) fn encode_pending_admission_legacy_v1_fixture(
    value: &StoredPendingAdmissionV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    super::encode_legacy_message(PENDING, &pending_to_proto(value))
}

/// Decodes one pending command admission.
pub fn decode_pending_admission_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredPendingAdmissionV1>, DurableCodecError> {
    if decode_record_variant(encoded, PENDING_V2, PENDING)? {
        decode_message::<wire::StoredPendingAdmissionV2, _, _>(
            PENDING_V2,
            encoded,
            pending_v2_from_proto,
        )
    } else {
        decode_message::<wire::StoredPendingAdmissionV1, _, _>(PENDING, encoded, pending_from_proto)
    }
}

/// Encodes one terminal deterministic execution failure.
pub fn encode_execution_failed_v1(
    value: &StoredExecutionFailedV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        EXECUTION_FAILED_V2,
        &wire::StoredExecutionFailedV2 {
            pending: Some(pending_v2_to_proto(value.pending())),
            code: execution_code_to_proto(value.code()),
        },
    )
}

#[cfg(test)]
pub(super) fn encode_execution_failed_legacy_v1_fixture(
    value: &StoredExecutionFailedV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    super::encode_legacy_message(
        EXECUTION_FAILED,
        &wire::StoredExecutionFailedV1 {
            pending: Some(pending_to_proto(value.pending())),
            code: execution_code_to_proto(value.code()),
        },
    )
}

/// Decodes one terminal deterministic execution failure.
pub fn decode_execution_failed_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredExecutionFailedV1>, DurableCodecError> {
    if decode_record_variant(encoded, EXECUTION_FAILED_V2, EXECUTION_FAILED)? {
        decode_message::<wire::StoredExecutionFailedV2, _, _>(
            EXECUTION_FAILED_V2,
            encoded,
            |value| {
                Ok(StoredExecutionFailedV1::new(
                    pending_v2_from_proto(require(value.pending)?)?,
                    execution_code_from_proto(value.code)?,
                ))
            },
        )
    } else {
        decode_message::<wire::StoredExecutionFailedV1, _, _>(EXECUTION_FAILED, encoded, |value| {
            Ok(StoredExecutionFailedV1::new(
                pending_from_proto(require(value.pending)?)?,
                execution_code_from_proto(value.code)?,
            ))
        })
    }
}

/// Encodes the one complete terminal idempotency outcome row.
pub fn encode_stored_outcome_v1(
    value: &StoredOutcomeV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        OUTCOME_V2,
        &wire::StoredOutcomeV2 {
            base: Some(outcome_to_proto(value)),
            causation: value.causation().map(causation_to_proto),
        },
    )
}

#[cfg(test)]
pub(super) fn encode_stored_outcome_legacy_v1_fixture(
    value: &StoredOutcomeV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    super::encode_legacy_message(OUTCOME, &outcome_to_proto(value))
}

fn outcome_to_proto(value: &StoredOutcomeV1) -> wire::StoredOutcomeV1 {
    wire::StoredOutcomeV1 {
        identity: Some(identity_to_proto(value.identity())),
        commit_sequence: value.commit_sequence().get(),
        admission_request_id: value.admission_request_id().as_bytes().to_vec(),
        plan: Some(plan_to_proto(value.plan())),
        canonical_input_hash: value.canonical_input_hash().as_bytes().to_vec(),
        actor: Some(actor_to_proto(value.actor())),
        logical_time: Some(timestamp_to_proto(value.logical_time().timestamp())),
        partition_hash: value.partition_hash().as_bytes().to_vec(),
        conflict_hashes: hashes_to_proto(value.conflict_hashes()),
        declared_outcome: Some(declared_outcome_to_proto(value.declared_outcome())),
        admitted_claims: Some(claims_to_proto(value.admitted_claims())),
        provenance_id: value.provenance_id().as_bytes().to_vec(),
        durability_mode: durability_to_proto(value.durability_mode()),
        partition_key: value.partition_key().as_bytes().to_vec(),
    }
}

fn outcome_from_proto(value: wire::StoredOutcomeV1) -> Result<StoredOutcomeV1, DurableCodecError> {
    storage_result(StoredOutcomeV1::new(
        identity_from_proto(require(value.identity)?)?,
        CommitSequence::new(value.commit_sequence).ok_or_else(DurableCodecError::corrupt)?,
        RequestId::from_bytes(fixed(value.admission_request_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        plan_from_proto(require(value.plan)?)?,
        CanonicalInputHash::from_bytes(fixed(value.canonical_input_hash)?),
        actor_from_proto(require(value.actor)?)?,
        logical_time_from_proto(require(value.logical_time)?)?,
        PartitionKey::from_bytes(value.partition_key).map_err(|_| DurableCodecError::corrupt())?,
        PartitionKeyHash::from_bytes(fixed(value.partition_hash)?),
        hashes_from_proto(value.conflict_hashes)?,
        declared_outcome_from_proto(require(value.declared_outcome)?)?,
        claims_from_proto(require(value.admitted_claims)?)?,
        ProvenanceId::from_bytes(fixed(value.provenance_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        durability_from_proto(value.durability_mode)?,
    ))
}

/// Decodes the one complete terminal idempotency outcome row.
pub fn decode_stored_outcome_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredOutcomeV1>, DurableCodecError> {
    if decode_record_variant(encoded, OUTCOME_V2, OUTCOME)? {
        decode_message::<wire::StoredOutcomeV2, _, _>(OUTCOME_V2, encoded, |value| {
            let base = outcome_from_proto(require(value.base)?)?;
            match value.causation {
                Some(causation) => {
                    storage_result(base.with_causation(causation_from_proto(causation)?))
                }
                None => Ok(base),
            }
        })
    } else {
        decode_message::<wire::StoredOutcomeV1, _, _>(OUTCOME, encoded, outcome_from_proto)
    }
}

/// Encodes one standalone authoritative durable event.
pub fn encode_durable_event_v1(
    value: &StoredDurableEventV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(EVENT, &event_to_proto(value))
}

/// Decodes one standalone authoritative durable event and verifies its hash.
pub fn decode_durable_event_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredDurableEventV1>, DurableCodecError> {
    decode_message::<wire::StoredDurableEventV1, _, _>(EVENT, encoded, event_from_proto)
}

/// Encodes one payload-free partition event route.
pub fn encode_event_route_v1(
    value: StoredEventRouteV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(EVENT_ROUTE, &event_route_to_proto(value))
}

/// Decodes one payload-free partition event route.
pub fn decode_event_route_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredEventRouteV1>, DurableCodecError> {
    decode_message::<wire::StoredEventRouteV1, _, _>(EVENT_ROUTE, encoded, event_route_from_proto)
}

/// Encodes one immutable command provenance record.
pub fn encode_provenance_record_v1(
    value: &StoredProvenanceRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        PROVENANCE_V2,
        &wire::StoredProvenanceRecordV2 {
            base: Some(provenance_to_proto(value)),
            causation: value.causation().map(causation_to_proto),
        },
    )
}

#[cfg(test)]
pub(super) fn encode_provenance_record_legacy_v1_fixture(
    value: &StoredProvenanceRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    super::encode_legacy_message(PROVENANCE, &provenance_to_proto(value))
}

fn provenance_to_proto(value: &StoredProvenanceRecordV1) -> wire::StoredProvenanceRecordV1 {
    wire::StoredProvenanceRecordV1 {
        provenance_id: value.provenance_id().as_bytes().to_vec(),
        commit_sequence: value.commit_sequence().get(),
        identity: Some(identity_to_proto(value.identity())),
        admission_request_id: value.admission_request_id().as_bytes().to_vec(),
        plan: Some(plan_to_proto(value.plan())),
        canonical_input_hash: value.canonical_input_hash().as_bytes().to_vec(),
        actor: Some(actor_to_proto(value.actor())),
        logical_time: Some(timestamp_to_proto(value.logical_time().timestamp())),
        partition_hash: value.partition_hash().as_bytes().to_vec(),
        conflict_hashes: hashes_to_proto(value.conflict_hashes()),
        outcome_id: value.outcome_id().get(),
        affected_entities: value
            .affected_entities()
            .iter()
            .map(|value| wire::AffectedEntityV1 {
                target: Some(entity_target_to_proto(value.target())),
                entity_version: value.entity_version().get(),
            })
            .collect(),
        event_ids: value
            .event_ids()
            .iter()
            .copied()
            .map(event_id_to_proto)
            .collect(),
        admitted_claims: Some(claims_to_proto(value.admitted_claims())),
    }
}

/// Decodes one immutable command provenance record.
pub fn decode_provenance_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredProvenanceRecordV1>, DurableCodecError> {
    if decode_record_variant(encoded, PROVENANCE_V2, PROVENANCE)? {
        decode_message::<wire::StoredProvenanceRecordV2, _, _>(PROVENANCE_V2, encoded, |value| {
            let base = provenance_from_proto(require(value.base)?)?;
            match value.causation {
                Some(causation) => {
                    storage_result(base.with_causation(causation_from_proto(causation)?))
                }
                None => Ok(base),
            }
        })
    } else {
        decode_message::<wire::StoredProvenanceRecordV1, _, _>(
            PROVENANCE,
            encoded,
            provenance_from_proto,
        )
    }
}

fn provenance_from_proto(
    value: wire::StoredProvenanceRecordV1,
) -> Result<StoredProvenanceRecordV1, DurableCodecError> {
    let affected_entities = value
        .affected_entities
        .into_iter()
        .map(|value| {
            Ok(AffectedEntityV1::from_stored_parts(
                entity_target_from_proto(require(value.target)?)?,
                EntityVersion::new(value.entity_version).ok_or_else(DurableCodecError::corrupt)?,
            ))
        })
        .collect::<Result<Vec<_>, DurableCodecError>>()?;
    let event_ids = value
        .event_ids
        .into_iter()
        .map(event_id_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    storage_result(StoredProvenanceRecordV1::new(
        ProvenanceId::from_bytes(fixed(value.provenance_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        CommitSequence::new(value.commit_sequence).ok_or_else(DurableCodecError::corrupt)?,
        identity_from_proto(require(value.identity)?)?,
        RequestId::from_bytes(fixed(value.admission_request_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        plan_from_proto(require(value.plan)?)?,
        CanonicalInputHash::from_bytes(fixed(value.canonical_input_hash)?),
        actor_from_proto(require(value.actor)?)?,
        logical_time_from_proto(require(value.logical_time)?)?,
        PartitionKeyHash::from_bytes(fixed(value.partition_hash)?),
        hashes_from_proto(value.conflict_hashes)?,
        OutcomeId::new(value.outcome_id).ok_or_else(DurableCodecError::corrupt)?,
        affected_entities,
        event_ids,
        claims_from_proto(require(value.admitted_claims)?)?,
    ))
}

fn entity_reference_to_proto(
    value: &CommittedEntityReferenceV2,
) -> wire::CommittedEntityReferenceV2 {
    wire::CommittedEntityReferenceV2 {
        target: Some(entity_target_to_proto(value.target())),
        entity_version: value.entity_version().get(),
        post_image_hash: value.post_image_hash().as_bytes().to_vec(),
    }
}

fn entity_reference_from_proto(
    value: wire::CommittedEntityReferenceV2,
) -> Result<CommittedEntityReferenceV2, DurableCodecError> {
    let target = entity_target_from_proto(require(value.target)?)?;
    let entity_version =
        EntityVersion::new(value.entity_version).ok_or_else(DurableCodecError::corrupt)?;
    let post_image_hash = EntityRecordHash::from_bytes(fixed(value.post_image_hash)?);
    Ok(CommittedEntityReferenceV2::new(
        target,
        entity_version,
        post_image_hash,
    ))
}

fn entity_references_from_mutations(
    mutations: Vec<CommittedEntityMutationV1>,
) -> Result<Vec<CommittedEntityReferenceV2>, DurableCodecError> {
    mutations
        .into_iter()
        .map(|mutation| {
            // Hash once: from_mutation already derives the post-image hash.
            storage_result(CommittedEntityReferenceV2::from_mutation(&mutation))
        })
        .collect()
}

/// Test helper: duplicates the first entity reference in a V3 commit envelope.
#[cfg(any(test, feature = "test-fixtures"))]
pub fn inject_duplicate_entity_reference_v3(
    encoded: &[u8],
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let item = decode_message::<wire::StoredCommitRecordV3, _, _>(COMMIT, encoded, Ok)?;
    let mut value = item.into_parts().0;
    if let Some(first) = value.entity_references.first().cloned() {
        value.entity_references.push(first);
    }
    encode_message(COMMIT, &value)
}

/// Rewrites a durable commit envelope to current V3 entity-reference form.
///
/// Self-contained: hashes embedded post-images from V1/V2 payloads. Does **not**
/// join the EVENTS table — event damage must surface as a later structural finding,
/// not as a migration open failure.
pub fn transcode_commit_to_entity_reference_v3(
    encoded: &[u8],
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if let Ok(item) = decode_message::<wire::StoredCommitRecordV3, _, _>(COMMIT, encoded, Ok) {
        return encode_message(COMMIT, &item.into_parts().0);
    }

    if let Ok(item) = decode_message::<wire::StoredCommitRecordV2, _, _>(COMMIT_V2, encoded, Ok) {
        let v2 = item.into_parts().0;
        let mutations = v2
            .mutations
            .into_iter()
            .map(|value| {
                storage_result(CommittedEntityMutationV1::new(
                    expected_from_proto(require(value.expected)?)?,
                    entity_from_proto(require(value.post_image)?)?,
                ))
            })
            .collect::<Result<Vec<_>, DurableCodecError>>()?;
        let entity_references = entity_references_from_mutations(mutations)?;
        return encode_message(
            COMMIT,
            &wire::StoredCommitRecordV3 {
                commit_sequence: v2.commit_sequence,
                admission_request_id: v2.admission_request_id,
                plan: v2.plan,
                canonical_input_hash: v2.canonical_input_hash,
                actor: v2.actor,
                logical_time: v2.logical_time,
                partition_hash: v2.partition_hash,
                conflict_hashes: v2.conflict_hashes,
                read_dependencies: v2.read_dependencies,
                entity_references: entity_references
                    .iter()
                    .map(entity_reference_to_proto)
                    .collect(),
                event_references: v2.event_references,
                declared_outcome: v2.declared_outcome,
                provenance_id: v2.provenance_id,
                outbox_event_ids: v2.outbox_event_ids,
                durability_mode: v2.durability_mode,
            },
        );
    }

    let item = decode_message::<wire::StoredCommitRecordV1, _, _>(LEGACY_COMMIT, encoded, Ok)?;
    let v1 = item.into_parts().0;
    let mutations = v1
        .mutations
        .into_iter()
        .map(|value| {
            storage_result(CommittedEntityMutationV1::new(
                expected_from_proto(require(value.expected)?)?,
                entity_from_proto(require(value.post_image)?)?,
            ))
        })
        .collect::<Result<Vec<_>, DurableCodecError>>()?;
    let entity_references = entity_references_from_mutations(mutations)?;
    let event_references = v1
        .events
        .iter()
        .map(|event| {
            let event_id = event_id_from_proto(require(event.event_id)?)?;
            let event_hash = EventHash::from_bytes(fixed(event.event_hash.clone())?);
            Ok(event_reference_to_proto(EventReferenceV2::new(
                event_id, event_hash,
            )))
        })
        .collect::<Result<Vec<_>, DurableCodecError>>()?;
    encode_message(
        COMMIT,
        &wire::StoredCommitRecordV3 {
            commit_sequence: v1.commit_sequence,
            admission_request_id: v1.admission_request_id,
            plan: v1.plan,
            canonical_input_hash: v1.canonical_input_hash,
            actor: v1.actor,
            logical_time: v1.logical_time,
            partition_hash: v1.partition_hash,
            conflict_hashes: v1.conflict_hashes,
            read_dependencies: v1.read_dependencies,
            entity_references: entity_references
                .iter()
                .map(entity_reference_to_proto)
                .collect(),
            event_references,
            declared_outcome: v1.declared_outcome,
            provenance_id: v1.provenance_id,
            outbox_event_ids: v1.outbox_event_ids,
            durability_mode: v1.durability_mode,
        },
    )
}

/// Encodes one complete authoritative commit-log record.
pub fn encode_commit_record_v1(
    value: &StoredCommitRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        COMMIT,
        &wire::StoredCommitRecordV3 {
            commit_sequence: value.commit_sequence().get(),
            admission_request_id: value.admission_request_id().as_bytes().to_vec(),
            plan: Some(plan_to_proto(value.plan())),
            canonical_input_hash: value.canonical_input_hash().as_bytes().to_vec(),
            actor: Some(actor_to_proto(value.actor())),
            logical_time: Some(timestamp_to_proto(value.logical_time().timestamp())),
            partition_hash: value.partition_hash().as_bytes().to_vec(),
            conflict_hashes: hashes_to_proto(value.conflict_hashes()),
            read_dependencies: Some(dependencies_to_proto(value.read_dependencies())),
            entity_references: value
                .entity_references()
                .iter()
                .map(entity_reference_to_proto)
                .collect(),
            event_references: value
                .event_references()
                .into_iter()
                .map(event_reference_to_proto)
                .collect(),
            declared_outcome: Some(declared_outcome_to_proto(value.declared_outcome())),
            provenance_id: value.provenance_id().as_bytes().to_vec(),
            outbox_event_ids: value
                .outbox_event_ids()
                .iter()
                .copied()
                .map(event_id_to_proto)
                .collect(),
            durability_mode: durability_to_proto(value.durability_mode()),
        },
    )
}

#[cfg(test)]
pub(super) fn encode_commit_record_legacy_v1(
    value: &StoredCommitRecordV1,
    mutations: &[CommittedEntityMutationV1],
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    super::encode_legacy_message(
        LEGACY_COMMIT,
        &wire::StoredCommitRecordV1 {
            commit_sequence: value.commit_sequence().get(),
            admission_request_id: value.admission_request_id().as_bytes().to_vec(),
            plan: Some(plan_to_proto(value.plan())),
            canonical_input_hash: value.canonical_input_hash().as_bytes().to_vec(),
            actor: Some(actor_to_proto(value.actor())),
            logical_time: Some(timestamp_to_proto(value.logical_time().timestamp())),
            partition_hash: value.partition_hash().as_bytes().to_vec(),
            conflict_hashes: hashes_to_proto(value.conflict_hashes()),
            read_dependencies: Some(dependencies_to_proto(value.read_dependencies())),
            mutations: mutations
                .iter()
                .map(|value| wire::CommittedEntityMutationV1 {
                    expected: Some(expected_to_proto(value.expected())),
                    post_image: Some(entity_to_proto(value.post_image())),
                })
                .collect(),
            events: value.events().iter().map(event_to_proto).collect(),
            declared_outcome: Some(declared_outcome_to_proto(value.declared_outcome())),
            provenance_id: value.provenance_id().as_bytes().to_vec(),
            outbox_event_ids: value
                .outbox_event_ids()
                .iter()
                .copied()
                .map(event_id_to_proto)
                .collect(),
            durability_mode: durability_to_proto(value.durability_mode()),
        },
    )
}

/// Rewraps a legacy V1 commit envelope as compact V2 with embedded mutations
/// retained from the V1 payload (entity-reference migration fixture aid).
#[cfg(any(test, feature = "test-fixtures"))]
pub fn rewrap_legacy_commit_v1_as_v2_fixture(
    legacy_v1_envelope: &[u8],
) -> Result<(CanonicalStoredEnvelopeV1, CanonicalStoredEnvelopeV1), DurableCodecError> {
    let decoded = riffdb_proto::durable::readable_record_registry()
        .decode(legacy_v1_envelope)
        .map_err(DurableCodecError::from_decode_envelope)?;
    if decoded.record_type() != LEGACY_COMMIT {
        return Err(DurableCodecError::new(
            super::DurableCodecErrorKind::UnexpectedRecordType,
        ));
    }
    let v1 = wire::StoredCommitRecordV1::decode(decoded.payload())
        .map_err(|_| DurableCodecError::corrupt())?;
    let event = v1
        .events
        .first()
        .cloned()
        .ok_or_else(DurableCodecError::corrupt)?;
    let event_envelope = encode_message(EVENT, &event)?;
    let v2 = wire::StoredCommitRecordV2 {
        commit_sequence: v1.commit_sequence,
        admission_request_id: v1.admission_request_id,
        plan: v1.plan,
        canonical_input_hash: v1.canonical_input_hash,
        actor: v1.actor,
        logical_time: v1.logical_time,
        partition_hash: v1.partition_hash,
        conflict_hashes: v1.conflict_hashes,
        read_dependencies: v1.read_dependencies,
        mutations: v1.mutations,
        event_references: vec![wire::EventReferenceV2 {
            event_id: event.event_id,
            event_hash: event.event_hash,
        }],
        declared_outcome: v1.declared_outcome,
        provenance_id: v1.provenance_id,
        outbox_event_ids: v1.outbox_event_ids,
        durability_mode: v1.durability_mode,
    };
    Ok((
        super::encode_readable_compact_message(COMMIT_V2, &v2)?,
        event_envelope,
    ))
}

/// Encodes a rev-2 commit with embedded entity post-images (migration/fixture aid).
#[cfg(any(test, feature = "test-fixtures"))]
pub fn encode_commit_record_v2_fixture(
    value: &StoredCommitRecordV1,
    mutations: &[CommittedEntityMutationV1],
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    super::encode_readable_compact_message(
        COMMIT_V2,
        &wire::StoredCommitRecordV2 {
            commit_sequence: value.commit_sequence().get(),
            admission_request_id: value.admission_request_id().as_bytes().to_vec(),
            plan: Some(plan_to_proto(value.plan())),
            canonical_input_hash: value.canonical_input_hash().as_bytes().to_vec(),
            actor: Some(actor_to_proto(value.actor())),
            logical_time: Some(timestamp_to_proto(value.logical_time().timestamp())),
            partition_hash: value.partition_hash().as_bytes().to_vec(),
            conflict_hashes: hashes_to_proto(value.conflict_hashes()),
            read_dependencies: Some(dependencies_to_proto(value.read_dependencies())),
            mutations: mutations
                .iter()
                .map(|value| wire::CommittedEntityMutationV1 {
                    expected: Some(expected_to_proto(value.expected())),
                    post_image: Some(entity_to_proto(value.post_image())),
                })
                .collect(),
            event_references: value
                .event_references()
                .into_iter()
                .map(event_reference_to_proto)
                .collect(),
            declared_outcome: Some(declared_outcome_to_proto(value.declared_outcome())),
            provenance_id: value.provenance_id().as_bytes().to_vec(),
            outbox_event_ids: value
                .outbox_event_ids()
                .iter()
                .copied()
                .map(event_id_to_proto)
                .collect(),
            durability_mode: durability_to_proto(value.durability_mode()),
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn commit_from_wire_parts(
    commit_sequence: u64,
    admission_request_id: Vec<u8>,
    plan: Option<wire::ExecutablePlanRefV1>,
    canonical_input_hash: Vec<u8>,
    actor: Option<wire::AdmittedActorContextV1>,
    logical_time: Option<wire::TimestampV1>,
    partition_hash: Vec<u8>,
    conflict_hashes: Vec<Vec<u8>>,
    read_dependencies: Option<wire::StoredReadDependenciesV1>,
    entity_references: Vec<CommittedEntityReferenceV2>,
    events: Vec<StoredDurableEventV1>,
    declared_outcome: Option<wire::DeclaredOutcomeV1>,
    provenance_id: Vec<u8>,
    outbox_event_ids: Vec<wire::EventIdV1>,
    durability_mode: i32,
) -> Result<StoredCommitRecordV1, DurableCodecError> {
    let outbox_event_ids = outbox_event_ids
        .into_iter()
        .map(event_id_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    storage_result(StoredCommitRecordV1::new(
        CommitSequence::new(commit_sequence).ok_or_else(DurableCodecError::corrupt)?,
        RequestId::from_bytes(fixed(admission_request_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        plan_from_proto(require(plan)?)?,
        CanonicalInputHash::from_bytes(fixed(canonical_input_hash)?),
        actor_from_proto(require(actor)?)?,
        logical_time_from_proto(require(logical_time)?)?,
        PartitionKeyHash::from_bytes(fixed(partition_hash)?),
        hashes_from_proto(conflict_hashes)?,
        dependencies_from_proto(require(read_dependencies)?)?,
        entity_references,
        events,
        declared_outcome_from_proto(require(declared_outcome)?)?,
        ProvenanceId::from_bytes(fixed(provenance_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        outbox_event_ids,
        durability_from_proto(durability_mode)?,
    ))
}

/// Decodes one complete authoritative commit-log record (legacy V1 with embedded events).
pub fn decode_commit_record_legacy_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCommitRecordV1>, DurableCodecError> {
    decode_message::<wire::StoredCommitRecordV1, _, _>(LEGACY_COMMIT, encoded, |value| {
        let mutations = value
            .mutations
            .into_iter()
            .map(|value| {
                storage_result(CommittedEntityMutationV1::new(
                    expected_from_proto(require(value.expected)?)?,
                    entity_from_proto(require(value.post_image)?)?,
                ))
            })
            .collect::<Result<Vec<_>, DurableCodecError>>()?;
        let entity_references = entity_references_from_mutations(mutations)?;
        let events = value
            .events
            .into_iter()
            .map(event_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        commit_from_wire_parts(
            value.commit_sequence,
            value.admission_request_id,
            value.plan,
            value.canonical_input_hash,
            value.actor,
            value.logical_time,
            value.partition_hash,
            value.conflict_hashes,
            value.read_dependencies,
            entity_references,
            events,
            value.declared_outcome,
            value.provenance_id,
            value.outbox_event_ids,
            value.durability_mode,
        )
    })
}

/// Decodes a rev-2 commit (embedded entity post-images, event references).
pub fn decode_commit_record_v2(
    encoded: &[u8],
    events: Vec<StoredDurableEventV1>,
) -> Result<EncodedPageItem<StoredCommitRecordV1>, DurableCodecError> {
    decode_message::<wire::StoredCommitRecordV2, _, _>(COMMIT_V2, encoded, |value| {
        let mutations = value
            .mutations
            .into_iter()
            .map(|value| {
                storage_result(CommittedEntityMutationV1::new(
                    expected_from_proto(require(value.expected)?)?,
                    entity_from_proto(require(value.post_image)?)?,
                ))
            })
            .collect::<Result<Vec<_>, DurableCodecError>>()?;
        let entity_references = entity_references_from_mutations(mutations)?;
        let references = value
            .event_references
            .into_iter()
            .map(event_reference_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        if references.len() != events.len()
            || references
                .iter()
                .zip(&events)
                .any(|(reference, event)| !reference.matches(event))
        {
            return Err(DurableCodecError::corrupt());
        }
        commit_from_wire_parts(
            value.commit_sequence,
            value.admission_request_id,
            value.plan,
            value.canonical_input_hash,
            value.actor,
            value.logical_time,
            value.partition_hash,
            value.conflict_hashes,
            value.read_dependencies,
            entity_references,
            events,
            value.declared_outcome,
            value.provenance_id,
            value.outbox_event_ids,
            value.durability_mode,
        )
    })
}

/// Decodes a current V3 payload-free commit reference record using event rows
/// loaded from the same authoritative snapshot.
pub fn decode_commit_record_v3(
    encoded: &[u8],
    events: Vec<StoredDurableEventV1>,
) -> Result<EncodedPageItem<StoredCommitRecordV1>, DurableCodecError> {
    decode_message::<wire::StoredCommitRecordV3, _, _>(COMMIT, encoded, |value| {
        let entity_references = value
            .entity_references
            .into_iter()
            .map(entity_reference_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let references = value
            .event_references
            .into_iter()
            .map(event_reference_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        if references.len() != events.len()
            || references
                .iter()
                .zip(&events)
                .any(|(reference, event)| !reference.matches(event))
        {
            return Err(DurableCodecError::corrupt());
        }
        commit_from_wire_parts(
            value.commit_sequence,
            value.admission_request_id,
            value.plan,
            value.canonical_input_hash,
            value.actor,
            value.logical_time,
            value.partition_hash,
            value.conflict_hashes,
            value.read_dependencies,
            entity_references,
            events,
            value.declared_outcome,
            value.provenance_id,
            value.outbox_event_ids,
            value.durability_mode,
        )
    })
}

/// Decodes either historical embedded-event commits or current references,
/// proving the supplied authoritative event rows in both cases.
pub fn decode_commit_record_with_events(
    encoded: &[u8],
    events: Vec<StoredDurableEventV1>,
) -> Result<EncodedPageItem<StoredCommitRecordV1>, DurableCodecError> {
    match decode_commit_record_v3(encoded, events.clone()) {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == super::DurableCodecErrorKind::UnexpectedRecordType => {
            match decode_commit_record_v2(encoded, events.clone()) {
                Ok(value) => Ok(value),
                Err(error)
                    if error.kind() == super::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    let decoded = decode_commit_record_legacy_v1(encoded)?;
                    if decoded.value().events() != events {
                        return Err(DurableCodecError::corrupt());
                    }
                    Ok(decoded)
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Decodes only the ordered event references needed to materialize a commit
/// from the authoritative event table.
pub fn decode_commit_event_references(
    encoded: &[u8],
) -> Result<EncodedPageItem<Vec<EventReferenceV2>>, DurableCodecError> {
    match decode_message::<wire::StoredCommitRecordV3, _, _>(COMMIT, encoded, |value| {
        value
            .event_references
            .into_iter()
            .map(event_reference_from_proto)
            .collect()
    }) {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == super::DurableCodecErrorKind::UnexpectedRecordType => {
            match decode_message::<wire::StoredCommitRecordV2, _, _>(COMMIT_V2, encoded, |value| {
                value
                    .event_references
                    .into_iter()
                    .map(event_reference_from_proto)
                    .collect()
            }) {
                Ok(value) => Ok(value),
                Err(error)
                    if error.kind() == super::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    let decoded = decode_commit_record_legacy_v1(encoded)?;
                    let (commit, charge) = decoded.into_parts();
                    Ok(EncodedPageItem::new(commit.event_references(), charge))
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Decodes only the ordered entity references from a commit row (no event join).
pub fn decode_commit_entity_references(
    encoded: &[u8],
) -> Result<EncodedPageItem<Vec<CommittedEntityReferenceV2>>, DurableCodecError> {
    match decode_message::<wire::StoredCommitRecordV3, _, _>(COMMIT, encoded, |value| {
        value
            .entity_references
            .into_iter()
            .map(entity_reference_from_proto)
            .collect()
    }) {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == super::DurableCodecErrorKind::UnexpectedRecordType => {
            match decode_message::<wire::StoredCommitRecordV2, _, _>(COMMIT_V2, encoded, |value| {
                let mutations = value
                    .mutations
                    .into_iter()
                    .map(|value| {
                        storage_result(CommittedEntityMutationV1::new(
                            expected_from_proto(require(value.expected)?)?,
                            entity_from_proto(require(value.post_image)?)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, DurableCodecError>>()?;
                entity_references_from_mutations(mutations)
            }) {
                Ok(value) => Ok(value),
                Err(error)
                    if error.kind() == super::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    decode_message::<wire::StoredCommitRecordV1, _, _>(
                        LEGACY_COMMIT,
                        encoded,
                        |value| {
                            let mutations = value
                                .mutations
                                .into_iter()
                                .map(|value| {
                                    storage_result(CommittedEntityMutationV1::new(
                                        expected_from_proto(require(value.expected)?)?,
                                        entity_from_proto(require(value.post_image)?)?,
                                    ))
                                })
                                .collect::<Result<Vec<_>, DurableCodecError>>()?;
                            entity_references_from_mutations(mutations)
                        },
                    )
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Decodes a legacy V1 commit envelope with embedded events and mutations.
///
/// Name retained for historical call sites; this is not the current V3 decoder.
/// Prefer [`decode_commit_record_v3`] or [`decode_commit_record_with_events`] for
/// production materialization.
pub fn decode_commit_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCommitRecordV1>, DurableCodecError> {
    decode_commit_record_legacy_v1(encoded)
}
