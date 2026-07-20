use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    CanonicalInputHash, CommitSequence, ConflictKeyHash, ContractVersion, EntityVersion, EventHash,
    EventTypeId, IndexEntryKey, IndexEpoch, OutcomeId, PartitionKey, PartitionKeyHash,
    ProvenanceId, RequestId, encode_canonical_record,
};

use crate::{
    AffectedEntityV1, CommittedEntityMutationV1, DurabilityMode, EncodedPageItem,
    StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1, StoredExecutionFailedV1,
    StoredIndexEntryV1, StoredIndexEpochV1, StoredOutcomeV1, StoredPendingAdmissionV1,
    StoredProvenanceRecordV1, StoredReadDependenciesV1, StoredReadDependencyV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, actor_from_proto, actor_to_proto,
    binding_from_proto, binding_to_proto, canonical_record_from_bytes, claims_from_proto,
    claims_to_proto, declared_outcome_from_proto, declared_outcome_to_proto, decode_message,
    encode_message, entity_target_from_proto, entity_target_to_proto, epoch_from_proto,
    epoch_to_proto, event_id_from_proto, event_id_to_proto, expected_from_proto, expected_to_proto,
    fixed, identity_from_proto, identity_to_proto, logical_time_from_proto, plan_from_proto,
    plan_to_proto, require, storage_result, structural_prefix_from_bytes, timestamp_to_proto,
};

pub(super) const ENTITY: &str = "riffdb.storage.v1.StoredEntityRecordV1";
pub(super) const INDEX_ENTRY: &str = "riffdb.storage.v1.StoredIndexEntryV1";
pub(super) const INDEX_EPOCH: &str = "riffdb.storage.v1.StoredIndexEpochV1";
const PENDING: &str = "riffdb.storage.v1.StoredPendingAdmissionV1";
const EXECUTION_FAILED: &str = "riffdb.storage.v1.StoredExecutionFailedV1";
pub(super) const OUTCOME: &str = "riffdb.storage.v1.StoredOutcomeV1";
pub(super) const EVENT: &str = "riffdb.storage.v1.StoredDurableEventV1";
pub(super) const PROVENANCE: &str = "riffdb.storage.v1.StoredProvenanceRecordV1";
pub(super) const COMMIT: &str = "riffdb.storage.v1.StoredCommitRecordV1";

pub(super) fn durability_to_proto(value: DurabilityMode) -> i32 {
    match value {
        DurabilityMode::Sync => wire::DurabilityModeV1::DurabilityModeSync as i32,
        DurabilityMode::Group => wire::DurabilityModeV1::DurabilityModeGroup as i32,
        DurabilityMode::Memory => wire::DurabilityModeV1::DurabilityModeMemory as i32,
    }
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
        canonical_fields: encode_canonical_record(value.fields())
            .expect("checked entity record must encode"),
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

pub(super) fn index_entry_to_proto(value: &StoredIndexEntryV1) -> wire::StoredIndexEntryV1 {
    wire::StoredIndexEntryV1 {
        index_entry_key: value.key().as_bytes().to_vec(),
        schema_binding: Some(binding_to_proto(value.schema_binding())),
        canonical_covered_values: encode_canonical_record(value.covered_values())
            .expect("checked index record must encode"),
    }
}

fn index_entry_from_proto(
    value: wire::StoredIndexEntryV1,
) -> Result<StoredIndexEntryV1, DurableCodecError> {
    storage_result(StoredIndexEntryV1::new(
        IndexEntryKey::from_bytes(value.index_entry_key)
            .map_err(|_| DurableCodecError::corrupt())?,
        binding_from_proto(require(value.schema_binding)?)?,
        canonical_record_from_bytes(&value.canonical_covered_values)?,
    ))
}

pub(super) fn index_epoch_to_proto(value: &StoredIndexEpochV1) -> wire::StoredIndexEpochV1 {
    wire::StoredIndexEpochV1 {
        canonical_index_range_prefix: value.target().as_bytes().to_vec(),
        schema_binding: Some(binding_to_proto(value.schema_binding())),
        epoch: value.epoch().get(),
    }
}

fn index_epoch_from_proto(
    value: wire::StoredIndexEpochV1,
) -> Result<StoredIndexEpochV1, DurableCodecError> {
    Ok(StoredIndexEpochV1::new(
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
        canonical_payload: encode_canonical_record(value.payload())
            .expect("checked durable event must encode"),
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

/// Encodes one authoritative index-entry post-image.
pub fn encode_index_entry_v1(
    value: &StoredIndexEntryV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(INDEX_ENTRY, &index_entry_to_proto(value))
}

/// Decodes one authoritative index-entry post-image.
pub fn decode_index_entry_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredIndexEntryV1>, DurableCodecError> {
    decode_message::<wire::StoredIndexEntryV1, _, _>(INDEX_ENTRY, encoded, index_entry_from_proto)
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
    decode_message::<wire::StoredIndexEpochV1, _, _>(INDEX_EPOCH, encoded, index_epoch_from_proto)
}

/// Encodes one pending command admission.
pub fn encode_pending_admission_v1(
    value: &StoredPendingAdmissionV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(PENDING, &pending_to_proto(value))
}

/// Decodes one pending command admission.
pub fn decode_pending_admission_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredPendingAdmissionV1>, DurableCodecError> {
    decode_message::<wire::StoredPendingAdmissionV1, _, _>(PENDING, encoded, pending_from_proto)
}

/// Encodes one terminal deterministic execution failure.
pub fn encode_execution_failed_v1(
    value: &StoredExecutionFailedV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
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
    decode_message::<wire::StoredExecutionFailedV1, _, _>(EXECUTION_FAILED, encoded, |value| {
        Ok(StoredExecutionFailedV1::new(
            pending_from_proto(require(value.pending)?)?,
            execution_code_from_proto(value.code)?,
        ))
    })
}

/// Encodes the one complete terminal idempotency outcome row.
pub fn encode_stored_outcome_v1(
    value: &StoredOutcomeV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        OUTCOME,
        &wire::StoredOutcomeV1 {
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
        },
    )
}

/// Decodes the one complete terminal idempotency outcome row.
pub fn decode_stored_outcome_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredOutcomeV1>, DurableCodecError> {
    decode_message::<wire::StoredOutcomeV1, _, _>(OUTCOME, encoded, |value| {
        storage_result(StoredOutcomeV1::new(
            identity_from_proto(require(value.identity)?)?,
            CommitSequence::new(value.commit_sequence).ok_or_else(DurableCodecError::corrupt)?,
            RequestId::from_bytes(fixed(value.admission_request_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            plan_from_proto(require(value.plan)?)?,
            CanonicalInputHash::from_bytes(fixed(value.canonical_input_hash)?),
            actor_from_proto(require(value.actor)?)?,
            logical_time_from_proto(require(value.logical_time)?)?,
            PartitionKeyHash::from_bytes(fixed(value.partition_hash)?),
            hashes_from_proto(value.conflict_hashes)?,
            declared_outcome_from_proto(require(value.declared_outcome)?)?,
            claims_from_proto(require(value.admitted_claims)?)?,
            ProvenanceId::from_bytes(fixed(value.provenance_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            durability_from_proto(value.durability_mode)?,
        ))
    })
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

/// Encodes one immutable command provenance record.
pub fn encode_provenance_record_v1(
    value: &StoredProvenanceRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        PROVENANCE,
        &wire::StoredProvenanceRecordV1 {
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
        },
    )
}

/// Decodes one immutable command provenance record.
pub fn decode_provenance_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredProvenanceRecordV1>, DurableCodecError> {
    decode_message::<wire::StoredProvenanceRecordV1, _, _>(PROVENANCE, encoded, |value| {
        let affected_entities = value
            .affected_entities
            .into_iter()
            .map(|value| {
                Ok(AffectedEntityV1::from_stored_parts(
                    entity_target_from_proto(require(value.target)?)?,
                    EntityVersion::new(value.entity_version)
                        .ok_or_else(DurableCodecError::corrupt)?,
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
    })
}

/// Encodes one complete authoritative commit-log record.
pub fn encode_commit_record_v1(
    value: &StoredCommitRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        COMMIT,
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
            mutations: value
                .mutations()
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

/// Decodes one complete authoritative commit-log record.
pub fn decode_commit_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCommitRecordV1>, DurableCodecError> {
    decode_message::<wire::StoredCommitRecordV1, _, _>(COMMIT, encoded, |value| {
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
        let events = value
            .events
            .into_iter()
            .map(event_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let outbox_event_ids = value
            .outbox_event_ids
            .into_iter()
            .map(event_id_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        storage_result(StoredCommitRecordV1::new(
            CommitSequence::new(value.commit_sequence).ok_or_else(DurableCodecError::corrupt)?,
            RequestId::from_bytes(fixed(value.admission_request_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            plan_from_proto(require(value.plan)?)?,
            CanonicalInputHash::from_bytes(fixed(value.canonical_input_hash)?),
            actor_from_proto(require(value.actor)?)?,
            logical_time_from_proto(require(value.logical_time)?)?,
            PartitionKeyHash::from_bytes(fixed(value.partition_hash)?),
            hashes_from_proto(value.conflict_hashes)?,
            dependencies_from_proto(require(value.read_dependencies)?)?,
            mutations,
            events,
            declared_outcome_from_proto(require(value.declared_outcome)?)?,
            ProvenanceId::from_bytes(fixed(value.provenance_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            outbox_event_ids,
            durability_from_proto(value.durability_mode)?,
        ))
    })
}
