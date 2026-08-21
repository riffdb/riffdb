#![allow(
    dead_code,
    reason = "WP-070 freezes physical keys before every persistence path is wired"
)]

//! Exact physical-key mappings for the frozen redb table layout.

use riffdb_storage_api::{
    EntityTarget, IdempotencyIdentityKey, PartitionIndexTarget,
    StructurallyDecodedIndexRangePrefixV1, VectorObservationTargetV1,
};
use riffdb_types::{
    AdministrationSequence, CapabilityId, CapabilityTokenDigest, CommitSequence,
    ContractBundleHash, ContractLineage, ContractMigrationOperationId, ContractVersion,
    DIGEST_SCHEME_V1, DigestKeyId, EntityKey, EntityTypeId, EventConsumerIdentityHash, EventId,
    IndexEntryKey, IndexId, MAX_CONTRACT_LINEAGE_BYTES, PartitionKey, PartitionKeyHash,
    ProjectionApplyKey, ProjectionFrontierKey, ProjectionGroupKey, ProvenanceId, QueryModuleHash,
    ReactiveModuleHash, RequestId,
};

pub(crate) const SINGLETON_KEY: [u8; 1] = [0x01];

const U64_KEY_BYTES: usize = 8;
const UUID_KEY_BYTES: usize = 16;
const EVENT_KEY_BYTES: usize = 12;
const EVENT_ROUTE_KEY_BYTES: usize = 32 + EVENT_KEY_BYTES;
const CAPABILITY_KEY_BYTES: usize = 1 + UUID_KEY_BYTES;
const CAPABILITY_TOKEN_KEY_BYTES: usize = 1 + 1 + 4 + 32;
const AUDIT_KEY_BYTES: usize = 1 + U64_KEY_BYTES;
const AUDIT_BY_REQUEST_KEY_BYTES: usize = UUID_KEY_BYTES + U64_KEY_BYTES;
const MIGRATION_RETIREMENT_KEY_BYTES: usize = 32;
const EVENT_CONSUMER_DELIVERY_KEY_BYTES: usize = 32 + EVENT_KEY_BYTES;

pub(crate) const fn encode_reactive_module_key(hash: ReactiveModuleHash) -> [u8; 32] {
    hash.into_bytes()
}

pub(crate) fn decode_reactive_module_key(
    bytes: &[u8],
) -> Result<ReactiveModuleHash, PhysicalKeyError> {
    let hash = ReactiveModuleHash::from_bytes(exact_array(bytes)?);
    require_canonical(bytes, &encode_reactive_module_key(hash))?;
    Ok(hash)
}

pub(crate) const fn encode_event_consumer_key(hash: EventConsumerIdentityHash) -> [u8; 32] {
    hash.into_bytes()
}

pub(crate) fn decode_event_consumer_key(
    bytes: &[u8],
) -> Result<EventConsumerIdentityHash, PhysicalKeyError> {
    let hash = EventConsumerIdentityHash::from_bytes(exact_array(bytes)?);
    require_canonical(bytes, &encode_event_consumer_key(hash))?;
    Ok(hash)
}

pub(crate) fn encode_event_consumer_delivery_key(
    hash: EventConsumerIdentityHash,
    event_id: EventId,
) -> [u8; EVENT_CONSUMER_DELIVERY_KEY_BYTES] {
    let mut bytes = [0_u8; EVENT_CONSUMER_DELIVERY_KEY_BYTES];
    bytes[..32].copy_from_slice(hash.as_bytes());
    bytes[32..].copy_from_slice(&event_id.to_be_bytes());
    bytes
}

pub(crate) fn decode_event_consumer_delivery_key(
    bytes: &[u8],
) -> Result<(EventConsumerIdentityHash, EventId), PhysicalKeyError> {
    let bytes = exact_array::<EVENT_CONSUMER_DELIVERY_KEY_BYTES>(bytes)?;
    let hash = EventConsumerIdentityHash::from_bytes(
        bytes[..32]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    );
    let event_id = EventId::from_be_bytes(
        bytes[32..]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    )
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    require_canonical(
        bytes.as_slice(),
        &encode_event_consumer_delivery_key(hash, event_id),
    )?;
    Ok((hash, event_id))
}

/// Redacted failure to structurally decode one physical table key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PhysicalKeyError {
    InvalidLength,
    InvalidPrefix,
    InvalidComponent,
    NonCanonical,
    SizeOverflow,
}

pub(crate) const fn encode_application_sequence_key(sequence: CommitSequence) -> [u8; 8] {
    sequence.to_be_bytes()
}

pub(crate) fn decode_application_sequence_key(
    bytes: &[u8],
) -> Result<CommitSequence, PhysicalKeyError> {
    let encoded = exact_array::<U64_KEY_BYTES>(bytes)?;
    let sequence = CommitSequence::new(u64::from_be_bytes(encoded))
        .ok_or(PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, &encode_application_sequence_key(sequence))?;
    Ok(sequence)
}

pub(crate) const fn encode_administration_sequence_key(
    sequence: AdministrationSequence,
) -> [u8; 8] {
    sequence.to_be_bytes()
}

pub(crate) fn decode_administration_sequence_key(
    bytes: &[u8],
) -> Result<AdministrationSequence, PhysicalKeyError> {
    let encoded = exact_array::<U64_KEY_BYTES>(bytes)?;
    let sequence = AdministrationSequence::new(u64::from_be_bytes(encoded))
        .ok_or(PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, &encode_administration_sequence_key(sequence))?;
    Ok(sequence)
}

pub(crate) const fn encode_contract_migration_operation_key(
    operation_id: ContractMigrationOperationId,
) -> [u8; UUID_KEY_BYTES] {
    operation_id.into_bytes()
}

pub(crate) fn decode_contract_migration_operation_key(
    bytes: &[u8],
) -> Result<ContractMigrationOperationId, PhysicalKeyError> {
    let operation_id = ContractMigrationOperationId::from_bytes(exact_array(bytes)?)
        .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(
        bytes,
        &encode_contract_migration_operation_key(operation_id),
    )?;
    Ok(operation_id)
}

pub(crate) const fn encode_contract_write_retirement_key(
    parent: ContractBundleHash,
) -> [u8; MIGRATION_RETIREMENT_KEY_BYTES] {
    parent.into_bytes()
}

pub(crate) fn decode_contract_write_retirement_key(
    bytes: &[u8],
) -> Result<ContractBundleHash, PhysicalKeyError> {
    let parent = ContractBundleHash::from_bytes(exact_array(bytes)?);
    require_canonical(bytes, &encode_contract_write_retirement_key(parent))?;
    Ok(parent)
}

pub(crate) fn encode_retired_entity_key(
    operation_id: ContractMigrationOperationId,
    target: &EntityTarget,
) -> Result<Vec<u8>, PhysicalKeyError> {
    let key = target.key().as_bytes();
    let target_length = 4_usize
        .checked_add(4)
        .and_then(|value| value.checked_add(key.len()))
        .ok_or(PhysicalKeyError::SizeOverflow)?;
    let mut encoded = Vec::with_capacity(
        UUID_KEY_BYTES
            .checked_add(4)
            .and_then(|value| value.checked_add(target_length))
            .ok_or(PhysicalKeyError::SizeOverflow)?,
    );
    encoded.extend_from_slice(operation_id.as_bytes());
    encoded.extend_from_slice(
        &u32::try_from(target_length)
            .map_err(|_| PhysicalKeyError::SizeOverflow)?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(&target.entity_type_id().get().to_be_bytes());
    encoded.extend_from_slice(
        &u32::try_from(key.len())
            .map_err(|_| PhysicalKeyError::SizeOverflow)?
            .to_be_bytes(),
    );
    encoded.extend_from_slice(key);
    Ok(encoded)
}

pub(crate) fn decode_retired_entity_key(
    bytes: &[u8],
) -> Result<(ContractMigrationOperationId, EntityTarget), PhysicalKeyError> {
    let operation_end = UUID_KEY_BYTES;
    let target_length_end = operation_end + 4;
    let type_end = target_length_end + 4;
    let key_length_end = type_end + 4;
    if bytes.len() < key_length_end {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let operation_id = ContractMigrationOperationId::from_bytes(
        bytes[..operation_end]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    )
    .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let target_length = usize::try_from(u32::from_be_bytes(
        bytes[operation_end..target_length_end]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .map_err(|_| PhysicalKeyError::SizeOverflow)?;
    if target_length != bytes.len() - target_length_end {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let entity_type = EntityTypeId::new(u32::from_be_bytes(
        bytes[target_length_end..type_end]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    let key_length = usize::try_from(u32::from_be_bytes(
        bytes[type_end..key_length_end]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .map_err(|_| PhysicalKeyError::SizeOverflow)?;
    if key_length != bytes.len() - key_length_end {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let key = EntityKey::from_bytes(bytes[key_length_end..].to_vec())
        .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let target =
        EntityTarget::new(entity_type, key).map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, &encode_retired_entity_key(operation_id, &target)?)?;
    Ok((operation_id, target))
}

pub(crate) const fn encode_provenance_key(provenance_id: ProvenanceId) -> [u8; 16] {
    provenance_id.into_bytes()
}

pub(crate) fn decode_provenance_key(bytes: &[u8]) -> Result<ProvenanceId, PhysicalKeyError> {
    let encoded = exact_array::<UUID_KEY_BYTES>(bytes)?;
    let provenance_id =
        ProvenanceId::from_bytes(encoded).map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, &encode_provenance_key(provenance_id))?;
    Ok(provenance_id)
}

pub(crate) const fn encode_event_key(event_id: EventId) -> [u8; 12] {
    event_id.to_be_bytes()
}

pub(crate) fn decode_event_key(bytes: &[u8]) -> Result<EventId, PhysicalKeyError> {
    let encoded = exact_array::<EVENT_KEY_BYTES>(bytes)?;
    let event_id = EventId::from_be_bytes(encoded).ok_or(PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, &encode_event_key(event_id))?;
    Ok(event_id)
}

pub(crate) fn encode_event_route_key(
    partition_hash: PartitionKeyHash,
    event_id: EventId,
) -> [u8; EVENT_ROUTE_KEY_BYTES] {
    let mut encoded = [0_u8; EVENT_ROUTE_KEY_BYTES];
    encoded[..32].copy_from_slice(partition_hash.as_bytes());
    encoded[32..].copy_from_slice(&event_id.to_be_bytes());
    encoded
}

pub(crate) fn decode_event_route_key(
    bytes: &[u8],
) -> Result<(PartitionKeyHash, EventId), PhysicalKeyError> {
    let encoded = exact_array::<EVENT_ROUTE_KEY_BYTES>(bytes)?;
    let partition_hash = PartitionKeyHash::from_bytes(
        encoded[..32]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    );
    let event_id = EventId::from_be_bytes(
        encoded[32..]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    )
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, &encode_event_route_key(partition_hash, event_id))?;
    Ok((partition_hash, event_id))
}

pub(crate) const fn encode_singleton_key() -> [u8; 1] {
    SINGLETON_KEY
}

pub(crate) fn decode_singleton_key(bytes: &[u8]) -> Result<(), PhysicalKeyError> {
    if bytes != SINGLETON_KEY {
        return Err(if bytes.len() == SINGLETON_KEY.len() {
            PhysicalKeyError::InvalidPrefix
        } else {
            PhysicalKeyError::InvalidLength
        });
    }
    Ok(())
}

pub(crate) fn encode_contract_bundle_key(
    lineage: &ContractLineage,
    version: ContractVersion,
) -> Result<Vec<u8>, PhysicalKeyError> {
    let lineage_length =
        u32::try_from(lineage.as_bytes().len()).map_err(|_| PhysicalKeyError::SizeOverflow)?;
    let capacity = 4usize
        .checked_add(lineage.as_bytes().len())
        .and_then(|value| value.checked_add(U64_KEY_BYTES))
        .ok_or(PhysicalKeyError::SizeOverflow)?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(&lineage_length.to_be_bytes());
    encoded.extend_from_slice(lineage.as_bytes());
    encoded.extend_from_slice(&version.to_be_bytes());
    Ok(encoded)
}

pub(crate) fn decode_contract_bundle_key(
    bytes: &[u8],
) -> Result<(ContractLineage, ContractVersion), PhysicalKeyError> {
    let length_bytes = bytes
        .get(..4)
        .ok_or(PhysicalKeyError::InvalidLength)?
        .try_into()
        .map_err(|_| PhysicalKeyError::InvalidLength)?;
    let lineage_length = usize::try_from(u32::from_be_bytes(length_bytes))
        .map_err(|_| PhysicalKeyError::SizeOverflow)?;
    if lineage_length == 0 || lineage_length > MAX_CONTRACT_LINEAGE_BYTES {
        return Err(PhysicalKeyError::InvalidComponent);
    }
    let version_start = 4usize
        .checked_add(lineage_length)
        .ok_or(PhysicalKeyError::SizeOverflow)?;
    let expected_length = version_start
        .checked_add(U64_KEY_BYTES)
        .ok_or(PhysicalKeyError::SizeOverflow)?;
    if bytes.len() != expected_length {
        return Err(PhysicalKeyError::InvalidLength);
    }

    let lineage = std::str::from_utf8(&bytes[4..version_start])
        .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let lineage =
        ContractLineage::new(lineage.to_owned()).map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let version_bytes = exact_array::<U64_KEY_BYTES>(&bytes[version_start..])?;
    let version = ContractVersion::new(u64::from_be_bytes(version_bytes))
        .ok_or(PhysicalKeyError::InvalidComponent)?;
    let canonical = encode_contract_bundle_key(&lineage, version)?;
    require_canonical(bytes, &canonical)?;
    Ok((lineage, version))
}

pub(crate) const fn encode_query_module_key(module_hash: QueryModuleHash) -> [u8; 32] {
    module_hash.into_bytes()
}

pub(crate) fn decode_query_module_key(bytes: &[u8]) -> Result<QueryModuleHash, PhysicalKeyError> {
    Ok(QueryModuleHash::from_bytes(exact_array::<32>(bytes)?))
}

pub(crate) fn encode_active_query_module_key(
    lineage: &ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
) -> Result<Vec<u8>, PhysicalKeyError> {
    let mut encoded = encode_contract_bundle_key(lineage, version)?;
    encoded.extend_from_slice(bundle_hash.as_bytes());
    Ok(encoded)
}

pub(crate) fn decode_active_query_module_key(
    bytes: &[u8],
) -> Result<(ContractLineage, ContractVersion, ContractBundleHash), PhysicalKeyError> {
    let contract_end = bytes
        .len()
        .checked_sub(32)
        .ok_or(PhysicalKeyError::InvalidLength)?;
    let (lineage, version) = decode_contract_bundle_key(&bytes[..contract_end])?;
    let bundle_hash = ContractBundleHash::from_bytes(exact_array::<32>(&bytes[contract_end..])?);
    let canonical = encode_active_query_module_key(&lineage, version, bundle_hash)?;
    require_canonical(bytes, &canonical)?;
    Ok((lineage, version, bundle_hash))
}

pub(crate) fn encode_capability_key(capability_id: CapabilityId) -> [u8; 17] {
    let mut encoded = [0; CAPABILITY_KEY_BYTES];
    encoded[0] = SINGLETON_KEY[0];
    encoded[1..].copy_from_slice(capability_id.as_bytes());
    encoded
}

pub(crate) fn decode_capability_key(bytes: &[u8]) -> Result<CapabilityId, PhysicalKeyError> {
    let encoded = exact_array::<CAPABILITY_KEY_BYTES>(bytes)?;
    if encoded[0] != SINGLETON_KEY[0] {
        return Err(PhysicalKeyError::InvalidPrefix);
    }
    let capability_id = CapabilityId::from_bytes(
        encoded[1..]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    )
    .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, &encode_capability_key(capability_id))?;
    Ok(capability_id)
}

pub(crate) fn encode_capability_token_key(digest: CapabilityTokenDigest) -> [u8; 38] {
    let mut encoded = [0; CAPABILITY_TOKEN_KEY_BYTES];
    encoded[0] = SINGLETON_KEY[0];
    encoded[1] = digest.scheme();
    encoded[2..6].copy_from_slice(&digest.key_id().to_be_bytes());
    encoded[6..].copy_from_slice(digest.as_bytes());
    encoded
}

pub(crate) fn decode_capability_token_key(
    bytes: &[u8],
) -> Result<CapabilityTokenDigest, PhysicalKeyError> {
    let encoded = exact_array::<CAPABILITY_TOKEN_KEY_BYTES>(bytes)?;
    if encoded[0] != SINGLETON_KEY[0] || encoded[1] != DIGEST_SCHEME_V1 {
        return Err(PhysicalKeyError::InvalidPrefix);
    }
    let key_id = DigestKeyId::new(u32::from_be_bytes(
        encoded[2..6]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    let digest = CapabilityTokenDigest::from_hmac_bytes(
        key_id,
        encoded[6..]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    );
    require_canonical(bytes, &encode_capability_token_key(digest))?;
    Ok(digest)
}

pub(crate) fn encode_audit_key(sequence: AdministrationSequence) -> [u8; 9] {
    let mut encoded = [0; AUDIT_KEY_BYTES];
    encoded[0] = SINGLETON_KEY[0];
    encoded[1..].copy_from_slice(&sequence.to_be_bytes());
    encoded
}

pub(crate) fn decode_audit_key(bytes: &[u8]) -> Result<AdministrationSequence, PhysicalKeyError> {
    let encoded = exact_array::<AUDIT_KEY_BYTES>(bytes)?;
    if encoded[0] != SINGLETON_KEY[0] {
        return Err(PhysicalKeyError::InvalidPrefix);
    }
    let sequence = AdministrationSequence::new(u64::from_be_bytes(
        encoded[1..]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, &encode_audit_key(sequence))?;
    Ok(sequence)
}

pub(crate) fn encode_audit_by_request_key(
    request_id: RequestId,
    sequence: AdministrationSequence,
) -> [u8; AUDIT_BY_REQUEST_KEY_BYTES] {
    let mut encoded = [0; AUDIT_BY_REQUEST_KEY_BYTES];
    encoded[..UUID_KEY_BYTES].copy_from_slice(request_id.as_bytes());
    encoded[UUID_KEY_BYTES..].copy_from_slice(&sequence.to_be_bytes());
    encoded
}

pub(crate) fn encode_audit_by_request_prefix(request_id: RequestId) -> [u8; UUID_KEY_BYTES] {
    *request_id.as_bytes()
}

pub(crate) fn decode_audit_by_request_key(
    bytes: &[u8],
) -> Result<(RequestId, AdministrationSequence), PhysicalKeyError> {
    let encoded = exact_array::<AUDIT_BY_REQUEST_KEY_BYTES>(bytes)?;
    let request_id = RequestId::from_bytes(
        encoded[..UUID_KEY_BYTES]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    )
    .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let sequence = AdministrationSequence::new(u64::from_be_bytes(
        encoded[UUID_KEY_BYTES..]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, &encode_audit_by_request_key(request_id, sequence))?;
    Ok((request_id, sequence))
}

pub(crate) fn encode_entity_key(key: &EntityKey) -> &[u8] {
    key.as_bytes()
}

pub(crate) fn decode_entity_key(bytes: &[u8]) -> Result<EntityKey, PhysicalKeyError> {
    let key =
        EntityKey::from_bytes(bytes.to_vec()).map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, key.as_bytes())?;
    Ok(key)
}

pub(crate) fn encode_vector_evidence_key(
    key: &EntityKey,
    field: riffdb_types::FieldId,
) -> Result<Vec<u8>, PhysicalKeyError> {
    let mut encoded = Vec::with_capacity(key.as_bytes().len().saturating_add(4));
    encoded.extend_from_slice(key.as_bytes());
    encoded.extend_from_slice(&field.get().to_be_bytes());
    if encoded.len() != key.as_bytes().len().saturating_add(4) {
        return Err(PhysicalKeyError::InvalidLength);
    }
    Ok(encoded)
}

pub(crate) fn decode_vector_evidence_key(
    bytes: &[u8],
) -> Result<(EntityKey, riffdb_types::FieldId), PhysicalKeyError> {
    let split = bytes
        .len()
        .checked_sub(4)
        .ok_or(PhysicalKeyError::InvalidLength)?;
    let key = decode_entity_key(&bytes[..split])?;
    let field = riffdb_types::FieldId::new(u32::from_be_bytes(
        bytes[split..]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    let canonical = encode_vector_evidence_key(&key, field)?;
    require_canonical(bytes, &canonical)?;
    Ok((key, field))
}

pub(crate) fn encode_vector_observation_key(
    target: &VectorObservationTargetV1,
) -> Result<Vec<u8>, PhysicalKeyError> {
    let lineage = target.lineage().as_bytes();
    let partition = target.partition_key().as_bytes();
    let lineage_len = u16::try_from(lineage.len()).map_err(|_| PhysicalKeyError::InvalidLength)?;
    let partition_len =
        u32::try_from(partition.len()).map_err(|_| PhysicalKeyError::InvalidLength)?;
    let capacity = 2_usize
        .checked_add(lineage.len())
        .and_then(|value| value.checked_add(4 + partition.len() + 4 + 4))
        .ok_or(PhysicalKeyError::InvalidLength)?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(&lineage_len.to_be_bytes());
    encoded.extend_from_slice(lineage);
    encoded.extend_from_slice(&partition_len.to_be_bytes());
    encoded.extend_from_slice(partition);
    encoded.extend_from_slice(&target.entity_type().get().to_be_bytes());
    encoded.extend_from_slice(&target.vector_field().get().to_be_bytes());
    if encoded.len() != capacity {
        return Err(PhysicalKeyError::InvalidLength);
    }
    Ok(encoded)
}

pub(crate) fn decode_vector_observation_key(
    bytes: &[u8],
) -> Result<VectorObservationTargetV1, PhysicalKeyError> {
    let lineage_len = usize::from(u16::from_be_bytes(
        bytes
            .get(..2)
            .ok_or(PhysicalKeyError::InvalidLength)?
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ));
    let lineage_end = 2_usize
        .checked_add(lineage_len)
        .ok_or(PhysicalKeyError::InvalidLength)?;
    let partition_len_end = lineage_end
        .checked_add(4)
        .ok_or(PhysicalKeyError::InvalidLength)?;
    let partition_len = usize::try_from(u32::from_be_bytes(
        bytes
            .get(lineage_end..partition_len_end)
            .ok_or(PhysicalKeyError::InvalidLength)?
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .map_err(|_| PhysicalKeyError::InvalidLength)?;
    let partition_end = partition_len_end
        .checked_add(partition_len)
        .ok_or(PhysicalKeyError::InvalidLength)?;
    let entity_end = partition_end
        .checked_add(4)
        .ok_or(PhysicalKeyError::InvalidLength)?;
    let field_end = entity_end
        .checked_add(4)
        .ok_or(PhysicalKeyError::InvalidLength)?;
    if field_end != bytes.len() || lineage_len == 0 || lineage_len > MAX_CONTRACT_LINEAGE_BYTES {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let lineage = ContractLineage::new(
        std::str::from_utf8(
            bytes
                .get(2..lineage_end)
                .ok_or(PhysicalKeyError::InvalidLength)?,
        )
        .map_err(|_| PhysicalKeyError::InvalidComponent)?,
    )
    .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let partition = PartitionKey::from_bytes(
        bytes
            .get(partition_len_end..partition_end)
            .ok_or(PhysicalKeyError::InvalidLength)?
            .to_vec(),
    )
    .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let entity_type = EntityTypeId::new(u32::from_be_bytes(
        bytes
            .get(partition_end..entity_end)
            .ok_or(PhysicalKeyError::InvalidLength)?
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    let vector_field = riffdb_types::FieldId::new(u32::from_be_bytes(
        bytes
            .get(entity_end..field_end)
            .ok_or(PhysicalKeyError::InvalidLength)?
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    let target = VectorObservationTargetV1::new(lineage, partition, entity_type, vector_field);
    require_canonical(bytes, &encode_vector_observation_key(&target)?)?;
    Ok(target)
}

pub(crate) fn encode_index_entry_key(key: &IndexEntryKey) -> &[u8] {
    key.as_bytes()
}

pub(crate) fn decode_index_entry_key(bytes: &[u8]) -> Result<IndexEntryKey, PhysicalKeyError> {
    let key = IndexEntryKey::from_bytes(bytes.to_vec())
        .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, key.as_bytes())?;
    Ok(key)
}

pub(crate) fn encode_index_range_prefix_key(key: &StructurallyDecodedIndexRangePrefixV1) -> &[u8] {
    key.as_bytes()
}

pub(crate) fn decode_index_range_prefix_key(
    bytes: &[u8],
) -> Result<StructurallyDecodedIndexRangePrefixV1, PhysicalKeyError> {
    let owner = bytes.get(2..6).ok_or(PhysicalKeyError::InvalidLength)?;
    let index_id = IndexId::new(u32::from_be_bytes(
        owner
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    let key = StructurallyDecodedIndexRangePrefixV1::new(index_id, bytes.to_vec())
        .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, key.as_bytes())?;
    Ok(key)
}

pub(crate) fn encode_partition_index_key(key: &PartitionIndexTarget) -> Vec<u8> {
    key.to_key_bytes()
}

pub(crate) fn decode_partition_index_key(
    bytes: &[u8],
) -> Result<PartitionIndexTarget, PhysicalKeyError> {
    let length = bytes
        .get(..4)
        .ok_or(PhysicalKeyError::InvalidLength)?
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| PhysicalKeyError::InvalidLength)?;
    let length = usize::try_from(length).map_err(|_| PhysicalKeyError::SizeOverflow)?;
    let partition_end = 4usize
        .checked_add(length)
        .ok_or(PhysicalKeyError::SizeOverflow)?;
    let expected = partition_end
        .checked_add(4)
        .ok_or(PhysicalKeyError::SizeOverflow)?;
    if bytes.len() != expected {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let partition = PartitionKey::from_bytes(
        bytes
            .get(4..partition_end)
            .ok_or(PhysicalKeyError::InvalidLength)?
            .to_vec(),
    )
    .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let index = bytes
        .get(partition_end..expected)
        .ok_or(PhysicalKeyError::InvalidLength)?
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| PhysicalKeyError::InvalidLength)?;
    let index_id = IndexId::new(index).ok_or(PhysicalKeyError::InvalidComponent)?;
    let target = PartitionIndexTarget::new(partition, index_id);
    require_canonical(bytes, &target.to_key_bytes())?;
    Ok(target)
}

pub(crate) fn encode_idempotency_key(key: &IdempotencyIdentityKey) -> &[u8] {
    key.as_bytes()
}

pub(crate) fn decode_idempotency_key(
    bytes: &[u8],
) -> Result<IdempotencyIdentityKey, PhysicalKeyError> {
    let identity =
        IdempotencyIdentityKey::decode(bytes).map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let key = identity
        .storage_key()
        .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, key.as_bytes())?;
    Ok(key)
}

pub(crate) fn encode_projection_group_key(key: &ProjectionGroupKey) -> &[u8] {
    key.as_bytes()
}

pub(crate) fn decode_projection_group_key(
    bytes: &[u8],
) -> Result<ProjectionGroupKey, PhysicalKeyError> {
    let key = ProjectionGroupKey::from_bytes(bytes.to_vec())
        .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, key.as_bytes())?;
    Ok(key)
}

pub(crate) fn encode_projection_frontier_key(key: &ProjectionFrontierKey) -> &[u8] {
    key.as_bytes()
}

pub(crate) fn decode_projection_frontier_key(
    bytes: &[u8],
) -> Result<ProjectionFrontierKey, PhysicalKeyError> {
    let key = ProjectionFrontierKey::from_bytes(bytes.to_vec())
        .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, key.as_bytes())?;
    Ok(key)
}

pub(crate) fn encode_projection_apply_key(key: &ProjectionApplyKey) -> &[u8] {
    key.as_bytes()
}

pub(crate) fn decode_projection_apply_key(
    bytes: &[u8],
) -> Result<ProjectionApplyKey, PhysicalKeyError> {
    let key = ProjectionApplyKey::from_bytes(bytes.to_vec())
        .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    require_canonical(bytes, key.as_bytes())?;
    Ok(key)
}

fn exact_array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], PhysicalKeyError> {
    bytes
        .try_into()
        .map_err(|_| PhysicalKeyError::InvalidLength)
}

fn require_canonical(bytes: &[u8], canonical: &[u8]) -> Result<(), PhysicalKeyError> {
    if bytes == canonical {
        Ok(())
    } else {
        Err(PhysicalKeyError::NonCanonical)
    }
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{IdempotencyIdentity, IdempotencyKeyDigest, IndexRangePrefixBuilder};
    use riffdb_types::{
        ActorId, CanonicalValue, CommandId, DatabaseId, EntityKeyBuilder, EntityTypeId,
        Environment, IndexEntryKeyBuilder, ProjectionGeneration, ProjectionGroupKeyBuilder,
        ProjectionId, ProjectionIdentity, ProjectionPlanHash, TenantScope,
    };

    use super::*;

    fn uuid_bytes(last: u8) -> [u8; 16] {
        [
            0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, last,
        ]
    }

    fn projection_identity() -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new("budget").expect("lineage"),
            ProjectionId::new(3).expect("projection ID"),
            ProjectionPlanHash::from_bytes([0x44; 32]),
        )
    }

    #[test]
    fn fixed_width_keys_have_exact_goldens_and_numeric_order() {
        let first = CommitSequence::new(1).expect("sequence");
        let later = CommitSequence::new(0x0102_0304_0506_0708).expect("sequence");
        assert_eq!(
            encode_application_sequence_key(first),
            [0, 0, 0, 0, 0, 0, 0, 1]
        );
        assert!(encode_application_sequence_key(first) < encode_application_sequence_key(later));
        assert_eq!(
            decode_application_sequence_key(&later.to_be_bytes()),
            Ok(later)
        );

        let administration = AdministrationSequence::new(7).expect("sequence");
        assert_eq!(
            encode_administration_sequence_key(administration),
            [0, 0, 0, 0, 0, 0, 0, 7]
        );
        assert_eq!(
            decode_administration_sequence_key(&administration.to_be_bytes()),
            Ok(administration)
        );

        let provenance = ProvenanceId::from_bytes(uuid_bytes(9)).expect("provenance UUIDv7");
        assert_eq!(encode_provenance_key(provenance), uuid_bytes(9));
        assert_eq!(decode_provenance_key(&uuid_bytes(9)), Ok(provenance));

        let event = EventId::new(later, 11);
        let mut expected_event = later.to_be_bytes().to_vec();
        expected_event.extend_from_slice(&11_u32.to_be_bytes());
        assert_eq!(encode_event_key(event).as_slice(), expected_event);
        assert_eq!(decode_event_key(&expected_event), Ok(event));

        let partition_hash = PartitionKeyHash::from_bytes([0xa5; 32]);
        let mut expected_route = [0_u8; 44];
        expected_route[..32].fill(0xa5);
        expected_route[32..].copy_from_slice(&expected_event);
        assert_eq!(
            encode_event_route_key(partition_hash, event),
            expected_route
        );
        assert_eq!(
            decode_event_route_key(&expected_route),
            Ok((partition_hash, event))
        );

        assert_eq!(encode_singleton_key(), [0x01]);
        assert_eq!(decode_singleton_key(&[0x01]), Ok(()));
    }

    #[test]
    fn vector_observation_key_round_trips_every_identity_component() {
        let mut partition =
            riffdb_types::PartitionKeyBuilder::new(riffdb_types::AggregateTypeId::first());
        partition.push_str("org-a").expect("partition component");
        let target = VectorObservationTargetV1::new(
            ContractLineage::new("vectors").expect("lineage"),
            partition.finish().expect("partition"),
            EntityTypeId::new(7).expect("entity type"),
            riffdb_types::FieldId::new(9).expect("vector field"),
        );
        let encoded = encode_vector_observation_key(&target).expect("encode");
        assert_eq!(
            decode_vector_observation_key(&encoded).expect("decode"),
            target
        );
        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            decode_vector_observation_key(&trailing),
            Err(PhysicalKeyError::InvalidLength)
        );
    }

    #[test]
    fn contract_bundle_key_is_length_framed_and_version_ordered() {
        let lineage = ContractLineage::new("ab").expect("lineage");
        let version = ContractVersion::new(3).expect("version");
        let encoded = encode_contract_bundle_key(&lineage, version).expect("encode");
        assert_eq!(encoded, [0, 0, 0, 2, b'a', b'b', 0, 0, 0, 0, 0, 0, 0, 3]);
        assert_eq!(
            decode_contract_bundle_key(&encoded),
            Ok((lineage.clone(), version))
        );
        assert!(
            encode_contract_bundle_key(&lineage, ContractVersion::new(2).expect("version"))
                .expect("encode")
                < encoded
        );
    }

    #[test]
    fn capability_and_audit_keys_have_exact_goldens() {
        let capability = CapabilityId::from_bytes(uuid_bytes(4)).expect("capability UUIDv7");
        let mut expected_capability = vec![0x01];
        expected_capability.extend_from_slice(&uuid_bytes(4));
        assert_eq!(
            encode_capability_key(capability).as_slice(),
            expected_capability
        );
        assert_eq!(decode_capability_key(&expected_capability), Ok(capability));

        let digest = CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(7).expect("digest key ID"),
            [0xa5; 32],
        );
        let mut expected_digest = vec![0x01, DIGEST_SCHEME_V1, 0, 0, 0, 7];
        expected_digest.extend_from_slice(&[0xa5; 32]);
        assert_eq!(
            encode_capability_token_key(digest).as_slice(),
            expected_digest
        );
        assert_eq!(decode_capability_token_key(&expected_digest), Ok(digest));

        let sequence = AdministrationSequence::new(0x0102_0304_0506_0708).expect("sequence");
        let mut expected_audit = vec![0x01];
        expected_audit.extend_from_slice(&sequence.to_be_bytes());
        assert_eq!(encode_audit_key(sequence).as_slice(), expected_audit);
        assert_eq!(decode_audit_key(&expected_audit), Ok(sequence));
    }

    #[test]
    fn transparent_semantic_keys_round_trip_exact_bytes() {
        let mut entity_builder = EntityKeyBuilder::new(EntityTypeId::new(7).expect("entity type"));
        entity_builder.push_u64(9).expect("component");
        let entity = entity_builder.finish().expect("entity key");
        assert_eq!(
            decode_entity_key(encode_entity_key(&entity))
                .expect("decode")
                .as_bytes(),
            entity.as_bytes()
        );

        let index_id = IndexId::new(8).expect("index ID");
        let mut index_builder = IndexEntryKeyBuilder::new(index_id);
        index_builder.push_str("open").expect("component");
        let index = index_builder.finish(entity).expect("index key");
        assert_eq!(
            decode_index_entry_key(encode_index_entry_key(&index))
                .expect("decode")
                .as_bytes(),
            index.as_bytes()
        );

        let mut prefix_builder = IndexRangePrefixBuilder::new(index_id);
        prefix_builder.push_u64(13).expect("component");
        let prefix = StructurallyDecodedIndexRangePrefixV1::from_live(&prefix_builder.finish());
        assert_eq!(
            decode_index_range_prefix_key(encode_index_range_prefix_key(&prefix))
                .expect("decode")
                .as_bytes(),
            prefix.as_bytes()
        );

        let identity = IdempotencyIdentity::new(
            DatabaseId::from_bytes(uuid_bytes(1)).expect("database UUIDv7"),
            Environment::new("test").expect("environment"),
            TenantScope::Global,
            ActorId::new("operator").expect("actor"),
            ContractLineage::new("budget").expect("lineage"),
            CommandId::new(5).expect("command ID"),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(6).expect("digest key ID"),
                [0x33; 32],
            ),
        );
        let idempotency = identity.storage_key().expect("idempotency key");
        assert_eq!(
            decode_idempotency_key(encode_idempotency_key(&idempotency))
                .expect("decode")
                .as_bytes(),
            idempotency.as_bytes()
        );

        let identity = projection_identity();
        let generation = ProjectionGeneration::new(2).expect("generation");
        let mut group_builder = ProjectionGroupKeyBuilder::new(identity.clone(), generation);
        group_builder
            .push_component(CanonicalValue::U64(17))
            .expect("component");
        let group = group_builder.finish().expect("group key");
        assert_eq!(
            decode_projection_group_key(encode_projection_group_key(&group))
                .expect("decode")
                .as_bytes(),
            group.as_bytes()
        );

        let frontier = ProjectionFrontierKey::new(identity.clone());
        assert_eq!(
            decode_projection_frontier_key(encode_projection_frontier_key(&frontier))
                .expect("decode")
                .as_bytes(),
            frontier.as_bytes()
        );

        let apply = ProjectionApplyKey::new(identity, generation, CommitSequence::first());
        assert_eq!(
            decode_projection_apply_key(encode_projection_apply_key(&apply))
                .expect("decode")
                .as_bytes(),
            apply.as_bytes()
        );
    }

    #[test]
    fn malformed_zero_trailing_and_noncanonical_keys_are_rejected() {
        assert_eq!(
            decode_application_sequence_key(&[0; 8]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        assert_eq!(
            decode_application_sequence_key(&[0; 9]),
            Err(PhysicalKeyError::InvalidLength)
        );
        assert_eq!(
            decode_administration_sequence_key(&[0; 8]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        assert_eq!(
            decode_event_key(&[0; 12]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        assert_eq!(
            decode_event_route_key(&[0; 44]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        assert_eq!(
            decode_event_route_key(&[0; 45]),
            Err(PhysicalKeyError::InvalidLength)
        );
        assert_eq!(
            decode_provenance_key(&[0; 16]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        assert_eq!(
            decode_singleton_key(&[0x01, 0x00]),
            Err(PhysicalKeyError::InvalidLength)
        );
        assert_eq!(
            decode_singleton_key(&[0x02]),
            Err(PhysicalKeyError::InvalidPrefix)
        );

        let mut zero_bundle_version = vec![0, 0, 0, 1, b'a'];
        zero_bundle_version.extend_from_slice(&[0; 8]);
        assert_eq!(
            decode_contract_bundle_key(&zero_bundle_version),
            Err(PhysicalKeyError::InvalidComponent)
        );
        let mut trailing_bundle = encode_contract_bundle_key(
            &ContractLineage::new("a").expect("lineage"),
            ContractVersion::new(1).expect("version"),
        )
        .expect("encode");
        trailing_bundle.push(0);
        assert_eq!(
            decode_contract_bundle_key(&trailing_bundle),
            Err(PhysicalKeyError::InvalidLength)
        );
        assert_eq!(
            decode_contract_bundle_key(&[0, 0, 0, 1, 0xff, 0, 0, 0, 0, 0, 0, 0, 1]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        let mut oversized_lineage = Vec::new();
        oversized_lineage.extend_from_slice(
            &u32::try_from(MAX_CONTRACT_LINEAGE_BYTES + 1)
                .expect("lineage test length")
                .to_be_bytes(),
        );
        oversized_lineage.extend(std::iter::repeat_n(b'a', MAX_CONTRACT_LINEAGE_BYTES + 1));
        oversized_lineage.extend_from_slice(&1_u64.to_be_bytes());
        assert_eq!(
            decode_contract_bundle_key(&oversized_lineage),
            Err(PhysicalKeyError::InvalidComponent)
        );

        let mut capability = encode_capability_key(
            CapabilityId::from_bytes(uuid_bytes(2)).expect("capability UUIDv7"),
        );
        capability[0] = 0;
        assert_eq!(
            decode_capability_key(&capability),
            Err(PhysicalKeyError::InvalidPrefix)
        );

        let digest = CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            [0x55; 32],
        );
        let mut token = encode_capability_token_key(digest);
        token[1] = 0xff;
        assert_eq!(
            decode_capability_token_key(&token),
            Err(PhysicalKeyError::InvalidPrefix)
        );
        token = encode_capability_token_key(digest);
        token[2..6].fill(0);
        assert_eq!(
            decode_capability_token_key(&token),
            Err(PhysicalKeyError::InvalidComponent)
        );

        assert_eq!(
            decode_audit_key(&[0x01, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        assert_eq!(
            decode_entity_key(&[0x45, 0x01, 0, 0, 0, 0]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        assert_eq!(
            decode_index_entry_key(&[0x49, 0x02, 0, 0, 0, 1]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        assert_eq!(
            decode_index_range_prefix_key(&[0x49, 0x01, 0, 0, 0, 0]),
            Err(PhysicalKeyError::InvalidComponent)
        );
        assert!(decode_idempotency_key(&[0x59, 0x01, 0]).is_err());
        assert!(decode_projection_group_key(&[0x47, 0x01]).is_err());
        assert!(decode_projection_frontier_key(&[0x46, 0x01, 0]).is_err());
        assert!(decode_projection_apply_key(&[0x41, 0x01, 0]).is_err());
        assert_eq!(
            require_canonical(&[0x01, 0x00], &[0x01]),
            Err(PhysicalKeyError::NonCanonical)
        );
    }

    #[test]
    fn lexicographic_fixed_keys_preserve_semantic_order() {
        let first = CommitSequence::new(255).expect("sequence");
        let second = CommitSequence::new(256).expect("sequence");
        assert!(encode_application_sequence_key(first) < encode_application_sequence_key(second));

        let first_event = EventId::new(first, u32::MAX);
        let second_event = EventId::new(second, 0);
        assert!(encode_event_key(first_event) < encode_event_key(second_event));

        let first_audit = AdministrationSequence::new(255).expect("sequence");
        let second_audit = AdministrationSequence::new(256).expect("sequence");
        assert!(encode_audit_key(first_audit) < encode_audit_key(second_audit));
    }
}
