//! Catalog-owned checks for supplied command-prefix index images.

use crate::{CatalogError, CatalogErrorKind, ValidatedContractBundle};
use riffdb_storage_api::{AuthoritativeNamespaceV1 as N, StoredCommandCapsuleV2};

fn corrupt() -> CatalogError {
    CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
}

/// Checks supplied secondary-index keys and derives put keys and covers from
/// their owning entity post-images using the exact retained command bundle.
/// This pure check grants no readiness, mutation or reconstruction authority;
/// complete mutation inventory and predecessor proofs remain separate.
pub fn validate_command_prefix_index_images_v1(
    bundle: &ValidatedContractBundle,
    command: &StoredCommandCapsuleV2,
) -> Result<(), CatalogError> {
    let Some(prefix) = command.prefix_evidence() else {
        return Ok(());
    };
    let plan = command.base().commit().plan();
    if bundle.lineage() != plan.contract_lineage()
        || bundle.contract_version() != plan.contract_version()
        || bundle.bundle_hash() != plan.contract_bundle_hash()
    {
        return Err(corrupt());
    }
    for mutation in prefix
        .mutations()
        .iter()
        .filter(|m| m.namespace() == N::SecondaryIndexes)
    {
        let key = riffdb_types::IndexEntryKey::from_bytes(mutation.key().to_vec())
            .map_err(|_| corrupt())?;
        let (entity, index) = bundle
            .bundle()
            .schema()
            .entities()
            .iter()
            .find_map(|entity| {
                entity
                    .indexes()
                    .iter()
                    .find(|index| index.id() == key.index_id())
                    .map(|index| (entity, index))
            })
            .ok_or_else(corrupt)?;
        let decoded = index
            .key_schema()
            .decode_index(&key)
            .map_err(|_| corrupt())?;
        if !command.entity_transitions().iter().any(|transition| {
            transition.target().entity_type_id() == entity.id()
                && transition.target().key() == decoded.entity_key()
        }) {
            return Err(corrupt());
        }
        let Some(bytes) = mutation.value() else {
            continue;
        };
        let entry = riffdb_storage_api::decode_index_entry_v2(bytes).map_err(|_| corrupt())?;
        let entity_key = decoded.entity_key().as_bytes();
        let offset = prefix
            .mutations()
            .binary_search_by(|row| (row.namespace(), row.key()).cmp(&(N::Entities, entity_key)))
            .map_err(|_| corrupt())?;
        let image = prefix.mutations()[offset].value().ok_or_else(corrupt)?;
        let image = riffdb_storage_api::decode_entity_record_v1(image).map_err(|_| corrupt())?;
        if image.value().target().entity_type_id() != entity.id()
            || image.value().target().key() != decoded.entity_key()
        {
            return Err(corrupt());
        }
        let fields = image.value().fields();
        let values = riffdb_contract_ir::encode_operational_index_values_v1(index, fields)
            .map_err(|_| corrupt())?;
        let expected = index
            .key_schema()
            .encode_index(&values, decoded.entity_key().clone())
            .map_err(|_| corrupt())?;
        let cover = riffdb_contract_ir::encode_operational_index_cover_v1(index, fields)
            .map_err(|_| corrupt())?;
        let partition = crate::history::derive_historical_partition(
            bundle.bundle().schema(),
            entity,
            decoded.entity_key(),
        )?;
        if expected != key
            || entry.value().covered_values() != &cover
            || entry.value().partition_key() != &partition
        {
            return Err(corrupt());
        }
    }
    Ok(())
}

/// Checks the complete secondary-index mutation inventory for one command-owned
/// entity whose actual predecessor is available. Missing historical predecessors
/// must not be represented as absent; callers skip this proof until they have one.
pub fn validate_command_prefix_entity_indexes_v1(
    bundle: &ValidatedContractBundle,
    command: &StoredCommandCapsuleV2,
    transition: &riffdb_storage_api::CommittedEntityTransitionV1,
    prior: Option<&riffdb_storage_api::StoredEntityRecordV1>,
) -> Result<(), CatalogError> {
    use riffdb_storage_api::EntityChainStateV1 as State;
    let prefix = command.prefix_evidence().ok_or_else(corrupt)?;
    let plan = command.base().commit().plan();
    if bundle.lineage() != plan.contract_lineage()
        || bundle.contract_version() != plan.contract_version()
        || bundle.bundle_hash() != plan.contract_bundle_hash()
        || !command.entity_transitions().contains(transition)
    {
        return Err(corrupt());
    }
    match (transition.prior_state(), prior) {
        (
            State::Live {
                version,
                value_hash,
            },
            Some(record),
        ) if record.target() == transition.target()
            && record.entity_version() == version
            && riffdb_storage_api::derive_entity_record_hash_v1(record)
                .map_err(|_| corrupt())?
                == value_hash => {}
        (State::NeverExisted | State::Deleted, None) => {}
        _ => return Err(corrupt()),
    }
    let target = transition.target();
    let entity = bundle
        .bundle()
        .schema()
        .entity(target.entity_type_id())
        .ok_or_else(corrupt)?;
    let entity_offset = prefix
        .mutations()
        .binary_search_by(|row| {
            (row.namespace(), row.key()).cmp(&(N::Entities, target.key().as_bytes()))
        })
        .map_err(|_| corrupt())?;
    let next = prefix.mutations()[entity_offset]
        .value()
        .map(riffdb_storage_api::decode_entity_record_v1)
        .transpose()
        .map_err(|_| corrupt())?;
    if next
        .as_ref()
        .is_some_and(|row| row.value().target() != target)
    {
        return Err(corrupt());
    }
    let mut required = 0usize;
    for index in entity.indexes() {
        let old = prior.map(|row| index_image(index, row)).transpose()?;
        let new = next
            .as_ref()
            .map(|row| index_image(index, row.value()))
            .transpose()?;
        if old == new {
            continue;
        }
        if let Some((key, _)) = &old
            && new.as_ref().is_none_or(|(new_key, _)| new_key != key)
        {
            require_index_mutation(prefix, key, false, true)?;
            required += 1;
        }
        if let Some((key, _)) = &new {
            let replaces = old.as_ref().is_some_and(|(old_key, _)| old_key == key);
            require_index_mutation(prefix, key, true, replaces)?;
            required += 1;
        }
    }
    let mut supplied = 0usize;
    for row in prefix
        .mutations()
        .iter()
        .filter(|row| row.namespace() == N::SecondaryIndexes)
    {
        let key =
            riffdb_types::IndexEntryKey::from_bytes(row.key().to_vec()).map_err(|_| corrupt())?;
        if let Some(index) = entity
            .indexes()
            .iter()
            .find(|index| index.id() == key.index_id())
        {
            let decoded = index
                .key_schema()
                .decode_index(&key)
                .map_err(|_| corrupt())?;
            if decoded.entity_key() == target.key() {
                supplied += 1;
            }
        }
    }
    if supplied != required {
        return Err(corrupt());
    }
    Ok(())
}

fn index_image(
    index: &riffdb_contract_ir::IndexSchema,
    record: &riffdb_storage_api::StoredEntityRecordV1,
) -> Result<(riffdb_types::IndexEntryKey, riffdb_types::CanonicalRecord), CatalogError> {
    let values = riffdb_contract_ir::encode_operational_index_values_v1(index, record.fields())
        .map_err(|_| corrupt())?;
    let key = index
        .key_schema()
        .encode_index(&values, record.target().key().clone())
        .map_err(|_| corrupt())?;
    let cover = riffdb_contract_ir::encode_operational_index_cover_v1(index, record.fields())
        .map_err(|_| corrupt())?;
    Ok((key, cover))
}

fn require_index_mutation(
    prefix: &riffdb_storage_api::CommandPrefixEvidenceV1,
    key: &riffdb_types::IndexEntryKey,
    put: bool,
    has_prior: bool,
) -> Result<(), CatalogError> {
    let offset = prefix
        .mutations()
        .binary_search_by(|row| {
            (row.namespace(), row.key()).cmp(&(N::SecondaryIndexes, key.as_bytes()))
        })
        .map_err(|_| corrupt())?;
    let row = &prefix.mutations()[offset];
    if row.value().is_some() != put || row.expected_hash().is_some() != has_prior {
        return Err(corrupt());
    }
    Ok(())
}
