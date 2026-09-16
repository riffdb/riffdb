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
        if expected != key || entry.value().covered_values() != &cover {
            return Err(corrupt());
        }
    }
    Ok(())
}
