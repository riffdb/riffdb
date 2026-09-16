//! Bind supplied secondary-index images to retained schema and entity evidence.
//! Complete mutation inventory and historical prior-image proofs are separate.

use redb::ReadableTable;
use riffdb_catalog::ValidatedContractBundle;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, StorageError, StorageErrorKind, StoredCommandCapsuleV2,
};

use crate::error::{precommit_storage_error, storage_error};

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

pub(crate) fn validate_catalog_images<'a>(
    bundles: &impl ReadableTable<&'static [u8], &'static [u8]>,
    commands: impl IntoIterator<Item = &'a StoredCommandCapsuleV2>,
) -> Result<(), StorageError> {
    // Keep at most one decoded bundle, irrespective of retained history length.
    let mut cached: Option<ValidatedContractBundle> = None;
    for command in commands {
        let Some(prefix) = command.prefix_evidence() else {
            continue;
        };
        if !prefix
            .mutations()
            .iter()
            .any(|m| m.namespace() == N::SecondaryIndexes)
        {
            continue;
        }
        let plan = command.base().commit().plan();
        if cached.as_ref().is_none_or(|bundle| {
            bundle.lineage() != plan.contract_lineage()
                || bundle.contract_version() != plan.contract_version()
                || bundle.bundle_hash() != plan.contract_bundle_hash()
        }) {
            let key = crate::keys::encode_contract_bundle_key(
                plan.contract_lineage(),
                plan.contract_version(),
            )
            .map_err(|_| corrupt())?;
            let bytes = bundles
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            let stored = crate::codec::decode_contract_bundle_v1(bytes.value())?;
            let bundle =
                ValidatedContractBundle::from_stored(stored.value()).map_err(|_| corrupt())?;
            if bundle.lineage() != plan.contract_lineage()
                || bundle.contract_version() != plan.contract_version()
                || bundle.bundle_hash() != plan.contract_bundle_hash()
            {
                return Err(corrupt());
            }
            cached = Some(bundle);
        }
        let bundle = cached.as_ref().ok_or_else(corrupt)?;
        riffdb_catalog::validate_command_prefix_index_images_v1(bundle, command)
            .map_err(|_| corrupt())?;
    }
    Ok(())
}
