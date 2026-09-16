//! Bind supplied secondary-index images to retained schema and entity evidence.
//! Known entity predecessors also prove each owned index mutation inventory.

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
    validate_catalog_sequence(bundles, commands, None)
}

pub(super) fn validate_received_catalog_images<'a>(
    transaction: &redb::WriteTransaction,
    tables: &std::collections::BTreeSet<&'static str>,
    bundles: &impl ReadableTable<&'static [u8], &'static [u8]>,
    commands: impl IntoIterator<Item = &'a StoredCommandCapsuleV2>,
) -> Result<(), StorageError> {
    validate_catalog_sequence(bundles, commands, Some((transaction, tables)))
}

fn validate_catalog_sequence<'a>(
    bundles: &impl ReadableTable<&'static [u8], &'static [u8]>,
    commands: impl IntoIterator<Item = &'a StoredCommandCapsuleV2>,
    physical: super::predecessor::PhysicalPrior<'_>,
) -> Result<(), StorageError> {
    let mut prior = super::predecessor::PriorImages::new();
    // Keep at most one decoded bundle, irrespective of retained history length.
    let mut cached: Option<ValidatedContractBundle> = None;
    for command in commands {
        let Some(prefix) = command.prefix_evidence() else {
            continue;
        };
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
        for transition in command.entity_transitions() {
            let check = |bytes: Option<&[u8]>| {
                let row = bytes
                    .map(riffdb_storage_api::decode_entity_record_v1)
                    .transpose()
                    .map_err(crate::error::codec_error)?;
                riffdb_catalog::validate_command_prefix_entity_indexes_v1(
                    bundle,
                    command,
                    transition,
                    row.as_ref().map(|row| row.value()),
                )
                .map_err(|_| corrupt())
            };
            if physical.is_none()
                && !matches!(
                    transition.prior_state(),
                    riffdb_storage_api::EntityChainStateV1::Live { .. }
                )
            {
                // Absence is explicit in the transition; live historical values
                // may never be invented when this segment lacks its predecessor.
                check(None)?;
            } else {
                super::predecessor::with_prior(
                    physical,
                    &prior,
                    N::Entities,
                    transition.target().key().as_bytes(),
                    check,
                )?;
            }
        }
        // Borrow only entity images, with explicit deletion shadows. Prefix
        // and segment/receipt bounds already cap this overlay's total size.
        for row in prefix
            .mutations()
            .iter()
            .filter(|row| row.namespace() == N::Entities)
        {
            prior.insert((row.namespace(), row.key()), row.value());
        }
    }
    Ok(())
}
