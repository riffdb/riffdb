//! Bind supplied secondary-index images to retained schema and entity evidence.
//! Known entity predecessors also prove each owned index mutation inventory.

#[path = "catalog_read.rs"]
mod read;
use read::CatalogRead;
use riffdb_catalog::ValidatedContractBundle;
use riffdb_storage_api::CatalogRepository;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, StorageError, StorageErrorKind, StoredCommandCapsuleV2,
};

use crate::error::storage_error;

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

pub(crate) fn validate_catalog_images<'a>(
    transaction: &redb::ReadTransaction,
    commands: impl IntoIterator<Item = &'a StoredCommandCapsuleV2>,
) -> Result<(), StorageError> {
    validate_catalog_sequence(CatalogRead::Retained(transaction), commands, None)
}

pub(super) fn validate_received_catalog_images<'a>(
    transaction: &redb::WriteTransaction,
    tables: &std::collections::BTreeSet<&'static str>,
    commands: impl IntoIterator<Item = &'a StoredCommandCapsuleV2>,
) -> Result<(), StorageError> {
    validate_catalog_sequence(
        CatalogRead::Received(transaction, tables),
        commands,
        Some((transaction, tables)),
    )
}

fn validate_catalog_sequence<'a>(
    repository: CatalogRead<'_>,
    commands: impl IntoIterator<Item = &'a StoredCommandCapsuleV2>,
    physical: super::predecessor::PhysicalPrior<'_>,
) -> Result<(), StorageError> {
    let mut prior = super::predecessor::PriorImages::new();
    // Keep one direct bundle and at most one bounded resolved lineage. Load
    // the latter only when raw writer and executing schema bindings differ.
    let mut cached: Option<ValidatedContractBundle> = None;
    let mut resolved: Option<riffdb_catalog::ResolvedExecutablePlan> = None;
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
            let stored = repository
                .read_contract_bundle(plan.contract_lineage(), plan.contract_version())?
                .ok_or_else(corrupt)?;
            let bundle = ValidatedContractBundle::from_stored(&stored).map_err(|_| corrupt())?;
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
            let mut check = |bytes: Option<&[u8]>| {
                let row = bytes
                    .map(riffdb_storage_api::decode_entity_record_v1)
                    .transpose()
                    .map_err(crate::error::codec_error)?;
                if row
                    .as_ref()
                    .is_some_and(|row| !row.value().schema_binding().matches_plan(plan))
                    && resolved
                        .as_ref()
                        .is_none_or(|resolved| resolved.reference() != plan)
                {
                    resolved = Some(
                        riffdb_catalog::resolve_executable_plan(&repository, plan)
                            .map_err(|_| corrupt())?,
                    );
                }
                let expected = riffdb_catalog::validate_command_prefix_entity_indexes_v1(
                    bundle,
                    command,
                    transition,
                    row.as_ref().map(|row| row.value()),
                    resolved.as_ref(),
                )
                .map_err(|_| corrupt())?;
                for expected in expected {
                    let expected = expected.map_err(|_| corrupt())?;
                    super::predecessor::with_prior(
                        physical,
                        &prior,
                        N::SecondaryIndexes,
                        expected.key().as_bytes(),
                        |bytes| {
                            let actual = bytes
                                .map(riffdb_storage_api::decode_index_entry_v2)
                                .transpose()
                                .map_err(crate::error::codec_error)?;
                            expected
                                .validate(actual.as_ref().map(|row| row.value()))
                                .map_err(|_| corrupt())
                        },
                    )?;
                }
                Ok(())
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
        // Borrow entity and index images, with explicit deletion shadows. Prefix
        // and segment/receipt bounds already cap this overlay's total size.
        for row in prefix
            .mutations()
            .iter()
            .filter(|row| matches!(row.namespace(), N::Entities | N::SecondaryIndexes))
        {
            prior.insert((row.namespace(), row.key()), row.value());
        }
    }
    Ok(())
}
