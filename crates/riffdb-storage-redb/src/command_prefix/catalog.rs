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
        for row in prefix
            .mutations()
            .iter()
            .filter(|row| row.namespace() == N::VectorEvidence)
        {
            let (key, field) =
                crate::keys::decode_vector_evidence_key(row.key()).map_err(|_| corrupt())?;
            let target = riffdb_storage_api::EntityTarget::new(key.entity_type_id(), key)
                .map_err(|_| corrupt())?;
            if !riffdb_catalog::command_prefix_vector_fields_v1(bundle, command, &target)
                .map_err(|_| corrupt())?
                .any(|declared| declared == field)
            {
                return Err(corrupt());
            }
        }
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
                validate_vectors(
                    bundle,
                    command,
                    transition,
                    row.as_ref().map(|row| row.value()),
                    resolved.as_ref(),
                    physical,
                    &prior,
                )?;
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
        // Borrow entity, ordinary index and vector images with deletion shadows. Prefix
        // and segment/receipt bounds already cap this overlay's total size.
        for row in prefix.mutations().iter().filter(|row| {
            matches!(
                row.namespace(),
                N::Entities | N::SecondaryIndexes | N::VectorEvidence | N::VectorEvidenceIndex
            )
        }) {
            prior.insert((row.namespace(), row.key()), row.value());
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_vectors(
    bundle: &ValidatedContractBundle,
    command: &StoredCommandCapsuleV2,
    transition: &riffdb_storage_api::CommittedEntityTransitionV1,
    entity_prior: Option<&riffdb_storage_api::StoredEntityRecordV1>,
    resolved: Option<&riffdb_catalog::ResolvedExecutablePlan>,
    physical: super::predecessor::PhysicalPrior<'_>,
    prior: &super::predecessor::PriorImages<'_>,
) -> Result<(), StorageError> {
    use riffdb_storage_api::{
        VectorEvidenceIndexEntryV1, VectorEvidenceMutationV1 as Mutation, VectorObservationTargetV1,
    };
    let mut fields =
        riffdb_catalog::command_prefix_vector_fields_v1(bundle, command, transition.target())
            .map_err(|_| corrupt())?
            .peekable();
    if fields.peek().is_none() {
        return Ok(());
    }
    let context = riffdb_catalog::command_prefix_entity_vectors_v1(
        bundle,
        command,
        transition,
        entity_prior,
        resolved,
    )
    .map_err(|_| corrupt())?;
    let prefix = command.prefix_evidence().ok_or_else(corrupt)?;
    for field in fields {
        let key = crate::keys::encode_vector_evidence_key(transition.target().key(), field)
            .map_err(|_| corrupt())?;
        let supplied_row = prefix
            .mutations()
            .binary_search_by(|row| {
                (row.namespace(), row.key()).cmp(&(N::VectorEvidence, key.as_slice()))
            })
            .ok()
            .map(|offset| &prefix.mutations()[offset]);
        let supplied = supplied_row
            .map(|row| match row.value() {
                Some(bytes) => riffdb_storage_api::decode_vector_evidence_v1(bytes)
                    .map(|decoded| Mutation::Put(Box::new(decoded.into_parts().0)))
                    .map_err(crate::error::codec_error),
                None => Ok(Mutation::Delete {
                    target: transition.target().clone(),
                    vector_field: field,
                }),
            })
            .transpose()?;
        context
            .validate_inventory(field, supplied.as_ref())
            .map_err(|_| corrupt())?;
        let check = |bytes: Option<&[u8]>| {
            if supplied_row.is_some_and(|row| !row.matches_prior(bytes)) {
                return Err(corrupt());
            }
            let decoded = bytes
                .map(riffdb_storage_api::decode_vector_evidence_v1)
                .transpose()
                .map_err(crate::error::codec_error)?;
            let evidence = decoded.as_ref().map(|value| value.value());
            // Derive with the existing checked transition owner. Counter and
            // health arithmetic still require their own predecessor proof.
            context
                .validate(field, evidence, supplied.as_ref())
                .map_err(|_| corrupt())?;
            let target = if let Some(evidence) = evidence {
                VectorEvidenceIndexEntryV1::from_evidence(evidence)
                    .map_err(|_| corrupt())?
                    .target()
                    .clone()
            } else {
                VectorObservationTargetV1::new(
                    command.base().commit().plan().contract_lineage().clone(),
                    command.base().outcome().partition_key().clone(),
                    transition.target().entity_type_id(),
                    field,
                )
            };
            let index_key =
                crate::keys::encode_vector_evidence_index_key(&target, transition.target().key())
                    .map_err(|_| corrupt())?;
            let mutation = if supplied.is_some() {
                Some(
                    super::rows::required(prefix.mutations(), N::VectorEvidenceIndex, &index_key)
                        .map_err(crate::error::codec_error)?,
                )
            } else {
                None
            };
            super::predecessor::with_prior(
                physical,
                prior,
                N::VectorEvidenceIndex,
                &index_key,
                |bytes| {
                    if mutation.is_some_and(|row| !row.matches_prior(bytes)) {
                        return Err(corrupt());
                    }
                    let index = bytes
                        .map(riffdb_storage_api::decode_vector_evidence_index_v1)
                        .transpose()
                        .map_err(crate::error::codec_error)?;
                    match (evidence, index.as_ref().map(|value| value.value())) {
                        (None, None) => Ok(()),
                        (Some(evidence), Some(index)) if index.matches_evidence(evidence) => Ok(()),
                        _ => Err(corrupt()),
                    }
                },
            )
        };
        if physical.is_none()
            && entity_prior.is_none()
            && !prior.contains_key(&(N::VectorEvidence, key.as_slice()))
        {
            check(None)?;
        } else {
            super::predecessor::with_prior(physical, prior, N::VectorEvidence, &key, check)?;
        }
        // An unobserved retained live prior remains unknown; inventory above
        // is still proved from entity fields, never from invented evidence.
    }
    Ok(())
}
