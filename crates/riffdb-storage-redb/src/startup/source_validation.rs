//! Bounded Source namespace visitation and local structural cross-links.
//! This first component cannot complete a nonempty Source graph: publication,
//! partial-copy/collection progress, exact row roots and inherited closure are
//! independent required integrations before Source activation.
use super::*;
use riffdb_storage_api::proto_codec::*;
use riffdb_storage_api::{AuthoritativeNamespaceV3 as Source, ProjectionSourceLifecycleV1};
use riffdb_types::{ProjectionGeneration, ProjectionGroupKeyBuilder, ProjectionSourceLocatorV1};

pub(super) const FIRST_PHASE: usize = 37;
pub(super) const SOURCE_PHASES: usize = 4;
pub(super) const TABLES: [Source; SOURCE_PHASES] = [
    Source::ProjectionSourceRows,
    Source::ProjectionSourceObjects,
    Source::ProjectionSourceRoots,
    Source::ProjectionSourceMaintenance,
];
const _: () =
    assert!(FIRST_PHASE == STRUCTURAL_TABLE_COUNT + EXISTING_ADDITIVE_STRUCTURAL_TABLE_COUNT);

pub(super) fn inspect_work_for_phase(
    phase: usize,
    value: &[u8],
) -> Result<ProjectionSourceCallerDecodeWorkV1, StorageError> {
    match phase {
        37 => inspect_projection_source_row_caller_decode_v1(value)
            .map_err(source_validation_admission::decode_error),
        38 => inspect_projection_source_object_caller_decode_v1(value)
            .map_err(source_validation_admission::decode_error),
        39 => inspect_projection_source_root_caller_decode_v1(value)
            .map_err(source_validation_admission::decode_error),
        40 => inspect_projection_source_maintenance_caller_decode_v1(value)
            .map_err(source_validation_admission::decode_error),
        _ => Err(invariant()),
    }
}

pub(super) fn definition(
    index: usize,
) -> Result<TableDefinition<'static, &'static [u8], &'static [u8]>, StorageError> {
    let namespace = TABLES.get(index).ok_or_else(invariant)?;
    Ok(TableDefinition::new(namespace.table()))
}
pub(super) fn counts(read: &ReadTransaction) -> Result<[u64; SOURCE_PHASES], StorageError> {
    let mut result = [0; SOURCE_PHASES];
    for (index, count) in result.iter_mut().enumerate() {
        *count = match read.open_table(definition(index)?) {
            Ok(table) => table.len().map_err(precommit_storage_error)?,
            Err(redb::TableError::TableDoesNotExist(_)) => 0,
            Err(error) => return Err(table_error(error)),
        };
    }
    Ok(result)
}
/// A canonical population cannot yet establish complete Source authority.
/// Retain typed refusal until all required graph finalizers are implemented.
pub(super) fn finish(counts: &[u64]) -> Result<(), StorageError> {
    if counts.iter().any(|count| *count != 0) {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    Ok(())
}

pub(super) fn inspect<O: EvidenceScanReadOwnerV1>(
    owner: &O,
    transaction: &ReadTransaction,
    database: DatabaseId,
    phase: usize,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let result = inspect_record(owner, transaction, database, phase, key, value);
    match result {
        Err(error) if error.kind() == StorageErrorKind::CorruptData => {
            Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)))
        }
        other => other,
    }
}
fn inspect_record<O: EvidenceScanReadOwnerV1>(
    owner: &O,
    transaction: &ReadTransaction,
    database: DatabaseId,
    phase: usize,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    use source_validation_admission::decode;
    match phase {
        37 => {
            let locator =
                crate::source_keys::source_row_locator_prefix(key).map_err(|_| corrupt())?;
            let row = decode(
                owner,
                value,
                inspect_projection_source_row_caller_decode_v1,
                decode_projection_source_row_with_caller_scratch_v1,
            )?;
            let row = row.value();
            if locator != row.locator() || locator.database_id() != database {
                return mismatch();
            }
            let work = riffdb_storage_api::ProjectionSourceRowV1::stored_parts_work_bound_v1(
                row.group_values(),
                row.measures(),
            )
            .map_err(|_| limit_exceeded())?;
            let _key_work = owner.admit_source_maintenance_decode(
                usize::try_from(work).map_err(|_| limit_exceeded())?,
            )?;
            let mut group = ProjectionGroupKeyBuilder::new(
                row.identity().clone(),
                ProjectionGeneration::first(),
            );
            for value in row.group_values() {
                group.push_component(value.clone()).map_err(|_| corrupt())?;
            }
            let group = group.finish().map_err(|_| corrupt())?;
            crate::source_keys::check_source_row_key(key, locator, &group)
                .map_err(|_| corrupt())?;
            let Some(object) = object(owner, transaction, locator)? else {
                return missing();
            };
            if object.value().descriptor().reference().identity() != row.identity()
                || row.last_changed_sequence() > object.value().descriptor().applied_through()
            {
                return mismatch();
            }
            Ok(None)
        }
        38 => {
            let locator =
                crate::source_keys::decode_source_object_key(key).map_err(|_| corrupt())?;
            let object = decode(
                owner,
                value,
                inspect_projection_source_object_caller_decode_v1,
                decode_projection_source_object_with_caller_scratch_v1,
            )?;
            if object.value().descriptor().reference().locator() != locator
                || locator.database_id() != database
            {
                return mismatch();
            }
            Ok(None)
        }
        39 => {
            let root = decode(
                owner,
                value,
                inspect_projection_source_root_caller_decode_v1,
                decode_projection_source_root_with_caller_scratch_v1,
            )?;
            let reference = root.value().reference();
            crate::source_keys::check_source_root_key(
                key,
                reference.identity(),
                reference.locator(),
            )
            .map_err(|_| corrupt())?;
            if reference.locator().database_id() != database {
                return mismatch();
            }
            let Some(object) = object(owner, transaction, reference.locator())? else {
                return missing();
            };
            if object.value().descriptor().reference() != reference
                || object.value().lifecycle() != ProjectionSourceLifecycleV1::Published
            {
                return mismatch();
            }
            Ok(None)
        }
        40 => {
            let locator =
                crate::source_keys::decode_source_maintenance_key(key).map_err(|_| corrupt())?;
            let progress = decode(
                owner,
                value,
                inspect_projection_source_maintenance_caller_decode_v1,
                decode_projection_source_maintenance_with_caller_scratch_v1,
            )?;
            if progress.value().descriptor().reference().locator() != locator
                || locator.database_id() != database
            {
                return mismatch();
            }
            let Some(object) = object(owner, transaction, locator)? else {
                return missing();
            };
            if object.value().descriptor() != progress.value().descriptor()
                || !matches!(
                    object.value().lifecycle(),
                    ProjectionSourceLifecycleV1::Copying | ProjectionSourceLifecycleV1::Aborting
                )
            {
                return mismatch();
            }
            Ok(None)
        }
        _ => Err(invariant()),
    }
}
fn object<O: EvidenceScanReadOwnerV1>(
    owner: &O,
    read: &ReadTransaction,
    locator: ProjectionSourceLocatorV1,
) -> Result<
    Option<
        source_validation_admission::MaintenanceDecodedSourceV1<
            riffdb_storage_api::ProjectionSourceObjectV1,
        >,
    >,
    StorageError,
> {
    let table = match read.open_table(definition(1)?) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(table_error(error)),
    };
    let key = crate::source_keys::source_object_key(locator);
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let object = source_validation_admission::decode(
        owner,
        value.value(),
        inspect_projection_source_object_caller_decode_v1,
        decode_projection_source_object_with_caller_scratch_v1,
    )?;
    if object.value().descriptor().reference().locator() != locator {
        return Err(corrupt());
    }
    Ok(Some(object))
}
fn missing() -> Result<Option<StructuralFinding>, StorageError> {
    Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)))
}
fn mismatch() -> Result<Option<StructuralFinding>, StorageError> {
    Ok(Some(authoritative(
        StructuralFindingCode::CrossLinkMismatch,
    )))
}
