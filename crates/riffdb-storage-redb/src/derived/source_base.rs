//! Persisted generation-base reads and refusal-only boundary checks.
//! A Base is structural: it cannot replace selected Source custody/full-tail
//! verification. No seeder, Source factory or MetadataReader is implemented.
use super::*;
use redb::{TableDefinition, TableHandle};
use riffdb_storage_api::{
    GenerationSourceBaseV1, ProjectionSourceRecordDecodeErrorV1,
    decode_generation_source_base_admitted_v1,
};
use riffdb_types::{ProjectionSourceWorkAllocationV1, ProjectionSourceWorkingBudgetV1};

pub(super) const BASES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("projection_generation_source_base");

struct BaseKey {
    bytes: [u8; riffdb_types::MAX_KEY_BYTES],
    len: usize,
}
impl BaseKey {
    fn new(
        identity: &ProjectionIdentity,
        generation: ProjectionGeneration,
    ) -> Result<Self, StorageError> {
        let mut key = Self {
            bytes: [0; riffdb_types::MAX_KEY_BYTES],
            len: 0,
        };
        key.len = crate::source_keys::encode_generation_source_base_key_slice_into(
            &mut key.bytes,
            identity,
            generation,
        )
        .map_err(|_| corrupt())?;
        Ok(key)
    }
    fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

// Only an exact native row's existence is observed. It never grants Source
// closure, so callers without the canonical Source owner must refuse.
pub(super) fn exists(
    transaction: &redb::ReadTransaction,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Result<bool, StorageError> {
    let table = match transaction.open_table(BASES) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
        Err(error) => return Err(table_error(error)),
    };
    let key = BaseKey::new(identity, generation)?;
    let present = table
        .get(key.as_bytes())
        .map_err(precommit_storage_error)?
        .is_some();
    Ok(present)
}

pub(super) fn read_read(
    transaction: &redb::ReadTransaction,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    frontier: FrontierPosition,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<Option<ProjectionSourceWorkAllocationV1<GenerationSourceBaseV1>>, StorageError> {
    let table = match transaction.open_table(BASES) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(table_error(error)),
    };
    read(&table, identity, generation, frontier, budget)
}

pub(super) fn exists_write(
    transaction: &redb::WriteTransaction,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Result<bool, StorageError> {
    if !transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .any(|table| table.name() == "projection_generation_source_base")
    {
        return Ok(false);
    }
    let table = transaction.open_table(BASES).map_err(table_error)?;
    let key = BaseKey::new(identity, generation)?;
    let present = table
        .get(key.as_bytes())
        .map_err(precommit_storage_error)?
        .is_some();
    Ok(present)
}

pub(super) fn read_write(
    transaction: &redb::WriteTransaction,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    frontier: FrontierPosition,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<Option<ProjectionSourceWorkAllocationV1<GenerationSourceBaseV1>>, StorageError> {
    // open_table would CREATE an empty table. Preserve old unseeded writes.
    if !exists_write(transaction, identity, generation)? {
        return Ok(None);
    }
    let table = transaction.open_table(BASES).map_err(table_error)?;
    read(&table, identity, generation, frontier, budget)
}

fn read<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    frontier: FrontierPosition,
    budget: &ProjectionSourceWorkingBudgetV1,
) -> Result<Option<ProjectionSourceWorkAllocationV1<GenerationSourceBaseV1>>, StorageError> {
    let key = BaseKey::new(identity, generation)?;
    let Some(encoded) = table.get(key.as_bytes()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let base = decode_generation_source_base_admitted_v1(encoded.value(), budget)
        .map_err(|error| match error {
            ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
                storage_error(StorageErrorKind::Unavailable)
            }
            ProjectionSourceRecordDecodeErrorV1::Codec(error) => crate::error::codec_error(error),
        })?
        .into_parts()
        .0;
    // The value repeats the full identity and generation; the native key cannot
    // substitute for them. A seed above its retained frontier is corrupt.
    crate::source_keys::check_generation_source_base_key(
        key.as_bytes(),
        base.reference().identity(),
        base.generation(),
    )
    .map_err(|_| corrupt())?;
    if base.reference().identity() != identity
        || base.generation() != generation
        || !sequence_is_at_or_before(base.applied_through(), frontier)
    {
        return Err(corrupt());
    }
    Ok(Some(base))
}

pub(crate) fn require_unseeded_control(
    transaction: &redb::ReadTransaction,
    control: &StoredProjectionControlV1,
) -> Result<(), StorageError> {
    for position in [control.published(), control.candidate()]
        .into_iter()
        .flatten()
    {
        if exists(transaction, control.identity(), position.generation())? {
            // A scalar cache frontier/Base is not D11 authoritative closure.
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
    }
    Ok(())
}
