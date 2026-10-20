//! Strict recognition of already-activated V3 before any legacy migration.
//! This is bounded format/root validation, not activation or complete startup.

use super::*;

pub(super) fn classify_read(
    transaction: &ReadTransaction,
    tables: &BTreeSet<String>,
    multimaps: &BTreeSet<String>,
) -> Result<Option<LayoutState>, StorageError> {
    if !tables.contains(META.name())
        || !crate::changelog_v3_journal::has_recovery_roots(transaction)?
    {
        return Ok(None);
    }
    let meta = transaction.open_table(META).map_err(table_error)?;
    let catalog = crate::catalog_dispatch::declared_catalog(&meta)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    exact_current_tables(tables, multimaps, catalog)?;
    require_current_format(&meta)?;
    drop(meta);
    crate::changelog_v3_roots::read_checkpoint_roots_for_registry_completion(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    Ok(Some(LayoutState::Initialized))
}

pub(super) fn classify_write(
    transaction: &WriteTransaction,
    tables: &BTreeSet<String>,
    multimaps: &BTreeSet<String>,
) -> Result<Option<LayoutState>, StorageError> {
    if !tables.contains(META.name())
        || !crate::changelog_v3_journal::has_write_recovery_roots(transaction)?
    {
        return Ok(None);
    }
    let meta = transaction.open_table(META).map_err(table_error)?;
    let catalog = crate::catalog_dispatch::declared_catalog(&meta)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    exact_current_tables(tables, multimaps, catalog)?;
    require_current_format(&meta)?;
    drop(meta);
    crate::changelog_v3_roots::read_checkpoint_roots_for_write(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    Ok(Some(LayoutState::Initialized))
}

pub(crate) fn exact_current_tables(
    tables: &BTreeSet<String>,
    multimaps: &BTreeSet<String>,
    catalog: [u8; 32],
) -> Result<(), StorageError> {
    // Select only the exact declared catalog; never repair missing authority or
    // infer source activation from a newly recognized physical table.
    crate::catalog_dispatch::exact_physical_tables(catalog, tables, multimaps)
}

fn require_current_format(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<(), StorageError> {
    let encoded = meta
        .get(META_FORMAT_VERSION)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if encoded.value().len() > 512 {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    if *decode_storage_format_version_v1(encoded.value())
        .map_err(crate::error::codec_error)?
        .value()
        != StorageFormatVersion::V2
    {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    Ok(())
}
