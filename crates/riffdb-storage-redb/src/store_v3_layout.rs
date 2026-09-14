//! Strict recognition of already-activated V3 before any legacy migration.
//! This is bounded format/root validation, not activation or complete startup.

use super::*;
use riffdb_storage_api::AuthoritativeNamespaceV1 as N;

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
    exact_current_tables(tables, multimaps)?;
    require_current_format(&transaction.open_table(META).map_err(table_error)?)?;
    crate::changelog_v3_roots::read_checkpoint_roots(transaction)?
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
    exact_current_tables(tables, multimaps)?;
    require_current_format(&transaction.open_table(META).map_err(table_error)?)?;
    crate::changelog_v3_roots::read_checkpoint_roots_for_write(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    Ok(Some(LayoutState::Initialized))
}

fn exact_current_tables(
    tables: &BTreeSet<String>,
    multimaps: &BTreeSet<String>,
) -> Result<(), StorageError> {
    // One catalog-derived set, not another copied layout or additive-table
    // normalization. Active V3 never permits repairing a missing authority table.
    let expected: BTreeSet<_> = N::ALL.into_iter().map(N::table).collect();
    if !multimaps.is_empty()
        || tables.len() != expected.len()
        || tables.iter().any(|name| !expected.contains(name.as_str()))
    {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    Ok(())
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
