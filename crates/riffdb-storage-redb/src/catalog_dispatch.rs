//! Exact closed catalog dispatch for native physical/state inventories.
//! New source domains cannot enter a predecessor catalog by table-name guess.

use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2,
    AuthoritativeStateCatalogV3, StorageError, StorageErrorKind,
};

pub(crate) fn namespaces(catalog: [u8; 32]) -> Result<impl Iterator<Item = N>, StorageError> {
    if catalog != AuthoritativeStateCatalogV1.digest()
        && catalog != AuthoritativeStateCatalogV2.digest()
        && catalog != AuthoritativeStateCatalogV3.digest()
    {
        return Err(crate::error::storage_error(StorageErrorKind::CorruptData));
    }
    Ok(AuthoritativeStateCatalogV3
        .namespaces()
        .filter(move |namespace| {
            let selected = if catalog == AuthoritativeStateCatalogV1.digest() {
                AuthoritativeStateCatalogV1
                    .by_tag(namespace.tag())
                    .map(N::from)
            } else if catalog == AuthoritativeStateCatalogV2.digest() {
                AuthoritativeStateCatalogV2
                    .by_tag(namespace.tag())
                    .map(N::from)
            } else {
                AuthoritativeStateCatalogV3.by_tag(namespace.tag())
            };
            selected == Some(*namespace)
        }))
}

pub(crate) fn requires_primary_admission(catalog: [u8; 32]) -> Result<bool, StorageError> {
    if catalog == AuthoritativeStateCatalogV1.digest() {
        Ok(false)
    } else if catalog == AuthoritativeStateCatalogV2.digest()
        || catalog == AuthoritativeStateCatalogV3.digest()
    {
        Ok(true)
    } else {
        Err(crate::error::storage_error(
            StorageErrorKind::IncompatibleFormat,
        ))
    }
}

pub(crate) fn lookup(
    catalog: [u8; 32],
    table: &str,
    key: &[u8],
) -> Result<Option<N>, StorageError> {
    Ok(namespaces(catalog)?.find(|namespace| {
        namespace.table() == table
            && namespace
                .metadata_key()
                .is_none_or(|expected| expected.as_bytes() == key)
    }))
}

pub(crate) fn primary_physical_namespace(namespace: N) -> bool {
    // Legacy empty projection tables remain in the old frozen physical layout.
    // The new base domain is exclusively in the derived engine (ADR-0316).
    namespace != N::GenerationSourceBases && !namespace.is_retired_locator_table()
}

/// Read only the exact declared catalog. Absence is not activation authority.
pub(crate) fn declared_catalog(
    meta: &impl redb::ReadableTable<&'static str, &'static [u8]>,
) -> Result<Option<[u8; 32]>, StorageError> {
    let key = riffdb_storage_api::AuthoritativeNamespaceV1::AuthoritativeStateCatalog
        .metadata_key()
        .ok_or_else(|| crate::error::storage_error(StorageErrorKind::InvariantViolation))?;
    meta.get(key)
        .map_err(crate::error::precommit_storage_error)?
        .map(|value| {
            if value.value().len() > 512 {
                return Err(crate::error::storage_error(StorageErrorKind::LimitExceeded));
            }
            // Exact closed outer decoder dispatch; no source-worker ledger,
            // Source DTO allocation or registry/activation decision is created.
            crate::primary_admission_roots::catalog_digest(value.value())
        })
        .transpose()
}

pub(crate) fn exact_physical_tables(
    catalog: [u8; 32],
    tables: &std::collections::BTreeSet<String>,
    multimaps: &std::collections::BTreeSet<String>,
) -> Result<(), StorageError> {
    let expected: std::collections::BTreeSet<_> = namespaces(catalog)?
        .filter(|namespace| primary_physical_namespace(*namespace))
        .map(N::table)
        .collect();
    let active: std::collections::BTreeSet<_> = tables
        .iter()
        .map(String::as_str)
        .filter(|name| {
            !matches!(
                *name,
                "idempotency_locators" | "provenance_locators" | "audit_by_request_locators"
            )
        })
        .collect();
    if !multimaps.is_empty() || active != expected {
        return Err(crate::error::storage_error(
            StorageErrorKind::IncompatibleFormat,
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "catalog_dispatch_tests.rs"]
mod tests;
