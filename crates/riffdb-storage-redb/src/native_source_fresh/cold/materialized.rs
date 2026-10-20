//! Additional bounded origin proof for the closed same-store materialization path.
//! The materialized read stays unbranded and remains the complete scan's pin.
use super::*;

const FIXED_ROOTS: [AuthoritativeNamespaceV1; 5] = [
    AuthoritativeNamespaceV1::AuthoritativeStateCatalog,
    AuthoritativeNamespaceV1::ChangelogHistoryState,
    AuthoritativeNamespaceV1::NextChangelogTransaction,
    AuthoritativeNamespaceV1::LeadershipEpoch,
    AuthoritativeNamespaceV1::ReplicationFollowerState,
];

pub(crate) fn install_actual_origin_complement(
    shared: &SharedRedb,
    materialized: &StartupOriginReadV1<'_, '_>,
    lease: &ExclusiveLease,
) -> Result<(), StorageError> {
    // Existing callers create this materialized pin immediately from this same
    // store. Neither this entry nor equal preimages brands that retained read.
    let retained = materialized.read()?;
    let meta = retained.open_table(META).map_err(table_error)?;
    let declared = crate::catalog_dispatch::declared_catalog(&meta)?;
    drop(meta);
    if declared != Some(AuthoritativeStateCatalogV3.digest()) {
        return validate_bounded_cold_origin(materialized, lease)?
            .install_after_complete_bounded_validation();
    }

    // Only this internally minted read can supply actual-owner origin authority.
    // The existing lease precedes capture and remains held through installation.
    let actual = StartupOriginReadV1::from_engine(shared, lease)?;
    compare_bounded_preimages(actual.read()?, retained)?;
    validate_bounded_cold_origin(&actual, lease)?.install_after_complete_bounded_validation()?;
    validate_same_owner_cold_origin(&shared.database, retained)
}

fn compare_bounded_preimages(
    actual: &redb::ReadTransaction,
    retained: &redb::ReadTransaction,
) -> Result<(), StorageError> {
    let actual_meta = actual.open_table(META).map_err(table_error)?;
    let retained_meta = retained.open_table(META).map_err(table_error)?;
    // This includes exact history anchor/tail/minimum resume and frontiers,
    // both sequence allocators, admission, catalog/incarnation/epoch, original
    // registry state and the permanent source ordinal. Comparison allocates no
    // first-party graph and does not replace either native read.
    for key in INITIAL_KEYS
        .into_iter()
        .map(Ok::<_, StorageError>)
        .chain(
            FIXED_ROOTS
                .into_iter()
                .map(|namespace| namespace.metadata_key().ok_or_else(invariant)),
        )
        .chain(std::iter::once(crate::primary_admission_roots::key()))
    {
        let key = key?;
        let a = actual_meta.get(key).map_err(precommit_storage_error)?;
        let b = retained_meta.get(key).map_err(precommit_storage_error)?;
        compare_value(
            a.as_ref().map(|value| value.value()),
            b.as_ref().map(|value| value.value()),
            MAX_PROJECTION_SOURCE_CONTROL_BYTES,
        )?;
    }
    let actual_active = actual.open_table(CATALOG_ACTIVE).map_err(table_error)?;
    let retained_active = retained.open_table(CATALOG_ACTIVE).map_err(table_error)?;
    let a = actual_active
        .get(CATALOG_ACTIVE_KEY.as_slice())
        .map_err(precommit_storage_error)?;
    let b = retained_active
        .get(CATALOG_ACTIVE_KEY.as_slice())
        .map_err(precommit_storage_error)?;
    compare_value(
        a.as_ref().map(|value| value.value()),
        b.as_ref().map(|value| value.value()),
        MAX_PROJECTION_SOURCE_CONTROL_BYTES,
    )?;
    // The currently supported closed cold recipe is FreshV3 at administration1.
    // Cut/inheritance still require their actual independent cold owners.
    let key = crate::keys::encode_audit_key(AdministrationSequence::first());
    let actual_audit = actual.open_table(AUDIT).map_err(table_error)?;
    let retained_audit = retained.open_table(AUDIT).map_err(table_error)?;
    let a = actual_audit
        .get(key.as_slice())
        .map_err(precommit_storage_error)?;
    let b = retained_audit
        .get(key.as_slice())
        .map_err(precommit_storage_error)?;
    compare_value(
        a.as_ref().map(|value| value.value()),
        b.as_ref().map(|value| value.value()),
        ORIGIN_CAP,
    )
}

fn compare_value(a: Option<&[u8]>, b: Option<&[u8]>, maximum: usize) -> Result<(), StorageError> {
    if a.is_some_and(|bytes| bytes.len() > maximum)
        || b.is_some_and(|bytes| bytes.len() > maximum)
        || a != b
    {
        return Err(value_error(()));
    }
    Ok(())
}
