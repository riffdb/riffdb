//! Non-recursive receipt preflight for the existing lifecycle transaction owners.
//! No transaction, commit, publication or lifecycle decision is owned here.

use super::*;
use crate::changelog_v3_write::{PreparedHistoryAdvance, value_error};
use riffdb_storage_api::{
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
};

pub(super) enum LifecycleSource {
    Clean,
    Dirty,
}

#[cfg(test)]
pub(super) fn crash_edge(edge: &str) {
    if std::env::var("RIFFDB_V3_LIFECYCLE_EDGE").ok().as_deref() == Some(edge) {
        std::process::exit(93);
    }
}

pub(super) fn prepare(
    transaction: &WriteTransaction,
    source: LifecycleSource,
    database_id: DatabaseId,
    history_incarnation: u64,
) -> Result<Option<PreparedHistoryAdvance>, StorageError> {
    // Transitional routing until the complete production activation gate lands.
    // Any surviving V3 control requires strict roots; no partial state is healed.
    if !crate::changelog_v3_journal::has_write_recovery_roots(transaction)? {
        return Ok(None);
    }
    let history = crate::changelog_v3_roots::read_checkpoint_roots_for_write(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if history.lineage().database_id() != database_id
        || history.lineage().history_incarnation() != history_incarnation
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let sequence = history
        .expected_allocator()
        .allocate_one()
        .map_err(value_error)?
        .0;
    let receipt = AuthoritativeTransactionV3::new_for_catalog(
        AuthoritativeTransactionBindingV3 {
            database_id,
            history_incarnation,
            predecessor: Some(history.tail().sequence()),
            sequence,
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: history.tail().frontier(),
            prior_history_hash: history.tail().history_hash(),
        },
        match source {
            LifecycleSource::Clean => ChangelogAttributionV3::CleanClose,
            LifecycleSource::Dirty => ChangelogAttributionV3::DirtyActivation,
        },
        Vec::new(),
        history.lineage().catalog_digest(),
    )
    .map_err(value_error)?;
    PreparedHistoryAdvance::prepare(transaction, &receipt).map(Some)
}

/// Checks bounded-start eligibility without granting authority. ADR-0156
/// Amendment 6 requires complete validation for V2: the frozen V1 binding does
/// not cover source admission. No history scan or lifecycle byte changes here.
pub(crate) fn clean_roots_available(transaction: &ReadTransaction) -> Result<bool, StorageError> {
    if !crate::changelog_v3_journal::has_recovery_roots(transaction)? {
        return Ok(true); // Transitional legacy lane; final activation closes it.
    }
    let validate = || -> Result<bool, StorageError> {
        let history = crate::changelog_v3_roots::read_checkpoint_roots(transaction)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        history
            .expected_allocator()
            .allocate_one()
            .map_err(value_error)?;
        Ok(history.lineage().catalog_digest()
            == riffdb_storage_api::AuthoritativeStateCatalogV1.digest())
    };
    match validate() {
        Ok(eligible) => Ok(eligible),
        Err(error)
            if matches!(
                error.kind(),
                StorageErrorKind::CorruptData
                    | StorageErrorKind::IncompatibleFormat
                    | StorageErrorKind::SequenceExhausted
                    | StorageErrorKind::LimitExceeded
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}
