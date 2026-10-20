//! Same-capture terminal proof with Source64 workspace and exact resolver.
#![cfg_attr(not(feature = "projection-source-driver"), allow(dead_code))]
use super::*;

pub(crate) fn history_source_admitted_v1(
    access: &RedbReadAccess,
    source: &crate::store::OriginalSourceReadContext<'_>,
) -> Result<ChangelogHistoryStateV3, StorageError> {
    match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            crate::changelog_v3_roots::read_checkpoint_roots_source_v1(root, source)?
                .ok_or_else(corrupt)
        }
        RedbReadAccess::Composite(view) => {
            let checkpoint = crate::changelog_v3_roots::read_checkpoint_roots_source_v1(
                view.checkpoint_root(),
                source,
            )?
            .ok_or_else(corrupt)?;
            validate_source_composite_suffix_v1(view, checkpoint, source)
        }
    }
}

/// Structural same-read position only, used to install conservative logical
/// floors. It is never a validated source or publication capability. The full
/// checker must later prove exactly this History outside the primary gate.
pub(crate) fn capture_history_position_v1(
    access: &RedbReadAccess,
) -> Result<ChangelogHistoryStateV3, StorageError> {
    match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            let meta = root.open_table(crate::layout::META).map_err(table_error)?;
            let key = riffdb_storage_api::AuthoritativeNamespaceV1::ChangelogHistoryState
                .metadata_key()
                .ok_or_else(corrupt)?;
            let value = meta
                .get(key)
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            if value.value().len() > 512 {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            riffdb_storage_api::proto_codec::decode_changelog_history_state_v3(value.value())
                .map(|decoded| *decoded.value())
                .map_err(crate::error::codec_error)
        }
        RedbReadAccess::Composite(view) => capture_history_position_v1_from_composite(view),
    }
}

pub(crate) fn validate_source_composite_suffix_v1(
    view: &crate::composite_view::RedbCompositeReadView,
    checkpoint: ChangelogHistoryStateV3,
    source: &crate::store::OriginalSourceReadContext<'_>,
) -> Result<ChangelogHistoryStateV3, StorageError> {
    let suffix = view.changelog_suffix();
    let history = suffix.history.ok_or_else(corrupt)?;
    if suffix.checkpoint != Some(checkpoint)
        || history.tail().frontier()
            != riffdb_types::DualFrontier::new(
                view.overlay().published_application(),
                view.overlay().published_administration(),
            )
    {
        return Err(corrupt());
    }
    let Some(head) = suffix.head.as_ref() else {
        return if history == checkpoint {
            Ok(history)
        } else {
            Err(corrupt())
        };
    };
    let terminal = &head.source;
    if terminal.history != history
        || terminal.binding.sequence != history.tail().sequence()
        || terminal.binding.database_id != history.lineage().database_id()
        || terminal.binding.history_incarnation != history.lineage().history_incarnation()
    {
        return Err(corrupt());
    }
    let bytes = riffdb_storage_api::provider_receipt_work_bound_v1(
        terminal.mutations.len(),
        terminal.encoded_bytes,
    )
    .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    let _work = source.admit_work(bytes)?;
    #[cfg(test)]
    crate::store::pause_original_fold_for_test()?;
    let summary = crate::changelog_v3::summary_from_validated_mutations_for_catalog(
        terminal.binding,
        terminal.attribution,
        &terminal.mutations,
        history.lineage().catalog_digest(),
    )
    .map_err(value_error)?;
    summary
        .validate_terminal_receipt(history)
        .map_err(value_error)?;
    Ok(history)
}

pub(crate) fn capture_history_position_v1_from_composite(
    view: &crate::composite_view::RedbCompositeReadView,
) -> Result<ChangelogHistoryStateV3, StorageError> {
    let history = view.changelog_suffix().history.ok_or_else(corrupt)?;
    if history.tail().frontier()
        != riffdb_types::DualFrontier::new(
            view.overlay().published_application(),
            view.overlay().published_administration(),
        )
    {
        return Err(corrupt());
    }
    Ok(history)
}
