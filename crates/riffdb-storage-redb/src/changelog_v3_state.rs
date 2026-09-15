//! Catalog-driven state stream over the same immutable checkpoint/overlay pin
//! as the V3 receipt cursor. No writable handle or durability owner is held.

use std::collections::BTreeSet;
use std::sync::Arc;

use redb::{ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV1, AuthoritativeStateCursorV3,
    AuthoritativeStateRowV3, AuthoritativeStateStepV3, ChangelogCursorErrorV3,
    ChangelogHistoryStateV3, CompositeTableV1, MAX_COMPOSITE_OVERLAY_TRANSITIONS, OverlayLookup,
    ReplicationAuthorityClassV1 as Class, StorageError, StorageErrorKind, StorageValueError,
};

use crate::{
    checkpoint_root::CheckpointRoot,
    composite_view::RedbCompositeReadView,
    error::{precommit_storage_error, storage_error, table_error},
    journal::JournalTable,
    layout::META,
    store::RedbReadAccess,
};

pub(crate) fn open(
    access: &RedbReadAccess,
) -> Result<Box<dyn AuthoritativeStateCursorV3>, ChangelogCursorErrorV3> {
    let (root, view, history) = match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => (
            Arc::clone(root),
            None,
            super::read_checkpoint_roots(root)?
                .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?,
        ),
        RedbReadAccess::Composite(view) => (
            view.checkpoint_root_shared(),
            Some(Arc::clone(view)),
            view.changelog_suffix()
                .history
                .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?,
        ),
    };
    // Reuse receipt-cursor validation of the published frontier and exact tail,
    // including a tail whose only source is the validated journal suffix.
    let verified = super::open(access, history.lineage(), history.tail())?;
    if verified.history() != history {
        return Err(super::corrupt().into());
    }
    drop(verified);
    validate_inventory(&root)?;
    Ok(Box::new(StateCursor {
        root,
        view,
        history,
        namespace_index: 0,
        last_key: None,
        failure: None,
    }))
}

fn validate_inventory(root: &CheckpointRoot) -> Result<(), StorageError> {
    let expected: BTreeSet<_> = N::ALL.into_iter().map(N::table).collect();
    let mut observed = BTreeSet::new();
    for table in root.list_tables().map_err(precommit_storage_error)? {
        let name = table.name();
        if !expected.contains(name) || !observed.insert(name.to_owned()) {
            return Err(super::corrupt());
        }
    }
    if observed.len() != expected.len() {
        return Err(super::corrupt());
    }
    let meta = root.open_table(META).map_err(table_error)?;
    for row in meta.iter().map_err(precommit_storage_error)? {
        let (key, _) = row.map_err(precommit_storage_error)?;
        if AuthoritativeStateCatalogV1
            .lookup("meta", key.value().as_bytes())
            .is_none()
        {
            return Err(super::corrupt());
        }
    }
    Ok(())
}

/// Exact attached authority reader for private bootstrap verification. A
/// follower deliberately has no source receipts; its existing attached root
/// must prove the tail instead. Source cursor admission above is unchanged.
pub(crate) fn open_attached(
    root: Arc<CheckpointRoot>,
    expected: ChangelogHistoryStateV3,
) -> Result<Box<dyn AuthoritativeStateCursorV3>, ChangelogCursorErrorV3> {
    if crate::changelog_v3_roots::validate_retained_history(&root)? != Some(expected)
        || root
            .list_multimap_tables()
            .map_err(precommit_storage_error)?
            .next()
            .is_some()
        || !crate::follower_lifecycle::is_attached(&root)?
        || !root
            .open_table(crate::changelog_v3_activation::HISTORY)
            .map_err(table_error)?
            .is_empty()
            .map_err(precommit_storage_error)?
    {
        return Err(super::corrupt().into());
    }
    validate_inventory(&root)?;
    Ok(Box::new(StateCursor {
        root,
        view: None,
        history: expected,
        namespace_index: 0,
        last_key: None,
        failure: None,
    }))
}

struct StateCursor {
    root: Arc<CheckpointRoot>,
    view: Option<Arc<RedbCompositeReadView>>,
    history: ChangelogHistoryStateV3,
    namespace_index: usize,
    last_key: Option<Box<[u8]>>,
    failure: Option<ChangelogCursorErrorV3>,
}

impl StateCursor {
    fn metadata(
        &self,
        namespace: N,
        key: &str,
    ) -> Result<Option<AuthoritativeStateRowV3>, StorageError> {
        if self.last_key.is_some() {
            return Ok(None);
        }
        if let Some(view) = &self.view {
            match view
                .overlay()
                .lookup(CompositeTableV1::Meta, key.as_bytes())
            {
                OverlayLookup::Value(value) => {
                    return checked_row(namespace, key.as_bytes(), value).map(Some);
                }
                OverlayLookup::Tombstone => return Ok(None),
                OverlayLookup::Unchanged => {}
            }
        }
        let meta = self.root.open_table(META).map_err(table_error)?;
        meta.get(key)
            .map_err(precommit_storage_error)?
            .map(|value| checked_row(namespace, key.as_bytes(), value.value()))
            .transpose()
    }

    fn next_row(&self, namespace: N) -> Result<Option<AuthoritativeStateRowV3>, StorageError> {
        if let Some(key) = namespace.metadata_key() {
            return self.metadata(namespace, key);
        }
        // Appending NUL is the least byte string strictly greater than this
        // complete key. The endpoint is transient and at most one byte larger
        // than a checked V3 row; no persisted key encoding is changed.
        let mut start = self.last_key.as_deref().unwrap_or_default().to_vec();
        if self.last_key.is_some() {
            start.push(0);
        }
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
        let table = self.root.open_table(definition).map_err(table_error)?;
        let mut rows = table
            .range::<&[u8]>(start.as_slice()..)
            .map_err(precommit_storage_error)?;
        let journal_table = JournalTable::ALL
            .into_iter()
            .find(|table| table.label() == namespace.table());
        if let (Some(view), Some(journal_table)) = (&self.view, journal_table) {
            // The existing overlay merge owns tombstone/overwrite semantics.
            // Check borrowed engine row lengths before copying into that merge.
            let base = rows.map(|row| {
                let (key, value) = row.map_err(|_| StorageValueError::InvalidShape)?;
                let row = AuthoritativeStateRowV3::new(namespace, key.value(), value.value())
                    .map_err(|_| StorageValueError::InvalidShape)?;
                let (_, key, value) = row.into_parts();
                Ok((key, value))
            });
            let page = view
                .overlay()
                .merge_bounded(
                    journal_table.composite(),
                    base,
                    &start,
                    None,
                    1,
                    MAX_COMPOSITE_OVERLAY_TRANSITIONS + 2,
                )
                .map_err(|_| super::corrupt())?;
            return page
                .rows()
                .first()
                .map(|(key, value)| checked_row(namespace, key, value))
                .transpose();
        }
        rows.next()
            .map(|row| {
                let (key, value) = row.map_err(precommit_storage_error)?;
                checked_row(namespace, key.value(), value.value())
            })
            .transpose()
    }

    fn advance(&mut self) -> Result<Option<AuthoritativeStateStepV3>, ChangelogCursorErrorV3> {
        while let Some(namespace) = N::ALL.get(self.namespace_index).copied() {
            if namespace.class() != Class::ReplicatedAuthoritative {
                self.namespace_index += 1;
                continue;
            }
            if let Some(row) = self.next_row(namespace)? {
                if self.last_key.as_deref().is_some_and(|key| row.key() <= key) {
                    return Err(super::corrupt().into());
                }
                self.last_key = Some(row.key().into());
                return Ok(Some(AuthoritativeStateStepV3::Row(row)));
            }
            self.namespace_index += 1;
            self.last_key = None;
            return Ok(Some(AuthoritativeStateStepV3::EndNamespace(namespace)));
        }
        Ok(None)
    }
}

impl AuthoritativeStateCursorV3 for StateCursor {
    fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }

    fn next_item(&mut self) -> Result<Option<AuthoritativeStateStepV3>, ChangelogCursorErrorV3> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let result = self.advance();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
}

fn checked_row(
    namespace: N,
    key: &[u8],
    value: &[u8],
) -> Result<AuthoritativeStateRowV3, StorageError> {
    AuthoritativeStateRowV3::new(namespace, key, value).map_err(super::value_error)
}
