//! Bounded exact physical mutation capture for the private direct-write owner.
//! No population diff, raw writable-table escape, or independent commit.

use crate::{changelog_v3_write::value_error, error::storage_error};
use redb::{AccessGuard, Key, ReadableTable, Table, TableHandle};
use riffdb_storage_api::{
    AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3, AuthoritativeStateCatalogV1,
    ChangelogV3Error, MAX_CHANGELOG_FRAME_BYTES, ReplicationAuthorityClassV1, StorageError,
    StorageErrorKind,
};
use std::{
    borrow::Borrow,
    cell::{Cell, RefCell},
    ops::Deref,
};

pub(crate) struct MutationCapture {
    changes: RefCell<AuthoritativeMutationAccumulatorV3>,
    failure: Cell<Option<StorageErrorKind>>,
    inactive_legacy: bool,
}

impl Default for MutationCapture {
    fn default() -> Self {
        Self {
            changes: RefCell::default(),
            failure: Cell::new(None),
            inactive_legacy: false,
        }
    }
}

impl MutationCapture {
    // Only the transaction owner may select this after proving no V3 roots.
    // Startup's final activation gate must remove this transitional routing.
    pub(crate) fn for_inactive_legacy() -> Self {
        Self {
            inactive_legacy: true,
            ..Self::default()
        }
    }

    pub(crate) fn refuse(&self, kind: StorageErrorKind) -> StorageError {
        if self.failure.get().is_none() {
            self.failure.set(Some(kind));
        }
        storage_error(kind)
    }

    pub(crate) fn ensure_healthy(&self) -> Result<(), StorageError> {
        self.failure
            .get()
            .map_or(Ok(()), |kind| Err(storage_error(kind)))
    }

    pub(crate) fn table<'txn, 'capture, K: Key + 'static>(
        &'capture self,
        table: Table<'txn, K, &'static [u8]>,
    ) -> CapturedTable<'txn, 'capture, K> {
        CapturedTable {
            table,
            capture: self,
        }
    }

    /// A failed capture can never yield a partial receipt, even if its caller
    /// ignores a failed insert/remove and attempts to finish the transaction.
    pub(crate) fn finish(self) -> Result<Vec<AuthoritativeMutationV3>, StorageError> {
        if let Some(kind) = self.failure.get() {
            return Err(storage_error(kind));
        }
        self.changes.into_inner().finish().map_err(value_error)
    }

    fn healthy(&self) -> Result<(), redb::StorageError> {
        if self.failure.get().is_some() {
            return Err(redb::StorageError::Corrupted(
                "refused receipt capture".into(),
            ));
        }
        Ok(())
    }

    fn failed(&self, error: redb::StorageError) -> redb::StorageError {
        let kind = match &error {
            redb::StorageError::ValueTooLarge(_) => StorageErrorKind::LimitExceeded,
            redb::StorageError::Corrupted(_) => StorageErrorKind::CorruptData,
            _ => StorageErrorKind::Unavailable,
        };
        if self.failure.get().is_none() {
            self.failure.set(Some(kind));
        }
        error
    }

    pub(crate) fn record(
        &self,
        table: &str,
        key: &[u8],
        before: Option<&[u8]>,
        after: Option<&[u8]>,
    ) -> Result<(), redb::StorageError> {
        self.healthy()?;
        if self.inactive_legacy {
            return Ok(());
        }
        let result = (|| {
            let namespace = AuthoritativeStateCatalogV1
                .lookup(table, key)
                .ok_or(ChangelogV3Error::InvalidNamespace)?;
            if matches!(
                namespace.class(),
                ReplicationAuthorityClassV1::ReplicationControl(_)
            ) {
                return Err(ChangelogV3Error::InvalidNamespace);
            }
            let largest = before
                .map_or(0, <[u8]>::len)
                .max(after.map_or(0, <[u8]>::len));
            if key.is_empty() {
                return Err(ChangelogV3Error::InvalidEncoding);
            }
            if key
                .len()
                .checked_add(largest)
                .and_then(|bytes| bytes.checked_add(44))
                .is_none_or(|bytes| bytes > MAX_CHANGELOG_FRAME_BYTES)
            {
                return Err(ChangelogV3Error::LimitExceeded);
            }
            if namespace.class() == ReplicationAuthorityClassV1::RebuildableLocal || before == after
            {
                return Ok(());
            }
            let mutation = match (before, after) {
                (None, Some(after)) => AuthoritativeMutationV3::put(namespace, key, None, after),
                (Some(before), Some(after)) => {
                    AuthoritativeMutationV3::replace(namespace, key, before, after)
                }
                (Some(before), None) => {
                    AuthoritativeMutationV3::delete_matching(namespace, key, before)
                }
                (None, None) => return Ok(()),
            }?;
            self.changes
                .try_borrow_mut()
                .map_err(|_| ChangelogV3Error::InvalidEncoding)?
                .record(mutation)
        })();
        result.map_err(|error| {
            self.failed(match error {
                ChangelogV3Error::LimitExceeded => {
                    redb::StorageError::ValueTooLarge(MAX_CHANGELOG_FRAME_BYTES + 1)
                }
                _ => redb::StorageError::Corrupted("invalid receipt capture".into()),
            })
        })
    }
}

/// Only insert/remove are exposed for mutation. Read-only dereferencing keeps
/// existing table reads available but there is deliberately no DerefMut, raw
/// extraction, mutable cursor, retain callback, or unrecorded table operation.
pub(crate) struct CapturedTable<'txn, 'capture, K: Key + 'static> {
    table: Table<'txn, K, &'static [u8]>,
    capture: &'capture MutationCapture,
}

impl<'txn, K: Key + 'static> Deref for CapturedTable<'txn, '_, K> {
    type Target = Table<'txn, K, &'static [u8]>;
    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl<K: Key + 'static> redb::ReadableTableMetadata for CapturedTable<'_, '_, K> {
    fn stats(&self) -> Result<redb::TableStats, redb::StorageError> {
        self.table.stats()
    }
    fn len(&self) -> Result<u64, redb::StorageError> {
        self.table.len()
    }
}

impl<K: Key + 'static> ReadableTable<K, &'static [u8]> for CapturedTable<'_, '_, K> {
    fn get<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<AccessGuard<'_, &'static [u8]>>, redb::StorageError> {
        self.table.get(key)
    }
    fn range<'a, KR>(
        &self,
        range: impl std::ops::RangeBounds<KR> + 'a,
    ) -> Result<redb::Range<'_, K, &'static [u8]>, redb::StorageError>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.table.range(range)
    }
    fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, &'static [u8]>)>, redb::StorageError>
    {
        self.table.first()
    }
    fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, &'static [u8]>)>, redb::StorageError>
    {
        self.table.last()
    }
}

impl<K: Key + 'static> CapturedTable<'_, '_, K> {
    pub(crate) fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<&'v [u8]>,
    ) -> Result<Option<AccessGuard<'_, &'static [u8]>>, redb::StorageError> {
        self.capture.healthy()?;
        {
            let before = self
                .table
                .get(key.borrow())
                .map_err(|error| self.capture.failed(error))?;
            self.capture.record(
                self.table.name(),
                K::as_bytes(key.borrow()).as_ref(),
                before.as_ref().map(|row| row.value()),
                Some(value.borrow()),
            )?;
        }
        self.table
            .insert(key, value)
            .map_err(|error| self.capture.failed(error))
    }

    pub(crate) fn remove<'k>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
    ) -> Result<Option<AccessGuard<'_, &'static [u8]>>, redb::StorageError> {
        self.capture.healthy()?;
        {
            let before = self
                .table
                .get(key.borrow())
                .map_err(|error| self.capture.failed(error))?;
            self.capture.record(
                self.table.name(),
                K::as_bytes(key.borrow()).as_ref(),
                before.as_ref().map(|row| row.value()),
                None,
            )?;
        }
        self.table
            .remove(key)
            .map_err(|error| self.capture.failed(error))
    }
}
