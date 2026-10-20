//! Borrowed inspection of command authority before builder allocation.

use std::ops::Bound::{Excluded, Included, Unbounded};

use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::CommitSequence;

use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::journal::JournalTable;
use crate::keys::decode_application_sequence_key;
use crate::store::RedbReadAccess;

impl RedbReadAccess {
    pub(crate) fn resolve_command_segment_value<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<std::borrow::Cow<'a, [u8]>, StorageError> {
        let root = match self {
            Self::Current(root) | Self::Durable(root) => root.as_ref(),
            Self::Composite(view) => view.checkpoint_root(),
        };
        root.resolve_command_segment_value(bytes)
    }

    pub(crate) fn visit_next_locator_row<T>(
        &self,
        table_kind: JournalTable,
        after: Option<&[u8]>,
        visit: impl FnOnce(&[u8], &[u8]) -> Result<T, StorageError>,
    ) -> Result<Option<T>, StorageError> {
        let root = match self {
            Self::Current(root) | Self::Durable(root) => root.as_ref(),
            Self::Composite(view) => view.checkpoint_root(),
        };
        let table = root
            .journal_byte_table(table_kind)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .map_err(table_error)?;
        const MAX_KEY: usize = 1024;
        if after.is_some_and(|key| key.len() >= MAX_KEY) {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let bound = after.map_or(Included(b"".as_slice()), Excluded);
        let mut rows = table
            .range::<&[u8]>((bound, Unbounded))
            .map_err(precommit_storage_error)?;
        let mut base = rows.next().transpose().map_err(precommit_storage_error)?;
        let mut cursor = [0u8; MAX_KEY];
        let mut cursor_len = after.map(|key| {
            cursor[..key.len()].copy_from_slice(key);
            key.len()
        });
        let end = [u8::MAX; MAX_KEY];
        // Every skipped row is one overlay tombstone, whose population already
        // has a compiled ceiling. A missing/corrupt key never becomes absence.
        for _ in 0..=riffdb_storage_api::MAX_COMPOSITE_OVERLAY_TRANSITIONS {
            let overlay = match self {
                Self::Composite(view) => view.overlay().borrowed_provider_next_v1(
                    table_kind.composite(),
                    b"",
                    cursor_len.map(|len| &cursor[..len]),
                    &end,
                ),
                _ => None,
            };
            if let Some((key, value)) = overlay
                && base
                    .as_ref()
                    .is_none_or(|(base_key, _)| key <= base_key.value())
            {
                if key.len() >= MAX_KEY {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                if let Some(value) = value {
                    return visit(key, value).map(Some);
                }
                if base
                    .as_ref()
                    .is_some_and(|(base_key, _)| key == base_key.value())
                {
                    base = rows.next().transpose().map_err(precommit_storage_error)?;
                }
                cursor[..key.len()].copy_from_slice(key);
                cursor_len = Some(key.len());
                continue;
            }
            return base
                .map(|(key, value)| visit(key.value(), value.value()))
                .transpose();
        }
        Err(storage_error(StorageErrorKind::LimitExceeded))
    }

    pub(crate) fn visit_command_rows(
        &self,
        cancelled: &mut impl FnMut() -> bool,
        unavailable: fn() -> StorageError,
        mut visit: impl FnMut(CommitSequence, &[u8]) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        self.visit_locator_rows(
            JournalTable::Commits,
            cancelled,
            unavailable,
            |key, value| {
                let sequence = decode_application_sequence_key(key)
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                visit(sequence, value)
            },
        )
    }

    pub(crate) fn visit_locator_rows(
        &self,
        table_kind: JournalTable,
        cancelled: &mut impl FnMut() -> bool,
        unavailable: fn() -> StorageError,
        mut visit: impl FnMut(&[u8], &[u8]) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        const MAX_KEY: usize = 1024;
        if cancelled() {
            return Err(unavailable());
        }
        let has_overlay = match self {
            Self::Composite(view) => view
                .overlay()
                .borrowed_provider_next_v1(table_kind.composite(), b"", None, &[u8::MAX; MAX_KEY])
                .is_some(),
            Self::Current(_) | Self::Durable(_) => false,
        };
        if !has_overlay {
            let root = match self {
                Self::Current(root) | Self::Durable(root) => root.as_ref(),
                Self::Composite(view) => view.checkpoint_root(),
            };
            let table = root
                .journal_byte_table(table_kind)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .map_err(table_error)?;
            let mut rows = table
                .range::<&[u8]>((Included(b"".as_slice()), Unbounded))
                .map_err(precommit_storage_error)?;
            while let Some(entry) = rows.next().transpose().map_err(precommit_storage_error)? {
                if cancelled() {
                    return Err(unavailable());
                }
                let (key, value) = entry;
                let key_bytes = key.value();
                visit(key_bytes, value.value())?;
                if key_bytes.len() >= MAX_KEY {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
            }
            return Ok(());
        }
        let mut after: Option<Vec<u8>> = None;
        loop {
            if cancelled() {
                return Err(unavailable());
            }
            let next =
                self.visit_next_locator_row(table_kind, after.as_deref(), |key, value| {
                    if cancelled() {
                        return Err(unavailable());
                    }
                    visit(key, value)?;
                    Ok(key.to_vec())
                })?;
            let Some(next) = next else {
                return Ok(());
            };
            after = Some(next);
        }
    }
}
