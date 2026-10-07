//! Read-only accounting uses the same retained-view validation as native pruning.
use super::*;
use crate::journal::JournalTable;
use crate::store::RedbReadAccess;
use std::collections::BTreeMap;

#[path = "logical_observer.rs"]
mod observer;
pub(super) use observer::Observer;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Bytes {
    values: u64,
    keys: u64,
}

struct CountingViews<'a> {
    read: &'a RedbReadAccess,
    changed: BTreeMap<(JournalTable, Vec<u8>), Change>,
    workspace: usize,
}

struct Change {
    original: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
}

impl<'a> CountingViews<'a> {
    fn new(read: &'a RedbReadAccess) -> Self {
        Self {
            read,
            changed: BTreeMap::new(),
            workspace: 0,
        }
    }

    fn stage(
        &mut self,
        table: JournalTable,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let prior = self.get(table, key)?;
        let index = (table, key.to_vec());
        let existing = self.changed.get(&index);
        let original_len = existing.map_or_else(
            || prior.as_ref().map_or(0, Vec::len),
            |change| change.original.as_ref().map_or(0, Vec::len),
        );
        let old_size = existing.map_or(0, |change| {
            key.len() + 64 + original_len + change.after.as_ref().map_or(0, Vec::len)
        });
        let new_size = key
            .len()
            .checked_add(64)
            .and_then(|n| n.checked_add(original_len))
            .and_then(|n| n.checked_add(value.map_or(0, <[u8]>::len)))
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let workspace = self
            .workspace
            .checked_sub(old_size)
            .and_then(|n| n.checked_add(new_size))
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        if workspace > crate::journal::MAX_JOURNAL_FRAME_BYTES
            || (existing.is_none()
                && self.changed.len() >= riffdb_storage_api::MAX_CHANGELOG_FRAME_ENTRIES)
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        self.workspace = workspace;
        self.changed
            .entry(index)
            .and_modify(|change| change.after = value.map(<[u8]>::to_vec))
            .or_insert_with(|| Change {
                original: prior.clone(),
                after: value.map(<[u8]>::to_vec),
            });
        Ok(prior)
    }

    fn bytes(&self) -> Result<Bytes, StorageError> {
        let mut bytes = Bytes::default();
        for ((table, key), change) in &self.changed {
            if change.original != change.after
                && let Some(original) = &change.original
            {
                // Retained full records stay in their separate resource model,
                // including a compatible envelope rewrite. Only superseded
                // command locators and their removed lookup keys are eligible.
                if change.after.is_none() {
                    if !matches!(
                        table,
                        JournalTable::IdempotencyLocators
                            | JournalTable::ProvenanceLocators
                            | JournalTable::AuditByRequestLocators
                    ) {
                        return Err(storage_error(StorageErrorKind::InvariantViolation));
                    }
                    bytes.add(original.len(), key.len())?;
                } else if match table {
                    JournalTable::Idempotency | JournalTable::Provenance => {
                        riffdb_storage_api::decode_command_locator_v1(original).is_ok()
                    }
                    JournalTable::Audit => {
                        riffdb_storage_api::decode_command_audit_locator_v1(original).is_ok()
                    }
                    _ => false,
                } {
                    bytes.add(original.len(), 0)?;
                }
            }
        }
        Ok(bytes)
    }
}

impl Bytes {
    fn add(&mut self, values: usize, keys: usize) -> Result<(), StorageError> {
        let error = || storage_error(StorageErrorKind::LimitExceeded);
        self.values = self
            .values
            .checked_add(u64::try_from(values).map_err(|_| error())?)
            .ok_or_else(error)?;
        self.keys = self
            .keys
            .checked_add(u64::try_from(keys).map_err(|_| error())?)
            .ok_or_else(error)?;
        Ok(())
    }
    fn extend(&mut self, other: Self) -> Result<(), StorageError> {
        self.values = self
            .values
            .checked_add(other.values)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        self.keys = self
            .keys
            .checked_add(other.keys)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        Ok(())
    }
}

impl RetainedViewAccess for CountingViews<'_> {
    fn get(&self, table: JournalTable, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(value) = self.changed.get(&(table, key.to_vec())) {
            return Ok(value.after.clone());
        }
        self.read.read_value(table, key)
    }
    fn put(
        &mut self,
        table: JournalTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.stage(table, key, Some(value))
    }
    fn remove(&mut self, table: JournalTable, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.stage(table, key, None)
    }
}

#[cfg(test)]
#[path = "eligible_bytes_tests.rs"]
mod tests;
