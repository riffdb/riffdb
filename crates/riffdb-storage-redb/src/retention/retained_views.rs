//! Shared retained-view validation over direct transactions or private prune staging.

use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::journal::JournalTable;
use redb::ReadableTable;
use riffdb_storage_api::{StorageError, StorageErrorKind};

/// Private storage adapters; applications cannot supply callbacks or table access.
pub(super) trait RetainedViewAccess {
    fn get(&self, table: JournalTable, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    fn put(
        &mut self,
        table: JournalTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError>;
    fn remove(&mut self, table: JournalTable, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
}

type RetainedTable<'a> = crate::changelog_v3_capture::CapturedTable<'a, 'a, &'static [u8]>;

pub(super) struct DirectRetainedViews<'a> {
    tables: [(JournalTable, RetainedTable<'a>); 8],
}

impl<'a> DirectRetainedViews<'a> {
    pub(super) fn new(
        write: &'a crate::store::OperationalWriteTransaction,
    ) -> Result<Self, StorageError> {
        use crate::layout::*;
        Ok(Self {
            tables: [
                (
                    JournalTable::Idempotency,
                    write.open_table(IDEMPOTENCY).map_err(table_error)?,
                ),
                (
                    JournalTable::Provenance,
                    write.open_table(PROVENANCE).map_err(table_error)?,
                ),
                (
                    JournalTable::Audit,
                    write.open_table(AUDIT).map_err(table_error)?,
                ),
                (
                    JournalTable::AuditByRequest,
                    write.open_table(AUDIT_BY_REQUEST).map_err(table_error)?,
                ),
                (
                    JournalTable::EventRoutes,
                    write.open_table(EVENT_ROUTES).map_err(table_error)?,
                ),
                (
                    JournalTable::IdempotencyLocators,
                    write
                        .open_table(IDEMPOTENCY_LOCATORS)
                        .map_err(table_error)?,
                ),
                (
                    JournalTable::ProvenanceLocators,
                    write.open_table(PROVENANCE_LOCATORS).map_err(table_error)?,
                ),
                (
                    JournalTable::AuditByRequestLocators,
                    write
                        .open_table(AUDIT_BY_REQUEST_LOCATORS)
                        .map_err(table_error)?,
                ),
            ],
        })
    }

    fn table(&self, table: JournalTable) -> Result<&RetainedTable<'a>, StorageError> {
        self.tables
            .iter()
            .find(|(tag, _)| *tag == table)
            .map(|(_, table)| table)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn table_mut(&mut self, table: JournalTable) -> Result<&mut RetainedTable<'a>, StorageError> {
        self.tables
            .iter_mut()
            .find(|(tag, _)| *tag == table)
            .map(|(_, table)| table)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }
}

impl RetainedViewAccess for DirectRetainedViews<'_> {
    fn get(&self, table: JournalTable, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.table(table)?
            .get(key)
            .map(|value| value.map(|value| value.value().to_vec()))
            .map_err(precommit_storage_error)
    }
    fn put(
        &mut self,
        table: JournalTable,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.table_mut(table)?
            .insert(key, value)
            .map(|value| value.map(|value| value.value().to_vec()))
            .map_err(precommit_storage_error)
    }
    fn remove(&mut self, table: JournalTable, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.table_mut(table)?
            .remove(key)
            .map(|value| value.map(|value| value.value().to_vec()))
            .map_err(precommit_storage_error)
    }
}
